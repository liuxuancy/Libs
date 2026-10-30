package logs

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// resetLoggingForTest 测试结束时收尾各路收集，恢复默认 logger。
func resetLoggingForTest(t *testing.T) {
	t.Helper()
	previous := slog.Default()
	previousStdout := stdoutHandler
	t.Cleanup(func() {
		_ = Shutdown(context.Background())
		slog.SetDefault(previous)
		stdoutHandler = previousStdout
	})
}

// fakeIngestServer 假的日志平台接收接口：记下收到的密钥和日志。
type fakeIngestServer struct {
	mutex      sync.Mutex
	ingestKeys []string
	entries    []LogEntry
}

func (server *fakeIngestServer) handleIngest(writer http.ResponseWriter, request *http.Request) {
	var batch LogBatch
	_ = json.NewDecoder(request.Body).Decode(&batch)
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.ingestKeys = append(server.ingestKeys, request.Header.Get(IngestKeyHeader))
	server.entries = append(server.entries, batch.Entries...)
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv(ServiceNameEnv, "order-service")
	t.Setenv(EnvironmentEnv, "prod")
	t.Setenv(LevelEnv, "WARN")
	t.Setenv(PlatformEnabledEnv, "true")
	t.Setenv(PlatformURLEnv, " https://example.com/api/logs/ingest ")
	t.Setenv(PlatformKeyEnv, "secret")

	cfg := LoadConfigFromEnv()
	if cfg.ServiceName != "order-service" || cfg.Environment != "prod" || cfg.Level != slog.LevelWarn {
		t.Errorf("基本配置读得不对: %+v", cfg)
	}
	if !cfg.Platform.Enabled || cfg.Platform.IngestURL != "https://example.com/api/logs/ingest" || cfg.Platform.IngestKey != "secret" {
		t.Errorf("平台配置读得不对: %+v", cfg.Platform)
	}
}

func TestLoadConfigFromEnvToleratesBadValues(t *testing.T) {
	t.Setenv(LevelEnv, "loud")
	t.Setenv(PlatformEnabledEnv, "yes please")
	cfg := LoadConfigFromEnv()
	if cfg.Level != slog.LevelInfo || cfg.Platform.Enabled {
		t.Errorf("写错的级别按 INFO、写错的开关按关闭处理，实际 %+v", cfg)
	}
}

func TestApplyConfigDefaults(t *testing.T) {
	devConfig := applyConfigDefaults(Config{})
	if devConfig.ServiceName == "" || devConfig.Environment != EnvironmentDev {
		t.Errorf("不填时服务名用程序名、环境是 dev，实际 %+v", devConfig)
	}
	if devConfig.Platform.RetryInterval != defaultRetryInterval || devConfig.Platform.MaxRetryCount != defaultMaxRetryCount ||
		devConfig.Platform.MaxRetryDuration != defaultMaxRetryDuration || devConfig.Platform.RetryCooldown >= 0 {
		t.Errorf("dev 的重试默认值不对（一轮用完不再重试）: %+v", devConfig.Platform)
	}
	if prodConfig := applyConfigDefaults(Config{Environment: EnvironmentProd}); prodConfig.Platform.RetryCooldown != defaultProdRetryCooldown {
		t.Errorf("prod 一轮用完默认冷却 10 分钟，实际 %s", prodConfig.Platform.RetryCooldown)
	}
	if badConfig := applyConfigDefaults(Config{Environment: "staging"}); badConfig.Environment != EnvironmentDev {
		t.Errorf("不认识的环境按 dev 处理，实际 %q", badConfig.Environment)
	}
}

func TestInitCopiesToPlatformWithSameEntryIDAsMainPath(t *testing.T) {
	resetLoggingForTest(t)
	ingestServer := &fakeIngestServer{}
	httpServer := httptest.NewServer(http.HandlerFunc(ingestServer.handleIngest))
	defer httpServer.Close()

	Init(Config{
		ServiceName: "order-service",
		Environment: EnvironmentDev,
		Platform:    PlatformConfig{Enabled: true, IngestURL: httpServer.URL + "/api/logs/ingest", IngestKey: "secret"},
	})
	mainRecorder := &recordingSender{}
	if err := SetupCollector(CollectorOptions{ServiceName: "order-service", Environment: EnvironmentDev, Sender: mainRecorder, FlushInterval: 20 * time.Millisecond}); err != nil {
		t.Fatalf("SetupCollector: %v", err)
	}

	CtxInfo(context.Background(), "下单成功 order_id=%d", 42)
	// Shutdown 要等两路都把剩下的日志发完才返回
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	ingestServer.mutex.Lock()
	defer ingestServer.mutex.Unlock()
	mainEntries := mainRecorder.getAllEntries()
	if len(mainEntries) != 1 || len(ingestServer.entries) != 1 {
		t.Fatalf("主路和平台各应该收到 1 条，实际 %d、%d", len(mainEntries), len(ingestServer.entries))
	}
	platformEntry := ingestServer.entries[0]
	if platformEntry.EntryID != mainEntries[0].EntryID {
		t.Errorf("同一条日志在两路的条目 ID 应该相同，实际 %s / %s", mainEntries[0].EntryID, platformEntry.EntryID)
	}
	if platformEntry.Message != "下单成功 order_id=42" || platformEntry.ServiceName != "order-service" || platformEntry.Environment != EnvironmentDev {
		t.Errorf("抄送到平台的日志内容不对: %+v", platformEntry)
	}
	if ingestServer.ingestKeys[0] != "secret" {
		t.Errorf("请求应该带上接收密钥，实际 %q", ingestServer.ingestKeys[0])
	}
}

func TestInitWithoutPlatformOnlyPrintsTerminal(t *testing.T) {
	cases := []struct {
		name     string
		platform PlatformConfig
	}{
		{"没打开开关", PlatformConfig{IngestURL: "https://example.com/api/logs/ingest", IngestKey: "secret"}},
		{"打开了但没填地址", PlatformConfig{Enabled: true, IngestKey: "secret"}},
		{"打开了但没填密钥", PlatformConfig{Enabled: true, IngestURL: "https://example.com/api/logs/ingest"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			resetLoggingForTest(t)
			Init(Config{Platform: testCase.platform})
			if platformPath != nil {
				t.Error("不应该打开平台抄送")
			}
			CtxInfo(context.Background(), "只打终端")
		})
	}
}
