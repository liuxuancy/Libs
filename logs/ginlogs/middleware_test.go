package ginlogs

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liuxuancy/Libs/logs"
)

// newTestEngine 挂上两个中间件和几条测试路由，日志打到返回的 buffer 里。
func newTestEngine(t *testing.T) (*gin.Engine, *bytes.Buffer) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	output := &bytes.Buffer{}
	logs.Setup(slog.NewJSONHandler(output, nil))

	engine := gin.New()
	engine.Use(AssignLogID(), AccessLog("/api/health"))
	engine.GET("/api/echo", func(ctx *gin.Context) {
		ctx.String(http.StatusOK, logs.GetLogID(ctx.Request.Context()))
	})
	engine.GET("/api/fail", func(ctx *gin.Context) {
		ctx.Status(http.StatusBadGateway)
	})
	engine.GET("/api/health", func(ctx *gin.Context) {
		ctx.Status(http.StatusOK)
	})
	return engine, output
}

func sendRequest(engine *gin.Engine, method, path string, header http.Header) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	for key, values := range header {
		request.Header[key] = values
	}
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, request)
	return recorder
}

func TestAssignLogIDGeneratesAndReturnsHeader(t *testing.T) {
	engine, output := newTestEngine(t)
	recorder := sendRequest(engine, http.MethodGet, "/api/echo?token=secret", nil)

	logID := recorder.Header().Get(logs.LogIDHeader)
	if !logs.IsValidLogID(logID) || recorder.Body.String() != logID {
		t.Fatalf("handler ctx and response header should share the logid, header=%q body=%q", logID, recorder.Body.String())
	}

	var line map[string]any
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatalf("access log should be one json line, got %q", output.String())
	}
	if line[logs.LogIDKey] != logID || line["method"] != "GET" || line["path"] != "/api/echo" || line["status"] != float64(200) {
		t.Fatalf("bad access log: %v", line)
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatal("query string should not be logged")
	}
}

func TestAssignLogIDReusesValidUpstreamHeader(t *testing.T) {
	engine, _ := newTestEngine(t)
	recorder := sendRequest(engine, http.MethodGet, "/api/echo", http.Header{logs.LogIDHeader: {"upstream-123"}})
	if recorder.Header().Get(logs.LogIDHeader) != "upstream-123" || recorder.Body.String() != "upstream-123" {
		t.Fatalf("valid upstream logid should be reused, got %q", recorder.Header().Get(logs.LogIDHeader))
	}

	recorder = sendRequest(engine, http.MethodGet, "/api/echo", http.Header{logs.LogIDHeader: {"bad id\nx"}})
	if got := recorder.Header().Get(logs.LogIDHeader); got == "bad id\nx" || !logs.IsValidLogID(got) {
		t.Fatalf("invalid upstream logid should be replaced, got %q", got)
	}
}

func TestAccessLogLevelAndSkips(t *testing.T) {
	engine, output := newTestEngine(t)

	sendRequest(engine, http.MethodGet, "/api/fail", nil)
	if !strings.Contains(output.String(), `"level":"WARN"`) {
		t.Fatalf("5xx should be WARN, got %q", output.String())
	}

	output.Reset()
	sendRequest(engine, http.MethodGet, "/api/health", nil)
	sendRequest(engine, http.MethodOptions, "/api/echo", nil)
	if output.Len() != 0 {
		t.Fatalf("health check and OPTIONS should not be logged, got %q", output.String())
	}
}
