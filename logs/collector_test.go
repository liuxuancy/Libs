package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingSender 把收到的每一批都记下来，测试用。
type recordingSender struct {
	mutex   sync.Mutex
	batches [][]LogEntry
	err     error
}

func (sender *recordingSender) SendBatch(_ context.Context, entries []LogEntry) error {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	sender.batches = append(sender.batches, entries)
	return sender.err
}

func (sender *recordingSender) getAllEntries() []LogEntry {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	var all []LogEntry
	for _, batch := range sender.batches {
		all = append(all, batch...)
	}
	return all
}

// setupCollectorForTest 先 Setup 到内存 buffer，再挂收集器；测试结束时摘掉收集器并恢复默认 logger。
func setupCollectorForTest(t *testing.T, sender BatchSender, stdoutLevel slog.Level) *bytes.Buffer {
	t.Helper()
	previous := slog.Default()
	previousStdout := stdoutHandler
	t.Cleanup(func() {
		_ = ShutdownCollector(context.Background())
		slog.SetDefault(previous)
		stdoutHandler = previousStdout
	})

	stdout := &bytes.Buffer{}
	Setup(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: stdoutLevel}))
	err := SetupCollector(CollectorOptions{
		ServiceName:   "suguang",
		Environment:   "dev",
		Sender:        sender,
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("SetupCollector: %v", err)
	}
	return stdout
}

func TestCollectorBuildsEntryFromContextAndAttrs(t *testing.T) {
	sender := &recordingSender{}
	stdout := setupCollectorForTest(t, sender, slog.LevelInfo)

	ctx := WithLogID(context.Background(), "20261028142000a1b2c3d4e5")
	ctx = WithAttrs(ctx, slog.Int64("user_id", 42))
	CtxWarn(ctx, "支付超时 %dms", 3000)
	slog.Default().WithGroup("http").Info("带分组", slog.Int("status", 504), slog.Any("err", errors.New("boom")))

	if err := ShutdownCollector(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	entries := sender.getAllEntries()
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d: %+v", len(entries), entries)
	}

	first := entries[0]
	if first.LogID != "20261028142000a1b2c3d4e5" || first.Level != "WARN" || first.Message != "支付超时 3000ms" {
		t.Fatalf("bad entry: %+v", first)
	}
	if first.ServiceName != "suguang" || first.Environment != "dev" || first.EntryID == "" {
		t.Fatalf("missing service/env/entry id: %+v", first)
	}
	if !strings.HasPrefix(first.Source, "logs/collector_test.go:") {
		t.Fatalf("source should point to caller, got %q", first.Source)
	}
	if _, hasLogID := first.Attributes[LogIDKey]; hasLogID {
		t.Fatal("log_id should be moved out of attrs")
	}
	if first.Attributes["user_id"] != int64(42) {
		t.Fatalf("missing user_id: %+v", first.Attributes)
	}

	second := entries[1]
	if second.Attributes["http.status"] != int64(504) || second.Attributes["http.err"] != "boom" {
		t.Fatalf("group attrs should be flattened: %+v", second.Attributes)
	}
	if second.EntryID == first.EntryID {
		t.Fatal("entry ids should differ")
	}

	// stdout 照常输出，不受收集器影响
	if strings.Count(stdout.String(), "\n") != 2 {
		t.Fatalf("stdout should still get both lines, got %q", stdout.String())
	}
}

func TestCollectorSkipsBelowMinLevel(t *testing.T) {
	sender := &recordingSender{}
	stdout := setupCollectorForTest(t, sender, slog.LevelDebug)

	CtxDebug(context.Background(), "只打 stdout")
	CtxInfo(context.Background(), "两边都有")
	if err := ShutdownCollector(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	entries := sender.getAllEntries()
	if len(entries) != 1 || entries[0].Message != "两边都有" {
		t.Fatalf("debug should not be collected, got %+v", entries)
	}
	if !strings.Contains(stdout.String(), "只打 stdout") {
		t.Fatalf("debug should still reach stdout, got %q", stdout.String())
	}
}

func TestShutdownCollectorRestoresStdoutOnly(t *testing.T) {
	sender := &recordingSender{}
	setupCollectorForTest(t, sender, slog.LevelInfo)
	if err := ShutdownCollector(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	CtxInfo(context.Background(), "摘掉之后打的")
	if len(sender.getAllEntries()) != 0 {
		t.Fatal("logs after shutdown should not be collected")
	}
	if err := ShutdownCollector(context.Background()); err != nil {
		t.Fatalf("second shutdown should be a no-op: %v", err)
	}
}

func TestSetupCollectorRejectsBadOptions(t *testing.T) {
	previousStdout := stdoutHandler
	t.Cleanup(func() { stdoutHandler = previousStdout })

	stdoutHandler = nil
	if err := SetupCollector(CollectorOptions{Sender: &recordingSender{}}); err == nil {
		t.Fatal("should require Setup first")
	}
	stdoutHandler = slog.NewJSONHandler(&bytes.Buffer{}, nil)
	if err := SetupCollector(CollectorOptions{}); err == nil {
		t.Fatal("should require a sender")
	}
}

func TestConvertAttributeValueFreezesObjects(t *testing.T) {
	tags := []string{"a"}
	converted := convertAttributeValue(slog.AnyValue(tags))
	tags[0] = "changed"

	encoded, err := json.Marshal(converted)
	if err != nil || string(encoded) != `["a"]` {
		t.Fatalf("value should be frozen at log time, got %s (%v)", encoded, err)
	}
	if convertAttributeValue(slog.DurationValue(1500*time.Millisecond)) != "1.5s" {
		t.Fatal("duration should become a string")
	}
}
