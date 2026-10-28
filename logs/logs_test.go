package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// setupBuffer 把默认 logger 指向内存 buffer，测试结束后恢复。
func setupBuffer(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	buffer := &bytes.Buffer{}
	Setup(slog.NewJSONHandler(buffer, &slog.HandlerOptions{
		Level:       level,
		AddSource:   true,
		ReplaceAttr: ShortSource,
	}))
	return buffer
}

func decodeLine(t *testing.T, buffer *bytes.Buffer) map[string]any {
	t.Helper()
	var line map[string]any
	if err := json.Unmarshal(buffer.Bytes(), &line); err != nil {
		t.Fatalf("not json: %q", buffer.String())
	}
	return line
}

func TestCtxInfoCarriesContextAttrs(t *testing.T) {
	buffer := setupBuffer(t, slog.LevelInfo)
	ctx := WithLogID(context.Background(), "req-1")
	ctx = WithAttrs(ctx, slog.Int64("user_id", 42))

	CtxInfo(ctx, "输入的 id：%d，名字：%s，启用：%t", 7, "tom", true)

	line := decodeLine(t, buffer)
	if line[slog.MessageKey] != "输入的 id：7，名字：tom，启用：true" {
		t.Fatalf("bad msg: %v", line[slog.MessageKey])
	}
	if line[LogIDKey] != "req-1" || line["user_id"] != float64(42) {
		t.Fatalf("missing fields: %v", line)
	}
	source, _ := line[slog.SourceKey].(string)
	if !strings.HasPrefix(source, "logs/logs_test.go:") {
		t.Fatalf("source should point to caller, got %q", source)
	}
}

func TestStdSlogAlsoCarriesContextAttrs(t *testing.T) {
	buffer := setupBuffer(t, slog.LevelInfo)
	ctx := WithLogID(context.Background(), "req-2")

	slog.InfoContext(ctx, "hello")

	if line := decodeLine(t, buffer); line[LogIDKey] != "req-2" {
		t.Fatalf("missing log_id: %v", line)
	}
}

func TestLevelFiltered(t *testing.T) {
	buffer := setupBuffer(t, slog.LevelInfo)
	CtxDebug(context.Background(), "hidden")
	if buffer.Len() != 0 {
		t.Fatalf("debug should be filtered, got %q", buffer.String())
	}
}

func TestWithAttrsDoesNotLeakToParent(t *testing.T) {
	parent := WithAttrs(context.Background(), slog.String("a", "1"))
	_ = WithAttrs(parent, slog.String("b", "2"))
	if attrs := getContextAttrs(parent); len(attrs) != 1 {
		t.Fatalf("parent attrs changed: %v", attrs)
	}
	if GetLogID(parent) != "" {
		t.Fatal("unexpected log id")
	}
}
