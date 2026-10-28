// 在 ctx 中存取日志字段和 logid。

package logs

import (
	"context"
	"log/slog"
)

// LogIDKey 日志中 logid 的字段名。
const LogIDKey = "log_id"

type attrsKey struct{}

type logIDKey struct{}

// WithAttrs 往 ctx 追加日志字段，之后用该 ctx 打的日志都会带上。
// 例如鉴权通过后：ctx = logs.WithAttrs(ctx, slog.Int64("user_id", uid))
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	existing := getContextAttrs(ctx)
	// 复制一份再追加，避免和父 ctx 共享底层数组
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, attrsKey{}, merged)
}

// WithLogID 写入 logid（一次请求一个），同时作为日志字段，之后这个 ctx 打的日志都带上它。
func WithLogID(ctx context.Context, logID string) context.Context {
	ctx = context.WithValue(ctx, logIDKey{}, logID)
	return WithAttrs(ctx, slog.String(LogIDKey, logID))
}

// GetLogID 取出 logid，没有则返回空串。
func GetLogID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	logID, _ := ctx.Value(logIDKey{}).(string)
	return logID
}

func getContextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	attrs, _ := ctx.Value(attrsKey{}).([]slog.Attr)
	return attrs
}
