// Package logs 基于 slog 的日志封装：从 ctx 自动带出 log_id 等请求级字段，
// 还可以挂一个收集器，把日志攒成一批交给发送器（落库或推送到日志平台）。
//
// 用法：logs.CtxInfo(ctx, "输入的 id：%d", inputID)，占位符同 fmt（%s %v %t %d 等）。
package logs

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// stdoutHandler Setup 传进来的原始输出（一般是打到 stdout 的 JSON handler），挂收集器时要在它旁边再并一路。
var stdoutHandler slog.Handler

// Setup 用 ContextHandler 包装 inner 并设为全局默认 logger。一般直接用 Init，它会按默认格式创建 inner。
// 之后直接调用 slog.InfoContext(ctx, ...) 也会带上 ctx 字段。
// 全局只初始化一次：启动时由 main.go 调用。
func Setup(inner slog.Handler) {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	stdoutHandler = inner
	// 已经挂了收集器的话保留它们，只换掉终端这一路
	installDefaultLogger()
}

func CtxDebug(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelDebug, format, args...)
}

func CtxInfo(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelInfo, format, args...)
}

func CtxWarn(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelWarn, format, args...)
}

func CtxError(ctx context.Context, format string, args ...any) {
	write(ctx, slog.LevelError, format, args...)
}

// write 自己构造 Record，让 source 指向业务调用处而不是本包。
// 只能被 CtxXxx 直接调用，调用层级变了要同步调整 skip。
func write(ctx context.Context, level slog.Level, format string, args ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	handler := slog.Default().Handler()
	if !handler.Enabled(ctx, level) {
		return // 级别不够就不格式化，省掉 Sprintf 开销
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // 跳过 runtime.Callers、write、CtxXxx
	record := slog.NewRecord(time.Now(), level, formatMessage(format, args...), pcs[0])
	_ = handler.Handle(ctx, record)
}

// formatMessage 没有参数时原样输出，避免 "100%" 被当成占位符。
func formatMessage(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

// ShortSource 用作 HandlerOptions.ReplaceAttr，把 source 缩短为 "目录/文件.go:行号"。
func ShortSource(groups []string, attr slog.Attr) slog.Attr {
	if attr.Key != slog.SourceKey || len(groups) > 0 {
		return attr
	}
	source, ok := attr.Value.Any().(*slog.Source)
	if !ok || source == nil {
		return attr
	}
	return slog.String(slog.SourceKey, formatShortSource(source.File, source.Line))
}

// getShortSourceFromPC 把 Record 里的程序计数器换算成 "目录/文件.go:行号"，没有调用位置时返回空串。
func getShortSourceFromPC(pc uintptr) string {
	if pc == 0 {
		return ""
	}
	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	if frame.File == "" {
		return ""
	}
	return formatShortSource(frame.File, frame.Line)
}

// formatShortSource 只保留最后一级目录和文件名，日志里看得出是哪个包、又不会太长。
func formatShortSource(file string, line int) string {
	dir := filepath.Base(filepath.Dir(file))
	return dir + "/" + filepath.Base(file) + ":" + strconv.Itoa(line)
}
