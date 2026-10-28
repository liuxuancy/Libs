// Package ginlogs 给 Gin 用的两个日志中间件：为每个请求分配 logid，请求结束后打一条访问日志。
package ginlogs

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liuxuancy/Libs/logs"
)

// AssignLogID 沿用请求头 X-Log-Id 里上游服务传过来的 logid（不合法就忽略），没有就新生成一个；
// 写进请求的 ctx（之后用这个 ctx 打的日志都带上它）和响应头（前端出错时可以拿给开发查问题）。
// 要挂在 AccessLog 前面，访问日志才带得上 logid。
// 全局只初始化一次：启动时挂到 gin engine 上。
func AssignLogID() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		logID := ctx.GetHeader(logs.LogIDHeader)
		if !logs.IsValidLogID(logID) {
			logID = logs.NewLogID()
		}
		ctx.Request = ctx.Request.WithContext(logs.WithLogID(ctx.Request.Context(), logID))
		ctx.Header(logs.LogIDHeader, logID)
		ctx.Next()
	}
}

// AccessLog 每个请求结束后打一条访问日志：方法、路径、状态码、耗时、客户端 IP。
// 5xx 记 WARN，其余记 INFO；OPTIONS 预检请求和 skipPaths 里的路径（比如健康检查）不记，免得刷屏。
// 路径只记 URL.Path，不记查询参数，避免把参数里的敏感内容写进日志。
// 全局只初始化一次：启动时挂到 gin engine 上。
func AccessLog(skipPaths ...string) gin.HandlerFunc {
	skippedPaths := make(map[string]bool, len(skipPaths))
	for _, path := range skipPaths {
		skippedPaths[path] = true
	}
	return func(ctx *gin.Context) {
		path := ctx.Request.URL.Path
		if ctx.Request.Method == http.MethodOptions || skippedPaths[path] {
			ctx.Next()
			return
		}

		startedAt := time.Now()
		ctx.Next()
		status := ctx.Writer.Status()
		durationMilliseconds := time.Since(startedAt).Milliseconds()

		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelWarn
		}
		message := fmt.Sprintf("%s %s %d %dms", ctx.Request.Method, path, status, durationMilliseconds)
		slog.LogAttrs(ctx.Request.Context(), level, message,
			slog.String("method", ctx.Request.Method),
			slog.String("path", path),
			slog.Int("status", status),
			slog.Int64("duration_ms", durationMilliseconds),
			slog.String("client_ip", ctx.ClientIP()),
		)
	}
}
