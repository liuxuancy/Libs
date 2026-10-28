# logs

基于 Go 标准库 `log/slog` 的日志库：打日志时自动带上 logid 等请求级字段；可以挂一个收集器，把日志攒成一批交给发送器（写库、推送到 MQ 或日志平台）。附带两个 Gin 中间件。

```bash
go get github.com/liuxuancy/Libs/logs@logs/v0.1.0
```

`Libs` 仓库里每个子目录是一个独立的 go module，版本标签带目录前缀，例如 `logs/v0.1.0`。仓库还没公开时，引用方要设置 `GOPRIVATE=github.com/liuxuancy/*`。

## 基本用法

```go
// 启动时设置一次：stdout 输出 JSON，source 缩短成 "目录/文件.go:行号"
logs.Setup(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
	Level:       slog.LevelInfo,
	AddSource:   true,
	ReplaceAttr: logs.ShortSource,
}))

// 打日志：占位符同 fmt；ctx 里的 log_id、WithAttrs 加的字段会自动带上
logs.CtxInfo(ctx, "认领成功 bug_id=%d", bugID)
ctx = logs.WithAttrs(ctx, slog.Int64("user_id", userID))
```

## logid 和 Gin 中间件

logid 是一次请求一个的 ID，这次请求打出的所有日志都带上它（字段名 `log_id`），在请求头和响应头里叫 `X-Log-Id`。格式是 14 位本地时间加 10 位随机十六进制，例如 `20261028142000a1b2c3d4e5`。

```go
engine := gin.New()
engine.Use(ginlogs.AssignLogID(), ginlogs.AccessLog("/api/health"), gin.Recovery())
```

- `AssignLogID`：请求头里有合法的 `X-Log-Id` 就沿用（调用方传过来的），否则新生成；写进请求 ctx 和响应头
- `AccessLog`：请求结束后打一条访问日志（方法、路径、状态码、耗时、客户端 IP），5xx 记 WARN；参数里的路径和 OPTIONS 预检不记
- 浏览器跨域调用时，要在 CORS 配置里把 `logs.LogIDHeader` 加进 `ExposeHeaders`，前端才读得到
- 调用其他服务时，把 `logs.GetLogID(ctx)` 放进请求头 `X-Log-Id`，两边的日志就能按同一个 logid 查到
- 不是请求触发的任务（定时任务等）可以每一轮自己生成一个：`ctx = logs.WithLogID(ctx, logs.NewLogID())`

## 收集器

```go
err := logs.SetupCollector(logs.CollectorOptions{
	ServiceName: "my-service",
	Environment: "prod", // prod / dev
	Sender:      sender, // 实现 logs.BatchSender
})
defer logs.ShutdownCollector(context.Background()) // 退出前把剩下的日志发完
```

- 达到 `MinLevel`（默认 INFO）的日志，除了照常输出到 Setup 的 handler，还会转成 `LogEntry` 放进内存队列
- 攒够 `BatchSize`（默认 200）条或每隔 `FlushInterval`（默认 1 秒）交给发送器一次；队列（默认 10000 条）满了直接丢弃，不阻塞业务
- 发送失败、丢弃条数只写 stderr，不走日志本身，避免死循环；发送器返回错误时这一批丢弃，需要重试的发送器自己在内部重试
- `LogEntry` 里的 `entry_id` 是每一行日志自己的 ID，接收方写库时用它去重
- `InProcessSender` 是进程内发送器：把一批日志直接交给同一进程里的一个函数

## 测试

```bash
go test ./...
```
