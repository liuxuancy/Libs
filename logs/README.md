# logs

基于 Go 标准库 `log/slog` 的日志包：打日志时自动带上 logid 等请求级字段；配了日志平台就自动抄送一份过去；也可以挂一个收集器，把日志攒成一批交给自己的发送器（写库、推送到 MQ）。日志平台那一边用它接收日志。附带两个 Gin 中间件。

```bash
go get github.com/liuxuancy/Libs/logs@logs/v0.2.0
```

`Libs` 仓库里每个子目录是一个独立的 go module，版本标签带目录前缀，例如 `logs/v0.2.0`（v0.2.0 新增了 `logs.Init` 一步配置、抄送到日志平台，以及给日志平台用的 `logs.ReadIngestRequest` 接收）。仓库还没公开时，引用方要设置 `GOPRIVATE=github.com/liuxuancy/*`。

## 基本用法

```go
// 启动时一次：按 LOG_ 环境变量配好日志（有 .env 的程序先加载它，日志包不读文件）
logs.Init(logs.LoadConfigFromEnv())
defer logs.Shutdown(context.Background()) // 退出前把剩下的日志发完

// 打日志：占位符同 fmt；ctx 里的 log_id、WithAttrs 加的字段会自动带上
logs.CtxInfo(ctx, "认领成功 bug_id=%d", bugID)
ctx = logs.WithAttrs(ctx, slog.Int64("user_id", userID))
```

- `Init` 让终端输出 JSON（带代码位置，缩短成 `目录/文件.go:行号`）；打开了平台抄送时，还会在后台把日志抄送到日志平台（见下一节）
- 不想读环境变量，也可以自己填 `logs.Config` 传给 `Init`，字段都可以不填
- 配置写错了只往 stderr 提示一次、按默认值处理，`Init` 不会报错，也不会让程序启动失败
- 想完全自己控制终端输出的格式，可以改用 `logs.Setup(自己的 slog.Handler)`

| 环境变量 | 对应 `logs.Config` 字段 | 不设时 |
|---|---|---|
| `LOG_SERVICE_NAME` | `ServiceName`：服务名，日志平台上按它区分程序 | 程序文件名 |
| `LOG_ENVIRONMENT` | `Environment`：`prod` / `dev` | `dev` |
| `LOG_LEVEL` | `Level`：`debug` / `info` / `warn` / `error`，终端和平台都只要这个级别及以上 | `info` |
| `LOG_PLATFORM_ENABLED` | `Platform.Enabled`：是否抄送到日志平台 | 不抄送 |
| `LOG_PLATFORM_URL` | `Platform.IngestURL`：平台的日志接收接口，例如 `https://日志平台地址/api/logs/ingest` | — |
| `LOG_PLATFORM_KEY` | `Platform.IngestKey`：接收密钥，和平台配置的一致 | — |

## 抄送到日志平台

`LOG_PLATFORM_ENABLED=true` 并且填了地址和密钥，日志照常打终端，同时在后台攒批经 HTTP 发到平台。调用方不用写别的代码，照常 `logs.CtxInfo` 就行。

- **先打终端、再连平台**：终端输出不等网络；第一批发成功算连上，stderr 提示一次"已连上日志平台"
- **这一路独立**：有自己的队列和协程，平台慢、连不上都不影响终端输出，也不影响 `SetupCollector` 挂的那一路
- **连不上时重试**：每 10 秒试一次，最多 30 次或 10 分钟（先到为准）算一轮；这期间的日志只打终端、不抄送，内存不会一直涨。
  一轮用完后：`dev` 停止抄送，直到程序重启；`prod` 冷却 10 分钟再开始新的一轮，一直这样直到平台恢复。
  发成功一次就恢复，次数和计时清零。这几个数可以在 `logs.Config.Platform` 里改（`RetryInterval`、`MaxRetryCount`、`MaxRetryDuration`、`RetryCooldown`，`RetryCooldown` 小于 0 表示用完不再重试）
- **提示只在状态变化时打**：开始重试、冷却、放弃、恢复各往 stderr 写一句，不会每批刷屏
- 平台断开期间的日志在平台上会缺这一段，终端里是全的
- 日志平台本身不要打开它（会把自己的日志绕一圈发回给自己）

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
defer logs.Shutdown(context.Background()) // 退出前把各路剩下的日志发完（只收尾这一路用 ShutdownCollector）
```

自己有存储的程序（比如日志平台自己：先发 MQ、失败直接写库），用收集器挂一路自己的发送器；和平台抄送可以同时开，两路互不影响。

- 达到 `MinLevel`（默认 INFO）的日志，除了照常输出到终端，还会转成 `LogEntry` 放进内存队列
- 攒够 `BatchSize`（默认 200）条或每隔 `FlushInterval`（默认 1 秒）交给发送器一次；队列（默认 10000 条）满了直接丢弃，不阻塞业务
- 发送失败、丢弃条数只写 stderr，不走日志本身，避免死循环；发送器返回错误时这一批丢弃，需要重试的发送器自己在内部重试
- `LogEntry` 里的 `entry_id` 是每一行日志自己的 ID，接收方写库时用它去重；同一条日志进了收集器和平台抄送两路时 ID 相同
- `InProcessSender` 是进程内发送器：把一批日志直接交给同一进程里的一个函数
- `NewFallbackSender(主, 备)` 是备用发送器：主发送器失败时这一批改交给备用发送器，比如"先发 MQ，MQ 不可用时直接写库"；
  主发送器失败只在刚开始失败和恢复时各往 stderr 提示一次
- 一批日志在网络上传输的格式是 `logs.LogBatch`，即 `{"entries":[...]}`，HTTP 接收接口的请求体和 MQ 里的一条消息都是它

## HTTP 发送器

只是想把日志抄送到平台，直接用上面的 `LOG_PLATFORM_*` 环境变量更简单。要自己组装发送器时，用 HTTP 发送器把日志推到接收接口：

```go
sender := httpsender.NewSender("https://日志平台地址/api/logs/ingest", os.Getenv("LOG_INGEST_KEY"))
err := logs.SetupCollector(logs.CollectorOptions{ServiceName: "my-service", Environment: "dev", Sender: sender})
```

- 实现在 `logs.HTTPSender`（`logs.NewHTTPSender`），`httpsender.NewSender` 是它原来的名字
- 请求头 `X-Log-Ingest-Key`（`httpsender.IngestKeyHeader`）放共享密钥，请求体是 `logs.LogBatch`，即 `{"entries":[...]}`
- 网络错误或服务端 5xx 重试一次，还失败就返回错误（这一批丢弃）；4xx 说明密钥或数据有问题，不重试

## 接收日志（给日志平台用）

想自己做一个日志平台、接收别的程序发来的日志，在自己的接收接口里调用 `ReadIngestRequest`，拿到 `[]LogEntry` 后存到哪里由你决定：

```go
func handleLogIngest(writer http.ResponseWriter, request *http.Request) {
	entries, err := logs.ReadIngestRequest(request, os.Getenv("LOG_INGEST_KEY")) // 核对密钥、限制大小、解析、校验
	if err != nil {
		http.Error(writer, err.Error(), logs.GetIngestErrorStatus(err)) // 401 / 400 / 413 / 503
		return
	}
	if err := saveLogEntries(request.Context(), entries); err != nil { // 自己存：写库、写文件、发 MQ 都可以
		http.Error(writer, "日志存储失败", http.StatusServiceUnavailable) // 5xx：发送方会重试
		return
	}
	writer.WriteHeader(http.StatusOK)
}
```

- `ReadIngestRequest` 做的事：密钥为空回 503（接收没打开），密钥不对回 401，请求体超过 4MB（`logs.MaxIngestBodyBytes`）回 413，
  不是合法 JSON 回 400，再用 `ValidateLogEntries` 校验每一条（1 到 2000 条、环境是 prod 或 dev、带服务名），不合格回 400
- 拿到日志后，接收方要做到三条，否则发送方的重试和去重就不起作用：
  1. **真正存好了才回复 2xx**，存失败回复 5xx（发送方会重试）
  2. **按 `EntryID` 去重**：重试时同一条可能来两次，比如给 `entry_id` 建唯一索引、用 `INSERT IGNORE`
  3. **尽快回复**：发送方一次请求最多等 5 秒，耗时的处理不要放在这里同步做
- 用 Gin 的，在 handler 里传 `ctx.Request` 一样用，响应格式可以按自己平台的习惯来
- 不经过 HTTP 收到的日志（比如 MQ 消费者），用 `logs.ValidateLogEntries(entries)` 做同样的校验
- 落库可以参考 Issue_Platform 后端的 `modules/log_center`：`schema.go`（日志表，按天分区）、`writer.go`（多行 `INSERT IGNORE` 按条目 ID 去重）

## MQTT 发送器

能连到日志 MQ（Mosquitto 等 MQTT 服务端）的服务，用 MQTT 发送器把日志发进 MQ，由日志平台消费后写库：

```go
sender, err := mqttsender.NewSender(mqttsender.Options{
	BrokerURL: "tcp://127.0.0.1:1883",
	Username:  os.Getenv("LOG_MQ_USERNAME"),
	Password:  os.Getenv("LOG_MQ_PASSWORD"),
})
defer sender.Close()
err = logs.SetupCollector(logs.CollectorOptions{ServiceName: "my-service", Environment: "prod", Sender: sender})
```

- topic 是 `logs/<环境>/<服务名>`（`mqttsender.GetLogTopic`），日志平台订阅 `logs/#`（`mqttsender.TopicFilter`）；QoS 1，等 MQ 确认收到才返回
- 一条 MQTT 消息最多 256KB（`mqttsender.MaxPayloadBytes`），一批超过就拆成几条；单条日志自己就超过的，丢掉附加字段、截短内容
- 创建时最多等 3 秒连上 MQ，没连上也不报错，后台每隔几秒重连；没连上期间 `SendBatch` 直接返回 `mqttsender.ErrNotConnected`，
  不在内存里排队（MQ 长时间不可用会一直涨内存），想不丢就用 `logs.NewFallbackSender` 配一个备用发送器
- 连接失败（包括账号密码错误）、断开、恢复只写 stderr，并且只在状态变化时写（`mqttsender.ConnectionReporter`，自己写 MQ 客户端也可以用）
- client id 默认自动生成（`logs-` + 10 位随机十六进制），同一个 MQ 上不能重复

## 测试

```bash
go test ./...
```

MQTT 发送器连真实 MQ 的测试默认跳过，设置了 `LOGS_TEST_MQTT_URL`（以及 `LOGS_TEST_MQTT_USERNAME`、`LOGS_TEST_MQTT_PASSWORD`）才跑。
