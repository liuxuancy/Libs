// 收集器交给发送器的日志条目结构，以及发送器接口。

package logs

import (
	"context"
	"time"
)

// LogEntry 一行日志。JSON 字段名就是推送到 MQ / HTTP 接收接口时的格式，改名要和日志平台同步。
type LogEntry struct {
	EntryID     string         `json:"entry_id"` // 条目 ID：每行日志自己的 ID，写库时靠它去重
	LogID       string         `json:"log_id"`   // 一次请求一个，同一个请求打的日志都相同；没有请求上下文时为空
	Time        time.Time      `json:"time"`
	Level       string         `json:"level"` // DEBUG / INFO / WARN / ERROR
	ServiceName string         `json:"service"`
	Environment string         `json:"env"`    // prod / dev
	Source      string         `json:"source"` // 打日志的代码位置 "目录/文件.go:行号"
	Message     string         `json:"message"`
	Attributes  map[string]any `json:"attrs,omitempty"` // 其余附加字段（user_id、method、status 等），分组的字段用 "组.字段" 展平
}

// BatchSender 发送器：收集器攒好一批日志后交给它，由它决定送到哪里（进程内写库、MQ、HTTP）。
// 同一时刻只会被一个协程调用；返回错误时这一批会被丢弃，需要重试的发送器自己在内部重试。
type BatchSender interface {
	SendBatch(ctx context.Context, entries []LogEntry) error
}

// InProcessSender 进程内发送器：把一批日志直接交给同一进程里的处理函数（比如写库器），不经过网络。
type InProcessSender func(ctx context.Context, entries []LogEntry) error

func (sender InProcessSender) SendBatch(ctx context.Context, entries []LogEntry) error {
	return sender(ctx, entries)
}
