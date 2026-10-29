// 备用发送器：主发送器（比如 MQ）发不出去时，这一批改交给备用发送器（比如直接写库），减少 MQ 故障时丢的日志。

package logs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

// FallbackSender 先交给主发送器，失败了再交给备用发送器；两个都失败才返回错误。
// 主发送器失败只在"刚开始失败"和"恢复正常"时各往 stderr 提示一次，不是每批都提示，免得 MQ 断线期间刷屏。
// 并发安全的前提是主、备两个发送器本身并发安全。
type FallbackSender struct {
	primary     BatchSender
	fallback    BatchSender
	errorOutput io.Writer
	isFailing   atomic.Bool
}

func NewFallbackSender(primary, fallback BatchSender) *FallbackSender {
	return &FallbackSender{primary: primary, fallback: fallback, errorOutput: os.Stderr}
}

func (sender *FallbackSender) SendBatch(ctx context.Context, entries []LogEntry) error {
	primaryErr := sender.primary.SendBatch(ctx, entries)
	if primaryErr == nil {
		if sender.isFailing.CompareAndSwap(true, false) {
			fmt.Fprintln(sender.errorOutput, "logs: 主发送器已恢复")
		}
		return nil
	}
	if sender.isFailing.CompareAndSwap(false, true) {
		fmt.Fprintf(sender.errorOutput, "logs: 主发送器失败，改用备用发送器，恢复前不再提示: %v\n", primaryErr)
	}
	if fallbackErr := sender.fallback.SendBatch(ctx, entries); fallbackErr != nil {
		return fmt.Errorf("主发送器: %v；备用发送器: %w", primaryErr, fallbackErr)
	}
	return nil
}
