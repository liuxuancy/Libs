// 批量缓冲：日志先进内存队列，攒够一批或到时间就交给发送器；队列满了直接丢弃，绝不卡住业务。

package logs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// sendTimeout 单次调用发送器最多等多久，防止发送器卡住导致后面的日志越积越多。
const sendTimeout = 10 * time.Second

// entryBuffer 批量缓冲。发送失败、丢弃日志这类问题只写到 errorOutput（默认 stderr），
// 不能再走本包打日志，否则"发送日志失败"这条日志又会进队列，形成死循环。
type entryBuffer struct {
	sender        BatchSender
	batchSize     int
	flushInterval time.Duration
	errorOutput   io.Writer

	queue           chan LogEntry
	stopSignal      chan struct{}
	stopOnce        sync.Once
	stopped         chan struct{}
	droppedCount    atomic.Int64
	reportedDropped int64 // 只在 runFlushLoop 协程里读写
}

func newEntryBuffer(sender BatchSender, batchSize int, flushInterval time.Duration, queueCapacity int, errorOutput io.Writer) *entryBuffer {
	return &entryBuffer{
		sender:        sender,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		errorOutput:   errorOutput,
		queue:         make(chan LogEntry, queueCapacity),
		stopSignal:    make(chan struct{}),
		stopped:       make(chan struct{}),
	}
}

// enqueue 放进队列；队列满了直接丢弃并计数，不等待。
func (buffer *entryBuffer) enqueue(entry LogEntry) {
	select {
	case buffer.queue <- entry:
	default:
		buffer.droppedCount.Add(1)
	}
}

// getDroppedCount 累计因为队列满而丢弃的条数。
func (buffer *entryBuffer) getDroppedCount() int64 {
	return buffer.droppedCount.Load()
}

// start 启动后台发送协程。
func (buffer *entryBuffer) start() {
	go buffer.runFlushLoop()
}

// stop 通知后台协程把队列里剩下的日志发完再退出，最多等到 ctx 结束。
func (buffer *entryBuffer) stop(ctx context.Context) error {
	buffer.stopOnce.Do(func() { close(buffer.stopSignal) })
	select {
	case <-buffer.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("logs: 等待剩余日志发送完成超时: %w", ctx.Err())
	}
}

// runFlushLoop 后台协程：攒够 batchSize 条立刻发，否则每隔 flushInterval 把攒到的发一次；收到停止信号后清空队列再退出。
func (buffer *entryBuffer) runFlushLoop() {
	defer close(buffer.stopped)
	ticker := time.NewTicker(buffer.flushInterval)
	defer ticker.Stop()

	batch := make([]LogEntry, 0, buffer.batchSize)
	for {
		select {
		case entry := <-buffer.queue:
			batch = append(batch, entry)
			if len(batch) >= buffer.batchSize {
				batch = buffer.sendBatch(batch)
			}
		case <-ticker.C:
			batch = buffer.sendBatch(batch)
			buffer.reportDroppedEntries()
		case <-buffer.stopSignal:
			buffer.drainQueue(batch)
			buffer.reportDroppedEntries()
			return
		}
	}
}

// drainQueue 停止前把队列里已有的日志全部取出来发掉。
func (buffer *entryBuffer) drainQueue(batch []LogEntry) {
	for {
		select {
		case entry := <-buffer.queue:
			batch = append(batch, entry)
			if len(batch) >= buffer.batchSize {
				batch = buffer.sendBatch(batch)
			}
		default:
			buffer.sendBatch(batch)
			return
		}
	}
}

// sendBatch 把这一批交给发送器，返回一个新的空批次。
// 不复用原来的切片，因为发送器可能还拿着它（比如异步发 MQ）。
func (buffer *entryBuffer) sendBatch(batch []LogEntry) []LogEntry {
	if len(batch) == 0 {
		return batch
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if err := buffer.sender.SendBatch(ctx, batch); err != nil {
		fmt.Fprintf(buffer.errorOutput, "logs: 发送 %d 条日志失败，这一批已丢弃: %v\n", len(batch), err)
	}
	return make([]LogEntry, 0, buffer.batchSize)
}

// reportDroppedEntries 有新的丢弃时提示一次累计丢弃条数，不是每丢一条都提示。
func (buffer *entryBuffer) reportDroppedEntries() {
	dropped := buffer.droppedCount.Load()
	if dropped > buffer.reportedDropped {
		fmt.Fprintf(buffer.errorOutput, "logs: 日志队列已满，累计丢弃 %d 条日志\n", dropped)
		buffer.reportedDropped = dropped
	}
}
