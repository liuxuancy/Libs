package logs

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// blockingSender 收到第一批后卡住，直到 release 被关闭，用来把队列堆满。
type blockingSender struct {
	received chan struct{}
	release  chan struct{}
}

func (sender *blockingSender) SendBatch(ctx context.Context, _ []LogEntry) error {
	select {
	case sender.received <- struct{}{}:
	default:
	}
	select {
	case <-sender.release:
	case <-ctx.Done():
	}
	return nil
}

func TestBufferSendsWhenBatchIsFull(t *testing.T) {
	sender := &recordingSender{}
	buffer := newEntryBuffer(sender, 3, time.Hour, 100, &bytes.Buffer{})
	buffer.start()

	for index := 0; index < 7; index++ {
		buffer.enqueue(LogEntry{Message: "x"})
	}
	if err := buffer.stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}

	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	sizes := make([]int, 0, len(sender.batches))
	for _, batch := range sender.batches {
		sizes = append(sizes, len(batch))
	}
	// 满 3 条发两批，停止时把剩下的 1 条也发掉
	if len(sizes) != 3 || sizes[0] != 3 || sizes[1] != 3 || sizes[2] != 1 {
		t.Fatalf("unexpected batch sizes %v", sizes)
	}
}

func TestBufferSendsOnInterval(t *testing.T) {
	sender := &recordingSender{}
	buffer := newEntryBuffer(sender, 100, 10*time.Millisecond, 100, &bytes.Buffer{})
	buffer.start()
	defer buffer.stop(context.Background())

	buffer.enqueue(LogEntry{Message: "x"})
	deadline := time.Now().Add(time.Second)
	for len(sender.getAllEntries()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("entry should be flushed by the ticker")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBufferDropsWhenQueueIsFull(t *testing.T) {
	sender := &blockingSender{received: make(chan struct{}, 1), release: make(chan struct{})}
	errorOutput := &bytes.Buffer{}
	buffer := newEntryBuffer(sender, 1, time.Hour, 2, errorOutput)
	buffer.start()

	// 第一条被取走后发送器卡住，队列容量 2，再放 5 条就有 3 条被丢弃
	buffer.enqueue(LogEntry{Message: "first"})
	<-sender.received
	for index := 0; index < 5; index++ {
		buffer.enqueue(LogEntry{Message: "x"})
	}
	if dropped := buffer.getDroppedCount(); dropped != 3 {
		t.Fatalf("want 3 dropped, got %d", dropped)
	}

	close(sender.release)
	if err := buffer.stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !strings.Contains(errorOutput.String(), "累计丢弃 3 条") {
		t.Fatalf("dropped count should be reported to stderr, got %q", errorOutput.String())
	}
}

func TestBufferReportsSendErrorsWithoutLogging(t *testing.T) {
	sender := &recordingSender{err: errors.New("db down")}
	errorOutput := &bytes.Buffer{}
	buffer := newEntryBuffer(sender, 10, time.Hour, 10, errorOutput)
	buffer.start()
	buffer.enqueue(LogEntry{Message: "x"})
	if err := buffer.stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !strings.Contains(errorOutput.String(), "发送 1 条日志失败") || !strings.Contains(errorOutput.String(), "db down") {
		t.Fatalf("send error should go to error output, got %q", errorOutput.String())
	}
}

func TestBufferStopTimesOut(t *testing.T) {
	sender := &blockingSender{received: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(sender.release)
	buffer := newEntryBuffer(sender, 1, time.Hour, 10, &bytes.Buffer{})
	buffer.start()
	buffer.enqueue(LogEntry{Message: "x"})
	<-sender.received

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := buffer.stop(ctx); err == nil {
		t.Fatal("stop should give up when ctx ends")
	}
}
