package logs

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// countingSender 记下被调用的次数，按 failing 决定成功还是失败。
type countingSender struct {
	calls   int
	failing bool
}

func (sender *countingSender) SendBatch(_ context.Context, _ []LogEntry) error {
	sender.calls++
	if sender.failing {
		return errors.New("boom")
	}
	return nil
}

func newTestFallbackSender(primary, fallback *countingSender) (*FallbackSender, *bytes.Buffer) {
	sender := NewFallbackSender(primary, fallback)
	output := &bytes.Buffer{}
	sender.errorOutput = output
	return sender, output
}

func TestFallbackSenderUsesPrimaryWhenItWorks(t *testing.T) {
	primary, fallback := &countingSender{}, &countingSender{}
	sender, output := newTestFallbackSender(primary, fallback)
	if err := sender.SendBatch(context.Background(), []LogEntry{{EntryID: "a"}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if primary.calls != 1 || fallback.calls != 0 || output.Len() != 0 {
		t.Fatalf("primary %d, fallback %d, output %q", primary.calls, fallback.calls, output.String())
	}
}

func TestFallbackSenderReportsOnlyOnStateChange(t *testing.T) {
	primary, fallback := &countingSender{failing: true}, &countingSender{}
	sender, output := newTestFallbackSender(primary, fallback)
	for attempt := 0; attempt < 3; attempt++ {
		if err := sender.SendBatch(context.Background(), []LogEntry{{EntryID: "a"}}); err != nil {
			t.Fatalf("fallback should succeed: %v", err)
		}
	}
	if fallback.calls != 3 || strings.Count(output.String(), "改用备用发送器") != 1 {
		t.Fatalf("fallback %d, output %q", fallback.calls, output.String())
	}

	primary.failing = false
	_ = sender.SendBatch(context.Background(), []LogEntry{{EntryID: "a"}})
	_ = sender.SendBatch(context.Background(), []LogEntry{{EntryID: "a"}})
	if strings.Count(output.String(), "已恢复") != 1 || fallback.calls != 3 {
		t.Fatalf("fallback %d, output %q", fallback.calls, output.String())
	}
}

func TestFallbackSenderFailsWhenBothFail(t *testing.T) {
	sender, _ := newTestFallbackSender(&countingSender{failing: true}, &countingSender{failing: true})
	if err := sender.SendBatch(context.Background(), []LogEntry{{EntryID: "a"}}); err == nil {
		t.Fatal("want error when both senders fail")
	}
}
