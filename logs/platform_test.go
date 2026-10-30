package logs

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeClock 可以手动拨快的时钟，测试重试和冷却不用真的等。
type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) getNow() time.Time {
	return clock.now
}

func (clock *fakeClock) advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

// switchableTarget 假的日志平台：isDown 为 true 时发送失败，记下真正发出去的次数。
type switchableTarget struct {
	isDown    bool
	sendCount int
}

func (target *switchableTarget) SendBatch(context.Context, []LogEntry) error {
	target.sendCount++
	if target.isDown {
		return errors.New("platform is down")
	}
	return nil
}

// newTestPlatformSender 每 10 秒重试一次，maxRetryCount 次或 maxRetryDuration 算一轮，冷却 retryCooldown（<0 不再重试）。
func newTestPlatformSender(maxRetryCount int, maxRetryDuration time.Duration, retryCooldown time.Duration) (*platformSender, *switchableTarget, *fakeClock, *bytes.Buffer) {
	target := &switchableTarget{}
	clock := &fakeClock{now: time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)}
	output := &bytes.Buffer{}
	sender := newPlatformSender(target, "https://example.com/api/logs/ingest", PlatformConfig{
		RetryInterval:    10 * time.Second,
		MaxRetryCount:    maxRetryCount,
		MaxRetryDuration: maxRetryDuration,
		RetryCooldown:    retryCooldown,
	}, output)
	sender.getNow = clock.getNow
	return sender, target, clock, output
}

// sendTestBatch 发一批，确认永远不把错误返回给收集器。
func sendTestBatch(t *testing.T, sender *platformSender) {
	t.Helper()
	if err := sender.SendBatch(context.Background(), []LogEntry{{EntryID: newEntryID()}}); err != nil {
		t.Fatalf("平台抄送不应该把错误返回给收集器，实际 %v", err)
	}
}

func TestPlatformSenderReportsFirstConnect(t *testing.T) {
	sender, target, _, output := newTestPlatformSender(3, time.Hour, -1)
	sendTestBatch(t, sender)
	sendTestBatch(t, sender)
	if target.sendCount != 2 || strings.Count(output.String(), "已连上日志平台") != 1 {
		t.Fatalf("连上后每批都要发、只提示一次连上，实际发了 %d 次，输出:\n%s", target.sendCount, output.String())
	}
}

func TestPlatformSenderGivesUpAfterRetryRoundInDev(t *testing.T) {
	sender, target, clock, output := newTestPlatformSender(3, time.Hour, -1)
	target.isDown = true

	sendTestBatch(t, sender) // 第一次失败，开始一轮重试
	clock.advance(5 * time.Second)
	sendTestBatch(t, sender) // 不到 10 秒，不发，直接丢弃
	if target.sendCount != 1 {
		t.Fatalf("没到重试时间不应该发，实际发了 %d 次", target.sendCount)
	}
	for retry := 1; retry <= 3; retry++ {
		clock.advance(10 * time.Second)
		sendTestBatch(t, sender)
	}
	if target.sendCount != 4 || sender.state != platformGivenUp {
		t.Fatalf("重试 3 次后应该放弃，实际发了 %d 次，状态 %d", target.sendCount, sender.state)
	}

	// 放弃之后平台恢复了也不再发，直到重启
	target.isDown = false
	clock.advance(time.Hour)
	sendTestBatch(t, sender)
	if target.sendCount != 4 {
		t.Errorf("放弃之后不应该再发，实际发了 %d 次", target.sendCount)
	}
	text := output.String()
	if strings.Count(text, "抄送到日志平台失败") != 1 || strings.Count(text, "已放弃抄送") != 1 {
		t.Errorf("开始重试和放弃各提示一次，实际:\n%s", text)
	}
}

func TestPlatformSenderCoolsDownThenRetriesInProd(t *testing.T) {
	sender, target, clock, output := newTestPlatformSender(2, time.Hour, 10*time.Minute)
	target.isDown = true
	sendTestBatch(t, sender)
	for retry := 1; retry <= 2; retry++ {
		clock.advance(10 * time.Second)
		sendTestBatch(t, sender)
	}
	if sender.state != platformCoolingDown {
		t.Fatalf("一轮用完应该进入冷却，实际状态 %d", sender.state)
	}

	clock.advance(5 * time.Minute)
	sendTestBatch(t, sender)
	if target.sendCount != 3 {
		t.Fatalf("冷却中不应该发，实际发了 %d 次", target.sendCount)
	}

	// 冷却到期，平台也恢复了：马上试一次，成功就恢复正常抄送
	target.isDown = false
	clock.advance(5 * time.Minute)
	sendTestBatch(t, sender)
	sendTestBatch(t, sender)
	if target.sendCount != 5 || sender.state != platformConnected {
		t.Fatalf("冷却到期应该重新尝试并恢复，实际发了 %d 次，状态 %d", target.sendCount, sender.state)
	}
	if !strings.Contains(output.String(), "后再开始新一轮重试") || !strings.Contains(output.String(), "日志平台已恢复") {
		t.Errorf("应该提示冷却和恢复，实际:\n%s", output.String())
	}
}

func TestPlatformSenderRoundEndsByDuration(t *testing.T) {
	// 次数上限很大，靠时间上限结束这一轮：10、20、30 秒各试一次，第 30 秒到了 30 秒上限
	sender, target, clock, _ := newTestPlatformSender(100, 30*time.Second, -1)
	target.isDown = true
	sendTestBatch(t, sender)
	for retry := 1; retry <= 3; retry++ {
		clock.advance(10 * time.Second)
		sendTestBatch(t, sender)
	}
	if sender.state != platformGivenUp {
		t.Fatalf("到了时间上限应该放弃，实际状态 %d，重试了 %d 次", sender.state, sender.retryCount)
	}
}

func TestPlatformSenderRecoveryResetsRetryCount(t *testing.T) {
	sender, target, clock, _ := newTestPlatformSender(2, time.Hour, -1)
	target.isDown = true
	sendTestBatch(t, sender)
	clock.advance(10 * time.Second)
	sendTestBatch(t, sender) // 第 1 次重试，失败

	target.isDown = false
	clock.advance(10 * time.Second)
	sendTestBatch(t, sender) // 第 2 次重试成功，恢复

	// 再断开：重新开始一轮，前面用掉的次数不算
	target.isDown = true
	sendTestBatch(t, sender)
	clock.advance(10 * time.Second)
	sendTestBatch(t, sender)
	if sender.state != platformRetrying || sender.retryCount != 1 {
		t.Fatalf("恢复后次数应该清零，实际状态 %d，这一轮重试了 %d 次", sender.state, sender.retryCount)
	}
}
