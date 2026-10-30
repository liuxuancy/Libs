// 抄送到日志平台：Init 打开了 LOG_PLATFORM_ENABLED 时，日志照常打终端，同时由单独一路收集器攒批，经 HTTP 发到平台的接收接口。
// 这一路有自己的队列和协程，平台慢、连不上都只影响这一路；连不上时按 PlatformConfig 的规则重试，期间的日志直接丢弃，不在内存里堆积。

package logs

import (
	"context"
	"fmt"
	"io"
	"time"
)

// platformState 抄送这一路现在处在哪个阶段。
type platformState int

const (
	// platformConnecting 刚启动，还没发成功过
	platformConnecting platformState = iota
	// platformConnected 最近一次发成功了，每一批都发
	platformConnected
	// platformRetrying 发失败了，在一轮重试里：没到下次重试时间的批次直接丢弃
	platformRetrying
	// platformCoolingDown 一轮重试用完，冷却中，冷却完再开始新的一轮
	platformCoolingDown
	// platformGivenUp 一轮重试用完且不再重试（dev 默认），直到程序重启
	platformGivenUp
)

// platformSender 抄送这一路的发送器：按状态决定这一批发不发，失败按重试规则处理。
// 永远不把错误返回给收集器：收集器收到错误会每批往 stderr 打一行"已丢弃"，平台断开期间会刷屏；提示由它自己按状态变化打。
// 收集器只用一个协程调用它，不需要加锁。
type platformSender struct {
	target      BatchSender
	targetURL   string
	settings    PlatformConfig
	errorOutput io.Writer
	// getNow 取当前时间，测试里换成可以拨快的时钟
	getNow func() time.Time

	state          platformState
	roundStartTime time.Time // 这一轮重试是什么时候开始的
	retryCount     int       // 这一轮已经重试了几次
	nextRetryTime  time.Time // 重试中：下次什么时候再试
	cooldownEnd    time.Time // 冷却中：什么时候开始下一轮
}

// newPlatformSender settings 要先经过 applyConfigDefaults 补好默认值。
func newPlatformSender(target BatchSender, targetURL string, settings PlatformConfig, errorOutput io.Writer) *platformSender {
	return &platformSender{
		target:      target,
		targetURL:   targetURL,
		settings:    settings,
		errorOutput: errorOutput,
		getNow:      time.Now,
		state:       platformConnecting,
	}
}

func (sender *platformSender) SendBatch(ctx context.Context, entries []LogEntry) error {
	if !sender.isSendAllowed(sender.getNow()) {
		return nil
	}
	err := sender.target.SendBatch(ctx, entries)
	if err == nil {
		sender.markConnected()
	} else {
		sender.markSendFailed(err, sender.getNow())
	}
	return nil
}

// isSendAllowed 这一批要不要真的发；冷却到期时顺带开始新的一轮重试。
func (sender *platformSender) isSendAllowed(now time.Time) bool {
	switch sender.state {
	case platformGivenUp:
		return false
	case platformCoolingDown:
		if now.Before(sender.cooldownEnd) {
			return false
		}
		sender.startRetryRound(now)
		return true
	case platformRetrying:
		return !now.Before(sender.nextRetryTime)
	default:
		return true
	}
}

// markConnected 发成功了：第一次连上或者从重试中恢复时提示一次，次数和计时清零。
func (sender *platformSender) markConnected() {
	switch sender.state {
	case platformConnecting:
		fmt.Fprintf(sender.errorOutput, "logs: 已连上日志平台 %s，开始抄送\n", sender.targetURL)
	case platformRetrying, platformCoolingDown:
		fmt.Fprintf(sender.errorOutput, "logs: 日志平台已恢复，继续抄送\n")
	}
	sender.state = platformConnected
	sender.retryCount = 0
}

// markSendFailed 发失败了：不在重试中就开始一轮；在重试中就记一次，用完这一轮就冷却或者放弃。
func (sender *platformSender) markSendFailed(err error, now time.Time) {
	if sender.state != platformRetrying {
		fmt.Fprintf(sender.errorOutput, "logs: 抄送到日志平台失败，每 %s 重试一次，最多 %d 次或 %s（这期间的日志只打终端）: %v\n",
			sender.settings.RetryInterval, sender.settings.MaxRetryCount, sender.settings.MaxRetryDuration, err)
		sender.startRetryRound(now)
		return
	}

	sender.retryCount++
	isRoundUsedUp := sender.retryCount >= sender.settings.MaxRetryCount ||
		now.Sub(sender.roundStartTime) >= sender.settings.MaxRetryDuration
	if !isRoundUsedUp {
		sender.nextRetryTime = now.Add(sender.settings.RetryInterval)
		return
	}
	if sender.settings.RetryCooldown < 0 {
		fmt.Fprintf(sender.errorOutput, "logs: 重试 %d 次仍连不上日志平台，已放弃抄送，重启程序后再试: %v\n", sender.retryCount, err)
		sender.state = platformGivenUp
		return
	}
	fmt.Fprintf(sender.errorOutput, "logs: 重试 %d 次仍连不上日志平台，%s 后再开始新一轮重试: %v\n", sender.retryCount, sender.settings.RetryCooldown, err)
	sender.state = platformCoolingDown
	sender.cooldownEnd = now.Add(sender.settings.RetryCooldown)
}

// startRetryRound 开始新的一轮重试：次数清零，下一次重试在 RetryInterval 之后。
func (sender *platformSender) startRetryRound(now time.Time) {
	sender.state = platformRetrying
	sender.roundStartTime = now
	sender.retryCount = 0
	sender.nextRetryTime = now.Add(sender.settings.RetryInterval)
}
