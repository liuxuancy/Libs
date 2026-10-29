// 连接状态报告器：把 MQ 连接失败、断开、恢复写到 stderr，只在状态变化时写，MQ 长时间连不上也不会刷屏。

package mqttsender

import (
	"fmt"
	"io"
	"os"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ConnectionReporter 接到 Paho 的 SetConnectionNotificationHandler 上。
// 不走 logs 打日志：连不上 MQ 时"连不上"这条日志本身也发不出去，还可能进一步堆积。
type ConnectionReporter struct {
	clientName      string
	output          io.Writer
	mutex           sync.Mutex
	lastProblemText string // 上一次报告的问题；为空表示当前正常
}

// NewConnectionReporter clientName 写在每条提示前面，用来区分是哪个客户端，比如"日志 MQ 消费者"。
func NewConnectionReporter(clientName string) *ConnectionReporter {
	return &ConnectionReporter{clientName: clientName, output: os.Stderr}
}

// HandleConnectionNotification 连接失败、断开时提示一次（原因变了才再提示），之后连上时提示恢复。
func (reporter *ConnectionReporter) HandleConnectionNotification(_ mqtt.Client, notification mqtt.ConnectionNotification) {
	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()
	switch typed := notification.(type) {
	case mqtt.ConnectionNotificationConnected:
		if reporter.lastProblemText != "" {
			fmt.Fprintf(reporter.output, "mqttsender: %s已连上 MQ\n", reporter.clientName)
			reporter.lastProblemText = ""
		}
	case mqtt.ConnectionNotificationFailed:
		reporter.reportProblem(fmt.Sprintf("连接 MQ 失败，后台会继续重试: %v", typed.Reason))
	case mqtt.ConnectionNotificationLost:
		reporter.reportProblem(fmt.Sprintf("和 MQ 的连接断开，后台会自动重连: %v", typed.Reason))
	}
}

// reportProblem 和上一次报告的问题不同才写出来。
func (reporter *ConnectionReporter) reportProblem(problemText string) {
	if problemText == reporter.lastProblemText {
		return
	}
	fmt.Fprintf(reporter.output, "mqttsender: %s%s\n", reporter.clientName, problemText)
	reporter.lastProblemText = problemText
}
