// Package mqttsender MQTT 发送器：把一批日志发到日志 MQ（Mosquitto 等 MQTT 服务端），日志平台从 MQ 消费后写库。
// 给能连到日志 MQ 的服务用（和日志平台在同一台机器或同一内网）；连不到的用 httpsender 走 HTTP 接收接口。
package mqttsender

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/liuxuancy/Libs/logs"
)

const (
	// TopicPrefix 日志消息的 topic 前缀，完整 topic 是 "logs/<环境>/<服务名>"，见 GetLogTopic
	TopicPrefix = "logs/"
	// TopicFilter 日志平台的消费者订阅这个，收所有环境、所有服务的日志
	TopicFilter = "logs/#"
	// MaxPayloadBytes 一条 MQTT 消息最多多大，一批日志超过就拆成几条发。
	// 实测 MQ 对大消息的处理不可靠（RobustMQ 约 0.9MB 以上悄悄丢弃，Mosquitto 配置的上限是 1MB），留足余量
	MaxPayloadBytes = 256 << 10
	// maxMessageBytes 单条日志自己就超过 MaxPayloadBytes 时，message 先截到这么长（和日志平台存库的上限一致）
	maxMessageBytes = 60000
)

const (
	// connectWaitTimeout 创建发送器时最多等多久连上 MQ；没连上也不报错，后台继续重连，这段时间的日志由调用方决定怎么办
	connectWaitTimeout = 3 * time.Second
	// disconnectQuiesceMilliseconds 关闭时最多等多久把还没发完的消息发出去
	disconnectQuiesceMilliseconds = 1000
	// truncatedSuffix 内容被截断时加在末尾的提示
	truncatedSuffix = "…(已截断)"
)

// ErrNotConnected 当前没连上 MQ，这一批没有发。
var ErrNotConnected = errors.New("mqttsender: 当前没有连上 MQ")

// Options 发送器配置。
type Options struct {
	BrokerURL string // MQ 地址，例如 tcp://127.0.0.1:1883
	Username  string
	Password  string
	ClientID  string // 同一个 MQ 上不能和别的客户端重复，留空自动生成 "logs-" + 10 位随机十六进制；MQTT 3.1.1 要求不超过 23 字节
}

// Sender MQTT 发送器，实现 logs.BatchSender，并发安全。
type Sender struct {
	client mqtt.Client
}

// NewSender 创建发送器并开始连接 MQ。最多等 3 秒，没连上也返回发送器，后台每隔几秒重连一次。
// 连不上的原因（包括账号密码错误，Paho 也会一直重试）只写到 stderr，见 ConnectionReporter。
func NewSender(options Options) (*Sender, error) {
	if options.BrokerURL == "" {
		return nil, errors.New("mqttsender: BrokerURL 不能为空")
	}
	if options.ClientID == "" {
		options.ClientID = generateClientID()
	}
	// 发送器不需要持久会话：没发出去的由调用方处理，不靠 MQ 记着
	clientOptions := mqtt.NewClientOptions().
		AddBroker(options.BrokerURL).
		SetClientID(options.ClientID).
		SetUsername(options.Username).
		SetPassword(options.Password).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetKeepAlive(30 * time.Second).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectionNotificationHandler(NewConnectionReporter("日志 MQ 发送器").HandleConnectionNotification)
	client := mqtt.NewClient(clientOptions)
	token := client.Connect()
	if token.WaitTimeout(connectWaitTimeout) && token.Error() != nil {
		return nil, fmt.Errorf("mqttsender: 连接 MQ 失败: %w", token.Error())
	}
	return &Sender{client: client}, nil
}

// SendBatch 按"环境 + 服务名"分 topic、按 MaxPayloadBytes 拆成几条消息，用 QoS 1 发出，等 MQ 确认收到才返回。
// 没连上 MQ 时直接返回 ErrNotConnected，不交给 Paho 排队：断线期间 Paho 会把消息全堆在内存里等重连，MQ 长时间不可用会一直涨内存。
// 中途失败时前面几条可能已经发出去了，调用方整批重发也没关系，日志平台按条目 ID 去重。
func (sender *Sender) SendBatch(ctx context.Context, entries []logs.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if !sender.client.IsConnectionOpen() {
		return ErrNotConnected
	}
	messages, err := buildTopicMessages(entries, MaxPayloadBytes)
	if err != nil {
		return err
	}
	for _, message := range messages {
		token := sender.client.Publish(message.topic, 1, false, message.payload)
		select {
		case <-token.Done():
			if token.Error() != nil {
				return fmt.Errorf("mqttsender: 发送到 %s 失败: %w", message.topic, token.Error())
			}
		case <-ctx.Done():
			return fmt.Errorf("mqttsender: 等待 MQ 确认超时: %w", ctx.Err())
		}
	}
	return nil
}

// IsConnected 当前是否连着 MQ。
func (sender *Sender) IsConnected() bool {
	return sender.client.IsConnectionOpen()
}

// Close 断开连接，最多等 1 秒把已经交给 Paho 的消息发完。
func (sender *Sender) Close() {
	sender.client.Disconnect(disconnectQuiesceMilliseconds)
}

// GetLogTopic 一条日志发到哪个 topic："logs/<环境>/<服务名>"。
// 环境和服务名里的 / + # 是 MQTT topic 的特殊字符，换成下划线；为空时用 unknown。
func GetLogTopic(environment, serviceName string) string {
	return TopicPrefix + sanitizeTopicLevel(environment) + "/" + sanitizeTopicLevel(serviceName)
}

// sanitizeTopicLevel 把一段 topic 里 MQTT 的特殊字符换掉。
func sanitizeTopicLevel(level string) string {
	if level == "" {
		return "unknown"
	}
	return strings.NewReplacer("/", "_", "+", "_", "#", "_").Replace(level)
}

// topicMessage 一条要发出去的 MQTT 消息。
type topicMessage struct {
	topic   string
	payload []byte
}

// buildTopicMessages 按 topic 分组（保持第一次出现的顺序），每组再按大小拆成几条消息。
func buildTopicMessages(entries []logs.LogEntry, maxPayloadBytes int) ([]topicMessage, error) {
	var topics []string
	entriesByTopic := map[string][]logs.LogEntry{}
	for _, entry := range entries {
		topic := GetLogTopic(entry.Environment, entry.ServiceName)
		if _, exists := entriesByTopic[topic]; !exists {
			topics = append(topics, topic)
		}
		entriesByTopic[topic] = append(entriesByTopic[topic], entry)
	}

	var messages []topicMessage
	for _, topic := range topics {
		payloads, err := splitIntoPayloads(entriesByTopic[topic], maxPayloadBytes)
		if err != nil {
			return nil, err
		}
		for _, payload := range payloads {
			messages = append(messages, topicMessage{topic: topic, payload: payload})
		}
	}
	return messages, nil
}

// payloadPrefix / payloadSuffix 一条消息就是 logs.LogBatch 的 JSON：{"entries":[条目1,条目2,...]}
const (
	payloadPrefix = `{"entries":[`
	payloadSuffix = `]}`
)

// splitIntoPayloads 把一组日志拼成若干条 JSON 消息，每条不超过 maxPayloadBytes；单条日志自己就超过的，先缩小再放进去。
func splitIntoPayloads(entries []logs.LogEntry, maxPayloadBytes int) ([][]byte, error) {
	maxEntryBytes := maxPayloadBytes - len(payloadPrefix) - len(payloadSuffix)
	var payloads [][]byte
	current := bytes.NewBufferString(payloadPrefix)
	entryCount := 0
	for _, entry := range entries {
		encoded, err := encodeEntryWithinLimit(entry, maxEntryBytes)
		if err != nil {
			return nil, err
		}
		// 放不下了先把当前这条消息收尾；逗号也算在长度里
		if entryCount > 0 && current.Len()+1+len(encoded)+len(payloadSuffix) > maxPayloadBytes {
			current.WriteString(payloadSuffix)
			payloads = append(payloads, current.Bytes())
			current = bytes.NewBufferString(payloadPrefix)
			entryCount = 0
		}
		if entryCount > 0 {
			current.WriteByte(',')
		}
		current.Write(encoded)
		entryCount++
	}
	if entryCount > 0 {
		current.WriteString(payloadSuffix)
		payloads = append(payloads, current.Bytes())
	}
	return payloads, nil
}

// encodeEntryWithinLimit 把一条日志编码成 JSON；超过 maxEntryBytes 时先丢掉附加字段、message 截到 6 万字节，
// 还超就把 message 再对半截，直到放得下。
func encodeEntryWithinLimit(entry logs.LogEntry, maxEntryBytes int) ([]byte, error) {
	encoded, err := encodeEntry(entry)
	if err != nil || len(encoded) <= maxEntryBytes {
		return encoded, err
	}

	originalBytes := len(encoded)
	if len(entry.Attributes) > 0 {
		entry.Attributes = map[string]any{"_truncated": fmt.Sprintf("这条日志编码后 %d 字节，超过 MQ 单条消息上限，附加字段已丢弃", originalBytes)}
	}
	keepMessageBytes := min(len(entry.Message), maxMessageBytes)
	for {
		entry.Message = truncateToBytes(entry.Message, keepMessageBytes)
		encoded, err = encodeEntry(entry)
		if err != nil || len(encoded) <= maxEntryBytes {
			return encoded, err
		}
		if keepMessageBytes == 0 {
			return nil, fmt.Errorf("mqttsender: 条目 %s 去掉附加字段、清空内容后仍有 %d 字节，超过上限 %d", entry.EntryID, len(encoded), maxEntryBytes)
		}
		keepMessageBytes /= 2
	}
}

// encodeEntry 编码一条日志。不转义 < > &：默认会转成 6 个字节的 <，日志里这些字符很常见，白白变大。
func encodeEntry(entry logs.LogEntry) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(entry); err != nil {
		return nil, fmt.Errorf("mqttsender: 序列化日志失败: %w", err)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// truncateToBytes 按字节数截断并加上截断提示；不会把一个汉字切成两半。maxBytes 放不下提示时直接截成空。
func truncateToBytes(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	cut := maxBytes - len(truncatedSuffix)
	if cut <= 0 {
		return ""
	}
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + truncatedSuffix
}

// generateClientID 生成发送器的 client id："logs-" + 10 位随机十六进制，共 15 字节。
func generateClientID() string {
	randomBytes := make([]byte, 5)
	_, _ = rand.Read(randomBytes) // Go 1.24 起 crypto/rand.Read 保证不会返回错误
	return "logs-" + hex.EncodeToString(randomBytes)
}
