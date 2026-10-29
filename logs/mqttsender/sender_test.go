package mqttsender

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/liuxuancy/Libs/logs"
)

func decodePayload(t *testing.T, payload []byte) logs.LogBatch {
	t.Helper()
	var batch logs.LogBatch
	if err := json.Unmarshal(payload, &batch); err != nil {
		t.Fatalf("payload is not valid LogBatch JSON: %v\n%s", err, payload)
	}
	return batch
}

func TestGetLogTopicReplacesSpecialCharacters(t *testing.T) {
	cases := map[string][2]string{
		"logs/prod/suguang":    {"prod", "suguang"},
		"logs/dev/a_b_c_d":     {"dev", "a/b+c#d"},
		"logs/unknown/unknown": {"", ""},
	}
	for want, input := range cases {
		if got := GetLogTopic(input[0], input[1]); got != want {
			t.Errorf("GetLogTopic(%q, %q) = %q, want %q", input[0], input[1], got, want)
		}
	}
}

func TestBuildTopicMessagesGroupsByTopic(t *testing.T) {
	entries := []logs.LogEntry{
		{EntryID: "1", Environment: "prod", ServiceName: "a"},
		{EntryID: "2", Environment: "dev", ServiceName: "a"},
		{EntryID: "3", Environment: "prod", ServiceName: "a"},
	}
	messages, err := buildTopicMessages(entries, MaxPayloadBytes)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(messages) != 2 || messages[0].topic != "logs/prod/a" || messages[1].topic != "logs/dev/a" {
		t.Fatalf("unexpected messages: %+v", messages)
	}
	if got := decodePayload(t, messages[0].payload); len(got.Entries) != 2 || got.Entries[1].EntryID != "3" {
		t.Fatalf("prod payload: %+v", got)
	}
}

func TestSplitIntoPayloadsRespectsLimit(t *testing.T) {
	const limit = 4096
	var entries []logs.LogEntry
	for index := 0; index < 100; index++ {
		entries = append(entries, logs.LogEntry{EntryID: fmt.Sprint(index), Message: strings.Repeat("x", 300)})
	}
	payloads, err := splitIntoPayloads(entries, limit)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	total := 0
	for _, payload := range payloads {
		if len(payload) > limit {
			t.Fatalf("payload %d bytes exceeds limit %d", len(payload), limit)
		}
		total += len(decodePayload(t, payload).Entries)
	}
	if len(payloads) < 2 || total != len(entries) {
		t.Fatalf("got %d payloads with %d entries", len(payloads), total)
	}
}

func TestSplitIntoPayloadsShrinksOversizedEntry(t *testing.T) {
	huge := logs.LogEntry{
		EntryID:    "big",
		Message:    strings.Repeat("汉", 200000), // 60 万字节
		Attributes: map[string]any{"payload": strings.Repeat("y", 300000)},
	}
	payloads, err := splitIntoPayloads([]logs.LogEntry{{EntryID: "small"}, huge}, MaxPayloadBytes)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	var shrunk logs.LogEntry
	for _, payload := range payloads {
		if len(payload) > MaxPayloadBytes {
			t.Fatalf("payload %d bytes exceeds limit", len(payload))
		}
		for _, entry := range decodePayload(t, payload).Entries {
			if entry.EntryID == "big" {
				shrunk = entry
			}
		}
	}
	if len(shrunk.Message) > maxMessageBytes || !strings.HasSuffix(shrunk.Message, truncatedSuffix) {
		t.Fatalf("message not truncated: %d bytes", len(shrunk.Message))
	}
	if _, ok := shrunk.Attributes["_truncated"]; !ok || len(shrunk.Attributes) != 1 {
		t.Fatalf("attributes not replaced: %v", shrunk.Attributes)
	}
}

func TestEncodeEntryKeepsHTMLCharacters(t *testing.T) {
	encoded, err := encodeEntry(logs.LogEntry{Message: "<a> & <b>"})
	if err != nil || !strings.Contains(string(encoded), "<a> & <b>") {
		t.Fatalf("encoded %s, err %v", encoded, err)
	}
}

// TestSendBatchThroughBroker 连真实的 MQ 收发一遍：设置了 LOGS_TEST_MQTT_URL（以及账号密码）才跑，否则跳过。
// 例：LOGS_TEST_MQTT_URL=tcp://127.0.0.1:1883 LOGS_TEST_MQTT_USERNAME=logmq LOGS_TEST_MQTT_PASSWORD=logmq-dev go test ./mqttsender/
func TestSendBatchThroughBroker(t *testing.T) {
	brokerURL := os.Getenv("LOGS_TEST_MQTT_URL")
	if brokerURL == "" {
		t.Skip("没有设置 LOGS_TEST_MQTT_URL，跳过连 MQ 的测试")
	}
	options := Options{BrokerURL: brokerURL, Username: os.Getenv("LOGS_TEST_MQTT_USERNAME"), Password: os.Getenv("LOGS_TEST_MQTT_PASSWORD")}
	serviceName := fmt.Sprintf("mqttsender-test-%d", time.Now().UnixNano())

	recorder := &entryRecorder{received: map[string]bool{}}
	subscriberOptions := mqtt.NewClientOptions().AddBroker(brokerURL).SetUsername(options.Username).SetPassword(options.Password).
		SetClientID(generateClientID())
	subscriber := mqtt.NewClient(subscriberOptions)
	if token := subscriber.Connect(); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("subscriber connect: %v", token.Error())
	}
	defer subscriber.Disconnect(100)
	if token := subscriber.Subscribe(GetLogTopic("dev", serviceName), 1, recorder.handleMessage); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}

	sender, err := NewSender(options)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer sender.Close()
	if !sender.IsConnected() {
		t.Fatal("sender should be connected within 3 seconds")
	}
	// 1000 条 × 约 1KB，超过 256KB，会拆成几条消息
	var entries []logs.LogEntry
	for index := 0; index < 1000; index++ {
		entries = append(entries, logs.LogEntry{EntryID: fmt.Sprintf("e%04d", index), Environment: "dev", ServiceName: serviceName, Message: strings.Repeat("m", 1000)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sender.SendBatch(ctx, entries); err != nil {
		t.Fatalf("SendBatch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if entryCount, _ := recorder.getCounts(); entryCount == len(entries) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if entryCount, payloadCount := recorder.getCounts(); entryCount != len(entries) || payloadCount < 4 {
		t.Fatalf("received %d entries in %d payloads", entryCount, payloadCount)
	}
}

// entryRecorder 订阅端收到消息后记下条目 ID 和消息条数（并发安全）。
type entryRecorder struct {
	mutex        sync.Mutex
	received     map[string]bool
	payloadCount int
}

func (recorder *entryRecorder) handleMessage(_ mqtt.Client, message mqtt.Message) {
	var batch logs.LogBatch
	_ = json.Unmarshal(message.Payload(), &batch)
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.payloadCount++
	for _, entry := range batch.Entries {
		recorder.received[entry.EntryID] = true
	}
}

func (recorder *entryRecorder) getCounts() (entryCount int, payloadCount int) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return len(recorder.received), recorder.payloadCount
}

func TestNewSenderWithWrongPasswordDoesNotConnect(t *testing.T) {
	brokerURL := os.Getenv("LOGS_TEST_MQTT_URL")
	if brokerURL == "" {
		t.Skip("没有设置 LOGS_TEST_MQTT_URL，跳过连 MQ 的测试")
	}
	sender, err := NewSender(Options{BrokerURL: brokerURL, Username: "nobody", Password: "wrong"})
	if err != nil {
		t.Fatalf("NewSender should not fail, the reason only goes to stderr: %v", err)
	}
	defer sender.Close()
	if sender.IsConnected() {
		t.Fatal("wrong password should not connect")
	}
	if err := sender.SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a"}}); err != ErrNotConnected {
		t.Fatalf("want ErrNotConnected, got %v", err)
	}
}

func TestConnectionReporterReportsOnlyChanges(t *testing.T) {
	reporter := NewConnectionReporter("测试客户端")
	output := &strings.Builder{}
	reporter.output = output
	failed := mqtt.ConnectionNotificationFailed{Reason: fmt.Errorf("not Authorized")}
	reporter.HandleConnectionNotification(nil, mqtt.ConnectionNotificationConnected{})
	reporter.HandleConnectionNotification(nil, failed)
	reporter.HandleConnectionNotification(nil, failed)
	reporter.HandleConnectionNotification(nil, mqtt.ConnectionNotificationConnected{})
	reporter.HandleConnectionNotification(nil, mqtt.ConnectionNotificationConnected{})
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "not Authorized") || !strings.Contains(lines[1], "已连上") {
		t.Fatalf("unexpected output:\n%s", output.String())
	}
}
