package logs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newIngestTestRequest 造一个发给接收接口的请求，ingestKey 为空时不带密钥请求头。
func newIngestTestRequest(t *testing.T, ingestKey string, body string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/logs/ingest", strings.NewReader(body))
	if ingestKey != "" {
		request.Header.Set(IngestKeyHeader, ingestKey)
	}
	return request
}

// encodeIngestTestBody 把日志编码成接收接口的请求体。
func encodeIngestTestBody(t *testing.T, entries ...LogEntry) string {
	t.Helper()
	body, err := json.Marshal(LogBatch{Entries: entries})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(body)
}

func TestReadIngestRequestReturnsEntries(t *testing.T) {
	body := encodeIngestTestBody(t,
		LogEntry{EntryID: "a1", Environment: EnvironmentDev, ServiceName: "order-service", Message: "下单成功"},
		LogEntry{EntryID: "a2", Environment: EnvironmentProd, ServiceName: "order-service"},
	)
	entries, err := ReadIngestRequest(newIngestTestRequest(t, "secret", body), "secret")
	if err != nil {
		t.Fatalf("ReadIngestRequest: %v", err)
	}
	if len(entries) != 2 || entries[0].EntryID != "a1" || entries[0].Message != "下单成功" {
		t.Errorf("取出的日志不对: %+v", entries)
	}
}

func TestReadIngestRequestRejectsBadRequests(t *testing.T) {
	validBody := encodeIngestTestBody(t, LogEntry{Environment: EnvironmentDev, ServiceName: "x"})
	tooManyEntries := make([]LogEntry, MaxIngestEntries+1)
	for index := range tooManyEntries {
		tooManyEntries[index] = LogEntry{Environment: EnvironmentDev, ServiceName: "x"}
	}
	cases := []struct {
		name               string
		serverKey          string
		requestKey         string
		body               string
		expectedStatusCode int
	}{
		{"服务端没配密钥", "", "secret", validBody, http.StatusServiceUnavailable},
		{"没带密钥", "secret", "", validBody, http.StatusUnauthorized},
		{"密钥不对", "secret", "wrong", validBody, http.StatusUnauthorized},
		{"请求体太大", "secret", "secret", `{"entries":["` + strings.Repeat("a", MaxIngestBodyBytes) + `"]}`, http.StatusRequestEntityTooLarge},
		{"不是 JSON", "secret", "secret", "not json", http.StatusBadRequest},
		{"空批次", "secret", "secret", `{"entries":[]}`, http.StatusBadRequest},
		{"条数太多", "secret", "secret", encodeIngestTestBody(t, tooManyEntries...), http.StatusBadRequest},
		{"环境不对", "secret", "secret", encodeIngestTestBody(t, LogEntry{Environment: "staging", ServiceName: "x"}), http.StatusBadRequest},
		{"缺服务名", "secret", "secret", encodeIngestTestBody(t, LogEntry{Environment: EnvironmentDev}), http.StatusBadRequest},
	}
	for _, testCase := range cases {
		entries, err := ReadIngestRequest(newIngestTestRequest(t, testCase.requestKey, testCase.body), testCase.serverKey)
		if err == nil || entries != nil {
			t.Errorf("%s: 应该拒收，实际收下了 %d 条", testCase.name, len(entries))
			continue
		}
		if status := GetIngestErrorStatus(err); status != testCase.expectedStatusCode {
			t.Errorf("%s: 状态码期望 %d，实际 %d（%v）", testCase.name, testCase.expectedStatusCode, status, err)
		}
	}
}

func TestGetIngestErrorStatusDefaultsTo500(t *testing.T) {
	if status := GetIngestErrorStatus(errors.New("database is down")); status != http.StatusInternalServerError {
		t.Errorf("不是接收时的错误应该按 500，实际 %d", status)
	}
}

// ingestTestServer 用 ReadIngestRequest 写的最小接收接口，记下收到的日志。
type ingestTestServer struct {
	ingestKey string
	received  []LogEntry
}

func (server *ingestTestServer) handleIngest(writer http.ResponseWriter, request *http.Request) {
	entries, err := ReadIngestRequest(request, server.ingestKey)
	if err != nil {
		http.Error(writer, err.Error(), GetIngestErrorStatus(err))
		return
	}
	server.received = append(server.received, entries...)
	writer.WriteHeader(http.StatusOK)
}

func TestHTTPSenderAndReadIngestRequestWorkTogether(t *testing.T) {
	receiver := &ingestTestServer{ingestKey: "secret"}
	httpServer := httptest.NewServer(http.HandlerFunc(receiver.handleIngest))
	defer httpServer.Close()

	entries := []LogEntry{{EntryID: "a1", Environment: EnvironmentDev, ServiceName: "order-service", Message: "hello"}}
	if err := NewHTTPSender(httpServer.URL, "secret").SendBatch(context.Background(), entries); err != nil {
		t.Fatalf("发送方发给接收方失败: %v", err)
	}
	if len(receiver.received) != 1 || receiver.received[0].EntryID != "a1" {
		t.Errorf("接收方收到的不对: %+v", receiver.received)
	}

	// 密钥不对时发送方拿到 401，HTTP 发送器不重试
	err := NewHTTPSender(httpServer.URL, "wrong").SendBatch(context.Background(), entries)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("密钥不对应该拿到 401，实际 %v", err)
	}
}
