package httpsender

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuxuancy/Libs/logs"
)

// newTestServer 前 failTimes 次返回 failStatus，之后返回 200；记下请求次数和最后一次收到的内容。
func newTestServer(t *testing.T, failTimes int32, failStatus int) (*httptest.Server, *atomic.Int32, *logs.LogBatch) {
	t.Helper()
	var requestCount atomic.Int32
	received := &logs.LogBatch{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		count := requestCount.Add(1)
		if request.Header.Get(IngestKeyHeader) != "secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if count <= failTimes {
			http.Error(writer, "boom", failStatus)
			return
		}
		_ = json.NewDecoder(request.Body).Decode(received)
	}))
	t.Cleanup(server.Close)
	return server, &requestCount, received
}

func TestSendBatchPostsEntriesWithKey(t *testing.T) {
	server, requestCount, received := newTestServer(t, 0, 0)
	sender := NewSender(server.URL, "secret")

	err := sender.SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a1", Message: "hello"}})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if requestCount.Load() != 1 || len(received.Entries) != 1 || received.Entries[0].Message != "hello" {
		t.Fatalf("server got %d requests, entries %+v", requestCount.Load(), received.Entries)
	}
}

func TestSendBatchRetriesOnceOnServerError(t *testing.T) {
	server, requestCount, _ := newTestServer(t, 1, http.StatusBadGateway)
	if err := NewSender(server.URL, "secret").SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a1"}}); err != nil {
		t.Fatalf("second try should succeed: %v", err)
	}
	if requestCount.Load() != 2 {
		t.Fatalf("want 2 requests, got %d", requestCount.Load())
	}
}

func TestSendBatchGivesUpAfterOneRetry(t *testing.T) {
	server, requestCount, _ := newTestServer(t, 5, http.StatusInternalServerError)
	err := NewSender(server.URL, "secret").SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a1"}})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("want 500 error, got %v", err)
	}
	if requestCount.Load() != 2 {
		t.Fatalf("should try exactly twice, got %d", requestCount.Load())
	}
}

func TestSendBatchDoesNotRetryClientError(t *testing.T) {
	server, requestCount, _ := newTestServer(t, 0, 0)
	err := NewSender(server.URL, "wrong-key").SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a1"}})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want 401 error, got %v", err)
	}
	if requestCount.Load() != 1 {
		t.Fatalf("4xx should not be retried, got %d requests", requestCount.Load())
	}
}

func TestSendBatchRetriesNetworkError(t *testing.T) {
	server, _, _ := newTestServer(t, 0, 0)
	address := server.URL
	server.Close() // 地址还在，但连不上了

	err := NewSender(address, "secret").SendBatch(context.Background(), []logs.LogEntry{{EntryID: "a1"}})
	if err == nil || !strings.Contains(err.Error(), "请求接收接口失败") {
		t.Fatalf("want network error, got %v", err)
	}
}
