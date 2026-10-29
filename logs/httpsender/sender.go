// Package httpsender HTTP 发送器：把一批日志 POST 到日志平台的接收接口。
// 给和日志平台不在同一台机器、连不到它的 MQ 的服务用（比如本地开发机）；同一台机器上的服务直接用 mqttsender 发 MQ。
// 请求体是 logs.LogBatch，即 {"entries":[...]}。
package httpsender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/liuxuancy/Libs/logs"
)

// IngestKeyHeader 请求头里放共享密钥的名字，接收接口靠它判断是不是自己人发来的。
const IngestKeyHeader = "X-Log-Ingest-Key"

// requestTimeout 单次 HTTP 请求最多等多久。
const requestTimeout = 5 * time.Second

// maxErrorBodyBytes 请求失败时最多读多少字节的响应内容放进错误信息。
const maxErrorBodyBytes = 1024

// Sender HTTP 发送器，实现 logs.BatchSender。
type Sender struct {
	endpoint  string
	ingestKey string
	client    *http.Client
}

// NewSender endpoint 是接收接口的完整地址，例如 https://example.com/api/logs/ingest；ingestKey 是共享密钥。
func NewSender(endpoint, ingestKey string) *Sender {
	return &Sender{
		endpoint:  endpoint,
		ingestKey: ingestKey,
		client:    &http.Client{Timeout: requestTimeout},
	}
}

// SendBatch 把一批日志发出去。网络错误或服务端 5xx 时重试一次，还失败就返回错误（这一批由批量缓冲丢弃）；
// 4xx 说明密钥或数据有问题，重试也没用，直接返回。
func (sender *Sender) SendBatch(ctx context.Context, entries []logs.LogEntry) error {
	body, err := json.Marshal(logs.LogBatch{Entries: entries})
	if err != nil {
		return fmt.Errorf("httpsender: 序列化日志失败: %w", err)
	}
	err = sender.postBatch(ctx, body)
	if isRetryableError(err) {
		err = sender.postBatch(ctx, body)
	}
	return err
}

// statusError 接收接口返回了非 2xx 状态码。
type statusError struct {
	statusCode int
	body       string
}

func (statusErr *statusError) Error() string {
	return fmt.Sprintf("httpsender: 接收接口返回 %d: %s", statusErr.statusCode, statusErr.body)
}

// postBatch 发一次请求。
func (sender *Sender) postBatch(ctx context.Context, body []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sender.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("httpsender: 构造请求失败: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(IngestKeyHeader, sender.ingestKey)

	response, err := sender.client.Do(request)
	if err != nil {
		return fmt.Errorf("httpsender: 请求接收接口失败: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, response.Body) // 读完响应体，连接才能复用
		return nil
	}
	errorBody, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	return &statusError{statusCode: response.StatusCode, body: string(errorBody)}
}

// isRetryableError 网络错误和 5xx 值得再试一次；4xx 和成功都不重试。
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *statusError
	if errors.As(err, &statusErr) {
		return statusErr.statusCode >= http.StatusInternalServerError
	}
	return true
}
