// HTTP 发送器：把一批日志 POST 到日志平台的接收接口，请求体是 LogBatch，即 {"entries":[...]}。
// 平台抄送（Init 打开 LOG_PLATFORM_ENABLED）用它发；httpsender 子包保留了原来的名字，内部就是它。

package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// IngestKeyHeader 请求头里放共享密钥的名字，接收接口靠它判断是不是自己人发来的。
const IngestKeyHeader = "X-Log-Ingest-Key"

// httpRequestTimeout 单次 HTTP 请求最多等多久。
const httpRequestTimeout = 5 * time.Second

// maxErrorBodyBytes 请求失败时最多读多少字节的响应内容放进错误信息。
const maxErrorBodyBytes = 1024

// HTTPSender HTTP 发送器，实现 BatchSender。
type HTTPSender struct {
	endpoint  string
	ingestKey string
	client    *http.Client
}

// NewHTTPSender endpoint 是接收接口的完整地址，例如 https://example.com/api/logs/ingest；ingestKey 是共享密钥。
func NewHTTPSender(endpoint, ingestKey string) *HTTPSender {
	return &HTTPSender{
		endpoint:  endpoint,
		ingestKey: ingestKey,
		client:    &http.Client{Timeout: httpRequestTimeout},
	}
}

// SendBatch 把一批日志发出去。网络错误或服务端 5xx 时重试一次，还失败就返回错误；
// 4xx 说明密钥或数据有问题，重试也没用，直接返回。
func (sender *HTTPSender) SendBatch(ctx context.Context, entries []LogEntry) error {
	body, err := json.Marshal(LogBatch{Entries: entries})
	if err != nil {
		return fmt.Errorf("httpsender: 序列化日志失败: %w", err)
	}
	err = sender.postBatch(ctx, body)
	if isRetryableHTTPError(err) {
		err = sender.postBatch(ctx, body)
	}
	return err
}

// httpStatusError 接收接口返回了非 2xx 状态码。
type httpStatusError struct {
	statusCode int
	body       string
}

func (statusErr *httpStatusError) Error() string {
	return fmt.Sprintf("httpsender: 接收接口返回 %d: %s", statusErr.statusCode, statusErr.body)
}

// postBatch 发一次请求。
func (sender *HTTPSender) postBatch(ctx context.Context, body []byte) error {
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
	return &httpStatusError{statusCode: response.StatusCode, body: string(errorBody)}
}

// isRetryableHTTPError 网络错误和 5xx 值得再试一次；4xx 和成功都不重试。
func isRetryableHTTPError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.statusCode >= http.StatusInternalServerError
	}
	return true
}
