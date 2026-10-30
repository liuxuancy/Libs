// 接收：日志平台用 ReadIngestRequest 从发送方的 HTTP 请求里取出一批日志（核对密钥、限制大小、解析、校验），
// 之后存到哪里、怎么回复都由平台自己决定。不经过 HTTP 的接收方（比如 MQ 消费者）用 ValidateLogEntries 做同样的校验。

package logs

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	// MaxIngestBodyBytes 一次请求的请求体最多多大，防止被一次性写爆
	MaxIngestBodyBytes = 4 << 20
	// MaxIngestEntries 一次最多多少条日志；收集器默认一批 200 条，留足余量
	MaxIngestEntries = 2000
)

// IngestError 接收时发现的问题，StatusCode 是平台应该回复的 HTTP 状态码，发送方按它决定要不要重试。
type IngestError struct {
	StatusCode int
	Message    string
}

func (ingestErr *IngestError) Error() string {
	return ingestErr.Message
}

// ReadIngestRequest 从一个 HTTP 请求里取出发来的日志：核对请求头里的密钥、限制请求体大小、解析成 LogBatch、校验每一条，
// 全部通过才返回日志。出错时返回 *IngestError，用 GetIngestErrorStatus 取该回复的状态码。
// ingestKey 是平台配置的接收密钥，为空表示接收功能没打开，一律返回 503。
// 平台拿到日志后：真正存好了才回复 2xx，存失败回复 5xx（发送方会重试），按 EntryID 去重（重试时同一条可能来两次）。
func ReadIngestRequest(request *http.Request, ingestKey string) ([]LogEntry, error) {
	if ingestKey == "" {
		return nil, &IngestError{StatusCode: http.StatusServiceUnavailable, Message: "日志接收接口未启用：服务端没有配置接收密钥"}
	}
	// 用固定耗时的比较，避免通过响应快慢一位一位猜出密钥
	providedKey := request.Header.Get(IngestKeyHeader)
	if subtle.ConstantTimeCompare([]byte(providedKey), []byte(ingestKey)) != 1 {
		return nil, &IngestError{StatusCode: http.StatusUnauthorized, Message: "日志接收密钥不正确"}
	}

	// 多读 1 个字节：读到了就说明超过上限
	body, err := io.ReadAll(io.LimitReader(request.Body, MaxIngestBodyBytes+1))
	if err != nil {
		return nil, &IngestError{StatusCode: http.StatusBadRequest, Message: "读取请求体失败: " + err.Error()}
	}
	if len(body) > MaxIngestBodyBytes {
		return nil, &IngestError{StatusCode: http.StatusRequestEntityTooLarge, Message: fmt.Sprintf("请求体不能超过 %dMB", MaxIngestBodyBytes>>20)}
	}
	var batch LogBatch
	if err := json.Unmarshal(body, &batch); err != nil {
		return nil, &IngestError{StatusCode: http.StatusBadRequest, Message: "请求体不是合法的 JSON"}
	}
	if err := ValidateLogEntries(batch.Entries); err != nil {
		return nil, err
	}
	return batch.Entries, nil
}

// ValidateLogEntries 检查一批日志：条数在 1 到 MaxIngestEntries 之间，每一条的环境是 prod 或 dev、带了服务名。
// 其余字段超长、缺失由接收方自己截断或补默认值。出错时返回 *IngestError（400）。
func ValidateLogEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return &IngestError{StatusCode: http.StatusBadRequest, Message: "entries 不能为空"}
	}
	if len(entries) > MaxIngestEntries {
		return &IngestError{StatusCode: http.StatusBadRequest, Message: fmt.Sprintf("一次最多发送 %d 条日志", MaxIngestEntries)}
	}
	for index, entry := range entries {
		if entry.Environment != EnvironmentProd && entry.Environment != EnvironmentDev {
			return &IngestError{StatusCode: http.StatusBadRequest, Message: fmt.Sprintf("第 %d 条日志的 env 只能是 prod 或 dev", index+1)}
		}
		if entry.ServiceName == "" {
			return &IngestError{StatusCode: http.StatusBadRequest, Message: fmt.Sprintf("第 %d 条日志缺少 service", index+1)}
		}
	}
	return nil
}

// GetIngestErrorStatus 取出 ReadIngestRequest / ValidateLogEntries 返回的错误该回复的 HTTP 状态码；不是它们返回的错误按 500 处理。
func GetIngestErrorStatus(err error) int {
	// 用 errors.As 而不是 errors.AsType：日志包的 go.mod 声明的是 go 1.25，AsType 要 1.26 才有
	var ingestErr *IngestError
	if errors.As(err, &ingestErr) {
		return ingestErr.StatusCode
	}
	return http.StatusInternalServerError
}
