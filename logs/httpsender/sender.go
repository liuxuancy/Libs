// Package httpsender HTTP 发送器：把一批日志 POST 到日志平台的接收接口。
// 实现已经挪到 logs 包（logs.HTTPSender），平台抄送也用它；这里保留原来的名字，已有的代码不用改。
// 想把日志抄送到日志平台，直接用 logs.Init 加 LOG_PLATFORM_* 环境变量更简单，不用自己创建发送器。
package httpsender

import "github.com/liuxuancy/Libs/logs"

// IngestKeyHeader 请求头里放共享密钥的名字，接收接口靠它判断是不是自己人发来的。
const IngestKeyHeader = logs.IngestKeyHeader

// Sender HTTP 发送器，实现 logs.BatchSender。
type Sender = logs.HTTPSender

// NewSender endpoint 是接收接口的完整地址，例如 https://example.com/api/logs/ingest；ingestKey 是共享密钥。
func NewSender(endpoint, ingestKey string) *Sender {
	return logs.NewHTTPSender(endpoint, ingestKey)
}
