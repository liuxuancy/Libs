// logid 的生成、校验和在 HTTP 头里的名字。

package logs

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// LogIDHeader 请求头和响应头里传 logid 用的名字。服务之间互相调用时把它带过去，两边的日志就能按同一个 logid 查到。
const LogIDHeader = "X-Log-Id"

// maxLogIDLength 外部传进来的 logid 最长多少个字符，超过就当作无效、重新生成。
const maxLogIDLength = 64

// NewLogID 生成一个新的 logid：前 14 位是本地时间 yyyyMMddHHmmss，后面 10 位随机十六进制。
// 开头放时间，是为了只按 logid 查日志时能算出日期，只查那一天的分区。
func NewLogID() string {
	return time.Now().Format("20060102150405") + generateRandomHex(5)
}

// IsValidLogID 判断外部传进来的 logid 能不能直接沿用：非空、不太长、只含字母数字和 - _。
// 防止有人在请求头里塞超长内容或者换行之类的字符污染日志。
func IsValidLogID(logID string) bool {
	if logID == "" || len(logID) > maxLogIDLength {
		return false
	}
	for _, character := range logID {
		isLetterOrDigit := (character >= '0' && character <= '9') || (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		if !isLetterOrDigit && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

// newEntryID 生成每一行日志自己的 ID（条目 ID），写库时靠它去重。
func newEntryID() string {
	return generateRandomHex(12)
}

// generateRandomHex 生成 byteCount 个随机字节的十六进制串。
// Go 1.24 起 crypto/rand.Read 保证不会返回错误，所以这里不处理错误。
func generateRandomHex(byteCount int) string {
	randomBytes := make([]byte, byteCount)
	_, _ = rand.Read(randomBytes)
	return hex.EncodeToString(randomBytes)
}
