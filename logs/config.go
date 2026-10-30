// 一步配好日志用的配置：Config 交给 Init；LoadConfigFromEnv 按约定好的 LOG_ 环境变量把它填好。

package logs

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 日志环境只有这两种取值，日志平台按它区分线上和本地的日志。
const (
	EnvironmentProd = "prod"
	EnvironmentDev  = "dev"
)

// LoadConfigFromEnv 读的环境变量。
const (
	ServiceNameEnv     = "LOG_SERVICE_NAME"
	EnvironmentEnv     = "LOG_ENVIRONMENT"
	LevelEnv           = "LOG_LEVEL"
	PlatformEnabledEnv = "LOG_PLATFORM_ENABLED"
	PlatformURLEnv     = "LOG_PLATFORM_URL"
	PlatformKeyEnv     = "LOG_PLATFORM_KEY"
)

// 平台抄送重试的默认值。
const (
	defaultRetryInterval    = 10 * time.Second
	defaultMaxRetryCount    = 30
	defaultMaxRetryDuration = 10 * time.Minute
	// defaultProdRetryCooldown 线上一轮重试用完后冷却多久再开始下一轮；本地（dev）默认用完就不再重试
	defaultProdRetryCooldown = 10 * time.Minute
)

// Config Init 的配置。字段都可以不填，不填用默认值。
type Config struct {
	ServiceName string     // 服务名，日志平台上按它区分程序；不填用程序文件名
	Environment string     // prod / dev，不填是 dev
	Level       slog.Level // 终端和平台都只要这个级别及以上，零值就是 INFO
	Platform    PlatformConfig
}

// PlatformConfig 抄送到日志平台的配置：日志照常打终端，同时在后台抄送一份到平台的日志接收接口。
type PlatformConfig struct {
	Enabled   bool   // 总开关，不打开就只打终端
	IngestURL string // 平台的日志接收接口，例如 https://example.com/api/logs/ingest
	IngestKey string // 接收密钥，和平台配置的一致

	// 连不上平台时的重试：隔 RetryInterval 试一次，试满 MaxRetryCount 次或者过了 MaxRetryDuration（先到为准）算一轮用完。
	// 不填分别用 10 秒、30 次、10 分钟
	RetryInterval    time.Duration
	MaxRetryCount    int
	MaxRetryDuration time.Duration
	// RetryCooldown 一轮用完后冷却多久再开始下一轮；小于 0 表示不再重试，直到程序重启。
	// 不填（0）按环境取：prod 冷却 10 分钟，dev 不再重试
	RetryCooldown time.Duration
}

// LoadConfigFromEnv 按约定的环境变量填好 Config。只读进程的环境变量，不读 .env 文件：用 .env 的程序要先加载它再调用这里。
// 某一项写得不对（比如级别拼错）只在 stderr 提示一次，用默认值，不让程序启动失败。
func LoadConfigFromEnv() Config {
	return Config{
		ServiceName: strings.TrimSpace(os.Getenv(ServiceNameEnv)),
		Environment: strings.TrimSpace(os.Getenv(EnvironmentEnv)),
		Level:       parseLevelEnv(os.Getenv(LevelEnv)),
		Platform: PlatformConfig{
			Enabled:   parseBoolEnv(PlatformEnabledEnv, os.Getenv(PlatformEnabledEnv)),
			IngestURL: strings.TrimSpace(os.Getenv(PlatformURLEnv)),
			IngestKey: strings.TrimSpace(os.Getenv(PlatformKeyEnv)),
		},
	}
}

// parseLevelEnv 把 debug / info / warn / error 换成 slog 的级别，不区分大小写；空的或写错了用 INFO。
func parseLevelEnv(text string) slog.Level {
	text = strings.TrimSpace(text)
	if text == "" {
		return slog.LevelInfo
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(text)); err != nil {
		fmt.Fprintf(os.Stderr, "logs: %s=%q 不认识，只能是 debug / info / warn / error，按 info 处理\n", LevelEnv, text)
		return slog.LevelInfo
	}
	return level
}

// parseBoolEnv 认 true / false / 1 / 0 等；空的或写错了都算关。
func parseBoolEnv(envName string, text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	value, err := strconv.ParseBool(text)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logs: %s=%q 不认识，只能是 true 或 false，按 false 处理\n", envName, text)
		return false
	}
	return value
}

// applyConfigDefaults 把没填的字段补上默认值；环境写错了提示一次并按 dev 处理（日志平台只收 prod / dev）。
func applyConfigDefaults(cfg Config) Config {
	if cfg.ServiceName == "" {
		cfg.ServiceName = getProgramName()
	}
	switch cfg.Environment {
	case EnvironmentProd, EnvironmentDev:
	case "":
		cfg.Environment = EnvironmentDev
	default:
		fmt.Fprintf(os.Stderr, "logs: 环境 %q 不认识，只能是 prod 或 dev，按 dev 处理\n", cfg.Environment)
		cfg.Environment = EnvironmentDev
	}

	platform := &cfg.Platform
	if platform.RetryInterval <= 0 {
		platform.RetryInterval = defaultRetryInterval
	}
	if platform.MaxRetryCount <= 0 {
		platform.MaxRetryCount = defaultMaxRetryCount
	}
	if platform.MaxRetryDuration <= 0 {
		platform.MaxRetryDuration = defaultMaxRetryDuration
	}
	if platform.RetryCooldown == 0 {
		platform.RetryCooldown = -1
		if cfg.Environment == EnvironmentProd {
			platform.RetryCooldown = defaultProdRetryCooldown
		}
	}
	return cfg
}

// getProgramName 程序文件名，去掉 Windows 上的 .exe，没填服务名时用它。
func getProgramName() string {
	return strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe")
}
