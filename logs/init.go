// 一步配好日志的入口：Init 按 Config 设置终端输出、打开平台抄送；Shutdown 退出前把各路剩下的日志发完。

package logs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

// Init 按 cfg 一步配好日志：终端输出 JSON（带代码位置，级别 cfg.Level）；cfg.Platform.Enabled 时，
// 再在后台把日志抄送到日志平台。之后直接用 CtxInfo 等打日志。
// 配置有问题只往 stderr 提示、不报错，日志功能出问题不能拖累程序启动。退出前调用 Shutdown。
// 全局只初始化一次：启动时由 main 调用。
func Init(cfg Config) {
	cfg = applyConfigDefaults(cfg)
	Setup(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:       cfg.Level,
		AddSource:   true,
		ReplaceAttr: ShortSource,
	}))
	startPlatformPath(cfg)
}

// Shutdown 退出前收尾：SetupCollector 挂的那一路和平台抄送那一路都把剩下的日志发完，最多等到 ctx 结束。
// 全局只调用一次：进程退出前由 main 调用。
func Shutdown(ctx context.Context) error {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	return errors.Join(shutdownCollectorPath(ctx, &mainPath), shutdownCollectorPath(ctx, &platformPath))
}

// startPlatformPath 开关打开、地址和密钥都填了，就挂上抄送到平台的那一路；否则只打终端。
func startPlatformPath(cfg Config) {
	platform := cfg.Platform
	if !platform.Enabled {
		return
	}
	if platform.IngestURL == "" || platform.IngestKey == "" {
		fmt.Fprintf(os.Stderr, "logs: 打开了 %s，但 %s 或 %s 没填，不抄送到日志平台\n", PlatformEnabledEnv, PlatformURLEnv, PlatformKeyEnv)
		return
	}

	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	if platformPath != nil {
		fmt.Fprintln(os.Stderr, "logs: 平台抄送已经打开过了，不重复打开")
		return
	}
	sender := newPlatformSender(NewHTTPSender(platform.IngestURL, platform.IngestKey), platform.IngestURL, platform, os.Stderr)
	platformPath = newCollectorPath(CollectorOptions{
		ServiceName:   cfg.ServiceName,
		Environment:   cfg.Environment,
		Sender:        sender,
		MinLevel:      cfg.Level,
		BatchSize:     defaultBatchSize,
		FlushInterval: defaultFlushInterval,
		QueueCapacity: defaultQueueCapacity,
	})
	installDefaultLogger()
}
