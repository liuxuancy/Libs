// 日志收集器：在 stdout 输出旁边再并一路，把达到收集级别的日志转成 LogEntry 交给批量缓冲。

package logs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// 收集器参数的默认值。
const (
	defaultBatchSize     = 200
	defaultFlushInterval = time.Second
	defaultQueueCapacity = 10000
)

// CollectorOptions 收集器配置。
type CollectorOptions struct {
	ServiceName   string        // 服务名（平台名），每条日志都会带上
	Environment   string        // 环境：prod / dev
	Sender        BatchSender   // 攒好一批后交给它，不能为 nil
	MinLevel      slog.Level    // 低于这个级别只打 stdout、不收集；零值就是 INFO
	BatchSize     int           // 攒够多少条就发一批，<=0 用默认 200
	FlushInterval time.Duration // 最长隔多久发一批，<=0 用默认 1 秒
	QueueCapacity int           // 内存队列容量，满了直接丢弃，<=0 用默认 10000
}

// collectorPath 一路收集：批量缓冲加上往里放日志的 handler。
// 主路（SetupCollector 挂的）和平台抄送（Init 打开的）各是一路，有各自的队列和协程，一路卡住不影响另一路。
type collectorPath struct {
	buffer  *entryBuffer
	handler *collectorHandler
}

var (
	collectorMutex sync.Mutex
	// mainPath SetupCollector 挂的那一路，比如"先发 MQ、失败直接写库"；没挂时为 nil
	mainPath *collectorPath
	// platformPath Init 打开平台抄送时挂的那一路；没打开时为 nil
	platformPath *collectorPath
)

// SetupCollector 挂上收集器：之后打的日志除了照常输出到 Setup 传入的 handler，达到 MinLevel 的还会攒批交给 Sender。
// 必须在 Setup（或 Init）之后调用；退出前调用 Shutdown（或 ShutdownCollector）把剩下的日志发完。
// 全局只初始化一次：启动时由 main.go 调用。
func SetupCollector(options CollectorOptions) error {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	if stdoutHandler == nil {
		return errors.New("logs: 必须先调用 Setup 再挂收集器")
	}
	if options.Sender == nil {
		return errors.New("logs: 收集器的 Sender 不能为空")
	}
	if mainPath != nil {
		return errors.New("logs: 收集器已经挂过了，不能重复挂")
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultBatchSize
	}
	if options.FlushInterval <= 0 {
		options.FlushInterval = defaultFlushInterval
	}
	if options.QueueCapacity <= 0 {
		options.QueueCapacity = defaultQueueCapacity
	}
	mainPath = newCollectorPath(options)
	installDefaultLogger()
	return nil
}

// ShutdownCollector 摘掉 SetupCollector 挂的那一路，并把队列里剩下的日志发完，最多等到 ctx 结束。没挂时什么也不做。
// 平台抄送那一路不在这里收尾，用了 Init 的程序退出前调用 Shutdown，两路一起收尾。
// 全局只调用一次：进程退出前由 main.go 调用。
func ShutdownCollector(ctx context.Context) error {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	return shutdownCollectorPath(ctx, &mainPath)
}

// newCollectorPath 创建一路收集并启动它的后台协程；options 要先补好默认值。
func newCollectorPath(options CollectorOptions) *collectorPath {
	buffer := newEntryBuffer(options.Sender, options.BatchSize, options.FlushInterval, options.QueueCapacity, os.Stderr)
	buffer.start()
	return &collectorPath{
		buffer: buffer,
		handler: &collectorHandler{
			buffer:      buffer,
			serviceName: options.ServiceName,
			environment: options.Environment,
			minLevel:    options.MinLevel,
		},
	}
}

// shutdownCollectorPath 摘掉一路收集并把它剩下的日志发完；path 指向 mainPath 或 platformPath。调用方要持有 collectorMutex。
func shutdownCollectorPath(ctx context.Context, path **collectorPath) error {
	if *path == nil {
		return nil
	}
	stoppingPath := *path
	*path = nil
	// 先把这一路从全局 logger 里摘掉，后面再打的日志不再进它的队列
	installDefaultLogger()
	return stoppingPath.buffer.stop(ctx)
}

// installDefaultLogger 按现在挂着的几路重建全局 logger：终端输出，再加上主路、平台抄送（挂了哪路加哪路）。
// 调用方要持有 collectorMutex。
func installDefaultLogger() {
	handlers := []slog.Handler{stdoutHandler}
	for _, path := range []*collectorPath{mainPath, platformPath} {
		if path != nil {
			handlers = append(handlers, path.handler)
		}
	}
	if len(handlers) == 1 {
		slog.SetDefault(slog.New(NewContextHandler(stdoutHandler)))
		return
	}
	slog.SetDefault(slog.New(NewContextHandler(newFanoutHandler(handlers...))))
}

// collectorHandler 把日志转成 LogEntry 放进批量缓冲。实现 slog.Handler。
type collectorHandler struct {
	buffer      *entryBuffer
	serviceName string
	environment string
	minLevel    slog.Level
	presetAttrs map[string]any // 通过 logger.With 预先挂上的字段，已经按分组展平
	groupPrefix string         // 通过 logger.WithGroup 进入的分组，形如 "组1.组2."
}

func (handler *collectorHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= handler.minLevel
}

func (handler *collectorHandler) Handle(ctx context.Context, record slog.Record) error {
	return handler.handleWithEntryID(ctx, record, newEntryID())
}

// handleWithEntryID 用指定的条目 ID 转成 LogEntry 放进队列。
// 同一条日志同时进主路和平台抄送两路时，fanoutHandler 给两路传同一个 ID，接收方按 ID 去重时认得出是同一条。
func (handler *collectorHandler) handleWithEntryID(_ context.Context, record slog.Record, entryID string) error {
	entry := LogEntry{
		EntryID:     entryID,
		Time:        record.Time,
		Level:       record.Level.String(),
		ServiceName: handler.serviceName,
		Environment: handler.environment,
		Source:      getShortSourceFromPC(record.PC),
		Message:     record.Message,
	}

	attributes := make(map[string]any, len(handler.presetAttrs)+record.NumAttrs())
	for key, value := range handler.presetAttrs {
		attributes[key] = value
	}
	record.Attrs(func(attr slog.Attr) bool {
		addFlattenedAttribute(attributes, handler.groupPrefix, attr)
		return true
	})
	// log_id 是单独的一列，从附加字段里挪出来
	if logID, ok := attributes[LogIDKey].(string); ok {
		entry.LogID = logID
		delete(attributes, LogIDKey)
	}
	if len(attributes) > 0 {
		entry.Attributes = attributes
	}

	handler.buffer.enqueue(entry)
	return nil
}

func (handler *collectorHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cloned := *handler
	cloned.presetAttrs = make(map[string]any, len(handler.presetAttrs)+len(attrs))
	for key, value := range handler.presetAttrs {
		cloned.presetAttrs[key] = value
	}
	for _, attr := range attrs {
		addFlattenedAttribute(cloned.presetAttrs, handler.groupPrefix, attr)
	}
	return &cloned
}

func (handler *collectorHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return handler
	}
	cloned := *handler
	cloned.groupPrefix = handler.groupPrefix + name + "."
	return &cloned
}

// addFlattenedAttribute 把一个字段写进 attributes；分组字段展平成 "组.字段"，和 slog 自带的规则一样忽略空 key。
func addFlattenedAttribute(attributes map[string]any, prefix string, attr slog.Attr) {
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		groupPrefix := prefix
		if attr.Key != "" {
			groupPrefix = prefix + attr.Key + "."
		}
		for _, groupAttr := range value.Group() {
			addFlattenedAttribute(attributes, groupPrefix, groupAttr)
		}
		return
	}
	if attr.Key == "" {
		return
	}
	attributes[prefix+attr.Key] = convertAttributeValue(value)
}

// convertAttributeValue 把字段值转成能直接 JSON 序列化、之后也不会再被改动的值。
// 复杂对象在这里就序列化成 JSON，防止业务代码在日志发出去之前又改了这个对象。
func convertAttributeValue(value slog.Value) any {
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time().Format(time.RFC3339Nano)
	}

	switch typedValue := value.Any().(type) {
	case error:
		return typedValue.Error()
	case fmt.Stringer:
		return typedValue.String()
	}
	encoded, err := json.Marshal(value.Any())
	if err != nil {
		return fmt.Sprint(value.Any())
	}
	return json.RawMessage(encoded)
}

// fanoutHandler 把同一条日志同时交给多个下游（stdout + 收集器），各自按自己的级别过滤。
type fanoutHandler struct {
	handlers []slog.Handler
}

func newFanoutHandler(handlers ...slog.Handler) *fanoutHandler {
	return &fanoutHandler{handlers: handlers}
}

func (handler *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, downstream := range handler.handlers {
		if downstream.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle 每个下游各拿一份 Record 副本，一个下游出错不影响其他下游，最后返回第一个错误。
// 收集器那几路共用同一个条目 ID：同一条日志进了主路和平台抄送，在两边都是同一个 ID。
func (handler *fanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	var firstErr error
	entryID := ""
	for _, downstream := range handler.handlers {
		if !downstream.Enabled(ctx, record.Level) {
			continue
		}
		var err error
		if collector, isCollector := downstream.(*collectorHandler); isCollector {
			if entryID == "" {
				entryID = newEntryID()
			}
			err = collector.handleWithEntryID(ctx, record.Clone(), entryID)
		} else {
			err = downstream.Handle(ctx, record.Clone())
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (handler *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(handler.handlers))
	for index, downstream := range handler.handlers {
		handlers[index] = downstream.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: handlers}
}

func (handler *fanoutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(handler.handlers))
	for index, downstream := range handler.handlers {
		handlers[index] = downstream.WithGroup(name)
	}
	return &fanoutHandler{handlers: handlers}
}
