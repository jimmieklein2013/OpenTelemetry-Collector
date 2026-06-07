package exporterhelper

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var ErrQueueFull = errors.New("sending queue is full")

// Request represents the telemetry data to be sent.
type Request interface {
	// ItemsCount returns the number of items (spans, metric points, log records) in the request.
	ItemsCount() int
	// Export exports the request.
	Export(ctx context.Context) error
}

type QueueSettings struct {
	Enabled      bool
	NumConsumers int
	QueueSize    int
}

type RetrySettings struct {
	Enabled         bool
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
}

type QueuedRetrySender struct {
	cfg           QueueSettings
	retryCfg      RetrySettings
	exporterName  string
	dataType      string
	queue         chan Request
	wg            sync.WaitGroup
	stopCh        chan struct{}
	running       int32

	// Metrics
	meter               metric.Meter
	failedSpansCounter  metric.Int64Counter
	queueCapacityGauge  metric.Int64ObservableGauge

	// Internal counters for testing/fallback
	FailedSpansMetric   int64
	QueueCapacityMetric int64
}

func NewQueuedRetrySender(exporterName string, dataType string, qCfg QueueSettings, rCfg RetrySettings, meter metric.Meter) (*QueuedRetrySender, error) {
	if qCfg.QueueSize <= 0 {
		qCfg.QueueSize = 1000
	}
	if qCfg.NumConsumers <= 0 {
		qCfg.NumConsumers = 10
	}

	sender := &QueuedRetrySender{
		cfg:                 qCfg,
		retryCfg:            rCfg,
		exporterName:        exporterName,
		dataType:            dataType,
		queue:               make(chan Request, qCfg.QueueSize),
		stopCh:              make(chan struct{}),
		QueueCapacityMetric: int64(qCfg.QueueSize),
		meter:               meter,
	}

	if meter != nil {
		var err error
		sender.failedSpansCounter, err = meter.Int64Counter(
			"otelcol_exporter_enqueue_failed_spans",
			metric.WithDescription("Number of spans failed to be enqueued"),
		)
		if err != nil {
			return nil, err
		}

		sender.queueCapacityGauge, err = meter.Int64ObservableGauge(
			"otelcol_exporter_queue_capacity",
			metric.WithDescription("Capacity of the exporter queue"),
			metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
				obs.Observe(int64(qCfg.QueueSize), metric.WithAttributes(
					attribute.String("exporter", exporterName),
					attribute.String("data_type", dataType),
				))
				return nil
			}),
		)
		if err != nil {
			return nil, err
		}
	}

	return sender, nil
}

func (s *QueuedRetrySender) Start(ctx context.Context) {
	atomic.StoreInt32(&s.running, 1)
	for i := 0; i < s.cfg.NumConsumers; i++ {
		s.wg.Add(1)
		go s.consumerLoop()
	}
}

func (s *QueuedRetrySender) Send(ctx context.Context, req Request) error {
	if atomic.LoadInt32(&s.running) == 0 {
		return errors.New("sender is not running")
	}

	select {
	case s.queue <- req:
		return nil
	default:
		// Queue is full
		s.recordDrop(ctx, req.ItemsCount())
		log.Printf("WARN: Exporter queue is full. Dropping data. exporter=%s, datatype=%s, items=%d",
			s.exporterName, s.dataType, req.ItemsCount())
		return ErrQueueFull
	}
}

func (s *QueuedRetrySender) recordDrop(ctx context.Context, items int) {
	atomic.AddInt64(&s.FailedSpansMetric, int64(items))
	if s.failedSpansCounter != nil {
		s.failedSpansCounter.Add(ctx, int64(items), metric.WithAttributes(
			attribute.String("exporter", s.exporterName),
			attribute.String("data_type", s.dataType),
		))
	}
}

func (s *QueuedRetrySender) consumerLoop() {
	defer s.wg.Done()
	for {
		select {
		case req, ok := <-s.queue:
			if !ok {
				return
			}
			s.sendWithRetry(req)
		case <-s.stopCh:
			return
		}
	}
}

func (s *QueuedRetrySender) sendWithRetry(req Request) {
	ctx := context.Background()
	if !s.retryCfg.Enabled {
		err := req.Export(ctx)
		if err != nil {
			log.Printf("ERROR: Export failed. Dropping data. exporter=%s, datatype=%s, items=%d, error=%v",
				s.exporterName, s.dataType, req.ItemsCount(), err)
			s.recordDrop(ctx, req.ItemsCount())
		}
		return
	}

	interval := s.retryCfg.InitialInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	maxInterval := s.retryCfg.MaxInterval
	if maxInterval <= 0 {
		maxInterval = 30 * time.Second
	}
	maxElapsedTime := s.retryCfg.MaxElapsedTime
	if maxElapsedTime <= 0 {
		maxElapsedTime = 5 * time.Minute
	}

	startTime := time.Now()
	for {
		err := req.Export(ctx)
		if err == nil {
			return
		}

		if time.Since(startTime) > maxElapsedTime {
			log.Printf("ERROR: Max retry elapsed time exceeded. Dropping data. exporter=%s, datatype=%s, items=%d, error=%v",
				s.exporterName, s.dataType, req.ItemsCount(), err)
			s.recordDrop(ctx, req.ItemsCount())
			return
		}

		select {
		case <-time.After(interval):
		case <-s.stopCh:
			log.Printf("WARN: Shutdown during retry. Dropping data. exporter=%s, datatype=%s, items=%d",
				s.exporterName, s.dataType, req.ItemsCount())
			s.recordDrop(ctx, req.ItemsCount())
			return
		}

		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
	}
}

func (s *QueuedRetrySender) Shutdown(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return nil
	}

	close(s.stopCh)

	// Wait for consumers to finish current tasks
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Flush remaining items in the queue
	close(s.queue)
	for req := range s.queue {
		exportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := req.Export(exportCtx)
		cancel()
		if err != nil {
			log.Printf("ERROR: Failed to flush request during shutdown. exporter=%s, datatype=%s, items=%d, error=%v",
				s.exporterName, s.dataType, req.ItemsCount(), err)
			s.recordDrop(context.Background(), req.ItemsCount())
		}
	}

	return nil
}
