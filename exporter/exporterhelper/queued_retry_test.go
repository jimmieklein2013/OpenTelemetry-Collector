package exporterhelper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRequest struct {
	itemsCount int
	exportFunc func(ctx context.Context) error
}

func (m *mockRequest) ItemsCount() int {
	return m.itemsCount
}

func (m *mockRequest) Export(ctx context.Context) error {
	if m.exportFunc != nil {
		return m.exportFunc(ctx)
	}
	return nil
}

func TestQueuedRetrySender_QueueFull(t *testing.T) {
	qCfg := QueueSettings{
		Enabled:      true,
		NumConsumers: 1,
		QueueSize:    1,
	}
	rCfg := RetrySettings{
		Enabled: false,
	}

	sender, err := NewQueuedRetrySender("mock_exporter", "spans", qCfg, rCfg, nil)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}

	// Block the consumer by making the first request block
	blockChan := make(chan struct{})
	firstReq := &mockRequest{
		itemsCount: 10,
		exportFunc: func(ctx context.Context) error {
			<-blockChan
			return nil
		},
	}

	sender.Start(context.Background())
	defer func() {
		close(blockChan)
		sender.Shutdown(context.Background())
	}()

	// Send first request, which will be picked up by the consumer and block
	err = sender.Send(context.Background(), firstReq)
	if err != nil {
		t.Fatalf("expected no error on first send, got %v", err)
	}

	// Send second request, which will fill the queue (size 1)
	secondReq := &mockRequest{
		itemsCount: 5,
	}
	err = sender.Send(context.Background(), secondReq)
	if err != nil {
		t.Fatalf("expected no error on second send (fills queue), got %v", err)
	}

	// Send third request, which should fail with ErrQueueFull
	thirdReq := &mockRequest{
		itemsCount: 8,
	}
	err = sender.Send(context.Background(), thirdReq)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Verify metrics
	if sender.FailedSpansMetric != 8 {
		t.Errorf("expected FailedSpansMetric to be 8, got %d", sender.FailedSpansMetric)
	}
	if sender.QueueCapacityMetric != 1 {
		t.Errorf("expected QueueCapacityMetric to be 1, got %d", sender.QueueCapacityMetric)
	}
}

func TestQueuedRetrySender_GracefulShutdown(t *testing.T) {
	qCfg := QueueSettings{
		Enabled:      true,
		NumConsumers: 1,
		QueueSize:    5,
	}
	rCfg := RetrySettings{
		Enabled: false,
	}

	sender, err := NewQueuedRetrySender("mock_exporter", "spans", qCfg, rCfg, nil)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	sender.Start(context.Background())

	var mu sync.Mutex
	var exportedItems []int

	// Send multiple requests
	for i := 1; i <= 3; i++ {
		val := i
		req := &mockRequest{
			itemsCount: val,
			exportFunc: func(ctx context.Context) error {
				mu.Lock()
				exportedItems = append(exportedItems, val)
				mu.Unlock()
				return nil
			},
		}
		err := sender.Send(context.Background(), req)
		if err != nil {
			t.Fatalf("failed to send: %v", err)
		}
	}

	// Shutdown should flush remaining items
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = sender.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(exportedItems) != 3 {
		t.Errorf("expected 3 items to be exported, got %d", len(exportedItems))
	}
}
