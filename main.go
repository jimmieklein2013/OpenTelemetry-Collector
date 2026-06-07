package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jimmieklein2013/OpenTelemetry-Collector/exporter/exporterhelper"
)

type simpleRequest struct {
	id    int
	items int
}

func (r *simpleRequest) ItemsCount() int {
	return r.items
}

func (r *simpleRequest) Export(ctx context.Context) error {
	fmt.Printf("Exporting request %d with %d items...\n", r.id, r.items)
	time.Sleep(100 * time.Millisecond) // Simulate work
	return nil
}

func main() {
	fmt.Println("Starting OpenTelemetry Collector Queue Backpressure Demo...")

	qCfg := exporterhelper.QueueSettings{
		Enabled:      true,
		NumConsumers: 1,
		QueueSize:    2,
	}
	rCfg := exporterhelper.RetrySettings{
		Enabled: false,
	}

	sender, err := exporterhelper.NewQueuedRetrySender("demo_exporter", "spans", qCfg, rCfg, nil)
	if err != nil {
		fmt.Printf("Failed to create sender: %v\n", err)
		return
	}

	sender.Start(context.Background())

	// Send requests to fill the queue and trigger backpressure
	for i := 1; i <= 5; i++ {
		req := &simpleRequest{id: i, items: 10}
		err := sender.Send(context.Background(), req)
		if err != nil {
			if errors.Is(err, exporterhelper.ErrQueueFull) {
				fmt.Printf("Request %d failed: Queue is full (Backpressure propagated successfully!)\n", i)
			} else {
				fmt.Printf("Request %d failed: %v\n", i, err)
			}
		} else {
			fmt.Printf("Request %d enqueued successfully\n", i)
		}
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("Shutting down sender...")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sender.Shutdown(ctx); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
	}
	fmt.Println("Demo finished.")
}
