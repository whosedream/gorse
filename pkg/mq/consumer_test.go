package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func collectBatches(t *testing.T, out <-chan Batch, want int) []Batch {
	t.Helper()
	batches := make([]Batch, 0, want)
	deadline := time.After(time.Second)
	for len(batches) < want {
		select {
		case b := <-out:
			batches = append(batches, b)
		case <-deadline:
			t.Fatalf("timed out waiting for %d batches, got %d", want, len(batches))
		}
	}
	return batches
}

func runConsumer(t *testing.T, c *Consumer, ctx context.Context, out chan<- Batch) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, out) }()
	return done
}

func TestConsumerBatchingFlushCancelAndBaseline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "按 UserID 聚合且 MaxBatch 触发 flush 并取最大时间戳",
			run: func(t *testing.T) {
				events := make(chan Event, 4)
				out := make(chan Batch, 4)
				c, err := NewConsumer(MemorySource{C: events}, Options{MaxBatch: 2, FlushInterval: time.Hour})
				if err != nil {
					t.Fatalf("NewConsumer returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := runConsumer(t, c, ctx, out)

				events <- Event{UserID: "u1", ItemID: "i1", Timestamp: 10, Action: "click"}
				events <- Event{UserID: "u2", ItemID: "i9", Timestamp: 99, Action: "cart"}
				events <- Event{UserID: "u1", ItemID: "i2", Timestamp: 20, Action: "buy"}

				batches := collectBatches(t, out, 1)
				got := batches[0]
				if got.UserID != "u1" || len(got.Events) != 2 || got.BaselineVersion != 20 {
					t.Fatalf("unexpected u1 batch: %+v", got)
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected context.Canceled, got %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("consumer did not stop after cancel")
				}
			},
		},
		{
			name: "FlushInterval 到期 flush 未满批次事件",
			run: func(t *testing.T) {
				events := make(chan Event, 2)
				out := make(chan Batch, 2)
				c, err := NewConsumer(MemorySource{C: events}, Options{MaxBatch: 8, FlushInterval: 10 * time.Millisecond})
				if err != nil {
					t.Fatalf("NewConsumer returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := runConsumer(t, c, ctx, out)

				events <- Event{UserID: "u3", ItemID: "long-malicious-item-id-但只能作为普通字段", CategoryID: "c", Timestamp: 7, Action: "view"}
				got := collectBatches(t, out, 1)[0]
				if got.UserID != "u3" || len(got.Events) != 1 || got.BaselineVersion != 7 {
					t.Fatalf("unexpected interval batch: %+v", got)
				}
				cancel()
				<-done
			},
		},
		{
			name: "ctx cancel 前 flush 已聚合事件且快速退出",
			run: func(t *testing.T) {
				events := make(chan Event, 4)
				out := make(chan Batch, 4)
				c, err := NewConsumer(MemorySource{C: events}, Options{MaxBatch: 10, FlushInterval: time.Hour})
				if err != nil {
					t.Fatalf("NewConsumer returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := runConsumer(t, c, ctx, out)

				events <- Event{UserID: "u4", ItemID: "i1", Timestamp: 1, Action: "click"}
				events <- Event{UserID: "u4", ItemID: "i2", Timestamp: 5, Action: "click"}
				cancel()

				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected context.Canceled, got %v", err)
					}
				case <-time.After(100 * time.Millisecond):
					t.Fatal("consumer cancel path was not fast")
				}
				got := collectBatches(t, out, 1)[0]
				if got.UserID != "u4" || len(got.Events) != 2 || got.BaselineVersion != 5 {
					t.Fatalf("cancel did not flush aggregate: %+v", got)
				}
			},
		},
		{
			name: "全空输入 cancel 不产生空 batch",
			run: func(t *testing.T) {
				events := make(chan Event)
				out := make(chan Batch, 1)
				c, err := NewConsumer(MemorySource{C: events}, Options{MaxBatch: 2, FlushInterval: time.Millisecond})
				if err != nil {
					t.Fatalf("NewConsumer returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := runConsumer(t, c, ctx, out)
				cancel()
				<-done
				select {
				case b := <-out:
					t.Fatalf("unexpected empty batch: %+v", b)
				default:
				}
			},
		},
		{
			name: "高并发瞬时浪涌按用户分批且不丢事件",
			run: func(t *testing.T) {
				const total = 256
				events := make(chan Event, total)
				out := make(chan Batch, total)
				c, err := NewConsumer(MemorySource{C: events}, Options{MaxBatch: 64, FlushInterval: time.Hour})
				if err != nil {
					t.Fatalf("NewConsumer returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := runConsumer(t, c, ctx, out)
				for i := 0; i < total; i++ {
					events <- Event{UserID: "hot", ItemID: "sku", Timestamp: int64(i), Action: "click"}
				}
				batches := collectBatches(t, out, total/64)
				seen := 0
				for _, b := range batches {
					seen += len(b.Events)
					if b.UserID != "hot" {
						t.Fatalf("unexpected user in surge batch: %+v", b)
					}
				}
				if seen != total {
					t.Fatalf("surge lost events: seen=%d want=%d", seen, total)
				}
				cancel()
				<-done
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}
