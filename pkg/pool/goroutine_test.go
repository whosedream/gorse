package pool

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoroutinePoolExecutionBackpressureTimeoutAndScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "nil task returns ErrInvalidTask",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 2)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				err = gp.Submit(context.Background(), nil)
				if !errors.Is(err, ErrInvalidTask) {
					t.Fatalf("expected ErrInvalidTask, got %v", err)
				}
			},
		},
		{
			name: "submit after shutdown returns ErrPoolClosed",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 2)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				if err := gp.Shutdown(context.Background()); err != nil {
					t.Fatalf("Shutdown returned error: %v", err)
				}

				err = gp.Submit(context.Background(), func(context.Context) error { return nil })
				if !errors.Is(err, ErrPoolClosed) {
					t.Fatalf("expected ErrPoolClosed, got %v", err)
				}
			},
		},
		{
			name: "executes submitted tasks normally",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 4, 8)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				done := make(chan struct{})
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					defer close(done)
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						return nil
					}
				}); err != nil {
					t.Fatalf("Submit returned error: %v", err)
				}

				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("task did not execute")
				}
			},
		},
		{
			name: "full queue returns ErrOverloaded without blocking",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 1, 1)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				block := make(chan struct{})
				started := make(chan struct{})
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					close(started)
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}); err != nil {
					t.Fatalf("blocking Submit returned error: %v", err)
				}
				select {
				case <-started:
				case <-time.After(time.Second):
					close(block)
					t.Fatal("blocking task did not start")
				}
				if err := gp.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
					t.Fatalf("queued Submit returned error: %v", err)
				}

				start := time.Now()
				err = gp.Submit(context.Background(), func(context.Context) error { return nil })
				elapsed := time.Since(start)
				close(block)

				if !errors.Is(err, ErrOverloaded) {
					t.Fatalf("expected ErrOverloaded, got %v", err)
				}
				if elapsed > 25*time.Millisecond {
					t.Fatalf("overloaded submit blocked for %s", elapsed)
				}
			},
		},
		{
			name: "shutdown drains queued tasks before workers exit",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 1, 8)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}

				block := make(chan struct{})
				started := make(chan struct{})
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					close(started)
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}); err != nil {
					t.Fatalf("blocking Submit returned error: %v", err)
				}
				select {
				case <-started:
				case <-time.After(time.Second):
					close(block)
					t.Fatal("blocking task did not start")
				}

				var completed atomic.Int32
				for i := 0; i < 4; i++ {
					if err := gp.Submit(context.Background(), func(context.Context) error {
						completed.Add(1)
						return nil
					}); err != nil {
						close(block)
						t.Fatalf("queued Submit %d returned error: %v", i, err)
					}
				}

				shutdownDone := make(chan error, 1)
				go func() {
					shutdownDone <- gp.Shutdown(context.Background())
				}()
				close(block)

				select {
				case err := <-shutdownDone:
					if err != nil {
						t.Fatalf("Shutdown returned error: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Shutdown did not finish")
				}
				if got := completed.Load(); got != 4 {
					t.Fatalf("Shutdown dropped queued tasks: completed=%d", got)
				}
			},
		},
		{
			name: "concurrent shutdown and submit never loses accepted tasks",
			run: func(t *testing.T) {
				for round := 0; round < 200; round++ {
					gp, err := NewGoroutinePool(1, 8, 256)
					if err != nil {
						t.Fatalf("NewGoroutinePool returned error: %v", err)
					}

					var accepted atomic.Int32
					var executed atomic.Int32
					start := make(chan struct{})
					var submitters sync.WaitGroup
					const submitterCount = 32
					submitters.Add(submitterCount)
					for i := 0; i < submitterCount; i++ {
						go func() {
							defer submitters.Done()
							<-start
							err := gp.Submit(context.Background(), func(context.Context) error {
								executed.Add(1)
								return nil
							})
							if err == nil {
								accepted.Add(1)
								return
							}
							if !errors.Is(err, ErrPoolClosed) && !errors.Is(err, ErrOverloaded) {
								t.Errorf("unexpected Submit error: %v", err)
							}
						}()
					}

					shutdownDone := make(chan error, 1)
					close(start)
					go func() { shutdownDone <- gp.Shutdown(context.Background()) }()
					submitters.Wait()
					select {
					case err := <-shutdownDone:
						if err != nil {
							t.Fatalf("Shutdown returned error: %v", err)
						}
					case <-time.After(time.Second):
						t.Fatal("Shutdown did not finish")
					}
					if got, want := executed.Load(), accepted.Load(); got != want {
						t.Fatalf("round %d accepted tasks were lost: accepted=%d executed=%d", round, want, got)
					}
				}
			},
		},
		{
			name: "fixed pool keeps constant worker count",
			run: func(t *testing.T) {
				gp, err := NewFixedPool(4, 16)
				if err != nil {
					t.Fatalf("NewFixedPool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				// Workers are pre-spawned, count must be 4 from the start.
				if got := gp.WorkerCount(); got != 4 {
					t.Fatalf("worker count = %d, want 4", got)
				}

				block := make(chan struct{})
				for i := 0; i < 12; i++ {
					if err := gp.Submit(context.Background(), func(ctx context.Context) error {
						select {
						case <-block:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}); err != nil {
						close(block)
						t.Fatalf("Submit returned error: %v", err)
					}
				}
				close(block)
				// Wait for tasks to drain.
				time.Sleep(10 * time.Millisecond)
				// Fixed pool never shrinks.
				if got := gp.WorkerCount(); got != 4 {
					t.Fatalf("worker count = %d, want 4 (fixed pool)", got)
				}
			},
		},
		{
			name: "shutdown wait signal is allocated once",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 2)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				block := make(chan struct{})
				started := make(chan struct{})
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					close(started)
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}); err != nil {
					t.Fatalf("Submit returned error: %v", err)
				}
				<-started

				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				_ = gp.Shutdown(ctx)
				done1 := reflect.ValueOf(gp.shutdownDone).Pointer()
				_ = gp.Shutdown(ctx)
				done2 := reflect.ValueOf(gp.shutdownDone).Pointer()
				close(block)
				if err := gp.Shutdown(context.Background()); err != nil {
					t.Fatalf("final Shutdown returned error: %v", err)
				}
				if done1 == 0 || done1 != done2 {
					t.Fatalf("shutdownDone channel changed across calls: %x -> %x", done1, done2)
				}
			},
		},
		{
			name: "canceled context is rejected before enqueue",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 2)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				called := atomic.Bool{}
				err = gp.Submit(ctx, func(context.Context) error {
					called.Store(true)
					return nil
				})
				if err == nil {
					t.Fatal("expected canceled context error")
				}
				if called.Load() {
					t.Fatal("task ran with canceled submit context")
				}
			},
		},
		{
			name: "concurrent submit surge never exceeds max workers",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 256)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				block := make(chan struct{})
				start := make(chan struct{})
				var submitters sync.WaitGroup
				for i := 0; i < 128; i++ {
					submitters.Add(1)
					go func() {
						defer submitters.Done()
						<-start
						_ = gp.Submit(context.Background(), func(ctx context.Context) error {
							select {
							case <-block:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						})
					}()
				}
				close(start)
				submitters.Wait()
				if got := gp.WorkerCount(); got > 2 {
					close(block)
					t.Fatalf("WorkerCount = %d, want <= 2", got)
				}
				close(block)
			},
		},
		{
			name: "fixed pool rejects when queue is full",
			run: func(t *testing.T) {
				gp, err := NewFixedPool(1, 1)
				if err != nil {
					t.Fatalf("NewFixedPool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				block := make(chan struct{})
				started := make(chan struct{})
				// 1 worker picks up task immediately, 1 goes in queue.
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					close(started)
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}); err != nil {
					t.Fatalf("Submit 0 returned error: %v", err)
				}
				<-started
				// Queue has capacity 1, this fills it.
				if err := gp.Submit(context.Background(), func(ctx context.Context) error {
					select {
					case <-block:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}); err != nil {
					t.Fatalf("Submit 1 returned error: %v", err)
				}
				// Queue is full, next submit must be rejected.
				err = gp.Submit(context.Background(), func(ctx context.Context) error { return nil })
				if err != ErrOverloaded {
					t.Fatalf("expected ErrOverloaded, got %v", err)
				}
				close(block)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestParallelExtractFastFailsAndCancelsOtherBranches(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "all branches succeed",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(2, 4, 8)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				var ran atomic.Int32
				err = ParallelExtract(context.Background(), gp, 100*time.Millisecond,
					func(ctx context.Context) error { ran.Add(1); return nil },
					func(ctx context.Context) error { ran.Add(1); return nil },
				)
				if err != nil {
					t.Fatalf("ParallelExtract returned error: %v", err)
				}
				if got := ran.Load(); got != 2 {
					t.Fatalf("expected 2 branches, got %d", got)
				}
			},
		},
		{
			name: "one branch error cancels sibling",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(2, 4, 8)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				cancelSeen := make(chan struct{})
				err = ParallelExtract(context.Background(), gp, time.Second,
					func(ctx context.Context) error { return boom },
					func(ctx context.Context) error {
						select {
						case <-ctx.Done():
							close(cancelSeen)
							return ctx.Err()
						case <-time.After(time.Second):
							return nil
						}
					},
				)
				if !errors.Is(err, boom) {
					t.Fatalf("expected boom, got %v", err)
				}
				select {
				case <-cancelSeen:
				case <-time.After(time.Second):
					t.Fatal("sibling branch did not observe cancellation")
				}
			},
		},
		{
			name: "timeout cancels slow branch",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(1, 2, 4)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				start := time.Now()
				err = ParallelExtract(context.Background(), gp, 10*time.Millisecond,
					func(ctx context.Context) error {
						<-ctx.Done()
						return ctx.Err()
					},
				)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected DeadlineExceeded, got %v", err)
				}
				if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
					t.Fatalf("timeout was not fast: %s", elapsed)
				}
			},
		},
		{
			name: "fast branch error wins over later submit cancellation",
			run: func(t *testing.T) {
				oldProcs := runtime.GOMAXPROCS(2)
				defer runtime.GOMAXPROCS(oldProcs)

				for attempt := 0; attempt < 200; attempt++ {
					gp, err := NewGoroutinePool(1, 1, 128)
					if err != nil {
						t.Fatalf("NewGoroutinePool returned error: %v", err)
					}

					branches := make([]Extractor, 64)
					branches[0] = func(context.Context) error { return boom }
					for i := 1; i < len(branches); i++ {
						branches[i] = func(ctx context.Context) error {
							<-ctx.Done()
							return ctx.Err()
						}
					}

					err = ParallelExtract(context.Background(), gp, time.Second, branches...)
					_ = gp.Shutdown(context.Background())
					if errors.Is(err, context.Canceled) {
						t.Fatalf("attempt %d returned cancellation instead of original branch error", attempt)
					}
					if !errors.Is(err, boom) {
						t.Fatalf("attempt %d expected boom, got %v", attempt, err)
					}
				}
			},
		},
		{
			name: "returns only after canceled sibling exits",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(2, 2, 4)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				started := make(chan struct{})
				exited := atomic.Bool{}
				err = ParallelExtract(context.Background(), gp, time.Second,
					func(context.Context) error { return boom },
					func(ctx context.Context) error {
						close(started)
						<-ctx.Done()
						exited.Store(true)
						return ctx.Err()
					},
				)
				if !errors.Is(err, boom) {
					t.Fatalf("expected boom, got %v", err)
				}
				select {
				case <-started:
				default:
					t.Fatal("sibling did not start")
				}
				if !exited.Load() {
					t.Fatal("ParallelExtract returned before canceled sibling exited")
				}
			},
		},
		{
			name: "concurrent submit surge completes without lost tasks",
			run: func(t *testing.T) {
				gp, err := NewGoroutinePool(2, 8, 256)
				if err != nil {
					t.Fatalf("NewGoroutinePool returned error: %v", err)
				}
				defer gp.Shutdown(context.Background())

				const total = 128
				var done atomic.Int32
				var wg sync.WaitGroup
				wg.Add(total)
				for i := 0; i < total; i++ {
					if err := gp.Submit(context.Background(), func(ctx context.Context) error {
						defer wg.Done()
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							done.Add(1)
							return nil
						}
					}); err != nil {
						wg.Done()
						t.Fatalf("Submit returned error: %v", err)
					}
				}

				wait := make(chan struct{})
				go func() {
					wg.Wait()
					close(wait)
				}()
				select {
				case <-wait:
				case <-time.After(time.Second):
					t.Fatal("surge tasks did not complete")
				}
				if got := done.Load(); got != total {
					t.Fatalf("expected %d completed tasks, got %d", total, got)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}
