package cache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryClientMGet(t *testing.T) {
	t.Parallel()

	t.Run("keeps order for batch hits", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 4, IOTimeout: 50 * time.Millisecond})
		c.Set(Feature{ID: 3, Vector: []float32{3, 30}, Category: "cat-c", Brand: "brand-c", Available: true})
		c.Set(Feature{ID: 1, Vector: []float32{1, 10}, Category: "cat-a", Brand: "brand-a", Available: true})
		c.Set(Feature{ID: 2, Vector: []float32{2, 20}, Category: "cat-b", Brand: "brand-b", Available: true})

		ids := []int64{1, 2, 3}
		out := make([]Feature, len(ids))
		err := c.MGet(context.Background(), ids, out)
		if err != nil {
			t.Fatalf("MGet() error = %v", err)
		}
		for i, id := range ids {
			if out[i].ID != id || !out[i].Available {
				t.Fatalf("out[%d] = %+v, want id=%d available", i, out[i], id)
			}
		}
		if got := out[1].Vector[1]; got != 20 {
			t.Fatalf("ordered vector = %v, want 20", got)
		}
	})

	t.Run("miss marks unavailable and never panics", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 2, IOTimeout: 50 * time.Millisecond})
		c.Set(Feature{ID: 1, Vector: []float32{1}, Category: "cat", Brand: "brand", Available: true})
		out := make([]Feature, 3)
		err := c.MGet(context.Background(), []int64{1, 404, 405}, out)
		if err != nil {
			t.Fatalf("partial miss error = %v, want nil", err)
		}
		if !out[0].Available || out[1].Available || out[2].Available {
			t.Fatalf("availability = [%v,%v,%v], want [true,false,false]", out[0].Available, out[1].Available, out[2].Available)
		}
		if out[1].ID != 404 || len(out[1].Vector) != 0 || out[1].Category != "" || out[1].Brand != "" {
			t.Fatalf("miss out = %+v, want id only unavailable zero payload", out[1])
		}

		allMiss := make([]Feature, 1)
		err = c.MGet(context.Background(), []int64{999}, allMiss)
		if !errors.Is(err, ErrMiss) {
			t.Fatalf("all miss error = %v, want ErrMiss", err)
		}
	})

	t.Run("partial shard timeout fast fails", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 2, IOTimeout: 3 * time.Millisecond})
		c.Set(Feature{ID: 1, Vector: []float32{1}, Category: "cat", Brand: "brand", Available: true})
		c.SetShardDelayForTest(1, 50*time.Millisecond)
		out := make([]Feature, 2)
		start := time.Now()
		err := c.MGet(context.Background(), []int64{1, 3}, out)
		elapsed := time.Since(start)
		if !errors.Is(err, ErrPartialTimeout) {
			t.Fatalf("MGet() error = %v, want ErrPartialTimeout", err)
		}
		if elapsed > 40*time.Millisecond {
			t.Fatalf("timeout elapsed = %s, want fast fail", elapsed)
		}
	})

	t.Run("pre canceled context fails before work", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 2, IOTimeout: 50 * time.Millisecond})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := make([]Feature, 1)
		err := c.MGet(ctx, []int64{1}, out)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("MGet() error = %v, want context.Canceled", err)
		}
	})

	t.Run("set and get deep copy vectors", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 2, IOTimeout: 50 * time.Millisecond})
		inVec := []float32{1, 2, 3}
		c.Set(Feature{ID: 7, Vector: inVec, Category: "cat", Brand: "brand", Available: true})
		inVec[0] = 99

		out := make([]Feature, 1)
		if err := c.MGet(context.Background(), []int64{7}, out); err != nil {
			t.Fatalf("MGet() error = %v", err)
		}
		if out[0].Vector[0] != 1 {
			t.Fatalf("cache polluted by input mutation: got %v", out[0].Vector[0])
		}
		out[0].Vector[0] = 88

		again := make([]Feature, 1)
		if err := c.MGet(context.Background(), []int64{7}, again); err != nil {
			t.Fatalf("MGet() second error = %v", err)
		}
		if again[0].Vector[0] != 1 {
			t.Fatalf("cache polluted by output mutation: got %v", again[0].Vector[0])
		}
	})

	t.Run("invalid output length returns ErrInvalidKey", func(t *testing.T) {
		t.Parallel()
		c := NewMemoryClient(Options{Shards: 1, IOTimeout: time.Millisecond})
		err := c.MGet(context.Background(), []int64{1, 2}, make([]Feature, 1))
		if !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("MGet() error = %v, want ErrInvalidKey", err)
		}
	})
}

func TestMemoryClientMGetWaitsForShardGoroutinesBeforeReturn(t *testing.T) {
	c := NewMemoryClient(Options{Shards: 2, IOTimeout: time.Second})
	c.Set(Feature{ID: 1, Vector: []float32{1}, Category: "cat", Brand: "brand", Available: true})
	c.Set(Feature{ID: 3, Vector: []float32{3}, Category: "cat", Brand: "brand", Available: true})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c.SetShardWriteBlockForTest(1, started, release)

	ctx, cancel := context.WithCancel(context.Background())
	out := make([]Feature, 2)
	done := make(chan error, 1)
	go func() { done <- c.MGet(ctx, []int64{1, 3}, out) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("blocked shard did not start writing")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("MGet returned before shard goroutine was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrPartialTimeout) {
			t.Fatalf("MGet() error = %v, want ErrPartialTimeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MGet did not return after shard release")
	}
}

func TestMemoryClientMGetIntoUsesCallerVectorStorage(t *testing.T) {
	c := NewMemoryClient(Options{Shards: 4, IOTimeout: 50 * time.Millisecond})
	c.Set(Feature{ID: 1, Vector: []float32{1, 2}, Category: "cat-a", Brand: "brand-a", Available: true})
	c.Set(Feature{ID: 2, Vector: []float32{3, 4}, Category: "cat-b", Brand: "brand-b", Available: true})

	ids := []int64{1, 2}
	out := make([]Feature, len(ids))
	vectorBuf := make([]float32, len(ids)*2)
	if err := c.MGetInto(context.Background(), ids, out, vectorBuf, 2); err != nil {
		t.Fatalf("MGetInto() error = %v", err)
	}
	if &out[0].Vector[0] != &vectorBuf[0] || &out[1].Vector[0] != &vectorBuf[2] {
		t.Fatalf("MGetInto did not use caller vector buffer")
	}
	out[0].Vector[0] = 99
	again := make([]Feature, 1)
	if err := c.MGet(context.Background(), []int64{1}, again); err != nil {
		t.Fatalf("MGet() error = %v", err)
	}
	if again[0].Vector[0] != 1 {
		t.Fatalf("MGetInto caller buffer mutation polluted cache: %v", again[0].Vector[0])
	}
}

func TestMemoryClientMGetIntoHotPathNoAlloc(t *testing.T) {
	c := NewMemoryClient(Options{Shards: 1, IOTimeout: time.Millisecond})
	for i := int64(1); i <= 8; i++ {
		c.Set(Feature{ID: i, Vector: []float32{float32(i), float32(i + 1)}, Category: "cat", Brand: "brand", Available: true})
	}
	ids := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	out := make([]Feature, len(ids))
	vectorBuf := make([]float32, len(ids)*2)
	ctx := context.Background()
	if err := c.MGetInto(ctx, ids, out, vectorBuf, 2); err != nil {
		t.Fatalf("warm MGetInto() error = %v", err)
	}
	var gotErr error
	allocs := testing.AllocsPerRun(1000, func() {
		gotErr = c.MGetInto(ctx, ids, out, vectorBuf, 2)
	})
	if gotErr != nil {
		t.Fatalf("MGetInto() error = %v", gotErr)
	}
	if allocs != 0 {
		t.Fatalf("MGetInto hot path allocs = %v, want 0", allocs)
	}
}

func TestMemoryClientMGetHighConcurrencySurge(t *testing.T) {
	c := NewMemoryClient(Options{Shards: 8, IOTimeout: 100 * time.Millisecond})
	for i := int64(1); i <= 64; i++ {
		c.Set(Feature{ID: i, Vector: []float32{float32(i)}, Category: "cat", Brand: "brand", Available: true})
	}

	errCh := make(chan error, 128)
	for g := 0; g < cap(errCh); g++ {
		go func() {
			out := make([]Feature, 4)
			errCh <- c.MGet(context.Background(), []int64{1, 17, 33, 64}, out)
		}()
	}
	for i := 0; i < cap(errCh); i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent MGet error = %v", err)
		}
	}
}
