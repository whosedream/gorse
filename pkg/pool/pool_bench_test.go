package pool

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type benchState struct {
	A uint64
	B uint64
	C [8]byte
}

func resetBenchState(s *benchState) {
	*s = benchState{}
}

func withBenchState(s *benchState) error {
	s.A = 1
	s.B = 2
	s.C[0] = 3
	return nil
}

func withBenchBytes(buf []byte) error {
	buf = append(buf, 1, 2, 3, 4)
	_ = buf[0]
	return nil
}

func BenchmarkMemoryPoolHotReuse(b *testing.B) {
	mp := NewMemoryPool(resetBenchState)
	if err := mp.With(context.Background(), withBenchState); err != nil {
		b.Fatalf("warm With returned error: %v", err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mp.With(ctx, withBenchState); err != nil {
			b.Fatalf("With returned error: %v", err)
		}
	}
}

func BenchmarkByteBufferPoolHotReuse(b *testing.B) {
	bp := NewByteBufferPool(64, 1024)
	if err := bp.With(context.Background(), 64, withBenchBytes); err != nil {
		b.Fatalf("warm With returned error: %v", err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bp.With(ctx, 64, withBenchBytes); err != nil {
			b.Fatalf("With returned error: %v", err)
		}
	}
}

func BenchmarkGoroutinePoolSubmitBackpressure(b *testing.B) {
	gp, err := NewGoroutinePool(1, 1, 1)
	if err != nil {
		b.Fatalf("NewGoroutinePool returned error: %v", err)
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
		b.Fatalf("blocking Submit returned error: %v", err)
	}
	<-started
	if err := gp.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
		close(block)
		b.Fatalf("queued Submit returned error: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := gp.Submit(ctx, func(context.Context) error { return nil }); err != ErrOverloaded {
			b.Fatalf("expected ErrOverloaded, got %v", err)
		}
	}
	b.StopTimer()
	close(block)
}

func BenchmarkGoroutinePoolStressP99(b *testing.B) {
	const (
		workers     = 64
		queueCap    = 32768
		concurrency = 128
	)
	gp, err := NewGoroutinePool(workers, workers, queueCap)
	if err != nil {
		b.Fatalf("NewGoroutinePool returned error: %v", err)
	}
	defer gp.Shutdown(context.Background())

	latencies := make([]int64, b.N)
	var next atomic.Int64
	var done sync.WaitGroup
	inflight := make(chan struct{}, concurrency)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetParallelism(concurrency)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			inflight <- struct{}{}
			idx := int(next.Add(1)) - 1
			submittedAt := time.Now()
			done.Add(1)
			err := gp.Submit(ctx, func(context.Context) error {
				latencies[idx] = time.Since(submittedAt).Nanoseconds()
				<-inflight
				done.Done()
				return nil
			})
			if err != nil {
				<-inflight
				done.Done()
				b.Fatalf("Submit returned error: %v", err)
			}
		}
	})
	done.Wait()
	b.StopTimer()

	count := int(next.Load())
	if count == 0 {
		b.Fatal("no samples collected")
	}
	samples := latencies[:count]
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 := percentile(samples, 50)
	p95 := percentile(samples, 95)
	p99 := percentile(samples, 99)
	b.ReportMetric(float64(p50)/1e6, "p50_ms")
	b.ReportMetric(float64(p95)/1e6, "p95_ms")
	b.ReportMetric(float64(p99)/1e6, "p99_ms")
	if p99 > int64(5*time.Millisecond) {
		b.Fatalf("pool stress p99 exceeded 5ms: %.3fms", float64(p99)/1e6)
	}
}

func percentile(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*pct + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}
