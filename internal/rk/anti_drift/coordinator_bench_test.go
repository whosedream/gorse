package anti_drift

import (
	"context"
	"testing"
	"time"
)

func newBenchmarkCoordinator(b *testing.B) *Coordinator {
	b.Helper()
	c, err := NewCoordinator(Options{
		MinWorkers:        1,
		MaxWorkers:        1,
		QueueCapacity:     4096,
		Alpha:             0.25,
		SlowTimeout:       time.Millisecond,
		DriftWindowMillis: 200,
		SlowTrack: slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
			return update, nil
		}),
		Ranker: rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
			return FeatureRecord{SessionID: sessionID, Version: 1, IntentVector: []float32{1, 2, 3}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
		}),
	})
	if err != nil {
		b.Fatalf("NewCoordinator returned error: %v", err)
	}
	return c
}

func BenchmarkApplySlowFusion(b *testing.B) {
	c := newBenchmarkCoordinator(b)
	defer c.Shutdown(context.Background())
	fastVec := make([]float32, 1024)
	slowVec := make([]float32, 1024)
	for i := range fastVec {
		fastVec[i] = float32(i)
		slowVec[i] = float32(1024 - i)
	}
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "bench", LatestVersion: 200, IntentVector: fastVec, CategoryWeights: map[string]float32{"fast": 1}}); err != nil {
		b.Fatalf("UpdateFast returned error: %v", err)
	}
	update := IntentFeatureUpdate{SessionID: "bench", BaselineVersion: 100, IntentVector: slowVec, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.ApplySlow(context.Background(), update); err != nil {
			b.Fatalf("ApplySlow returned error: %v", err)
		}
	}
}

func BenchmarkUpdateFast(b *testing.B) {
	c := newBenchmarkCoordinator(b)
	defer c.Shutdown(context.Background())
	snapshot := FastTrackSnapshot{SessionID: "bench", LatestVersion: 1, IntentVector: []float32{1, 2, 3}, CategoryWeights: map[string]float32{"fast": 1}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot.LatestVersion = int64(i + 1)
		if err := c.UpdateFast(context.Background(), snapshot); err != nil {
			b.Fatalf("UpdateFast returned error: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	c := newBenchmarkCoordinator(b)
	defer c.Shutdown(context.Background())
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "bench", LatestVersion: 1, IntentVector: []float32{1, 2, 3}, CategoryWeights: map[string]float32{"fast": 1}}); err != nil {
		b.Fatalf("UpdateFast returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.Get("bench"); !ok {
			b.Fatal("Get missed record")
		}
	}
}

func BenchmarkDecideSlowTrackRoundTrip(b *testing.B) {
	c := newBenchmarkCoordinator(b)
	defer c.Shutdown(context.Background())
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "bench", LatestVersion: 100, IntentVector: []float32{1, 2, 3}, CategoryWeights: map[string]float32{"fast": 1}}); err != nil {
		b.Fatalf("UpdateFast returned error: %v", err)
	}
	update := IntentFeatureUpdate{SessionID: "bench", BaselineVersion: 100, IntentVector: []float32{3, 2, 1}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Decide(context.Background(), update); err != nil {
			b.Fatalf("Decide returned error: %v", err)
		}
	}
}

func BenchmarkDecideFallbackOverload(b *testing.B) {
	block := make(chan struct{})
	c, err := NewCoordinator(Options{
		MinWorkers:        1,
		MaxWorkers:        1,
		QueueCapacity:     1,
		Alpha:             0.25,
		SlowTimeout:       time.Second,
		DriftWindowMillis: 200,
		SlowTrack: slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
			select {
			case <-block:
				return update, nil
			case <-ctx.Done():
				return IntentFeatureUpdate{}, ctx.Err()
			}
		}),
		Ranker: rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
			return FeatureRecord{SessionID: sessionID, Version: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
		}),
	})
	if err != nil {
		b.Fatalf("NewCoordinator returned error: %v", err)
	}
	defer func() {
		close(block)
		_ = c.Shutdown(context.Background())
	}()
	started := make(chan struct{})
	if err := c.pool.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}); err != nil {
		b.Fatalf("blocking Submit returned error: %v", err)
	}
	<-started
	if err := c.pool.Submit(context.Background(), func(ctx context.Context) error { <-block; return nil }); err != nil {
		b.Fatalf("queued Submit returned error: %v", err)
	}
	update := IntentFeatureUpdate{SessionID: "overload", BaselineVersion: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"x": 1}, DriftThreshold: 0.05}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Decide(context.Background(), update); err != nil {
			b.Fatalf("Decide returned error: %v", err)
		}
	}
}
