package scorer

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"go-rec/internal/rk/anti_drift"
	"go-rec/pkg/cache"
	"go-rec/pkg/fsm"
	"go-rec/pkg/pool"
)

func TestFastTrackPipelineGoldenPath(t *testing.T) {
	ctx := context.Background()
	parser := fsm.NewParser()
	var req fsm.RerankRequest
	input := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1000,"slots":{"category":"phone","brand":"brand-a"}}`)
	if err := parser.Parse(ctx, input, &req); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	mc := cache.NewMemoryClient(cache.Options{Shards: 4, IOTimeout: time.Millisecond})
	mc.Set(cache.Feature{ID: 1, Vector: []float32{1, 0}, Category: "phone", Brand: "brand-a", Available: true})
	mc.Set(cache.Feature{ID: 2, Vector: []float32{0, 2}, Category: "case", Brand: "brand-b", Available: true})
	features := make([]cache.Feature, 2)
	if err := mc.MGet(ctx, []int64{1, 2}, features); err != nil {
		t.Fatalf("MGet() error = %v", err)
	}

	coordinator, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 1, QueueCapacity: 4, Alpha: 0.5})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	defer coordinator.Shutdown(context.Background())
	session := req.SessionIDString()
	if err := coordinator.UpdateFast(ctx, anti_drift.FastTrackSnapshot{SessionID: session, LatestVersion: req.VersionStamp, IntentVector: []float32{2, 1}}); err != nil {
		t.Fatalf("UpdateFast() error = %v", err)
	}
	rec, ok := coordinator.Get(session)
	if !ok {
		t.Fatal("coordinator.Get() missing record")
	}

	candidates := make([]Candidate, 0, len(features))
	for _, f := range features {
		if f.Available {
			candidates = append(candidates, Candidate{ID: f.ID, Category: f.Category, Brand: f.Brand, Feature: f.Vector})
		}
	}
	engine := NewEngine(Options{TopK: 2, DiversityWindow: 2, MaxSameCategory: 1, MaxSameBrand: 1})
	out := make([]Result, 2)
	n, err := engine.Rank(ctx, rec.IntentVector, candidates, out)
	if err != nil {
		t.Fatalf("Rank() error = %v", err)
	}
	if n != 2 || out[0].ID != 1 {
		t.Fatalf("pipeline rank = n=%d out=%+v", n, out[:n])
	}
}

func TestFastTrackPipelineUsesAntiDriftFusionAndConcurrentRank(t *testing.T) {
	ctx := context.Background()
	coordinator, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 2, QueueCapacity: 8, Alpha: 0.25, DriftWindowMillis: 100})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	defer coordinator.Shutdown(context.Background())
	const session = "123e4567-e89b-12d3-a456-426614174000"
	if err := coordinator.UpdateFast(ctx, anti_drift.FastTrackSnapshot{SessionID: session, LatestVersion: 200, IntentVector: []float32{10, 0}}); err != nil {
		t.Fatalf("UpdateFast() error = %v", err)
	}
	rec, err := coordinator.ApplySlow(ctx, anti_drift.IntentFeatureUpdate{SessionID: session, BaselineVersion: 100, IntentVector: []float32{0, 4}, DriftThreshold: 0.01})
	if err != nil {
		t.Fatalf("ApplySlow() error = %v", err)
	}
	if !rec.Fused {
		t.Fatalf("record was not fused: %#v", rec)
	}

	engine := NewEngine(Options{TopK: 2, DiversityWindow: 2, MaxSameCategory: 1, MaxSameBrand: 1, MaxCandidates: 4})
	gp, err := pool.NewGoroutinePool(2, 2, 4)
	if err != nil {
		t.Fatalf("NewGoroutinePool() error = %v", err)
	}
	defer gp.Shutdown(context.Background())
	candidates := []Candidate{
		{ID: 1, Category: "A", Brand: "a", Feature: []float32{1, 0}},
		{ID: 2, Category: "A", Brand: "b", Feature: []float32{0, 2}},
		{ID: 3, Category: "B", Brand: "c", Feature: []float32{2, 1}},
		{ID: 4, Category: "C", Brand: "d", Feature: []float32{1, 1}},
	}
	out := make([]Result, 2)
	n, err := engine.RankParallel(ctx, gp, rec.IntentVector, candidates, out)
	if err != nil {
		t.Fatalf("RankParallel() error = %v", err)
	}
	if n != 2 || out[0].ID == 0 || out[1].ID == 0 {
		t.Fatalf("RankParallel result = n=%d out=%+v", n, out[:n])
	}
}

func TestFastTrackPipelineCachePartialTimeoutBoundary(t *testing.T) {
	mc := cache.NewMemoryClient(cache.Options{Shards: 2, IOTimeout: 2 * time.Millisecond})
	mc.Set(cache.Feature{ID: 1, Vector: []float32{1}, Category: "phone", Brand: "brand", Available: true})
	mc.SetShardDelayForTest(1, 50*time.Millisecond)
	out := make([]cache.Feature, 2)
	err := mc.MGet(context.Background(), []int64{1, 3}, out)
	if !errors.Is(err, cache.ErrPartialTimeout) {
		t.Fatalf("MGet() error = %v, want ErrPartialTimeout", err)
	}
}

func BenchmarkScorerRankHotPathNoAlloc(b *testing.B) {
	const candidatesN = 256
	const dims = 32
	const topK = 32

	engine := NewEngine(Options{TopK: topK, DiversityWindow: 8, MaxSameCategory: 2, MaxSameBrand: 2, MaxCandidates: candidatesN})
	intent := make([]float32, dims)
	candidates := make([]Candidate, candidatesN)
	for i := 0; i < dims; i++ {
		intent[i] = float32((i % 7) + 1)
	}
	for i := range candidates {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32((i+j)%11) / 10
		}
		candidates[i] = Candidate{ID: int64(i + 1), Category: benchCategory(i), Brand: benchBrand(i), Feature: vec}
	}
	out := make([]Result, topK)
	ctx := context.Background()
	if _, err := engine.Rank(ctx, intent, candidates, out); err != nil {
		b.Fatalf("warm Rank() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.Rank(ctx, intent, candidates, out); err != nil {
			b.Fatalf("Rank() error = %v", err)
		}
	}
}

func BenchmarkFastTrackPipelineP99(b *testing.B) {
	const candidatesN = 512
	const dims = 32
	const topK = 32

	ctx := context.Background()
	parser := fsm.NewParser()
	input := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1000,"slots":{"category":"phone","brand":"brand-a"}}`)
	mc := cache.NewMemoryClient(cache.Options{Shards: 8, IOTimeout: 20 * time.Millisecond})
	ids := make([]int64, candidatesN)
	for i := 0; i < candidatesN; i++ {
		ids[i] = int64(i + 1)
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = float32((i+j)%13) / 10
		}
		mc.Set(cache.Feature{ID: ids[i], Vector: vec, Category: benchCategory(i), Brand: benchBrand(i), Available: true})
	}
	features := make([]cache.Feature, candidatesN)
	vectorBuf := make([]float32, candidatesN*dims)
	candidates := make([]Candidate, candidatesN)
	engine := NewEngine(Options{TopK: topK, DiversityWindow: 8, MaxSameCategory: 2, MaxSameBrand: 2, MaxCandidates: candidatesN})
	out := make([]Result, topK)
	gp, err := pool.NewGoroutinePool(2, 2, 8)
	if err != nil {
		b.Fatalf("NewGoroutinePool() error = %v", err)
	}
	defer gp.Shutdown(context.Background())
	coordinator, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 2, QueueCapacity: 8, Alpha: 0.5, DriftWindowMillis: 100})
	if err != nil {
		b.Fatalf("NewCoordinator() error = %v", err)
	}
	defer coordinator.Shutdown(context.Background())
	intent := make([]float32, dims)
	for i := range intent {
		intent[i] = float32((i % 7) + 1)
	}
	if err := coordinator.UpdateFast(ctx, anti_drift.FastTrackSnapshot{SessionID: "123e4567-e89b-12d3-a456-426614174000", LatestVersion: 1000, IntentVector: intent}); err != nil {
		b.Fatalf("UpdateFast() error = %v", err)
	}
	slow := make([]float32, dims)
	for i := range slow {
		slow[i] = float32((i % 5) + 1)
	}
	fused, err := coordinator.ApplySlow(ctx, anti_drift.IntentFeatureUpdate{SessionID: "123e4567-e89b-12d3-a456-426614174000", BaselineVersion: 900, IntentVector: slow, DriftThreshold: 0.01})
	if err != nil {
		b.Fatalf("ApplySlow() error = %v", err)
	}
	if !fused.Fused {
		b.Fatalf("benchmark record was not fused: %#v", fused)
	}
	elapsed := make([]int64, b.N)
	var req fsm.RerankRequest

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := parser.Parse(ctx, input, &req); err != nil {
			b.Fatalf("Parse() error = %v", err)
		}
		if err := mc.MGetInto(ctx, ids, features, vectorBuf, dims); err != nil {
			b.Fatalf("MGetInto() error = %v", err)
		}
		for j := range features {
			candidates[j] = Candidate{ID: features[j].ID, Category: features[j].Category, Brand: features[j].Brand, Feature: features[j].Vector}
		}
		rec, ok := coordinator.Get(req.SessionIDString())
		if !ok {
			b.Fatal("coordinator.Get() missing record")
		}
		if !rec.Fused {
			b.Fatal("pipeline lost fused anti-drift record")
		}
		if _, err := engine.RankParallel(ctx, gp, rec.IntentVector, candidates, out); err != nil {
			b.Fatalf("RankParallel() error = %v", err)
		}
		elapsed[i] = time.Since(start).Nanoseconds()
	}
	b.StopTimer()

	sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
	idx := (len(elapsed) * 99) / 100
	if idx >= len(elapsed) {
		idx = len(elapsed) - 1
	}
	p99ms := float64(elapsed[idx]) / float64(time.Millisecond)
	b.ReportMetric(p99ms, "p99_ms")
	if p99ms >= 20 {
		b.Fatalf("p99_ms = %.3f, want < 20", p99ms)
	}
}

func benchCategory(i int) string {
	switch i & 3 {
	case 0:
		return "phone"
	case 1:
		return "case"
	case 2:
		return "watch"
	default:
		return "audio"
	}
}

func benchBrand(i int) string {
	switch i & 7 {
	case 0:
		return "brand-a"
	case 1:
		return "brand-b"
	case 2:
		return "brand-c"
	case 3:
		return "brand-d"
	case 4:
		return "brand-e"
	case 5:
		return "brand-f"
	case 6:
		return "brand-g"
	default:
		return "brand-h"
	}
}
