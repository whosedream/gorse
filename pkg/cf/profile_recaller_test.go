package cf

import (
	"context"
	"math"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"go-rec/pkg/cache"
)

// writeTestIntentToRedis writes a serialized intent vector into miniredis
// using the same format as cache.IntentKey + HMGET("version","vector").
func writeTestIntentToRedis(t *testing.T, rdb *redis.Client, sessionID string, vector []float32, version int64) {
	t.Helper()
	if len(vector) != cache.IntentVectorDim {
		t.Fatalf("writeTestIntentToRedis: vector length %d, want %d", len(vector), cache.IntentVectorDim)
	}
	var raw [cache.IntentVectorBytes]byte
	if err := cache.MarshalIntentVectorInto(&raw, vector); err != nil {
		t.Fatalf("MarshalIntentVectorInto: %v", err)
	}
	key := cache.IntentKey(sessionID)
	if err := rdb.HSet(context.Background(), key, "version", version, "vector", string(raw[:])).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
}

// testIntent fills a vector with a deterministic pattern for testing.
func testIntent(seed float32, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = seed + float32(i)*0.001
	}
	return v
}

func TestProfileRecaller_Recall_Basic(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	dim := cache.IntentVectorDim

	// Intent vector: all ones — will score higher on items with larger embeddings.
	intentVec := make([]float32, dim)
	for i := range intentVec {
		intentVec[i] = 1.0
	}
	writeTestIntentToRedis(t, rdb, "s1", intentVec, 42)

	// Catalog: 3 items with known embeddings.
	items := []CatalogItem{
		{ItemID: "item-a", Embedding: testIntent(0.1, dim)}, // lower dot-product
		{ItemID: "item-b", Embedding: testIntent(0.5, dim)}, // highest dot-product
		{ItemID: "item-c", Embedding: testIntent(0.3, dim)}, // middle
	}
	catalog := NewCatalog(items)

	recaller := NewProfileRecaller(intentReader, catalog, dim)
	candidates, err := recaller.Recall(context.Background(), "s1", 2)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}

	// item-b has the highest embedding values, should be first.
	if candidates[0].ItemID != "item-b" {
		t.Errorf("expected top candidate item-b, got %s", candidates[0].ItemID)
	}
	if candidates[0].Source != "profile" {
		t.Errorf("expected source 'profile', got '%s'", candidates[0].Source)
	}
	// Scores should be descending.
	if candidates[0].Score < candidates[1].Score {
		t.Errorf("scores not descending: %f < %f", candidates[0].Score, candidates[1].Score)
	}
}

func TestProfileRecaller_Recall_AllItems(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	dim := cache.IntentVectorDim
	intentVec := testIntent(2.0, dim)
	writeTestIntentToRedis(t, rdb, "s2", intentVec, 100)

	items := []CatalogItem{
		{ItemID: "x", Embedding: testIntent(1.0, dim)},
		{ItemID: "y", Embedding: testIntent(0.5, dim)},
		{ItemID: "z", Embedding: testIntent(0.0, dim)},
	}
	catalog := NewCatalog(items)

	recaller := NewProfileRecaller(intentReader, catalog, dim)

	// topK > catalog size — should return all items.
	candidates, err := recaller.Recall(context.Background(), "s2", 100)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(candidates))
	}
	for _, c := range candidates {
		if c.Source != "profile" {
			t.Errorf("expected source 'profile', got '%s'", c.Source)
		}
	}
}

func TestProfileRecaller_Recall_EmptyCatalog(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	intentVec := testIntent(1.0, cache.IntentVectorDim)
	writeTestIntentToRedis(t, rdb, "s3", intentVec, 1)

	catalog := NewCatalog(nil)
	recaller := NewProfileRecaller(intentReader, catalog, cache.IntentVectorDim)

	candidates, err := recaller.Recall(context.Background(), "s3", 10)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if candidates != nil {
		t.Errorf("expected nil for empty catalog, got %d candidates", len(candidates))
	}
}

func TestProfileRecaller_Recall_IntentNotFound(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	items := []CatalogItem{{ItemID: "i1", Embedding: testIntent(1.0, cache.IntentVectorDim)}}
	catalog := NewCatalog(items)
	recaller := NewProfileRecaller(intentReader, catalog, cache.IntentVectorDim)

	// Session does not exist in Redis — should return error.
	_, err = recaller.Recall(context.Background(), "nonexistent", 5)
	if err == nil {
		t.Error("expected error for missing intent, got nil")
	}
}

func TestProfileRecaller_Recall_ContextCancel(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	intentVec := testIntent(1.0, cache.IntentVectorDim)
	writeTestIntentToRedis(t, rdb, "s4", intentVec, 1)

	items := []CatalogItem{{ItemID: "i1", Embedding: testIntent(1.0, cache.IntentVectorDim)}}
	catalog := NewCatalog(items)
	recaller := NewProfileRecaller(intentReader, catalog, cache.IntentVectorDim)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = recaller.Recall(ctx, "s4", 5)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestProfileRecaller_Recall_NilRecaller(t *testing.T) {
	var recaller *ProfileRecaller
	_, err := recaller.Recall(context.Background(), "s", 5)
	if err == nil {
		t.Error("expected error for nil recaller")
	}
}

func TestProfileRecaller_Recall_DotProductCorrectness(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	dim := cache.IntentVectorDim

	// Intent: unit vector along dimension 0.
	intentVec := make([]float32, dim)
	intentVec[0] = 1.0
	writeTestIntentToRedis(t, rdb, "s5", intentVec, 1)

	// Item A: also unit vector along dim 0 — dot = 1.0.
	itemAEmb := make([]float32, dim)
	itemAEmb[0] = 1.0

	// Item B: unit vector along dim 1 — dot = 0.0.
	itemBEmb := make([]float32, dim)
	itemBEmb[1] = 1.0

	items := []CatalogItem{
		{ItemID: "b", Embedding: itemBEmb},
		{ItemID: "a", Embedding: itemAEmb},
	}
	catalog := NewCatalog(items)

	recaller := NewProfileRecaller(intentReader, catalog, dim)
	candidates, err := recaller.Recall(context.Background(), "s5", 2)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}

	// item-a should be first (dot = 1.0 > 0.0).
	if candidates[0].ItemID != "a" {
		t.Errorf("expected top candidate 'a', got '%s'", candidates[0].ItemID)
	}
	if math.Abs(float64(candidates[0].Score-1.0)) > 1e-6 {
		t.Errorf("expected score ~1.0 for item-a, got %f", candidates[0].Score)
	}
	if candidates[1].ItemID != "b" {
		t.Errorf("expected second candidate 'b', got '%s'", candidates[1].ItemID)
	}
	if math.Abs(float64(candidates[1].Score)) > 1e-6 {
		t.Errorf("expected score ~0.0 for item-b, got %f", candidates[1].Score)
	}
}

func TestProfileRecaller_Recall_TopKZero(t *testing.T) {
	t.Parallel()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		t.Fatalf("NewRedisIntentReader: %v", err)
	}

	intentVec := testIntent(1.0, cache.IntentVectorDim)
	writeTestIntentToRedis(t, rdb, "s6", intentVec, 1)

	items := []CatalogItem{{ItemID: "i1", Embedding: testIntent(1.0, cache.IntentVectorDim)}}
	catalog := NewCatalog(items)
	recaller := NewProfileRecaller(intentReader, catalog, cache.IntentVectorDim)

	candidates, err := recaller.Recall(context.Background(), "s6", 0)
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if candidates != nil {
		t.Errorf("expected nil for topK=0, got %v", candidates)
	}
}

// --- Benchmark ---

func BenchmarkProfileRecaller_Recall(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	b.Cleanup(func() { _ = rdb.Close() })

	intentReader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		b.Fatalf("NewRedisIntentReader: %v", err)
	}

	dim := cache.IntentVectorDim
	intentVec := testIntent(1.0, dim)
	var raw [cache.IntentVectorBytes]byte
	if err := cache.MarshalIntentVectorInto(&raw, intentVec); err != nil {
		b.Fatalf("MarshalIntentVectorInto: %v", err)
	}
	key := cache.IntentKey("bench-session")
	if err := rdb.HSet(context.Background(), key, "version", int64(1), "vector", string(raw[:])).Err(); err != nil {
		b.Fatalf("HSet: %v", err)
	}

	// Build a catalog with 1000 items.
	items := make([]CatalogItem, 1000)
	for i := range items {
		emb := make([]float32, dim)
		for j := range emb {
			emb[j] = float32(i*dim+j) / float32(dim*1000)
		}
		items[i] = CatalogItem{ItemID: "item-" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)), Embedding: emb}
	}
	catalog := NewCatalog(items)
	recaller := NewProfileRecaller(intentReader, catalog, dim)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = recaller.Recall(ctx, "bench-session", 50)
	}
}
