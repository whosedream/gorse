//go:build cgo

package storage

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
)

// skipIfDuckDBUnlinkable guards against MinGW toolchain / GCC TLS ABI
// incompatibility with the prebuilt libduckdb.a shipped by go-duckdb.
func skipIfDuckDBUnlinkable(t *testing.T) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("duckdb driver unavailable: %v", err)
	}
	_ = db.Close()
}

func TestDuckDBDriverAvailable(t *testing.T) {
	skipIfDuckDBUnlinkable(t)
}

func TestSearchWithIntent(t *testing.T) {
	skipIfDuckDBUnlinkable(t)

	client, err := NewDuckDBClient("")
	if err != nil {
		t.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()

	if err := client.initBenchmarkData(); err != nil {
		t.Fatalf("initBenchmarkData error = %v", err)
	}

	ctx := context.Background()

	t.Run("category filter returns correct count", func(t *testing.T) {
		vec := make([]float32, 1024)
		for i := range vec {
			vec[i] = float32(i%256) / 256.0
		}
		prods, err := client.SearchWithIntent(ctx, vec, "宠物生活", 5)
		if err != nil {
			t.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) > 5 {
			t.Fatalf("limit exceeded: got %d", len(prods))
		}
		for _, p := range prods {
			if p.Category != "宠物生活" {
				t.Fatalf("wrong category: %s", p.Category)
			}
			if p.ItemID == "" || p.Title == "" {
				t.Fatalf("empty fields in product: %+v", p)
			}
		}
	})

	t.Run("full table search", func(t *testing.T) {
		vec := make([]float32, 1024)
		prods, err := client.SearchWithIntent(ctx, vec, "", 20)
		if err != nil {
			t.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) == 0 {
			t.Fatal("expected at least one product")
		}
	})

	t.Run("limit clips results", func(t *testing.T) {
		vec := make([]float32, 1024)
		prods, err := client.SearchWithIntent(ctx, vec, "", 3)
		if err != nil {
			t.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) != 3 {
			t.Fatalf("limit not respected: got %d, want 3", len(prods))
		}
	})

	t.Run("empty vector returns error", func(t *testing.T) {
		_, err := client.SearchWithIntent(ctx, nil, "", 5)
		if err == nil {
			t.Fatal("expected error for empty vector")
		}
	})
}

func TestSearchWithIntentConcurrentReads(t *testing.T) {
	skipIfDuckDBUnlinkable(t)

	client, err := NewDuckDBClient("")
	if err != nil {
		t.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()
	if err := client.initBenchmarkData(); err != nil {
		t.Fatalf("initBenchmarkData error = %v", err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	const readers = 64
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vec := make([]float32, 1024)
			_, err := client.SearchWithIntent(ctx, vec, "食品饮料", 3)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent read error = %v", err)
		}
	}
}

func TestSearchWithIntentContextCancellation(t *testing.T) {
	skipIfDuckDBUnlinkable(t)

	client, err := NewDuckDBClient("")
	if err != nil {
		t.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()
	if err := client.initBenchmarkData(); err != nil {
		t.Fatalf("initBenchmarkData error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vec := make([]float32, 1024)
	_, err = client.SearchWithIntent(ctx, vec, "", 5)
	if err == nil {
		t.Fatal("expected context canceled error")
	}
	if !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected canceled, got %v", err)
	}
}

// TestSearchByIntentDigitalIntentReturnsDigitalCategory verifies that
// SearchByIntent returns top results from the "数码电子" (digital electronics)
// category when presented with a vector biased towards the digital dimension
// segment (dims 0..127 set to 1.0, others to 0).
func TestSearchByIntentDigitalIntentReturnsDigitalCategory(t *testing.T) {
	skipIfDuckDBUnlinkable(t)

	client, err := NewDuckDBClient("")
	if err != nil {
		t.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()

	if err := client.initBenchmarkData(); err != nil {
		t.Fatalf("initBenchmarkData error = %v", err)
	}

	// Build digital-biased intent vector: dims 0..127 = 1.0, rest = 0.
	vec := make([]float32, 1024)
	for i := 0; i < 128; i++ {
		vec[i] = 1.0
	}

	ctx := context.Background()
	results, err := client.SearchByIntent(ctx, vec, "", 5)
	if err != nil {
		t.Fatalf("SearchByIntent error = %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("limit not respected: got %d, want 5", len(results))
	}
	for i, r := range results {
		if r.ItemID == "" {
			t.Fatalf("result[%d] has empty ItemID", i)
		}
		t.Logf("result[%d]: item_id=%s score=%.6f", i, r.ItemID, r.Score)
	}
	// Top result's item_id should start with "dig_" (digital category).
	firstID := results[0].ItemID
	if !strings.HasPrefix(firstID, "dig_") {
		t.Fatalf("top result item_id=%q does not start with dig_ (digital bias)", firstID)
	}
	// Count how many of top 5 are digital.
	digCount := 0
	for _, r := range results {
		if strings.HasPrefix(r.ItemID, "dig_") {
			digCount++
		}
	}
	if digCount < 3 {
		t.Fatalf("digital bias too weak: only %d/5 results from digital category", digCount)
	}
}

func TestSearchByIntentWithCategoryFilter(t *testing.T) {
	skipIfDuckDBUnlinkable(t)

	client, err := NewDuckDBClient("")
	if err != nil {
		t.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()

	if err := client.initBenchmarkData(); err != nil {
		t.Fatalf("initBenchmarkData error = %v", err)
	}

	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i%256) / 256.0
	}

	ctx := context.Background()
	results, err := client.SearchByIntent(ctx, vec, "食品饮料", 10)
	if err != nil {
		t.Fatalf("SearchByIntent error = %v", err)
	}
	if len(results) > 10 {
		t.Fatalf("limit exceeded: got %d", len(results))
	}
	for _, r := range results {
		if r.ItemID == "" {
			t.Fatal("empty ItemID in search result")
		}
		if !strings.HasPrefix(r.ItemID, "food_") {
			t.Fatalf("category filter failed: got item_id=%q, expected food_ prefix", r.ItemID)
		}
	}
}

func BenchmarkSearchWithIntent(b *testing.B) {
	client, err := NewDuckDBClient("")
	if err != nil {
		b.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()
	if err := client.initBenchmarkData(); err != nil {
		b.Fatalf("initBenchmarkData error = %v", err)
	}

	ctx := context.Background()
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i%256) / 256.0
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prods, err := client.SearchWithIntent(ctx, vec, "宠物生活", 10)
		if err != nil {
			b.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) == 0 {
			b.Fatal("expected products")
		}
	}
}
