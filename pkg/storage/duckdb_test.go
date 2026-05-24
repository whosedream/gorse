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

	if err := client.initPresetData(); err != nil {
		t.Fatalf("initPresetData error = %v", err)
	}

	ctx := context.Background()

	t.Run("category filter returns correct count", func(t *testing.T) {
		vec := make([]float32, 1024)
		for i := range vec {
			vec[i] = float32(i%256) / 256.0
		}
		prods, err := client.SearchWithIntent(ctx, vec, "猫咪用品", 5)
		if err != nil {
			t.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) > 5 {
			t.Fatalf("limit exceeded: got %d", len(prods))
		}
		for _, p := range prods {
			if p.Category != "猫咪用品" {
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
	if err := client.initPresetData(); err != nil {
		t.Fatalf("initPresetData error = %v", err)
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
			_, err := client.SearchWithIntent(ctx, vec, "咖啡茶饮", 3)
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
	if err := client.initPresetData(); err != nil {
		t.Fatalf("initPresetData error = %v", err)
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

func BenchmarkSearchWithIntent(b *testing.B) {
	client, err := NewDuckDBClient("")
	if err != nil {
		b.Fatalf("NewDuckDBClient error = %v", err)
	}
	defer client.Close()
	if err := client.initPresetData(); err != nil {
		b.Fatalf("initPresetData error = %v", err)
	}

	ctx := context.Background()
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i%256) / 256.0
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prods, err := client.SearchWithIntent(ctx, vec, "猫咪用品", 10)
		if err != nil {
			b.Fatalf("SearchWithIntent error = %v", err)
		}
		if len(prods) == 0 {
			b.Fatal("expected products")
		}
	}
}
