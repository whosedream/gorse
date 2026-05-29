package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testCSVPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test_items.csv")
}

func writeTestCSV(t *testing.T) string {
	t.Helper()
	dir := testCSVPath(t)
	csv := "item_id,title,category,price,image_url\n" +
		"test_001,测试商品一,数码电子,99.50,https://example.com/img1.jpg\n" +
		"test_002,测试商品二,母婴用品,199.00,https://example.com/img2.jpg\n" +
		"test_003,测试商品三,数码电子,broken_price_format_should_fallback\n"
	os.WriteFile(dir, []byte(csv), 0o644)
	return dir
}

func TestNewLoadsFromCSV(t *testing.T) {
	t.Parallel()
	c := New(writeTestCSV(t))

	// 3 items from CSV (2 valid + 1 with broken price → fallback 99.0).
	items := c.Get([]string{"test_001", "test_002", "test_003"})
	if len(items) != 3 {
		t.Fatalf("Get(all) = %d items, want 3", len(items))
	}

	if items[0].Title != "测试商品一" {
		t.Fatalf("items[0].Title = %q, want 测试商品一", items[0].Title)
	}
	if items[0].Category != "数码电子" {
		t.Fatalf("items[0].Category = %q, want 数码电子", items[0].Category)
	}
	if items[0].Price != 99.50 {
		t.Fatalf("items[0].Price = %f, want 99.50", items[0].Price)
	}

	// Image URLs from CSV must be ignored; SVG placeholder used instead.
	if items[0].ImageURL == "https://example.com/img1.jpg" {
		t.Fatal("items[0].ImageURL uses external CSV URL; SVG placeholder expected")
	}
	if items[0].ImageURL == "" {
		t.Fatal("items[0].ImageURL is empty; SVG placeholder expected")
	}

	// Broken price falls back to 99.0.
	if items[2].Price != 99.0 {
		t.Fatalf("items[2].Price = %f, want 99.0 (fallback for broken price)", items[2].Price)
	}
}

func TestNewFallbackWhenCSVMissing(t *testing.T) {
	t.Parallel()

	// Passing a nonexistent path forces synthetic fallback.
	c := New("/nonexistent/path/products.csv")

	// Should fall back to synthetic data (1000 items).
	items := c.Get([]string{"1"})
	if len(items) != 1 {
		t.Fatalf("Get('1') = %d items, want 1 (synthetic fallback)", len(items))
	}
	if items[0].ID != "1" {
		t.Fatalf("items[0].ID = %q, want '1'", items[0].ID)
	}
}

func TestNewFromRealCSV(t *testing.T) {
	// Not parallel — uses real CSV path.

	realPath := filepath.FromSlash("../../data/taobao_items.csv")
	if _, err := os.Stat(realPath); os.IsNotExist(err) {
		t.Skipf("real CSV not found at %s, skipping", realPath)
	}

	c := New(realPath)

	// Should load 50 items from the real CSV (51 lines minus header).
	knownIDs := []string{"cat_001", "phone_001", "sport_001", "book_001", "coffee_001"}
	items := c.Get(knownIDs)
	if len(items) != len(knownIDs) {
		t.Fatalf("Get(known IDs) = %d items, want %d", len(items), len(knownIDs))
	}

	// cat_001 should be 天然有机猫薄荷逗猫棒羽毛款.
	if items[0].ID != "cat_001" || items[0].Title != "天然有机猫薄荷逗猫棒羽毛款" {
		t.Fatalf("cat_001 = %+v, want title 天然有机猫薄荷逗猫棒羽毛款", items[0])
	}
	if items[0].Category != "猫咪用品" || items[0].Price != 19.90 {
		t.Fatalf("cat_001: category=%s price=%f, want 猫咪用品 19.90", items[0].Category, items[0].Price)
	}
	if items[0].ImageURL == "" || items[0].ImageURL == "https://img.alicdn.com/cat/catnip001.jpg" {
		t.Fatal("cat_001 ImageURL should be SVG placeholder, not CSV external URL")
	}
}

func TestCategoryPlaceholderForKnownCategory(t *testing.T) {
	t.Parallel()

	uri := categoryPlaceholderFor("手机数码")
	if uri == "" {
		t.Fatal("categoryPlaceholderFor returned empty string")
	}
	if uri[:5] != "data:" {
		t.Fatalf("categoryPlaceholderFor returned non-data-URI: %.40s...", uri)
	}
}

func TestCategoryPlaceholderForUnknownCategory(t *testing.T) {
	t.Parallel()

	uri := categoryPlaceholderFor("不存在的分类")
	if uri == "" {
		t.Fatal("categoryPlaceholderFor should return a placeholder for unknown categories")
	}
	if uri[:5] != "data:" {
		t.Fatalf("categoryPlaceholderFor returned non-data-URI: %.40s...", uri)
	}
}

func TestGetReturnsRequestOrder(t *testing.T) {
	t.Parallel()

	c := New(writeTestCSV(t))

	ids := []string{"test_003", "test_001"}
	items := c.Get(ids)
	if len(items) != 2 {
		t.Fatalf("len = %d, want 2", len(items))
	}
	for i, want := range ids {
		if items[i].ID != want {
			t.Fatalf("items[%d].ID = %q, want %q", i, items[i].ID, want)
		}
	}
}

func TestGetSkipsUnknownIDs(t *testing.T) {
	t.Parallel()

	c := New(writeTestCSV(t))

	ids := []string{"test_001", "nonexistent", "test_002"}
	items := c.Get(ids)
	if len(items) != 2 {
		t.Fatalf("len = %d, want 2", len(items))
	}
	want := []string{"test_001", "test_002"}
	for i, w := range want {
		if items[i].ID != w {
			t.Fatalf("items[%d].ID = %q, want %q", i, items[i].ID, w)
		}
	}
}

func TestGetEmptyIDsReturnsEmpty(t *testing.T) {
	t.Parallel()

	c := New("")
	items := c.Get(nil)
	if len(items) != 0 {
		t.Fatalf("nil ids: len = %d, want 0", len(items))
	}
	items = c.Get([]string{})
	if len(items) != 0 {
		t.Fatalf("empty ids: len = %d, want 0", len(items))
	}
}

func TestNilCatalogGetReturnsNil(t *testing.T) {
	t.Parallel()

	var c *Catalog
	items := c.Get([]string{"1"})
	if items != nil {
		t.Fatalf("nil catalog Get() = %v, want nil", items)
	}
}

func TestConcurrentReads(t *testing.T) {
	c := New(writeTestCSV(t))

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			// Alternate reads between test_001 and test_002.
			id := "test_001"
			if seed%2 == 0 {
				id = "test_002"
			}
			for i := 0; i < 100; i++ {
				items := c.Get([]string{id})
				if len(items) != 1 {
					errs <- fmt.Errorf("Get(%q) = %d items", id, len(items))
					return
				}
				if items[0].ID != id {
					errs <- fmt.Errorf("Get(%q).ID = %q", id, items[0].ID)
					return
				}
			}
		}(int64(g))
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}