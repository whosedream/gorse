package catalog

import (
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
)

// Item is a single product metadata record.
type Item struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	ImageURL string  `json:"image_url"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
}

// Catalog is a concurrent-safe in-memory dictionary of product metadata.
type Catalog struct {
	mu    sync.RWMutex
	items map[string]Item
}

// categoryPlaceholders maps category names to SVG data URI placeholders.
// Categories not in this map receive a generic gray placeholder.
var categoryPlaceholders = map[string]string{
	"数码电子": "%231a73e8",
	"母婴用品": "%23e91e63",
	"宠物生活": "%234caf50",
	"运动户外": "%23ff9800",
	"食品饮料": "%23ff5722",
	"猫咪用品": "%23ff9800",
	"手机数码": "%231a73e8",
	"图书":   "%23e91e63",
	"咖啡茶饮": "%23ff5722",
}

func categoryPlaceholderFor(category string) string {
	color, ok := categoryPlaceholders[category]
	if !ok {
		color = "%23666666"
	}
	return "data:image/svg+xml," +
		"%3Csvg xmlns='http://www.w3.org/2000/svg' width='400' height='400'%3E" +
		"%3Crect fill='" + color + "' width='400' height='400'/%3E" +
		"%3Ctext fill='white' font-family='sans-serif' font-size='24' " +
		"x='200' y='210' text-anchor='middle'%3E" + category + "%3C/text%3E" +
		"%3C/svg%3E"
}

const syntheticSeed = 42

// New creates a Catalog populated from csvPath. If csvPath is empty, it
// checks the CATALOG_CSV_PATH environment variable, then defaults to
// "data/taobao_items.csv". Falls back to synthetic data when no CSV is
// available or loaded.
func New(csvPath string) *Catalog {
	if csvPath == "" {
		csvPath = os.Getenv("CATALOG_CSV_PATH")
	}
	if csvPath == "" {
		csvPath = "data/taobao_items.csv"
	}
	c := &Catalog{items: make(map[string]Item)}
	if err := c.loadCSV(csvPath); err != nil {
		log.Printf("catalog: loadCSV(%q): %v (falling back to synthetic)", csvPath, err)
		return newSynthetic()
	}
	return c
}

// loadCSV reads product records from csvPath and populates c.items.
// The CSV must have columns: item_id, title, category, price, image_url.
// Image URLs from CSV are discarded; SVG placeholders are used instead.
// Price parse errors fall back to 99.0.
func (c *Catalog) loadCSV(csvPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1 // allow variable columns per row
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}
	if len(records) < 2 {
		return nil
	}

	for _, row := range records[1:] {
		if len(row) < 4 {
			continue
		}
		id := row[0]
		title := row[1]
		category := row[2]
		price, priceErr := strconv.ParseFloat(row[3], 64)
		if priceErr != nil {
			price = 99.0
		}

		c.items[id] = Item{
			ID:       id,
			Title:    title,
			Category: category,
			Price:    price,
			ImageURL: categoryPlaceholderFor(category),
		}
	}
	return nil
}

// newSynthetic returns a Catalog with 1000 benchmark items across 5
// synthetic categories. Used as a fallback when no real CSV is available.
func newSynthetic() *Catalog {
	const totalItems = 1000
	const itemsPerCat = 200

	catNames := []string{"数码电子", "母婴用品", "宠物生活", "运动户外", "食品饮料"}

	c := &Catalog{items: make(map[string]Item, totalItems)}
	rng := rand.New(rand.NewSource(syntheticSeed))

	for i := 1; i <= totalItems; i++ {
		catIdx := (i - 1) / itemsPerCat
		catName := catNames[catIdx]
		seq := (i-1)%itemsPerCat + 1

		c.items[strconv.Itoa(i)] = Item{
			ID:       strconv.Itoa(i),
			Title:    catName + strconv.Itoa(seq),
			ImageURL: categoryPlaceholderFor(catName),
			Price:    float64(rng.Intn(1991) + 10),
			Category: catName,
		}
	}
	return c
}

// IDs returns all item IDs in the catalog. Returns nil when c is nil.
func (c *Catalog) IDs() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.items))
	for id := range c.items {
		ids = append(ids, id)
	}
	return ids
}

// Count returns the number of items in the catalog.
func (c *Catalog) Count() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Get returns the items identified by ids in the order requested.
// Unknown IDs are silently skipped. Returns nil when c is nil.
//
// Concurrent-safe: uses RLock to allow concurrent reads.
func (c *Catalog) Get(ids []string) []Item {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]Item, 0, len(ids))
	for _, id := range ids {
		if item, ok := c.items[id]; ok {
			result = append(result, item)
		}
	}
	return result
}