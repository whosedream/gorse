package cf

import (
	"context"
	"fmt"
	"sort"

	"go-rec/pkg/cache"
)

// CatalogItem represents an item with its pre-computed embedding vector.
type CatalogItem struct {
	ItemID    string
	Embedding []float32
}

// Catalog provides access to all items and their embedding vectors.
type Catalog struct {
	items []CatalogItem
 index map[string]int // itemID -> index into items slice
}

// NewCatalog creates a Catalog from a slice of items.
func NewCatalog(items []CatalogItem) *Catalog {
	index := make(map[string]int, len(items))
	for i, item := range items {
		index[item.ItemID] = i
	}
	return &Catalog{items: items, index: index}
}

// Items returns the full item list.
func (c *Catalog) Items() []CatalogItem {
	if c == nil {
		return nil
	}
	return c.items
}

// Len returns the number of items.
func (c *Catalog) Len() int {
	if c == nil {
		return 0
	}
	return len(c.items)
}

// ProfileRecaller implements profile-based recall.
// It reads an intent vector from the slow-track LLM DAG via IntentReader,
// then scores all catalog items by dot-product against that vector,
// and returns the Top-K candidates.
type ProfileRecaller struct {
	intentReader cache.IntentReader
	dim          int
	catalog      *Catalog
}

// NewProfileRecaller creates a new ProfileRecaller.
// intentReader provides access to the LLM-produced intent vectors.
// catalog holds all items with their pre-computed embedding vectors.
// dim must match cache.IntentVectorDim (1024).
func NewProfileRecaller(intentReader cache.IntentReader, catalog *Catalog, dim int) *ProfileRecaller {
	if dim <= 0 {
		dim = cache.IntentVectorDim
	}
	return &ProfileRecaller{
		intentReader: intentReader,
		dim:          dim,
		catalog:      catalog,
	}
}

// Recall reads the intent vector for the given session (userID is treated as sessionID),
// computes dot-product scores against all catalog item embeddings,
// and returns the top-K candidates sorted by descending score.
func (r *ProfileRecaller) Recall(ctx context.Context, userID string, topK int) ([]Candidate, error) {
	if r == nil || r.intentReader == nil {
		return nil, fmt.Errorf("profile recaller not initialized")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if topK <= 0 {
		return nil, nil
	}

	// Read intent vector from Redis via IntentReader.
	dst := make([]float32, r.dim)
	if _, err := r.intentReader.ReadIntent(ctx, userID, dst); err != nil {
		return nil, fmt.Errorf("profile recaller: read intent: %w", err)
	}

	catalog := r.catalog
	if catalog == nil || catalog.Len() == 0 {
		return nil, nil
	}

	items := catalog.Items()

	// Score all items by dot-product with the intent vector.
	scores := make([]Candidate, len(items))
	for i, item := range items {
		scores[i] = Candidate{
			ItemID: item.ItemID,
			Score:  dotProduct(dst, item.Embedding),
			Source: "profile",
		}
	}

	// Sort descending by score, stable on equal scores by itemID for determinism.
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Score != scores[j].Score {
			return scores[i].Score > scores[j].Score
		}
		return scores[i].ItemID < scores[j].ItemID
	})

	if topK > len(scores) {
		topK = len(scores)
	}

	return scores[:topK], nil
}

// dotProduct computes the dot-product of two float32 slices of equal length.
// Uses the same semantics as scorer.dot but kept local to avoid import cycles.
func dotProduct(a, b []float32) float32 {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	var s float32
	for i := 0; i < limit; i++ {
		s += a[i] * b[i]
	}
	return s
}
