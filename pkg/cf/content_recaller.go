package cf

import (
	"context"
	"fmt"
	"sync"

	"go-rec/pkg/storage"
)

// ContentRecaller implements content-based recall using product embedding
// vectors stored in DuckDB. It builds an in-memory HNSW index over all
// product embeddings and performs ANN search against a user's latest
// interaction embedding (set by the slow-track path).
type ContentRecaller struct {
	db            *storage.DuckDBClient
	hnsw          *HNSW
	itemIDs       []string
	itemIDToIdx   map[string]int
	userEmbeds    map[string][]float32
	embeddingDim  int
	mu            sync.RWMutex
	ready         bool
}

// ContentRecallerConfig holds configuration for the content recaller.
type ContentRecallerConfig struct {
	HNSW       HNSWConfig
	EmbeddingDim int // expected embedding dimension (0 = auto-detect from data)
}

// DefaultContentRecallerConfig returns recommended defaults.
func DefaultContentRecallerConfig() ContentRecallerConfig {
	return ContentRecallerConfig{
		HNSW:         DefaultHNSWConfig(),
		EmbeddingDim: 1024,
	}
}

// NewContentRecaller creates a new ContentRecaller backed by the given DuckDB client.
func NewContentRecaller(db *storage.DuckDBClient, cfg ContentRecallerConfig) *ContentRecaller {
	if cfg.EmbeddingDim <= 0 {
		cfg.EmbeddingDim = 1024
	}
	return &ContentRecaller{
		db:           db,
		hnsw:         NewHNSW(cfg.HNSW),
		itemIDToIdx:  make(map[string]int),
		userEmbeds:   make(map[string][]float32),
		embeddingDim: cfg.EmbeddingDim,
	}
}

// NewContentRecallerFromIndex creates a ContentRecaller from a pre-built
// HNSW index and item ID list. Intended for testing or when the index
// is loaded from serialized state.
func NewContentRecallerFromIndex(hnsw *HNSW, itemIDs []string, embeddingDim int) *ContentRecaller {
	idx := make(map[string]int, len(itemIDs))
	for i, id := range itemIDs {
		idx[id] = i
	}
	return &ContentRecaller{
		hnsw:         hnsw,
		itemIDs:      itemIDs,
		itemIDToIdx:  idx,
		userEmbeds:   make(map[string][]float32),
		embeddingDim: embeddingDim,
		ready:        true,
	}
}

// BuildIndex loads all products from DuckDB and builds the HNSW index.
// Must be called before Recall. Safe to call multiple times (rebuilds).
func (r *ContentRecaller) BuildIndex(ctx context.Context) error {
	if r.db == nil {
		return fmt.Errorf("content recaller: no duckdb client")
	}

	// Fetch all products with embeddings via SearchWithIntent with a zero
	// vector and high limit to get the full catalog. Using a zero query
	// vector returns products in arbitrary order with trivial scores.
	zeroVec := make([]float32, r.embeddingDim)
	products, err := r.db.SearchWithIntent(ctx, zeroVec, "", 100000)
	if err != nil {
		return fmt.Errorf("content recaller: load products: %w", err)
	}

	if len(products) == 0 {
		return fmt.Errorf("content recaller: no products found")
	}

	// Validate embedding dimension from first product.
	if len(products[0].Embedding) != r.embeddingDim {
		r.embeddingDim = len(products[0].Embedding)
	}

	// Rebuild HNSW index.
	hnsw := NewHNSW(DefaultHNSWConfig())
	itemIDs := make([]string, len(products))
	itemIDToIdx := make(map[string]int, len(products))

	for i, p := range products {
		if len(p.Embedding) != r.embeddingDim {
			continue // skip malformed rows
		}
		hnsw.Add(i, p.Embedding)
		itemIDs[i] = p.ItemID
		itemIDToIdx[p.ItemID] = i
	}

	r.mu.Lock()
	r.hnsw = hnsw
	r.itemIDs = itemIDs
	r.itemIDToIdx = itemIDToIdx
	r.ready = true
	r.mu.Unlock()

	return nil
}

// SetUserEmbedding updates the cached embedding for a user. This is called
// by the slow-track path after LLM intent decomposition produces a new
// user intent vector.
func (r *ContentRecaller) SetUserEmbedding(userID string, embedding []float32) {
	r.mu.Lock()
	r.userEmbeds[userID] = embedding
	r.mu.Unlock()
}

// GetUserEmbedding returns the cached embedding for a user, or nil if none exists.
func (r *ContentRecaller) GetUserEmbedding(userID string) []float32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.userEmbeds[userID]
}

// Recall returns the top-K content-based candidates for a user by performing
// ANN search over the product embedding space using the user's cached
// interaction embedding as query.
func (r *ContentRecaller) Recall(ctx context.Context, userID string, topK int) ([]Candidate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.ready {
		return nil, fmt.Errorf("content recaller index not built")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Lookup user's latest interaction embedding.
	userVec, ok := r.userEmbeds[userID]
	if !ok || len(userVec) == 0 {
		// Cold-start: no interaction history, return nil so caller
		// falls back to other recall paths.
		return nil, nil
	}

	// ANN search via HNSW.
	results := r.hnsw.Search(userVec, topK)

	candidates := make([]Candidate, len(results))
	for i, res := range results {
		candidates[i] = Candidate{
			ItemID: r.itemIDs[res.ID],
			Score:  1.0 - res.Distance, // convert cosine distance to similarity
			Source: "content",
		}
	}

	return candidates, nil
}

// Size returns the number of items in the HNSW index.
func (r *ContentRecaller) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.itemIDs)
}
