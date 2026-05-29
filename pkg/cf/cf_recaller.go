package cf

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sort"
	"sync"
)

// CFRecaller implements collaborative filtering recall using BPR + HNSW.
// It trains a BPR model, builds an HNSW index over item embeddings,
// and performs ANN search for user recall.
type CFRecaller struct {
	mu          sync.RWMutex
	bpr         *BPR
	hnsw        *HNSW
	itemIDs     []string   // index -> itemID mapping for HNSW
	itemIDToIdx map[string]int
	ready       bool
}

// NewCFRecaller creates a new CFRecaller with default parameters.
func NewCFRecaller() *CFRecaller {
	return &CFRecaller{
		itemIDToIdx: make(map[string]int),
	}
}

// CFTrainConfig holds training configuration.
type CFTrainConfig struct {
	BPRParams BPRParams
	HNSW      HNSWConfig
	TopK      int // number of candidates to return
}

// DefaultCFTrainConfig returns recommended defaults.
func DefaultCFTrainConfig() CFTrainConfig {
	return CFTrainConfig{
		BPRParams: DefaultBPRParams(),
		HNSW:      DefaultHNSWConfig(),
		TopK:      50,
	}
}

// Train builds the BPR model and HNSW index from user-item interactions.
func (r *CFRecaller) Train(interactions []Interaction, cfg CFTrainConfig) {
	// Train BPR model
	r.bpr = NewBPR(cfg.BPRParams)
	r.bpr.Fit(interactions)

	// Build item ID mapping
	r.itemIDs = r.bpr.ItemIDs()
	r.itemIDToIdx = make(map[string]int, len(r.itemIDs))
	for i, id := range r.itemIDs {
		r.itemIDToIdx[id] = i
	}

	// Build HNSW index over item latent vectors
	r.hnsw = NewHNSW(cfg.HNSW)
	for i, vec := range r.bpr.ItemFactor {
		r.hnsw.Add(i, vec)
	}

	r.mu.Lock()
	r.ready = true
	r.mu.Unlock()
}

// Recall returns the top-K candidates for a user using ANN search.
func (r *CFRecaller) Recall(ctx context.Context, userID string, topK int) ([]Candidate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.ready {
		return nil, fmt.Errorf("cf recaller not trained")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get user latent vector
	userVec, ok := r.bpr.UserFactors(userID)
	if !ok {
		// Cold-start user: return empty (caller should fall back to other recall paths)
		return nil, nil
	}

	// ANN search via HNSW
	results := r.hnsw.Search(userVec, topK)

	candidates := make([]Candidate, len(results))
	for i, res := range results {
		candidates[i] = Candidate{
			ItemID: r.itemIDs[res.ID],
			Score:  1.0 - res.Distance, // convert distance back to similarity
			Source: "cf",
		}
	}

	return candidates, nil
}

// Predict returns the BPR score for a (user, item) pair.
func (r *CFRecaller) Predict(userID, itemID string) float32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.bpr == nil {
		return 0
	}
	return r.bpr.Predict(userID, itemID)
}

// Marshal serializes the entire CFRecaller state.
func (r *CFRecaller) Marshal() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.bpr == nil {
		return nil, fmt.Errorf("nothing to serialize")
	}

	type cfState struct {
		BPR  []byte
		HNSW []byte
	}

	bprData, err := r.bpr.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal bpr: %w", err)
	}
	hnswData, err := r.hnsw.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal hnsw: %w", err)
	}

	// Use gob for the wrapper
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cfState{BPR: bprData, HNSW: hnswData}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalCFRecaller deserializes a CFRecaller from bytes.
func UnmarshalCFRecaller(data []byte) (*CFRecaller, error) {
	type cfState struct {
		BPR  []byte
		HNSW []byte
	}

	var s cfState
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}

	bpr, err := UnmarshalBPR(s.BPR)
	if err != nil {
		return nil, fmt.Errorf("unmarshal bpr: %w", err)
	}
	hnsw, err := UnmarshalHNSW(s.HNSW)
	if err != nil {
		return nil, fmt.Errorf("unmarshal hnsw: %w", err)
	}

	itemIDs := bpr.ItemIDs()
	itemIDToIdx := make(map[string]int, len(itemIDs))
	for i, id := range itemIDs {
		itemIDToIdx[id] = i
	}

	return &CFRecaller{
		bpr:         bpr,
		hnsw:        hnsw,
		itemIDs:     itemIDs,
		itemIDToIdx: itemIDToIdx,
		ready:       true,
	}, nil
}

// SortCandidates sorts candidates by score descending in-place.
func SortCandidates(candidates []Candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})
}
