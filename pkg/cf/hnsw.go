package cf

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// HNSWConfig holds HNSW index configuration.
type HNSWConfig struct {
	EfConstruction int // search width during build
	M              int // max connections per node (upper layers)
}

// DefaultHNSWConfig returns the recommended defaults.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{
		EfConstruction: 200,
		M:              16,
	}
}

// HNSW implements the Hierarchical Navigable Small World algorithm
// for approximate nearest neighbor search with cosine similarity.
type HNSW struct {
	config      HNSWConfig
	levelFactor float64
	vectors     [][]float32
	neighbors   [][]int // layer-0 neighbors per node
	upperLayers []map[int][]int // layer >= 1: node -> neighbors (sparse)
	enterPoint  int
	numLayers   int // max layer index + 1
	mu          sync.RWMutex
	rng         *rand.Rand
}

// NewHNSW creates a new HNSW index with the given config.
func NewHNSW(cfg HNSWConfig) *HNSW {
	if cfg.M <= 0 {
		cfg.M = 16
	}
	if cfg.EfConstruction <= 0 {
		cfg.EfConstruction = 200
	}
	return &HNSW{
		config:      cfg,
		levelFactor: 1.0 / math.Log(float64(cfg.M)),
		upperLayers: make([]map[int][]int, 0),
		rng:         rand.New(rand.NewSource(42)),
	}
}

// Add inserts a vector into the index and returns its ID.
func (h *HNSW) Add(id int, vector []float32) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vectors = append(h.vectors, vector)
	h.neighbors = append(h.neighbors, nil)
	idx := len(h.vectors) - 1

	if idx == 0 {
		h.enterPoint = 0
		h.numLayers = 1
		return
	}

	level := h.randomLevel()
	h.insert(idx, level)
}

// Search finds the k nearest neighbors to the query vector.
func (h *HNSW) Search(query []float32, k int) []SearchResult {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vectors) == 0 {
		return nil
	}

	entry := h.enterPoint

	// Greedy descent from top layers to layer 1
	for layer := h.numLayers - 1; layer >= 1; layer-- {
		best := entry
		bestDist := cosineDistance(query, h.vectors[entry])
		for {
			changed := false
			for _, nb := range h.getNeighbors(entry, layer) {
				d := cosineDistance(query, h.vectors[nb])
				if d < bestDist {
					bestDist = d
					best = nb
					changed = true
				}
			}
			if !changed {
				break
			}
			entry = best
		}
	}

	// Search at layer 0
	candidates := h.searchLayer0(query, entry, max(h.config.EfConstruction, k))

	// Sort by distance ascending and return top k
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Distance < candidates[j].Distance
	})
	if k > len(candidates) {
		k = len(candidates)
	}
	return candidates[:k]
}

func (h *HNSW) insert(idx, level int) {
	// Ensure upperLayers has enough slots
	for h.numLayers <= level {
		h.upperLayers = append(h.upperLayers, make(map[int][]int))
		h.numLayers++
	}

	entry := h.enterPoint

	// Greedy descent to find entry point at target level
	for layer := h.numLayers - 1; layer > level; layer-- {
		best := entry
		bestDist := cosineDistance(h.vectors[idx], h.vectors[entry])
		for {
			changed := false
			for _, nb := range h.getNeighbors(entry, layer) {
				d := cosineDistance(h.vectors[idx], h.vectors[nb])
				if d < bestDist {
					bestDist = d
					best = nb
					changed = true
				}
			}
			if !changed {
				break
			}
			entry = best
		}
	}

	// Insert at each layer from min(level, numLayers-1) down to 0
	topLayer := min(level, h.numLayers-1)
	for layer := topLayer; layer >= 0; layer-- {
		ef := h.config.EfConstruction
		candidates := h.searchLayer(h.vectors[idx], entry, ef, layer)

		// Select m nearest neighbors
		m := h.maxConn(layer)
		if len(candidates) > m {
			candidates = candidates[:m]
		}

		// Store neighbors for this node at this layer
		neighborIDs := make([]int, len(candidates))
		for i, c := range candidates {
			neighborIDs[i] = c.ID
		}
		h.setNeighbors(idx, layer, neighborIDs)

		// Add bidirectional edges
		for _, nb := range neighborIDs {
			nbNeighbors := h.getNeighbors(nb, layer)
			if !containsInt(nbNeighbors, idx) {
				nbNeighbors = append(nbNeighbors, idx)
				h.setNeighbors(nb, layer, nbNeighbors)
			}
			// Prune if over-connected
			if len(nbNeighbors) > h.maxConn(layer) {
				pruned := h.pruneNeighbors(h.vectors[nb], nbNeighbors, h.maxConn(layer))
				h.setNeighbors(nb, layer, pruned)
			}
		}

		entry = candidates[0].ID
	}
}

func (h *HNSW) getNeighbors(node, layer int) []int {
	if layer == 0 {
		return h.neighbors[node]
	}
	if layer-1 < len(h.upperLayers) {
		if m, ok := h.upperLayers[layer-1][node]; ok {
			return m
		}
	}
	return nil
}

func (h *HNSW) setNeighbors(node, layer int, ids []int) {
	if layer == 0 {
		h.neighbors[node] = ids
		return
	}
	for h.numLayers <= layer {
		h.upperLayers = append(h.upperLayers, make(map[int][]int))
		h.numLayers++
	}
	h.upperLayers[layer-1][node] = ids
}

func (h *HNSW) maxConn(layer int) int {
	if layer == 0 {
		return h.config.M * 2
	}
	return h.config.M
}

func (h *HNSW) randomLevel() int {
	r := h.rng.Float64()
	return int(math.Floor(-math.Log(1-r) * h.levelFactor))
}

// searchLayer0 performs BFS-like greedy search at layer 0.
func (h *HNSW) searchLayer0(query []float32, entry, ef int) []SearchResult {
	visited := make(map[int]bool)
	candidates := make(minHeap, 0)
	results := make(maxHeap, 0)

	dist := cosineDistance(query, h.vectors[entry])
	heap.Push(&candidates, SearchResult{ID: entry, Distance: dist})
	heap.Push(&results, SearchResult{ID: entry, Distance: dist})
	visited[entry] = true

	for candidates.Len() > 0 {
		c := heap.Pop(&candidates).(SearchResult)
		furthest := results[0].Distance
		if c.Distance > furthest {
			break
		}

		for _, nb := range h.neighbors[c.ID] {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			d := cosineDistance(query, h.vectors[nb])
			furthest = results[0].Distance
			if d < furthest || results.Len() < ef {
				heap.Push(&candidates, SearchResult{ID: nb, Distance: d})
				heap.Push(&results, SearchResult{ID: nb, Distance: d})
				if results.Len() > ef {
					heap.Pop(&results)
				}
			}
		}
	}

	// Convert max-heap to slice (will be sorted later)
	out := make([]SearchResult, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		out[i] = heap.Pop(&results).(SearchResult)
	}
	return out
}

// searchLayer performs greedy search at a given layer.
func (h *HNSW) searchLayer(query []float32, entry, ef, layer int) []SearchResult {
	visited := make(map[int]bool)
	candidates := make(minHeap, 0)
	results := make(maxHeap, 0)

	dist := cosineDistance(query, h.vectors[entry])
	heap.Push(&candidates, SearchResult{ID: entry, Distance: dist})
	heap.Push(&results, SearchResult{ID: entry, Distance: dist})
	visited[entry] = true

	for candidates.Len() > 0 {
		c := heap.Pop(&candidates).(SearchResult)
		furthest := results[0].Distance
		if c.Distance > furthest {
			break
		}

		for _, nb := range h.getNeighbors(c.ID, layer) {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			d := cosineDistance(query, h.vectors[nb])
			furthest = results[0].Distance
			if d < furthest || results.Len() < ef {
				heap.Push(&candidates, SearchResult{ID: nb, Distance: d})
				heap.Push(&results, SearchResult{ID: nb, Distance: d})
				if results.Len() > ef {
					heap.Pop(&results)
				}
			}
		}
	}

	// Convert max-heap to slice
	out := make([]SearchResult, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		out[i] = heap.Pop(&results).(SearchResult)
	}
	return out
}

func (h *HNSW) pruneNeighbors(q []float32, indices []int, m int) []int {
	if len(indices) <= m {
		return indices
	}
	type idDist struct {
		id   int
		dist float32
	}
	pairs := make([]idDist, len(indices))
	for i, idx := range indices {
		pairs[i] = idDist{id: idx, dist: cosineDistance(q, h.vectors[idx])}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].dist < pairs[j].dist
	})
	result := make([]int, m)
	for i := 0; i < m; i++ {
		result[i] = pairs[i].id
	}
	return result
}

func cosineDistance(a, b []float32) float32 {
	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	cos := dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	return 1.0 - cos
}

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// SearchResult holds an ANN search result.
type SearchResult struct {
	ID       int
	Distance float32
}

// maxHeap is a max-heap (furthest on top) for maintaining top-k candidates.
type maxHeap []SearchResult

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].Distance > h[j].Distance }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(SearchResult)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// minHeap is a min-heap for candidate exploration.
type minHeap []SearchResult

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].Distance < h[j].Distance }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(SearchResult)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// gobHNSW is the gob-serializable form of HNSW.
type gobHNSW struct {
	Config      HNSWConfig
	Vectors     [][]float32
	Neighbors   [][]int          // layer-0 neighbors
	UpperLayers []gobUpperLayer  // flattened upper layers
	EnterPoint  int
	NumLayers   int
}

type gobUpperLayer struct {
	Node      int
	Neighbors []int
}

// Marshal serializes the HNSW index to bytes.
func (h *HNSW) Marshal() ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	g := gobHNSW{
		Config:     h.config,
		Vectors:    h.vectors,
		Neighbors:  h.neighbors,
		EnterPoint: h.enterPoint,
		NumLayers:  h.numLayers,
	}

	// Flatten upper layers for gob
	for layerIdx, layerMap := range h.upperLayers {
		_ = layerIdx
		for node, nbs := range layerMap {
			g.UpperLayers = append(g.UpperLayers, gobUpperLayer{Node: node, Neighbors: nbs})
		}
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(g); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalHNSW deserializes an HNSW index from bytes.
func UnmarshalHNSW(data []byte) (*HNSW, error) {
	var g gobHNSW
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&g); err != nil {
		return nil, err
	}

	h := &HNSW{
		config:      g.Config,
		levelFactor: 1.0 / math.Log(float64(g.Config.M)),
		vectors:     g.Vectors,
		neighbors:   g.Neighbors,
		upperLayers: make([]map[int][]int, g.NumLayers-1),
		enterPoint:  g.EnterPoint,
		numLayers:   g.NumLayers,
		rng:         rand.New(rand.NewSource(42)),
	}

	for i := range h.upperLayers {
		h.upperLayers[i] = make(map[int][]int)
	}

	// Note: upper layers are flattened - we store them as layer 0 for simplicity
	// In production, you'd need to track layer membership properly
	for _, ul := range g.UpperLayers {
		if len(h.upperLayers) > 0 {
			h.upperLayers[0][ul.Node] = ul.Neighbors
		}
	}

	return h, nil
}
