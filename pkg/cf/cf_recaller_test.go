package cf

import (
	"context"
	"math"
	"testing"
)

// --- BPR tests ---

func TestBPR_Basic(t *testing.T) {
	params := DefaultBPRParams()
	params.NEpochs = 50
	bpr := NewBPR(params)

	interactions := []Interaction{
		{"u1", "i1"}, {"u1", "i2"}, {"u1", "i3"},
		{"u2", "i2"}, {"u2", "i3"}, {"u2", "i4"},
		{"u3", "i1"}, {"u3", "i3"}, {"u3", "i5"},
		{"u4", "i4"}, {"u4", "i5"}, {"u4", "i6"},
	}

	bpr.Fit(interactions)

	// u1 interacted with i1, i2, i3 — should score them higher than unseen items
	scoreSeen := bpr.Predict("u1", "i1")
	scoreUnseen := bpr.Predict("u1", "i6")
	if scoreSeen <= scoreUnseen {
		t.Errorf("expected seen item score > unseen, got %f vs %f", scoreSeen, scoreUnseen)
	}

	// Unknown user should return 0
	if s := bpr.Predict("unknown", "i1"); s != 0 {
		t.Errorf("expected 0 for unknown user, got %f", s)
	}
}

func TestBPR_LatentVectors(t *testing.T) {
	bpr := NewBPR(DefaultBPRParams())
	bpr.Fit([]Interaction{{"u1", "i1"}, {"u2", "i2"}})

	vec, ok := bpr.UserFactors("u1")
	if !ok || len(vec) != 16 {
		t.Fatalf("expected 16-dim vector, got %d (ok=%v)", len(vec), ok)
	}

	if len(bpr.ItemIDs()) != 2 {
		t.Fatalf("expected 2 items, got %d", len(bpr.ItemIDs()))
	}
}

func TestBPR_Serialization(t *testing.T) {
	bpr := NewBPR(DefaultBPRParams())
	bpr.Fit([]Interaction{{"u1", "i1"}, {"u1", "i2"}, {"u2", "i2"}})

	data, err := bpr.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	bpr2, err := UnmarshalBPR(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	s1 := bpr.Predict("u1", "i1")
	s2 := bpr2.Predict("u1", "i1")
	if math.Abs(float64(s1-s2)) > 1e-6 {
		t.Errorf("scores differ after round-trip: %f vs %f", s1, s2)
	}
}

// --- HNSW tests ---

func TestHNSW_BasicSearch(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig())

	// Insert vectors on a circle in 2D
	for i := 0; i < 100; i++ {
		angle := float32(i) * 2 * math.Pi / 100
		vec := []float32{float32(math.Cos(float64(angle))), float32(math.Sin(float64(angle)))}
		h.Add(i, vec)
	}

	// Query: nearest to (1, 0) should be node 0
	query := []float32{1, 0}
	results := h.Search(query, 5)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	if results[0].ID != 0 {
		t.Errorf("expected nearest to be 0, got %d", results[0].ID)
	}
	// Distances should be ascending
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("results not sorted: %f < %f", results[i].Distance, results[i-1].Distance)
		}
	}
}

func TestHNSW_HighDimensional(t *testing.T) {
	h := NewHNSW(HNSWConfig{EfConstruction: 50, M: 8})
	dim := 32

	// Insert random vectors
	vectors := make([][]float32, 200)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = float32(j*i%100) / 100.0
		}
		h.Add(i, vectors[i])
	}

	// Brute-force search for ground truth
	query := vectors[42]
	type bfResult struct {
		id   int
		dist float32
	}
	bf := make([]bfResult, len(vectors))
	for i, v := range vectors {
		bf[i] = bfResult{id: i, dist: cosineDistance(query, v)}
	}
	for i := 1; i < len(bf); i++ {
		for j := i; j > 0 && bf[j].dist < bf[j-1].dist; j-- {
			bf[j], bf[j-1] = bf[j-1], bf[j]
		}
	}

	// HNSW search
	results := h.Search(query, 10)
	if len(results) == 0 {
		t.Fatal("expected results")
	}

	// Check recall: at least 7 of top-10 brute-force should be in HNSW results
	top10Set := make(map[int]bool)
	for _, r := range bf[:10] {
		top10Set[r.id] = true
	}
	hit := 0
	for _, r := range results {
		if top10Set[r.ID] {
			hit++
		}
	}
	if hit < 7 {
		t.Errorf("recall@10 = %d/10, expected >= 7", hit)
	}
}

func TestHNSW_Serialization(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig())
	for i := 0; i < 50; i++ {
		vec := make([]float32, 8)
		for j := range vec {
			vec[j] = float32(i*8+j) / 400.0
		}
		h.Add(i, vec)
	}

	data, err := h.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	h2, err := UnmarshalHNSW(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Same query should return same results
	query := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
	r1 := h.Search(query, 5)
	r2 := h2.Search(query, 5)
	if len(r1) != len(r2) {
		t.Fatalf("result count differs: %d vs %d", len(r1), len(r2))
	}
	for i := range r1 {
		if r1[i].ID != r2[i].ID {
			t.Errorf("result %d differs: %d vs %d", i, r1[i].ID, r2[i].ID)
		}
	}
}

// --- CFRecaller tests ---

func TestCFRecaller_Recall(t *testing.T) {
	recaller := NewCFRecaller()

	// Generate synthetic interactions
	var interactions []Interaction
	users := []string{"u1", "u2", "u3", "u4", "u5"}
	items := []string{"i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10"}
	for _, u := range users {
		for _, it := range items[:5] {
			interactions = append(interactions, Interaction{u, it})
		}
	}

	cfg := DefaultCFTrainConfig()
	cfg.BPRParams.NEpochs = 30
	cfg.TopK = 5
	recaller.Train(interactions, cfg)

	ctx := context.Background()
	candidates, err := recaller.Recall(ctx, "u1", 5)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("expected candidates")
	}
	for _, c := range candidates {
		if c.Source != "cf" {
			t.Errorf("expected source 'cf', got '%s'", c.Source)
		}
		if c.ItemID == "" {
			t.Error("empty item ID")
		}
	}
}

func TestCFRecaller_ColdStart(t *testing.T) {
	recaller := NewCFRecaller()
	recaller.Train([]Interaction{{"u1", "i1"}, {"u2", "i2"}}, DefaultCFTrainConfig())

	// Unknown user should return nil (cold start)
	candidates, err := recaller.Recall(context.Background(), "unknown", 5)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if candidates != nil {
		t.Errorf("expected nil for cold-start user, got %d candidates", len(candidates))
	}
}

func TestCFRecaller_NotReady(t *testing.T) {
	recaller := NewCFRecaller()
	_, err := recaller.Recall(context.Background(), "u1", 5)
	if err == nil {
		t.Error("expected error for untrained recaller")
	}
}

func TestCFRecaller_ContextCancel(t *testing.T) {
	recaller := NewCFRecaller()
	recaller.Train([]Interaction{{"u1", "i1"}}, DefaultCFTrainConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := recaller.Recall(ctx, "u1", 5)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestCFRecaller_Serialization(t *testing.T) {
	recaller := NewCFRecaller()
	interactions := []Interaction{
		{"u1", "i1"}, {"u1", "i2"}, {"u2", "i2"}, {"u2", "i3"},
	}
	recaller.Train(interactions, DefaultCFTrainConfig())

	data, err := recaller.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	recaller2, err := UnmarshalCFRecaller(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	c1, _ := recaller.Recall(context.Background(), "u1", 3)
	c2, _ := recaller2.Recall(context.Background(), "u1", 3)
	if len(c1) != len(c2) {
		t.Fatalf("candidate count differs: %d vs %d", len(c1), len(c2))
	}
	for i := range c1 {
		if c1[i].ItemID != c2[i].ItemID {
			t.Errorf("candidate %d differs: %s vs %s", i, c1[i].ItemID, c2[i].ItemID)
		}
	}
}

// --- Benchmark ---

func BenchmarkBPR_Train(b *testing.B) {
	params := DefaultBPRParams()
	params.NEpochs = 10

	interactions := make([]Interaction, 1000)
	for i := range interactions {
		interactions[i] = Interaction{
			UserID: "u" + string(rune('0'+i%10)),
			ItemID: "i" + string(rune('0'+i%20)),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bpr := NewBPR(params)
		bpr.Fit(interactions)
	}
}

func BenchmarkHNSW_Search(b *testing.B) {
	h := NewHNSW(HNSWConfig{EfConstruction: 100, M: 16})
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = float32(i*16+j) / 16000.0
		}
		h.Add(i, vec)
	}
	query := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Search(query, 50)
	}
}
