package ctr

import (
	"context"
	"math"
	"testing"
)

func TestSigmoid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		x    float32
		want float32
	}{
		{0, 0.5},
		{100, 1.0},   // saturates
		{-100, 0.0},  // saturates
		{1, 0.7310586}, // approx
		{-1, 0.2689414},
	}
	for _, tt := range tests {
		got := sigmoid(tt.x)
		if math.Abs(float64(got-tt.want)) > 1e-5 {
			t.Errorf("sigmoid(%v) = %v, want %v", tt.x, got, tt.want)
		}
	}
}

func TestNewAFM(t *testing.T) {
	t.Parallel()
	cfg := DefaultAFMConfig()
	m := NewAFM(cfg)
	if m == nil {
		t.Fatal("NewAFM returned nil")
	}
	if len(m.linear) != cfg.NumFeatures {
		t.Fatalf("linear len = %d, want %d", len(m.linear), cfg.NumFeatures)
	}
	if len(m.factors) != cfg.NumFeatures {
		t.Fatalf("factors rows = %d, want %d", len(m.factors), cfg.NumFeatures)
	}
	for i, row := range m.factors {
		if len(row) != cfg.NumFactors {
			t.Fatalf("factors[%d] len = %d, want %d", i, len(row), cfg.NumFactors)
		}
	}
}

func TestAFMPredictOutputRange(t *testing.T) {
	t.Parallel()
	cfg := DefaultAFMConfig()
	m := NewAFM(cfg)

	features := []float32{1, 0.5, 0, 0.3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	p := m.Predict(features)
	if p < 0 || p > 1 {
		t.Fatalf("Predict() = %v, want in [0,1]", p)
	}
}

func TestAFMTrainBatchReducesLoss(t *testing.T) {
	t.Parallel()
	cfg := AFMConfig{
		NumFeatures:  16,
		NumFactors:   4,
		LearningRate: 0.01,
		L2Reg:        1e-6,
		AdamEps:      1e-8,
	}
	m := NewAFM(cfg)

	// Generate synthetic data: positive features -> label 1, negative -> label 0
	dim := cfg.NumFeatures
	batchSize := 32
	batch := make([][]float32, batchSize)
	labels := make([]float32, batchSize)
	rng := newRng(123)
	for i := 0; i < batchSize; i++ {
		f := make([]float32, dim)
		sum := float32(0)
		for k := 0; k < dim; k++ {
			f[k] = rng.Float32()*2 - 1
			sum += f[k]
		}
		batch[i] = f
		if sum > 0 {
			labels[i] = 1
		}
	}

	// Train for several epochs and verify loss decreases
	var prevLoss float32 = math.MaxFloat32
	for epoch := 0; epoch < 20; epoch++ {
		loss := m.TrainBatch(batch, labels)
		if loss >= prevLoss && epoch > 5 {
			t.Errorf("epoch %d: loss %v not decreasing (prev %v)", epoch, loss, prevLoss)
		}
		prevLoss = loss
	}

	// After training, positive samples should have higher predictions
	correct := 0
	for i := 0; i < batchSize; i++ {
		p := m.Predict(batch[i])
		if (p > 0.5) == (labels[i] == 1) {
			correct++
		}
	}
	acc := float32(correct) / float32(batchSize)
	if acc < 0.6 {
		t.Errorf("accuracy = %v, want >= 0.6", acc)
	}
}

func TestAFMTrainBatchEmpty(t *testing.T) {
	t.Parallel()
	m := NewAFM(DefaultAFMConfig())
	loss := m.TrainBatch(nil, nil)
	if loss != 0 {
		t.Errorf("TrainBatch(nil) = %v, want 0", loss)
	}
}

func TestAFMRanker(t *testing.T) {
	t.Parallel()
	cfg := AFMConfig{NumFeatures: 8, NumFactors: 2}
	m := NewAFM(cfg)
	ranker := NewAFMRanker(m)

	candidates := []Candidate{
		{ItemID: "a", Score: 0.9, Source: "cf", Features: []float32{1, 0, 0, 0, 0, 0, 0, 0}},
		{ItemID: "b", Score: 0.5, Source: "content", Features: []float32{0, 1, 0, 0, 0, 0, 0, 0}},
		{ItemID: "c", Score: 0.3, Source: "profile", Features: []float32{0, 0, 1, 0, 0, 0, 0, 0}},
	}

	results, err := ranker.Rank(context.Background(), candidates, 2)
	if err != nil {
		t.Fatalf("Rank() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Rank() returned %d results, want 2", len(results))
	}
	// All results should have CTR scores
	for i, r := range results {
		if r.CTRScore < 0 || r.CTRScore > 1 {
			t.Errorf("results[%d].CTRScore = %v, want in [0,1]", i, r.CTRScore)
		}
	}
}

func TestAFMRankerEmpty(t *testing.T) {
	t.Parallel()
	ranker := NewAFMRanker(NewAFM(DefaultAFMConfig()))
	results, err := ranker.Rank(nil, nil, 10)
	if err != nil {
		t.Fatalf("Rank() error = %v", err)
	}
	if results != nil {
		t.Fatalf("Rank(nil) = %v, want nil", results)
	}
}

// newRng creates a deterministic pseudo-random source for tests.
func newRng(seed int64) *simpleRng {
	return &simpleRng{s: uint64(seed)}
}

type simpleRng struct {
	s uint64
}

func (r *simpleRng) Float32() float32 {
	r.s ^= r.s << 13
	r.s ^= r.s >> 7
	r.s ^= r.s << 17
	return float32(r.s&0xFFFFFF) / float32(0xFFFFFF)
}
