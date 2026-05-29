package ctr

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkAFMPredict(b *testing.B) {
	cfg := DefaultAFMConfig()
	m := NewAFM(cfg)
	features := make([]float32, cfg.NumFeatures)
	for i := range features {
		features[i] = float32(i) / float32(cfg.NumFeatures)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Predict(features)
	}
}

func BenchmarkAFMTrainBatch(b *testing.B) {
	cfg := AFMConfig{NumFeatures: 64, NumFactors: 8, LearningRate: 0.001}
	m := NewAFM(cfg)
	batch := make([][]float32, 32)
	labels := make([]float32, 32)
	for i := range batch {
		f := make([]float32, cfg.NumFeatures)
		for k := range f {
			f[k] = float32(k%2) * 0.5
		}
		batch[i] = f
		labels[i] = float32(i % 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.TrainBatch(batch, labels)
	}
}

func BenchmarkAFMRankerRank(b *testing.B) {
	cfg := AFMConfig{NumFeatures: 32, NumFactors: 8}
	m := NewAFM(cfg)
	ranker := NewAFMRanker(m)
	candidates := make([]Candidate, 150)
	for i := range candidates {
		features := make([]float32, cfg.NumFeatures)
		for k := range features {
			features[k] = float32(i*k%7) * 0.1
		}
		candidates[i] = Candidate{
			ItemID:   fmt.Sprintf("item-%d", i),
			Score:    float32(i) * 0.005,
			Source:   "cf",
			Features: features,
		}
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ranker.Rank(ctx, candidates, 20)
	}
}
