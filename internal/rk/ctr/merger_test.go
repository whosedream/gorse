package ctr

import (
	"fmt"
	"testing"
)

func TestEqualWeightMergerMerge(t *testing.T) {
	t.Parallel()

	t.Run("dedup keeps highest score", func(t *testing.T) {
		t.Parallel()
		m := NewEqualWeightMerger(150)
		cf := []Candidate{{ItemID: "1", Score: 0.5, Source: "cf"}}
		content := []Candidate{{ItemID: "1", Score: 0.8, Source: "content"}}
		profile := []Candidate{{ItemID: "2", Score: 0.3, Source: "profile"}}

		result := m.Merge(cf, content, profile)
		if len(result) != 2 {
			t.Fatalf("len = %d, want 2", len(result))
		}
		// Item "1" should use the content score (0.8)
		if result[0].ItemID != "1" || result[0].Score != 0.8 {
			t.Errorf("result[0] = %+v, want ItemID=1 Score=0.8", result[0])
		}
		if result[1].ItemID != "2" {
			t.Errorf("result[1].ItemID = %v, want 2", result[1].ItemID)
		}
	})

	t.Run("caps at MaxCandidates", func(t *testing.T) {
		t.Parallel()
		m := NewEqualWeightMerger(3)
		candidates := make([][]Candidate, 3)
		for i := range candidates {
			candidates[i] = make([]Candidate, 2)
			for j := range candidates[i] {
				candidates[i][j] = Candidate{
					ItemID: fmt.Sprintf("item-%d-%d", i, j),
					Score:  float32(i*2+j) * 0.1,
					Source: "test",
				}
			}
		}
		result := m.Merge(candidates...)
		if len(result) != 3 {
			t.Fatalf("len = %d, want 3", len(result))
		}
		// Should be sorted by score descending
		for i := 0; i+1 < len(result); i++ {
			if result[i].Score < result[i+1].Score {
				t.Errorf("not sorted: [%d].Score=%v < [%d].Score=%v", i, result[i].Score, i+1, result[i+1].Score)
			}
		}
	})

	t.Run("empty input returns nil", func(t *testing.T) {
		t.Parallel()
		m := NewEqualWeightMerger(150)
		result := m.Merge()
		if result != nil {
			t.Errorf("Merge() = %v, want nil", result)
		}
	})

	t.Run("single source passes through", func(t *testing.T) {
		t.Parallel()
		m := NewEqualWeightMerger(150)
		input := []Candidate{
			{ItemID: "a", Score: 0.1, Source: "cf"},
			{ItemID: "b", Score: 0.9, Source: "cf"},
		}
		result := m.Merge(input)
		if len(result) != 2 {
			t.Fatalf("len = %d, want 2", len(result))
		}
		if result[0].ItemID != "b" || result[0].Score != 0.9 {
			t.Errorf("result[0] = %+v, want b/0.9", result[0])
		}
	})

	t.Run("default max candidates is 150", func(t *testing.T) {
		t.Parallel()
		m := NewEqualWeightMerger(0)
		if m.MaxCandidates != 150 {
			t.Errorf("MaxCandidates = %d, want 150", m.MaxCandidates)
		}
	})
}

func BenchmarkEqualWeightMergerMerge(b *testing.B) {
	// Simulate 3 sources x 50 candidates = 150 total
	sources := make([][]Candidate, 3)
	for s := range sources {
		sources[s] = make([]Candidate, 50)
		for i := range sources[s] {
			sources[s][i] = Candidate{
				ItemID: fmt.Sprintf("item-%d", i),
				Score:  float32(i) * 0.01,
				Source: []string{"cf", "content", "profile"}[s],
			}
		}
	}
	m := NewEqualWeightMerger(150)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Merge(sources[0], sources[1], sources[2])
	}
}
