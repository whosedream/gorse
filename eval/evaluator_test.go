package eval

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"go-rec/pkg/cf"
)

func TestNDCG(t *testing.T) {
	tests := []struct {
		name     string
		relevance map[string]struct{}
		ranked   []string
		k        int
		want     float64
	}{
		{
			name:      "perfect ranking",
			relevance: map[string]struct{}{"a": {}, "b": {}},
			ranked:    []string{"a", "b", "c", "d"},
			k:         4,
			want:      1.0,
		},
		{
			name:      "worst ranking",
			relevance: map[string]struct{}{"a": {}, "b": {}},
			ranked:    []string{"c", "d", "a", "b"},
			k:         4,
			want:      (1.0/math.Log2(3+1) + 1.0/math.Log2(4+1)) / (1.0/math.Log2(1+1) + 1.0/math.Log2(2+1)), // DCG/IDCG
		},
		{
			name:      "empty ranked",
			relevance: map[string]struct{}{"a": {}},
			ranked:    []string{},
			k:         10,
			want:      0,
		},
		{
			name:      "no relevant in top-k",
			relevance: map[string]struct{}{"z": {}},
			ranked:    []string{"a", "b", "c"},
			k:         3,
			want:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NDCG(tt.relevance, tt.ranked, tt.k)
			if math.Abs(got-tt.want) > 1e-10 {
				t.Errorf("NDCG() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHR(t *testing.T) {
	tests := []struct {
		name      string
		relevance map[string]struct{}
		ranked    []string
		k         int
		want      float64
	}{
		{
			name:      "hit in top-1",
			relevance: map[string]struct{}{"a": {}},
			ranked:    []string{"a", "b", "c"},
			k:         1,
			want:      1.0,
		},
		{
			name:      "no hit",
			relevance: map[string]struct{}{"z": {}},
			ranked:    []string{"a", "b", "c"},
			k:         3,
			want:      0.0,
		},
		{
			name:      "hit at position 3",
			relevance: map[string]struct{}{"c": {}},
			ranked:    []string{"a", "b", "c"},
			k:         3,
			want:      1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HR(tt.relevance, tt.ranked, tt.k)
			if got != tt.want {
				t.Errorf("HR() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPairedTTest(t *testing.T) {
	// a is consistently better than b (larger sample, clearer difference)
	a := []float64{0.5, 0.55, 0.6, 0.58, 0.62, 0.53, 0.57, 0.59, 0.61, 0.56}
	b := []float64{0.2, 0.25, 0.22, 0.28, 0.26, 0.23, 0.27, 0.24, 0.21, 0.29}

	_, sig := PairedTTest(a, b, 0.05)
	if !sig {
		t.Error("expected significant difference, got none")
	}

	// Same values -> no significance
	c := []float64{0.3, 0.35, 0.4}
	d := []float64{0.3, 0.35, 0.4}
	_, sig2 := PairedTTest(c, d, 0.05)
	if sig2 {
		t.Error("expected no significance for identical values")
	}
}

func TestLoadCSV(t *testing.T) {
	// Create a temporary CSV file
	content := `reviewerID,asin,category,unixReviewTime
user1,itemA,Electronics,1609459200
user1,itemB,Electronics,1609545600
user2,itemA,Electronics,1609632000
user2,itemC,Electronics,1609718400
user3,itemB,Electronics,1609804800
user3,itemC,Electronics,1609891200
user3,itemD,Electronics,1609977600
`

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadCSV(csvPath, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Interactions) != 7 {
		t.Errorf("expected 7 interactions, got %d", len(result.Interactions))
	}
	if len(result.UserIDs) != 3 {
		t.Errorf("expected 3 users, got %d", len(result.UserIDs))
	}
	if len(result.ItemIDs) != 4 {
		t.Errorf("expected 4 items, got %d", len(result.ItemIDs))
	}
}

func TestSplitTrainTest(t *testing.T) {
	data := &LoadResult{
		Interactions: []cf.Interaction{
			{UserID: "u1", ItemID: "i1"},
			{UserID: "u1", ItemID: "i2"},
			{UserID: "u1", ItemID: "i3"},
			{UserID: "u1", ItemID: "i4"},
			{UserID: "u1", ItemID: "i5"},
			{UserID: "u2", ItemID: "i1"},
			{UserID: "u2", ItemID: "i2"},
			{UserID: "u2", ItemID: "i3"},
		},
		UserItems: map[string]map[string]struct{}{
			"u1": {"i1": {}, "i2": {}, "i3": {}, "i4": {}, "i5": {}},
			"u2": {"i1": {}, "i2": {}, "i3": {}},
		},
		ItemUsers: map[string]map[string]struct{}{
			"i1": {"u1": {}, "u2": {}},
			"i2": {"u1": {}, "u2": {}},
			"i3": {"u1": {}, "u2": {}},
			"i4": {"u1": {}},
			"i5": {"u1": {}},
		},
		UserIDs: []string{"u1", "u2"},
		ItemIDs: []string{"i1", "i2", "i3", "i4", "i5"},
	}

	train, test := SplitTrainTest(data, 0, 0.8)

	// Each user should have some train and some test
	trainUserItems := make(map[string]int)
	for _, it := range train {
		trainUserItems[it.UserID]++
	}
	testUserItems := make(map[string]int)
	for _, it := range test {
		testUserItems[it.UserID]++
	}

	// u1: 5 interactions -> 4 train, 1 test
	if trainUserItems["u1"] != 4 {
		t.Errorf("u1 train: expected 4, got %d", trainUserItems["u1"])
	}
	if testUserItems["u1"] != 1 {
		t.Errorf("u1 test: expected 1, got %d", testUserItems["u1"])
	}

	// u2: 3 interactions -> 2 train, 1 test
	if trainUserItems["u2"] != 2 {
		t.Errorf("u2 train: expected 2, got %d", trainUserItems["u2"])
	}
	if testUserItems["u2"] != 1 {
		t.Errorf("u2 test: expected 1, got %d", testUserItems["u2"])
	}

	// Total should equal original
	total := len(train) + len(test)
	if total != len(data.Interactions) {
		t.Errorf("total %d != original %d", total, len(data.Interactions))
	}
}

func TestFilterMinInteractions(t *testing.T) {
	data := &LoadResult{
		Interactions: []cf.Interaction{
			{UserID: "u1", ItemID: "i1"},
			{UserID: "u1", ItemID: "i2"},
			{UserID: "u1", ItemID: "i3"},
			{UserID: "u2", ItemID: "i1"},
		},
		UserItems: map[string]map[string]struct{}{
			"u1": {"i1": {}, "i2": {}, "i3": {}},
			"u2": {"i1": {}},
		},
		ItemUsers: map[string]map[string]struct{}{
			"i1": {"u1": {}, "u2": {}},
			"i2": {"u1": {}},
			"i3": {"u1": {}},
		},
		UserIDs: []string{"u1", "u2"},
		ItemIDs: []string{"i1", "i2", "i3"},
	}

	filtered := FilterMinInteractions(data, 2)

	if len(filtered.Interactions) != 3 {
		t.Errorf("expected 3 interactions after filtering, got %d", len(filtered.Interactions))
	}
	if len(filtered.UserIDs) != 1 {
		t.Errorf("expected 1 user after filtering, got %d", len(filtered.UserIDs))
	}
}

func TestAverageMetrics(t *testing.T) {
	results := []Metrics{
		{NDCG10: 0.3, HR20: 0.6},
		{NDCG10: 0.4, HR20: 0.8},
		{NDCG10: 0.5, HR20: 0.7},
	}

	avg := AverageMetrics(results)

	if math.Abs(avg.NDCG10-0.4) > 1e-10 {
		t.Errorf("expected NDCG10=0.4, got %v", avg.NDCG10)
	}
	if math.Abs(avg.HR20-0.7) > 1e-10 {
		t.Errorf("expected HR20=0.7, got %v", avg.HR20)
	}
}
