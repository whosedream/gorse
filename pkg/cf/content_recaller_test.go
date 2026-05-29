package cf

import (
	"context"
	"testing"
)

// --- helper: build a ContentRecaller with synthetic embeddings (no DuckDB) ---

func buildTestContentRecaller(dim int, itemCount int) *ContentRecaller {
	hnsw := NewHNSW(DefaultHNSWConfig())
	itemIDs := make([]string, itemCount)

	for i := 0; i < itemCount; i++ {
		vec := make([]float32, dim)
		// Each item gets a unique bias in its embedding so ANN can separate them.
		for j := range vec {
			vec[j] = float32(j*itemCount+i%itemCount) / float32(dim*itemCount)
		}
		id := "item_" + string(rune('A'+i%26)) + "_" + string(rune('0'+i/26))
		itemIDs[i] = id
		hnsw.Add(i, vec)
	}

	return NewContentRecallerFromIndex(hnsw, itemIDs, dim)
}

// --- Table-driven tests ---

func TestContentRecaller_Recall(t *testing.T) {
	dim := 32
	r := buildTestContentRecaller(dim, 100)

	// Set user embedding: identical to item 0 → item 0 should be the top hit.
	userVec := make([]float32, dim)
	for j := range userVec {
		userVec[j] = float32(j*100+0%100) / float32(dim*100)
	}
	r.SetUserEmbedding("user1", userVec)

	tests := []struct {
		name      string
		userID    string
		topK      int
		wantMin   int // minimum candidates expected
		wantFirst string
	}{
		{
			name:      "basic recall returns candidates",
			userID:    "user1",
			topK:      5,
			wantMin:   5,
			wantFirst: "item_A_0",
		},
		{
			name:    "topK larger than catalog returns all",
			userID:  "user1",
			topK:    200,
			wantMin: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			candidates, err := r.Recall(ctx, tt.userID, tt.topK)
			if err != nil {
				t.Fatalf("Recall() error = %v", err)
			}
			if len(candidates) < tt.wantMin {
				t.Fatalf("Recall() returned %d candidates, want >= %d", len(candidates), tt.wantMin)
			}
			if tt.wantFirst != "" && candidates[0].ItemID != tt.wantFirst {
				t.Errorf("first candidate = %s, want %s", candidates[0].ItemID, tt.wantFirst)
			}
			// All candidates should have source "content".
			for _, c := range candidates {
				if c.Source != "content" {
					t.Errorf("candidate %s source = %q, want %q", c.ItemID, c.Source, "content")
				}
			}
		})
	}
}

func TestContentRecaller_ColdStart(t *testing.T) {
	r := buildTestContentRecaller(16, 10)

	tests := []struct {
		name   string
		userID string
	}{
		{name: "unknown user returns nil", userID: "unknown_user"},
		{name: "empty string user returns nil", userID: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates, err := r.Recall(context.Background(), tt.userID, 5)
			if err != nil {
				t.Fatalf("Recall() error = %v", err)
			}
			if candidates != nil {
				t.Errorf("Recall() = %v, want nil for cold-start user", candidates)
			}
		})
	}
}

func TestContentRecaller_NotReady(t *testing.T) {
	// Construct a ContentRecaller without calling BuildIndex or NewContentRecallerFromIndex.
	r := &ContentRecaller{
		itemIDToIdx: make(map[string]int),
		userEmbeds:  make(map[string][]float32),
	}

	_, err := r.Recall(context.Background(), "u1", 5)
	if err == nil {
		t.Error("expected error for uninitialized recaller, got nil")
	}
}

func TestContentRecaller_ContextCancel(t *testing.T) {
	r := buildTestContentRecaller(16, 10)
	r.SetUserEmbedding("u1", make([]float32, 16))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.Recall(ctx, "u1", 5)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestContentRecaller_SetGetUserEmbedding(t *testing.T) {
	r := buildTestContentRecaller(8, 5)

	emb := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
	r.SetUserEmbedding("u1", emb)

	got := r.GetUserEmbedding("u1")
	if len(got) != len(emb) {
		t.Fatalf("GetUserEmbedding() dim = %d, want %d", len(got), len(emb))
	}
	for i := range emb {
		if got[i] != emb[i] {
			t.Errorf("GetUserEmbedding()[%d] = %f, want %f", i, got[i], emb[i])
		}
	}

	// Unknown user returns nil.
	if got := r.GetUserEmbedding("nobody"); got != nil {
		t.Errorf("GetUserEmbedding(nobody) = %v, want nil", got)
	}
}

func TestContentRecaller_ScoreDescending(t *testing.T) {
	dim := 16
	r := buildTestContentRecaller(dim, 50)

	// Query identical to item 10.
	query := make([]float32, dim)
	for j := range query {
		query[j] = float32(j*50+10%50) / float32(dim*50)
	}
	r.SetUserEmbedding("u1", query)

	candidates, err := r.Recall(context.Background(), "u1", 20)
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}

	// Verify scores are descending.
	for i := 1; i < len(candidates); i++ {
		if candidates[i].Score > candidates[i-1].Score {
			t.Errorf("scores not descending: candidates[%d].Score=%f > candidates[%d].Score=%f",
				i, candidates[i].Score, i-1, candidates[i-1].Score)
		}
	}
}

func TestContentRecaller_UpdateUserEmbedding(t *testing.T) {
	dim := 16
	r := buildTestContentRecaller(dim, 50)

	// First embedding → top hit is item 0.
	emb0 := make([]float32, dim)
	for j := range emb0 {
		emb0[j] = float32(j*50+0) / float32(dim*50)
	}
	r.SetUserEmbedding("u1", emb0)

	c1, _ := r.Recall(context.Background(), "u1", 3)
	if c1[0].ItemID != "item_A_0" {
		t.Fatalf("first recall: top item = %s, want item_A_0", c1[0].ItemID)
	}

	// Update embedding → top hit should change to item 5.
	emb5 := make([]float32, dim)
	for j := range emb5 {
		emb5[j] = float32(j*50+5) / float32(dim*50)
	}
	r.SetUserEmbedding("u1", emb5)

	c2, _ := r.Recall(context.Background(), "u1", 3)
	if c2[0].ItemID != "item_F_0" {
		t.Errorf("after update: top item = %s, want item_F_0", c2[0].ItemID)
	}
}

func TestContentRecaller_Size(t *testing.T) {
	r := buildTestContentRecaller(8, 42)
	if got := r.Size(); got != 42 {
		t.Errorf("Size() = %d, want 42", got)
	}
}

// --- Benchmark ---

func BenchmarkContentRecaller_Recall(b *testing.B) {
	dim := 64
	r := buildTestContentRecaller(dim, 1000)

	emb := make([]float32, dim)
	for j := range emb {
		emb[j] = float32(j) / float32(dim)
	}
	r.SetUserEmbedding("bench_user", emb)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Recall(ctx, "bench_user", 50)
	}
}
