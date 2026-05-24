package scorer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEngineRank(t *testing.T) {
	t.Parallel()

	t.Run("dot product ranking is descending", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 3, DiversityWindow: 2, MaxSameCategory: 2, MaxSameBrand: 2})
		candidates := []Candidate{
			{ID: 1, Category: "phone", Brand: "a", Feature: []float32{1, 0}},
			{ID: 2, Category: "phone", Brand: "b", Feature: []float32{0, 2}},
			{ID: 3, Category: "case", Brand: "c", Feature: []float32{2, 2}},
		}
		out := make([]Result, 3)
		n, err := e.Rank(context.Background(), []float32{2, 1}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 3 {
			t.Fatalf("n = %d, want 3", n)
		}
		wantIDs := []int64{3, 1, 2}
		for i, id := range wantIDs {
			if out[i].ID != id {
				t.Fatalf("out[%d].ID = %d, want %d; out=%+v", i, out[i].ID, id, out[:n])
			}
		}
	})

	t.Run("empty and short vectors use min dimension", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 3})
		candidates := []Candidate{
			{ID: 1, Category: "a", Brand: "x", Feature: nil},
			{ID: 2, Category: "b", Brand: "y", Feature: []float32{4}},
			{ID: 3, Category: "c", Brand: "z", Feature: []float32{1, 100}},
		}
		out := make([]Result, 3)
		n, err := e.Rank(context.Background(), []float32{2}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 3 || out[0].ID != 2 || out[0].Score != 8 || out[2].ID != 1 || out[2].Score != 0 {
			t.Fatalf("rank with short vectors = n=%d out=%+v", n, out[:n])
		}
	})

	t.Run("topk smaller than candidates", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 2})
		candidates := []Candidate{
			{ID: 1, Feature: []float32{1}},
			{ID: 2, Feature: []float32{3}},
			{ID: 3, Feature: []float32{2}},
		}
		out := make([]Result, 2)
		n, err := e.Rank(context.Background(), []float32{1}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 2 || out[0].ID != 2 || out[1].ID != 3 {
			t.Fatalf("topk out = n=%d %+v", n, out[:n])
		}
	})

	t.Run("diversity disperses category and brand in window", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 5, DiversityWindow: 3, MaxSameCategory: 1, MaxSameBrand: 1})
		candidates := []Candidate{
			{ID: 1, Category: "phone", Brand: "a", Feature: []float32{100}},
			{ID: 2, Category: "phone", Brand: "a", Feature: []float32{99}},
			{ID: 3, Category: "phone", Brand: "b", Feature: []float32{98}},
			{ID: 4, Category: "case", Brand: "c", Feature: []float32{10}},
			{ID: 5, Category: "watch", Brand: "d", Feature: []float32{9}},
		}
		out := make([]Result, 5)
		n, err := e.Rank(context.Background(), []float32{1}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 5 {
			t.Fatalf("n = %d, want 5", n)
		}
		for i := 0; i+1 < 3 && i+1 < n; i++ {
			if out[i].Category == out[i+1].Category || out[i].Brand == out[i+1].Brand {
				t.Fatalf("diversity failed in first window: %+v", out[:n])
			}
		}
	})

	t.Run("diversity can pull candidates beyond topk cutoff", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 2, DiversityWindow: 2, MaxSameCategory: 1, MaxSameBrand: 2, MaxCandidates: 3})
		candidates := []Candidate{
			{ID: 1, Category: "A", Brand: "x", Feature: []float32{100}},
			{ID: 2, Category: "A", Brand: "y", Feature: []float32{99}},
			{ID: 3, Category: "B", Brand: "z", Feature: []float32{98}},
		}
		out := make([]Result, 2)
		n, err := e.Rank(context.Background(), []float32{1}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 2 || out[0].ID != 1 || out[1].ID != 3 {
			t.Fatalf("diversity cutoff result = n=%d out=%+v, want [1,3]", n, out[:n])
		}
	})

	t.Run("too many candidates returns ErrTooManyCandidates", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 2, MaxCandidates: 2})
		_, err := e.Rank(context.Background(), []float32{1}, []Candidate{{ID: 1}, {ID: 2}, {ID: 3}}, make([]Result, 2))
		if !errors.Is(err, ErrTooManyCandidates) {
			t.Fatalf("Rank() error = %v, want ErrTooManyCandidates", err)
		}
	})

	t.Run("deadline during selection returns context error", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 4096, MaxCandidates: 8192})
		candidates := make([]Candidate, 8192)
		for i := range candidates {
			candidates[i] = Candidate{ID: int64(i + 1), Feature: []float32{float32(i)}}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Microsecond)
		defer cancel()
		_, err := e.Rank(ctx, []float32{1}, candidates, make([]Result, 4096))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Rank() error = %v, want context deadline", err)
		}
	})

	t.Run("extreme same category brand never loops", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 4, DiversityWindow: 4, MaxSameCategory: 1, MaxSameBrand: 1})
		candidates := []Candidate{
			{ID: 1, Category: "same", Brand: "same", Feature: []float32{4}},
			{ID: 2, Category: "same", Brand: "same", Feature: []float32{3}},
			{ID: 3, Category: "same", Brand: "same", Feature: []float32{2}},
			{ID: 4, Category: "same", Brand: "same", Feature: []float32{1}},
		}
		out := make([]Result, 4)
		n, err := e.Rank(context.Background(), []float32{1}, candidates, out)
		if err != nil {
			t.Fatalf("Rank() error = %v", err)
		}
		if n != 4 || out[0].ID != 1 || out[3].ID != 4 {
			t.Fatalf("same-category rank changed unexpectedly: n=%d out=%+v", n, out[:n])
		}
	})

	t.Run("out too small returns ErrOutputTooSmall", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 2})
		_, err := e.Rank(context.Background(), []float32{1}, []Candidate{{ID: 1, Feature: []float32{1}}, {ID: 2, Feature: []float32{2}}}, make([]Result, 1))
		if !errors.Is(err, ErrOutputTooSmall) {
			t.Fatalf("Rank() error = %v, want ErrOutputTooSmall", err)
		}
	})

	t.Run("pre canceled context fails", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 1})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := e.Rank(ctx, []float32{1}, []Candidate{{ID: 1, Feature: []float32{1}}}, make([]Result, 1))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Rank() error = %v, want context.Canceled", err)
		}
	})

	t.Run("multiple calls have no cross request pollution", func(t *testing.T) {
		t.Parallel()
		e := NewEngine(Options{TopK: 2, DiversityWindow: 2, MaxSameCategory: 1, MaxSameBrand: 1})
		out := make([]Result, 2)
		first := []Candidate{{ID: 1, Category: "x", Brand: "x", Feature: []float32{10}}, {ID: 2, Category: "y", Brand: "y", Feature: []float32{1}}}
		if _, err := e.Rank(context.Background(), []float32{1}, first, out); err != nil {
			t.Fatalf("first Rank() error = %v", err)
		}
		second := []Candidate{{ID: 9, Category: "z", Brand: "z", Feature: []float32{1}}}
		n, err := e.Rank(context.Background(), []float32{1}, second, out)
		if err != nil {
			t.Fatalf("second Rank() error = %v", err)
		}
		if n != 1 || out[0].ID != 9 {
			t.Fatalf("second out polluted: n=%d out=%+v", n, out[:n])
		}
	})
}

func TestEngineRankHighConcurrencySurge(t *testing.T) {
	e := NewEngine(Options{TopK: 8, DiversityWindow: 4, MaxSameCategory: 2, MaxSameBrand: 2})
	candidates := make([]Candidate, 64)
	for i := range candidates {
		candidates[i] = Candidate{ID: int64(i + 1), Category: "cat", Brand: "brand", Feature: []float32{float32(i), 1}}
	}
	errCh := make(chan error, 64)
	for g := 0; g < cap(errCh); g++ {
		go func() {
			out := make([]Result, 8)
			_, err := e.Rank(context.Background(), []float32{1, 1}, candidates, out)
			errCh <- err
		}()
	}
	for i := 0; i < cap(errCh); i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent Rank error = %v", err)
		}
	}
}
