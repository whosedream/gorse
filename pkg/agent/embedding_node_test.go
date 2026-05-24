package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeEmbedder struct {
	text string
	vec  []float32
	err  error
}

func (f *fakeEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	f.text = text
	if f.err != nil {
		return nil, f.err
	}
	return f.vec, nil
}

type captureLogger struct{ lines []string }

func (l *captureLogger) Printf(format string, args ...any) {
	l.lines = append(l.lines, format)
}

func TestEmbeddingNode(t *testing.T) {
	t.Parallel()

	t.Run("extracts LLM JSON builds text calls embedder and writes vector", func(t *testing.T) {
		t.Parallel()
		vec := make([]float32, 1024)
		for i := range vec {
			vec[i] = float32(i)
		}
		fe := &fakeEmbedder{vec: vec}
		n := NewEmbeddingNode(EmbeddingNodeOptions{ID: "embed", Deps: []string{"llm"}, Embedder: fe})
		st := &State{LLMOutput: `{"session_id":"s123","baseline_version":88,"category_weights":{"phone":0.91,"case":0.12},"intent_vector":[]}`}
		if err := n.Run(context.Background(), st); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if len(st.IntentVector) != 1024 || st.IntentVector[17] != 17 {
			t.Fatalf("vector not written len=%d v17=%v", len(st.IntentVector), st.IntentVector[17])
		}
		for _, want := range []string{"s123", "88", "phone", "case"} {
			if !strings.Contains(fe.text, want) {
				t.Fatalf("embedding text %q missing %q", fe.text, want)
			}
		}
		if n.ID() != "embed" || n.Kind() != NodeSymbol || len(n.Deps()) != 1 || n.Deps()[0] != "llm" {
			t.Fatalf("node metadata invalid id=%s kind=%v deps=%v", n.ID(), n.Kind(), n.Deps())
		}
	})

	t.Run("embedder failure falls back to SimulateEmbedding logs and returns nil", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("remote down")
		lg := &captureLogger{}
		n := NewEmbeddingNode(EmbeddingNodeOptions{Embedder: &fakeEmbedder{err: boom}, Logger: lg})
		st := &State{SessionID: "s-fallback", BaselineVersion: 9, LLMOutput: `{"session_id":"s-fallback","baseline_version":9,"category_weights":{"tv":1}}`}
		if err := n.Run(context.Background(), st); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if len(st.IntentVector) != 1024 {
			t.Fatalf("fallback len=%d, want 1024", len(st.IntentVector))
		}
		if len(lg.lines) == 0 {
			t.Fatal("expected fallback log")
		}
	})

	t.Run("canceled context returns ctx err and does not fallback", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		n := NewEmbeddingNode(EmbeddingNodeOptions{Embedder: &fakeEmbedder{err: errors.New("should not matter")}})
		st := &State{LLMOutput: `{"session_id":"s","baseline_version":1,"category_weights":{"x":1}}`}
		err := n.Run(ctx, st)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if len(st.IntentVector) != 0 {
			t.Fatalf("canceled context should not fallback len=%d", len(st.IntentVector))
		}
	})

	t.Run("empty data still builds fallback-safe text", func(t *testing.T) {
		t.Parallel()
		vec := make([]float32, 1024)
		fe := &fakeEmbedder{vec: vec}
		n := NewEmbeddingNode(EmbeddingNodeOptions{Embedder: fe})
		if err := n.Run(context.Background(), &State{}); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if fe.text == "" {
			t.Fatal("empty state produced empty embedding text")
		}
	})

	t.Run("malicious long LLM output is bounded in embedding text", func(t *testing.T) {
		t.Parallel()
		vec := make([]float32, 1024)
		fe := &fakeEmbedder{vec: vec}
		long := strings.Repeat("x", 16384)
		n := NewEmbeddingNode(EmbeddingNodeOptions{Embedder: fe})
		st := &State{SessionID: "s", LLMOutput: long + `{"session_id":"s","baseline_version":2,"category_weights":{"phone":1}}`}
		if err := n.Run(context.Background(), st); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if len(fe.text) > 4096 {
			t.Fatalf("embedding text too long: %d", len(fe.text))
		}
	})
}
