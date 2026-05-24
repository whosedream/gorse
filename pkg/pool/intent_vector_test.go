package pool

import (
	"context"
	"errors"
	"testing"
)

func TestIntentVectorPool(t *testing.T) {
	t.Parallel()

	t.Run("borrowed slice length is 1024", func(t *testing.T) {
		t.Parallel()
		p := NewIntentVectorPool()
		if err := p.With(context.Background(), func(v []float32) error {
			if len(v) != 1024 {
				t.Fatalf("len = %d, want 1024", len(v))
			}
			if cap(v) != 1024 {
				t.Fatalf("cap = %d, want 1024", cap(v))
			}
			return nil
		}); err != nil {
			t.Fatalf("With error = %v", err)
		}
	})

	t.Run("data is zeroed between borrows", func(t *testing.T) {
		t.Parallel()
		p := NewIntentVectorPool()
		if err := p.With(context.Background(), func(v []float32) error {
			for i := range v {
				v[i] = float32(i + 1)
			}
			return nil
		}); err != nil {
			t.Fatalf("first With error = %v", err)
		}
		if err := p.With(context.Background(), func(v []float32) error {
			for i := range v {
				if v[i] != 0 {
					t.Fatalf("v[%d] = %v, want 0", i, v[i])
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("second With error = %v", err)
		}
	})

	t.Run("pre-canceled context returns context.Canceled", func(t *testing.T) {
		t.Parallel()
		p := NewIntentVectorPool()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := p.With(ctx, func([]float32) error {
			t.Fatal("callback must not run after context cancel")
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("With error = %v, want context.Canceled", err)
		}
	})
}
