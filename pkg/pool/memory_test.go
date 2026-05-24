package pool

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type pooledRequestState struct {
	SessionID string
	Payload   []byte
	Scores    []float32
	Meta      map[string]string
}

func resetPooledRequestState(s *pooledRequestState) {
	s.SessionID = ""
	for i := range s.Payload {
		s.Payload[i] = 0
	}
	s.Payload = nil
	for i := range s.Scores {
		s.Scores[i] = 0
	}
	s.Scores = nil
	for k := range s.Meta {
		delete(s.Meta, k)
	}
	s.Meta = nil
}

func TestMemoryPoolWithResetsAndPreventsCrossRequestPollution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "object is thoroughly reset after reuse",
			run: func(t *testing.T) {
				mp := NewMemoryPool(resetPooledRequestState)

				err := mp.With(context.Background(), func(s *pooledRequestState) error {
					s.SessionID = "session-a"
					s.Payload = append(s.Payload, []byte("secret-payload")...)
					s.Scores = append(s.Scores, 1.5, 2.5)
					s.Meta = map[string]string{"token": "secret-token"}
					return nil
				})
				if err != nil {
					t.Fatalf("first With returned error: %v", err)
				}

				err = mp.With(context.Background(), func(s *pooledRequestState) error {
					if s.SessionID != "" {
						t.Fatalf("SessionID leaked across requests: %q", s.SessionID)
					}
					if len(s.Payload) != 0 || cap(s.Payload) != 0 {
						t.Fatalf("Payload was not released: len=%d cap=%d", len(s.Payload), cap(s.Payload))
					}
					if len(s.Scores) != 0 || cap(s.Scores) != 0 {
						t.Fatalf("Scores were not released: len=%d cap=%d", len(s.Scores), cap(s.Scores))
					}
					if s.Meta != nil {
						t.Fatalf("Meta leaked across requests: %#v", s.Meta)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("second With returned error: %v", err)
				}
			},
		},
		{
			name: "empty input leaves zero state",
			run: func(t *testing.T) {
				mp := NewMemoryPool(resetPooledRequestState)
				if err := mp.With(context.Background(), func(s *pooledRequestState) error {
					if s.SessionID != "" || len(s.Payload) != 0 || len(s.Scores) != 0 || s.Meta != nil {
						t.Fatalf("fresh state is not zero: %#v", s)
					}
					return nil
				}); err != nil {
					t.Fatalf("With returned error: %v", err)
				}
			},
		},
		{
			name: "context cancellation rejects before acquisition",
			run: func(t *testing.T) {
				mp := NewMemoryPool(resetPooledRequestState)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				called := false
				err := mp.With(ctx, func(s *pooledRequestState) error {
					called = true
					return nil
				})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context canceled error, got %v", err)
				}
				if called {
					t.Fatal("callback ran after context was canceled")
				}
			},
		},
		{
			name: "expired deadline rejects before acquisition",
			run: func(t *testing.T) {
				mp := NewMemoryPool(resetPooledRequestState)
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()

				called := false
				err := mp.With(ctx, func(s *pooledRequestState) error {
					called = true
					return nil
				})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected context deadline error, got %v", err)
				}
				if called {
					t.Fatal("callback ran after context deadline")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestByteBufferPoolClearsSensitiveDataAndHandlesLongInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "sensitive bytes are zeroed before reuse",
			run: func(t *testing.T) {
				bp := NewByteBufferPool(8, 1<<20)
				secret := []byte("top-secret-token")

				if err := bp.With(context.Background(), len(secret), func(buf []byte) error {
					buf = append(buf, secret...)
					return nil
				}); err != nil {
					t.Fatalf("first With returned error: %v", err)
				}

				if err := bp.With(context.Background(), len(secret), func(buf []byte) error {
					if len(buf) != 0 {
						t.Fatalf("buffer len was not reset: %d", len(buf))
					}
					full := buf[:cap(buf)]
					if bytes.Contains(full, secret) {
						t.Fatalf("sensitive bytes leaked in pooled capacity: %q", full)
					}
					for i, b := range full[:len(secret)] {
						if b != 0 {
							t.Fatalf("buffer byte %d was not zeroed: %d", i, b)
						}
					}
					return nil
				}); err != nil {
					t.Fatalf("second With returned error: %v", err)
				}
			},
		},
		{
			name: "overlong malicious input is not retained",
			run: func(t *testing.T) {
				bp := NewByteBufferPool(16, 64)
				if err := bp.With(context.Background(), 1024, func(buf []byte) error {
					if cap(buf) < 1024 {
						t.Fatalf("expected buffer for overlong input, cap=%d", cap(buf))
					}
					buf = append(buf, bytes.Repeat([]byte{'x'}, 1024)...)
					return nil
				}); err != nil {
					t.Fatalf("first With returned error: %v", err)
				}

				if err := bp.With(context.Background(), 1, func(buf []byte) error {
					if cap(buf) > 64 {
						t.Fatalf("oversized buffer retained in pool: cap=%d", cap(buf))
					}
					return nil
				}); err != nil {
					t.Fatalf("second With returned error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestMemoryPoolsHandleConcurrentGetPutSurge(t *testing.T) {
	t.Parallel()

	mp := NewMemoryPool(resetPooledRequestState)
	bp := NewByteBufferPool(32, 4096)
	const goroutines = 128
	const iterations = 256

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := mp.With(context.Background(), func(s *pooledRequestState) error {
					s.SessionID = "surge"
					s.Payload = append(s.Payload, byte(id), byte(i))
					return nil
				}); err != nil {
					t.Errorf("MemoryPool.With failed: %v", err)
					return
				}
				if err := bp.With(context.Background(), 64, func(buf []byte) error {
					buf = append(buf, byte(id), byte(i))
					return nil
				}); err != nil {
					t.Errorf("ByteBufferPool.With failed: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
