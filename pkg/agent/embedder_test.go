package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteEmbedderOptionsFromEnv(t *testing.T) {
	t.Run("reads generic embedding environment", func(t *testing.T) {
		t.Setenv("EMBEDDING_BASE_URL", "https://generic.example.invalid/v1/embeddings")
		t.Setenv("EMBEDDING_API_KEY", "generic_placeholder_key")
		t.Setenv("EMBEDDING_MODEL", "BAAI/bge-m3")
		t.Setenv("SILICONFLOW_BASE_URL", "https://legacy.example.invalid/v1/embeddings")
		t.Setenv("SILICONFLOW_API_KEY", "legacy_placeholder_key")
		t.Setenv("SILICONFLOW_EMBEDDING_MODEL", "legacy-model")

		opts := RemoteEmbedderOptionsFromEnv()
		if opts.BaseURL != "https://generic.example.invalid/v1/embeddings" || opts.APIKey != "generic_placeholder_key" || opts.Model != "BAAI/bge-m3" {
			t.Fatalf("generic env options mismatch: %+v", opts)
		}
	})

	for _, tc := range []struct {
		name        string
		baseURL     string
		apiKey      string
		model       string
		wantBaseURL string
		wantAPIKey  string
		wantModel   string
	}{
		{
			name:        "falls back to legacy siliconflow environment",
			baseURL:     "https://legacy.example.invalid/v1/embeddings",
			apiKey:      "legacy_placeholder_key",
			model:       "BAAI/bge-m3",
			wantBaseURL: "https://legacy.example.invalid/v1/embeddings",
			wantAPIKey:  "legacy_placeholder_key",
			wantModel:   "BAAI/bge-m3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SILICONFLOW_BASE_URL", tc.baseURL)
			t.Setenv("SILICONFLOW_API_KEY", tc.apiKey)
			t.Setenv("SILICONFLOW_EMBEDDING_MODEL", tc.model)

			opts := RemoteEmbedderOptionsFromEnv()
			if opts.BaseURL != tc.wantBaseURL || opts.APIKey != tc.wantAPIKey || opts.Model != tc.wantModel {
				t.Fatalf("legacy env options mismatch: %+v", opts)
			}
		})
	}
}

func TestRemoteEmbedder(t *testing.T) {
	t.Parallel()

	t.Run("happy path posts model input and authorization then returns 1024 dims", func(t *testing.T) {
		t.Parallel()
		var gotAuth string
		var gotReq struct {
			Model string `json:"model"`
			Input string `json:"input"`
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			writeEmbeddingResponse(t, w, 1024)
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "test_key_placeholder", Model: "BAAI/bge-m3", ExpectedDim: 1024, MaxRetries: 0})
		vec, err := emb.Embed(context.Background(), "用户想买轻薄手机")
		if err != nil {
			t.Fatalf("Embed returned error: %v", err)
		}
		if len(vec) != 1024 {
			t.Fatalf("vector len=%d, want 1024", len(vec))
		}
		if gotReq.Model != "BAAI/bge-m3" || gotReq.Input != "用户想买轻薄手机" {
			t.Fatalf("unexpected request body: %+v", gotReq)
		}
		if gotAuth != "Bearer test_key_placeholder" {
			t.Fatalf("authorization not set: %q", gotAuth)
		}
	})

	t.Run("dimension mismatch returns ErrEmbeddingDimension", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeEmbeddingResponse(t, w, 3)
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024})
		_, err := emb.Embed(context.Background(), "x")
		if !errors.Is(err, ErrEmbeddingDimension) {
			t.Fatalf("expected ErrEmbeddingDimension, got %v", err)
		}
	})

	t.Run("429 retries once then succeeds", func(t *testing.T) {
		t.Parallel()
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt32(&calls, 1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limited"}`))
				return
			}
			writeEmbeddingResponse(t, w, 1024)
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024, MaxRetries: 1, BaseBackoff: time.Microsecond})
		vec, err := emb.Embed(context.Background(), "x")
		if err != nil {
			t.Fatalf("Embed returned error: %v", err)
		}
		if len(vec) != 1024 || atomic.LoadInt32(&calls) != 2 {
			t.Fatalf("len=%d calls=%d, want len=1024 calls=2", len(vec), calls)
		}
	})

	t.Run("consecutive 5xx opens circuit and next call does not hit server", func(t *testing.T) {
		t.Parallel()
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024, MaxRetries: 0, BreakerThreshold: 2, BreakerOpenFor: time.Minute})
		for i := 0; i < 2; i++ {
			_, err := emb.Embed(context.Background(), "x")
			if err == nil {
				t.Fatal("expected 5xx error")
			}
		}
		before := atomic.LoadInt32(&calls)
		_, err := emb.Embed(context.Background(), "x")
		if !errors.Is(err, ErrEmbeddingCircuitOpen) {
			t.Fatalf("expected ErrEmbeddingCircuitOpen, got %v", err)
		}
		if after := atomic.LoadInt32(&calls); after != before {
			t.Fatalf("circuit open still hit server: before=%d after=%d", before, after)
		}
	})

	t.Run("context canceled returns ctx error without retry", func(t *testing.T) {
		t.Parallel()
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			<-r.Context().Done()
		}))
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024, MaxRetries: 3})
		_, err := emb.Embed(ctx, "x")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if atomic.LoadInt32(&calls) != 0 {
			t.Fatalf("canceled request hit server calls=%d", calls)
		}
	})

	t.Run("deadline exceeded returns ctx error quickly", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(50 * time.Millisecond)
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024, MaxRetries: 3, BaseBackoff: time.Millisecond})
		start := time.Now()
		_, err := emb.Embed(ctx, "x")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context deadline, got %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("deadline was not fast")
		}
	})

	t.Run("4xx except 429 does not retry", func(t *testing.T) {
		t.Parallel()
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad input"}`))
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024, MaxRetries: 3, BaseBackoff: time.Microsecond})
		_, err := emb.Embed(context.Background(), "x")
		if !errors.Is(err, ErrEmbeddingRequest) {
			t.Fatalf("expected ErrEmbeddingRequest, got %v", err)
		}
		if atomic.LoadInt32(&calls) != 1 {
			t.Fatalf("4xx retried: calls=%d", calls)
		}
	})

	t.Run("empty options fail defensively", func(t *testing.T) {
		t.Parallel()
		emb := NewRemoteEmbedder(RemoteEmbedderOptions{})
		_, err := emb.Embed(context.Background(), "")
		if !errors.Is(err, ErrEmbeddingRequest) {
			t.Fatalf("expected ErrEmbeddingRequest, got %v", err)
		}
	})

	t.Run("malicious long input is encoded and does not panic", func(t *testing.T) {
		t.Parallel()
		longInput := strings.Repeat("<script>{}\n", 4096)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var got struct {
				Input string `json:"input"`
			}
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			if got.Input != longInput {
				t.Fatal("long input was corrupted")
			}
			writeEmbeddingResponse(t, w, 1024)
		}))
		defer srv.Close()

		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024})
		vec, err := emb.Embed(context.Background(), longInput)
		if err != nil || len(vec) != 1024 {
			t.Fatalf("long input result len=%d err=%v", len(vec), err)
		}
	})

	t.Run("high concurrency surge completes without races", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeEmbeddingResponse(t, w, 1024)
		}))
		defer srv.Close()
		emb := NewRemoteEmbedder(RemoteEmbedderOptions{BaseURL: srv.URL, APIKey: "placeholder", Model: "m", ExpectedDim: 1024})
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vec, err := emb.Embed(context.Background(), "surge")
				if err != nil || len(vec) != 1024 {
					t.Errorf("surge len=%d err=%v", len(vec), err)
				}
			}()
		}
		wg.Wait()
	})
}

func writeEmbeddingResponse(t *testing.T, w http.ResponseWriter, dim int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"data":[{"embedding":[`))
	for i := 0; i < dim; i++ {
		if i > 0 {
			_, _ = w.Write([]byte(","))
		}
		_, _ = w.Write([]byte("0.125"))
	}
	_, _ = w.Write([]byte(`]}]}`))
}
