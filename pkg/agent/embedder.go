package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	ErrEmbeddingCircuitOpen = errors.New("embedding circuit open")
	ErrEmbeddingDimension   = errors.New("embedding dimension mismatch")
	ErrEmbeddingRequest     = errors.New("embedding request failed")
)

// Embedder is the anti-corruption boundary for external vector services.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

type RemoteEmbedderOptions struct {
	BaseURL          string
	APIKey           string
	Model            string
	HTTPClient       *http.Client
	MaxRetries       int
	BreakerThreshold int
	BreakerOpenFor   time.Duration
	ExpectedDim      int
	BaseBackoff      time.Duration
}

type RemoteEmbedder struct {
	baseURL          string
	apiKey           string
	model            string
	client           *http.Client
	maxRetries       int
	breakerThreshold int
	breakerOpenFor   time.Duration
	expectedDim      int
	baseBackoff      time.Duration

	mu           sync.Mutex
	failures     int
	circuitUntil time.Time
}

func RemoteEmbedderOptionsFromEnv() RemoteEmbedderOptions {
	return RemoteEmbedderOptions{
		BaseURL: envFirst("EMBEDDING_BASE_URL", "SILICONFLOW_BASE_URL"),
		APIKey:  envFirst("EMBEDDING_API_KEY", "SILICONFLOW_API_KEY"),
		Model:   envFirst("EMBEDDING_MODEL", "SILICONFLOW_EMBEDDING_MODEL"),
	}
}

func envFirst(primary, fallback string) string {
	if v := os.Getenv(primary); v != "" {
		return v
	}
	return os.Getenv(fallback)
}

func NewRemoteEmbedderFromEnv() *RemoteEmbedder {
	return NewRemoteEmbedder(RemoteEmbedderOptionsFromEnv())
}

func NewRemoteEmbedder(opts RemoteEmbedderOptions) *RemoteEmbedder {
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 800 * time.Millisecond}
	}
	model := opts.Model
	if model == "" {
		model = "BAAI/bge-m3"
	}
	expectedDim := opts.ExpectedDim
	if expectedDim <= 0 {
		expectedDim = 1024
	}
	threshold := opts.BreakerThreshold
	if threshold <= 0 {
		threshold = 5
	}
	openFor := opts.BreakerOpenFor
	if openFor <= 0 {
		openFor = 2 * time.Second
	}
	backoff := opts.BaseBackoff
	if backoff <= 0 {
		backoff = 10 * time.Millisecond
	}
	return &RemoteEmbedder{
		baseURL:          opts.BaseURL,
		apiKey:           opts.APIKey,
		model:            model,
		client:           client,
		maxRetries:       opts.MaxRetries,
		breakerThreshold: threshold,
		breakerOpenFor:   openFor,
		expectedDim:      expectedDim,
		baseBackoff:      backoff,
	}
}

func (r *RemoteEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if r == nil || r.baseURL == "" || r.client == nil {
		return nil, ErrEmbeddingRequest
	}
	if r.isCircuitOpen(time.Now()) {
		return nil, ErrEmbeddingCircuitOpen
	}
	attempts := r.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		vec, retry, err := r.doEmbed(ctx, text)
		if err == nil {
			r.recordSuccess()
			return vec, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		lastErr = err
		if !retry || attempt == attempts-1 {
			r.recordFailure()
			return nil, err
		}
		if err := sleepEmbeddingBackoff(ctx, r.baseBackoff, attempt); err != nil {
			return nil, err
		}
	}
	if lastErr == nil {
		lastErr = ErrEmbeddingRequest
	}
	r.recordFailure()
	return nil, lastErr
}

func (r *RemoteEmbedder) isCircuitOpen(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.circuitUntil.IsZero() || !now.Before(r.circuitUntil) {
		return false
	}
	return true
}

func (r *RemoteEmbedder) recordSuccess() {
	r.mu.Lock()
	r.failures = 0
	r.circuitUntil = time.Time{}
	r.mu.Unlock()
}

func (r *RemoteEmbedder) recordFailure() {
	r.mu.Lock()
	r.failures++
	if r.failures >= r.breakerThreshold {
		r.circuitUntil = time.Now().Add(r.breakerOpenFor)
	}
	r.mu.Unlock()
}

type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (r *RemoteEmbedder) doEmbed(ctx context.Context, text string) ([]float32, bool, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(embeddingRequest{Model: r.model, Input: text}); err != nil {
		return nil, false, fmt.Errorf("%w: encode: %v", ErrEmbeddingRequest, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL, &buf)
	if err != nil {
		return nil, false, fmt.Errorf("%w: new request: %v", ErrEmbeddingRequest, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if r.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.apiKey)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, true, fmt.Errorf("%w: network: %v", ErrEmbeddingRequest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, true, fmt.Errorf("%w: status %d", ErrEmbeddingRequest, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, false, fmt.Errorf("%w: status %d", ErrEmbeddingRequest, resp.StatusCode)
	}
	var out embeddingResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return nil, true, fmt.Errorf("%w: decode: %v", ErrEmbeddingRequest, err)
	}
	if len(out.Data) == 0 || len(out.Data[0].Embedding) != r.expectedDim {
		got := 0
		if len(out.Data) > 0 {
			got = len(out.Data[0].Embedding)
		}
		return nil, false, fmt.Errorf("%w: got %d want %d", ErrEmbeddingDimension, got, r.expectedDim)
	}
	return out.Data[0].Embedding, false, nil
}

func sleepEmbeddingBackoff(ctx context.Context, base time.Duration, attempt int) error {
	if base <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	multiplier := 1 << attempt
	if multiplier < 1 {
		multiplier = 1
	}
	jitter := time.Duration(rand.Int63n(int64(base) + 1))
	timer := time.NewTimer(time.Duration(multiplier)*base + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
