package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"go-rec/internal/slow_track"
	"go-rec/pkg/cache"
	"go-rec/pkg/mq"
)

type mockLLM struct{}

func (mockLLM) Complete(ctx context.Context, req LLMRequest) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if !req.EnableCoT || req.UserPrompt == "" {
		return "", errors.New("bad llm request")
	}
	return `{"session_id":"s-1","baseline_version":7,"category_weights":{"c-1":0.9},"intent_vector":[]}`, nil
}

// happyPathLLM returns category_weights only; the 1024-dim intent vector is
// generated downstream by the embedding node.
type happyPathLLM struct{}

func (happyPathLLM) Complete(ctx context.Context, req LLMRequest) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	v := map[string]any{
		"session_id":       "s-happy",
		"baseline_version": float64(42),
		"category_weights": map[string]float32{"手机数码": 0.8, "运动户外": 0.2},
		"intent_vector":    []float32{},
	}
	buf, _ := json.Marshal(v)
	return " thinking 分析中… response" + string(buf), nil
}

// slowTrackMockClient returns controlled slow_track.Response values for
// testing the slowTrackLLMAdapter independently of a real HTTP client.
type slowTrackMockClient struct {
	resp slow_track.Response
	err   error
}

func (m slowTrackMockClient) Complete(_ context.Context, _ slow_track.Request) (slow_track.Response, error) {
	return m.resp, m.err
}

func TestSlowTrackLLMAdapterMergesReasoningAndText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resp     slow_track.Response
		wantSub  string // substring expected in the merged output
		wantFull string // exact output expected
	}{
		{
			name: "text only, no reasoning",
			resp: slow_track.Response{Text: `{"session_id":"s1","baseline_version":1,"category_weights":{},"intent_vector":[]}`, Reasoning: ""},
			wantFull: `{"session_id":"s1","baseline_version":1,"category_weights":{},"intent_vector":[]}`,
		},
		{
			name: "reasoning has JSON, text is empty",
			resp: slow_track.Response{Text: "", Reasoning: `<think>... </think>{"session_id":"s2","baseline_version":2,"category_weights":{"c":0.5},"intent_vector":[]}`},
			wantFull: `<think>... </think>{"session_id":"s2","baseline_version":2,"category_weights":{"c":0.5},"intent_vector":[]}`,
		},
		{
			name:     "both present — prefer text content, no merge",
			resp:     slow_track.Response{Text: `{"session_id":"s3","baseline_version":3,"category_weights":{},"intent_vector":[]}`, Reasoning: "<think>正在分析…</think>"},
			wantFull: `{"session_id":"s3","baseline_version":3,"category_weights":{},"intent_vector":[]}`,
		},
		{
			name: "both empty",
			resp: slow_track.Response{Text: "", Reasoning: ""},
			wantFull: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			adapter := slowTrackLLMAdapter{client: slowTrackMockClient{resp: tt.resp}}
			got, err := adapter.Complete(context.Background(), LLMRequest{UserPrompt: "test", EnableCoT: true})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantFull != "" && got != tt.wantFull {
				t.Fatalf("got %q, want %q", got, tt.wantFull)
			}
			if tt.wantSub != "" && !strings.Contains(got, tt.wantSub) {
				t.Fatalf("output missing %q: %q", tt.wantSub, got)
			}
		})
	}
}

type mockEmbedder struct{}

func (mockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	vec := make([]float32, cache.IntentVectorDim)
	for i := range vec {
		vec[i] = float32(i) / 1024
	}
	return vec, nil
}

type captureWriter struct {
	mu      sync.Mutex
	called  chan struct{}
	release chan struct{}
	session string
	version int64
	vecLen  int
}

func newCaptureWriter(block bool) *captureWriter {
	w := &captureWriter{called: make(chan struct{}, 1)}
	if block {
		w.release = make(chan struct{})
	}
	return w
}

func (w *captureWriter) WriteIntent(ctx context.Context, sessionID string, vector []float32, version int64) error {
	w.mu.Lock()
	w.session = sessionID
	w.version = version
	w.vecLen = len(vector)
	w.mu.Unlock()
	select {
	case w.called <- struct{}{}:
	default:
	}
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestRunBatchBuildsDAGAndWritesIntentVector(t *testing.T) {
	t.Parallel()

	writer := newCaptureWriter(false)
	batch := mq.Batch{
		SessionID:       "s-1",
		UserID:          "u-1",
		BaselineVersion: 7,
		Events: []mq.Event{{
			SessionID:  "s-1",
			UserID:     "u-1",
			ItemID:     "i-1",
			CategoryID: "c-1",
			Timestamp:  7,
			Action:     "click",
		}},
	}

	if err := RunBatch(context.Background(), batch, mockLLM{}, mockEmbedder{}, writer, nil, nil, nil); err != nil {
		t.Fatalf("RunBatch returned error: %v", err)
	}

	select {
	case <-writer.called:
	default:
		t.Fatal("writer was not called")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.session != "s-1" || writer.version != 7 || writer.vecLen != cache.IntentVectorDim {
		t.Fatalf("writer mismatch: session=%s version=%d vecLen=%d", writer.session, writer.version, writer.vecLen)
	}
}

type fakeWindowConsumer struct {
	batch       mq.Batch
	sent        chan struct{}
	closeCalled chan struct{}
}

func (f *fakeWindowConsumer) Consume(ctx context.Context, out chan<- mq.Batch) error {
	select {
	case out <- f.batch:
		close(f.sent)
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeWindowConsumer) Close() error {
	close(f.closeCalled)
	return nil
}

func TestAgentDaemonWaitsForWorkflowAfterConsumerClose(t *testing.T) {
	t.Parallel()

	consumer := &fakeWindowConsumer{
		batch: mq.Batch{
			SessionID:       "s-2",
			UserID:          "u-2",
			BaselineVersion: 11,
			Events:          []mq.Event{{SessionID: "s-2", UserID: "u-2", ItemID: "i-2", CategoryID: "c-2", Timestamp: 11}},
		},
		sent:        make(chan struct{}),
		closeCalled: make(chan struct{}),
	}
	writer := newCaptureWriter(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunDaemon(ctx, DaemonDeps{Consumer: consumer, LLM: mockLLM{}, Embedder: mockEmbedder{}, Writer: writer, BatchBuffer: 1})
	}()

	<-consumer.sent
	<-writer.called
	cancel()
	select {
	case <-consumer.closeCalled:
	case err := <-done:
		t.Fatalf("daemon returned before closing consumer: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("daemon returned before writer release: %v", err)
	default:
	}
	close(writer.release)
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		t.Fatalf("RunDaemon returned error: %v", err)
	}
}

func TestRedisDefaultAddressUsesIPv4Loopback(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	if got := envDefault("REDIS_ADDR", defaultRedisAddr); got != "127.0.0.1:6379" {
		t.Fatalf("default redis addr = %q, want 127.0.0.1:6379", got)
	}
}

func TestEnvExampleContainsLLMPlaceholders(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("..\\..\\.env.example")
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"LLM_BASE_URL=https://api.deepseek.com/v1",
		"LLM_API_KEY=",
		"LLM_MODEL=deepseek-v4-pro",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf(".env.example missing %s", want)
		}
	}
}

// TestRunBatchHappyPathLLMReturns1024DimVector verifies the full DAG
// path when the LLM produces a valid 1024-dim intent vector on the first
// attempt — no simulation fallback involved.
func TestRunBatchHappyPathLLMReturns1024DimVector(t *testing.T) {
	t.Parallel()

	writer := newCaptureWriter(false)
	batch := mq.Batch{
		SessionID:       "s-happy",
		UserID:          "u-1",
		BaselineVersion: 42,
		Events: []mq.Event{{
			SessionID:  "s-happy",
			UserID:     "u-1",
			ItemID:     "i-1",
			CategoryID: "手机数码",
			Timestamp:  42,
			Action:     "click",
		}},
	}

	if err := RunBatch(context.Background(), batch, happyPathLLM{}, mockEmbedder{}, writer, nil, nil, nil); err != nil {
		t.Fatalf("RunBatch returned error: %v", err)
	}

	select {
	case <-writer.called:
	default:
		t.Fatal("writer was not called")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.session != "s-happy" {
		t.Fatalf("session mismatch: got %q, want s-happy", writer.session)
	}
	if writer.version != 42 {
		t.Fatalf("version mismatch: got %d, want 42", writer.version)
	}
	if writer.vecLen != cache.IntentVectorDim {
		t.Fatalf("vector dim mismatch: got %d, want %d", writer.vecLen, cache.IntentVectorDim)
	}
}
