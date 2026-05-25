package main

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

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

	if err := RunBatch(context.Background(), batch, mockLLM{}, mockEmbedder{}, writer); err != nil {
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
