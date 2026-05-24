package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type sequenceClient struct {
	mu        sync.Mutex
	responses []string
	errs      []error
	calls     int
	prompts   []string
}

func (c *sequenceClient) Complete(ctx context.Context, prompt string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.prompts = append(c.prompts, prompt)
	idx := c.calls - 1
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if idx < len(c.errs) && c.errs[idx] != nil {
		return "", c.errs[idx]
	}
	if idx < len(c.responses) {
		return c.responses[idx], nil
	}
	return "", errors.New("unexpected call")
}

func (c *sequenceClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func makeVectorJSON(session string, baseline int64, value float32) string {
	var b strings.Builder
	b.WriteString(`{"session_id":"`)
	b.WriteString(session)
	b.WriteString(`","baseline_version":`)
	b.WriteString(fmt.Sprint(baseline))
	b.WriteString(`,"category_weights":{"phone":0.75},"intent_vector":[`)
	for i := 0; i < 1024; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(fmt.Sprintf("%.2f", value))
	}
	b.WriteString(`]}`)
	return b.String()
}

func TestCleanModelJSONHandlesThinkMarkdownNoiseAndFinalPayload(t *testing.T) {
	t.Parallel()

	raw := "废话<think>secret {bad}</think>```json\n" + makeVectorJSON("s-clean", 9, 0.25) + "\n```结语"
	cleaned, err := CleanModelJSON(raw)
	if err != nil {
		t.Fatalf("CleanModelJSON returned error: %v", err)
	}
	if strings.Contains(string(cleaned), "think") || strings.Contains(string(cleaned), "secret") || strings.Contains(string(cleaned), "```") || strings.Contains(string(cleaned), "废话") {
		t.Fatalf("cleaned payload retained noise: %s", string(cleaned))
	}
	payload, err := ParseIntentPayload(string(cleaned))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s-clean" || payload.BaselineVersion != 9 || len(payload.IntentVector) != 1024 {
		t.Fatalf("unexpected payload: session=%s baseline=%d vector=%d", payload.SessionID, payload.BaselineVersion, len(payload.IntentVector))
	}
}

func TestCleanModelJSONKeepsBracesInsideStrings(t *testing.T) {
	t.Parallel()

	raw := `prefix {"session_id":"s{brace}","baseline_version":7,"category_weights":{"weird{}":0.5},"intent_vector":[0.1]} suffix {"bad":true}`
	cleaned, err := CleanModelJSON(raw)
	if err != nil {
		t.Fatalf("CleanModelJSON returned error: %v", err)
	}
	payload, err := ParseIntentPayload(string(cleaned))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s{brace}" || payload.CategoryWeights["weird{}"] != 0.5 || len(payload.IntentVector) != 1 {
		t.Fatalf("payload was truncated or corrupted: %+v", payload)
	}
}

func TestParseIntentPayloadSuccessWith1024DimVector(t *testing.T) {
	t.Parallel()

	payload, err := ParseIntentPayload(makeVectorJSON("s-parse", 123, 0.42))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s-parse" || payload.BaselineVersion != 123 || len(payload.IntentVector) != 1024 || payload.IntentVector[0] != 0.42 {
		t.Fatalf("unexpected payload: session=%s baseline=%d len=%d first=%f", payload.SessionID, payload.BaselineVersion, len(payload.IntentVector), payload.IntentVector[0])
	}
}

func TestNeuralIntentNodeRetriesAfterHallucinatedBrokenJSONThenSucceeds(t *testing.T) {
	t.Parallel()

	client := &sequenceClient{responses: []string{
		"废话 <think>secret</think> ```json { broken ::: ``` 结语",
		"ok ```JSON\n" + makeVectorJSON("s-ok", 456, 0.33) + "\n```",
	}}
	node := NewNeuralIntentNode(NeuralNodeOptions{
		ID:            "intent",
		Client:        client,
		PromptBuilder: DefaultPromptBuilder(),
		MaxRetries:    1,
		BaseBackoff:   0,
	})
	st := &State{SessionID: "s-in", BaselineVersion: 111}
	if err := node.Run(context.Background(), st); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if client.callCount() != 2 {
		t.Fatalf("expected 2 calls, got %d", client.callCount())
	}
	if len(st.IntentVector) != 1024 || st.BaselineVersion != 456 || st.LLMOutput == "" {
		t.Fatalf("state not updated from successful retry: len=%d baseline=%d output=%q", len(st.IntentVector), st.BaselineVersion, st.LLMOutput)
	}
}

func TestNeuralIntentNodeFallbackAfterRetriesExhaustedLogsAndReturnsNil(t *testing.T) {
	t.Parallel()

	logger := &recordingLogger{}
	client := &sequenceClient{responses: []string{"not json", "```json { still broken ```"}}
	node := NewNeuralIntentNode(NeuralNodeOptions{ID: "intent", Client: client, PromptBuilder: DefaultPromptBuilder(), MaxRetries: 1, BaseBackoff: 0, Logger: logger})
	st := &State{SessionID: "s-fallback", BaselineVersion: 88}
	err := node.Run(context.Background(), st)
	if err != nil {
		t.Fatalf("Run should not crash DAG after retry exhaustion, got: %v", err)
	}
	if client.callCount() != 2 {
		t.Fatalf("expected 2 calls, got %d", client.callCount())
	}
	if len(st.IntentVector) != 1024 || st.BaselineVersion != 88 {
		t.Fatalf("fallback vector invalid: len=%d baseline=%d", len(st.IntentVector), st.BaselineVersion)
	}
	if !logger.contains("fallback") {
		t.Fatalf("logger did not record fallback/error: %+v", logger.lines)
	}
}

func TestNeuralIntentNodeContextCanceledReturnsErrorWithoutFallback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &sequenceClient{responses: []string{makeVectorJSON("s", 1, 0.1)}}
	node := NewNeuralIntentNode(NeuralNodeOptions{ID: "intent", Client: client, PromptBuilder: DefaultPromptBuilder(), MaxRetries: 3, BaseBackoff: time.Millisecond})
	st := &State{SessionID: "s-cancel"}
	err := node.Run(ctx, st)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(st.IntentVector) != 0 {
		t.Fatalf("fallback should not run on cancellation, got len=%d", len(st.IntentVector))
	}
}

func TestNeuralIntentNodeGraphIntegrationWithPromptAndValidation(t *testing.T) {
	t.Parallel()

	client := &sequenceClient{responses: []string{makeVectorJSON("s-graph", 999, 0.66)}}
	graph, err := NewGraph(
		SymbolNode("prompt", nil, func(ctx context.Context, st *State) error {
			prompt, err := DefaultPromptBuilder().Build(st)
			if err != nil {
				return err
			}
			st.Prompt = prompt
			return nil
		}),
		NewNeuralIntentNode(NeuralNodeOptions{ID: "intent", Deps: []string{"prompt"}, Client: client, PromptBuilder: DefaultPromptBuilder(), MaxRetries: 0, BaseBackoff: 0}),
		SymbolNode("validate", []string{"intent"}, func(ctx context.Context, st *State) error {
			if len(st.IntentVector) != 1024 {
				return fmt.Errorf("invalid vector length %d", len(st.IntentVector))
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewGraph returned error: %v", err)
	}
	st := &State{SessionID: "s-graph", BaselineVersion: 100, Events: nil}
	if err := graph.Run(context.Background(), st); err != nil {
		t.Fatalf("Graph.Run returned error: %v", err)
	}
	if len(st.IntentVector) != 1024 || st.BaselineVersion != 999 {
		t.Fatalf("graph did not produce vector/baseline: len=%d baseline=%d", len(st.IntentVector), st.BaselineVersion)
	}
}
