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
	b.WriteString(`,"category_weights":{"phone":`)
	b.WriteString(fmt.Sprintf("%.2f", value))
	b.WriteString(`},"intent_vector":[]}`)
	return b.String()
}

func TestCleanModelJSONHandlesThinkMarkdownNoiseAndFinalPayload(t *testing.T) {
	t.Parallel()

	raw := "废话 <think>正在分析用户多维意图…</think>\n```json\n" + makeVectorJSON("s-clean", 9, 0.75) + "\n```\n结语：推荐完成"
	cleaned, err := CleanModelJSON(raw)
	if err != nil {
		t.Fatalf("CleanModelJSON returned error: %v", err)
	}
	payload, err := ParseIntentPayload(string(cleaned))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s-clean" || payload.BaselineVersion != 9 || payload.CategoryWeights["phone"] != 0.75 {
		t.Fatalf("unexpected payload: session=%s baseline=%d weights=%v", payload.SessionID, payload.BaselineVersion, payload.CategoryWeights)
	}
}

func TestCleanModelJSONKeepsBracesInsideStrings(t *testing.T) {
	t.Parallel()

	raw := `<think>reasoning</think> {"session_id":"s{brace}","baseline_version":7,"category_weights":{"weird{}":0.5},"intent_vector":[0.1]} 结语`
	cleaned, err := CleanModelJSON(raw)
	if err != nil {
		t.Fatalf("CleanModelJSON returned error: %v", err)
	}
	payload, err := ParseIntentPayload(string(cleaned))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s{brace}" || payload.CategoryWeights["weird{}"] != 0.5 {
		t.Fatalf("payload was truncated or corrupted: %+v", payload)
	}
}

func TestParseIntentPayloadValidatesSessionAndCategories(t *testing.T) {
	t.Parallel()

	payload, err := ParseIntentPayload(makeVectorJSON("s-parse", 123, 0.42))
	if err != nil {
		t.Fatalf("ParseIntentPayload returned error: %v", err)
	}
	if payload.SessionID != "s-parse" || payload.BaselineVersion != 123 || payload.CategoryWeights["phone"] != 0.42 {
		t.Fatalf("unexpected payload: session=%s baseline=%d weights=%v", payload.SessionID, payload.BaselineVersion, payload.CategoryWeights)
	}
	// Empty session_id should still parse (validation happens at Run level)
	rawNoSession := `{"session_id":"","baseline_version":1,"category_weights":{"a":0.5},"intent_vector":[]}`
	p2, err := ParseIntentPayload(rawNoSession)
	if err != nil {
		t.Fatalf("ParseIntentPayload should not reject empty session: %v", err)
	}
	if p2.SessionID != "" || p2.CategoryWeights["a"] != 0.5 {
		t.Fatalf("unexpected payload with empty session: %+v", p2)
	}
}

func TestNeuralIntentNodeRetriesAfterHallucinatedBrokenJSONThenSucceeds(t *testing.T) {
	t.Parallel()

	client := &sequenceClient{responses: []string{
		"废话  thinkingsecret response ```json { broken ::: ``` 结语",
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
	if st.BaselineVersion != 456 || st.LLMOutput == "" {
		t.Fatalf("state not updated from successful retry: baseline=%d output=%q", st.BaselineVersion, st.LLMOutput)
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
			if st.BaselineVersion != 999 {
				return fmt.Errorf("baseline not propagated: got %d", st.BaselineVersion)
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
	if st.BaselineVersion != 999 {
		t.Fatalf("graph did not propagate baseline: got %d", st.BaselineVersion)
	}
}