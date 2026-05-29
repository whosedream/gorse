package main

import (
	"context"
	"strings"
	"testing"

	"go-rec/internal/slow_track"
)

// TestAdapterPreferTextOverReasoning verifies that slowTrackLLMAdapter
// returns resp.Text (the final content) when available, and falls back
// to resp.Reasoning only when Text is empty. DeepSeek v4-pro places the
// structured JSON output in the content field.
func TestAdapterPreferTextOverReasoning(t *testing.T) {
	t.Parallel()

	jsonPayload := `{"session_id":"s1","baseline_version":1,"category_weights":{"c":0.5},"intent_vector":[]}`

	// Case 1: Both text and reasoning present → prefer text.
	rawResp := slow_track.Response{
		Text:      jsonPayload,
		Reasoning: " thinking分析… schema noise {\"wrong\":1} response",
	}
	adapter := slowTrackLLMAdapter{client: slowTrackMockClient{resp: rawResp}}
	got, err := adapter.Complete(context.Background(), LLMRequest{UserPrompt: "test", EnableCoT: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != jsonPayload {
		t.Fatalf("adapter should return text content verbatim, got %q", got)
	}
	t.Log("adapter returns text content (no reasoning pollution)")

	// Case 2: Only reasoning present → fall back to reasoning.
	rawResp2 := slow_track.Response{
		Text:      "",
		Reasoning: " thinking… response" + jsonPayload,
	}
	adapter2 := slowTrackLLMAdapter{client: slowTrackMockClient{resp: rawResp2}}
	got2, err2 := adapter2.Complete(context.Background(), LLMRequest{UserPrompt: "test", EnableCoT: true})
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if !strings.Contains(got2, jsonPayload) {
		t.Fatalf("reasoning fallback should contain JSON payload, got %q", got2)
	}
	t.Log("adapter falls back to reasoning when text is empty")
}

// TestAdapterProductionCodeMatchesNewBehavior verifies the actual
// slowTrackLLMAdapter in main.go prefers text over reasoning.
func TestAdapterProductionCodeMatchesNewBehavior(t *testing.T) {
	t.Parallel()

	jsonPayload := `{"session_id":"s2","baseline_version":2,"category_weights":{},"intent_vector":[]}`
	rawResp := slow_track.Response{Text: jsonPayload, Reasoning: " thinking噪音… response"}

	adapter := slowTrackLLMAdapter{client: slowTrackMockClient{resp: rawResp}}
	got, err := adapter.Complete(context.Background(), LLMRequest{UserPrompt: "test", EnableCoT: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != jsonPayload {
		t.Fatalf("adapter should return text content, not reasoning: %q", got)
	}
	t.Log("production adapter returns text (no reasoning merge)")
}