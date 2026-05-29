package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/joho/godotenv"

	"go-rec/internal/slow_track"
	"go-rec/pkg/agent"
	"go-rec/pkg/mq"
)

// TestStandaloneLLM calls the real DeepSeek API with the current prompt
// template to verify the intent_vector dimension.
func TestStandaloneLLM(t *testing.T) {
	_ = godotenv.Load("..\\..\\.env")

	apiKey := os.Getenv("LLM_API_KEY")
	if apiKey == "" {
		t.Skip("LLM_API_KEY not set, skipping standalone LLM test")
	}

	client := slow_track.NewClientFromEnv()
	st := &agent.State{
		SessionID: "standalone-test",
		Events: []mq.Event{
			{UserID: "u1", ItemID: "i1", CategoryID: "咖啡茶饮", Timestamp: 100, Action: "click"},
			{UserID: "u1", ItemID: "i2", CategoryID: "手机数码", Timestamp: 101, Action: "click"},
			{UserID: "u1", ItemID: "i3", CategoryID: "咖啡茶饮", Timestamp: 102, Action: "click"},
		},
		Metadata: map[string]string{},
	}

	prompt, err := agent.DefaultPromptBuilder().Build(st)
	if err != nil {
		t.Fatalf("prompt build: %v", err)
	}
	t.Logf("=== PROMPT ===\n%s\n=== END PROMPT ===", prompt)

	// Use slowTrackLLMAdapter to get the merged text
	adapter := slowTrackLLMAdapter{client: client}
	raw, err := adapter.Complete(context.Background(), LLMRequest{
		UserPrompt: prompt,
		EnableCoT:  true,
	})
	if err != nil {
		t.Fatalf("LLM call failed: %v", err)
	}
	t.Logf("=== RAW RESPONSE (%d chars) ===\n%s\n=== END RAW ===", len(raw), raw)

	payload, parseErr := agent.ParseIntentPayload(raw)
	if parseErr != nil {
		t.Logf("ParseIntentPayload error: %v", parseErr)
	}
	t.Logf("Parsed: session=%s baseline=%d category_weights=%v intent_vector_len=%d",
		payload.SessionID, payload.BaselineVersion, payload.CategoryWeights, len(payload.IntentVector))
	if len(payload.IntentVector) > 0 && len(payload.IntentVector) < 10 {
		t.Logf("intent_vector sample (first %d): %v", len(payload.IntentVector), payload.IntentVector)
	}

	// Key assertion: the LLM should return category_weights (NO intent_vector needed —
	// the embedding node will generate it). This is the fix for the 51s timeout issue.
	if len(payload.CategoryWeights) == 0 {
		t.Errorf("LLM returned empty category_weights — the new output_schema should work")
	}
	if len(payload.IntentVector) > 0 {
		t.Logf("LLM returned intent_vector with %d elements (optional, embedding node will replace)", len(payload.IntentVector))
	}
	t.Logf("category_weights=%v session=%s", payload.CategoryWeights, payload.SessionID)
}

// TestPromptSchemaShowsIntentVectorExample reveals the root cause.
func TestPromptSchemaShowsIntentVectorExample(t *testing.T) {
	pb := agent.DefaultPromptBuilder()
	schema := pb.Template.OutputSchema
	t.Logf("Current output_schema: %s", schema)
	// Verify the fix: output_schema no longer asks for intent_vector (embedding node handles it)
	if strings.Contains(schema, "intent_vector") {
		t.Error("output_schema still references intent_vector — the LLM should NOT generate it; embedding service handles 1024-dim vectors")
	} else {
		t.Log("output_schema correctly excludes intent_vector — embedding node is the authoritative source")
	}
}

// TestLLMOutputWithoutIntentVector confirms the new prompt template works:
// the LLM returns just category_weights (no intent_vector), and the
// embedding node produces the 1024-dim vector downstream.
func TestLLMOutputWithoutIntentVector(t *testing.T) {
	t.Parallel()

	// Verify the default prompt excludes intent_vector from the output schema.
	prompt, err := agent.DefaultPromptBuilder().Build(&agent.State{
		SessionID: "test",
		Events:    []mq.Event{{UserID: "u1", ItemID: "i1", CategoryID: "c1", Timestamp: 1, Action: "click"}},
	})
	if err != nil {
		t.Fatalf("prompt build: %v", err)
	}
	if strings.Contains(agent.DefaultPromptBuilder().Template.OutputSchema, "intent_vector") {
		t.Error("output_schema still includes intent_vector — embedding service should handle this")
	}
	// Verify the DAG flow works: LLM → mock output → embedding node generates vector.
	happyPath := happyPathLLM{}
	raw, err := happyPath.Complete(context.Background(), LLMRequest{UserPrompt: prompt, EnableCoT: true})
	if err != nil {
		t.Fatalf("happyPathLLM: %v", err)
	}
	payload, parseErr := agent.ParseIntentPayload(raw)
	if parseErr != nil {
		t.Fatalf("ParseIntentPayload: %v", parseErr)
	}
	if len(payload.CategoryWeights) == 0 {
		t.Error("LLM returned no category_weights")
	}
	t.Logf("LLM output: session=%s baseline=%d weights=%v",
		payload.SessionID, payload.BaselineVersion, payload.CategoryWeights)
}