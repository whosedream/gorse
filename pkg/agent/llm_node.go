package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

var (
	ErrNoJSONPayload = errors.New("agent model json payload not found")
	ErrBadJSON       = errors.New("agent model json invalid")
)

var (
	thinkBlockRE  = regexp.MustCompile(`(?is)<think>.*?</think>`)
	markdownFence = regexp.MustCompile("(?i)```json|```")
)

// CompletionClient is the narrow LLM interface accepted by neural nodes.
type CompletionClient interface {
	Complete(context.Context, string) (string, error)
}

// IntentPayload is the final JSON contract expected from the model.
type IntentPayload struct {
	SessionID       string             `json:"session_id"`
	BaselineVersion int64              `json:"baseline_version"`
	CategoryWeights map[string]float32 `json:"category_weights"`
	IntentVector    []float32          `json:"intent_vector"`
}

// NeuralNodeOptions configures an LLM-backed DAG node.
type NeuralNodeOptions struct {
	ID            string
	Deps          []string
	Client        ModelClient
	PromptBuilder PromptBuilder
	MaxRetries    int
	BaseBackoff   time.Duration
	Logger        interface{ Printf(string, ...any) }
}

type neuralIntentNode struct {
	id            string
	deps          []string
	client        ModelClient
	promptBuilder PromptBuilder
	maxRetries    int
	baseBackoff   time.Duration
	logger        interface{ Printf(string, ...any) }
}

// NewNeuralIntentNode creates a resilient neural intent extraction DAG node.
func NewNeuralIntentNode(opts NeuralNodeOptions) Node {
	id := opts.ID
	if id == "" {
		id = "neural_intent"
	}
	return &neuralIntentNode{
		id:            id,
		deps:          append([]string(nil), opts.Deps...),
		client:        opts.Client,
		promptBuilder: opts.PromptBuilder,
		maxRetries:    opts.MaxRetries,
		baseBackoff:   opts.BaseBackoff,
		logger:        opts.Logger,
	}
}

func (n *neuralIntentNode) ID() string { return n.id }

func (n *neuralIntentNode) Deps() []string { return append([]string(nil), n.deps...) }

func (n *neuralIntentNode) Kind() NodeKind { return NodeLLM }

func (n *neuralIntentNode) Run(ctx context.Context, st *State) error {
	if st == nil || n.client == nil {
		return ErrInvalidNode
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	builder := n.promptBuilder
	prompt, err := builder.Build(st)
	if err != nil {
		return err
	}
	st.Prompt = prompt
	attempts := n.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		raw, err := n.client.Complete(ctx, prompt)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
		} else {
			st.LLMOutput = raw
			payload, err := ParseIntentPayload(raw)
			if err == nil && len(payload.IntentVector) == 1024 {
				st.IntentVector = payload.IntentVector
				if payload.BaselineVersion > 0 {
					st.BaselineVersion = payload.BaselineVersion
				}
				return nil
			}
			if err == nil {
				err = fmt.Errorf("%w: intent vector length %d", ErrBadJSON, len(payload.IntentVector))
			}
			lastErr = err
		}
		if attempt < attempts-1 {
			if err := sleepBackoff(ctx, n.baseBackoff, attempt); err != nil {
				return err
			}
		}
	}
	if n.logger != nil {
		n.logger.Printf("neural intent fallback after retries exhausted: %v", lastErr)
	}
	if err := SimulateEmbedding(st); err != nil {
		return err
	}
	return nil
}

// CleanModelJSON strips model-only reasoning/markdown wrappers and returns the first complete JSON object.
func CleanModelJSON(raw string) ([]byte, error) {
	cleaned := thinkBlockRE.ReplaceAllString(raw, "")
	cleaned = markdownFence.ReplaceAllString(cleaned, "")
	start := strings.IndexByte(cleaned, '{')
	if start < 0 {
		return nil, ErrNoJSONPayload
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(cleaned); i++ {
		c := cleaned[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch c {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return []byte(strings.TrimSpace(cleaned[start : i+1])), nil
			}
			if depth < 0 {
				return nil, ErrNoJSONPayload
			}
		}
	}
	return nil, ErrNoJSONPayload
}

// ParseIntentPayload extracts and unmarshals the model's final JSON payload.
func ParseIntentPayload(raw string) (IntentPayload, error) {
	payload, err := CleanModelJSON(raw)
	if err != nil {
		return IntentPayload{}, err
	}
	var out IntentPayload
	if err := json.Unmarshal(payload, &out); err != nil {
		return IntentPayload{}, fmt.Errorf("%w: %v", ErrBadJSON, err)
	}
	return out, nil
}

func sleepBackoff(ctx context.Context, base time.Duration, attempt int) error {
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
	d := time.Duration(multiplier) * base
	jitter := time.Duration(rand.Int63n(int64(base) + 1))
	timer := time.NewTimer(d + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
