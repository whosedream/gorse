package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"go-rec/pkg/mq"
	"sort"
	"strings"
	"time"
)

var (
	ErrNoJSONPayload = errors.New("agent model json payload not found")
	ErrBadJSON       = errors.New("agent model json invalid")
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
	LogPublisher  LogPublisher
}

type neuralIntentNode struct {
	id            string
	deps          []string
	client        ModelClient
	promptBuilder PromptBuilder
	maxRetries    int
	baseBackoff   time.Duration
	logger        interface{ Printf(string, ...any) }
	logPublisher  LogPublisher
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
		logPublisher:  opts.LogPublisher,
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
	start := time.Now()
	publishDAGLog(ctx, n.logPublisher, st.SessionID, "[LLM推理开始] 慢轨意图解构启动")
	builder := n.promptBuilder
	prompt, err := builder.Build(st)
	if err != nil {
		return err
	}
	if st.Metadata[MetadataReflectionActive] == "true" {
		publishDAGLog(ctx, n.logPublisher, st.SessionID, "[反思触发] 检测到感知漂移反思上下文")
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
			if err == nil && payload.SessionID != "" && len(payload.CategoryWeights) > 0 {
				st.IntentVector = payload.IntentVector
				if payload.BaselineVersion > 0 {
					st.BaselineVersion = payload.BaselineVersion
				}
				publishDAGLog(ctx, n.logPublisher, st.SessionID, raw)
					publishDAGLog(ctx, n.logPublisher, st.SessionID, formatCategoryWeights(payload.CategoryWeights))
					publishDAGLog(ctx, n.logPublisher, st.SessionID, "[LLM推理完成] 意图解构完成 耗时="+time.Since(start).String())
				return nil
			}
			if err == nil {
				err = fmt.Errorf("%w: empty category_weights", ErrBadJSON)
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

	simulateCoT := buildSimulatedCoT(st, lastErr)
	st.LLMOutput = simulateCoT
	publishDAGLog(ctx, n.logPublisher, st.SessionID, simulateCoT)
	publishDAGLog(ctx, n.logPublisher, st.SessionID, "[LLM推理完成] 意图解构完成 (simulated) 耗时="+time.Since(start).String())
	if err := SimulateEmbedding(st); err != nil {
		return err
	}
	return nil
}

// buildSimulatedCoT produces a DeepSeek-style chain-of-thought trace from
// the current state. Category weights are derived from actual click data.
func buildSimulatedCoT(st *State, lastErr error) string {
	weights := categoryWeightsFromEvents(st.Events)

	var b strings.Builder
	b.WriteString(" thinking 正在分析用户多维意图 | 【品类匹配】")
	first := true
	for cat, w := range weights {
		if w < 0.05 { continue }
		if !first { b.WriteString(", ") }
		b.WriteString(fmt.Sprintf("%s:%.2f", cat, w))
		first = false
	}
	b.WriteString(" | 【品牌识别】品牌亲和度:0.76 | ")
	b.WriteString("【型号匹配】价格敏感度:中等 新品偏好:0.62 | ")
	b.WriteString("【多维决策】1024维意图向量已合成")
	if lastErr != nil {
		b.WriteString(fmt.Sprintf(" | 注:本地模拟 (LLM不可用: %v)", lastErr))
	}
	b.WriteString("  response")

	// JSON payload with real weights.
	b.WriteString(` {"session_id":"` + st.SessionID + `","baseline_version":` + fmt.Sprintf("%d", st.BaselineVersion) + `,"category_weights":{`)
	first = true
	for cat, w := range weights {
		if !first { b.WriteString(",") }
		b.WriteString(fmt.Sprintf(`"%s":%.2f`, cat, w))
		first = false
	}
	b.WriteString(`}`)
	return b.String()
}

// categoryWeightsFromEvents computes dynamic category weights from
// behavior events. The clicked category gets 0.50; rest split 0.50.
func categoryWeightsFromEvents(events []mq.Event) map[string]float32 {
	allCats := []string{"手机数码", "运动户外", "咖啡茶饮", "猫咪用品", "图书"}
	weights := make(map[string]float32, len(allCats))
	var dominant string
	for _, ev := range events {
		if ev.CategoryID != "" { dominant = ev.CategoryID; break }
	}
	if dominant == "" {
		for _, cat := range allCats { weights[cat] = 0.20 }
		return weights
	}
	n := len(allCats) - 1
	rest := float32(0.50) / float32(n)
	for _, cat := range allCats {
		if cat == dominant { weights[cat] = 0.50 } else { weights[cat] = rest }
	}
	return weights
}

// formatCategoryWeights renders extracted category weights as a compact,
// deterministically sorted log line for the SSE console.
func formatCategoryWeights(weights map[string]float32) string {
	keys := make([]string, 0, len(weights))
	for k := range weights {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString("[意图解构] 分类权重: ")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(fmt.Sprintf("%.2f", weights[k]))
	}
	return sb.String()
}

func CleanModelJSON(raw string) ([]byte, error) {
	start := strings.IndexByte(raw, '{')
	if start < 0 {
		return nil, ErrNoJSONPayload
	}
	end := strings.LastIndexByte(raw, '}')
	if end <= start {
		return nil, ErrNoJSONPayload
	}
	return []byte(raw[start : end+1]), nil
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