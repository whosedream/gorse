package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type EmbeddingNodeOptions struct {
	ID           string
	Deps         []string
	Embedder     Embedder
	Logger       interface{ Printf(string, ...any) }
	LogPublisher LogPublisher
}

type embeddingNode struct {
	id           string
	deps         []string
	embedder     Embedder
	logger       interface{ Printf(string, ...any) }
	logPublisher LogPublisher
}

func NewEmbeddingNode(opts EmbeddingNodeOptions) Node {
	id := opts.ID
	if id == "" {
		id = "embedding"
	}
	return &embeddingNode{id: id, deps: append([]string(nil), opts.Deps...), embedder: opts.Embedder, logger: opts.Logger, logPublisher: opts.LogPublisher}
}

func (n *embeddingNode) ID() string { return n.id }

func (n *embeddingNode) Deps() []string { return append([]string(nil), n.deps...) }

func (n *embeddingNode) Kind() NodeKind { return NodeSymbol }

func (n *embeddingNode) Run(ctx context.Context, st *State) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if st == nil || n.embedder == nil {
		return ErrInvalidNode
	}
	text := BuildEmbeddingText(st)
	vec, err := n.embedder.Embed(ctx, text)
	if err == nil {
		st.IntentVector = vec
		publishDAGLog(ctx, n.logPublisher, st.SessionID, "[向量生成] 远程 embedding 生成完成")
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if n.logger != nil {
		n.logger.Printf("embedding fallback after embedder error: %v", err)
	}
	if err := SimulateEmbedding(st); err != nil {
		return err
	}
	publishDAGLog(ctx, n.logPublisher, st.SessionID, "[向量生成] fallback embedding 生成完成")
	return nil
}

func BuildEmbeddingText(st *State) string {
	if st == nil {
		return "empty intent state"
	}
	payload, err := ParseIntentPayload(st.LLMOutput)
	var b strings.Builder
	b.Grow(256)
	if err == nil {
		if payload.SessionID != "" {
			b.WriteString("session=")
			b.WriteString(payload.SessionID)
			b.WriteByte(' ')
		}
		if payload.BaselineVersion > 0 {
			_, _ = fmt.Fprintf(&b, "baseline=%d ", payload.BaselineVersion)
		}
		if len(payload.CategoryWeights) > 0 {
			keys := make([]string, 0, len(payload.CategoryWeights))
			for k := range payload.CategoryWeights {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString("categories=")
			for i, k := range keys {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(k)
				b.WriteByte(':')
				_, _ = fmt.Fprintf(&b, "%.3f", payload.CategoryWeights[k])
			}
			b.WriteByte(' ')
		}
	}
	if st.SessionID != "" && !strings.Contains(b.String(), st.SessionID) {
		b.WriteString("state_session=")
		b.WriteString(st.SessionID)
		b.WriteByte(' ')
	}
	if st.BaselineVersion > 0 {
		_, _ = fmt.Fprintf(&b, "state_baseline=%d ", st.BaselineVersion)
	}
	clean := strings.TrimSpace(b.String())
	if clean == "" {
		clean = strings.TrimSpace(st.LLMOutput)
	}
	if clean == "" {
		clean = "empty intent state"
	}
	if len(clean) > 4096 {
		clean = clean[:4096]
	}
	return clean
}
