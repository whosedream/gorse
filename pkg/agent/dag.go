package agent

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"go-rec/pkg/mq"
)

var (
	ErrCycle             = errors.New("agent graph cycle")
	ErrDuplicateNode     = errors.New("agent duplicate node")
	ErrMissingDependency = errors.New("agent missing dependency")
	ErrInvalidNode       = errors.New("agent invalid node")
)

// NodeKind separates neural LLM calls from deterministic Go symbol work.
type NodeKind int

const (
	NodeLLM NodeKind = iota + 1
	NodeSymbol
)

// State is the mutable slow-track workflow state.
type State struct {
	SessionID       string
	BaselineVersion int64
	Events          []mq.Event
	Prompt          string
	LLMOutput       string
	IntentVector    []float32
	Metadata        map[string]string
}

// Node is a DAG executable unit.
type Node interface {
	ID() string
	Deps() []string
	Kind() NodeKind
	Run(context.Context, *State) error
}

// ModelClient is the narrow LLM interface used by neural nodes.
type ModelClient interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

// Graph executes nodes in topological order.
type Graph struct {
	nodes []Node
}

// NewGraph validates duplicate IDs, missing deps, and cycles.
func NewGraph(nodes ...Node) (*Graph, error) {
	byID := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		if n == nil || n.ID() == "" {
			return nil, ErrInvalidNode
		}
		if _, ok := byID[n.ID()]; ok {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateNode, n.ID())
		}
		byID[n.ID()] = n
	}
	indegree := make(map[string]int, len(nodes))
	children := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		id := n.ID()
		indegree[id] = indegree[id]
		for _, dep := range n.Deps() {
			if _, ok := byID[dep]; !ok {
				return nil, fmt.Errorf("%w: %s -> %s", ErrMissingDependency, id, dep)
			}
			indegree[id]++
			children[dep] = append(children[dep], id)
		}
	}
	queue := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if indegree[n.ID()] == 0 {
			queue = append(queue, n.ID())
		}
	}
	order := make([]Node, 0, len(nodes))
	for head := 0; head < len(queue); head++ {
		id := queue[head]
		order = append(order, byID[id])
		for _, child := range children[id] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(order) != len(nodes) {
		return nil, ErrCycle
	}
	return &Graph{nodes: order}, nil
}

// Run executes all nodes under the caller context.
func (g *Graph) Run(ctx context.Context, st *State) error {
	if st == nil {
		st = &State{}
	}
	for _, n := range g.nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := n.Run(ctx, st); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
	return nil
}

type funcNode struct {
	id   string
	deps []string
	kind NodeKind
	fn   func(context.Context, *State) error
}

// FuncNode creates a generic node for tests and simple workflows.
func FuncNode(id string, deps []string, kind NodeKind, fn func(context.Context, *State) error) Node {
	return &funcNode{id: id, deps: append([]string(nil), deps...), kind: kind, fn: fn}
}

// SymbolNode creates a deterministic Go node.
func SymbolNode(id string, deps []string, fn func(context.Context, *State) error) Node {
	return FuncNode(id, deps, NodeSymbol, fn)
}

// LLMNode creates a neural node backed by ModelClient.
func LLMNode(id string, deps []string, client ModelClient) Node {
	return FuncNode(id, deps, NodeLLM, func(ctx context.Context, st *State) error {
		if client == nil {
			return ErrInvalidNode
		}
		text, err := client.Complete(ctx, st.Prompt)
		if err != nil {
			return err
		}
		st.LLMOutput = text
		return nil
	})
}

func (n *funcNode) ID() string { return n.id }

func (n *funcNode) Deps() []string { return append([]string(nil), n.deps...) }

func (n *funcNode) Kind() NodeKind { return n.kind }

func (n *funcNode) Run(ctx context.Context, st *State) error {
	if n.fn == nil {
		return nil
	}
	return n.fn(ctx, st)
}

// SimulateEmbedding deterministically emits a 1024-dimensional intent vector and
// preserves BaselineVersion for future anti_drift integration.
func SimulateEmbedding(st *State) error {
	if st == nil {
		return ErrInvalidNode
	}
	if cap(st.IntentVector) < 1024 {
		st.IntentVector = make([]float32, 1024)
	} else {
		st.IntentVector = st.IntentVector[:1024]
	}
	seed := hashState(st)
	if seed == 0 {
		seed = 2166136261
	}
	for i := 0; i < 1024; i++ {
		seed ^= uint32(i + 1)
		seed *= 16777619
		st.IntentVector[i] = float32(seed%1000) / 1000.0
	}
	return nil
}

func hashState(st *State) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(st.SessionID))
	_, _ = h.Write([]byte(st.Prompt))
	_, _ = h.Write([]byte(st.LLMOutput))
	for _, ev := range st.Events {
		_, _ = h.Write([]byte(ev.UserID))
		_, _ = h.Write([]byte(ev.ItemID))
		_, _ = h.Write([]byte(ev.CategoryID))
		_, _ = h.Write([]byte(ev.Action))
	}
	return h.Sum32()
}
