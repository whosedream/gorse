package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"go-rec/pkg/mq"
)

type fakeModel struct {
	mu    sync.Mutex
	calls int
	err   error
	text  string
}

func (f *fakeModel) Complete(ctx context.Context, prompt string) (string, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if f.err != nil {
		return "", f.err
	}
	return f.text, nil
}

func TestGraphTopologyErrorsTimeoutAndEmbedding(t *testing.T) {
	t.Parallel()

	boom := errors.New("llm boom")
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "拓扑顺序满足依赖并执行 LLM 与符号节点",
			run: func(t *testing.T) {
				order := make([]string, 0, 3)
				model := &fakeModel{text: "intent: phone"}
				g, err := NewGraph(
					SymbolNode("prepare", nil, func(ctx context.Context, st *State) error {
						order = append(order, "prepare")
						st.Prompt = "用户点击手机"
						return nil
					}),
					LLMNode("infer", []string{"prepare"}, model),
					SymbolNode("embed", []string{"infer"}, func(ctx context.Context, st *State) error {
						order = append(order, "embed")
						return SimulateEmbedding(st)
					}),
				)
				if err != nil {
					t.Fatalf("NewGraph returned error: %v", err)
				}
				st := &State{SessionID: "s1", BaselineVersion: 42}
				if err := g.Run(context.Background(), st); err != nil {
					t.Fatalf("Run returned error: %v", err)
				}
				if !reflect.DeepEqual(order, []string{"prepare", "embed"}) {
					t.Fatalf("unexpected symbol order: %v", order)
				}
				if st.LLMOutput != "intent: phone" {
					t.Fatalf("LLMOutput not set: %q", st.LLMOutput)
				}
				if len(st.IntentVector) != 1024 || st.BaselineVersion != 42 {
					t.Fatalf("embedding/baseline invalid: len=%d baseline=%d", len(st.IntentVector), st.BaselineVersion)
				}
			},
		},
		{
			name: "环状依赖返回 ErrCycle",
			run: func(t *testing.T) {
				_, err := NewGraph(
					SymbolNode("a", []string{"b"}, func(context.Context, *State) error { return nil }),
					SymbolNode("b", []string{"a"}, func(context.Context, *State) error { return nil }),
				)
				if !errors.Is(err, ErrCycle) {
					t.Fatalf("expected ErrCycle, got %v", err)
				}
			},
		},
		{
			name: "缺失依赖返回错误",
			run: func(t *testing.T) {
				_, err := NewGraph(SymbolNode("a", []string{"missing"}, func(context.Context, *State) error { return nil }))
				if !errors.Is(err, ErrMissingDependency) {
					t.Fatalf("expected ErrMissingDependency, got %v", err)
				}
			},
		},
		{
			name: "重复 ID 返回错误",
			run: func(t *testing.T) {
				_, err := NewGraph(
					SymbolNode("dup", nil, func(context.Context, *State) error { return nil }),
					SymbolNode("dup", nil, func(context.Context, *State) error { return nil }),
				)
				if !errors.Is(err, ErrDuplicateNode) {
					t.Fatalf("expected ErrDuplicateNode, got %v", err)
				}
			},
		},
		{
			name: "节点 timeout 返回 context deadline",
			run: func(t *testing.T) {
				g, err := NewGraph(SymbolNode("slow", nil, func(ctx context.Context, st *State) error {
					<-ctx.Done()
					return ctx.Err()
				}))
				if err != nil {
					t.Fatalf("NewGraph returned error: %v", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				err = g.Run(ctx, &State{})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected DeadlineExceeded, got %v", err)
				}
			},
		},
		{
			name: "已取消上下文快速返回 context canceled",
			run: func(t *testing.T) {
				called := false
				g, err := NewGraph(SymbolNode("n", nil, func(ctx context.Context, st *State) error { called = true; return nil }))
				if err != nil {
					t.Fatalf("NewGraph returned error: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err = g.Run(ctx, &State{})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context.Canceled, got %v", err)
				}
				if called {
					t.Fatal("node ran after context canceled")
				}
			},
		},
		{
			name: "LLM 节点错误向上传播",
			run: func(t *testing.T) {
				g, err := NewGraph(LLMNode("llm", nil, &fakeModel{err: boom}))
				if err != nil {
					t.Fatalf("NewGraph returned error: %v", err)
				}
				err = g.Run(context.Background(), &State{Prompt: "p"})
				if !errors.Is(err, boom) {
					t.Fatalf("expected boom, got %v", err)
				}
			},
		},
		{
			name: "全空 State 仍可生成 1024 维兜底向量",
			run: func(t *testing.T) {
				st := &State{BaselineVersion: 77}
				if err := SimulateEmbedding(st); err != nil {
					t.Fatalf("SimulateEmbedding returned error: %v", err)
				}
				if len(st.IntentVector) != 1024 || st.BaselineVersion != 77 {
					t.Fatalf("fallback vector invalid: len=%d baseline=%d", len(st.IntentVector), st.BaselineVersion)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestSlowTrackEndToEndBatchToGraphVector(t *testing.T) {
	t.Parallel()

	events := make(chan mq.Event, 2)
	out := make(chan mq.Batch, 1)
	consumer, err := mq.NewConsumer(mq.MemorySource{C: events}, mq.Options{MaxBatch: 2, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewConsumer returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Consume(ctx, out) }()
	events <- mq.Event{UserID: "u-e2e", ItemID: "sku1", CategoryID: "phone", Timestamp: 100, Action: "click"}
	events <- mq.Event{UserID: "u-e2e", ItemID: "sku2", CategoryID: "case", Timestamp: 120, Action: "cart"}

	var batch mq.Batch
	select {
	case batch = <-out:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mq batch")
	}
	cancel()
	<-done

	model := &fakeModel{text: "category=phone brand=deterministic"}
	g, err := NewGraph(
		SymbolNode("prompt", nil, func(ctx context.Context, st *State) error {
			st.Prompt = "聚合用户 " + st.SessionID
			return nil
		}),
		LLMNode("reason", []string{"prompt"}, model),
		SymbolNode("embed", []string{"reason"}, func(ctx context.Context, st *State) error { return SimulateEmbedding(st) }),
	)
	if err != nil {
		t.Fatalf("NewGraph returned error: %v", err)
	}
	st := &State{SessionID: batch.UserID, BaselineVersion: batch.BaselineVersion, Events: batch.Events, Metadata: map[string]string{"source": "mq"}}
	if err := g.Run(context.Background(), st); err != nil {
		t.Fatalf("Graph Run returned error: %v", err)
	}
	if st.SessionID != "u-e2e" || st.BaselineVersion != 120 || len(st.IntentVector) != 1024 {
		t.Fatalf("unexpected final state: session=%s baseline=%d vector=%d", st.SessionID, st.BaselineVersion, len(st.IntentVector))
	}
}
