package agent

import (
	"context"
	"time"

	"go-rec/pkg/cache"
)

const defaultStateSyncTimeout = 2 * time.Second

type StateSyncNodeOptions struct {
	ID           string
	Deps         []string
	Writer       cache.IntentWriter
	Timeout      time.Duration
	Async        bool
	Logger       interface{ Printf(string, ...any) }
	LogPublisher LogPublisher
}

type stateSyncNode struct {
	id           string
	deps         []string
	writer       cache.IntentWriter
	timeout      time.Duration
	async        bool
	logger       interface{ Printf(string, ...any) }
	logPublisher LogPublisher
}

func NewStateSyncNode(opts StateSyncNodeOptions) Node {
	id := opts.ID
	if id == "" {
		id = "state_sync"
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultStateSyncTimeout
	}
	return &stateSyncNode{
		id:           id,
		deps:         append([]string(nil), opts.Deps...),
		writer:       opts.Writer,
		timeout:      timeout,
		async:        opts.Async,
		logger:       opts.Logger,
		logPublisher: opts.LogPublisher,
	}
}

func (n *stateSyncNode) ID() string { return n.id }

func (n *stateSyncNode) Deps() []string { return append([]string(nil), n.deps...) }

func (n *stateSyncNode) Kind() NodeKind { return NodeSymbol }

func clearReflectionMetadata(st *State) {
	if st == nil || len(st.Metadata) == 0 {
		return
	}
	delete(st.Metadata, MetadataPreviousIntentText)
	delete(st.Metadata, MetadataReflectionActive)
}

func hasActiveReflection(st *State) bool {
	if st == nil || len(st.Metadata) == 0 {
		return false
	}
	return st.Metadata[MetadataReflectionActive] == "true"
}

func (n *stateSyncNode) Run(ctx context.Context, st *State) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if n == nil || n.writer == nil || st == nil || st.SessionID == "" || len(st.IntentVector) != cache.IntentVectorDim || st.BaselineVersion <= 0 {
		return ErrInvalidNode
	}
	clearReflection := hasActiveReflection(st)
	sessionID := st.SessionID
	vector := make([]float32, len(st.IntentVector))
	copy(vector, st.IntentVector)
	version := st.BaselineVersion
	write := func() error {
		base := context.WithoutCancel(ctx)
		writeCtx, cancel := context.WithTimeout(base, n.timeout)
		defer cancel()
		if err := n.writer.WriteIntent(writeCtx, sessionID, vector, version); err != nil {
			return err
		}
		publishDAGLog(writeCtx, n.logPublisher, sessionID, "[Redis写入成功] 慢轨意图向量已回写 Redis")
		if clearReflection {
			clearReflectionMetadata(st)
		}
		return nil
	}
	if !n.async || clearReflection {
		return write()
	}
	go func() {
		if err := write(); err != nil && n.logger != nil {
			n.logger.Printf("state sync write failed: %v", err)
		}
	}()
	return nil
}
