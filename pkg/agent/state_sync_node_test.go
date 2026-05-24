package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockIntentWriter struct {
	writes  chan stateSyncWrite
	err     error
	block   bool
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

type stateSyncWrite struct {
	ctx       context.Context
	sessionID string
	vector    []float32
	version   int64
}

func (m *mockIntentWriter) WriteIntent(ctx context.Context, sessionID string, vector []float32, version int64) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.started != nil {
		m.started <- struct{}{}
	}
	if m.release != nil {
		select {
		case <-m.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if m.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if m.writes != nil {
		m.writes <- stateSyncWrite{ctx: ctx, sessionID: sessionID, vector: append([]float32(nil), vector...), version: version}
	}
	return m.err
}

func (m *mockIntentWriter) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type stateSyncCaptureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *stateSyncCaptureLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, format)
}

func (l *stateSyncCaptureLogger) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}

func TestStateSyncNodeRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "Graph Run Async false writes final state",
			run: func(t *testing.T) {
				writer := &mockIntentWriter{writes: make(chan stateSyncWrite, 1)}
				g, err := NewGraph(NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: false, Timeout: time.Second}))
				if err != nil {
					t.Fatalf("NewGraph error = %v", err)
				}
				st := &State{SessionID: "s-sync", IntentVector: testStateSyncVector(), BaselineVersion: 11}
				if err := g.Run(context.Background(), st); err != nil {
					t.Fatalf("Graph Run error = %v", err)
				}
				select {
				case got := <-writer.writes:
					if got.sessionID != "s-sync" || got.version != 11 || len(got.vector) != 1024 {
						t.Fatalf("write = %+v len=%d", got, len(got.vector))
					}
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for sync write")
				}
			},
		},
		{
			name: "Async true uses WithoutCancel after parent cancellation",
			run: func(t *testing.T) {
				started := make(chan struct{}, 1)
				release := make(chan struct{})
				writer := &mockIntentWriter{writes: make(chan stateSyncWrite, 1), started: started, release: release}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: true, Timeout: time.Second})
				ctx, cancel := context.WithCancel(context.Background())
				st := &State{SessionID: "s-async", IntentVector: testStateSyncVector(), BaselineVersion: 12}
				if err := n.Run(ctx, st); err != nil {
					t.Fatalf("Run error = %v", err)
				}
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("writer did not start")
				}
				cancel()
				close(release)
				select {
				case got := <-writer.writes:
					if got.sessionID != "s-async" || got.version != 12 {
						t.Fatalf("write = %+v", got)
					}
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for async write")
				}
			},
		},
		{
			name: "Async write snapshots vector before caller mutation",
			run: func(t *testing.T) {
				started := make(chan struct{}, 1)
				release := make(chan struct{})
				writer := &mockIntentWriter{writes: make(chan stateSyncWrite, 1), started: started, release: release}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: true, Timeout: time.Second})
				vec := testStateSyncVector()
				st := &State{SessionID: "s-snapshot", IntentVector: vec, BaselineVersion: 16}
				if err := n.Run(context.Background(), st); err != nil {
					t.Fatalf("Run error = %v", err)
				}
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("writer did not start")
				}
				vec[0] = 999
				st.IntentVector[1] = 888
				close(release)
				select {
				case got := <-writer.writes:
					if got.vector[0] == 999 || got.vector[1] == 888 {
						t.Fatalf("async writer observed caller vector mutation: first=%v second=%v", got.vector[0], got.vector[1])
					}
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for async write")
				}
			},
		},
		{
			name: "Async writer error is logged but not returned",
			run: func(t *testing.T) {
				boom := errors.New("redis down")
				logger := &stateSyncCaptureLogger{}
				writer := &mockIntentWriter{err: boom}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: true, Timeout: time.Second, Logger: logger})
				err := n.Run(context.Background(), &State{SessionID: "s-log", IntentVector: testStateSyncVector(), BaselineVersion: 13})
				if err != nil {
					t.Fatalf("Run error = %v, want nil", err)
				}
				deadline := time.After(time.Second)
				for !logger.contains("state sync write failed") {
					select {
					case <-deadline:
						t.Fatal("writer error was not logged")
					default:
						time.Sleep(time.Millisecond)
					}
				}
			},
		},
		{
			name: "Already canceled context returns before writer call",
			run: func(t *testing.T) {
				writer := &mockIntentWriter{writes: make(chan stateSyncWrite, 1)}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: false, Timeout: time.Second})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := n.Run(ctx, &State{SessionID: "s-canceled", IntentVector: testStateSyncVector(), BaselineVersion: 14})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run error = %v, want context.Canceled", err)
				}
				if writer.callCount() != 0 {
					t.Fatalf("writer called %d times after canceled ctx", writer.callCount())
				}
			},
		},
		{
			name: "Sync timeout returns deadline exceeded",
			run: func(t *testing.T) {
				writer := &mockIntentWriter{block: true}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, Async: false, Timeout: 10 * time.Millisecond})
				err := n.Run(context.Background(), &State{SessionID: "s-timeout", IntentVector: testStateSyncVector(), BaselineVersion: 15})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Run error = %v, want context deadline", err)
				}
			},
		},
		{
			name: "Invalid empty state rejected",
			run: func(t *testing.T) {
				writer := &mockIntentWriter{}
				n := NewStateSyncNode(StateSyncNodeOptions{Writer: writer})
				err := n.Run(context.Background(), &State{})
				if !errors.Is(err, ErrInvalidNode) {
					t.Fatalf("Run error = %v, want ErrInvalidNode", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func testStateSyncVector() []float32 {
	v := make([]float32, 1024)
	for i := range v {
		v[i] = float32(i) / 1024
	}
	return v
}
