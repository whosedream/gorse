package mq

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeEventSource struct {
	messages chan Message
}

func newFakeEventSource(capacity int) *fakeEventSource {
	return &fakeEventSource{messages: make(chan Message, capacity)}
}

func (s *fakeEventSource) Fetch(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case msg, ok := <-s.messages:
		if !ok {
			return Message{}, io.EOF
		}
		return msg, nil
	}
}

type commitProbe struct {
	mu    sync.Mutex
	count int
	order []string
}

func (p *commitProbe) commit(label string) func(context.Context) error {
	return func(context.Context) error {
		p.mu.Lock()
		p.count++
		p.order = append(p.order, label)
		p.mu.Unlock()
		return nil
	}
}

func (p *commitProbe) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func runWindowConsumer(t *testing.T, c *WindowConsumer, ctx context.Context, out chan<- Batch) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, out) }()
	return done
}

func TestWindowConsumerAggregatesBySessionMaxBatchAndBaseline(t *testing.T) {
	t.Parallel()

	src := newFakeEventSource(8)
	probe := &commitProbe{}
	for i := 0; i < 5; i++ {
		src.messages <- Message{Event: Event{SessionID: "s-1", UserID: "u-1", ItemID: "sku", Timestamp: int64(10 + i), Action: "click"}, commit: probe.commit("m")}
	}
	out := make(chan Batch, 1)
	consumer, err := NewWindowConsumer(src, Options{MaxBatch: 5, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewWindowConsumer returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runWindowConsumer(t, consumer, ctx, out)

	got := collectBatches(t, out, 1)[0]
	if got.SessionID != "s-1" || got.UserID != "u-1" || len(got.Events) != 5 || got.BaselineVersion != 14 {
		t.Fatalf("unexpected batch: %+v", got)
	}
	if probe.Count() != 5 {
		t.Fatalf("commit count before/after delivery=%d want=5", probe.Count())
	}
	cancel()
	<-done
}

func TestWindowConsumerFlushIntervalFlushesFromFirstSessionMessage(t *testing.T) {
	t.Parallel()

	src := newFakeEventSource(2)
	probe := &commitProbe{}
	out := make(chan Batch, 1)
	consumer, err := NewWindowConsumer(src, Options{MaxBatch: 10, FlushInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewWindowConsumer returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runWindowConsumer(t, consumer, ctx, out)

	src.messages <- Message{Event: Event{SessionID: "s-interval", UserID: "u", Timestamp: 7, Action: "view"}, commit: probe.commit("interval")}
	got := collectBatches(t, out, 1)[0]
	if got.SessionID != "s-interval" || len(got.Events) != 1 || got.BaselineVersion != 7 {
		t.Fatalf("unexpected interval batch: %+v", got)
	}
	if probe.Count() != 1 {
		t.Fatalf("commit count=%d want=1", probe.Count())
	}
	cancel()
	<-done
}

func TestWindowConsumerCommitsOnlyAfterSuccessfulOutDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepareOut func() chan Batch
		cancelNow  bool
		wantCommit int
	}{
		{
			name: "successful send commits",
			prepareOut: func() chan Batch {
				return make(chan Batch, 1)
			},
			wantCommit: 1,
		},
		{
			name: "canceled blocked send does not commit",
			prepareOut: func() chan Batch {
				out := make(chan Batch)
				return out
			},
			cancelNow:  true,
			wantCommit: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src := newFakeEventSource(1)
			probe := &commitProbe{}
			src.messages <- Message{Event: Event{SessionID: "s", UserID: "u", Timestamp: 1, Action: "click"}, commit: probe.commit("one")}
			consumer, err := NewWindowConsumer(src, Options{MaxBatch: 1, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("NewWindowConsumer returned error: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			out := tt.prepareOut()
			done := runWindowConsumer(t, consumer, ctx, out)
			if tt.cancelNow {
				cancel()
			} else {
				_ = collectBatches(t, out, 1)
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("consumer did not stop")
			}
			if probe.Count() != tt.wantCommit {
				t.Fatalf("commit count=%d want=%d", probe.Count(), tt.wantCommit)
			}
		})
	}
}

func TestKafkaEventSourceSkipsAndCommitsMalformedJSON(t *testing.T) {
	t.Parallel()

	reader := &fakeKafkaReader{messages: []kafka.Message{{Value: []byte("{malformed")}, {Value: mustEncodeEvent(t, Event{SessionID: "s", UserID: "u", Timestamp: 3})}}}
	src := NewKafkaEventSource(reader)
	msg, err := src.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if msg.Event.SessionID != "s" || msg.Event.Timestamp != 3 {
		t.Fatalf("unexpected event after poison skip: %+v", msg.Event)
	}
	if reader.commits != 1 {
		t.Fatalf("malformed message commits=%d want=1", reader.commits)
	}
	if err := msg.commit(context.Background()); err != nil {
		t.Fatalf("message commit returned error: %v", err)
	}
	if reader.commits != 2 {
		t.Fatalf("total commits=%d want=2", reader.commits)
	}
}

func TestWindowConsumerContextCanceledFastPath(t *testing.T) {
	t.Parallel()

	src := newFakeEventSource(0)
	consumer, err := NewWindowConsumer(src, Options{MaxBatch: 1, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewWindowConsumer returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = consumer.Consume(ctx, make(chan Batch))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

type fakeKafkaReader struct {
	messages []kafka.Message
	idx      int
	commits  int
}

func (r *fakeKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	default:
	}
	if r.idx >= len(r.messages) {
		return kafka.Message{}, io.EOF
	}
	msg := r.messages[r.idx]
	r.idx++
	return msg, nil
}

func (r *fakeKafkaReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	return nil
}

func (r *fakeKafkaReader) Close() error { return nil }

func mustEncodeEvent(t *testing.T, ev Event) []byte {
	t.Helper()
	b, err := EncodeEvent(ev)
	if err != nil {
		t.Fatalf("EncodeEvent returned error: %v", err)
	}
	return b
}
