package mq

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var ErrInvalidOptions = errors.New("invalid consumer options")

// Event is a raw behavior event pumped from the async message source.
type Event struct {
	SessionID  string `json:"session_id,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	ItemID     string `json:"item_id,omitempty"`
	CategoryID string `json:"category_id,omitempty"`
	Timestamp  int64  `json:"timestamp,omitempty"`
	Action     string `json:"action,omitempty"`
	ExpID      string `json:"exp_id,omitempty"`
}

// Batch is a per-session aggregate. BaselineVersion is the max event timestamp.
type Batch struct {
	SessionID       string
	UserID          string
	Events          []Event
	BaselineVersion int64
}

// Source abstracts Kafka or any in-memory replacement used by tests/local runs.
type Source interface {
	Receive(ctx context.Context) (Event, error)
}

// Options controls batch size and timed flushing.
type Options struct {
	MaxBatch      int
	FlushInterval time.Duration
}

// Consumer is a lightweight high-throughput event pump.
type Consumer struct {
	src           Source
	maxBatch      int
	flushInterval time.Duration
}

type aggregate struct {
	events   []Event
	baseline int64
}

type tryReceiver interface {
	tryReceive() (Event, bool)
}

// NewConsumer validates and constructs a batch consumer.
func NewConsumer(src Source, opts Options) (*Consumer, error) {
	if src == nil || opts.MaxBatch <= 0 || opts.FlushInterval <= 0 {
		return nil, ErrInvalidOptions
	}
	return &Consumer{src: src, maxBatch: opts.MaxBatch, flushInterval: opts.FlushInterval}, nil
}

// Consume groups events by UserID. It flushes on MaxBatch, FlushInterval, source
// EOF, and context cancellation. On cancellation it drains currently buffered
// in-memory events before the final flush so accepted events are not dropped.
func (c *Consumer) Consume(ctx context.Context, out chan<- Batch) error {
	groups := make(map[string]*aggregate)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.drainAvailable(groups)
			flushAll(out, groups)
			return ctx.Err()
		case <-ticker.C:
			flushAll(out, groups)
		default:
		}

		recvCtx, cancel := context.WithTimeout(ctx, c.flushInterval)
		ev, err := c.src.Receive(recvCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				flushAll(out, groups)
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.drainAvailable(groups)
				flushAll(out, groups)
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				flushAll(out, groups)
				return nil
			}
			flushAll(out, groups)
			return err
		}
		if ev.UserID == "" {
			continue
		}
		if addEvent(groups, ev) >= c.maxBatch {
			flushUser(out, groups, ev.UserID)
		}
	}
}

func (c *Consumer) drainAvailable(groups map[string]*aggregate) {
	tr, ok := c.src.(tryReceiver)
	if !ok {
		return
	}
	for {
		ev, ok := tr.tryReceive()
		if !ok {
			return
		}
		if ev.UserID != "" {
			addEvent(groups, ev)
		}
	}
}

func addEvent(groups map[string]*aggregate, ev Event) int {
	agg := groups[ev.UserID]
	if agg == nil {
		agg = &aggregate{}
		groups[ev.UserID] = agg
	}
	agg.events = append(agg.events, ev)
	if len(agg.events) == 1 || ev.Timestamp > agg.baseline {
		agg.baseline = ev.Timestamp
	}
	return len(agg.events)
}

func flushAll(out chan<- Batch, groups map[string]*aggregate) {
	for userID := range groups {
		flushUser(out, groups, userID)
	}
}

func flushUser(out chan<- Batch, groups map[string]*aggregate, userID string) {
	agg := groups[userID]
	if agg == nil || len(agg.events) == 0 {
		delete(groups, userID)
		return
	}
	events := make([]Event, len(agg.events))
	copy(events, agg.events)
	out <- Batch{UserID: userID, Events: events, BaselineVersion: agg.baseline}
	delete(groups, userID)
}

// MemorySource is a channel-backed Source for tests and local integration.
type MemorySource struct {
	C <-chan Event
}

// Receive blocks until an event, source close, or context cancellation.
func (m MemorySource) Receive(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	case ev, ok := <-m.C:
		if !ok {
			return Event{}, io.EOF
		}
		return ev, nil
	}
}

func (m MemorySource) tryReceive() (Event, bool) {
	select {
	case ev, ok := <-m.C:
		if !ok {
			return Event{}, false
		}
		return ev, true
	default:
		return Event{}, false
	}
}

// Message carries a decoded event and an optional acknowledgement callback.
type Message struct {
	Event  Event
	commit func(context.Context) error
}

// EventSource abstracts committed sources such as Kafka.
type EventSource interface {
	Fetch(ctx context.Context) (Message, error)
}

type kafkaRawReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type KafkaEventSource struct {
	reader kafkaRawReader
}

func NewKafkaEventSource(reader kafkaRawReader) *KafkaEventSource {
	return &KafkaEventSource{reader: reader}
}

func NewKafkaEventSourceFromEnv() (*KafkaEventSource, error) {
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = "rk_behavior_log"
	}
	groupID := strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID"))
	if groupID == "" {
		groupID = "rk_slow_track"
	}
	return &KafkaEventSource{reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID})}, nil
}

func (s *KafkaEventSource) Fetch(ctx context.Context) (Message, error) {
	if s == nil || s.reader == nil {
		return Message{}, ErrInvalidOptions
	}
	for {
		raw, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return Message{}, err
		}
		ev, err := DecodeEvent(raw.Value)
		if err != nil {
			if commitErr := s.reader.CommitMessages(ctx, raw); commitErr != nil {
				return Message{}, commitErr
			}
			continue
		}
		return Message{Event: ev, commit: func(ctx context.Context) error { return s.reader.CommitMessages(ctx, raw) }}, nil
	}
}

func (s *KafkaEventSource) Close() error {
	if s == nil || s.reader == nil {
		return nil
	}
	return s.reader.Close()
}

type WindowConsumer struct {
	src           EventSource
	maxBatch      int
	flushInterval time.Duration
}

type windowAggregate struct {
	sessionID string
	userID    string
	events    []Event
	commits   []func(context.Context) error
	baseline  int64
	firstSeen time.Time
}

func NewWindowConsumer(src EventSource, opts Options) (*WindowConsumer, error) {
	if src == nil || opts.MaxBatch <= 0 || opts.FlushInterval <= 0 {
		return nil, ErrInvalidOptions
	}
	return &WindowConsumer{src: src, maxBatch: opts.MaxBatch, flushInterval: opts.FlushInterval}, nil
}

func (c *WindowConsumer) Consume(ctx context.Context, out chan<- Batch) error {
	groups := make(map[string]*windowAggregate)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if err := c.flushExpired(ctx, out, groups, now); err != nil {
				return err
			}
		default:
		}

		recvCtx, cancel := context.WithTimeout(ctx, c.flushInterval)
		msg, err := c.src.Fetch(recvCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				if err := c.flushExpired(ctx, out, groups, time.Now()); err != nil {
					return err
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				return c.flushAllWindow(ctx, out, groups)
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}
		key := msg.Event.SessionID
		if key == "" {
			key = msg.Event.UserID
		}
		if key == "" {
			if msg.commit != nil {
				if err := msg.commit(ctx); err != nil {
					return err
				}
			}
			continue
		}
		if c.addWindowEvent(groups, key, msg) >= c.maxBatch {
			if err := c.flushWindow(ctx, out, groups, key); err != nil {
				return err
			}
		}
	}
}

func (c *WindowConsumer) addWindowEvent(groups map[string]*windowAggregate, key string, msg Message) int {
	agg := groups[key]
	if agg == nil {
		agg = &windowAggregate{sessionID: msg.Event.SessionID, userID: msg.Event.UserID, firstSeen: time.Now()}
		groups[key] = agg
	}
	if agg.sessionID == "" {
		agg.sessionID = msg.Event.SessionID
	}
	if agg.userID == "" {
		agg.userID = msg.Event.UserID
	}
	agg.events = append(agg.events, msg.Event)
	agg.commits = append(agg.commits, msg.commit)
	if len(agg.events) == 1 || msg.Event.Timestamp > agg.baseline {
		agg.baseline = msg.Event.Timestamp
	}
	return len(agg.events)
}

func (c *WindowConsumer) flushExpired(ctx context.Context, out chan<- Batch, groups map[string]*windowAggregate, now time.Time) error {
	for key, agg := range groups {
		if !agg.firstSeen.IsZero() && now.Sub(agg.firstSeen) >= c.flushInterval {
			if err := c.flushWindow(ctx, out, groups, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *WindowConsumer) flushAllWindow(ctx context.Context, out chan<- Batch, groups map[string]*windowAggregate) error {
	for key := range groups {
		if err := c.flushWindow(ctx, out, groups, key); err != nil {
			return err
		}
	}
	return nil
}

func (c *WindowConsumer) flushWindow(ctx context.Context, out chan<- Batch, groups map[string]*windowAggregate, key string) error {
	agg := groups[key]
	if agg == nil || len(agg.events) == 0 {
		delete(groups, key)
		return nil
	}
	events := make([]Event, len(agg.events))
	copy(events, agg.events)
	batch := Batch{SessionID: agg.sessionID, UserID: agg.userID, Events: events, BaselineVersion: agg.baseline}
	select {
	case out <- batch:
		for _, commit := range agg.commits {
			if commit != nil {
				if err := commit(ctx); err != nil {
					return err
				}
			}
		}
		delete(groups, key)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
