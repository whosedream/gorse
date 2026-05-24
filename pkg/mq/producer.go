package mq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var ErrInvalidProducerOptions = errors.New("mq: invalid kafka producer options")

// Producer publishes slow-track behavior events without coupling callers to Kafka.
type Producer interface {
	Publish(ctx context.Context, ev Event) error
	Close() error
}

type KafkaProducerOptions struct {
	Brokers      []string
	Topic        string
	Async        bool
	BatchSize    int
	BatchTimeout time.Duration
	RequiredAcks int
}

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type KafkaProducer struct {
	writer kafkaMessageWriter
	topic  string
}

func NewKafkaProducer(opts KafkaProducerOptions) (*KafkaProducer, error) {
	if len(opts.Brokers) == 0 || strings.TrimSpace(opts.Topic) == "" {
		return nil, ErrInvalidProducerOptions
	}
	w := newKafkaWriter(opts)
	return &KafkaProducer{writer: w, topic: opts.Topic}, nil
}

func NewKafkaProducerFromEnv() (*KafkaProducer, error) {
	return NewKafkaProducer(KafkaProducerOptionsFromEnv())
}

func KafkaProducerOptionsFromEnv() KafkaProducerOptions {
	brokers := splitCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = "rk_behavior_log"
	}
	return KafkaProducerOptions{
		Brokers:      brokers,
		Topic:        topic,
		Async:        true,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: int(kafka.RequireAll),
	}
}

func newKafkaWriter(opts KafkaProducerOptions) *kafka.Writer {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.BatchTimeout <= 0 {
		opts.BatchTimeout = 10 * time.Millisecond
	}
	if opts.RequiredAcks == 0 {
		opts.RequiredAcks = int(kafka.RequireAll)
	}
	return &kafka.Writer{
		Addr:         kafka.TCP(opts.Brokers...),
		Topic:        opts.Topic,
		Async:        true,
		BatchSize:    opts.BatchSize,
		BatchTimeout: opts.BatchTimeout,
		RequiredAcks: kafka.RequiredAcks(opts.RequiredAcks),
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, ev Event) error {
	if p == nil || p.writer == nil {
		return ErrInvalidProducerOptions
	}
	value, err := EncodeEvent(ev)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(ev.SessionID), Value: value})
}

func (p *KafkaProducer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func EncodeEvent(ev Event) ([]byte, error) {
	return json.Marshal(ev)
}

func DecodeEvent(raw []byte) (Event, error) {
	var ev Event
	err := json.Unmarshal(raw, &ev)
	return ev, err
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
