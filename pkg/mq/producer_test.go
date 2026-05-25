package mq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type captureWriter struct {
	messages []kafka.Message
	closed   bool
}

func (w *captureWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.messages = append(w.messages, msgs...)
	return nil
}

func (w *captureWriter) Close() error {
	w.closed = true
	return nil
}

func TestKafkaProducerPublishesSessionKeyAndJSONValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ev   Event
	}{
		{
			name: "happy path uses SessionID as key and preserves exp_id",
			ev:   Event{SessionID: "s-1", UserID: "u-1", ItemID: "sku-1", CategoryID: "cat", Timestamp: 42, Action: "click", ExpID: "baseline"},
		},
		{
			name: "empty payload remains valid JSON and empty key",
			ev:   Event{},
		},
		{
			name: "long malicious fields stay JSON data",
			ev:   Event{SessionID: "s-2", UserID: "u-2", ItemID: string(make([]byte, 4096)), CategoryID: "<script>", Timestamp: 99, Action: "click\ncart"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			writer := &captureWriter{}
			producer := &KafkaProducer{writer: writer, topic: "rk_behavior_log"}

			if err := producer.Publish(context.Background(), tt.ev); err != nil {
				t.Fatalf("Publish returned error: %v", err)
			}
			if len(writer.messages) != 1 {
				t.Fatalf("messages len=%d want=1", len(writer.messages))
			}
			msg := writer.messages[0]
			if string(msg.Key) != tt.ev.SessionID {
				t.Fatalf("key=%q want session=%q", string(msg.Key), tt.ev.SessionID)
			}
			var got Event
			if err := json.Unmarshal(msg.Value, &got); err != nil {
				t.Fatalf("value is not Event JSON: %v raw=%q", err, string(msg.Value))
			}
			if got.SessionID != tt.ev.SessionID || got.UserID != tt.ev.UserID || got.ItemID != tt.ev.ItemID || got.CategoryID != tt.ev.CategoryID || got.Timestamp != tt.ev.Timestamp || got.Action != tt.ev.Action || got.ExpID != tt.ev.ExpID {
				t.Fatalf("decoded event mismatch: got=%+v want=%+v", got, tt.ev)
			}
		})
	}
}

func TestKafkaProducerWriterConfigDefaultsAsyncTrue(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "b1:9092,b2:9092")
	t.Setenv("KAFKA_TOPIC", "rk_behavior_log")
	t.Setenv("KAFKA_GROUP_ID", "rk_slow_track")

	opts := KafkaProducerOptionsFromEnv()
	if len(opts.Brokers) != 2 || opts.Topic != "rk_behavior_log" || !opts.Async {
		t.Fatalf("env options mismatch: %+v", opts)
	}

	w := newKafkaWriter(KafkaProducerOptions{Brokers: []string{"localhost:9092"}, Topic: "rk_behavior_log", BatchSize: 7, BatchTimeout: 5 * time.Millisecond})
	if !w.Async {
		t.Fatalf("writer Async=false, want true")
	}
	if w.Topic != "rk_behavior_log" || w.BatchSize != 7 || w.BatchTimeout != 5*time.Millisecond {
		t.Fatalf("writer config mismatch: topic=%q batch=%d timeout=%s", w.Topic, w.BatchSize, w.BatchTimeout)
	}
}

func TestKafkaProducerCloseFlushesWriter(t *testing.T) {
	t.Parallel()

	writer := &captureWriter{}
	producer := &KafkaProducer{writer: writer, topic: "rk_behavior_log"}
	if err := producer.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !writer.closed {
		t.Fatal("writer was not closed")
	}
}
