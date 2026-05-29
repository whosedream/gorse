package mq

import (
	"context"
	"io"

	"github.com/redis/go-redis/v9"
)

// RedisEventSource subscribes to a Redis channel and delivers decoded
// events as Message values. It implements EventSource for WindowConsumer.
type RedisEventSource struct {
	pubsub  *redis.PubSub
	msgChan <-chan *redis.Message
	channel string
}

// NewRedisEventSource creates an EventSource that subscribes to the given
// Redis channel. Each message payload is JSON-decoded into an Event.
func NewRedisEventSource(client redis.UniversalClient, channel string) *RedisEventSource {
	if channel == "" {
		channel = "behavior:events"
	}
	ps := client.Subscribe(context.Background(), channel)
	_, _ = ps.Receive(context.Background()) // wait for subscription confirmation
	return &RedisEventSource{
		pubsub:  ps,
		msgChan: ps.Channel(),
		channel: channel,
	}
}

func (s *RedisEventSource) Fetch(ctx context.Context) (Message, error) {
	if s == nil || s.msgChan == nil {
		return Message{}, ErrInvalidOptions
	}
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case msg, ok := <-s.msgChan:
		if !ok {
			return Message{}, io.EOF
		}
		ev, err := DecodeEvent([]byte(msg.Payload))
		if err != nil {
			// Skip malformed messages.
			return Message{Event: Event{}, commit: nil}, nil
		}
		return Message{Event: ev, commit: nil}, nil
	}
}

func (s *RedisEventSource) Close() error {
	if s == nil || s.pubsub == nil {
		return nil
	}
	return s.pubsub.Close()
}
