package mq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisProducer publishes behavior events to a Redis channel so the
// slow-track agent process can consume them without Kafka.
type RedisProducer struct {
	client  redis.Cmdable
	channel string
}

// NewRedisProducer creates a Producer that publishes events as JSON to
// the given Redis channel.
func NewRedisProducer(client redis.Cmdable, channel string) *RedisProducer {
	if channel == "" {
		channel = "behavior:events"
	}
	return &RedisProducer{client: client, channel: channel}
}

func (p *RedisProducer) Publish(ctx context.Context, ev Event) error {
	if p == nil || p.client == nil {
		return ErrInvalidProducerOptions
	}
	data, err := EncodeEvent(ev)
	if err != nil {
		return err
	}
	return p.client.Publish(ctx, p.channel, string(data)).Err()
}

func (p *RedisProducer) Close() error { return nil }
