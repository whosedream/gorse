package agent

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const SSELogChannelPrefix = "sse_log:"

type LogPublisher interface {
	PublishLog(ctx context.Context, sessionID string, message string) error
}

type RedisLogPublisher struct {
	client redis.UniversalClient
}

func NewRedisLogPublisher(client redis.UniversalClient) LogPublisher {
	if client == nil {
		return noopLogPublisher{}
	}
	return RedisLogPublisher{client: client}
}

func (p RedisLogPublisher) PublishLog(ctx context.Context, sessionID string, message string) error {
	if p.client == nil || sessionID == "" || message == "" {
		return nil
	}
	return p.client.Publish(ctx, SSELogChannelPrefix+sessionID, message).Err()
}

type noopLogPublisher struct{}

func (noopLogPublisher) PublishLog(context.Context, string, string) error { return nil }

func publishDAGLog(ctx context.Context, publisher LogPublisher, sessionID string, message string) {
	if publisher == nil || sessionID == "" || message == "" {
		return
	}
	_ = publisher.PublishLog(ctx, sessionID, message)
}
