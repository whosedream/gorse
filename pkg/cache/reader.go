package cache

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	ErrIntentNotFound = errors.New("cache: intent not found")
	ErrCorruptIntent  = errors.New("cache: corrupt intent")
)

type IntentReader interface {
	ReadIntent(ctx context.Context, sessionID string, dst []float32) (version int64, err error)
}

type RedisIntentReaderOptions struct {
	Client redis.UniversalClient
}

type RedisIntentReader struct {
	client redis.UniversalClient
}

func NewRedisIntentReader(opts RedisIntentReaderOptions) (*RedisIntentReader, error) {
	if opts.Client == nil {
		return nil, ErrInvalidIntent
	}
	return &RedisIntentReader{client: opts.Client}, nil
}

func (r *RedisIntentReader) ReadIntent(ctx context.Context, sessionID string, dst []float32) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if r == nil || r.client == nil || sessionID == "" || len(dst) < IntentVectorDim {
		return 0, ErrInvalidIntent
	}
	vals, err := r.client.HMGet(ctx, IntentKey(sessionID), "version", "vector").Result()
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, err
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return 0, ErrIntentNotFound
	}
	version, ok := parseRedisInt64(vals[0])
	if !ok || version <= 0 {
		return 0, ErrCorruptIntent
	}
	raw, ok := redisBulkBytes(vals[1])
	if !ok {
		return 0, ErrCorruptIntent
	}
	if err := UnmarshalIntentVector(raw, dst); err != nil {
		return 0, err
	}
	return version, nil
}

func parseRedisInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func redisBulkBytes(v any) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	default:
		return nil, false
	}
}
