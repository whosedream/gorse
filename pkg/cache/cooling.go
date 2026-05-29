package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultCoolingWindow = 60 * time.Second
	coolingKeyPrefix     = "cooling:"
	coolingKeyTTL        = 300 * time.Second
)

// CoolingChecker controls LLM inference frequency per session.
//同一 session 在冷却窗口内不重复调用 LLM。
type CoolingChecker interface {
	IsCooled(ctx context.Context, sessionID string) (bool, error)
	MarkCalled(ctx context.Context, sessionID string) error
}

// RedisCoolingChecker implements CoolingChecker backed by Redis sorted set.
// Each sessionID maps to a sorted set with score = unix timestamp (nanoseconds).
type RedisCoolingChecker struct {
	client redis.UniversalClient
	window time.Duration
}

// RedisCoolingCheckerOptions configures the cooling checker.
type RedisCoolingCheckerOptions struct {
	Client redis.UniversalClient
	Window time.Duration
}

func NewRedisCoolingChecker(opts RedisCoolingCheckerOptions) (*RedisCoolingChecker, error) {
	if opts.Client == nil {
		return nil, ErrInvalidIntent
	}
	w := opts.Window
	if w <= 0 {
		w = defaultCoolingWindow
	}
	return &RedisCoolingChecker{client: opts.Client, window: w}, nil
}

func coolingKey(sessionID string) string {
	return coolingKeyPrefix + sessionID
}

// IsCooled returns true if the session is NOT in cooling period (i.e. safe to call LLM).
func (c *RedisCoolingChecker) IsCooled(ctx context.Context, sessionID string) (bool, error) {
	if c == nil || c.client == nil || sessionID == "" {
		return true, nil
	}
	val, err := c.client.ZRevRangeWithScores(ctx, coolingKey(sessionID), 0, 0).Result()
	if err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		// On error, allow LLM call (fail-open).
		return true, nil
	}
	if len(val) == 0 {
		return true, nil
	}
	lastCall := time.Unix(0, int64(val[0].Score))
	if time.Since(lastCall) >= c.window {
		return true, nil
	}
	return false, nil
}

// MarkCalled records the current timestamp for the session and sets key TTL.
func (c *RedisCoolingChecker) MarkCalled(ctx context.Context, sessionID string) error {
	if c == nil || c.client == nil || sessionID == "" {
		return nil
	}
	key := coolingKey(sessionID)
	now := time.Now().UnixNano()
	pipe := c.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: now})
	pipe.Expire(ctx, key, coolingKeyTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}
