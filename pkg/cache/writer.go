package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/redis/go-redis/v9"
	"math"
	"time"

	"go-rec/pkg/pool"
)

const (
	IntentVectorDim   = 1024
	IntentVectorBytes = 4096
	defaultIntentTTL  = 24 * time.Hour
)

var (
	ErrInvalidIntent = errors.New("cache: invalid intent")
	ErrStaleIntent   = errors.New("cache: stale intent")
)

// IntentWriter persists slow-track intent vectors into online fast-track state.
type IntentWriter interface {
	WriteIntent(ctx context.Context, sessionID string, vector []float32, version int64) error
}

// RedisIntentWriterOptions configures Redis-backed intent writes.
type RedisIntentWriterOptions struct {
	Client redis.UniversalClient
	TTL    time.Duration
	Pool   *pool.Bytes4KPool
}

type RedisIntentWriter struct {
	client redis.UniversalClient
	ttlMS  int64
	pool   *pool.Bytes4KPool
}

var intentWriteScript = redis.NewScript(`
local old = redis.call('HGET', KEYS[1], 'version')
if old and tonumber(old) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'vector', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

func IntentKey(sessionID string) string {
	return "rk:intent:{" + sessionID + "}"
}

func NewRedisIntentWriter(opts RedisIntentWriterOptions) (*RedisIntentWriter, error) {
	if opts.Client == nil {
		return nil, ErrInvalidIntent
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = defaultIntentTTL
	}
	p := opts.Pool
	if p == nil {
		p = pool.NewBytes4KPool()
	}
	return &RedisIntentWriter{client: opts.Client, ttlMS: int64(ttl / time.Millisecond), pool: p}, nil
}

func MarshalIntentVectorInto(dst *[IntentVectorBytes]byte, vector []float32) error {
	if dst == nil || len(vector) != IntentVectorDim {
		return ErrInvalidIntent
	}
	for i := 0; i < IntentVectorDim; i++ {
		binary.LittleEndian.PutUint32(dst[i*4:i*4+4], math.Float32bits(vector[i]))
	}
	return nil
}

func UnmarshalIntentVector(raw []byte, out []float32) error {
	if len(raw) != IntentVectorBytes || len(out) < IntentVectorDim {
		return ErrCorruptIntent
	}
	for i := 0; i < IntentVectorDim; i++ {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4 : i*4+4]))
	}
	return nil
}

func (w *RedisIntentWriter) WriteIntent(ctx context.Context, sessionID string, vector []float32, version int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if w == nil || w.client == nil || w.pool == nil || sessionID == "" || len(vector) != IntentVectorDim || version <= 0 {
		return ErrInvalidIntent
	}
	return w.pool.With(ctx, func(buf *[IntentVectorBytes]byte) error {
		if err := MarshalIntentVectorInto(buf, vector); err != nil {
			return err
		}
		vectorRaw := string(buf[:])
		res, err := intentWriteScript.Run(ctx, w.client, []string{IntentKey(sessionID)}, version, vectorRaw, w.ttlMS).Int()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if res == 0 {
			return ErrStaleIntent
		}
		return nil
	})
}
