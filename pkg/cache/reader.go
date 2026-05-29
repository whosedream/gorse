package cache

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/redis/go-redis/v9"
)

var (
	ErrIntentNotFound = errors.New("cache: intent not found")
	ErrCorruptIntent  = errors.New("cache: corrupt intent")
)

const (
	redisIntentReaderL1MaxItems       = 4096
	redisIntentReaderL1NumCounters    = redisIntentReaderL1MaxItems * 10
	redisIntentReaderL1BufferItems    = 64
	redisIntentReaderL1TTL            = 5 * time.Minute
	redisIntentReaderHotSlots         = 128
	redisIntentReaderHotSlotsMask     = redisIntentReaderHotSlots - 1
	redisIntentReaderHotExpireUnixNil = 0
)

type cachedIntentVector struct {
	sessionID   string
	expiresUnix int64
	version     int64
	vector      [IntentVectorDim]float32
}

type redisIntentReaderHotSlot struct {
	mu          sync.RWMutex
	key         uint64
	sessionID   string
	expiresUnix int64
	version     int64
	vector      [IntentVectorDim]float32
}

type IntentReader interface {
	ReadIntent(ctx context.Context, sessionID string, dst []float32) (version int64, err error)
}

type RedisIntentReaderOptions struct {
	Client redis.UniversalClient
}

type RedisIntentReader struct {
	client redis.UniversalClient
	l1     *ristretto.Cache
	hot    [redisIntentReaderHotSlots]redisIntentReaderHotSlot
}

func NewRedisIntentReader(opts RedisIntentReaderOptions) (*RedisIntentReader, error) {
	if opts.Client == nil {
		return nil, ErrInvalidIntent
	}
	l1, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: redisIntentReaderL1NumCounters,
		MaxCost:     redisIntentReaderL1MaxItems,
		BufferItems: redisIntentReaderL1BufferItems,
	})
	if err != nil {
		return nil, err
	}
	return &RedisIntentReader{client: opts.Client, l1: l1}, nil
}

func (r *RedisIntentReader) ReadIntent(ctx context.Context, sessionID string, dst []float32) (int64, error) {
	if r == nil || r.client == nil || sessionID == "" || len(dst) < IntentVectorDim {
		return 0, ErrInvalidIntent
	}
	cacheKey := intentCacheKey(sessionID)
	now := time.Now().UnixNano()
	if version, ok := r.hotRead(cacheKey, sessionID, now, dst); ok {
		return version, nil
	}
	if r.l1 != nil {
		if v, ok := r.l1.Get(cacheKey); ok {
			if cached, ok := v.(*cachedIntentVector); ok && cached != nil && cached.sessionID == sessionID && cached.expiresUnix > now {
				copy(dst, cached.vector[:])
				r.hotWrite(cacheKey, sessionID, cached.expiresUnix, cached.version, cached.vector[:])
				return cached.version, nil
			}
		}
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
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
	if r.l1 != nil {
		cached := new(cachedIntentVector)
		cached.sessionID = sessionID
		cached.expiresUnix = now + int64(redisIntentReaderL1TTL)
		cached.version = version
		copy(cached.vector[:], dst[:IntentVectorDim])
		r.hotWrite(cacheKey, sessionID, cached.expiresUnix, cached.version, cached.vector[:])
		r.l1.SetWithTTL(cacheKey, cached, 1, redisIntentReaderL1TTL)
	}
	return version, nil
}

func (r *RedisIntentReader) hotRead(key uint64, sessionID string, now int64, dst []float32) (int64, bool) {
	slot := &r.hot[key&redisIntentReaderHotSlotsMask]
	slot.mu.RLock()
	if slot.key != key || slot.sessionID != sessionID || slot.expiresUnix <= now || slot.expiresUnix == redisIntentReaderHotExpireUnixNil {
		slot.mu.RUnlock()
		return 0, false
	}
	version := slot.version
	copy(dst, slot.vector[:])
	slot.mu.RUnlock()
	return version, true
}

func (r *RedisIntentReader) hotWrite(key uint64, sessionID string, expiresUnix int64, version int64, vector []float32) {
	slot := &r.hot[key&redisIntentReaderHotSlotsMask]
	slot.mu.Lock()
	slot.key = key
	slot.sessionID = sessionID
	slot.expiresUnix = expiresUnix
	slot.version = version
	copy(slot.vector[:], vector[:IntentVectorDim])
	slot.mu.Unlock()
}

func intentCacheKey(sessionID string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(sessionID); i++ {
		h ^= uint64(sessionID[i])
		h *= 1099511628211
	}
	return h
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

// --- Profile Cache: bloom filter -> Redis -> LLM ---

const (
	profileCacheKeyPrefix = "profile:"
	profileCacheTTL       = 5 * time.Minute
)

// BloomFilterer is the subset of bloom.Filter needed by ProfileCacheReader.
type BloomFilterer interface {
	Add(key string) bool
	Contains(key string) bool
}

// ProfileCacheReader implements a three-tier read chain:
// 1. Bloom filter check (fast rejection)
// 2. Redis cached intent vector
// 3. Fallback to caller (LLM inference)
type ProfileCacheReader struct {
	client    redis.UniversalClient
	bloom     BloomFilterer
	intentDim int
}

type ProfileCacheReaderOptions struct {
	Client    redis.UniversalClient
	Bloom     BloomFilterer
	IntentDim int
}

func NewProfileCacheReader(opts ProfileCacheReaderOptions) *ProfileCacheReader {
	dim := opts.IntentDim
	if dim <= 0 {
		dim = IntentVectorDim
	}
	return &ProfileCacheReader{
		client:    opts.Client,
		bloom:     opts.Bloom,
		intentDim: dim,
	}
}

// ReadProfile attempts to read a cached profile for sessionID.
// Returns (vector, version, nil) on cache hit, or (nil, 0, ErrMiss) on miss.
func (r *ProfileCacheReader) ReadProfile(ctx context.Context, sessionID string, dst []float32) (version int64, err error) {
	if r == nil || sessionID == "" || len(dst) < r.intentDim {
		return 0, ErrInvalidIntent
	}
	// Tier 1: Bloom filter fast rejection.
	if r.bloom != nil && !r.bloom.Contains(sessionID) {
		return 0, ErrMiss
	}
	// Tier 2: Redis cached profile.
	if r.client == nil {
		return 0, ErrMiss
	}
	vals, err := r.client.HMGet(ctx, profileCacheKey(sessionID), "version", "vector").Result()
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, ErrMiss
	}
	if len(vals) != 2 || vals[0] == nil || vals[1] == nil {
		return 0, ErrMiss
	}
	v, ok := parseRedisInt64(vals[0])
	if !ok || v <= 0 {
		return 0, ErrCorruptIntent
	}
	raw, ok := redisBulkBytes(vals[1])
	if !ok {
		return 0, ErrCorruptIntent
	}
	if err := UnmarshalIntentVector(raw, dst); err != nil {
		return 0, err
	}
	return v, nil
}

// CacheProfile writes a profile to Redis and adds session to bloom filter.
func (r *ProfileCacheReader) CacheProfile(ctx context.Context, sessionID string, vector []float32, version int64) error {
	if r == nil || sessionID == "" || len(vector) < r.intentDim || version <= 0 {
		return ErrInvalidIntent
	}
	// Write to Redis.
	if r.client != nil {
		raw, err := marshalIntentVector(vector[:r.intentDim])
		if err != nil {
			return err
		}
		pipe := r.client.Pipeline()
		pipe.HSet(ctx, profileCacheKey(sessionID), "version", version, "vector", raw)
		pipe.Expire(ctx, profileCacheKey(sessionID), profileCacheTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
	// Add to bloom filter.
	if r.bloom != nil {
		r.bloom.Add(sessionID)
	}
	return nil
}

func profileCacheKey(sessionID string) string {
	return profileCacheKeyPrefix + sessionID
}

// marshalIntentVector serializes a float32 slice to bytes using binary encoding.
func marshalIntentVector(vector []float32) ([]byte, error) {
	buf := make([]byte, len(vector)*4)
	for i, v := range vector {
		bits := math.Float32bits(v)
		buf[i*4] = byte(bits)
		buf[i*4+1] = byte(bits >> 8)
		buf[i*4+2] = byte(bits >> 16)
		buf[i*4+3] = byte(bits >> 24)
	}
	return buf, nil
}

