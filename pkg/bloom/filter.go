package bloom

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Filter defines the bloom filter interface.
type Filter interface {
	Add(key string) bool
	Contains(key string) bool
}

const (
	defaultRedisKeyPrefix = "bloom:profile:"
	defaultRedisTTL       = 0 // no expiry; managed by caller
)

// RedisBloomFilter implements Filter with Redis bitfield backend.
// Supports Redis persistence for cross-instance consistency.
type RedisBloomFilter struct {
	client    redis.UniversalClient
	redisKey  string
	m         uint   // bit array size
	k         uint   // number of hash functions
	localMu   sync.RWMutex
	localKeys map[string]struct{} // fast local check
}

type Options struct {
	Client redis.UniversalClient
	M      uint // bit array size (recommended: n * ln(2) / ln(1/p))
	K      uint // hash functions (recommended: m/n * ln(2))
	Key    string // Redis key name
}

// NewRedisBloomFilter creates a bloom filter backed by Redis.
// M and K are tuned for < 1% false positive rate at expected capacity.
func NewRedisBloomFilter(opts Options) (*RedisBloomFilter, error) {
	if opts.Client == nil {
		return nil, ErrBloomInvalid
	}
	if opts.M == 0 {
		opts.M = 1 << 20 // ~1M bits
	}
	if opts.K == 0 {
		opts.K = 7 // optimal for m=1M, n=100K
	}
	if opts.Key == "" {
		opts.Key = defaultRedisKeyPrefix + "default"
	}
	return &RedisBloomFilter{
		client:    opts.Client,
		redisKey:  opts.Key,
		m:         opts.M,
		k:         opts.K,
		localKeys: make(map[string]struct{}),
	}, nil
}

// Add inserts a key into the bloom filter. Returns true if the key was newly added.
func (f *RedisBloomFilter) Add(key string) bool {
	if f == nil || f.client == nil || key == "" {
		return false
	}
	f.localMu.Lock()
	if _, exists := f.localKeys[key]; exists {
		f.localMu.Unlock()
		return false
	}
	f.localKeys[key] = struct{}{}
	f.localMu.Unlock()

	ctx := context.Background()
	offsets := f.getOffsets(key)
	for _, off := range offsets {
		if err := f.client.SetBit(ctx, f.redisKey, int64(off), 1).Err(); err != nil {
			// On Redis error, local cache still marks it.
			return true
		}
	}
	return true
}

// Contains checks if a key might exist in the bloom filter.
// Returns false definitely means not present; true means probably present.
func (f *RedisBloomFilter) Contains(key string) bool {
	if f == nil || f.client == nil || key == "" {
		return false
	}
	// Fast local check first.
	f.localMu.RLock()
	_, exists := f.localKeys[key]
	f.localMu.RUnlock()
	if exists {
		return true
	}

	ctx := context.Background()
	offsets := f.getOffsets(key)
	for _, off := range offsets {
		val, err := f.client.GetBit(ctx, f.redisKey, int64(off)).Result()
		if err != nil || val == 0 {
			return false
		}
	}
	return true
}

// getOffsets generates k independent hash positions using double hashing.
func (f *RedisBloomFilter) getOffsets(key string) []uint {
	h := sha256.Sum256([]byte(key))
	h1 := binary.BigEndian.Uint64(h[:8])
	h2 := binary.BigEndian.Uint64(h[8:16])

	offsets := make([]uint, f.k)
	for i := uint64(0); i < uint64(f.k); i++ {
		offsets[i] = uint((h1 + i*h2) % uint64(f.m))
	}
	return offsets
}

var ErrBloomInvalid = &bloomError{"bloom: invalid arguments"}

type bloomError struct{ msg string }

func (e *bloomError) Error() string { return e.msg }
