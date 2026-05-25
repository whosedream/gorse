package cache

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisIntentReader(t *testing.T) {
	t.Parallel()

	t.Run("HMGET reads version and decodes 4096 byte vector into caller dst", func(t *testing.T) {
		t.Parallel()
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}
		want := testIntentVector(33)
		var raw [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&raw, want); err != nil {
			t.Fatalf("MarshalIntentVectorInto error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey("session-a"), "version", int64(100), "vector", string(raw[:])).Err(); err != nil {
			t.Fatalf("HSet error = %v", err)
		}

		dst := make([]float32, IntentVectorDim)
		version, err := reader.ReadIntent(context.Background(), "session-a", dst)
		if err != nil {
			t.Fatalf("ReadIntent error = %v", err)
		}
		if version != 100 {
			t.Fatalf("version = %d, want 100", version)
		}
		for i := range want {
			if dst[i] != want[i] {
				t.Fatalf("dst[%d] = %v, want %v", i, dst[i], want[i])
			}
		}
	})

	tests := []struct {
		name    string
		setup   func(*redis.Client) error
		wantErr error
	}{
		{
			name:    "missing key returns ErrIntentNotFound",
			setup:   func(*redis.Client) error { return nil },
			wantErr: ErrIntentNotFound,
		},
		{
			name: "corrupt vector length returns ErrCorruptIntent",
			setup: func(rdb *redis.Client) error {
				return rdb.HSet(context.Background(), IntentKey("session-a"), "version", int64(101), "vector", "short").Err()
			},
			wantErr: ErrCorruptIntent,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })
			if err := tt.setup(rdb); err != nil {
				t.Fatalf("setup error = %v", err)
			}
			reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
			if err != nil {
				t.Fatalf("NewRedisIntentReader error = %v", err)
			}
			dst := make([]float32, IntentVectorDim)
			_, err = reader.ReadIntent(context.Background(), "session-a", dst)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadIntent error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisIntentReaderL1Cache(t *testing.T) {
	t.Run("hit survives blocked redis and copies stable vector into caller dst", func(t *testing.T) {
		t.Parallel()
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}

		want := testIntentVector(91)
		var raw [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&raw, want); err != nil {
			t.Fatalf("MarshalIntentVectorInto error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey("hot-session"), "version", int64(909), "vector", string(raw[:])).Err(); err != nil {
			t.Fatalf("HSet error = %v", err)
		}

		first := make([]float32, IntentVectorDim)
		version, err := reader.ReadIntent(context.Background(), "hot-session", first)
		if err != nil {
			t.Fatalf("first ReadIntent error = %v", err)
		}
		if version != 909 {
			t.Fatalf("first version = %d, want 909", version)
		}
		if reader.l1 != nil {
			reader.l1.Wait()
		}
		for i := range first {
			first[i] = -1
		}

		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		dst := make([]float32, IntentVectorDim)
		for i := range dst {
			dst[i] = -7
		}
		version, err = reader.ReadIntent(ctx, "hot-session", dst)
		if err != nil {
			t.Fatalf("cached ReadIntent error = %v", err)
		}
		if version != 909 {
			t.Fatalf("cached version = %d, want 909", version)
		}
		for i := range want {
			if dst[i] != want[i] {
				t.Fatalf("cached dst[%d] = %v, want %v", i, dst[i], want[i])
			}
		}
	})

	t.Run("hit path reports zero allocations", func(t *testing.T) {
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}

		want := testIntentVector(93)
		var raw [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&raw, want); err != nil {
			t.Fatalf("MarshalIntentVectorInto error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey("alloc-session"), "version", int64(930), "vector", string(raw[:])).Err(); err != nil {
			t.Fatalf("HSet error = %v", err)
		}

		var dst [IntentVectorDim]float32
		ctx := context.Background()
		if _, err := reader.ReadIntent(ctx, "alloc-session", dst[:]); err != nil {
			t.Fatalf("warmup ReadIntent error = %v", err)
		}
		if reader.l1 != nil {
			reader.l1.Wait()
		}

		allocs := testing.AllocsPerRun(1000, func() {
			version, err := reader.ReadIntent(ctx, "alloc-session", dst[:])
			if err != nil {
				t.Fatalf("ReadIntent error = %v", err)
			}
			if version != 930 {
				t.Fatalf("version = %d, want 930", version)
			}
		})
		if allocs != 0 {
			t.Fatalf("ReadIntent L1 hit allocs/run = %v, want 0", allocs)
		}
	})

	t.Run("hot slot collision misses instead of returning wrong vector", func(t *testing.T) {
		t.Parallel()
		sessionA, sessionB := testRedisIntentReaderHotCollision(t)
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}

		vecA := testIntentVector(94)
		vecB := testIntentVector(95)
		var rawA [IntentVectorBytes]byte
		var rawB [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&rawA, vecA); err != nil {
			t.Fatalf("MarshalIntentVectorInto A error = %v", err)
		}
		if err := MarshalIntentVectorInto(&rawB, vecB); err != nil {
			t.Fatalf("MarshalIntentVectorInto B error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey(sessionA), "version", int64(940), "vector", string(rawA[:])).Err(); err != nil {
			t.Fatalf("HSet A error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey(sessionB), "version", int64(950), "vector", string(rawB[:])).Err(); err != nil {
			t.Fatalf("HSet B error = %v", err)
		}

		var dst [IntentVectorDim]float32
		if version, err := reader.ReadIntent(context.Background(), sessionA, dst[:]); err != nil || version != 940 {
			t.Fatalf("ReadIntent A warmup version = %d, err = %v", version, err)
		}
		if version, err := reader.ReadIntent(context.Background(), sessionB, dst[:]); err != nil || version != 950 {
			t.Fatalf("ReadIntent B warmup version = %d, err = %v", version, err)
		}
		if reader.l1 != nil {
			reader.l1.Wait()
		}
		for i := range dst {
			dst[i] = -11
		}
		version, err := reader.ReadIntent(context.Background(), sessionA, dst[:])
		if err != nil {
			t.Fatalf("ReadIntent A after collision error = %v", err)
		}
		if version != 940 {
			t.Fatalf("ReadIntent A after collision version = %d, want 940", version)
		}
		for i := range vecA {
			if dst[i] != vecA[i] {
				t.Fatalf("ReadIntent A after collision dst[%d] = %v, want %v", i, dst[i], vecA[i])
			}
		}
	})

	t.Run("cache key collision misses instead of returning wrong vector", func(t *testing.T) {
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}

		target := testIntentVector(96)
		wrong := testIntentVector(97)
		var raw [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&raw, target); err != nil {
			t.Fatalf("MarshalIntentVectorInto error = %v", err)
		}
		if err := rdb.HSet(context.Background(), IntentKey("target-session"), "version", int64(960), "vector", string(raw[:])).Err(); err != nil {
			t.Fatalf("HSet target error = %v", err)
		}

		key := intentCacheKey("target-session")
		expiresUnix := time.Now().Add(redisIntentReaderL1TTL).UnixNano()
		reader.hotWrite(key, "other-session", expiresUnix, 970, wrong)
		if reader.l1 != nil {
			cached := &cachedIntentVector{sessionID: "other-session", expiresUnix: expiresUnix, version: 970}
			copy(cached.vector[:], wrong)
			reader.l1.SetWithTTL(key, cached, 1, redisIntentReaderL1TTL)
			reader.l1.Wait()
		}

		var dst [IntentVectorDim]float32
		version, err := reader.ReadIntent(context.Background(), "target-session", dst[:])
		if err != nil {
			t.Fatalf("ReadIntent target error = %v", err)
		}
		if version != 960 {
			t.Fatalf("version = %d, want 960", version)
		}
		for i := range target {
			if dst[i] != target[i] {
				t.Fatalf("dst[%d] = %v, want %v", i, dst[i], target[i])
			}
		}
	})
	t.Run("redis errors do not populate cache", func(t *testing.T) {
		t.Parallel()
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
		if err != nil {
			t.Fatalf("NewRedisIntentReader error = %v", err)
		}

		if err := rdb.HSet(context.Background(), IntentKey("bad-session"), "version", int64(910), "vector", "short").Err(); err != nil {
			t.Fatalf("HSet corrupt error = %v", err)
		}
		dst := make([]float32, IntentVectorDim)
		_, err = reader.ReadIntent(context.Background(), "bad-session", dst)
		if !errors.Is(err, ErrCorruptIntent) {
			t.Fatalf("corrupt ReadIntent error = %v, want ErrCorruptIntent", err)
		}

		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		_, err = reader.ReadIntent(ctx, "bad-session", dst)
		if err == nil {
			t.Fatal("second ReadIntent error = nil, want redis/context error proving corrupt data was not cached")
		}
		if errors.Is(err, ErrCorruptIntent) {
			t.Fatalf("second ReadIntent error = %v, want redis/context error not cached corrupt replay", err)
		}
	})
}

func TestUnmarshalIntentVector(t *testing.T) {
	t.Parallel()

	t.Run("decodes raw bytes in place without retaining stale data", func(t *testing.T) {
		t.Parallel()
		want := testIntentVector(77)
		var raw [IntentVectorBytes]byte
		if err := MarshalIntentVectorInto(&raw, want); err != nil {
			t.Fatalf("MarshalIntentVectorInto error = %v", err)
		}
		dst := make([]float32, IntentVectorDim)
		if err := UnmarshalIntentVector(raw[:], dst); err != nil {
			t.Fatalf("UnmarshalIntentVector error = %v", err)
		}
		for i := range want {
			if dst[i] != want[i] {
				t.Fatalf("dst[%d] = %v, want %v", i, dst[i], want[i])
			}
		}
	})

	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "empty raw", raw: nil},
		{name: "overlong malicious raw", raw: bytes.Repeat([]byte{1}, IntentVectorBytes+1)},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dst := make([]float32, IntentVectorDim)
			err := UnmarshalIntentVector(tt.raw, dst)
			if !errors.Is(err, ErrCorruptIntent) {
				t.Fatalf("UnmarshalIntentVector error = %v, want ErrCorruptIntent", err)
			}
		})
	}
}

func testRedisIntentReaderHotCollision(t *testing.T) (string, string) {
	t.Helper()
	const base = "collision-session-"
	var seen [redisIntentReaderHotSlots]string
	for i := 0; i < 10000; i++ {
		sessionID := base + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26))
		slot := intentCacheKey(sessionID) & redisIntentReaderHotSlotsMask
		if seen[slot] != "" && seen[slot] != sessionID {
			return seen[slot], sessionID
		}
		seen[slot] = sessionID
	}
	t.Fatal("unable to find hot-slot collision")
	return "", ""
}

func BenchmarkUnmarshalIntentVector(b *testing.B) {
	vec := testIntentVector(7)
	var raw [IntentVectorBytes]byte
	if err := MarshalIntentVectorInto(&raw, vec); err != nil {
		b.Fatal(err)
	}
	var dst [IntentVectorDim]float32
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := UnmarshalIntentVector(raw[:], dst[:]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisIntentReaderL1Hit(b *testing.B) {
	s := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	b.Cleanup(func() { _ = rdb.Close() })
	reader, err := NewRedisIntentReader(RedisIntentReaderOptions{Client: rdb})
	if err != nil {
		b.Fatalf("NewRedisIntentReader error = %v", err)
	}
	vec := testIntentVector(11)
	var raw [IntentVectorBytes]byte
	if err := MarshalIntentVectorInto(&raw, vec); err != nil {
		b.Fatalf("MarshalIntentVectorInto error = %v", err)
	}
	if err := rdb.HSet(context.Background(), IntentKey("bench-session"), "version", int64(110), "vector", string(raw[:])).Err(); err != nil {
		b.Fatalf("HSet error = %v", err)
	}
	var dst [IntentVectorDim]float32
	if _, err := reader.ReadIntent(context.Background(), "bench-session", dst[:]); err != nil {
		b.Fatalf("ReadIntent warmup error = %v", err)
	}
	if reader.l1 != nil {
		reader.l1.Wait()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version, err := reader.ReadIntent(context.Background(), "bench-session", dst[:])
		if err != nil {
			b.Fatalf("ReadIntent error = %v", err)
		}
		if version != 110 {
			b.Fatalf("version = %d, want 110", version)
		}
	}
}
