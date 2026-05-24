package cache

import (
	"bytes"
	"context"
	"errors"
	"testing"

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
