package cache

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMarshalIntentVectorInto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		vector  []float32
		wantErr error
	}{
		{name: "1024 dimensions marshal to strict 4096 bytes and roundtrip", vector: testIntentVector(1)},
		{name: "all-zero vector roundtrips without stale bytes", vector: make([]float32, IntentVectorDim)},
		{name: "empty vector returns ErrInvalidIntent", vector: nil, wantErr: ErrInvalidIntent},
		{name: "overlong malicious vector returns ErrInvalidIntent", vector: make([]float32, IntentVectorDim+1), wantErr: ErrInvalidIntent},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [IntentVectorBytes]byte
			err := MarshalIntentVectorInto(&buf, tt.vector)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("MarshalIntentVectorInto error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("MarshalIntentVectorInto error = %v", err)
			}
			if len(buf) != IntentVectorBytes {
				t.Fatalf("marshal bytes = %d, want %d", len(buf), IntentVectorBytes)
			}
			out := make([]float32, IntentVectorDim)
			if err := UnmarshalIntentVector(buf[:], out); err != nil {
				t.Fatalf("UnmarshalIntentVector error = %v", err)
			}
			for i := range tt.vector {
				if out[i] != tt.vector[i] {
					t.Fatalf("roundtrip[%d] = %v, want %v", i, out[i], tt.vector[i])
				}
			}
		})
	}
}

func TestRedisIntentWriterValidationAndContext(t *testing.T) {
	t.Parallel()

	w, err := NewRedisIntentWriter(RedisIntentWriterOptions{Client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})})
	if err != nil {
		t.Fatalf("NewRedisIntentWriter error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name      string
		ctx       context.Context
		sessionID string
		vector    []float32
		version   int64
		wantErr   error
	}{
		{name: "empty session rejects", ctx: context.Background(), sessionID: "", vector: testIntentVector(1), version: 1, wantErr: ErrInvalidIntent},
		{name: "empty vector rejects", ctx: context.Background(), sessionID: "s", vector: nil, version: 1, wantErr: ErrInvalidIntent},
		{name: "non-positive version rejects", ctx: context.Background(), sessionID: "s", vector: testIntentVector(1), version: 0, wantErr: ErrInvalidIntent},
		{name: "pre-canceled context rejects before network", ctx: ctx, sessionID: "s", vector: testIntentVector(1), version: 1, wantErr: context.Canceled},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := w.WriteIntent(tt.ctx, tt.sessionID, tt.vector, tt.version)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("WriteIntent error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisIntentWriterLuaVersioningBytesTTLAndConcurrency(t *testing.T) {
	t.Parallel()

	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	w, err := NewRedisIntentWriter(RedisIntentWriterOptions{Client: rdb, TTL: 24 * time.Hour})
	if err != nil {
		t.Fatalf("NewRedisIntentWriter error = %v", err)
	}
	ctx := context.Background()
	key := IntentKey("session-a")
	v100 := testIntentVector(100)
	if err := w.WriteIntent(ctx, "session-a", v100, 100); err != nil {
		t.Fatalf("first WriteIntent error = %v", err)
	}
	assertStoredIntent(t, s, key, 100, v100)
	if ttl := s.TTL(key); ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("TTL = %s, want (0,24h]", ttl)
	}

	beforeRaw := s.HGet(key, "vector")
	err = w.WriteIntent(ctx, "session-a", testIntentVector(90), 90)
	if !errors.Is(err, ErrStaleIntent) {
		t.Fatalf("old WriteIntent error = %v, want ErrStaleIntent", err)
	}
	if got := s.HGet(key, "version"); got != "100" {
		t.Fatalf("stale version overwrite = %q, want 100", got)
	}
	if got := s.HGet(key, "vector"); got != beforeRaw {
		t.Fatal("stale write changed vector bytes")
	}

	v101 := testIntentVector(101)
	if err := w.WriteIntent(ctx, "session-a", v101, 101); err != nil {
		t.Fatalf("new WriteIntent error = %v", err)
	}
	assertStoredIntent(t, s, key, 101, v101)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		version := int64(200 + i)
		go func() {
			defer wg.Done()
			errCh <- w.WriteIntent(ctx, "session-a", testIntentVector(int(version)), version)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, ErrStaleIntent) {
			t.Fatalf("concurrent WriteIntent error = %v", err)
		}
	}
	assertStoredIntent(t, s, key, 263, testIntentVector(263))
}

func BenchmarkMarshalIntentVectorInto(b *testing.B) {
	vec := testIntentVector(7)
	var buf [IntentVectorBytes]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := MarshalIntentVectorInto(&buf, vec); err != nil {
			b.Fatal(err)
		}
	}
}

func testIntentVector(seed int) []float32 {
	v := make([]float32, IntentVectorDim)
	for i := range v {
		v[i] = float32(seed*100000+i-512) / 1024.0
	}
	return v
}

func assertStoredIntent(t *testing.T, s *miniredis.Miniredis, key string, version int64, vector []float32) {
	t.Helper()
	if got := s.HGet(key, "version"); got != strconv.FormatInt(version, 10) {
		t.Fatalf("stored version = %q, want %d", got, version)
	}
	raw := s.HGet(key, "vector")
	if len(raw) != IntentVectorBytes {
		t.Fatalf("stored vector bytes = %d, want %d", len(raw), IntentVectorBytes)
	}
	var want [IntentVectorBytes]byte
	if err := MarshalIntentVectorInto(&want, vector); err != nil {
		t.Fatalf("marshal expected vector: %v", err)
	}
	if !bytes.Equal([]byte(raw), want[:]) {
		t.Fatal("stored vector bytes do not match little-endian marshal")
	}
	out := make([]float32, IntentVectorDim)
	if err := UnmarshalIntentVector([]byte(raw), out); err != nil {
		t.Fatalf("UnmarshalIntentVector stored raw: %v", err)
	}
	for i := range vector {
		if out[i] != vector[i] {
			t.Fatalf("stored roundtrip[%d] = %v, want %v", i, out[i], vector[i])
		}
	}
}
