package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type stubBloom struct {
	added   map[string]bool
	contains map[string]bool
}

func newStubBloom() *stubBloom {
	return &stubBloom{
		added:    make(map[string]bool),
		contains: make(map[string]bool),
	}
}

func (b *stubBloom) Add(key string) bool {
	if b.added[key] {
		return false
	}
	b.added[key] = true
	return true
}

func (b *stubBloom) Contains(key string) bool {
	return b.added[key]
}

func TestProfileCacheReader_ReadProfile_Miss(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	bloom := newStubBloom()

	reader := NewProfileCacheReader(ProfileCacheReaderOptions{
		Client: client,
		Bloom:  bloom,
	})

	dst := make([]float32, IntentVectorDim)
	_, err = reader.ReadProfile(context.Background(), "session-1", dst)
	if err != ErrMiss {
		t.Errorf("expected ErrMiss, got %v", err)
	}
}

func TestProfileCacheReader_ReadProfile_BloomReject(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	bloom := newStubBloom() // empty bloom - nothing added

	reader := NewProfileCacheReader(ProfileCacheReaderOptions{
		Client: client,
		Bloom:  bloom,
	})

	dst := make([]float32, IntentVectorDim)
	// Bloom rejects, so should return ErrMiss without hitting Redis.
	_, err = reader.ReadProfile(context.Background(), "session-1", dst)
	if err != ErrMiss {
		t.Errorf("expected ErrMiss from bloom rejection, got %v", err)
	}
}

func TestProfileCacheReader_CacheAndRead(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	bloom := newStubBloom()

	reader := NewProfileCacheReader(ProfileCacheReaderOptions{
		Client: client,
		Bloom:  bloom,
	})

	ctx := context.Background()

	// Create a test vector.
	vector := make([]float32, IntentVectorDim)
	for i := range vector {
		vector[i] = float32(i) * 0.1
	}

	// Cache the profile.
	if err := reader.CacheProfile(ctx, "session-1", vector, 42); err != nil {
		t.Fatal(err)
	}

	// Verify bloom was updated.
	if !bloom.Contains("session-1") {
		t.Error("expected bloom filter to contain session-1 after CacheProfile")
	}

	// Read back.
	dst := make([]float32, IntentVectorDim)
	version, err := reader.ReadProfile(ctx, "session-1", dst)
	if err != nil {
		t.Fatal(err)
	}
	if version != 42 {
		t.Errorf("expected version 42, got %d", version)
	}

	// Verify vector values match.
	for i := 0; i < IntentVectorDim; i++ {
		if dst[i] != vector[i] {
			t.Errorf("vector[%d]: expected %f, got %f", i, vector[i], dst[i])
			break
		}
	}
}

func TestProfileCacheReader_CacheProfile_Expired(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	bloom := newStubBloom()

	reader := NewProfileCacheReader(ProfileCacheReaderOptions{
		Client: client,
		Bloom:  bloom,
	})

	ctx := context.Background()

	vector := make([]float32, IntentVectorDim)
	vector[0] = 1.0

	// Cache profile.
	if err := reader.CacheProfile(ctx, "session-1", vector, 1); err != nil {
		t.Fatal(err)
	}

	// Fast-forward Redis time past TTL.
	mr.FastForward(6 * time.Minute)

	// Read should miss due to TTL expiry.
	dst := make([]float32, IntentVectorDim)
	_, err = reader.ReadProfile(ctx, "session-1", dst)
	if err != ErrMiss {
		t.Errorf("expected ErrMiss after TTL expiry, got %v", err)
	}
}

func TestProfileCacheReader_NilBloom(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	reader := NewProfileCacheReader(ProfileCacheReaderOptions{
		Client: client,
		Bloom:  nil, // no bloom filter
	})

	ctx := context.Background()

	vector := make([]float32, IntentVectorDim)
	vector[0] = 99.0

	// Cache and read should work without bloom.
	if err := reader.CacheProfile(ctx, "session-1", vector, 7); err != nil {
		t.Fatal(err)
	}

	dst := make([]float32, IntentVectorDim)
	version, err := reader.ReadProfile(ctx, "session-1", dst)
	if err != nil {
		t.Fatal(err)
	}
	if version != 7 {
		t.Errorf("expected version 7, got %d", version)
	}
}

func TestProfileCacheReader_InvalidArgs(t *testing.T) {
	reader := NewProfileCacheReader(ProfileCacheReaderOptions{})

	// Empty session ID.
	dst := make([]float32, IntentVectorDim)
	_, err := reader.ReadProfile(context.Background(), "", dst)
	if err != ErrInvalidIntent {
		t.Errorf("expected ErrInvalidIntent for empty session, got %v", err)
	}

	// Nil reader.
	var nilReader *ProfileCacheReader
	_, err = nilReader.ReadProfile(context.Background(), "session-1", dst)
	if err != ErrInvalidIntent {
		t.Errorf("expected ErrInvalidIntent for nil reader, got %v", err)
	}
}
