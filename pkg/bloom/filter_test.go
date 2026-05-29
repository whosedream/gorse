package bloom

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisBloomFilter_AddAndContains(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f, err := NewRedisBloomFilter(Options{
		Client: client,
		M:      1 << 16, // 64K bits for test
		K:      5,
		Key:    "test:bloom:1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add some keys.
	f.Add("session-1")
	f.Add("session-2")
	f.Add("session-3")

	// Check existing keys.
	if !f.Contains("session-1") {
		t.Error("expected session-1 to be in bloom filter")
	}
	if !f.Contains("session-2") {
		t.Error("expected session-2 to be in bloom filter")
	}
	if !f.Contains("session-3") {
		t.Error("expected session-3 to be in bloom filter")
	}

	// Check non-existing key.
	if f.Contains("session-999") {
		t.Error("expected session-999 to NOT be in bloom filter")
	}
}

func TestRedisBloomFilter_FalsePositiveRate(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f, err := NewRedisBloomFilter(Options{
		Client: client,
		M:      1 << 18, // 256K bits
		K:      7,       // optimal for < 1% FP
		Key:    "test:bloom:fp",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert 1000 keys.
	n := 1000
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key-%d", i))
	}

	// Check 1000 non-existing keys and count false positives.
	falsePositives := 0
	checkCount := 1000
	for i := 0; i < checkCount; i++ {
		key := fmt.Sprintf("missing-%d", i)
		if f.Contains(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(checkCount) * 100
	t.Logf("false positive rate: %.2f%% (%d/%d)", fpRate, falsePositives, checkCount)

	// Should be well under 1% for these parameters.
	if fpRate > 5.0 {
		t.Errorf("false positive rate too high: %.2f%%", fpRate)
	}
}

func TestRedisBloomFilter_EmptyFilter(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f, err := NewRedisBloomFilter(Options{
		Client: client,
		M:      1 << 16,
		K:      5,
		Key:    "test:bloom:empty",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Empty filter should not contain anything.
	if f.Contains("anything") {
		t.Error("empty bloom filter should not contain any key")
	}
}

func TestRedisBloomFilter_AddIdempotent(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f, err := NewRedisBloomFilter(Options{
		Client: client,
		M:      1 << 16,
		K:      5,
		Key:    "test:bloom:idempotent",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add same key twice - second add should return false.
	first := f.Add("key-1")
	second := f.Add("key-1")

	if !first {
		t.Error("first Add should return true")
	}
	if second {
		t.Error("second Add should return false (already exists)")
	}
}

func TestRedisBloomFilter_NilClient(t *testing.T) {
	f, err := NewRedisBloomFilter(Options{
		Client: nil,
	})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
	_ = f
}

func TestRedisBloomFilter_LocalFastPath(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f, err := NewRedisBloomFilter(Options{
		Client: client,
		M:      1 << 16,
		K:      5,
		Key:    "test:bloom:local",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add a key locally.
	f.Add("local-key")

	// Contains should work via local cache even without Redis round-trip.
	if !f.Contains("local-key") {
		t.Error("local cache should find the key")
	}
}
