package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisCoolingChecker_IsCooled_NewSession(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	checker, err := NewRedisCoolingChecker(RedisCoolingCheckerOptions{
		Client: client,
		Window: 60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// New session should not be in cooling period.
	cooled, err := checker.IsCooled(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if !cooled {
		t.Error("expected new session to be cooled (not in cooling period)")
	}
}

func TestRedisCoolingChecker_IsCooled_InWindow(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	checker, err := NewRedisCoolingChecker(RedisCoolingCheckerOptions{
		Client: client,
		Window: 60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Mark as called.
	if err := checker.MarkCalled(ctx, "session-1"); err != nil {
		t.Fatal(err)
	}

	// Should NOT be cooled (in cooling period).
	cooled, err := checker.IsCooled(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if cooled {
		t.Error("expected session to NOT be cooled right after MarkCalled")
	}
}

func TestRedisCoolingChecker_IsCooled_Expired(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	// Use a very short window so wall clock expiry happens quickly.
	checker, err := NewRedisCoolingChecker(RedisCoolingCheckerOptions{
		Client: client,
		Window: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Mark as called.
	if err := checker.MarkCalled(ctx, "session-1"); err != nil {
		t.Fatal(err)
	}

	// Wait for real wall clock to pass the window.
	time.Sleep(20 * time.Millisecond)

	// Should be cooled now (window expired).
	cooled, err := checker.IsCooled(ctx, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if !cooled {
		t.Error("expected session to be cooled after window expiration")
	}
}

func TestRedisCoolingChecker_NilReceiver(t *testing.T) {
	var checker *RedisCoolingChecker

	// Nil receiver should be fail-open.
	cooled, err := checker.IsCooled(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if !cooled {
		t.Error("expected nil checker to be fail-open")
	}
}

func TestRedisCoolingChecker_EmptySessionID(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	checker, err := NewRedisCoolingChecker(RedisCoolingCheckerOptions{
		Client: client,
		Window: 60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Empty session ID should be fail-open.
	cooled, err := checker.IsCooled(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !cooled {
		t.Error("expected empty session to be fail-open")
	}
}
