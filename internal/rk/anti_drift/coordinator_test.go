package anti_drift

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type slowServiceFunc func(context.Context, IntentFeatureUpdate) (IntentFeatureUpdate, error)

func (f slowServiceFunc) Infer(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
	return f(ctx, update)
}

type rankerFunc func(context.Context, string) (FeatureRecord, error)

func (f rankerFunc) Score(ctx context.Context, sessionID string) (FeatureRecord, error) {
	return f(ctx, sessionID)
}

func newTestCoordinator(t *testing.T, slow SlowTrackService, ranker DistilledRanker) *Coordinator {
	t.Helper()
	c, err := NewCoordinator(Options{
		MinWorkers:        1,
		MaxWorkers:        2,
		QueueCapacity:     4,
		Alpha:             0.25,
		SlowTimeout:       50 * time.Millisecond,
		DriftWindowMillis: 200,
		SlowTrack:         slow,
		Ranker:            ranker,
	})
	if err != nil {
		t.Fatalf("NewCoordinator returned error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	})
	return c
}

func TestCoordinatorApplySlowControlLoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fast       FastTrackSnapshot
		update     IntentFeatureUpdate
		want       FeatureRecord
		wantErr    error
		wantStored bool
	}{
		{
			name: "baseline equal latest accepts slow feature without fusion",
			fast: FastTrackSnapshot{
				SessionID:       "s1",
				LatestVersion:   100,
				IntentVector:    []float32{10, 20},
				CategoryWeights: map[string]float32{"phone": 0.9},
			},
			update: IntentFeatureUpdate{
				SessionID:       "s1",
				BaselineVersion: 100,
				IntentVector:    []float32{1, 2},
				CategoryWeights: map[string]float32{"phone": 0.4, "case": 0.6},
				DriftThreshold:  0.05,
			},
			want: FeatureRecord{
				SessionID:       "s1",
				Version:         100,
				IntentVector:    []float32{1, 2},
				CategoryWeights: map[string]float32{"phone": 0.4, "case": 0.6},
				DriftIndex:      0,
				Fused:           false,
				Fallback:        false,
				ReviewPassed:    true,
			},
			wantStored: true,
		},
		{
			name: "drift above threshold fuses slow intent with latest fast snapshot",
			fast: FastTrackSnapshot{
				SessionID:       "s2",
				LatestVersion:   200,
				IntentVector:    []float32{10, 20},
				CategoryWeights: map[string]float32{"phone": 0.8, "case": 0.2},
			},
			update: IntentFeatureUpdate{
				SessionID:       "s2",
				BaselineVersion: 100,
				IntentVector:    []float32{2, 4},
				CategoryWeights: map[string]float32{"phone": 0.4, "watch": 0.6},
				DriftThreshold:  0.05,
			},
			want: FeatureRecord{
				SessionID:       "s2",
				Version:         200,
				IntentVector:    []float32{8, 16},
				CategoryWeights: map[string]float32{"phone": 0.7, "case": 0.15, "watch": 0.15},
				DriftIndex:      0.5,
				Fused:           true,
				Fallback:        false,
				ReviewPassed:    true,
			},
			wantStored: true,
		},
		{
			name: "real millisecond timestamp drift uses configured window and fuses",
			fast: FastTrackSnapshot{
				SessionID:       "epoch",
				LatestVersion:   1716400001000,
				IntentVector:    []float32{10, 20},
				CategoryWeights: map[string]float32{"phone": 0.8},
			},
			update: IntentFeatureUpdate{
				SessionID:       "epoch",
				BaselineVersion: 1716400000000,
				IntentVector:    []float32{2, 4},
				CategoryWeights: map[string]float32{"phone": 0.4},
				DriftThreshold:  0.05,
			},
			want: FeatureRecord{
				SessionID:       "epoch",
				Version:         1716400001000,
				IntentVector:    []float32{8, 16},
				CategoryWeights: map[string]float32{"phone": 0.7},
				DriftIndex:      1,
				Fused:           true,
				Fallback:        false,
				ReviewPassed:    true,
			},
			wantStored: true,
		},
		{
			name: "extreme drift with fast snapshot still fuses instead of rejecting",
			fast: FastTrackSnapshot{
				SessionID:       "ancient",
				LatestVersion:   1000,
				IntentVector:    []float32{10},
				CategoryWeights: map[string]float32{"fast": 1},
			},
			update: IntentFeatureUpdate{
				SessionID:       "ancient",
				BaselineVersion: 1,
				IntentVector:    []float32{2},
				CategoryWeights: map[string]float32{"slow": 1},
				DriftThreshold:  0.05,
			},
			want: FeatureRecord{
				SessionID:       "ancient",
				Version:         1000,
				IntentVector:    []float32{8},
				CategoryWeights: map[string]float32{"fast": 0.75, "slow": 0.25},
				DriftIndex:      1,
				Fused:           true,
				Fallback:        false,
				ReviewPassed:    true,
			},
			wantStored: true,
		},
		{
			name: "mismatched vector dimensions fuse by max length with missing dimensions as zero",
			fast: FastTrackSnapshot{
				SessionID:       "dims",
				LatestVersion:   200,
				IntentVector:    []float32{10},
				CategoryWeights: map[string]float32{"fast": 1},
			},
			update: IntentFeatureUpdate{
				SessionID:       "dims",
				BaselineVersion: 100,
				IntentVector:    []float32{2, 4, 6},
				CategoryWeights: map[string]float32{"slow": 1},
				DriftThreshold:  0.05,
			},
			want: FeatureRecord{
				SessionID:       "dims",
				Version:         200,
				IntentVector:    []float32{8, 1, 1.5},
				CategoryWeights: map[string]float32{"fast": 0.75, "slow": 0.25},
				DriftIndex:      0.5,
				Fused:           true,
				Fallback:        false,
				ReviewPassed:    true,
			},
			wantStored: true,
		},
		{
			name: "baseline missing rejects invalid update",
			fast: FastTrackSnapshot{
				SessionID:       "s3",
				LatestVersion:   10,
				IntentVector:    []float32{1},
				CategoryWeights: map[string]float32{"x": 1},
			},
			update: IntentFeatureUpdate{
				SessionID:       "s3",
				BaselineVersion: 0,
				IntentVector:    []float32{1},
				CategoryWeights: map[string]float32{"x": 1},
				DriftThreshold:  0.05,
			},
			wantErr:    ErrInvalidUpdate,
			wantStored: false,
		},
		{
			name: "empty session rejects invalid update",
			fast: FastTrackSnapshot{
				SessionID:       "s4",
				LatestVersion:   10,
				IntentVector:    []float32{1},
				CategoryWeights: map[string]float32{"x": 1},
			},
			update: IntentFeatureUpdate{
				SessionID:       "",
				BaselineVersion: 10,
				IntentVector:    []float32{1},
				CategoryWeights: map[string]float32{"x": 1},
				DriftThreshold:  0.05,
			},
			wantErr:    ErrInvalidUpdate,
			wantStored: false,
		},
		{
			name: "empty vector rejects invalid update",
			fast: FastTrackSnapshot{
				SessionID:       "s5",
				LatestVersion:   10,
				IntentVector:    []float32{1},
				CategoryWeights: map[string]float32{"x": 1},
			},
			update: IntentFeatureUpdate{
				SessionID:       "s5",
				BaselineVersion: 10,
				IntentVector:    nil,
				CategoryWeights: map[string]float32{"x": 1},
				DriftThreshold:  0.05,
			},
			wantErr:    ErrInvalidUpdate,
			wantStored: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newTestCoordinator(t, nil, nil)
			if tt.fast.SessionID != "" {
				if err := c.UpdateFast(context.Background(), tt.fast); err != nil {
					t.Fatalf("UpdateFast returned error: %v", err)
				}
			}

			var before FeatureRecord
			var hadBefore bool
			if tt.update.SessionID != "" {
				before, hadBefore = c.Get(tt.update.SessionID)
			}

			got, err := c.ApplySlow(context.Background(), tt.update)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				if tt.update.SessionID != "" {
					after, ok := c.Get(tt.update.SessionID)
					if ok != hadBefore {
						t.Fatalf("invalid update changed ledger presence for session %q", tt.update.SessionID)
					}
					if ok {
						assertRecordApprox(t, after, before)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplySlow returned error: %v", err)
			}
			assertRecordApprox(t, got, tt.want)
			stored, ok := c.Get(tt.update.SessionID)
			if !ok {
				t.Fatal("Get did not return applied record")
			}
			assertRecordApprox(t, stored, tt.want)
		})
	}
}

func TestCoordinatorRejectsOlderSlowBaselineAndPreservesHistory(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "mono", LatestVersion: 300, IntentVector: []float32{10}, CategoryWeights: map[string]float32{"fast": 1}}); err != nil {
		t.Fatalf("UpdateFast returned error: %v", err)
	}
	first, err := c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: "mono", BaselineVersion: 250, IntentVector: []float32{2}, CategoryWeights: map[string]float32{"newer": 1}, DriftThreshold: 0.05})
	if err != nil {
		t.Fatalf("first ApplySlow returned error: %v", err)
	}
	_, err = c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: "mono", BaselineVersion: 100, IntentVector: []float32{99}, CategoryWeights: map[string]float32{"older": 1}, DriftThreshold: 0.05})
	if !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("expected ErrInvalidUpdate for older baseline, got %v", err)
	}
	stored, ok := c.Get("mono")
	if !ok {
		t.Fatal("expected stored record")
	}
	assertRecordApprox(t, stored, first)
}

func TestCoordinatorUpdateFastDoesNotDowngradeSlowRecord(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	want, err := c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: "no-downgrade", BaselineVersion: 100, IntentVector: []float32{4}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05})
	if err != nil {
		t.Fatalf("ApplySlow returned error: %v", err)
	}
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "no-downgrade", LatestVersion: 50, IntentVector: []float32{9}, CategoryWeights: map[string]float32{"old-fast": 1}}); err != nil {
		t.Fatalf("UpdateFast returned error: %v", err)
	}
	got, ok := c.Get("no-downgrade")
	if !ok {
		t.Fatal("expected stored record")
	}
	assertRecordApprox(t, got, want)
}

func TestCoordinatorApplySlowRetryIsBounded(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	const dims = 65536
	fastVec := make([]float32, dims)
	slowVec := make([]float32, dims)
	for i := range fastVec {
		fastVec[i] = float32(i)
		slowVec[i] = float32(dims - i)
	}
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "retry", LatestVersion: 100, IntentVector: fastVec, CategoryWeights: map[string]float32{"fast": 1}}); err != nil {
		t.Fatalf("UpdateFast returned error: %v", err)
	}

	stop := atomic.Bool{}
	advanced := make(chan struct{})
	go func() {
		defer close(advanced)
		version := int64(101)
		for !stop.Load() {
			_ = c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "retry", LatestVersion: version, IntentVector: fastVec, CategoryWeights: map[string]float32{"fast": 1}})
			version++
		}
	}()
	defer stop.Store(true)

	start := time.Now()
	_, err := c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: "retry", BaselineVersion: 100, IntentVector: slowVec, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05})
	elapsed := time.Since(start)
	stop.Store(true)
	<-advanced
	if err != nil && !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("expected nil or ErrRetryExhausted, got %v", err)
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("retry exhaustion was not bounded: %s", elapsed)
	}
}

func TestCoordinatorInvalidStaleAndCanceledBoundaries(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{
		SessionID:       "stale",
		LatestVersion:   100,
		IntentVector:    []float32{1},
		CategoryWeights: map[string]float32{"good": 1},
	}); err != nil {
		t.Fatalf("UpdateFast returned error: %v", err)
	}
	before, ok := c.Get("stale")
	if !ok {
		t.Fatal("expected existing history")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.ApplySlow(ctx, IntentFeatureUpdate{
		SessionID:       "stale",
		BaselineVersion: 100,
		IntentVector:    []float32{9},
		CategoryWeights: map[string]float32{"bad": 1},
		DriftThreshold:  0.05,
	})
	if err == nil {
		t.Fatal("expected canceled context error")
	}
	after, ok := c.Get("stale")
	if !ok {
		t.Fatal("history was deleted")
	}
	assertRecordApprox(t, after, before)
}

func TestCoordinatorDefaultSlowTimeoutFallsBackQuickly(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started := make(chan struct{})
	slow := slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
		close(started)
		select {
		case <-release:
			return update, nil
		case <-ctx.Done():
			return IntentFeatureUpdate{}, ctx.Err()
		}
	})
	c, err := NewCoordinator(Options{
		MinWorkers:        1,
		MaxWorkers:        1,
		QueueCapacity:     2,
		Alpha:             0.25,
		DriftWindowMillis: 200,
		SlowTrack:         slow,
		Ranker: rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
			return FeatureRecord{SessionID: sessionID, Version: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewCoordinator returned error: %v", err)
	}
	defer func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	}()

	start := time.Now()
	got, err := c.Decide(context.Background(), IntentFeatureUpdate{SessionID: "default-timeout", BaselineVersion: 1, IntentVector: []float32{2}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if !got.Fallback {
		t.Fatalf("expected fallback with default SlowTimeout, got %#v", got)
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("default slow timeout fallback took %s", elapsed)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow service did not start")
	}
}

func TestCoordinatorDecideFallbackPreservesHistory(t *testing.T) {
	t.Parallel()

	boom := errors.New("llm unavailable")
	tests := []struct {
		name string
		slow SlowTrackService
	}{
		{
			name: "slow track error falls back to distilled ranker",
			slow: slowServiceFunc(func(context.Context, IntentFeatureUpdate) (IntentFeatureUpdate, error) {
				return IntentFeatureUpdate{}, boom
			}),
		},
		{
			name: "slow track timeout falls back without waiting for long inference",
			slow: slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
				select {
				case <-ctx.Done():
					return IntentFeatureUpdate{}, ctx.Err()
				case <-time.After(time.Second):
					return update, nil
				}
			}),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var rankerCalls atomic.Int32
			c := newTestCoordinator(t, tt.slow, rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
				rankerCalls.Add(1)
				return FeatureRecord{SessionID: sessionID, Version: 777, IntentVector: []float32{7}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
			}))
			if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "history", LatestVersion: 10, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"history": 1}}); err != nil {
				t.Fatalf("UpdateFast returned error: %v", err)
			}
			before, _ := c.Get("history")

			start := time.Now()
			got, err := c.Decide(context.Background(), IntentFeatureUpdate{SessionID: "history", BaselineVersion: 10, IntentVector: []float32{2}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05})
			if err != nil {
				t.Fatalf("Decide returned error: %v", err)
			}
			if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
				t.Fatalf("fallback was not millisecond-level: %s", elapsed)
			}
			if !got.Fallback {
				t.Fatalf("expected Fallback=true, got %#v", got)
			}
			if rankerCalls.Load() != 1 {
				t.Fatalf("expected one ranker call, got %d", rankerCalls.Load())
			}
			after, ok := c.Get("history")
			if !ok {
				t.Fatal("existing memory was deleted")
			}
			assertRecordApprox(t, after, before)
		})
	}
}

func TestCoordinatorDecideCopiesUpdateBeforeAsyncSlowTrack(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan IntentFeatureUpdate, 1)
	slow := slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
		close(started)
		<-release
		observed <- update
		return update, nil
	})
	c, err := NewCoordinator(Options{
		MinWorkers:        1,
		MaxWorkers:        1,
		QueueCapacity:     2,
		Alpha:             0.25,
		SlowTimeout:       25 * time.Millisecond,
		DriftWindowMillis: 200,
		SlowTrack:         slow,
		Ranker: rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
			return FeatureRecord{SessionID: sessionID, Version: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewCoordinator returned error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	}()

	update := IntentFeatureUpdate{SessionID: "copy-async", BaselineVersion: 100, IntentVector: []float32{2, 4}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 0.05}
	result := make(chan FeatureRecord, 1)
	errs := make(chan error, 1)
	go func() {
		rec, err := c.Decide(context.Background(), update)
		if err != nil {
			errs <- err
			return
		}
		result <- rec
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow service did not start")
	}
	select {
	case err := <-errs:
		t.Fatalf("Decide returned error: %v", err)
	case got := <-result:
		if !got.Fallback {
			t.Fatalf("expected fallback after timeout, got %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Decide did not timeout into fallback")
	}

	update.IntentVector[0] = 99
	update.CategoryWeights["slow"] = 99
	close(release)
	select {
	case got := <-observed:
		if got.IntentVector[0] != 2 || got.CategoryWeights["slow"] != 1 {
			t.Fatalf("slow task observed caller mutation: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("slow service did not report observed update")
	}
}

func TestCoordinatorDecidePoolOverloadFallsBackNonBlocking(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	block := make(chan struct{})
	slow := slowServiceFunc(func(ctx context.Context, update IntentFeatureUpdate) (IntentFeatureUpdate, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-block:
			return update, nil
		case <-ctx.Done():
			return IntentFeatureUpdate{}, ctx.Err()
		}
	})
	var rankerCalls atomic.Int32
	c, err := NewCoordinator(Options{
		MinWorkers:    1,
		MaxWorkers:    1,
		QueueCapacity: 1,
		Alpha:         0.5,
		SlowTimeout:   time.Second,
		SlowTrack:     slow,
		Ranker: rankerFunc(func(ctx context.Context, sessionID string) (FeatureRecord, error) {
			rankerCalls.Add(1)
			return FeatureRecord{SessionID: sessionID, Version: 99, IntentVector: []float32{9}, CategoryWeights: map[string]float32{"fallback": 1}}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewCoordinator returned error: %v", err)
	}
	defer func() {
		close(block)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	}()

	go func() {
		_, _ = c.Decide(context.Background(), IntentFeatureUpdate{SessionID: "busy-1", BaselineVersion: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"a": 1}, DriftThreshold: 0.05})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first slow task did not start")
	}
	go func() {
		_, _ = c.Decide(context.Background(), IntentFeatureUpdate{SessionID: "busy-2", BaselineVersion: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"a": 1}, DriftThreshold: 0.05})
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && c.pool.QueueLen() == 0 {
		time.Sleep(time.Millisecond)
	}
	if c.pool.QueueLen() == 0 {
		t.Fatal("second slow task was not queued")
	}

	start := time.Now()
	got, err := c.Decide(context.Background(), IntentFeatureUpdate{SessionID: "overload", BaselineVersion: 1, IntentVector: []float32{1}, CategoryWeights: map[string]float32{"a": 1}, DriftThreshold: 0.05})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 25*time.Millisecond {
		t.Fatalf("overload fallback blocked for %s", elapsed)
	}
	if !got.Fallback {
		t.Fatalf("expected fallback on overload, got %#v", got)
	}
	if rankerCalls.Load() == 0 {
		t.Fatal("distilled ranker was not called")
	}
}

func TestCoordinatorConcurrentLedgerRaceSafe(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	const workers = 32
	const rounds = 200
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			for j := 1; j <= rounds; j++ {
				sid := "race"
				version := int64(i*rounds + j)
				_ = c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: sid, LatestVersion: version, IntentVector: []float32{float32(version)}, CategoryWeights: map[string]float32{"fast": float32(j)}})
				_, _ = c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: sid, BaselineVersion: version, IntentVector: []float32{float32(j)}, CategoryWeights: map[string]float32{"slow": float32(i)}, DriftThreshold: 1})
				_, _ = c.Get(sid)
			}
		}()
	}
	wg.Wait()
	if _, ok := c.Get("race"); !ok {
		t.Fatal("expected final race-safe record")
	}
}

func TestCoordinatorCopiesInputsAndOutputs(t *testing.T) {
	t.Parallel()

	c := newTestCoordinator(t, nil, nil)
	fastVec := []float32{1, 2}
	fastWeights := map[string]float32{"fast": 1}
	if err := c.UpdateFast(context.Background(), FastTrackSnapshot{SessionID: "copy", LatestVersion: 10, IntentVector: fastVec, CategoryWeights: fastWeights}); err != nil {
		t.Fatalf("UpdateFast returned error: %v", err)
	}
	fastVec[0] = 99
	fastWeights["fast"] = 99
	stored, ok := c.Get("copy")
	if !ok {
		t.Fatal("expected stored fast record")
	}
	if stored.IntentVector[0] != 1 || stored.CategoryWeights["fast"] != 1 {
		t.Fatalf("fast input mutation polluted ledger: %#v", stored)
	}

	got, err := c.ApplySlow(context.Background(), IntentFeatureUpdate{SessionID: "copy", BaselineVersion: 10, IntentVector: []float32{3, 4}, CategoryWeights: map[string]float32{"slow": 1}, DriftThreshold: 1})
	if err != nil {
		t.Fatalf("ApplySlow returned error: %v", err)
	}
	got.IntentVector[0] = 88
	got.CategoryWeights["slow"] = 88
	stored, ok = c.Get("copy")
	if !ok {
		t.Fatal("expected stored slow record")
	}
	if stored.IntentVector[0] != 3 || stored.CategoryWeights["slow"] != 1 {
		t.Fatalf("returned record mutation polluted ledger: %#v", stored)
	}
}

func assertRecordApprox(t *testing.T, got, want FeatureRecord) {
	t.Helper()
	if got.SessionID != want.SessionID || got.Version != want.Version || got.Fused != want.Fused || got.Fallback != want.Fallback || got.ReviewPassed != want.ReviewPassed {
		t.Fatalf("record metadata mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if !near(got.DriftIndex, want.DriftIndex) {
		t.Fatalf("DriftIndex got %v want %v", got.DriftIndex, want.DriftIndex)
	}
	if len(got.IntentVector) != len(want.IntentVector) {
		t.Fatalf("vector length got %d want %d", len(got.IntentVector), len(want.IntentVector))
	}
	for i := range want.IntentVector {
		if !near(got.IntentVector[i], want.IntentVector[i]) {
			t.Fatalf("vector[%d] got %v want %v", i, got.IntentVector[i], want.IntentVector[i])
		}
	}
	if len(got.CategoryWeights) != len(want.CategoryWeights) {
		t.Fatalf("weights length got %d want %d: %#v", len(got.CategoryWeights), len(want.CategoryWeights), got.CategoryWeights)
	}
	for k, wantValue := range want.CategoryWeights {
		if !near(got.CategoryWeights[k], wantValue) {
			t.Fatalf("weight[%s] got %v want %v", k, got.CategoryWeights[k], wantValue)
		}
	}
}

func near(a, b float32) bool {
	return math.Abs(float64(a-b)) < 0.0001
}
