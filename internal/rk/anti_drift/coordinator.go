package anti_drift

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"time"

	"go-rec/pkg/pool"
)

var (
	ErrInvalidOptions  = errors.New("invalid anti drift coordinator options")
	ErrInvalidUpdate   = errors.New("invalid intent feature update")
	ErrInvalidSnapshot = errors.New("invalid fast track snapshot")
	ErrNoFallback      = errors.New("distilled ranker unavailable")
	ErrRetryExhausted  = errors.New("anti drift apply retry exhausted")
)

const (
	defaultDriftWindowMillis = int64(1000)
	defaultSlowTimeout       = 25 * time.Millisecond
	maxApplySlowRetries      = 8
)

type IntentFeatureUpdate struct {
	SessionID       string
	BaselineVersion int64
	IntentVector    []float32
	CategoryWeights map[string]float32
	DriftThreshold  float32
}

type FastTrackSnapshot struct {
	SessionID       string
	LatestVersion   int64
	IntentVector    []float32
	CategoryWeights map[string]float32
}

type FeatureRecord struct {
	SessionID       string
	Version         int64
	IntentVector    []float32
	CategoryWeights map[string]float32
	DriftIndex      float32
	Fused           bool
	Fallback        bool
	ReviewPassed    bool
}

type SlowTrackService interface {
	Infer(context.Context, IntentFeatureUpdate) (IntentFeatureUpdate, error)
}

type DistilledRanker interface {
	Score(context.Context, string) (FeatureRecord, error)
}

type Options struct {
	MinWorkers        int
	MaxWorkers        int
	QueueCapacity     int
	Alpha             float32
	SlowTimeout       time.Duration
	DriftWindowMillis int64
	SlowTrack         SlowTrackService
	Ranker            DistilledRanker
}

type fastRecord struct {
	version int64
	vector  []float32
	weights map[string]float32
}

type Coordinator struct {
	mu                sync.RWMutex
	records           map[string]FeatureRecord
	fast              map[string]fastRecord
	appliedBaseline   map[string]int64
	pool              *pool.GoroutinePool
	alpha             float32
	slowTimeout       time.Duration
	driftWindowMillis int64
	slow              SlowTrackService
	ranker            DistilledRanker
}

func NewCoordinator(opts Options) (*Coordinator, error) {
	minWorkers := opts.MinWorkers
	maxWorkers := opts.MaxWorkers
	queueCap := opts.QueueCapacity
	if minWorkers == 0 {
		minWorkers = 1
	}
	if maxWorkers == 0 {
		maxWorkers = minWorkers
	}
	if queueCap == 0 {
		queueCap = 64
	}
	if minWorkers < 0 || maxWorkers < minWorkers || queueCap < 0 {
		return nil, ErrInvalidOptions
	}
	gp, err := pool.NewGoroutinePool(minWorkers, maxWorkers, queueCap)
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		records:           make(map[string]FeatureRecord),
		fast:              make(map[string]fastRecord),
		appliedBaseline:   make(map[string]int64),
		pool:              gp,
		alpha:             clamp01(opts.Alpha),
		slowTimeout:       opts.SlowTimeout,
		driftWindowMillis: opts.DriftWindowMillis,
		slow:              opts.SlowTrack,
		ranker:            opts.Ranker,
	}
	if c.slowTimeout <= 0 {
		c.slowTimeout = defaultSlowTimeout
	}
	if c.driftWindowMillis <= 0 {
		c.driftWindowMillis = defaultDriftWindowMillis
	}
	return c, nil
}

func (c *Coordinator) UpdateFast(ctx context.Context, snapshot FastTrackSnapshot) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if snapshot.SessionID == "" || snapshot.LatestVersion <= 0 || len(snapshot.IntentVector) == 0 {
		return ErrInvalidSnapshot
	}
	vec := cloneFloat32s(snapshot.IntentVector)
	weights := cloneWeights(snapshot.CategoryWeights)

	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.fast[snapshot.SessionID]
	if ok && current.version > snapshot.LatestVersion {
		return nil
	}
	c.fast[snapshot.SessionID] = fastRecord{version: snapshot.LatestVersion, vector: vec, weights: weights}
	if rec, ok := c.records[snapshot.SessionID]; ok && rec.Version > snapshot.LatestVersion {
		return nil
	}
	c.records[snapshot.SessionID] = FeatureRecord{
		SessionID:       snapshot.SessionID,
		Version:         snapshot.LatestVersion,
		IntentVector:    cloneFloat32s(vec),
		CategoryWeights: cloneWeights(weights),
		ReviewPassed:    true,
	}
	return nil
}

func (c *Coordinator) ApplySlow(ctx context.Context, update IntentFeatureUpdate) (FeatureRecord, error) {
	select {
	case <-ctx.Done():
		return FeatureRecord{}, ctx.Err()
	default:
	}
	if invalidUpdate(update) {
		return FeatureRecord{}, ErrInvalidUpdate
	}
	update = cloneUpdate(update)

	for attempt := 0; attempt < maxApplySlowRetries; attempt++ {
		fast, hasFast := c.fastSnapshot(update.SessionID)
		rec := c.buildSlowRecord(update, fast, hasFast)

		c.mu.Lock()
		if applied, ok := c.appliedBaseline[update.SessionID]; ok && update.BaselineVersion < applied {
			c.mu.Unlock()
			return FeatureRecord{}, ErrInvalidUpdate
		}
		current, ok := c.fast[update.SessionID]
		if ok && (!hasFast || current.version != fast.version) {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return FeatureRecord{}, ctx.Err()
			default:
			}
			runtime.Gosched()
			continue
		}
		c.records[update.SessionID] = cloneRecord(rec)
		if update.BaselineVersion > c.appliedBaseline[update.SessionID] {
			c.appliedBaseline[update.SessionID] = update.BaselineVersion
		}
		c.mu.Unlock()
		return cloneRecord(rec), nil
	}
	return FeatureRecord{}, ErrRetryExhausted
}

func (c *Coordinator) Decide(ctx context.Context, update IntentFeatureUpdate) (FeatureRecord, error) {
	select {
	case <-ctx.Done():
		return FeatureRecord{}, ctx.Err()
	default:
	}
	if c.slow == nil {
		return c.fallback(ctx, update.SessionID)
	}

	update = cloneUpdate(update)
	resultCh := make(chan slowResult, 1)
	slowCtx := ctx
	var cancel context.CancelFunc
	if c.slowTimeout > 0 {
		slowCtx, cancel = context.WithTimeout(ctx, c.slowTimeout)
	} else {
		slowCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	err := c.pool.Submit(slowCtx, func(taskCtx context.Context) error {
		out, inferErr := c.slow.Infer(taskCtx, update)
		select {
		case resultCh <- slowResult{update: out, err: inferErr}:
		case <-taskCtx.Done():
		}
		return inferErr
	})
	if err != nil {
		return c.fallback(ctx, update.SessionID)
	}

	select {
	case res := <-resultCh:
		if res.err != nil {
			return c.fallback(ctx, update.SessionID)
		}
		rec, applyErr := c.ApplySlow(ctx, res.update)
		if applyErr != nil {
			return c.fallback(ctx, update.SessionID)
		}
		return rec, nil
	case <-slowCtx.Done():
		return c.fallback(ctx, update.SessionID)
	case <-ctx.Done():
		return FeatureRecord{}, ctx.Err()
	}
}

func (c *Coordinator) Get(sessionID string) (FeatureRecord, bool) {
	c.mu.RLock()
	rec, ok := c.records[sessionID]
	c.mu.RUnlock()
	if !ok {
		return FeatureRecord{}, false
	}
	return cloneRecord(rec), true
}

func (c *Coordinator) Shutdown(ctx context.Context) error {
	if c == nil || c.pool == nil {
		return nil
	}
	return c.pool.Shutdown(ctx)
}

type slowResult struct {
	update IntentFeatureUpdate
	err    error
}

func (c *Coordinator) fallback(ctx context.Context, sessionID string) (FeatureRecord, error) {
	if c.ranker == nil {
		return FeatureRecord{}, ErrNoFallback
	}
	rec, err := c.ranker.Score(ctx, sessionID)
	if err != nil {
		return FeatureRecord{}, err
	}
	rec.SessionID = sessionID
	rec.Fallback = true
	rec.ReviewPassed = true
	rec = cloneRecord(rec)

	c.mu.Lock()
	_, exists := c.records[sessionID]
	if !exists {
		c.records[sessionID] = cloneRecord(rec)
	}
	c.mu.Unlock()
	return rec, nil
}

func invalidUpdate(update IntentFeatureUpdate) bool {
	return update.SessionID == "" || update.BaselineVersion <= 0 || len(update.IntentVector) == 0
}

func (c *Coordinator) fastSnapshot(sessionID string) (fastRecord, bool) {
	c.mu.RLock()
	fast, ok := c.fast[sessionID]
	c.mu.RUnlock()
	if !ok {
		return fastRecord{}, false
	}
	fast.vector = cloneFloat32s(fast.vector)
	fast.weights = cloneWeights(fast.weights)
	return fast, true
}

func (c *Coordinator) buildSlowRecord(update IntentFeatureUpdate, fast fastRecord, hasFast bool) FeatureRecord {
	latest := update.BaselineVersion
	if hasFast && fast.version > latest {
		latest = fast.version
	}
	drift := driftIndex(update.BaselineVersion, latest, c.driftWindowMillis)
	threshold := update.DriftThreshold
	if threshold < 0 {
		threshold = 0
	}
	if hasFast && drift > threshold {
		return FeatureRecord{
			SessionID:       update.SessionID,
			Version:         latest,
			IntentVector:    fuseVectors(c.alpha, update.IntentVector, fast.vector),
			CategoryWeights: fuseWeights(c.alpha, update.CategoryWeights, fast.weights),
			DriftIndex:      drift,
			Fused:           true,
			ReviewPassed:    true,
		}
	}
	return FeatureRecord{
		SessionID:       update.SessionID,
		Version:         latest,
		IntentVector:    cloneFloat32s(update.IntentVector),
		CategoryWeights: cloneWeights(update.CategoryWeights),
		DriftIndex:      drift,
		ReviewPassed:    true,
	}
}

func driftIndex(baseline, latest, windowMillis int64) float32 {
	if latest <= baseline {
		return 0
	}
	if windowMillis <= 0 {
		windowMillis = defaultDriftWindowMillis
	}
	drift := float32(latest-baseline) / float32(windowMillis)
	if drift > 1 {
		return 1
	}
	return drift
}

func fuseVectors(alpha float32, slow, fast []float32) []float32 {
	limit := len(slow)
	if len(fast) > limit {
		limit = len(fast)
	}
	if limit == 0 {
		return nil
	}
	out := make([]float32, limit)
	beta := 1 - alpha
	for i := 0; i < limit; i++ {
		var slowValue float32
		if i < len(slow) {
			slowValue = slow[i]
		}
		var fastValue float32
		if i < len(fast) {
			fastValue = fast[i]
		}
		out[i] = alpha*slowValue + beta*fastValue
	}
	return out
}

func fuseWeights(alpha float32, slow, fast map[string]float32) map[string]float32 {
	if len(slow) == 0 && len(fast) == 0 {
		return nil
	}
	out := make(map[string]float32, len(slow)+len(fast))
	beta := 1 - alpha
	for k, v := range fast {
		out[k] = beta * v
	}
	for k, v := range slow {
		out[k] += alpha * v
	}
	return out
}

func cloneRecord(rec FeatureRecord) FeatureRecord {
	rec.IntentVector = cloneFloat32s(rec.IntentVector)
	rec.CategoryWeights = cloneWeights(rec.CategoryWeights)
	return rec
}

func cloneUpdate(update IntentFeatureUpdate) IntentFeatureUpdate {
	update.IntentVector = cloneFloat32s(update.IntentVector)
	update.CategoryWeights = cloneWeights(update.CategoryWeights)
	return update
}

func cloneFloat32s(in []float32) []float32 {
	if len(in) == 0 {
		return nil
	}
	out := make([]float32, len(in))
	copy(out, in)
	return out
}

func cloneWeights(in map[string]float32) map[string]float32 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clamp01(v float32) float32 {
	if math.IsNaN(float64(v)) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
