package scorer

import (
	"context"
	"errors"

	"go-rec/pkg/pool"
)

var (
	ErrOutputTooSmall    = errors.New("scorer: output too small")
	ErrTooManyCandidates = errors.New("scorer: too many candidates")
)

type Candidate struct {
	ID       int64
	Category string
	Brand    string
	Feature  []float32
}

type Result struct {
	ID       int64
	Score    float32
	Category string
	Brand    string
}

type Options struct {
	TopK            int
	DiversityWindow int
	MaxSameCategory int
	MaxSameBrand    int
	MaxCandidates   int
}

type Engine struct {
	topK            int
	window          int
	maxSameCategory int
	maxSameBrand    int
	maxCandidates   int
	scratchPool     *pool.MemoryPool[rankScratch]
}

type rankItem struct {
	idx   int
	score float32
}

type rankScratch struct {
	items []rankItem
	used  int
}

func NewEngine(opts Options) *Engine {
	maxCandidates := opts.MaxCandidates
	if maxCandidates < 0 {
		maxCandidates = 0
	}
	e := &Engine{
		topK:            opts.TopK,
		window:          opts.DiversityWindow,
		maxSameCategory: opts.MaxSameCategory,
		maxSameBrand:    opts.MaxSameBrand,
		maxCandidates:   maxCandidates,
	}
	e.scratchPool = pool.NewMemoryPool(func(s *rankScratch) {
		for i := 0; i < s.used && i < len(s.items); i++ {
			s.items[i] = rankItem{}
		}
		s.items = s.items[:0]
		s.used = 0
	})
	return e
}

func (e *Engine) RankParallel(ctx context.Context, gp *pool.GoroutinePool, intent []float32, candidates []Candidate, out []Result) (int, error) {
	if gp == nil || len(candidates) < 2 {
		return e.Rank(ctx, intent, candidates, out)
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if e == nil {
		return 0, ErrOutputTooSmall
	}
	if e.maxCandidates > 0 && len(candidates) > e.maxCandidates {
		return 0, ErrTooManyCandidates
	}
	limit := e.topK
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	if len(out) < limit {
		return 0, ErrOutputTooSmall
	}
	if limit == 0 {
		return 0, nil
	}
	var n int
	err := e.scratchPool.With(ctx, func(s *rankScratch) error {
		if cap(s.items) < len(candidates) {
			capNeed := len(candidates)
			if e.maxCandidates > capNeed {
				capNeed = e.maxCandidates
			}
			s.items = make([]rankItem, 0, capNeed)
		}
		s.items = s.items[:len(candidates)]
		s.used = len(candidates)
		mid := len(candidates) / 2
		if err := pool.ParallelExtract(ctx, gp, 0,
			func(taskCtx context.Context) error { return scoreRange(taskCtx, intent, candidates, s.items, 0, mid) },
			func(taskCtx context.Context) error {
				return scoreRange(taskCtx, intent, candidates, s.items, mid, len(candidates))
			},
		); err != nil {
			return err
		}
		if err := selectTopK(ctx, s.items, len(s.items)); err != nil {
			return err
		}
		if err := fillDiverseResults(ctx, candidates, s.items, out[:limit], e.window, e.maxSameCategory, e.maxSameBrand); err != nil {
			return err
		}
		n = limit
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (e *Engine) Rank(ctx context.Context, intent []float32, candidates []Candidate, out []Result) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if e == nil {
		return 0, ErrOutputTooSmall
	}
	if e.maxCandidates > 0 && len(candidates) > e.maxCandidates {
		return 0, ErrTooManyCandidates
	}
	limit := e.topK
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	if len(out) < limit {
		return 0, ErrOutputTooSmall
	}
	if limit == 0 {
		return 0, nil
	}

	var n int
	err := e.scratchPool.With(ctx, func(s *rankScratch) error {
		if cap(s.items) < len(candidates) {
			capNeed := len(candidates)
			if e.maxCandidates > capNeed {
				capNeed = e.maxCandidates
			}
			s.items = make([]rankItem, 0, capNeed)
		}
		s.items = s.items[:len(candidates)]
		s.used = len(candidates)
		for i := range candidates {
			if (i & 63) == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			s.items[i] = rankItem{idx: i, score: dot(intent, candidates[i].Feature)}
		}
		if err := selectTopK(ctx, s.items, len(s.items)); err != nil {
			return err
		}
		if err := fillDiverseResults(ctx, candidates, s.items, out[:limit], e.window, e.maxSameCategory, e.maxSameBrand); err != nil {
			return err
		}
		n = limit
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func scoreRange(ctx context.Context, intent []float32, candidates []Candidate, items []rankItem, start, end int) error {
	for i := start; i < end; i++ {
		if (i & 63) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		items[i] = rankItem{idx: i, score: dot(intent, candidates[i].Feature)}
	}
	return nil
}

func dot(a, b []float32) float32 {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	var s float32
	for i := 0; i < limit; i++ {
		s += a[i] * b[i]
	}
	return s
}

func selectTopK(ctx context.Context, items []rankItem, k int) error {
	for i := 0; i < k; i++ {
		if (i & 31) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		best := i
		bestScore := items[i].score
		bestIdx := items[i].idx
		for j := i + 1; j < len(items); j++ {
			if (j & 255) == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			if items[j].score > bestScore || (items[j].score == bestScore && items[j].idx < bestIdx) {
				best = j
				bestScore = items[j].score
				bestIdx = items[j].idx
			}
		}
		if best != i {
			items[i], items[best] = items[best], items[i]
		}
	}
	return nil
}

func fillDiverseResults(ctx context.Context, candidates []Candidate, items []rankItem, out []Result, window, maxCat, maxBrand int) error {
	if maxCat <= 0 {
		maxCat = window
	}
	if maxBrand <= 0 {
		maxBrand = window
	}
	selected := 0
	usedPrefix := 0
	for selected < len(out) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pick := -1
		for j := usedPrefix; j < len(items); j++ {
			c := candidates[items[j].idx]
			candidate := Result{ID: c.ID, Score: items[j].score, Category: c.Category, Brand: c.Brand}
			out[selected] = candidate
			if window <= 1 || !violatesWindow(out[:selected+1], selected, window, maxCat, maxBrand) {
				pick = j
				break
			}
		}
		if pick < 0 {
			pick = usedPrefix
			c := candidates[items[pick].idx]
			out[selected] = Result{ID: c.ID, Score: items[pick].score, Category: c.Category, Brand: c.Brand}
		}
		items[usedPrefix], items[pick] = items[pick], items[usedPrefix]
		usedPrefix++
		selected++
	}
	return nil
}

func applyDiversity(results []Result, window, maxCat, maxBrand int) {
	if len(results) <= 1 || window <= 1 {
		return
	}
	if maxCat <= 0 {
		maxCat = window
	}
	if maxBrand <= 0 {
		maxBrand = window
	}
	for pos := 0; pos < len(results); pos++ {
		if !violatesWindow(results, pos, window, maxCat, maxBrand) {
			continue
		}
		swap := -1
		for j := pos + 1; j < len(results); j++ {
			if candidateFits(results, pos, j, window, maxCat, maxBrand) {
				swap = j
				break
			}
		}
		if swap >= 0 {
			results[pos], results[swap] = results[swap], results[pos]
		}
	}
}

func violatesWindow(results []Result, pos, window, maxCat, maxBrand int) bool {
	catCount := 1
	brandCount := 1
	start := pos - window + 1
	if start < 0 {
		start = 0
	}
	for i := start; i < pos; i++ {
		if results[i].Category == results[pos].Category {
			catCount++
		}
		if results[i].Brand == results[pos].Brand {
			brandCount++
		}
	}
	return catCount > maxCat || brandCount > maxBrand
}

func candidateFits(results []Result, pos, cand, window, maxCat, maxBrand int) bool {
	catCount := 1
	brandCount := 1
	start := pos - window + 1
	if start < 0 {
		start = 0
	}
	for i := start; i < pos; i++ {
		if results[i].Category == results[cand].Category {
			catCount++
		}
		if results[i].Brand == results[cand].Brand {
			brandCount++
		}
	}
	return catCount <= maxCat && brandCount <= maxBrand
}
