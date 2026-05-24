package cache

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrMiss           = errors.New("cache: miss")
	ErrPartialTimeout = errors.New("cache: partial shard timeout")
	ErrInvalidKey     = errors.New("cache: invalid key")
)

type Feature struct {
	ID        int64
	Vector    []float32
	Category  string
	Brand     string
	Available bool
}

type Client interface {
	MGet(ctx context.Context, ids []int64, out []Feature) error
}

type Options struct {
	Shards    int
	IOTimeout time.Duration
}

type shard struct {
	mu           sync.RWMutex
	data         map[int64]Feature
	delay        time.Duration
	blockStarted chan struct{}
	blockRelease chan struct{}
}

type MemoryClient struct {
	shards    []shard
	ioTimeout time.Duration
}

func NewMemoryClient(opts Options) *MemoryClient {
	shardsN := opts.Shards
	if shardsN <= 0 {
		shardsN = 16
	}
	c := &MemoryClient{
		shards:    make([]shard, shardsN),
		ioTimeout: opts.IOTimeout,
	}
	if c.ioTimeout <= 0 {
		c.ioTimeout = time.Millisecond
	}
	for i := range c.shards {
		c.shards[i].data = make(map[int64]Feature)
	}
	return c
}

func (c *MemoryClient) Set(feature Feature) {
	if c == nil || len(c.shards) == 0 || feature.ID <= 0 {
		return
	}
	idx := c.shardIndex(feature.ID)
	cloned := cloneFeature(feature)
	cloned.Available = true
	s := &c.shards[idx]
	s.mu.Lock()
	s.data[feature.ID] = cloned
	s.mu.Unlock()
}

func (c *MemoryClient) MGet(ctx context.Context, ids []int64, out []Feature) error {
	return c.mget(ctx, ids, out, nil, 0, true)
}

func (c *MemoryClient) MGetInto(ctx context.Context, ids []int64, out []Feature, vectorBuf []float32, dim int) error {
	if c != nil && len(c.shards) == 1 {
		return c.mgetIntoSingleShard(ctx, ids, out, vectorBuf, dim)
	}
	return c.mget(ctx, ids, out, vectorBuf, dim, false)
}

func (c *MemoryClient) mgetIntoSingleShard(ctx context.Context, ids []int64, out []Feature, vectorBuf []float32, dim int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if c == nil || len(out) < len(ids) || dim <= 0 || len(vectorBuf) < len(ids)*dim {
		return ErrInvalidKey
	}
	for i, id := range ids {
		if id <= 0 {
			return ErrInvalidKey
		}
		out[i] = Feature{ID: id, Vector: vectorBuf[i*dim : i*dim], Available: false}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := c.loadShard(ctx, 0, ids, out, vectorBuf, dim, false); err != nil {
		return err
	}
	return c.finishMGet(ctx, ids, out)
}

func (c *MemoryClient) mget(ctx context.Context, ids []int64, out []Feature, vectorBuf []float32, dim int, cloneVectors bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if c == nil || len(c.shards) == 0 || len(out) < len(ids) {
		return ErrInvalidKey
	}
	if !cloneVectors && (dim <= 0 || len(vectorBuf) < len(ids)*dim) {
		return ErrInvalidKey
	}
	if len(ids) == 0 {
		return nil
	}
	for i, id := range ids {
		if id <= 0 {
			return ErrInvalidKey
		}
		out[i] = Feature{ID: id, Available: false}
		if !cloneVectors {
			out[i].Vector = vectorBuf[i*dim : i*dim]
		}
	}

	var used [64]bool
	var dynamic []bool
	if len(c.shards) > len(used) {
		dynamic = make([]bool, len(c.shards))
	}
	branches := 0
	for _, id := range ids {
		idx := c.shardIndex(id)
		if dynamic != nil {
			if !dynamic[idx] {
				dynamic[idx] = true
				branches++
			}
		} else if !used[idx] {
			used[idx] = true
			branches++
		}
	}

	if branches == 1 && !cloneVectors {
		for idx := range c.shards {
			isUsed := false
			if dynamic != nil {
				isUsed = dynamic[idx]
			} else {
				isUsed = used[idx]
			}
			if isUsed {
				if err := c.loadShard(ctx, idx, ids, out, vectorBuf, dim, cloneVectors); err != nil {
					return err
				}
				return c.finishMGet(ctx, ids, out)
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, c.ioTimeout)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(branches)
	errCh := make(chan error, branches)
	for idx := range c.shards {
		isUsed := false
		if dynamic != nil {
			isUsed = dynamic[idx]
		} else {
			isUsed = used[idx]
		}
		if !isUsed {
			continue
		}
		shardIdx := idx
		go func() {
			defer wg.Done()
			if err := c.loadShard(ctx, shardIdx, ids, out, vectorBuf, dim, cloneVectors); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return c.finishMGet(ctx, ids, out)
}

func (c *MemoryClient) finishMGet(ctx context.Context, ids []int64, out []Feature) error {
	if ctx.Err() != nil {
		return ErrPartialTimeout
	}
	allMiss := true
	for i := 0; i < len(ids); i++ {
		if out[i].Available {
			allMiss = false
			break
		}
	}
	if allMiss {
		return ErrMiss
	}
	return nil
}

func (c *MemoryClient) loadShard(ctx context.Context, shardIdx int, ids []int64, out []Feature, vectorBuf []float32, dim int, cloneVectors bool) error {
	s := &c.shards[shardIdx]
	if s.delay > 0 {
		t := time.NewTimer(s.delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return ErrPartialTimeout
		}
	}
	select {
	case <-ctx.Done():
		return ErrPartialTimeout
	default:
	}

	s.mu.RLock()
	for i, id := range ids {
		if c.shardIndex(id) != shardIdx {
			continue
		}
		select {
		case <-ctx.Done():
			s.mu.RUnlock()
			return ErrPartialTimeout
		default:
		}
		if s.blockStarted != nil {
			select {
			case s.blockStarted <- struct{}{}:
			default:
			}
			<-s.blockRelease
			select {
			case <-ctx.Done():
				s.mu.RUnlock()
				return ErrPartialTimeout
			default:
			}
		}
		if f, ok := s.data[id]; ok {
			if cloneVectors {
				out[i] = cloneFeature(f)
			} else {
				vec := vectorBuf[i*dim : i*dim+dim]
				copy(vec, f.Vector)
				out[i] = Feature{ID: f.ID, Vector: vec, Category: f.Category, Brand: f.Brand, Available: true}
			}
			out[i].Available = true
		} else {
			out[i] = Feature{ID: id, Available: false}
			if !cloneVectors {
				out[i].Vector = vectorBuf[i*dim : i*dim]
			}
		}
	}
	s.mu.RUnlock()
	return nil
}

func (c *MemoryClient) shardIndex(id int64) int {
	u := uint64(id)
	return int((u * 11400714819323198485) % uint64(len(c.shards)))
}

func cloneFeature(in Feature) Feature {
	out := in
	if len(in.Vector) > 0 {
		out.Vector = make([]float32, len(in.Vector))
		copy(out.Vector, in.Vector)
	} else {
		out.Vector = nil
	}
	return out
}

func (c *MemoryClient) SetShardDelayForTest(shardIdx int, delay time.Duration) {
	if c == nil || shardIdx < 0 || shardIdx >= len(c.shards) {
		return
	}
	c.shards[shardIdx].delay = delay
}

func (c *MemoryClient) SetShardWriteBlockForTest(shardIdx int, started chan struct{}, release chan struct{}) {
	if c == nil || shardIdx < 0 || shardIdx >= len(c.shards) {
		return
	}
	c.shards[shardIdx].blockStarted = started
	c.shards[shardIdx].blockRelease = release
}
