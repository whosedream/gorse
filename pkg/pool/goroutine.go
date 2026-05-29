package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrOverloaded is returned when the bounded queue is full.
	ErrOverloaded = errors.New("pool overloaded")
	// ErrPoolClosed is returned after shutdown begins.
	ErrPoolClosed = errors.New("pool closed")
	// ErrInvalidPoolConfig reports invalid pool sizing.
	ErrInvalidPoolConfig = errors.New("invalid pool config")
	// ErrInvalidTask reports an empty task submission.
	ErrInvalidTask = errors.New("invalid task")
)

// Task is the unit executed by GoroutinePool workers.
type Task func(context.Context) error

type queuedTask struct {
	ctx context.Context
	fn  Task
}

// GoroutinePool is a fixed-size bounded worker pool with non-blocking
// backpressure. All workers are pre-spawned at creation time; the queue
// has a fixed capacity. When the queue is saturated, Submit returns
// ErrOverloaded immediately without blocking.
type GoroutinePool struct {
	queue        chan queuedTask
	stop         chan struct{}
	shutdownDone chan struct{}
	workerCount  int
	closed       atomic.Bool
	wg           sync.WaitGroup
	acceptMu     sync.RWMutex
	shutdownOnce sync.Once
}

// NewFixedPool creates a pool with exactly workerCount pre-spawned workers
// and a queue of queueCap capacity. This is the preferred constructor for
// the production gateway path (200 workers, 4096 queue).
func NewFixedPool(workerCount, queueCap int) (*GoroutinePool, error) {
	if workerCount <= 0 || queueCap <= 0 {
		return nil, ErrInvalidPoolConfig
	}
	p := &GoroutinePool{
		queue:       make(chan queuedTask, queueCap),
		stop:        make(chan struct{}),
		shutdownDone: make(chan struct{}),
		workerCount: workerCount,
	}
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p, nil
}

// NewGoroutinePool starts minWorkers workers and allocates a fixed queue.
// Kept for backward compatibility with existing callers.
func NewGoroutinePool(minWorkers, maxWorkers, queueCap int) (*GoroutinePool, error) {
	if minWorkers <= 0 || maxWorkers < minWorkers || queueCap <= 0 {
		return nil, ErrInvalidPoolConfig
	}
	p := &GoroutinePool{
		queue:       make(chan queuedTask, queueCap),
		stop:        make(chan struct{}),
		shutdownDone: make(chan struct{}),
		workerCount: minWorkers,
	}
	for i := 0; i < minWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p, nil
}

// Submit enqueues task or immediately returns ErrOverloaded when the queue is
// saturated. It never waits for queue capacity.
func (p *GoroutinePool) Submit(ctx context.Context, fn Task) error {
	if fn == nil {
		return ErrInvalidTask
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.acceptMu.RLock()
	defer p.acceptMu.RUnlock()
	if p.closed.Load() {
		return ErrPoolClosed
	}
	qt := queuedTask{ctx: ctx, fn: fn}
	select {
	case p.queue <- qt:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrOverloaded
	}
}

// WorkerCount returns the currently live worker count.
func (p *GoroutinePool) WorkerCount() int {
	return p.workerCount
}

// QueueLen returns the queued task count.
func (p *GoroutinePool) QueueLen() int {
	return len(p.queue)
}

// Shutdown stops accepting work and waits for workers to exit or ctx to cancel.
func (p *GoroutinePool) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		p.acceptMu.Lock()
		p.closed.Store(true)
		close(p.stop)
		p.acceptMu.Unlock()
		go func() {
			p.wg.Wait()
			close(p.shutdownDone)
		}()
	})
	select {
	case <-p.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *GoroutinePool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			p.drainQueue()
			return
		case task := <-p.queue:
			runTask(task)
		}
	}
}

func (p *GoroutinePool) drainQueue() {
	for {
		select {
		case task := <-p.queue:
			runTask(task)
		default:
			return
		}
	}
}

func runTask(task queuedTask) {
	_ = task.fn(task.ctx)
}

// Extractor is a context-bound parallel feature extraction branch.
type Extractor func(context.Context) error

// ParallelExtract runs branches through the pool under a shared timeout. The
// first branch error cancels siblings and is returned to the caller.
func ParallelExtract(ctx context.Context, p *GoroutinePool, timeout time.Duration, branches ...Extractor) error {
	if len(branches) == 0 {
		return nil
	}
	var cancel context.CancelFunc
	if timeout <= 0 {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	var once sync.Once
	errCh := make(chan error, 1)
	done := make(chan struct{}, len(branches))

	submitted := 0
	for _, branch := range branches {
		b := branch
		err := p.Submit(ctx, func(taskCtx context.Context) error {
			defer func() { done <- struct{}{} }()
			err := b(taskCtx)
			if err != nil {
				once.Do(func() {
					errCh <- err
					cancel()
				})
			}
			return err
		})
		if err != nil {
			cancel()
			for submitted > 0 {
				<-done
				submitted--
			}
			select {
			case branchErr := <-errCh:
				return branchErr
			default:
				return err
			}
		}
		submitted++
	}

	remaining := submitted
	var firstErr error
	for remaining > 0 {
		select {
		case err := <-errCh:
			if firstErr == nil {
				firstErr = err
				cancel()
			}
		case <-done:
			remaining--
		case <-ctx.Done():
			if firstErr == nil {
				select {
				case err := <-errCh:
					firstErr = err
				default:
					firstErr = ctx.Err()
				}
			}
		}
	}
	return firstErr
}
