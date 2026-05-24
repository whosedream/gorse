package pool

import (
	"context"
	"sync"
)

// MemoryPool is a small typed wrapper around sync.Pool for hot-path temporary
// request state. Callers must keep the pooled pointer inside the callback.
type MemoryPool[T any] struct {
	pool  sync.Pool
	reset func(*T)
}

// NewMemoryPool creates a typed pool. reset is called immediately after Get and
// immediately before Put to prevent cross-request data contamination.
func NewMemoryPool[T any](reset func(*T)) *MemoryPool[T] {
	mp := &MemoryPool[T]{reset: reset}
	mp.pool.New = func() any { return new(T) }
	return mp
}

// With borrows one object for fn and returns it to the pool after a full reset.
func (p *MemoryPool[T]) With(ctx context.Context, fn func(*T) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	v := p.pool.Get().(*T)
	p.resetObject(v)
	defer func() {
		p.resetObject(v)
		p.pool.Put(v)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fn(v)
	}
}

func (p *MemoryPool[T]) resetObject(v *T) {
	if p.reset != nil {
		p.reset(v)
		return
	}
	var zero T
	*v = zero
}

// ByteBufferPool reuses byte buffers while zeroing retained capacity before Put.
type ByteBufferPool struct {
	pool      sync.Pool
	minCap    int
	maxRetain int
}

// NewByteBufferPool creates a byte buffer pool. maxRetain caps buffers retained
// after malicious or accidental oversized input.
func NewByteBufferPool(minCap, maxRetain int) *ByteBufferPool {
	if minCap < 0 {
		minCap = 0
	}
	if maxRetain < minCap {
		maxRetain = minCap
	}
	bp := &ByteBufferPool{minCap: minCap, maxRetain: maxRetain}
	bp.pool.New = func() any {
		buf := make([]byte, 0, minCap)
		return &buf
	}
	return bp
}

// With borrows a zero-length byte slice with at least need capacity.
func (p *ByteBufferPool) With(ctx context.Context, need int, fn func([]byte) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if need < 0 {
		need = 0
	}

	bufp := p.pool.Get().(*[]byte)
	buf := *bufp
	if cap(buf) < need {
		buf = make([]byte, 0, need)
	} else {
		buf = buf[:0]
	}

	defer func() {
		if cap(buf) > p.maxRetain {
			buf = make([]byte, 0, p.minCap)
		} else {
			zeroBytes(buf[:cap(buf)])
			buf = buf[:0]
		}
		*bufp = buf
		p.pool.Put(bufp)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fn(buf[:0])
	}
}

func zeroBytes(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}
