package pool

import (
	"context"
	"sync"
)

// Bytes4KPool reuses fixed 4096-byte buffers for intent vector serialization.
// The borrowed pointer must not escape the callback.
type Bytes4KPool struct {
	pool sync.Pool
}

// NewBytes4KPool creates a pool for fixed 4KB byte arrays.
func NewBytes4KPool() *Bytes4KPool {
	p := &Bytes4KPool{}
	p.pool.New = func() any { return new([4096]byte) }
	return p
}

// With borrows a zeroed 4KB array, executes fn, then zeroes before Put to avoid
// cross-request contamination.
func (p *Bytes4KPool) With(ctx context.Context, fn func(*[4096]byte) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p == nil {
		p = NewBytes4KPool()
	}
	buf := p.pool.Get().(*[4096]byte)
	zero4K(buf)
	defer func() {
		zero4K(buf)
		p.pool.Put(buf)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fn(buf)
	}
}

func zero4K(buf *[4096]byte) {
	for i := range buf {
		buf[i] = 0
	}
}
