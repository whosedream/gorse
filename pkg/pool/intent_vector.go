package pool

import (
	"context"
	"sync"
)

const IntentVectorDim = 1024

type IntentVectorPool struct {
	pool sync.Pool
}

func NewIntentVectorPool() *IntentVectorPool {
	p := &IntentVectorPool{}
	p.pool.New = func() any { return new([IntentVectorDim]float32) }
	return p
}

func (p *IntentVectorPool) With(ctx context.Context, fn func([]float32) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p == nil {
		p = NewIntentVectorPool()
	}
	arr := p.pool.Get().(*[IntentVectorDim]float32)
	zeroIntentVector(arr)
	defer func() {
		zeroIntentVector(arr)
		p.pool.Put(arr)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fn(arr[:])
	}
}

func zeroIntentVector(arr *[IntentVectorDim]float32) {
	for i := range arr {
		arr[i] = 0
	}
}
