package mq

import (
	"context"
	"log"
	"sync"
)

// FanoutProducer publishes to all contained producers. Errors from any
// single producer are logged but do not stop the fanout.
type FanoutProducer struct {
	Producers []Producer
}

func NewFanoutProducer(producers ...Producer) *FanoutProducer {
	return &FanoutProducer{Producers: producers}
}

func (p *FanoutProducer) Publish(ctx context.Context, ev Event) error {
	var wg sync.WaitGroup
	for _, prod := range p.Producers {
		if prod == nil {
			continue
		}
		prod := prod
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := prod.Publish(ctx, ev); err != nil {
				log.Printf("fanout: publish to %T failed: %v", prod, err)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (p *FanoutProducer) Close() error {
	for _, prod := range p.Producers {
		if prod == nil {
			continue
		}
		_ = prod.Close()
	}
	return nil
}
