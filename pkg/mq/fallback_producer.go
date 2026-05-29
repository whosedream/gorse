package mq

import (
	"context"
)

// FallbackProducer tries Primary first; if it returns an error, falls
// through to Fallback. Useful for Kafka → Redis dual transport.
type FallbackProducer struct {
	Primary  Producer
	Fallback Producer
}

func (p *FallbackProducer) Publish(ctx context.Context, ev Event) error {
	if p.Primary != nil {
		if err := p.Primary.Publish(ctx, ev); err == nil {
			return nil
		}
	}
	if p.Fallback != nil {
		return p.Fallback.Publish(ctx, ev)
	}
	return ErrInvalidProducerOptions
}

func (p *FallbackProducer) Close() error {
	var errs []error
	if p.Primary != nil {
		if err := p.Primary.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.Fallback != nil {
		if err := p.Fallback.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
