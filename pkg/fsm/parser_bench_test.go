package fsm

import (
	"context"
	"testing"
)

func BenchmarkParserHotPathNoAlloc(b *testing.B) {
	p := NewParser()
	input := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1710000000123,"slots":{"category":"phone","brand":"acme"},"candidate_ids":[1001,1002,1003]}`)
	ctx := context.Background()
	var out RerankRequest

	if err := p.Parse(ctx, input, &out); err != nil {
		b.Fatalf("warm Parse error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Parse(ctx, input, &out); err != nil {
			b.Fatalf("Parse error = %v", err)
		}
	}
}
