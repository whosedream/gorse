package fsm

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParser(t *testing.T) {
	t.Parallel()

	const validSession = "123e4567-e89b-12d3-a456-426614174000"
	tests := []struct {
		name         string
		input        string
		wantErr      error
		wantFallback bool
		wantSession  string
		wantVersion  int64
		wantCategory string
		wantBrand    string
		ctx          func() (context.Context, context.CancelFunc)
	}{
		{
			name:         "happy path parses core fields",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1710000000123,"slots":{"category":"phone","brand":"acme"}}`,
			wantSession:  validSession,
			wantVersion:  1710000000123,
			wantCategory: "phone",
			wantBrand:    "acme",
		},
		{
			name:         "skips unordered unknown fields and candidate array",
			input:        `{"candidate_ids":[1001,1002,{"nested":[true,false,null,"x"]}],"slots":{"unknown":"skip-me","brand":"contoso","category":"laptop"},"ignored":{"a":[1,2,3]},"version_stamp":"1710000000999","session_id":"123e4567-e89b-12d3-a456-426614174000"}`,
			wantSession:  validSession,
			wantVersion:  1710000000999,
			wantCategory: "laptop",
			wantBrand:    "contoso",
		},
		{
			name:         "escaped known key and slash value use scratch path",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":171,"slots":{"category":"laptop","brand":"acme\/pro"}}`,
			wantSession:  validSession,
			wantVersion:  171,
			wantCategory: "laptop",
			wantBrand:    "acme/pro",
		},
		{
			name:         "escaped value decodes control and bmp unicode bytes",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":171,"slots":{"category":"a\b\f\n\r\tz","brand":"中A"}}`,
			wantSession:  validSession,
			wantVersion:  171,
			wantCategory: "a\b\f\n\r\tz",
			wantBrand:    "中A",
		},
		{
			name:         "long escaped unknown key does not fail when required fields are valid",
			input:        `{"` + strings.Repeat(`a\/`, scratchCap) + `":true,"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":7,"slots":{"category":"phone","brand":"acme"}}`,
			wantSession:  validSession,
			wantVersion:  7,
			wantCategory: "phone",
			wantBrand:    "acme",
		},
		{
			name:         "top level overflow escaped unknown key validates trailing invalid escape",
			input:        `{"` + strings.Repeat(`a\/`, scratchCap) + `\q":true,"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":7,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "slots overflow escaped unknown key validates trailing invalid escape",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":7,"slots":{"category":"phone","brand":"acme","` + strings.Repeat(`a\/`, scratchCap) + `\q":true}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "empty slots triggers fallback without parse error",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{}}`,
			wantFallback: true,
			wantSession:  validSession,
			wantVersion:  1,
		},
		{
			name:         "empty category and brand trigger fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":2,"slots":{"category":"","brand":""}}`,
			wantFallback: true,
			wantSession:  validSession,
			wantVersion:  2,
		},
		{
			name:         "short session id is malformed and fallback",
			input:        `{"session_id":"123e4567","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "empty session id is malformed and fallback",
			input:        `{"session_id":"","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "session id with wrong dash layout is malformed and fallback",
			input:        `{"session_id":"123e4567e89b-12d3-a456-4266141740000","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "session id with non hex char is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-42661417400g","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "numeric decimal version stamp is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1.9,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "numeric exponent version stamp is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1e6,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "string decimal version stamp is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":"1.9","slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "string exponent version stamp is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":"1e6","slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "version stamp int64 overflow is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":9223372036854775808,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "string version stamp int64 overflow is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":"9223372036854775808","slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "target string invalid escape is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"bad\q","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "target string invalid unicode hex is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"bad\u12xz","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown string invalid escape is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"unknown":"bad\q"}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown object key invalid escape is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"unknown":{"a\q":1}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "missing session id is malformed and fallback",
			input:        `{"version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "missing version stamp is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "missing slots is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "missing colon is malformed and fallback",
			input:        `{"session_id" "123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unterminated string is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone,"brand":"acme"}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "slots non object is malformed and fallback",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":"bad"}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown object missing colon is malformed",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"unknown":{"a" 1}}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown array trailing comma is malformed",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"candidate_ids":[1,2,]}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown leading zero number is malformed",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"bad":01}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown decimal without fraction is malformed",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"bad":1.}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "unknown exponent without digits is malformed",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"bad":1e}`,
			wantErr:      ErrMalformed,
			wantFallback: true,
		},
		{
			name:         "malicious long input is rejected quickly",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"},"pad":"` + strings.Repeat("x", MaxInputSize) + `"}`,
			wantErr:      ErrInputTooLarge,
			wantFallback: true,
		},
		{
			name:         "oversized category value fails fast",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"` + strings.Repeat("c", 65) + `","brand":"acme"}}`,
			wantErr:      ErrValueTooLarge,
			wantFallback: true,
		},
		{
			name:         "context canceled fails before parse",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      context.Canceled,
			wantFallback: true,
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
		},
		{
			name:         "context deadline fails before parse",
			input:        `{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":1,"slots":{"category":"phone","brand":"acme"}}`,
			wantErr:      context.DeadlineExceeded,
			wantFallback: true,
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				return ctx, cancel
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cancel := func() {}
			if tt.ctx != nil {
				ctx, cancel = tt.ctx()
			}
			defer cancel()

			p := NewParser()
			var out RerankRequest
			err := p.Parse(ctx, []byte(tt.input), &out)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Parse error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Parse unexpected error = %v", err)
			}
			if out.Fallback != tt.wantFallback {
				t.Fatalf("Fallback = %v, want %v", out.Fallback, tt.wantFallback)
			}
			if got := out.SessionIDString(); got != tt.wantSession {
				t.Fatalf("SessionIDString = %q, want %q", got, tt.wantSession)
			}
			if out.VersionStamp != tt.wantVersion {
				t.Fatalf("VersionStamp = %d, want %d", out.VersionStamp, tt.wantVersion)
			}
			if got := out.CategoryString(); got != tt.wantCategory {
				t.Fatalf("CategoryString = %q, want %q", got, tt.wantCategory)
			}
			if got := out.BrandString(); got != tt.wantBrand {
				t.Fatalf("BrandString = %q, want %q", got, tt.wantBrand)
			}
		})
	}
}

func TestParserReuseDoesNotCrossPollute(t *testing.T) {
	t.Parallel()

	p := NewParser()
	var out RerankRequest
	first := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":11,"slots":{"category":"phone","brand":"acme"}}`)
	if err := p.Parse(context.Background(), first, &out); err != nil {
		t.Fatalf("first Parse error = %v", err)
	}
	if out.CategoryString() != "phone" || out.BrandString() != "acme" {
		t.Fatalf("first parse got category=%q brand=%q", out.CategoryString(), out.BrandString())
	}

	second := []byte(`{"session_id":"00000000-0000-0000-0000-000000000000","version_stamp":12,"slots":{}}`)
	if err := p.Parse(context.Background(), second, &out); err != nil {
		t.Fatalf("second Parse error = %v", err)
	}
	if !out.Fallback {
		t.Fatalf("second parse Fallback=false, want true")
	}
	if got := out.SessionIDString(); got != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("second SessionIDString = %q", got)
	}
	if got := out.CategoryString(); got != "" {
		t.Fatalf("category polluted across requests: %q", got)
	}
	if got := out.BrandString(); got != "" {
		t.Fatalf("brand polluted across requests: %q", got)
	}
}

func TestParserConcurrentSurge(t *testing.T) {
	p := NewParser()
	input := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","version_stamp":99,"slots":{"category":"phone","brand":"acme"},"candidate_ids":[1,2,3,4]}`)
	const goroutines = 128
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			var out RerankRequest
			if err := p.Parse(context.Background(), input, &out); err != nil {
				t.Errorf("Parse error = %v", err)
				return
			}
			if out.Fallback || out.VersionStamp != 99 || out.CategoryString() != "phone" || out.BrandString() != "acme" {
				t.Errorf("bad output: fallback=%v version=%d category=%q brand=%q", out.Fallback, out.VersionStamp, out.CategoryString(), out.BrandString())
			}
		}()
	}
	wg.Wait()
}
