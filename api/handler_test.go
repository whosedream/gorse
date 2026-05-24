package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-rec/internal/rk/anti_drift"
	"go-rec/internal/rk/scorer"
	"go-rec/pkg/cache"
	"go-rec/pkg/fsm"
	"go-rec/pkg/mq"
	"go-rec/pkg/pool"
)

const testSessionID = "123e4567-e89b-12d3-a456-426614174000"

func TestServer(t *testing.T) {
	t.Parallel()

	t.Run("golden path completes cache anti drift and parallel scorer", func(t *testing.T) {
		t.Parallel()
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2, 3}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1710000000123, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1710000000123)))
		s.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"session_id":"`+testSessionID+`"`) || !strings.Contains(body, `"results":[`) || !strings.Contains(body, `"id":3`) {
			t.Fatalf("unexpected response body: %s", body)
		}
	})

	malformedCases := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "malformed json", body: `{"session_id":`},
		{name: "oversized malicious payload", body: strings.Repeat("x", fsm.MaxInputSize+8)},
	}
	for _, tc := range malformedCases {
		tc := tc
		t.Run("malformed payload returns 400/"+tc.name, func(t *testing.T) {
			t.Parallel()
			s := newTestServer(t, Options{CandidateIDs: []int64{1}, VectorDim: 2, MaxInFlight: 4, Timeout: 50 * time.Millisecond})
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(tc.body))
			s.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%q, want 400", rr.Code, rr.Body.String())
			}
		})
	}

	t.Run("limiter saturation returns 429 without waiting", func(t *testing.T) {
		t.Parallel()
		s := newTestServer(t, Options{CandidateIDs: []int64{1}, VectorDim: 2, MaxInFlight: 1, Timeout: 50 * time.Millisecond})
		s.limiter <- struct{}{}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d body=%q, want 429", rr.Code, rr.Body.String())
		}
	})

	t.Run("cache timeout returns 504", func(t *testing.T) {
		t.Parallel()
		c := cache.NewMemoryClient(cache.Options{Shards: 1, IOTimeout: time.Millisecond})
		c.Set(cache.Feature{ID: 1, Vector: []float32{1, 1}, Category: "phone", Brand: "acme", Available: true})
		c.SetShardDelayForTest(0, 50*time.Millisecond)
		s := newTestServer(t, Options{Cache: c, CandidateIDs: []int64{1}, VectorDim: 2, MaxInFlight: 2, Timeout: 3 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusGatewayTimeout {
			t.Fatalf("status = %d body=%q, want 504", rr.Code, rr.Body.String())
		}
	})

	t.Run("high concurrency surge either succeeds or explicitly sheds load", func(t *testing.T) {
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2, 3}, VectorDim: 2, MaxInFlight: 4, Timeout: 50 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{1, 1})
		var ok, shed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rr := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
				s.ServeHTTP(rr, req)
				switch rr.Code {
				case http.StatusOK:
					ok.Add(1)
				case http.StatusTooManyRequests:
					shed.Add(1)
				default:
					t.Errorf("unexpected status = %d body=%q", rr.Code, rr.Body.String())
				}
			}()
		}
		wg.Wait()
		if ok.Load() == 0 || ok.Load()+shed.Load() != 64 {
			t.Fatalf("surge ok=%d shed=%d, want all requests handled with success or explicit backpressure", ok.Load(), shed.Load())
		}
	})
}

func TestServerIntentReader(t *testing.T) {
	t.Parallel()

	t.Run("reader vector changes sorting", func(t *testing.T) {
		t.Parallel()
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			dst[0] = 0
			dst[1] = 10
		}}
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{10, 0})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[{"id":2`) {
			t.Fatalf("reader intent did not move candidate 2 first: %s", body)
		}
	})

	t.Run("blocked reader times out and silently degrades with HTTP 200", func(t *testing.T) {
		t.Parallel()
		reader := &stubIntentReader{block: 50 * time.Millisecond, fill: func(dst []float32) {
			dst[0] = 0
			dst[1] = 10
		}}
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{10, 0})

		start := time.Now()
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		elapsed := time.Since(start)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		if elapsed >= 30*time.Millisecond {
			t.Fatalf("elapsed = %s, want bounded degradation under 30ms", elapsed)
		}
		if !strings.Contains(rr.Body.String(), `"results":[{"id":1`) {
			t.Fatalf("timeout path did not fall back to coordinator intent: %s", rr.Body.String())
		}
	})

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "notfound degrades", err: cache.ErrIntentNotFound},
		{name: "corrupt degrades", err: cache.ErrCorruptIntent},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reader := &stubIntentReader{err: tc.err, fill: func(dst []float32) {
				dst[0] = 0
				dst[1] = 10
			}}
			s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
			seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{10, 0})
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
			s.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), `"results":[{"id":1`) {
				t.Fatalf("%s did not fall back to coordinator intent: %s", tc.name, rr.Body.String())
			}
		})
	}

	t.Run("reader observes deadline", func(t *testing.T) {
		t.Parallel()
		reader := &stubIntentReader{checkDeadline: true, minDeadline: time.Millisecond, maxDeadline: 20 * time.Millisecond}
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{10, 0})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		if reader.deadlineErr.Load() != nil {
			t.Fatalf("deadline check failed: %v", reader.deadlineErr.Load())
		}
	})
}

func TestServerPublishesBehaviorWithoutBlockingResponse(t *testing.T) {
	t.Parallel()
	producer := &blockingProducer{started: make(chan mq.Event, 1), release: make(chan struct{})}
	s := newTestServer(t, Options{CandidateIDs: []int64{1, 2, 3}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, BehaviorProducer: producer})
	seedFastRecord(t, s.coordinator, testSessionID, 1710000000123, []float32{1, 1})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(testSessionID, 1710000000123)))

	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
	}
	defer close(producer.release)
	select {
	case ev := <-producer.started:
		if ev.SessionID != testSessionID || ev.Timestamp != 1710000000123 || ev.Action != "rerank" {
			t.Fatalf("unexpected produced event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("producer was not called")
	}
}

type stubIntentReader struct {
	version       int64
	err           error
	block         time.Duration
	fill          func([]float32)
	checkDeadline bool
	minDeadline   time.Duration
	maxDeadline   time.Duration
	deadlineErr   atomic.Value
}

func (r *stubIntentReader) ReadIntent(ctx context.Context, sessionID string, dst []float32) (int64, error) {
	if r.checkDeadline {
		deadline, ok := ctx.Deadline()
		if !ok {
			r.deadlineErr.Store(errors.New("missing deadline"))
		} else {
			remaining := time.Until(deadline)
			if remaining < r.minDeadline || remaining > r.maxDeadline {
				r.deadlineErr.Store(errors.New("deadline outside expected range"))
			}
		}
	}
	if r.block > 0 {
		t := time.NewTimer(r.block)
		select {
		case <-t.C:
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return 0, ctx.Err()
		}
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.fill != nil {
		r.fill(dst)
	}
	if r.version == 0 {
		return 1, nil
	}
	return r.version, nil
}

type blockingProducer struct {
	started chan mq.Event
	release chan struct{}
}

func (p *blockingProducer) Publish(ctx context.Context, ev mq.Event) error {
	select {
	case p.started <- ev:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return nil
	}
}

func (p *blockingProducer) Close() error { return nil }

type mockProductSearcher struct {
	results []ProductResult
	err     error
}

func (m *mockProductSearcher) SearchWithIntent(
	_ context.Context, _ []float32, _ string, _ int,
) ([]ProductResult, error) {
	return m.results, m.err
}

func TestServerProductSearchIntegration(t *testing.T) {
	t.Run("product search results used for scoring when available", func(t *testing.T) {
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			for i := range dst {
				dst[i] = 0.01
			}
		}}
		searcher := &mockProductSearcher{
			results: []ProductResult{
				{ItemID: "db1", Title: "DuckDB Product", Category: "phone", Price: 99, Embedding: []float32{1, 0}},
			},
		}
		s := newTestServer(t, Options{
			CandidateIDs:      []int64{1, 2, 3},
			VectorDim:         2,
			MaxInFlight:       8,
			Timeout:           50 * time.Millisecond,
			IntentReader:      reader,
			IntentReadTimeout: 2 * time.Millisecond,
			ProductSearch:     searcher,
		})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank",
			bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[`) {
			t.Fatal("response must contain results array")
		}
		if !strings.Contains(body, `"session_id":"`+testSessionID+`"`) {
			t.Fatal("response must contain session_id")
		}
		t.Logf("product search integration response: %s", body)
	})

	t.Run("product search nil falls back to cache-based scoring", func(t *testing.T) {
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			dst[0] = 0
			dst[1] = 10
		}}
		s := newTestServer(t, Options{
			CandidateIDs:      []int64{1, 2},
			VectorDim:         2,
			MaxInFlight:       8,
			Timeout:           50 * time.Millisecond,
			IntentReader:      reader,
			IntentReadTimeout: 2 * time.Millisecond,
			ProductSearch:     nil,
		})
		seedFastRecord(t, s.coordinator, testSessionID, 1, []float32{10, 0})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank",
			bytes.NewBufferString(validPayload(testSessionID, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[{"id":2`) {
			t.Fatalf("cache fallback did not put candidate 2 first: %s", body)
		}
	})
}

func BenchmarkServerGoldenPath(b *testing.B) {
	s := newTestServerB(b, Options{CandidateIDs: []int64{1, 2, 3}, VectorDim: 2, MaxInFlight: 1024, Timeout: 50 * time.Millisecond})
	seedFastRecordB(b, s.coordinator, testSessionID, 1, []float32{1, 1})
	payload := []byte(validPayload(testSessionID, 1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewReader(payload))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			b.Fatalf("status = %d body=%q", rr.Code, rr.Body.String())
		}
	}
}

func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	return newTestServerB(t, opts)
}

func newTestServerB(tb testing.TB, opts Options) *Server {
	tb.Helper()
	if opts.Cache == nil {
		c := cache.NewMemoryClient(cache.Options{Shards: 1, IOTimeout: time.Millisecond})
		c.Set(cache.Feature{ID: 1, Vector: []float32{1, 0}, Category: "phone", Brand: "a", Available: true})
		c.Set(cache.Feature{ID: 2, Vector: []float32{0, 1}, Category: "case", Brand: "b", Available: true})
		c.Set(cache.Feature{ID: 3, Vector: []float32{2, 2}, Category: "watch", Brand: "c", Available: true})
		opts.Cache = c
	}
	if opts.Coordinator == nil {
		coord, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 2, QueueCapacity: 4, Alpha: 0.5, SlowTimeout: time.Millisecond})
		if err != nil {
			tb.Fatalf("NewCoordinator() error = %v", err)
		}
		tb.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = coord.Shutdown(ctx)
		})
		opts.Coordinator = coord
	}
	if opts.Scorer == nil {
		opts.Scorer = scorer.NewEngine(scorer.Options{TopK: 3, DiversityWindow: 2, MaxSameCategory: 2, MaxSameBrand: 2, MaxCandidates: 8})
	}
	if opts.Pool == nil {
		gp, err := pool.NewGoroutinePool(2, 8, 128)
		if err != nil {
			tb.Fatalf("NewGoroutinePool() error = %v", err)
		}
		tb.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = gp.Shutdown(ctx)
		})
		opts.Pool = gp
	}
	s, err := NewServer(opts)
	if err != nil {
		tb.Fatalf("NewServer() error = %v", err)
	}
	return s
}

func seedFastRecord(t *testing.T, coord *anti_drift.Coordinator, session string, version int64, vec []float32) {
	t.Helper()
	seedFastRecordB(t, coord, session, version, vec)
}

func seedFastRecordB(tb testing.TB, coord *anti_drift.Coordinator, session string, version int64, vec []float32) {
	tb.Helper()
	if err := coord.UpdateFast(context.Background(), anti_drift.FastTrackSnapshot{SessionID: session, LatestVersion: version, IntentVector: vec}); err != nil {
		tb.Fatalf("UpdateFast() error = %v", err)
	}
}

func validPayload(session string, version int64) string {
	return `{"session_id":"` + session + `","version_stamp":` + itoa64(version) + `,"slots":{"category":"phone","brand":"acme"}}`
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
