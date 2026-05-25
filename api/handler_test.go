package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

func TestServerCORSPreflightAllowsViteOrigin(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, Options{CandidateIDs: []int64{1}, VectorDim: 2, MaxInFlight: 4, Timeout: 50 * time.Millisecond})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/rerank", nil)
	req.Header.Set("Origin", "http://127.0.0.1:5173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d body=%q, want 204", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:5173" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodPost) {
		t.Fatalf("Access-Control-Allow-Methods = %q, want POST", got)
	}
}

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
	session := findSessionForExperiment(t, true)

	t.Run("reader vector changes sorting", func(t *testing.T) {
		t.Parallel()
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			dst[0] = 0
			dst[1] = 10
		}}
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
		seedFastRecord(t, s.coordinator, session, 1, []float32{10, 0})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[{"id":2`) {
			t.Fatalf("reader intent did not move candidate 2 first: %s", body)
		}
		if !strings.Contains(body, `"intent_hit":true`) {
			t.Fatalf("reader intent path did not expose intent_hit=true: %s", body)
		}
	})

	t.Run("blocked reader times out and silently degrades with HTTP 200", func(t *testing.T) {
		t.Parallel()
		reader := &stubIntentReader{block: 50 * time.Millisecond, fill: func(dst []float32) {
			dst[0] = 0
			dst[1] = 10
		}}
		s := newTestServer(t, Options{CandidateIDs: []int64{1, 2}, VectorDim: 2, MaxInFlight: 8, Timeout: 50 * time.Millisecond, IntentReader: reader, IntentReadTimeout: 2 * time.Millisecond})
		seedFastRecord(t, s.coordinator, session, 1, []float32{10, 0})

		start := time.Now()
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		elapsed := time.Since(start)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		if elapsed >= 30*time.Millisecond {
			t.Fatalf("elapsed = %s, want bounded degradation under 30ms", elapsed)
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[{"id":1`) {
			t.Fatalf("timeout path did not fall back to coordinator intent: %s", body)
		}
		if !strings.Contains(body, `"intent_hit":false`) {
			t.Fatalf("timeout path did not expose intent_hit=false: %s", body)
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
			seedFastRecord(t, s.coordinator, session, 1, []float32{10, 0})
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
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
		seedFastRecord(t, s.coordinator, session, 1, []float32{10, 0})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		if reader.deadlineErr.Load() != nil {
			t.Fatalf("deadline check failed: %v", reader.deadlineErr.Load())
		}
	})
}

func TestExperimentBucketStableAndBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		session string
	}{
		{name: "canonical session", session: testSessionID},
		{name: "zero session", session: "00000000-0000-4000-8000-000000000000"},
		{name: "max hex session", session: "ffffffff-ffff-4fff-8fff-ffffffffffff"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			first := experimentBucket(tt.session)
			for i := 0; i < 16; i++ {
				if got := experimentBucket(tt.session); got != first {
					t.Fatalf("bucket changed: first=%d got=%d", first, got)
				}
			}
			if first < 0 || first > 99 {
				t.Fatalf("bucket=%d, want 0..99", first)
			}
		})
	}
}

func TestServerABTrafficSplit(t *testing.T) {
	t.Parallel()

	baselineSession := findSessionForExperiment(t, false)
	experimentSession := findSessionForExperiment(t, true)

	tests := []struct {
		name              string
		session           string
		wantExpID         string
		wantReaderCalls   int64
		wantIntentCalls   int64
		wantBaselineCalls int64
	}{
		{
			name:              "baseline skips intent reader and uses baseline search",
			session:           baselineSession,
			wantExpID:         "baseline",
			wantReaderCalls:   0,
			wantIntentCalls:   0,
			wantBaselineCalls: 1,
		},
		{
			name:              "experiment reads intent and uses intent search",
			session:           experimentSession,
			wantExpID:         "neuro_rerank",
			wantReaderCalls:   1,
			wantIntentCalls:   1,
			wantBaselineCalls: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
				for i := range dst {
					dst[i] = 1
				}
			}}
			searcher := &mockProductSearcher{
				results: []ProductResult{
					{ItemID: "intent-db", Category: "phone", Embedding: []float32{1, 1}},
				},
				baselineResults: []ProductResult{
					{ItemID: "baseline-db", Category: "phone", Embedding: []float32{1, 1}},
				},
			}
			producer := &captureProducer{events: make(chan mq.Event, 1)}
			s := newTestServer(t, Options{
				CandidateIDs:      []int64{1, 2, 3},
				VectorDim:         2,
				MaxInFlight:       8,
				Timeout:           50 * time.Millisecond,
				IntentReader:      reader,
				IntentReadTimeout: 2 * time.Millisecond,
				ProductSearch:     searcher,
				BehaviorProducer:  producer,
			})
			seedFastRecord(t, s.coordinator, tt.session, 1, []float32{1, 1})

			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(tt.session, 1)))
			s.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
			}
			if got := reader.calls.Load(); got != tt.wantReaderCalls {
				t.Fatalf("intent reader calls=%d want=%d", got, tt.wantReaderCalls)
			}
			if got := searcher.intentCalls.Load(); got != tt.wantIntentCalls {
				t.Fatalf("intent search calls=%d want=%d", got, tt.wantIntentCalls)
			}
			if got := searcher.baselineCalls.Load(); got != tt.wantBaselineCalls {
				t.Fatalf("baseline search calls=%d want=%d", got, tt.wantBaselineCalls)
			}

			select {
			case ev := <-producer.events:
				if ev.SessionID != tt.session || ev.ExpID != tt.wantExpID {
					t.Fatalf("event got session=%q exp_id=%q want session=%q exp_id=%q", ev.SessionID, ev.ExpID, tt.session, tt.wantExpID)
				}
			case <-time.After(time.Second):
				t.Fatal("producer was not called")
			}
		})
	}

	t.Run("baseline does not fall back to vector ranking when baseline search is empty", func(t *testing.T) {
		t.Parallel()
		session := baselineSession
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			for i := range dst {
				dst[i] = 1
			}
		}}
		searcher := &mockProductSearcher{}
		s := newTestServer(t, Options{
			CandidateIDs:      []int64{1, 2, 3},
			VectorDim:         2,
			MaxInFlight:       8,
			Timeout:           50 * time.Millisecond,
			IntentReader:      reader,
			IntentReadTimeout: 2 * time.Millisecond,
			ProductSearch:     searcher,
		})
		seedFastRecord(t, s.coordinator, session, 1, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d body=%q, want 503", rr.Code, rr.Body.String())
		}
		if got := reader.calls.Load(); got != 0 {
			t.Fatalf("intent reader calls=%d want=0", got)
		}
		if got := searcher.intentCalls.Load(); got != 0 {
			t.Fatalf("intent search calls=%d want=0", got)
		}
		if got := searcher.baselineCalls.Load(); got != 1 {
			t.Fatalf("baseline search calls=%d want=1", got)
		}
	})

	t.Run("experiment reaches redis and duckdb before local cache fallback", func(t *testing.T) {
		t.Parallel()
		session := experimentSession
		reader := &stubIntentReader{version: 900, fill: func(dst []float32) {
			for i := range dst {
				dst[i] = 1
			}
		}}
		searcher := &mockProductSearcher{results: []ProductResult{{ItemID: "intent-db", Category: "phone", Embedding: []float32{1, 1}}}}
		s := newTestServer(t, Options{
			Cache:             cache.NewMemoryClient(cache.Options{Shards: 1, IOTimeout: time.Millisecond}),
			CandidateIDs:      []int64{999},
			VectorDim:         2,
			MaxInFlight:       8,
			Timeout:           50 * time.Millisecond,
			IntentReader:      reader,
			IntentReadTimeout: 2 * time.Millisecond,
			ProductSearch:     searcher,
		})
		seedFastRecord(t, s.coordinator, session, 1, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank", bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		if got := reader.calls.Load(); got != 1 {
			t.Fatalf("intent reader calls=%d want=1", got)
		}
		if got := searcher.intentCalls.Load(); got != 1 {
			t.Fatalf("intent search calls=%d want=1", got)
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
	calls         atomic.Int64
}

func (r *stubIntentReader) ReadIntent(ctx context.Context, sessionID string, dst []float32) (int64, error) {
	r.calls.Add(1)
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

type captureProducer struct {
	events chan mq.Event
}

func (p *captureProducer) Publish(ctx context.Context, ev mq.Event) error {
	select {
	case p.events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *captureProducer) Close() error { return nil }

func findSessionForExperiment(t *testing.T, experiment bool) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		session := fmt.Sprintf("00000000-0000-4000-8000-%012x", i)
		bucket := experimentBucket(session)
		if (bucket >= 50) == experiment {
			return session
		}
	}
	t.Fatalf("unable to find session for experiment=%v", experiment)
	return ""
}

type mockProductSearcher struct {
	results         []ProductResult
	baselineResults []ProductResult
	err             error
	baselineErr     error
	intentCalls     atomic.Int64
	baselineCalls   atomic.Int64
}

func (m *mockProductSearcher) SearchWithIntent(
	_ context.Context, _ []float32, _ string, _ int,
) ([]ProductResult, error) {
	m.intentCalls.Add(1)
	return m.results, m.err
}

func (m *mockProductSearcher) SearchBaseline(
	_ context.Context, _ string, _ int,
) ([]ProductResult, error) {
	m.baselineCalls.Add(1)
	return m.baselineResults, m.baselineErr
}

func TestServerProductSearchIntegration(t *testing.T) {
	session := findSessionForExperiment(t, true)
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
		seedFastRecord(t, s.coordinator, session, 1, []float32{1, 1})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank",
			bytes.NewBufferString(validPayload(session, 1)))
		s.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q, want 200", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"results":[`) {
			t.Fatal("response must contain results array")
		}
		if !strings.Contains(body, `"session_id":"`+session+`"`) {
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
		seedFastRecord(t, s.coordinator, session, 1, []float32{10, 0})

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/rerank",
			bytes.NewBufferString(validPayload(session, 1)))
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
