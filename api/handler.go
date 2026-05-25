package api

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"go-rec/internal/rk/anti_drift"
	"go-rec/internal/rk/scorer"
	"go-rec/pkg/agent"
	"go-rec/pkg/cache"
	"go-rec/pkg/fsm"
	"go-rec/pkg/mq"
	"go-rec/pkg/pool"
)

var (
	ErrInvalidOptions = errors.New("api: invalid options")
	ErrNoCandidates   = errors.New("api: no available candidates")
)

const (
	experimentBaseline    = "baseline"
	experimentNeuroRerank = "neuro_rerank"
)

var fnv32aOffset = fnv.New32a().Sum32()

// ProductSearcher abstracts a DuckDB-like vector search backend.  nil means
// the handler falls back to cache-based scoring.
type ProductSearcher interface {
	SearchWithIntent(ctx context.Context, vector []float32, category string, limit int) ([]ProductResult, error)
}

type baselineProductSearcher interface {
	SearchBaseline(ctx context.Context, category string, limit int) ([]ProductResult, error)
}

// ProductResult is a single row returned by ProductSearcher.
type ProductResult struct {
	ItemID    string
	Title     string
	Category  string
	Price     float64
	ImageURL  string
	Score     float64
	Embedding []float32
}

type streamPubSub interface {
	Channel() <-chan *redis.Message
	Close() error
}

type streamSubscriber interface {
	Subscribe(ctx context.Context, channels ...string) streamPubSub
}

type redisStreamSubscriber struct {
	client redis.UniversalClient
}

func (s redisStreamSubscriber) Subscribe(ctx context.Context, channels ...string) streamPubSub {
	return redisStreamPubSub{pubsub: s.client.Subscribe(ctx, channels...)}
}

type redisStreamPubSub struct {
	pubsub *redis.PubSub
}

func (p redisStreamPubSub) Channel() <-chan *redis.Message { return p.pubsub.Channel() }

func (p redisStreamPubSub) Close() error { return p.pubsub.Close() }

type Server struct {
	parser            *fsm.Parser
	cache             *cache.MemoryClient
	coordinator       *anti_drift.Coordinator
	scorer            *scorer.Engine
	pool              *pool.GoroutinePool
	reqPool           *pool.MemoryPool[requestState]
	limiter           chan struct{}
	timeout           time.Duration
	candidateIDs      []int64
	vectorDim         int
	producer          mq.Producer
	intentReader      cache.IntentReader
	intentReadTimeout time.Duration
	intentPool        *pool.IntentVectorPool
	productSearch     ProductSearcher
	streamSubscriber  streamSubscriber
}

type Options struct {
	Timeout           time.Duration
	MaxInFlight       int
	CandidateIDs      []int64
	Cache             *cache.MemoryClient
	Coordinator       *anti_drift.Coordinator
	Scorer            *scorer.Engine
	Pool              *pool.GoroutinePool
	VectorDim         int
	BehaviorProducer  mq.Producer
	IntentReader      cache.IntentReader
	IntentReadTimeout time.Duration
	ProductSearch     ProductSearcher
	StreamSubscriber  streamSubscriber
	RedisClient       redis.UniversalClient
}

type requestState struct {
	req        fsm.RerankRequest
	body       []byte
	features   []cache.Feature
	candidates []scorer.Candidate
	results    []scorer.Result
	vectorBuf  []float32
	intent     []float32
}

func NewServer(opts Options) (*Server, error) {
	if opts.Cache == nil || opts.Coordinator == nil || opts.Scorer == nil || opts.Pool == nil {
		return nil, ErrInvalidOptions
	}
	if opts.MaxInFlight < 0 || opts.VectorDim < 0 {
		return nil, ErrInvalidOptions
	}
	if opts.MaxInFlight == 0 {
		opts.MaxInFlight = 1024
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 25 * time.Millisecond
	}
	if opts.VectorDim == 0 {
		opts.VectorDim = 2
	}
	streamSub := opts.StreamSubscriber
	if streamSub == nil && opts.RedisClient != nil {
		streamSub = redisStreamSubscriber{client: opts.RedisClient}
	}
	ids := opts.CandidateIDs
	if len(ids) == 0 {
		ids = []int64{1, 2, 3}
	}
	candidateIDs := make([]int64, len(ids))
	copy(candidateIDs, ids)
	s := &Server{
		parser:            fsm.NewParser(),
		cache:             opts.Cache,
		coordinator:       opts.Coordinator,
		scorer:            opts.Scorer,
		pool:              opts.Pool,
		limiter:           make(chan struct{}, opts.MaxInFlight),
		timeout:           opts.Timeout,
		candidateIDs:      candidateIDs,
		vectorDim:         opts.VectorDim,
		producer:          opts.BehaviorProducer,
		intentReader:      opts.IntentReader,
		intentReadTimeout: opts.IntentReadTimeout,
		intentPool:        pool.NewIntentVectorPool(),
		productSearch:     opts.ProductSearch,
		streamSubscriber:  streamSub,
	}
	if s.intentReadTimeout <= 0 {
		s.intentReadTimeout = 2 * time.Millisecond
	}
	s.reqPool = pool.NewMemoryPool(resetRequestState)
	return s, nil
}

func (s *Server) Handler() http.Handler { return s }

func applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "http://127.0.0.1:5173" || origin == "http://localhost:5173" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	applyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/stream" {
		s.handleStream(w, r)
		return
	}
	if s == nil || s.parser == nil || s.cache == nil || s.coordinator == nil || s.scorer == nil || s.pool == nil || s.reqPool == nil {
		http.Error(w, "server unavailable", http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	select {
	case s.limiter <- struct{}{}:
		defer func() { <-s.limiter }()
	default:
		http.Error(w, "overloaded", http.StatusTooManyRequests)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	err := s.reqPool.With(ctx, func(st *requestState) error {
		return s.handle(ctx, w, r, st)
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, cache.ErrPartialTimeout) {
			http.Error(w, "timeout", http.StatusGatewayTimeout)
			return
		}
		if errors.Is(err, fsm.ErrMalformed) || errors.Is(err, fsm.ErrInputTooLarge) || errors.Is(err, fsm.ErrValueTooLarge) {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if errors.Is(err, ErrNoCandidates) || errors.Is(err, cache.ErrMiss) {
			http.Error(w, "no candidates", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.streamSubscriber == nil {
		http.Error(w, "stream unavailable", http.StatusServiceUnavailable)
		return
	}
	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	pubsub := s.streamSubscriber.Subscribe(r.Context(), agent.SSELogChannelPrefix+sessionID)
	defer pubsub.Close()
	messages := pubsub.Channel()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", sanitizeSSEData(msg.Payload))
			flusher.Flush()
		}
	}
}

func sanitizeSSEData(in string) string {
	if in == "" {
		return ""
	}
	return strings.Map(func(r rune) rune {
		switch r {
		case '\r', '\n':
			return ' '
		default:
			return r
		}
	}, in)
}

func (s *Server) handle(ctx context.Context, w http.ResponseWriter, r *http.Request, st *requestState) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, fsm.MaxInputSize+1))
	if err != nil {
		return err
	}
	if len(body) > fsm.MaxInputSize {
		return fsm.ErrInputTooLarge
	}
	st.body = append(st.body[:0], body...)
	if err := s.parser.Parse(ctx, st.body, &st.req); err != nil {
		return err
	}
	sessionID := st.req.SessionIDString()
	expID := experimentID(sessionID)
	s.publishBehavior(st.req, expID)

	if expID == experimentBaseline {
		ensureBaselineCapacity(st, 20)
		if bs, ok := s.productSearch.(baselineProductSearcher); ok {
			category := st.req.CategoryString()
			dbResults, dbErr := bs.SearchBaseline(ctx, category, 20)
			if dbErr == nil && len(dbResults) > 0 {
				results := st.results[:len(dbResults)]
				n := productResultsToResults(dbResults, results)
				writeResponse(w, sessionID, st.req.Fallback, false, results[:n])
				return nil
			}
		}
		return ErrNoCandidates
	}

	ids := s.candidateIDs
	ensureRequestCapacity(st, len(ids), s.vectorDim)
	ensureDuckDBCapacity(st, 20)
	if expID == experimentNeuroRerank && s.intentReader != nil {
		return s.intentPool.With(ctx, func(intent []float32) error {
			readCtx, cancel := context.WithTimeout(ctx, s.intentReadTimeout)
			_, readErr := s.intentReader.ReadIntent(readCtx, sessionID, intent)
			cancel()
			if readErr == nil && s.productSearch != nil {
				category := st.req.CategoryString()
				dbResults, dbErr := s.productSearch.SearchWithIntent(ctx, intent, category, 20)
				if dbErr == nil && len(dbResults) > 0 {
					dbCandidates := st.candidates[:len(dbResults)]
					productResultsToCandidates(dbResults, dbCandidates)
					results := st.results[:len(dbCandidates)]
					n, err := s.scorer.Rank(ctx, intent, dbCandidates, results)
					if err != nil {
						return err
					}
					fallback := st.req.Fallback
					if rec, ok := s.coordinator.Get(sessionID); ok {
						fallback = fallback || rec.Fallback
					}
					writeResponse(w, sessionID, fallback, true, results[:n])
					return nil
				}
			}
			return s.handleCacheFallback(ctx, w, st, sessionID, readErr == nil, intent)
		})
	}
	return s.handleCacheFallback(ctx, w, st, sessionID, false, nil)
}

func (s *Server) handleCacheFallback(ctx context.Context, w http.ResponseWriter, st *requestState, sessionID string, useIntent bool, intent []float32) error {
	ids := s.candidateIDs
	features := st.features[:len(ids)]
	vectorBuf := st.vectorBuf[:len(ids)*s.vectorDim]
	if err := s.cache.MGetInto(ctx, ids, features, vectorBuf, s.vectorDim); err != nil {
		return err
	}

	rec, ok := s.coordinator.Get(sessionID)
	if !ok || len(rec.IntentVector) == 0 {
		defaultIntent := st.intent[:s.vectorDim]
		fillDefaultIntent(defaultIntent, st.req.CategoryString(), st.req.BrandString())
		if err := s.coordinator.UpdateFast(ctx, anti_drift.FastTrackSnapshot{SessionID: sessionID, LatestVersion: st.req.VersionStamp, IntentVector: defaultIntent}); err != nil {
			return err
		}
		rec, _ = s.coordinator.Get(sessionID)
	}
	if len(rec.IntentVector) == 0 {
		return ErrNoCandidates
	}

	candidates := st.candidates[:0]
	for i := 0; i < len(features); i++ {
		f := features[i]
		if !f.Available || len(f.Vector) == 0 {
			continue
		}
		candidates = append(candidates, scorer.Candidate{ID: f.ID, Category: f.Category, Brand: f.Brand, Feature: f.Vector})
	}
	st.candidates = candidates
	if len(candidates) == 0 {
		return ErrNoCandidates
	}

	results := st.results[:len(candidates)]
	var n int
	var err error
	if useIntent {
		n, err = s.scorer.Rank(ctx, intent, candidates, results)
	} else {
		n, err = s.scorer.RankParallel(ctx, s.pool, rec.IntentVector, candidates, results)
	}
	if err != nil {
		return err
	}
	writeResponse(w, sessionID, rec.Fallback || st.req.Fallback, useIntent, results[:n])
	return nil
}

func (s *Server) publishBehavior(req fsm.RerankRequest, expID string) {
	if s.producer == nil {
		return
	}
	ev := mq.Event{SessionID: req.SessionIDString(), Timestamp: req.VersionStamp, Action: "rerank", ExpID: expID}
	_ = s.pool.Submit(context.Background(), func(ctx context.Context) error {
		return s.producer.Publish(ctx, ev)
	})
}

func experimentID(sessionID string) string {
	if experimentBucket(sessionID) < 50 {
		return experimentBaseline
	}
	return experimentNeuroRerank
}

func experimentBucket(sessionID string) int {
	h := fnv32aOffset
	for i := 0; i < len(sessionID); i++ {
		h ^= uint32(sessionID[i])
		h *= 16777619
	}
	return int(h % 100)
}

func ensureBaselineCapacity(st *requestState, n int) {
	if cap(st.results) < n {
		st.results = make([]scorer.Result, n)
	}
}

func ensureDuckDBCapacity(st *requestState, n int) {
	ensureBaselineCapacity(st, n)
	if cap(st.candidates) < n {
		st.candidates = make([]scorer.Candidate, 0, n)
	}
}

func ensureRequestCapacity(st *requestState, n int, dim int) {
	if cap(st.features) < n {
		st.features = make([]cache.Feature, n)
	}
	st.features = st.features[:n]
	if cap(st.candidates) < n {
		st.candidates = make([]scorer.Candidate, 0, n)
	}
	st.candidates = st.candidates[:0]
	if cap(st.results) < n {
		st.results = make([]scorer.Result, n)
	}
	st.results = st.results[:n]
	need := n * dim
	if cap(st.vectorBuf) < need {
		st.vectorBuf = make([]float32, need)
	}
	st.vectorBuf = st.vectorBuf[:need]
	if cap(st.intent) < dim {
		st.intent = make([]float32, dim)
	}
	st.intent = st.intent[:dim]
}

func resetRequestState(st *requestState) {
	st.req.Reset()
	for i := range st.body {
		st.body[i] = 0
	}
	st.body = st.body[:0]
	for i := range st.features {
		st.features[i] = cache.Feature{}
	}
	st.features = st.features[:0]
	for i := range st.candidates {
		st.candidates[i] = scorer.Candidate{}
	}
	st.candidates = st.candidates[:0]
	for i := range st.results {
		st.results[i] = scorer.Result{}
	}
	st.results = st.results[:0]
	for i := range st.vectorBuf {
		st.vectorBuf[i] = 0
	}
	st.vectorBuf = st.vectorBuf[:0]
	for i := range st.intent {
		st.intent[i] = 0
	}
	st.intent = st.intent[:0]
}

func fillDefaultIntent(out []float32, category, brand string) {
	var a uint32 = 2166136261
	for i := 0; i < len(category); i++ {
		a ^= uint32(category[i])
		a *= 16777619
	}
	var b uint32 = 2166136261
	for i := 0; i < len(brand); i++ {
		b ^= uint32(brand[i])
		b *= 16777619
	}
	for i := range out {
		v := a + uint32(i+1)*b
		out[i] = float32((v%1000)+1) / 1000
	}
}

func productResultsToResults(products []ProductResult, out []scorer.Result) int {
	n := len(products)
	if n > len(out) {
		n = len(out)
	}
	for i := 0; i < n; i++ {
		out[i] = scorer.Result{
			ID:       hashStringToInt64(products[i].ItemID),
			Score:    float32(products[i].Score),
			Category: products[i].Category,
		}
	}
	return n
}

// productResultsToCandidates converts search results to scorer.Candidate
// values. Each result's embedding becomes the Feature for dot-product scoring.
// The ItemID is hashed to produce an int64 ID for the scorer pipeline.
func productResultsToCandidates(results []ProductResult, candidates []scorer.Candidate) {
	for i, r := range results {
		id := hashStringToInt64(r.ItemID)
		candidates[i] = scorer.Candidate{
			ID:       id,
			Category: r.Category,
			Brand:    "",
			Feature:  r.Embedding,
		}
	}
}

// hashStringToInt64 produces a positive int64 from a string using FNV-1a.
func hashStringToInt64(s string) int64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h & 0x7FFFFFFFFFFFFFFF)
}

func writeResponse(w http.ResponseWriter, sessionID string, fallback bool, intentHit bool, results []scorer.Result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"session_id":"`))
	_, _ = w.Write([]byte(sessionID))
	_, _ = w.Write([]byte(`","fallback":`))
	if fallback {
		_, _ = w.Write([]byte("true"))
	} else {
		_, _ = w.Write([]byte("false"))
	}
	_, _ = w.Write([]byte(`,"intent_hit":`))
	if intentHit {
		_, _ = w.Write([]byte("true"))
	} else {
		_, _ = w.Write([]byte("false"))
	}
	_, _ = w.Write([]byte(`,"results":[`))
	for i := range results {
		if i > 0 {
			_, _ = w.Write([]byte{','})
		}
		_, _ = w.Write([]byte(`{"id":`))
		_, _ = w.Write([]byte(strconv.FormatInt(results[i].ID, 10)))
		_, _ = w.Write([]byte(`,"score":`))
		_, _ = w.Write([]byte(strconv.FormatFloat(float64(results[i].Score), 'f', -1, 32)))
		_, _ = w.Write([]byte("}"))
	}
	_, _ = w.Write([]byte("]}"))
}
