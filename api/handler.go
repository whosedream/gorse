package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"

	"go-rec/internal/rk/anti_drift"
	"go-rec/internal/rk/scorer"
	"go-rec/pkg/agent"
	"go-rec/pkg/cache"
	"go-rec/pkg/catalog"
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
	catalog           *catalog.Catalog
	rateLimiter       *rate.Limiter
	cachedRouter      *gin.Engine
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
	Catalog           *catalog.Catalog
	RateLimit         float64 // requests per second; 0 = unlimited
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

	// Default rate limit: 20000 QPS as per spec
	rateLimit := opts.RateLimit
	if rateLimit <= 0 {
		rateLimit = 20000
	}

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
		catalog:           opts.Catalog,
		rateLimiter:       rate.NewLimiter(rate.Limit(rateLimit), int(rateLimit)),
	}
	if s.intentReadTimeout <= 0 {
		s.intentReadTimeout = 2 * time.Millisecond
	}
	s.reqPool = pool.NewMemoryPool(resetRequestState)
	return s, nil
}

// Handler returns an http.Handler wrapping the Gin engine. Useful for
// embedding in http.Server or test adapters.
func (s *Server) Handler() http.Handler { return s.SetupRouter() }

// ServeHTTP implements http.Handler so existing test code that calls
// s.ServeHTTP(rr, req) continues to work without modification.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router().ServeHTTP(w, r)
}

func (s *Server) router() *gin.Engine {
	if s.cachedRouter == nil {
		s.cachedRouter = s.SetupRouter()
	}
	return s.cachedRouter
}

// SetupRouter creates and configures a Gin engine with all routes and middleware.
func (s *Server) SetupRouter() *gin.Engine {
	r := gin.New()

	// Global middleware
	r.Use(gin.Recovery())
	r.Use(requestLogger())
	r.Use(corsMiddleware())
	r.Use(tokenBucketLimiter(s.rateLimiter))
	r.Use(timeoutMiddleware(s.timeout))

	// Routes
	r.POST("/rerank", s.handleRerank)
	r.GET("/stream", s.handleStreamGin)
	r.GET("/products/meta", s.handleProductMetaGin)
	r.GET("/products/ids", s.handleProductIDsGin)

	return r
}

// corsMiddleware handles CORS for the Vite dev server.
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Cache-Control, Accept, Last-Event-ID")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// tokenBucketLimiter enforces a token-bucket rate limit.
func tokenBucketLimiter(lim *rate.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !lim.Allow() {
			c.AbortWithStatus(http.StatusTooManyRequests)
			return
		}
		c.Next()
	}
}

// timeoutMiddleware enforces a per-request deadline via context.
func timeoutMiddleware(d time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// requestLogger logs method, path, status, and latency.
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		status := c.Writer.Status()
		_ = status // structured logging can be added here
		_ = latency
	}
}

// handleRerank handles POST /rerank requests using the FSM parser.
func (s *Server) handleRerank(c *gin.Context) {
	// Backpressure limiter (non-blocking)
	select {
	case s.limiter <- struct{}{}:
		defer func() { <-s.limiter }()
	default:
		c.String(http.StatusTooManyRequests, "overloaded")
		return
	}

	ctx := c.Request.Context()
	if ctx.Err() != nil {
		c.String(http.StatusGatewayTimeout, "timeout")
		return
	}

	err := s.reqPool.With(ctx, func(st *requestState) error {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, fsm.MaxInputSize+1))
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

		// Extract fields that the FSM parser does not handle.
		var extra struct {
			ItemID string `json:"item_id"`
			UserID string `json:"user_id"`
		}
		_ = json.Unmarshal(st.body, &extra)

		s.publishBehavior(st.req, expID, extra.ItemID, extra.UserID)

		if expID == experimentBaseline {
			ensureBaselineCapacity(st, 20)
			if bs, ok := s.productSearch.(baselineProductSearcher); ok {
				category := st.req.CategoryString()
				dbResults, dbErr := bs.SearchBaseline(ctx, category, 20)
				if dbErr == nil && len(dbResults) > 0 {
					results := st.results[:len(dbResults)]
					n := productResultsToResults(dbResults, results)
					writeGinResponse(c, sessionID, st.req.Fallback, false, results[:n])
					return nil
				}
			}
			if s.catalog != nil {
				return s.handleCatalogRerankGin(ctx, c, st)
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
						writeGinResponse(c, sessionID, false, true, results[:n])
						return nil
					}
				}
				return s.handleCacheFallbackGin(ctx, c, st, sessionID, readErr == nil, intent)
			})
		}
		return s.handleCacheFallbackGin(ctx, c, st, sessionID, false, nil)
	})

	if err != nil {
		mapErrorToGin(c, err)
	}
}

func (s *Server) handleCatalogRerankGin(_ context.Context, c *gin.Context, st *requestState) error {
	all := s.catalog.IDs()
	catalogItems := s.catalog.Get(all)

	limit := 20
	if limit > len(catalogItems) {
		limit = len(catalogItems)
	}

	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write([]byte(`{"session_id":"`))
	_, _ = c.Writer.Write([]byte(st.req.SessionIDString()))
	_, _ = c.Writer.Write([]byte(`","fallback":true,"intent_hit":false,"results":[`))
	for i := 0; i < limit; i++ {
		if i > 0 {
			_, _ = c.Writer.Write([]byte{','})
		}
		_, _ = c.Writer.Write([]byte(`{"id":"`))
		_, _ = c.Writer.Write([]byte(catalogItems[i].ID))
		_, _ = c.Writer.Write([]byte(`","score":`))
		score := 20.0 - float64(i)
		_, _ = c.Writer.Write([]byte(strconv.FormatFloat(score, 'f', -1, 32)))
		_, _ = c.Writer.Write([]byte("}"))
	}
	_, _ = c.Writer.Write([]byte("]}"))
	return nil
}

func (s *Server) handleCacheFallbackGin(ctx context.Context, c *gin.Context, st *requestState, sessionID string, useIntent bool, intent []float32) error {
	if s.productSearch == nil && s.catalog != nil {
		return s.handleCatalogRerankGin(ctx, c, st)
	}
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
	writeGinResponse(c, sessionID, rec.Fallback || st.req.Fallback, useIntent, results[:n])
	return nil
}

// handleStreamGin handles GET /stream for SSE via Redis Pub/Sub.
func (s *Server) handleStreamGin(c *gin.Context) {
	if s == nil || s.streamSubscriber == nil {
		c.String(http.StatusServiceUnavailable, "stream unavailable")
		return
	}
	sessionID := c.Query("session_id")
	if sessionID == "" {
		c.String(http.StatusBadRequest, "missing session_id")
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.String(http.StatusInternalServerError, "stream unsupported")
		return
	}

	header := c.Writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	// Ensure CORS headers for SSE
	origin := c.GetHeader("Origin")
	if origin != "" {
		header.Set("Access-Control-Allow-Origin", origin)
	}

	ctx := c.Request.Context()
	pubsub := s.streamSubscriber.Subscribe(ctx, agent.SSELogChannelPrefix+sessionID)
	defer pubsub.Close()

	// Flush headers immediately so EventSource fires onopen.
	_, _ = fmt.Fprintf(c.Writer, ":ok\n\n")
	flusher.Flush()

	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", sanitizeSSEData(msg.Payload))
			flusher.Flush()
		}
	}
}

// handleProductMetaGin handles GET /products/meta.
func (s *Server) handleProductMetaGin(c *gin.Context) {
	if s.catalog == nil {
		c.String(http.StatusServiceUnavailable, "catalog unavailable")
		return
	}
	raw := c.Query("ids")
	if raw == "" {
		c.String(http.StatusBadRequest, "missing ids parameter")
		return
	}

	parts := strings.Split(raw, ",")
	if len(parts) > 50 {
		c.String(http.StatusBadRequest, "too many ids (max 50)")
		return
	}

	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		ids = append(ids, p)
	}
	if len(ids) == 0 {
		c.String(http.StatusBadRequest, "empty ids")
		return
	}

	items := s.catalog.Get(ids)

	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)

	if len(items) == 0 {
		_, _ = c.Writer.Write([]byte("[]"))
		return
	}

	_, _ = c.Writer.Write([]byte(`[`))
	for i := range items {
		if i > 0 {
			_, _ = c.Writer.Write([]byte(`,`))
		}
		_, _ = c.Writer.Write([]byte(`{"id":"`))
		_, _ = c.Writer.Write([]byte(items[i].ID))
		_, _ = c.Writer.Write([]byte(`","title":"`))
		_, _ = c.Writer.Write([]byte(escapeProductJSON(items[i].Title)))
		_, _ = c.Writer.Write([]byte(`","price":`))
		_, _ = c.Writer.Write([]byte(strconv.FormatFloat(items[i].Price, 'f', -1, 64)))
		_, _ = c.Writer.Write([]byte(`,"image_url":"`))
		_, _ = c.Writer.Write([]byte(items[i].ImageURL))
		_, _ = c.Writer.Write([]byte(`","category":"`))
		_, _ = c.Writer.Write([]byte(escapeProductJSON(items[i].Category)))
		_, _ = c.Writer.Write([]byte(`"}`))
	}
	_, _ = c.Writer.Write([]byte(`]`))
}

// handleProductIDsGin handles GET /products/ids.
func (s *Server) handleProductIDsGin(c *gin.Context) {
	if s.catalog == nil {
		c.String(http.StatusServiceUnavailable, "catalog unavailable")
		return
	}
	ids := s.catalog.IDs()
	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	if len(ids) == 0 {
		_, _ = c.Writer.Write([]byte("[]"))
		return
	}
	_, _ = c.Writer.Write([]byte(`["`))
	_, _ = c.Writer.Write([]byte(ids[0]))
	_, _ = c.Writer.Write([]byte(`"`))
	for _, id := range ids[1:] {
		_, _ = c.Writer.Write([]byte(`,"`))
		_, _ = c.Writer.Write([]byte(id))
		_, _ = c.Writer.Write([]byte(`"`))
	}
	_, _ = c.Writer.Write([]byte("]"))
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

// escapeProductJSON escapes a string for safe insertion in a JSON string value.
func escapeProductJSON(s string) string {
	if s == "" {
		return ""
	}
	if !containsSpecialJSON(s) {
		return s
	}
	b := make([]byte, 0, len(s)+4)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			b = append(b, '\\', '\\')
		case '"':
			b = append(b, '\\', '"')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			if c < 0x20 {
				b = append(b, '\\', 'u', '0', '0', "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xf])
			} else {
				b = append(b, c)
			}
		}
	}
	return string(b)
}

func containsSpecialJSON(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' || c == '"' || c < 0x20 {
			return true
		}
	}
	return false
}

func (s *Server) publishBehavior(req fsm.RerankRequest, expID, itemID, userID string) {
	if s.producer == nil {
		return
	}
	if itemID == "" {
		itemID = "unknown"
	}
	if userID == "" {
		userID = req.SessionIDString()
	}
	ev := mq.Event{
		SessionID:  req.SessionIDString(),
		UserID:     userID,
		ItemID:     itemID,
		Timestamp:  req.VersionStamp,
		Action:     "rerank",
		ExpID:      expID,
		CategoryID: req.CategoryString(),
	}
	// Raw goroutine, not the bounded pool — this publish MUST NOT be gated
	// by pool capacity or the slow-track agent will miss behavior events.
	go func() {
		_ = s.producer.Publish(context.Background(), ev)
	}()
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

func hashStringToInt64(s string) int64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h & 0x7FFFFFFFFFFFFFFF)
}

func writeGinResponse(c *gin.Context, sessionID string, fallback bool, intentHit bool, results []scorer.Result) {
	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, _ = c.Writer.Write([]byte(`{"session_id":"`))
	_, _ = c.Writer.Write([]byte(sessionID))
	_, _ = c.Writer.Write([]byte(`","fallback":`))
	if fallback {
		_, _ = c.Writer.Write([]byte("true"))
	} else {
		_, _ = c.Writer.Write([]byte("false"))
	}
	_, _ = c.Writer.Write([]byte(`,"intent_hit":`))
	if intentHit {
		_, _ = c.Writer.Write([]byte("true"))
	} else {
		_, _ = c.Writer.Write([]byte("false"))
	}
	_, _ = c.Writer.Write([]byte(`,"results":[`))
	for i := range results {
		if i > 0 {
			_, _ = c.Writer.Write([]byte{','})
		}
		_, _ = c.Writer.Write([]byte(`{"id":`))
		_, _ = c.Writer.Write([]byte(strconv.FormatInt(results[i].ID, 10)))
		_, _ = c.Writer.Write([]byte(`,"score":`))
		_, _ = c.Writer.Write([]byte(strconv.FormatFloat(float64(results[i].Score), 'f', -1, 32)))
		_, _ = c.Writer.Write([]byte("}"))
	}
	_, _ = c.Writer.Write([]byte("]}"))
}

func mapErrorToGin(c *gin.Context, err error) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, cache.ErrPartialTimeout) {
		c.String(http.StatusGatewayTimeout, "timeout")
		return
	}
	if errors.Is(err, fsm.ErrMalformed) || errors.Is(err, fsm.ErrInputTooLarge) || errors.Is(err, fsm.ErrValueTooLarge) {
		c.String(http.StatusBadRequest, "bad request")
		return
	}
	if errors.Is(err, ErrNoCandidates) || errors.Is(err, cache.ErrMiss) {
		c.String(http.StatusServiceUnavailable, "no candidates")
		return
	}
	c.String(http.StatusInternalServerError, "internal error")
}
