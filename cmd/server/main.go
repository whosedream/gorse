package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"go-rec/api"
	"go-rec/internal/rk/anti_drift"
	"go-rec/internal/rk/scorer"
	"go-rec/pkg/agent"
	"go-rec/pkg/cache"
	"go-rec/pkg/mq"
	"go-rec/pkg/pool"
)

const defaultRedisAddr = "127.0.0.1:6379"

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func main() {
	_ = godotenv.Load()
	_ = agent.RemoteEmbedderOptionsFromEnv()

	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxInFlight := flag.Int("max-inflight", 2048, "maximum in-flight requests")
	timeout := flag.Duration("timeout", 25*time.Millisecond, "request timeout")
	flag.Parse()

	cacheClient := cache.NewMemoryClient(cache.Options{Shards: 16, IOTimeout: 2 * time.Millisecond})
	for i := int64(1); i <= 128; i++ {
		cacheClient.Set(cache.Feature{ID: i, Vector: []float32{float32(i % 7), float32((i + 3) % 11)}, Category: "default", Brand: "default", Available: true})
	}
	coord, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 4, QueueCapacity: 256, Alpha: 0.5, SlowTimeout: 10 * time.Millisecond})
	if err != nil {
		log.Fatalf("coordinator: %v", err)
	}
	gp, err := pool.NewGoroutinePool(2, 8, 1024)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	var behaviorProducer mq.Producer
	if producer, err := mq.NewKafkaProducerFromEnv(); err == nil {
		behaviorProducer = producer
	} else {
		log.Printf("kafka producer disabled: %v", err)
	}
	var intentReader cache.IntentReader
	redisClient := redis.NewClient(&redis.Options{Addr: envDefault("REDIS_ADDR", defaultRedisAddr), Password: os.Getenv("REDIS_PASSWORD"), DB: envInt("REDIS_DB", 0)})
	if reader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: redisClient}); err == nil {
		intentReader = reader
	} else {
		log.Printf("redis intent reader disabled: %v", err)
		_ = redisClient.Close()
	}

	ids := make([]int64, 128)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	gateway, err := api.NewServer(api.Options{
		Timeout:          *timeout,
		MaxInFlight:      *maxInFlight,
		CandidateIDs:     ids,
		Cache:            cacheClient,
		Coordinator:      coord,
		Scorer:           scorer.NewEngine(scorer.Options{TopK: 20, DiversityWindow: 5, MaxSameCategory: 3, MaxSameBrand: 3, MaxCandidates: 256}),
		Pool:             gp,
		VectorDim:        2,
		BehaviorProducer: behaviorProducer,
		IntentReader:     intentReader,
		ProductSearch:    initDuckDB(),
	})
	if err != nil {
		log.Fatalf("api: %v", err)
	}

	srv := &http.Server{Addr: *addr, Handler: gateway.Handler(), ReadHeaderTimeout: time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()
	log.Printf("server listening on %s", *addr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if behaviorProducer != nil {
		if err := behaviorProducer.Close(); err != nil {
			log.Printf("kafka producer close: %v", err)
		}
	}
	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			log.Printf("redis client close: %v", err)
		}
	}
	if err := coord.Shutdown(ctx); err != nil {
		log.Printf("coordinator shutdown: %v", err)
	}
	if err := gp.Shutdown(ctx); err != nil {
		log.Printf("pool shutdown: %v", err)
	}
}
