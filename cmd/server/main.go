package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"go-rec/api"
	"go-rec/internal/rk/anti_drift"
	"go-rec/internal/rk/scorer"
	"go-rec/pkg/agent"
	"go-rec/pkg/cache"
	"go-rec/pkg/catalog"
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

	// Gin release mode for production
	gin.SetMode(gin.ReleaseMode)

	cacheClient := cache.NewMemoryClient(cache.Options{Shards: 16, IOTimeout: 2 * time.Millisecond})
	for i := int64(1); i <= 128; i++ {
		cacheClient.Set(cache.Feature{ID: i, Vector: []float32{float32(i % 7), float32((i + 3) % 11)}, Category: "default", Brand: "default", Available: true})
	}
	coord, err := anti_drift.NewCoordinator(anti_drift.Options{MinWorkers: 1, MaxWorkers: 4, QueueCapacity: 256, Alpha: 0.5, SlowTimeout: 10 * time.Millisecond})
	if err != nil {
		log.Fatalf("coordinator: %v", err)
	}

	// Fixed 200-worker pool with 4096 queue capacity
	gp, err := pool.NewFixedPool(200, 4096)
	if err != nil {
		log.Fatalf("pool: %v", err)
	}

	// Behavior event transport: fanout to Kafka + Redis.
	// Redis is always active; Kafka is wired when configured.
	var kafkaProducer mq.Producer
	if producer, err := mq.NewKafkaProducerFromEnv(); err == nil {
		kafkaProducer = producer
		log.Printf("kafka producer ready")
	} else {
		log.Printf("kafka producer skipped: %v", err)
	}

	var intentReader cache.IntentReader
	redisClient := redis.NewClient(&redis.Options{Addr: envDefault("REDIS_ADDR", defaultRedisAddr), Password: os.Getenv("REDIS_PASSWORD"), DB: envInt("REDIS_DB", 0)})
	if reader, err := cache.NewRedisIntentReader(cache.RedisIntentReaderOptions{Client: redisClient}); err == nil {
		intentReader = reader
	} else {
		log.Printf("redis intent reader disabled (non-critical): %v", err)
	}

	redisProducer := mq.NewRedisProducer(redisClient, "behavior:events")
	behaviorProducer := mq.NewFanoutProducer(kafkaProducer, redisProducer)
	log.Printf("behavior events: kafka=%v redis=behavior:events (fanout)", kafkaProducer != nil)

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
		RedisClient:      redisClient,
		Catalog:          catalog.New(""),
	})
	if err != nil {
		log.Fatalf("api: %v", err)
	}

	// Build Gin engine with middleware and routes
	r := gateway.SetupRouter()

	srv := &gin.Engine{}
	_ = srv // unused, we use r directly
	go func() {
		if err := r.Run(*addr); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}()
	log.Printf("server listening on %s", *addr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down...")
	if behaviorProducer != nil {
		if err := behaviorProducer.Close(); err != nil {
			log.Printf("behavior producer close: %v", err)
		}
	}
	if redisClient != nil {
		if err := redisClient.Close(); err != nil {
			log.Printf("redis client close: %v", err)
		}
	}
	if err := coord.Shutdown(context.Background()); err != nil {
		log.Printf("coordinator shutdown: %v", err)
	}
	if err := gp.Shutdown(context.Background()); err != nil {
		log.Printf("pool shutdown: %v", err)
	}
}
