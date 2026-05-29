package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"go-rec/internal/slow_track"
	"go-rec/pkg/agent"
	"go-rec/pkg/bloom"
	"go-rec/pkg/cache"
	"go-rec/pkg/mq"
)

type LLMRequest = slow_track.Request

type LLM interface {
	Complete(context.Context, LLMRequest) (string, error)
}

type batchConsumer interface {
	Consume(context.Context, chan<- mq.Batch) error
	Close() error
}

type DaemonDeps struct {
	Consumer     batchConsumer
	LLM          LLM
	Embedder     agent.Embedder
	Writer       cache.IntentWriter
	LogPublisher agent.LogPublisher
	Cooling      cache.CoolingChecker
	ProfileCache *cache.ProfileCacheReader
	BatchBuffer  int
	Logger       interface{ Printf(string, ...any) }
}

type Daemon struct {
	deps DaemonDeps
}

type slowTrackLLMAdapter struct {
	client interface {
		Complete(context.Context, slow_track.Request) (slow_track.Response, error)
	}
}

func (a slowTrackLLMAdapter) Complete(ctx context.Context, req LLMRequest) (string, error) {
	resp, err := a.client.Complete(ctx, slow_track.Request(req))
	if err != nil {
		return "", err
	}
	// Prefer the content field — DeepSeek v4-pro places the final JSON
	// output there. Fall back to reasoning only when content is empty.
	if resp.Text != "" {
		return resp.Text, nil
	}
	return resp.Reasoning, nil
}

type graphLLMAdapter struct {
	llm LLM
}

func (a graphLLMAdapter) Complete(ctx context.Context, prompt string) (string, error) {
	return a.llm.Complete(ctx, LLMRequest{UserPrompt: prompt, EnableCoT: true})
}

func BuildGraph(llm LLM, embedder agent.Embedder, writer cache.IntentWriter, publisher agent.LogPublisher) (*agent.Graph, error) {
	return agent.NewGraph(
		agent.NewNeuralIntentNode(agent.NeuralNodeOptions{ID: "neural_intent", Client: graphLLMAdapter{llm: llm}, PromptBuilder: agent.DefaultPromptBuilder(), LogPublisher: publisher}),
		agent.NewEmbeddingNode(agent.EmbeddingNodeOptions{ID: "embedding", Deps: []string{"neural_intent"}, Embedder: embedder, LogPublisher: publisher}),
		agent.NewStateSyncNode(agent.StateSyncNodeOptions{ID: "state_sync", Deps: []string{"embedding"}, Writer: writer, LogPublisher: publisher}),
	)
}

func RunBatch(ctx context.Context, batch mq.Batch, llm LLM, embedder agent.Embedder, writer cache.IntentWriter, publisher agent.LogPublisher, cooling cache.CoolingChecker, profileCache *cache.ProfileCacheReader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	sessionID := batch.SessionID
	if sessionID == "" {
		sessionID = batch.UserID
	}

	// Check cooling window: if session is in cooling period, skip LLM and use cached profile.
	if cooling != nil {
		cooled, err := cooling.IsCooled(ctx, sessionID)
		if err != nil {
			log.Printf("[冷却窗口] 检查失败 session=%s err=%v", sessionID, err)
		}
		if !cooled {
			log.Printf("[冷却窗口] session=%s 在冷却期内，跳过 LLM", sessionID)
			// Try to use cached profile.
			if profileCache != nil {
				dst := make([]float32, cache.IntentVectorDim)
				if ver, readErr := profileCache.ReadProfile(ctx, sessionID, dst); readErr == nil {
					log.Printf("[冷却窗口] session=%s 使用缓存画像 version=%d", sessionID, ver)
					return nil
				}
				log.Printf("[冷却窗口] session=%s 缓存未命中，仍跳过 LLM", sessionID)
			}
			return nil
		}
	}

	log.Printf("[慢轨触发] session=%s events=%d baseline=%d", sessionID, len(batch.Events), batch.BaselineVersion)
	st := &agent.State{SessionID: sessionID, BaselineVersion: batch.BaselineVersion, Events: batch.Events, Metadata: map[string]string{agent.MetadataReflectionActive: "true"}}
	g, err := BuildGraph(llm, embedder, writer, publisher)
	if err != nil {
		log.Printf("[慢轨错误] BuildGraph 失败: %v", err)
		return err
	}
	runErr := g.Run(ctx, st)
	if runErr != nil {
		log.Printf("[慢轨错误] DAG 执行失败: %v", runErr)
	} else {
		log.Printf("[慢轨完成] session=%s", sessionID)
		// Mark cooling window after successful LLM call.
		if cooling != nil {
			if markErr := cooling.MarkCalled(ctx, sessionID); markErr != nil {
				log.Printf("[冷却窗口] 标记失败 session=%s err=%v", sessionID, markErr)
			}
		}
	}
	return runErr
}

func RunDaemon(ctx context.Context, deps DaemonDeps) error {
	if deps.Consumer == nil || deps.LLM == nil || deps.Embedder == nil || deps.Writer == nil {
		return agent.ErrInvalidNode
	}
	buffer := deps.BatchBuffer
	if buffer <= 0 {
		buffer = 64
	}
	daemonCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	batches := make(chan mq.Batch, buffer)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- deps.Consumer.Consume(daemonCtx, batches)
		close(batches)
	}()

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	recordErr := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for {
		select {
		case batch, ok := <-batches:
			if !ok {
				wg.Wait()
				consumeErr := <-consumeDone
				if firstErr != nil {
					return firstErr
				}
				return consumeErr
			}
			workflowCtx := context.WithoutCancel(ctx)
			wg.Add(1)
			go func(b mq.Batch) {
				defer wg.Done()
				recordErr(RunBatch(workflowCtx, b, deps.LLM, deps.Embedder, deps.Writer, deps.LogPublisher, deps.Cooling, deps.ProfileCache))
			}(batch)
		case <-ctx.Done():
			_ = deps.Consumer.Close()
			cancel()
			wg.Wait()
			consumeErr := <-consumeDone
			if firstErr != nil {
				return firstErr
			}
			if consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
				return consumeErr
			}
			return ctx.Err()
		}
	}
}

func RunWithSignals(parent context.Context, deps DaemonDeps, sigCh <-chan os.Signal) error {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() { done <- RunDaemon(ctx, deps) }()
	select {
	case sig := <-sigCh:
		if deps.Logger != nil {
			deps.Logger.Printf("received shutdown signal: %v", sig)
		}
		cancel()
		return <-done
	case err := <-done:
		cancel()
		return err
	case <-parent.Done():
		cancel()
		return <-done
	}
}

type closableWindowConsumer struct {
	consumer *mq.WindowConsumer
	closer   interface{ Close() error }
}

func (c *closableWindowConsumer) Consume(ctx context.Context, out chan<- mq.Batch) error {
	return c.consumer.Consume(ctx, out)
}

func (c *closableWindowConsumer) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

const defaultRedisAddr = "127.0.0.1:6379"

func main() {
	_ = godotenv.Load()

	redisClient := redis.NewClient(&redis.Options{Addr: envDefault("REDIS_ADDR", defaultRedisAddr), Password: os.Getenv("REDIS_PASSWORD"), DB: envInt("REDIS_DB", 0)})
	writer, err := cache.NewRedisIntentWriter(cache.RedisIntentWriterOptions{Client: redisClient})
	if err != nil {
		log.Fatalf("init redis writer: %v", err)
	}

	// Event source: Redis Pub/Sub by default. Kafka is wired and
	// available — set USE_KAFKA=true with a running cluster to activate.
	var batchConsumer batchConsumer

	if os.Getenv("USE_KAFKA") != "true" {
		log.Printf("using Redis Pub/Sub for behavior event consumption (channel: behavior:events)")
		src := mq.NewRedisEventSource(redisClient, "behavior:events")
		cons, consErr := mq.NewWindowConsumer(src, mq.Options{MaxBatch: 1, FlushInterval: 200 * time.Millisecond})
		if consErr != nil {
			log.Fatalf("init redis window consumer: %v", consErr)
		}
		batchConsumer = &closableWindowConsumer{consumer: cons, closer: src}
	} else {
		src, err := mq.NewKafkaEventSourceFromEnv()
		if err != nil {
			log.Fatalf("init kafka source: %v", err)
		}
		log.Printf("using Kafka as behavior event source")
		cons, consErr := mq.NewWindowConsumer(src, mq.Options{MaxBatch: envInt("KAFKA_MAX_BATCH", 64), FlushInterval: time.Duration(envInt("KAFKA_FLUSH_MS", 200)) * time.Millisecond})
		if consErr != nil {
			log.Fatalf("init kafka window consumer: %v", consErr)
		}
		batchConsumer = &closableWindowConsumer{consumer: cons, closer: src}
	}

	llmClient := slow_track.NewClientFromEnv()
	embedder := agent.NewRemoteEmbedderFromEnv()
	logPublisher := agent.NewRedisLogPublisher(redisClient)
	if logPublisher == nil {
		log.Printf("[警告] LogPublisher 为 nil!")
	} else {
		log.Printf("[初始化] LogPublisher 已就绪")
	}

	// Initialize cooling window and profile cache for LLM optimization.
	coolingChecker, coolingErr := cache.NewRedisCoolingChecker(cache.RedisCoolingCheckerOptions{
		Client: redisClient,
		Window: 60 * time.Second,
	})
	if coolingErr != nil {
		log.Printf("[警告] CoolingChecker 初始化失败: %v", coolingErr)
	}

	bloomFilter, bloomErr := bloom.NewRedisBloomFilter(bloom.Options{
		Client: redisClient,
		M:      1 << 20, // ~1M bits
		K:      7,       // optimal for < 1% false positive
		Key:    "bloom:profile:default",
	})
	if bloomErr != nil {
		log.Printf("[警告] BloomFilter 初始化失败: %v", bloomErr)
	}

	profileCache := cache.NewProfileCacheReader(cache.ProfileCacheReaderOptions{
		Client:    redisClient,
		Bloom:     bloomFilter,
		IntentDim: cache.IntentVectorDim,
	})

	deps := DaemonDeps{
		Consumer:     batchConsumer,
		LLM:          slowTrackLLMAdapter{client: llmClient},
		Embedder:     embedder,
		Writer:       writer,
		LogPublisher: logPublisher,
		Cooling:      coolingChecker,
		ProfileCache: profileCache,
		Logger:       log.Default(),
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	if err := RunWithSignals(context.Background(), deps, sigCh); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("agent daemon stopped: %v", err)
	}
}

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
