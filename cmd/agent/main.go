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
	return resp.Text, nil
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

func RunBatch(ctx context.Context, batch mq.Batch, llm LLM, embedder agent.Embedder, writer cache.IntentWriter, publisher agent.LogPublisher) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	sessionID := batch.SessionID
	if sessionID == "" {
		sessionID = batch.UserID
	}
	st := &agent.State{SessionID: sessionID, BaselineVersion: batch.BaselineVersion, Events: batch.Events}
	g, err := BuildGraph(llm, embedder, writer, publisher)
	if err != nil {
		return err
	}
	return g.Run(ctx, st)
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
				recordErr(RunBatch(workflowCtx, b, deps.LLM, deps.Embedder, deps.Writer, deps.LogPublisher))
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
	source, err := mq.NewKafkaEventSourceFromEnv()
	if err != nil {
		log.Fatalf("init kafka source: %v", err)
	}
	consumer, err := mq.NewWindowConsumer(source, mq.Options{MaxBatch: envInt("KAFKA_MAX_BATCH", 64), FlushInterval: time.Duration(envInt("KAFKA_FLUSH_MS", 200)) * time.Millisecond})
	if err != nil {
		log.Fatalf("init window consumer: %v", err)
	}
	llmClient := slow_track.NewClientFromEnv()
	embedder := agent.NewRemoteEmbedderFromEnv()
	logPublisher := agent.NewRedisLogPublisher(redisClient)
	deps := DaemonDeps{Consumer: &closableWindowConsumer{consumer: consumer, closer: source}, LLM: slowTrackLLMAdapter{client: llmClient}, Embedder: embedder, Writer: writer, LogPublisher: logPublisher, Logger: log.Default()}

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
