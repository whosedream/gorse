package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisLogPublisherPublishesToSessionChannel(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pubsub := client.Subscribe(ctx, SSELogChannelPrefix+"s1")
	t.Cleanup(func() { _ = pubsub.Close() })
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}

	publisher := NewRedisLogPublisher(client)
	if err := publisher.PublishLog(ctx, "s1", "[LLM推理开始] hello"); err != nil {
		t.Fatalf("PublishLog returned error: %v", err)
	}

	select {
	case msg := <-pubsub.Channel():
		if msg.Channel != SSELogChannelPrefix+"s1" || msg.Payload != "[LLM推理开始] hello" {
			t.Fatalf("message = channel %q payload %q", msg.Channel, msg.Payload)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for pubsub message: %v", ctx.Err())
	}
}

func TestDAGNodesPublishObservableSlowTrackLogs(t *testing.T) {
	t.Parallel()

	publisher := &recordingLogPublisher{}
	client := &sequenceClient{responses: []string{makeVectorJSON("s-log", 77, 0.5)}}
	neural := NewNeuralIntentNode(NeuralNodeOptions{Client: client, PromptBuilder: DefaultPromptBuilder(), LogPublisher: publisher})
	st := &State{SessionID: "s-log", BaselineVersion: 77, Metadata: map[string]string{MetadataReflectionActive: "true"}}
	if err := neural.Run(context.Background(), st); err != nil {
		t.Fatalf("neural Run returned error: %v", err)
	}

	vec := make([]float32, 1024)
	embedder := &fakeEmbedder{vec: vec}
	embedding := NewEmbeddingNode(EmbeddingNodeOptions{Embedder: embedder, LogPublisher: publisher})
	if err := embedding.Run(context.Background(), st); err != nil {
		t.Fatalf("embedding Run returned error: %v", err)
	}

	writer := &mockIntentWriter{writes: make(chan stateSyncWrite, 1)}
	syncNode := NewStateSyncNode(StateSyncNodeOptions{Writer: writer, LogPublisher: publisher})
	if err := syncNode.Run(context.Background(), st); err != nil {
		t.Fatalf("state sync Run returned error: %v", err)
	}

	publisher.assertContains(t, "s-log", "[LLM推理开始]")
	publisher.assertContains(t, "s-log", "[反思触发]")
	publisher.assertContains(t, "s-log", "[LLM推理完成]")
	publisher.assertContains(t, "s-log", "[向量生成]")
	publisher.assertContains(t, "s-log", "[Redis写入成功]")
}

type recordingLogPublisher struct {
	entries []recordedLog
}

type recordedLog struct {
	sessionID string
	message   string
}

func (p *recordingLogPublisher) PublishLog(ctx context.Context, sessionID string, message string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.entries = append(p.entries, recordedLog{sessionID: sessionID, message: message})
	return nil
}

func (p *recordingLogPublisher) assertContains(t *testing.T, sessionID string, want string) {
	t.Helper()
	for _, entry := range p.entries {
		if entry.sessionID == sessionID && strings.Contains(entry.message, want) {
			return
		}
	}
	t.Fatalf("missing log session=%q containing %q in %+v", sessionID, want, p.entries)
}
