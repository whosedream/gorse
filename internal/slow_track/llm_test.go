package slow_track

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeepSeekEnvOptionsAndChatCompletionsRequest(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "env 读取 base url api key model 并补齐 chat completions endpoint",
			run: func(t *testing.T) {
				t.Setenv("LLM_BASE_URL", "https://api.deepseek.com")
				t.Setenv("LLM_API_KEY", "placeholder-key")
				t.Setenv("LLM_MODEL", "deepseek-v4-pro")

				opts := OptionsFromEnv()
				if opts.Endpoint != "https://api.deepseek.com/chat/completions" {
					t.Fatalf("endpoint mismatch: %s", opts.Endpoint)
				}
				if opts.APIKey != "placeholder-key" {
					t.Fatalf("api key mismatch: %q", opts.APIKey)
				}
				if opts.Model != "deepseek-v4-pro" {
					t.Fatalf("model mismatch: %q", opts.Model)
				}
			},
		},
		{
			name: "httptest 验证 body 包含 model 且 Authorization 来自 env",
			run: func(t *testing.T) {
				t.Setenv("LLM_API_KEY", "placeholder-key")
				t.Setenv("LLM_MODEL", "deepseek-v4-pro")

				seen := make(chan struct{}, 1)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/chat/completions" {
						t.Fatalf("path mismatch: %s", r.URL.Path)
					}
					if got := r.Header.Get("Authorization"); got != "Bearer placeholder-key" {
						t.Fatalf("authorization mismatch: %q", got)
					}
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatalf("decode body: %v", err)
					}
					if got, _ := body["model"].(string); got != "deepseek-v4-pro" {
						t.Fatalf("model body mismatch: %#v", body["model"])
					}
					seen <- struct{}{}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
				}))
				defer srv.Close()

				t.Setenv("LLM_BASE_URL", srv.URL)
				c := NewClientFromEnv()
				resp, err := c.Complete(context.Background(), Request{UserPrompt: "hello", EnableCoT: true})
				if err != nil {
					t.Fatalf("Complete returned error: %v", err)
				}
				if resp.Text != "ok" {
					t.Fatalf("response mismatch: %+v", resp)
				}
				select {
				case <-seen:
				default:
					t.Fatal("server did not receive request")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestDeepSeekClientCoTRetryBreakerAndHTTPStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "请求体包含 thinking 字段且解析响应含 reasoning_content",
			run: func(t *testing.T) {
				var sawThinking atomic.Bool
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					buf := make([]byte, r.ContentLength)
					_, _ = r.Body.Read(buf)
					body := string(buf)
					if strings.Contains(body, `"thinking"`) && strings.Contains(body, `"enabled"`) {
						sawThinking.Store(true)
					}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok","reasoning_content":"cot"}}],"usage":{"prompt_tokens":10,"completion_tokens":20}}`))
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, APIKey: "k", MaxRetries: 1, BreakerThreshold: 3, BreakerOpenFor: time.Second})
				resp, err := c.Complete(context.Background(), Request{SystemPrompt: "sys", UserPrompt: "user", EnableCoT: true, CacheKey: "cache-key"})
				if err != nil {
					t.Fatalf("Complete returned error: %v", err)
				}
				if !sawThinking.Load() || resp.Text != "ok" || resp.Reasoning != "cot" {
					t.Fatalf("unexpected: saw=%v resp=%+v", sawThinking.Load(), resp)
				}
			},
		},
		{
			name: "500 后重试成功",
			run: func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if calls.Add(1) == 1 {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`down`))
						return
					}
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok-after-retry"}}],"usage":{}}`))
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, MaxRetries: 2, BreakerThreshold: 5, BreakerOpenFor: time.Second})
				resp, err := c.Complete(context.Background(), Request{UserPrompt: "u"})
				if err != nil {
					t.Fatalf("Complete returned error: %v", err)
				}
				if resp.Text != "ok-after-retry" || calls.Load() != 2 {
					t.Fatalf("retry mismatch: calls=%d resp=%+v", calls.Load(), resp)
				}
			},
		},
		{
			name: "429 连续失败打开熔断且 open 时不打到 server",
			run: func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`rate limited`))
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, MaxRetries: 0, BreakerThreshold: 2, BreakerOpenFor: time.Second})
				_, err1 := c.Complete(context.Background(), Request{UserPrompt: "u"})
				_, err2 := c.Complete(context.Background(), Request{UserPrompt: "u"})
				if err1 == nil || err2 == nil {
					t.Fatalf("expected first two failures, got %v %v", err1, err2)
				}
				before := calls.Load()
				_, err3 := c.Complete(context.Background(), Request{UserPrompt: "u"})
				if !errors.Is(err3, ErrCircuitOpen) {
					t.Fatalf("expected ErrCircuitOpen, got %v", err3)
				}
				if after := calls.Load(); after != before {
					t.Fatalf("breaker open still hit server: before=%d after=%d", before, after)
				}
			},
		},
		{
			name: "5xx 连续失败打开熔断",
			run: func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.WriteHeader(http.StatusBadGateway)
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, MaxRetries: 0, BreakerThreshold: 1, BreakerOpenFor: time.Second})
				_, err := c.Complete(context.Background(), Request{UserPrompt: "u"})
				if err == nil {
					t.Fatal("expected 5xx error")
				}
				_, err = c.Complete(context.Background(), Request{UserPrompt: "u"})
				if !errors.Is(err, ErrCircuitOpen) {
					t.Fatalf("expected ErrCircuitOpen, got %v", err)
				}
				if calls.Load() != 1 {
					t.Fatalf("unexpected calls after open breaker: %d", calls.Load())
				}
			},
		},
		{
			name: "4xx 非 429 不重试",
			run: func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`bad`))
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, MaxRetries: 5, BreakerThreshold: 5, BreakerOpenFor: time.Second})
				_, err := c.Complete(context.Background(), Request{UserPrompt: "bad"})
				if err == nil {
					t.Fatal("expected 400 error")
				}
				if calls.Load() != 1 {
					t.Fatalf("4xx non-429 retried: calls=%d", calls.Load())
				}
			},
		},
		{
			name: "上下文超时取消快速返回",
			run: func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(50 * time.Millisecond)
				}))
				defer srv.Close()
				c := NewClient(Options{Endpoint: srv.URL, MaxRetries: 1, BreakerThreshold: 5, BreakerOpenFor: time.Second})
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				_, err := c.Complete(ctx, Request{UserPrompt: "slow"})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected DeadlineExceeded, got %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}
