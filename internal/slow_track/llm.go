package slow_track

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("slow track circuit open")

// Message is one context-management chat message.
type Message struct {
	Role    string
	Content string
}

// Request captures prompt, multi-turn context, CoT, and input cache hint.
type Request struct {
	SystemPrompt string
	UserPrompt   string
	Messages     []Message
	EnableCoT    bool
	CacheKey     string
}

// Response is the normalized DeepSeek response.
type Response struct {
	Text      string
	Reasoning string
	Cached    bool
}

// Options controls the DeepSeek V4 Pro HTTP client.
type Options struct {
	Endpoint         string
	APIKey           string
	Model            string
	HTTPClient       *http.Client
	MaxRetries       int
	BreakerThreshold int
	BreakerOpenFor   time.Duration
}

// Client wraps retry, context cancellation, and a compact circuit breaker.
type Client struct {
	endpoint         string
	apiKey           string
	model            string
	httpClient       *http.Client
	maxRetries       int
	breakerThreshold int
	breakerOpenFor   time.Duration

	mu           sync.Mutex
	failures     int
	breakerUntil time.Time
}

// NewClient constructs a DeepSeek client with defensive defaults.
func NewClient(opts Options) *Client {
	hc := opts.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	threshold := opts.BreakerThreshold
	if threshold <= 0 {
		threshold = 3
	}
	openFor := opts.BreakerOpenFor
	if openFor <= 0 {
		openFor = 200 * time.Millisecond
	}
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = "deepseek-v4-pro"
	}
	return &Client{endpoint: normalizeChatEndpoint(opts.Endpoint), apiKey: opts.APIKey, model: model, httpClient: hc, maxRetries: opts.MaxRetries, breakerThreshold: threshold, breakerOpenFor: openFor}
}

func OptionsFromEnv() Options {
	return Options{
		Endpoint: normalizeChatEndpoint(os.Getenv("LLM_BASE_URL")),
		APIKey:   os.Getenv("LLM_API_KEY"),
		Model:    os.Getenv("LLM_MODEL"),
	}
}

func NewClientFromEnv() *Client {
	return NewClient(OptionsFromEnv())
}

func normalizeChatEndpoint(raw string) string {
	endpoint := strings.TrimRight(strings.TrimSpace(raw), "/")
	if endpoint == "" {
		endpoint = "https://api.deepseek.com"
	}
	if strings.HasSuffix(endpoint, "/chat/completions") {
		return endpoint
	}
	return endpoint + "/chat/completions"
}

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type thinkingConfig struct {
	Type string `json:"type"`
}

type wireRequest struct {
	Model    string          `json:"model"`
	Messages []wireMessage   `json:"messages"`
	Stream   bool            `json:"stream"`
	MaxTokens int            `json:"max_tokens,omitempty"`
	Thinking *thinkingConfig `json:"thinking,omitempty"`
}

type wireResponse struct {
	Choices []struct {
		Message struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// Complete calls the model endpoint. It retries 5xx/429/network failures and
// never retries other 4xx responses.
func (c *Client) Complete(ctx context.Context, req Request) (Response, error) {
	if err := c.allow(); err != nil {
		return Response{}, err
	}
	attempts := c.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		default:
		}
		resp, retry, err := c.doComplete(ctx, req)
		if err == nil {
			c.recordSuccess()
			return resp, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		if !retry || attempt == attempts-1 {
			c.recordFailure()
			return Response{}, err
		}
	}
	c.recordFailure()
	return Response{}, lastErr
}

func (c *Client) doComplete(ctx context.Context, req Request) (Response, bool, error) {
	body, err := json.Marshal(makeWireRequest(req, c.model))
	if err != nil {
		return Response{}, false, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return Response{}, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return Response{}, true, err
	}
	defer httpResp.Body.Close()
	payload, readErr := io.ReadAll(httpResp.Body)
	if readErr != nil {
		return Response{}, true, readErr
	}
	if httpResp.StatusCode >= 200 && httpResp.StatusCode < 300 {
		var wr wireResponse
		if err := json.Unmarshal(payload, &wr); err != nil {
			return Response{}, false, err
		}
		var text, reasoning string
		if len(wr.Choices) > 0 {
			text = wr.Choices[0].Message.Content
			reasoning = wr.Choices[0].Message.ReasoningContent
		}
		cached := wr.Usage.PromptTokens > 0 && wr.Usage.CompletionTokens == 0
		return Response{Text: text, Reasoning: reasoning, Cached: cached}, false, nil
	}
	statusErr := fmt.Errorf("deepseek status %d: %s", httpResp.StatusCode, string(payload))
	if httpResp.StatusCode == http.StatusTooManyRequests || httpResp.StatusCode >= 500 {
		return Response{}, true, statusErr
	}
	return Response{}, false, statusErr
}

func makeWireRequest(req Request, model string) wireRequest {
	var messages []wireMessage
	if req.SystemPrompt != "" {
		messages = append(messages, wireMessage{Role: "system", Content: req.SystemPrompt})
	}
	if req.UserPrompt != "" {
		messages = append(messages, wireMessage{Role: "user", Content: req.UserPrompt})
	}
	for _, msg := range req.Messages {
		messages = append(messages, wireMessage{Role: msg.Role, Content: msg.Content})
	}
	wr := wireRequest{Model: model, Messages: messages, Stream: false, MaxTokens: 4096}
	if req.EnableCoT {
		wr.Thinking = &thinkingConfig{Type: "enabled"}
	}
	return wr
}

func (c *Client) allow() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.breakerUntil.IsZero() || time.Now().After(c.breakerUntil) {
		if !c.breakerUntil.IsZero() {
			c.breakerUntil = time.Time{}
			c.failures = 0
		}
		return nil
	}
	return ErrCircuitOpen
}

func (c *Client) recordSuccess() {
	c.mu.Lock()
	c.failures = 0
	c.breakerUntil = time.Time{}
	c.mu.Unlock()
}

func (c *Client) recordFailure() {
	c.mu.Lock()
	c.failures++
	if c.failures >= c.breakerThreshold {
		c.breakerUntil = time.Now().Add(c.breakerOpenFor)
	}
	c.mu.Unlock()
}
