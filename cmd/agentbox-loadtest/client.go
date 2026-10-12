package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Protocol is a wire format: the client's (which endpoint and auth header) or the provider's.
type Protocol string

const (
	OpenAI    Protocol = "openai"
	Anthropic Protocol = "anthropic"
)

// reqSpec is one request to send.
type reqSpec struct {
	Proto       Protocol
	Model       string
	Stream      bool
	MaxTokens   int
	Prompt      string
	Key         string // platform key; empty for a direct request to the fake provider
	CancelAfter bool   // cancel right after the first content event (a client that goes away mid-stream)
}

// body encodes the request in its format.
func (s reqSpec) body() []byte {
	msgs := []map[string]any{{"role": "user", "content": s.Prompt}}
	var v map[string]any
	if s.Proto == Anthropic {
		v = map[string]any{"model": s.Model, "max_tokens": s.MaxTokens, "messages": msgs}
	} else {
		v = map[string]any{"model": s.Model, "max_tokens": s.MaxTokens, "messages": msgs}
		if s.Stream {
			v["stream_options"] = map[string]any{"include_usage": true}
		}
	}
	if s.Stream {
		v["stream"] = true
	}
	b, _ := json.Marshal(v)
	return b
}

// target is where requests go: the gateway (base = server root) or a fake provider directly.
type target struct {
	OpenAIURL    string // full URL of chat/completions
	AnthropicURL string // full URL of messages
}

func gatewayTarget(addr string) target {
	addr = strings.TrimRight(addr, "/")
	return target{OpenAIURL: addr + "/v1/chat/completions", AnthropicURL: addr + "/v1/messages"}
}

// directTarget addresses fakeupstream itself: openaiBase is its model_base_url (…/v1), root its url.
func directTarget(openaiBase, root string) target {
	return target{OpenAIURL: strings.TrimRight(openaiBase, "/") + "/chat/completions",
		AnthropicURL: strings.TrimRight(root, "/") + "/v1/messages"}
}

// newHTTPClient returns a keep-alive client sized for conc concurrent requests.
func newHTTPClient(conc int) *http.Client {
	tr := &http.Transport{
		Proxy:               nil, // never through a proxy: everything is local
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        conc * 2,
		MaxIdleConnsPerHost: conc * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &http.Client{Transport: tr, Timeout: 2 * time.Minute}
}

// keptHeaders are response headers recorded in results (rate-limit evidence).
var keptHeaders = []string{"Retry-After", "X-Ratelimit-Limit-Requests", "X-Ratelimit-Remaining-Requests",
	"X-Ratelimit-Reset-Requests", "X-Ratelimit-Limit-Tokens", "X-Ratelimit-Remaining-Tokens", "X-Ratelimit-Reset-Tokens",
	"X-Request-Id"}

// send performs one request and measures it. A deadline on ctx ends the request; such a result has Status 0.
func send(ctx context.Context, hc *http.Client, t target, s reqSpec) result {
	url := t.OpenAIURL
	if s.Proto == Anthropic {
		url = t.AnthropicURL
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(s.body()))
	r := result{Stream: s.Stream}
	if err != nil {
		r.Code = "transport"
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Proto == Anthropic {
		req.Header.Set("anthropic-version", "2023-06-01")
		if s.Key != "" {
			req.Header.Set("x-api-key", s.Key)
		}
	} else if s.Key != "" {
		req.Header.Set("Authorization", "Bearer "+s.Key)
	}
	r.Start = time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		r.Code, r.Total = "transport", time.Since(r.Start)
		return r
	}
	defer func() { _ = resp.Body.Close() }()
	r.TTFB, r.Status = time.Since(r.Start), resp.StatusCode
	for _, h := range keptHeaders {
		if v := resp.Header.Get(h); v != "" {
			if r.Header == nil {
				r.Header = map[string]string{}
			}
			r.Header[strings.ToLower(h)] = v
		}
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		r.Code, r.Msg = errorInfo(b)
		r.Total = time.Since(r.Start)
		return r
	}
	if !s.Stream {
		_, err := io.Copy(io.Discard, resp.Body)
		r.Total = time.Since(r.Start)
		if err != nil {
			r.Status, r.Code = 0, "transport"
		}
		return r
	}
	readStream(resp.Body, s.Proto, &r, s.CancelAfter, cancel)
	r.Total = time.Since(r.Start)
	return r
}

// readStream reads an SSE body, recording the first content event, completion and error events. With cancelAfter it
// cancels the request right after the first content event and stops reading.
func readStream(body io.Reader, proto Protocol, r *result, cancelAfter bool, cancel func()) {
	br := bufio.NewReaderSize(body, 64<<10)
	event := ""
	for {
		line, err := br.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			done := classifyLine(proto, line, &event, r)
			if r.TTFT == 0 && done == lineContent {
				r.TTFT = time.Since(r.Start)
				if cancelAfter {
					r.Canceled = true
					cancel()
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

type lineKind int

const (
	lineOther lineKind = iota
	lineContent
)

// classifyLine inspects one SSE line. OpenAI: "data: {…}" chunks, "data: [DONE]", an error object. Anthropic: the
// "event:" line names the next data line; content_block_delta is content, message_stop completes, error is an error.
func classifyLine(proto Protocol, line string, event *string, r *result) lineKind {
	if ev, ok := strings.CutPrefix(line, "event:"); ok {
		*event = strings.TrimSpace(ev)
		return lineOther
	}
	data, ok := strings.CutPrefix(line, "data:")
	if !ok {
		return lineOther
	}
	data = strings.TrimSpace(data)
	if proto == Anthropic {
		switch *event {
		case "content_block_delta":
			return lineContent
		case "message_stop":
			r.Complete = true
		case "error":
			r.ErrEvent = true
		}
		return lineOther
	}
	if data == "[DONE]" {
		r.Complete = true
		return lineOther
	}
	if strings.HasPrefix(data, `{"error"`) {
		r.ErrEvent = true
		return lineOther
	}
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content   string            `json:"content"`
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(data), &chunk) == nil {
		for _, c := range chunk.Choices {
			if c.Delta.Content != "" || len(c.Delta.ToolCalls) > 0 {
				return lineContent
			}
		}
	}
	return lineOther
}

// errorInfo extracts the error code and message from an OpenAI ({"error":{"code","type","message"}}) or Anthropic
// ({"type":"error","error":{"type","message"}}) error body. For a 429 in Anthropic format, insufficient quota is
// recognised by its message (the type is rate_limit_error for both).
func errorInfo(b []byte) (string, string) {
	var e struct {
		Error struct {
			Code    any    `json:"code"`
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		Code string `json:"code"` // runtime API {code,message}
	}
	if json.Unmarshal(b, &e) != nil {
		return "", ""
	}
	msg := e.Error.Message
	if c, ok := e.Error.Code.(string); ok && c != "" {
		return c, msg
	}
	if e.Error.Type == "rate_limit_error" && strings.Contains(msg, "insufficient quota") {
		return "insufficient_quota", msg
	}
	if e.Error.Type != "" {
		return e.Error.Type, msg
	}
	return e.Code, msg
}

// operator is the operator-token HTTP client used for setup (keys).
type operator struct {
	addr, token string
	hc          *http.Client
}

func (o operator) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(o.addr, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(b))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}
