package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// Provider protocols (model gateway design §7.3, §7.6): an Anthropic route on the sandbox path (OpenAI in,
// OpenAI out) and DoStream on both protocols, against tests/e2e/fakeupstream.

// mixedAdapter is an OpenAI primary on p and an Anthropic fallback ("claude") on q; logical model m-1 is
// "claude-x" on the Anthropic route.
func mixedAdapter(t *testing.T, p, q *fakeupstream.Server) ChatAdapter {
	t.Helper()
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	a := NewChat(ChatConfig{BaseURL: p.ModelBaseURL(), Model: "m-1", Models: []string{"m-1", "m-2"}, APIKey: "key-openai",
		Pricing: Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}, MaxTokensDefault: 100,
		HTTP: d.HTTPClient(DefaultModelMaxBody, 10*time.Second),
		Fallbacks: []ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: q.URL(), APIKey: "key-anthropic",
			Models:  map[string]string{"m-1": "claude-x"},
			Pricing: Pricing{InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000, CacheReadMicroPerMTok: 300_000, CacheWriteMicroPerMTok: 3_750_000},
			HTTP:    d.HTTPClient(DefaultModelMaxBody, 10*time.Second)}}})
	ca, ok := a.(ChatAdapter)
	if !ok {
		t.Fatal("NewChat does not implement ChatAdapter")
	}
	return ca
}

func fakePair(t *testing.T) (*fakeupstream.Server, *fakeupstream.Server) {
	t.Helper()
	p, q := fakeupstream.New(), fakeupstream.New()
	t.Cleanup(p.Close)
	t.Cleanup(q.Close)
	q.SetAnthropicKey("key-anthropic")
	return p, q
}

// The sandbox path on an Anthropic route: the OpenAI request is converted (x-api-key, anthropic-version,
// /v1/messages, provider model name, reasoning_content left out) and the reply comes back in OpenAI shape with
// usage that includes cache tokens.
func TestAnthropicRouteSandboxPath(t *testing.T) {
	p, q := fakePair(t)
	q.SetCacheUsage(5, 7)
	a := mixedAdapter(t, p, q)
	if a.RouteProtocol(0) != llmproto.ProtocolOpenAI || a.RouteProtocol(1) != llmproto.ProtocolAnthropic {
		t.Fatalf("protocols %s %s", a.RouteProtocol(0), a.RouteProtocol(1))
	}
	resolved := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"},`+
		`{"role":"assistant","content":"x","reasoning_content":"thought"},{"role":"user","content":"again"}]}`)
	resp, e := a.DoOn(context.Background(), 1, resolved)
	if e != nil {
		t.Fatal(e)
	}
	reqs := q.Requests(fakeupstream.Messages)
	if len(reqs) != 1 || reqs[0].APIKey != "key-anthropic" || reqs[0].Authorization != "" || reqs[0].Path != "/v1/messages" {
		t.Fatalf("upstream request %+v", reqs)
	}
	var sent struct {
		Model     string `json:"model"`
		System    any    `json:"system"`
		MaxTokens int    `json:"max_tokens"`
	}
	unmarshal(t, []byte(reqs[0].Body), &sent)
	if sent.Model != "claude-x" || sent.MaxTokens != 100 || sent.System == nil || strings.Contains(reqs[0].Body, "thought") {
		t.Fatalf("converted request %s", reqs[0].Body)
	}
	var out struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	unmarshal(t, resp.Body, &out)
	if out.Object != "chat.completion" || len(out.Choices) != 1 || out.Choices[0].Message.Role != "assistant" ||
		!strings.HasPrefix(out.Choices[0].Message.Content, "fake 回复") || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("reply not OpenAI-shaped: %s", resp.Body)
	}
	u := resp.Usage
	if u.CacheReadTokens != 5 || u.CacheWriteTokens != 7 || u.InputTokens != out.Usage.PromptTokens || u.InputTokens <= 12 ||
		u.OutputTokens != out.Usage.CompletionTokens || resp.UpstreamRequestID == "" {
		t.Fatalf("usage %+v (reply %+v)", u, out.Usage)
	}
	// Cost: plain input × 3 + 5 × 0.3 + 7 × 3.75 + output × 15 (micro-USD per token here: price / 1e6).
	price := a.PricingOn(1, "m-1")
	got, ok := price.ChatCost(u)
	want := ceilDiv((u.InputTokens-12)*3_000_000) + ceilDiv(5*300_000) + ceilDiv(7*3_750_000) + ceilDiv(u.OutputTokens*15_000_000)
	if !ok || got != want {
		t.Fatalf("cost %d, want %d", got, want)
	}
}

func ceilDiv(v int64) int64 { return (v + 999_999) / 1_000_000 }

// Tool calls round-trip on the sandbox path: the Anthropic tool_use comes back as OpenAI tool_calls with
// finish_reason tool_calls; a follow-up turn with the tool result converts back.
func TestAnthropicRouteToolCalls(t *testing.T) {
	p, q := fakePair(t)
	q.SetMessagesHook(func([]byte) json.RawMessage {
		return json.RawMessage(`[{"type":"text","text":"let me look"},{"type":"tool_use","id":"toolu_1","name":"search","input":{"q":"go"}}]`)
	})
	a := mixedAdapter(t, p, q)
	resolved := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"user","content":"find"}],`+
		`"tools":[{"type":"function","function":{"name":"search","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}}]}`)
	resp, e := a.DoOn(context.Background(), 1, resolved)
	if e != nil {
		t.Fatal(e)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	unmarshal(t, resp.Body, &out)
	c := out.Choices[0]
	if c.FinishReason != "tool_calls" || len(c.Message.ToolCalls) != 1 || c.Message.ToolCalls[0].ID != "toolu_1" ||
		c.Message.ToolCalls[0].Function.Name != "search" || c.Message.ToolCalls[0].Function.Arguments != `{"q":"go"}` {
		t.Fatalf("tool call reply %s", resp.Body)
	}
	follow := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"user","content":"find"},`+
		`{"role":"assistant","content":null,"tool_calls":[{"id":"toolu_1","type":"function","function":{"name":"search","arguments":"{\"q\":\"go\"}"}}]},`+
		`{"role":"tool","tool_call_id":"toolu_1","content":"result"}]}`)
	if _, e := a.DoOn(context.Background(), 1, follow); e != nil {
		t.Fatal(e)
	}
	body := q.Requests(fakeupstream.Messages)[1].Body
	if !strings.Contains(body, `"tool_result"`) || !strings.Contains(body, `"tool_use_id":"toolu_1"`) {
		t.Fatalf("follow-up not converted: %s", body)
	}
}

// Errors on an Anthropic route are classified like the OpenAI route: 529 and 429 retryable (with Retry-After),
// 400 fatal, a wrong key fatal 401; a request with a field Anthropic cannot express is not Convertible there.
func TestAnthropicRouteErrors(t *testing.T) {
	p, q := fakePair(t)
	a := mixedAdapter(t, p, q)
	resolved := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"user","content":"hi"}]}`)
	q.Inject(fakeupstream.Messages, 1, fakeupstream.Action{Status: 529})
	q.Inject(fakeupstream.Messages, 2, fakeupstream.Action{Status: 429, RetryAfter: "7"})
	q.Inject(fakeupstream.Messages, 3, fakeupstream.Action{Status: 400})
	_, e := a.DoOn(context.Background(), 1, resolved)
	wantErr(t, e, OutcomeRetryable, CodeUpstreamUnavailable)
	r, e := a.DoOn(context.Background(), 1, resolved)
	wantErr(t, e, OutcomeRetryable, CodeUpstreamRateLimited)
	if r.RetryAfter != 7*time.Second {
		t.Fatalf("retry-after %v", r.RetryAfter)
	}
	_, e = a.DoOn(context.Background(), 1, resolved)
	wantErr(t, e, OutcomeFatal, CodeUpstreamRejected)
	q.SetAnthropicKey("other")
	_, e = a.DoOn(context.Background(), 1, resolved)
	wantErr(t, e, OutcomeFatal, CodeUpstreamRejected)
	if e.Status != http.StatusUnauthorized || strings.Contains(e.Error(), "key-anthropic") {
		t.Fatalf("auth error %v", e)
	}
	schema := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"user","content":"hi"}],`+
		`"response_format":{"type":"json_schema","json_schema":{"name":"x","schema":{"type":"object"}}}}`)
	if !a.Convertible(0, schema) || a.Convertible(1, schema) || !a.Convertible(1, resolved) {
		t.Fatal("Convertible: json_schema must stay on OpenAI routes")
	}
	_, e = a.DoOn(context.Background(), 1, schema)
	wantErr(t, e, OutcomeFatal, CodeUnsupportedField)
}

// collect reads a stream to its end.
func collect(t *testing.T, s Stream) (llmproto.Response, error) {
	t.Helper()
	var c llmproto.Collector
	for {
		ev, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return c.Response(), nil
			}
			return c.Response(), err
		}
		c.Add(ev)
	}
}

// DoStream on both protocols, pass-through and converted: the events assemble the reply, usage is reported
// (include_usage is added for OpenAI providers), the provider model name and stream flag are sent.
func TestDoStreamBothProtocols(t *testing.T) {
	p, q := fakePair(t)
	q.SetCacheUsage(2, 3)
	a := mixedAdapter(t, p, q)
	openaiBody := []byte(`{"model":"m-1","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`)
	anthropicBody := []byte(`{"model":"m-1","max_tokens":50,"metadata":{"user_id":"u1"},"messages":[{"role":"user","content":"hi"}]}`)
	for _, c := range []struct {
		name      string
		route     int
		body      []byte
		proto     llmproto.Protocol
		converted bool
		dropped   bool
	}{
		{"openai→openai", 0, openaiBody, llmproto.ProtocolOpenAI, false, false},
		{"anthropic→openai", 0, anthropicBody, llmproto.ProtocolAnthropic, true, true},
		{"anthropic→anthropic", 1, anthropicBody, llmproto.ProtocolAnthropic, false, false},
		{"openai→anthropic", 1, openaiBody, llmproto.ProtocolOpenAI, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, e := a.DoStream(context.Background(), c.route, c.body, c.proto)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			resp, err := collect(t, s)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Blocks) != 1 || !strings.HasPrefix(resp.Blocks[0].Text, "fake 回复") || resp.StopReason != llmproto.StopEnd {
				t.Fatalf("response %+v", resp)
			}
			u, ok := s.Usage()
			if !ok || u.OutputTokens == 0 {
				t.Fatalf("usage %+v %v", u, ok)
			}
			if c.route == 1 && (u.CacheReadTokens != 2 || u.CacheWriteTokens != 3) {
				t.Fatalf("anthropic cache usage %+v", u)
			}
			info := s.(StreamInfo)
			if info.Converted() != c.converted || (len(info.Dropped()) > 0) != c.dropped {
				t.Fatalf("converted %v dropped %v", info.Converted(), info.Dropped())
			}
		})
	}
	var sent struct {
		Model         string `json:"model"`
		Stream        bool   `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	for _, r := range p.Requests(fakeupstream.Chat) {
		unmarshal(t, []byte(r.Body), &sent)
		if sent.Model != "m-1" || !sent.Stream || !sent.StreamOptions.IncludeUsage || r.Authorization != "Bearer key-openai" {
			t.Fatalf("openai stream request %s", r.Body)
		}
	}
	for _, r := range q.Requests(fakeupstream.Messages) {
		unmarshal(t, []byte(r.Body), &sent)
		if sent.Model != "claude-x" || !sent.Stream || r.APIKey != "key-anthropic" {
			t.Fatalf("anthropic stream request %s", r.Body)
		}
	}
}

// Failures after the first event are returned by Next (no failover possible): a cut stream gives
// io.ErrUnexpectedEOF; an error event ends the stream with the error recorded.
func TestDoStreamFailAfterChunks(t *testing.T) {
	p, q := fakePair(t)
	a := mixedAdapter(t, p, q)
	body := []byte(`{"model":"m-1","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`)
	for _, c := range []struct {
		name  string
		route int
		srv   *fakeupstream.Server
		kind  fakeupstream.Kind
	}{{"openai", 0, p, fakeupstream.Chat}, {"anthropic", 1, q, fakeupstream.Messages}} {
		t.Run(c.name, func(t *testing.T) {
			c.srv.Inject(c.kind, 1, fakeupstream.Action{FailAfterChunks: 2})
			c.srv.Inject(c.kind, 2, fakeupstream.Action{FailAfterChunks: 2, FailWithError: true})
			s, e := a.DoStream(context.Background(), c.route, body, llmproto.ProtocolOpenAI)
			if e != nil {
				t.Fatal(e)
			}
			if _, err := collect(t, s); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("cut stream: %v", err)
			}
			_ = s.Close()
			s, e = a.DoStream(context.Background(), c.route, body, llmproto.ProtocolOpenAI)
			if e != nil {
				t.Fatal(e)
			}
			var col llmproto.Collector
			for {
				ev, err := s.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("error stream: %v", err)
				}
				col.Add(ev)
			}
			if col.Complete() || col.Err() == nil {
				t.Fatalf("error event not seen: complete=%v err=%v", col.Complete(), col.Err())
			}
			_ = s.Close()
		})
	}
}

// Errors before the first event are classified like Do: 429/529 retryable, 400 fatal, a refused connection
// retryable, a cut before any event or a timeout after sending unknown; an error event as the first event is
// retryable when it is an overload.
func TestDoStreamErrorsBeforeFirstByte(t *testing.T) {
	p, q := fakePair(t)
	a := mixedAdapter(t, p, q)
	body := []byte(`{"model":"m-1","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`)
	q.Inject(fakeupstream.Messages, 1, fakeupstream.Action{Status: 529})
	q.Inject(fakeupstream.Messages, 2, fakeupstream.Action{Status: 429})
	q.Inject(fakeupstream.Messages, 3, fakeupstream.Action{Status: 400})
	q.Inject(fakeupstream.Messages, 4, fakeupstream.Action{Disconnect: true})
	q.Inject(fakeupstream.Messages, 5, fakeupstream.Action{Hang: true})
	q.Inject(fakeupstream.Messages, 6, fakeupstream.Action{FailAfterChunks: 1, FailWithError: true})
	for i, want := range []struct {
		o    Outcome
		code string
	}{{OutcomeRetryable, CodeUpstreamUnavailable}, {OutcomeRetryable, CodeUpstreamRateLimited}, {OutcomeFatal, CodeUpstreamRejected},
		{OutcomeUnknown, CodeUpstreamUnconfirmed}} {
		s, e := a.DoStream(context.Background(), 1, body, llmproto.ProtocolOpenAI)
		if s != nil {
			t.Fatalf("case %d: got a stream", i)
		}
		wantErr(t, e, want.o, want.code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, e := a.DoStream(ctx, 1, body, llmproto.ProtocolOpenAI)
	wantErr(t, e, OutcomeUnknown, CodeUpstreamUnconfirmed)
	// The 6th request sends message_start and then an error event: the first event is not an error, so the
	// stream is returned and the error is seen by the reader.
	s, e := a.DoStream(context.Background(), 1, body, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	_ = s.Close()
	// A provider that is down hard: not sent → retryable.
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	dead := NewChat(ChatConfig{BaseURL: "http://" + closedAddr(t) + "/v1", Model: "m-1", HTTP: d.HTTPClient(DefaultModelMaxBody, 0)}).(ChatAdapter)
	_, e = dead.DoStream(context.Background(), 0, body, llmproto.ProtocolOpenAI)
	wantErr(t, e, OutcomeRetryable, CodeUpstreamUnreachable)
	// Requests that cannot be sent at all are fatal before any I/O.
	_, e = a.DoStream(context.Background(), 1, []byte(`{"model":"m-2","messages":[{"role":"user","content":"hi"}]}`), llmproto.ProtocolOpenAI)
	wantErr(t, e, OutcomeFatal, CodeUnsupportedModel)
	_, e = a.DoStream(context.Background(), 1, []byte(`{"model":"m-1","n":2,"messages":[{"role":"user","content":"hi"}]}`), llmproto.ProtocolOpenAI)
	wantErr(t, e, OutcomeFatal, CodeUnsupportedField)
}

// An error event as the very first event (no content yet) is classified by its class.
func TestStreamErrorEventClassification(t *testing.T) {
	for _, c := range []struct {
		class llmproto.ErrorClass
		o     Outcome
	}{{llmproto.ClassRateLimit, OutcomeRetryable}, {llmproto.ClassModelDegraded, OutcomeRetryable}, {llmproto.ClassUpstream, OutcomeRetryable},
		{llmproto.ClassInvalidRequest, OutcomeFatal}, {llmproto.ClassInvalidAPIKey, OutcomeFatal}} {
		if e := streamErrorEvent(&llmproto.ErrorInfo{Class: c.class}); e.Outcome != c.o {
			t.Errorf("%s: %s", c.class, e.Outcome)
		}
	}
}

// Pricing.ChatCost without cache prices is bit-identical to input × in + output × out (the cached tokens an
// OpenAI provider reports are charged as input); estimates use the higher of input and cache-write prices.
func TestChatCostCompat(t *testing.T) {
	p := Pricing{InputMicroPerMTok: 1_234_567, OutputMicroPerMTok: 7_654_321}
	for _, u := range []Usage{{InputTokens: 1, OutputTokens: 1}, {InputTokens: 999, OutputTokens: 3, CacheReadTokens: 500}, {InputTokens: 0, OutputTokens: 0}} {
		got, ok := p.ChatCost(u)
		want := ceilDiv(u.InputTokens*p.InputMicroPerMTok) + ceilDiv(u.OutputTokens*p.OutputMicroPerMTok)
		if !ok || got != want {
			t.Errorf("%+v: %d want %d", u, got, want)
		}
	}
	if _, ok := p.ChatCost(Usage{InputTokens: -1}); ok {
		t.Error("negative usage accepted")
	}
	c := Pricing{InputMicroPerMTok: 3, CacheReadMicroPerMTok: 1}
	if _, ok := c.ChatCost(Usage{InputTokens: 1, CacheReadTokens: 2}); ok {
		t.Error("cache tokens above input accepted")
	}
	if p.EstimateInputPrice() != p.InputMicroPerMTok || (Pricing{InputMicroPerMTok: 3, CacheWriteMicroPerMTok: 4}).EstimateInputPrice() != 4 {
		t.Error("EstimateInputPrice")
	}
}
