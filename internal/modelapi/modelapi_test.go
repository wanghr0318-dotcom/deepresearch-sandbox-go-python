package modelapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

var protos = []llmproto.Protocol{llmproto.ProtocolOpenAI, llmproto.ProtocolAnthropic}

// All four client → provider paths, stream and non-stream: the reply arrives in the client's format, the hold is
// settled with the actual cost, and the request is recorded with its route, conversion, usage and completeness.
func TestFourPaths(t *testing.T) {
	for _, m := range []mode{modeOpenAI, modeAnthropic} {
		e := newEnv(t, m)
		provider, providerProto := "primary", llmproto.ProtocolOpenAI
		if m == modeAnthropic {
			provider, providerProto = "claude", llmproto.ProtocolAnthropic
		}
		for _, client := range protos {
			for _, stream := range []bool{false, true} {
				name := string(client) + "→" + string(providerProto)
				if stream {
					name += "/stream"
				}
				t.Run(name, func(t *testing.T) {
					resp := e.post(context.Background(), client, chatBody(client, "m-1", stream, ""))
					if resp.StatusCode != http.StatusOK || resp.Header.Get("x-request-id") == "" {
						t.Fatalf("status %d %s", resp.StatusCode, readAll(t, resp))
					}
					var got llmproto.Response
					if stream {
						if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
							t.Fatalf("content type %s", resp.Header.Get("Content-Type"))
						}
						var col *llmproto.Collector
						var err error
						got, col, err = streamEvents(t, resp, client)
						if err != nil || !col.Complete() {
							t.Fatalf("stream: %v complete=%v", err, col.Complete())
						}
					} else {
						var err error
						if got, err = llmproto.DecodeResponse(client, readAll(t, resp)); err != nil {
							t.Fatal(err)
						}
						if client == llmproto.ProtocolOpenAI && got.Created == 0 {
							t.Fatal("created not set")
						}
					}
					if len(got.Blocks) != 1 || !strings.HasPrefix(got.Blocks[0].Text, "fake 回复") || got.StopReason != llmproto.StopEnd {
						t.Fatalf("reply %+v", got)
					}
					r := e.record()
					if r.Status != 200 || r.ErrorCode != "" || r.Provider != provider || r.ProviderProtocol != providerProto ||
						r.Converted != (client != providerProto) || r.ClientProtocol != client || r.Stream != stream ||
						!r.UsageKnown || r.Usage.OutputTokens == 0 || r.CostMicro <= 0 || r.TTFB == nil || r.KeyID != "k_test" {
						t.Fatalf("record %+v", r)
					}
					if stream != (r.StreamComplete != nil) || (stream && !*r.StreamComplete) {
						t.Fatalf("stream_complete %v", r.StreamComplete)
					}
					h := e.holder.get(r.HoldID)
					if h.state != quota.StateSettled || h.charged != r.CostMicro || h.ref != r.ID || h.charged > h.amount {
						t.Fatalf("hold %+v, record cost %d", h, r.CostMicro)
					}
					if last := r.Tries[len(r.Tries)-1]; last.Outcome != "ok" || last.Provider != provider || last.CostMicro != r.CostMicro {
						t.Fatalf("tries %+v", r.Tries)
					}
				})
			}
		}
	}
}

// Same-format streams are forwarded as the provider sent them: the OpenAI usage chunk the gateway asked for is
// left out unless the client asked for it; Anthropic pings are kept.
func TestPassThroughStream(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	b := string(readAll(t, e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))))
	if strings.Contains(b, `"choices":[]`) || !strings.Contains(b, "data: [DONE]") {
		t.Fatalf("stream without include_usage:\n%s", b)
	}
	e.record()
	b = string(readAll(t, e.post(context.Background(), llmproto.ProtocolOpenAI,
		chatBody(llmproto.ProtocolOpenAI, "m-1", true, `"stream_options":{"include_usage":true},`))))
	if !strings.Contains(b, `"choices":[]`) || !strings.Contains(b, `"usage"`) {
		t.Fatalf("stream with include_usage:\n%s", b)
	}
	e.record()
	a := newEnv(t, modeAnthropic)
	b = string(readAll(t, a.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-1", true, ""))))
	if !strings.Contains(b, "event: ping") || !strings.Contains(b, `"model":"claude-x"`) {
		t.Fatalf("anthropic pass-through:\n%s", b)
	}
	a.record()
}

const openAITools = `"tools":[{"type":"function","function":{"name":"get_weather","description":"Weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}],`
const anthropicTools = `"tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}],`

// Tool calls on all four paths, stream and non-stream: the client receives the call in its own format with the full
// arguments.
func TestToolCalls(t *testing.T) {
	for _, m := range []mode{modeOpenAI, modeAnthropic} {
		e := newEnv(t, m)
		e.p.SetChatHook(func([]byte) json.RawMessage {
			return json.RawMessage(`{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]}`)
		})
		e.q.SetMessagesHook(func([]byte) json.RawMessage {
			return json.RawMessage(`[{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}]`)
		})
		for _, client := range protos {
			tools := openAITools
			if client == llmproto.ProtocolAnthropic {
				tools = anthropicTools
			}
			for _, stream := range []bool{false, true} {
				resp := e.post(context.Background(), client, chatBody(client, "m-1", stream, tools))
				if resp.StatusCode != 200 {
					t.Fatalf("mode %d %s stream=%v: %d %s", m, client, stream, resp.StatusCode, readAll(t, resp))
				}
				var got llmproto.Response
				if stream {
					got, _, _ = streamEvents(t, resp, client)
				} else {
					var err error
					if got, err = llmproto.DecodeResponse(client, readAll(t, resp)); err != nil {
						t.Fatal(err)
					}
				}
				var call *llmproto.Block
				for i := range got.Blocks {
					if got.Blocks[i].Type == llmproto.BlockToolCall {
						call = &got.Blocks[i]
					}
				}
				if call == nil || call.Name != "get_weather" || got.StopReason != llmproto.StopToolUse {
					t.Fatalf("mode %d %s stream=%v: %+v", m, client, stream, got)
				}
				var args map[string]string
				if json.Unmarshal([]byte(call.Arguments), &args) != nil || args["city"] != "Paris" {
					t.Fatalf("arguments %q", call.Arguments)
				}
				e.record()
			}
		}
	}
}

// A retryable failure before the first byte fails over: an HTTP 503 from the primary, and an overloaded error event
// that an Anthropic provider sends before any content. Each try is recorded.
func TestFailoverBeforeFirstByte(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	e.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Status: 503})
	for _, stream := range []bool{false, true} {
		e.p.Inject(fakeupstream.Chat, e.p.Count(fakeupstream.Chat)+1, fakeupstream.Action{Status: 503})
		resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", stream, ""))
		if b := readAll(t, resp); resp.StatusCode != 200 || !strings.Contains(string(b), "fake 回复") {
			t.Fatalf("stream=%v: %d %s", stream, resp.StatusCode, b)
		}
		r := e.record()
		if len(r.Tries) != 2 || r.Tries[0].Provider != "primary" || r.Tries[0].Outcome != "retryable" || r.Tries[0].Status != 503 ||
			r.Tries[1].Provider != "claude" || r.Tries[1].Outcome != "ok" || r.Provider != "claude" || !r.Converted {
			t.Fatalf("tries %+v", r.Tries)
		}
		if h := e.holder.get(r.HoldID); h.state != quota.StateSettled {
			t.Fatalf("hold %+v", h)
		}
	}

	three := newEnv(t, modeThree)
	three.q.Inject(fakeupstream.Messages, 1, fakeupstream.Action{FailAfterChunks: 1, FailWithError: true}) // message_start, then overloaded_error
	resp := three.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-1", true, ""))
	got, col, err := streamEvents(t, resp, llmproto.ProtocolAnthropic)
	if err != nil || !col.Complete() || !strings.HasPrefix(got.Blocks[0].Text, "fake 回复") {
		t.Fatalf("%v %+v", err, got)
	}
	r := three.record()
	if len(r.Tries) != 3 || r.Tries[1].Provider != "claude" || r.Tries[1].Outcome != "retryable" || r.Tries[1].Status != 529 ||
		r.Tries[2].Provider != "backup" || r.Tries[2].Outcome != "ok" || !*r.StreamComplete {
		t.Fatalf("tries %+v", r.Tries)
	}
}

// A provider failure after the first byte cannot fail over: the client gets an error event in its format, the
// request is recorded with stream_complete=false, and the hold is charged as unknown (no final usage).
func TestMidStreamFailure(t *testing.T) {
	for _, client := range protos {
		for _, withError := range []bool{false, true} {
			e := newEnv(t, modeOpenAI)
			e.p.SetChatHook(func([]byte) json.RawMessage {
				return json.RawMessage(`{"role":"assistant","content":"a long enough reply to be split into several chunks"}`)
			})
			e.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{FailAfterChunks: 3, FailWithError: withError})
			resp := e.post(context.Background(), client, chatBody(client, "m-1", true, ""))
			b := string(readAll(t, resp))
			if resp.StatusCode != 200 {
				t.Fatalf("%d %s", resp.StatusCode, b)
			}
			want := `"error"`
			if client == llmproto.ProtocolAnthropic {
				want = "event: error"
			}
			if !strings.Contains(b, want) || strings.Contains(b, "[DONE]") || strings.Contains(b, "message_stop") {
				t.Fatalf("%s error=%v:\n%s", client, withError, b)
			}
			r := e.record()
			if r.StreamComplete == nil || *r.StreamComplete || r.ErrorCode != codeStreamError || r.UsageKnown || len(r.Tries) != 1 {
				t.Fatalf("record %+v", r)
			}
			h := e.holder.get(r.HoldID)
			if h.state != quota.StateChargedUnknown || h.charged < h.amount || r.CostMicro != h.charged {
				t.Fatalf("hold %+v record cost %d", h, r.CostMicro)
			}
		}
	}
}

// A non-stream request whose reply is cut short is failed over (nothing was written to the client yet); the cut try
// counts as unknown in the settlement.
func TestNonStreamCutFailsOver(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	e.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{FailAfterChunks: 2})
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	r := e.record()
	if len(r.Tries) != 2 || r.Tries[0].Outcome != "unknown" || r.Tries[1].Outcome != "ok" || r.Provider != "claude" {
		t.Fatalf("tries %+v", r.Tries)
	}
	if h := e.holder.get(r.HoldID); h.state != quota.StateChargedUnknown || h.charged < h.amount {
		t.Fatalf("hold %+v", h)
	}
}

// R7: an OpenAI stream cut after finish_reason but before its usage chunk is complete, but without usage: it is
// settled as charged_unknown with the conservative estimate.
func TestCompleteWithoutUsageIsUnknown(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	// role, text chunks, finish chunk; the usage chunk would be next: cut before it.
	e.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{FailAfterChunks: 3})
	e.p.SetChatHook(func([]byte) json.RawMessage { return json.RawMessage(`{"role":"assistant","content":"short ok"}`) })
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	readAll(t, resp)
	r := e.record()
	if r.UsageKnown || r.StreamComplete == nil || !*r.StreamComplete || r.ErrorCode != "" {
		t.Fatalf("complete without usage: %+v", r)
	}
	if h := e.holder.get(r.HoldID); h.state != quota.StateChargedUnknown || h.charged < h.amount {
		t.Fatalf("hold %+v", h)
	}
}

// A client that disconnects mid-stream cancels the upstream request; the request is settled (usage never arrived:
// unknown) and recorded as client_disconnected.
func TestClientDisconnect(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	e.p.SetChunkDelay(fakeupstream.Chat, 300*time.Millisecond)
	e.p.SetChatHook(func([]byte) json.RawMessage {
		return json.RawMessage(`{"role":"assistant","content":"a reply long enough for several delayed chunks"}`)
	})
	ctx, cancel := context.WithCancel(context.Background())
	resp := e.post(ctx, llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = resp.Body.Close()
	r := e.record()
	if r.ErrorCode != codeClientDisconnected || r.StreamComplete == nil || *r.StreamComplete {
		t.Fatalf("record %+v", r)
	}
	if h := e.holder.get(r.HoldID); h.state != quota.StateChargedUnknown {
		t.Fatalf("hold %+v", h)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		reqs := e.p.Requests(fakeupstream.Chat)
		if o := reqs[len(reqs)-1].Outcome; o != "" && o != fakeupstream.OutcomeOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("upstream request not cancelled: %+v", reqs[len(reqs)-1])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// An exhausted budget is 429 insufficient_quota (Anthropic: rate_limit_error) naming the level; nothing is sent
// upstream or recorded.
func TestQuotaExhausted(t *testing.T) {
	_, rec := obstest.Install(t)
	e := newEnv(t, modeOpenAI)
	e.holder.reserve = &quota.InsufficientError{Level: quota.LevelUser, ID: "1/2", Limit: 10, Available: 0, Amount: 50}
	for _, client := range protos {
		resp := e.post(context.Background(), client, chatBody(client, "m-1", false, ""))
		code, msg := errorOf(t, client, readAll(t, resp))
		want := "insufficient_quota"
		if client == llmproto.ProtocolAnthropic {
			want = "rate_limit_error"
		}
		if resp.StatusCode != 429 || code != want || !strings.Contains(msg, "user level") {
			t.Fatalf("%s: %d %s %s", client, resp.StatusCode, code, msg)
		}
	}
	if e.p.Count(fakeupstream.Chat)+e.q.Count(fakeupstream.Messages) != 0 {
		t.Fatal("a provider was called")
	}
	e.noRecord()
	if !strings.Contains(strings.Join(rec.Events(), "\n"), "ModelRejected protocol=anthropic reason=quota") {
		t.Fatalf("metrics %v", rec.Events())
	}
}

// Rate limits: rpm, tpm and max_streams at the caller's levels; 429 rate_limit_exceeded with Retry-After and the
// x-ratelimit headers, in the client's format.
func TestRateLimits(t *testing.T) {
	one := int64(1)
	e := newEnv(t, modeOpenAI)
	e.limits.ls = map[quota.Level]quota.Limits{quota.LevelKey: {RPM: &one}}
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	readAll(t, resp)
	if resp.StatusCode != 200 || resp.Header.Get("x-ratelimit-limit-requests") != "1" || resp.Header.Get("x-ratelimit-remaining-requests") != "0" ||
		resp.Header.Get("x-ratelimit-reset-requests") == "" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	e.record()
	resp = e.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-1", false, ""))
	code, _ := errorOf(t, llmproto.ProtocolAnthropic, readAll(t, resp))
	if resp.StatusCode != 429 || code != "rate_limit_error" || resp.Header.Get("Retry-After") == "" ||
		resp.Header.Get("x-ratelimit-limit-requests") != "1" {
		t.Fatalf("%d %s %v", resp.StatusCode, code, resp.Header)
	}
	e.noRecord()

	tpm := int64(50) // below the estimate of any request (max_tokens 100)
	e2 := newEnv(t, modeOpenAI)
	e2.limits.ls = map[quota.Level]quota.Limits{quota.LevelOrg: {TPM: &tpm}}
	resp = e2.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, `"max_tokens":100,`))
	code, msg := errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp))
	if resp.StatusCode != 429 || code != "rate_limit_exceeded" || !strings.Contains(msg, "tokens-per-minute") ||
		resp.Header.Get("x-ratelimit-limit-tokens") != "50" {
		t.Fatalf("%d %s %s %v", resp.StatusCode, code, msg, resp.Header)
	}

	e3 := newEnv(t, modeOpenAI)
	e3.limits.ls = map[quota.Level]quota.Limits{quota.LevelUser: {MaxStreams: &one}}
	e3.p.SetChunkDelay(fakeupstream.Chat, 200*time.Millisecond)
	first := e3.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	resp = e3.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	code, msg = errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp))
	if resp.StatusCode != 429 || code != "rate_limit_exceeded" || !strings.Contains(msg, "concurrent streams") {
		t.Fatalf("%d %s %s", resp.StatusCode, code, msg)
	}
	readAll(t, first)
	e3.record()
	resp = e3.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", true, ""))
	if readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("slot not released: %d", resp.StatusCode)
	}
	e3.record()
}

// A request that uses a feature only its own format has is routed only to providers of that format (§7.4); if none
// serves the model, 400 unsupported_field. Things no provider can serve are refused up front.
func TestFormatOnlyFeatures(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	// OpenAI-only seed: the primary fails, and the Anthropic route is not an alternative.
	e.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Status: 503})
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, `"seed":7,`))
	code, _ := errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp))
	if resp.StatusCode != 502 || code != "upstream_error" || e.q.Count(fakeupstream.Messages) != 0 {
		t.Fatalf("%d %s, anthropic calls %d", resp.StatusCode, code, e.q.Count(fakeupstream.Messages))
	}
	r := e.record()
	if len(r.Tries) != 1 || r.Tries[0].Provider != "primary" {
		t.Fatalf("tries %+v", r.Tries)
	}
	// Anthropic-only thinking: goes straight to the Anthropic route (the OpenAI primary is skipped).
	thinking := `"thinking":{"type":"enabled","budget_tokens":1024},`
	resp = e.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-1", false, thinking))
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	if r = e.record(); r.Provider != "claude" || r.Converted || len(r.Tries) != 1 || !strings.Contains(r.Tries[0].Skipped, "primary:") {
		t.Fatalf("record %+v", r)
	}
	// No Anthropic route serves m-2: 400 unsupported_field naming the field.
	resp = e.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-2", false, thinking))
	code, msg := errorOf(t, llmproto.ProtocolAnthropic, readAll(t, resp))
	if resp.StatusCode != 400 || code != "invalid_request_error" || !strings.Contains(msg, "thinking") {
		t.Fatalf("%d %s %s", resp.StatusCode, code, msg)
	}
	// Refused whatever the provider, and unknown models.
	for _, c := range []struct {
		proto      llmproto.Protocol
		body, code string
		status     int
	}{
		{llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, `"n":2,`), "unsupported_field", 400},
		{llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, `"logprobs":true,`), "unsupported_field", 400},
		{llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "nope", false, ""), "model_not_found", 404},
		{llmproto.ProtocolOpenAI, `{"model":"m-1"}`, "invalid_request_error", 400},
		{llmproto.ProtocolAnthropic, `not json`, "invalid_request_error", 400},
	} {
		resp := e.post(context.Background(), c.proto, c.body)
		code, msg := errorOf(t, c.proto, readAll(t, resp))
		if resp.StatusCode != c.status || code != c.code {
			t.Fatalf("%s: %d %s %s", c.body, resp.StatusCode, code, msg)
		}
	}
	e.noRecord()
}

// --model-api-max-duration bounds a whole request: a stream past it ends with an error event (max_duration), and a
// provider that never answers is cut before the first token.
func TestMaxDuration(t *testing.T) {
	e := newEnv(t, modeOpenAI, func(c *Config) { c.MaxDuration = 700 * time.Millisecond })
	e.p.SetChunkDelay(fakeupstream.Chat, 250*time.Millisecond)
	e.p.SetChatHook(func([]byte) json.RawMessage {
		return json.RawMessage(`{"role":"assistant","content":"a reply long enough for several delayed chunks"}`)
	})
	resp := e.post(context.Background(), llmproto.ProtocolAnthropic, chatBody(llmproto.ProtocolAnthropic, "m-1", true, ""))
	b := string(readAll(t, resp))
	if !strings.Contains(b, "event: error") || !strings.Contains(b, "maximum duration") {
		t.Fatalf("%s", b)
	}
	r := e.record()
	if r.ErrorCode != codeMaxDuration || *r.StreamComplete {
		t.Fatalf("record %+v", r)
	}
	// A non-stream reply still being collected at the limit: an error in the client's format, no failover.
	resp = e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	code, msg := errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp))
	if resp.StatusCode != 502 || code != "upstream_error" || !strings.Contains(msg, "maximum duration") {
		t.Fatalf("%d %s %s", resp.StatusCode, code, msg)
	}
	if r = e.record(); r.Status != 502 || r.ErrorCode != codeMaxDuration || len(r.Tries) != 1 || r.Tries[0].Outcome != "unknown" ||
		e.holder.get(r.HoldID).state != quota.StateChargedUnknown {
		t.Fatalf("record %+v", r)
	}

	e2 := newEnv(t, modeOpenAI, func(c *Config) { c.MaxDuration = 300 * time.Millisecond })
	e2.p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	resp = e2.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	code, msg = errorOf(t, llmproto.ProtocolOpenAI, readAll(t, resp))
	if resp.StatusCode != 502 || code != "upstream_error" || !strings.Contains(msg, "maximum duration") {
		t.Fatalf("%d %s %s", resp.StatusCode, code, msg)
	}
	if r := e2.record(); r.ErrorCode != codeMaxDuration || e2.holder.get(r.HoldID).state != quota.StateChargedUnknown {
		t.Fatalf("record %+v", r)
	}
}

// PostgreSQL unavailable: no model call without a hold (503, fail closed), and limits that cannot be read are also 503.
func TestStoreUnavailableFailsClosed(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	e.holder.reserve = persistence.ErrUnavailable
	for _, client := range protos {
		resp := e.post(context.Background(), client, chatBody(client, "m-1", true, ""))
		code, _ := errorOf(t, client, readAll(t, resp))
		want := "service_unavailable"
		if client == llmproto.ProtocolAnthropic {
			want = "api_error"
		}
		if resp.StatusCode != 503 || code != want {
			t.Fatalf("%s: %d %s", client, resp.StatusCode, code)
		}
	}
	e.holder.reserve = nil
	e.limits.err = errors.New("db down")
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	if readAll(t, resp); resp.StatusCode != 503 {
		t.Fatalf("limits down: %d", resp.StatusCode)
	}
	if e.p.Count(fakeupstream.Chat) != 0 {
		t.Fatal("a provider was called without a hold")
	}
	e.noRecord()
	// A settlement that keeps failing leaves the hold for the sweep and writes no row (invariant G3).
	e.limits.err = nil
	e.holder.settle = persistence.ErrUnavailable
	resp = e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	if readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}
	e.noRecord()
}

// Operators are not rate limited (as on the runtime API).
func TestOperatorNotRateLimited(t *testing.T) {
	zero := int64(0)
	e := newEnv(t, modeOpenAI)
	e.limits.ls = map[quota.Level]quota.Limits{quota.LevelOrg: {RPM: &zero}}
	e.princ.OrgID, e.princ.UserID, e.princ.KeyID, e.princ.Operator = 0, 0, "", true
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	if readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}
	if r := e.record(); r.OrgID != 0 || r.KeyID != "" {
		t.Fatalf("record %+v", r)
	}
}

// Converting drops the fields only the source format has; they are recorded and counted.
func TestDroppedFields(t *testing.T) {
	_, rec := obstest.Install(t)
	e := newEnv(t, modeOpenAI)
	body := `{"model":"m-1","max_tokens":50,"metadata":{"user_id":"u1"},"system":[{"type":"text","text":"be brief","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}`
	resp := e.post(context.Background(), llmproto.ProtocolAnthropic, body)
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	r := e.record()
	if !r.Converted || len(r.Dropped) != 2 {
		t.Fatalf("dropped %v", r.Dropped)
	}
	lines := strings.Join(rec.Events(), "\n")
	if !strings.Contains(lines, "ModelDropped field=cache_control") || !strings.Contains(lines, "ModelDropped field=user_id") ||
		!strings.Contains(lines, "ModelRequest protocol=anthropic model=m-1 provider=primary status=200") {
		t.Fatalf("metrics:\n%s", lines)
	}
}

// GET /v1/models lists the served logical models, in OpenAI format or (with anthropic-version) Anthropic format.
func TestModels(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	for _, client := range protos {
		req, _ := http.NewRequest(http.MethodGet, e.srv.URL+PathModels, nil)
		if client == llmproto.ProtocolAnthropic {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Object string `json:"object"`
			Data   []struct {
				ID, Type, Object string
			} `json:"data"`
			HasMore *bool `json:"has_more"`
		}
		if err := json.Unmarshal(readAll(t, resp), &v); err != nil || len(v.Data) != 2 || v.Data[0].ID != "m-1" {
			t.Fatalf("%s: %+v %v", client, v, err)
		}
		if (client == llmproto.ProtocolAnthropic) != (v.HasMore != nil && v.Data[0].Type == "model") {
			t.Fatalf("%s shape %+v", client, v)
		}
	}
}
