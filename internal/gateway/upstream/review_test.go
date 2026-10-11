package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// Tests for the B2 review round 1.

// sseServer serves an OpenAI stream written by write.
func sseServer(t *testing.T, write func(w http.ResponseWriter, flush func())) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f, _ := w.(http.Flusher)
		write(w, func() { f.Flush() })
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func chunk(delta string) string {
	return `data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-1","choices":[{"index":0,"delta":` + delta +
		`,"finish_reason":null}]}` + "\n\n"
}

const streamTail = `data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
	`data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}` + "\n\n" +
	"data: [DONE]\n\n"

func streamAdapter(t *testing.T, base string, streamMax int64) ChatAdapter {
	t.Helper()
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	return NewChat(ChatConfig{BaseURL: base + "/v1", Model: "m-1", HTTP: d.HTTPClient(DefaultModelMaxBody, 0),
		StreamHTTP: d.HTTPClient(streamMax, 0)}).(ChatAdapter)
}

var openaiBody = []byte(`{"model":"m-1","messages":[{"role":"user","content":"hi"}],"max_tokens":50}`)

// DoStream returns at the first content, not at the first event: a role-only chunk with empty content is
// buffered (the call does not return while the provider stalls after it) and replayed by Next.
func TestDoStreamWaitsForFirstContent(t *testing.T) {
	base := sseServer(t, func(w http.ResponseWriter, flush func()) {
		_, _ = io.WriteString(w, chunk(`{"role":"assistant","content":""}`))
		flush()
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, chunk(`{"content":"hello"}`)+streamTail)
	})
	a := streamAdapter(t, base, 0)
	start := time.Now()
	s, e := a.DoStream(context.Background(), 0, openaiBody, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("DoStream returned after %s, before the first content", d)
	}
	ev, err := s.Next()
	if err != nil || ev.Type != llmproto.EventMessageStart {
		t.Fatalf("first replayed event %+v %v", ev, err)
	}
	resp, err := collect(t, s)
	if err != nil || len(resp.Blocks) != 1 || resp.Blocks[0].Text != "hello" {
		t.Fatalf("response %+v %v", resp, err)
	}
}

// Streams are not capped by the 8 MiB body limit of non-stream calls: a 10 MiB stream arrives whole. A read
// error after the first content (here a deliberately small stream cap) is io.ErrUnexpectedEOF with the cause.
func TestDoStreamLargerThanBodyCap(t *testing.T) {
	const n = 42_000 // ≈ 250 bytes per chunk → ≈ 10 MiB
	piece := strings.Repeat("x", 100)
	base := sseServer(t, func(w http.ResponseWriter, flush func()) {
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteString(chunk(`{"content":"` + piece + `"}`))
			if b.Len() > 1<<20 {
				_, _ = io.WriteString(w, b.String())
				b.Reset()
			}
		}
		b.WriteString(streamTail)
		_, _ = io.WriteString(w, b.String())
		flush()
	})
	s, e := streamAdapter(t, base, 0).DoStream(context.Background(), 0, openaiBody, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	resp, err := collect(t, s)
	_ = s.Close()
	if err != nil || len(resp.Blocks) != 1 || len(resp.Blocks[0].Text) != n*len(piece) {
		t.Fatalf("10 MiB stream: err=%v blocks=%d", err, len(resp.Blocks))
	}
	capped, e := streamAdapter(t, base, 1<<20).DoStream(context.Background(), 0, openaiBody, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	defer capped.Close()
	_, err = collect(t, capped)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrTruncated) {
		t.Fatalf("cut stream error %v, want io.ErrUnexpectedEOF wrapping the cause", err)
	}
}

// prompt_tokens_details of an unexpected shape is ignored, not a bad response.
func TestPromptTokensDetailsLenient(t *testing.T) {
	for _, details := range []string{`[]`, `{"cached_tokens":"12"}`, `null`, `"x"`, `{"cached_tokens":-3}`} {
		reply := `{"id":"c1","choices":[{"message":{"role":"assistant","content":"hi"}}],` +
			`"usage":{"prompt_tokens":30,"completion_tokens":2,"prompt_tokens_details":` + details + `}}`
		srv := newRouteServer(t, reply)
		a := streamAdapter(t, srv.URL, 0)
		resp, e := a.Do(context.Background(), mustResolve(t, a, `{"messages":[{"role":"user","content":"hi"}]}`))
		if e != nil || resp.Usage.InputTokens != 30 || resp.Usage.CacheReadTokens != 0 {
			t.Errorf("%s: %v %+v", details, e, resp.Usage)
		}
	}
	srv := newRouteServer(t, `{"choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":30,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":12}}}`)
	a := streamAdapter(t, srv.URL, 0)
	resp, e := a.Do(context.Background(), mustResolve(t, a, `{"messages":[{"role":"user","content":"hi"}]}`))
	if e != nil || resp.Usage.CacheReadTokens != 12 {
		t.Errorf("well-formed details: %v %+v", e, resp.Usage)
	}
}

var anthropicIDRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// A chat-agent transcript with OpenAI-compatible tool ids (Kimi's functions.web_search:0, the worker's c1#1)
// reaches the Anthropic provider with valid ids, each tool_use still paired with its tool_result
// (fakeupstream answers 400 to invalid ids, like the real API).
func TestAnthropicRouteToolIDs(t *testing.T) {
	p, q := fakePair(t)
	a := mixedAdapter(t, p, q)
	resolved := mustResolve(t, a, `{"model":"m-1","messages":[{"role":"system","content":"agent"},{"role":"user","content":"find"},`+
		`{"role":"assistant","content":"","tool_calls":[{"id":"functions.web_search:0","type":"function","function":{"name":"web_search","arguments":"{\"q\":\"a\"}"}},`+
		`{"id":"c1#1","type":"function","function":{"name":"read","arguments":"{}"}}]},`+
		`{"role":"tool","tool_call_id":"functions.web_search:0","content":"r1"},{"role":"tool","tool_call_id":"c1#1","content":"r2"}]}`)
	if _, e := a.DoOn(context.Background(), 1, resolved); e != nil {
		t.Fatalf("Anthropic route refused the transcript: %v", e)
	}
	var sent struct {
		Messages []struct {
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	unmarshal(t, []byte(q.Requests(fakeupstream.Messages)[0].Body), &sent)
	var uses, results []string
	for _, m := range sent.Messages {
		var blocks []struct {
			Type      string `json:"type"`
			ID        string `json:"id"`
			ToolUseID string `json:"tool_use_id"`
		}
		if json.Unmarshal(m.Content, &blocks) != nil {
			continue
		}
		for _, b := range blocks {
			switch b.Type {
			case "tool_use":
				uses = append(uses, b.ID)
			case "tool_result":
				results = append(results, b.ToolUseID)
			}
		}
	}
	if len(uses) != 2 || fmt.Sprint(uses) != fmt.Sprint(results) || uses[0] == uses[1] {
		t.Fatalf("tool ids uses=%v results=%v", uses, results)
	}
	for _, id := range uses {
		if !anthropicIDRE.MatchString(id) {
			t.Fatalf("invalid id sent: %q", id)
		}
	}
}

// The estimate never under-reserves when the actual usage stays within the estimate's assumptions (input ≤
// input_estimate, output ≤ max_tokens), whatever the cache prices and the split of the input into plain,
// cache-read and cache-write tokens.
func TestEstimateCoversActualCost(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	price := func() int64 {
		if r.IntN(3) == 0 {
			return 0
		}
		return r.Int64N(20_000_000)
	}
	for n := 0; n < 2000; n++ {
		p := Pricing{InputMicroPerMTok: r.Int64N(20_000_000) + 1, OutputMicroPerMTok: r.Int64N(80_000_000),
			CacheReadMicroPerMTok: price(), CacheWriteMicroPerMTok: price()}
		a := NewChat(ChatConfig{BaseURL: "http://127.0.0.1:1/v1", Model: "m-1", Pricing: p})
		body := fmt.Sprintf(`{"messages":[{"role":"user","content":%q}],"max_tokens":%d}`, strings.Repeat("y", r.IntN(5000)), 1+r.IntN(4000))
		resolved := mustResolve(t, a, body)
		est, err := a.Estimate(resolved)
		if err != nil {
			t.Fatal(err)
		}
		inEst, maxTok, _, _ := chatInputs(resolved)
		in := r.Int64N(inEst + 1)
		cr := r.Int64N(in + 1)
		cw := r.Int64N(in - cr + 1)
		u := Usage{InputTokens: in, CacheReadTokens: cr, CacheWriteTokens: cw, OutputTokens: r.Int64N(maxTok + 1)}
		cost, ok := p.ChatCost(u)
		if !ok || cost > est {
			t.Fatalf("price %+v usage %+v: cost %d > estimate %d", p, u, cost, est)
		}
	}
}
