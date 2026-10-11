package llmproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// Tests for the review findings of PR #43 (F1–F12); fixtures cover the rest (see testdata).

// F2: max_completion_tokens maps to MaxTokens (the smaller of the two when both are given) and keeps its
// name in a same-format encoding.
func TestMaxCompletionTokens(t *testing.T) {
	req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":300}`))
	if err != nil || req.MaxTokens != 300 || len(req.Features) != 0 {
		t.Fatalf("decode: %v %d %v", err, req.MaxTokens, req.Features)
	}
	req.MaxTokens = 100 // e.g. capped by the gateway
	body, err := EncodeOpenAIRequest(req)
	if err != nil || !bytes.Contains(body, []byte(`"max_completion_tokens":100`)) || bytes.Contains(body, []byte(`"max_tokens"`)) {
		t.Fatalf("same-format body: %v %s", err, body)
	}
	if body, _ := EncodeAnthropicRequest(req); !bytes.Contains(body, []byte(`"max_tokens":100`)) {
		t.Fatalf("anthropic body: %s", body)
	}
	for _, c := range []string{`"max_tokens":50,"max_completion_tokens":80`, `"max_tokens":80,"max_completion_tokens":50`} {
		req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],` + c + `}`))
		if err != nil || req.MaxTokens != 50 {
			t.Fatalf("%s: %v %d", c, err, req.MaxTokens)
		}
	}
}

// F2: assistant fields echoed by the openai SDK are accepted; null or empty ones are ignored, others pin the
// request to OpenAI and are re-emitted there.
func TestEchoedAssistantFields(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"q"},{"role":"assistant","content":"a","refusal":null,"annotations":[],` +
		`"audio":null,"function_call":null},{"role":"user","content":"q2"}],"max_tokens":5}`
	req, err := DecodeOpenAIRequest([]byte(body))
	if err != nil || len(req.Features) != 0 || req.Messages[1].Extras != nil {
		t.Fatalf("null/empty echoed fields: %v %v", err, req.Features)
	}
	if _, err := EncodeAnthropicRequest(req); err != nil {
		t.Fatal(err)
	}
	body = strings.Replace(body, `"refusal":null`, `"refusal":"I can't"`, 1)
	req, err = DecodeOpenAIRequest([]byte(body))
	if err != nil || !slices.Equal(req.FormatOnly(), []string{"messages[1].refusal"}) {
		t.Fatalf("refusal: %v %v", err, req.FormatOnly())
	}
	out, err := EncodeOpenAIRequest(req)
	if err != nil || !bytes.Contains(out, []byte(`"refusal":"I can't"`)) {
		t.Fatalf("pass-through: %v %s", err, out)
	}
	// stream_options.include_obfuscation is accepted and dropped.
	req, err = DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"stream":true,"stream_options":{"include_usage":true,"include_obfuscation":false}}`))
	if err != nil || !req.IncludeUsage || len(req.Features) != 1 || req.Features[0].Kind != FeatureDrop {
		t.Fatalf("include_obfuscation: %v %+v", err, req.Features)
	}
}

// F3: tool ids are mapped to Anthropic's pattern deterministically and stay distinct.
func TestAnthropicToolID(t *testing.T) {
	valid := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	for _, id := range []string{"toolu_01A-b", "call_abc123"} {
		if got := AnthropicToolID(id); got != id {
			t.Errorf("valid id %q changed to %q", id, got)
		}
	}
	seen := map[string]string{}
	for _, id := range []string{"functions.web_search:0", "functions.web_search:1", "functions_web_search_0", "a.b", "a:b", "工具 1"} {
		got := AnthropicToolID(id)
		if !valid.MatchString(got) || got != AnthropicToolID(id) {
			t.Errorf("%q -> %q", id, got)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%q and %q both map to %q", prev, id, got)
		}
		seen[got] = id
	}
}

// F5: provider response failures are upstream errors (502), not client errors.
func TestBadResponseIsUpstream(t *testing.T) {
	_, err1 := DecodeOpenAIResponse([]byte(`{"choices":[]}`))
	_, err2 := DecodeAnthropicResponse([]byte(`{"type":"error"}`))
	_, err3 := readAll(t, "data: {not json\n\n", ProtocolOpenAI)
	for i, err := range []error{err1, err2, err3} {
		if ClassOf(err) != ClassUpstream || ErrorFrom(err).Class.Status() != 502 {
			t.Errorf("#%d: %v -> %s", i, err, ClassOf(err))
		}
	}
	// Truncated tool arguments: kept verbatim, listed in Repaired, sent as {} to Anthropic clients.
	body := `{"id":"c","model":"m","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[` +
		`{"id":"t","function":{"name":"f","arguments":"{\"q\":\"ab"}}]},"finish_reason":"length"}]}`
	resp, err := DecodeOpenAIResponse([]byte(body))
	if err != nil || !slices.Equal(resp.Repaired, []string{"choices[0].message.tool_calls[0].function.arguments"}) ||
		resp.Blocks[0].Arguments != `{"q":"ab` {
		t.Fatalf("decode: %v %+v", err, resp)
	}
	out, err := EncodeAnthropicResponse(resp)
	if err != nil || !bytes.Contains(out, []byte(`"input":{}`)) {
		t.Fatalf("encode: %v %s", err, out)
	}
	// The same through a stream: the Collector lists it.
	var col Collector
	for _, ev := range ResponseEvents(resp) {
		col.Add(ev)
	}
	if r := col.Response(); !slices.Equal(r.Repaired, []string{"blocks[0].arguments"}) {
		t.Fatalf("collector repaired = %v", r.Repaired)
	}
}

// F6: a stream cut inside a line (the usual network cut) is io.ErrUnexpectedEOF in both formats; an OpenAI
// stream cut after its finish chunk is still complete.
func TestCutMidLine(t *testing.T) {
	cases := []struct {
		name string
		in   string
		p    Protocol
		err  error
	}{
		{"openai mid chunk", "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"id\":\"c\",\"cho", ProtocolOpenAI, io.ErrUnexpectedEOF},
		{"openai no blank line", "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n", ProtocolOpenAI, io.ErrUnexpectedEOF},
		{"openai cut in [DONE]", "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DO", ProtocolOpenAI, nil},
		{"anthropic mid event", "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\"}}\n\nevent: content_block_start\ndata: {\"type\":\"content_bl", ProtocolAnthropic, io.ErrUnexpectedEOF},
	}
	for _, c := range cases {
		_, err := readAll(t, c.in, c.p)
		if (c.err == nil && err != nil) || (c.err != nil && !errors.Is(err, c.err)) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.err)
		}
	}
}

// F8: collecting is linear: 100k small deltas take well under a second.
func TestCollectorManyDeltas(t *testing.T) {
	start := time.Now()
	var c Collector
	c.Add(Event{Type: EventBlockStart, Index: 0, Block: BlockText})
	for range 100_000 {
		c.Add(Event{Type: EventBlockDelta, Index: 0, Text: "abcd"})
	}
	if got := len(c.Response().Blocks[0].Text); got != 400_000 {
		t.Fatalf("text length %d", got)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("100k deltas took %v", d)
	}
}

func BenchmarkCollector(b *testing.B) {
	for range b.N {
		var c Collector
		c.Add(Event{Type: EventBlockStart, Index: 0, Block: BlockText})
		for range 10_000 {
			c.Add(Event{Type: EventBlockDelta, Index: 0, Text: "abcd"})
		}
		_ = c.Response()
	}
}

// F9: negative usage is clamped to 0 and sums saturate.
func TestUsageClampAndSaturate(t *testing.T) {
	r, err := DecodeOpenAIResponse([]byte(`{"choices":[{"message":{"content":"x"}}],"usage":{"prompt_tokens":-100,"completion_tokens":-1,"prompt_tokens_details":{"cached_tokens":-5}}}`))
	if err != nil || r.Usage != (Usage{}) {
		t.Fatalf("openai: %v %+v", err, r.Usage)
	}
	r, err = DecodeAnthropicResponse([]byte(`{"type":"message","content":[],"usage":{"input_tokens":-3,"output_tokens":4,"cache_read_input_tokens":-9}}`))
	if err != nil || r.Usage != (Usage{OutputTokens: 4}) {
		t.Fatalf("anthropic: %v %+v", err, r.Usage)
	}
	evs, err := readAll(t, "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":-7}}}\n\ndata: {\"type\":\"message_stop\"}\n\n", ProtocolAnthropic)
	if err != nil || evs[0].Usage.InputTokens != 0 {
		t.Fatalf("stream: %v %+v", err, evs)
	}
	u := Usage{InputTokens: math.MaxInt64, CacheReadTokens: 10, OutputTokens: 5}
	if u.PromptTotal() != math.MaxInt64 || u.Total() != math.MaxInt64 {
		t.Fatalf("saturation: %d %d", u.PromptTotal(), u.Total())
	}
	if MaxTokensLimit != 1<<20 {
		t.Fatal("MaxTokensLimit")
	}
}

// F10: cache_control inside a tool result's content is reported as dropped.
func TestToolResultInnerCacheControl(t *testing.T) {
	req, err := DecodeAnthropicRequest([]byte(`{"max_tokens":5,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t",` +
		`"content":[{"type":"text","text":"a"},{"type":"text","text":"b","cache_control":{"type":"ephemeral"}}]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Dropped(ProtocolOpenAI); !slices.Equal(got, []string{"messages[0].content[0].content[1].cache_control"}) {
		t.Fatalf("dropped = %v", got)
	}
}

// F11: a request with only system messages decodes but stays on OpenAI providers.
func TestSystemOnly(t *testing.T) {
	req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"system","content":"s"}]}`))
	if err != nil || len(req.Messages) != 0 || !slices.Equal(req.FormatOnly(), []string{"messages"}) {
		t.Fatalf("%v %v", err, req.FormatOnly())
	}
	if out, err := EncodeOpenAIRequest(req); err != nil || !bytes.Contains(out, []byte(`"role":"system"`)) {
		t.Fatalf("pass-through: %v %s", err, out)
	}
}

// F12: after a bare CR the reader does not wait for the next byte.
func TestSSEBareCRDoesNotBlock(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	go func() { _, _ = pw.Write([]byte("data: x\r\r")) }()
	done := make(chan error, 1)
	go func() {
		ev, err := NewSSEReader(pr, 0).Next()
		if err == nil && ev.Data != "x" {
			err = errors.New("data " + ev.Data)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next blocked after a bare CR")
	}
}

// F12: stream encoders fill an empty message id (generated) and model (from the options).
func TestStreamEncoderFallbacks(t *testing.T) {
	start := Event{Type: EventMessageStart}
	o, _ := NewOpenAIStreamEncoder(OpenAIStreamOptions{Model: "smart"}).Encode(start)
	a, _ := NewAnthropicStreamEncoder(AnthropicStreamOptions{Model: "smart"}).Encode(start)
	var oc struct{ ID, Model string }
	if err := json.Unmarshal(bytes.TrimSpace(bytes.TrimPrefix(o, []byte("data: "))), &oc); err != nil ||
		!strings.HasPrefix(oc.ID, "chatcmpl-") || len(oc.ID) < 20 || oc.Model != "smart" {
		t.Fatalf("openai: %v %s", err, o)
	}
	_, data, _ := bytes.Cut(a, []byte("data: "))
	var ac struct{ Message struct{ ID, Model string } }
	if err := json.Unmarshal(bytes.TrimSpace(data), &ac); err != nil || !strings.HasPrefix(ac.Message.ID, "msg_") || ac.Message.Model != "smart" {
		t.Fatalf("anthropic: %v %s", err, a)
	}
}
