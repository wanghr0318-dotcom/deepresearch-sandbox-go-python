package llmproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

// Property tests: random requests, responses and streams in the subset both formats express survive
// encode → decode in each format, and a request survives OpenAI → Anthropic → OpenAI.

var sampleTexts = []string{"hi", "多语言 文本", "a <b> & \"c\"", "line1\nline2", " spaced ", "{\"not\":\"json\"}", "emoji 🙂"}

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.IntN(len(xs))] }

func randArgs(r *rand.Rand) string {
	obj := map[string]any{}
	for i := range r.IntN(3) {
		switch r.IntN(3) {
		case 0:
			obj[fmt.Sprintf("k%d", i)] = pick(r, sampleTexts)
		case 1:
			obj[fmt.Sprintf("n%d", i)] = r.IntN(1000)
		default:
			obj[fmt.Sprintf("a%d", i)] = []any{true, nil, 1.5}
		}
	}
	b, _ := json.Marshal(obj)
	return string(b)
}

func ptr[T any](v T) *T { return &v }

// randRequest returns a request in the canonical form both formats preserve: roles alternate starting with
// user; a user turn is one text block or one or more tool results; an assistant turn is at most one text
// block followed by tool calls; texts are not empty; arguments are compact JSON objects.
func randRequest(r *rand.Rand) Request {
	req := Request{Model: "m", MaxTokens: int64(1 + r.IntN(4096))}
	for range r.IntN(3) {
		req.System = append(req.System, Block{Type: BlockText, Text: pick(r, sampleTexts)})
	}
	nTools := r.IntN(3)
	for i := range nTools {
		t := Tool{Name: fmt.Sprintf("tool_%d", i), Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)}
		if r.IntN(2) == 0 {
			t.Description = pick(r, sampleTexts)
		}
		req.Tools = append(req.Tools, t)
	}
	if nTools > 0 && r.IntN(2) == 0 {
		switch r.IntN(4) {
		case 0:
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceAuto}
		case 1:
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceRequired}
		case 2:
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceNone}
		default:
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceTool, Name: "tool_0"}
		}
	}
	if nTools > 0 && (req.ToolChoice == nil || req.ToolChoice.Mode != ToolChoiceNone) && r.IntN(3) == 0 {
		req.ParallelToolCalls = ptr(false)
		if req.ToolChoice == nil { // Anthropic carries the flag on an explicit auto tool choice
			req.ToolChoice = &ToolChoice{Mode: ToolChoiceAuto}
		}
	}
	if r.IntN(2) == 0 {
		req.Temperature = ptr(float64(r.IntN(10)) / 10)
	}
	if req.Temperature == nil && r.IntN(2) == 0 { // not both: top_p is dropped for Anthropic
		req.TopP = ptr(0.5)
	}
	for range r.IntN(3) {
		req.Stop = append(req.Stop, pick(r, []string{"END", "\n\n", "###"}))
	}
	req.Stream = r.IntN(2) == 0
	var pendingCalls []string
	for turn := range 1 + r.IntN(6) {
		if turn%2 == 0 { // user
			if len(pendingCalls) > 0 {
				m := Message{Role: RoleUser}
				for _, id := range pendingCalls {
					m.Blocks = append(m.Blocks, Block{Type: BlockToolResult, ID: id, Text: pick(r, sampleTexts)})
				}
				req.Messages = append(req.Messages, m)
				pendingCalls = nil
				continue
			}
			req.Messages = append(req.Messages, Message{Role: RoleUser, Blocks: []Block{{Type: BlockText, Text: pick(r, sampleTexts)}}})
			continue
		}
		m := Message{Role: RoleAssistant}
		calls := 0
		if nTools > 0 {
			calls = r.IntN(3)
		}
		if calls == 0 || r.IntN(2) == 0 {
			m.Blocks = append(m.Blocks, Block{Type: BlockText, Text: pick(r, sampleTexts)})
		}
		for c := range calls {
			id := fmt.Sprintf("call_%d_%d", turn, c)
			m.Blocks = append(m.Blocks, Block{Type: BlockToolCall, ID: id, Name: fmt.Sprintf("tool_%d", r.IntN(nTools)), Arguments: randArgs(r)})
			pendingCalls = append(pendingCalls, id)
		}
		req.Messages = append(req.Messages, m)
	}
	return req
}

func TestRequestRoundTripProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 500 {
		req := randRequest(r)
		for _, p := range protocols {
			if p == ProtocolOpenAI && req.Stream {
				req.IncludeUsage = r.IntN(2) == 0
			}
			body, err := EncodeRequest(req, p)
			if err != nil {
				t.Fatalf("#%d encode %s: %v", i, p, err)
			}
			got, err := DecodeRequest(p, body.Body)
			if err != nil {
				t.Fatalf("#%d decode %s: %v\n%s", i, p, err, body.Body)
			}
			want := req
			want.Source = p
			if p == ProtocolAnthropic {
				want.IncludeUsage = false
			}
			if len(got.Features) != 0 {
				t.Fatalf("#%d %s: features %v in a canonical request", i, p, got.Features)
			}
			got.Features = nil
			if !reflect.DeepEqual(normalizeRequest(got), normalizeRequest(want)) {
				t.Fatalf("#%d %s round trip\n got: %#v\nwant: %#v\nbody: %s", i, p, got, want, body.Body)
			}
		}
		// OpenAI → Anthropic → OpenAI keeps the request.
		oa, _ := EncodeRequest(req, ProtocolOpenAI)
		r1, _ := DecodeOpenAIRequest(oa.Body)
		an, err := EncodeRequest(r1, ProtocolAnthropic)
		if err != nil {
			t.Fatalf("#%d to anthropic: %v", i, err)
		}
		r2, _ := DecodeAnthropicRequest(an.Body)
		oa2, err := EncodeRequest(r2, ProtocolOpenAI)
		if err != nil {
			t.Fatalf("#%d back to openai: %v", i, err)
		}
		r3, _ := DecodeOpenAIRequest(oa2.Body)
		r1.IncludeUsage = r1.IncludeUsage || r1.Stream // a converted stream always asks for usage
		if !reflect.DeepEqual(normalizeRequest(r3), normalizeRequest(r1)) {
			t.Fatalf("#%d openai→anthropic→openai\n got: %#v\nwant: %#v", i, r3, r1)
		}
	}
}

func randResponse(r *rand.Rand, p Protocol) Response {
	resp := Response{ID: fmt.Sprintf("id-%d", r.IntN(1000)), Model: "m", HasUsage: true,
		Usage: Usage{InputTokens: int64(r.IntN(500)), OutputTokens: int64(r.IntN(500)), CacheReadTokens: int64(r.IntN(100))}}
	if p == ProtocolOpenAI {
		resp.Created = 1700000000
	} else {
		resp.Usage.CacheWriteTokens = int64(r.IntN(100))
	}
	if p == ProtocolAnthropic && r.IntN(3) == 0 {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockThinking, Text: pick(r, sampleTexts), Signature: "sig" + fmt.Sprint(r.IntN(100))})
	}
	if p == ProtocolOpenAI && r.IntN(3) == 0 {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockThinking, Text: pick(r, sampleTexts)})
	}
	if r.IntN(3) > 0 {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockText, Text: pick(r, sampleTexts)})
	}
	resp.StopReason = pick(r, []StopReason{StopEnd, StopMaxTokens, StopRefusal})
	if p == ProtocolAnthropic && r.IntN(4) == 0 {
		resp.StopReason, resp.StopSequence = StopSequence, "###"
	}
	for c := range r.IntN(3) {
		resp.Blocks = append(resp.Blocks, Block{Type: BlockToolCall, ID: fmt.Sprintf("t%d", c), Name: "f", Arguments: randArgs(r)})
		resp.StopReason, resp.StopSequence = StopToolUse, ""
	}
	return resp
}

// split cuts a delta event's fragment into random pieces (tool arguments and text arrive in chunks).
func split(r *rand.Rand, ev Event) []Event {
	s := ev.Text + ev.Arguments
	if s == "" || r.IntN(3) == 0 {
		return []Event{ev}
	}
	var out []Event
	runes := []rune(s)
	for len(runes) > 0 {
		n := 1 + r.IntN(len(runes))
		piece := ev
		if ev.Arguments != "" {
			piece.Arguments = string(runes[:n])
		} else {
			piece.Text = string(runes[:n])
		}
		piece.Signature = ""
		out = append(out, piece)
		runes = runes[n:]
	}
	if ev.Signature != "" {
		out = append(out, Event{Type: EventBlockDelta, Index: ev.Index, Signature: ev.Signature})
	}
	return out
}

func TestStreamRoundTripProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 500 {
		for _, p := range protocols {
			resp := randResponse(r, p)
			var enc StreamEncoder
			if p == ProtocolOpenAI {
				enc = NewOpenAIStreamEncoder(OpenAIStreamOptions{IncludeUsage: true})
			} else {
				enc = NewAnthropicStreamEncoder(AnthropicStreamOptions{Source: ProtocolAnthropic})
			}
			var sse bytes.Buffer
			for _, ev := range ResponseEvents(resp) {
				for _, piece := range split(r, ev) {
					b, err := enc.Encode(piece)
					if err != nil {
						t.Fatal(err)
					}
					sse.Write(b)
				}
			}
			rd := NewEventReader(bytes.NewReader(sse.Bytes()), p)
			var col Collector
			for {
				ev, err := rd.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("#%d %s decode: %v\n%s", i, p, err, sse.String())
				}
				col.Add(ev)
			}
			got := col.Response()
			if !col.Complete() || !reflect.DeepEqual(got, resp) {
				t.Fatalf("#%d %s stream round trip\n got: %+v\nwant: %+v\n%s", i, p, got, resp, sse.String())
			}
			// The non-stream encoding of the same response decodes to the same response.
			body, err := EncodeResponse(p, resp)
			if err != nil {
				t.Fatal(err)
			}
			back, err := DecodeResponse(p, body)
			if err != nil {
				t.Fatal(err)
			}
			back.Source = ""
			if !reflect.DeepEqual(back, resp) {
				t.Fatalf("#%d %s response round trip\n got: %+v\nwant: %+v\n%s", i, p, back, resp, body)
			}
		}
	}
}

func TestResponseEventsShape(t *testing.T) {
	evs := ResponseEvents(Response{Blocks: []Block{{Type: BlockText, Text: "a"}, {Type: BlockOpaque}, {Type: BlockToolCall, ID: "x", Name: "f", Arguments: "{}"}}})
	var kinds []string
	for _, e := range evs {
		kinds = append(kinds, fmt.Sprintf("%s:%d", e.Type, e.Index))
	}
	want := "message_start:0 block_start:0 block_delta:0 block_stop:0 block_start:1 block_delta:1 block_stop:1 message_delta:0 message_stop:0"
	if got := strings.Join(kinds, " "); got != want {
		t.Fatalf("events = %s", got)
	}
}
