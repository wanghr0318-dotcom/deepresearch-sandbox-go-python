package llmproto

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func TestDecodeOpenAIRequestErrors(t *testing.T) {
	cases := []struct {
		name, body  string
		unsupported bool   // *UnsupportedFieldError (else *InvalidRequestError)
		field       string // the field the error names
	}{
		{"not object", `[]`, false, ""},
		{"unknown field", `{"messages":[{"role":"user","content":"x"}],"max_output_tokens":5}`, true, "max_output_tokens"},
		{"max_tokens too large", `{"messages":[{"role":"user","content":"x"}],"max_tokens":1048577}`, false, "max_tokens"},
		{"max_completion_tokens too large", `{"messages":[{"role":"user","content":"x"}],"max_completion_tokens":1048577}`, false, "max_tokens"},
		{"echoed refusal on user", `{"messages":[{"role":"user","content":"x","refusal":"no"}]}`, true, "messages[0].refusal"},
		{"functions", `{"messages":[{"role":"user","content":"x"}],"functions":[]}`, true, "functions"},
		{"no messages", `{"model":"m"}`, false, "messages"},
		{"empty messages", `{"messages":[]}`, false, "messages"},
		{"bad max_tokens", `{"messages":[{"role":"user","content":"x"}],"max_tokens":0}`, false, "max_tokens"},
		{"bad stream", `{"messages":[{"role":"user","content":"x"}],"stream":"yes"}`, false, "stream"},
		{"stream_options key", `{"messages":[{"role":"user","content":"x"}],"stream_options":{"x":1}}`, true, "stream_options.x"},
		{"unknown message field", `{"messages":[{"role":"user","content":"x","foo":1}]}`, true, "messages[0].foo"},
		{"function role", `{"messages":[{"role":"function","content":"x","name":"f"}]}`, true, "messages[0].role"},
		{"reasoning on user", `{"messages":[{"role":"user","content":"x","reasoning_content":"r"}]}`, true, "messages[0].reasoning_content"},
		{"user without content", `{"messages":[{"role":"user"}]}`, false, "messages[0].content"},
		{"content number", `{"messages":[{"role":"user","content":5}]}`, false, "messages[0].content"},
		{"tool without id", `{"messages":[{"role":"user","content":"x"},{"role":"tool","content":"r"}]}`, false, "messages[1].tool_call_id"},
		{"tool image", `{"messages":[{"role":"user","content":"x"},{"role":"tool","tool_call_id":"c","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`, true, "messages[1].content"},
		{"bad tool call", `{"messages":[{"role":"assistant","tool_calls":[{"id":"","type":"function","function":{"name":"f"}}]}]}`, false, "messages[0].tool_calls[0]"},
		{"bad tool_choice", `{"messages":[{"role":"user","content":"x"}],"tool_choice":"any"}`, false, "tool_choice"},
		{"tool without name", `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{}}]}`, false, "tools[0].function.name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeOpenAIRequest([]byte(c.body))
			checkErr(t, err, c.unsupported, c.field)
		})
	}
}

func TestDecodeAnthropicRequestErrors(t *testing.T) {
	cases := []struct {
		name, body  string
		unsupported bool
		field       string
	}{
		{"unknown field", `{"max_tokens":5,"messages":[{"role":"user","content":"x"}],"container":"c"}`, true, "container"},
		{"max_tokens too large", `{"max_tokens":1048577,"messages":[{"role":"user","content":"x"}]}`, false, "max_tokens"},
		{"tool_choice field", `{"max_tokens":5,"messages":[{"role":"user","content":"x"}],"tool_choice":{"type":"auto","foo":1}}`, true, "tool_choice.foo"},
		{"bad role", `{"max_tokens":5,"messages":[{"role":"system","content":"x"}]}`, false, "messages[0].role"},
		{"tool_use in user", `{"max_tokens":5,"messages":[{"role":"user","content":[{"type":"tool_use","id":"t","name":"f","input":{}}]}]}`, false, "messages[0].content[0]"},
		{"tool_result in assistant", `{"max_tokens":5,"messages":[{"role":"assistant","content":[{"type":"tool_result","tool_use_id":"t"}]}]}`, false, "messages[0].content[0]"},
		{"input not object", `{"max_tokens":5,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"f","input":[1]}]}]}`, false, "messages[0].content[0].input"},
		{"unknown block field", `{"max_tokens":5,"messages":[{"role":"user","content":[{"type":"text","text":"x","foo":1}]}]}`, true, "messages[0].content[0].foo"},
		{"bad tool_choice", `{"max_tokens":5,"messages":[{"role":"user","content":"x"}],"tool_choice":{"type":"required"}}`, false, "tool_choice.type"},
		{"tool without schema", `{"max_tokens":5,"messages":[{"role":"user","content":"x"}],"tools":[{"name":"f"}]}`, false, "tools[0].input_schema"},
		{"system image", `{"max_tokens":5,"system":[{"type":"image","source":{}}],"messages":[{"role":"user","content":"x"}]}`, false, "system[0]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeAnthropicRequest([]byte(c.body))
			checkErr(t, err, c.unsupported, c.field)
		})
	}
}

func checkErr(t *testing.T, err error, unsupported bool, field string) {
	t.Helper()
	var u *UnsupportedFieldError
	var i *InvalidRequestError
	switch {
	case unsupported && errors.As(err, &u):
		if u.Field != field {
			t.Fatalf("unsupported field = %q, want %q (%v)", u.Field, field, err)
		}
	case !unsupported && errors.As(err, &i):
		if i.Field != field {
			t.Fatalf("invalid field = %q, want %q (%v)", i.Field, field, err)
		}
	default:
		t.Fatalf("err = %v (%T), want unsupported=%v on %q", err, err, unsupported, field)
	}
}

// TestGatewayCompatible：bodies that gateway/upstream accepts for sandbox calls decode, including a
// reasoning model's multi-turn tool call (assistant with reasoning_content and content "").
func TestGatewayCompatible(t *testing.T) {
	body := `{"model":"kimi-k3","max_tokens":4096,"temperature":0.2,"response_format":{"type":"json_object"},"seed":1,
	"messages":[{"role":"system","content":"s"},{"role":"user","content":"q"},
	{"role":"assistant","content":"","reasoning_content":"r","tool_calls":[{"id":"c1","type":"function","function":{"name":"web_search","arguments":"{\"q\":\"x\"}"}}]},
	{"role":"tool","tool_call_id":"c1","name":"web_search","content":"res"}],
	"tools":[{"type":"function","function":{"name":"web_search","parameters":{"type":"object"}}}],"tool_choice":"auto"}`
	req, err := DecodeOpenAIRequest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if got := req.FormatOnly(); !slices.Equal(got, []string{"response_format", "seed", "messages[2].reasoning_content"}) {
		t.Fatalf("FormatOnly = %v", got)
	}
	if p, ok := req.OnlyProtocol(); !ok || p != ProtocolOpenAI {
		t.Fatalf("OnlyProtocol = %v %v", p, ok)
	}
	// Pass-through encoding keeps everything, including the extras.
	out, err := EncodeOpenAIRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"seed":1`, `"response_format":{"type":"json_object"}`, `"reasoning_content":"r"`, `"name":"web_search"`} {
		if !strings.Contains(string(out), want) {
			t.Errorf("re-encoded request lacks %s: %s", want, out)
		}
	}
	// Converting fails, naming the first field.
	_, err = EncodeAnthropicRequest(req)
	var u *UnsupportedFieldError
	if !errors.As(err, &u) || u.Field != "response_format" {
		t.Fatalf("convert err = %v", err)
	}
}

func TestFeaturesNoOps(t *testing.T) {
	req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"n":1,"logprobs":false,"response_format":{"type":"text"},"max_tokens":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Features) != 0 {
		t.Fatalf("no-op values recorded as features: %v", req.Features)
	}
	for _, f := range []string{`"n":2`, `"logprobs":true`, `"top_logprobs":2`, `"reasoning_effort":"low"`, `"response_format":{"type":"json_schema","json_schema":{}}`} {
		req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],` + f + `}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, only := req.OnlyProtocol(); !only {
			t.Errorf("%s is not format-only", f)
		}
	}
}

func TestEncodeAnthropicNeedsMaxTokens(t *testing.T) {
	req, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = EncodeAnthropicRequest(req)
	var i *InvalidRequestError
	if !errors.As(err, &i) || i.Field != "max_tokens" {
		t.Fatalf("err = %v", err)
	}
	req.MaxTokens = 10
	if _, err := EncodeAnthropicRequest(req); err != nil {
		t.Fatal(err)
	}
}

func TestToolArgumentsMustBeObject(t *testing.T) {
	req, err := DecodeOpenAIRequest([]byte(`{"max_tokens":5,"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"not json"}}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	// Recorded on decode (F4): the request is pinned to OpenAI, and CanConvertTo agrees with the encoder.
	field := "messages[0].tool_calls[0].function.arguments"
	if got := req.FormatOnly(); !slices.Equal(got, []string{field}) {
		t.Fatalf("FormatOnly = %v", got)
	}
	var u *UnsupportedFieldError
	if err := req.CanConvertTo(ProtocolAnthropic); !errors.As(err, &u) || u.Field != field {
		t.Fatalf("CanConvertTo = %v", err)
	}
	if _, err := EncodeAnthropicRequest(req); !errors.As(err, &u) || u.Field != field {
		t.Fatalf("encode err = %v", err)
	}
	if _, err := EncodeOpenAIRequest(req); err != nil { // pass-through still works
		t.Fatal(err)
	}
}

func TestErrorObjects(t *testing.T) {
	cases := []struct {
		class                       ErrorClass
		status                      int
		openaiType, code, anthropic string
	}{
		{ClassInvalidAPIKey, 401, "invalid_request_error", "invalid_api_key", "authentication_error"},
		{ClassPermissionDenied, 403, "invalid_request_error", "permission_denied", "permission_error"},
		{ClassInvalidRequest, 400, "invalid_request_error", "invalid_request_error", "invalid_request_error"},
		{ClassUnsupportedField, 400, "invalid_request_error", "unsupported_field", "invalid_request_error"},
		{ClassRateLimit, 429, "rate_limit_error", "rate_limit_exceeded", "rate_limit_error"},
		{ClassInsufficientQuota, 429, "insufficient_quota", "insufficient_quota", "rate_limit_error"},
		{ClassModelDegraded, 503, "server_error", "model_degraded", "overloaded_error"},
		{ClassUpstream, 502, "server_error", "upstream_error", "api_error"},
		{ClassModelNotFound, 404, "invalid_request_error", "model_not_found", "not_found_error"},
		{ClassInternal, 500, "server_error", "internal_error", "api_error"},
	}
	for _, c := range cases {
		e := ErrorInfo{Class: c.class, Message: "m <x>"}
		st, body := EncodeError(ProtocolOpenAI, e)
		var o struct {
			Error struct {
				Message, Type, Code string
				Param               *string
			}
		}
		if err := json.Unmarshal(body, &o); err != nil || st != c.status || o.Error.Type != c.openaiType ||
			o.Error.Code != c.code || o.Error.Message != "m <x>" || o.Error.Param != nil {
			t.Errorf("%s openai: %d %s", c.class, st, body)
		}
		if !bytes.Contains(body, []byte("m <x>")) {
			t.Errorf("message HTML-escaped: %s", body)
		}
		st, body = EncodeError(ProtocolAnthropic, e)
		var a struct {
			Type  string
			Error struct{ Type, Message string }
		}
		if err := json.Unmarshal(body, &a); err != nil || st != c.status || a.Type != "error" || a.Error.Type != c.anthropic || a.Error.Message != "m <x>" {
			t.Errorf("%s anthropic: %d %s", c.class, st, body)
		}
	}
	// A codec error names its field as the OpenAI param.
	_, err := DecodeOpenAIRequest([]byte(`{"messages":[{"role":"user","content":"x"}],"functions":[]}`))
	info := ErrorFrom(err)
	if info.Class != ClassUnsupportedField || info.Param != "functions" {
		t.Fatalf("ErrorFrom = %+v", info)
	}
	if !bytes.Contains(EncodeOpenAIError(info), []byte(`"param":"functions"`)) {
		t.Fatalf("param missing: %s", EncodeOpenAIError(info))
	}
	if ErrorClass("nope").Status() != http.StatusInternalServerError {
		t.Fatal("unknown class status")
	}
}

func TestSSEReader(t *testing.T) {
	in := ": comment\r\nevent: a\r\ndata: one\r\ndata:two\r\nid: 7\r\n\r\n\nevent: lonely\n\ndata: x\rdata: y\r\rdata: tail"
	r := NewSSEReader(strings.NewReader(in), 0)
	want := []SSEEvent{{"a", "one\ntwo"}, {"", "x\ny"}}
	for _, w := range want {
		ev, err := r.Next()
		if err != nil || ev != w {
			t.Fatalf("Next = %+v, %v; want %+v", ev, err, w)
		}
	}
	// An event cut off by the end of input (here a partial line) is dropped: the stream was cut (F6).
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("cut event err = %v", err)
	}
	// A whole line without the blank line after it is cut as well; a clean end is io.EOF.
	if _, err := NewSSEReader(strings.NewReader("data: x\n"), 0).Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("unterminated event err = %v", err)
	}
	if _, err := NewSSEReader(strings.NewReader("data: x\n\n: bye\n"), 0).Next(); err != nil {
		t.Fatal(err)
	}
	r = NewSSEReader(strings.NewReader("data: x\n\n"), 0)
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end err = %v", err)
	}
	big := "data: " + strings.Repeat("x", 100) + "\n\n"
	if _, err := NewSSEReader(strings.NewReader(big), 50).Next(); !errors.Is(err, ErrSSETooLarge) {
		t.Fatalf("too large err = %v", err)
	}
	if got := string(FormatSSE("ev", []byte("a\nb"))); got != "event: ev\ndata: a\ndata: b\n\n" {
		t.Fatalf("FormatSSE = %q", got)
	}
}

func readAll(t *testing.T, sse string, p Protocol) ([]Event, error) {
	t.Helper()
	r := NewEventReader(strings.NewReader(sse), p)
	var out []Event
	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

func TestOpenAIStreamEdges(t *testing.T) {
	chunk := func(delta, finish string) string {
		f := "null"
		if finish != "" {
			f = `"` + finish + `"`
		}
		return `data: {"id":"c","model":"m","choices":[{"index":0,"delta":` + delta + `,"finish_reason":` + f + "}]}\n\n"
	}
	// Complete without [DONE]: closed at end of input.
	evs, err := readAll(t, chunk(`{"content":"hi"}`, "")+chunk(`{}`, "stop"), ProtocolOpenAI)
	if err != nil || evs[len(evs)-1].Type != EventMessageStop {
		t.Fatalf("no [DONE]: %v %+v", err, evs)
	}
	// Cut before the finish: unexpected EOF.
	if _, err := readAll(t, chunk(`{"content":"hi"}`, ""), ProtocolOpenAI); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("cut stream err = %v", err)
	}
	// Interleaved tool call deltas are refused.
	in := chunk(`{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":""}}]}`, "") +
		chunk(`{"tool_calls":[{"index":1,"id":"b","function":{"name":"g","arguments":""}}]}`, "") +
		chunk(`{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}`, "")
	if _, err := readAll(t, in, ProtocolOpenAI); !errors.Is(err, ErrBadStream) {
		t.Fatalf("interleaved err = %v", err)
	}
	// An error object in the stream becomes an error event.
	evs, err = readAll(t, chunk(`{"content":"x"}`, "")+`data: {"error":{"message":"boom","type":"server_error","code":"overloaded"}}`+"\n\n", ProtocolOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	last := evs[len(evs)-1]
	if last.Type != EventError || last.Err.Message != "boom" || last.Err.ProviderType != "overloaded" || last.Err.Class != ClassUpstream {
		t.Fatalf("error event = %+v", last)
	}
	// Without include_usage the encoder sends no usage chunk.
	enc := NewOpenAIStreamEncoder(OpenAIStreamOptions{})
	var out []byte
	for _, ev := range ResponseEvents(Response{ID: "x", Blocks: []Block{{Type: BlockText, Text: "t"}}, StopReason: StopEnd, HasUsage: true, Usage: Usage{InputTokens: 1}}) {
		b, err := enc.Encode(ev)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b...)
	}
	if bytes.Contains(out, []byte(`"usage"`)) || !bytes.HasSuffix(out, []byte("data: [DONE]\n\n")) {
		t.Fatalf("stream without include_usage:\n%s", out)
	}
}

func TestAnthropicStreamEdges(t *testing.T) {
	// A stream cut before message_stop is incomplete.
	in := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"x\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n"
	evs, err := readAll(t, in, ProtocolAnthropic)
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(evs) != 1 || evs[0].Usage == nil || evs[0].Usage.InputTokens != 3 {
		t.Fatalf("cut: %v %+v", err, evs)
	}
	// Unknown blocks are skipped; usage is still read.
	in += "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"x\"}}\n\n" +
		"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	r := NewEventReader(strings.NewReader(in), ProtocolAnthropic)
	var types []EventType
	for {
		ev, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, ev.Type)
	}
	if !slices.Equal(types, []EventType{EventMessageStart, EventMessageDelta, EventMessageStop}) {
		t.Fatalf("types = %v", types)
	}
	if u, ok := r.Usage(); !ok || u != (Usage{InputTokens: 3, OutputTokens: 9}) {
		t.Fatalf("usage = %+v", u)
	}
}
