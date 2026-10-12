package llmproto

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// Fields that the official SDKs add when a client sends a previous response back as model_dump() (fixture
// testdata/request/sdk_dump was produced with anthropic 1.13.0 and openai 3.29.0). Null or empty unknown
// fields on messages and content blocks are ignored; unknown fields with a value are still refused.
func TestSDKDumpArtifacts(t *testing.T) {
	anthropicMsg := func(block string) string {
		return `{"max_tokens":5,"messages":[{"role":"user","content":"q"},{"role":"assistant","content":[` + block + `]}]}`
	}
	openaiMsg := func(fields string) string {
		return `{"messages":[{"role":"user","content":"q"},{"role":"assistant","content":"a"` + fields + `},{"role":"user","content":"q2"}]}`
	}
	cases := []struct {
		name string
		p    Protocol
		body string
	}{
		{"anthropic tool_use caller null", ProtocolAnthropic, anthropicMsg(`{"type":"tool_use","id":"t","name":"f","input":{},"caller":null}`)},
		{"anthropic tool_use toolset_name null", ProtocolAnthropic, anthropicMsg(`{"type":"tool_use","id":"t","name":"f","input":{},"toolset_name":null}`)},
		{"anthropic text citations null", ProtocolAnthropic, anthropicMsg(`{"type":"text","text":"a","citations":null}`)},
		{"anthropic text cache_control null", ProtocolAnthropic, anthropicMsg(`{"type":"text","text":"a","cache_control":null}`)},
		{"anthropic thinking unknown empty", ProtocolAnthropic, anthropicMsg(`{"type":"thinking","thinking":"h","signature":"s","extra":{}}`)},
		{"anthropic message unknown null", ProtocolAnthropic, `{"max_tokens":5,"messages":[{"role":"user","content":"q","container":null}]}`},
		{"anthropic tool_result unknown null", ProtocolAnthropic, `{"max_tokens":5,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":"r","caller":null}]}]}`},
		{"openai parsed null", ProtocolOpenAI, openaiMsg(`,"parsed":null`)},
		{"openai tool_calls null", ProtocolOpenAI, openaiMsg(`,"tool_calls":null`)},
		{"openai echoed nulls", ProtocolOpenAI, openaiMsg(`,"refusal":null,"annotations":null,"audio":null,"function_call":null`)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := DecodeRequest(c.p, []byte(c.body))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if fs := req.FormatOnly(); len(fs) != 0 && !(c.p == ProtocolAnthropic && strings.Contains(c.name, "thinking")) {
				t.Fatalf("format-only %v", fs)
			}
		})
	}
	// Unknown fields with a value are still refused.
	for _, c := range []struct {
		p           Protocol
		body, field string
	}{
		{ProtocolAnthropic, anthropicMsg(`{"type":"tool_use","id":"t","name":"f","input":{},"caller":{"type":"direct"}}`), "messages[1].content[0].caller"},
		{ProtocolAnthropic, `{"max_tokens":5,"messages":[{"role":"user","content":"q","container":"c1"}]}`, "messages[0].container"},
		{ProtocolOpenAI, openaiMsg(`,"parsed":{"x":1}`), "messages[1].parsed"},
	} {
		_, err := DecodeRequest(c.p, []byte(c.body))
		var u *UnsupportedFieldError
		if !errors.As(err, &u) || u.Field != c.field {
			t.Errorf("%s: err = %v, want unsupported %s", c.body, err, c.field)
		}
	}
}

// The real model_dump() fixture decodes in its own format and converts; ids and tool pairing are kept.
func TestSDKDumpFixture(t *testing.T) {
	for _, p := range protocols {
		body, ok := readFile(t, "testdata/request/sdk_dump/"+string(p)+".json")
		if !ok {
			t.Fatal("missing fixture")
		}
		req, err := DecodeRequest(p, body)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		var ids []string
		for _, m := range req.Messages {
			for _, b := range m.Blocks {
				if b.Type == BlockToolCall || b.Type == BlockToolResult {
					ids = append(ids, b.ID)
				}
			}
		}
		if len(ids) != 2 || ids[0] != ids[1] {
			t.Fatalf("%s: tool ids %v", p, ids)
		}
		if p == ProtocolOpenAI && !slices.Equal(req.FormatOnly(), nil) {
			t.Fatalf("openai dump is format-only: %v", req.FormatOnly())
		}
	}
}
