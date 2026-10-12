package fakeupstream

import (
	"encoding/json"
	"testing"
)

// TestRequestUsageMatchesReply：每个请求记录的用量等于回复中报告的用量（OpenAI 非流式与流式、Anthropic），负载测试
// 据此独立核对网关记录的 token。
func TestRequestUsageMatchesReply(t *testing.T) {
	s := New()
	defer s.Close()
	resp := post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	var u struct {
		Prompt     int64 `json:"prompt_tokens"`
		Completion int64 `json:"completion_tokens"`
	}
	if err := json.Unmarshal(raw["usage"], &u); err != nil || u.Prompt == 0 || u.Completion == 0 {
		t.Fatalf("reply usage %s: %v", raw["usage"], err)
	}
	post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
	resp = post(t, s.URL()+"/v1/messages", `{"model":"m","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, "anthropic-version", "2023-06-01")
	var m struct {
		Usage struct {
			Input  int64 `json:"input_tokens"`
			Output int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	chat := s.Requests(Chat)
	if len(chat) != 2 || chat[0].PromptTokens != u.Prompt || chat[0].CompletionTokens != u.Completion || chat[1].PromptTokens == 0 {
		t.Fatalf("chat usage records %+v, reply %+v", chat, u)
	}
	msgs := s.Requests(Messages)
	if len(msgs) != 1 || msgs[0].PromptTokens != m.Usage.Input || msgs[0].CompletionTokens != m.Usage.Output {
		t.Fatalf("messages usage records %+v, reply %+v", msgs, m.Usage)
	}
}
