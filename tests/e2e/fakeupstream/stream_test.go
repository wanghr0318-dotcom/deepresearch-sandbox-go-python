package fakeupstream

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// post 发出模型请求；header 为附加的请求头（成对的名与值）。
func post(t *testing.T, url, body string, header ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// collect 用 llmproto 读完一个流式回复（与本包手写的线路格式交叉验证）。
func collect(t *testing.T, resp *http.Response, p llmproto.Protocol) (llmproto.Collector, int, error) {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	rd := llmproto.NewEventReader(resp.Body, p)
	var col llmproto.Collector
	deltas := 0
	for {
		ev, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return col, deltas, nil
		}
		if err != nil {
			return col, deltas, err
		}
		if ev.Type == llmproto.EventBlockDelta {
			deltas++
		}
		col.Add(ev)
	}
}

const toolMsg = `{"role":"assistant","content":"查一下。","tool_calls":[` +
	`{"id":"call_1","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"固态电池 量产\"}"}},` +
	`{"id":"call_2","type":"function","function":{"name":"fetch","arguments":"{\"url\":\"https://example.com/a\"}"}}]}`

// TestChatStream：stream: true 的 chat 以 chunk 流回复（文本与工具参数分多个 chunk、finish_reason、[DONE]），
// include_usage 为真时带用量 chunk；非流式回复的形状不变（没有 prompt_tokens_details）。
func TestChatStream(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetChatHook(func(body []byte) json.RawMessage {
		if strings.Contains(string(body), "tools please") {
			return json.RawMessage(toolMsg)
		}
		return nil
	})
	resp := post(t, s.ModelBaseURL()+"/chat/completions",
		`{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"tools please"}]}`)
	col, deltas, err := collect(t, resp, llmproto.ProtocolOpenAI)
	if err != nil || !col.Complete() {
		t.Fatalf("stream: %v complete=%v", err, col.Complete())
	}
	r := col.Response()
	if len(r.Blocks) != 3 || r.Blocks[0].Text != "查一下。" || r.Blocks[1].Name != "web_search" ||
		r.Blocks[1].Arguments != `{"query":"固态电池 量产"}` || r.Blocks[2].ID != "call_2" || r.StopReason != llmproto.StopToolUse {
		t.Fatalf("response = %+v", r)
	}
	if deltas < 6 { // 工具参数分多个 chunk 到达
		t.Fatalf("only %d deltas", deltas)
	}
	if !r.HasUsage || r.Usage.OutputTokens == 0 || r.Usage.InputTokens == 0 {
		t.Fatalf("usage = %+v (has %v)", r.Usage, r.HasUsage)
	}
	if rec := s.Requests(Chat)[0]; !rec.Stream || rec.Outcome != OutcomeOK {
		t.Fatalf("record = %+v", rec)
	}

	// 不要求用量时流中没有用量（与 OpenAI 一致）。
	resp = post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	col, _, err = collect(t, resp, llmproto.ProtocolOpenAI)
	if err != nil || col.Response().HasUsage || !strings.HasPrefix(col.Response().Blocks[0].Text, "fake 回复 ") {
		t.Fatalf("plain stream: %v %+v", err, col.Response())
	}

	// 非流式回复不变；SetCacheUsage 之后才带 prompt_tokens_details。
	plain := func() map[string]any {
		resp := post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out["usage"].(map[string]any)
	}
	if u := plain(); u["prompt_tokens_details"] != nil || len(u) != 3 {
		t.Fatalf("usage = %v", u)
	}
	s.SetCacheUsage(100, 0)
	resp = post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	b, _ := io.ReadAll(resp.Body)
	got, err := llmproto.DecodeOpenAIResponse(b)
	if err != nil || got.Usage.CacheReadTokens != 100 || got.Usage.InputTokens == 0 {
		t.Fatalf("cached usage: %v %+v", err, got.Usage)
	}
}

// TestMessages：/v1/messages 检查 anthropic-version 与 x-api-key，非流式回复 message 对象，流式回复 Anthropic
// 事件序列（输入与缓存用量在 message_start，输出用量在 message_delta），工具调用的 input 分多个 chunk。
func TestMessages(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetAnthropicKey("sk-test")
	s.SetCacheUsage(300, 20)
	s.SetMessagesHook(func(body []byte) json.RawMessage {
		if !strings.Contains(string(body), "tools please") {
			return nil
		}
		return json.RawMessage(`[{"type":"thinking","thinking":"先搜索","signature":"sig1"},{"type":"text","text":"查一下。"},` +
			`{"type":"tool_use","id":"toolu_1","name":"web_search","input":{"query":"固态电池"}}]`)
	})
	url := s.URL() + "/v1/messages"
	body := `{"model":"claude","max_tokens":100,"system":"` + StagePlan + ` plan","messages":[{"role":"user","content":"q"}]}`

	resp := post(t, url, body, "anthropic-version", "2023-06-01", "x-api-key", "wrong")
	var e struct {
		Type  string
		Error struct{ Type, Message string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || resp.StatusCode != 401 || e.Type != "error" || e.Error.Type != "authentication_error" {
		t.Fatalf("bad key: %d %+v %v", resp.StatusCode, e, err)
	}
	if resp := post(t, url, body, "x-api-key", "sk-test"); resp.StatusCode != 400 {
		t.Fatalf("missing anthropic-version: %d", resp.StatusCode)
	}

	resp = post(t, url, body, "anthropic-version", "2023-06-01", "x-api-key", "sk-test")
	b, _ := io.ReadAll(resp.Body)
	got, err := llmproto.DecodeAnthropicResponse(b)
	if err != nil || resp.StatusCode != 200 || len(got.Blocks) != 1 || !strings.Contains(got.Blocks[0].Text, "fake 任务") ||
		got.StopReason != llmproto.StopEnd || got.Usage.CacheReadTokens != 300 || got.Usage.CacheWriteTokens != 20 {
		t.Fatalf("non-stream: %d %v %+v\n%s", resp.StatusCode, err, got, b)
	}
	if s.StageCount(StagePlan) != 3 || s.Count(Messages) != 3 || s.Requests(Messages)[2].APIKey != "sk-test" {
		t.Fatalf("records: stage=%d count=%d", s.StageCount(StagePlan), s.Count(Messages))
	}

	stream := `{"model":"claude","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"tools please"}]}`
	resp = post(t, url, stream, "anthropic-version", "2023-06-01", "x-api-key", "sk-test")
	col, deltas, err := collect(t, resp, llmproto.ProtocolAnthropic)
	if err != nil || !col.Complete() {
		t.Fatalf("stream: %v", err)
	}
	r := col.Response()
	if len(r.Blocks) != 3 || r.Blocks[0].Signature != "sig1" || r.Blocks[0].Text != "先搜索" || r.Blocks[1].Text != "查一下。" ||
		r.Blocks[2].Arguments != `{"query":"固态电池"}` || r.StopReason != llmproto.StopToolUse || deltas < 5 {
		t.Fatalf("stream response = %+v (deltas %d)", r, deltas)
	}
	if r.Usage.InputTokens == 0 || r.Usage.OutputTokens == 0 || r.Usage.CacheReadTokens != 300 || r.Usage.CacheWriteTokens != 20 {
		t.Fatalf("stream usage = %+v", r.Usage)
	}
}

// TestStreamFaults：FailAfterChunks 在发出 N 个事件后切断连接或发出错误事件；注入的状态在 Messages 类别中是
// Anthropic 错误形状；SetChunkDelay 不改变内容。
func TestStreamFaults(t *testing.T) {
	s := New()
	defer s.Close()
	s.Inject(Chat, 1, Action{FailAfterChunks: 2})
	s.Inject(Messages, 1, Action{FailAfterChunks: 3, FailWithError: true})
	s.Inject(Messages, 2, Action{Status: 529})
	s.SetChunkDelay(Chat, 1)

	resp := post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","stream":true,"messages":[{"role":"user","content":"hello there"}]}`)
	if _, _, err := collect(t, resp, llmproto.ProtocolOpenAI); err == nil {
		t.Fatal("cut stream read without error")
	}
	if rec := s.Requests(Chat)[0]; rec.Outcome != OutcomeDisconnected || rec.Action.String() != "cut after 2 chunks" {
		t.Fatalf("record = %+v", rec)
	}

	stream := `{"model":"c","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hello there"}]}`
	resp = post(t, s.URL()+"/v1/messages", stream, "anthropic-version", "2023-06-01")
	col, _, err := collect(t, resp, llmproto.ProtocolAnthropic)
	if err != nil || col.Err() == nil || col.Err().ProviderType != "overloaded_error" {
		t.Fatalf("error event: %v %+v", err, col.Err())
	}
	if rec := s.Requests(Messages)[0]; rec.Outcome != OutcomeStreamError {
		t.Fatalf("record = %+v", rec)
	}

	resp = post(t, s.URL()+"/v1/messages", stream, "anthropic-version", "2023-06-01")
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 529 || !bytes.Contains(b, []byte(`"overloaded_error"`)) {
		t.Fatalf("injected status: %d %s", resp.StatusCode, b)
	}

	// 非流式请求不受 FailAfterChunks 影响。
	s.Inject(Chat, 2, Action{FailAfterChunks: 1})
	resp = post(t, s.ModelBaseURL()+"/chat/completions", `{"model":"m","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("non-stream with FailAfterChunks: %d", resp.StatusCode)
	}
}
