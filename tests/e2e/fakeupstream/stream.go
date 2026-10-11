package fakeupstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// 流式回复与 Anthropic Messages 模式（模型网关设计 §10）。线路格式在这里手写，与 internal/llmproto 的编解码器
// 相互独立，测试才能交叉验证二者。

// SetAnthropicKey 设置 /v1/messages 要求的 x-api-key（空串表示不检查）。
func (s *Server) SetAnthropicKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.anthropicKey = key
}

// SetMessagesHook 设置 /v1/messages 的脚本：hook 收到请求体，返回 Anthropic content 数组（text、tool_use、
// thinking 块；含 tool_use 时 stop_reason 为 tool_use）或 nil（按阶段标记与占位文本回复）。
func (s *Server) SetMessagesHook(hook func(body []byte) json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messagesHook = hook
}

// SetChunkDelay 让类别 kind 的流式回复在相邻两个 SSE 事件之间等待 d（客户端离开时提前结束）。
func (s *Server) SetChunkDelay(kind Kind, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunkDelay[kind] = d
}

// SetCacheUsage 让此后的模型回复用量带缓存 token：read 为缓存读取（OpenAI 计入 prompt_tokens 并在
// prompt_tokens_details.cached_tokens 中给出；Anthropic 为 cache_read_input_tokens，不计入 input_tokens），
// write 为缓存写入（只有 Anthropic 有：cache_creation_input_tokens）。
func (s *Server) SetCacheUsage(read, write int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cacheRead, s.cacheWrite = read, write
}

// chatUsage 是 OpenAI 格式的用量；设置了缓存读取时 prompt_tokens 包含它。
func (s *Server) chatUsage(prompt, completion int64) ChatUsage {
	s.mu.Lock()
	read := s.cacheRead
	s.mu.Unlock()
	u := ChatUsage{PromptTokens: prompt + read, CompletionTokens: completion, TotalTokens: prompt + read + completion}
	if read > 0 {
		u.PromptTokensDetails = &PromptTokensDetails{CachedTokens: read}
	}
	return u
}

// streamRequested 报告模型请求是否要求流式回复。
func streamRequested(kind Kind, body []byte) bool {
	if kind != Chat && kind != Messages {
		return false
	}
	var req struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &req) == nil && req.Stream
}

// sseStream 按顺序发出 SSE 事件，并执行 Action.FailAfterChunks 与 chunk 间延迟。
type sseStream struct {
	s     *Server
	w     http.ResponseWriter
	r     *http.Request
	rec   *Request
	delay time.Duration
	sent  int
	// failFrame 是 FailWithError 时代替第 FailAfterChunks+1 个事件发出的错误事件。
	failFrame string
}

func (s *Server) newStream(w http.ResponseWriter, r *http.Request, rec *Request, failFrame string) *sseStream {
	s.mu.Lock()
	delay := s.chunkDelay[rec.Kind]
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Request-Id", fmt.Sprintf("fake-req-%d", rec.Seq))
	w.WriteHeader(http.StatusOK)
	return &sseStream{s: s, w: w, r: r, rec: rec, delay: delay, failFrame: failFrame}
}

// send 发出一个事件；返回 false 时流已结束（客户端离开、注入的失败），调用方停止发送。注入的切断以
// panic(http.ErrAbortHandler) 中止连接：客户端读到不完整的 chunked 正文。
func (st *sseStream) send(event string, data string) bool {
	if a := st.rec.Action; a.FailAfterChunks > 0 && st.sent == a.FailAfterChunks {
		if a.FailWithError {
			_, _ = fmt.Fprint(st.w, st.failFrame)
			st.flush()
			st.s.setOutcome(st.rec, OutcomeStreamError)
			return false
		}
		st.flush()
		st.s.setOutcome(st.rec, OutcomeDisconnected)
		panic(http.ErrAbortHandler)
	}
	if st.sent > 0 && st.delay > 0 {
		timer := time.NewTimer(st.delay)
		select {
		case <-timer.C:
		case <-st.r.Context().Done():
			timer.Stop()
			st.s.setOutcome(st.rec, OutcomeAborted)
			return false
		}
	}
	var b strings.Builder
	if event != "" {
		b.WriteString("event: " + event + "\n")
	}
	b.WriteString("data: " + data + "\n\n")
	if _, err := fmt.Fprint(st.w, b.String()); err != nil {
		st.s.setOutcome(st.rec, OutcomeAborted)
		return false
	}
	st.flush()
	st.sent++
	return true
}

// frame is one SSE event to send: its event name (empty for OpenAI) and data.
type frame struct{ event, data string }

// sendAll sends frames in order and reports whether all were sent.
func (st *sseStream) sendAll(frames []frame) bool {
	for _, f := range frames {
		if !st.send(f.event, f.data) {
			return false
		}
	}
	return true
}

func (st *sseStream) flush() {
	if f, ok := st.w.(http.Flusher); ok {
		f.Flush()
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // 只编码本包构造的值
	}
	return string(b)
}

// pieces 把 s 按 n 个字符切成若干片（流式回复中文本与工具参数分多个 chunk 到达）。
func pieces(s string, n int) []string {
	rs := []rune(s)
	var out []string
	for len(rs) > 0 {
		k := min(n, len(rs))
		out = append(out, string(rs[:k]))
		rs = rs[k:]
	}
	return out
}

// streamChat 以 OpenAI chat.completion.chunk 流发出 msg（assistant message：content、reasoning_content、
// tool_calls）：角色 chunk、思考与文本每 8 个字符一个 chunk、每个工具调用一个起始 chunk 加每 5 个字符一个
// 参数 chunk、finish_reason chunk、（include_usage 时）用量 chunk、[DONE]。
func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, rec *Request, req chatRequest, msg json.RawMessage, finish string, usage ChatUsage) {
	var m struct {
		Content          *string `json:"content"`
		ReasoningContent string  `json:"reasoning_content"`
		ToolCalls        []struct {
			ID       string `json:"id"`
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls"`
	}
	if err := json.Unmarshal(msg, &m); err != nil {
		panic(fmt.Sprintf("fakeupstream: chat hook 返回的 message 不合法: %v", err))
	}
	id, created := fmt.Sprintf("chatcmpl-fake-%d", rec.Seq), time.Now().Unix()
	var frames []frame
	add := func(delta map[string]any, finish any) {
		frames = append(frames, frame{data: mustJSON(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created,
			"model": req.Model, "choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}}})})
	}
	add(map[string]any{"role": "assistant", "content": ""}, nil)
	for _, p := range pieces(m.ReasoningContent, 8) {
		add(map[string]any{"reasoning_content": p}, nil)
	}
	if m.Content != nil {
		for _, p := range pieces(*m.Content, 8) {
			add(map[string]any{"content": p}, nil)
		}
	}
	for i, tc := range m.ToolCalls {
		add(map[string]any{"tool_calls": []map[string]any{{"index": i, "id": tc.ID, "type": "function",
			"function": map[string]string{"name": tc.Function.Name, "arguments": ""}}}}, nil)
		for _, p := range pieces(tc.Function.Arguments, 5) {
			add(map[string]any{"tool_calls": []map[string]any{{"index": i, "function": map[string]string{"arguments": p}}}}, nil)
		}
	}
	add(map[string]any{}, finish)
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		frames = append(frames, frame{data: mustJSON(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created,
			"model": req.Model, "choices": []any{}, "usage": usage})})
	}
	frames = append(frames, frame{data: "[DONE]"})
	st := s.newStream(w, r, rec, "data: "+mustJSON(errBody("stream_failed"))+"\n\n")
	if st.sendAll(frames) {
		s.setOutcome(rec, OutcomeOK)
	}
}

// messagesRequest 是 /v1/messages 请求中本服务器读取的字段。
type messagesRequest struct {
	Model     string          `json:"model"`
	MaxTokens int64           `json:"max_tokens"`
	System    json.RawMessage `json:"system"`
	Messages  []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Stream bool `json:"stream"`
}

// systemText 是 system 字段的文本（字符串，或文本块数组的第一块）。
func (m messagesRequest) systemText() string {
	var s string
	if json.Unmarshal(m.System, &s) == nil {
		return s
	}
	var blocks []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(m.System, &blocks) == nil && len(blocks) > 0 {
		return blocks[0].Text
	}
	return ""
}

// firstUserText 是第一条用户消息的文本（字符串，或第一个文本块）。
func (m messagesRequest) firstUserText() string {
	for _, msg := range m.Messages {
		if msg.Role != "user" {
			continue
		}
		var s string
		if json.Unmarshal(msg.Content, &s) == nil {
			return s
		}
		var blocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(msg.Content, &blocks) == nil {
			for _, b := range blocks {
				if b.Type == "text" {
					return b.Text
				}
			}
		}
		return ""
	}
	return ""
}

// messagesStageOf 返回 /v1/messages 请求 system 开头的阶段标记；没有时为空。
func messagesStageOf(body []byte) string {
	var req messagesRequest
	if json.Unmarshal(body, &req) != nil {
		return ""
	}
	sys := req.systemText()
	for _, st := range []string{StagePlan, StageSummarize, StageReport, StageCode} {
		if strings.HasPrefix(sys, st) {
			return st
		}
	}
	return ""
}

// anthropicErrType 是状态对应的 Anthropic 错误类型。
func anthropicErrType(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request_error"
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case 529, http.StatusServiceUnavailable:
		return "overloaded_error"
	}
	return "api_error"
}

func anthropicErrBody(status int, message string) map[string]any {
	return map[string]any{"type": "error", "error": map[string]string{"type": anthropicErrType(status), "message": message}}
}

// messagesText 是没有脚本时的回复文本：阶段标记对应的回复（[stage:code] 按评测任务）或确定性的占位文本。
func (s *Server) messagesText(rec *Request, req messagesRequest, body []byte) string {
	sum := sha256.Sum256(body)
	text := "fake 回复 " + hex.EncodeToString(sum[:4])
	s.mu.Lock()
	defer s.mu.Unlock()
	if reply, ok := s.stages[rec.Stage]; ok && rec.Stage != "" {
		text = reply
	}
	if id, ok := evalTaskID(req.firstUserText()); ok && rec.Stage == StageCode {
		if reply, ok := s.code[id]; ok {
			text = reply
		}
	}
	return text
}

// anthropicUsage 是 Anthropic 格式的用量。
type anthropicUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request, rec *Request, body []byte) {
	s.mu.Lock()
	key, hook := s.anthropicKey, s.messagesHook
	read, write := s.cacheRead, s.cacheWrite
	s.mu.Unlock()
	if key != "" && r.Header.Get("X-Api-Key") != key {
		writeJSON(w, http.StatusUnauthorized, anthropicErrBody(http.StatusUnauthorized, "invalid x-api-key"))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	if r.Header.Get("Anthropic-Version") == "" {
		writeJSON(w, http.StatusBadRequest, anthropicErrBody(http.StatusBadRequest, "anthropic-version: header is required"))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	var req messagesRequest
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 || req.MaxTokens <= 0 {
		writeJSON(w, http.StatusBadRequest, anthropicErrBody(http.StatusBadRequest, "messages and max_tokens are required"))
		s.setOutcome(rec, OutcomeStatus)
		return
	}
	var content json.RawMessage
	if hook != nil {
		content = hook(body)
	}
	if content == nil {
		content = json.RawMessage(mustJSON([]map[string]string{{"type": "text", "text": s.messagesText(rec, req, body)}}))
	}
	var blocks []map[string]any
	if err := json.Unmarshal(content, &blocks); err != nil {
		panic(fmt.Sprintf("fakeupstream: messages hook 返回的 content 不合法: %v", err))
	}
	stop := "end_turn"
	for _, b := range blocks {
		if b["type"] == "tool_use" {
			stop = "tool_use"
		}
	}
	usage := anthropicUsage{InputTokens: int64(len(body)+3) / 4, OutputTokens: int64(len(content)+3) / 4,
		CacheCreationInputTokens: write, CacheReadInputTokens: read}
	id := fmt.Sprintf("msg_fake_%d", rec.Seq)
	if req.Stream {
		s.streamMessages(w, r, rec, id, req.Model, blocks, stop, usage)
		return
	}
	w.Header().Set("Request-Id", fmt.Sprintf("fake-req-%d", rec.Seq))
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "type": "message", "role": "assistant", "model": req.Model,
		"content": blocks, "stop_reason": stop, "stop_sequence": nil, "usage": usage})
	s.setOutcome(rec, OutcomeOK)
}

// streamMessages 以 Anthropic 的事件序列发出回复：message_start（输入用量，output_tokens 为 1）、每个内容块的
// content_block_start / content_block_delta（text_delta、input_json_delta、thinking_delta、signature_delta）/
// content_block_stop、第一个块开始后一个 ping、message_delta（stop_reason 与累计 output_tokens）、message_stop。
func (s *Server) streamMessages(w http.ResponseWriter, r *http.Request, rec *Request, id, model string, blocks []map[string]any, stop string, usage anthropicUsage) {
	var frames []frame
	add := func(v map[string]any) { frames = append(frames, frame{event: v["type"].(string), data: mustJSON(v)}) }
	start := usage
	start.OutputTokens = 1
	add(map[string]any{"type": "message_start", "message": map[string]any{"id": id, "type": "message", "role": "assistant",
		"model": model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": start}})
	for i, b := range blocks {
		cb, deltas := blockEvents(b)
		add(map[string]any{"type": "content_block_start", "index": i, "content_block": cb})
		if i == 0 {
			add(map[string]any{"type": "ping"})
		}
		for _, d := range deltas {
			add(map[string]any{"type": "content_block_delta", "index": i, "delta": d})
		}
		add(map[string]any{"type": "content_block_stop", "index": i})
	}
	add(map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": map[string]int64{"output_tokens": usage.OutputTokens}})
	add(map[string]any{"type": "message_stop"})
	st := s.newStream(w, r, rec, "event: error\ndata: "+mustJSON(anthropicErrBody(529, "Overloaded"))+"\n\n")
	if st.sendAll(frames) {
		s.setOutcome(rec, OutcomeOK)
	}
}

// blockEvents 返回一个内容块的 content_block_start 内容与各个 delta。
func blockEvents(b map[string]any) (map[string]any, []map[string]any) {
	var deltas []map[string]any
	switch typ, _ := b["type"].(string); typ {
	case "text":
		text, _ := b["text"].(string)
		for _, p := range pieces(text, 8) {
			deltas = append(deltas, map[string]any{"type": "text_delta", "text": p})
		}
		return map[string]any{"type": "text", "text": ""}, deltas
	case "tool_use":
		for _, p := range pieces(mustJSON(b["input"]), 5) {
			deltas = append(deltas, map[string]any{"type": "input_json_delta", "partial_json": p})
		}
		return map[string]any{"type": "tool_use", "id": b["id"], "name": b["name"], "input": map[string]any{}}, deltas
	case "thinking":
		th, _ := b["thinking"].(string)
		for _, p := range pieces(th, 8) {
			deltas = append(deltas, map[string]any{"type": "thinking_delta", "thinking": p})
		}
		if sig, _ := b["signature"].(string); sig != "" {
			deltas = append(deltas, map[string]any{"type": "signature_delta", "signature": sig})
		}
		return map[string]any{"type": "thinking", "thinking": "", "signature": ""}, deltas
	default:
		panic("fakeupstream: 不支持的内容块类型 " + typ)
	}
}
