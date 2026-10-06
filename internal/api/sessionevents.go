package api

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
)

// 会话事件的对外映射与原文脱敏（M4 Plan 12；契约见 api/openapi.yaml 的 SessionEvent）。
//
// 用户（会话所有者）看到的每种事件只保留 sessionEventDataFields 列出的 data 顶层字段，并在任意深度去掉内部键
// （internalKey：费用、价格、用量、模型、调用与 attempt 等内部 ID）；raw 子对象只保留 request、
// request_truncated、response_ref。运维额外得到原始记录（Internal），映射不到对外类型的记录以 type = internal 返回。

// failedTurnMessage 是 turn 失败时面向用户的固定文案（不暴露内部失败原因的细节）。
const failedTurnMessage = "模型服务暂时不可用，请重试"

// sessionEventDataFields 是每种对外事件 data 的字段允许列表（与 OpenAPI 的 <Type>Data 模式字段一致）。
var sessionEventDataFields = map[string][]string{
	SEvSessionState:   {"state", "user_message"},
	SEvTurnCreated:    {"turn_index", "text", "deep_research", "restored_from_turn_id"},
	SEvTurnStatus:     {"status", "reason", "user_message"},
	SEvRoute:          {"route", "forced"},
	SEvSkillRead:      {"step_id", "name", "description", "file"},
	SEvThinking:       {"step_id", "text", "raw"},
	SEvAskUser:        {"step_id", "question_id", "questions"},
	SEvTodoUpdated:    {"items"},
	SEvSubtopic:       {"id", "title", "status", "summary"},
	SEvToolCall:       {"step_id", "tool_call_id", "tool", "input", "subtopic_id", "raw"},
	SEvToolResult:     {"step_id", "tool_call_id", "tool", "ok", "subtopic_id", "preview", "error", "raw"},
	SEvBudget:         {"used", "limit"},
	SEvAssistantDelta: {"message_id", "text", "final"},
	SEvReportReady:    {"artifact_id", "version", "title", "partial", "tool_budget_reached", "note"},
	SEvTurnStopped:    {"card", "findings", "can_finish"},
	SEvTurnResult:     {"summary", "outputs"},
}

// progressEventTypes 是来自 Worker progress 的事件类型：progress.kind 即事件 type，progress.data 即 data。
// 其他 kind 对用户不可见。
var progressEventTypes = []string{
	SEvRoute, SEvSkillRead, SEvThinking, SEvAskUser, SEvTodoUpdated, SEvSubtopic, SEvToolCall, SEvToolResult,
	SEvBudget, SEvAssistantDelta, SEvReportReady, SEvTurnStopped,
}

// sessionEventStates 是 session_state 事件对用户可见的会话状态；过渡态（creating、quiescing、evicting、
// running、closing）不产生用户事件。
var sessionEventStates = []string{"idle", "frozen", "evicted", "restoring", "closed"}

// internalKey 报告对象键是否为用户不可见的内部字段（任意深度）。
func internalKey(k string) bool {
	switch k {
	case "usage", "model", "call_id", "attempt_id", "worker_seq", "upstream_request_id", "system_fingerprint":
		return true
	}
	return strings.HasPrefix(k, "cost") || strings.Contains(k, "price")
}

// ToSessionEvent 把存储记录映射为对外事件；ok=false 表示该记录对用户（admin=false）不可见（游标照常前进）。
// admin=true 时附 Internal，映射不到对外 type 的记录也返回（type = internal，data = {}）。
func ToSessionEvent(r SessionEventRecord, admin bool) (SessionEvent, bool) {
	ev := SessionEvent{Seq: r.SessionSeq, TurnID: r.TaskID, TS: r.TS}
	typ, data, ok := userEventData(r)
	if ok {
		b, err := encodeJSON(data)
		if err != nil {
			ok = false
		} else {
			ev.Type, ev.Data = typ, b
		}
	}
	if !admin {
		return ev, ok
	}
	if !ok {
		ev.Type, ev.Data = SEvInternal, json.RawMessage(`{}`)
	}
	ev.Internal = &SessionEventInternal{Source: r.Source, Type: r.Type, AttemptID: r.AttemptID, TaskSeq: r.TaskSeq, Payload: r.Payload}
	return ev, true
}

// userEventData 返回记录的对外类型与已过滤的 data；记录对用户不可见时 ok 为 false。
func userEventData(r SessionEventRecord) (string, map[string]any, bool) {
	p, _ := decodeJSON(r.Payload) // payload 不是对象时按空对象处理
	obj, _ := p.(map[string]any)
	switch r.Source + "/" + r.Type {
	case "session/" + SEvSessionState:
		state := str(obj, "state")
		if !slices.Contains(sessionEventStates, state) {
			return "", nil, false
		}
		data := map[string]any{"state": state}
		if m := str(obj, "user_message"); m != "" {
			data["user_message"] = m
		}
		return SEvSessionState, data, true
	case "host/task_created":
		return SEvTurnCreated, pick(obj, SEvTurnCreated), true
	case "host/control_applied":
		return turnStatusData(str(obj, "status"), "")
	case "host/attempt_ended", "host/" + EventTaskTerminal:
		return turnStatusData(str(obj, "task_status"), str(obj, "status_reason"))
	case "worker/progress":
		kind := str(obj, "kind")
		if !slices.Contains(progressEventTypes, kind) {
			return "", nil, false
		}
		d, _ := obj["data"].(map[string]any)
		if d == nil {
			d = map[string]any{}
		}
		// step_id 在 progress 信封上；契约把它放进 data。
		if _, has := d["step_id"]; !has && slices.Contains(sessionEventDataFields[kind], "step_id") {
			if s := str(obj, "step_id"); s != "" {
				d["step_id"] = s
			}
		}
		return kind, pick(d, kind), true
	case "worker/result":
		return SEvTurnResult, pick(obj, SEvTurnResult), true
	}
	return "", nil, false
}

func turnStatusData(taskStatus, reason string) (string, map[string]any, bool) {
	s := UserTurnStatus(taskStatus, reason)
	if !slices.Contains(userTurnStatuses, s) {
		return "", nil, false
	}
	data := map[string]any{"status": s}
	if reason != "" && s != "awaiting_input" {
		data["reason"] = reason
	}
	if s == "failed" {
		data["user_message"] = failedTurnMessage
	}
	return SEvTurnStatus, data, true
}

// UserTurnStatus 把 tasks.status/status_reason 映射为 Turn.status：pausing、cancelling → stopping；
// paused + awaiting_input → awaiting_input；其余同名。
func UserTurnStatus(taskStatus, statusReason string) string {
	switch {
	case taskStatus == "pausing" || taskStatus == "cancelling":
		return "stopping"
	case taskStatus == "paused" && statusReason == "awaiting_input":
		return "awaiting_input"
	}
	return taskStatus
}

// UserSessionState 把 sessions.status 映射为用户视图 state：creating、quiescing → idle；evicting → frozen；其余同名。
func UserSessionState(status string) string {
	switch status {
	case "creating", "quiescing":
		return "idle"
	case "evicting":
		return "frozen"
	}
	return status
}

// RedactRaw 对 ⟨/⟩ 原文脱敏：JSON 时递归去掉内部键（见 internalKey）与顶层 id，其余内容（含数字精度与
// 字符）保持不变；非 JSON 原样返回（以附件下载）。
func RedactRaw(b []byte) []byte {
	v, ok := decodeJSON(b)
	if !ok {
		return b
	}
	if m, isObj := v.(map[string]any); isObj {
		delete(m, "id")
	}
	out, err := encodeJSON(strip(v))
	if err != nil {
		return []byte("{}") // 解码得到的值总能编码；万一失败也不返回未脱敏的原文
	}
	return out
}

// pick 保留 typ 允许的顶层字段并去掉其中的内部键；raw 按 rawRef 处理。
func pick(obj map[string]any, typ string) map[string]any {
	out := map[string]any{}
	for _, f := range sessionEventDataFields[typ] {
		v, ok := obj[f]
		if !ok || v == nil { // null 等同缺省：契约中的可选字段不取 null
			continue
		}
		if f == "raw" {
			if r, ok := rawRef(v); ok {
				out[f] = r
			}
			continue
		}
		out[f] = strip(v)
	}
	return out
}

// rawRef 只保留原始请求（内联，递归去掉内部键）、截断标记与响应 blob 的 sha256。
func rawRef(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	if req, ok := m["request"]; ok {
		out["request"] = strip(req)
	}
	for _, k := range []string{"request_truncated", "response_ref"} {
		if x, ok := m[k]; ok {
			out[k] = x
		}
	}
	return out, len(out) > 0
}

// strip 递归去掉内部键。
func strip(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, c := range x {
			if !internalKey(k) {
				out[k] = strip(c)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, c := range x {
			out[i] = strip(c)
		}
		return out
	}
	return v
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

// decodeJSON 解码一个 JSON 值（数字保持原文，不经 float64）；不是合法 JSON 时 ok 为 false。
func decodeJSON(b []byte) (any, bool) {
	if !json.Valid(b) {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	return v, true
}

// encodeJSON 编码且不转义 HTML 字符（<、>、& 原样保留）。
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
