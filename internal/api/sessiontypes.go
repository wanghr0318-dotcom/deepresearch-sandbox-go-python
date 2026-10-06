package api

import (
	"encoding/json"
	"time"
)

// 会话 API 的对外类型（M4 Plan 12）。api/openapi.yaml 是唯一契约：SessionView、TurnView、SessionEvent 与其中的
// Session、Turn、SessionEvent 一一对应，字段改名或增减须先改契约。

// SessionView 是一个会话的对外视图（OpenAPI Session）。
type SessionView struct {
	SessionID     string    `json:"session_id"`
	Title         string    `json:"title"`
	State         string    `json:"state"`
	LastActiveAt  time.Time `json:"last_active_at"`
	CreatedAt     time.Time `json:"created_at"`
	OwnerUserID   int64     `json:"-"`
	Owner         string    `json:"owner,omitempty"`          // 仅运维视图
	InternalState string    `json:"internal_state,omitempty"` // 仅运维视图：sessions.status 原值
}

// ReportRef 指向 turn 登记的报告产物（经 /tasks/{turn_id}/artifacts/{artifact_id} 下载）。
type ReportRef struct {
	ArtifactID string `json:"artifact_id"`
	Version    int64  `json:"version"`
}

// TurnView 是会话中一轮的对外视图（OpenAPI Turn）；turn 即 task，TurnID 为 task_id。
type TurnView struct {
	TurnID             string     `json:"turn_id"`
	TurnIndex          int64      `json:"turn_index"`
	Text               string     `json:"text"`
	DeepResearch       bool       `json:"deep_research"`
	Status             string     `json:"status"`
	StatusReason       string     `json:"status_reason,omitempty"`
	Route              string     `json:"route,omitempty"`
	RestoredFromTurnID string     `json:"restored_from_turn_id,omitempty"`
	Restorable         bool       `json:"restorable"`
	ToolCallsUsed      int64      `json:"tool_calls_used"`
	ToolCallLimit      int64      `json:"tool_call_limit"`
	Summary            string     `json:"summary,omitempty"`
	Report             *ReportRef `json:"report,omitempty"`
	UserMessage        string     `json:"user_message,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

// SessionEventRecord 是会话流中的一条存储记录：Source 为 host、worker（events 表）或 session（session_events 表）；
// TaskID、TaskSeq、AttemptID 只在前两者非空。session 记录的 Type 为 session_state，Payload 为
// {state, user_message?}（state 为 sessions.status 的新值）。
type SessionEventRecord struct {
	SessionSeq int64
	TaskID     string
	TaskSeq    int64
	AttemptID  string
	Source     string
	Type       string
	Payload    json.RawMessage
	TS         time.Time
}

// SessionEventInternal 是只在运维视图中附带的原始记录。
type SessionEventInternal struct {
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	AttemptID string          `json:"attempt_id,omitempty"`
	TaskSeq   int64           `json:"task_seq,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

// SessionEvent 是整会话 SSE 的一条事件（OpenAPI SessionEvent）：SSE id = Seq，event = Type，data = 本结构的 JSON。
type SessionEvent struct {
	Seq      int64                 `json:"seq"`
	TurnID   string                `json:"turn_id,omitempty"`
	Type     string                `json:"type"`
	TS       time.Time             `json:"ts"`
	Data     json.RawMessage       `json:"data"`
	Internal *SessionEventInternal `json:"internal,omitempty"`
}

// 事件类型（与 OpenAPI 的 SessionEventType 枚举一致）。前三种与 turn_result 由宿主记录产生，其余为 Worker
// progress 的 kind（progress.data 即事件 data）。SEvInternal 只出现在运维视图：映射不到对外类型的记录。
const (
	SEvSessionState   = "session_state"
	SEvTurnCreated    = "turn_created"
	SEvTurnStatus     = "turn_status"
	SEvRoute          = "route"
	SEvSkillRead      = "skill_read"
	SEvThinking       = "thinking"
	SEvAskUser        = "ask_user"
	SEvTodoUpdated    = "todo_updated"
	SEvSubtopic       = "subtopic"
	SEvToolCall       = "tool_call"
	SEvToolResult     = "tool_result"
	SEvBudget         = "budget"
	SEvAssistantDelta = "assistant_delta"
	SEvReportReady    = "report_ready"
	SEvTurnStopped    = "turn_stopped"
	SEvTurnResult     = "turn_result"
	SEvInternal       = "internal"
)

// SessionEventTypes 是 SessionEvent.type 的全集（含运维专用的 internal）。
var SessionEventTypes = []string{
	SEvSessionState, SEvTurnCreated, SEvTurnStatus, SEvRoute, SEvSkillRead, SEvThinking, SEvAskUser,
	SEvTodoUpdated, SEvSubtopic, SEvToolCall, SEvToolResult, SEvBudget, SEvAssistantDelta, SEvReportReady,
	SEvTurnStopped, SEvTurnResult, SEvInternal,
}

// userSessionStates 是 Session.state 的取值（用户与运维视图相同；运维另见 internal_state）。
var userSessionStates = []string{"idle", "running", "frozen", "evicted", "restoring", "closing", "closed"}

// userTurnStatuses 是 Turn.status 与 turn_status 事件 status 的取值。
var userTurnStatuses = []string{"queued", "running", "stopping", "paused", "awaiting_input", "succeeded", "failed", "cancelled"}
