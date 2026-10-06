package api

import (
	"context"
	"encoding/json"
	"errors"
)

// 会话 API 的持久化用例（M4 Plan 12；代码组织设计 §4）；实现位于 internal/persistence/postgres。

// 会话与 turn 用例的业务拒绝（HTTP 409；见 api/openapi.yaml 的 SessionErrorCode）。
var (
	ErrSessionClosed    = errors.New("api: 会话已关闭")
	ErrTurnInProgress   = errors.New("api: 会话中有进行中的 turn")
	ErrInvalidTurnState = errors.New("api: turn 状态不允许该操作")
	ErrNotRestorable    = errors.New("api: turn 不可恢复")
)

// FailedTurnMessage 是 failed 的 turn 面向用户的固定文案（Turn.user_message；与 turn_status 事件一致）。
const FailedTurnMessage = failedTurnMessage

// CreateSessionRequest 是创建会话的输入。SessionID 由调用方在事务前生成。
type CreateSessionRequest struct {
	RequestID   string
	BodyHash    []byte
	SessionID   string
	OwnerUserID int64 // 0 = 运维创建（无主）
	Title       string
}

// CreateTurnRequest 是向会话发送一条消息（或恢复一个 turn）的输入。TaskID 由调用方在事务前生成。
type CreateTurnRequest struct {
	RequestID, SessionID, TaskID string
	BodyHash                     []byte
	OwnerUserID                  int64
	Text                         string
	DeepResearch                 bool
	Spec, Limits                 json.RawMessage // 由 TurnSpec 生成；Limits 含 max_tool_calls
	ConfigVersion                string
	MaxFaultRetries              int64
	RestoredFromTaskID           string // 非空 = POST /turns/{id}/restore
	// Operator 为运维路径（POST /tasks 带 session_id）：不比较 OwnerUserID 与会话所有者，turn 归会话所有者；
	// Research 覆盖服务端生成的 spec.research（只在 api 层合并进 Spec）。
	Operator bool
	Research json.RawMessage
}

// CreateTurnResult 是 CreateTurn 的结果（OpenAPI MessageResult）。
type CreateTurnResult struct {
	TurnID           string `json:"turn_id"`
	TurnIndex        int64  `json:"turn_index"`
	SupersededTurnID string `json:"superseded_turn_id,omitempty"`
	Replayed         bool   `json:"-"`
}

// TurnControlRequest 是对一个 turn 的控制：stop | continue | finish | answer。
type TurnControlRequest struct {
	RequestID string
	BodyHash  []byte
	TaskID    string
	Action    string          // stop | continue | finish | answer
	Answers   json.RawMessage // 仅 answer
}

// Sessions 是会话 API 的窄接口。写用例以 request_id 幂等（同 CreateTask）；会话或 turn 不存在为
// persistence.ErrNotFound。
type Sessions interface {
	CreateSession(ctx context.Context, req CreateSessionRequest) (SessionView, bool, error)
	// ListSessions：ownerID > 0 只含该用户且不含 closed；ownerID = 0 为全部（运维），Owner 填用户名。按
	// last_active_at 倒序 keyset：after 为上一页最后一个 session_id；返回的游标在没有下一页时为空。
	ListSessions(ctx context.Context, ownerID int64, after string, limit int) ([]SessionView, string, error)
	GetSession(ctx context.Context, sessionID string) (SessionView, error)
	// RenameSession 写入标题（title_source = user）；会话关闭中或已关闭为 ErrSessionClosed。
	RenameSession(ctx context.Context, sessionID, title string) (SessionView, error)
	// CloseSession（幂等）：session_control.desired=closed、control_version+1；同事务对每个非终态 turn 写 cancel 控制
	// （reason session_closed，已为 cancel 的不重复）。已 closed 返回原视图。
	CloseSession(ctx context.Context, sessionID string) (SessionView, error)
	// WakeSession 请求唤醒（wake_requested_version 前进）；关闭中或已关闭为 ErrSessionClosed。
	WakeSession(ctx context.Context, requestID string, bodyHash []byte, sessionID string) error
	// CreateTurn 见 Plan 12 Task 3：D5 取代暂停中的 turn、turn_in_progress、每用户 1 个运行中、恢复种子。
	CreateTurn(ctx context.Context, req CreateTurnRequest) (CreateTurnResult, error)
	// ListTurns 按 turn_index 升序返回 turn_index > afterIndex 的至多 limit 个 turn（afterIndex < 0 为从头）。
	ListTurns(ctx context.Context, sessionID string, afterIndex int64, limit int) ([]TurnView, error)
	// TurnSession 返回 turn 所属会话与所有者；不是会话 turn 或不存在为 persistence.ErrNotFound。
	TurnSession(ctx context.Context, taskID string) (sessionID string, ownerID int64, err error)
	// TurnControl 写入 turn 的控制意图：stop = pause（要求 queued|running）；continue/finish = run（要求 paused 且
	// 不在等待回答），answer = run（要求 paused 且等待回答），并写 resume_directive；不满足为 ErrInvalidTurnState；
	// 恢复会使该用户超过 1 个运行中时为 ErrUserTaskRunning。
	TurnControl(ctx context.Context, req TurnControlRequest) (ControlResult, error)
	// ListSessionEvents 按 session_seq 升序返回 afterSeq 之后的至多 limit 条记录（会话 task 事件与生命周期事件）。
	ListSessionEvents(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]SessionEventRecord, error)
	// TurnBlobAuthorized：sha 授权到该 turn 的 task scope 或其会话 scope。
	TurnBlobAuthorized(ctx context.Context, taskID, sha string) (bool, error)
}
