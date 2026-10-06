// Package session 是规格 §12 的会话：每会话一个长期 incarnation，冻结、驱逐、恢复与关闭（M4 Plan 12）。
// 本文件声明 session actor 需要的持久化用例（代码组织设计 §4）；实现位于 internal/persistence/postgres。
package session

import (
	"context"
	"encoding/json"
	"time"
)

// 会话状态（规格 §12.2）。
const (
	StatusCreating  = "creating"
	StatusIdle      = "idle"
	StatusRunning   = "running"
	StatusQuiescing = "quiescing"
	StatusFrozen    = "frozen"
	StatusEvicting  = "evicting"
	StatusEvicted   = "evicted"
	StatusRestoring = "restoring"
	StatusClosing   = "closing"
	StatusClosed    = "closed"
)

// incarnation 状态。
const (
	IncStarting  = "starting"
	IncIdle      = "idle"
	IncBusy      = "busy"
	IncReleasing = "releasing"
	IncQuiescing = "quiescing"
	IncFrozen    = "frozen"
	IncEnded     = "ended"
)

// 拒绝原因码（persistence.RejectedError.Code）。
const (
	// CodeInvalidTransition：当前状态不在转换的来源集合中。
	CodeInvalidTransition = "invalid_transition"
	// CodeCloseNotReady：closing → closed 的条件（turn 全部终态、incarnation 已结束且环境已停止）尚未满足。
	CodeCloseNotReady = "close_not_ready"
)

// EndLostOnRestart 是 server 重启时结束 incarnation 的 end_reason（规格 §12.6）。
const EndLostOnRestart = "lost_on_restart"

// Checkpoint 是会话最新已提交的 session checkpoint（checkpoints 表 scope = session）。
type Checkpoint struct {
	CheckpointID string
	CommitSeq    int64
	State        json.RawMessage
	StateRef     string
	Refs         []string
}

// Incarnation 是一个 incarnation 行。
type Incarnation struct {
	IncarnationID, SessionID, EnvID, Status string
	StartedAt                               time.Time
	EndedAt                                 *time.Time
	EndReason                               string
}

// State 是 LoadSession 一次读取的会话事实（Decide 的输入）。
type State struct {
	SessionID, Status                        string
	OwnerUserID                              int64
	UIDRangeID                               string
	CurrentIncarnationID, CurrentTaskID      string
	BlockedByTaskID                          string
	IdleSince, FrozenSince                   *time.Time
	LastActiveAt                             time.Time
	RowVersion                               int64
	Desired                                  string // session_control.desired
	ControlVersion                           int64
	WakeRequestedVersion, AppliedWakeVersion int64
	Latest                                   *Checkpoint  // session_progress 指向的 checkpoint
	Incarnation                              *Incarnation // 当前 incarnation（current_incarnation_id；无则 nil）
	NonTerminalTurns                         []TurnFact   // turn_index 升序
}

// TurnFact 是一个非终态 turn（会话 task）的事实。
type TurnFact struct {
	TaskID, Status, StatusReason, Desired string
	TurnIndex                             int64
}

// Transition 以 row_version CAS 改变 status（From 须包含当前状态），同事务：To=idle 时 idle_since=now()，
// To=frozen 时 frozen_since=now()，其余清空二者；To=running 时 last_active_at=now()；SetIncarnation 非 nil 时写
// current_incarnation_id（空串即 NULL）；AppliedWake 非 nil 时写 applied_wake_version；LastError 非 nil 时写 last_error；
// Event 非 nil 时追加 session_events（event_key = "<To>:<row_version+1>"，payload 为 Event）。
type Transition struct {
	SessionID      string
	FromRowVersion int64
	From           []string
	To             string
	SetIncarnation *string
	AppliedWake    *int64
	LastError      *string
	Event          json.RawMessage
}

// NewIncarnation 是 CreateIncarnation 的输入；FromRowVersion 是调用方读到的会话 row_version（CAS 守卫）。
type NewIncarnation struct {
	IncarnationID, SessionID, EnvID string
	FromRowVersion                  int64
}

// LRUCandidate 是内存压力下的驱逐候选（frozen 先于 idle，各自按 last_active_at 升序）。
type LRUCandidate struct {
	SessionID, Status string
	LastActiveAt      time.Time
}

// Store 是 session actor 的窄接口。错误类别见 internal/persistence。
type Store interface {
	LoadSession(ctx context.Context, sessionID string) (State, error)
	// ListOpenSessions 返回 status ≠ closed 的会话（Scheduler 启动时建立 actor）。
	ListOpenSessions(ctx context.Context) ([]string, error)
	// Transition 见 Transition 类型。row_version 不等于 FromRowVersion 为 ErrConflict；当前状态不在 From 中为
	// ErrRejected（CodeInvalidTransition）。同一转换（To 与 FromRowVersion+1 已是当前状态与版本）重跑返回当前事实、
	// 不重复追加事件。
	Transition(ctx context.Context, t Transition) (State, error)
	// CreateIncarnation：会话须无存活 incarnation（否则 ErrConflict）；同事务插入 environments(kind=session,
	// session_id, status=creating) 与 incarnations(starting)，写 current_incarnation_id；首次时分配 uid_range_id 由
	// 调用方经 SetUIDRange 写入。以 incarnation_id 幂等。row_version 只作守卫（不等为 ErrConflict），不递增。
	CreateIncarnation(ctx context.Context, n NewIncarnation) (Incarnation, error)
	// SetUIDRange 写入会话保留的 UID 范围（为空或相同时成功，已是另一个范围为 ErrConflict）。
	SetUIDRange(ctx context.Context, sessionID, uidRangeID string) error
	// SetIncarnationStatus 以 from 集合 CAS；不在 from 中为 ErrRejected（code invalid_transition）。已是 to 时
	// 返回当前行（重跑）。结束 incarnation 用 EndIncarnation。
	SetIncarnationStatus(ctx context.Context, incarnationID string, from []string, to string) (Incarnation, error)
	// EndIncarnation 置 ended、ended_at、end_reason（幂等；end_reason 首次写入为准），若为当前则清空 current_incarnation_id。
	EndIncarnation(ctx context.Context, incarnationID, endReason string) (Incarnation, error)
	LRUCandidates(ctx context.Context, limit int) ([]LRUCandidate, error)
	// FinishClose：全部 turn 终态、当前 incarnation 已 ended 且其环境 stopped_at 已记录时 closing → closed 并追加事件；
	// 条件不满足为 ErrRejected（code close_not_ready）。
	FinishClose(ctx context.Context, sessionID string, fromRowVersion int64) (State, error)
}
