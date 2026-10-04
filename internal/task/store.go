// Package task 是任务状态机与 task actor（后续计划实现）。本文件声明 task actor 需要的
// 持久化用例（代码组织设计 §4）；实现位于 internal/persistence/postgres。
package task

import (
	"context"
	"encoding/json"
)

// Store 是 task actor 的窄接口。每个方法是一个完整、幂等的事务用例：
// 提交结果未知时，实现会在同一个整体 deadline 内以同一身份重跑；仍无结论时返回
// *persistence.CommitUnknownError（设计 §2.3）。
type Store interface {
	// CreateAttempt 以事务前生成的 attempt_id 创建 attempt、其访问权与任务环境记录。
	CreateAttempt(ctx context.Context, a NewAttempt) (Attempt, error)
	// GetAttempt 读取 attempt；不存在为 persistence.ErrNotFound。
	GetAttempt(ctx context.Context, attemptID string) (Attempt, error)
	// ApplyControl 以 applied_control_version 的 CAS 应用控制意图。
	ApplyControl(ctx context.Context, c ApplyControl) (ControlState, error)
	// GetControlState 读取任务的控制版本与已应用版本。
	GetControlState(ctx context.Context, taskID string) (ControlState, error)
	// FinalizeAttempt 提交 attempt 的判决、任务状态与终态事件；完整判决内容一致才视为同一次提交。
	FinalizeAttempt(ctx context.Context, v Verdict) (Attempt, error)
}

// NewAttempt 是创建 attempt 的输入；AttemptID 与 EnvID 由调用方在事务前生成。
type NewAttempt struct {
	TaskID    string
	AttemptID string
	AttemptNo int64
	EnvID     string
}

// Attempt 是 attempts 中的一行。
type Attempt struct {
	AttemptID    string
	TaskID       string
	AttemptNo    int64
	EnvID        string
	Status       string
	OutcomeClass string
	VerdictHash  []byte
}

// ApplyControl 把任务的 applied_control_version 推进到 ControlVersion，并写入新的任务状态。
type ApplyControl struct {
	TaskID         string
	ControlVersion int64
	Status         string
	StatusReason   string
}

// ControlState 是任务的控制状态。
type ControlState struct {
	TaskID                string
	Desired               string
	ControlVersion        int64
	AppliedControlVersion int64
	Status                string
}

// Verdict 是 attempt 的完整判决。FromStatus 是 CAS 的来源状态，不属于判决内容；
// 其余字段共同决定 verdict_hash。
type Verdict struct {
	AttemptID        string
	TaskID           string
	ControlVersion   int64 // 计算判决时读取的 task_control.control_version；提交时不是最新则拒绝（control_changed）
	FromStatus       string
	AttemptStatus    string
	OutcomeClass     string
	ExitCode         *int64
	ExitSignal       *int64
	OOMKillDelta     int64
	PlatformKilled   bool
	TaskStatus       string
	TaskStatusReason string
	Result           json.RawMessage
	EventType        string
	EventPayload     json.RawMessage
}
