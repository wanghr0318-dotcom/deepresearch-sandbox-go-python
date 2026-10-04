// Package task 是任务状态机与 task actor。本文件声明 task actor 需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package task

import (
	"context"
	"encoding/json"
	"time"
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
	// LoadTask 一次读取 Decide 需要的任务事实，含最新已提交 checkpoint 的完整内容（用于 init.resume）。
	LoadTask(ctx context.Context, taskID string) (TaskState, error)
	// ListActiveTasks 返回非终态任务的 ID（actor 启动时据此建立 actor）。
	ListActiveTasks(ctx context.Context) ([]string, error)
	// PersistRunTime 把任务累计运行时间推进到 totalMs（单调：取较大者，因而提交结果未知后的重跑不重复累计）；
	// 只接受当前 attempt，否则 stale_attempt（规格 §14.4）。返回持久化后的累计值。
	PersistRunTime(ctx context.Context, taskID, attemptID string, totalMs int64) (int64, error)
	// RevokeAttemptAccess 把 attempt 的访问置为 revoked（幂等）；此后该 attempt 的提交以 stale_attempt 拒绝。
	RevokeAttemptAccess(ctx context.Context, attemptID, reason string) error
}

// RetryKind 是创建 attempt 的重试类别（规格 §8.1"重试计数"、§14.2）。
type RetryKind string

const (
	RetryNone  RetryKind = ""      // 首个 attempt 或暂停恢复：只增加 attempts_total
	RetryFault RetryKind = "fault" // 故障重试：同事务递增 fault_retries_used
	RetryOOM   RetryKind = "oom"   // OOM 重试：同事务递增 oom_retries_used
)

// NewAttempt 是创建 attempt 的输入；AttemptID 与 EnvID 由调用方在事务前生成。
type NewAttempt struct {
	TaskID    string
	AttemptID string
	AttemptNo int64
	EnvID     string
	Retry     RetryKind
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
	NotBefore        *time.Time // 仅 TaskStatus = queued（故障重试）时可设置：下一次 attempt 的最早时间
}

// TaskState 是 LoadTask 读取的任务事实。
type TaskState struct {
	TaskID, Status, StatusReason, CurrentAttemptID string
	Spec, Limits                                   json.RawMessage
	ConfigVersion                                  string
	Desired                                        string
	ControlVersion, AppliedControlVersion          int64
	AttemptsTotal, FaultRetriesUsed                int64
	MaxFaultRetries, OOMRetriesUsed, RunTimeMs     int64
	NotBefore                                      *time.Time
	Latest                                         *LatestCheckpoint // 最新已提交 checkpoint；无则 nil
}

// LatestCheckpoint 是最新已提交 checkpoint 的完整内容（init.resume 所需）。
type LatestCheckpoint struct {
	CheckpointID, StepID string
	State                json.RawMessage
	StateRef             string
	Refs                 []string
}
