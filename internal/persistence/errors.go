// Package persistence 定义持久化层对消费者公开的错误契约（规格 §7.3、§14.5）。
//
// 本包不依赖任何数据库驱动；消费者（api、task、runner、resource 等）可以导入它来识别
// 事务用例返回的错误类别，而不必依赖 internal/persistence/postgres。
package persistence

import (
	"errors"
	"fmt"
)

var (
	// ErrContention 表示锁等待超时，或可重试的中止在重跑后仍失败。单次操作失败，
	// 不计入 Store 故障阈值。
	ErrContention = errors.New("persistence: 锁争用")
	// ErrUnavailable 表示连接失败或 deadline 到期，且已确认事务未提交。计入 Store 故障阈值。
	ErrUnavailable = errors.New("persistence: 存储不可用")
	// ErrCommitUnknown 表示提交结果未知。具体错误为 *CommitUnknownError，携带操作身份。
	ErrCommitUnknown = errors.New("persistence: 提交结果未知")
	// ErrConflict 表示同一身份已存在且内容不同，或 CAS 前置条件不成立。
	ErrConflict = errors.New("persistence: 冲突")
	// ErrNotFound 表示查询的对象不存在。对提交结果未知的操作而言，查不到仍是"未知"，
	// 不代表没有提交（设计 §2.3）。
	ErrNotFound = errors.New("persistence: 不存在")
	// ErrOwnershipLost 表示已失去数据库所有权（advisory lock），此后不再执行业务操作。
	ErrOwnershipLost = errors.New("persistence: 已失去所有权")
	// ErrRejected 表示事务内读取的最新事实不满足用例的前置条件（规格 §5.5、§8.1）。
	// 具体错误为 *RejectedError，Code 说明原因；不重跑，不计入故障阈值。
	ErrRejected = errors.New("persistence: 前置条件不满足")
	// ErrInvalid 表示调用方传入了不合法的参数（编程错误或未校验的外部输入）；不重跑，不计入故障阈值。
	ErrInvalid = errors.New("persistence: 参数不合法")
)

// RejectedError 的原因码。stale_attempt 与 control_changed 表示调用方依据的事实已过期：
// 提交者不再是当前 attempt 时应停止写入；控制版本变化时应按最新控制重算后再提交。
const (
	CodeTaskEnded          = "task_ended"           // 任务已终态，不再接受控制
	CodeCancelPending      = "cancel_pending"       // 已接受的 cancel 不可被 pause/resume 覆盖
	CodeNotPaused          = "not_paused"           // resume 要求任务处于 paused
	CodeNotRunnable        = "not_runnable"         // 创建 attempt 要求 queued 且 desired = run
	CodePreviousNotStopped = "previous_not_stopped" // 旧执行的环境尚未确认停止
	CodeStaleAttempt       = "stale_attempt"        // 提交者不是任务的当前 attempt，或其访问已撤销
	CodeControlChanged     = "control_changed"      // 判决依据的控制版本已不是最新
	CodeRefNotAuthorized   = "ref_not_authorized"   // 引用的 blob 不存在或未授权到当前 scope
)

// 会话 turn 的原因码（规格 §8.1、§12.3；M4 Plan 12）。
const (
	// CodeSessionBlocked：会话被另一个 turn 阻塞（blocked_by_task_id），或已有另一个运行中的 turn，或会话不在
	// idle/running——会话 turn 的 attempt 创建须等待。
	CodeSessionBlocked = "session_blocked"
	// CodeIncarnationNotIdle：授予的 incarnation 不是会话的当前 incarnation，或不处于 idle（上一 turn 尚未释放）。
	CodeIncarnationNotIdle = "incarnation_not_idle"
	// CodeSessionBaseMismatch：turn 的 base_session_checkpoint_id 不等于会话当前指针（§12.3），成功裁决不能提交。
	CodeSessionBaseMismatch = "session_base_mismatch"
)

// Gateway 事务用例（gateway/call 的 Store）的原因码（规格 §9.2、§9.4–§9.7）。
const (
	CodeAccessRevoked        = "access_revoked"                  // attempt_access.state ≠ active
	CodeNotCurrentAttempt    = "not_current_attempt"             // 调用方不是任务的当前 attempt
	CodeCancelRequested      = "cancel_requested"                // task_control.desired = cancel
	CodeBudgetExhausted      = "budget_exhausted"                // 预算可用 ≤ 0
	CodeBudgetInsufficient   = "budget_insufficient_for_request" // 可用 > 0 但小于本次估算
	CodeTriesExhausted       = "tries_exhausted"                 // 累计 try 已达上限
	CodeCallDeadlineExceeded = "call_deadline_exceeded"          // 数据库时间已到 deadline_at
	CodeFingerprintMismatch  = "fingerprint_mismatch"            // 同一 call_id 的指纹不同
	CodeCallInProgress       = "call_in_progress"                // 该调用已有执行中的 try 或仍在解析
	CodeToolBudgetExhausted  = "tool_budget_exhausted"           // 搜索/抓取的每 turn 工具调用额度已用完（429，不可重试）
)

// sub-run 用例的原因码（规格 §8.4、§9.2、§9.6、§13；M4 Plan 14）。值与协议 subrun_started.code 及 Gateway 的
// 错误码一致；invalid_transition 与 session.CodeInvalidTransition 同值。
const (
	CodeConflict              = "conflict"                // 同一 sub-run ID 的定义不同
	CodeSubrunClosed          = "subrun_closed"           // sub-run 已终态，或不是 started / 未绑定当前 attempt
	CodeSubrunBudgetExhausted = "subrun_budget_exhausted" // sub-run 层可用 ≤ 0
	CodeSubrunLimit           = "subrun_limit"            // 该任务已有 4 个逻辑 sub-run
	CodeInvalidTransition     = "invalid_transition"      // 状态机不允许的转换（含 checkpoint 列出未知 sub-run）
)

// RejectedError 携带前置条件不满足的原因。
type RejectedError struct {
	Code   string
	Detail string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("persistence: %s: %s", e.Code, e.Detail)
}

// Is 使 errors.Is(err, ErrRejected) 成立。
func (e *RejectedError) Is(target error) bool { return target == ErrRejected }

// CommitUnknownError 携带提交结果未知的操作及其身份，供调用方之后以同一身份核对或重试。
type CommitUnknownError struct {
	Op       string // 事务用例名，例如 "CreateAttempt"
	Identity string // 操作身份，例如 attempt_id
	Err      error  // 最后一次观察到的底层错误
}

func (e *CommitUnknownError) Error() string {
	return fmt.Sprintf("persistence: %s(%s) 提交结果未知: %v", e.Op, e.Identity, e.Err)
}

// Is 使 errors.Is(err, ErrCommitUnknown) 成立。
func (e *CommitUnknownError) Is(target error) bool { return target == ErrCommitUnknown }

func (e *CommitUnknownError) Unwrap() error { return e.Err }

// CountsTowardFailureThreshold 报告该错误是否计入 Store 故障阈值（规格 §14.5）。
func CountsTowardFailureThreshold(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrCommitUnknown)
}
