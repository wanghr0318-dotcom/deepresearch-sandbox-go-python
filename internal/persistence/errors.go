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
