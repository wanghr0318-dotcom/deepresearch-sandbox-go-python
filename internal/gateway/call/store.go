// Package call 是 Gateway 唯一的记账与 journal 所有者（规格 §9.4–§9.7）。
//
// 本文件只声明它对持久化层的窄接口 Store 与相关类型；事务用例由 internal/persistence/postgres 实现，
// 错误契约沿用 internal/persistence（ErrRejected 的原因码见 persistence.Code*）。
package call

import (
	"context"
	"time"
)

// AccessFacts 是规格 §9.2 访问检查所需的事实，在一致快照中读取。
type AccessFacts struct {
	TaskID, AttemptID string
	Active            bool   // attempt_access.state = active
	Current           bool   // tasks.current_attempt_id = AttemptID
	Desired           string // task_control.desired
}

// Budget 是 task 层账本（微美元）。
type Budget struct{ LimitMicro, ReservedMicro, SpentMicro, UnknownMicro int64 }

// Available 返回可用额度：limit − spent − reserved − unknown（规格 §9.6；可为负，即赤字）。
func (b Budget) Available() int64 {
	return b.LimitMicro - b.SpentMicro - b.ReservedMicro - b.UnknownMicro
}

// CallState 是逻辑调用的状态：resolving | in_flight | completed | failed | unknown。
type CallState string

const (
	StateResolving CallState = "resolving"
	StateInFlight  CallState = "in_flight"
	StateCompleted CallState = "completed"
	StateFailed    CallState = "failed"
	StateUnknown   CallState = "unknown"
)

// CallRecord 是 calls 表中的一条 journal 记录。
type CallRecord struct {
	TaskID, CallID, Fingerprint, Endpoint string
	State                                 CallState
	Source                                string // upstream（M2 唯一来源）
	ResultRef                             string // 结果 blob 的 sha256（仅 completed）
	TriesUsed                             int
	CreatedAt, DeadlineAt                 time.Time
	CostCharged                           int64
	FirstAttemptID, UpstreamRequestID     string
	SupersedesCallID, SupersedeReason     string
	PossibleExternalDuplicate             bool
	FailReason                            string // 置为 failed 的原因（FailCall 或 fatal 结算）
	// ResolvingSince 只在 resolving 时可能非空：非空表示某次请求正在解析（重复请求得到 call_in_progress）；
	// 为空表示已由 ResetResolving 复位，下一次 BeginCall 会接管它（Existing=false），期限不变。
	ResolvingSince *time.Time
}

// BeginCallRequest 是 Tx1 的输入。
type BeginCallRequest struct {
	TaskID, CallID, AttemptID, Fingerprint, Endpoint string
	Deadline                                         time.Duration
	SupersedesCallID, SupersedeReason                string
}

// BeginCallResult：Existing 表示已有同 ID 记录（调用方按 §9.4 表处理）。
type BeginCallResult struct {
	Record   CallRecord
	Existing bool
}

// ReserveTryRequest 是 Tx2 的输入。
type ReserveTryRequest struct {
	TaskID, CallID, AttemptID, EnvID string
	EstimateMicro                    int64
	MaxTries                         int
}

// Try 标识一次已预留的 try；身份为 (TaskID, CallID, TryNo)。
type Try struct {
	TaskID, CallID string
	TryNo          int
	ReservationID  string
	AttemptID      string
}

// Settlement 是一次 try 的结算输入。Outcome：ok | retryable | fatal | unknown。
// ResultSHA256 与 ResultSize 只在 ok 时提供：结果 blob 已完整保存（规格 §9.5），其 sha256 即 calls.result_ref。
type Settlement struct {
	Try               Try
	Outcome           string
	ActualMicro       int64
	LatencyMs         int64
	UpstreamRequestID string
	ResultSHA256      string
	ResultSize        int64
	Error             string
}

// Store 是 call 对持久化层的窄接口。每个方法一个事务（规格 §7.1 锁顺序）。
type Store interface {
	// CheckAccess 在一致快照中读取 §9.2 的事实（attempt_access.state、tasks.current_attempt_id、task_control.desired）。
	CheckAccess(ctx context.Context, taskID, attemptID string) (AccessFacts, error)
	// BeginCall 是 Tx1：复查访问 → 无记录则登记 resolving（created_at、deadline_at = created_at + Deadline）；有记录则返回它（含指纹供比较）；
	// 已复位的 resolving（ResolvingSince 为空）由本次请求接管并按新登记返回（Existing=false），created_at、deadline_at 不变。
	BeginCall(ctx context.Context, r BeginCallRequest) (BeginCallResult, error)
	// ReserveTry 是 Tx2：复查访问与期限（db now() < deadline_at）、累计 tries_used < MaxTries、预算可用 ≥ 估算；
	// 分配 try_no、创建 reservation（held）、budgets.reserved += 估算、calls.state = in_flight、tries_used += 1。
	// 身份 (task_id, call_id, try_no) 幂等：COMMIT 丢失后以同一 try_no 重试返回原记录，不重复预留（E11b）。
	ReserveTry(ctx context.Context, r ReserveTryRequest) (Try, error)
	// SettleTry 完成一次 try 的结算：ok → spent += actual（可赤字），reservation settled，calls completed + result_ref + scope_blobs(task)；
	// retryable/fatal → 释放预留（released），calls 保持 in_flight（retryable，由调用方决定是否再 try）或 failed（fatal）；
	// unknown → unknown += 估算（charged_unknown），calls.state = unknown（BeginCall 见到 unknown 可在上限内新建 try）。
	// 每笔 reservation 只进入一个桶（互斥记账）；幂等：同 try 重复结算返回已有结果。
	SettleTry(ctx context.Context, s Settlement) (CallRecord, error)
	// FailCall 把没有 try 或已耗尽的调用置为 failed（含 call_deadline_exceeded）。
	FailCall(ctx context.Context, taskID, callID, reason string) error
	// ResetResolving 把遗留的 resolving（无 try）复位为可重新解析（启动时调用，§11.2）：清空 resolving_since，
	// 状态仍为 resolving，created_at、deadline_at 不变（重启不重置期限，§9.7）；返回复位的数量。
	ResetResolving(ctx context.Context) (int, error)
	// LoadBudget / LoadCall / ListCalls 供 /v1/budget、重放与 inspect 使用。
	LoadBudget(ctx context.Context, taskID string) (Budget, error)
	LoadCall(ctx context.Context, taskID, callID string) (CallRecord, []TryRecord, error)
	ListCalls(ctx context.Context, taskID string) ([]CallRecord, error)
}

// TryRecord 是 call_tries 中的一行（审计元数据，§9.9）。
type TryRecord struct {
	TryNo                int
	AttemptID, EnvID     string
	State, Outcome       string
	LatencyMs, CostMicro int64
	ReservationID        string
	Error                string
}
