package call

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// exec 的持久化窄接口与类型（规格 §10.2–§10.4；Plan 15 D6、D9、D10）。事务用例由 internal/persistence/postgres 实现。

// ExecEndpoint 是 exec 调用登记在 calls.endpoint 中的端点；ReserveExec 只为该端点的调用预留。
const ExecEndpoint = "/v1/exec"

// ExecPolicy 是每任务 exec 配额（server 策略）；首次 exec 的预留事务以它建立 exec_quotas 行（之后不随策略变化）。
type ExecPolicy struct{ CountLimit, CPULimitUsec, WallLimitMs int64 }

// ReserveExecRequest 是 exec 的 Tx2 输入。
type ReserveExecRequest struct {
	TaskID, CallID, AttemptID, SubrunID string
	ReservationID                       string // 事务前生成（§7.3 操作身份）；以它幂等
	CPUEstimateUsec, WallMs, QueueMs    int64
	MaxTries                            int
	Policy                              ExecPolicy
}

// ExecTry 是一次已预留的 exec try；EnvID = ExecEnvID(TaskID, CallID, TryNo)。
type ExecTry struct {
	Try
	EnvID string
}

// ExecEnvID 由 (task, call, try_no) 确定性派生（"exec-" + sha256 前 24 个十六进制字符），使预留事务结果未知后的
// 重试指向同一环境。
func ExecEnvID(taskID, callID string, tryNo int) string {
	sum := sha256.Sum256([]byte(taskID + "\x00" + callID + "\x00" + strconv.Itoa(tryNo)))
	return "exec-" + hex.EncodeToString(sum[:])[:24]
}

// ExecOutcome 是 exec try 的结局（§10.4；D6）。
type ExecOutcome string

const (
	ExecCompleted   ExecOutcome = "completed"    // journal completed（try outcome ok）
	ExecTimedOut    ExecOutcome = "timed_out"    // journal completed（try outcome ok）
	ExecCancelled   ExecOutcome = "cancelled"    // journal failed{exec_cancelled}，可重试（try outcome retryable）
	ExecStartFailed ExecOutcome = "start_failed" // journal failed{exec_start_failed}，不计 exec_count（try outcome fatal）
	ExecUnknown     ExecOutcome = "unknown"      // journal unknown；CPU 全额转 unknown（try outcome unknown）
)

// ExecOutput 是 /out 中收集到的一个文件（blob 已完整保存）；结算时登记到 blobs 并授权到 scope_blobs(task)。
type ExecOutput struct {
	SHA256 string
	Size   int64
}

// ExecSettlement 是一次 exec try 的结算输入。
type ExecSettlement struct {
	Try             ExecTry
	Outcome         ExecOutcome
	Started         bool  // 已收到 start_ack：计 exec_count（start_failed 不得为真）
	CPUUsec         int64 // cpu.stat usage_usec
	CPUKnown        bool  // false：按全额预留计入 spent（call_tries.cpu_usec 为 NULL）
	WallMs, QueueMs int64
	ResultSHA256    string // completed / timed_out：结果 blob（已完整保存）
	ResultSize      int64
	Outputs         []ExecOutput // 只用于 completed / timed_out：写入 blobs 与 scope_blobs(task)
	Error           string       // call_tries.error 摘要
}

// ExecQuota 是任务的 exec 配额（exec_quotas 行）。
type ExecQuota struct {
	CountLimit, CountUsed                                       int64
	CPULimitUsec, CPUReservedUsec, CPUSpentUsec, CPUUnknownUsec int64
	WallLimitMs, WallSpentMs                                    int64
	Blocked                                                     bool
}

// CPUAvailable 返回可预留的 CPU：limit − reserved − spent − unknown（可为负）。
func (q ExecQuota) CPUAvailable() int64 {
	return q.CPULimitUsec - q.CPUReservedUsec - q.CPUSpentUsec - q.CPUUnknownUsec
}

// ExecStore：每个方法一个事务（锁顺序 tasks FOR SHARE → task_control FOR SHARE → subruns FOR SHARE →
// attempt_access FOR SHARE → environments → exec_quotas → calls → reservations → call_tries）。
type ExecStore interface {
	// ReserveExec 是 exec 的 Tx2：复查 §9.2 访问与期限（db now() < deadline_at）、tries_used < MaxTries、
	// 本调用此前 try 的 exec 环境均已 stopped_at 且没有持有预留的 try（否则 ErrRejected{call_in_progress}）、
	// exec_quotas 未 blocked（exec_blocked）、count_used + 在途（held）exec 数 < limit（exec_quota_exhausted）、
	// cpu 可用 ≥ 估算（exec_cpu_exhausted）、wall_spent + WallMs ≤ wall_limit（exec_wall_exhausted）；分配 try_no，
	// 插入 environments（kind exec、attempt_id、status creating）、reservation（kind cpu、held，不带 subrun_id：
	// exec 配额是任务级）、call_tries（in_flight、env_id、queue_ms），cpu_reserved += 估算，calls in_flight
	// （source exec）、tries_used += 1。exec_quotas 行不存在时以 Policy 建立。以 ReservationID 幂等（E11b 同法）：
	// 已存在该 reservation 时原样返回其 try，不复查。
	ReserveExec(ctx context.Context, r ReserveExecRequest) (ExecTry, error)
	// MarkExecStarting 在 StartExec 之前复查访问（active、current、desired ≠ cancel；调用属于 sub-run 时另要求其可用）
	// 并写 exec_started_at；访问已失效为 ErrRejected（对应访问码），调用方不得启动。已写过时保持原值（幂等），
	// 但访问仍每次复查。try 已结算为 ErrConflict。
	MarkExecStarting(ctx context.Context, t ExecTry) error
	// SettleExec 结算：cpu_reserved -= 估算；CPUKnown ? spent += CPUUsec : spent += 估算；unknown → cpu_unknown += 估算；
	// Started → exec_count_used += 1；wall_spent += WallMs；spent > limit → blocked = true；reservation 只入一个桶
	// （settled 或 charged_unknown）；completed/timed_out → calls completed、result_ref、scope_blobs(task) 含结果与
	// 全部输出；cancelled → calls failed{exec_cancelled}；start_failed → calls failed{exec_start_failed}；
	// unknown → calls unknown。按 reservation 状态幂等（已不是 held 时返回调用的当前记录）。
	SettleExec(ctx context.Context, s ExecSettlement) (CallRecord, error)
	// LoadExecQuota 读取任务的 exec 配额；首次 exec 之前没有行，为 ErrNotFound（调用方按策略显示）。
	LoadExecQuota(ctx context.Context, taskID string) (ExecQuota, error)
}

// exec 的拒绝码与失败原因（ErrRejected 的 Code；edge 映射状态码）。
const (
	CodeExecQuotaExhausted = "exec_quota_exhausted" // 402
	CodeExecCPUExhausted   = "exec_cpu_exhausted"   // 402
	CodeExecWallExhausted  = "exec_wall_exhausted"  // 402
	CodeExecBlocked        = "exec_blocked"         // 402
	CodeExecCancelled      = "exec_cancelled"       // fail_reason，可重试类别（409）
	CodeExecStartFailed    = "exec_start_failed"    // fail_reason（502）
)
