// Package subrun 是 sub-run（受限多 Agent，规格 §13）的纯逻辑：ID 规则、定义校验与哈希、状态机（§8.4）、
// 恢复判定（§13.5）与 sub-run 层账本的可用额计算。存储、协议与 HTTP 由调用方负责。
package subrun

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
)

// Status 是 subruns.status。
type Status string

const (
	Started         Status = "started"
	EndProposed     Status = "end_proposed"
	CancelRequested Status = "cancel_requested"
	Completed       Status = "completed"
	Cancelled       Status = "cancelled"
	Failed          Status = "failed"
	TimedOut        Status = "timed_out"
)

const (
	MaxPerTask           = 4
	MaxDeadlineMS        = 3_600_000
	DefaultCancelTimeout = 10 * time.Second // T_subrun_cancel（§19）
	PerSubrunInflight    = 2                // §9.7

	// MaxParentStepIDBytes 是 parent_step_id 的上限（UTF-8 字节）。
	MaxParentStepIDBytes = 256
	// maxSafeInteger 是 I-JSON 可精确表示的最大整数；更大的 budget_cap_micro 在 JCS 中会丢失精度，
	// 使不同定义得到相同哈希，故在校验时拒绝。
	maxSafeInteger = 1<<53 - 1
	maxIDBytes     = 32
)

// 取消原因（cancel_reason 列）。Deadline 结束为 TimedOut，其余为 Cancelled。
const (
	ReasonDeadline     = "deadline"
	ReasonTaskCancel   = "task_cancel"
	ReasonOrchestrator = "orchestrator" // Worker 的 subrun_cancel
	ReasonPolicy       = "policy"       // 保留
)

var (
	ErrInvalidID         = errors.New("subrun: ID 须为 1–32 位小写字母、数字、_、-，且不是 root")
	ErrInvalidDefinition = errors.New("subrun: 定义不合法")
	ErrInvalidTransition = errors.New("subrun: 非法状态转换")
)

// ValidID 判断 id 是否满足 ^[a-z0-9][a-z0-9_-]{0,31}$ 且不等于 root。
func ValidID(id string) bool {
	if id == "" || len(id) > maxIDBytes || id == "root" {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '_' || c == '-') && i > 0:
		default:
			return false
		}
	}
	return true
}

// Terminal 报告 s 是否为终态（completed、cancelled、failed、timed_out）。
func (s Status) Terminal() bool {
	switch s {
	case Completed, Cancelled, Failed, TimedOut:
		return true
	}
	return false
}

// Definition 是 subrun_start 的可比较部分；Hash 为 sha256(JCS({parent_step_id, budget_cap_micro|null, deadline_ms}))。
type Definition struct {
	ParentStepID   string
	BudgetCapMicro *int64
	DeadlineMS     int64
}

// Validate 检查 parent_step_id 非空且 ≤ 256 字节、deadline_ms ∈ [1, 3 600 000]、budget_cap_micro 为 nil 或
// ∈ [0, 2^53−1]。错误包装 ErrInvalidDefinition。
func (d Definition) Validate() error {
	switch {
	case d.ParentStepID == "":
		return fmt.Errorf("%w: parent_step_id 为空", ErrInvalidDefinition)
	case len(d.ParentStepID) > MaxParentStepIDBytes:
		return fmt.Errorf("%w: parent_step_id 超过 %d 字节", ErrInvalidDefinition, MaxParentStepIDBytes)
	case d.DeadlineMS < 1 || d.DeadlineMS > MaxDeadlineMS:
		return fmt.Errorf("%w: deadline_ms %d 不在 1–%d", ErrInvalidDefinition, d.DeadlineMS, MaxDeadlineMS)
	case d.BudgetCapMicro != nil && (*d.BudgetCapMicro < 0 || *d.BudgetCapMicro > maxSafeInteger):
		return fmt.Errorf("%w: budget_cap_micro %d 不在 0–%d", ErrInvalidDefinition, *d.BudgetCapMicro, int64(maxSafeInteger))
	}
	return nil
}

type hashInput struct {
	ParentStepID   string `json:"parent_step_id"`
	BudgetCapMicro *int64 `json:"budget_cap_micro"`
	DeadlineMS     int64  `json:"deadline_ms"`
}

// Hash 返回定义的 32 字节 sha256。nil 上限编码为 null，因此与上限 0 不同。
func (d Definition) Hash() []byte {
	canon, err := jcs.Canonical(hashInput(d))
	if err != nil {
		// 字符串、整数与 null 总能规范化（encoding/json 会替换非法 UTF-8），不可达。
		panic(fmt.Sprintf("subrun: 定义无法规范化: %v", err))
	}
	sum := sha256.Sum256(canon)
	return sum[:]
}

// Event 是驱动转换的输入。
type Event int

const (
	EvEndSucceeded        Event = iota + 1 // subrun_end{succeeded}
	EvEndFailed                            // subrun_end{failed}
	EvEndCancelled                         // subrun_end{cancelled}
	EvCancelRequest                        // 宿主或 Worker 发起取消
	EvCheckpointCompleted                  // 已提交 checkpoint 列出 completed + result_ref
	EvCheckpointFailed
	EvCheckpointCancelled
	EvCheckpointStarted // 只是确认仍在进行；非终态时为空转换
	EvExpired           // 恢复时 deadline_at 已过
	EvTaskCancelled     // task 取消裁决
	EvTaskSucceeded     // task 成功裁决时仍未终态
	EvTaskFailed        // task 以 failed 裁决时仍未终态（P14-T11：终态任务不留下未终态 sub-run）
)

// cancelFamily 是表中的占位目标：按 cancelReason 解析为 TimedOut（deadline）或 Cancelled（其他）。
const cancelFamily Status = "<cancel>"

// transitions 是显式转换表；表中没有的 (状态, 事件) 组合为 ErrInvalidTransition。
//
// 设计取舍（brief 未逐格规定的单元）：
//   - started/end_proposed 上的 subrun_end{cancelled} 或 checkpoint 列 cancelled 视为编排层取消（与 Task 3 的
//     ProposeSubrunEnd"未处于 cancel_requested 时视为 orchestrator 取消"一致）。
//   - cancel_requested 上的 subrun_end{succeeded} 非法（不能走向完成，E41 的同类）；subrun_end{failed} 与
//     checkpoint 列 failed 允许置 failed（不是完成，且结束了取消等待）。
//   - cancel_requested 上的 EvExpired 按取消原因解析，与 ResumeDecision 一致（已请求的取消优先）。
//   - 终态只接受"同一终态的重复确认"：checkpoint 只能表达 cancelled，故 timed_out 上的 checkpoint cancelled /
//     subrun_end{cancelled} 视为确认；failed 上的 subrun_end{failed} 亦然。
var transitions = map[Status]map[Event]Status{
	Started: {
		EvEndSucceeded:        EndProposed,
		EvEndFailed:           Failed,
		EvEndCancelled:        cancelFamily,
		EvCancelRequest:       CancelRequested,
		EvCheckpointCompleted: Completed,
		EvCheckpointFailed:    Failed,
		EvCheckpointCancelled: cancelFamily,
		EvCheckpointStarted:   Started,
		EvExpired:             TimedOut,
		EvTaskCancelled:       Cancelled,
		EvTaskSucceeded:       Failed,
		EvTaskFailed:          Failed,
	},
	EndProposed: {
		EvEndFailed:           Failed,
		EvEndCancelled:        cancelFamily,
		EvCancelRequest:       CancelRequested,
		EvCheckpointCompleted: Completed,
		EvCheckpointFailed:    Failed,
		EvCheckpointCancelled: cancelFamily,
		EvCheckpointStarted:   EndProposed,
		EvExpired:             TimedOut,
		EvTaskCancelled:       Cancelled,
		EvTaskSucceeded:       Failed,
		EvTaskFailed:          Failed,
	},
	CancelRequested: {
		EvEndFailed:           Failed,
		EvEndCancelled:        cancelFamily,
		EvCancelRequest:       CancelRequested,
		EvCheckpointFailed:    Failed,
		EvCheckpointCancelled: cancelFamily,
		EvCheckpointStarted:   CancelRequested,
		EvExpired:             cancelFamily,
		EvTaskCancelled:       Cancelled,
		EvTaskSucceeded:       Failed,
		EvTaskFailed:          Failed,
	},
	Completed: {
		EvCheckpointCompleted: Completed,
	},
	Failed: {
		EvEndFailed:        Failed,
		EvCheckpointFailed: Failed,
	},
	Cancelled: {
		EvEndCancelled:        Cancelled,
		EvCheckpointCancelled: Cancelled,
	},
	TimedOut: {
		EvEndCancelled:        TimedOut,
		EvCheckpointCancelled: TimedOut,
	},
}

// Next 按 §8.4 与 Global Constraints 计算转换；cancelReason 只在目标为取消族终态时用于区分 Cancelled / TimedOut
// （EvEndCancelled、EvCheckpointCancelled，以及 cancel_requested 上的 EvExpired）。
// 终态上的任何事件、cancel_requested/cancelled 上的 EvCheckpointCompleted 返回 ErrInvalidTransition；
// 同一终态的重复确认（例如 completed 上再次 EvCheckpointCompleted）返回 (同一状态, nil)。
func Next(from Status, ev Event, cancelReason string) (Status, error) {
	to, ok := transitions[from][ev]
	if !ok {
		return from, fmt.Errorf("%w: %s 上的事件 %d", ErrInvalidTransition, from, ev)
	}
	if to == cancelFamily {
		return cancelTerminal(cancelReason), nil
	}
	return to, nil
}

func cancelTerminal(reason string) Status {
	if reason == ReasonDeadline {
		return TimedOut
	}
	return Cancelled
}

// Record 是一行 subruns。
type Record struct {
	TaskID, SubrunID, ParentStepID string
	DefinitionHash                 []byte
	Status                         Status
	BoundAttemptID                 string
	DeadlineAt                     time.Time
	BudgetCapMicro                 *int64
	ResultRef, FailureReason       string
	CancelReason                   string
	StartedAt, EndedAt             time.Time
}

// ResumeDecision 按 §13.5 表给出恢复时的处理：Rerun 表示该 sub-run 需在新 attempt 中继续（重新绑定），
// Status 是告知 Worker 的状态（过期的非终态为 TimedOut）。
//
// 终态保持不变且不重跑；cancel_requested 按取消原因收尾为 cancelled / timed_out（E40：取消后崩溃不恢复执行）；
// started / end_proposed 在 now ≥ deadline_at 时为 timed_out（E45），否则回到 started 并重跑（E42：未入
// checkpoint 的 end_proposed 不视为完成）。
func ResumeDecision(r Record, now time.Time) (status Status, rerun bool) {
	switch r.Status {
	case CancelRequested:
		return cancelTerminal(r.CancelReason), false
	case Started, EndProposed:
		if !now.Before(r.DeadlineAt) {
			return TimedOut, false
		}
		return Started, true
	}
	return r.Status, false
}

// Budget 是 sub-run 层账本（微美元）；CapMicro 为 nil 表示不设上限。
type Budget struct {
	CapMicro                                *int64
	ReservedMicro, SpentMicro, UnknownMicro int64
}

// Available 返回 cap − spent − reserved − unknown（§9.6，可为负）；无上限时返回 math.MaxInt64。
func (b Budget) Available() int64 {
	if b.CapMicro == nil {
		return math.MaxInt64
	}
	return *b.CapMicro - b.SpentMicro - b.ReservedMicro - b.UnknownMicro
}
