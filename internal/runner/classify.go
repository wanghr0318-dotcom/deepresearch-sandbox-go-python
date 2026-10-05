package runner

import (
	"encoding/json"
	"errors"
	"strings"
	"syscall"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// 本文件是规格 §5.8（宿主裁决）与 §14.3（故障分类）的唯一实现：把一次 attempt 的结束事实映射为
// outcome_class 与重试资格。task.Decide 只按类别与 desired 裁决，不重新分类。

// outcome_class 取值。规格 §14.3 列出的类别之外，有两个规格未单列的情况，按保守原则（不重试、
// 不判成功）给出独立类别，便于诊断：
//   - exited_without_proposal：没有终态提议、也不是被信号杀死就退出（任意退出码），或平台因
//     stdout 已关闭而进程不退出、exit_grace 到期终止。§5.8 "按退出原因分类" 而 §14.3 只为信号
//     退出给出了类别；正常退出却不报告结果是 Worker 的确定性缺陷，重跑不会改变。
//   - output_incomplete：result 且 exit 0、协议无违规，但完成屏障未能确认输出完整（屏障 A 或 B
//     超时）。§5.8 的 succeeded 要求"输出已保存"，此时无法确认。
const (
	ClassSucceeded             = "succeeded"
	ClassOOMObserved           = "oom_observed_in_attempt"
	ClassCrashedSignal         = "crashed_signal"
	ClassControlLost           = "control_lost"
	ClassReadyTimeout          = "ready_timeout"
	ClassLostOnRestart         = "lost_on_restart"
	ClassSubrunCancelTimeout   = "subrun_cancel_timeout"
	ClassReleaseTimeout        = "release_timeout"
	ClassCreateFailedTransient = "create_failed_transient"
	ClassCreateFailedEnv       = "create_failed_env"
	ClassCreateOutcomeUnknown  = "create_outcome_unknown"
	ClassStoreUnavailable      = "store_unavailable"
	ClassWorkerOOMLikely       = "worker_oom_likely"
	ClassWorkerError           = "worker_error"
	ClassProtocolMismatch      = "protocol_mismatch"
	ClassProtocolViolation     = "protocol_violation"
	ClassOutputLimitExceeded   = "output_limit_exceeded"
	ClassExitAfterResult       = "exit_after_result"
	ClassDeadlineExceeded      = "task_deadline_exceeded"
	ClassCancelled             = "cancelled"
	ClassPaused                = "paused"
	ClassExitedNoProposal      = "exited_without_proposal"
	ClassOutputIncomplete      = "output_incomplete"
)

// 重试类别，与 task.RetryKind 的取值相同（runner 不导入 task）。
const (
	RetryNone  = ""
	RetryFault = "fault"
	RetryOOM   = "oom"
)

// PlatformKill 取值：平台主动终止执行的原因（空表示没有主动终止）。
const (
	KillCancel           = "cancel"            // cancel 的 grace 到期，或 cancel 无法送达
	KillPause            = "pause"             // pause 的 grace 到期，或 pause 无法送达
	KillTimeout          = "timeout"           // 累计运行时限超限（ctx 的 cause 为 ErrRunTimeExceeded 或 deadline）
	KillExitGrace        = "exit_grace"        // 终态提议（或违规、stdout 关闭）后 exit_grace 内未退出
	KillReadyTimeout     = "ready_timeout"     // T_ready 内没有 ready
	KillStoreUnavailable = "store_unavailable" // Store 连续失败达阈值（§14.5）
	KillShutdown         = "shutdown"          // 调用方取消 ctx（服务停止）：执行随本进程丢失
	KillStdinBroken      = "stdin_broken"      // stdin 部分写出：控制流损坏（§5.5 第 8 条）
)

// ClassifyInput 是分类所需的全部事实。
type ClassifyInput struct {
	Proposal *TerminalProposal
	// Payload 是终态提议被保存的内容（Outcome.ResultPayload）；error 提议的 retryable 与 code 从这里读取。
	Payload          json.RawMessage
	Exit             provider.ExitStatus
	ExitErr          error
	Diag             provider.ResourceDiag
	PlatformKill     string // "" 或 Kill* 之一
	Control          string // 宿主已请求的控制：""、cancel 或 pause（cancel 优先）
	Violation        string
	OutputIncomplete bool
	StartErr         error // Worker 未能启动（init 不合法、out_dir 无法打开、StartExec 失败）
	ControlLost      bool  // Wait 返回错误：退出状态未知
}

// Classify 返回 outcome_class 与重试资格（""、fault 或 oom）。判定顺序：
//  1. 启动失败；
//  2. 协议违规（含输出超限、握手失败）——不重试；
//  3. 与 Worker 无关的平台终止：累计运行时限、Store 不可用、ready 超时、服务停止；
//  4. 有效 result（result + exit 0、无违规、输出完整）：succeeded，观察到 OOM 时为
//     oom_observed_in_attempt。cancel/pause 生效期间同样如此，由 Decide 据 desired 记
//     completed_during_cancel / completed_during_pause；
//  5. 宿主 cancel/pause 已生效：按宿主意图（cancelled / paused），不判崩溃、不重试；
//  6. 其余按终态提议与退出原因（§5.8、§14.3）。
func Classify(in ClassifyInput) (class string, retry string) {
	if in.StartErr != nil {
		return classifyStart(in)
	}
	if in.Violation != "" {
		switch in.Violation {
		case ViolationOutputLimit:
			return ClassOutputLimitExceeded, RetryNone
		case ViolationHandshakeError, ViolationModeMismatch:
			return ClassProtocolMismatch, RetryNone
		}
		return ClassProtocolViolation, RetryNone
	}
	switch in.PlatformKill {
	case KillTimeout:
		return ClassDeadlineExceeded, RetryNone
	case KillStoreUnavailable:
		return ClassStoreUnavailable, RetryOf(ClassStoreUnavailable)
	case KillReadyTimeout:
		return ClassReadyTimeout, RetryOf(ClassReadyTimeout)
	case KillShutdown:
		return ClassLostOnRestart, RetryOf(ClassLostOnRestart)
	case KillStdinBroken:
		// 部分写出说明 Worker 停止读取控制通道（写期限到期）或在写入中途关闭它。规格只规定"关闭
		// stdin 并终止"，未给类别；保守地按协议违规，不重试、不判成功。
		return ClassProtocolViolation, RetryNone
	}
	kind := ""
	if in.Proposal != nil {
		kind = in.Proposal.Kind
	}
	clean := !in.ControlLost && in.Exit.Code == 0 && in.Exit.Signal == 0
	if kind == "result" && clean && !in.OutputIncomplete {
		if in.Diag.OOMKillDelta > 0 {
			return ClassOOMObserved, RetryNone
		}
		return ClassSucceeded, RetryNone
	}
	control := in.Control
	if control == "" && (in.PlatformKill == KillCancel || in.PlatformKill == KillPause) {
		control = in.PlatformKill
	}
	switch control {
	case KillCancel:
		return ClassCancelled, RetryNone
	case KillPause:
		// paused 提议的 checkpoint 不是最新已提交者时 runner 已记为违规（第 2 步）；grace 到期
		// 未暂停同样是 paused（§5.9：有已提交 checkpoint 则停在该点，否则从头）。
		return ClassPaused, RetryNone
	}
	switch kind {
	case "result":
		switch {
		case in.ControlLost: // 退出状态未知：基础设施故障，按 control_lost 重试
			return ClassControlLost, RetryOf(ClassControlLost)
		case in.Exit.Code == 0 && in.Exit.Signal == 0: // 只差输出完整性
			return ClassOutputIncomplete, RetryNone
		}
		return ClassExitAfterResult, RetryNone // 含 exit_grace 到期被终止
	case "error":
		return ClassWorkerError, workerErrorRetry(in.Payload)
	case "paused":
		// 宿主没有请求暂停：§5.8 只在"宿主确有暂停请求"时接受 paused。保守地判为协议违规。
		return ClassProtocolViolation, RetryNone
	}
	switch {
	case in.ControlLost:
		return ClassControlLost, RetryOf(ClassControlLost)
	case in.PlatformKill == KillExitGrace: // stdout 已关闭却不退出：平台终止，不判崩溃
		return ClassExitedNoProposal, RetryNone
	case in.Exit.Signal == syscall.SIGKILL && in.Diag.OOMKillDelta > 0:
		return ClassWorkerOOMLikely, RetryOf(ClassWorkerOOMLikely)
	case in.Exit.Signal != 0:
		return ClassCrashedSignal, RetryOf(ClassCrashedSignal)
	}
	return ClassExitedNoProposal, RetryNone
}

// classifyStart：Worker 没有运行起来。StartExec 的控制连接断开（启动结果未知）按 control_lost；
// 调用方取消导致的失败按取消原因；其余（init 不合法、out_dir 无法打开、start_err、环境正在停止）
// 是环境或配置问题，重跑不会改变（§14.3 create_failed_env 的含义），不重试。
func classifyStart(in ClassifyInput) (string, string) {
	switch {
	case errors.Is(in.StartErr, provider.ErrControlLost):
		return ClassControlLost, RetryOf(ClassControlLost)
	case in.PlatformKill == KillTimeout:
		return ClassDeadlineExceeded, RetryNone
	case in.PlatformKill == KillShutdown:
		return ClassLostOnRestart, RetryOf(ClassLostOnRestart)
	}
	return ClassCreateFailedEnv, RetryNone
}

// workerErrorRetry 是 §14.3 "worker_error，retryable: true → 是，除非错误码在拒绝列表
// （budget_exhausted、protocol_*）"。retryable 只是建议；内容无法解析时不重试。
func workerErrorRetry(payload json.RawMessage) string {
	var e struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(payload, &e); err != nil || !e.Retryable {
		return RetryNone
	}
	if e.Code == "budget_exhausted" || strings.HasPrefix(e.Code, "protocol_") {
		return RetryNone
	}
	return RetryFault
}

// RetryOf 是 §14.3 中只由类别决定的重试资格（worker_error 取决于 Worker 的 retryable 与错误码，
// 不能只由类别决定，这里返回 ""，以 Classify 的结果为准）。create_outcome_unknown 须先核对资源，
// 不直接重建；release_timeout 与裁决后的违规不适用重试。
func RetryOf(class string) string {
	switch class {
	case ClassCrashedSignal, ClassControlLost, ClassReadyTimeout, ClassLostOnRestart, ClassSubrunCancelTimeout,
		ClassCreateFailedTransient, ClassStoreUnavailable:
		return RetryFault
	case ClassWorkerOOMLikely:
		return RetryOOM
	}
	return RetryNone
}
