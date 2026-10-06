package task

// 本文件是规格 §8.1 控制与最终裁决规则的唯一实现：Store（internal/persistence/postgres）在事务内
// 用它们校验写入，Decide 用它们计算写入内容（代码组织 §9"同一业务规则只有一个权威实现"）。

// ControlTransition 是规格 §8.1 中控制意图引起的任务状态转换：cancel 使 queued、paused 直接
// cancelled，使执行中的任务进入 cancelling；pause 使 queued 进入 paused，使执行中的任务进入
// pausing；run（resume）使 paused 回到 queued。其余组合不是合法转换。
func ControlTransition(status, desired string) (next string, ok bool) {
	switch desired {
	case "cancel":
		switch status {
		case "queued", "paused":
			return "cancelled", true
		case "running", "pausing", "cancelling":
			return "cancelling", true
		}
	case "pause":
		switch status {
		case "queued":
			return "paused", true
		case "running", "pausing":
			return "pausing", true
		case "paused":
			return "paused", true
		}
	case "run":
		switch status {
		case "paused", "queued":
			return "queued", true
		case "running":
			return "running", true
		}
	}
	return "", false
}

// VerdictAllowed 是规格 §8.1 最终裁决对 desired 的约束：cancel → cancelled；pause → paused；
// run → 按 §5.8 与 §14.3 裁决为 succeeded、failed，或故障重试回到 queued。
func VerdictAllowed(desired, taskStatus string) bool {
	switch desired {
	case "cancel":
		return taskStatus == "cancelled"
	case "pause":
		return taskStatus == "paused"
	default:
		return taskStatus == "succeeded" || taskStatus == "failed" || taskStatus == "queued"
	}
}

// ReasonAwaitingInput 是会话 turn 等待用户回答时的 status_reason（契约 A：paused + awaiting_input）。
const ReasonAwaitingInput = "awaiting_input"

// VerdictAllowedReason 是 VerdictAllowed 的会话扩展：desired = run 时另允许 (paused, awaiting_input)——Worker 的
// awaiting_input 提议是一种不经暂停请求的暂停（保留 checkpoint、释放 run slot）。其余组合与 VerdictAllowed 相同；
// desired = pause 时 paused 可带任意原因。Store 用它校验；VerdictAllowed 保留给尚未迁移的调用方。
func VerdictAllowedReason(desired, taskStatus, statusReason string) bool {
	if desired == "run" && taskStatus == "paused" {
		return statusReason == ReasonAwaitingInput
	}
	return VerdictAllowed(desired, taskStatus)
}

// IsTerminal 报告任务状态是否为终态。
func IsTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "cancelled"
}
