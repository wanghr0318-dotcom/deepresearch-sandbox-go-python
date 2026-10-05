// Package recovery 是启动恢复的执行器（代码组织设计 §6.1）：幂等执行 reconcile 生成的 RecoveryPlan。
// 本文件声明它需要的持久化用例（代码组织设计 §4）；实现位于 internal/persistence/postgres。
package recovery

import (
	"context"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
)

// Store 是 recovery（及其事实提供给 reconcile）的窄接口。
type Store interface {
	// LoadRecoveryFacts 读取启动恢复需要的全部数据库事实（规格 §14.1 第 5 步、§14.2）。
	LoadRecoveryFacts(ctx context.Context) (Facts, error)
	// RevokeAllActive 在单个事务中撤销全部 active 的 attempt 访问（规格 §14.1 第 3 步）；返回撤销的数量。
	RevokeAllActive(ctx context.Context, reason string) (int, error)
	// AccountUnrecordedRunTime 把上次持久化之后到 until 的未记账区间按墙钟差全额计入（规格 §14.4）；
	// 同一 until 重复调用不重复计入。只接受当前 attempt。
	AccountUnrecordedRunTime(ctx context.Context, taskID, attemptID string, until time.Time) error
	// ConvertLedger 是启动账本转换（规格 §14.1 第 4 步）：在单个事务中把全部 held 的 reservation 转为
	// charged_unknown（reserved 减、unknown 加同额，I3 保持），其 try 按 unknown 结算，全部 in_flight 调用置为
	// unknown（deadline_at 不变）。幂等：再次调用不转换任何记录。resolving 调用的复位不在此（ResetResolving）。
	ConvertLedger(ctx context.Context) (LedgerConversion, error)
}

// LedgerConversion 是一次账本转换的结果。
type LedgerConversion struct {
	Reservations int   // held → charged_unknown 的 reservation 数（即按 unknown 结算的 try 数）
	Calls        int   // in_flight → unknown 的调用数
	UnknownMicro int64 // 转入 unknown 的金额合计（微美元）
}

// Facts 是恢复所需的数据库事实。
type Facts struct {
	Tasks            []TaskFact          // 非终态任务
	Environments     []EnvFact           // 未停止或清理未完成的环境
	PendingIntents   []resource.Intent   // pending 或 acquired 的资源意图
	UnreleasedRanges []resource.UIDRange // 所属环境已清理完成但仍为 assigned 的 UID 范围
}

// TaskFact 是一个非终态任务的事实。
type TaskFact struct {
	TaskID, Status, Desired string
	ControlVersion          int64
	CurrentAttempt          *AttemptFact
	NotBefore               *time.Time
	RunTimePersistedAt      *time.Time
}

// AttemptFact 是任务当前 attempt 的事实。
type AttemptFact struct {
	AttemptID, Status, EnvID string
	HasVerdict               bool
	ProposalKind             string // 终态提议；无则为空
}

// EnvFact 是一个环境的事实。
type EnvFact struct {
	EnvID, AttemptID string
	StoppedAt        *time.Time
	CleanupState     string
}
