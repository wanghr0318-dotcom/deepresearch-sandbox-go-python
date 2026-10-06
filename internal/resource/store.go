// Package resource 是资源 coordinator 与 cleanup loop（后续计划实现）。本文件声明它们需要的
// 持久化用例（代码组织设计 §4）；实现位于 internal/persistence/postgres。
package resource

import (
	"context"
	"time"
)

// Store 是 resource 的窄接口。每个方法是一个完整、幂等的事务用例（设计 §2.4）。
type Store interface {
	// RecordIntent 以事务前生成的 intent_id 记录资源意图；同一意图重试不会出现第二份，
	// (kind, name) 已被其他意图占用为 persistence.ErrConflict。
	RecordIntent(ctx context.Context, i Intent) (Intent, error)
	// ResolveIntent 推进意图状态：pending→acquired→released、pending→failed；
	// 已在目标状态返回原结果，倒退或跳跃为冲突。
	ResolveIntent(ctx context.Context, intentID, state string) (Intent, error)
	// GetIntent 读取资源意图。
	GetIntent(ctx context.Context, intentID string) (Intent, error)
	// SeedUIDRanges 幂等地登记 count 段 UID 范围：第 i 段为 [base+i*size, base+(i+1)*size)。
	SeedUIDRanges(ctx context.Context, base, size int64, count int) error
	// AssignUIDRange 为环境分配一段空闲 UID 范围；以 (env_id, allocation_id) 为身份，
	// 结果丢失后重试返回原分配，不再占第二段。没有空闲范围时返回 ErrNoFreeUIDRange（暂时性：
	// 范围在清理完成后归还）。
	AssignUIDRange(ctx context.Context, envID, allocationID string) (UIDRange, error)
	// ReleaseUIDRange 释放一段范围；只有分配代次匹配才释放，已被后来的分配复用为冲突。
	ReleaseUIDRange(ctx context.Context, uidRangeID, allocationID string) (UIDRange, error)
	// GetUIDRange 读取分配给环境的 UID 范围。
	GetUIDRange(ctx context.Context, envID string) (UIDRange, error)
	// MarkStopped 记录环境已停止；只在尚未记录时写入，重试不倒退。
	MarkStopped(ctx context.Context, envID string, at time.Time) (Environment, error)
	// UpdateCleanup 以 cleanup_tries 的 CAS 记录一次清理尝试；同一次尝试不重复累计，
	// cleanup_state 不倒退。
	UpdateCleanup(ctx context.Context, u CleanupUpdate) (Environment, error)
	// GetEnvironment 读取环境记录。
	GetEnvironment(ctx context.Context, envID string) (Environment, error)
	// ListCleanupCandidates 返回待清理的环境：stopped_at 已记录、清理未完成、所属 attempt 已有判决、
	// next_retry_at 已到（或为空）；按停止时间排序，至多 limit 个。
	ListCleanupCandidates(ctx context.Context, now time.Time, limit int) ([]Environment, error)
	// RecordQuarantine 记录一个隔离资源（规格 §14.1 扫描表）；按路径幂等。
	RecordQuarantine(ctx context.Context, q Quarantine) error
	// MarkQuarantineAlerted 记录隔离资源已报警（规格 §16.3 I8）；M1 的报警为结构化错误日志，发出后调用。
	MarkQuarantineAlerted(ctx context.Context, path string) error
	// QuarantineUIDRange 把范围置为 quarantined（只在分配代次匹配时，否则为 persistence.ErrConflict；范围不存在为
	// ErrNotFound），同一事务 RecordQuarantine(layer = uid_files, path = UIDRangeQuarantinePath(id))。已隔离时幂等。
	// 规格 §4.5"存疑则隔离该范围"、E39；隔离的范围不再分配，也不由启动核对归还。
	QuarantineUIDRange(ctx context.Context, uidRangeID, allocationID, reason string) error
}

// UIDRangeQuarantinePath 是被隔离 UID 范围在 quarantined_resources 中的路径（resource_path）。
func UIDRangeQuarantinePath(uidRangeID string) string { return "uid_range/" + uidRangeID }

// Quarantine 是一个归属不明或冲突的资源：不自动销毁，报警并计入占用。
type Quarantine struct {
	Layer         string // 与 provider.ScanItem.Layer 相同的取值
	Path          string
	ObservedOwner string
	Reason        string
}

// 资源意图状态。
const (
	IntentPending  = "pending"
	IntentAcquired = "acquired"
	IntentReleased = "released"
	IntentFailed   = "failed"
)

// 清理状态，只能前进：none → pending → done。
const (
	CleanupNone    = "none"
	CleanupPending = "pending"
	CleanupDone    = "done"
)

// Intent 是 resource_intents 中的一行。
type Intent struct {
	IntentID string
	EnvID    string
	Kind     string
	Name     string
	State    string
}

// UIDRange 是 uid_ranges 中的一行。
type UIDRange struct {
	UIDRangeID   string
	Base         int64
	Size         int64
	State        string
	OwnerID      string
	AllocationID string
}

// CleanupUpdate 记录一次清理尝试：ExpectedTries 是调用方读到的 cleanup_tries。
type CleanupUpdate struct {
	EnvID         string
	ExpectedTries int64
	State         string
	Error         string
	NextRetryAt   *time.Time
}

// Environment 是 environments 中的一行（M1 用到的列）。
type Environment struct {
	EnvID        string
	Kind         string
	AttemptID    string
	Status       string
	StoppedAt    *time.Time
	CleanupState string
	CleanupTries int64
	CleanupError string
	NextRetryAt  *time.Time
}
