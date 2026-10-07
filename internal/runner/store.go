// Package runner 是 AttemptRunner（后续计划实现）。本文件声明 runner 需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package runner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
)

// Store 是 runner 的窄接口。每个方法是一个完整、幂等的事务用例（设计 §2.3、§2.4）。
type Store interface {
	// AppendWorkerEvents 原子追加一批序号连续的 Worker 事件。与已存在部分重叠的事件逐条
	// 校验内容（同序号不同内容为 persistence.ErrConflict），只追加连续的新后缀；有缺口则拒绝。
	AppendWorkerEvents(ctx context.Context, attemptID string, events []WorkerEvent) (Watermark, error)
	// AppendSessionWorkerEvents 是 session 模式的追加：worker_seq 为 incarnation 序号，同一 attempt 内严格递增、
	// 不要求连续（同一 incarnation 的其他 attempt 与会话级消息占用其间的序号）。与已存在部分重叠的事件逐条校验内容
	// （同序号不同内容为 persistence.ErrConflict）；新事件的序号须大于该 attempt 已提交的最大序号，否则为冲突。
	// 返回该 attempt 已提交的最大序号。
	AppendSessionWorkerEvents(ctx context.Context, attemptID string, events []WorkerEvent) (Watermark, error)
	// WorkerEventWatermark 返回该 attempt 已提交的最大 worker_seq（连续水位，仅用于优化）。
	WorkerEventWatermark(ctx context.Context, attemptID string) (Watermark, error)
	// CommitCheckpoint 以 (scope, checkpoint_id) 为身份提交 checkpoint；内容哈希不同为冲突。
	CommitCheckpoint(ctx context.Context, c Checkpoint) (CommittedCheckpoint, error)
	// QueryCheckpoint 读取已提交的 checkpoint；不存在为 persistence.ErrNotFound。
	QueryCheckpoint(ctx context.Context, scope Scope, checkpointID string) (CommittedCheckpoint, error)
	// RegisterArtifact 登记已写入 BlobStore 的产物，以 (task, artifact, sha256) 为身份分配版本。
	RegisterArtifact(ctx context.Context, a Artifact) (ArtifactVersion, error)
	// GetArtifact 读取已登记的产物版本。
	GetArtifact(ctx context.Context, taskID, artifactID, sha256 string) (ArtifactVersion, error)
	// LatestArtifact 返回任务中该产物的最新版本（用于固定 result 输出：恢复后的 attempt 可能不再登记之前保存的产物）；
	// 不存在为 persistence.ErrNotFound。
	LatestArtifact(ctx context.Context, taskID, artifactID string) (ArtifactVersion, error)
	// RecordTerminalProposal 记录 attempt 的终态提议：相同内容返回原结果，不同内容为冲突。
	RecordTerminalProposal(ctx context.Context, p TerminalProposal) (TerminalProposal, error)
	// GetTerminalProposal 读取 attempt 的终态提议；未记录为 persistence.ErrNotFound。
	GetTerminalProposal(ctx context.Context, attemptID string) (TerminalProposal, error)
}

// WorkerEvent 是一条 Worker 事件。Payload 是事件的原始 JSON，其字节参与内容哈希。
type WorkerEvent struct {
	Seq     int64
	Type    string
	Payload json.RawMessage
	TS      time.Time
}

// Watermark 是某个 attempt 已提交的最大 worker_seq；0 表示尚无事件。
type Watermark struct {
	AttemptID string
	WorkerSeq int64
}

// Scope 标识 checkpoint 的所属范围。CommitCheckpoint 只接受 task；session scope 的 checkpoint 只经成功裁决
// （result.session_state）提交（规格 §5.5 第 7 条）。
type Scope struct {
	Kind string
	ID   string
}

// Checkpoint 是待提交的 checkpoint；State 与 StateRef 必须且只能提供一个。
type Checkpoint struct {
	Scope        Scope
	CheckpointID string
	AttemptID    string
	StepID       string
	State        json.RawMessage
	StateRef     string
	Refs         []string
	// Subruns 是 checkpoint 的 subruns[]（sub-run 扩展，规格 §5.5 规则 2、6）：与 checkpoint 在同一事务中按状态机转换，
	// 不允许的转换以 invalid_transition 拒绝整个 checkpoint（指针不变，E41）；参与内容哈希。
	Subruns []protocol.CheckpointSubrun
}

// CommittedCheckpoint 是已提交的 checkpoint。
type CommittedCheckpoint struct {
	Scope        Scope
	CheckpointID string
	CommitSeq    int64
	ContentHash  []byte
}

// Artifact 是待登记的产物；对应的 blob 必须已写入 BlobStore。
type Artifact struct {
	TaskID     string
	AttemptID  string
	ArtifactID string
	SHA256     string
	Size       int64
	MediaType  string
	Visibility string
}

// ArtifactVersion 是已登记的产物版本。
type ArtifactVersion struct {
	TaskID     string
	ArtifactID string
	Version    int64
	SHA256     string
}

// TerminalProposal 是 Worker 发出的终态提议：Kind 取 result、error、paused 或 awaiting_input（会话 turn 等待用户回答，
// 契约 A），Ref 指向其内容（result 的内容含 session_state，按原样保存）。
type TerminalProposal struct {
	AttemptID string
	Kind      string
	Ref       string
}
