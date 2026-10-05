// Package api 是 HTTP 入口（后续计划实现）。本文件声明 API 层需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package api

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// Store 是 API 层的窄接口。每个方法是一个完整的事务用例；错误类别见 internal/persistence。
type Store interface {
	// CreateTask 创建任务。以 request_id 幂等：同 ID 同 body_hash 返回原结果（Replayed=true），
	// 同 ID 不同 body_hash 返回 persistence.ErrConflict。
	CreateTask(ctx context.Context, req CreateTaskRequest) (CreateTaskResult, error)
	// AcceptControl 写入控制意图并递增 control_version；以 request_id 幂等。
	AcceptControl(ctx context.Context, req ControlRequest) (ControlResult, error)
	// GetRequest 按 request_id 读取已提交的请求记录；不存在为 persistence.ErrNotFound。
	GetRequest(ctx context.Context, requestID string) (RequestRecord, error)
	// GetTask 读取任务的当前视图。
	GetTask(ctx context.Context, taskID string) (TaskView, error)
	// ListEvents 按 task_seq 升序返回 afterSeq 之后的至多 limit 条事件。
	ListEvents(ctx context.Context, taskID string, afterSeq int64, limit int) ([]Event, error)
	// ListTasks 按创建时间倒序 keyset 分页：after 为上一页最后一个 task_id（首页为空）；
	// 返回的游标为本页最后一个 task_id，没有下一页时为空。
	ListTasks(ctx context.Context, after string, limit int) ([]TaskView, string, error)
	// Inspect 读取任务的诊断时间线：attempt 与 outcome、退出与 OOM、环境与清理、checkpoint（规格 §14.6），
	// 以及 Gateway 调用与每次 try 的审计元数据（§9.9、G11：task_id → attempt_id → call_id → try_no）。
	Inspect(ctx context.Context, taskID string) (Inspection, error)
	// TaskResult 读取任务状态与固定的结果（tasks.result_json；规格 §5.6：输出固定为
	// (artifact_id, version, sha256)）。任务不存在为 persistence.ErrNotFound；没有结果时 Result 为 nil。
	TaskResult(ctx context.Context, taskID string) (ResultView, error)
	// PinnedArtifact 读取任务中一个产物的指定版本（version 为 0 时取最新版本）。只返回 visibility = output 且
	// blob 已授权到 scope_blobs(task) 的版本。任务不存在为 persistence.ErrNotFound；产物或版本不存在、为 internal
	// 或未授权时 found 为 false（与不存在不可区分）。
	PinnedArtifact(ctx context.Context, taskID, artifactID string, version int64) (ArtifactView, bool, error)
}

// Blobs 是 API 读取内容寻址存储的窄接口（blob.Store 满足）。
type Blobs interface {
	// Open 打开 blob 以读取。
	Open(sha256 string) (io.ReadCloser, error)
}

// ResultView 是 TaskResult 的结果。
type ResultView struct {
	Status string
	Result json.RawMessage
}

// ArtifactView 是一个已登记的产物版本：内容为 BlobStore 中的 SHA256，MediaType 为登记时的媒体类型。
type ArtifactView struct {
	ArtifactID string
	Version    int64
	SHA256     string
	Size       int64
	MediaType  string
}

// Inspection 是 Inspect 的结果。
type Inspection struct {
	Task        TaskView
	Attempts    []AttemptView
	Checkpoints []CheckpointView
	Calls       []CallView
}

// CallView 是一个 Gateway 逻辑调用的审计视图（§9.9）：只有元数据——端点、状态、费用、上游请求 ID 与
// 每次 try 的 attempt、结果、延迟、费用；不含请求或响应正文、提示词与凭据。ResultRef 是结果 blob 的
// sha256（只在 completed 时非空）。Model 是 journal 记录的解析后模型（chat 调用；其他端点为空）。
type CallView struct {
	CallID, Endpoint, State, Source   string
	Model                             string
	FirstAttemptID, UpstreamRequestID string
	ResultRef, FailReason             string
	SupersedesCallID, SupersedeReason string
	TriesUsed, CostChargedMicro       int64
	PossibleExternalDuplicate         bool
	CreatedAt, DeadlineAt             time.Time
	Tries                             []TryView
}

// TryView 是一次 try 的审计视图（call_tries）。
type TryView struct {
	TryNo                 int64
	AttemptID, EnvID      string
	State, Outcome, Error string
	LatencyMs, CostMicro  int64
}

// AttemptView 是一个 attempt 及其环境的诊断视图。
type AttemptView struct {
	AttemptID, Status, OutcomeClass string
	AttemptNo                       int64
	ExitCode, ExitSignal            *int64
	OOMKillDelta                    int64
	PlatformKilled                  bool
	EnvID, EnvStatus, CleanupState  string
	StoppedAt                       *time.Time
	CleanupTries                    int64
	CleanupError                    string
}

// CheckpointView 是一个已提交 checkpoint 的诊断视图。
type CheckpointView struct {
	CheckpointID, StepID, AttemptID string
	CommitSeq                       int64
	CommittedAt                     time.Time
}

// CreateTaskRequest 是创建任务的输入。TaskID 由调用方在事务前生成。
type CreateTaskRequest struct {
	RequestID       string
	BodyHash        []byte
	TaskID          string
	Spec            json.RawMessage
	ConfigVersion   string
	Limits          json.RawMessage
	MaxFaultRetries int64
}

// CreateTaskResult 是创建任务的结果。Replayed 表示返回的是同一请求先前提交的结果。
type CreateTaskResult struct {
	TaskID   string `json:"task_id"`
	Replayed bool   `json:"-"`
}

// ControlRequest 是一次控制请求：desired 取 run、pause 或 cancel。
type ControlRequest struct {
	RequestID string
	BodyHash  []byte
	TaskID    string
	Desired   string
	Reason    string
}

// ControlResult 是控制请求的结果。
type ControlResult struct {
	TaskID         string `json:"task_id"`
	ControlVersion int64  `json:"control_version"`
	Replayed       bool   `json:"-"`
}

// RequestRecord 是 api_requests 中的一行。
type RequestRecord struct {
	RequestID  string
	Kind       string
	BodyHash   []byte
	ResourceID string
	Response   json.RawMessage
}

// TaskView 是任务的当前视图。
type TaskView struct {
	TaskID                string
	Status                string
	StatusReason          string
	CurrentAttemptID      string
	Desired               string
	ControlVersion        int64
	AppliedControlVersion int64
	AttemptsTotal         int64
}

// Event 是事件流中的一条；host 事件的 WorkerSeq 为 0。
type Event struct {
	TaskSeq   int64
	AttemptID string
	Source    string
	Type      string
	WorkerSeq int64
	Payload   json.RawMessage
	TS        time.Time
}
