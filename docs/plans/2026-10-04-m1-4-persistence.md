# M1 Plan 4：PostgreSQL 持久化与 BlobStore 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 v0.2 M1 的持久化基础——PostgreSQL 上的 M1 全部表、事务辅助与错误契约、提交结果未知的幂等仲裁、advisory lock 与失锁信号、安装身份引导，以及本地内容寻址 BlobStore；首批事务用例覆盖 api、task、runner、resource 四个消费者。

**Architecture:** 消费者包（`internal/api`、`task`、`runner`、`resource`）声明窄接口，`internal/persistence/postgres` 反向依赖并实现它们；错误契约在无驱动依赖的 `internal/persistence`。每个事务用例的事务体本身幂等，在同一整体 deadline 内以同一身份重跑来仲裁提交结果未知，"查不到"不等于"没提交"。所有权分三处：`datadir`（flock 与 install_id 文件）、`postgres`（advisory lock 与失锁信号）、`ownership`（§7.4 决策表与引导编排）。

**Tech Stack:** Go 1.23（`go.mod` 声明 `go 1.23.0`）；pgx v5.7.6（v5.8 起要求更高的 Go 版本，故固定此版本）；PostgreSQL 16（`postgres:16-alpine`）。

**设计依据：** [Plan 4 设计](../design/2026-10-04-m1-4-persistence-design.md)（两轮评审结论已并入）；[v0.2 规格](../design/2026-10-03-v0.2-first-release-design.md) §6、§7、§14.1、§14.5；[代码组织设计](../design/2026-10-03-code-organization.md) §2–§4、§9。

## Global Constraints

- 本计划只实现设计文档 §1.3 所列 M1 表与首批事务用例；`recovery.Store` 交给 Plan 6，其所需字段由本计划的 schema 提供。
- **依赖方向**：消费者包与 `ownership`、`datadir`、`blob`、`persistence` 不依赖 `internal/persistence/postgres` 或 pgx（由 `internal/archtest` 固定）；纯决策代码不做 I/O。
- **事务**：`READ COMMITTED` + 显式行锁，锁顺序遵循规格 §7.1（`tasks → task_control → task_progress → task_event_seq → attempts → attempt_access → environments → artifact_heads`）；事务内不做网络、文件或沙箱操作。
- **前置条件**（设计 §2.6）：规格 §5.5、§8.1 要求的前置条件（控制写入规则、attempt 准入、判决对最新控制与当前 attempt 的核对、checkpoint 的 fencing 与引用授权）在同一事务内、持锁后检查；顺序固定为"身份查询 → 前置条件 → 写入"，不满足返回 `*persistence.RejectedError`，不留任何改变。
- **默认参数**（规格 §19）：操作整体 deadline 2 s；`lock_timeout` 1 s；`statement_timeout` 2 s；迁移超时 5 min；失锁检测周期 1 s、单次超时 1 s。
- **未知提交**（设计 §2.3）：查不到仍是未知；重跑由事务内的唯一约束与行锁仲裁；deadline 用完返回携带身份的 `*persistence.CommitUnknownError`；调用方在成功前不得启动依赖该写入的外部动作。
- **测试**：事务语义只在真实 PostgreSQL 上测试，不以 fake 代替；每个 Go 包一个 `*_test.go`（代码组织设计 §9.3）。`AGENTBOX_TEST_DATABASE_URL` 未设置时本地跳过、CI 中失败。
- 函数级质量约束见代码组织设计 §9；golangci-lint 只在 CI 中运行（本机未安装），本地以 `go vet` 与测试为准。
- **执行环境**：Go 只在 WSL Ubuntu 中运行（`/usr/local/go/bin`），WSL 无外网：Go 模块经本地文件代理 `F:\go-agentbox\.superpowers\goproxy` 获取（已含 pgx v5.7.6 及其依赖）；PostgreSQL 由 Windows 上的 Docker Desktop 运行，WSL 经 `127.0.0.1:5432` 访问。下文 WSL 命令统一带以下环境（已写在每条命令里）：`export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable";`
- **执行工作区**：`git worktree add ../go-agentbox-m1-4 -b m1-4-persistence m1-3-protocol-worker`（叠在 Plan 3 之上，二者都修改 `.github/workflows/ci.yml`）。Windows `F:\go-agentbox-m1-4`，WSL `/mnt/f/go-agentbox-m1-4`，Git Bash `/f/go-agentbox-m1-4`。
- 工作区为 CRLF：gofmt 检查以暂存内容为准（`scripts/dev/gofmt-staged.sh`，在 linked worktree 中需设置 `GIT_DIR=/mnt/f/go-agentbox/.git/worktrees/go-agentbox-m1-4 GIT_WORK_TREE=/mnt/f/go-agentbox-m1-4`）。新文件用编辑工具写入，不用 shell heredoc（会吞掉反斜杠）。
- **提交署名**：有真实共同作者时才加 `Co-Authored-By` 行，没有就省略；不得编造署名或邮箱，缺少署名也不阻止提交。执行者可把真实署名行放进环境变量 `COAUTHOR`；各任务的提交命令写作 `${COAUTHOR:+-m "$COAUTHOR"}`，未设置时自动省略。只暂存本任务列出的文件，禁止 `git add -A`。只在本地提交，不推送。

## 子 agent 上下文包

每个任务派发时附上：本任务全文；"Global Constraints"；设计文档 §1–§3 中与本任务相关的小节；已完成任务产出的接口签名（见各任务 Interfaces）。

**文件所有权**：每个任务只修改其 Files 列出的文件。`postgres_test.go` 由 Task 4 新建，Task 5、6、7 依次替换其 import 块并在末尾追加一节，因此这三个任务必须顺序执行。

## 派发批次

| 批次 | 任务 | 依赖 |
|---|---|---|
| 1 | Task 1 | — |
| 2 | Task 2 ‖ Task 3（可并行，各用独立 worktree） | Task 1 |
| 3 | Task 4 | Task 1、3 |
| 4 | Task 5 | Task 4 |
| 5 | Task 6 | Task 5 |
| 6 | Task 7 | Task 6 |
| 7 | Task 8 | Task 2–7 |

---
### Task 1：错误契约、消费者窄接口与依赖方向检查

**风险：中（接口形状被后续所有任务依赖）——单次评审。**

**Files:**
- Create: `internal/persistence/errors.go`
- Create: `internal/api/store.go`、`internal/task/store.go`、`internal/runner/store.go`、`internal/resource/store.go`
- Create: `internal/archtest/archtest_test.go`

**Interfaces:**
- Produces：`persistence.ErrContention`、`ErrUnavailable`、`ErrCommitUnknown`、`ErrConflict`、`ErrNotFound`、`ErrOwnershipLost`；`*persistence.CommitUnknownError{Op, Identity, Err}`（`errors.Is(err, ErrCommitUnknown)` 成立）；`persistence.CountsTowardFailureThreshold(err) bool`；`persistence.ErrRejected` 与 `*persistence.RejectedError{Code, Detail}`，原因码常量 `CodeTaskEnded`、`CodeCancelPending`、`CodeNotPaused`、`CodeNotRunnable`、`CodePreviousNotStopped`、`CodeStaleAttempt`、`CodeControlChanged`、`CodeRefNotAuthorized`。
- Produces：`api.Store`、`task.Store`、`runner.Store`、`resource.Store` 及其参数与结果类型（见下方代码）。这些包本身没有实现，只声明接口。`task.Verdict.ControlVersion` 是计算判决时读取的控制版本，提交时用于拒绝过期判决。

- [ ] **Step 1：写入错误契约与接口**

`internal/persistence/errors.go`：

```go
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
```

`internal/api/store.go`：

```go
// Package api 是 HTTP 入口（后续计划实现）。本文件声明 API 层需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package api

import (
	"context"
	"encoding/json"
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
```

`internal/task/store.go`：

```go
// Package task 是任务状态机与 task actor（后续计划实现）。本文件声明 task actor 需要的
// 持久化用例（代码组织设计 §4）；实现位于 internal/persistence/postgres。
package task

import (
	"context"
	"encoding/json"
)

// Store 是 task actor 的窄接口。每个方法是一个完整、幂等的事务用例：
// 提交结果未知时，实现会在同一个整体 deadline 内以同一身份重跑；仍无结论时返回
// *persistence.CommitUnknownError（设计 §2.3）。
type Store interface {
	// CreateAttempt 以事务前生成的 attempt_id 创建 attempt、其访问权与任务环境记录。
	CreateAttempt(ctx context.Context, a NewAttempt) (Attempt, error)
	// GetAttempt 读取 attempt；不存在为 persistence.ErrNotFound。
	GetAttempt(ctx context.Context, attemptID string) (Attempt, error)
	// ApplyControl 以 applied_control_version 的 CAS 应用控制意图。
	ApplyControl(ctx context.Context, c ApplyControl) (ControlState, error)
	// GetControlState 读取任务的控制版本与已应用版本。
	GetControlState(ctx context.Context, taskID string) (ControlState, error)
	// FinalizeAttempt 提交 attempt 的判决、任务状态与终态事件；完整判决内容一致才视为同一次提交。
	FinalizeAttempt(ctx context.Context, v Verdict) (Attempt, error)
}

// NewAttempt 是创建 attempt 的输入；AttemptID 与 EnvID 由调用方在事务前生成。
type NewAttempt struct {
	TaskID    string
	AttemptID string
	AttemptNo int64
	EnvID     string
}

// Attempt 是 attempts 中的一行。
type Attempt struct {
	AttemptID    string
	TaskID       string
	AttemptNo    int64
	EnvID        string
	Status       string
	OutcomeClass string
	VerdictHash  []byte
}

// ApplyControl 把任务的 applied_control_version 推进到 ControlVersion，并写入新的任务状态。
type ApplyControl struct {
	TaskID         string
	ControlVersion int64
	Status         string
	StatusReason   string
}

// ControlState 是任务的控制状态。
type ControlState struct {
	TaskID                string
	Desired               string
	ControlVersion        int64
	AppliedControlVersion int64
	Status                string
}

// Verdict 是 attempt 的完整判决。FromStatus 是 CAS 的来源状态，不属于判决内容；
// 其余字段共同决定 verdict_hash。
type Verdict struct {
	AttemptID        string
	TaskID           string
	ControlVersion   int64 // 计算判决时读取的 task_control.control_version；提交时不是最新则拒绝（control_changed）
	FromStatus       string
	AttemptStatus    string
	OutcomeClass     string
	ExitCode         *int64
	ExitSignal       *int64
	OOMKillDelta     int64
	PlatformKilled   bool
	TaskStatus       string
	TaskStatusReason string
	Result           json.RawMessage
	EventType        string
	EventPayload     json.RawMessage
}
```

`internal/runner/store.go`：

```go
// Package runner 是 AttemptRunner（后续计划实现）。本文件声明 runner 需要的持久化用例
// （代码组织设计 §4）；实现位于 internal/persistence/postgres。
package runner

import (
	"context"
	"encoding/json"
	"time"
)

// Store 是 runner 的窄接口。每个方法是一个完整、幂等的事务用例（设计 §2.3、§2.4）。
type Store interface {
	// AppendWorkerEvents 原子追加一批序号连续的 Worker 事件。与已存在部分重叠的事件逐条
	// 校验内容（同序号不同内容为 persistence.ErrConflict），只追加连续的新后缀；有缺口则拒绝。
	AppendWorkerEvents(ctx context.Context, attemptID string, events []WorkerEvent) (Watermark, error)
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

// Scope 标识 checkpoint 的所属范围；M1 只有 task。
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

// TerminalProposal 是 Worker 发出的终态提议：Kind 取 result、error 或 paused，Ref 指向其内容。
type TerminalProposal struct {
	AttemptID string
	Kind      string
	Ref       string
}
```

`internal/resource/store.go`：

```go
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
	// 结果丢失后重试返回原分配，不再占第二段。
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
```

- [ ] **Step 2：写入依赖方向检查**

这是守护性测试：当前没有任何包违反规则，它防止后续任务（尤其 Task 4 引入 pgx 之后）把驱动带进消费者包。此时只列出已存在的五个包；`ownership`、`datadir`、`blob` 由 Task 4 在引入 pgx 时加入列表（`go list` 对不存在的包会失败，不能提前列出，也不应以跳过的方式放宽检查）。

`internal/archtest/archtest_test.go`：

```go
// Package archtest 以测试固定包之间的依赖规则（代码组织设计 §3.2）。
//
// Plan 4 先固定持久化的依赖方向；其余规则随 Plan 6 加入同一文件。
package archtest

import (
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/wanghr0318-dotcom/go-agentbox"

// deps 返回包的全部传递依赖（含自身），以 GOOS=linux 计算。
func deps(t *testing.T, pkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", module+"/"+pkg)
	cmd.Env = append(cmd.Environ(), "GOOS=linux")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	return strings.Fields(string(out))
}

// TestConsumersDoNotDependOnPostgres：消费者包与引导决策不依赖 PostgreSQL 实现或数据库驱动；
// 实现（internal/persistence/postgres）反向依赖消费者的契约（设计 §1.1）。
func TestConsumersDoNotDependOnPostgres(t *testing.T) {
	forbidden := []string{module + "/internal/persistence/postgres", "github.com/jackc/pgx"}
	for _, pkg := range []string{
		"internal/api", "internal/task", "internal/runner", "internal/resource", "internal/persistence",
	} {
		for _, d := range deps(t, pkg) {
			for _, f := range forbidden {
				if d == f || strings.HasPrefix(d, f+"/") {
					t.Errorf("%s 依赖了 %s", pkg, d)
				}
			}
		}
	}
}
```

- [ ] **Step 3：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/persistence internal/api internal/task internal/runner internal/resource internal/archtest; go vet ./internal/... && go test -count=1 ./internal/archtest/'
```

Expected: gofmt 无输出；`ok  .../internal/archtest`。

- [ ] **Step 4：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/persistence/errors.go internal/api/store.go internal/task/store.go internal/runner/store.go internal/resource/store.go internal/archtest/archtest_test.go && git commit -m "feat(persistence): 错误契约与 api/task/runner/resource 窄接口；依赖方向检查" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 2：数据目录所有权与 BlobStore

**风险：中（持久化写入顺序）——单次评审。**

**Files:**
- Create: `internal/datadir/datadir.go`、`internal/datadir/lock_unix.go`、`internal/datadir/lock_other.go`、`internal/datadir/datadir_test.go`
- Create: `internal/blob/blob.go`、`internal/blob/sync_unix.go`、`internal/blob/sync_other.go`、`internal/blob/blob_test.go`

**Interfaces:**
- Produces：`datadir.Acquire(dir) (*Lock, error)`、`(*Lock).Release() error`、`datadir.ErrLocked`；`datadir.NewIDFile(dir) IDFile`，`IDFile.Read() (id string, exists bool, err error)`、`IDFile.Write(id string) error`（满足 `ownership.IDFile`）。
- Produces：`blob.Store` 接口（`Put`、`Open`、`Stat`）、`blob.Ref{SHA256, Size}`、`blob.ErrNotFound`、`blob.NewLocal(root) (*Local, error)`。

- [ ] **Step 1：写失败的测试**

`internal/datadir/datadir_test.go`：

```go
//go:build unix

package datadir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLockIsExclusiveAndReleasable(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatalf("首次取锁: %v", err)
	}
	if _, err := Acquire(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("锁被持有时应返回 ErrLocked，得到 %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("释放: %v", err)
	}
	again, err := Acquire(dir)
	if err != nil {
		t.Fatalf("释放后重新取锁: %v", err)
	}
	_ = again.Release()
}

func TestIDFileWriteReadReplace(t *testing.T) {
	dir := t.TempDir()
	f := NewIDFile(dir)
	if _, exists, err := f.Read(); err != nil || exists {
		t.Fatalf("初始应不存在：exists=%v err=%v", exists, err)
	}
	for _, id := range []string{"install-a", "install-b"} {
		if err := f.Write(id); err != nil {
			t.Fatalf("写入 %s: %v", id, err)
		}
		got, exists, err := f.Read()
		if err != nil || !exists || got != id {
			t.Fatalf("读取得到 (%q, %v, %v)，期望 %q", got, exists, err, id)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(idName) {
		t.Fatalf("目录中应只剩 install_id，得到 %v", entries)
	}
}
```

`internal/blob/blob_test.go`：

```go
package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutIsContentAddressedAndIdempotent(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("# 报告\n")
	sum := sha256.Sum256(content)
	want := Ref{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content))}

	for i := 0; i < 2; i++ { // 第二次写入同一内容视为成功
		got, err := s.Put(context.Background(), bytes.NewReader(content))
		if err != nil || got != want {
			t.Fatalf("第 %d 次 Put 得到 (%+v, %v)，期望 %+v", i+1, got, err, want)
		}
	}
	r, err := s.Open(want.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if b, _ := io.ReadAll(r); !bytes.Equal(b, content) {
		t.Fatalf("读回内容不一致：%q", b)
	}
	if tmp, _ := os.ReadDir(filepath.Join(root, "tmp")); len(tmp) != 0 {
		t.Fatalf("临时目录应为空，得到 %d 项", len(tmp))
	}
}

func TestMissingAndInvalid(t *testing.T) {
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(strings.Repeat("a", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("不存在应为 ErrNotFound，得到 %v", err)
	}
	if _, err := s.Open("../etc/passwd"); err == nil {
		t.Fatal("非法 sha256 应被拒绝")
	}
}

func TestPutHonoursCancellation(t *testing.T) {
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, strings.NewReader("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消的 context 应使 Put 失败，得到 %v", err)
	}
	if tmp, _ := os.ReadDir(filepath.Join(s.root, "tmp")); len(tmp) != 0 {
		t.Fatalf("失败后临时目录应为空，得到 %d 项", len(tmp))
	}
}

// TestPutReplacesDamagedExistingBlob：内容路径上已有的文件不被信任——同样大小的损坏内容与
// 截断的内容都被刚校验过的副本替换。
func TestPutReplacesDamagedExistingBlob(t *testing.T) {
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("checkpoint state")
	ref, err := s.Put(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	for name, damaged := range map[string][]byte{
		"同样大小的损坏内容": bytes.Repeat([]byte("x"), len(content)),
		"截断":        content[:3],
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(s.path(ref.SHA256), damaged, 0o600); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Put(context.Background(), bytes.NewReader(content)); err != nil || got != ref {
				t.Fatalf("Put 应修复已有文件：(%+v, %v)", got, err)
			}
			if b, _ := os.ReadFile(s.path(ref.SHA256)); !bytes.Equal(b, content) {
				t.Fatalf("内容应被替换为校验过的副本，得到 %q", b)
			}
		})
	}
}
```

- [ ] **Step 2：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/datadir/ ./internal/blob/'
```

Expected: 编译失败（`undefined: Acquire`、`undefined: NewLocal` 等）。

- [ ] **Step 3：实现**

`install_id` 的写入顺序：临时文件 → 写入并 fsync → rename → **fsync 父目录**（rename 保证原子替换，父目录 fsync 保证断电后仍在）。blob 写入同理，且 blob 文件在引用它的事务提交之前持久化。

`internal/datadir/datadir.go`：

```go
// Package datadir 管理本机数据目录的所有权与安装身份文件（规格 §7.4）。
//
// 它只负责本机文件：`<data>/agentbox.lock` 上的 flock，以及 `<data>/install_id` 的读取与
// 持久化写入。数据库侧的所有权（advisory lock）由 internal/persistence/postgres 负责。
package datadir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	lockName = "agentbox.lock"
	idName   = "install_id"
)

// ErrLocked 表示另一个进程正持有数据目录锁。
var ErrLocked = errors.New("datadir: 数据目录已被另一个进程锁定")

// Lock 是 `<data>/agentbox.lock` 上的独占 flock，持有至 Release 或进程退出。
// 锁文件本身不删除、不重建（规格 §7.4）。
type Lock struct {
	f *os.File
}

// Acquire 以非阻塞方式取得数据目录锁；已被占用时返回 ErrLocked。
func Acquire(dir string) (*Lock, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("datadir: 打开锁文件: %w", err)
	}
	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Release 释放锁并关闭锁文件。
func (l *Lock) Release() error {
	return l.f.Close() // 关闭文件描述符即释放 flock
}

// IDFile 读写 `<data>/install_id`。
type IDFile struct {
	dir string
}

// NewIDFile 返回 dir 下的安装身份文件。
func NewIDFile(dir string) IDFile { return IDFile{dir: dir} }

// Read 读取安装身份；文件不存在时 exists 为 false。
func (f IDFile) Read() (id string, exists bool, err error) {
	b, err := os.ReadFile(filepath.Join(f.dir, idName))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("datadir: 读取 install_id: %w", err)
	}
	id = strings.TrimSpace(string(b))
	if id == "" {
		return "", false, fmt.Errorf("datadir: install_id 文件为空")
	}
	return id, true, nil
}

// Write 持久化写入安装身份：临时文件 → 写入并 fsync → rename → fsync 父目录。
// rename 保证原子替换（读者看到旧值或新值），父目录 fsync 保证断电后新目录项仍在。
func (f IDFile) Write(id string) error {
	tmp, err := os.CreateTemp(f.dir, idName+".tmp-*")
	if err != nil {
		return fmt.Errorf("datadir: 创建临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删
	if _, err := tmp.WriteString(id + "\n"); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("datadir: 写入临时文件: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("datadir: fsync 临时文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("datadir: 关闭临时文件: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(f.dir, idName)); err != nil {
		return fmt.Errorf("datadir: rename: %w", err)
	}
	return syncDir(f.dir)
}
```

`internal/datadir/lock_unix.go`：

```go
//go:build unix

package datadir

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrLocked
	}
	if err != nil {
		return fmt.Errorf("datadir: flock: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("datadir: 打开目录以 fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("datadir: fsync 目录: %w", err)
	}
	return nil
}
```

`internal/datadir/lock_other.go`：

```go
//go:build !unix

package datadir

import (
	"errors"
	"os"
)

// 执行主机只支持 Linux；其他平台只为开发机能够编译。
func lockFile(*os.File) error { return errors.New("datadir: flock 只在 unix 上支持") }

func syncDir(string) error { return nil }
```

`internal/blob/blob.go`：

```go
// Package blob 提供不可变的内容寻址存储（规格 §6；设计 §3.2）。
//
// 本包只存放字节；授权关联（scope_blobs）与来源（blob_provenance）由持久化层的事务用例写入。
// blob 文件在引用它的事务提交之前就已持久化。不做垃圾回收。
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrNotFound 表示 blob 不存在。
var ErrNotFound = errors.New("blob: 不存在")

// Ref 标识一个 blob：小写十六进制 sha256 与字节数。
type Ref struct {
	SHA256 string
	Size   int64
}

// Store 是内容寻址存储。
type Store interface {
	// Put 写入 r 的全部内容并返回其引用；内容已存在时视为成功。
	Put(ctx context.Context, r io.Reader) (Ref, error)
	// Open 打开 blob 以读取；不存在为 ErrNotFound。
	Open(sha256 string) (io.ReadCloser, error)
	// Stat 返回 blob 的引用；不存在为 ErrNotFound。
	Stat(sha256 string) (Ref, error)
}

// Local 是基于本地目录的实现：`<root>/tmp` 存放写入中的临时文件，
// `<root>/sha256/<前两位>/<其余>` 存放内容。
type Local struct {
	root string
}

// NewLocal 返回以 root 为根目录的本地存储。目录结构（含全部 256 个分片目录）在这里一次建好并
// fsync，因此 Put 不再创建目录，返回成功前只需 fsync 分片目录。
func NewLocal(root string) (*Local, error) {
	content := filepath.Join(root, "sha256")
	for _, d := range []string{filepath.Join(root, "tmp"), content} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("blob: 创建目录: %w", err)
		}
	}
	for i := 0; i < 256; i++ {
		if err := os.Mkdir(filepath.Join(content, fmt.Sprintf("%02x", i)), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("blob: 创建分片目录: %w", err)
		}
	}
	for _, d := range []string{content, root} {
		if err := syncDir(d); err != nil {
			return nil, err
		}
	}
	return &Local{root: root}, nil
}

func (l *Local) path(sum string) string {
	return filepath.Join(l.root, "sha256", sum[:2], sum[2:])
}

// Put 边写边哈希到临时文件，fsync 后 rename 到内容路径，再 fsync 分片目录。
//
// 内容路径已存在时也用刚校验过的副本原子替换，而不是直接返回成功：已存在的文件可能损坏
// （同样大小也不可信），也可能来自 rename 成功但目录 fsync 失败的上一次写入或并发的同一内容
// 写入。替换后再 fsync 目录，保证返回成功时 blob 已持久化。
func (l *Local) Put(ctx context.Context, r io.Reader) (Ref, error) {
	tmp, err := os.CreateTemp(filepath.Join(l.root, "tmp"), "put-*")
	if err != nil {
		return Ref{}, fmt.Errorf("blob: 创建临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), contextReader{ctx: ctx, r: r})
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return Ref{}, fmt.Errorf("blob: 写入: %w", err)
	}
	ref := Ref{SHA256: hex.EncodeToString(h.Sum(nil)), Size: size}
	dest := l.path(ref.SHA256)
	if err := os.Rename(tmpPath, dest); err != nil {
		return Ref{}, fmt.Errorf("blob: rename: %w", err)
	}
	if err := syncDir(filepath.Dir(dest)); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// Open 打开 blob 以读取。
func (l *Local) Open(sum string) (io.ReadCloser, error) {
	if !validSHA256(sum) {
		return nil, fmt.Errorf("blob: 非法的 sha256 %q", sum)
	}
	f, err := os.Open(l.path(sum))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// Stat 返回 blob 的引用。
func (l *Local) Stat(sum string) (Ref, error) {
	if !validSHA256(sum) {
		return Ref{}, fmt.Errorf("blob: 非法的 sha256 %q", sum)
	}
	fi, err := os.Stat(l.path(sum))
	if errors.Is(err, os.ErrNotExist) {
		return Ref{}, ErrNotFound
	}
	if err != nil {
		return Ref{}, err
	}
	return Ref{SHA256: sum, Size: fi.Size()}, nil
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// contextReader 使长时间的复制可以被取消。
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
```

`internal/blob/sync_unix.go`：

```go
//go:build unix

package blob

import (
	"fmt"
	"os"
)

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("blob: 打开目录以 fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("blob: fsync 目录: %w", err)
	}
	return nil
}
```

`internal/blob/sync_other.go`：

```go
//go:build !unix

package blob

// 执行主机只支持 Linux；其他平台只为开发机能够编译。
func syncDir(string) error { return nil }
```

- [ ] **Step 4：验证（含 Windows 构建）**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/datadir internal/blob; go vet ./internal/datadir/ ./internal/blob/ && GOOS=windows go build ./internal/datadir/ ./internal/blob/ && go test -count=1 ./internal/datadir/ ./internal/blob/'
```

Expected: gofmt 无输出；两个包 `ok`。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/datadir internal/blob && git commit -m "feat(datadir,blob): 数据目录 flock 与 install_id 持久化写入；本地内容寻址 BlobStore" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 3：安装身份引导决策（E46 决策表）

**风险：高（启动安全）——双评审。**

**Files:**
- Create: `internal/ownership/ownership.go`、`internal/ownership/ownership_test.go`

**Interfaces:**
- Produces：`ownership.Decide(DBState, FileState) Decision`（动作 `Initialize`、`WriteFileAndComplete`、`Complete`、`Proceed`、`Refuse`）；`ownership.InstallStore`（`InspectInstallation`、`InitializeInstallation`、`CompleteInstallation`，由 Task 4 的 postgres 实现）；`ownership.IDFile`（由 Task 2 的 datadir 实现）；`ownership.Bootstrap(ctx, store, file, newID) (string, error)`；`ownership.ErrRefused` 与 `*RefusedError{Reason}`。

- [ ] **Step 1：写失败的测试**

决策表测试逐行对应规格 §7.4；引导测试覆盖每个中断点后重新引导都能完成。

`internal/ownership/ownership_test.go`：

```go
package ownership

import (
	"context"
	"errors"
	"testing"
)

// TestDecideCoversSpecTable 逐行对应规格 §7.4 的决策表。
func TestDecideCoversSpecTable(t *testing.T) {
	empty := DBState{}
	noRecord := DBState{HasMigrations: true, HasAgentboxTables: true}
	unknown := DBState{HasAgentboxTables: true}
	pending := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a"}}
	done := DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: "a", Complete: true}}
	none, same, other := FileState{}, FileState{Exists: true, InstallID: "a"}, FileState{Exists: true, InstallID: "b"}

	cases := []struct {
		name string
		db   DBState
		file FileState
		want Action
	}{
		{"空库，无身份文件：全新安装", empty, none, Initialize},
		{"有 schema_migrations 无记录", noRecord, none, Refuse},
		{"有 schema_migrations 无记录（有文件）", noRecord, same, Refuse},
		{"空库但有身份文件", empty, same, Refuse},
		{"未知 schema", unknown, none, Refuse},
		{"未知 schema（有文件）", unknown, same, Refuse},
		{"pending，无身份文件：继续引导", pending, none, WriteFileAndComplete},
		{"pending，身份一致", pending, same, Complete},
		{"pending，身份不一致", pending, other, Refuse},
		{"complete，身份不一致", done, other, Refuse},
		{"complete，无身份文件", done, none, Refuse},
		{"complete，身份一致", done, same, Proceed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(c.db, c.file)
			if d.Action != c.want {
				t.Fatalf("得到 %v（%s），期望 %v", d.Action, d.Reason, c.want)
			}
			if (d.Action == Refuse) != (d.Reason != "") {
				t.Fatalf("只有拒绝时才有原因：%+v", d)
			}
		})
	}
}

type fakeStore struct {
	db        DBState
	failAfter string // 在该步骤成功后模拟进程中断
}

var errCrash = errors.New("模拟中断")

func (s *fakeStore) InspectInstallation(context.Context) (DBState, error) { return s.db, nil }

func (s *fakeStore) InitializeInstallation(_ context.Context, id string) error {
	s.db = DBState{HasMigrations: true, HasAgentboxTables: true, Installation: &Installation{InstallID: id}}
	if s.failAfter == "initialize" {
		return errCrash
	}
	return nil
}

func (s *fakeStore) CompleteInstallation(_ context.Context, id string) error {
	if s.db.Installation == nil || s.db.Installation.InstallID != id {
		return errors.New("CompleteInstallation 的 install_id 与 pending 记录不一致")
	}
	s.db.Installation.Complete = true
	return nil
}

type fakeFile struct {
	id        string
	exists    bool
	failAfter bool
}

func (f *fakeFile) Read() (string, bool, error) { return f.id, f.exists, nil }

func (f *fakeFile) Write(id string) error {
	f.id, f.exists = id, true
	if f.failAfter {
		return errCrash
	}
	return nil
}

// TestBootstrapResumesAfterEachInterruption 覆盖 E46 中初始迁移提交之后的两个中断点：每次中断后
// 重新引导都能完成，且中断时不返回 install_id。提交前中断由 postgres 包的 TestInstallationBootstrapE46
// 在真实事务上覆盖。
func TestBootstrapResumesAfterEachInterruption(t *testing.T) {
	for _, point := range []string{"initialize", "write"} {
		t.Run(point, func(t *testing.T) {
			store := &fakeStore{failAfter: point}
			file := &fakeFile{failAfter: point == "write"}
			if id, err := Bootstrap(context.Background(), store, file, func() string { return "new" }); !errors.Is(err, errCrash) || id != "" {
				t.Fatalf("第一次引导应中断且不返回 install_id，得到 (%q, %v)", id, err)
			}
			store.failAfter, file.failAfter = "", false
			id, err := Bootstrap(context.Background(), store, file, func() string { t.Fatal("不应再生成新 ID"); return "" })
			if err != nil || id != "new" || !store.db.Installation.Complete || file.id != "new" {
				t.Fatalf("重新引导得到 (%q, %v)，状态 %+v 文件 %+v", id, err, store.db.Installation, file)
			}
		})
	}
}

func TestBootstrapRejectsEmptyNewID(t *testing.T) {
	store := &fakeStore{}
	if id, err := Bootstrap(context.Background(), store, &fakeFile{}, func() string { return "" }); err == nil || id != "" {
		t.Fatalf("空的 install_id 应被拒绝，得到 (%q, %v)", id, err)
	}
	if store.db.Installation != nil {
		t.Fatal("拒绝空 install_id 时不应初始化数据库")
	}
}

func TestBootstrapRefusesWithReason(t *testing.T) {
	store := &fakeStore{db: DBState{HasMigrations: true}}
	_, err := Bootstrap(context.Background(), store, &fakeFile{}, func() string { return "x" })
	var refused *RefusedError
	if !errors.As(err, &refused) || !errors.Is(err, ErrRefused) || refused.Reason == "" {
		t.Fatalf("应拒绝并给出原因，得到 %v", err)
	}
}
```

- [ ] **Step 2：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/ownership/'
```

Expected: 编译失败（`undefined: Decide` 等）。

- [ ] **Step 3：实现**

`internal/ownership/ownership.go`：

```go
// Package ownership 实现安装身份引导与校验（规格 §7.4；设计 §3.3）。
//
// Decide 是纯函数，覆盖 §7.4 决策表的全部情况；Bootstrap 经两个窄接口编排引导步骤：
// InstallStore（数据库侧，由 internal/persistence/postgres 实现）与 IDFile（数据目录侧，
// 由 internal/datadir 实现）。本包不直接做 I/O。
package ownership

import (
	"context"
	"errors"
	"fmt"
)

// DBState 是数据库侧与安装身份有关的事实。
type DBState struct {
	HasMigrations     bool          // 存在 schema_migrations 表
	HasAgentboxTables bool          // 存在任何 agentbox 业务表
	Installation      *Installation // installation 记录；nil 表示无记录
}

// Installation 是 installation 表中的单行。
type Installation struct {
	InstallID string
	Complete  bool
}

// FileState 是数据目录中 install_id 文件的状态。
type FileState struct {
	Exists    bool
	InstallID string
}

// Action 是引导决策的动作。
type Action int

const (
	// Initialize：全新安装。初始迁移与 installation(新 ID, pending) 同一事务提交 → 写身份文件 → 置 complete。
	Initialize Action = iota + 1
	// WriteFileAndComplete：继续中断的引导，写身份文件后置 complete。
	WriteFileAndComplete
	// Complete：身份文件已一致，只需置 complete。
	Complete
	// Proceed：已完成且一致，正常启动。
	Proceed
	// Refuse：拒绝启动，需要人工处理。
	Refuse
)

// Decision 是一次决策的结果；Refuse 时 Reason 说明原因。
type Decision struct {
	Action Action
	Reason string
}

// Decide 按规格 §7.4 的决策表判定下一步。"表不存在"本身不被当作全新安装：
// 只有整个库为空且数据目录无身份时才初始化。
func Decide(db DBState, file FileState) Decision {
	empty := !db.HasMigrations && !db.HasAgentboxTables && db.Installation == nil
	switch {
	case empty && !file.Exists:
		return Decision{Action: Initialize}
	case empty:
		return refuse("数据库为空但数据目录已有 install_id：数据库丢失或被替换")
	case !db.HasMigrations:
		return refuse("存在 agentbox 表但没有 schema_migrations：未知 schema")
	case db.Installation == nil:
		return refuse("有 schema_migrations 但无 installation 记录：初始迁移与记录原子提交，只能来自外部改动、损坏，或该库属于另一个使用 schema_migrations 的应用")
	case file.Exists && file.InstallID != db.Installation.InstallID:
		return refuse(fmt.Sprintf("安装身份不一致：数据库 %q，数据目录 %q", db.Installation.InstallID, file.InstallID))
	case !db.Installation.Complete && !file.Exists:
		return Decision{Action: WriteFileAndComplete}
	case !db.Installation.Complete:
		return Decision{Action: Complete}
	case !file.Exists:
		return refuse("installation 已完成但数据目录没有 install_id：数据目录丢失或被替换")
	default:
		return Decision{Action: Proceed}
	}
}

func refuse(reason string) Decision { return Decision{Action: Refuse, Reason: reason} }

// InstallStore 是引导所需的数据库操作；调用方须已持有 advisory lock。
type InstallStore interface {
	InspectInstallation(ctx context.Context) (DBState, error)
	// InitializeInstallation 在同一事务中执行初始迁移并插入 installation(installID, pending)。
	InitializeInstallation(ctx context.Context, installID string) error
	// CompleteInstallation 把 installation 置为 complete。
	CompleteInstallation(ctx context.Context, installID string) error
}

// IDFile 是数据目录中的安装身份文件；Write 必须持久化（含父目录 fsync）。
type IDFile interface {
	Read() (id string, exists bool, err error)
	Write(id string) error
}

// ErrRefused 表示引导决策为拒绝启动；具体错误为 *RefusedError。
var ErrRefused = errors.New("ownership: 拒绝启动")

// RefusedError 携带拒绝启动的原因。
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string        { return "ownership: 拒绝启动: " + e.Reason }
func (e *RefusedError) Is(target error) bool { return target == ErrRefused }

// Bootstrap 执行安装身份引导与校验，返回本安装的 install_id。任何一步中断后重新调用，
// 都会按当时的事实继续或明确拒绝。newID 只在全新安装时调用。
//
// 前置条件（规格 §7.4 的顺序）：调用方已持有数据目录的 flock，然后已持有数据库 advisory lock；
// 两把锁共同保证同一时刻只有一个进程对同一数据目录与同一数据库执行引导。返回错误时 install_id
// 为空，调用方不得使用；拒绝原因不含数据目录与数据库位置，由装配层包装后报告给运维。
func Bootstrap(ctx context.Context, store InstallStore, file IDFile, newID func() string) (string, error) {
	id, err := bootstrap(ctx, store, file, newID)
	if err != nil {
		return "", err
	}
	return id, nil
}

func bootstrap(ctx context.Context, store InstallStore, file IDFile, newID func() string) (string, error) {
	db, err := store.InspectInstallation(ctx)
	if err != nil {
		return "", fmt.Errorf("ownership: 读取数据库状态: %w", err)
	}
	id, exists, err := file.Read()
	if err != nil {
		return "", fmt.Errorf("ownership: 读取 install_id: %w", err)
	}
	d := Decide(db, FileState{Exists: exists, InstallID: id})
	switch d.Action {
	case Initialize:
		id = newID()
		if id == "" {
			return "", errors.New("ownership: 生成的 install_id 为空")
		}
		if err := store.InitializeInstallation(ctx, id); err != nil {
			return "", fmt.Errorf("ownership: 初始化安装: %w", err)
		}
		return id, writeAndComplete(ctx, store, file, id)
	case WriteFileAndComplete:
		return db.Installation.InstallID, writeAndComplete(ctx, store, file, db.Installation.InstallID)
	case Complete:
		return id, complete(ctx, store, id)
	case Proceed:
		return id, nil
	default:
		return "", &RefusedError{Reason: d.Reason}
	}
}

func writeAndComplete(ctx context.Context, store InstallStore, file IDFile, id string) error {
	if err := file.Write(id); err != nil {
		return fmt.Errorf("ownership: 写入 install_id: %w", err)
	}
	return complete(ctx, store, id)
}

func complete(ctx context.Context, store InstallStore, id string) error {
	if err := store.CompleteInstallation(ctx, id); err != nil {
		return fmt.Errorf("ownership: 置 complete: %w", err)
	}
	return nil
}
```

- [ ] **Step 4：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/ownership; go vet ./internal/ownership/ && go test -count=1 ./internal/ownership/'
```

Expected: `ok  .../internal/ownership`。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/ownership && git commit -m "feat(ownership): 安装身份引导决策表与引导编排（规格 §7.4）" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 4：PostgreSQL 基础——迁移、事务辅助、advisory lock 与安装存储

**风险：高（事务语义、所有权）——双评审。**

**Files:**
- Modify: `go.mod`、Create: `go.sum`（引入 pgx v5.7.6）
- Create: `deploy/docker-compose.yml`
- Create: `internal/persistence/postgres/migrations/0001_init.sql`
- Create: `internal/persistence/postgres/postgres.go`、`lock.go`、`migrate.go`
- Create: `internal/persistence/postgres/postgres_test.go`（第一节；Task 5–7 依次追加）
- Modify: `internal/archtest/archtest_test.go`（把 Task 2、3 新建的包加入依赖方向检查）

**Interfaces:**
- Consumes：Task 1 的 `persistence` 错误；Task 3 的 `ownership.InstallStore`、`ownership.Bootstrap`；Task 2 的 `datadir.IDFile`（测试中）。
- Produces：`postgres.Open(ctx, Options) (*Store, error)`、`(*Store).Close()`；`Options{DSN, OpDeadline, LockTimeout, StatementTimeout, Ownership}`；`(*Store).InspectInstallation`、`InitializeInstallation`、`CompleteInstallation`、`Migrate`；`postgres.AcquireOwnership(ctx, dsn, OwnershipOptions) (*Ownership, error)`、`ErrAlreadyOwned`、`(*Ownership).Lost()`、`IsLost()`、`Context()`、`Close()`。
- 包内供 Task 5–7 使用：`(*Store).run(ctx, op, identity, txFunc)`（幂等用例的执行框架）、`(*Store).read(ctx, op, func(ctx, queryer) error)`、`lockTask`、`lockTaskShared`、`nullJSON`、`contentHash`、`invalidf`、`conflictf`、`notFoundf`、`rejectf`、`errInvalid`；测试钩子 `hooks.beforeCommit`、`hooks.afterCommit`。

- [ ] **Step 1：引入依赖并启动 PostgreSQL**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && GOFLAGS=-mod=mod go get github.com/jackc/pgx/v5@v5.7.6 && go mod tidy && cat go.mod'
```

Expected: `go.mod` 与下方一致（`go mod tidy` 在 Task 4 的代码写入后才会保留 pgx；可先写代码再执行本步，或执行两次）。`go.sum` 由命令生成，内容应与下方一致。

`go.mod`：

```text
module github.com/wanghr0318-dotcom/go-agentbox

go 1.23.0

require github.com/jackc/pgx/v5 v5.7.6

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)
```

`go.sum`：

```text
github.com/davecgh/go-spew v1.1.0/go.mod h1:J7Y8YcW2NihsgmVo/mv3lAwl/skON4iLHjSsI+c5H38=
github.com/davecgh/go-spew v1.1.1 h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=
github.com/davecgh/go-spew v1.1.1/go.mod h1:J7Y8YcW2NihsgmVo/mv3lAwl/skON4iLHjSsI+c5H38=
github.com/jackc/pgpassfile v1.0.0 h1:/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=
github.com/jackc/pgpassfile v1.0.0/go.mod h1:CEx0iS5ambNFdcRtxPj5JhEz+xB6uRky5eyVu/W2HEg=
github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 h1:iCEnooe7UlwOQYpKFhBabPMi4aNAfoODPEFNiAnClxo=
github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761/go.mod h1:5TJZWKEWniPve33vlWYSoGYefn3gLQRzjfDlhSJ9ZKM=
github.com/jackc/pgx/v5 v5.7.6 h1:rWQc5FwZSPX58r1OQmkuaNicxdmExaEz5A2DO2hUuTk=
github.com/jackc/pgx/v5 v5.7.6/go.mod h1:aruU7o91Tc2q2cFp5h4uP3f6ztExVpyVv88Xl/8Vl8M=
github.com/jackc/puddle/v2 v2.2.2 h1:PR8nw+E/1w0GLuRFSmiioY6UooMp6KJv0/61nB7icHo=
github.com/jackc/puddle/v2 v2.2.2/go.mod h1:vriiEXHvEE654aYKXXjOvZM39qJ0q+azkZFrfEOc3H4=
github.com/pmezard/go-difflib v1.0.0 h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=
github.com/pmezard/go-difflib v1.0.0/go.mod h1:iKH77koFhYxTK1pcRnkKkqfTogsbg7gZNVY4sRDYZ/4=
github.com/stretchr/objx v0.1.0/go.mod h1:HFkY916IF+rwdDfMAkV7OtwuqBVzrE8GR6GFx+wExME=
github.com/stretchr/testify v1.3.0/go.mod h1:M5WIy9Dh21IEIfnGCwXGc5bZfKNJtfHm1UVUgZn+9EI=
github.com/stretchr/testify v1.7.0/go.mod h1:6Fq8oRcR53rry900zMqJjRRixrwX3KX962/h/Wwjteg=
github.com/stretchr/testify v1.8.1 h1:w7B6lhMri9wdJUVmEZPGGhZzrYTPvgJArz7wNPgYKsk=
github.com/stretchr/testify v1.8.1/go.mod h1:w2LPCIKwWwSfY2zedu0+kehJoqGctiVI29o6fzry7u4=
golang.org/x/crypto v0.37.0 h1:kJNSjF/Xp7kU0iB2Z+9viTPMW4EqqsrywMXLJOOsXSE=
golang.org/x/crypto v0.37.0/go.mod h1:vg+k43peMZ0pUMhYmVAWysMK35e6ioLh3wB8ZCAfbVc=
golang.org/x/sync v0.13.0 h1:AauUjRAJ9OSnvULf/ARrrVywoJDy0YS2AwQ98I37610=
golang.org/x/sync v0.13.0/go.mod h1:1dzgHSNfp02xaA81J2MS99Qcpr2w7fw1gpm99rleRqA=
golang.org/x/text v0.24.0 h1:dd5Bzh4yt5KYA8f9CJHCP4FB4D51c2c6JvN37xJJkJ0=
golang.org/x/text v0.24.0/go.mod h1:L8rBsPeo2pSS+xqN0d5u2ikmjtmoJbDBT1b7nHvFCdU=
gopkg.in/check.v1 v0.0.0-20161208181325-20d25e280405/go.mod h1:Co6ibVJAznAaIkqp8huTwlJQCZ016jof/cbN4VW5Yz0=
gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c/go.mod h1:K4uyk7z7BCEPqu6E+C64Yfv1cQ7kz7rIZviUmN+EgEM=
gopkg.in/yaml.v3 v3.0.1 h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=
gopkg.in/yaml.v3 v3.0.1/go.mod h1:K4uyk7z7BCEPqu6E+C64Yfv1cQ7kz7rIZviUmN+EgEM=
```

`deploy/docker-compose.yml`：

```yaml
# 最小运行依赖（规格 §3.1）：PostgreSQL 是唯一的元数据存储。Redis 随 M3 加入。
# 本地开发与测试：
#   docker compose -f deploy/docker-compose.yml up -d
#   export AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable'
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: agentbox
      POSTGRES_PASSWORD: agentbox
      POSTGRES_DB: agentbox
    ports:
      - "127.0.0.1:5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U agentbox -d agentbox"]
      interval: 2s
      timeout: 2s
      retries: 30
    volumes:
      - agentbox-pg:/var/lib/postgresql/data

volumes:
  agentbox-pg:
```

```bash
cd /f/go-agentbox-m1-4 && docker compose -f deploy/docker-compose.yml up -d && docker compose -f deploy/docker-compose.yml ps
```

Expected: `postgres` 服务为 healthy。若 `127.0.0.1:5432` 上已有同凭据的 PostgreSQL 容器在运行，直接复用即可。

- [ ] **Step 2：写失败的测试**

第一节覆盖：E46 的空库首启、三个中断点与三种拒绝；事务辅助的死锁重跑（E12 存储部分）、锁争用归类、提交结果始终未知时返回带身份的错误；失锁检测与在途操作取消（E13 存储部分）。这一节只用一张测试用的 `probe` 表，不依赖任何业务用例。

`internal/persistence/postgres/postgres_test.go`：

```go
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// ---- 基础：测试数据库、安装引导（E46）、事务辅助（E12、争用、未知提交）、失锁（E13） ----

// 事务语义只在真实 PostgreSQL 上测试（代码组织设计 §4.2）。本地未设置
// AGENTBOX_TEST_DATABASE_URL 时跳过；CI 中未设置则失败，不允许静默跳过。
const dsnEnv = "AGENTBOX_TEST_DATABASE_URL"

func adminDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("CI 中必须设置 %s", dsnEnv)
		}
		t.Skipf("未设置 %s，跳过 PostgreSQL 测试", dsnEnv)
	}
	return dsn
}

// newDatabase 建立一个独立的临时数据库并在测试结束时删除，返回其 DSN。
func newDatabase(t *testing.T) string {
	t.Helper()
	admin := adminDSN(t)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "agentbox_test_" + hex.EncodeToString(b)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("创建测试数据库: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// newStore 返回已完成安装引导的 Store。
func newStore(t *testing.T, opt Options) *Store {
	t.Helper()
	if opt.DSN == "" {
		opt.DSN = newDatabase(t)
	}
	s, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.InitializeInstallation(context.Background(), "install-test"); err != nil {
		t.Fatalf("初始化: %v", err)
	}
	if err := s.CompleteInstallation(context.Background(), "install-test"); err != nil {
		t.Fatalf("完成安装: %v", err)
	}
	return s
}

func count(t *testing.T, s *Store, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// TestInstallationBootstrapE46 覆盖规格 E46：空库首启、三个中断点、以及三种拒绝启动的情况。
func TestInstallationBootstrapE46(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T) (*Store, datadir.IDFile) {
		s, err := Open(ctx, Options{DSN: newDatabase(t)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s, datadir.NewIDFile(t.TempDir())
	}
	var seq atomic.Int64
	newID := func() string { return fmt.Sprintf("install-%d", seq.Add(1)) }
	finish := func(t *testing.T, s *Store, f datadir.IDFile, want string) {
		t.Helper()
		id, err := ownership.Bootstrap(ctx, s, f, newID)
		if err != nil || (want != "" && id != want) {
			t.Fatalf("重新引导得到 (%q, %v)，期望 %q", id, err, want)
		}
		st, _ := s.InspectInstallation(ctx)
		fid, _, _ := f.Read()
		if st.Installation == nil || !st.Installation.Complete || st.Installation.InstallID != id || fid != id {
			t.Fatalf("引导后状态不一致：%+v 文件 %q", st.Installation, fid)
		}
	}
	refused := func(t *testing.T, s *Store, f datadir.IDFile) {
		t.Helper()
		if _, err := ownership.Bootstrap(ctx, s, f, newID); !errors.Is(err, ownership.ErrRefused) {
			t.Fatalf("应拒绝启动，得到 %v", err)
		}
	}

	t.Run("空库首启", func(t *testing.T) {
		s, f := open(t)
		finish(t, s, f, "")
	})
	t.Run("初始迁移提交前中断", func(t *testing.T) {
		s, f := open(t)
		fired := false
		s.hooks.beforeCommit = func(string) error { fired = true; return errors.New("模拟中断") }
		if _, err := ownership.Bootstrap(ctx, s, f, newID); err == nil || !fired {
			t.Fatalf("中断应使引导失败（钩子触发 %v），得到 %v", fired, err)
		}
		s.hooks.beforeCommit = nil
		if st, _ := s.InspectInstallation(ctx); st.HasMigrations || st.HasAgentboxTables {
			t.Fatalf("事务未提交时库应仍为空：%+v", st)
		}
		finish(t, s, f, "")
	})
	t.Run("提交后、写身份文件前中断", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-a"); err != nil {
			t.Fatal(err)
		}
		finish(t, s, f, "install-a")
	})
	t.Run("写身份文件后、置 complete 前中断", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-b"); err != nil {
			t.Fatal(err)
		}
		if err := f.Write("install-b"); err != nil {
			t.Fatal(err)
		}
		finish(t, s, f, "install-b")
	})
	t.Run("有 schema 无 installation 记录", func(t *testing.T) {
		s, f := open(t)
		if err := s.InitializeInstallation(ctx, "install-c"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx, "DELETE FROM installation"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
	t.Run("身份不一致", func(t *testing.T) {
		s, f := open(t)
		finish(t, s, f, "")
		if err := f.Write("install-other"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
	t.Run("未知 schema", func(t *testing.T) {
		s, f := open(t)
		if _, err := s.pool.Exec(ctx, "CREATE TABLE tasks (task_id text)"); err != nil {
			t.Fatal(err)
		}
		refused(t, s, f)
	})
}

// probeTable 建立一张只供事务辅助测试使用的表，含 id 为 1、2 的两行。
func probeTable(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		"CREATE TABLE probe (id int PRIMARY KEY, v text NOT NULL DEFAULT ''); INSERT INTO probe (id) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
}

func lockProbe(ctx context.Context, tx pgx.Tx, id int) error {
	_, err := tx.Exec(ctx, "SELECT 1 FROM probe WHERE id = $1 FOR UPDATE", id)
	return err
}

// holdProbe 在独立事务中持有 probe 行锁，测试结束时回滚。
func holdProbe(t *testing.T, s *Store, id int) {
	t.Helper()
	ctx := context.Background()
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(ctx) })
	if err := lockProbe(ctx, holder, id); err != nil {
		t.Fatal(err)
	}
}

// waitLockWait 等到当前数据库中有会话阻塞在行锁上，确认在途操作确实停在锁等待中。
func waitLockWait(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for count(t, s, `SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND wait_event_type = 'Lock'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("5 s 内在途操作没有阻塞在行锁上")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDeadlockIsRetried 覆盖 E12 的存储部分：两事务经屏障违反锁顺序形成环，
// 被中止的一方重跑，两者最终都提交。
func TestDeadlockIsRetried(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 10 * time.Second, LockTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second})
	probeTable(t, s)
	var barrier sync.WaitGroup
	barrier.Add(2)
	var attempts atomic.Int32
	lockBoth := func(first, second int) error {
		var round atomic.Int32
		return s.run(ctx, "deadlock", fmt.Sprint(first), func(ctx context.Context, tx pgx.Tx) error {
			attempts.Add(1)
			if err := lockProbe(ctx, tx, first); err != nil {
				return err
			}
			if round.Add(1) == 1 {
				barrier.Done()
				barrier.Wait() // 两边都持有第一把锁后再去拿第二把
			}
			return lockProbe(ctx, tx, second)
		})
	}
	errs := make(chan error, 2)
	go func() { errs <- lockBoth(1, 2) }()
	go func() { errs <- lockBoth(2, 1) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("死锁应被检测并重跑，得到 %v", err)
		}
	}
	if attempts.Load() < 3 {
		t.Fatalf("应有一方被中止后重跑（尝试次数 %d）", attempts.Load())
	}
}

// TestContentionAndUnknownDeadline：持有行锁使操作在 deadline 内得到 ErrContention（不计入
// 故障阈值）；提交结果始终未知时，deadline 用尽返回携带身份的 CommitUnknownError。
func TestContentionAndUnknownDeadline(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 800 * time.Millisecond, LockTimeout: 200 * time.Millisecond, StatementTimeout: 500 * time.Millisecond})
	probeTable(t, s)

	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, "SELECT 1 FROM probe WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	err = s.run(ctx, "contended", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
	_ = holder.Rollback(ctx)
	if !errors.Is(err, persistence.ErrContention) || persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("锁被持有时应为 ErrContention 且不计入故障阈值，得到 %v", err)
	}

	s.hooks.afterCommit = func(string) error { return errors.New("模拟：回复始终丢失") }
	err = s.run(ctx, "insert-probe", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
		return err
	})
	var unknown *persistence.CommitUnknownError
	if !errors.As(err, &unknown) || unknown.Identity != "probe-3" || !persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("应返回携带身份的 CommitUnknownError，得到 %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 1 {
		t.Fatalf("幂等的重跑只应留下一行，得到 %d", n)
	}
}

// TestOwnershipLossCancelsOperations 覆盖 E13 的存储部分：终止锁连接后检测到失锁，
// 在途数据库操作被取消，新操作被拒绝；记录检测延迟。
func TestOwnershipLossCancelsOperations(t *testing.T) {
	ctx := context.Background()
	dsn := newDatabase(t)
	own, err := AcquireOwnership(ctx, dsn, OwnershipOptions{CheckInterval: 200 * time.Millisecond, CheckTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = own.Close() }()
	if _, err := AcquireOwnership(ctx, dsn, OwnershipOptions{}); !errors.Is(err, ErrAlreadyOwned) {
		t.Fatalf("第二个实例应取不到 advisory lock，得到 %v", err)
	}
	s := newStore(t, Options{DSN: dsn, Ownership: own, OpDeadline: 10 * time.Second, LockTimeout: 9 * time.Second, StatementTimeout: 9 * time.Second})
	probeTable(t, s)

	holdProbe(t, s, 1)
	inflight := make(chan error, 1)
	go func() {
		inflight <- s.run(ctx, "inflight", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
	}()
	waitLockWait(t, s)

	// 只取本测试数据库中的锁连接：其他并行测试的库里也可能有同键的 advisory lock。
	var pid int
	if err := s.pool.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-own.Lost():
		latency, bound := time.Since(start), 2*(200*time.Millisecond+200*time.Millisecond)
		t.Logf("失锁检测延迟 %v", latency)
		if latency >= bound {
			t.Fatalf("失锁检测延迟 %v 超过 2*(CheckInterval+CheckTimeout) = %v", latency, bound)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("5 s 内没有检测到失锁")
	}
	select {
	case err := <-inflight:
		if !errors.Is(err, persistence.ErrOwnershipLost) {
			t.Fatalf("在途操作应以 ErrOwnershipLost 结束，得到 %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("在途操作没有被取消")
	}
	if _, err := s.InspectInstallation(ctx); !errors.Is(err, persistence.ErrOwnershipLost) {
		t.Fatalf("失锁后新操作应被拒绝，得到 %v", err)
	}
	if err := s.Migrate(ctx); !errors.Is(err, persistence.ErrOwnershipLost) {
		t.Fatalf("失锁后迁移应被拒绝，得到 %v", err)
	}
	if err := own.Close(); err != nil {
		t.Fatalf("关闭锁连接: %v", err)
	}
	if err := own.Close(); err != nil { // 幂等；随后 defer 中的第三次调用同样无害
		t.Fatalf("再次关闭应返回 nil，得到 %v", err)
	}
}

// TestErrorClassification 在真实 PostgreSQL 上核对事务辅助的错误归类与是否计入故障阈值。
func TestErrorClassification(t *testing.T) {
	ctx := context.Background()
	long := Options{OpDeadline: 10 * time.Second, LockTimeout: 9 * time.Second, StatementTimeout: 9 * time.Second}

	t.Run("事务中后端被终止", func(t *testing.T) {
		s := newStore(t, long)
		probeTable(t, s)
		pids, killed := make(chan int, 1), make(chan struct{})
		go func() {
			defer close(killed)
			pid := <-pids
			_, _ = s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
			// 等后端真正退出，事务体的下一条语句必然落在已终止的连接上。
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				var alive bool
				if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&alive); err != nil || !alive {
					return
				}
			}
		}()
		var attempts atomic.Int32
		var firstErr error
		err := s.run(ctx, "terminated", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
			n := attempts.Add(1)
			if n == 1 {
				var pid int
				if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
					return err
				}
				pids <- pid
				<-killed
			}
			_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
			if n == 1 {
				firstErr = err
			}
			return err
		})
		if err != nil || attempts.Load() != 2 {
			t.Fatalf("被终止的尝试应重跑并成功：err %v，尝试 %d 次", err, attempts.Load())
		}
		// 通常是 FATAL 57P01；若客户端先看到套接字关闭，则是网络错误——两者都应按连接错误重跑。
		if !isConnection(firstErr) {
			t.Fatalf("第一次尝试应以连接错误（57P01 或套接字关闭）结束，得到 %v", firstErr)
		}
		if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 1 {
			t.Fatalf("应只写入一行，得到 %d", n)
		}
	})

	t.Run("语句超时", func(t *testing.T) {
		s := newStore(t, Options{OpDeadline: 500 * time.Millisecond, LockTimeout: time.Second, StatementTimeout: 50 * time.Millisecond})
		var timeouts, attempts int
		var last error
		err := s.run(ctx, "slow", "1", func(ctx context.Context, tx pgx.Tx) error {
			attempts++
			_, last = tx.Exec(ctx, "SELECT pg_sleep(0.2)")
			if sqlState(last) == "57014" {
				timeouts++
			}
			return last
		})
		if !errors.Is(err, persistence.ErrUnavailable) || !persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("语句超时应为 ErrUnavailable 且计入故障阈值，得到 %v", err)
		}
		// 57014 在 deadline 内重跑；最后一次尝试可能被操作 deadline 截断，故只要求出现过 57014。
		if timeouts == 0 || attempts < 2 {
			t.Fatalf("语句超时应在 deadline 内重跑：尝试 %d 次，其中 57014 %d 次", attempts, timeouts)
		}
		// 底层错误以 %w 保留：最后一次尝试的错误，或它在事务体之前被 deadline 截断时的 context 错误。
		if !errors.Is(err, last) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("结果应以 %%w 保留最后一次的底层错误 %v，得到 %v", last, err)
		}
	})

	t.Run("事务体吞掉语句错误", func(t *testing.T) {
		s := newStore(t, long)
		var attempts atomic.Int32
		err := s.run(ctx, "swallow", "1", func(ctx context.Context, tx pgx.Tx) error {
			attempts.Add(1)
			_, _ = tx.Exec(ctx, "SELECT 1/0")
			return nil
		})
		if !errors.Is(err, errInvalid) || persistence.CountsTowardFailureThreshold(err) || attempts.Load() != 1 {
			t.Fatalf("应为 errInvalid、不计入阈值且只尝试一次：%v（尝试 %d 次）", err, attempts.Load())
		}
	})

	t.Run("COMMIT 时连接被终止", func(t *testing.T) {
		// 每次尝试在 COMMIT 前终止自己的后端：COMMIT 得到 FATAL 57P01，可能已到达服务端，
		// 只能视为结果未知；deadline 用尽时返回 CommitUnknownError。
		s := newStore(t, Options{OpDeadline: time.Second, LockTimeout: time.Second, StatementTimeout: time.Second})
		var pid int
		s.hooks.beforeCommit = func(string) error {
			_, _ = s.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				if count(t, s, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid) == 0 {
					break
				}
			}
			return nil
		}
		err := s.run(ctx, "commit-fatal", "1", func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		})
		var unknown *persistence.CommitUnknownError
		if !errors.As(err, &unknown) || !persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("COMMIT 得到 FATAL 时应为 CommitUnknownError，得到 %v", err)
		}
		if !isConnection(unknown.Err) {
			t.Fatalf("未知提交应携带连接错误（57P01 或套接字关闭），得到 %v", unknown.Err)
		}
	})

	t.Run("COMMIT 前调用方取消", func(t *testing.T) {
		// context 已结束时 COMMIT 不会发出（SafeToRetry）：确定未提交，归为调用方取消而非未知。
		s := newStore(t, long)
		probeTable(t, s)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		s.hooks.beforeCommit = func(string) error { cancel(); return nil }
		err := s.run(cctx, "cancel-before-commit", "probe-3", func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "INSERT INTO probe (id) VALUES (3) ON CONFLICT DO NOTHING")
			return err
		})
		if !errors.Is(err, context.Canceled) || errors.Is(err, persistence.ErrCommitUnknown) || persistence.CountsTowardFailureThreshold(err) {
			t.Fatalf("COMMIT 未发出时应为调用方取消、不是未知提交，得到 %v", err)
		}
		if n := count(t, s, "SELECT count(*) FROM probe WHERE id = 3"); n != 0 {
			t.Fatalf("未提交的事务不应留下行，得到 %d", n)
		}
	})

	t.Run("调用方取消", func(t *testing.T) {
		s := newStore(t, long)
		probeTable(t, s)
		holdProbe(t, s, 1)
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- s.run(cctx, "canceled", "1", func(ctx context.Context, tx pgx.Tx) error { return lockProbe(ctx, tx, 1) })
		}()
		waitLockWait(t, s)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || errors.Is(err, persistence.ErrUnavailable) || persistence.CountsTowardFailureThreshold(err) {
				t.Fatalf("调用方取消应包装 context.Canceled、不是 ErrUnavailable、不计入阈值，得到 %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("取消后操作没有结束")
		}
	})
}

// TestCompositeForeignKeys：任务内引用使用复合外键，事件不能指向另一个任务的 attempt（规格 §6）。
func TestCompositeForeignKeys(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (task_id, spec_json, status, max_fault_retries) VALUES ('A', '{}', 'queued', 0), ('B', '{}', 'queued', 0);
		INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ('b-1', 'B', 1, 'env-b', 'running');
		INSERT INTO blobs (sha256, size) VALUES ('h', 1);
		INSERT INTO artifact_heads (task_id, artifact_id) VALUES ('A', 'out'), ('B', 'out')`); err != nil {
		t.Fatal(err)
	}
	event := func(taskID string) error {
		_, err := s.pool.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, source, type, payload, content_hash)
			VALUES ($1, 1, 'k', 'b-1', 'host', 'x', '{}', '\x00')`, taskID)
		return err
	}
	artifact := func(taskID, artifactID string) error {
		_, err := s.pool.Exec(ctx, `INSERT INTO artifacts (task_id, artifact_id, version, sha256, size, media_type, visibility, attempt_id)
			VALUES ($1, $2, 1, 'h', 1, 'text/plain', 'output', 'b-1')`, taskID, artifactID)
		return err
	}
	if err := event("A"); sqlState(err) != "23503" {
		t.Fatalf("任务 A 的事件引用任务 B 的 attempt 应违反外键，得到 %v", err)
	}
	if err := artifact("A", "out"); sqlState(err) != "23503" {
		t.Fatalf("任务 A 的产物引用任务 B 的 attempt 应违反外键，得到 %v", err)
	}
	if err := artifact("B", "missing"); sqlState(err) != "23503" {
		t.Fatalf("没有 artifact_heads 的产物应违反外键，得到 %v", err)
	}
	if err := event("B"); err != nil {
		t.Fatalf("同一任务内的引用应被接受，得到 %v", err)
	}
	if err := artifact("B", "out"); err != nil {
		t.Fatalf("同一任务内的引用应被接受，得到 %v", err)
	}
}
```

- [ ] **Step 3：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/persistence/postgres/'
```

Expected: 编译失败（`undefined: Open`、`undefined: Store` 等）。

- [ ] **Step 4：实现**

`internal/persistence/postgres/migrations/0001_init.sql`：

```sql
-- 0001：M1 需要的全部表（规格 §6；设计 §1.3）。
-- 与 installation(新 ID, pending) 在同一事务中执行（规格 §7.4）。
-- 相对 §6 的补充列：events.worker_seq / content_hash，attempts.verdict_hash /
-- terminal_proposal_hash，uid_ranges.allocation_id（设计 §1.3、§2.4）。

CREATE TABLE installation (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    install_id text NOT NULL,
    state      text NOT NULL CHECK (state IN ('pending', 'complete')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_requests (
    request_id  text PRIMARY KEY,
    kind        text NOT NULL,
    body_hash   bytea NOT NULL,
    resource_id text NOT NULL,
    response    jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    task_id                    text PRIMARY KEY,
    session_id                 text,
    spec_json                  jsonb NOT NULL,
    config_version             text NOT NULL DEFAULT '',
    limits_json                jsonb,
    status                     text NOT NULL,
    status_reason              text NOT NULL DEFAULT '',
    current_attempt_id         text,
    resume_point               jsonb,
    base_session_checkpoint_id text,
    result_json                jsonb,
    attempts_total             bigint NOT NULL DEFAULT 0,
    fault_retries_used         bigint NOT NULL DEFAULT 0,
    max_fault_retries          bigint NOT NULL,
    oom_retries_used           bigint NOT NULL DEFAULT 0,
    run_time_ms                bigint NOT NULL DEFAULT 0,
    run_time_persisted_at      timestamptz,
    not_before                 timestamptz,
    applied_control_version    bigint NOT NULL DEFAULT 0,
    row_version                bigint NOT NULL DEFAULT 0,
    created_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE task_control (
    task_id         text PRIMARY KEY REFERENCES tasks (task_id),
    control_version bigint NOT NULL,
    desired         text NOT NULL CHECK (desired IN ('run', 'pause', 'cancel')),
    reason          text NOT NULL DEFAULT ''
);

CREATE TABLE task_progress (
    task_id              text PRIMARY KEY REFERENCES tasks (task_id),
    latest_checkpoint_id text,
    latest_commit_seq    bigint NOT NULL DEFAULT 0
);

CREATE TABLE task_event_seq (
    task_id text PRIMARY KEY REFERENCES tasks (task_id),
    next    bigint NOT NULL DEFAULT 0
);

CREATE TABLE attempts (
    attempt_id             text PRIMARY KEY,
    task_id                text NOT NULL REFERENCES tasks (task_id),
    attempt_no             bigint NOT NULL,
    env_id                 text NOT NULL,
    status                 text NOT NULL,
    outcome_class          text NOT NULL DEFAULT '',
    terminal_proposal      text,
    terminal_proposal_ref  text,
    terminal_proposal_hash bytea,
    verdict_hash           bytea,
    exit_code              bigint,
    exit_signal            bigint,
    oom_kill_delta         bigint NOT NULL DEFAULT 0,
    platform_killed        boolean NOT NULL DEFAULT false,
    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task_id, attempt_id),
    UNIQUE (task_id, attempt_no)
);

CREATE TABLE attempt_access (
    attempt_id text PRIMARY KEY,
    task_id    text NOT NULL,
    state      text NOT NULL CHECK (state IN ('active', 'revoked')),
    revoked_at timestamptz,
    reason     text NOT NULL DEFAULT '',
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);

CREATE TABLE events (
    task_id      text NOT NULL REFERENCES tasks (task_id),
    task_seq     bigint NOT NULL,
    event_key    text NOT NULL,
    attempt_id   text,
    subrun_id    text,
    worker_seq   bigint,
    source       text NOT NULL CHECK (source IN ('host', 'worker')),
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    content_hash bytea NOT NULL,
    ts           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, task_seq),
    UNIQUE (task_id, event_key),
    CHECK ((source = 'worker') = (worker_seq IS NOT NULL)),
    -- 任务内引用用复合外键，防止跨任务引用（规格 §6）；MATCH SIMPLE：attempt_id 为 NULL 的宿主事件不受约束
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);
CREATE UNIQUE INDEX events_worker_seq ON events (attempt_id, worker_seq) WHERE worker_seq IS NOT NULL;

CREATE TABLE environments (
    env_id        text PRIMARY KEY,
    kind          text NOT NULL CHECK (kind IN ('task', 'session', 'exec')),
    session_id    text,
    attempt_id    text,
    status        text NOT NULL,
    sandbox_name  text NOT NULL DEFAULT '',
    uid_range_id  text,
    stopped_at    timestamptz,
    cleanup_state text NOT NULL DEFAULT 'none' CHECK (cleanup_state IN ('none', 'pending', 'done')),
    cleanup_tries bigint NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    cleanup_error text NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE uid_ranges (
    uid_range_id  text PRIMARY KEY,
    base          bigint NOT NULL UNIQUE,
    size          bigint NOT NULL,
    state         text NOT NULL CHECK (state IN ('free', 'assigned', 'quarantined')),
    owner_kind    text NOT NULL DEFAULT '',
    owner_id      text NOT NULL DEFAULT '',
    allocation_id text NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX uid_ranges_owner ON uid_ranges (owner_id) WHERE state = 'assigned';

CREATE TABLE resource_intents (
    intent_id text PRIMARY KEY,
    env_id    text NOT NULL,
    kind      text NOT NULL,
    name      text NOT NULL,
    state     text NOT NULL CHECK (state IN ('pending', 'acquired', 'released', 'failed')),
    UNIQUE (kind, name)
);

CREATE TABLE quarantined_resources (
    resource_path  text PRIMARY KEY,
    kind           text NOT NULL,
    observed_owner text NOT NULL DEFAULT '',
    reason         text NOT NULL,
    detected_at    timestamptz NOT NULL DEFAULT now(),
    alerted        boolean NOT NULL DEFAULT false
);

CREATE TABLE checkpoints (
    scope_kind    text NOT NULL,
    scope_id      text NOT NULL,
    checkpoint_id text NOT NULL,
    commit_seq    bigint NOT NULL,
    attempt_id    text NOT NULL,
    step_id       text NOT NULL,
    content_hash  bytea NOT NULL,
    state_inline  jsonb,
    state_ref     text,
    refs_json     jsonb NOT NULL DEFAULT '[]',
    subruns_json  jsonb NOT NULL DEFAULT '[]',
    committed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope_kind, scope_id, checkpoint_id),
    UNIQUE (scope_kind, scope_id, commit_seq),
    CHECK ((state_inline IS NULL) <> (state_ref IS NULL))
);

CREATE TABLE blobs (
    sha256 text PRIMARY KEY,
    size   bigint NOT NULL
);

CREATE TABLE scope_blobs (
    scope_kind text NOT NULL,
    scope_id   text NOT NULL,
    sha256     text NOT NULL REFERENCES blobs (sha256),
    PRIMARY KEY (scope_kind, scope_id, sha256)
);

CREATE TABLE blob_provenance (
    provenance_id bigserial PRIMARY KEY,
    scope_kind    text NOT NULL,
    scope_id      text NOT NULL,
    sha256        text NOT NULL REFERENCES blobs (sha256),
    source        text NOT NULL,
    ref           text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE artifact_heads (
    task_id      text NOT NULL REFERENCES tasks (task_id),
    artifact_id  text NOT NULL,
    next_version bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (task_id, artifact_id)
);

CREATE TABLE artifacts (
    task_id     text NOT NULL,
    artifact_id text NOT NULL,
    version     bigint NOT NULL,
    sha256      text NOT NULL REFERENCES blobs (sha256),
    size        bigint NOT NULL,
    media_type  text NOT NULL,
    visibility  text NOT NULL CHECK (visibility IN ('output', 'internal')),
    attempt_id  text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, artifact_id, version),
    UNIQUE (task_id, artifact_id, sha256),
    FOREIGN KEY (task_id, artifact_id) REFERENCES artifact_heads (task_id, artifact_id),
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts (task_id, attempt_id)
);
```

`internal/persistence/postgres/postgres.go`：

```go
// Package postgres 实现全部 SQL 与事务用例（规格 §6、§7；设计 §1–§3）。
//
// 它实现各消费者包声明的窄接口（api.Store、task.Store、runner.Store、resource.Store、
// ownership.InstallStore）；消费者不依赖本包。每个事务用例的事务体本身幂等：在事务内以
// 唯一约束与行锁按操作身份仲裁（设计 §2.3）。
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// 默认参数（规格 §19）。
const (
	DefaultOpDeadline       = 2 * time.Second
	DefaultLockTimeout      = time.Second
	DefaultStatementTimeout = 2 * time.Second
	migrationTimeout        = 5 * time.Minute
	rollbackTimeout         = time.Second // 延迟回滚的上限，连接卡住时不无限等待
)

// Options 配置 Store。零值字段取默认值。
type Options struct {
	DSN              string
	OpDeadline       time.Duration // 每个操作的整体 deadline，覆盖连接池获取、事务与全部重跑
	LockTimeout      time.Duration // SET LOCAL lock_timeout
	StatementTimeout time.Duration // SET LOCAL statement_timeout
	// Ownership 不为 nil 时，失去所有权后所有操作返回 persistence.ErrOwnershipLost，
	// 在途操作随之取消。
	Ownership *Ownership
}

// Store 是 PostgreSQL 上的事务用例实现。
type Store struct {
	pool  *pgxpool.Pool
	opt   Options
	hooks hooks
}

// hooks 只供本包测试注入故障，生产代码中恒为零值。
type hooks struct {
	// beforeCommit 在 COMMIT 之前调用；返回错误则回滚，模拟提交前中断。可以阻塞。
	beforeCommit func(op string) error
	// afterCommit 在 COMMIT 成功之后调用；返回错误则模拟"已提交但回复丢失"。
	afterCommit func(op string) error
}

// Open 连接数据库并返回 Store。
func Open(ctx context.Context, opt Options) (*Store, error) {
	if opt.OpDeadline == 0 {
		opt.OpDeadline = DefaultOpDeadline
	}
	if opt.LockTimeout == 0 {
		opt.LockTimeout = DefaultLockTimeout
	}
	if opt.StatementTimeout == 0 {
		opt.StatementTimeout = DefaultStatementTimeout
	}
	pool, err := pgxpool.New(ctx, opt.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: 连接池: %w", err)
	}
	return &Store{pool: pool, opt: opt}, nil
}

// Close 关闭连接池。
func (s *Store) Close() { s.pool.Close() }

func (s *Store) lost() bool { return s.opt.Ownership != nil && s.opt.Ownership.IsLost() }

// opContext 为一次操作建立整体 deadline；失去所有权时立即取消。
func (s *Store) opContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return s.boundContext(ctx, s.opt.OpDeadline)
}

// boundContext 从调用方 context 派生带超时的 context；配置了 Ownership 时失去所有权即取消。
func (s *Store) boundContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if s.lost() {
		return nil, nil, persistence.ErrOwnershipLost
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	if s.opt.Ownership == nil {
		return ctx, cancel, nil
	}
	stop := context.AfterFunc(s.opt.Ownership.Context(), cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// rollback 以有界的 context 回滚；已提交或已关闭时为空操作。
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// txFunc 是一个事务体；必须幂等（设计 §2.3）。
type txFunc func(ctx context.Context, tx pgx.Tx) error

// commitUnknown 表示 COMMIT 可能已到达服务端而结果未知。
type commitUnknown struct{ err error }

func (e *commitUnknown) Error() string { return "COMMIT 结果未知: " + e.err.Error() }

// txOnce 执行一次事务尝试。
func (s *Store) txOnce(ctx context.Context, op string, fn txFunc) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollback(tx) // 已提交时为空操作
	if _, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
		durationSetting(s.opt.LockTimeout), durationSetting(s.opt.StatementTimeout)); err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(op); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return commitError(err)
	}
	if s.hooks.afterCommit != nil {
		if err := s.hooks.afterCommit(op); err != nil {
			return &commitUnknown{err: err}
		}
	}
	return nil
}

// commitError 归类 COMMIT 的错误。只有确定未提交时才按普通失败返回；COMMIT 可能已到达
// 服务端时一律为未知（设计 §2.2）。
func commitError(err error) error {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrTxCommitRollback):
		// 事务已处于失败状态，COMMIT 实际执行了回滚：事务体忽略了某条语句的错误（编程错误）。
		return fmt.Errorf("%w: 事务体吞掉了语句错误，事务已回滚: %w", errInvalid, err)
	case pgconn.SafeToRetry(err):
		return err // COMMIT 未发出（例如 context 已结束），确定未提交
	case errors.As(err, &pgErr) && severity(pgErr) == "ERROR" && !isConnectionCode(pgErr.Code):
		return err // 服务端明确拒绝提交（例如提交时的序列化失败）
	default:
		return &commitUnknown{err: err} // 含 FATAL/PANIC 与连接类 SQLSTATE：连接已断，结果未知
	}
}

// severity 优先取不随语言环境变化的严重级别。
func severity(e *pgconn.PgError) string {
	if e.SeverityUnlocalized != "" {
		return e.SeverityUnlocalized
	}
	return e.Severity
}

// run 在一个整体 deadline 内执行幂等事务用例。遇到提交结果未知或可重试的中止、锁超时、
// 语句超时、连接错误时，以同一身份在剩余时间内重跑；查不到不等于没提交，结论只由事务内的
// 仲裁给出。deadline 用完仍无结论时：曾出现提交结果未知 → *persistence.CommitUnknownError；
// 调用方 context 结束 → 包装调用方的 ctx.Err()；否则按最后一次错误归类为 ErrContention 或
// ErrUnavailable。
func (s *Store) run(ctx context.Context, op, identity string, fn txFunc) error {
	caller := ctx
	ctx, cancel, err := s.opContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	var unknown, last error
	contended := false // 本次操作中出现过锁超时或可重试的中止
	for attempt := 0; ; attempt++ {
		err := s.txOnce(ctx, op, fn)
		if err == nil {
			return nil
		}
		var cu *commitUnknown
		switch {
		case errors.As(err, &cu):
			unknown, last = cu.err, err
		case retryable(err):
			last = err
			contended = contended || isLockTimeout(err) || isRetryableAbort(err)
		default:
			return s.final(caller, ctx, op, identity, unknown, contended, err)
		}
		if s.lost() {
			return persistence.ErrOwnershipLost
		}
		if !sleepBackoff(ctx, attempt) {
			return s.final(caller, ctx, op, identity, unknown, contended, last)
		}
	}
}

// final 把最后一次错误归类为对消费者公开的错误，并以 %w 保留底层错误。caller 是调用方的
// context，ctx 是由它派生的操作 context。contended 表示本次操作曾因锁超时或可重试的中止而
// 重跑：此时 deadline 到期归为 ErrContention（不计入 Store 故障阈值）。调用方取消不是存储
// 故障：返回包装调用方 ctx.Err() 的错误，不计入阈值。
func (s *Store) final(caller, ctx context.Context, op, identity string, unknown error, contended bool, err error) error {
	switch {
	case s.lost():
		return persistence.ErrOwnershipLost
	case isDomain(err):
		return err
	case unknown != nil:
		return &persistence.CommitUnknownError{Op: op, Identity: identity, Err: unknown}
	case caller.Err() != nil:
		return fmt.Errorf("%s(%s): 调用方已结束: %w: %w", op, identity, caller.Err(), err)
	case isUniqueViolation(err):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrConflict, err)
	case isLockTimeout(err) || isRetryableAbort(err) || (contended && ctx.Err() != nil):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrContention, err)
	case ctx.Err() != nil || isConnection(err) || isStatementTimeout(err):
		return fmt.Errorf("%s(%s): %w: %w", op, identity, persistence.ErrUnavailable, err)
	default:
		return fmt.Errorf("%s(%s): %w", op, identity, err)
	}
}

// read 在整体 deadline 内执行只读查询，不重跑。
// queryer 是只读查询所需的最小接口；连接池与事务都满足它。
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) read(ctx context.Context, op string, fn func(ctx context.Context, q queryer) error) error {
	caller := ctx
	ctx, cancel, err := s.opContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if err := fn(ctx, s.pool); err != nil {
		return s.final(caller, ctx, op, "", nil, false, err)
	}
	return nil
}

func isDomain(err error) bool {
	return errors.Is(err, persistence.ErrConflict) || errors.Is(err, persistence.ErrNotFound) ||
		errors.Is(err, persistence.ErrRejected) || errors.Is(err, errInvalid)
}

// errInvalid 表示调用方传入了不合法的参数（编程错误），不重跑。
var errInvalid = errors.New("postgres: 参数不合法")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalid, fmt.Sprintf(format, args...))
}

func conflictf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", persistence.ErrConflict, fmt.Sprintf(format, args...))
}

// rejectf 返回前置条件不满足的错误（code 取 persistence.Code* 常量）。
func rejectf(code, format string, args ...any) error {
	return &persistence.RejectedError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func notFoundf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", persistence.ErrNotFound, fmt.Sprintf(format, args...))
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func isRetryableAbort(err error) bool  { c := sqlState(err); return c == "40001" || c == "40P01" }
func isLockTimeout(err error) bool     { return sqlState(err) == "55P03" }
func isUniqueViolation(err error) bool { return sqlState(err) == "23505" }

// isStatementTimeout：57014 是 statement_timeout 或取消请求导致的语句中止；context 结束引起的
// 情形由 final 按 context 归类。
func isStatementTimeout(err error) bool { return sqlState(err) == "57014" }

// isConnectionCode 判断 SQLSTATE 是否表示连接已断或服务端暂不可用：类 08（连接异常）、
// 57P01–57P03（管理员终止、崩溃恢复、暂不接受连接）、53300（连接数已满）。
func isConnectionCode(code string) bool {
	return strings.HasPrefix(code, "08") || code == "57P01" || code == "57P02" || code == "57P03" || code == "53300"
}

// isConnection 判断错误是否来自连接层：网络错误、连接建立失败、超时、context 结束，或服务端
// 以连接类 SQLSTATE 报告的错误（含包在 *pgconn.ConnectError 中的）。其他带 SQLSTATE 的错误
// 与没有 SQLSTATE 的编程错误不在此列，不会被重跑。
func isConnection(err error) bool {
	if isDomain(err) {
		return false
	}
	if c := sqlState(err); c != "" {
		return isConnectionCode(c)
	}
	var netErr net.Error
	var connErr *pgconn.ConnectError
	return errors.As(err, &netErr) || errors.As(err, &connErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// retryable 判断一次事务尝试的失败是否可以在剩余时间内以同一身份重跑。
// 唯一约束冲突也重跑：并发的同一身份写入在对方提交后由事务内仲裁得出结论。
func retryable(err error) bool {
	return isRetryableAbort(err) || isLockTimeout(err) || isUniqueViolation(err) || isStatementTimeout(err) || isConnection(err)
}

// sleepBackoff 以带抖动的指数退避等待；deadline 不足时返回 false。
func sleepBackoff(ctx context.Context, attempt int) bool {
	d := min(10*time.Millisecond<<min(attempt, 5), 200*time.Millisecond)
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < d {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// durationSetting 把时长换成 PostgreSQL 的毫秒设置；不足 1 ms 的正值取 1ms（0 表示不限）。
func durationSetting(d time.Duration) string {
	ms := d.Milliseconds()
	if d > 0 && ms == 0 {
		ms = 1
	}
	return fmt.Sprintf("%dms", ms)
}

// contentHash 计算若干字段的内容哈希；每段带长度前缀，避免拼接歧义。
func contentHash(parts ...[]byte) []byte {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	return h.Sum(nil)
}

// lockTask 以 FOR UPDATE 锁住任务行（锁顺序的起点之一，规格 §7.1）。
func lockTask(ctx context.Context, tx pgx.Tx, taskID string) error {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR UPDATE", taskID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	return err
}

// lockTaskShared 以 FOR SHARE 锁住任务行，阻止并发的生命周期更新改动它。
func lockTaskShared(ctx context.Context, tx pgx.Tx, taskID string) error {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM tasks WHERE task_id = $1 FOR SHARE", taskID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	return err
}

// nullJSON 把空的 JSON 映射为 SQL NULL。
func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}
```

`internal/persistence/postgres/lock.go`：

```go
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// advisoryKey 是本安装在数据库中的会话级 advisory lock 键（"agentbox" 的 ASCII）。
const advisoryKey int64 = 0x6167656e74626f78

// ErrAlreadyOwned 表示另一个实例正持有 advisory lock。
var ErrAlreadyOwned = errors.New("postgres: advisory lock 已被另一个实例持有")

// OwnershipOptions 配置失锁检测；零值字段取默认值（周期 1 s，单次检查超时 1 s）。
type OwnershipOptions struct {
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}

// Ownership 是专用连接上的会话级 advisory lock（规格 §7.4；设计 §3.4）。
// 失去锁连接或无法确认锁仍在时进入不可逆的 ownership_lost：Lost() 关闭，Context() 取消。
type Ownership struct {
	conn      *pgx.Conn // 只由监视 goroutine 使用；Close 先停止监视再使用
	ctx       context.Context
	cancel    context.CancelFunc
	lost      chan struct{}
	lostOnce  sync.Once
	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
	opt       OwnershipOptions
}

// AcquireOwnership 建立专用连接并取得 advisory lock；已被持有时返回 ErrAlreadyOwned。
func AcquireOwnership(ctx context.Context, dsn string, opt OwnershipOptions) (*Ownership, error) {
	if opt.CheckInterval == 0 {
		opt.CheckInterval = time.Second
	}
	if opt.CheckTimeout == 0 {
		opt.CheckTimeout = time.Second
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: 建立锁连接: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", advisoryKey).Scan(&acquired); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("postgres: 取得 advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close(context.Background())
		return nil, ErrAlreadyOwned
	}
	o := &Ownership{conn: conn, lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}), opt: opt}
	o.ctx, o.cancel = context.WithCancel(context.Background())
	go o.monitor()
	return o, nil
}

// Lost 在失去所有权时关闭。
func (o *Ownership) Lost() <-chan struct{} { return o.lost }

// IsLost 报告是否已失去所有权。
func (o *Ownership) IsLost() bool {
	select {
	case <-o.lost:
		return true
	default:
		return false
	}
}

// Context 在失去所有权时取消；业务操作的 context 由它派生。
func (o *Ownership) Context() context.Context { return o.ctx }

// Close 停止监视并释放锁与连接。之后 IsLost 为 true。幂等：再次调用返回 nil。
func (o *Ownership) Close() error {
	var err error
	o.closeOnce.Do(func() {
		close(o.stop)
		<-o.done
		o.markLost()
		err = o.conn.Close(context.Background()) // 关闭会话即释放会话级 advisory lock
	})
	return err
}

func (o *Ownership) markLost() {
	o.lostOnce.Do(func() {
		close(o.lost)
		o.cancel()
	})
}

func (o *Ownership) monitor() {
	defer close(o.done)
	t := time.NewTicker(o.opt.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-t.C:
			if !o.stillHeld() {
				o.markLost()
				return
			}
		}
	}
}

// stillHeld 在锁连接上确认本会话仍持有 advisory lock。
func (o *Ownership) stillHeld() bool {
	ctx, cancel := context.WithTimeout(context.Background(), o.opt.CheckTimeout)
	defer cancel()
	var held bool
	err := o.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted)",
	).Scan(&held)
	return err == nil && held
}
```

`internal/persistence/postgres/migrate.go`：

```go
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

var _ ownership.InstallStore = (*Store)(nil)

//go:embed migrations/*.sql
var migrationFS embed.FS

// agentboxTables 是本项目的业务表；任何一个存在而 schema_migrations 不存在即为未知 schema。
var agentboxTables = []string{
	"installation", "api_requests", "tasks", "task_control", "task_progress", "task_event_seq",
	"attempts", "attempt_access", "events", "environments", "uid_ranges", "resource_intents",
	"quarantined_resources", "checkpoints", "blobs", "scope_blobs", "blob_provenance",
	"artifact_heads", "artifacts",
}

// inspectTablesSQL 在同一份目录中查三组表名是否存在：pg_catalog 限定 current_schema()。
// pg_class 对所有角色可见，不像 information_schema 那样只列出角色有权限的表；只看表类关系
// （普通表、分区表、视图、物化视图、外部表）。
const inspectTablesSQL = `WITH rel AS (
	SELECT c.relname::text AS name
	FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))
SELECT EXISTS (SELECT 1 FROM rel WHERE name = ANY($1::text[])),
       EXISTS (SELECT 1 FROM rel WHERE name = ANY($2::text[])),
       EXISTS (SELECT 1 FROM rel WHERE name = ANY($3::text[]))`

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var ms []migration
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil {
			return nil, fmt.Errorf("postgres: 迁移文件名不合法: %s", e.Name())
		}
		b, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	if len(ms) == 0 || ms[0].version != 1 {
		return nil, errors.New("postgres: 缺少初始迁移 0001")
	}
	return ms, nil
}

// InspectInstallation 读取与安装身份有关的数据库事实（实现 ownership.InstallStore）。
func (s *Store) InspectInstallation(ctx context.Context) (ownership.DBState, error) {
	var st ownership.DBState
	err := s.read(ctx, "InspectInstallation", func(ctx context.Context, q queryer) error {
		var hasInstallation bool
		if err := q.QueryRow(ctx, inspectTablesSQL, []string{"schema_migrations"}, agentboxTables, []string{"installation"}).
			Scan(&st.HasMigrations, &st.HasAgentboxTables, &hasInstallation); err != nil {
			return err
		}
		if !hasInstallation {
			return nil
		}
		var inst ownership.Installation
		var state string
		err := q.QueryRow(ctx, "SELECT install_id, state FROM installation").Scan(&inst.InstallID, &state)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		inst.Complete = state == "complete"
		st.Installation = &inst
		return nil
	})
	return st, err
}

// InitializeInstallation 在同一事务中执行初始迁移并插入 installation(installID, pending)
// （规格 §7.4）。事务未提交则库仍为空，重启后重新引导。
func (s *Store) InitializeInstallation(ctx context.Context, installID string) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	return s.migrationTx(ctx, "InitializeInstallation", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE TABLE schema_migrations (
			version    integer PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		if err := applyMigration(ctx, tx, ms[0]); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO installation (install_id, state) VALUES ($1, 'pending')", installID)
		return err
	})
}

// CompleteInstallation 把 installation 置为 complete（实现 ownership.InstallStore）。
func (s *Store) CompleteInstallation(ctx context.Context, installID string) error {
	return s.run(ctx, "CompleteInstallation", installID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE installation SET state = 'complete' WHERE install_id = $1", installID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return conflictf("installation 中没有 install_id %q", installID)
		}
		return nil
	})
}

// Migrate 依次执行尚未应用的迁移，每个迁移一个事务。必须在安装引导完成之后调用。
func (s *Store) Migrate(ctx context.Context) error {
	ms, err := loadMigrations()
	if err != nil {
		return err
	}
	applied, err := s.appliedVersion(ctx)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if m.version <= applied {
			continue
		}
		if err := s.migrationTx(ctx, "Migrate", func(ctx context.Context, tx pgx.Tx) error {
			return applyMigration(ctx, tx, m)
		}); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, tx pgx.Tx, m migration) error {
	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("postgres: 迁移 %s: %w", m.name, err)
	}
	_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name)
	return err
}

// appliedVersion 在迁移超时内读取已应用的最高迁移版本；失去所有权时返回 ErrOwnershipLost。
func (s *Store) appliedVersion(ctx context.Context) (int, error) {
	ctx, cancel, err := s.boundContext(ctx, migrationTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	var applied int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(max(version), 0) FROM schema_migrations").Scan(&applied); err != nil {
		if s.lost() {
			return 0, persistence.ErrOwnershipLost
		}
		return 0, fmt.Errorf("postgres: 读取迁移版本: %w", err)
	}
	return applied, nil
}

// migrationTx 以独立的迁移超时（规格 §7.3：5 min）执行一个事务；不重跑。context 同时由调用方
// 与所有权派生：失去所有权时取消并返回 ErrOwnershipLost。
func (s *Store) migrationTx(ctx context.Context, op string, fn txFunc) error {
	ctx, cancel, err := s.boundContext(ctx, migrationTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if err := s.migrationTxOnce(ctx, op, fn); err != nil {
		if s.lost() {
			return persistence.ErrOwnershipLost
		}
		return err
	}
	return nil
}

func (s *Store) migrationTxOnce(ctx context.Context, op string, fn txFunc) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", op, err)
	}
	defer rollback(tx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(op); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
```

然后在 `internal/archtest/archtest_test.go` 中把包列表

```go
	for _, pkg := range []string{
		"internal/api", "internal/task", "internal/runner", "internal/resource", "internal/persistence",
	} {
```

替换为

```go
	for _, pkg := range []string{
		"internal/api", "internal/task", "internal/runner", "internal/resource",
		"internal/ownership", "internal/persistence", "internal/datadir", "internal/blob",
	} {
```

- [ ] **Step 5：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/persistence; go vet ./internal/persistence/... && GOOS=windows go build ./... && go test -count=1 ./internal/archtest/ && CI=true go test -count=1 -v ./internal/persistence/postgres/ 2>&1 | grep -E "^(--- |ok|FAIL)|失锁检测延迟"'
```

Expected: 全部 PASS（`TestInstallationBootstrapE46` 的 7 个子测试、`TestDeadlockIsRetried`、`TestContentionAndUnknownDeadline`、`TestOwnershipLossCancelsOperations`），日志中有失锁检测延迟；最后一行 `ok`。

- [ ] **Step 6：提交**

```bash
cd /f/go-agentbox-m1-4 && git add go.mod go.sum deploy/docker-compose.yml internal/persistence/postgres/migrations/0001_init.sql internal/persistence/postgres/postgres.go internal/persistence/postgres/lock.go internal/persistence/postgres/migrate.go internal/persistence/postgres/postgres_test.go internal/archtest/archtest_test.go && git commit -m "feat(persistence): PostgreSQL 迁移、事务辅助、advisory lock 与安装存储（E46、E12/E13 存储部分）" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 5：事件追加与 api、task 用例

**风险：高（状态机持久化、幂等）——双评审。**

**Files:**
- Create: `internal/persistence/postgres/events.go`、`api.go`、`task.go`
- Modify: `internal/persistence/postgres/postgres_test.go`（替换 import 块，末尾追加第二节）

**Interfaces:**
- Consumes：Task 4 的 `run`、`read`、`lockTask`、`nullJSON`、`contentHash`、错误辅助；Task 1 的 `api.Store`、`task.Store`。
- Produces：`*Store` 实现 `api.Store`（`CreateTask`、`AcceptControl`、`GetRequest`、`GetTask`、`ListEvents`）与 `task.Store`（`CreateAttempt`、`GetAttempt`、`ApplyControl`、`GetControlState`、`FinalizeAttempt`），前置条件按设计 §2.6；包内事件辅助 `lockEventSeq`、`nextEventSeq`、`appendHostEvent(hostEvent)`。

- [ ] **Step 1：写失败的测试**

第二节覆盖：五个用例的"提交后回复丢失"（E11a）、"第一次提交尚未完成时查询为空、重跑得到原结果"、同身份不同内容的冲突（包括同为 failed 但判决不同）、请求重放与读取接口；以及设计 §2.6 的前置条件——取消后创建 attempt、旧环境未停止时创建、创建后重复请求返回原结果；取消先提交（过期判决以 `control_changed` 拒绝，按最新控制重算后提交）、判决先提交（随后的取消以 `task_ended` 拒绝）、旧 attempt 的迟到判决（`stale_attempt`）；控制写入的 `cancel_pending` 与 `not_paused`。每个拒绝都用 `snapshot` 断言任务状态、指针、事件与产物均未改变。

把 `internal/persistence/postgres/postgres_test.go` 的 import 块**替换**为：

```go
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)
```

然后在 `internal/persistence/postgres/postgres_test.go` **末尾追加**：

```go
// ---- api 与 task 用例：未知提交（E11a）、首次提交未完成、同身份不同内容、重放与读取 ----

// useCase 是一次幂等事务用例调用及其"只有一份结果"的检查。
type useCase struct {
	op     string
	setup  func(t *testing.T, s *Store)
	call   func(s *Store) (any, error)
	unique string // 应恰有 1 行的计数查询
}

// checkCommitLost 覆盖 E11a：COMMIT 真正执行而客户端收到连接错误。用例必须在同一 deadline
// 内以同一身份重跑并得到原结果；重复调用返回同一结果；只有一份结果。
func checkCommitLost(t *testing.T, cases []useCase) {
	for _, uc := range cases {
		t.Run(uc.op, func(t *testing.T) {
			s := newStore(t, Options{})
			uc.setup(t, s)
			var lost atomic.Bool
			s.hooks.afterCommit = func(op string) error {
				if op == uc.op && lost.CompareAndSwap(false, true) {
					return errors.New("模拟：COMMIT 已执行但回复丢失")
				}
				return nil
			}
			got, err := uc.call(s)
			if err != nil {
				t.Fatalf("提交回复丢失后应解析为成功，得到 %v", err)
			}
			if !lost.Load() {
				t.Fatal("故障钩子没有触发")
			}
			s.hooks.afterCommit = nil
			again, err := uc.call(s)
			if err != nil || fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", got) {
				t.Fatalf("重复调用应返回原结果：%+v / %v，原为 %+v", again, err, got)
			}
			if n := count(t, s, uc.unique); n != 1 {
				t.Fatalf("%s 应恰有 1 行，得到 %d", uc.unique, n)
			}
		})
	}
}

// expectConflicts 断言每个调用都返回 persistence.ErrConflict。
func expectConflicts(t *testing.T, cases map[string]func() error) {
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, persistence.ErrConflict) {
				t.Fatalf("应为冲突，得到 %v", err)
			}
		})
	}
}

// fixture 建立一个任务及其第一次 attempt（att-<taskID>，环境 env-<taskID>）。
func fixture(t *testing.T, s *Store, taskID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), MaxFaultRetries: 3}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1, EnvID: "env-" + taskID}); err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
}

// verdict 是 att-<taskID> 在 control_version 1（desired = run）下的失败判决。
func verdict(taskID string, exit int64) task.Verdict {
	return task.Verdict{AttemptID: "att-" + taskID, TaskID: taskID, ControlVersion: 1, FromStatus: "starting", AttemptStatus: "ended",
		OutcomeClass: "worker_error", ExitCode: &exit, TaskStatus: "failed", TaskStatusReason: "worker_error",
		EventType: "attempt_ended", EventPayload: json.RawMessage(fmt.Sprintf(`{"exit":%d}`, exit))}
}

func withFixture(t *testing.T, s *Store) { fixture(t, s, "t1") }

// stopEnv 模拟 Reconciler 确认环境已停止（stopped_at 由 resource 用例写入，Task 7 才实现）。
func stopEnv(t *testing.T, s *Store, envID string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), "UPDATE environments SET stopped_at = now() WHERE env_id = $1", envID); err != nil {
		t.Fatal(err)
	}
}

// retryWithNewAttempt 走一次故障重试：att-<taskID> 裁决回到 queued，旧环境停止，创建第 2 个 attempt。
func retryWithNewAttempt(t *testing.T, s *Store, taskID string) {
	t.Helper()
	ctx := context.Background()
	v := verdict(taskID, 1)
	v.TaskStatus = "queued"
	if _, err := s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatalf("故障重试的判决: %v", err)
	}
	stopEnv(t, s, "env-"+taskID)
	if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att2-" + taskID, AttemptNo: 2, EnvID: "env2-" + taskID}); err != nil {
		t.Fatalf("第 2 个 attempt: %v", err)
	}
}

// expectRejected 断言 err 是原因码为 code 的 *persistence.RejectedError。
func expectRejected(t *testing.T, err error, code string) {
	t.Helper()
	var rej *persistence.RejectedError
	if !errors.As(err, &rej) || rej.Code != code || !errors.Is(err, persistence.ErrRejected) || persistence.CountsTowardFailureThreshold(err) {
		t.Fatalf("应以 %s 拒绝，得到 %v", code, err)
	}
}

// snapshot 记录任务的可观察状态，用于确认被拒绝的写入没有留下任何改变。
func snapshot(t *testing.T, s *Store, taskID string) string {
	t.Helper()
	var out string
	if err := s.pool.QueryRow(context.Background(), `SELECT concat_ws('|', t.status, t.current_attempt_id, t.row_version,
			t.applied_control_version, t.status_reason, c.desired, c.control_version, c.reason,
			p.latest_checkpoint_id, p.latest_commit_seq,
			(SELECT count(*) FROM events e WHERE e.task_id = t.task_id),
			(SELECT count(*) FROM attempts a WHERE a.task_id = t.task_id),
			(SELECT count(*) FROM attempts a WHERE a.task_id = t.task_id AND a.verdict_hash IS NOT NULL),
			(SELECT string_agg(concat_ws(':', a.attempt_id, a.status, a.outcome_class), ',' ORDER BY a.attempt_no)
				FROM attempts a WHERE a.task_id = t.task_id),
			(SELECT string_agg(x.state, ',' ORDER BY x.attempt_id) FROM attempt_access x WHERE x.task_id = t.task_id),
			(SELECT count(*) FROM environments v JOIN attempts a USING (attempt_id) WHERE a.task_id = t.task_id),
			(SELECT count(*) FROM api_requests q WHERE q.resource_id = t.task_id),
			(SELECT count(*) FROM artifacts r WHERE r.task_id = t.task_id))
		FROM tasks t JOIN task_control c USING (task_id) JOIN task_progress p USING (task_id) WHERE t.task_id = $1`, taskID).Scan(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestCreateAttemptAdmission 覆盖规格 §8.1 创建 attempt 的准入：前置条件在同一事务内检查，
// 被拒绝时任务不变；已创建的 attempt 重复请求时返回原结果（即使前置条件此时已不成立）。
func TestCreateAttemptAdmission(t *testing.T) {
	ctx := context.Background()
	t.Run("取消后创建", func(t *testing.T) {
		s := newStore(t, Options{})
		if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"})
		expectRejected(t, err, persistence.CodeNotRunnable)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("旧环境未停止时创建，停止后创建，重复请求", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		v := verdict("t1", 1)
		v.TaskStatus = "queued"
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		req := task.NewAttempt{TaskID: "t1", AttemptID: "att2-t1", AttemptNo: 2, EnvID: "env2-t1"}
		before := snapshot(t, s, "t1")
		_, err := s.CreateAttempt(ctx, req)
		expectRejected(t, err, persistence.CodePreviousNotStopped)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		stopEnv(t, s, "env-t1")
		first, err := s.CreateAttempt(ctx, req)
		if err != nil {
			t.Fatalf("旧环境停止后应能创建：%v", err)
		}
		again, err := s.CreateAttempt(ctx, req) // 任务已是 running，准入不再成立，但这是同一请求
		if err != nil || fmt.Sprintf("%+v", again) != fmt.Sprintf("%+v", first) {
			t.Fatalf("重复请求应返回原结果：%+v %v，原为 %+v", again, err, first)
		}
		if v, _ := s.GetTask(ctx, "t1"); v.Status != "running" || v.CurrentAttemptID != "att2-t1" || v.AttemptsTotal != 2 {
			t.Fatalf("任务应由第 2 个 attempt 运行：%+v", v)
		}
	})
}

// TestFinalizeArbitratesControl 覆盖规格 §8.1 最终裁决：判决在持有任务锁后核对最新控制与当前 attempt。
func TestFinalizeArbitratesControl(t *testing.T) {
	ctx := context.Background()
	succeeded := func(cv int64) task.Verdict {
		zero := int64(0)
		return task.Verdict{AttemptID: "att-t1", TaskID: "t1", ControlVersion: cv, FromStatus: "starting", AttemptStatus: "ended",
			OutcomeClass: "succeeded", ExitCode: &zero, TaskStatus: "succeeded", Result: json.RawMessage(`{"ok":true}`),
			EventType: "attempt_ended", EventPayload: json.RawMessage(`{"exit":0}`)}
	}
	t.Run("取消先提交，过期的成功判决被拒绝，按最新控制重算", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		c, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
		if err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err = s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeControlChanged)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.FinalizeAttempt(ctx, succeeded(c.ControlVersion)); !errors.Is(err, errInvalid) {
			t.Fatalf("desired = cancel 时不能裁决为 succeeded，得到 %v", err)
		}
		v := succeeded(c.ControlVersion)
		v.TaskStatus, v.TaskStatusReason = "cancelled", "completed_during_cancel"
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatalf("按最新控制重算的判决应提交：%v", err)
		}
		if got, _ := s.GetTask(ctx, "t1"); got.Status != "cancelled" {
			t.Fatalf("任务应为 cancelled：%+v", got)
		}
	})
	t.Run("判决先提交，随后的取消被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		if _, err := s.FinalizeAttempt(ctx, succeeded(1)); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
		expectRejected(t, err, persistence.CodeTaskEnded)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.GetRequest(ctx, "c1"); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("被拒绝的请求不应留下记录，得到 %v", err)
		}
	})
	t.Run("旧 attempt 的迟到判决被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		// 模拟恢复（Plan 6）把执行交还队列而旧 attempt 尚无判决：任务回到 queued，旧环境已停止。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'queued' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		stopEnv(t, s, "env-t1")
		if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att2-t1", AttemptNo: 2, EnvID: "env2-t1"}); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeStaleAttempt)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("任务已回到 queued 而尚无新 attempt，判决被拒绝", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		// 恢复已把任务交还队列，current_attempt_id 仍是 att-t1：判决不能把 queued 的任务裁决为终态。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'queued' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := s.FinalizeAttempt(ctx, succeeded(1))
		expectRejected(t, err, persistence.CodeStaleAttempt)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
}

// TestControlWriteRules 覆盖规格 §8.1 控制写入：已接受的 cancel 不可被覆盖；resume 要求 paused；
// 被拒绝的请求不留下任何改变与请求记录；request_id 不能被另一个任务重用。
func TestControlWriteRules(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	control := func(id, taskID, desired string) error {
		_, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: id, BodyHash: []byte(desired), TaskID: taskID, Desired: desired})
		return err
	}
	rejected := func(id, taskID, desired, code string) {
		t.Helper()
		before := snapshot(t, s, taskID)
		expectRejected(t, control(id, taskID, desired), code)
		if after := snapshot(t, s, taskID); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		if _, err := s.GetRequest(ctx, id); !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("被拒绝的请求 %s 不应留下记录，得到 %v", id, err)
		}
	}
	rejected("c1", "t1", "run", persistence.CodeNotPaused)
	if err := control("c2", "t1", "cancel"); err != nil {
		t.Fatal(err)
	}
	rejected("c3", "t1", "pause", persistence.CodeCancelPending)
	if err := control("c4", "t1", "cancel"); err != nil {
		t.Fatalf("重复 cancel 应被接受：%v", err)
	}

	// resume：经由用例到达 paused（接受 pause → pausing → 以当前控制版本裁决为 paused），再接受 run 并应用。
	if err := control("p1", "t2", "pause"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t2", ControlVersion: 2, Status: "pausing"}); err != nil {
		t.Fatalf("应用 pause：%v", err)
	}
	v := verdict("t2", 0)
	v.ControlVersion, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = 2, "paused", "paused", ""
	if _, err := s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatalf("暂停判决：%v", err)
	}
	if err := control("p2", "t2", "run"); err != nil {
		t.Fatalf("paused 的任务应能 resume：%v", err)
	}
	st, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t2", ControlVersion: 3, Status: "queued"})
	if err != nil || st.Status != "queued" || st.AppliedControlVersion != 3 {
		t.Fatalf("应用 resume：%+v %v", st, err)
	}
	if got, _ := s.GetTask(ctx, "t2"); got.Status != "queued" || got.Desired != "run" {
		t.Fatalf("resume 后任务应为 queued：%+v", got)
	}

	// 同一 request_id 与请求体用于另一个任务：冲突，而不是返回 t1 的结果。
	before := snapshot(t, s, "t2")
	if err := control("c2", "t2", "cancel"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("跨任务重用 request_id 应为冲突，得到 %v", err)
	}
	if after := snapshot(t, s, "t2"); after != before {
		t.Fatalf("冲突后任务不应改变：%s → %s", before, after)
	}
}

// TestApplyControlTransitions 覆盖规格 §8.1 控制应用的来源状态：判决后迟到的应用是重放，
// 被取代的版本为 control_changed，状态机不允许的转换为无效输入；被拒绝时任务不变。
func TestApplyControlTransitions(t *testing.T) {
	ctx := context.Background()
	accept := func(t *testing.T, s *Store, id, desired string) int64 {
		t.Helper()
		r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: id, BodyHash: []byte(desired), TaskID: "t1", Desired: desired})
		if err != nil {
			t.Fatal(err)
		}
		return r.ControlVersion
	}
	apply := func(s *Store, cv int64, status string) (task.ControlState, error) {
		return s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: cv, Status: status})
	}
	t.Run("判决后迟到的应用是重放，终态保持", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "cancel")
		v := verdict("t1", 1)
		v.ControlVersion, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = cv, "cancelled", "cancelled", ""
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.GetTask(ctx, "t1"); got.AppliedControlVersion != 2 {
			t.Fatalf("判决应推进 applied_control_version 到 2：%+v", got)
		}
		before := snapshot(t, s, "t1")
		st, err := apply(s, cv, "cancelling")
		if err != nil || st.Status != "cancelled" || st.AppliedControlVersion != 2 {
			t.Fatalf("迟到的应用应返回当前状态：%+v %v", st, err)
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("重放不应改变任务：%s → %s", before, after)
		}
	})
	t.Run("被取代的版本被拒绝，最新版本可应用", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		pause := accept(t, s, "c1", "pause")
		cancel := accept(t, s, "c2", "cancel")
		before := snapshot(t, s, "t1")
		_, err := apply(s, pause, "pausing")
		expectRejected(t, err, persistence.CodeControlChanged)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
		st, err := apply(s, cancel, "cancelling")
		if err != nil || st.Status != "cancelling" || st.AppliedControlVersion != cancel {
			t.Fatalf("最新版本应能应用：%+v %v", st, err)
		}
	})
	t.Run("状态机不允许的转换是无效输入", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "pause")
		before := snapshot(t, s, "t1")
		for _, status := range []string{"paused", "cancelling", "queued"} { // running + pause 只能到 pausing
			if _, err := apply(s, cv, status); !errors.Is(err, errInvalid) {
				t.Fatalf("running 在 desired = pause 时不能转换为 %s，得到 %v", status, err)
			}
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
	t.Run("成功判决后按判决版本迟到的应用是重放", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		zero := int64(0)
		v := verdict("t1", 0)
		v.ExitCode, v.OutcomeClass, v.TaskStatus, v.TaskStatusReason = &zero, "succeeded", "succeeded", ""
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		st, err := apply(s, 1, "running")
		if err != nil || st.Status != "succeeded" {
			t.Fatalf("迟到的应用应返回 succeeded：%+v %v", st, err)
		}
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("重放不应改变任务：%s → %s", before, after)
		}
	})
	t.Run("终态而控制未应用时被拒绝为 task_ended", func(t *testing.T) {
		s := newStore(t, Options{})
		fixture(t, s, "t1")
		cv := accept(t, s, "c1", "cancel")
		// 用例不会产生这种组合（判决会推进 applied_control_version）；直接写入以检查终态防线。
		if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'cancelled' WHERE task_id = 't1'"); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, s, "t1")
		_, err := apply(s, cv, "cancelling")
		expectRejected(t, err, persistence.CodeTaskEnded)
		if after := snapshot(t, s, "t1"); after != before {
			t.Fatalf("被拒绝后任务不应改变：%s → %s", before, after)
		}
	})
}

// TestHostEventIdempotency 直接驱动 appendHostEvent：同一 event_key 同内容返回原 task_seq，
// 内容不同为冲突；task_seq 从 1 起连续无空洞。
func TestHostEventIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	appendEvent := func(payload string) (int64, error) {
		var seq int64
		err := s.run(ctx, "AppendHostEvent", "t1/k1", func(ctx context.Context, tx pgx.Tx) error {
			if err := lockEventSeq(ctx, tx, "t1"); err != nil {
				return err
			}
			n, err := appendHostEvent(ctx, tx, hostEvent{taskID: "t1", key: "k1", typ: "probe", payload: []byte(payload)})
			seq = n
			return err
		})
		return seq, err
	}
	first, err := appendEvent(`{"n":1}`)
	if err != nil {
		t.Fatal(err)
	}
	again, err := appendEvent(`{"n":1}`)
	if err != nil || again != first {
		t.Fatalf("同一 event_key 同内容应返回原 task_seq %d，得到 %d %v", first, again, err)
	}
	if _, err := appendEvent(`{"n":2}`); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("同一 event_key 不同内容应为冲突，得到 %v", err)
	}
	if n, m := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1'"),
		count(t, s, "SELECT COALESCE(max(task_seq), 0)::int FROM events WHERE task_id = 't1'"); n != m || n != 3 {
		t.Fatalf("task_seq 应从 1 起连续：count = %d，max = %d", n, m)
	}
}

func TestCommitLostResolvesAPIAndTaskUseCases(t *testing.T) {
	ctx := context.Background()
	createTask := func(t *testing.T, s *Store) {
		if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	checkCommitLost(t, []useCase{
		{"CreateTask", func(*testing.T, *Store) {}, func(s *Store) (any, error) {
			r, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)})
			r.Replayed = false
			return r, err
		}, "SELECT count(*) FROM tasks"},
		{"AcceptControl", withFixture, func(s *Store) (any, error) {
			r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"})
			r.Replayed = false
			return r, err
		}, "SELECT count(*) FROM events WHERE type = 'control_accepted'"},
		{"CreateAttempt", createTask, func(s *Store) (any, error) {
			return s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"})
		}, "SELECT count(*) FROM attempts"},
		{"ApplyControl", func(t *testing.T, s *Store) {
			withFixture(t, s)
			if _, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "cancel"}); err != nil {
				t.Fatal(err)
			}
		}, func(s *Store) (any, error) {
			return s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: 2, Status: "cancelling"})
		}, "SELECT count(*) FROM events WHERE type = 'control_applied'"},
		{"FinalizeAttempt", withFixture, func(s *Store) (any, error) {
			return s.FinalizeAttempt(ctx, verdict("t1", 1))
		}, "SELECT count(*) FROM events WHERE type = 'attempt_ended'"},
	})
}

// TestPendingFirstCommitIsArbitratedByRetry：第一次提交尚未完成 → 新连接查询为空 →
// 以同一身份重跑（等待锁）→ 第一次随后提交成功 → 重跑得到原结果，表中只有一份。
func TestPendingFirstCommitIsArbitratedByRetry(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{OpDeadline: 5 * time.Second, LockTimeout: 4 * time.Second, StatementTimeout: 4 * time.Second})
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	s.hooks.beforeCommit = func(op string) error {
		if op == "CreateAttempt" && first.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
		return nil
	}
	req := task.NewAttempt{TaskID: "t1", AttemptID: "a1", AttemptNo: 1, EnvID: "e1"}
	type result struct {
		a   task.Attempt
		err error
	}
	results := make(chan result, 2)
	go func() { a, err := s.CreateAttempt(ctx, req); results <- result{a, err} }()
	<-held
	if _, err := s.GetAttempt(ctx, "a1"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("第一次提交完成前查询应为空（仍是未知），得到 %v", err)
	}
	go func() { a, err := s.CreateAttempt(ctx, req); results <- result{a, err} }()
	time.Sleep(200 * time.Millisecond) // 让重跑进入锁等待；结论不依赖这段时间
	close(release)
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil || r.a.AttemptID != "a1" {
			t.Fatalf("两次调用都应得到原结果，得到 %+v %v", r.a, r.err)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM attempts"); n != 1 {
		t.Fatalf("attempts 应只有 1 行，得到 %d", n)
	}
}

func TestAPIAndTaskConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	if _, err := s.FinalizeAttempt(ctx, verdict("t1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-t3", BodyHash: []byte("h"), TaskID: "t3", Spec: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	expectConflicts(t, map[string]func() error{
		"CreateTask 同 request_id 不同内容": func() error {
			_, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-t1", BodyHash: []byte("other"), TaskID: "t9", Spec: json.RawMessage(`{}`)})
			return err
		},
		"CreateAttempt 同 attempt_id 不同环境": func() error {
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att-t1", AttemptNo: 1, EnvID: "env-other"})
			return err
		},
		"CreateAttempt 同 attempt_no 不同 attempt_id": func() error {
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: "att-x", AttemptNo: 1, EnvID: "env-x"})
			return err
		},
		"FinalizeAttempt 同为 failed 但退出码不同": func() error {
			_, err := s.FinalizeAttempt(ctx, verdict("t1", 2))
			return err
		},
		"ApplyControl 版本尚未被接受": func() error {
			_, err := s.ApplyControl(ctx, task.ApplyControl{TaskID: "t1", ControlVersion: 9, Status: "x"})
			return err
		},
		"CreateTask 已存在的 task_id（新 request_id）": func() error {
			start := time.Now()
			_, err := s.CreateTask(ctx, api.CreateTaskRequest{RequestID: "req-new", BodyHash: []byte("h"), TaskID: "t1", Spec: json.RawMessage(`{}`)})
			if d := time.Since(start); d > 500*time.Millisecond { // 不应重试到 OpDeadline
				return fmt.Errorf("冲突应立即返回，耗时 %v（%v）", d, err)
			}
			return err
		},
		"CreateAttempt 已存在的 env_id": func() error {
			start := time.Now()
			// t3 处于 queued，准入成立；只有 env_id 已被 t1 的环境占用
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t3", AttemptID: "att-t3", AttemptNo: 1, EnvID: "env-t1"})
			if d := time.Since(start); d > 500*time.Millisecond {
				return fmt.Errorf("冲突应立即返回，耗时 %v（%v）", d, err)
			}
			return err
		},
	})
}

// TestRequestReplayAndReads：同一请求重放返回 Replayed，读取接口与事件顺序正确。
func TestRequestReplayAndReads(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	r, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "pause"})
	if err != nil || r.ControlVersion != 2 || r.Replayed {
		t.Fatalf("首次控制：%+v %v", r, err)
	}
	r2, err := s.AcceptControl(ctx, api.ControlRequest{RequestID: "c1", BodyHash: []byte("h"), TaskID: "t1", Desired: "pause"})
	if err != nil || !r2.Replayed || r2.ControlVersion != 2 {
		t.Fatalf("重放应返回原结果并标记 Replayed：%+v %v", r2, err)
	}
	rec, err := s.GetRequest(ctx, "c1")
	if err != nil || rec.Kind != "control" || rec.ResourceID != "t1" {
		t.Fatalf("GetRequest：%+v %v", rec, err)
	}
	v, err := s.GetTask(ctx, "t1")
	if err != nil || v.Desired != "pause" || v.ControlVersion != 2 || v.CurrentAttemptID != "att-t1" || v.AttemptsTotal != 1 {
		t.Fatalf("GetTask：%+v %v", v, err)
	}
	events, err := s.ListEvents(ctx, "t1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for i, e := range events {
		if e.TaskSeq != int64(i+1) {
			t.Fatalf("事件序号应从 1 起连续：%+v", events)
		}
		types = append(types, e.Type)
	}
	if strings.Join(types, ",") != "task_created,attempt_created,control_accepted" {
		t.Fatalf("事件顺序：%v", types)
	}
	if _, err := s.GetTask(ctx, "missing"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的任务应为 ErrNotFound，得到 %v", err)
	}
}
```

- [ ] **Step 2：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/persistence/postgres/'
```

Expected: 编译失败（`s.CreateTask undefined` 等）。

- [ ] **Step 3：实现**

`internal/persistence/postgres/events.go`：

```go
package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// lockEventSeq 锁住任务的事件序号行（规格 §7.1 锁顺序中位于 task_progress 之后、attempts 之前）。
// 同一任务的事件追加由此串行化。
func lockEventSeq(ctx context.Context, tx pgx.Tx, taskID string) error {
	var next int64
	err := tx.QueryRow(ctx, "SELECT next FROM task_event_seq WHERE task_id = $1 FOR UPDATE", taskID).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s 没有事件序号行", taskID)
	}
	return err
}

// nextEventSeq 分配下一个 task_seq；调用方必须已持有 lockEventSeq（规格 §7.2）。
func nextEventSeq(ctx context.Context, tx pgx.Tx, taskID string) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, "UPDATE task_event_seq SET next = next + 1 WHERE task_id = $1 RETURNING next", taskID).Scan(&seq)
	return seq, err
}

// hostEvent 是一条 host 事件。
type hostEvent struct {
	taskID    string
	key       string // event_key，去重身份
	attemptID string
	typ       string
	payload   []byte
}

// appendHostEvent 以 event_key 幂等追加 host 事件：已存在且内容相同返回原 task_seq，
// 内容不同为冲突（规格 §7.2）。调用方必须已持有 lockEventSeq。
func appendHostEvent(ctx context.Context, tx pgx.Tx, e hostEvent) (int64, error) {
	if e.payload == nil {
		e.payload = []byte("{}")
	}
	hash := contentHash([]byte(e.typ), []byte(e.attemptID), e.payload)
	var seq int64
	var existing []byte
	err := tx.QueryRow(ctx, "SELECT task_seq, content_hash FROM events WHERE task_id = $1 AND event_key = $2",
		e.taskID, e.key).Scan(&seq, &existing)
	switch {
	case err == nil && bytes.Equal(existing, hash):
		return seq, nil
	case err == nil:
		return 0, conflictf("事件 %s 已存在且内容不同", e.key)
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	seq, err = nextEventSeq(ctx, tx, e.taskID)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, source, type, payload, content_hash)
		VALUES ($1, $2, $3, NULLIF($4, ''), 'host', $5, $6, $7)`,
		e.taskID, seq, e.key, e.attemptID, e.typ, e.payload, hash)
	return seq, err
}
```

`internal/persistence/postgres/api.go`：

```go
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

var _ api.Store = (*Store)(nil)

// claimRequest 以 request_id 认领一次 API 请求（规格 §7.3）。已存在时：kind 与 body_hash
// 相同返回其响应（replayed），不同为冲突。新认领的行在 finishRequest 中写入结果。
// INSERT … ON CONFLICT 会等待并发的同一 request_id 事务结束后再判定。
func claimRequest(ctx context.Context, tx pgx.Tx, requestID, kind string, bodyHash []byte) (replayed bool, response []byte, err error) {
	tag, err := tx.Exec(ctx, `INSERT INTO api_requests (request_id, kind, body_hash, resource_id, response)
		VALUES ($1, $2, $3, '', 'null') ON CONFLICT (request_id) DO NOTHING`, requestID, kind, bodyHash)
	if err != nil {
		return false, nil, err
	}
	if tag.RowsAffected() == 1 {
		return false, nil, nil
	}
	var existingKind string
	var existingHash []byte
	if err := tx.QueryRow(ctx, "SELECT kind, body_hash, response FROM api_requests WHERE request_id = $1",
		requestID).Scan(&existingKind, &existingHash, &response); err != nil {
		return false, nil, err
	}
	if existingKind != kind || !bytes.Equal(existingHash, bodyHash) {
		return false, nil, conflictf("request_conflict: request_id %s 已用于不同的请求", requestID)
	}
	return true, response, nil
}

func finishRequest(ctx context.Context, tx pgx.Tx, requestID, resourceID string, result any) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "UPDATE api_requests SET resource_id = $2, response = $3 WHERE request_id = $1", requestID, resourceID, b)
	return err
}

// CreateTask 创建任务、控制行、进度行、事件序号行与 task_created 事件（实现 api.Store）。
func (s *Store) CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.CreateTaskResult, error) {
	if req.RequestID == "" || req.TaskID == "" || len(req.BodyHash) == 0 || len(req.Spec) == 0 {
		return api.CreateTaskResult{}, invalidf("CreateTask 缺少 request_id、task_id、body_hash 或 spec")
	}
	var res api.CreateTaskResult
	err := s.run(ctx, "CreateTask", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.CreateTaskResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, "create_task", req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			res.Replayed = true
			return json.Unmarshal(stored, &res)
		}
		var exists bool // 否则 INSERT 的 23505 会被当作可重试错误一直重试到期限
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM tasks WHERE task_id = $1)", req.TaskID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return conflictf("任务 %s 已存在", req.TaskID)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO tasks (task_id, spec_json, config_version, limits_json, status, max_fault_retries)
			VALUES ($1, $2, $3, $4, 'queued', $5)`,
			req.TaskID, []byte(req.Spec), req.ConfigVersion, nullJSON(req.Limits), req.MaxFaultRetries); err != nil {
			return err
		}
		for _, q := range []string{
			"INSERT INTO task_control (task_id, control_version, desired) VALUES ($1, 1, 'run')",
			"INSERT INTO task_progress (task_id) VALUES ($1)",
			"INSERT INTO task_event_seq (task_id) VALUES ($1)",
		} {
			if _, err := tx.Exec(ctx, q, req.TaskID); err != nil {
				return err
			}
		}
		if err := lockEventSeq(ctx, tx, req.TaskID); err != nil {
			return err
		}
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: req.TaskID, key: "task_created", typ: "task_created"}); err != nil {
			return err
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// AcceptControl 写入控制意图并递增 control_version（实现 api.Store）。
func (s *Store) AcceptControl(ctx context.Context, req api.ControlRequest) (api.ControlResult, error) {
	switch req.Desired {
	case "run", "pause", "cancel":
	default:
		return api.ControlResult{}, invalidf("desired 必须是 run、pause 或 cancel，得到 %q", req.Desired)
	}
	if req.RequestID == "" || req.TaskID == "" || len(req.BodyHash) == 0 {
		return api.ControlResult{}, invalidf("AcceptControl 缺少 request_id、task_id 或 body_hash")
	}
	var res api.ControlResult
	err := s.run(ctx, "AcceptControl", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.ControlResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, "control", req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			if res.TaskID != req.TaskID { // body_hash 不含 task_id 时，同一 request_id 可能被另一个任务重用
				return conflictf("request_conflict: request_id %s 已用于任务 %s", req.RequestID, res.TaskID)
			}
			res.Replayed = true
			return nil
		}
		var status, desired string
		err = tx.QueryRow(ctx, "SELECT status FROM tasks WHERE task_id = $1 FOR UPDATE", req.TaskID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", req.TaskID)
		}
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR UPDATE", req.TaskID).Scan(&desired); err != nil {
			return err
		}
		if err := admitControl(req.TaskID, status, desired, req.Desired); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE task_control SET control_version = control_version + 1, desired = $2, reason = $3
			WHERE task_id = $1 RETURNING control_version`, req.TaskID, req.Desired, req.Reason).Scan(&res.ControlVersion); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, req.TaskID); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"desired": req.Desired, "reason": req.Reason, "control_version": res.ControlVersion})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: req.TaskID, key: fmt.Sprintf("control_accepted:%d", res.ControlVersion),
			typ: "control_accepted", payload: payload}); err != nil {
			return err
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// admitControl 是规格 §8.1 的控制写入规则：终态任务不再接受控制；已接受的 cancel 不可被
// pause 或 resume 覆盖；resume（desired = run）要求任务处于 paused。
func admitControl(taskID, status, current, next string) error {
	switch {
	case status == "succeeded" || status == "failed" || status == "cancelled":
		return rejectf(persistence.CodeTaskEnded, "任务 %s 已是 %s", taskID, status)
	case current == "cancel" && next != "cancel":
		return rejectf(persistence.CodeCancelPending, "任务 %s 已接受 cancel", taskID)
	case next == "run" && status != "paused":
		return rejectf(persistence.CodeNotPaused, "任务 %s 处于 %s，不能 resume", taskID, status)
	}
	return nil
}

// GetRequest 读取已提交的请求记录（实现 api.Store）。
func (s *Store) GetRequest(ctx context.Context, requestID string) (api.RequestRecord, error) {
	r := api.RequestRecord{RequestID: requestID}
	err := s.read(ctx, "GetRequest", func(ctx context.Context, q queryer) error {
		var resp []byte
		err := q.QueryRow(ctx, "SELECT kind, body_hash, resource_id, response FROM api_requests WHERE request_id = $1",
			requestID).Scan(&r.Kind, &r.BodyHash, &r.ResourceID, &resp)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("请求 %s", requestID)
		}
		r.Response = resp
		return err
	})
	return r, err
}

// GetTask 读取任务的当前视图（实现 api.Store）。
func (s *Store) GetTask(ctx context.Context, taskID string) (api.TaskView, error) {
	v := api.TaskView{TaskID: taskID}
	err := s.read(ctx, "GetTask", func(ctx context.Context, q queryer) error {
		var current *string
		err := q.QueryRow(ctx, `SELECT t.status, t.status_reason, t.current_attempt_id, c.desired, c.control_version,
				t.applied_control_version, t.attempts_total
			FROM tasks t JOIN task_control c USING (task_id) WHERE t.task_id = $1`, taskID).
			Scan(&v.Status, &v.StatusReason, &current, &v.Desired, &v.ControlVersion, &v.AppliedControlVersion, &v.AttemptsTotal)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		if current != nil {
			v.CurrentAttemptID = *current
		}
		return err
	})
	return v, err
}

// ListEvents 按 task_seq 升序返回事件（实现 api.Store）。
func (s *Store) ListEvents(ctx context.Context, taskID string, afterSeq int64, limit int) ([]api.Event, error) {
	var out []api.Event
	err := s.read(ctx, "ListEvents", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT task_seq, COALESCE(attempt_id, ''), source, type, COALESCE(worker_seq, 0), payload, ts
			FROM events WHERE task_id = $1 AND task_seq > $2 ORDER BY task_seq LIMIT $3`, taskID, afterSeq, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e api.Event
			var payload []byte
			if err := rows.Scan(&e.TaskSeq, &e.AttemptID, &e.Source, &e.Type, &e.WorkerSeq, &payload, &e.TS); err != nil {
				return err
			}
			e.Payload = payload
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}
```

`internal/persistence/postgres/task.go`：

```go
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

var _ task.Store = (*Store)(nil)

// CreateAttempt 创建 attempt、attempt_access（active）与任务环境记录，并把任务从 queued 推进到 running
// （实现 task.Store）。同一 attempt_id 已存在时只比较内容并返回原结果；新建时在同一事务内检查准入
// 前置条件（规格 §8.1）：任务处于 queued、desired = run、旧执行的环境均已确认停止。
// 锁顺序：tasks → task_control → task_event_seq → attempts → attempt_access → environments。
func (s *Store) CreateAttempt(ctx context.Context, a task.NewAttempt) (task.Attempt, error) {
	if a.TaskID == "" || a.AttemptID == "" || a.EnvID == "" || a.AttemptNo < 1 {
		return task.Attempt{}, invalidf("CreateAttempt 缺少 task_id、attempt_id、env_id 或 attempt_no")
	}
	var out task.Attempt
	err := s.run(ctx, "CreateAttempt", a.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		var status, desired string
		err := tx.QueryRow(ctx, "SELECT status FROM tasks WHERE task_id = $1 FOR UPDATE", a.TaskID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", a.TaskID)
		}
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR SHARE", a.TaskID).Scan(&desired); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, a.TaskID); err != nil {
			return err
		}
		existing, err := selectAttempt(ctx, tx, a.AttemptID, true)
		switch {
		case err == nil:
			if existing.TaskID != a.TaskID || existing.AttemptNo != a.AttemptNo || existing.EnvID != a.EnvID {
				return conflictf("attempt %s 已存在且内容不同", a.AttemptID)
			}
			out = existing
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		var taken string
		err = tx.QueryRow(ctx, "SELECT attempt_id FROM attempts WHERE task_id = $1 AND attempt_no = $2", a.TaskID, a.AttemptNo).Scan(&taken)
		if err == nil {
			return conflictf("任务 %s 的第 %d 次 attempt 已是 %s", a.TaskID, a.AttemptNo, taken)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var envTaken bool // 否则 INSERT 的 23505 会被当作可重试错误一直重试到期限
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM environments WHERE env_id = $1)", a.EnvID).Scan(&envTaken); err != nil {
			return err
		}
		if envTaken {
			return conflictf("环境 %s 已存在", a.EnvID)
		}
		if err := admitAttempt(ctx, tx, a.TaskID, status, desired); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ($1, $2, $3, $4, 'starting')",
			a.AttemptID, a.TaskID, a.AttemptNo, a.EnvID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempt_access (attempt_id, task_id, state) VALUES ($1, $2, 'active')", a.AttemptID, a.TaskID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO environments (env_id, kind, attempt_id, status) VALUES ($1, 'task', $2, 'creating')",
			a.EnvID, a.AttemptID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks SET current_attempt_id = $2, attempts_total = attempts_total + 1, status = 'running',
			row_version = row_version + 1 WHERE task_id = $1 AND status = 'queued'`, a.TaskID, a.AttemptID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 { // 持有任务行锁时不会发生；保留 CAS 作为最后一道检查
			return rejectf(persistence.CodeNotRunnable, "任务 %s 已不在 queued", a.TaskID)
		}
		payload, _ := json.Marshal(map[string]any{"attempt_no": a.AttemptNo, "env_id": a.EnvID})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: a.TaskID, key: "attempt_created:" + a.AttemptID,
			attemptID: a.AttemptID, typ: "attempt_created", payload: payload}); err != nil {
			return err
		}
		out = task.Attempt{AttemptID: a.AttemptID, TaskID: a.TaskID, AttemptNo: a.AttemptNo, EnvID: a.EnvID, Status: "starting"}
		return nil
	})
	return out, err
}

// admitAttempt 检查创建 attempt 的准入前置条件（规格 §8.1）。旧环境的 stopped_at 只会从空变为
// 非空，因此不加锁读取至多得到偏保守的"未停止"。
func admitAttempt(ctx context.Context, tx pgx.Tx, taskID, status, desired string) error {
	if status != "queued" || desired != "run" {
		return rejectf(persistence.CodeNotRunnable, "任务 %s 处于 %s、desired = %s", taskID, status, desired)
	}
	var running int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN environments e ON e.attempt_id = a.attempt_id
		WHERE a.task_id = $1 AND e.stopped_at IS NULL`, taskID).Scan(&running); err != nil {
		return err
	}
	if running > 0 {
		return rejectf(persistence.CodePreviousNotStopped, "任务 %s 有 %d 个旧环境尚未确认停止", taskID, running)
	}
	return nil
}

// GetAttempt 读取 attempt（实现 task.Store）。
func (s *Store) GetAttempt(ctx context.Context, attemptID string) (task.Attempt, error) {
	var out task.Attempt
	err := s.read(ctx, "GetAttempt", func(ctx context.Context, q queryer) error {
		a, err := selectAttempt(ctx, q, attemptID, false)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		out = a
		return err
	})
	return out, err
}

func selectAttempt(ctx context.Context, q queryer, attemptID string, forUpdate bool) (task.Attempt, error) {
	sql := "SELECT attempt_id, task_id, attempt_no, env_id, status, outcome_class, verdict_hash FROM attempts WHERE attempt_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var a task.Attempt
	err := q.QueryRow(ctx, sql, attemptID).Scan(&a.AttemptID, &a.TaskID, &a.AttemptNo, &a.EnvID, &a.Status, &a.OutcomeClass, &a.VerdictHash)
	return a, err
}

// controlTransition 是规格 §8.1 中控制意图引起的任务状态转换：cancel 使 queued、paused 直接
// cancelled，使执行中的任务进入 cancelling；pause 使 queued 进入 paused，使执行中的任务进入
// pausing；run（resume）使 paused 回到 queued。其余组合不是合法转换。
func controlTransition(status, desired string) (next string, ok bool) {
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

// ApplyControl 以 applied_control_version 的 CAS 应用控制意图（实现 task.Store）。持有任务锁后
// 依次核对：已应用到不低于目标的版本时返回当前状态（重放）；目标版本尚未被接受为冲突；目标版本
// 已被更新的控制取代为 control_changed（调用方应用最新版本）；终态任务为 task_ended；调用方给出的
// 状态必须是 controlTransition 对当前状态与 desired 的结果。写入以来源状态 CAS（规格 §8.1）。
// 锁顺序：tasks → task_control → task_event_seq。
func (s *Store) ApplyControl(ctx context.Context, c task.ApplyControl) (task.ControlState, error) {
	if c.TaskID == "" || c.ControlVersion < 1 || c.Status == "" {
		return task.ControlState{}, invalidf("ApplyControl 缺少 task_id、control_version 或 status")
	}
	var out task.ControlState
	err := s.run(ctx, "ApplyControl", fmt.Sprintf("%s@%d", c.TaskID, c.ControlVersion), func(ctx context.Context, tx pgx.Tx) error {
		st, err := selectControlState(ctx, tx, c.TaskID, true)
		if err != nil {
			return err
		}
		switch {
		case st.AppliedControlVersion >= c.ControlVersion:
			out = st
			return nil
		case c.ControlVersion > st.ControlVersion:
			return conflictf("任务 %s 的控制版本 %d 尚未被接受（当前 %d）", c.TaskID, c.ControlVersion, st.ControlVersion)
		case c.ControlVersion < st.ControlVersion:
			return rejectf(persistence.CodeControlChanged, "任务 %s 的控制版本 %d 已被 %d 取代", c.TaskID, c.ControlVersion, st.ControlVersion)
		case st.Status == "succeeded" || st.Status == "failed" || st.Status == "cancelled":
			return rejectf(persistence.CodeTaskEnded, "任务 %s 已是 %s", c.TaskID, st.Status)
		}
		if next, ok := controlTransition(st.Status, st.Desired); !ok || c.Status != next {
			return invalidf("任务 %s 处于 %s、desired = %s 时不能转换为 %s", c.TaskID, st.Status, st.Desired, c.Status)
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks SET applied_control_version = $2, status = $3, status_reason = $4,
			row_version = row_version + 1 WHERE task_id = $1 AND status = $5`, c.TaskID, c.ControlVersion, c.Status, c.StatusReason, st.Status)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 { // 持有任务行锁时不会发生；保留 CAS 作为最后一道检查
			return conflictf("任务 %s 已不在 %s", c.TaskID, st.Status)
		}
		if err := lockEventSeq(ctx, tx, c.TaskID); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"control_version": c.ControlVersion, "status": c.Status})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: c.TaskID, key: fmt.Sprintf("control_applied:%d", c.ControlVersion),
			typ: "control_applied", payload: payload}); err != nil {
			return err
		}
		st.AppliedControlVersion, st.Status = c.ControlVersion, c.Status
		out = st
		return nil
	})
	return out, err
}

// GetControlState 读取任务的控制状态（实现 task.Store）。
func (s *Store) GetControlState(ctx context.Context, taskID string) (task.ControlState, error) {
	var out task.ControlState
	err := s.read(ctx, "GetControlState", func(ctx context.Context, q queryer) error {
		st, err := selectControlState(ctx, q, taskID, false)
		out = st
		return err
	})
	return out, err
}

// selectControlState 读取任务的控制状态；forUpdate 时先锁 tasks 再锁 task_control（锁顺序）。
func selectControlState(ctx context.Context, q queryer, taskID string, forUpdate bool) (task.ControlState, error) {
	st := task.ControlState{TaskID: taskID}
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	err := q.QueryRow(ctx, "SELECT status, applied_control_version FROM tasks WHERE task_id = $1"+lock, taskID).
		Scan(&st.Status, &st.AppliedControlVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return st, err
	}
	err = q.QueryRow(ctx, "SELECT desired, control_version FROM task_control WHERE task_id = $1"+lock, taskID).
		Scan(&st.Desired, &st.ControlVersion)
	return st, err
}

// verdictAllowed 是规格 §8.1 最终裁决对 desired 的约束：cancel → cancelled；pause → paused；
// run → 按 §5.8 与 §14.3 裁决为 succeeded、failed，或故障重试回到 queued。
func verdictAllowed(desired, taskStatus string) bool {
	switch desired {
	case "cancel":
		return taskStatus == "cancelled"
	case "pause":
		return taskStatus == "paused"
	default:
		return taskStatus == "succeeded" || taskStatus == "failed" || taskStatus == "queued"
	}
}

// verdictHash 是完整判决内容的哈希；FromStatus 是 CAS 来源，不属于判决内容（设计 §2.4）。
func verdictHash(v task.Verdict) ([]byte, error) {
	content := v
	content.FromStatus = ""
	b, err := json.Marshal(content) // 字段顺序固定；RawMessage 被压缩为规范形式
	if err != nil {
		return nil, invalidf("判决无法编码: %v", err)
	}
	return contentHash(b), nil
}

// FinalizeAttempt 提交判决、任务状态与终态 host 事件（实现 task.Store）。完整判决内容一致
// 才返回原结果；已有不同判决为冲突。新判决在持有任务锁后核对最新事实（规格 §8.1）：
// 提交者必须仍是当前 attempt，且任务仍处于 running、pausing 或 cancelling（stale_attempt）；
// 判决依据的控制版本必须是最新（control_changed，调用方按最新控制重算）；任务状态必须与 desired
// 相符；attempt 必须处于 FromStatus。判决同时把 applied_control_version 推进到该控制版本。
// 锁顺序：tasks → task_control → task_event_seq → attempts。
func (s *Store) FinalizeAttempt(ctx context.Context, v task.Verdict) (task.Attempt, error) {
	if v.AttemptID == "" || v.TaskID == "" || v.ControlVersion < 1 || v.FromStatus == "" || v.AttemptStatus == "" ||
		v.TaskStatus == "" || v.EventType == "" {
		return task.Attempt{}, invalidf("FinalizeAttempt 缺少必填字段")
	}
	hash, err := verdictHash(v)
	if err != nil {
		return task.Attempt{}, err
	}
	var out task.Attempt
	err = s.run(ctx, "FinalizeAttempt", v.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		var current *string
		var status string
		err := tx.QueryRow(ctx, "SELECT current_attempt_id, status FROM tasks WHERE task_id = $1 FOR UPDATE", v.TaskID).Scan(&current, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", v.TaskID)
		}
		if err != nil {
			return err
		}
		var desired string
		var controlVersion int64
		if err := tx.QueryRow(ctx, "SELECT desired, control_version FROM task_control WHERE task_id = $1 FOR SHARE",
			v.TaskID).Scan(&desired, &controlVersion); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, v.TaskID); err != nil {
			return err
		}
		a, err := selectAttempt(ctx, tx, v.AttemptID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", v.AttemptID)
		}
		if err != nil {
			return err
		}
		switch {
		case a.TaskID != v.TaskID:
			return conflictf("attempt %s 不属于任务 %s", v.AttemptID, v.TaskID)
		case a.VerdictHash != nil && bytes.Equal(a.VerdictHash, hash):
			out = a
			return nil
		case a.VerdictHash != nil:
			return conflictf("attempt %s 已有不同的判决", v.AttemptID)
		case current == nil || *current != v.AttemptID:
			return rejectf(persistence.CodeStaleAttempt, "attempt %s 不是任务 %s 的当前 attempt", v.AttemptID, v.TaskID)
		case status != "running" && status != "pausing" && status != "cancelling":
			return rejectf(persistence.CodeStaleAttempt, "任务 %s 处于 %s，不接受判决", v.TaskID, status)
		case controlVersion != v.ControlVersion:
			return rejectf(persistence.CodeControlChanged, "判决依据控制版本 %d，当前为 %d（desired = %s）",
				v.ControlVersion, controlVersion, desired)
		case !verdictAllowed(desired, v.TaskStatus):
			return invalidf("desired = %s 时任务不能裁决为 %s", desired, v.TaskStatus)
		case a.Status != v.FromStatus:
			return conflictf("attempt %s 处于 %s，不是 %s", v.AttemptID, a.Status, v.FromStatus)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET status = $2, outcome_class = $3, exit_code = $4, exit_signal = $5,
			oom_kill_delta = $6, platform_killed = $7, verdict_hash = $8 WHERE attempt_id = $1`,
			v.AttemptID, v.AttemptStatus, v.OutcomeClass, v.ExitCode, v.ExitSignal, v.OOMKillDelta, v.PlatformKilled, hash); err != nil {
			return err
		}
		// 判决已按 controlVersion 裁决，一并推进 applied_control_version，actor 不再对已裁决的任务应用控制。
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, status_reason = $3, result_json = $4,
			applied_control_version = GREATEST(applied_control_version, $5), row_version = row_version + 1
			WHERE task_id = $1`, v.TaskID, v.TaskStatus, v.TaskStatusReason, nullJSON(v.Result), controlVersion); err != nil {
			return err
		}
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: v.TaskID, key: "attempt_finalized:" + v.AttemptID,
			attemptID: v.AttemptID, typ: v.EventType, payload: []byte(v.EventPayload)}); err != nil {
			return err
		}
		a.Status, a.OutcomeClass, a.VerdictHash = v.AttemptStatus, v.OutcomeClass, hash
		out = a
		return nil
	})
	return out, err
}
```

- [ ] **Step 4：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/persistence; go vet ./internal/persistence/... && CI=true go test -count=1 ./internal/persistence/postgres/ && go test -count=1 ./internal/archtest/'
```

Expected: 两个包 `ok`。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/persistence/postgres/events.go internal/persistence/postgres/api.go internal/persistence/postgres/task.go internal/persistence/postgres/postgres_test.go && git commit -m "feat(persistence): 事件追加与 api、task 事务用例（E11a）" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 6：runner 用例

**风险：高（事件内容校验、checkpoint 提交）——双评审。**

**Files:**
- Create: `internal/persistence/postgres/runner.go`
- Modify: `internal/persistence/postgres/postgres_test.go`（替换 import 块，末尾追加第三节）

**Interfaces:**
- Consumes：Task 4、5 的包内辅助（`run`、`read`、`lockTask`、`lockTaskShared`、`lockEventSeq`、`nextEventSeq`、`appendHostEvent`、`nullJSON`、`contentHash`）；Task 1 的 `runner.Store`。
- Produces：`*Store` 实现 `runner.Store`（`AppendWorkerEvents`、`WorkerEventWatermark`、`CommitCheckpoint`、`QueryCheckpoint`、`RegisterArtifact`、`GetArtifact`、`RecordTerminalProposal`、`GetTerminalProposal`）；包内 `fenceAttempt`、`authorizeRefs`。

- [ ] **Step 1：写失败的测试**

第三节覆盖：四个用例的"提交后回复丢失"、同身份不同内容的冲突（checkpoint、终态提议、同 sha256 不同大小）、Worker 事件批次（整批重放、部分重叠、同序号不同内容、缺口、非连续）；以及 checkpoint 的 fencing 与引用授权——旧 attempt、其他任务的 blob、不存在的 blob、其他任务的 scope 均被拒绝且指针与事件不变，旧 attempt 也不能登记产物，当前 attempt 引用本任务已授权的 blob 正常提交。

把 `internal/persistence/postgres/postgres_test.go` 的 import 块**替换**为：

```go
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)
```

然后在 `internal/persistence/postgres/postgres_test.go` **末尾追加**：

```go
// ---- runner 用例：未知提交、同身份不同内容、Worker 事件批次 ----

func TestCommitLostResolvesRunnerUseCases(t *testing.T) {
	ctx := context.Background()
	checkCommitLost(t, []useCase{
		{"CommitCheckpoint", withFixture, func(s *Store) (any, error) {
			return s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1",
				AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{"n":1}`)})
		}, "SELECT count(*) FROM checkpoints"},
		{"RegisterArtifact", withFixture, func(s *Store) (any, error) {
			return s.RegisterArtifact(ctx, runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report",
				SHA256: strings.Repeat("a", 64), Size: 9, MediaType: "text/markdown", Visibility: "output"})
		}, "SELECT count(*) FROM artifacts"},
		{"AppendWorkerEvents", withFixture, func(s *Store) (any, error) {
			return s.AppendWorkerEvents(ctx, "att-t1", []runner.WorkerEvent{{Seq: 1, Type: "ready", Payload: json.RawMessage(`{}`)}})
		}, "SELECT count(*) FROM events WHERE source = 'worker'"},
		{"RecordTerminalProposal", withFixture, func(s *Store) (any, error) {
			return s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:9"})
		}, "SELECT count(*) FROM attempts WHERE terminal_proposal = 'result'"},
	})
}

func TestRunnerConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: "cp1", AttemptID: "att-t1", StepID: "s1", State: json.RawMessage(`{"n":1}`)}
	if _, err := s.CommitCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "error", Ref: "seq:3"}); err != nil {
		t.Fatal(err)
	}
	art := runner.Artifact{TaskID: "t1", AttemptID: "att-t1", ArtifactID: "report", SHA256: strings.Repeat("b", 64), Size: 9,
		MediaType: "text/markdown", Visibility: "output"}
	if _, err := s.RegisterArtifact(ctx, art); err != nil {
		t.Fatal(err)
	}
	cp2 := cp
	cp2.State = json.RawMessage(`{"n":2}`)
	art2 := art
	art2.ArtifactID, art2.Size = "other", 10
	expectConflicts(t, map[string]func() error{
		"CommitCheckpoint 同 ID 不同 state": func() error { _, err := s.CommitCheckpoint(ctx, cp2); return err },
		"RecordTerminalProposal 不同内容": func() error {
			_, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: "att-t1", Kind: "result", Ref: "seq:3"})
			return err
		},
		"RegisterArtifact 同 sha256 不同大小": func() error { _, err := s.RegisterArtifact(ctx, art2); return err },
	})
}

// TestWorkerEventBatches：重放、部分重叠、同序号不同内容、缺口与非连续批次。
func TestWorkerEventBatches(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	ev := func(seq int64, body string) runner.WorkerEvent {
		return runner.WorkerEvent{Seq: seq, Type: "progress", Payload: json.RawMessage(`{"m":"` + body + `"}`)}
	}
	batch := func(from, to int64, body string) []runner.WorkerEvent {
		var out []runner.WorkerEvent
		for i := from; i <= to; i++ {
			out = append(out, ev(i, fmt.Sprintf("%s%d", body, i)))
		}
		return out
	}
	steps := []struct {
		name  string
		batch []runner.WorkerEvent
		want  int64
		err   error
	}{
		{"首批 1–3", batch(1, 3, "x"), 3, nil},
		{"整批重放", batch(1, 3, "x"), 3, nil},
		{"部分重叠 2–5", batch(2, 5, "x"), 5, nil},
		{"同序号不同内容", batch(5, 6, "y"), 0, persistence.ErrConflict},
		{"缺口 8–9", batch(8, 9, "x"), 0, persistence.ErrConflict},
		{"非连续批次", []runner.WorkerEvent{ev(6, "a"), ev(8, "b")}, 0, errInvalid},
	}
	for _, st := range steps {
		w, err := s.AppendWorkerEvents(ctx, "att-t1", st.batch)
		if st.err != nil {
			if !errors.Is(err, st.err) {
				t.Fatalf("%s：应为 %v，得到 %v", st.name, st.err, err)
			}
			continue
		}
		if err != nil || w.WorkerSeq != st.want {
			t.Fatalf("%s：得到 (%d, %v)，期望水位 %d", st.name, w.WorkerSeq, err, st.want)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM events WHERE source = 'worker'"); n != 5 {
		t.Fatalf("应只有 5 条 Worker 事件，得到 %d", n)
	}
	if w, err := s.WorkerEventWatermark(ctx, "att-t1"); err != nil || w.WorkerSeq != 5 {
		t.Fatalf("水位：%+v %v", w, err)
	}
	if n, m := count(t, s, "SELECT count(*) FROM events WHERE task_id = 't1'"),
		count(t, s, "SELECT max(task_seq)::int FROM events WHERE task_id = 't1'"); n != m {
		t.Fatalf("task_seq 应从 1 起连续无空洞：%d 条，最大 %d", n, m)
	}
}

// TestCheckpointFencingAndRefs 覆盖规格 §5.5 第 2、3 条：新 checkpoint 的 fencing 与引用授权在提交事务内
// 检查，被拒绝时指针与事件都不改变；旧 attempt 也不能登记产物。
func TestCheckpointFencingAndRefs(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	own, foreign, missing := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	register := func(taskID, attemptID, sha string) error {
		_, err := s.RegisterArtifact(ctx, runner.Artifact{TaskID: taskID, AttemptID: attemptID, ArtifactID: "state-" + sha[:1],
			SHA256: sha, Size: 3, MediaType: "application/json", Visibility: "internal"})
		return err
	}
	if err := register("t2", "att-t2", foreign); err != nil {
		t.Fatal(err)
	}
	retryWithNewAttempt(t, s, "t1")
	if err := register("t1", "att2-t1", own); err != nil {
		t.Fatal(err)
	}
	cp := func(id, attemptID, stateRef string, refs ...string) runner.Checkpoint {
		return runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, CheckpointID: id, AttemptID: attemptID, StepID: "s1",
			StateRef: stateRef, Refs: refs}
	}
	before := snapshot(t, s, "t1")
	for name, c := range map[string]struct {
		cp   runner.Checkpoint
		code string
	}{
		"旧 attempt":   {cp("cp-old", "att-t1", own), persistence.CodeStaleAttempt},
		"其他任务的 blob":  {cp("cp-foreign", "att2-t1", own, foreign), persistence.CodeRefNotAuthorized},
		"不存在的 blob":   {cp("cp-missing", "att2-t1", missing), persistence.CodeRefNotAuthorized},
		"其他任务的 scope": {runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t2"}, CheckpointID: "cp-x", AttemptID: "att2-t1", StepID: "s1", StateRef: foreign}, persistence.CodeStaleAttempt},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.CommitCheckpoint(ctx, c.cp)
			expectRejected(t, err, c.code)
		})
	}
	expectRejected(t, register("t1", "att-t1", strings.Repeat("d", 64)), persistence.CodeStaleAttempt)
	if after := snapshot(t, s, "t1"); after != before {
		t.Fatalf("被拒绝的写入不应改变指针、事件或产物：%s → %s", before, after)
	}
	got, err := s.CommitCheckpoint(ctx, cp("cp-ok", "att2-t1", own, own))
	if err != nil || got.CommitSeq != 1 {
		t.Fatalf("当前 attempt 引用本任务已授权的 blob 应提交：%+v %v", got, err)
	}
}
```

- [ ] **Step 2：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/persistence/postgres/'
```

Expected: 编译失败（`s.CommitCheckpoint undefined` 等）。

- [ ] **Step 3：实现**

`internal/persistence/postgres/runner.go`：

```go
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
)

var _ runner.Store = (*Store)(nil)

// AppendWorkerEvents 原子追加一批 Worker 事件（实现 runner.Store；设计 §3.1）。
func (s *Store) AppendWorkerEvents(ctx context.Context, attemptID string, events []runner.WorkerEvent) (runner.Watermark, error) {
	if err := validateWorkerBatch(events); err != nil {
		return runner.Watermark{}, err
	}
	out := runner.Watermark{AttemptID: attemptID}
	identity := fmt.Sprintf("%s:%d-%d", attemptID, events[0].Seq, events[len(events)-1].Seq)
	err := s.run(ctx, "AppendWorkerEvents", identity, func(ctx context.Context, tx pgx.Tx) error {
		var taskID string
		err := tx.QueryRow(ctx, "SELECT task_id FROM attempts WHERE attempt_id = $1", attemptID).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		if err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, taskID); err != nil {
			return err
		}
		out.WorkerSeq, err = appendWorkerBatch(ctx, tx, taskID, attemptID, events)
		return err
	})
	return out, err
}

// WorkerEventWatermark 返回已提交的最大 worker_seq（实现 runner.Store）。
func (s *Store) WorkerEventWatermark(ctx context.Context, attemptID string) (runner.Watermark, error) {
	out := runner.Watermark{AttemptID: attemptID}
	err := s.read(ctx, "WorkerEventWatermark", func(ctx context.Context, q queryer) error {
		return q.QueryRow(ctx, "SELECT COALESCE(max(worker_seq), 0) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL",
			attemptID).Scan(&out.WorkerSeq)
	})
	return out, err
}

func checkpointHash(c runner.Checkpoint) []byte {
	return contentHash([]byte(c.AttemptID), []byte(c.StepID), c.State, []byte(c.StateRef), []byte(strings.Join(c.Refs, "\n")))
}

// CommitCheckpoint 以 (scope, checkpoint_id) 为身份提交 checkpoint（实现 runner.Store）。
// 已存在的 ID 只比较内容并返回原结果（规格 §5.5 第 3 条）；新 checkpoint 在同一事务内检查
// fencing（提交者是当前 attempt 且访问有效）与引用授权（refs、state_ref 已保存并授权到当前 scope），
// 不通过时指针与事件都不改变。commit_seq 来自 task_progress 的行锁（规格 §7.2）。
// 锁顺序：tasks → task_progress → task_event_seq → attempt_access。
func (s *Store) CommitCheckpoint(ctx context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	if c.Scope.Kind != "task" || c.Scope.ID == "" {
		return runner.CommittedCheckpoint{}, invalidf("M1 只支持 task 范围的 checkpoint，得到 %+v", c.Scope)
	}
	if c.CheckpointID == "" || c.AttemptID == "" || c.StepID == "" || (len(c.State) == 0) == (c.StateRef == "") {
		return runner.CommittedCheckpoint{}, invalidf("checkpoint 缺少字段，或 state 与 state_ref 未恰好提供一个")
	}
	hash := checkpointHash(c)
	out := runner.CommittedCheckpoint{Scope: c.Scope, CheckpointID: c.CheckpointID, ContentHash: hash}
	err := s.run(ctx, "CommitCheckpoint", c.CheckpointID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockTask(ctx, tx, c.Scope.ID); err != nil {
			return err
		}
		var latest int64
		if err := tx.QueryRow(ctx, "SELECT latest_commit_seq FROM task_progress WHERE task_id = $1 FOR UPDATE", c.Scope.ID).Scan(&latest); err != nil {
			return err
		}
		existing, err := selectCheckpoint(ctx, tx, c.Scope, c.CheckpointID)
		switch {
		case err == nil && bytes.Equal(existing.ContentHash, hash):
			out = existing
			return nil
		case err == nil:
			return conflictf("checkpoint %s 已存在且内容不同", c.CheckpointID)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		if err := fenceAttempt(ctx, tx, c.Scope.ID, c.AttemptID); err != nil {
			return err
		}
		if err := authorizeRefs(ctx, tx, c); err != nil {
			return err
		}
		refs, _ := json.Marshal(nonNil(c.Refs))
		if _, err := tx.Exec(ctx, `INSERT INTO checkpoints (scope_kind, scope_id, checkpoint_id, commit_seq, attempt_id, step_id,
				content_hash, state_inline, state_ref, refs_json)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10)`,
			c.Scope.Kind, c.Scope.ID, c.CheckpointID, latest+1, c.AttemptID, c.StepID, hash, nullJSON(c.State), c.StateRef, refs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE task_progress SET latest_checkpoint_id = $2, latest_commit_seq = $3 WHERE task_id = $1",
			c.Scope.ID, c.CheckpointID, latest+1); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, c.Scope.ID); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"checkpoint_id": c.CheckpointID, "commit_seq": latest + 1, "step_id": c.StepID})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: c.Scope.ID, key: "checkpoint_committed:" + c.CheckpointID,
			attemptID: c.AttemptID, typ: "checkpoint_committed", payload: payload}); err != nil {
			return err
		}
		out.CommitSeq = latest + 1
		return nil
	})
	return out, err
}

// fenceAttempt 要求 attemptID 是任务的当前 attempt 且其访问仍为 active；调用方已持有任务行锁，
// 因此检查到提交之间 current_attempt_id 不会改变。
func fenceAttempt(ctx context.Context, tx pgx.Tx, taskID, attemptID string) error {
	var current *string
	var access string
	err := tx.QueryRow(ctx, `SELECT t.current_attempt_id, COALESCE(aa.state, '') FROM tasks t
		LEFT JOIN attempt_access aa ON aa.task_id = t.task_id AND aa.attempt_id = $2 WHERE t.task_id = $1`,
		taskID, attemptID).Scan(&current, &access)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return err
	}
	if current == nil || *current != attemptID || access != "active" {
		return rejectf(persistence.CodeStaleAttempt, "attempt %s 不是任务 %s 的当前 attempt，或访问已撤销", attemptID, taskID)
	}
	return nil
}

// authorizeRefs 要求 checkpoint 引用的每个 sha256 都已保存并授权到当前 scope（规格 §5.5 第 2 条）：
// scope 由宿主按 attempt → task 推导（session 在 M4 加入），不接受其他任务的 blob。
func authorizeRefs(ctx context.Context, tx pgx.Tx, c runner.Checkpoint) error {
	refs := append([]string(nil), c.Refs...)
	if c.StateRef != "" {
		refs = append(refs, c.StateRef)
	}
	if len(refs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT r FROM unnest($1::text[]) AS r WHERE NOT EXISTS (
		SELECT 1 FROM scope_blobs sb WHERE sb.sha256 = r AND
			((sb.scope_kind = 'attempt' AND sb.scope_id = $2) OR (sb.scope_kind = 'task' AND sb.scope_id = $3)))`,
		refs, c.AttemptID, c.Scope.ID)
	if err != nil {
		return err
	}
	missing, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return rejectf(persistence.CodeRefNotAuthorized, "引用 %v 不存在或未授权到任务 %s", missing, c.Scope.ID)
	}
	return nil
}

// QueryCheckpoint 读取已提交的 checkpoint（实现 runner.Store）。
func (s *Store) QueryCheckpoint(ctx context.Context, scope runner.Scope, checkpointID string) (runner.CommittedCheckpoint, error) {
	var out runner.CommittedCheckpoint
	err := s.read(ctx, "QueryCheckpoint", func(ctx context.Context, q queryer) error {
		c, err := selectCheckpoint(ctx, q, scope, checkpointID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("checkpoint %s", checkpointID)
		}
		out = c
		return err
	})
	return out, err
}

func selectCheckpoint(ctx context.Context, q queryer, scope runner.Scope, checkpointID string) (runner.CommittedCheckpoint, error) {
	c := runner.CommittedCheckpoint{Scope: scope, CheckpointID: checkpointID}
	err := q.QueryRow(ctx, "SELECT commit_seq, content_hash FROM checkpoints WHERE scope_kind = $1 AND scope_id = $2 AND checkpoint_id = $3",
		scope.Kind, scope.ID, checkpointID).Scan(&c.CommitSeq, &c.ContentHash)
	return c, err
}

// RegisterArtifact 登记已写入 BlobStore 的产物（实现 runner.Store）。以 (task, artifact, sha256)
// 为身份；新登记要求提交者是当前 attempt 且访问有效（与 checkpoint 相同的 fencing）。版本来自
// artifact_heads 的行锁（规格 §7.2）。锁顺序：tasks → task_event_seq → attempt_access → artifact_heads。
func (s *Store) RegisterArtifact(ctx context.Context, a runner.Artifact) (runner.ArtifactVersion, error) {
	if a.TaskID == "" || a.ArtifactID == "" || a.AttemptID == "" || len(a.SHA256) != 64 || a.Size < 0 || a.MediaType == "" {
		return runner.ArtifactVersion{}, invalidf("RegisterArtifact 缺少字段或 sha256 不合法")
	}
	if a.Visibility != "output" && a.Visibility != "internal" {
		return runner.ArtifactVersion{}, invalidf("visibility 必须是 output 或 internal")
	}
	out := runner.ArtifactVersion{TaskID: a.TaskID, ArtifactID: a.ArtifactID, SHA256: a.SHA256}
	err := s.run(ctx, "RegisterArtifact", a.TaskID+"/"+a.ArtifactID+"@"+a.SHA256, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockTaskShared(ctx, tx, a.TaskID); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, a.TaskID); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, "SELECT version FROM artifacts WHERE task_id = $1 AND artifact_id = $2 AND sha256 = $3",
			a.TaskID, a.ArtifactID, a.SHA256).Scan(&out.Version)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := fenceAttempt(ctx, tx, a.TaskID, a.AttemptID); err != nil {
			return err
		}
		if err := recordBlob(ctx, tx, a); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO artifact_heads (task_id, artifact_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			a.TaskID, a.ArtifactID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE artifact_heads SET next_version = next_version + 1
			WHERE task_id = $1 AND artifact_id = $2 RETURNING next_version - 1`, a.TaskID, a.ArtifactID).Scan(&out.Version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO artifacts (task_id, artifact_id, version, sha256, size, media_type, visibility, attempt_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			a.TaskID, a.ArtifactID, out.Version, a.SHA256, a.Size, a.MediaType, a.Visibility, a.AttemptID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref) VALUES ('task', $1, $2, 'artifact', $3)",
			a.TaskID, a.SHA256, fmt.Sprintf("%s@%d", a.ArtifactID, out.Version)); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"artifact_id": a.ArtifactID, "version": out.Version, "sha256": a.SHA256})
		_, err = appendHostEvent(ctx, tx, hostEvent{taskID: a.TaskID, key: fmt.Sprintf("artifact_saved:%s:%d", a.ArtifactID, out.Version),
			attemptID: a.AttemptID, typ: "artifact_saved", payload: payload})
		return err
	})
	return out, err
}

// recordBlob 登记 blob 与其授权关联；同一 sha256 的大小不同为冲突。
func recordBlob(ctx context.Context, tx pgx.Tx, a runner.Artifact) error {
	if _, err := tx.Exec(ctx, "INSERT INTO blobs (sha256, size) VALUES ($1, $2) ON CONFLICT DO NOTHING", a.SHA256, a.Size); err != nil {
		return err
	}
	var size int64
	if err := tx.QueryRow(ctx, "SELECT size FROM blobs WHERE sha256 = $1", a.SHA256).Scan(&size); err != nil {
		return err
	}
	if size != a.Size {
		return conflictf("blob %s 已登记为 %d 字节，不是 %d", a.SHA256, size, a.Size)
	}
	_, err := tx.Exec(ctx, "INSERT INTO scope_blobs (scope_kind, scope_id, sha256) VALUES ('task', $1, $2) ON CONFLICT DO NOTHING",
		a.TaskID, a.SHA256)
	return err
}

// GetArtifact 读取已登记的产物版本（实现 runner.Store）。
func (s *Store) GetArtifact(ctx context.Context, taskID, artifactID, sha256 string) (runner.ArtifactVersion, error) {
	out := runner.ArtifactVersion{TaskID: taskID, ArtifactID: artifactID, SHA256: sha256}
	err := s.read(ctx, "GetArtifact", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, "SELECT version FROM artifacts WHERE task_id = $1 AND artifact_id = $2 AND sha256 = $3",
			taskID, artifactID, sha256).Scan(&out.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("产物 %s/%s@%s", taskID, artifactID, sha256)
		}
		return err
	})
	return out, err
}

func proposalHash(p runner.TerminalProposal) []byte {
	return contentHash([]byte(p.Kind), []byte(p.Ref))
}

// RecordTerminalProposal 记录终态提议（实现 runner.Store）：相同内容返回原结果，不同内容为冲突，
// 不以 WHERE … IS NULL 静默忽略（设计 §2.4）。只写 attempts 的 terminal_proposal* 列。
func (s *Store) RecordTerminalProposal(ctx context.Context, p runner.TerminalProposal) (runner.TerminalProposal, error) {
	switch p.Kind {
	case "result", "error", "paused":
	default:
		return runner.TerminalProposal{}, invalidf("终态提议必须是 result、error 或 paused，得到 %q", p.Kind)
	}
	hash := proposalHash(p)
	var out runner.TerminalProposal
	err := s.run(ctx, "RecordTerminalProposal", p.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		existing, existingHash, err := selectProposal(ctx, tx, p.AttemptID, true)
		if err != nil {
			return err
		}
		switch {
		case existingHash != nil && bytes.Equal(existingHash, hash):
			out = existing
			return nil
		case existingHash != nil:
			return conflictf("attempt %s 已有不同的终态提议 %s", p.AttemptID, existing.Kind)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET terminal_proposal = $2, terminal_proposal_ref = $3, terminal_proposal_hash = $4
			WHERE attempt_id = $1`, p.AttemptID, p.Kind, p.Ref, hash); err != nil {
			return err
		}
		out = p
		return nil
	})
	return out, err
}

// GetTerminalProposal 读取终态提议（实现 runner.Store）。
func (s *Store) GetTerminalProposal(ctx context.Context, attemptID string) (runner.TerminalProposal, error) {
	var out runner.TerminalProposal
	err := s.read(ctx, "GetTerminalProposal", func(ctx context.Context, q queryer) error {
		p, hash, err := selectProposal(ctx, q, attemptID, false)
		if err == nil && hash == nil {
			return notFoundf("attempt %s 尚无终态提议", attemptID)
		}
		out = p
		return err
	})
	return out, err
}

func selectProposal(ctx context.Context, q queryer, attemptID string, forUpdate bool) (runner.TerminalProposal, []byte, error) {
	sql := "SELECT COALESCE(terminal_proposal, ''), COALESCE(terminal_proposal_ref, ''), terminal_proposal_hash FROM attempts WHERE attempt_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	p := runner.TerminalProposal{AttemptID: attemptID}
	var hash []byte
	err := q.QueryRow(ctx, sql, attemptID).Scan(&p.Kind, &p.Ref, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil, notFoundf("attempt %s", attemptID)
	}
	return p, hash, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func workerEventHash(e runner.WorkerEvent) []byte {
	return contentHash([]byte(e.Type), e.Payload)
}

// validateWorkerBatch 检查批次非空、序号从 1 起且连续、类型与载荷齐全。
func validateWorkerBatch(events []runner.WorkerEvent) error {
	if len(events) == 0 {
		return invalidf("Worker 事件批次为空")
	}
	for i, e := range events {
		if e.Seq < 1 || (i > 0 && e.Seq != events[i-1].Seq+1) {
			return invalidf("Worker 事件序号必须从 1 起且连续，第 %d 条为 %d", i, e.Seq)
		}
		if e.Type == "" || len(e.Payload) == 0 {
			return invalidf("Worker 事件 %d 缺少类型或载荷", e.Seq)
		}
	}
	return nil
}

// appendWorkerBatch 原子追加一批 Worker 事件（设计 §3.1）：重叠部分逐条校验内容，
// 只追加连续的新后缀，缺口拒绝。调用方必须已持有 lockEventSeq。返回新的水位。
func appendWorkerBatch(ctx context.Context, tx pgx.Tx, taskID, attemptID string, events []runner.WorkerEvent) (int64, error) {
	first, last := events[0].Seq, events[len(events)-1].Seq
	var watermark int64
	if err := tx.QueryRow(ctx, "SELECT COALESCE(max(worker_seq), 0) FROM events WHERE attempt_id = $1 AND worker_seq IS NOT NULL",
		attemptID).Scan(&watermark); err != nil {
		return 0, err
	}
	if first > watermark+1 {
		return 0, conflictf("attempt %s 的 Worker 事件有缺口：已提交到 %d，批次从 %d 开始", attemptID, watermark, first)
	}
	existing, err := existingWorkerHashes(ctx, tx, attemptID, first, min(last, watermark))
	if err != nil {
		return 0, err
	}
	for _, e := range events {
		if e.Seq > watermark {
			if err := insertWorkerEvent(ctx, tx, taskID, attemptID, e); err != nil {
				return 0, err
			}
			continue
		}
		if !bytes.Equal(existing[e.Seq], workerEventHash(e)) {
			return 0, conflictf("attempt %s 的 Worker 事件 %d 已存在且内容不同", attemptID, e.Seq)
		}
	}
	return max(watermark, last), nil
}

func existingWorkerHashes(ctx context.Context, tx pgx.Tx, attemptID string, from, to int64) (map[int64][]byte, error) {
	hashes := map[int64][]byte{}
	if from > to {
		return hashes, nil
	}
	rows, err := tx.Query(ctx, "SELECT worker_seq, content_hash FROM events WHERE attempt_id = $1 AND worker_seq BETWEEN $2 AND $3",
		attemptID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var h []byte
		if err := rows.Scan(&seq, &h); err != nil {
			return nil, err
		}
		hashes[seq] = h
	}
	return hashes, rows.Err()
}

func insertWorkerEvent(ctx context.Context, tx pgx.Tx, taskID, attemptID string, e runner.WorkerEvent) error {
	seq, err := nextEventSeq(ctx, tx, taskID)
	if err != nil {
		return err
	}
	ts := e.TS
	if ts.IsZero() {
		_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, worker_seq, source, type, payload, content_hash)
			VALUES ($1, $2, $3, $4, $5, 'worker', $6, $7, $8)`,
			taskID, seq, fmt.Sprintf("w:%s:%d", attemptID, e.Seq), attemptID, e.Seq, e.Type, []byte(e.Payload), workerEventHash(e))
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, worker_seq, source, type, payload, content_hash, ts)
		VALUES ($1, $2, $3, $4, $5, 'worker', $6, $7, $8, $9)`,
		taskID, seq, fmt.Sprintf("w:%s:%d", attemptID, e.Seq), attemptID, e.Seq, e.Type, []byte(e.Payload), workerEventHash(e), ts)
	return err
}
```

- [ ] **Step 4：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/persistence; go vet ./internal/persistence/... && CI=true go test -count=1 ./internal/persistence/postgres/'
```

Expected: `ok`。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/persistence/postgres/runner.go internal/persistence/postgres/postgres_test.go && git commit -m "feat(persistence): runner 事务用例——Worker 事件逐条内容校验、checkpoint、产物、终态提议" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 7：resource 用例

**风险：高（资源所有权、单调性）——双评审。**

**Files:**
- Create: `internal/persistence/postgres/resource.go`
- Modify: `internal/persistence/postgres/postgres_test.go`（替换 import 块，末尾追加第四节）

**Interfaces:**
- Consumes：Task 4 的 `run`、`read` 与错误辅助；Task 1 的 `resource.Store`。
- Produces：`*Store` 实现 `resource.Store`（`RecordIntent`、`ResolveIntent`、`GetIntent`、`SeedUIDRanges`、`AssignUIDRange`、`ReleaseUIDRange`、`GetUIDRange`、`MarkStopped`、`UpdateCleanup`、`GetEnvironment`）。

- [ ] **Step 1：写失败的测试**

第四节覆盖：六个用例的"提交后回复丢失"、冲突（意图占用、状态跳跃、同环境不同分配、分配代次不符、清理状态倒退、过期的尝试次数）、释放旧分配不得释放后来复用的范围。

把 `internal/persistence/postgres/postgres_test.go` 的 import 块**替换**为：

```go
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/datadir"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/ownership"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)
```

然后在 `internal/persistence/postgres/postgres_test.go` **末尾追加**：

```go
// ---- resource 用例：未知提交、单调性与分配代次 ----

func withRanges(t *testing.T, s *Store) {
	withFixture(t, s)
	if err := s.SeedUIDRanges(context.Background(), 100000, 4096, 4); err != nil {
		t.Fatal(err)
	}
}

func TestCommitLostResolvesResourceUseCases(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	checkCommitLost(t, []useCase{
		{"RecordIntent", withFixture, func(s *Store) (any, error) {
			return s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"})
		}, "SELECT count(*) FROM resource_intents"},
		{"ResolveIntent", func(t *testing.T, s *Store) {
			withFixture(t, s)
			if _, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"}); err != nil {
				t.Fatal(err)
			}
		}, func(s *Store) (any, error) {
			return s.ResolveIntent(ctx, "i1", resource.IntentAcquired)
		}, "SELECT count(*) FROM resource_intents WHERE state = 'acquired'"},
		{"AssignUIDRange", withRanges, func(s *Store) (any, error) {
			return s.AssignUIDRange(ctx, "env-t1", "alloc-1")
		}, "SELECT count(*) FROM uid_ranges WHERE state = 'assigned'"},
		{"ReleaseUIDRange", func(t *testing.T, s *Store) {
			withRanges(t, s)
			if _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1"); err != nil {
				t.Fatal(err)
			}
		}, func(s *Store) (any, error) {
			return s.ReleaseUIDRange(ctx, "uid-100000", "alloc-1")
		}, "SELECT count(*) FROM uid_ranges WHERE uid_range_id = 'uid-100000' AND state = 'free'"},
		{"MarkStopped", withFixture, func(s *Store) (any, error) {
			return s.MarkStopped(ctx, "env-t1", at)
		}, "SELECT count(*) FROM environments WHERE stopped_at IS NOT NULL"},
		{"UpdateCleanup", withFixture, func(s *Store) (any, error) {
			return s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 0, State: resource.CleanupPending, Error: "busy"})
		}, "SELECT count(*) FROM environments WHERE cleanup_tries = 1"},
	})
}

func TestResourceConflicts(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	withRanges(t, s)
	if _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i1", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", State: resource.CleanupDone}); err != nil {
		t.Fatal(err)
	}
	expectConflicts(t, map[string]func() error{
		"RecordIntent 占用同一资源": func() error {
			_, err := s.RecordIntent(ctx, resource.Intent{IntentID: "i2", EnvID: "env-t1", Kind: "cgroup", Name: "env-t1"})
			return err
		},
		"ResolveIntent 跳跃":       func() error { _, err := s.ResolveIntent(ctx, "i1", resource.IntentReleased); return err },
		"AssignUIDRange 同环境不同分配": func() error { _, err := s.AssignUIDRange(ctx, "env-t1", "alloc-2"); return err },
		"ReleaseUIDRange 分配代次不符": func() error { _, err := s.ReleaseUIDRange(ctx, "uid-100000", "alloc-old"); return err },
		"UpdateCleanup 状态倒退": func() error {
			_, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 1, State: resource.CleanupPending})
			return err
		},
		"UpdateCleanup 过期的尝试次数": func() error {
			_, err := s.UpdateCleanup(ctx, resource.CleanupUpdate{EnvID: "env-t1", ExpectedTries: 0, State: resource.CleanupDone, Error: "x"})
			return err
		},
	})
}

// TestReleaseDoesNotFreeLaterAllocation：释放旧分配不得释放后来复用的同一段范围。
func TestReleaseDoesNotFreeLaterAllocation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	fixture(t, s, "t2")
	if err := s.SeedUIDRanges(ctx, 100000, 4096, 1); err != nil {
		t.Fatal(err)
	}
	r, err := s.AssignUIDRange(ctx, "env-t1", "alloc-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignUIDRange(ctx, "env-t2", "alloc-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseUIDRange(ctx, r.UIDRangeID, "alloc-1"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("重放旧的释放应为冲突，得到 %v", err)
	}
	if got, err := s.GetUIDRange(ctx, "env-t2"); err != nil || got.AllocationID != "alloc-2" {
		t.Fatalf("后来的分配应保持不变：%+v %v", got, err)
	}
}
```

- [ ] **Step 2：确认测试失败**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go test -count=1 ./internal/persistence/postgres/'
```

Expected: 编译失败（`s.RecordIntent undefined` 等）。

- [ ] **Step 3：实现**

`internal/persistence/postgres/resource.go`：

```go
package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
)

var _ resource.Store = (*Store)(nil)

// RecordIntent 记录资源意图（实现 resource.Store）：同一 intent_id 内容相同返回原结果，
// 内容不同或 (kind, name) 已被其他意图占用为冲突。
func (s *Store) RecordIntent(ctx context.Context, i resource.Intent) (resource.Intent, error) {
	if i.IntentID == "" || i.EnvID == "" || i.Kind == "" || i.Name == "" {
		return resource.Intent{}, invalidf("RecordIntent 缺少字段")
	}
	var out resource.Intent
	err := s.run(ctx, "RecordIntent", i.IntentID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO resource_intents (intent_id, env_id, kind, name, state)
			VALUES ($1, $2, $3, $4, 'pending') ON CONFLICT DO NOTHING`, i.IntentID, i.EnvID, i.Kind, i.Name)
		if err != nil {
			return err
		}
		existing, err := selectIntent(ctx, tx, i.IntentID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return conflictf("资源 %s/%s 已被其他意图占用", i.Kind, i.Name)
		}
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 && (existing.EnvID != i.EnvID || existing.Kind != i.Kind || existing.Name != i.Name) {
			return conflictf("意图 %s 已存在且内容不同", i.IntentID)
		}
		out = existing
		return nil
	})
	return out, err
}

// intentTransitions 是意图状态允许的前进方向。
var intentTransitions = map[string][]string{
	resource.IntentPending:  {resource.IntentAcquired, resource.IntentFailed},
	resource.IntentAcquired: {resource.IntentReleased},
}

// ResolveIntent 推进意图状态（实现 resource.Store）：已在目标状态返回原结果，倒退或跳跃为冲突。
func (s *Store) ResolveIntent(ctx context.Context, intentID, state string) (resource.Intent, error) {
	var out resource.Intent
	err := s.run(ctx, "ResolveIntent", intentID+"->"+state, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectIntent(ctx, tx, intentID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("意图 %s", intentID)
		}
		if err != nil {
			return err
		}
		if cur.State == state {
			out = cur
			return nil
		}
		if !contains(intentTransitions[cur.State], state) {
			return conflictf("意图 %s 不能从 %s 变为 %s", intentID, cur.State, state)
		}
		if _, err := tx.Exec(ctx, "UPDATE resource_intents SET state = $2 WHERE intent_id = $1", intentID, state); err != nil {
			return err
		}
		cur.State = state
		out = cur
		return nil
	})
	return out, err
}

// GetIntent 读取资源意图（实现 resource.Store）。
func (s *Store) GetIntent(ctx context.Context, intentID string) (resource.Intent, error) {
	var out resource.Intent
	err := s.read(ctx, "GetIntent", func(ctx context.Context, q queryer) error {
		i, err := selectIntent(ctx, q, intentID, false)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("意图 %s", intentID)
		}
		out = i
		return err
	})
	return out, err
}

func selectIntent(ctx context.Context, q queryer, intentID string, forUpdate bool) (resource.Intent, error) {
	sql := "SELECT intent_id, env_id, kind, name, state FROM resource_intents WHERE intent_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var i resource.Intent
	err := q.QueryRow(ctx, sql, intentID).Scan(&i.IntentID, &i.EnvID, &i.Kind, &i.Name, &i.State)
	return i, err
}

// SeedUIDRanges 幂等地登记 UID 范围（实现 resource.Store）。
func (s *Store) SeedUIDRanges(ctx context.Context, base, size int64, count int) error {
	if base < 0 || size < 1 || count < 1 {
		return invalidf("SeedUIDRanges 参数不合法")
	}
	return s.run(ctx, "SeedUIDRanges", "", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO uid_ranges (uid_range_id, base, size, state)
			SELECT 'uid-' || b, b, $2, 'free' FROM generate_series($1::bigint, $1 + ($3::bigint - 1) * $2, $2) AS b
			ON CONFLICT DO NOTHING`, base, size, count)
		return err
	})
}

// AssignUIDRange 为环境分配 UID 范围（实现 resource.Store）：以 (env_id, allocation_id) 为身份，
// 结果丢失后重试返回原分配。锁顺序：environments → uid_ranges。
func (s *Store) AssignUIDRange(ctx context.Context, envID, allocationID string) (resource.UIDRange, error) {
	if envID == "" || allocationID == "" {
		return resource.UIDRange{}, invalidf("AssignUIDRange 缺少 env_id 或 allocation_id")
	}
	var out resource.UIDRange
	err := s.run(ctx, "AssignUIDRange", envID+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := selectEnvironment(ctx, tx, envID, true); err != nil {
			return err
		}
		cur, err := selectUIDRange(ctx, tx, "owner_id = $1 AND state = 'assigned'", envID)
		switch {
		case err == nil && cur.AllocationID == allocationID:
			out = cur
			return nil
		case err == nil:
			return conflictf("环境 %s 已有分配 %s", envID, cur.AllocationID)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		// SKIP LOCKED：并发分配不等待同一行，也不会因对方刚分配而误报无空闲范围
		free, err := selectUIDRange(ctx, tx, "state = 'free' ORDER BY base LIMIT 1 FOR UPDATE SKIP LOCKED", nil)
		if errors.Is(err, pgx.ErrNoRows) {
			return conflictf("没有空闲的 UID 范围")
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE uid_ranges SET state = 'assigned', owner_kind = 'environment', owner_id = $2, allocation_id = $3
			WHERE uid_range_id = $1`, free.UIDRangeID, envID, allocationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE environments SET uid_range_id = $2 WHERE env_id = $1", envID, free.UIDRangeID); err != nil {
			return err
		}
		free.State, free.OwnerID, free.AllocationID = "assigned", envID, allocationID
		out = free
		return nil
	})
	return out, err
}

// ReleaseUIDRange 释放 UID 范围（实现 resource.Store）：只有分配代次匹配才释放；
// 已释放且未复用返回原结果；已被后来的分配复用为冲突。
func (s *Store) ReleaseUIDRange(ctx context.Context, uidRangeID, allocationID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := s.run(ctx, "ReleaseUIDRange", uidRangeID+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectUIDRange(ctx, tx, "uid_range_id = $1", uidRangeID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("UID 范围 %s", uidRangeID)
		}
		if err != nil {
			return err
		}
		switch {
		case cur.AllocationID != allocationID:
			return conflictf("UID 范围 %s 当前分配为 %q，不是 %q", uidRangeID, cur.AllocationID, allocationID)
		case cur.State == "free":
			out = cur
			return nil
		}
		if _, err := tx.Exec(ctx, "UPDATE uid_ranges SET state = 'free', owner_kind = '', owner_id = '' WHERE uid_range_id = $1",
			uidRangeID); err != nil {
			return err
		}
		cur.State, cur.OwnerID = "free", ""
		out = cur
		return nil
	})
	return out, err
}

// GetUIDRange 读取分配给环境的 UID 范围（实现 resource.Store）。
func (s *Store) GetUIDRange(ctx context.Context, envID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := s.read(ctx, "GetUIDRange", func(ctx context.Context, q queryer) error {
		r, err := selectUIDRangeRead(ctx, q, envID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("环境 %s 没有 UID 范围", envID)
		}
		out = r
		return err
	})
	return out, err
}

// selectUIDRange 以 FOR UPDATE 读取一段范围；where 为条件子句（含可选参数）。
func selectUIDRange(ctx context.Context, tx pgx.Tx, where string, arg any) (resource.UIDRange, error) {
	sql := "SELECT uid_range_id, base, size, state, owner_id, allocation_id FROM uid_ranges WHERE " + where
	if !strings.Contains(where, "FOR UPDATE") {
		sql += " FOR UPDATE"
	}
	var r resource.UIDRange
	var row pgx.Row
	if arg == nil {
		row = tx.QueryRow(ctx, sql)
	} else {
		row = tx.QueryRow(ctx, sql, arg)
	}
	err := row.Scan(&r.UIDRangeID, &r.Base, &r.Size, &r.State, &r.OwnerID, &r.AllocationID)
	return r, err
}

func selectUIDRangeRead(ctx context.Context, q queryer, envID string) (resource.UIDRange, error) {
	var r resource.UIDRange
	err := q.QueryRow(ctx, `SELECT uid_range_id, base, size, state, owner_id, allocation_id FROM uid_ranges
		WHERE owner_id = $1 AND state = 'assigned'`, envID).Scan(&r.UIDRangeID, &r.Base, &r.Size, &r.State, &r.OwnerID, &r.AllocationID)
	return r, err
}

// MarkStopped 记录环境已停止（实现 resource.Store）：只在尚未记录时写入，重试不倒退。
func (s *Store) MarkStopped(ctx context.Context, envID string, at time.Time) (resource.Environment, error) {
	var out resource.Environment
	err := s.run(ctx, "MarkStopped", envID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE environments SET stopped_at = $2 WHERE env_id = $1 AND stopped_at IS NULL", envID, at); err != nil {
			return err
		}
		e, err := selectEnvironment(ctx, tx, envID, false)
		out = e
		return err
	})
	return out, err
}

// cleanupRank 定义清理状态的前进顺序。
var cleanupRank = map[string]int{resource.CleanupNone: 0, resource.CleanupPending: 1, resource.CleanupDone: 2}

// UpdateCleanup 以 cleanup_tries 的 CAS 记录一次清理尝试（实现 resource.Store）。
func (s *Store) UpdateCleanup(ctx context.Context, u resource.CleanupUpdate) (resource.Environment, error) {
	if _, ok := cleanupRank[u.State]; !ok || u.EnvID == "" || u.ExpectedTries < 0 {
		return resource.Environment{}, invalidf("UpdateCleanup 参数不合法")
	}
	var out resource.Environment
	err := s.run(ctx, "UpdateCleanup", u.EnvID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectEnvironment(ctx, tx, u.EnvID, true)
		if err != nil {
			return err
		}
		switch {
		case cur.CleanupTries == u.ExpectedTries+1 && cur.CleanupState == u.State && cur.CleanupError == u.Error:
			out = cur // 同一次尝试的重试
			return nil
		case cur.CleanupTries != u.ExpectedTries:
			return conflictf("环境 %s 的 cleanup_tries 为 %d，不是 %d", u.EnvID, cur.CleanupTries, u.ExpectedTries)
		case cleanupRank[u.State] < cleanupRank[cur.CleanupState]:
			return conflictf("环境 %s 的清理状态不能从 %s 退回 %s", u.EnvID, cur.CleanupState, u.State)
		}
		if _, err := tx.Exec(ctx, `UPDATE environments SET cleanup_state = $2, cleanup_tries = $3, cleanup_error = $4, next_retry_at = $5
			WHERE env_id = $1`, u.EnvID, u.State, u.ExpectedTries+1, u.Error, u.NextRetryAt); err != nil {
			return err
		}
		out, err = selectEnvironment(ctx, tx, u.EnvID, false)
		return err
	})
	return out, err
}

// GetEnvironment 读取环境记录（实现 resource.Store）。
func (s *Store) GetEnvironment(ctx context.Context, envID string) (resource.Environment, error) {
	var out resource.Environment
	err := s.read(ctx, "GetEnvironment", func(ctx context.Context, q queryer) error {
		e, err := selectEnvironment(ctx, q, envID, false)
		out = e
		return err
	})
	return out, err
}

func selectEnvironment(ctx context.Context, q queryer, envID string, forUpdate bool) (resource.Environment, error) {
	sql := `SELECT env_id, kind, COALESCE(attempt_id, ''), status, stopped_at, cleanup_state, cleanup_tries, cleanup_error, next_retry_at
		FROM environments WHERE env_id = $1`
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var e resource.Environment
	err := q.QueryRow(ctx, sql, envID).Scan(&e.EnvID, &e.Kind, &e.AttemptID, &e.Status, &e.StoppedAt,
		&e.CleanupState, &e.CleanupTries, &e.CleanupError, &e.NextRetryAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, notFoundf("环境 %s", envID)
	}
	return e, err
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4：验证**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && gofmt -l internal/persistence; go vet ./... && CI=true go test -count=1 ./internal/persistence/postgres/ ./internal/archtest/'
```

Expected: 两个包 `ok`。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-4 && git add internal/persistence/postgres/resource.go internal/persistence/postgres/postgres_test.go && git commit -m "feat(persistence): resource 事务用例——意图、UID 范围分配代次、停止与清理的单调更新" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

### Task 8：CI 中的 PostgreSQL 服务

**风险：低（配置）——单次评审。**

**Files:**
- Modify: `.github/workflows/ci.yml`（`correctness` 与 `linux-integration` 两个作业各在 `runs-on: ubuntu-24.04` 之后加入下面这段）

`linux-integration` 不允许任何跳过，因此数据库测试必须在这两个作业中实际运行。

```yaml
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: agentbox
          POSTGRES_PASSWORD: agentbox
          POSTGRES_DB: agentbox
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U agentbox -d agentbox"
          --health-interval 2s --health-timeout 2s --health-retries 30
    env:
      # 事务语义只在真实 PostgreSQL 上测试；CI 中未设置时数据库测试失败而不是跳过。
      AGENTBOX_TEST_DATABASE_URL: postgres://agentbox:agentbox@localhost:5432/agentbox?sslmode=disable
```

- [ ] **Step 1：修改并校验 YAML**

```powershell
cd F:\go-agentbox-m1-4\worker; uv run --with pyyaml python -c "import yaml;d=yaml.safe_load(open('../.github/workflows/ci.yml',encoding='utf-8'));print({k:list(v.get('services',{})) for k,v in d['jobs'].items()})"
```

Expected: `{'correctness': ['postgres'], 'complexity-report': [], 'linux-integration': ['postgres'], 'python': []}`。

- [ ] **Step 2：本地等价验证（模拟 correctness 作业）**

```bash
MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu -e bash -c 'export PATH=/usr/local/go/bin:/usr/bin:/bin GOTOOLCHAIN=local GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy GOSUMDB=off AGENTBOX_TEST_DATABASE_URL="postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable"; cd /mnt/f/go-agentbox-m1-4 && go build ./... && GOOS=windows go build ./... && go vet ./... && CI=true go test -count=1 ./...'
```

Expected: 全部包 `ok` 或 `[no test files]`。

- [ ] **Step 3：提交**

```bash
cd /f/go-agentbox-m1-4 && git add .github/workflows/ci.yml && git commit -m "ci: correctness 与 linux-integration 作业加入 PostgreSQL 服务" ${COAUTHOR:+-m "$COAUTHOR"}
```

---

## 联合验收（计划完成的判定）

1. **本地（WSL + Docker）**：Task 8 Step 2 的命令全部通过；`internal/persistence/postgres` 连续三次通过（`CI=true`，不得跳过）。
2. **阶段可构建**：Task 4、5、6、7 完成时的代码树各自能 vet 并通过测试（每个任务的 Step 4/5 已覆盖）。
3. **CI（Linux，判定依据）**：经项目负责人授权推送后，`correctness`、`complexity-report`、`linux-integration`、`python (3.11)`、`python (3.13)` 全部实际执行并通过；`linux-integration` 中数据库测试实际运行、无新增跳过；golangci-lint（正确性）通过。
4. **范围核对**：`git diff --stat m1-3-protocol-worker...HEAD` 只包含本计划 Files 中列出的文件。

计划完成后向项目负责人汇报：实现的接口、测试与 CI 结果、偏离计划之处及原因。推送与合并由项目负责人决定。

---

## 自查记录

- **设计覆盖**：设计 §1.1 依赖规则 → Task 1 archtest；§1.2 所有权三分 → Task 2、3、4；§1.3 表结构 → Task 4 迁移；§1.4 接口批次 → Task 1（接口）与 Task 5–7（实现），`recovery.Store` 明确交给 Plan 6；§2.1–§2.3 事务辅助与未知提交 → Task 4 `run`/`final` 与测试，Task 5 的"第一次提交未完成"；§2.4 每个用例的身份与内容比较 → Task 5–7 的实现与"提交回复丢失""冲突"测试；§2.6 前置条件 → Task 5（控制写入、准入、判决仲裁）与 Task 6（checkpoint 与产物的 fencing、引用授权）；§3.1 事件 → Task 5、6；§3.2 BlobStore → Task 2；§3.3 E46 → Task 3、4；§3.4 E13 存储部分 → Task 4；§3.5 迁移 → Task 4；§4 测试与环境 → Task 4、8。
- **验收归属**：E11a（Task 5–7 的提交回复丢失）、E12 存储部分（Task 4 死锁重跑）、E13 存储部分（Task 4 失锁）、E46（Task 3、4）。
- **演练**：本计划的全部代码已在临时 worktree 中按计划文本组装，并在 WSL + Docker Desktop 的 PostgreSQL 16.15 上运行：全仓库 `go build`（Linux 与 Windows）、`go vet`、`CI=true go test ./...` 通过；`internal/persistence/postgres` 连续三次通过；Task 4、5、6、7 各阶段的代码树分别通过；变异检查——去掉 `CreateAttempt` 的事务内身份仲裁、让终态提议静默忽略不同内容、跳过 Worker 事件的逐条内容比较、不重跑死锁中止——均被对应测试捕获；评审修订后又逐个去掉 9 条前置条件规则（checkpoint 的 fencing、引用授权，产物的 fencing，判决的当前 attempt、控制版本、desired 约束，attempt 准入、"准入先于幂等查询"的错误顺序，控制写入规则），均被捕获；失锁检测延迟约 100 ms（检测周期 200 ms 时）。
- **执行中修订**：Task 1 的 archtest 原先列出 Task 2、3 才创建的包，`go list` 失败（演练只逐阶段验证了 Task 4–7）；改为 Task 1 只列已存在的包、Task 4 扩展到全部八个，最终文件与原计划相同。
- **执行中评审修订（Task 2、3）**：BlobStore 的已存在路径曾直接返回成功（只比较大小、不 fsync），新分片目录的父目录未 fsync——改为 NewLocal 预建 256 个分片并 fsync，Put 总是以校验过的副本原子替换后 fsync 分片目录，并补损坏/截断替换测试；Bootstrap 的前置条件（先 flock、后 advisory lock）写入文档注释，出错时不返回 install_id，拒绝空的 newID，空库判定加上无 installation 记录，测试补齐（未知 schema 有文件、CompleteInstallation 校验 ID、空 ID）。
- **执行中评审修订（Task 4）**：事务辅助的错误分类（服务端终止会话与超时码、COMMIT 的 FATAL 与 SafeToRetry、`ErrTxCommitRollback`、调用方取消、`%w` 保留原因）按设计 §2.1 修正并补 `TestErrorClassification`；spec §6 的复合外键（events、artifacts）补上并有违反测试；失锁测试按当前数据库过滤 pid、等待在途操作阻塞并断言检测延迟；`Ownership.Close` 幂等；迁移路径随失锁取消；`InspectInstallation` 统一用 pg_catalog 查表；回滚有界超时。修订在含 Task 4–7 全部代码的演练树中验证：全部测试连续三次通过、各阶段独立通过、逐项回退均被测试捕获。
- **执行中评审修订（Task 5）**：ApplyControl 按规格 §8.1 的转换表（`controlTransition`）检查来源状态并以 CAS 写入，拒绝终态（`task_ended`）与被取代的控制版本（`control_changed`）；FinalizeAttempt 只接受 running/pausing/cancelling 的任务并推进 `applied_control_version`；AcceptControl 拒绝跨任务复用 request_id，CreateTask/CreateAttempt 对已存在的 task_id/env_id 立即冲突；`snapshot` 覆盖更多字段，控制拒绝与转换、宿主事件幂等均有测试。在含 Task 4–7 全部代码的演练树中验证并做逐项回退检查。
- **未在本地验证**：golangci-lint（本机未安装，由 CI 判定）。
- **评审修订（第三轮）**：checkpoint 缺少 fencing 与引用授权、最终判决未在锁内仲裁取消与当前 attempt、创建 attempt 缺少准入前置条件——均为规格 §5.5、§8.1 的实现遗漏，已按设计 §2.6 并入 Task 5、6，并补测试。同类遗漏 `AcceptControl` 的控制写入规则（`task_ended` 等）一并补上，"判决先提交"的测试依赖它。设计文档的 pgx 版本已统一为 v5.7.6；提交署名只在有真实共同作者时添加，不编造、不阻止提交。
- **占位符**：无（`COAUTHOR` 是可选的环境变量，未设置时提交命令省略署名）。
