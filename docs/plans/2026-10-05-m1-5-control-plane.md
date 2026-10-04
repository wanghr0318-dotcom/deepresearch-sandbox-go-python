# M1 Plan 5：控制面核心（不依赖 Plan 1B 的部分）实施计划

> **执行方式**：主 agent 逐任务实现与评审，不使用子 agent。每个任务 TDD：先写列出的测试并确认失败，再实现，按"验证"通过后提交。

**Goal:** 实现控制面核心：资源 coordinator 与 cleanup loop、admission、task 状态机（纯 `Decide`）与 task actor、AttemptRunner（事件处理、checkpoint 与产物提交、完成屏障、裁决分类）、fake Gateway；补齐它们需要的事务用例；以**进程型 fake provider**（在宿主上直接运行真实的 `sim_worker`，不隔离）与真实 PostgreSQL 完成控制面层面的首个切片与 E1、E4、E7、E8、E10。

**不在本计划内（等待 Plan 1B 与 Plan 2 的生产启动器）**：在真实隔离环境中的首个切片与 E2、E3；见文末。

**规格依据：** §2 执行所有权规则；§3.2 并发模型与写入所有权；§5.3–§5.9 协议语义、checkpoint、产物、屏障、裁决、暂停取消；§8.1–§8.3 状态机；§14.3 故障分类；§14.4 运行时限；§14.5 Store 故障；§19 默认参数；代码组织设计 §4–§6、§8、§9；Provider 契约；Plan 4 的事务用例与错误契约。

## Global Constraints

- **唯一决策者**（代码组织 §6）：task/attempt 生命周期只由 task actor 经 `task.Decide` 决定；coordinator 只执行物理操作并报告事实（写 `stopped_at`、intent、UID 范围、清理列）；AttemptRunner 不写生命周期字段；裁决分类只有 `runner.Classify` 一个权威实现（§9 "同一业务规则只有一个权威实现"）。
- **过期结果规则**（§3.2）：所有异步结果携带 `attempt_id`/`env_id`；非当前者不改变状态，携带的资源交给清理，admission 授予归还。
- **决策与副作用分开**（代码组织 §5）：`Decide(State, Event) (Decision, error)` 是纯函数，`Decision.Effects` 为封闭集合；actor 是 select 循环，用 `switch` 执行副作用，不阻塞；不设 bus、registry 或通用 handler。actor panic → 致命停止（Plan 6 的装配负责进程级处理；本计划的 actor 把 panic 转为致命错误上报）。
- **持久化**：只经各包的窄接口；新事务用例在 `persistence/postgres` 实现并只在真实 PostgreSQL 上测试（代码组织 §4.2）；顺序遵循"身份查询 → 前置条件 → 写入"（Plan 4 设计 §2.6）。
- **默认参数**按规格 §19：`T_ready` 30 s、`exit_grace` 10 s、屏障 A/B 5 s/120 s、Store 故障阈值连续 5 次或 30 s、`max_fault_retries` 3、OOM 重试 1、重试退避基数 2 s 上限 60 s 带抖动、运行时间持久化 10 s。
- **进程型 fake provider 只用于测试**：在宿主上直接运行 `python3 -m sim_worker`，不隔离；只出现在 `_test.go` 与 `tests/e2e`；archtest 已禁止 `cmd/agentbox` 导入 `provider/fake`。
- **测试**：每个包一个 `*_test.go`；跨包的控制面集成测试放在 `tests/e2e/`（代码组织 §8 的首个切片即首次集成验收，本计划起建立该目录）。Python 在 WSL 中为 `python3`（≥ 3.11，`sim_worker` 无运行时依赖），以 `PYTHONPATH=worker` 运行。
- 工作区：Plan 2 所在的 `m1-batch2` 分支（Plan 2 完成后继续）。其余约定同 Plan 2。

## 任务与依赖

| 任务 | 内容 | 依赖 |
|---|---|---|
| 1 | 事务用例补齐（task、runner、resource） | Plan 4 |
| 2 | admission | — |
| 3 | `task.Decide`：task 与 attempt 状态机 | 1（类型） |
| 4 | resource coordinator 与 cleanup loop | 1、Plan 2 Task 1 |
| 5 | AttemptRunner：事件管线、checkpoint、产物、终态提议 | 1 |
| 6 | AttemptRunner：完成屏障、期限、控制消息、`Classify`、Store 故障阈值 | 5 |
| 7 | task actor、调度与 fake Gateway | 2、3、4、6 |
| 8 | 控制面验收：首个切片与 E1、E4、E7、E8、E10（进程型 fake provider + 真实 PostgreSQL） | 7 |

---

### Task 1：事务用例补齐

**Files:** Modify `internal/task/store.go`、`internal/runner/store.go`、`internal/resource/store.go`、`internal/persistence/postgres/task.go`、`runner.go`、`resource.go`、`postgres_test.go`（末尾追加一节）；Create `internal/persistence/postgres/migrations/0003_control_plane.sql`（仅当需要新列或索引时；无需则不建）。

**Interfaces（新增或变更）：**

```go
// task
type RetryKind string // "" | "fault" | "oom"
type NewAttempt struct { TaskID, AttemptID string; AttemptNo int64; EnvID string; Retry RetryKind } // 新增 Retry：故障重试同事务递增 fault_retries_used，OOM 重试递增 oom_retries_used（§8.1"重试计数"、§14.2）
type Verdict struct { /* 既有字段 */ NotBefore *time.Time } // TaskStatus = queued 时写 tasks.not_before
type TaskState struct {
	TaskID, Status, StatusReason, CurrentAttemptID string
	Spec, Limits json.RawMessage; ConfigVersion string
	Desired string; ControlVersion, AppliedControlVersion int64
	AttemptsTotal, FaultRetriesUsed, MaxFaultRetries, OOMRetriesUsed, RunTimeMs int64
	NotBefore *time.Time
	Latest *LatestCheckpoint // 最新已提交 checkpoint 的完整内容，用于 init.resume；无则 nil
}
type LatestCheckpoint struct { CheckpointID, StepID string; State json.RawMessage; StateRef string; Refs []string }
// 新增方法
LoadTask(ctx, taskID string) (TaskState, error)
ListActiveTasks(ctx) ([]string, error)                       // 非终态任务，供 actor 启动
PersistRunTime(ctx, taskID, attemptID string, totalMs int64) (int64, error) // 单调写入累计值（取较大者）；只接受当前 attempt；返回累计值
RevokeAttemptAccess(ctx, attemptID, reason string) error     // attempt_access → revoked（幂等）

// resource
ListCleanupCandidates(ctx, now time.Time, limit int) ([]Environment, error) // stopped_at 已记录、cleanup 未完成、所属 attempt 已结束、next_retry_at 已到
RecordQuarantine(ctx, q Quarantine) error                  // 按 (layer, path) 幂等
type Quarantine struct { Layer, Path, ObservedOwner, Reason string }
```

**规则：** `CreateAttempt` 的 `Retry` 只在新建时递增对应计数，重放不重复递增（身份优先）；`FinalizeAttempt` 在 `TaskStatus = queued` 时写 `not_before`，其他状态拒绝非空 `NotBefore`（`errInvalid`）；`LoadTask` 一次读取（单个只读查询或只读事务），`Latest` 读取 `task_progress.latest_checkpoint_id` 指向的完整 checkpoint；`PersistRunTime` 只在 `current_attempt_id = attemptID` 时写入 `GREATEST(run_time_ms, totalMs)`，否则 `stale_attempt`（执行中修订：原设计为累加增量，提交结果未知后的重跑会重复累计；改为由 actor 传入累计值并单调写入）。

**Tests**（真实 PostgreSQL，追加在 `postgres_test.go` 末尾）：故障重试的 `CreateAttempt` 恰好递增一次（含提交结果未知后的重试）；`not_before` 写入与非 queued 时被拒；`LoadTask` 返回最新 checkpoint 的完整内容；`PersistRunTime` 拒绝旧 attempt；`RevokeAttemptAccess` 幂等且使 `CommitCheckpoint` 以 `stale_attempt` 拒绝；`ListCleanupCandidates` 只返回满足四个条件的环境；`RecordQuarantine` 幂等。

**验证：** `CI=true go test -count=1 ./internal/persistence/postgres/`（连续三次）。

---

### Task 2：admission

**Files:** Create `internal/admission/admission.go`、`admission_test.go`。

**Interfaces：**

```go
type Capacity struct { RunSlots int; MemoryBytes int64 }
type Request struct { TaskID string; MemoryBytes int64 }
type Grant struct { ID uint64; TaskID string; MemoryBytes int64 }
func New(c Capacity) *Admission
func (a *Admission) Acquire(ctx context.Context, r Request) (Grant, error) // FIFO；ctx 结束返回 ctx 错误且不占用
func (a *Admission) Release(g Grant)                                      // 幂等
func (a *Admission) Rebuild(occupied []Grant)                             // 启动时按 recovery 报告的实际占用（Plan 6）
func (a *Admission) Snapshot() Usage
```

**规则：** 严格 FIFO（队首不满足时后续等待，避免大请求饥饿）；单个请求超过总容量 → 立即返回错误；未确认停止的环境保持占用（由调用方在 `stopped_at` 之后才 `Release`，§14.5）；exec slots 属于 M4，本计划不实现。

**Tests：** 容量内立即授予；超出时排队并按 FIFO 授予；队首等待时 ctx 取消使其出队且后续请求前移；重复 `Release` 不超额；超过总容量的请求被拒；`Rebuild` 后可用容量正确。

**验证：** `go test -count=1 -race ./internal/admission/`

---

### Task 3：`task.Decide`

**Files:** Create `internal/task/decide.go`、`internal/task/task_test.go`。

**Interfaces：**

```go
type State struct {
	TaskStatus string     // queued | running | pausing | cancelling | paused | succeeded | failed | cancelled
	Desired string; ControlVersion, AppliedControlVersion int64
	Attempt *AttemptState // 当前 attempt；nil 表示没有
	FaultRetriesUsed, MaxFaultRetries, OOMRetriesUsed int64
	NotBefore *time.Time; Now time.Time
	SlotHeld bool
}
type AttemptState struct { AttemptID, EnvID, Status string /* created|starting|handshaking|active|finishing|ended|stop_blocked */; EnvStopped bool; Outcome *Outcome }
// Outcome 是 Decide 使用的 attempt 结果（纯数据，本包定义）；actor 把 runner.Outcome 转换为它，Decide 不导入 runner。
type Outcome struct { Class string; Retry RetryKind; ProposalKind string; Result json.RawMessage; ExitCode, ExitSignal *int64; OOMKillDelta int64; PlatformKilled, OutputIncomplete bool }
type Event interface{ isEvent() }
// 事件（封闭集合）：Tick、ControlChanged、SlotGranted、AttemptCreated、EnvCreated{Err}、WorkerStarted{Err}、
// AttemptFinished{Outcome}、VerdictCommitted、EnvStopped、StopUnconfirmed、StoreFailed{Err}
type Decision struct { Next State; Effects []Effect }
type Effect interface{ isEffect() }
// 副作用（封闭集合）：RequestSlot、ReleaseSlot、CreateAttempt{Retry}、CreateEnvironment、StartWorker、
// SendControl{Kind, GraceMs}、StopEnvironment、RevokeAccess、Finalize{Verdict}、ApplyControl{Version, Status}、WakeAt{Time}
func Decide(s State, e Event) (Decision, error)
```

**规则（逐条对应，每条一组表驱动用例）：**
- §8.1 状态图的每条边；创建 attempt 的前置条件（queued、desired = run、无未停止的旧执行、`not_before` 已到）；`stop_blocked` 阻止替代执行（§2）。
- 控制：`ApplyControl` 的目标状态等于 Plan 4 `controlTransition` 的结果（同一张表，Decide 不另写规则：调用 `persistence` 无关的纯函数——为避免两份实现，本任务把 `controlTransition` 从 `persistence/postgres` 移到 `internal/task`（纯函数），postgres 导入它）；cancel 优先于 pause；`starting`/`handshaking` 阶段的 cancel/pause 直接停止执行。
- 裁决：`Finalize` 的 TaskStatus 按 §8.1"最终裁决"读取 desired（cancel → cancelled 并在已有有效 result 时记 `completed_during_cancel`；pause → paused / `completed_during_pause`；run → 按 `Outcome.Class` 与 §14.3 重试资格：可重试且未超限 → queued + `not_before = now + 退避`，否则 failed）；OOM 重试至多一次；`task_deadline_exceeded` 不重试；终态事件类型为 `task_terminal`。
- 过期结果：事件的 `attempt_id` 非当前 → 无状态变化，副作用只含资源转交（`StopEnvironment`/`ReleaseSlot`）。

**Tests：** 表驱动穷举"状态 × 事件"（不合法组合返回错误）；重点用例：取消先于 result 被接受时永不 `succeeded`；result 先被裁决后迟到的 cancel 不改变终态；`stop_blocked` 时不发出 `CreateAttempt`；重试次数到上限后 failed；OOM 第二次 failed；过期 `AttemptFinished` 只产生资源转交。

**验证：** `go test -count=1 ./internal/task/ ./internal/persistence/postgres/`（后者确认 `controlTransition` 迁移后行为不变）。

---

### Task 4：resource coordinator 与 cleanup loop

**Files:** Create `internal/resource/coordinator.go`、`internal/resource/cleanup.go`、`internal/resource/resource_test.go`。

**Interfaces：**

```go
type Provider interface { // 消费者窄接口（Provider 契约第 1 节）
	Create(ctx, provider.EnvSpec) (provider.EnvInfo, error)
	Stop(ctx, envID string) error
	Destroy(ctx, envID string) error
	List(ctx) ([]provider.EnvInfo, error)
	Scan(ctx) (provider.ScanReport, error)
}
type EnvRequest struct { EnvID, AttemptID string; Kind provider.EnvKind; Template string; Limits provider.Limits; Mounts provider.Mounts }
type Coordinator struct { /* 每个 env_id 一个串行执行者 */ }
func NewCoordinator(store Store, p Provider, opt Options) *Coordinator // Options：InstallID、UID 范围参数、退避
func (c *Coordinator) CreateEnv(ctx, r EnvRequest) (provider.EnvInfo, error)
func (c *Coordinator) StopEnv(ctx, envID string) (StopResult, error) // StopResult{Stopped bool; At time.Time; Blocked bool}
func (c *Coordinator) RunCleanup(ctx) error                         // cleanup loop，直到 ctx 结束
```

**规则：**
- `CreateEnv`：同一 env 串行；`RecordIntent(env)` → `AssignUIDRange(env, allocation)` → `provider.Create` → `ResolveIntent(acquired)`；`ErrIncomplete` → `Stop` → `Destroy` → 以新的创建重建一次；`ErrForeign` → `RecordQuarantine` 并返回错误（`create_failed_env` 由调用方分类）；`ErrConflict` → 返回错误；ctx 取消或失败 → `ResolveIntent(failed)` 只在确认无残留时（规格 §8.3 intent 规则），否则保留 pending 交给清理。
- `StopEnv`：`provider.Stop` 成功 → `MarkStopped(now)` → 返回 `Stopped`、`Recorded`（执行中修订：`MarkStopped` 失败时返回 `Stopped=true, Recorded=false`、错误为空，保留事实，下次调用以原时间补记；调用方只在 `Recorded` 时归还容量；`ErrForeign` 时按 `Scan` 条目原样记录隔离；已确认停止的环境再 `CreateEnv` 为 `ErrEnvStopped`）；`ErrStopUnconfirmed` → `Blocked`（不写 `stopped_at`）；Store 写失败时保留"已停止"事实并退避重试提交（§14.5 在线补偿）。
- cleanup loop：只处理 `ListCleanupCandidates`；`Destroy` → `UpdateCleanup(done)` → `ReleaseUIDRange` → 关闭创建 intent（acquired→released，pending→failed）（执行中修订）；失败 → `UpdateCleanup(pending, error, next_retry_at = 退避)`；只写清理列（§3.2）。

**Tests**（fake provider + 内存 Store 替身实现 `resource.Store`；事务语义已在 Plan 4 覆盖）：正常创建的调用顺序；`ErrIncomplete` → 停止、销毁、重建；`ErrForeign` → 记录隔离且不销毁；创建中途取消时 intent 不被置为 failed；`StopEnv` 在 `ErrStopUnconfirmed` 时不写 `stopped_at`；同一 env 的并发 `CreateEnv`/`StopEnv` 被串行化（以 fake 的阻塞钩子检验顺序）；cleanup：成功后归还 UID 范围，失败后按退避重试，未停止的环境不处理。

**验证：** `go test -count=1 -race ./internal/resource/`

---

### Task 5：AttemptRunner——事件管线、checkpoint、产物、终态提议

**Files:** Create `internal/runner/runner.go`、`internal/runner/artifact.go`（Linux：`openat2`）、`internal/runner/artifact_other.go`、`internal/runner/runner_test.go`。

**Interfaces：**

```go
type Starter interface { StartExec(ctx, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) } // 消费者窄接口
type Attempt struct {
	TaskID, AttemptID string; AttemptNo int64; EnvID string
	Init protocol.Init      // 由 actor 依 LoadTask 构造（含 resume）
	OutDir string           // 宿主侧 workspace 中的 out/<attempt_id>
	Exec provider.ExecSpec
}
type Control struct { Kind string /* cancel | pause */; GraceMs int64 }
type Outcome struct {
	Proposal *runner.TerminalProposal; ResultPayload json.RawMessage
	Exit provider.ExitStatus; ExitErr error; Diag provider.ResourceDiag
	Class, Retry string; PlatformKilled, OutputIncomplete bool; Violation string
}
type Runner struct{ /* store、blobs、starter、diag、选项 */ }
func New(store Store, blobs blob.Store, starter Starter, diag func(ctx, envID string) (provider.ResourceDiag, error), opt Options) *Runner
func (r *Runner) Run(ctx context.Context, a Attempt, controls <-chan Control) Outcome
```

**规则：**
- 读取 goroutine 读 stdout 入有界队列；处理器按 `seq` 处理（`protocol.WorkerStream.Observe` 检查顺序与阶段）；stderr 保留最后 64 KiB；进程数据不经控制通道。
- 事件持久化：业务事件经 `AppendWorkerEvents` 分批原子追加；**artifact 保存完成后才处理其后的 checkpoint**（§5.7）。
- 产物（§5.6）：持有 `OutDir` 的目录 FD；`path` 校验（相对、非空、无 NUL、≤ 4 KiB、无 `.`/`..`）；以 `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV, O_RDONLY|O_NOFOLLOW|O_NONBLOCK)` 打开并 `fstat` 为普通文件；从已打开 FD 复制并哈希，`blob.Put` 后 `RegisterArtifact`，再发 `artifact_result`；声明的哈希与大小只是待验证数据；保存有独立超时与大小上限（§5.10）。
- checkpoint：`CommitCheckpoint` 的结果映射为 `checkpoint_result{committed|conflict|rejected|retryable_error, code}`（`ErrRejected` 的 code 原样传递；`ErrCommitUnknown` 与暂时性错误 → `retryable_error`）；`checkpoint_query` → `QueryCheckpoint` → `committed`/`not_found`/`retryable_error`；同一 attempt 至多一个在途提交（`commit_in_flight`）。
- 终态提议：`result`/`error`/`paused` 到达时 `RecordTerminalProposal` 并保存其内容；（执行中修订）`Attempt.OnReady func()`：处理完合法的 `ready` 后同步调用一次，供 actor 在 Worker 就绪后改用协议 `cancel`/`pause`；之后的业务事件记为协议违规（§5.8），`checkpoint_query`、`task_outcome_query`、`task_released` 例外。
- 大小与结构限制由 `protocol` 包执行；超限 → `output_limit_exceeded`。

**Tests**（fake `Starter` 回放 `protocol/fixtures` 的场景转录，不需要 Python；规格 §5.11 列出的 task 模式场景各一个用例）：`happy_path`、`checkpoint_conflict`、`seq_gap`、`missing_ref`、`ack_lost_retry_same_result`、`old_checkpoint_no_rewind`、`foreign_blob_rejected`、`artifact_fail_blocks_checkpoint`、`artifact_path_escape_out_to_in`、`state_ref_foreign_blob_rejected`、`historical_output_version_pinned`；另：符号链接、FIFO、`../in` 路径被拒（E10 的单元层）；登记后改写文件不影响已保存副本。

**验证：** `CI=true go test -count=1 ./internal/runner/`（使用真实 PostgreSQL 与临时 BlobStore）。

---

### Task 6：AttemptRunner——屏障、期限、控制与裁决分类

**Files:** Modify `internal/runner/runner.go`、`runner_test.go`；Create `internal/runner/classify.go`。

**Interfaces：**

```go
type ClassifyInput struct {
	Proposal *runner.TerminalProposal; Exit provider.ExitStatus; ExitErr error; Diag provider.ResourceDiag
	PlatformKill string /* "" | cancel | pause | timeout | exit_grace | ready_timeout | store_unavailable */
	Violation string; OutputIncomplete bool; StartErr error; ControlLost bool
}
func Classify(in ClassifyInput) (class string, retry string) // retry 取值 ""、"fault"、"oom"，与 task.RetryKind 相同；runner 不导入 task
```

**规则：**
- `Classify` 是 §5.8 与 §14.3 的唯一实现：（执行中修订）有效 result 在 cancel/pause 生效期间仍分类为 `succeeded`（或 `oom_observed_in_attempt`），由 `Decide` 据 desired 记 `completed_during_cancel`/`completed_during_pause`；`crashed_signal`（不含平台主动终止）、`control_lost`、`ready_timeout`、`worker_oom_likely`（SIGKILL 且 `OOMKillDelta > 0`）、`oom_observed_in_attempt`（正常结束但 delta > 0，按 Worker 结果裁决）、`worker_error`（`retryable` 与拒绝列表 `budget_exhausted`、`protocol_*`）、`protocol_violation`、`output_limit_exceeded`、`exit_after_result`、`store_unavailable` 等；平台主动终止按原因分类并置 `platform_killed`。
- 屏障（§5.7）：进程退出后 A：管道收尾（5 s，超时终止执行树、`output_incomplete`、丢弃末尾半行）；B：处理全部已完整接收的事件（120 s）；然后返回 `Outcome`。
- 期限：`T_ready` 内无 `ready` → 终止、`ready_timeout`；终态提议后 `exit_grace` 内未退出 → 终止后按提议与退出原因分类。
- 控制：`controls` 收到 cancel/pause → 发送协议 `cancel`/`pause{grace_ms}`；到期 → `Terminate`。
- Store 故障：连续 5 次或 30 s 内 `CountsTowardFailureThreshold` 的失败 → 终止、`store_unavailable`（§14.5）；每个 channel 发送 `select` 在 `ctx.Done()` 上。

**Tests：** `Classify` 表驱动覆盖 §14.3 每一行与 §5.8 每一行；屏障 A 超时 → `output_incomplete`；`T_ready` 超时；`exit_grace` 超时；cancel 在 grace 内正常结束与到期终止两种；`pause_deadline_expired`、`result_then_nonzero_exit`、`exit_after_cancel_no_restart`、`exit_before_result_processed` 场景 fixtures；Store 连续失败 5 次触发 `store_unavailable`。

**验证：** `CI=true go test -count=1 ./internal/runner/`

---

### Task 7：task actor、调度与 fake Gateway

**Files:** Create `internal/task/actor.go`、`internal/task/scheduler.go`、`internal/task/gateway.go`（窄接口与 M1 的 fake 实现）；Modify `internal/task/task_test.go`。

**Interfaces：**

```go
type Access interface { Bind(ctx, attemptID string) (socketPath string, err error); Revoke(ctx, attemptID string) error } // M1 fake：不建 listener，只记录；M2 由 gateway/edge 实现
type Deps struct { Store Store; Admission *admission.Admission; Coordinator Coordinator; Runner AttemptRunner; Access Access; Clock func() time.Time; IDs func() string }
type Coordinator interface { CreateEnv(ctx, resource.EnvRequest) (provider.EnvInfo, error); StopEnv(ctx, envID string) (resource.StopResult, error) }
type AttemptRunner interface { Run(ctx, runner.Attempt, <-chan runner.Control) runner.Outcome }
type Actor struct{ /* 每 task 一个 */ }
func Spawn(ctx context.Context, taskID string, d Deps) *Actor
func (a *Actor) Notify()        // 控制变化等外部信号（非阻塞）
func (a *Actor) Done() <-chan struct{}
type Scheduler struct{ /* 管理 actor 集合；ListActiveTasks 启动；新任务时 Spawn */ }
```

**依赖方向：** `decide.go` 只使用本包类型；actor 文件可导入 `runner`、`resource`、`admission`、`provider` 以声明窄接口并把 `runner.Outcome` 转换为 `task.Outcome`（规则 2 的直接依赖边不含驱动、HTTP、进程与文件系统包）。

**规则：** actor 循环：（执行中修订）待定的重试类别不单独持久化：queued 任务的 `StatusReason` 即上一次 attempt 的 outcome class，actor 以纯函数据此推出下一次 `CreateAttempt` 的 `Retry`（`worker_oom_likely` → oom，§14.3 可重试的故障类 → fault，其余 → none），重启后同样成立；`LoadTask` 建立 `State` → 对每个事件调用 `Decide` → 依次执行 `Effects`（异步操作在独立 goroutine 中执行，结果以带 `attempt_id` 的事件回到收件箱）→ 持久化经 Store 用例；运行时间每 10 s `PersistRunTime`（§14.4）；`RevokeAccess` 在停止前执行（DB 撤销 + `Access.Revoke`）；Store 写失败按退避重试并保留待提交事实（§14.5 在线补偿）；actor 不关闭 socket、不写 cgroup、不重试数据库连接（代码组织 §5）。

**Tests**（fake 依赖：内存 Store 替身、fake Coordinator、fake AttemptRunner；不需要数据库）：正常路径的副作用顺序（申请槽位 → 创建 attempt → 创建环境 → 启动 → 裁决 → 停止 → 归还槽位）；过期结果不改变状态；控制变化在各阶段的处理（含 starting 阶段直接停止）；`stop_blocked` 阻止替代执行；Store 写失败后恢复由同一 actor 补提交。

**验证：** `go test -count=1 -race ./internal/task/`

---

### Task 8：控制面验收（进程型 fake provider + 真实 PostgreSQL）

**Files:** Create `tests/e2e/e2e_test.go`（本目录唯一测试文件；Plan 6 追加）、`tests/e2e/helpers_test.go` 不建（辅助函数放同一文件）。

**测试装置：** 真实 PostgreSQL（`AGENTBOX_TEST_DATABASE_URL`）+ Plan 4 Store + 临时 BlobStore + `provider/fake` 的进程型 Program（`python3 -m sim_worker`，`PYTHONPATH=worker`，`out_dir` 指向测试临时目录）+ Plan 5 的 coordinator、admission、runner、actor、scheduler。

**Tests（每项对应规格 §16.4）：**
- **首个切片（控制面层）**：提交 → 握手 → progress → checkpoint → 杀死 Worker 进程 → 新 attempt 以该 checkpoint 恢复 → result → 裁决 `succeeded` → 环境停止并清理 → 产物可按固定版本读取。
- **E1**：第 2 个 checkpoint 提交后杀死 Worker → 新 attempt 从 checkpoint 2 恢复；`fault_retries_used = 1`；产物完整。
- **E4**：取消与 result 竞争——确定性屏障各一例（取消先接受 / result 先裁决），加 1000 次随机交错（记录 seed，失败时输出 seed）：恰一个裁决；取消先被接受则永不 `succeeded`；无泄漏（环境全部清理、槽位全部归还）。
- **E7**：丢弃第一个 `checkpoint_result`（runner 的测试钩子）→ SDK 重试或查询得到相同结果；指针不回退。
- **E8**：Store 阻塞（持有 `tasks` 行锁 / 注入慢查询），server 不重启：阻塞期间执行树在规定时间内停止、不依赖数据库写入的清理完成、容量保守保留；解除后由现有 actor 与 coordinator 补提交，最终状态正确。
- **E10**：产物经符号链接、`../in`、FIFO 提交，或登记后改写 → 拒绝，或已保存副本保持权威。

**验证：** `CI=true go test -count=1 ./tests/e2e/`（本地 WSL 与 CI `correctness` 作业；需要 `python3`）。

---

## 联合验收

1. 全量 `go vet`、`GOOS=windows go build ./...`、`CI=true go test -race ./...` 通过；`tests/e2e` 连续三次通过。
2. CI 通过；Python 作业不受影响。
3. 范围：diff 只含本计划 Files；archtest 通过（新包的依赖方向：`task` 决策代码不依赖驱动与进程，`runner`/`resource` 只依赖 `provider`）。
4. 验收归属（控制面层）：E1、E4、E7、E8、E10；规格 §5.11 的 task 模式 fixtures 驱动 AttemptRunner。

## 依赖 Plan 1B 的部分（本计划不编写）

| 内容 | 原因 |
|---|---|
| 以 `provider/local` 的生产启动器运行首个切片与 E1、E4、E7、E8、E10 | 需要真实隔离环境（1B + Plan 2 生产启动器） |
| E2（Worker 主进程超出 `memory.max`）、E3（子进程 OOM） | 需要真实 cgroup 内存限制下运行 workload |

## 自查记录

- **规格覆盖**：§8.1 → Task 3、7；§8.2 → Task 3、6；§8.3 与 intent 规则 → Task 4；§5.5–§5.8 → Task 5、6；§14.3 → Task 6（`Classify`）；§14.4 → Task 7；§14.5 → Task 4、6、7；§3.2 过期结果规则 → Task 3、7；代码组织 §5 → Task 3、7；§8 首个切片 → Task 8。
- **单一权威实现**：控制转换表从 `persistence/postgres` 移入 `internal/task`（Task 3），Store 与 `Decide` 共用；裁决分类只在 `runner.Classify`。
- **接口新增**：Task 1 的事务用例与字段是对 Plan 4 窄接口的扩展，需随本批计划一并审阅。
- **占位符**：无。
