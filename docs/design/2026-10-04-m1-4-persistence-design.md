# Plan 4 设计：PostgreSQL 持久化与 BlobStore（M1 第 1 批）

> 依据：[v0.2 规格](2026-10-03-v0.2-first-release-design.md) §3、§6、§7、§14.1、§14.5；[代码组织设计](2026-10-03-code-organization.md) §2–§4、§9；[计划索引](https://github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/blob/m4-gate/docs/plans/2026-10-03-m1-index.md) 中 Plan 4 一行。
> 状态：实现前设计，已经两轮评审，结论已并入。本文档不代表存储代码已经开始实现。

## 1. 包、依赖方向与职责

### 1.1 依赖规则（由 archtest 固定）

- 消费者包（`internal/api`、`internal/task`、`internal/runner`、`internal/resource`）**不依赖** `internal/persistence/postgres`，也不依赖任何数据库驱动（pgx）。纯决策代码（`Decide` 等）不做 I/O。
- 消费者包可以依赖其他内部契约（`protocol`、`blob`、`persistence` 的错误定义等）。"只依赖标准库"仅指 Plan 4 新建的接口文件本身，不是对整个消费者包的永久限制。
- `internal/persistence/postgres` 导入消费者包并实现其窄接口，反之不行。不引入通用 repository 层或共享的大 Store。

### 1.2 所有权分成三处

| 包 | 职责 |
|---|---|
| `internal/datadir` | `<data>/agentbox.lock` 的 `flock`（持有至进程退出）；`<data>/install_id` 的读取与持久化写入：临时文件 → 写入 → fsync 文件 → rename → **fsync 父目录**。rename 保证原子替换，父目录 fsync 保证断电后仍在 |
| `internal/persistence/postgres` | 专用连接上的会话级 advisory lock；`installation` 表；失锁检测与 `ownership_lost` 信号；迁移；全部 SQL 与事务用例 |
| `internal/ownership` | 规格 §7.4 安装身份引导的决策表（纯函数），以及经两个窄接口（`InstallStore` 由 postgres 实现，`IDFile` 由 datadir 实现）编排引导步骤；不直接做 I/O |

启动装配（`cmd/agentbox`，后续计划）按 §7.4 的顺序调用：datadir 取 flock → postgres 取 advisory lock → ownership 引导与校验 → 迁移 → 恢复。

### 1.3 数据模型与接口分开决定

**本次迁移建立 M1 需要的全部表**（含恢复所需字段）：

`schema_migrations`、`installation`、`api_requests`、`tasks`、`task_control`、`task_progress`、`task_event_seq`、`events`、`attempts`、`attempt_access`、`environments`（含 `stopped_at`、`cleanup_state`、`cleanup_tries`、`next_retry_at`、`cleanup_error`）、`uid_ranges`、`resource_intents`、`quarantined_resources`、`checkpoints`、`blobs`、`scope_blobs`、`blob_provenance`、`artifact_heads`、`artifacts`。

**延后的表**（随首次使用它们的里程碑建立）：`sessions`、`session_control`、`session_progress`、`incarnations`、`subruns`、`subrun_budgets`（M4）；`budgets`、`exec_quotas`、`reservations`、`calls`、`call_tries`（M2）。`tasks.session_id` 暂为可空列，外键在 M4 加上。

相对规格 §6 的补充列（为幂等内容校验所需）：`events.worker_seq`、`events.content_hash`；`attempts.verdict_hash`、`attempts.terminal_proposal_hash`；`uid_ranges.allocation_id`；`resource_intents` 的 `intent_id` 为主键。

### 1.4 事务用例接口分批，验收归属明确

| 批次 | 接口 | 验收归属 |
|---|---|---|
| Plan 4 第 1 批 | `api.Store`、`task.Store`、`runner.Store` | Plan 4：E11a、E12 存储部分 |
| Plan 4 第 2 批 | `resource.Store`：`RecordIntent`、`ResolveIntent`、`MarkStopped`、`UpdateCleanup`、`AssignUIDRange`、`ReleaseUIDRange` | Plan 4 |
| Plan 4 | `ownership`（安装引导、advisory lock、失锁信号）、`datadir`、`blob` | Plan 4：E46、E13 存储部分 |
| 交给 Plan 6 | `recovery.Store`：`LoadRecoveryFacts`、`RevokeAllActive`、`ApplyRecoveryStep`（形状依赖 `reconcile` 的 `RecoveryPlan`） | Plan 6：E5、E13 恢复部分。Plan 4 只保证 schema 具备所需字段 |

## 2. 事务辅助、错误分类与未知提交结果

### 2.1 `tx(ctx, name, fn)`（postgres 包内部）

- 整体 deadline（默认 2 s）覆盖连接池获取、事务与全部重跑；事务内 `SET LOCAL lock_timeout = '1s'`、`statement_timeout = '2s'`，`READ COMMITTED`。
- 任何错误回滚。`40001`/`40P01` 只在**确认中止**时重跑整个 `fn`（语句返回该码，或 `COMMIT` 返回该码），有界、带抖动退避。
- **分类原则：只有确定未提交时才报告"未提交"；`COMMIT` 可能已到达服务端时一律视为未知。**
  - 连接类错误：网络错误、连接建立失败，以及服务端的 `08*`、`57P01`/`57P02`/`57P03`（会话被终止、崩溃恢复、暂不接受连接）与 `53300`（连接数已满）。语句阶段遇到时在 deadline 内重跑，最终为 `ErrUnavailable`。
  - `57014`（语句超时）且操作 context 未结束：视为超时，在 deadline 内重跑，最终为 `ErrUnavailable`（规格 §7.3：timeout 计入阈值）。
  - `COMMIT` 的结果：`ERROR` 级 PgError（非连接类）→ 服务端拒绝，确认未提交；`pgconn.SafeToRetry`（COMMIT 未发出）→ 确认未提交，普通重跑；`pgx.ErrTxCommitRollback`（事务体吞掉了语句错误）→ 编程错误，不重跑、不计入阈值；其余（`FATAL`/`PANIC`、连接类、无 SQLSTATE）→ 结果未知，以同一身份在剩余时间内重跑幂等事务体，用尽时返回 `persistence.ErrCommitUnknown`。
  - **调用方取消**（调用方自己的 context 结束）不是存储故障：未出现未知结果时返回包装调用方 `ctx.Err()` 的错误，不计入阈值；出现过未知结果时仍返回 `ErrCommitUnknown`。操作自身的整体 deadline 到期仍按 `ErrUnavailable`/`ErrContention` 归类。失去所有权优先于其他归类。
  - 返回的错误以 `%w` 同时保留类别哨兵与最后一次的底层错误；调用方先按 `persistence` 哨兵判断。

### 2.2 错误契约（`internal/persistence`，无驱动依赖）

| 错误 | 含义 | 计入 Store 故障阈值（§14.5） |
|---|---|---|
| `ErrContention` | `55P03` 等锁超时，或可重试中止在重跑后仍失败 | 否 |
| `ErrUnavailable` | 连接失败或 deadline 到期，且已确认未提交 | 是 |
| `ErrCommitUnknown` | 提交结果未知；携带操作身份 | 是 |
| `ErrConflict` | 同一身份已存在且内容不同，或 CAS 前置条件不成立 | 否 |
| `ErrRejected`（`*RejectedError{Code}`） | 事务内读取的最新事实不满足用例前置条件（§2.6）；`Code` 为 `task_ended`、`cancel_pending`、`not_paused`、`not_runnable`、`previous_not_stopped`、`stale_attempt`、`control_changed`、`ref_not_authorized` | 否 |

### 2.3 未知提交结果：查不到不等于没提交

在 `READ COMMITTED` 下，新连接上的查询可能发生在原事务提交完成之前，因此"查不到"仍然是**未知**，不能据此认定第一次失败，更不能据此重复启动外部执行。规则：

1. 每个事务用例的事务体本身**幂等**：在事务内以唯一约束与行锁按操作身份仲裁——身份已存在且内容相同则返回原结果，内容不同则 `ErrConflict`，不存在才写入。对仍在进行中的第一次事务，唯一约束与行锁使重跑**等待**其结束后再仲裁。
2. 遇到 `ErrCommitUnknown` 时，用例在**同一个整体 deadline 的剩余时间内**以同一身份重跑该幂等事务；不另开新的超时窗口。重跑可能因等待锁而得到 `ErrContention`，同样在剩余时间内继续。
3. deadline 用完仍无确定结果，返回携带操作身份的 `ErrCommitUnknown`。调用方保留该操作为待定，之后可用查询方法核对；查询"查到相同身份、相同内容"即已提交，"查到相同身份、不同内容"即冲突，"查不到"仍是未知，只能再以同一身份重试幂等用例。
4. 调用方在用例返回成功之前，不得启动任何依赖该写入的外部动作（例如创建环境、启动 Worker）。

### 2.4 各用例的身份、查询与内容比较

| 用例 | 操作身份 | 内容比较 | 查询方法（各消费者自己的窄接口） |
|---|---|---|---|
| `api.CreateTask` / `api.AcceptControl` | `request_id` | `body_hash`；同 ID 不同哈希 → `ErrConflict`（`request_conflict`） | `api.GetRequest(request_id)` |
| `task.CreateAttempt` | 事务前生成的 `attempt_id`；另有 UNIQUE(`task_id`, `attempt_no`) | `task_id`、`attempt_no`、`env_id` 全部一致 | `task.GetAttempt(attempt_id)` |
| `task.ApplyControl` | (`task_id`, 目标 `control_version`) | `applied_control_version` 的 CAS：已 ≥ 目标即已应用 | `task.GetControlState(task_id)` |
| `task.FinalizeAttempt` | `attempt_id` 的判决 | **完整判决内容**的哈希（状态、`outcome_class`、退出码与信号、OOM 诊断、`platform_killed`、任务终态、结果、终态事件）存于 `attempts.verdict_hash`；完全一致才返回原结果，仅"都是 failed"不算一致。另含判决依据的 `control_version`；取消与正常完成的竞争按 §2.6 在锁内仲裁 | `task.GetAttempt(attempt_id)` 返回 `verdict_hash` |
| `runner.CommitCheckpoint` | (`scope_kind`, `scope_id`, `checkpoint_id`) | `content_hash` | `runner.QueryCheckpoint(scope, checkpoint_id)` |
| `runner.RegisterArtifact` | (`task_id`, `artifact_id`, `sha256`) | 同一身份即同一内容（sha256 已是内容） | `runner.GetArtifact(task_id, artifact_id, sha256)` |
| `runner.AppendWorkerEvents` | (`attempt_id`, `worker_seq`)，唯一 | 每条事件的 `content_hash`（类型与原始载荷的哈希）。规则见 §3.1 | `runner.WorkerEventWatermark(attempt_id)`：连续水位，仅用于优化，不替代逐条内容校验 |
| `runner.RecordTerminalProposal` | `attempt_id` | `terminal_proposal_hash`：重复写入相同内容返回原结果，不同内容 `ErrConflict`，**不得**以 `WHERE … IS NULL` 静默忽略 | `runner.GetTerminalProposal(attempt_id)`（runner 自己的窄接口，不引入 runner → task 依赖） |
| `resource.RecordIntent` | 事务前生成的 `intent_id`；另有 UNIQUE(`kind`, `name`) | `env_id`、`kind`、`name` 一致；(`kind`,`name`) 已被其他 `intent_id` 占用 → `ErrConflict`。同一意图重试不会出现第二份 | `resource.GetIntent(intent_id)` |
| `resource.ResolveIntent` | (`intent_id`, 目标状态) | 状态只能沿 `pending→acquired→released`、`pending→failed` 前进；已在目标状态 → 返回原结果；倒退或跳跃 → `ErrConflict` | `resource.GetIntent(intent_id)` |
| `resource.AssignUIDRange` | (`env_id`, 事务前生成的 `allocation_id`) | 结果丢失后重试返回**原分配**，不再占第二段：事务内先查 `owner_id = env_id AND allocation_id = …` | `resource.GetUIDRange(env_id)` |
| `resource.ReleaseUIDRange` | (`uid_range_id`, `allocation_id`) | 只有当前分配代次匹配才释放；已释放且未复用 → 返回原结果；已被后来的分配复用 → `ErrConflict`，不得释放 | `resource.GetUIDRange` |
| `resource.MarkStopped` | `env_id` | 只在 `stopped_at IS NULL` 时写入；已写入 → 返回原值，不倒退 | `resource.GetEnvironment(env_id)` |
| `resource.UpdateCleanup` | (`env_id`, 期望的 `cleanup_tries`) | CAS：只有 `cleanup_tries = 期望值` 才把次数加一并写入新状态；已是"期望值 + 1 且状态相同" → 返回原结果（同一次尝试不重复累计）；`cleanup_state` 不倒退（`done` 不回到 `pending`） | `resource.GetEnvironment(env_id)` |

### 2.5 测试（全部在真实 PostgreSQL 上）

- **提交后丢回复（E11a）**：测试用连接包装让 `COMMIT` 真正执行、客户端收到连接错误；每个用例必须得到原结果，且无重复行与重复事件。
- **第一次提交尚未完成**：第一次事务在提交前被测试钩子挂起 → 新连接查询为空 → 以同一身份重跑（因锁等待）→ 放行第一次提交 → 重跑得到原结果，表中只有一份。
- **死锁（E12 存储部分）**：两事务经屏障违反锁顺序形成环，断言中止被重跑、两者最终都提交。
- **争用**：持有行锁使用例得到 `ErrContention`；deadline 用尽返回带身份的 `ErrCommitUnknown`。
- 各用例的"同身份不同内容"冲突用例；事件"同序号不同内容"与"部分重叠批次"用例。
- **前置条件（§2.6）**：取消后创建 attempt、旧环境未停止时创建、创建后重复请求返回原结果；取消先提交（过期判决被拒绝、按最新控制重算）、判决先提交（随后的取消被拒绝）、旧 attempt 的迟到判决；旧 attempt、其他任务的 blob、不存在的 blob 提交 checkpoint；每个拒绝都断言任务的状态、指针、事件与产物均未改变。

### 2.6 前置条件在同一事务内检查

身份比较只回答"这次写入是否已经发生过"；写入是否**允许**发生，取决于持有锁后读到的最新事实。规格 §5.5 与 §8.1 的前置条件因此都在同一事务内检查，不留给调用方（actor、runner）在事务外判断——那样检查与提交之间仍有竞争窗口。顺序固定为：**先按身份查询（已存在则比较内容并返回原结果），再检查前置条件，最后写入**。这样提交结果未知后的重跑仍得到原结果，即使前置条件此时已不再成立（例如 attempt 创建后任务已是 `running`）。

| 用例 | 持锁后检查 | 不满足 |
|---|---|---|
| `api.AcceptControl` | 任务非终态；已接受的 cancel 不被 pause/resume 覆盖；resume 要求 `paused` | `task_ended`、`cancel_pending`、`not_paused`；请求记录随事务回滚，不留下 |
| `task.CreateAttempt` | 任务 `queued`；`desired = run`；该任务所有旧环境 `stopped_at` 已记录；然后 CAS `queued → running` | `not_runnable`、`previous_not_stopped` |
| `task.FinalizeAttempt` | 提交者是 `current_attempt_id`；`Verdict.ControlVersion` 等于当前 `control_version`；任务终态与 `desired` 相符（cancel → `cancelled`，pause → `paused`，run → `succeeded`/`failed`/`queued`） | `stale_attempt`；`control_changed`（调用方按最新控制重算后再提交）；与 desired 不符为调用方编程错误 |
| `runner.CommitCheckpoint` | 提交者是当前 attempt 且 `attempt_access` 为 `active`；`refs[]` 与 `state_ref` 的每个 sha256 都在 `scope_blobs` 中授权到当前 attempt 或当前 task | `stale_attempt`、`ref_not_authorized`；指针与事件不改变 |
| `runner.RegisterArtifact` | 与 checkpoint 相同的 fencing | `stale_attempt` |

锁顺序随之为：`tasks → task_control → task_progress → task_event_seq → attempts → attempt_access → environments`（规格 §7.1）。`AppendWorkerEvents` 与 `RecordTerminalProposal` 只写该 attempt 自己的行，不影响任务级指针与输出，本计划不加 fencing；它们迟到的写入由 runner 停止读取与访问撤销约束（Plan 5）。

## 3. 事件、BlobStore、安装引导与失锁

### 3.1 事件追加

- 序号来自行锁：`UPDATE task_event_seq SET next = next + 1 … RETURNING`，不用 `MAX()+1`。
- host 事件以 `event_key` 去重：`INSERT … ON CONFLICT DO NOTHING` 后用独立语句读取既有记录并比较 `content_hash`；相同返回原 `task_seq`，不同 `ErrConflict`。终态与其 host 事件在同一事务提交。
- Worker 事件批次（`AppendWorkerEvents`）：
  - (`attempt_id`, `worker_seq`) 唯一；每条保存 `content_hash`。
  - 一个批次在同一事务中原子提交，批内序号必须连续。
  - 与已存在部分重叠的事件逐条校验内容；同序号不同内容 → `ErrConflict`。
  - 只追加重叠之后的连续新后缀；批次首个新序号与已存在最大序号之间有缺口 → 拒绝（`ErrConflict`）。

### 3.2 BlobStore（`internal/blob`）

- 接口：`Put(ctx, r io.Reader) (Ref{SHA256, Size}, error)`、`Open(sha256) (io.ReadCloser, error)`、`Stat(sha256) (Ref, error)`。
- 本地实现：写入 `<data>/blobs/tmp` 下的临时文件并边写边哈希 → fsync 文件 → rename 到 `<data>/blobs/sha256/<前两位>/<其余>` → fsync 父目录。目标已存在时校验大小后视为成功（内容寻址天然幂等）。
- 不做垃圾回收。数据库中的 `blobs`、`scope_blobs`、`blob_provenance` 由事务用例写入；blob 文件在引用它的事务提交**之前**已持久化。

### 3.3 安装身份引导（E46）

按规格 §7.4 的决策表实现为纯函数 `ownership.Decide(DBState, FileState) Action`，覆盖全部九种情况（空库首启、有 schema 无记录、空库但有身份文件、未知 schema、`pending` 的三种情况、`complete` 的三种情况）。首启时初始迁移与 `installation(新 ID, pending)` 在同一事务提交；之后写身份文件（含父目录 fsync），再置 `complete`。E46 的每个中断点各有一个测试：提交前中断、提交后写文件前中断、写文件后置 `complete` 前中断，重启后都能继续或明确拒绝。

### 3.4 Advisory lock 与失锁检测（E13 存储部分）

- 专用连接（不进连接池）上 `pg_try_advisory_lock(固定键)`；取不到 → 另一实例在运行，拒绝启动。
- 失锁检测：在专用连接上周期性（默认 1 s，超时 1 s）执行一次查询确认连接与锁仍在；失败或连接断开 → 进入不可逆的 `ownership_lost`：关闭信号通道，业务操作所用的 context 随之取消，此后用例一律返回 `ErrOwnershipLost`（不计入 Store 故障阈值，由启动装配层负责停止执行树并退出）。
- 测试：由另一连接 `pg_terminate_backend` 锁连接，断言在检测周期内发出信号、在途业务操作被取消、新操作被拒绝；记录检测延迟。

### 3.5 迁移

- SQL 文件以 `embed.FS` 内嵌，按版本号顺序执行；`schema_migrations(version, applied_at)`；每个迁移一个事务，独立超时 5 min。
- 初始迁移与 `installation` 插入在同一事务中（PostgreSQL 事务性 DDL），使"有 schema 无 installation 记录"只能来自外部改动。

## 4. 测试与环境

- 测试布局遵循代码组织设计 §9.3：每个 Go 包一个 `*_test.go`。
- 数据库测试读取 `AGENTBOX_TEST_DATABASE_URL`，每个测试建立独立的临时数据库并在结束时删除。本地未设置时跳过并明确提示；CI 中（`CI=true`）未设置则失败，不允许静默跳过。
- 本地：Docker Desktop 上的 `postgres:16-alpine`，WSL 经 `127.0.0.1:5432` 访问；Go 依赖（pgx v5.7.6 及其依赖；v5.8 起要求 Go ≥ 1.24，与项目的 Go 1.23 下限不符）经本地文件代理离线获取。
- CI：Go 作业增加 `postgres:16-alpine` 服务容器并设置上述环境变量。
- 不以 fake 代替数据库验收；fake 只用于消费者的决策逻辑测试（Plan 5 起）。
