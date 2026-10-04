# M1 Plan 6：恢复与入口（不依赖 Plan 1B 的部分）实施计划

> **执行方式**：主 agent 逐任务实现与评审，不使用子 agent。每个任务 TDD；系统级端到端测试只在 `tests/e2e/`。

**Goal:** 实现启动恢复（`reconcile` 生成 `RecoveryPlan`、`recovery` 幂等执行）、REST + SSE API（`api/openapi.yaml` 为唯一契约）、CLI、server 装配与启动顺序（含 `ownership_lost` 与诊断模式）、完整的 archtest 规则、`verify-invariants`、故障注入钩子，并以进程型 fake provider 与真实 PostgreSQL 完成 E5、E6、E13、E14、E15、E16 与 I1、I2、I4–I8、I16 的控制面层验收。

**不在本计划内（等待 Plan 1B）**：以真实隔离环境运行的端到端切片、重启恢复与 M1 门槛（规格 §16.7）；见文末。

**规格依据：** §7.4（启动顺序、`ownership_lost`）、§14.1–§14.6、§15、§16.3、§16.4（E5、E6、E13–E16）；代码组织设计 §2–§6、§9.3；Provider 契约；Plan 4、Plan 5 的事务用例。

## Global Constraints

- **职责拆分**（代码组织 §6.1）：`reconcile` 只扫描事实并生成计划，不调用 actor、不执行；`recovery` 幂等执行计划、每步前重新检查条件、处理 `stop_blocked`，不创建 actor；启动顺序只由装配代码按 §14.1 调用。`reconcile` 不依赖 `task`、`session`、`recovery`（规则 4）。
- **单一权威**：恢复中丢失 attempt 的裁决由 `task.Decide` 计算（`recovery` 调用它），不另写裁决规则。
- **账本转换**（§14.1 第 4 步）属于 M2 的 Gateway journal；本计划在启动顺序中保留该步骤的位置但不实现（M1 无 reservation 与 call 记录）。
- **API**：`api/openapi.yaml` 为唯一契约，处理器与 CLI 按它实现；`request_id` 幂等沿用 Plan 4 的 `api_requests`；SSE 按 §15.2；访问控制按 §15.3（默认只绑定 `127.0.0.1`；其他地址要求 `<data>/api.token` 的 Bearer token，0600；`Origin`/`Host` 显式允许列表；token 不进入 URL、日志或构建产物）。
- **结构决定（需审阅）**：§14.1 的启动顺序与装配流程放在新包 `internal/app`（以接口接收依赖：Store、Provider、BlobStore 等）；`cmd/agentbox` 只负责参数、配置、构造具体实现并调用 `app.Run`（代码组织规则 5"具体实现只在 `cmd/agentbox` 装配"不变）。原因：E5/E13/E14 需要以真实进程运行 server 并 SIGKILL、重启，而 1B 之前只能用进程型 fake provider；`tests/e2e/agentbox-e2e`（只供测试的 main 包）以 fake provider 调用同一个 `app.Run`，生产二进制仍不能导入 fake。
- **故障注入**：`internal/faultinject` 提供命名钩子点；只有在启动参数显式开启时生效（生产默认关闭，archtest 检查 `cmd/agentbox` 只在测试构建标签下开启）。
- 测试位置：包内测试每包一个 `*_test.go`；系统级用例只在 `tests/e2e/e2e_test.go`（Plan 5 已建，向末尾追加）。工作区同 Plan 5。

## 任务与依赖

| 任务 | 内容 | 依赖 |
|---|---|---|
| 1 | 事务用例：恢复事实、撤销全部访问、运行时限补记、API 列表与 inspect | Plan 5 |
| 2 | `reconcile`：`RecoveryPlan` | 1 |
| 3 | `recovery`：执行器与就绪报告 | 1、2 |
| 4 | `api/openapi.yaml` 与 HTTP/SSE 处理器 | 1 |
| 5 | CLI | 4 |
| 6 | `internal/app` 启动顺序、`ownership_lost`、诊断模式、致命停止；`cmd/agentbox server` | 3、4 |
| 7 | archtest 全部规则与 `verify-invariants` | 6 |
| 8 | 故障注入钩子与系统级验收（E5、E6、E13–E16） | 5、6、7 |

---

### Task 1：事务用例

**Files:** Create `internal/recovery/store.go`；Modify `internal/api/store.go`、`internal/task/store.go`、`internal/persistence/postgres/`（新文件 `recovery.go`；`api.go`）、`postgres_test.go`（末尾追加一节）。

**Interfaces：**

```go
// recovery
type Facts struct { Tasks []TaskFact; Environments []EnvFact; PendingIntents []resource.Intent }
type TaskFact struct { TaskID, Status, Desired string; ControlVersion int64; CurrentAttempt *AttemptFact; NotBefore *time.Time; RunTimePersistedAt *time.Time }
type AttemptFact struct { AttemptID, Status, EnvID string; HasVerdict bool; ProposalKind string }
type EnvFact struct { EnvID, AttemptID string; StoppedAt *time.Time; CleanupState string }
type Store interface {
	LoadRecoveryFacts(ctx) (Facts, error)
	RevokeAllActive(ctx, reason string) (int, error)                                 // 单事务撤销全部 active 访问（§14.1 第 3 步）
	AccountUnrecordedRunTime(ctx, taskID, attemptID string, until time.Time) error  // §14.4：未记账区间按墙钟差全额计入
}
// api（新增）
ListTasks(ctx, after string, limit int) ([]TaskView, string, error) // keyset 分页
Inspect(ctx, taskID string) (Inspection, error)                     // attempts 与 outcome、退出与 OOM、环境与清理、checkpoint
```

**Tests**（真实 PostgreSQL）：`RevokeAllActive` 后所有 attempt 的 `CommitCheckpoint` 以 `stale_attempt` 拒绝；`LoadRecoveryFacts` 在包含各类状态的库上返回预期事实；`AccountUnrecordedRunTime` 幂等（同一 `until` 不重复累加）；`ListTasks` 分页稳定。

**验证：** `CI=true go test -count=1 ./internal/persistence/postgres/`（三次）。

---

### Task 2：`reconcile`

**Files:** Create `internal/reconcile/reconcile.go`、`reconcile_test.go`。

**Interfaces：**

```go
type Step struct { ID string; Kind StepKind; TaskID, AttemptID, EnvID string; Expect string }
// StepKind：StopEnv、MarkAttemptLost、CancelTask、PauseTask、KeepQueued、KeepTerminal、ReclaimOrphan、Quarantine、ResolveIntent
type RecoveryPlan struct { Steps []Step }
func Plan(f recovery.Facts, scan provider.ScanReport, installID string) RecoveryPlan // 纯函数；步骤 ID 稳定（由对象 ID 与种类派生）
```

**规则：** §14.2 每一行一类步骤（cancel 已接受 → 停止 → cancelled 不重试；pause → 停止并清理 → paused 保留恢复点；run 无未完成 attempt → 保持排队；run 有丢失 attempt → 确认停止后按故障恢复排队；已完成分类并安排重试 → 不再计数；裁决已提交 → 只处理残留）；§14.1 扫描表（属于本安装无记录 → 回收孤立资源；归属不明或冲突 → 隔离、报警、不销毁、计入占用）；未结束的 intent 按 §8.3 规则处理；计划不含进程句柄。

**Tests：** 表驱动覆盖 §14.2 每行与扫描表每行；同一事实两次生成的计划相同（稳定 ID）；无效组合（例如裁决已提交但当前 attempt 未结束）生成隔离与报警而不是猜测。

**验证：** `go test -count=1 ./internal/reconcile/`

---

### Task 3：`recovery`

**Files:** Create `internal/recovery/recovery.go`、`recovery_test.go`。

**Interfaces：**

```go
type Deps struct { Store Store; Tasks task.Store; Coordinator Coordinator; Clock func() time.Time }
type Report struct { Ready bool; Occupied []admission.Grant; StopBlocked []string; Quarantined []string }
func Execute(ctx context.Context, plan reconcile.RecoveryPlan, d Deps) (Report, error)
```

**规则：** 每步执行前重新读取条件，满足"预期状态"才执行，已完成则跳过（幂等，可在任意一步中断后重跑）；`StopEnv` 经 coordinator，`ErrStopUnconfirmed` → `stop_blocked`（占用、阻止替代执行）；`MarkAttemptLost` 以 `task.Decide`（`AttemptFinished{Class: lost_on_restart}`）计算裁决后 `FinalizeAttempt`；只有在实际创建替代 attempt 的事务中才递增 `fault_retries_used`（§14.2，由之后的 actor 完成）；全部前置条件满足才 `Ready`。

**Tests**（内存替身）：每类步骤一个用例；执行中途失败后重跑得到相同结果且不重复提交；`stop_blocked` 时报告占用且对应任务不排队；未确认停止前不提交 `lost`。

**验证：** `go test -count=1 ./internal/recovery/`

---

### Task 4：OpenAPI 与 HTTP/SSE

**Files:** Create `api/openapi.yaml`、`internal/api/http.go`、`internal/api/sse.go`、`internal/api/api_test.go`。

**契约（`api/openapi.yaml`，M1 范围）：** `GET/POST /tasks`、`GET /tasks/{id}`、`POST /tasks/{id}/cancel|pause|resume`、`GET /tasks/{id}/events`（SSE）、`GET /tasks/{id}/result`、`GET /tasks/{id}/artifacts/{artifact_id}/versions/{v}`（`ETag = sha256`）、`GET /tasks/{id}/inspect`、`GET /status`（`normal`/`diagnostic`/`ownership_lost`）。sessions 端点属于 M4，不写入本版契约。错误体统一 `{code, message}`；`409 request_conflict`、`400 invalid_cursor`、`409 task_ended`、`409 cancel_pending`、`409 not_paused`（来自 Plan 4 的 `RejectedError` 原因码）。

**规则：** `request_id` 必填；`body_hash` = 规范化 JSON 请求体的 SHA-256；SSE：`id = task_seq`、`Last-Event-ID` 续传、游标超出最新或非法 → `400 invalid_cursor`、每 15 s 注释行心跳、`task_terminal` 后关闭；诊断模式只开放 `status` 与 `inspect`；`ownership_lost` 模式拒绝写操作；访问控制按 §15.3。

**Tests：** 每个端点的成功与错误码一例；同 `request_id` 同内容返回首次结果、不同内容 409；SSE 续传无缺失无重复；`task_terminal` 后关闭；非法游标 400；非 loopback 无 token → 401、错误 `Origin`/`Host` → 403；日志与响应中不出现 token；处理器与 `openapi.yaml` 一致（测试读取 yaml 校验路径、方法与状态码的集合）。

**验证：** `CI=true go test -count=1 ./internal/api/`

---

### Task 5：CLI

**Files:** Create `internal/cli/cli.go`、`internal/cli/cli_test.go`；Modify `cmd/agentbox/main.go`（子命令分发）。

**命令：** `task submit|watch|cancel|pause|resume|result|inspect`、`status`（session 命令属于 M4）。

**规则：** `watch` 带退避的自动重连，按事件 ID 去重，收到 `task_terminal` 后**停止重连**（E16）；`result` 校验下载内容的 sha256；token 从 `<data>/api.token` 或环境变量读取，不出现在命令行参数与日志中。

**Tests**（对 `httptest` 服务器）：`watch` 在随机断开下无缺失无重复、`task_terminal` 后不再请求；`result` 哈希不符时报错；`submit` 重试使用同一 `request_id`。

**验证：** `go test -count=1 ./internal/cli/`

---

### Task 6：启动顺序与 server

**Files:** Create `internal/app/app.go`、`internal/app/app_test.go`、`cmd/agentbox/server.go`；Modify `cmd/agentbox/main.go`。

**Interfaces：**

```go
type Deps struct { /* Store（Plan 4/5/6 的各窄接口由同一实现提供）、Ownership、DataDir 文件、Provider、Blobs、Clock、FaultHooks */ }
type Config struct { Listen string; RecoveryDeadline time.Duration; Capacity admission.Capacity; /* 其余默认按 §19 */ }
func Run(ctx context.Context, cfg Config, d Deps) error // 返回即进程应退出
```

**规则（§14.1 顺序，逐步）：** 数据目录 → `flock` → advisory lock → 安装身份引导（Plan 4 Task 9）→ 迁移 → `RevokeAllActive` →（账本转换：M2）→ `List` + `Scan` + `LoadRecoveryFacts` → `reconcile.Plan` → `recovery.Execute` → 运行时限补记 → `admission.Rebuild(report.Occupied)` → 启动 cleanup loop、scheduler（actor）、API。超过恢复期限（60 s）未就绪 → API 以诊断模式启动（只读 `status`/`inspect`），禁止执行。`ownership_lost`（advisory lock 丢失）→ 不可逆：停止准入与新操作、取消在途、物理停止全部执行环境、有期限清理后退出（§7.4）。actor panic → 致命停止，同上（代码组织 §5）。`cmd/agentbox server` 构造 `provider/local`；生产启动器未就绪（1B 之前）时 server 启动失败并给出明确原因。

**Tests**（`app_test.go`，fake 依赖）：启动步骤顺序；恢复期限到期进入诊断模式；失锁后进入 `ownership_lost` 并停止全部环境后返回；actor panic 触发致命停止。

**验证：** `go test -count=1 ./internal/app/ ./cmd/agentbox/`

---

### Task 7：archtest 与 `verify-invariants`

**Files:** Modify `internal/archtest/archtest_test.go`；Create `internal/invariants/invariants.go`、`invariants_test.go`；Modify `internal/cli/cli.go`（`verify-invariants` 子命令）。

**archtest（代码组织 §3.2，按直接依赖边，规则 1、3 另查传递依赖）：** 规则 1–4 全部；`cmd/agentbox` 例外；`provider/fake` 与 `tests/e2e` 不被生产包导入；`faultinject` 只在测试构建标签下由 `cmd/agentbox` 开启。

**`verify-invariants`（§16.3，本计划范围 I1、I2、I4–I8、I16）：** I1：`cleanup_state = done` 的环境经 `Scan` 无任何实际资源；I2：每任务至多一个可能拥有执行树的 attempt，`stop_blocked` 时无替代执行；I4：`task_seq` 连续；I5：每个固定输出以正确哈希存在于 blobs（读取 BlobStore 复算）；I6：每个已提交 checkpoint 的 state 与 refs 存在、完整、在授权范围内，指针未回退；I7：每任务至多一个 active 访问；I8：每个隔离资源都已报警；I16：每个 `request_id` 恰对应一个资源。类别 A/B/Q 按 §16.3 标注，Q 类只在静止时检查。

**Tests：** 每条不变量各构造一个违反的库状态并断言被报告；正常库全部通过。

**验证：** `CI=true go test -count=1 ./internal/invariants/ ./internal/archtest/`

---

### Task 8：故障注入与系统级验收

**Files:** Create `internal/faultinject/faultinject.go`、`faultinject_test.go`、`tests/e2e/agentbox-e2e/main.go`（只供测试的 main：以进程型 fake provider 调用 `app.Run`）；Modify `tests/e2e/e2e_test.go`（末尾追加）。

**钩子点（E5）：** 至少覆盖：attempt 创建事务前后、环境创建前后、Worker 启动后、checkpoint 提交前后、裁决事务前后、停止前后、清理中；`AGENTBOX_FAULT=<点>:<次数>` 时在该点 SIGKILL 自身。

**Tests（每项对应 §16.4；每个用例结束时运行 `verify-invariants`）：**
- **E5**：在每个钩子点 SIGKILL server 并重启 → 按 §14.2 恢复；无重复执行（以 sim_worker 的执行计数与事件判断）；孤儿回收；无误隔离。
- **E6**：SSE 随机断开 → 事件无缺失无重复；任务不重新执行。
- **E13**：终止 advisory lock 连接、业务连接存活 → 检测到失锁后不启动新的业务操作、在途数据库操作被取消、执行环境停止、进程退出；记录检测延迟与在途事务最终结果；重启完整恢复。
- **E14**：任务中途 PostgreSQL 不可用（测试控制的 TCP 代理切断连接），server 不重启 → `store_unavailable`、物理停止；恢复连接后由现有所有者完成裁决与状态补齐。
- **E15**：`POST /tasks` 响应丢失（代理丢弃响应）后同 `request_id` 重试 → 只创建一个任务；再以不同内容重试 → `409 request_conflict`。
- **E16**：收到 `task_terminal` 后 CLI 停止重连；非法游标 → `400 invalid_cursor`。

**验证：** `CI=true go test -count=1 ./tests/e2e/`（本地 WSL 与 CI；需要 `python3` 与 PostgreSQL）。

---

## 联合验收

1. 全量 vet、Windows 构建、`CI=true go test -race ./...` 通过；`tests/e2e` 连续三次通过。
2. CI 通过（`tests/e2e` 在 `correctness` 作业中运行）。
3. 范围：diff 只含本计划 Files；archtest 全部规则通过；`verify-invariants` 在每个 e2e 用例结束时通过。
4. 验收归属（控制面层，进程型 fake provider）：E5、E6、E13、E14、E15、E16；I1、I2、I4–I8、I16。

## 依赖 Plan 1B 的部分（本计划不编写）

| 内容 | 原因 |
|---|---|
| 以真实隔离环境运行 `tests/e2e`（含首个切片、E1–E10、E5 的物理回收部分） | 需要 1B + Plan 2 生产启动器 |
| M1 门槛：spike 通过、E1–E10、E11a、E12–E16、E46 在目标环境、I1 与 I11 的物理检查 | §16.7 |
| `cmd/agentbox server` 的生产运行 | 生产启动器 |

## 自查记录

- **规格覆盖**：§14.1 → Task 6（顺序）、Task 1–3；§14.2 → Task 2、3；§14.4 → Task 1、6；§7.4 `ownership_lost` → Task 6；§15.1–§15.4 → Task 4、5；§16.3 → Task 7；§16.4 E5、E6、E13–E16 → Task 8。
- **结构决定**：`internal/app` 承载启动顺序（见 Global Constraints），需本批审阅。
- **M2 衔接**：账本转换步骤位置保留；`Access` 接口（Plan 5）由 M2 的 `gateway/edge` 实现。
- **占位符**：无。
