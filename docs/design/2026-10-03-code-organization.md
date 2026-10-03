# go-agentbox 代码组织设计

> 状态：评审通过 · 2026-10-03
> 适用于：[`2026-10-03-v0.2-first-release-design.md`](2026-10-03-v0.2-first-release-design.md)
> 本文不增加任何业务能力，只把规格中的架构约束落到代码边界：模块职责、允许的依赖、关键事务用例、资源与状态所有权。

---

## 1. 原则

1. **边界比抽象层重要。** 衡量标准是"变化原因是否独立、依赖能否替换测试"，不是 interface 的数量。
2. **决策与副作用分开，但不造工作流引擎。** 只对真正复杂、需要穷举测试的状态机抽出纯决策函数；其余直接调用清晰的方法。
3. **解耦不能破坏事务。** 需要原子完成的检查、预留、记录，由一个有明确业务目的的事务用例完成，不拆成各自提交的小服务。
4. **每个状态与资源只有一个决策者。**
5. **按需建包。** 包在第一个需要它的实现任务中创建，不预先建空目录。

---

## 2. 模块与职责

### 2.1 Go

| 包 | 职责 | 不做 |
|---|---|---|
| `cmd/agentbox` | 参数、配置、依赖装配、启动顺序；`init`、`exec-stage2` 子命令入口 | 具体业务或恢复逻辑 |
| `internal/api` | HTTP、SSE、认证、Origin/Host 校验、请求↔用例转换、`request_id` 处理入口 | 状态决策、SQL |
| `internal/protocol` | Worker 消息类型、校验、编解码（Go 侧与跨语言 fixtures 对应） | 依赖标准库以外的任何包 |
| `internal/task` | task/attempt 状态机（`Decide`）与 task actor | SQL、进程、文件、socket |
| `internal/session` | session/incarnation 状态机与 session actor | 同上 |
| `internal/admission` | run slots、环境内存、exec slots；排队与授予 | 单任务决策 |
| `internal/runner` | AttemptRunner：读取执行流、按序处理事件、完成屏障、产物保存调用、checkpoint 提交调用 | 生命周期决策 |
| `internal/resource` | 资源 coordinator（每环境串行化物理操作）、cleanup loop、intent 记录 | 业务状态决策 |
| `internal/reconcile` | 扫描事实、生成 `RecoveryPlan` | 调用 actor；执行计划 |
| `internal/recovery` | recovery executor：幂等执行 `RecoveryPlan`，处理 `stop_blocked`，满足前置条件后报告就绪 | 创建 actor |
| `internal/gateway/edge` | Unix socket HTTP 服务、连接↔attempt 绑定、撤销时关闭连接 | 预算、journal |
| `internal/gateway/call` | 调用协调：解析流程、预留、执行、结算、重放、exec 调度 | HTTP 细节、供应商协议 |
| `internal/gateway/upstream` | 模型、搜索、抓取 adapter；验证 dialer（SSRF） | 记账 |
| `internal/gateway/cache` | 缓存键、新鲜度、完整性、Redis 适配、熔断 | 记账、授权 |
| `internal/persistence/postgres` | 全部 SQL、事务用例、锁顺序、迁移、advisory lock | 业务决策 |
| `internal/blob` | 不可变内容寻址存储接口与本地实现 | 授权 |
| `internal/provider/local` | 环境生命周期：Create、StartExec、Stop、Destroy、List、Scan；组合 cgroup/rootfs/sandbox | 任何控制面概念 |
| `internal/sandbox` | 沙箱内 init、启动序列、控制通道、收割、stage-2 降权（由 `internal/runtime` 改名） | 控制面概念 |
| `internal/cgroup`、`internal/rootfs`、`internal/hostcheck` | 底层原语与宿主自检 | 同上 |

`provider/local` 之上的控制面业务包不直接导入 `cgroup`、`rootfs`、`sandbox`。**例外**：`cmd/agentbox` 可为装配与 re-exec 入口（`init`、`exec-stage2`）导入这些包的入口函数。

### 2.2 Python（最低版本 3.11）

`asyncio.TaskGroup` 要求 Python ≥ 3.11。旧 DeepResearch 的 `requires-python >= 3.10`、镜像与 CI 同步提升到 3.11。

| 包 | 职责 | 不做 |
|---|---|---|
| `agentbox_worker` | 协议读写、checkpoint 提交与查询、控制信号、Gateway 客户端、结构化并发（每 task / sub-run 一个 TaskGroup 作用域）、单一状态所有者 | 研究策略、提示词、planner |
| `agentbox_worker.steps` | 可选的步骤式便利封装 | 被强制使用 |
| `sim_worker` | 按配置睡眠、分配内存、输出事件、主动失败；协议与故障测试负载 | 业务逻辑 |
| `deepresearch` | 研究计划与状态、检索与阅读工具调用、证据组织、报告生成、sub-run 调度 | 访问数据库或 Redis；创建宿主进程 |

**状态即数据**：checkpoint 状态是带 `schema_version` 的 dataclass / pydantic 模型，配显式 `migrate()`；不使用 pickle，不保存客户端、协程、连接或任意对象。运行时依赖（Gateway 客户端、时钟、取消作用域）在恢复时另行注入。

---

## 3. 允许的依赖

### 3.1 规则

1. `sandbox`、`cgroup`、`rootfs`、`provider/local` 不依赖 `task`、`session`、`gateway/*`、`persistence/*`、`runner`、`api`。
2. `task`、`session` 的决策代码（`Decide` 及其类型）不依赖 HTTP、Redis、PostgreSQL 驱动、进程与文件系统。actor 通过本包定义的窄接口使用外部能力。
3. `protocol` 只依赖标准库。
4. `reconcile` 不依赖 `task`、`session`、`recovery`。
5. 具体实现只在 `cmd/agentbox` 装配；任何包不从全局变量获取依赖。
6. `deepresearch` 不导入数据库或 Redis 客户端，不直接调用 `subprocess`、`os.fork`、`os.system` 等；`agentbox_worker` 不导入 `deepresearch`。

### 3.2 自动检查

- **Go**：`internal/archtest` 中的测试以 `GOOS=linux` 运行 `go list -f '{{.ImportPath}}: {{join .Imports " "}}'`，按**直接依赖边**断言规则 1–4 的包依赖部分（含第 2.1 节 `cmd` 例外）；对规则 1、3 另用 `-deps` 检查**传递依赖**。Linux 专有包在目标构建条件下检查。
- **import 图检查不到的内容**：规则 2 中同一包内纯 `Decide` 与 actor 副作用代码的区分、规则 5 的"不从全局变量获取依赖"，由代码评审保证；如反复出现问题再增加 AST 检查，不为此另拆包。
- **Python**：`import-linter` 检查模块依赖契约；ruff 的 `banned-api`（TID251）禁止 `deepresearch` 调用 `subprocess`、`os.fork`、`os.system`、`os.exec*` 等函数级 API。
- 两者均在 CI 运行。架构检查约束代码组织，不是安全隔离机制。

---

## 4. 持久化接口：按消费者定义的事务用例

每个消费者包声明自己需要的窄接口；`persistence/postgres` 实现全部。每个方法是一个完整的事务用例：可以更新多张表，但有单一业务目的。

| 消费者 | 事务用例（示例） |
|---|---|
| `api` | `CreateTask`、`CreateSession`、`AcceptControl`（含 `request_id` 幂等）、列表与查询、读取事件 |
| `task` | `CreateAttempt`、`ApplyControl`（`applied_control_version`）、`FinalizeAttempt`（裁决 + 终态事件 + session 指针推进）、`ScheduleRetry`、`PersistRunTime` |
| `session` | `TransitionSession`、`CreateIncarnation`、`EndIncarnation`、`CloseSession` |
| `runner` | `AppendWorkerEvents`、`RegisterArtifact`、`CommitCheckpoint`、`QueryCheckpoint`、`RecordSubrunStart`、`RecordTerminalProposal` |
| `gateway/call` | `CheckAccess`、`BeginCall`、`CompleteFromCache`、`CompleteCall`、`SettleTry`、`MarkTryUnknown`、`BeginExec`、`CompleteExec` |
| `resource` | `RecordIntent`、`ResolveIntent`、`MarkStopped`、`UpdateCleanup`、`AssignUIDRange`、`ReleaseUIDRange` |
| `reconcile` / `recovery` | `LoadRecoveryFacts`、`RevokeAllActive`、`ConvertLedger`、`ApplyRecoveryStep` |

### 4.1 事务辅助函数 `tx(ctx, fn)`

**负责**：整体 context deadline（含连接池获取与重试）、`SET LOCAL lock_timeout/statement_timeout`、任何错误回滚、对**已确认中止**的 `40001`/`40P01` 有界重跑整个 `fn`、错误分类（`lock_contention` 与 `connection_error`/`timeout`）。

**不负责**：
- COMMIT 结果未知的处理——由各用例按规格 7.3 的操作身份查询；
- 锁顺序——由各用例按规格 7.1 编写，并由测试验证。

### 4.2 测试分工

- actor 与决策逻辑：内存 fake 实现窄接口。
- 事务语义（锁、冲突、提交结果未知、并发提交同 ID）：**只在真实 PostgreSQL 上测试**，不用 fake 模拟事务。
- 使用 pgx 与手写 SQL；暂不引入 ORM 或代码生成。

---

## 5. 状态决策与副作用

- 只有 task、attempt、session、sub-run 四个状态机使用纯决策函数：

  ```go
  func Decide(s State, e Event) (Decision, error)
  ```

  `Decision` 包含新状态与一个**封闭的**副作用类型集合（例如 `StartEnvironment`、`RevokeAccess`、`StopEnvironment`、`SendControl`、`ReleaseSlot`）。actor 用 `switch` 执行；不设 bus、registry 或通用 handler。决策函数用表驱动测试穷举状态×事件。
- **actor** 是一个 select 循环：处理收件箱与自己发起的异步操作结果；只作决策、提交状态、安排操作。它不关闭 socket、不写 cgroup 文件、不重试数据库连接。保持精简是目标，不设行数硬指标；不为缩短而把同一流程拆到难以追踪的位置。
- **actor panic**：首版不在进程内重建 actor。任一 actor panic → 服务进入致命停止：停止准入与 Gateway 新操作、取消在途操作、物理停止全部执行环境、退出；重启时走完整启动恢复。

---

## 6. 状态与资源所有权

| 对象 | 决策者 | 执行者 |
|---|---|---|
| task / attempt 生命周期、task 裁决 | task actor | — |
| session、incarnation、"session 可接受下一任务" | session actor | — |
| 创建 `task` 环境 | task actor | resource coordinator |
| 创建 `session` 环境 | session actor | resource coordinator |
| 创建 `exec` 环境 | gateway/call | resource coordinator |
| 撤销访问 | task actor（attempt）；session actor（incarnation 入口） | gateway/edge |
| 容量槽位 | admission | — |
| journal、两层预算、exec 配额 | gateway/call | — |
| `stopped_at`、intent、UID 范围分配与归还 | — | resource coordinator（只报告事实，不决定业务状态） |
| 清理列 | cleanup loop | resource coordinator |
| 启动恢复 | reconcile（生成计划）→ recovery（执行） | resource coordinator、persistence |

### 6.1 启动恢复的流程

下图只表示恢复**职责拆分**；完整启动顺序遵循 v0.2 规格 §14.1。前置步骤的执行组件：

| §14.1 步骤 | 执行组件 |
|---|---|
| 数据目录、`flock`、advisory lock、`install_id` 校验 | `persistence/postgres`（advisory lock、installation）与 `cmd`（只负责按顺序调用） |
| 迁移 | `persistence/postgres` |
| 撤销全部 active 访问、账本转换 | `recovery`（经 `RevokeAllActive`、`ConvertLedger` 事务用例） |
| 扫描、生成计划 | `reconcile` |
| 停止、提交 `lost`/`evicted`、运行时限补记 | `recovery` |
| 重建 admission | `admission`（读取 recovery 报告的实际占用） |
| 启动 cleanup loop、actor、API | `cmd`（只负责顺序） |

```
reconcile：扫描事实 → 生成 RecoveryPlan（稳定 ID、预期状态、操作类型；不含进程句柄）
recovery： 执行计划（停止、核对、数据库提交），每步执行前重新检查条件，幂等，处理 stop_blocked
           → 恢复前置条件满足后报告就绪
cmd：      装配并按顺序启动 cleanup loop、actor、API
```

生成计划不代表恢复完成。

---

## 7. 质量验收问题

每个里程碑结束时逐项回答，作为代码评审标准：

1. 修改 DeepResearch 的研究策略，是否无需修改 Go 代码？
2. 增加一个遵守协议的 Worker，Go 是否无需添加业务名称分支？
3. 更换搜索供应商，是否只影响 adapter、配置与相应测试？
4. 不启动 Linux 沙箱，能否测试任务状态转换与恢复决策？
5. 不调用真实模型，能否验证 Gateway 记账与重放？
6. 发生一次失败，能否快速找到负责决策、持久化与物理操作的代码？
7. 一次改动是否经常需要跨很多包同步修改？若是，边界可能划错了。

---

## 8. 首个切片（首次集成验收）

```
CLI submit → admission → 创建 task 环境 → StartExec → 握手 → progress
→ checkpoint 提交（PostgreSQL）→ 杀死 Worker → 新 attempt 恢复 → result → 裁决 → 回收
```

所需的包：`sandbox`、`provider/local`、`protocol`、`runner`、`resource`、`task`、`admission`、`persistence/postgres`、`blob`、`reconcile`、`recovery`、`api`、`cmd`、`archtest`；Python 侧 `sim_worker` 与 SDK 的协议部分；Gateway 用 fake。实现可分小步提交，以这条链路的端到端测试验收。`session`、真实 Gateway、`cache`、`deepresearch` 在各自里程碑开始时创建。

**首个切片 ≠ M1 完成。** M1 完成还要求控制面重启恢复、取消竞争、Store 故障等门槛实验（规格 §16.7，计划索引 Plan 6）。

`internal/runtime` → `internal/sandbox` 作为一次纯重命名提交：只改包名、路径、引用与文档，不改启动、收割或降权逻辑，并重跑适用的编译与测试。

---

## 9. 函数级质量约束

第 1–6 节约束架构层面；本节约束函数内部质量。它们是**每份实施计划的共同验收条件**，在代码评审中逐项检查。

| 约束 | 执行方式 |
|---|---|
| 优先提前返回，避免业务分支不断嵌套 | 业务条件超过三层时必须审查能否分解；作为提醒，不机械禁止。连续的 `if err != nil { return … }` 属于正常 Go 错误处理，不在此列 |
| 同一业务规则只有一个权威实现 | 重点检查状态转换、重试资格、预算判定、权限检查；不同边界上的必要复查（例如 Gateway 对访问的重复校验）保留 |
| 函数围绕一个操作或决策组织 | 不同时承担协议解析、业务裁决、SQL 与物理资源操作 |
| 重复代码按语义决定是否提取 | 同一规则被重复维护时合并；仅语法相似不强制抽象 |
| 生命周期代码显式展示顺序与所有权 | 不为降低复杂度指标而隐藏锁、事务、清理或取消顺序 |
| 旧实现退出主线时同步处理 | 删除或明确隔离旧实现、测试、配置与注释，不长期维护双轨 |
| 注释解释约束与原因 | 接口契约（FD 与 Wait 的所有权、调用方责任）留在代码；方案比较、实验背景与经验推断放进设计记录，不写成可靠性契约 |
| 复杂度指标用于发现问题 | 超标需要在评审中解释或重构；不靠拆成大量无意义小函数通过检查 |

### 9.1 自动检查

- `golangci-lint` 启用 `gocognit`、`nestif`、`dupl`、`funlen`，阈值作为**信号**：CI 中报告，超标须在 PR 描述中说明理由或重构；配合 `errcheck`、`govet`、`staticcheck`、`ineffassign`、`unused` 等正确性检查（这些必须通过）。
- Python 使用 ruff（含 `C90` 复杂度与 `TID251` 禁用 API）与 `import-linter`。
- 架构检查见第 3.2 节。所有检查在 GitHub Actions 中运行。

### 9.2 现有代码的处理（随 Plan 2 LocalProvider 重写进行，不做一次性"去 if"重构）

| 位置 | 处理 |
|---|---|
| `cgroup.Apply` | 保留：三组"配置存在→写入→返回错误"语义清晰，不引入通用配置执行器 |
| `cgroup.New` | 扩展时提取"确保控制器可用"私有函数；保留创建与失败处理的主流程 |
| `hostcheck.Check` | 移除 overlay 作为必需条件，改为检查当前方案的前置条件 |
| `rootfs/overlay.go` | 将仍需要的 bind/unmount 原语与已退出主线的 Overlay 实现分开；Overlay 实现及其测试删除 |
| `sandbox/spawn.go` 注释 | 保留 FD、Wait 所有权等契约；Pdeathsig 与线程生命周期的经验推断移入设计记录，并修正为"不作为可靠性依据"（规格已规定不依赖 Pdeathsig） |
