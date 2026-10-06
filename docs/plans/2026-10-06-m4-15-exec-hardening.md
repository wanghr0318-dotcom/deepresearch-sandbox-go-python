# M4 Plan 15：独立 exec 沙箱与隔离加固

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 v0.2 规格 §10（独立 exec 沙箱：单次、无状态、无 Gateway 入口、无网络）与路线图 M4 的"隔离加固"：模型生成的 Python 代码经 Gateway `POST /v1/exec` 在每次全新的 `exec` 环境中执行，带配额、journal 重放、取消与恢复语义；以 E34 权限边界回归套件、E35–E39 故障实验与 I11、I12 不变量验收；Agent 以工具 `run_python` 使用它。

**Architecture:** 分三层推进。**沙箱/provider 层**（`internal/sandbox`、`internal/provider/local`、`internal/rootfs`）：exec 环境的 `/out` 改为宿主侧 tmpfs 再 bind 进沙箱（执行树停止后宿主仍能收集），`/in` 由 provider 提供暂存目录，新增 exec 专用只读模板（不含 worker 包）与模板摘要，provider 增加 `OpenOutputs`（§5.6 打开规则）；UID 范围归还前回收/核查范围属主文件（E39、I11）；init 能力集保持 permitted = effective = {KILL} 并加回归锁。**Gateway 层**（`internal/gateway/call` 的 exec 调度、`internal/admission` 的 exec slots、`internal/persistence/postgres` 的 exec 配额事务、migration 0008）：exec 复用 `calls` journal（`source = exec`）与 `call_tries`，配额记在 `exec_quotas`，CPU 预留为 `reservations.kind = cpu`；`gateway/call` 决定创建 exec 环境，由 resource coordinator 执行（代码组织 §6）。**接入层**：edge 路由 `/v1/exec`，Python SDK `GatewayClient.exec()`，Plan 13 工具包中的 `run_python` 工具，sim-worker 的 `exec` 操作（e2e 用）。

**Tech Stack:** Go 1.24（标准库 + 已有 pgx；不引入新模块）、PostgreSQL 16、Linux cgroup v2 / user namespace / seccomp（纯 Go BPF，已有）、Python 3.11+ Worker SDK（无新依赖）。

**规格依据：** v0.2 规格 §10（全部）、§4.5（隔离配置、seccomp、UID 范围回收）、§4.6（降权启动序列与实现门槛）、§9.2–§9.7（访问检查、指纹、两个原子提交点、重试与调用期限）、§6/§7.1（`exec_quotas`、`reservations.kind`、锁顺序）、§16.2、§16.3（I11、I12）、§16.4（E34–E39）、§17（G1、G3）、§19、§20；路线图 M4"隔离加固与独立 exec"；[启动判据决策说明](../design/2026-10-05-startup-success-decision.md)（**约束性**）；[Provider 契约](../design/2026-10-05-provider-contract.md)；[M4 第一部分设计](../superpowers/specs/2026-10-06-m4-chat-sessions-design.md) §4.3（工具包）。

## Global Constraints

- **不可触碰（决策说明，永久）**：不实现、不实验、不规划任何基于 ptrace 的启动检测（含 `PTRACE_EVENT_EXEC`、`PTRACE_O_EXITKILL` 及其能力前提实验）；init 能力集保持 bounding = {KILL, SETUID, SETGID, SETPCAP}、permitted = effective = {KILL}，**不扩展**。启动判据维持"CLOEXEC exec-status 管道 + reaper wait 状态 + 提交点"。E34 中出现的 `ptrace`/`process_vm_readv` 只是**拒绝探针**：以无效参数调用一次、断言 seccomp 返回 `EPERM`，不建立跟踪关系、不改变任何能力。seccomp user-notification 同样不在本计划内。
- helper 与生产二进制固定 `CGO_ENABLED=0`；沙箱相关 root 测试以 `CGO_ENABLED=0` 运行（与 CI linux-integration 一致）。
- exec 环境：独立 user namespace 与 UID 范围、独立 netns 仅 `lo`、**不挂载 Gateway socket、不挂载 workspace**、`/sys` 与 cgroupfs 不挂载、`/proc` 按 §4.5 掩蔽；seccomp 用 `ProfileExec`（规则与 orchestrator 相同，不放宽）；`RLIMIT_FSIZE`、`RLIMIT_CORE=0`、`RLIMIT_NOFILE`。
- exec 请求（§10.1）：`language` 仅 `python3`；`code` ≤ 256 KiB（UTF-8 字节）；`inputs[]{sha256, path}`：sha 须已授权到本任务 scope，`path` 相对、规范（`filepath.Clean` 不变）、无 `..`、不以 `/` 开头、不重复、**不以 `.agentbox/` 开头**（保留给代码文件），合计 ≤ 256 MiB；`limits{wall_ms, memory_bytes}` 可选，按策略上限截断，截断后的值进入指纹与响应。其他字段 `400 unsupported_field`。
- 代码投递：宿主把 `code` 写入 `/in/.agentbox/main.py`（0444）；`argv = ["python3", "-I", "-B", "/in/.agentbox/main.py"]`；`Dir = "/out"`；`Env` 固定为 `PATH=/usr/local/bin:/usr/bin:/bin`、`HOME=/tmp`、`LANG=C.UTF-8`、`PYTHONDONTWRITEBYTECODE=1`、`PYTHONUNBUFFERED=1`；stdin 启动后立即关闭。调用方不能指定 argv 或环境变量。
- exec 默认值（§19 补充，均可由 server 标志配置）：wall 默认 60 s、上限 300 s；内存默认 512 MiB、上限 1 GiB；`pids.max` 128；`cpu.max` 1 核（100000/100000）；`/tmp` 64 MiB；`/out` 64 MiB、`nr_inodes=1024`、≤ 256 个文件；`RLIMIT_NOFILE` 256；`RLIMIT_FSIZE` 64 MiB；stdout/stderr 各 1 MiB；排队上限 60 s；调用期限 = 排队上限 + 生效 wall + 30 s（Tx1 写入，§9.7）。全局 exec slots 4、每任务 2。每任务配额：`exec_count_limit` 50、`cpu_limit_usec` 600 CPU 秒、`wall_limit_ms` 1800 s；CPU 预留余量 10%。
- 指纹（§10.1）：`sha256(JCS({endpoint: "/v1/exec", adapter_version: "exec/1", resolved: {language, code_sha256, inputs: [{sha256, path}]（按 path 排序）, image_digest, limits（生效值）}}))`。`image_digest` = exec 模板摘要（Task 2）。
- 结果语义（§10.4）：主进程退出 → `completed`；超过 wall → `timed_out`；二者 journal 为 `completed`（结果 blob 含状态，重放返回同一结果）。attempt 结束、撤销（**任何原因**）或取消 → exec 环境总是被终止 → `cancelled`，journal 为 `failed{exec_cancelled}`（可重试类别，新 attempt 带 `X-Agentbox-Retry: true` 可在累计 3 次 try 内重跑；用户取消的任务访问检查已拒绝，不会重跑）。只有无法确认停止或结果时为 `unknown`；`unknown` 的重跑须先确认该调用此前所有 exec 环境已 `stopped_at`。
- 记账：`exec_count` 在实际启动时扣除（`start_ack` 之后的结算事务中计入；启动失败不计；崩溃时处于"启动中"的保守计入）；journal 重放不扣。CPU 预留 = `cpu.max 速率 × 生效 wall × 1.1`，停止后按 `cpu.stat usage_usec` 结算，读取失败按全额预留计入；超额如实记账并置 `blocked`，阻止该任务后续 exec。wall 与排队时间分开记录。
- 并发与容量：exec slot 只在 exec 环境的 `stopped_at` **已持久化**之后归还（§14.5 同一原则）。exec 环境内存不计入任务环境内存池：exec 的内存总量 = `--exec-slots × --exec-memory-max`，由运维与 `--memory-bytes` 一并规划（README 写明）。
- 收集 `/out`：执行树确认 `populated 0` → 读 `cpu.stat`、`memory.events` → 按 §5.6 规则打开 `/out` 下的文件（`openat2` + `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS`、`O_NOFOLLOW`）：仅普通文件、递归、≤ 256 个，其余（符号链接、FIFO、设备、超额）报告并跳过；每个输出与结果 blob 都写入 `scope_blobs(task)`。
- 分层依赖：`internal/gateway/call` 只导入 `internal/provider`（纯类型），不导入 `provider/local`、`resource`、`admission`、`persistence/postgres`（以窄接口 `ExecEnvs`、`ExecSlots`、`ExecStore` 使用）；archtest 增加对应规则。`sandbox`、`cgroup`、`rootfs`、`provider/local` 的依赖规则不变。
- Python：`run_python` 工具不在 Worker 内执行任何代码，只经 SDK 调 `/v1/exec`；ruff `TID251` 禁用的子进程 API 规则不放宽。
- 测试：Go 每包一个 `*_test.go`（向已有文件追加一节）；每条契约规则或风险路径一个用例，不写琐碎测试。需要 root 的用例在非 root 时 `t.Skip`，并且必须在 root 命令下真实运行；CI linux-integration 中任何不在 `scripts/ci/allowed-skips.txt` 的 skip 都判失败，**本计划不新增允许跳过项**。需要数据库的用例读 `AGENTBOX_TEST_DATABASE_URL`，`CI=true` 时缺失即失败。
- 命令：见 `.superpowers/sdd-m4/exec-context.md`。下文"普通"= WSL 普通用户命令，"root"= WSL root 命令（`-u root`，`HOME=/root`），均 `GOTOOLCHAIN=local`、离线 GOPROXY；root 测试另加 `CGO_ENABLED=0`。**root 测试一律串行**（`-p 1`，同一时刻只跑一个 root 测试命令）。
- 迁移编号：**0008**（0006、0007 预留给 Plan 12–14）。若合入时编号已被占用，协调者顺延并同步更新本计划与 `migrate.go`。
- 分支：集成分支 `m4-batch`；每个任务一个 worktree；只提交任务列出的文件；推送与合并由项目负责人单独授权。

## 规格缺口与本计划的决定

| # | 缺口 | 决定 |
|---|---|---|
| D1 | §10.2 要求"停止整个执行树后、保留挂载收集 /out"，但现实现的 `/out` 是 init 在自己 mount namespace 内建立的 tmpfs，执行树停止后即随命名空间消失 | `/out` 改为 provider 在宿主 `<envdir>/out` 挂载的 tmpfs（`size=OutBytes,nr_inodes=1024,mode=0700,uid=gid=UIDBase+1000`，`nosuid,nodev`），init 以 `open_tree` 克隆后可写 bind 到 `/out`；Destroy 已有的 `unmountUnder` 负责卸载。页面仍按写入者计入环境 cgroup |
| D2 | 宿主如何放入 `/in` 未定 | 调用方不提供 `Mounts.In` 时，provider 为 exec 环境建立 `<envdir>/in`（root 0755），`EnvInfo.InDir` 返回其宿主路径；调用方在 `StartExec` 前写入（文件 0444、目录 0755）。环境目录以 0711 建立（init 需穿越）。删除随环境目录 |
| D3 | 代码如何作为入口 | `/in/.agentbox/main.py`，`python3 -I -B`，cwd `/out`；输入路径禁止 `.agentbox/` 前缀（见 Global Constraints） |
| D4 | §19 无 exec wall、内存、pids、配额默认值 | 见 Global Constraints 的默认值；全部有 server 标志 |
| D5 | 没有 `images/`，指纹中的"镜像 digest"无对象 | exec 模板 = 宿主路径集合（不含 `/opt/agentbox`）；`image_digest = sha256(JCS({template_paths, python3_realpath, python3_sha256}))`，server 启动时计算一次 |
| D6 | `cancelled` 若记为 journal `completed`，被替换的 attempt 恢复后会永远重放"已取消" | `cancelled` → `failed{exec_cancelled}`（可重试类别）；`timed_out` → `completed` |
| D7 | §9.1 规定非取消原因的撤销让上游 try 继续；§10.4 规定 exec 总是被终止 | exec 以 §10.4 为准：`CancelAttempt` 的任何原因都终止该 attempt 的全部 exec |
| D8 | cleanup loop 只清理"所属 attempt 已有判决"的环境，exec 环境须在 exec 结束即回收 | exec 流程在收集后同步尽力清理（Destroy + 归还 UID 范围）；失败时由 cleanup loop 接手，`kind = exec` 的候选条件不要求 attempt 判决 |
| D9 | §6 只列 `exec_quotas` 部分列；CPU 超额"阻止后续 exec"无列；`reservations.kind` 只有 `money` | migration 0008：`exec_quotas` 增加 `cpu_unknown_usec`、`blocked`；`reservations.kind ∈ {money, cpu}`；`call_tries` 增加 `cpu_usec`、`wall_ms`、`queue_ms`、`exec_started_at`。`exec_quotas` 行在首次 exec 的预留事务中按 server 策略建立（不改 `CreateTask`，避开 Plan 12 的改动） |
| D10 | 启动账本转换把全部 held reservation 计入 `budgets` | 改为按 kind：`money` → `budgets.unknown`，`cpu` → `exec_quotas.cpu_unknown_usec`；I3 按 kind 分别核对 |
| D11 | exec 环境内存与 admission 环境内存池的关系 | 不进入任务内存池（避免任务持 run slot 等待 exec 时与排队任务互相阻塞）；exec 内存 = slots × 单个上限，单独规划 |
| D12 | 响应格式与 stdout 编码 | 结果 JSON 见 Task 8；stdout/stderr 以 UTF-8 解码，非法字节替换为 U+FFFD 并置 `*_invalid_utf8` |
| D13 | E39 要求归还的范围不拥有文件，但 task workspace 在任务结束后仍归旧范围（M1 已知缺口） | 归还前：`<data>/workspaces` 下归该范围的文件改回 root 属主（下个 attempt 启动时会重新 chown，无损）；数据目录其他位置仍有范围属主文件 → 隔离该范围并报警（§4.5"存疑则隔离"） |
| D14 | E34"setuid 提权"未定义 | ① `setuid(0)`/`setresuid(0,…)` → `EPERM`；② 宿主 root 属主与映射 root 属主的 04755 二进制执行后 euid 仍为 1000；③ 带 `security.capability` xattr 的文件执行后 `CapEff` 仍为 0 |
| D15 | §4.5 允许 exec 配置比 orchestrator 更严 | 不分化：Python 标准库（`multiprocessing`、`asyncio`）需要 `AF_UNIX socketpair`；exec 环境无 socket 可连、netns 仅 `lo`。两套配置同时补充 `personality` 参数过滤与拒绝 `name_to_handle_at` |
| D16 | sub-run 内的 exec | 透传 `X-Agentbox-Subrun` 到 `ExecInvoke.SubrunID`（记入 `calls.subrun_id`）；exec 配额是任务级。`Coordinator.CancelSubrun` 由 Plan 14 接线；edge 对 sub-run 头的现行拒绝由 Plan 14 解除 |

## 文件结构

| 文件 | 职责 |
|---|---|
| `internal/sandbox/init.go`、`mounts.go` | `InitSpec.Out`；exec 环境 `/out` 由宿主 tmpfs 可写 bind |
| `internal/sandbox/seccomp.go`、`caps.go` | `personality` 过滤、拒绝 `name_to_handle_at`；能力边界注释改为决策说明的最终结论 |
| `internal/sandbox/spawn_test.go` | 能力边界回归锁、ptrace 启动检测禁用锁、seccomp 新规则、`/out` 宿主挂载、E34 套件 |
| `internal/rootfs/template.go`、`digest.go`、`template_test.go`、`bind_test.go` | exec 模板（`ExecTemplateName`）、`Ensure` 对 exec 模板要求 python3、`TemplateDigest` |
| `internal/provider/provider.go`、`provider_test.go`、`fake/*`、`providertest/*` | `EnvInfo.InDir`、`OutputFile`、`SkippedOutput`、`Provider.OpenOutputs`、`UIDFiles`/`ReclaimUIDFiles` |
| `internal/provider/local/local.go`、`starter.go`、`outputs.go`、`scan.go`、`local_test.go` | exec 环境创建（`in/`、宿主 tmpfs `out/`）、输出收集、`uid_files` 扫描与回收 |
| `docs/design/2026-10-05-provider-contract.md` | 契约增补（执行中修订） |
| `internal/hostcheck/*`、`scripts/ci/check-runner.sh` | user namespace、seccomp 动作、`CLONE_INTO_CGROUP`、`pidfd`、架构声明 |
| `internal/resource/cleanup.go`、`coordinator.go`、`store.go`、`resource_test.go` | 归还 UID 范围前的文件回收与隔离；exec 环境清理候选 |
| `internal/admission/admission.go`、`admission_test.go` | `ExecGate`：全局 exec slots + 每任务上限 |
| `internal/persistence/postgres/migrations/0008_exec.sql` | `exec_quotas`、`reservations.kind`、`call_tries` exec 列 |
| `internal/persistence/postgres/exec.go`、`gateway.go`、`cleanup.go`、`resource.go`、`invariants.go`、`migrate.go`、`postgres_test.go` | exec 事务用例、按 kind 的账本转换、I3/I11/I12、范围隔离 |
| `internal/gateway/call/execstore.go`、`exec.go`、`call.go`、`call_test.go` | exec 的窄接口与调度流程 |
| `internal/gateway/edge/edge.go`、`edge_test.go` | `/v1/exec` 路由；`/v1/budget` 增加 exec 配额 |
| `internal/archtest/archtest_test.go` | `gateway/call` 的新依赖规则 |
| `internal/app/app.go`、`adapters.go`、`app_test.go`、`cmd/agentbox/server.go`、`deploy/agentbox.env.example` | 装配、exec 环境适配器、标志、启动检查 |
| `internal/invariants/invariants.go`、`invariants_test.go` | I11 [Q] 文件扫描、I12 [Q] 资源扫描 |
| `internal/cli/cli.go`、`internal/persistence/postgres/apiread.go` | `inspect` 显示 exec try（排队、wall、CPU、环境） |
| `worker/agentbox_worker/gateway.py`、`worker/tests/test_sdk.py` | `GatewayClient.exec()` |
| `worker/agentbox_worker/tools/run_python.py`、`worker/tests/test_tools.py` | `run_python` 工具（Plan 13 工具包内） |
| `worker/sim_worker/app.py`、`worker/tests/test_process.py` | sim-worker `exec` 操作 |
| `tests/e2e/e2e_test.go` | 真实沙箱 exec 端到端、E34（经 Gateway 部分）、E35–E39 |
| `README.md`、`docs/design/2026-10-03-v0.2-first-release-design.md`（§10、§19 执行中修订）、`docs/evidence/2026-10-xx-m4-exec-hardening.md`、M4 计划索引 | 文档与验收记录 |

## 任务与依赖

| 任务 | 内容 | 本计划内依赖 | 与 Plan 12–14 的关系 | 何时可开始 |
|---|---|---|---|---|
| 1 | sandbox：`/out` 宿主侧 bind、能力边界与 ptrace 禁用回归锁、seccomp 补充 | — | 独立 | **立即** |
| 2 | provider/rootfs：exec 环境创建（`/in` 暂存、宿主 `/out`、`OpenOutputs`、exec 模板与摘要） | 1 | 独立 | 1 完成后 |
| 3 | sandbox：E34 权限边界回归套件（编排与 exec 两类环境） | 1 | 独立（session 场景直接以 session 类型 init 构造，不依赖 Plan 12） | 1 完成后，与 2 并行 |
| 4 | hostcheck / check-runner 加固 | — | 独立 | **立即** |
| 5 | UID 范围文件回收与隔离（E39、I11） | 2 | 文件重叠：`resource/cleanup.go`、`postgres/resource.go` 可能被 Plan 12（session 范围保留）修改，协调者排序 | 2 完成后 |
| 6 | admission：`ExecGate` | — | 独立 | **立即** |
| 7 | migration 0008 与 postgres exec 事务用例、按 kind 的账本转换、I3/I12 | — | 文件重叠：`postgres/gateway.go`（Plan 12 的工具额度改 `ReserveTry`）、`invariants.go`、`migrate.go`；只改启动账本转换函数，新代码放 `exec.go` | **立即** |
| 8 | gateway/call：exec 调度流程 | 2、6、7 | 文件重叠：`call/call.go`（Plan 12 工具额度、Plan 14 sub-run）；exec 代码放 `exec.go`，`call.go` 只加 Config 字段与 `CancelAttempt` 分支 | 2、6、7 完成后 |
| 9 | edge `/v1/exec`、`/v1/budget` exec 配额、SDK `GatewayClient.exec()` | 8 | 文件重叠：`edge.go`（Plan 14 解除 sub-run 拒绝）、`gateway.py` | 8 完成后 |
| 10 | 装配、标志、启动恢复（E37 确定性）、exec 清理候选、verify-invariants、inspect | 4、5、8、9 | **依赖 Plan 12 的装配先合入**（`app.go`、`adapters.go`、恢复路径同文件）；语义上不依赖会话 | 4、5、8、9 及 Plan 12 装配任务合入后 |
| 11 | Worker：`run_python` 工具；sim-worker `exec` 操作 | 9 | `run_python` **依赖 Plan 13 工具包**（`tools/base.py`、注册表）；sim-worker 操作只依赖 9 | sim 操作：9 完成后；工具：再加 Plan 13 工具包合入后 |
| 12 | 真实沙箱 e2e（E34 经 Gateway、E35–E39）、文档、CI、服务器验收 | 3、10、11 | 全量回归时与 Plan 12–14 一起跑 | 最后 |

可立即开始：**1、4、6、7**（彼此文件不重叠，可并行，各用独立 worktree）。之后 2 与 3 并行（不同包；3 只改 `spawn_test.go`，2 不改 sandbox）。8 是汇合点。

---

### Task 1：sandbox——`/out` 宿主侧 bind、能力边界与 ptrace 禁用回归锁、seccomp 补充

**Files:** Modify `internal/sandbox/init.go`、`internal/sandbox/mounts.go`、`internal/sandbox/seccomp.go`、`internal/sandbox/caps.go`（只改注释）、`internal/sandbox/spawn_test.go`（追加；并更新测试辅助 `initTestSpec` 为 exec 场景提供宿主 tmpfs）。不改 `helper.go`、`launcher.go`、`reaper.go`、`rawfork_linux.go`。

**Interfaces (Produces)：**

```go
// InitSpec 新字段。exec 环境必填：init 不再自建 /out tmpfs。
	// Out 是 exec 环境 /out 的宿主侧 tmpfs 挂载点（provider 在启动 init 前挂载：size=OutBytes、nr_inodes=1024、
	// mode=0700、uid=gid=映射 1000、nosuid、nodev）。init 以 O_PATH 固定后 open_tree(OPEN_TREE_CLONE) 克隆，
	// mount_setattr 设 NOSUID|NODEV（不设 RDONLY）后 move_mount 到 /out；回退路径 mount(2) MS_BIND。
	Out string `json:"out,omitempty"`

// 新步骤名：失败原因为 "init/mount_out: <原因>"。
const stepMountOut = "mount_out"
```

- `validate`：exec 环境 `Out` 须为规范绝对路径；编排环境 `Out` 必须为空。`OutBytes` 保留（`RLIMIT_FSIZE` 缺省值计算仍用它）。
- seccomp（两套配置相同）：`personality(persona)` 只允许 `0x0`、`0x8`、`0x20000`、`0x20008`、`0xffffffff`（查询），其余 `EPERM`；`name_to_handle_at`（amd64 303、arm64 264）加入无条件拒绝列表。
- `caps.go` 中"Plan 2 的 permitted 扩展……届时只改这里"的注释改为：能力集已按 2026-10-05 决策说明定案，permitted = effective = {KILL}，不扩展；ptrace 路径永久不采用。`initCaps` 的值不变。

- [ ] **Step 1：写失败测试**（`spawn_test.go` 追加一节）：

```go
// TestInitCapsBoundaryIsFinal：init 能力集是决策说明定案的边界（permitted = effective = {KILL}，
// bounding = {KILL, SETUID, SETGID, SETPCAP}）。任何扩展都必须先改决策说明，而不是改这里。
func TestInitCapsBoundaryIsFinal(t *testing.T) {
	want := capSets{
		Bounding:  1<<capKill | 1<<capSetuid | 1<<capSetgid | 1<<capSetpcap,
		Permitted: 1 << capKill,
		Effective: 1 << capKill,
	}
	if initCaps != want {
		t.Fatalf("initCaps = %+v，决策说明要求 %+v", initCaps, want)
	}
}

// TestNoPtraceStartDetection：启动判据只用 exec-status 管道 + reaper wait（决策说明 §5）。
// sandbox 的非测试源码不得出现 ptrace 跟踪相关调用；seccomp 拒绝列表中的 "ptrace" 名称除外。
func TestNoPtraceStartDetection(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	banned := regexp.MustCompile(`SYS_PTRACE|PtraceAttach|PtraceSeize|PTRACE_(SEIZE|ATTACH|TRACEME|O_|EVENT_)|PtraceSetOptions`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if loc := banned.FindIndex(b); loc != nil {
			t.Errorf("%s 出现 ptrace 跟踪调用 %q：启动检测不得使用 ptrace（决策说明）", f, b[loc[0]:loc[1]])
		}
	}
}
```

  另加：`TestSeccompFilterRules` 的表中追加 `personality`（允许值与 `0x0400000` 等拒绝值，两种架构、两套配置）与 `name_to_handle_at → EPERM`；`seccompTestNrs` 补调用号，`TestSeccompSyscallNumbersMatchStdlib` 覆盖新号。root 用例 `TestInitExecOutHostMount`：测试先在临时目录挂载宿主 tmpfs（`uid=gid=initTestIDBase+1000`），启动 exec 环境，workload 写 `/out/a/b.txt` 与 `/out/big`（超过 `size` → `ENOSPC`）、创建 1100 个空文件（第 1024 个左右 → `ENOSPC`）；`/out` 挂载选项含 `nosuid,nodev`、可写；终止执行树后宿主读到 `<out>/a/b.txt` 内容且属主为 `initTestIDBase+1000`；`Out` 指向不存在路径 → `init/mount_out: …`，workload 未运行。`TestIsolationAcceptance16_2` 的 exec 场景改用宿主 `/out` 后仍全部通过（`writable_areas` 检查不变）。

- [ ] **Step 2：运行确认失败**：普通 `go test ./internal/sandbox/ -run 'TestInitCapsBoundaryIsFinal|TestNoPtraceStartDetection|TestSeccompFilterRules|TestSeccompSyscallNumbersMatchStdlib'`（seccomp 新用例失败；前两个用于锁定现状，可直接通过——它们是回归锁，不是新行为）；root `CGO_ENABLED=0 go test -p 1 ./internal/sandbox/ -run 'TestInitExecOutHostMount'` → 编译失败（`InitSpec.Out` 未定义）。
- [ ] **Step 3：实现**。`mounts.go`：exec 分支不再在 `tmpfs` 列表中加 `/out`；在 `/in` 之后以与 `/in` 相同的 `pin` + `m.bind(fd, root("/out"), false)` 挂载，然后 `MS_BIND|MS_REMOUNT|MS_NOSUID|MS_NODEV` 补设（回退路径）。`seccomp.go`：`personality` 走参数比较分支（低 32 位，`JEQ` 链），`name_to_handle_at` 加入 `seccompDenyNames` 与两张调用号表。
- [ ] **Step 4：运行通过**：普通 `go test -count=3 ./internal/sandbox/`；root `CGO_ENABLED=0 go test -p 1 -count=1 ./internal/sandbox/`（含 `TestIsolationAcceptance16_2`、`TestInitExecOutHostMount`、全部 helper 提交点用例）。`gofmt` 检查所改文件。
- [ ] **Step 5：提交** `feat(sandbox): exec 环境 /out 改为宿主 tmpfs bind；能力边界与 ptrace 禁用回归锁；seccomp 补充 personality 与 name_to_handle_at`。

---

### Task 2：provider 与 rootfs——exec 环境创建

**Files:** Modify `internal/provider/provider.go`、`internal/provider/provider_test.go`、`internal/provider/fake/*`（实现新方法）、`internal/provider/providertest/*`（契约用例）、`internal/provider/local/local.go`、`internal/provider/local/starter.go`、`internal/provider/local/local_test.go`、`internal/rootfs/template.go`、`internal/rootfs/template_test.go`、`docs/design/2026-10-05-provider-contract.md`（"执行中修订（Plan 15）"一节）；Create `internal/provider/local/outputs.go`、`internal/rootfs/digest.go`。

**Interfaces (Produces，`internal/provider`)：**

```go
type EnvInfo struct {
	// ...已有字段
	// InDir 只对 exec 环境且 Mounts.In 为空时非空：provider 建立的输入暂存目录（宿主路径，root 0755），
	// 调用方在 StartExec 之前写入，沙箱内只读可见于 /in。幂等 Create 返回同一路径。
	InDir string
}

// MaxOutputFiles 是 exec /out 收集的文件上限（§10.2、§19）。
const MaxOutputFiles = 256

// OutputFile 是 /out 中一个已打开的普通文件（O_RDONLY|O_NOFOLLOW，按 §5.6 规则打开）。调用方负责关闭。
type OutputFile struct {
	Path string // 相对 /out 的规范路径，例如 "a/b.txt"
	Size int64
	File *os.File
}

// SkippedOutput 是未收集的条目。Reason：symlink | not_regular | too_many | open_failed。
type SkippedOutput struct{ Path, Reason string }

type Provider interface {
	// ...已有方法
	// OpenOutputs 打开 exec 环境 /out 下的普通文件（递归、按路径排序、至多 max 个）。环境须已确认停止，
	// 否则 ErrNotStopped；非 exec 环境返回错误；环境不存在为 ErrNotFound。
	OpenOutputs(ctx context.Context, envID string, max int) ([]OutputFile, []SkippedOutput, error)
}
```

**Interfaces (Produces，`internal/rootfs`)：**

```go
// ExecTemplateName 是 exec 环境的模板标识：默认模板去掉 WorkerDir 与 /etc/ssl（exec 无网络、不需要 SDK）。
const ExecTemplateName = "exec"
func ExecTemplate() Template
// ResolveTemplate 增加 "exec"。Ensure 对含 /usr 的模板不变；EnsureExec 另要求沙箱视图内 /usr/bin/python3 可解析为模板内文件。
func (t Template) EnsureExec() error
// TemplateDigest = sha256(JCS({"paths": 排序后的模板路径, "python3": 沙箱视图解析出的宿主真实路径, "python3_sha256": 该文件内容哈希}))，十六进制。
func TemplateDigest(t Template) (string, error)
```

**local 行为：**
- exec 环境的环境目录以 0711 建立（init 以映射 root 运行，需穿越）；`owner.json` 仍 0600。`Mounts.In` 为空时建立 `<envdir>/in`（0755）并把它作为 `InitSpec.In`；`<envdir>/out` 建目录后挂载 tmpfs（D1 的参数），作为 `InitSpec.Out`。挂载在启动 init 之前、写 `owner.json` 之后完成（失败即残留，由 Stop/Destroy 清理，与现有不完整语义一致）。
- `existing()` 对 exec 环境返回 `InDir`。
- `OpenOutputs`：要求 `stopped`；以 `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV)` 从 `<envdir>/out` 的目录 fd 逐级打开；目录 `O_DIRECTORY|O_NOFOLLOW`，文件 `O_RDONLY|O_NOFOLLOW|O_NONBLOCK` 后 `fstat` 确认普通文件；按路径字典序收集，超过 `max` 的普通文件记 `too_many`。实现与 `internal/runner/artifact.go` 规则相同，各自独立（provider 不依赖 runner）。
- `Scan`：`<envdir>/out` 的 tmpfs 已在 `mount` 层（数据目录下的挂载点）中，随环境目录分类，无需新层。

- [ ] **Step 1：写失败测试**。`provider_test.go`：`Validate` 接受 `KindExec` 且 `Mounts.In == ""`。`providertest` 契约（fake 与 local 都跑）：`OpenOutputs` 在未停止时为 `ErrNotStopped`、对编排环境报错。`rootfs` 测试（非 root）：`ExecTemplate()` 不含 `WorkerDir` 与 `/etc/ssl`、通过 `Validate`；`TemplateDigest` 对同一输入稳定、python3 文件内容变化（临时目录构造的模板）时改变；`EnsureExec` 在缺 python3 的临时模板上报告路径。`local_test.go`（root）：

```go
// TestExecEnvInOut：exec 环境的 /in 暂存与宿主侧 /out（§10.2，D1、D2）。
func TestExecEnvInOut(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	p, spec := newExecEnv(t) // ExecTemplate、Mounts{OutBytes: 1 << 20}
	info, err := p.Create(ctx, spec)
	// InDir 存在、0755、root 属主；环境目录 0711
	// 写 <InDir>/.agentbox/main.py 与 <InDir>/data/x.csv
	// StartExec: python3 -I -B /in/.agentbox/main.py：读 /in/data/x.csv，写 /out/r/a.txt、
	//   建符号链接 /out/l → /etc/passwd、mkfifo /out/f、写 300 个 /out/many/NNN、ls /opt/agentbox（应 ENOENT）、
	//   向 /in 写入（应 EROFS）
	// Wait 退出码 0；OpenOutputs 在 Stop 之前 → ErrNotStopped
	// Stop 后 OpenOutputs(256)：r/a.txt 内容正确；l → symlink、f → not_regular；many 中按序 256−1 个收集、其余 too_many
	// 再次 Create（同 spec）→ 同一 InDir
	// Destroy：<envdir>/out 已卸载、目录不存在；Scan 无该环境任何层
}
```

  另加 root 用例 `TestExecEnvOutQuota`（E38 provider 部分）：`/out` 写满 → `ENOSPC`；inode 耗尽 → `ENOSPC`；`/out` tmpfs 页面加进程匿名内存超过 `memory.max` → 环境内 OOM（`ResourceDiag.OOMKillDelta ≥ 1`），宿主进程不受影响，Stop + Destroy 成功。
- [ ] **Step 2：运行确认失败**：普通 `go test ./internal/provider/... ./internal/rootfs/`；root `CGO_ENABLED=0 go test -p 1 ./internal/provider/local/ -run 'TestExecEnv'`。
- [ ] **Step 3：实现**；`starter.go` 把 `InDir`/`<envdir>/out` 填入 `InitSpec.In`/`Out`；fake provider 用临时目录模拟 `InDir` 与 `OpenOutputs`（停止前 `ErrNotStopped`）。契约文档增补：`EnvInfo.InDir`、`OpenOutputs`、exec 环境目录权限与 `/out` 宿主挂载、Destroy 卸载顺序。
- [ ] **Step 4：运行通过**：普通 `go test -count=3 ./internal/provider/... ./internal/rootfs/`；root `CGO_ENABLED=0 go test -p 1 -count=1 ./internal/provider/local/ ./internal/rootfs/`。
- [ ] **Step 5：提交** `feat(provider): exec 环境的 /in 暂存、宿主侧 /out 与 OpenOutputs；exec 只读模板与模板摘要`。

---

### Task 3：E34 权限边界回归套件

**Files:** Modify `internal/sandbox/spawn_test.go`（追加：检查器新探针 + `TestE34PermissionBoundary`）。不改非测试文件；若发现某项实际未被拒绝，停下报告 BLOCKED（附探针输出），由协调者决定加固任务，不在本任务内顺手改沙箱。

**设计：** 复用 `runChecker` / `acceptItem` 机制（每项一个子测试，`-v` 逐项输出），检查器增加 `checkE34()` 一组探针，在两类环境各运行一遍：**编排**（task 环境，带 workspace 与 pong Gateway socket；另以 session 类型 + 外来 UID 范围运行一次，覆盖"访问 session workspace"）与 **exec**（Task 1 的宿主 `/out`、`/in`）。同时运行一个**旁邻环境**（另一 UID 范围的 task 环境，workspace 中有 `secret.txt`，0600）作为"他人文件"目标。

| E34 条目 | 探针（检查名） | 期望 |
|---|---|---|
| mount / umount2 / pivot_root / unshare / setns | `e34_mount_family` | 全部 `EPERM`（seccomp） |
| `clone` 带 `CLONE_NEW*`、`clone3` | `e34_clone_newns` | `EPERM`；`clone3` → `ENOSYS` |
| ptrace / process_vm_readv（**仅拒绝探针**：pid 取一个不存在的值，过滤器缺失时内核返回 `ESRCH`/`EINVAL`，可判别） | `e34_ptrace_denied`、`e34_process_vm_denied` | `EPERM` |
| 非 `AF_UNIX` socket：`AF_INET`、`AF_INET6`、`AF_NETLINK`、`AF_PACKET`、`AF_VSOCK`、`AF_BLUETOOTH`、`AF_ALG` | `e34_socket_families` | 全部 `EPERM`；`AF_UNIX` 成功 |
| exec 连接 Gateway：`/run/agentbox/gateway.sock` 不存在；遍历 `/`（跳过 `/proc`）找不到任何 socket 文件；连接抽象地址 `@agentbox`、`@gateway` | `e34_exec_no_gateway`（仅 exec） | 不存在 / `ECONNREFUSED`；编排环境对照：pong 可达 |
| 读 `/proc/kcore`、`/proc/keys`、`/proc/timer_list` | `e34_proc_masked_read` | 读到 0 字节或 `EACCES`/`ENOENT`，无内核数据 |
| 写 `/proc/sys/kernel/hostname`、`/proc/sysrq-trigger`、`/proc/sys/vm/drop_caches` | `e34_proc_write` | `EROFS` 或 `EACCES` |
| setuid 提权（D14） | `e34_setuid_syscalls`、`e34_setuid_binary_hostroot`、`e34_setuid_binary_nsroot`、`e34_file_caps` | `EPERM`；执行后 euid=1000、`CapEff=0` |
| `io_uring_setup`、`bpf`、`keyctl`、`add_key`、`request_key`、`perf_event_open`、`userfaultfd` | `e34_kernel_ifaces` | `EPERM` |
| 访问 session workspace 或他人文件 | `e34_foreign_workspace`、`e34_no_host_paths`、`e34_pidns_isolated`、`e34_kill_scope` | 旁邻环境的 workspace 与宿主数据目录路径不存在；`/proc` 只见本 pid ns；`kill(-1, SIGKILL)` 后宿主哨兵进程与旁邻环境进程存活；外来范围读 session workspace → `EACCES`（沿用 `session_workspace_foreign_range_denied`） |
| `personality(0x0400000)`（Task 1 新规则） | `e34_personality` | `EPERM` |

setuid 与文件能力的前置由宿主（测试进程，root）准备：把一个静态的探针二进制（测试二进制本身，以子命令参数输出 `getresuid` 与 `/proc/self/status` 的 `CapEff`）复制两份到 `/in`（exec）与 workspace（编排）：一份属主宿主 root、一份属主映射 root（`UIDBase+0`），均 `chmod 04755`；第三份设置 `security.capability` xattr（v2 结构，effective + permitted = `CAP_SETUID|CAP_DAC_OVERRIDE`）。

- [ ] **Step 1：写失败测试**：`TestE34PermissionBoundary` 按上表断言；检查器缺失某项输出即失败（沿用"未归入条目的检查"反向核对）。
- [ ] **Step 2：运行确认失败**：root `CGO_ENABLED=0 go test -p 1 ./internal/sandbox/ -run TestE34PermissionBoundary -v` → 检查器没有输出新探针。
- [ ] **Step 3：实现探针**（只在测试文件中）。
- [ ] **Step 4：运行通过**：root `CGO_ENABLED=0 go test -p 1 -count=3 ./internal/sandbox/ -run 'TestE34PermissionBoundary|TestIsolationAcceptance16_2' -v`，把 `-v` 输出保存到 worktree 的 `.superpowers` 草稿（不提交），供 Task 12 写证据。
- [ ] **Step 5：提交** `test(sandbox): E34 权限边界回归套件（编排与 exec 两类环境）`。

---

### Task 4：hostcheck 与 runner 能力检查加固

**Files:** Modify `internal/hostcheck/hostcheck.go`、`internal/hostcheck/probe_linux.go`、`internal/hostcheck/probe_other.go`、`internal/hostcheck/hostcheck_test.go`、`scripts/ci/check-runner.sh`。

**Interfaces (Produces)：**

```go
type Report struct {
	// ...已有字段
	// Warnings 不阻止启动，由 doctor 与 server 启动日志输出（例如 aarch64 未经验证）。
	Warnings []string
}
```

新增探针（`probes` 表）：
1. **user namespace**：`/proc/sys/user/max_user_namespaces` > 0。
2. **seccomp 动作**：`/proc/sys/kernel/seccomp/actions_avail` 含 `kill_process`、`errno`、`allow`。
3. **`CLONE_INTO_CGROUP`**：内核 ≥ 5.7（沿用 `kernelAtLeast`）。
4. **`pidfd_open`/`pidfd_send_signal`**：以非法参数调用，`ENOSYS` 即不可用。
5. **架构**：`amd64` 支持；`arm64` → `Warnings`"aarch64 未经单独验证，不构成支持主张（规格 §16.2）"；其余为问题。
6. 删除过时文案"需要 root 权限（M1 未启用 user namespace）"→"需要 root 权限（特权 Runtime 以 user namespace 隔离 workload，服务本身不是 rootless，规格 §4.5）"。

`check-runner.sh` 同步增加 1–4（shell + python3 ctypes，与现有新挂载 API 检查同法）。

- [ ] **Step 1：写失败测试**：`checkUserNS`、`checkSeccompActions` 以注入的文件内容为参数（纯函数，非 root 可测）：`0` → 问题；缺 `kill_process` → 问题；`kernelAtLeast("5.6.0", 5, 7) == false`；架构表三种结果；root 下 `Check()` 在 WSL 上 `Problems` 为空。
- [ ] **Step 2–4：实现并通过**：普通 `go test -count=3 ./internal/hostcheck/`；root `go test -p 1 ./internal/hostcheck/`；`bash -n scripts/ci/check-runner.sh`；root 运行 `sudo bash scripts/ci/check-runner.sh` 等价命令通过。
- [ ] **Step 5：提交** `feat(hostcheck): 检查 user namespace、seccomp 动作、CLONE_INTO_CGROUP 与 pidfd；声明架构支持范围`。

---

### Task 5：UID 范围文件回收与隔离（E39、I11）

**Files:** Modify `internal/provider/provider.go`、`internal/provider/fake/*`、`internal/provider/local/scan.go`、`internal/provider/local/local_test.go`、`internal/resource/coordinator.go`（窄接口）、`internal/resource/cleanup.go`、`internal/resource/store.go`、`internal/resource/resource_test.go`、`internal/persistence/postgres/resource.go`、`internal/persistence/postgres/invariants.go`（I11 [A]）、`internal/persistence/postgres/postgres_test.go`、`internal/invariants/invariants.go`、`internal/invariants/invariants_test.go`（I11 [Q]）。

**Interfaces (Produces)：**

```go
// internal/provider
type Provider interface {
	// ...
	// ReclaimUIDFiles 把 <data>/workspaces 下属主（uid 或 gid）落在 [base, base+size) 的条目（不跟随符号链接）
	// 改回 0:0，返回改动数量。只处理 workspaces；其他位置不改动。
	ReclaimUIDFiles(ctx context.Context, base, size uint32) (int, error)
	// UIDFiles 扫描数据目录（跳过 blobs 目录与 /proc 类伪文件系统，不跨挂载点进入其他文件系统以外的位置）中
	// 属主落在范围内的条目，至多返回 limit 个路径。
	UIDFiles(ctx context.Context, base, size uint32, limit int) ([]string, error)
}

// internal/resource
type Store interface {
	// ...
	// QuarantineUIDRange 把范围置为 quarantined（只在分配代次匹配时），同一事务 RecordQuarantine(layer = uid_files)。
	QuarantineUIDRange(ctx context.Context, uidRangeID, allocationID, reason string) error
}
```

**行为（`cleanupEnv` 在 `cleanup_state = done` 之后、`ReleaseUIDRange` 之前）：** `ReclaimUIDFiles` → `UIDFiles(limit 16)`；为空则归还；非空则 `QuarantineUIDRange`（原因列出前几个路径）并经现有隔离报警（I8）路径报警，不归还。`Scan` 增加 `uid_files` 层：`uid_ranges` 中**未分配**（free）的范围仍拥有的文件（由启动核对传入范围集合；`Scan` 签名不变，新增 `ScanUIDFiles(ctx, ranges)` 供 reconcile 使用）。session 环境：Plan 12 在会话存续期间不归还其范围，本任务的回收只在归还时发生，不影响会话 workspace。

- [ ] **Step 1：写失败测试**。`resource_test.go`（fake provider/store）：清理后先回收后扫描再归还；扫描有残留 → 范围 quarantined、未归还、记录隔离；`ReclaimUIDFiles` 出错 → 本轮不归还、下轮重试。`postgres_test.go`：`QuarantineUIDRange` 代次不匹配为 `ErrConflict`；I11 [A] SQL：两个存活环境同一范围 → 违例。`local_test.go`（root，E39）：

```go
// TestE39UIDRangeReuse：范围 R 的 task 环境在 workspace 与 /tmp 写文件 → 清理（Destroy）后 workspace 文件
// 回到 0:0、/tmp 随 tmpfs 消失，UIDFiles(R) 为空；范围 R 分配给新环境 B，B 的 uid 1000（与旧环境同一宿主 uid）
// 在其可见视图中找不到任何旧文件。另一轮：在数据目录其他位置植入属主 R+1000 的文件 → UIDFiles 报告该路径
// （resource 层据此隔离，见 resource_test）。
```

  `invariants_test.go`：I11 [Q] 对 free 范围拥有的文件报告违例。
- [ ] **Step 2–4：实现并通过**：普通 `go test -count=3 ./internal/resource/ ./internal/invariants/ ./internal/provider/...`；PG `CI=true AGENTBOX_TEST_DATABASE_URL=… go test -count=3 ./internal/persistence/postgres/ -run 'UIDRange|Invariant'`；root `CGO_ENABLED=0 go test -p 1 -count=1 ./internal/provider/local/ -run 'TestE39|TestExecEnv'`。
- [ ] **Step 5：提交** `feat(resource): 归还 UID 范围前回收 workspace 属主并核查残留文件，残留即隔离（E39、I11）`。

---

### Task 6：admission——`ExecGate`

**Files:** Modify `internal/admission/admission.go`（包注释去掉"exec slots 属于 M4，本包不建模"；新增类型放同文件或 `exec.go`）、`internal/admission/admission_test.go`。

**Interfaces (Produces)：**

```go
// ExecCapacity：全局 exec slots 与每任务上限（§10.3，默认 4、2）。
type ExecCapacity struct{ Slots, PerTask int }

type ExecGrant struct {
	ID     uint64
	TaskID string
}

// ExecGate 按 FIFO 授予 exec slot：队首的任务已达每任务上限时，跳过它授予后面第一个可满足的等待者
// （同一任务内保持 FIFO）。
type ExecGate struct{ /* mu, cap, held, perTask, queue */ }

func NewExecGate(c ExecCapacity) *ExecGate
// Acquire 等待一个 slot；ctx 结束返回 ctx.Err() 且不占用（已授予但调用方已离开时立即归还）。
func (g *ExecGate) Acquire(ctx context.Context, taskID string) (ExecGrant, error)
// Release 幂等。调用方只在 exec 环境 stopped_at 已持久化后调用。
func (g *ExecGate) Release(gr ExecGrant)
// Occupy 在启动恢复时登记仍被未确认停止的 exec 环境占用的 slot（stop_blocked），返回其授予。
func (g *ExecGate) Occupy(taskID string) ExecGrant
func (g *ExecGate) Snapshot() ExecUsage // {Capacity, Used, PerTask map[string]int, Queued int}
```

- [ ] **Step 1：写失败测试**：每任务第 3 个等待、其他任务不受阻（跳过队首）；同任务 FIFO；全局满时排队；ctx 取消不泄漏（授予与取消竞争：确定性屏障 + 1000 次随机，记录 seed）；`Release` 幂等且未知 ID 忽略；`Occupy` 计入容量。
- [ ] **Step 2–4：实现并通过**：普通 `go test -count=3 ./internal/admission/`。
- [ ] **Step 5：提交** `feat(admission): exec slots（全局与每任务上限）`。

---

### Task 7：migration 0008 与 postgres exec 事务用例

**Files:** Create `internal/persistence/postgres/migrations/0008_exec.sql`、`internal/persistence/postgres/exec.go`、`internal/gateway/call/execstore.go`（接口与类型，本任务产出）；Modify `internal/persistence/postgres/gateway.go`（**只改**启动账本转换函数，即注释"全部 held 的 reservation → charged_unknown"的那个）、`internal/persistence/postgres/invariants.go`（I3 按 kind、I12）、`internal/persistence/postgres/migrate.go`（表清单）、`internal/persistence/postgres/postgres_test.go`（追加）。

**Migration：**

```sql
-- 0008：exec 配额与运行记录（规格 §6、§10.3；Plan 15 D9）。
ALTER TABLE reservations DROP CONSTRAINT reservations_kind_check;
ALTER TABLE reservations ADD CONSTRAINT reservations_kind_check CHECK (kind IN ('money', 'cpu'));

CREATE TABLE exec_quotas (
    task_id           text PRIMARY KEY REFERENCES tasks (task_id),
    exec_count_limit  bigint NOT NULL CHECK (exec_count_limit >= 0),
    exec_count_used   bigint NOT NULL DEFAULT 0 CHECK (exec_count_used >= 0),
    cpu_limit_usec    bigint NOT NULL CHECK (cpu_limit_usec >= 0),
    cpu_reserved_usec bigint NOT NULL DEFAULT 0 CHECK (cpu_reserved_usec >= 0),
    cpu_spent_usec    bigint NOT NULL DEFAULT 0 CHECK (cpu_spent_usec >= 0),
    cpu_unknown_usec  bigint NOT NULL DEFAULT 0 CHECK (cpu_unknown_usec >= 0),
    wall_limit_ms     bigint NOT NULL CHECK (wall_limit_ms >= 0),
    wall_spent_ms     bigint NOT NULL DEFAULT 0 CHECK (wall_spent_ms >= 0),
    blocked           boolean NOT NULL DEFAULT false
);

ALTER TABLE call_tries ADD COLUMN cpu_usec bigint;
ALTER TABLE call_tries ADD COLUMN wall_ms bigint;
ALTER TABLE call_tries ADD COLUMN queue_ms bigint;
ALTER TABLE call_tries ADD COLUMN exec_started_at timestamptz;
CREATE INDEX environments_exec_attempt ON environments (attempt_id) WHERE kind = 'exec';
```

**Interfaces (Produces，`internal/gateway/call/execstore.go`)：**

```go
// ExecPolicy 是每任务 exec 配额（server 策略）；首次 exec 的预留事务以它建立 exec_quotas 行。
type ExecPolicy struct{ CountLimit, CPULimitUsec, WallLimitMs int64 }

type ReserveExecRequest struct {
	TaskID, CallID, AttemptID, SubrunID string
	ReservationID                       string // 事务前生成（§7.3 操作身份）
	CPUEstimateUsec, WallMs, QueueMs    int64
	MaxTries                            int
	Policy                              ExecPolicy
}

// ExecTry 是一次已预留的 exec try；EnvID = ExecEnvID(TaskID, CallID, TryNo)。
type ExecTry struct {
	Try
	EnvID string
}

// ExecEnvID 由 (task, call, try_no) 确定性派生（"exec-" + sha256 前 24 个十六进制字符），使预留事务结果未知后的重试指向同一环境。
func ExecEnvID(taskID, callID string, tryNo int) string

type ExecOutcome string

const (
	ExecCompleted   ExecOutcome = "completed"    // journal completed
	ExecTimedOut    ExecOutcome = "timed_out"    // journal completed
	ExecCancelled   ExecOutcome = "cancelled"    // journal failed{exec_cancelled}，可重试
	ExecStartFailed ExecOutcome = "start_failed" // journal failed{exec_start_failed}，不计 exec_count
	ExecUnknown     ExecOutcome = "unknown"      // journal unknown；CPU 全额转 unknown
)

type ExecSettlement struct {
	Try                  ExecTry
	Outcome              ExecOutcome
	Started              bool  // 已收到 start_ack：计 exec_count
	CPUUsec              int64 // cpu.stat usage_usec
	CPUKnown             bool  // false：按全额预留计入 spent
	WallMs, QueueMs      int64
	ResultSHA256         string // completed / timed_out：结果 blob（已完整保存）
	ResultSize           int64
	OutputSHAs           []string // 写入 scope_blobs(task)
	Error                string   // call_tries.error 摘要
}

type ExecQuota struct {
	CountLimit, CountUsed                                   int64
	CPULimitUsec, CPUReservedUsec, CPUSpentUsec, CPUUnknownUsec int64
	WallLimitMs, WallSpentMs                                int64
	Blocked                                                 bool
}

// ExecStore：每个方法一个事务（锁顺序 tasks FOR SHARE → task_control FOR SHARE → attempt_access FOR SHARE
// → environments → exec_quotas → calls → reservations → call_tries）。
type ExecStore interface {
	// ReserveExec 是 exec 的 Tx2：复查 §9.2 访问与期限（db now() < deadline_at）、tries_used < MaxTries、
	// 本调用此前 try 的 exec 环境均已 stopped_at（否则 ErrRejected{call_in_progress}）、exec_quotas 未 blocked
	// （exec_blocked）、count_used < limit（exec_quota_exhausted）、cpu 可用 ≥ 估算（exec_cpu_exhausted）、
	// wall_spent + WallMs ≤ wall_limit（exec_wall_exhausted）；分配 try_no，插入 environments（kind exec、
	// attempt_id、status creating）、reservation（kind cpu、held）、call_tries（in_flight、env_id、queue_ms），
	// cpu_reserved += 估算，calls in_flight（source exec）、tries_used += 1。以 ReservationID 幂等（E11b 同法）。
	ReserveExec(ctx context.Context, r ReserveExecRequest) (ExecTry, error)
	// MarkExecStarting 在 StartExec 之前复查访问（active、current、desired ≠ cancel）并写 exec_started_at；
	// 访问已失效为 ErrRejected（对应访问码），调用方不得启动。幂等。
	MarkExecStarting(ctx context.Context, t ExecTry) error
	// SettleExec 结算：cpu_reserved -= 估算；CPUKnown ? spent += CPUUsec : spent += 估算；unknown → cpu_unknown += 估算；
	// Started → exec_count_used += 1；wall_spent += WallMs；spent > limit → blocked = true；reservation 只入一个桶；
	// completed/timed_out → calls completed、result_ref、scope_blobs(task) 含结果与全部输出；
	// cancelled/start_failed → calls failed（fail_reason 如上）；按 reservation 状态幂等。
	SettleExec(ctx context.Context, s ExecSettlement) (CallRecord, error)
	LoadExecQuota(ctx context.Context, taskID string) (ExecQuota, error)
}

// 新拒绝码（加入 persistence.Code* 或 call 常量，二者择一并在 edge 映射状态码）。
const (
	CodeExecQuotaExhausted = "exec_quota_exhausted" // 402
	CodeExecCPUExhausted   = "exec_cpu_exhausted"   // 402
	CodeExecWallExhausted  = "exec_wall_exhausted"  // 402
	CodeExecBlocked        = "exec_blocked"         // 402
	CodeExecCancelled      = "exec_cancelled"       // fail_reason，可重试类别（409）
	CodeExecStartFailed    = "exec_start_failed"    // fail_reason（502）
)
```

**启动账本转换（D10）：** held 的 `money` reservation → `budgets.unknown`（不变）；held 的 `cpu` reservation → `exec_quotas.cpu_unknown_usec`、`cpu_reserved_usec -= amount`；该 try 有 `exec_started_at` 时 `exec_count_used += 1`（保守）。锁顺序 `budgets → exec_quotas → calls → reservations`。

**不变量：** I3 拆为 money（budgets）与 cpu（exec_quotas）两组同形 SQL。I12 [A]：`call_tries.exec_started_at` 晚于所属 attempt 的 `attempt_access.revoked_at` → 违例；I12 [B]：已结束 attempt（attempts.status 终态或 access revoked）的 exec 环境在 `--exec-stop-deadline`（默认 60 s，同启动恢复期限）后仍无 `stopped_at` 且未记隔离 → 违例。

- [ ] **Step 1：写失败测试**（真实库，`postgres_test.go` 追加 `Exec` 一节）：首次预留建立 `exec_quotas` 行（取 Policy）、写入 environments/reservation/call_tries；同一 `ReservationID` 重放返回同一 try（模拟 COMMIT 丢失）；`count_used == limit` → `exec_quota_exhausted`（`MarkExecStarting` 不计数、`SettleExec(Started)` 才计数，重放不计数）；CPU 不足、wall 不足、blocked 各一例；access 已撤销 / desired = cancel → 拒绝；上一 try 的环境无 `stopped_at` → `call_in_progress`，`MarkStopped` 后可预留新 try（unknown 重跑路径）；`SettleExec` 五种结局的账本与 calls 状态、结果与输出进入 `scope_blobs(task)`、重复结算幂等；超额后 `blocked`；两个 goroutine 并发预留同一任务不同调用、配额只剩 1 → 只有一个成功；启动转换：混合 money 与 cpu 的 held reservation 各入其账、budgets 不受 cpu 影响、有 `exec_started_at` 的计数 +1，之后 I3 两组均通过；I12 两条 SQL 各对植入的违例报告一次。
- [ ] **Step 2：运行确认失败**：`CI=true AGENTBOX_TEST_DATABASE_URL=… go test ./internal/persistence/postgres/ -run 'Exec|Ledger|Invariant'` → 编译失败。
- [ ] **Step 3：实现**。
- [ ] **Step 4：运行通过**：上述 `-count=3`；全量非 root 一次（含迁移幂等与 `migrate.go` 表清单测试）。
- [ ] **Step 5：提交** `feat(persistence): exec 配额与 exec try 事务（migration 0008），按 kind 的启动账本转换，I3 分账核对与 I12`。

---

### Task 8：gateway/call——exec 调度

**Files:** Create `internal/gateway/call/exec.go`；Modify `internal/gateway/call/call.go`（`Config.Exec *ExecConfig`；`CancelAttempt` 增加 exec 分支；`CancelSubrun` 新方法供 Plan 14）、`internal/gateway/call/call_test.go`（追加）、`internal/archtest/archtest_test.go`。

**Interfaces：**
- Consumes：Task 2 的 `provider.ExecSpec/ExecHandle/ExitStatus/ResourceDiag/OutputFile/SkippedOutput/MaxOutputFiles`；Task 6 的 `ExecGate`（经窄接口）；Task 7 的 `ExecStore` 与 `ExecEnvID`；已有 `Store.BeginCall/FailCall/LoadCall/BlobAuthorized`、`BlobStore`、`jcs`。
- Produces：

```go
// ExecEnvs 是 exec 调度使用的环境操作（Task 10 以 resource.Coordinator + provider 实现）。
type ExecEnvs interface {
	// Create 经 resource coordinator 创建 exec 环境（intent、owner.json、UID 范围）；返回 /in 暂存目录。
	Create(ctx context.Context, r ExecEnvRequest) (inDir string, err error)
	Start(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error)
	// Stop 停止执行树并记录 stopped_at。Recorded 为真才可归还 exec slot；Blocked 表示期限内未确认停止。
	Stop(ctx context.Context, envID string) (ExecStop, error)
	Diag(ctx context.Context, envID string) (provider.ResourceDiag, error)
	OpenOutputs(ctx context.Context, envID string, max int) ([]provider.OutputFile, []provider.SkippedOutput, error)
	// Cleanup 尽力同步清理（Destroy、归还 UID 范围）；失败只记日志，由 cleanup loop 接手（D8）。
	Cleanup(ctx context.Context, envID string) error
}

type ExecEnvRequest struct {
	EnvID, AttemptID string
	MemoryBytes      int64 // 生效值
}

type ExecStop struct{ Stopped, Recorded, Blocked bool }

// ExecSlots 是 admission.ExecGate 的窄接口。
type ExecSlots interface {
	Acquire(ctx context.Context, taskID string) (release func(), err error)
}

type ExecLimits struct{ WallMs, MemoryBytes int64 }

type ExecConfig struct {
	Store        ExecStore
	Envs         ExecEnvs
	Slots        ExecSlots
	Policy       ExecPolicy
	Default, Max ExecLimits
	QueueTimeout time.Duration // 默认 60 s
	CPURate      float64       // cpu.max 速率（核数），默认 1
	CPUMargin    float64       // 默认 0.1
	ImageDigest  string        // rootfs.TemplateDigest(ExecTemplate())
	StopTimeout  time.Duration // 停止执行树的期限，默认 10 s（§19 exit_grace）
}

type ExecInvoke struct {
	TaskID, AttemptID, CallID, SubrunID string
	Body                                []byte
	Retry                               bool
	Supersedes, SupersedeReason         string
}

// Exec 处理 POST /v1/exec（§10.2）。Exec 为 nil 时返回 404 endpoint_not_configured。
func (c *Coordinator) Exec(ctx context.Context, in ExecInvoke) (Result, error)
// CancelSubrun 终止该 sub-run 的全部 exec（Plan 14 接线）。
func (c *Coordinator) CancelSubrun(taskID, subrunID string)
```

**流程（每一步对应一个测试断言的顺序）：**

```
解析请求（拒绝重复键、未知字段；校验 language/code/inputs/limits；截断 limits）
→ 指纹 → Tx1 BeginCall（endpoint /v1/exec，Deadline = QueueTimeout + wall + 30 s）→ 按已有记录分流（§9.4 表；failed{exec_cancelled} 带 Retry 可新建 try）
→ 输入授权：每个 sha BlobAuthorized(task)，否则 FailCall + 403 input_not_authorized（不预留、不建环境）
→ 等待 exec slot（ctx = 调用期限与 QueueTimeout 取早；超时 → FailCall exec_queue_timeout，504）；记录 queue_ms
→ Tx2 ReserveExec（失败映射拒绝码；释放 slot）
→ Envs.Create（失败：Stop 确认无残留后 SettleExec(start_failed, Error=…)；Store/暂时性错误按 retryable 原因 exec_env_unavailable）
→ 暂存：/in/.agentbox/main.py；按 inputs 从 BlobStore 复制（累计字节 > 256 MiB → 中止，按 start_failed 结算，400 inputs_too_large）
→ 进程内检查 attempt 未被取消（I12 [A]，与 CancelAttempt 共用锁）并登记运行中 exec → MarkExecStarting（访问失效 → 不启动，按 cancelled 结算）
→ Envs.Start（stdin 立即关闭）；start_err → start_failed；ErrControlLost → 按 unknown 路径（停止后确认）
→ 并发读取 stdout/stderr（各保留 1 MiB，超额继续读并丢弃，置 truncated）；计时 wall
→ 等待 Wait 返回或 wall 到期或取消
→ Envs.Stop（整个执行树，populated 0）：Blocked → unknown（不归还 slot，不收集，环境留给恢复与清理）
→ 等读取 goroutine 结束（执行树已停，管道必然 EOF）
→ Envs.Diag（cpu.stat、memory.events；失败 → CPUKnown=false）
→ Envs.OpenOutputs(256) → 逐个 Blobs.Put → 组装结果 JSON → Blobs.Put 结果
→ Envs.Cleanup（尽力）→ SettleExec → Stop.Recorded 时归还 slot → 返回
```

**结果 JSON（结果 blob 的内容，也是 HTTP 200 正文）：**

```json
{"status": "completed", "exit": {"code": 0, "signal": 0},
 "diag": {"oom_kill_delta": 0, "oom_observed": false, "cpu_usage_usec": 81234},
 "stdout": "...", "stdout_truncated": false, "stdout_invalid_utf8": false,
 "stderr": "", "stderr_truncated": false, "stderr_invalid_utf8": false,
 "outputs": [{"path": "r/a.txt", "sha256": "…", "size": 12}],
 "skipped_outputs": [{"path": "l", "reason": "symlink"}],
 "queue_ms": 3, "wall_ms": 120, "limits": {"wall_ms": 60000, "memory_bytes": 536870912},
 "image_digest": "…"}
```

**取消：** `CancelAttempt(attemptID, reason)` 对任何 reason：标记该 attempt 为已取消（之后的启动检查失败），取消其全部运行中 exec 的上下文 → 流程进入 Stop → `cancelled`（附实际 CPU）。原有上游 try 的按原因处理不变。

- [ ] **Step 1：写失败测试**（`call_test.go` 追加 `exec` 一节；fake `ExecStore`（内存，语义同 Task 7 契约）、fake `ExecEnvs`（记录操作序列，句柄可脚本化：立即退出、挂起、输出 100 MB、start_err、ErrControlLost、Stop Blocked）、fake `ExecSlots`）：
  1. 请求校验表：`language: "bash"`、code 256 KiB + 1、`../x`、`/abs`、重复 path、`.agentbox/x`、未知字段、`limits.wall_ms` 超上限被截断且截断值进入指纹（截断前后两个请求指纹相同）。
  2. 指纹：输入顺序不同 → 指纹相同；`ImageDigest` 变化 → `409 fingerprint_mismatch`。
  3. 正常路径操作顺序如上（逐项断言 fake 记录的序列），`argv`/`Dir`/`Env` 精确等于 Global Constraints；slot 在 `Stop.Recorded` 之后才归还。
  4. 重放：第二次相同请求 `Replayed`，fake env 无任何操作、`exec_count` 不变。
  5. 超时：Wait 挂起 → wall 到期 → Stop → `timed_out`，journal completed。
  6. E36（单元）：stdout 产生 100 MB → 不挂起，`stdout` 恰 1 MiB 且 `stdout_truncated`；stderr 同时输出也被并发读取。
  7. 取消：运行中 `CancelAttempt(a, "replaced")` 与 `CancelAttempt(a, ReasonCancel)` 各一例 → Stop → `failed{exec_cancelled}`；随后新 attempt 带 Retry 重发 → 新 try 运行；`CancelAttempt` 发生在 `MarkExecStarting` 之前 → `Start` 从未被调用（I12 [A]）。
  8. Stop Blocked → `unknown`、slot 不归还；之后同一调用重发 → `call_in_progress`（fake store 检查前一 try 环境未停止）。
  9. start_err → `start_failed`、`Started=false`（不计数）；ErrControlLost → Stop 确认后 `unknown`。
  10. 输入未授权 → 403，无 Reserve、无 Create。
  11. 每任务第 3 个并发 exec 排队，排队超时 → `504 exec_queue_timeout`，无预留。
  12. 输出：OpenOutputs 返回的每个文件被 `Put`、全部关闭（fake 检查）；`skipped_outputs` 原样进入结果。
- [ ] **Step 2：运行确认失败**：普通 `go test ./internal/gateway/call/ -run Exec` → 编译失败。
- [ ] **Step 3：实现**；archtest：`forbidDirect("internal/gateway/call", [net/http, internal/provider/local, internal/resource, internal/admission, internal/persistence/postgres])`。
- [ ] **Step 4：运行通过**：普通 `go test -count=3 ./internal/gateway/call/ ./internal/archtest/`；全量非 root 一次。
- [ ] **Step 5：提交** `feat(gateway): exec 调度——单次无状态 exec 环境、配额、journal 重放、取消与超时（§10）`。

---

### Task 9：edge `/v1/exec`、`/v1/budget` exec 配额、SDK `exec()`

**Files:** Modify `internal/gateway/edge/edge.go`、`internal/gateway/edge/edge_test.go`、`worker/agentbox_worker/gateway.py`、`worker/tests/test_sdk.py`。

**Interfaces：**
- edge `Calls` 接口增加 `Exec(ctx context.Context, in call.ExecInvoke) (call.Result, error)` 与 `ExecQuota(ctx context.Context, taskID string) (call.ExecQuota, bool, error)`（`bool` 为是否已有配额行）。
- `/v1/exec`：仅 POST；必须 `X-Agentbox-Call-Id`；`X-Agentbox-Retry`、`X-Agentbox-Supersedes` 同计费端点；body ≤ 4 MiB（已有）；与计费端点相同，以 binding 的上下文调用（Worker 断开不取消 exec）。拒绝码到状态：402（配额类）、403（`input_not_authorized`）、409（`call_in_progress`、`exec_cancelled`、`fingerprint_mismatch`）、502（`exec_start_failed`）、503（`exec_env_unavailable`）、504（`call_deadline_exceeded`、`exec_queue_timeout`）。
- `/v1/budget` 响应增加 `"exec": {"count_limit","count_used","cpu_limit_usec","cpu_available_usec","wall_limit_ms","wall_spent_ms","blocked"}`（尚无配额行时为策略值、用量 0）。
- Python SDK：

```python
def exec(
    self,
    step_id: str,
    code: str,
    *,
    inputs: Sequence[tuple[str, str]] = (),  # (sha256, path)
    wall_ms: int | None = None,
    memory_bytes: int | None = None,
    retry: bool = False,
) -> GatewayResult:
    """POST /v1/exec；call_id 为 <root>/<step_id>/exec/<n>。超时 = 排队上限 + wall + 30 s + 余量。"""
```

- [ ] **Step 1：写失败测试**：edge——路由与方法（GET → 405）、缺 call id → 400、各拒绝码的状态映射、Worker 断开不取消 `Exec`（沿用 `TestClientDisconnectDoesNotCancelInvoke` 的 fake）、`/v1/budget` 含 exec 字段；SDK——对 `conftest.py` 的 fake AF_UNIX Gateway：请求体与头、call id 递增与 checkpoint 快照恢复后连续、错误码映射到既有 `GatewayError` 子类、`retry=True` 带头。
- [ ] **Step 2–4：实现并通过**：普通 `go test -count=3 ./internal/gateway/edge/`；Windows `cd worker && uv run pytest -q --basetemp=<WT>/.pytest-tmp tests/test_sdk.py`；WSL `AGENTBOX_REQUIRE_UNIX_TESTS=1` 下 AF_UNIX 用例（exec-context 的方法）；`uv run ruff check . && uv run ruff format --check . && uv run lint-imports`。
- [ ] **Step 5：提交** `feat(gateway): /v1/exec 端点与 exec 配额查询；SDK GatewayClient.exec()`。

---

### Task 10：装配、标志、启动恢复、清理候选、verify-invariants、inspect

**Files:** Modify `internal/app/app.go`、`internal/app/adapters.go`（`execEnvAdapter`）、`internal/app/app_test.go`、`cmd/agentbox/server.go`、`deploy/agentbox.env.example`、`internal/persistence/postgres/cleanup.go`（候选条件）、`internal/persistence/postgres/apiread.go`（inspect 的 exec 列）、`internal/cli/cli.go`（inspect 输出）、`internal/invariants/invariants.go`、`internal/invariants/invariants_test.go`（I12 [Q]）；若 `internal/reconcile` 的停止阶段不覆盖 `kind = exec` 的环境，Modify `internal/reconcile/reconcile.go`、`reconcile_test.go`（先读代码确认，覆盖则不改）。

**装配：**
- server 标志：`--exec-slots`（4）、`--exec-per-task`（2）、`--exec-count-limit`（50）、`--exec-cpu-seconds`（600）、`--exec-wall-limit`（1800s，每任务累计）、`--exec-wall-default`（60s）、`--exec-wall-max`（300s）、`--exec-memory-default`（512MiB）、`--exec-memory-max`（1GiB）、`--exec-queue-timeout`（60s）。`--exec-slots 0` 关闭 exec（`/v1/exec` → 404 `endpoint_not_configured`）。
- `prepareIsolation` 增加：`rootfs.ExecTemplate().EnsureExec()`（失败拒绝启动，报告缺失路径）、计算 `TemplateDigest`。
- `execEnvAdapter`：`Create` = `resource.Coordinator.CreateEnv(EnvRequest{Kind: exec, Template: "exec", Limits: …, Mounts: {OutBytes: 64 MiB}})` 返回 `EnvInfo.InDir`；`Stop` = `StopEnv`；`Diag`/`Start`/`OpenOutputs` 直达 provider；`Cleanup` = coordinator 对单个环境执行 `cleanupEnv`（导出 `CleanupNow(ctx, envID)`，在 `internal/resource/cleanup.go` 增加；若 Task 5 已改该文件，在其后提交）。
- cleanup 候选（`ListCleanupCandidates` SQL）：`kind = 'exec'` 的环境只要求 `stopped_at` 已记录（D8）。
- 启动恢复（§14.1）：先于 Gateway 打开，exec 环境与其他环境一样被停止并记录 `stopped_at`（核对 reconcile 已覆盖）；Task 7 的账本转换把在途 exec try 转为 unknown；`stop_blocked` 的 exec 环境以 `ExecGate.Occupy` 占用 slot。
- verify-invariants：I12 [Q]：`cleanup_state = done` 的 exec 环境无挂载、cgroup、目录（复用 I1 的扫描，按 kind 报告为 I12）。
- inspect：exec 调用的每个 try 显示 `env_id`、`queue_ms`、`wall_ms`、`cpu_usec`、`exec_started_at`、结局。

- [ ] **Step 1：写失败测试**（`app_test.go`，fake provider + 真实 PG）：启用 exec 后经 edge 完成一次 exec（fake provider 的 exec 句柄脚本化）；`--exec-slots 0` → 404；exec 模板缺 python3 → `Run` 返回错误；**E37 确定性版本**：exec 运行中模拟 server 崩溃（停止 app，不调用正常关闭）→ 重启 → 恢复阶段停止旧 exec 环境并记录 `stopped_at` 之后 Gateway 才接受连接（以 fake provider 的操作序列断言）、旧 try 为 unknown、`cpu_unknown` 增加；新 attempt 重发同一调用 → 新 try 运行完成；若任务在崩溃前已被用户取消 → 恢复后任务 `cancelled`、该调用不再出现新 try；cleanup loop 回收 exec 环境而不等待 attempt 判决；inspect 输出含 exec 列。
- [ ] **Step 2–4：实现并通过**：普通 `go test -count=3 ./internal/app/ ./internal/invariants/ ./internal/cli/ ./cmd/...`；PG 用例 `CI=true`；全量非 root 一次。
- [ ] **Step 5：提交** `feat(server): 装配 exec（标志、exec 模板检查、环境适配器）、启动恢复与清理覆盖 exec 环境、I12 与 inspect`。

---

### Task 11：Worker——`run_python` 工具与 sim-worker `exec` 操作

**Files:** Create `worker/agentbox_worker/tools/run_python.py`；Modify `worker/agentbox_worker/tools/__init__.py`（注册，Plan 13 的注册表）、`worker/tests/test_tools.py`（Plan 13 的工具测试文件，追加）、`worker/sim_worker/app.py`、`worker/tests/test_process.py`（或 sim-worker 现有测试所在文件，追加）。

**Interfaces：**
- Consumes（Plan 13 工具包；以其合入时的实际签名为准，不一致时报告 NEEDS_CONTEXT，不自行发明）：`Tool`（`name`、`description`、`parameters`（JSON schema）、`run(args, ctx) -> ToolResult`）、`ToolResult`（给模型的文本 + 结构化数据）、`ToolContext`（含 `gateway: GatewayClient`、当前 `step_id`）、注册表注册方式。Task 9 的 `GatewayClient.exec()`。
- Produces：

```python
class RunPython(Tool):
    name = "run_python"
    description = (
        "在隔离沙箱中运行一段 Python 3 代码（无网络、无状态、每次全新环境）。"
        "用于计算、数据处理与核对数字。可读取 /in 下的输入文件，写入 /out 的文件会作为输出返回。"
    )
    parameters = {
        "type": "object",
        "properties": {
            "code": {"type": "string", "maxLength": 262144},
            "inputs": {"type": "array", "maxItems": 64, "items": {
                "type": "object",
                "properties": {"sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                               "path": {"type": "string"}},
                "required": ["sha256", "path"], "additionalProperties": False}},
            "timeout_s": {"type": "integer", "minimum": 1, "maximum": 300},
        },
        "required": ["code"], "additionalProperties": False,
    }
```

  `run`：调用 `ctx.gateway.exec(ctx.step_id, code, inputs=…, wall_ms=timeout_s*1000)`；给模型的文本：状态、退出码/信号、stdout 前 4 KiB 与末 1 KiB（中间省略标注）、stderr 末 2 KiB、输出文件列表（path、size、sha256）、截断与 OOM 提示；结构化数据为完整结果（供界面的原始请求/响应 ⟨/⟩）。Gateway 错误（配额、取消、超时）转为给模型的明确文本，不抛出到 Agent 循环（与 Plan 13 其他工具的错误约定一致）。**不计入每轮 30 次工具额度**（M4 设计 D3：只计 `web_search`、`web_fetch`）。
- sim-worker：新操作 `{"op": "exec", "step_id": "...", "code": "...", "inputs": [[sha, path], ...], "wall_ms": N, "expect_status": "completed"}`；结果 blob 与输出 sha 加入 `held.refs`（与 chat/search/fetch 相同的 `_gateway_call` 路径）；`expect_status` 不符时以失败结束任务（e2e 判定用）。

- [ ] **Step 1：写失败测试**：工具——schema 由注册表生成函数调用定义时与上方一致；对 fake Gateway：正常结果的文本格式（截断规则）、`timed_out`、`exec_quota_exhausted` → 文本说明且不抛出、`inputs` 透传；`run_python` 不导入 `subprocess` 等（ruff TID251 与 lint-imports 已覆盖，测试不重复）。sim-worker——`exec` 操作对 fake Gateway 发出正确请求、refs 进入下一个 checkpoint、`expect_status` 不符时任务失败。
- [ ] **Step 2–4：实现并通过**：Windows `uv run pytest -q --basetemp=<WT>/.pytest-tmp`；WSL AF_UNIX 用例；`uv run ruff check . && uv run ruff format --check . && uv run lint-imports`。
- [ ] **Step 5：提交** `feat(worker): run_python 工具（经 Gateway /v1/exec）与 sim-worker exec 操作`（若 Plan 13 工具包尚未合入，sim-worker 部分可先单独提交：`feat(sim-worker): exec 操作`，工具部分随后提交）。

---

### Task 12：真实沙箱 e2e、文档、CI 与服务器验收

**Files:** Modify `tests/e2e/e2e_test.go`（追加）、`README.md`、`docs/design/2026-10-03-v0.2-first-release-design.md`（§10、§19、§6 以"执行中修订（Plan 15）"标注写回 D1–D16 中改变规格文本的决定）、M4 计划索引（若协调者已建立 `docs/plans/2026-10-06-m4-index.md`，增加 Plan 15 行）；Create `docs/evidence/2026-10-xx-m4-exec-hardening.md`。`scripts/ci/allowed-skips.txt` **不新增条目**。

**e2e（全部 `newRealHarness`，root，sim-worker 的 `exec` 操作驱动）：**

| 用例 | 内容 | 必须成立 |
|---|---|---|
| `TestRealExecViaGateway` | 任务先 `fetch`（fake upstream）得到 blob，再 `exec` 以其为输入、写 `/out/result.csv`；第二个 `exec` 以前者输出为输入 | 两次 `completed`；输出可经 `/blobs/{sha}` 读取；`exec_count_used = 2`；任务成功；I1–I16 通过 |
| `TestRealE34ExecViaGateway` | exec 代码在真实链路中尝试：连接 `/run/agentbox/gateway.sock`、遍历 socket 文件、`socket(AF_INET)` 连 server 的 API 端口、读 `/workspace`、`/opt/agentbox`、宿主数据目录路径 | 全部失败（逐项写入 stdout，宿主断言）；编排 Worker 本身仍能调用 Gateway |
| `TestRealE35BackgroundWriter` | 主进程 fork 后台进程持续追加 `/out/log`，主进程立即退出 | 收集发生在执行树清空之后：两次读取 `log` 的 sha 与结果中的一致，`populated 0` 先于收集（以环境 cgroup 不存在 + 文件不再增长断言） |
| `TestRealE36StdoutFlood` | 输出 100 MB 到 stdout | 不挂起；`stdout_truncated`；任务按时完成 |
| `TestRealE37ServerKilledDuringExec` | exec `sleep` 中 SIGKILL server（`TestRealE5…` 的方法），重启 | 旧 exec 环境确认停止（`stopped_at`）后才出现新 try；新 try 完成；另一例：先取消任务再杀 server → 恢复后无新 try、任务 `cancelled` |
| `TestRealE38ExecPressure` | ① `/out` 写满 + 进程内存逼近 `memory.max`；② 创建 2000 个文件；③ 同时 4 个任务各 2 个 exec 打满 slots | ① 环境内 OOM 或 `ENOSPC`，结果如实报告（`oom_observed`）；② `ENOSPC`；③ 第 9 个起排队；压力期间 `GET /tasks/{id}` 与 `GET /status` 在 2 s 内响应；全部环境回收，I1、I11、I12 通过 |
| `TestRealE39UIDReuseViaTasks` | 以 1 段 UID 范围运行（`--uid-count 1` 等价配置）连续两个任务，各含 exec | 第二个环境复用范围；无可访问残留；`UIDFiles` 为空；植入残留的变体 → 范围隔离、报警（I8） |

另：E34 两类环境的套件（Task 3）与 `TestIsolationAcceptance16_2` 在本任务的验收中重跑并记录。

- [ ] **Step 1：写失败测试**（上表）。
- [ ] **Step 2：运行确认失败**：root `CGO_ENABLED=0 go test -p 1 ./tests/e2e/ -run 'TestRealExec|TestRealE3[4-9]'`。
- [ ] **Step 3：补齐实现缺口**（只修本计划范围内的缺陷；涉及他人文件时报告协调者）。
- [ ] **Step 4：运行通过**：root 上述用例 `-count=1` 两轮；root 全量 `CGO_ENABLED=0 go test -p 1 -count=1 ./...`；非 root 全量；Python 全部检查；CI linux-integration 全绿且 citestjson 无未允许的 skip。
- [ ] **Step 5：文档**：README 增加"代码执行沙箱"一节（保证与边界：单次无状态、无网络、无 Gateway、配额与默认值、exec 内存规划公式、不保证 exactly-once、重跑可能结果不同、容器级隔离、E34 是回归测试不是无逃逸证明）；规格写回；证据文件记录命令、输出摘要、内核版本（WSL2 与服务器）、E34 逐项结果。
- [ ] **Step 6：提交** `test(e2e): exec 真实链路与 E34–E39；docs: exec 沙箱说明、规格执行中修订与验收记录`。

## 联合验收

全部在协调者的集成 worktree（`m4-batch` 合入本计划全部任务后）上执行；**root 测试串行**，同一时刻只运行一个 root 命令。

1. **WSL2**（内核 6.6.87.2，x86_64）：
   - 非 root：`go test -count=1 ./...`；Python `uv run pytest -q`、ruff、lint-imports。
   - root：`CGO_ENABLED=0 go test -p 1 -count=1 ./internal/sandbox/ ./internal/provider/local/ ./internal/rootfs/ ./internal/hostcheck/ ./internal/cgroup/`，然后 `CGO_ENABLED=0 go test -p 1 -count=1 ./tests/e2e/`，然后 root 全量 `./...` 一次。
   - `-v` 记录 `TestE34PermissionBoundary` 与 `TestIsolationAcceptance16_2` 的逐项输出。
2. **Linux 服务器**（CVM ins-22zj0xj5，经 SSH 隧道；先 `agentbox doctor` 记录内核、架构、hostcheck 结果与 Warnings）：同第 1 条的 root 序列（串行）；E34、16.2、E35–E39 无环境 skip。
3. **CI**：correctness、linux-integration（零未允许 skip）、python、web 全绿。
4. **服务器真实链路**：部署后以运维 Bearer 提交一个 sim-worker 任务（含 `fetch` + 两次 `exec`）并 `inspect`；以普通用户在对话中让 Agent 用 `run_python` 计算一个数字（Plan 13 合入后），记录工具行与原始请求/响应；`verify-invariants --quiescent` 通过。
5. 结果写入 `docs/evidence/2026-10-xx-m4-exec-hardening.md`（不含 token、cookie、密钥），并作为验收记录追加到本计划末尾。

## 自查记录

- **规格覆盖**：§10.1 模型、指纹、请求/响应 → Task 8（D3、D5、D12）；§10.2 流程 → Task 2（`/in`、`/out`、OpenOutputs）、Task 8（顺序）、Task 10（清理）；§10.3 配额 → Task 6、7、8；§10.4 结果语义 → Task 8（D6、D7）、Task 10/12（E37）；§4.5 exec 隔离配置 → Task 1、2；UID 范围回收 → Task 5；seccomp → Task 1；§4.6 门槛（不扩展能力、无 ptrace、CGO=0、hostcheck）→ Task 1、4；§16.2 → Task 1/3 重跑；I11 → Task 5；I12 → Task 7、10；E34 → Task 3、12；E35、E36 → Task 8、12；E37 → Task 10、12；E38 → Task 2、12；E39 → Task 5、12；G1、G3 的 exec 部分 → Task 3、12。
- **不在本计划**：E28–E33、E47、E49（Plan 12 会话）；E40–E45（Plan 14 sub-run 与 Plan 12 暂停）；§16.5 性能报告（含"exec 创建 → 退出"测量）与发布门槛——M4 收尾单独进行；notebook 式有状态 exec（规格非目标）；aarch64 支持主张。
- **约束复核**：没有任务涉及 ptrace 启动检测或其实验，没有任务改变 init 能力集（Task 1 只加回归锁与注释）；E34 的 ptrace 项是 seccomp 拒绝探针。
- **并行性复核**：可立即开始的 1、4、6、7 文件互不重叠；2 与 3 不同包；与 Plan 12–14 的重叠文件（`postgres/gateway.go`、`call/call.go`、`edge/edge.go`、`app/*`、`resource/cleanup.go`、`invariants.go`、`migrate.go`）均在任务表中标注，由协调者按合入顺序 rebase。
- **类型一致性**：`ExecTry`、`ExecSettlement`、`ExecOutcome`、`ExecEnvID` 在 Task 7 定义、Task 8 使用；`OutputFile`/`SkippedOutput`/`EnvInfo.InDir` 在 Task 2 定义、Task 8/10 使用；`ExecGate` 在 Task 6 定义，Task 8 以 `ExecSlots` 窄接口使用、Task 10 装配。
