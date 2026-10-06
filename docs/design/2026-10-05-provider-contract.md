# 提案：Provider 的 Go 接口契约（M1）

> 状态：**已审阅（第一轮三条更正已并入：第 3 节 Create 只返回完整环境、第 4 节按操作的后置条件判断、第 5 节执行与停止的并发边界）**，未实现。据此编写 Plan 2、5、6 中不依赖 Plan 1B 的任务。
> 依据：规格 §4.1（已固定 `StartExec`/`ExecHandle`/`ExitStatus`/`ResourceDiag`）、§4.2–§4.5、§8.3、§14.1 第 5 步与"资源归属"、§16.2；代码组织设计 §2（`provider/local` 的六个操作）、§4（消费者定义窄接口）。
> 只补齐规格未写明的部分：参数、返回值、取消、错误分类、幂等与资源所有权。不改变规格已定内容。

## 1. 包与依赖方向

与 `internal/persistence`（错误契约）+ `internal/persistence/postgres`（实现）的模式相同：

- **`internal/provider`（新增，只有类型与错误，无实现、无 I/O）**：`EnvSpec`、`ExecSpec`、`ExitStatus`、`ResourceDiag`、`EnvInfo`、`ScanReport` 与错误哨兵。
- **`internal/provider/local`**：实现，组合 `cgroup`、`rootfs`、`sandbox`（代码组织 §2）。
- **消费者窄接口**：`internal/resource` 声明 `Provider`（Create、Stop、Destroy、List、Scan、ResourceDiag）；`internal/runner` 声明 `Starter`（StartExec）。消费者只导入 `internal/provider`，不导入 `provider/local`（archtest 增加这条规则）。

需审阅的结构决定：新增 `internal/provider` 包（代码组织 §2 只列出 `provider/local`）。

## 2. 类型

```go
package provider

type EnvKind string // "task" | "session" | "exec"

// EnvSpec 描述一个环境；所有字段由控制面在调用前确定并持久化（intent、UID 范围）。
type EnvSpec struct {
	EnvID     string
	InstallID string   // 写入 owner.json，决定 cgroup 路径 agentbox-<install_id>/env-<env_id>
	Kind      EnvKind
	UIDBase   uint32   // UID 范围起点（resource.AssignUIDRange 的结果）
	UIDSize   uint32   // 默认 4096
	Template  string   // 只读 rootfs 模板标识
	Limits    Limits
	Mounts    Mounts
}

type Limits struct {
	MemoryMax  int64 // memory.max；memory.swap.max 固定为 0
	PidsMax    int64
	CPUQuotaUs int64 // cpu.max 的 quota；period 固定 100000
	NoFile     uint64
	FSize      uint64 // 仅 exec 环境（RLIMIT_FSIZE）；其他为 0
	TmpBytes   int64  // /tmp、/run tmpfs 限额
}

// Mounts 按 §4.5：编排环境有 workspace 与 Gateway socket；exec 环境有 /in（只读）与 /out（tmpfs）。
type Mounts struct {
	Workspace     string // 宿主目录，挂到 /workspace（attempt/session/out 子目录由宿主创建并 chown）；exec 为空
	GatewaySocket string // 宿主 socket 路径，挂到 /run/agentbox/gateway.sock；exec 为空
	In            string // 仅 exec：只读输入目录
	OutBytes      int64  // 仅 exec：/out tmpfs 大小
}

// ExecSpec 描述环境内一次执行。workload 身份固定为映射 uid/gid 1000，seccomp 配置由环境类型决定
// （orchestrator 或 exec）——两者都不由调用方指定。
type ExecSpec struct {
	ExecID string   // 调用方生成，用于控制通道 start/start_ack 关联与日志
	Argv   []string
	Env    []string
	Dir    string
}

type ExitStatus struct{ Code int; Signal syscall.Signal }                              // §4.1
type ResourceDiag struct{ OOMKillDelta uint64; OOMObserved bool; CPUUsageUsec uint64 } // §4.1

type ExecHandle interface { // §4.1，原样
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait() (ExitStatus, error)
	Terminate(grace time.Duration) error
}

// EnvInfo 是 List 返回的一个完整环境（owner.json 属于本安装）。
type EnvInfo struct {
	EnvID    string
	Kind     EnvKind
	Complete bool // 各层齐全且 init 就绪；false 即 Create 会返回 ErrIncomplete 的残留
	Running  bool // 环境 cgroup 存在且 populated 为 1
}

// ScanReport 是独立原始扫描的结果（§14.1 第 5 步），逐层报告。
type ScanReport struct {
	Items []ScanItem
}

type ScanItem struct {
	Layer string // "env_dir" | "mount" | "cgroup" | "listener" | "uid_files"
	Path  string
	EnvID string // 能识别时填写
	Owner Owner
}

type Owner int // OwnedComplete | OwnedPartial | Foreign | Unknown
```

## 3. 操作

```go
Create(ctx, EnvSpec) (EnvInfo, error)
StartExec(ctx, envID string, ExecSpec) (ExecHandle, error) // §4.1
Stop(ctx, envID string) error
Destroy(ctx, envID string) error
List(ctx) ([]EnvInfo, error)
Scan(ctx) (ScanReport, error)
ResourceDiag(ctx, envID string) (ResourceDiag, error)
```

| 操作 | 语义与成功的后置条件 | 幂等 | 取消 |
|---|---|---|---|
| `Create` | 建目录 `<data>/envs/<env_id>` → 写 `owner.json{install_id, env_id, spec_hash}`（在任何挂载之前）→ cgroup → rootfs 与挂载 → 在环境 cgroup 内启动 init。**成功的后置条件：环境完整、init 就绪，可 `StartExec`** | 按 `env_id`：已存在、owner 属于本安装、`spec_hash` 一致**且完整、init 就绪** → 返回现状；owner 属于本安装但不完整或 init 未就绪（中断的创建、已停止的环境）→ `ErrIncomplete`，**不补完、不认领**；`spec_hash` 不同 → `ErrConflict`；目录存在但 owner.json 缺失、损坏或属于其他安装 → `ErrForeign` | 有界；ctx 结束时停止推进并返回 ctx 错误，返回后不留后台操作；已建的部分留在原处，由 coordinator 按第 5 节清理后重建 |
| `StartExec` | 经控制通道启动一次 workload（§4.2–§4.3）。成功的后置条件：`start_ack` 已收到，进程在环境 cgroup 内运行 | **不幂等**：每次调用启动一个新进程。调用方以 attempt 状态保证至多一次；结果不明（`ErrControlLost`、ACK 前 ctx 结束）时不重试，按 §4.2 停止并拆除环境 | ctx 在 ACK 前结束 → 返回 ctx 错误；控制连接视为不可信 |
| `Stop` | 关闭执行闸门（第 5 节）→ `cgroup.kill` → 等待 `populated 0`。不卸载、不删除（§4.4 停止与清理分离）。**成功的后置条件由权威检查确认：环境 cgroup 不存在，或存在且 `populated 0`**（init 与全部 workload 都在环境 cgroup 内启动，进程不能离开该 cgroup，因此这两种情况都证明执行树不存在） | 是 | 有界；期限内未确认 → `ErrStopUnconfirmed`（`stop_blocked`），不报告成功 |
| `Destroy` | 前置条件：`Stop` 的权威检查成立（否则 `ErrNotStopped`）。逐层清理它负责的资源：数据目录下属于该环境的挂载 → 环境 cgroup → 环境目录（含 owner.json）。**成功的后置条件：逐层核对，三层都已不存在** | 是：重做时已不存在的层跳过，但成功前仍逐层核对 | 有界；中断后可重做 |
| `List` | 本安装的环境（owner.json 的 install_id 等于本安装），附完整性 | 只读 | 有界 |
| `Scan` | 独立原始扫描：环境目录（含无 owner.json 的）、数据目录下的挂载、`agentbox-*` cgroup（含其他 install_id 的）、listener socket、UID 范围文件属主；逐项分类 | 只读 | 有界 |
| `ResourceDiag` | 读取环境 cgroup 的 OOM 与 CPU 统计（§4.1） | 只读 | 有界 |

所有操作：不写 Store、不解析 JSONL（§4.1）。

**残留与认领**：`mkdir` 与写入 owner.json 之间存在失败窗口，因此残留目录**可能没有 owner.json**。provider 不认领任何不能证明属于本安装的资源：无 owner.json、owner.json 损坏或属于其他安装的目录由 `Scan` 报告为 `Unknown`/`Foreign`，进入 §14.1 的隔离流程（写 `quarantined_resources`、报警、不自动销毁）。owner.json 属于本安装的不完整环境（`ErrIncomplete`）由 coordinator `Stop` → `Destroy` 后以新的创建重建，不在原环境上继续——与"不接管存活环境"一致。

## 4. 错误分类（`internal/provider`）

| 错误 | 含义 | 调用方处理 |
|---|---|---|
| `ErrNotFound` | 本次操作需要的那一层资源不存在：`StartExec` 找不到可用的 init 或控制连接；`ResourceDiag` 找不到环境 cgroup | **只说明本操作无法进行，不说明其他层是否已清理**。是否停止由 `Stop` 的权威检查判断，是否清理完成由 `Destroy` 的逐层核对判断 |
| `ErrIncomplete` | 同名环境属于本安装，但不完整或 init 未就绪 | coordinator `Stop` → `Destroy` 后以新的创建重建；不补完 |
| `ErrConflict` | 同一 `env_id` 已存在且 `spec_hash` 不同 | 编程错误或身份复用；不重试 |
| `ErrForeign` | 目录存在但 owner.json 缺失、损坏或属于其他安装 | 不自动销毁：写 `quarantined_resources` 并报警（§14.1 表） |
| `ErrStopping` | 环境的执行闸门已关闭（第 5 节），不再接受新的执行 | attempt 不启动；按停止流程处理 |
| `*StartError{Reason}`（`ErrStartFailed`） | init 回复 `start_err`：启动序列某步失败，workload 未运行 | attempt 失败（启动失败类）；`Reason` 文本由 Plan 1B 的结论确定，本契约只规定"workload 未运行" |
| `ErrControlLost` | 控制连接断开：ACK 前（§4.2，启动结果未知）或 ACK 后由 `Wait` 返回（退出状态未知） | attempt 按 `control_lost` 分类（§14.3），停止并拆除环境；不重试 `StartExec` |
| `ErrStopUnconfirmed` | 期限内未确认执行树清空 | `stop_blocked`，占用槽位，阻止替代执行（§8.2） |
| `ErrNotStopped` | `Destroy` 的前置条件不成立 | 先 `Stop` |
| ctx 错误 | 调用方取消或期限到期 | 按第 3 节各操作的取消语义处理 |
| 其他（包装的宿主错误） | 暂时性宿主故障（EBUSY、ENOSPC 等） | coordinator 退避重试；cleanup 记 `cleanup_error` |

恢复代码不得把"某一层不存在"当作"全部不存在"：停止与清理完成只能分别来自 `Stop` 与 `Destroy` 的成功返回。

## 5. 执行与停止的并发边界

`Create`、`Stop`、`Destroy` 由 resource coordinator 按环境串行调用；`StartExec` 由 runner 直接调用，不经 coordinator。两条路径的约束由 **provider 内每个环境的执行闸门** 统一执行，而不依赖调用方之间的约定：

- 闸门状态：`open`（`Create` 成功后）→ `closed`（`Stop` 开始时，不可逆）。闸门是 provider 进程内状态；进程重启后不重建为 `open`，因此重启前创建的环境不能再 `StartExec`（与"不接管存活环境"一致）。
- **仲裁顺序**：`StartExec` 在闸门锁内检查 `open` 并登记一个在途启动，再在锁外发送 `start`；`Stop` 在闸门锁内把闸门置为 `closed`，此后到达的 `StartExec` 立即返回 `ErrStopping`。`Stop` **不等待**在途启动：它在关闭闸门之后执行 `cgroup.kill`，在途启动所创建的进程（由环境 cgroup 内的 init 创建，必然在该 cgroup 内）随之被终止，在途的 `StartExec` 以 `ErrControlLost` 或 `StartError` 返回。
- **停止成功后没有迟到的进程**：环境内唯一能创建进程的是 init；`Stop` 成功意味着 cgroup 已空，init 已不存在，控制连接已断开。`cgroup.kill` 对与之并发的 fork 同样生效。之后到达的 `StartExec` 被闸门拒绝，不触碰任何资源。
- **未确认停止不能清理或归还**：`Destroy` 以 `Stop` 的权威检查为前置条件；coordinator 只在 `Stop` 成功后写 `stopped_at`；UID 范围归还要求 `stopped_at` 已记录且 `cleanup_state = done`（Plan 4 已在事务内强制）。
- **测试**（Plan 2，确定性）："start 在途时 stop"——用测试钩子让 `StartExec` 在登记在途启动之后、发送 `start` 之前阻塞；执行 `Stop` 并确认成功；放行 `StartExec`：它必须返回 `ErrControlLost`/`ErrStopping`/`StartError` 之一，且环境 cgroup 中没有任何进程；之后的 `StartExec` 返回 `ErrStopping`。
- **契约一致性测试**：`internal/provider/providertest` 提供一组按本契约编写的行为测试（第 3 节的后置条件、第 4 节的错误、本节的闸门语义），同时运行在 Plan 5/6 使用的内存 fake 与 Plan 2 的 `provider/local` 上（后者需要 Linux 与 root），使两侧对契约的理解由同一组测试固定。

## 6. 资源所有权

- **命名即归属**：目录 `<data>/envs/<env_id>`、cgroup `agentbox-<install_id>/env-<env_id>`、listener `<data>/envs/<env_id>/gw.sock`；owner.json 在任何挂载之前写入（但不早于 `mkdir`，见第 3 节"残留与认领"）。清理不猜测路径（§14.1"资源归属"）。
- **intent 先于物理操作**：coordinator 在调用 `Create` 前以 `resource.RecordIntent` 持久化 intent，调用后 `ResolveIntent`；provider 不知道 intent。
- **UID 范围由控制面分配**（`resource.AssignUIDRange`），经 `EnvSpec` 传入；归还的前置条件由 Plan 4 的 `ReleaseUIDRange` 与 coordinator 的文件/挂载核对共同保证（§4.5）。
- **不接管存活环境**：重启后 `List`/`Scan` 只用于核对与停止（§14.1 第 6 步）。

## 7. 依赖 Plan 1B 的部分（不在本契约中固定）

本契约不暴露降权、挂载方式、附加组、securebits、seccomp 安装的任何参数；它们是 `provider/local` 与 `sandbox` 的内部实现，由 Plan 1B 的结论决定（B1–B5）。契约中唯一与 1B 相关的点是 `StartError.Reason` 的取值（B6），已约定为不透明文本。因此 Plan 5、6 可按本契约以内存 fake 实现 `Provider`/`Starter` 编写与测试；Plan 2 中启动序列以外的部分可按本契约实现。

## 8. 审阅记录

第一轮（2026-10-05）保留了 `internal/provider` 共享类型与错误、消费者声明窄接口的划分，并要求三处更正，均已并入：

1. `Create` 只把完整且 init 就绪的环境作为成功返回；同名残留为 `ErrIncomplete`，由 coordinator 清理后重建；无 owner.json 的目录不认领，进入扫描与隔离流程（第 3 节）。
2. 删除"`ErrNotFound` 视为已停止/已清理"；停止与清理完成只由 `Stop` 的权威检查与 `Destroy` 的逐层核对确认（第 3、4 节）。
3. 固定执行与停止的并发边界：provider 内的执行闸门、仲裁顺序、停止后无迟到进程、未确认停止不能清理或归还，以及确定性的"start 在途时 stop"测试（第 5 节）。

## 9. 执行中修订（Plan 15）

Plan 15（独立 exec 沙箱，规格 §10）对 exec 环境补充以下契约，决定见计划的 D1、D2、D5：

- **`EnvSpec.Validate`**：exec 环境须给出 `Mounts.OutBytes > 0`；`Mounts.In` 可以为空（由 provider 建立输入暂存目录）。
- **`EnvInfo.InDir`**：只对 exec 环境且 `Mounts.In` 为空时非空，是 provider 建立的输入暂存目录 `<data>/envs/<env_id>/in` 的宿主路径（root 属主、0755）。调用方在 `StartExec` 之前写入（文件 0444、目录 0755），沙箱内只读可见于 `/in`。幂等 `Create`（完整且 spec 一致）返回同一路径；目录随环境目录由 `Destroy` 删除。
- **环境目录权限**：`<data>/envs` 为 0711；exec 环境的目录为 0711（init 在 user namespace 中以映射 root 运行，只有"其他人"的权限，须能经过它们到达 `in/` 与 `out/`；数据目录本身由装配以 0711 建立），`owner.json` 仍 0600；编排环境的目录仍 0700。
- **宿主侧 `/out`**：`Create` 在写 owner.json 之后、启动 init 之前建立 `<envdir>/out` 并挂载 tmpfs（`size=OutBytes,nr_inodes=1024,mode=0700,uid=gid=UIDBase+1000`，`nosuid,nodev`），init 把它可写 bind 到沙箱内的 `/out`。页面按写入者计入环境 cgroup（与匿名内存合计受 `memory.max` 约束）。挂载之后的任何失败都使环境成为残留（`ErrIncomplete`），由 `Stop` → `Destroy` 清理。执行树停止后挂载仍在，`/out` 的内容可供收集。`Scan` 把它作为该环境的 `mount` 层报告，不新增层。
- **`OpenOutputs(ctx, envID, max) ([]OutputFile, []SkippedOutput, error)`**：前置条件是 `Stop` 的权威检查成立（否则 `ErrNotStopped`）；环境目录不存在 → `ErrNotFound`；归属无法证明 → `ErrForeign`；编排环境（没有 `/out`）→ 错误。从 `<envdir>/out` 的目录 FD 起逐级以 `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV)` 解析：子目录 `O_DIRECTORY|O_NOFOLLOW`，文件 `O_RDONLY|O_NOFOLLOW|O_NONBLOCK` 后 `fstat` 须为普通文件（规格 §5.6 的打开规则）。递归、按相对路径字典序；收集至多 `max` 个普通文件（`MaxOutputFiles = 256`），其余条目报告为跳过：`symlink`、`not_regular`（FIFO、设备、socket）、`too_many`（超过 `max` 的普通文件）、`open_failed`。返回的文件由调用方关闭；出错时不留下已打开的文件。
- **`Destroy` 的卸载顺序**：不变——数据目录下的挂载（含 `<envdir>/out` 的 tmpfs，深者先卸）→ 环境 cgroup → 环境目录。`OpenOutputs` 返回的文件仍打开时 tmpfs 卸载失败（`EBUSY`），`Destroy` 返回该宿主错误且不触碰后续各层；**调用方须先关闭全部输出文件再 `Destroy`**，之后重做即可。
- **一致性测试**：`providertest` 增加"`OpenOutputs` 对编排环境报错、未知环境为 `ErrNotFound`"，以及（`Harness.ExecSpec` 非空时）"exec 环境 `InDir` 幂等、`OpenOutputs` 要求先停止"。fake 以临时目录模拟 `InDir` 与 `/out`（`OutDir(envID)` 供测试写入输出）。
- **UID 范围的文件回收与核查**（Task 5，D13、E39）：`ReclaimUIDFiles(ctx, base, size)` 只遍历 `<data>/workspaces`，把属主落在范围内的条目以 `fchownat(fd, "", 0, 0, AT_EMPTY_PATH)` 改回 root（不跟随符号链接）；`UIDFiles(ctx, base, size, limit)` 遍历数据目录（跳过顶层 `blobs`，不进入其他文件系统的挂载，但报告挂载点本身的属主），返回至多 `limit` 个仍归该范围的宿主路径。纯函数 `provider.ScanUIDFiles(ctx, s, spans, limit)` 是 `Scan` 的 `uid_files` 层（报告项 EnvID 为空、归属 Unknown）。**范围归还前的唯一判据**：coordinator 在 cleanup loop、启动核对（recovery 的 `ReleaseUIDRange` 步骤经 `Coordinator.ReleaseCheckedUIDRange`）与会话关闭（`ReleaseOwnerUIDRange`，且只在使用过该范围的环境全部"已停止且清理完成"之后）三处都先 `ReclaimUIDFiles`，再以 `ScanUIDFiles` 核查：仍有条目则隔离该范围（`quarantined_resources` 的 `uid_range/<id>`，`kind = uid_files`）并报警、不归还。
- **exec 环境的生命周期（Task 10 装配）**：`resource.EnvRequest{Kind: exec, Template: "exec", Limits: {MemoryMax: 生效值, PidsMax, CPUQuotaUs, NoFile 256, FSize = OutBytes, TmpBytes}, Mounts: {OutBytes}}` 经 coordinator 创建（环境行由 Gateway 的 `ReserveExec` 先行写入）；`Stop` 经 coordinator（`stopped_at` 已记录才归还 exec slot）；`StartExec`、`ResourceDiag`、`OpenOutputs` 直达 provider；收集之后**关闭全部输出文件**，再以 `Coordinator.CleanupNow(envID)` 同步清理（Destroy → intent → `cleanup_state = done` → 回收、核查并归还 UID 范围）。同步清理失败（例如 `EBUSY`）按退避记入 cleanup 列，由 cleanup loop 接手：`kind = exec` 的环境只要 `stopped_at` 已记录即为候选，不等待所属 attempt 的判决。启动恢复与其他环境一样停止 exec 环境并记录 `stopped_at`；没有 actor 负责的 stop_blocked exec 环境以 `ExecGate.Occupy` 占用其任务的一个 exec slot（不占 run slot 与任务内存池），确认停止后归还。`verify-invariants` 以 I1 的同一扫描把清理完成的 exec 环境的残留报告为 I12 [Q]。

## 10. 执行中修订（Plan 12：会话环境）

Plan 12（规格 §12 会话：每会话一个长期 incarnation，冻结、驱逐、冷恢复）对 `kind = session` 的环境补充以下契约（Task 5 实现，Task 9 装配）：

- **`Mounts.RestoreDir`**（仅 session；task 与 exec 设置它时 `Validate` 拒绝）：宿主目录，只读 bind 到沙箱内的 `/run/agentbox/restore`（`ro,nosuid,nodev`，在 workspace 之后挂载）。冷恢复时宿主把会话 checkpoint 的 `state_ref` 内容暂存为其中的文件 `<sha256>`，Worker 以 `init.session_resume.staged_state_path = /run/agentbox/restore/<sha256>` 读取。bind 的是目录：宿主删除其中的文件后沙箱内随即不可见（incarnation 进入 idle 后的"卸载"）。**目录由调用方建立与删除**（root 属主、0755，文件 0444，上级目录 o+x），provider 只挂载；它不能在环境目录内（`Create` 要求环境目录不存在）。装配取 `<data>/restore/<env_id>`，incarnation ready 之后删除，环境停止之后再次删除（幂等）。
- **`Freeze(ctx, envID) error`**：写 `cgroup.freeze = 1`，轮询 `cgroup.events` 直到 `frozen 1`。期限为 ctx（没有期限时 10 s）；超时或无法读取冻结状态 → `ErrFreezeUnconfirmed`，返回前已写回 `cgroup.freeze = 0`（不等待解冻完成）。环境 cgroup 不存在 → `ErrNotFound`。**调用方只在 `Freeze` 成功返回之后记录 frozen**（E31：冻结未确认即驱逐）。
- **`Thaw(ctx, envID) error`**：写 `cgroup.freeze = 0` 并等待 `frozen 0`；cgroup 不存在 → `ErrNotFound`。
- **`Procs(ctx, envID) ([]int, error)`**：返回环境 cgroup 的 `cgroup.procs`（释放核验：每个 turn 释放后进程表须回到 incarnation ready 时的基线，§12.4、E30）；cgroup 不存在 → `ErrNotFound`。
- **`Stop` 与冻结**：语义不变；`cgroup.kill` 直接终止冻结的进程，权威检查仍是 `populated 0`。因此重启恢复（§14.1 第 6 步）以普通的 `StopEnv` 停止 frozen 会话的环境，不需要先解冻（E33）。
- **UID 范围按 owner 保留**：会话环境以 `resource.EnvRequest.UIDOwner = "session:<session_id>"` 创建，coordinator 经 `resource.OwnerUIDStore` 为 owner 分配一段范围并在该会话的各个环境之间复用（workspace 的属主因此跨 incarnation 不变）；环境清理时不归还，会话关闭时 `ReleaseOwnerUIDRange`（要求使用过它的环境全部停止且清理完成）。provider 接口不变：仍只经 `EnvSpec.UIDBase/UIDSize` 收到范围。
- **清理候选**：会话环境没有 `attempt_id`；它在 `stopped_at` 已记录且使用它的 incarnation 已 `ended` 时成为 cleanup 候选（task 环境仍要求 attempt 已有判决）。
- **一致性与 fake**：`provider/fake` 实现 `Freeze`/`Thaw`/`Procs`（`Frozen`、`FailFreeze`、`SetProcs` 供测试注入）；`tests/e2e/procprov` 以 SIGSTOP/SIGCONT 模拟冻结，`Procs` 返回存活进程。
