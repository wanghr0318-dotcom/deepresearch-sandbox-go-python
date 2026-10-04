# 提案：Provider 的 Go 接口契约（M1）

> 状态：**待审阅**，未实现。审阅通过后据此编写 Plan 2、5、6 中不依赖 Plan 1B 的任务。
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
	EnvID   string
	Kind    EnvKind
	Running bool // cgroup 的 populated 为 1
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

| 操作 | 语义 | 幂等 | 取消 |
|---|---|---|---|
| `Create` | 建目录 `<data>/envs/<env_id>` → **任何挂载前**写 `owner.json{install_id, env_id, spec_hash}` → cgroup → rootfs 与挂载 → 启动 init。返回时环境处于 running（init 就绪），可 `StartExec` | 按 `env_id`：已存在且 owner 与 `spec_hash` 一致 → 返回现状；`spec_hash` 不同 → `ErrConflict`；owner 不属于本安装 → `ErrForeign` | 有界；ctx 结束时停止推进并返回 ctx 错误，已创建的部分**保留为可识别的残留**（owner.json 已在），由调用方随后 `Stop`+`Destroy` 补偿；返回后不留后台操作 |
| `StartExec` | 经控制通道启动一次 workload（§4.2–§4.3）；成功即 `start_ack` 已收到 | **不幂等**：每次调用启动一个新进程。调用方以 attempt 状态保证至多一次；结果不明（`ErrControlLost`）时不重试，按 §4.2 拆除环境 | ctx 在 ACK 前结束 → 返回 ctx 错误，并视同控制连接不可信：调用方拆除环境 |
| `Stop` | `cgroup.kill` → 等待 `populated 0`（覆盖整个子树）；不卸载、不删除（§4.4 停止与清理分离） | 是：已停止或从未创建 → 成功 | 有界；ctx 结束前未确认清空 → `ErrStopUnconfirmed`（调用方据此进入 `stop_blocked`），不报告成功 |
| `Destroy` | 卸载 → 删除 cgroup → 删除目录（§4.4）；前置条件：已确认停止 | 是：缺失的部分视为已完成 | 有界；中断后可重做 |
| `List` | 本安装的完整环境（owner.json 的 install_id 等于本安装） | 只读 | 有界 |
| `Scan` | 独立原始扫描：环境目录（含无 owner.json 的）、数据目录下的挂载、`agentbox-*` cgroup（含其他 install_id 的）、listener socket、UID 范围文件属主；逐项分类 | 只读 | 有界 |
| `ResourceDiag` | 读取环境 cgroup 的 OOM 与 CPU 统计（§4.1） | 只读 | 有界 |

所有操作：不写 Store、不解析 JSONL（§4.1）；同一 `env_id` 的物理操作由 resource coordinator 串行化（§3.2），provider 不另加跨调用的锁。

## 4. 错误分类（`internal/provider`）

| 错误 | 含义 | 调用方处理 |
|---|---|---|
| `ErrNotFound` | 环境不存在（`StartExec`、`ResourceDiag`） | 视为已停止/已清理 |
| `ErrConflict` | 同一 `env_id` 已存在且 `spec_hash` 不同 | 编程错误或身份复用；不重试 |
| `ErrForeign` | owner.json 缺失、损坏或属于其他安装 | 不自动销毁：写 `quarantined_resources` 并报警（§14.1 表） |
| `*StartError{Reason}`（`ErrStartFailed`） | init 回复 `start_err`：启动序列某步失败，workload 未运行 | attempt 失败（启动失败类）；`Reason` 文本由 Plan 1B 的结论确定，本契约只规定"workload 未运行" |
| `ErrControlLost` | ACK 前控制连接断开（§4.2） | attempt 失败并拆除环境；不重试 `StartExec` |
| `ErrStopUnconfirmed` | 期限内未确认执行树清空 | `stop_blocked`，占用槽位，阻止替代执行（§8.2） |
| `ErrNotStopped` | `Destroy` 的前置条件不成立 | 先 `Stop` |
| ctx 错误 | 调用方取消或期限到期 | 按上表各操作的取消语义补偿 |
| 其他（包装的宿主错误） | 暂时性宿主故障（EBUSY、ENOSPC 等） | coordinator 退避重试；cleanup 记 `cleanup_error` |

## 5. 资源所有权

- **命名即归属**：目录 `<data>/envs/<env_id>`、cgroup `agentbox-<install_id>/env-<env_id>`、listener `<data>/envs/<env_id>/gw.sock`；owner.json 在任何挂载前写入。清理不猜测路径（§14.1"资源归属"）。
- **intent 先于物理操作**：coordinator 在调用 `Create` 前以 `resource.RecordIntent` 持久化 intent，调用后 `ResolveIntent`；provider 不知道 intent。
- **UID 范围由控制面分配**（`resource.AssignUIDRange`），经 `EnvSpec` 传入；归还的前置条件由 Plan 4 的 `ReleaseUIDRange` 与 coordinator 的文件/挂载核对共同保证（§4.5）。
- **不接管存活环境**：重启后 `List`/`Scan` 只用于核对与停止（§14.1 第 6 步）。

## 6. 依赖 Plan 1B 的部分（不在本契约中固定）

本契约不暴露降权、挂载方式、附加组、securebits、seccomp 安装的任何参数；它们是 `provider/local` 与 `sandbox` 的内部实现，由 Plan 1B 的结论决定（B1–B5）。契约中唯一与 1B 相关的点是 `StartError.Reason` 的取值（B6），已约定为不透明文本。因此 Plan 5、6 可按本契约以内存 fake 实现 `Provider`/`Starter` 编写与测试；Plan 2 中启动序列以外的部分可按本契约实现。

## 7. 审阅要点

1. 新增 `internal/provider` 契约包（第 1 节）。
2. `Create` 被取消时保留残留、由调用方补偿，而不是在 `Create` 内部回滚（第 3 节）——理由：owner.json 先写，残留总能被识别与清理；内部回滚本身也可能被中断。
3. `StartExec` 不幂等、结果不明即拆除环境（与 §4.2 一致）。
4. `Stop` 只在确认清空时成功，否则 `ErrStopUnconfirmed`。
