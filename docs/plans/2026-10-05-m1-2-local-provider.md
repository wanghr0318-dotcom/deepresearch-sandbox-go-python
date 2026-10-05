# M1 Plan 2：LocalProvider（不依赖 Plan 1B 的部分）实施计划

> **执行方式**：有界多 agent（2026-10-05 起）：主 agent 负责接口、集成与验收，子 agent 在隔离 worktree 中实现指定文件。每个任务 TDD：先写本任务列出的测试，确认失败，再实现，按"验证"一节的命令通过后提交。

**Goal:** 按已审阅的 [Provider 契约](../design/2026-10-05-provider-contract.md) 实现 `internal/provider`（契约类型、错误、内存 fake、契约一致性测试）与 `internal/provider/local` 中不依赖启动序列 spike 的全部部分：控制通道、带登记表的单一 reaper、init 服务循环、宿主侧 `StartExec`/`ExecHandle` 与执行闸门、环境生命周期（Create/Stop/Destroy/List/Scan/ResourceDiag）、seccomp 配置生成，以及代码组织设计 §9.2 规定的旧代码处理。

**不在本计划内（等待 Plan 1B，见文末"依赖 1B 的任务"）**：user namespace 与 UID 映射、挂载方式（只读 rootfs、tmpfs、`/proc` 掩蔽、pivot_root）、stage-2 helper（降权、securebits、no_new_privs、seccomp 安装时机）、生产环境的环境启动器，以及依赖它们的验收（E34、I11 的隔离部分、§16.2 spike 验收）。

**规格依据：** v0.2 规格 §4.1–§4.5、§8.3、§14.1（资源归属、扫描）、§16.4 E9、§18；代码组织设计 §2.1、§3、§9.2、§9.3；Provider 契约（全部）。

## Global Constraints

- **契约是唯一来源**：类型、错误、各操作的后置条件、执行闸门与仲裁顺序按 Provider 契约第 2–5 节，逐字段实现；契约未写明的行为不自行发明，遇到即停下记录并报告。
- **依赖方向**（archtest 固定）：`internal/resource`、`internal/runner` 只导入 `internal/provider`；`provider/local` 之上的包不导入 `cgroup`、`rootfs`、`sandbox`、`provider/local`（`cmd/agentbox` 例外）；`internal/provider/fake` 只供测试，`cmd/agentbox` 不得导入。
- **不在生产路径上运行未隔离的 workload**：本计划用于测试的环境启动器（不建 namespace、不降权）放在 `_test.go` 或 `internal/provider/local` 的测试钩子中，生产装配无法选用；生产启动器在 1B 之后实现，在此之前 `local.New` 没有可用的生产启动器时返回错误。
- **平台**：`provider/local`、`sandbox`、`cgroup` 为 Linux 专有（`//go:build linux`）；其余包需在 Windows 上可编译（`GOOS=windows go build ./...`）。需要 root 与 cgroup v2 的测试在非 root 时跳过，且在 CI 的 `linux-integration`（root）中实际运行（不得进入允许跳过的清单）。本地以 `wsl.exe -d Ubuntu -u root` 运行 root 测试。
- **测试**：每个包一个 `*_test.go`（代码组织 §9.3）；每条契约规则或风险路径一个用例；不为简单取值写测试。
- **执行工作区**：分支 `m1-batch2`，自 `m1-4-persistence` 创建，第一个提交是 Plan 1A 已验收的改名提交（`git cherry-pick 44beee9`，`internal/runtime` → `internal/sandbox`）。Plan 2、5、6 依次在该分支上执行。worktree：`git worktree add ../go-agentbox-batch2 -b m1-batch2 m1-4-persistence`。
- WSL 命令环境同 Plan 4（`GOPROXY=file:///mnt/f/go-agentbox/.superpowers/goproxy`、`GOSUMDB=off`、`GOTOOLCHAIN=local`）。只暂存任务列出的文件；不推送（推送另行授权）。提交署名只在有真实共同作者时添加。

## 任务与依赖

| 任务 | 内容 | 依赖 |
|---|---|---|
| 1 | `internal/provider` 契约包、内存 fake、契约一致性测试、archtest 规则 | — |
| 2 | 旧代码处理与 cgroup 扩展（§9.2；`cgroup.kill`、`populated`、OOM 与 CPU 统计） | — |
| 3 | seccomp 配置生成（纯 Go BPF，两套配置） | — |
| 4 | 控制通道（`SOCK_SEQPACKET`、帧、FD 传递与失败协议） | — |
| 5 | 登记表与单一 reaper | 4 |
| 6 | init 服务循环（start、terminate、exit） | 4、5 |
| 7 | 宿主侧 `StartExec`/`ExecHandle` 与执行闸门 | 1、4 |
| 8 | 环境生命周期：Create/Stop/Destroy/List/Scan/ResourceDiag；E9；"start 在途时 stop" | 1、2、6、7 |

---

### Task 1：`internal/provider` 契约包、内存 fake 与契约一致性测试

**Files:** Create `internal/provider/provider.go`、`internal/provider/provider_test.go`、`internal/provider/fake/fake.go`、`internal/provider/fake/fake_test.go`、`internal/provider/providertest/providertest.go`；Modify `internal/archtest/archtest_test.go`。

**Interfaces**（`internal/provider`，只依赖标准库）：

```go
type EnvKind string
const (KindTask EnvKind = "task"; KindSession EnvKind = "session"; KindExec EnvKind = "exec")

type EnvSpec struct { EnvID, InstallID string; Kind EnvKind; UIDBase, UIDSize uint32; Template string; Limits Limits; Mounts Mounts }
type Limits struct { MemoryMax, PidsMax, CPUQuotaUs int64; NoFile, FSize uint64; TmpBytes int64 }
type Mounts struct { Workspace, GatewaySocket, In string; OutBytes int64 }
type ExecSpec struct { ExecID string; Argv, Env []string; Dir string }
type ExitStatus struct { Code int; Signal syscall.Signal }
type ResourceDiag struct { OOMKillDelta uint64; OOMObserved bool; CPUUsageUsec uint64 }
type ExecHandle interface { Stdin() io.WriteCloser; Stdout() io.ReadCloser; Stderr() io.ReadCloser; Wait() (ExitStatus, error); Terminate(grace time.Duration) error }
type EnvInfo struct { EnvID string; Kind EnvKind; Complete, Running bool }
type Owner int // OwnedComplete、OwnedPartial、Foreign、Unknown
type ScanItem struct { Layer, Path, EnvID string; Owner Owner }
type ScanReport struct { Items []ScanItem }

// SpecHash 是 EnvSpec 的规范化哈希（JSON 编码后 SHA-256，十六进制），写入 owner.json 并用于 Create 的幂等比较。
func SpecHash(EnvSpec) string
// Validate 检查 EnvSpec 的必填字段与按 Kind 的挂载组合（契约第 2 节）。
func (EnvSpec) Validate() error

var ErrNotFound, ErrIncomplete, ErrConflict, ErrForeign, ErrStopping, ErrStartFailed, ErrControlLost, ErrStopUnconfirmed, ErrNotStopped error
type StartError struct{ Reason string } // Is(ErrStartFailed)

// 完整的环境操作集合；消费者各自声明子集（resource.Provider、runner.Starter）。
type Provider interface {
	Create(ctx, EnvSpec) (EnvInfo, error)
	StartExec(ctx, envID string, ExecSpec) (ExecHandle, error)
	Stop(ctx, envID string) error
	Destroy(ctx, envID string) error
	List(ctx) ([]EnvInfo, error)
	Scan(ctx) (ScanReport, error)
	ResourceDiag(ctx, envID string) (ResourceDiag, error)
}
```

**`internal/provider/fake`**（仅测试使用）：`fake.New() *Provider` 实现 `provider.Provider` 的全部契约语义（Create 的完整/残留/冲突/外来四种结果、执行闸门、Stop 的权威检查、Destroy 的逐层核对），执行由测试注入的 `Program`（`func(ctx, stdin io.Reader, stdout, stderr io.Writer) ExitStatus`）驱动；提供故障注入：`InjectCreateResidue(envID)`、`InjectForeign(envID)`、`BlockStartBeforeSend(envID) (release func())`、`FailStop(envID, ErrStopUnconfirmed)`。

**`providertest.Run(t, newProvider func(t *testing.T) provider.Provider, newSpec func(envID string) provider.EnvSpec, prog ProgramSpec)`**：契约一致性测试，Task 1 在 fake 上运行，Task 8 在 `provider/local` 上运行。

**Tests**（`providertest` 中，每条一个子测试）：Create 后 `List` 报告 `Complete`；同 `env_id` 同 spec 重复 Create 返回现状；不同 spec → `ErrConflict`；残留 → `ErrIncomplete`；外来 → `ErrForeign`（`Scan` 报告 `Foreign`/`Unknown`，不认领）；`StartExec` 回显程序的 stdout 与退出码；`Stop` 后 `StartExec` → `ErrStopping`；"start 在途时 stop"（`BlockStartBeforeSend` 或 Task 8 的等价钩子）：在途启动返回 `ErrControlLost`/`ErrStopping`/`*StartError` 之一且无存活进程；`Destroy` 在未停止时 → `ErrNotStopped`，停止后成功且 `List`/`Scan` 不再报告该环境；`ResourceDiag` 在 Destroy 后 → `ErrNotFound`。`provider_test.go`：`SpecHash` 对字段顺序稳定、任一字段变化即改变；`Validate` 拒绝 exec 环境带 Gateway socket、编排环境带 `In`。archtest：新增两条规则的断言。

**验证：** `go vet ./... && GOOS=windows go build ./... && go test -count=1 ./internal/provider/... ./internal/archtest/`

---

### Task 2：旧代码处理与 cgroup 扩展

**Files:** Modify `internal/cgroup/cgroup.go`、`cgroup_test.go`、`internal/rootfs/overlay.go`（拆分）、`internal/rootfs/overlay_test.go`（删除 Overlay 部分）、`internal/hostcheck/hostcheck.go`、`hostcheck_test.go`、`internal/sandbox/spawn.go`（注释）。可新建 `internal/rootfs/bind.go` 承接保留的 bind/unmount 原语（同包测试仍在一个 `_test.go`）。

**Interfaces**（新增于 `cgroup.Group`）：

```go
func (g *Group) Kill() error                                  // 写 cgroup.kill
func (g *Group) Populated() (bool, error)                     // 读 cgroup.events 的 populated
func (g *Group) WaitEmpty(ctx context.Context) error          // 轮询或 inotify 等待 populated 0；ctx 结束返回 ctx 错误
func (g *Group) OOMKills() (uint64, error)                    // memory.events 的 oom_kill
func (g *Group) CPUUsageUsec() (uint64, error)                // cpu.stat 的 usage_usec
func Exists(root, name string) (bool, error)                  // 路径存在且是 cgroup 目录
```

**规则：** `Limits` 增加 `memory.swap.max = 0`（规格 §4.5）；`New` 提取私有 `ensureControllers`（§9.2），主流程不变；`Destroy` 只删除空 cgroup（`EBUSY` 原样返回）。`rootfs`：保留 `BindMount`、`Unmount`，删除 `Overlay` 与 `Mount` 及其测试（§9.2；规格不再使用 overlay）。`hostcheck.Check`：移除 overlay 必需项；检查 cgroup v2 统一层级、`cgroup.kill` 存在（内核 ≥ 5.14）、所需控制器（memory、pids、cpu）可启用；挂载方式相关的检查等 1B。`sandbox/spawn.go`：保留 FD 与 Wait 所有权契约，删除把 Pdeathsig 当作可靠性依据的表述（§9.2）。

**Tests：** `Kill` 后 `WaitEmpty` 返回且 `Populated` 为 false（root；用一个在该 cgroup 内双重 fork + `setsid` 的进程验证子树全部被杀）；`WaitEmpty` 在 ctx 到期时返回 ctx 错误；`OOMKills` 在超出 `memory.max` 的进程被杀后增加；`hostcheck` 不再要求 overlay（非 root 单元测试：以假的 `/sys/fs/cgroup` 根目录驱动）。

**验证：** 非 root：`go test -count=1 ./internal/cgroup/ ./internal/rootfs/ ./internal/hostcheck/`；root：`wsl.exe -d Ubuntu -u root -e bash -c '…; go test -count=1 ./internal/cgroup/'`。

---

### Task 3：seccomp 配置生成

**Files:** Create `internal/sandbox/seccomp.go`；Modify `internal/sandbox/spawn_test.go`（本包唯一测试文件，追加一节）。

**Interfaces：**

```go
type SeccompProfile int // ProfileOrchestrator、ProfileExec
// BuildFilter 生成 classic BPF 程序（[]unix.SockFilter 的等价纯 Go 结构，不引入新依赖），供 1B 确定的安装时机使用。
func BuildFilter(p SeccompProfile, arch Arch) ([]SockFilter, error) // Arch：AMD64、ARM64（按 runtime.GOARCH 选择）
```

**规则（规格 §4.5，逐条）：** 先检查 `seccomp_data.arch`：仅原生 x86_64 / aarch64，其他 ABI（含 x32 的 `__X32_SYSCALL_BIT`）→ `SECCOMP_RET_KILL_PROCESS`；默认允许；显式拒绝列表 → `EPERM`：`mount`、`umount2`、`pivot_root`、`unshare`、`setns`、`clone` 带任何 `CLONE_NEW*`、`ptrace`、`process_vm_readv/writev`、`bpf`、`perf_event_open`、`userfaultfd`、`io_uring_setup/enter/register`、`keyctl`、`add_key`、`request_key`、`open_by_handle_at`、`init_module`、`finit_module`、`delete_module`、`kexec_load`、`kexec_file_load`、`socket()` 非 `AF_UNIX`；`clone3` → `ENOSYS`。exec 配置在此基础上不额外放宽。

**Tests：** 测试内实现一个最小 classic-BPF 解释器（只支持本程序用到的指令），对每条规则以构造的 `seccomp_data` 断言动作：每个拒绝的系统调用 → `EPERM`；`clone` 带 `CLONE_NEWUSER` → `EPERM`、不带 → 允许；`socket(AF_INET)` → `EPERM`、`socket(AF_UNIX)` → 允许；`clone3` → `ENOSYS`；i386 架构与 x32 位 → kill；任一普通调用（`read`）→ 允许。另一个用例在子进程中设置 `no_new_privs` 并以 `seccomp(SECCOMP_SET_MODE_FILTER)` 安装 orchestrator 配置（不需要 root），断言 `unshare(0)` 返回 `EPERM`、`socket(AF_INET)` 返回 `EPERM`、`clone3` 返回 `ENOSYS`、`getpid` 正常——只验证配置内容，不涉及 1B 的安装时机与 TSYNC。

**验证：** `go test -count=1 -run 'Seccomp' ./internal/sandbox/`

---

### Task 4：控制通道

**Files:** Create `internal/sandbox/control.go`；Modify `internal/sandbox/spawn_test.go`（追加一节）。

**Interfaces：**

```go
type Message struct { Type string; ExecID string; Spec json.RawMessage; PID int; Reason string; Exit *ExitInfo; GraceMS int64 }
type ExitInfo struct { Code int; Signal syscall.Signal } // 与规格 §4.1 的 ExitStatus 对应
// 执行中修订：GraceMS 承载 terminate 的宽限（Task 6、7 需要）；接收端收到的管道 FD 保留发送端的 O_NONBLOCK，
// 不经 exec.Cmd 的启动器（stage-2 helper）须自行清除。
// Type：start、start_ack、start_err、exit、terminate（规格 §4.2–§4.4）
type Conn struct { /* 一端：一个写 goroutine 经有界队列串行写出 */ }
func NewConn(f *os.File, queueLen int) *Conn
func (c *Conn) Send(m Message, files []*os.File) error   // files 只允许随 start 传递，恰好 3 个；在同一 sendmsg 中发送
func (c *Conn) Recv() (Message, []*os.File, error)      // recvmsg(MSG_CMSG_CLOEXEC)
func (c *Conn) Close() error
var ErrProtocol, ErrQueueFull error
```

**规则（§4.2）：** `SOCK_SEQPACKET`；一条消息一帧，帧 ≤ 64 KiB，`spec` ≤ 32 KiB；检查 `MSG_TRUNC` 与 `MSG_CTRUNC`；`start` 必须恰好 3 个 FD，其他消息 0 个；任何违规关闭全部已收 FD 并返回 `ErrProtocol`；发送方在 `sendmsg` 返回后关闭子端副本，发送失败时同时关闭父端（由调用方按 `Send` 的文档约定执行）；队列满返回 `ErrQueueFull`（调用方视为控制通道故障）。

**Tests：** 往返 `start`（3 个 FD，接收端 FD 带 `FD_CLOEXEC`，经 FD 写入的数据可读）；超长帧被拒；`start` 带 2 个 FD 与非 `start` 带 FD → `ErrProtocol` 且接收端已关闭全部收到的 FD（检查 `/proc/self/fd` 数量不增加）；`spec` 超 32 KiB 被发送端拒绝；队列满 → `ErrQueueFull`；对端关闭 → `Recv` 返回 `io.EOF`。

**验证：** `go test -count=1 -run 'Control' ./internal/sandbox/`

---

### Task 5：登记表与单一 reaper

**Files:** Create `internal/sandbox/reaper.go`；Modify `internal/sandbox/spawn_test.go`（追加一节）。

**Interfaces：**

```go
// Launcher 启动一个 workload 并返回其 pid；生产实现是 1B 之后的 stage-2 helper，测试用直接 exec。
type Launcher interface { Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (pid int, err error) }
type Registry struct { /* reg.mu 覆盖启动与收割 */ }
func NewRegistry(out func(Message) error) *Registry   // out 为非阻塞入队；返回错误视为控制通道故障
func (r *Registry) Start(execID string, l Launcher, spec json.RawMessage, fds [3]*os.File) error
func (r *Registry) Reap() error                       // 循环 Wait4(-1, WNOHANG) 至 0 或 ECHILD；EINTR 重试
```

**规则（§4.3）：** 一把锁 `reg.mu` 覆盖启动（启动 → 登记 `pid → exec_id` → 入队 `start_ack`）与收割（`Wait4` → 查表 → 删登记 → 入队 `exit`）；锁内只做非阻塞入队；`start_ack` 必先于对应 `exit`；未登记的 pid（孤儿被收养后退出）收割后丢弃；不调用 `Process.Wait`/`exec.Cmd.Wait`。

**Tests**（测试进程以 `prctl(PR_SET_CHILD_SUBREAPER)` 充当收割者，不需要 root）：立即退出的进程也得到先 `start_ack` 后 `exit` 的顺序（重复 200 次）；`EINTR`（向自身发信号）不丢失退出；双重 fork 的孙进程被收割但不产生 `exit` 消息；`out` 返回错误时 `Start` 与 `Reap` 返回错误。

**验证：** `go test -count=1 -run 'Reaper' ./internal/sandbox/`

---

### Task 6：init 服务循环

**Files:** Modify `internal/sandbox/init.go`（重写服务循环部分）、`internal/sandbox/spawn_test.go`（追加一节）。

**Interfaces：** `func Serve(ctx context.Context, conn *Conn, reg *Registry, l Launcher) error`——处理 `start`（经 `Registry.Start`，失败回 `start_err{reason}`）、`terminate{exec_id, grace_ms}`（向进程组发 SIGTERM，计时器到期发 SIGKILL，期间继续服务）、SIGCHLD 触发 `Reap`；控制连接断开时返回（init 随之退出，环境内进程由 cgroup 回收）。

**规则：** 不解析 workload 输出；`terminate` 对未知 `exec_id` 忽略；多个 exec 并行；不依赖 Pdeathsig。`start` 的 spec 以 `sandbox.StartSpec{Argv, Env, Dir}` 解码（与宿主侧共用的唯一定义，执行中修订）；init 收到 `start`/`terminate` 以外的消息为协议错误。

**Tests**（以测试进程作为 init、`socketpair` 作为控制通道、直接 exec 的 Launcher）：`start` → 收到 `start_ack{pid}` 后收到 `exit{code}`；Launcher 失败 → `start_err`，无 `start_ack`；`terminate` 对忽略 SIGTERM 的进程在 grace 到期后 SIGKILL（`exit.signal = SIGKILL`）；两个并行 exec 各自收到 `exit`；宿主端关闭后 `Serve` 返回。

**验证：** `go test -count=1 -run 'Serve' ./internal/sandbox/`

---

### Task 7：宿主侧 `StartExec`/`ExecHandle` 与执行闸门

**Files:** Create `internal/provider/local/exec.go`、`internal/provider/local/local_test.go`（本包唯一测试文件）。

**Interfaces**（包内）：

```go
type gate struct { /* mu；state open|closed；inflight 计数 */ }
func (g *gate) enter() error   // closed → provider.ErrStopping；否则 inflight++
func (g *gate) leave()
func (g *gate) close()         // 不可逆；不等待 inflight
type execClient struct { /* 宿主端 Conn；exec_id → 等待者；先到的 exit 暂存 */ }
func (c *execClient) start(ctx context.Context, g *gate, spec provider.ExecSpec) (provider.ExecHandle, error)
```

**规则：** 按契约第 3 节 `StartExec` 行与第 5 节：闸门锁内检查并登记在途启动，锁外创建 3 个管道并发送 `start`（发送后关闭子端副本）；等待 `start_ack`/`start_err`；ACK 前连接断开 → `ErrControlLost`；ACK 前 ctx 结束 → ctx 错误；`start_err` → `*StartError{Reason}`；先于 `start_ack` 到达的 `exit` 暂存（§4.3 防御）；`Wait` 返回 `exit` 中的退出状态或控制连接错误；`Terminate` 发送 `terminate{grace}`。

**Tests**（宿主端对接一个测试内的假 init：同一进程中另一端 `Conn` 按脚本回复，不需要 root）：成功路径的 stdout 与退出码；`start_err` → `*StartError`；ACK 前对端关闭 → `ErrControlLost`；ACK 前 ctx 取消；`exit` 先于 `start_ack` 到达仍正确；`gate.close()` 后 `start` → `ErrStopping` 且未发送任何消息；在途启动期间 `close()` 不阻塞。

**验证：** `go test -count=1 -run 'Exec|Gate' ./internal/provider/local/`

---

### Task 8：环境生命周期

**Files:** Create `internal/provider/local/local.go`、`internal/provider/local/scan.go`；Modify `internal/provider/local/local_test.go`。

**Interfaces：**

```go
// EnvStarter 在环境 cgroup 内启动 init 并返回宿主端控制连接；生产实现（namespace、UID 映射、挂载、pivot_root）等 1B。
type EnvStarter interface { StartInit(ctx context.Context, spec provider.EnvSpec, dir string, cg *cgroup.Group) (*sandbox.Conn, int, error) }
type Options struct { DataDir, CgroupRoot, InstallID string; Starter EnvStarter }
func New(opt Options) (*Provider, error) // Starter 为 nil 时返回错误（生产启动器在 1B 之后提供）
// *Provider 实现 provider.Provider
```

**规则（契约第 3–6 节，逐条）：**
- `Create`：`<data>/envs/<env_id>` → 写 `owner.json{install_id, env_id, spec_hash}`（临时文件 + fsync + rename + fsync 父目录）→ cgroup `agentbox-<install_id>/env-<env_id>` 并 `Apply` 限制 → `Starter.StartInit` → 打开闸门。已存在：按 owner.json 与 `spec_hash` 及"init 就绪且闸门打开"分类为现状 / `ErrIncomplete` / `ErrConflict` / `ErrForeign`；不补完、不认领。ctx 结束时停止推进，返回后不留 goroutine。
- `Stop`：关闭闸门 → `Kill` → `WaitEmpty(ctx)`；成功仅当 cgroup 不存在或 `populated 0`；期限内未确认 → `ErrStopUnconfirmed`。
- `Destroy`：前置为 `Stop` 的权威检查（否则 `ErrNotStopped`）；卸载数据目录下属于该环境的挂载（读 `/proc/self/mountinfo`）→ 删除 cgroup → 删除目录；成功前逐层核对三层均不存在。
- `List`：读 `<data>/envs/*/owner.json`，install_id 一致者报告 `Complete`（闸门打开且 init 存活）与 `Running`（`Populated`）。
- `Scan`：独立扫描环境目录（含无/损坏 owner.json）、`mountinfo` 中数据目录下的挂载、`agentbox-*` cgroup（含其他 install_id）、`<data>/envs/*/gw.sock`；逐项分类 `OwnedComplete`/`OwnedPartial`/`Foreign`/`Unknown`。环境目录类条目即使没有 owner.json 也要以目录名填写 `EnvID`（执行中修订：coordinator 据 `EnvID` 匹配残留并按 `Layer`/`Path` 原样记录隔离）。UID 范围文件属主一层在 1B 引入 UID 映射后补充（见文末）。
- `ResourceDiag`：`OOMKills` 相对 `Create` 时记录的基线之差、`OOMObserved = delta > 0`、`CPUUsageUsec`；cgroup 不存在 → `ErrNotFound`。

**测试用环境启动器**（`local_test.go` 内）：以 `/proc/self/exe` 的测试辅助模式启动一个运行 Task 6 `Serve` 的 init，直接放入环境 cgroup（不建 namespace、不降权）。只在测试中构造。

**Tests**（root + cgroup v2；非 root 跳过；CI `linux-integration` 中运行）：`providertest.Run` 契约一致性全集；**E9**：workload 双重 fork 并 `setsid` 长睡眠，`Stop` 成功、cgroup 中无进程、`Destroy` 后三层均不存在；**"start 在途时 stop"**（契约第 5 节）：测试钩子让 `StartExec` 在登记在途启动后、发送 `start` 前阻塞 → `Stop` 成功 → 放行 → 返回 `ErrControlLost`/`ErrStopping`/`*StartError` 之一、cgroup 中无进程、后续 `StartExec` → `ErrStopping`；中断的 Create（在写 owner.json 之后、启动 init 之前取消 ctx）→ 再次 Create 为 `ErrIncomplete`，`Stop`+`Destroy` 后可重建；无 owner.json 的目录 → `Create` 为 `ErrForeign`、`Scan` 报告 `Unknown`、`Destroy` 不删除它；`ResourceDiag` 在 workload 超出 `memory.max` 被杀后 `OOMKillDelta > 0`。

**验证：** 非 root：`go vet ./... && GOOS=windows go build ./... && go test -count=1 ./internal/provider/...`；root：`wsl.exe -d Ubuntu -u root -e bash -c '…; CI=true go test -count=1 ./internal/provider/local/ ./internal/cgroup/'`；CI `linux-integration` 中上述用例实际运行（`scripts/ci/allowed-skips.txt` 不新增条目）。

---

## 联合验收

1. 本地：非 root 全量 `go test ./...`、root 运行 `cgroup`、`sandbox`、`provider/local` 的测试，全部通过；`GOOS=windows go build ./...` 通过。
2. CI：`correctness` 与 `linux-integration` 通过，root 用例实际运行。
3. 范围：diff 只含本计划 Files；archtest 新规则通过。
4. 验收归属：E9（cgroup 层）；provider 集成测试；契约一致性测试在 fake 与 local 上都通过。

## 依赖 Plan 1B 的任务（Task 9–13；依据回写后的规格 §4.6、§16.2，2026-10-05 用户审阅结论）

**审阅结论带来的两条实现门槛（规格 §4.6 末尾）贯穿以下任务：**进程级凭据边界（附加组只在专用启动进程中清空，server 凭据不变）；
不依赖名称的启动成功判据（R3）。"实验可行"与"生产启动路径通过验收"分开判定：Task 9–12 各自的测试通过不等于生产启动路径验收，
后者是 Task 12 的 §16.2 生产验收加 Task 13 的真实端到端。

**规格增量（随本节一并审阅）**：为实现 R3 的判据，init 的 permitted 集合由 `{KILL}` 改为 `{KILL, SETUID, SETGID, SETPCAP}`
（effective 仍只有 `{KILL}`，inheritable 与 ambient 为空）。原因：判据依赖 ptrace 的 exec 事件，而被跟踪进程 execve 时内核把
新的 permitted 限制在原 permitted 之内（tracer 无 `CAP_SYS_PTRACE` 时），init 若只保留 `{KILL}`，helper 将拿不到降权所需的能力。
Task 11 第一步以测试验证这一点；验证不成立则停止并报告，不退回到基于名称或轮询的判据。

**用户审阅（2026-10-05）对上述增量与 Task 11 的约束（有界授权，先于实现）：**
- permitted 集合扩展只为有界的 ptrace 实验**有条件授权**：先以测试验证，再采纳进生产；不得把 `effective = {KILL}` 描述为"阻止其余 permitted 能力被重新启用"（持有 permitted 的进程可以自行将其提升为 effective，真正的收缩发生在 helper 的降权序列中）。
- 实现被跟踪的启动路径前先写明：等待 ptrace 事件**之前释放 `reg.mu`**；reaper 在持有登记表锁时的事件投递保持**非阻塞**；成功 detach 后保证 **`start_ack` 先于 `exit`** 投递；**detach 失败视为启动失败**，须终止并清理；**exec 成功与 Worker 就绪握手是两回事**，不得合并。
- 实验失败或需要契约变更时，报告证据与所需决定；不退回基于名称或轮询的判定。

| 任务 | 内容 | 依赖 |
|---|---|---|
| 9 | 专用启动进程与生产 `EnvStarter`（user namespace、ID 映射、`CLONE_INTO_CGROUP`、pidfd 交还、server 为 subreaper） | 8 |
| 10 | init 环境建立：挂载（新挂载 API + 回退）、tmpfs、`/dev`、proc 与掩蔽、pivot_root、init 能力；`init_err` | 9 |
| 11 | stage-2 helper 与 init 的生产 `Launcher`：降权序列、exec-status 管道、**R3 启动成功判据（ptrace exec 事件）**、reaper 集成 | 10 |
| 12 | `hostcheck` 补充、构建约束（`CGO_ENABLED=0`）、§16.2 生产验收（隔离检查器） | 11 |
| 13 | 启用 `agentbox server`；真实隔离环境上的端到端与重启恢复验收（首个切片、E1–E10、E2/E3、E5 物理回收）；README 快速开始 | 12；Plan 5、6 |

### Task 9：专用启动进程与生产 `EnvStarter`

**Files:** Create `internal/sandbox/launch.go`（`sandbox-launch` 子命令的实现）、`internal/provider/local/starter.go`（生产 `EnvStarter`）；Modify `cmd/agentbox/main.go`（在 `init` 之后分流 `sandbox-launch`）、`internal/sandbox/spawn_test.go`、`internal/provider/local/local_test.go`；Delete `internal/sandbox/spawn.go` 中旧的无 user namespace 的 `Spawn`（及其调用与测试）。

**规则：**
- server 以 `/proc/self/exe sandbox-launch` re-exec 一个专用启动进程（每个环境一个，短生命周期），经继承的 FD 传入：控制 socket 的沙箱端、环境 cgroup 目录 fd、启动规格（JSON：UID/GID 范围、hostname）与一个用于交还结果的 SEQPACKET socket。
- 启动进程：`setgroups([])` 清空**自身**附加组并以 `getgroups` 断言为空 → 以 `exec.Cmd` 启动 `/proc/self/exe init`，`SysProcAttr`：`Cloneflags = NEWUSER|NEWNS|NEWPID|NEWUTS|NEWIPC|NEWNET`、`UseCgroupFD/CgroupFD`（`CLONE_INTO_CGROUP`）、`UidMappings/GidMappings`（ns 0 → 范围基址，长度为范围大小）、`GidMappingsEnableSetgroups = false`（写 `setgroups=deny`）、`Credential{Uid: 0, Gid: 0, NoSetGroups: true}`、`PidFD` → 经结果 socket 以 `SCM_RIGHTS` 交还 init 的 pidfd 与 pid → 退出。任何一步失败：以结果 socket 报告原因文本后以非零码退出，不留进程（clone 之后的失败先 `pidfd_send_signal(SIGKILL)`）。
- server 在启动时设置 `PR_SET_CHILD_SUBREAPER`，启动进程退出后 init 由 server 收养；`EnvStarter` 用 pidfd 等待并收割 init（满足 Task 8 记录的 `EnvStarter` 保证：成功返回即 init 就绪、starter 收割 init、init 在执行任何代码前已在环境 cgroup 中、ctx 取消后的残留留在 cgroup 中由 Stop 清理）。
- **凭据边界**：server 进程不调用 `setgroups`/`setresuid` 等改变自身凭据的系统调用；archtest 增加源码检查——这些调用只出现在 `internal/sandbox` 的启动进程与 helper 代码中。
- `local.New` 的生产装配使用本启动器；测试启动器保留在 `_test.go`。

**Tests（root，cgroup v2）：** server（测试进程）的 `/proc/self/status` `Groups` 在启动若干环境前后不变；init 的 `/proc/<pid>/status`：`Groups` 为空、`NSpid` 两级、uid/gid 映射与范围一致；init 在执行代码前已在环境 cgroup（以 `cgroup.procs` 与 init 启动时自报的 cgroup 路径核对）；启动进程在 clone 前/后各注入失败 → `ErrIncomplete` 语义成立（残留只在 cgroup 中，Stop+Destroy 后可重建）；启动进程退出后 init 由 server 收割（无僵尸）。

**验证：** root：`go test -count=1 ./internal/sandbox/ ./internal/provider/local/`；非 root 全量；`GOOS=windows go build ./...`。

### Task 10：init 环境建立

**Files:** Create `internal/sandbox/mounts.go`、`internal/sandbox/caps.go`；Modify `internal/sandbox/init.go`（`RunInit` 按 §4.6 的 init 段执行，失败报告 `init_err{reason}`）、`internal/sandbox/spawn_test.go`、`internal/rootfs/*`（模板描述）。

**规则（规格 §4.5、§4.6 init 段，逐条）：** 控制 socket 立即 `F_DUPFD_CLOEXEC` 移到高位并关闭原 fd → `mount("/", MS_REC|MS_PRIVATE)` → rootfs：新挂载 API（`open_tree(OPEN_TREE_CLONE|AT_RECURSIVE)` → `mount_setattr(AT_RECURSIVE, RDONLY|NOSUID|NODEV, MS_PRIVATE)` → `move_mount`），不可用时回退 `mount(2)` 递归 bind 后逐个子挂载 remount ro（回退路径有独立测试）→ tmpfs `/tmp`、`/run`（限额）→ `/dev`（tmpfs + bind null/zero/random/urandom，remount ro）→ Gateway socket bind（编排环境，属主映射 uid 1000，0600；M1 无 Gateway 时该步按 spec 跳过且有测试断言 exec 环境不可见）→ `/workspace/{attempt,out}` bind（宿主已 chown 到映射 UID）→ 新 proc（`nosuid,nodev,noexec`）→ 掩蔽清单（目录：只读空 tmpfs；文件：bind `/dev/null`）与只读路径 → 打开 helper 二进制 `O_PATH|O_CLOEXEC` 移到高位 → `pivot_root(".", ".")` → `umount2(".", MNT_DETACH)` → `chdir("/")` → 全部线程：bounding = `{KILL, SETUID, SETGID, SETPCAP}`，permitted = 同上，effective = `{KILL}`，inheritable、ambient 为空，不设 securebits。
- **不提供 `/dev/shm`**（规格 §4.5 环境限制），有测试断言其不存在。
- **模板**：M1 模板为只读 bind 的宿主目录集合（配置项给出 `/usr`、`/etc` 子集、`/lib*` 与 worker 包目录 `/opt/agentbox`），`rootfs.EnsureTemplate` 校验；不提供可写 rootfs。
- 每一步失败报告 `init_err{init/<step>: <原因>}`（实验记录 §8 的 9 个注入点），环境创建失败（`Create` 按契约处理残留）。注入点只在测试构建中可用。

**Tests（root）：** 沙箱内 `/proc/self/mountinfo`：全部私有传播、旧根已脱离、rootfs 与只读路径为 `ro,nosuid,nodev`、掩蔽项生效、`/sys` 与 cgroupfs 未挂载、`/dev/shm` 不存在；init 的能力集合如上（逐线程）；控制 socket 与 helper fd 带 close-on-exec；9 个注入点各报告对应 `init_err` 且 workload 未运行；新挂载 API 与回退路径各运行一次。

**验证：** 同 Task 9。

### Task 11：stage-2 helper、生产 `Launcher` 与启动成功判据

**Files:** Create `internal/sandbox/helper.go`（`exec-stage2` 子命令）、`internal/sandbox/rawfork_linux.go`（raw clone + `execveat`，移植 spike 的 `rawfork.go`）、`internal/sandbox/launcher.go`（init 的生产 `Launcher`）；Modify `internal/sandbox/reaper.go`（ptrace 停止事件交给启动路径）、`internal/sandbox/spawn_test.go`、`cmd/agentbox/main.go`（分流 `exec-stage2`）。

**降权序列（规格 §4.6 helper 段，逐条）：** `PR_SET_NAME` → fd 3 设 `FD_CLOEXEC`、`close_range(4, ~0, CLOSE_RANGE_CLOEXEC)` → 清空 bounding → 设置并锁定 securebits 0x0f（在 `setresuid` 之前）→ `setresgid/setresuid(1000)` → 显式清空 eff/prm/inh 与 ambient → 显式设置 rlimit（`RLIMIT_NOFILE`、`RLIMIT_CORE=0`；exec 环境另 `RLIMIT_FSIZE`）→ `no_new_privs` → seccomp（Task 3 的过滤器，`SECCOMP_FILTER_FLAG_TSYNC`，`clone3 → ENOSYS`）→ `execve` workload。`LockOSThread`；身份与能力调用经 `syscall.AllThreadsSyscall`。任一步失败经 fd 3 写 `helper/<step>: <原因>` 后退出。

**init 的启动路径（持 `reg.mu`）：** `recvmsg(MSG_CMSG_CLOEXEC)` 恰好 3 个 FD → `pipe2(O_CLOEXEC)` 作为 exec-status → 阻塞全部信号 → raw `clone(SIGCHLD)` → 子进程：`dup3` 到 0/1/2/3，`setpgid(0,0)`，`ptrace(PTRACE_TRACEME)`，清空信号掩码，`execveat(helper_fd, "", AT_EMPTY_PATH)`。

**R3 启动成功判据（ptrace exec 事件，不依赖名称或轮询）：**
1. 子进程 `PTRACE_TRACEME` 后 `execveat` helper：成功时内核使其停在 exec 后的 `SIGTRAP`；init 收到该停止后 `PTRACE_SETOPTIONS(PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL)` 并 `PTRACE_CONT`。
2. helper 执行降权序列后 `execve` workload：成功时内核产生 `PTRACE_EVENT_EXEC` 停止——这是"workload 已完成 execve、尚未执行任何指令"的内核事实。init 随即 `PTRACE_DETACH` 并回复 `start_ack`。
3. 在 `PTRACE_EVENT_EXEC` 之前：exec-status 管道有数据 → `start_err{helper/<step>}`；进程退出或被信号终止 → `start_err{helper/died_before_exec: <状态>}`。两种情况 workload 都未运行。
4. 因为事件发生在 workload 的第一条指令之前，workload 快速退出（如 `true`）总是报告为 `start_ack` 后的 `exit`；workload 与 helper 同名不影响判定。
5. ptrace 请求必须由 clone 子进程的那个 OS 线程发出（tracer 是线程）：启动路径在一个锁定的 goroutine 中完成 clone、等待与 detach。reaper 仍是唯一的 `wait` 调用者：对处于"启动中"的 pid，reaper 把停止事件（`WIFSTOPPED`）经登记表交给该启动路径，不收割、不丢弃。
6. ptrace 不可用（`ptrace_scope ≥ 2`、LSM 拒绝）→ `start_err{init/ptrace: …}`，不退回启发式判据；`hostcheck` 预先检查（Task 12）。
7. 第一步验证：helper 在被跟踪的情况下 execve 后能否获得 `{SETUID, SETGID, SETPCAP}`（取决于 init 的 permitted，见本节开头的规格增量）。测试先行；不成立则停止报告。

**Tests（root）：** R3 矩阵——workload `true`（快速退出）→ `start_ack` + exit 0；helper 在每个降权步骤注入失败（12 个注入点）→ 对应 `start_err`、workload 未运行（以 workload 写入的标记文件不存在判定）；helper 在 exec 前被 SIGKILL（注入点处自杀）→ `start_err{helper/died_before_exec: signal killed}`；workload 二进制与 helper 同名（复制为同名文件、`PR_SET_NAME` 相同）→ 判定不受影响；workload 被 SIGKILL 立即终止 → `start_ack` + 信号退出；并发 32 次启动无错配。workload 内核验：uid/gid 1000、能力全 0（逐线程）、`NoNewPrivs: 1`、securebits 0x0f 锁定、除 0/1/2 外无 FD、`TracerPid: 0`（已 detach）。

**验证：** 同 Task 9。

### Task 12：宿主检查、构建约束与 §16.2 生产验收

**Files:** Modify `internal/hostcheck/*`、`internal/archtest/archtest_test.go`、`.github/workflows/ci.yml`、`scripts/ci/check-runner.sh`；Create `internal/sandbox/isolation_test.go`（或并入 `spawn_test.go`，以本包唯一测试文件规则为准）与检查器（移植 spike `check.go` 为测试辅助二进制）。

**规则：** `hostcheck` 增加：新挂载 API（`open_tree`/`mount_setattr` 可用）、`close_range`、`kernel.yama.ptrace_scope ≤ 1`、user namespace 可用（含 AppArmor 的非特权 userns 限制不影响 root 创建）；`agentbox doctor` 输出每项结果。构建约束：helper 所在二进制以 `CGO_ENABLED=0` 构建——`internal/sandbox` 增加 `//go:build cgo` 的文件使 helper 在 cgo 构建下拒绝运行（`start_err{helper/cgo_enabled}`），CI 的构建步骤固定 `CGO_ENABLED=0`。

**§16.2 生产验收（检查器在生产启动路径启动的 workload 内运行）：** 规格 §16.2 全部条目，`Seccomp_filters ≥ 宿主基线 + 1`（基线在宿主上同法读取）；启动成功判据条目；server 凭据不变条目。CI 的 `linux-integration`（ubuntu-24.04，x86_64）实际运行且无 skip。验收范围按 §16.2 表述：WSL2 指定环境 + CI 的 GitHub runner；不代表其他 Linux 环境或 aarch64。

**验证：** root：`go test -count=1 ./internal/sandbox/ ./internal/hostcheck/`；`sudo ./bin/agentbox doctor`。

### Task 13：启用 server 与真实隔离环境的端到端验收

**Files:** Modify `cmd/agentbox/server.go`（去掉"生产启动器未就绪"的拒绝，装配生产启动器与模板配置）、`tests/e2e/e2e_test.go`（追加 root 用例）、`README.md`（快速开始）、`deploy/`（配置示例）。

**规则与验收：** root 下以 `provider/local` + 生产启动器运行：首个切片（提交 → checkpoint → 杀死 Worker → 恢复 → result）、E1、E4（降低次数，记录 seed）、E7、E8、E10；E2（Worker 主进程超出 `memory.max` → `worker_oom_likely` 与 OOM 重试）、E3（子进程 OOM）；E5 的物理回收部分（server 被 SIGKILL 后重启，残留执行树经 cgroup 停止、孤儿环境回收、无误隔离）；每个用例结束运行 `verify-invariants --quiescent`。累计运行时限按规格 §14.4：时限到期 → `task_deadline_exceeded`，重启不重置（一条用例）。README 增加可复现快速开始（`docker compose` 起 PostgreSQL → 构建 → `sudo agentbox server` → `agentbox task submit/watch/result`），只写本任务实际执行过的命令；配置示例与 `server` 标志一致。

**验证：** root：`CI=true go test -count=1 ./tests/e2e/...`（连续三次）；按 README 快速开始逐条执行一遍并记录输出。

**M1 门槛**：Task 13 通过后，Plan 2 联合验收（含本节）与 M1 门槛一并判定。

## 自查记录

- **契约覆盖**：契约第 2 节类型 → Task 1；第 3 节各操作 → Task 7（StartExec）、Task 8（其余）；第 4 节错误 → Task 1 定义、Task 7–8 产生；第 5 节闸门与测试 → Task 7、Task 8、providertest；第 6 节归属 → Task 8（owner.json、命名）。
- **规格覆盖**：§4.2 → Task 4；§4.3 → Task 5、6；§4.4 → Task 6（terminate）、Task 8（Stop/Destroy）；§4.5 seccomp → Task 3，其余隔离项 → 1B 之后；§9.2 旧代码处理 → Task 2；§18 Task 7–11 的修订 → Task 5–8。
- **未在本计划内验证**：真实隔离（namespace、降权、挂载）——等待 1B。
- **占位符**：无；接口签名为本计划的契约，实现时如需偏离，先修订本计划。
