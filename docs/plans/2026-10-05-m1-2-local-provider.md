# M1 Plan 2：LocalProvider（不依赖 Plan 1B 的部分）实施计划

> **执行方式**：主 agent 逐任务实现与评审（项目负责人 2026-10-04 的要求：不使用子 agent）。每个任务 TDD：先写本任务列出的测试，确认失败，再实现，按"验证"一节的命令通过后提交。

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
type Message struct { Type string; ExecID string; Spec json.RawMessage; PID int; Reason string; Exit *ExitInfo }
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

**规则：** 不解析 workload 输出；`terminate` 对未知 `exec_id` 忽略；多个 exec 并行；不依赖 Pdeathsig。

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
- `Scan`：独立扫描环境目录（含无/损坏 owner.json）、`mountinfo` 中数据目录下的挂载、`agentbox-*` cgroup（含其他 install_id）、`<data>/envs/*/gw.sock`；逐项分类 `OwnedComplete`/`OwnedPartial`/`Foreign`/`Unknown`。UID 范围文件属主一层在 1B 引入 UID 映射后补充（见文末）。
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

## 依赖 Plan 1B 的任务（本计划不编写，1B 结论回写规格 §4.6 后补充为 Task 9 起）

| 任务 | 依赖的 1B 结论 |
|---|---|
| 生产 `EnvStarter`：clone 各 namespace、`setgroups=deny` 与 UID/GID 映射、附加组清空 | B1 |
| 挂载：只读 rootfs bind、tmpfs、`/proc` 掩蔽、不挂载 `/sys`、pivot_root 与脱离旧根；Gateway socket 与 `/in`、`/out` 挂载 | B2 |
| stage-2 helper：bounding 清空、setresuid/gid、能力清空、securebits、no_new_privs、seccomp（TSYNC）安装、FD 全部 close-on-exec | B3、B4、B5 |
| `Launcher` 的生产实现与 `start_err` 原因文本 | B6 |
| `hostcheck` 的挂载 API 与内核能力检查；`Scan` 的 UID 范围文件属主一层 | B1、B2、B4 |
| 验收：§16.2 spike 验收、E34、I11 的隔离部分；E2、E3 在真实环境中的运行 | 全部 |

## 自查记录

- **契约覆盖**：契约第 2 节类型 → Task 1；第 3 节各操作 → Task 7（StartExec）、Task 8（其余）；第 4 节错误 → Task 1 定义、Task 7–8 产生；第 5 节闸门与测试 → Task 7、Task 8、providertest；第 6 节归属 → Task 8（owner.json、命名）。
- **规格覆盖**：§4.2 → Task 4；§4.3 → Task 5、6；§4.4 → Task 6（terminate）、Task 8（Stop/Destroy）；§4.5 seccomp → Task 3，其余隔离项 → 1B 之后；§9.2 旧代码处理 → Task 2；§18 Task 7–11 的修订 → Task 5–8。
- **未在本计划内验证**：真实隔离（namespace、降权、挂载）——等待 1B。
- **占位符**：无；接口签名为本计划的契约，实现时如需偏离，先修订本计划。
