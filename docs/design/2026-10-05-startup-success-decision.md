# 决策说明：沙箱 workload 的「启动成功」判据（Plan 2 Task 11 门槛）

> 状态：决策说明，待审阅。Task 11–13 在此前保持阻塞；原能力边界（init permitted = effective = {KILL}）保持不变；M1 不记为已验收；进度分母不变（12）。
> 本说明只做调研与比较，不包含能力操作或 ptrace 执行。安全工具拦截另见文末，与技术结论分开。

## 1. 我们确切需要的启动保证

`local.StartExec`（以及环境内每个 workload 的启动）需要：在 `start_ack` 之前，**host 侧能可靠区分以下三类结局，且三类互斥、无误判**：

- **A 已启动**：降权 helper 成功 `execve` 了 workload，workload 开始按自身生命周期运行（之后的退出是正常 `exit`，不是启动失败）。
- **B 启动失败**：降权序列任一步失败，或 `execve` 本身失败（如 ENOENT）。workload **从未运行**，须回 `start_err{reason}`。
- **C helper 在 execve 之前死亡**（被 SIGKILL、崩溃）。workload **从未运行**，须回 `start_err{died_before_exec}`。

额外约束（规格 §4.6 实现门槛、用户 2026-10-05 审阅）：判据**不得依赖 pipe EOF 单独**、不得依赖进程名（comm）或轮询；`exec 成功` 与 `Worker 就绪握手`（协议层 ready）是两回事，不得合并。

## 2. 已验证事实 vs 未验证假设

**已由 spike-1b 在 WSL2 6.6 x86_64 上验证（`docs/experiments/2026-10-05-spike-1b.md`）：**
- 降权序列本身（附加组清空、securebits 时机、逐线程能力、TSYNC seccomp、FD 卫生）成立。
- init 经 `execveat(fd,"",AT_EMPTY_PATH)` + raw clone 启动 helper 成立。
- **exec-status 管道（CLOEXEC）已存在**：helper 失败时写原因文本，host 读到；成功 execve 时管道因 CLOEXEC 关闭。
- spike 当时用 **comm 变化 + 短轮询** 区分 C 类——该启发式被明确否决（误判、依赖进程名）。

**未验证假设（本环境无法取证）：**
- 「被跟踪进程 execve 后 permitted 被限制在 tracer 的 permitted 内」这一内核行为，以及由此需要的 init permitted 扩展 {KILL,SETUID,SETGID,SETPCAP}——**未跑通，安全工具拦截**（文末）。
- `PTRACE_EVENT_EXEC` 在本降权路径下按预期投递、`PTRACE_O_EXITKILL` 的清理语义——未取证。

## 3. 与成熟运行时的比较（主来源）

| 方案 | 谁发信号 / 何时 | 证明了什么 | 需要的权限 |
|---|---|---|---|
| **runc/crun `exec.fifo`** | init 自身在 `execve` **之前**写 FIFO；父进程读到即视为「已启动」 | init 到达了「即将 exec 用户进程」这一点——**不证明 execve 成功**，是一个约定的 commit point | 无特殊权限；不用 ptrace |
| **CLOEXEC errno 管道**（glibc/libiberty、UML 的标准做法） | 子进程：execve 失败→写 errno 后退出；成功→内核关 fd | 区分 A/B（EOF 无字节=成功路径；有字节=errno=失败）；**单凭 EOF 无法区分 A 与 C** | 无特殊权限 |
| **本提案 ptrace `PTRACE_EVENT_EXEC`** | 内核在 workload execve 完成、首条指令前投递事件 | 唯一能对 A/B/C **全部无误判** 的正向证据 | 需 init permitted 扩展 + tracer 线程纪律 + reaper 集成 |

要点：**成熟运行时不用 ptrace**。runc 的 FIFO 是 init 自身在 exec 前的协作信号，runc 把「写了 FIFO」当作提交点，并**不**区分「workload 在 exec 后 1µs 被 SIGKILL」与「init 在 exec 前 1µs 被 SIGKILL」——它接受一个定义好的 commit point，而非证明首条指令已执行。

主来源：
- runc PR #886（以 FIFO 取代信号同步）：https://github.com/opencontainers/runc/pull/886
- OCI runtime create 流程：https://acotten.com/2023/08/17/oci-runtime-create-flow/
- CLOEXEC errno 管道模式（libiberty / UML）：execve 成功则 fd 自动关闭、父读 EOF；失败则子写 errno。

## 4. 逐项评估

**权限需求**：exec-status 管道方案**不需要**任何能力扩展——保持原 permitted=effective={KILL} 边界（用户要求保持）。ptrace 方案需要扩大 init permitted，且该扩大本身尚未验证。

**失败语义**：
- A/B 两类，exec-status 管道已足够且无误判（errno vs EOF-无字节），**前提是同时读 reaper 的 wait 结果**，不单凭 EOF。
- C 类（exec 前被杀）与 A 类的「workload exec 后立即被信号杀死」在「EOF 无字节 + 以信号终止」这一点上**存在不可消除的微小竞争窗口**（从 helper 写完最后一步到 execve 返回之间）。只有内核正向事件（ptrace EVENT_EXEC，或 seccomp user-notif on execve）能完全消除它。

**reaper 集成**：exec-status 管道方案——reaper 仍是唯一 `wait` 者，启动路径只读管道 + 查 reaper 记录的子进程 wait 状态；复杂度低。ptrace 方案——tracer 必须是 clone 子进程的那个 OS 线程，`reg.mu` 须在等待事件前释放、reaper 对「启动中」pid 的停止事件须非阻塞转交；复杂度显著更高（用户已就此列出五条前置约束）。

**实现复杂度**：管道+wait ≈ spike-1b 现有 exec-status 管道去掉 comm 启发式，加一段「EOF 时查 wait 状态」的消歧；小。ptrace：新增 tracer 状态机、线程纪律、能力扩展、与 reaper 的事件路由；大。

**验证需求**：管道+wait 可在本环境（WSL root）用纯管道/wait 取证，与 spike-1b 同类，**不触发安全工具**。ptrace 的关键前提（能力限制行为、EVENT_EXEC 投递）在本会话**无法取证**。

## 5. 建议：最小且可落地的 sound 方案

**采用 CLOEXEC exec-status 管道 + reaper wait 状态，并显式定义 commit point；放弃 ptrace 路径。**

判据（无 ptrace、无 comm、无轮询、不单凭 EOF）：
1. helper 的 exec-status 管道为 O_CLOEXEC。降权任一步失败 → 写 `helper/<step>: errno` 后 `exit(126)`；`execve(workload)` 失败 → 写 `execve: errno` 后退出。
2. host 启动路径**同时**等待两个事实：管道可读结果，与 reaper 对该直接子进程的 wait 结果。
   - 管道读到**字节** → B 类 `start_err{reason}`（workload 未运行）。
   - 管道 **EOF 无字节** 且 reaper 显示子进程**仍存活或已作为 workload 正常退出（exit，非降权期信号）** → A 类 `start_ack`。
   - 管道 **EOF 无字节** 且 reaper 显示子进程在产生任何 workload 行为前**以信号终止** → C 类 `start_err{died_before_exec}`。
3. **commit point** 明确记入规格：与 runc 一致，「execve 成功即视为已启动」，host 不证明 workload 首条指令已执行；§1 的 A 定义据此收敛。exec 成功仍与协议层 ready 握手分开。

此方案保持原能力边界、reaper 复杂度低、可在本环境验证，且与成熟运行时的做法一致。

**残余局限（须在规格写明，不得overclaim）**：§4 所述「exec 前最后几条指令被 SIGKILL」与「exec 后立即被 SIGKILL」的微小窗口，本方案按 commit point 归类，**不声称**对该窗口做到内核级无误判。若项目确需消除该窗口（真正证明首条指令已运行），只有内核正向 exec 事件可行。

## 6. 若需要更强保证所需的外部工程验证（并就此停止）

若审阅后认为 commit-point 不够、必须内核级证明 execve 完成，则需要在本会话之外完成下列任一项的工程验证，且因安全工具限制**本会话不继续该工作**：
- **ptrace `PTRACE_EVENT_EXEC`**：验证被跟踪 execve 的 permitted 限制行为、所需 init permitted 扩展、EVENT_EXEC 投递与 `PTRACE_O_EXITKILL` 清理、tracer 线程纪律；或
- **seccomp user-notification（`SECCOMP_RET_USER_NOTIF`）on execve**：验证 notif 在降权 seccomp 安装后仍可投递、监听端生命周期与 reaper 的关系。

两者都需在真实 Linux 目标环境（非本会话）跑通后回写，再决定是否替换 §5 方案。

## 7. 安全工具拦截（与技术结论分开）

本会话中，用于验证 ptrace 能力前提的有界实验被安全分类器拦截：子代理一次、协调者两次，均在写入/运行前停止，无代码产出，`m1-exp-ptrace` 分支已删除。该实验是对用户自有沙箱运行时的、用户已授权的一等公民安全工程（能力降权 + ptrace 跨特权边界取证），拦截为误报——其形态与进程注入/沙箱逃逸工具相似，分类器据形态而非项目合法性反应。此为工具环境限制，非技术失败；已另行排入反馈草稿（经用户 `/feedback` 批准才发送）。**按用户指示不再以新会话、其他代理或改写提示重试该被拦截的实验。**
