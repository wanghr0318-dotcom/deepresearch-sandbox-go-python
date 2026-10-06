# M4 Plan 15 验收记录：独立 exec 沙箱与加固（WSL2 本地部分）

> 日期 2026-10-06。分支 `m4-p15-t12`（基于 `m4-batch` 948d8f8）。本文件记录 Plan 15 Task 12 在本地 WSL2 上的验收。**服务器（CVM ins-22zj0xj5）、CI 与真实模型 `run_python` 的验收由协调者在联合验收中补入本文件**，在此之前这些项不成立。文中不含 token、cookie 或密钥。

## 环境

| 项 | 值 |
|---|---|
| 内核 | `Linux 6.6.87.2-microsoft-standard-WSL2 x86_64`（WSL2，Ubuntu 26.04 LTS） |
| cgroup | v2（`cgroup2fs`），24 CPU，15 GiB 内存 |
| Go / Python | go1.27.1 linux/amd64（`GOTOOLCHAIN=local`，`CGO_ENABLED=0`）；Python 3.14.4（exec 模板的 `/usr/bin/python3`） |
| 依赖 | PostgreSQL 16（127.0.0.1:5432）、Redis（127.0.0.1:6379），Docker |
| 服务器 | 待补（联合验收：`agentbox doctor` 的内核、架构、hostcheck 与 Warnings） |

## 命令

root（WSL `-u root`，`HOME=/root`、`TMPDIR=/tmp`、`GOFLAGS="-mod=mod -buildvcs=false"`、离线 GOPROXY；`CI=true`、`AGENTBOX_TEST_DATABASE_URL`、`AGENTBOX_TEST_REDIS_ADDR`）：

```bash
CGO_ENABLED=0 go test -p 1 -count=3 -v -run 'TestRealExec|TestRealE3[5-8]' ./tests/e2e/
CGO_ENABLED=0 go test -p 1 -count=1 -timeout 60m -v ./internal/... ./tests/e2e/...
```

非 root：`go vet ./...`、`GOOS=windows go build ./...`、`CI=true go test -count=1 ./...`（PostgreSQL + Redis）。

## 结果摘要

| 项 | 结果 |
|---|---|
| 新增 root e2e（5 个用例，×3） | 全部 PASS（`ok tests/e2e 78.7s`） |
| root 全量 `./internal/... ./tests/e2e/...` | exit 0：34 个包 ok，674 个顶层用例 PASS（含子测试 1842），唯一 SKIP 为允许的 `TestCacheBenefit`，0 FAIL；tests/e2e 441.6 s |
| 非 root 全量（DB + Redis） | exit 0：36 个包 ok |
| gofmt（CR 去除后）、vet、Windows 构建 | 通过 |

## 逐项（exec 真实链路与 E35–E39）

全部在真实沙箱（provider/local + 生产启动器）中以 root 运行；Worker 是沙箱中的 sim_worker，经 `/run/agentbox/gateway.sock` 调 `POST /v1/exec`；每次 exec 一个独立的 exec 环境。三轮的观测值一致，下面取第 1 轮。

| 用例 | 断言 | 观测 |
|---|---|---|
| `TestRealExecViaGateway` | fetch（fake upstream）→ exec x1 以 fetch 结果 blob 为 `/in/page.json` 写 `/out/result.csv` → exec x2 以其输出为 `/in/data/result.csv` 写 `total.txt`；两次 `completed` 退出码 0；输出经 attempt 的 Gateway `GET /blobs/{sha}` 读回预期内容；checkpoint refs 含全部 blob；`exec_count_used = 2`；每次 exec 的环境 kind = exec、独立于 attempt 环境、已停止并删除；任务成功；静止时 I1–I16 通过 | result.csv `73aaed7abdff…`、total.txt `673650f936cb…`（`40\n`），两个不同的 exec 环境 |
| `TestRealE35BackgroundWriter` | 主进程 fork 后台写入者（继承 stdout/stderr 管道）后退出；结果中 `log` 的 size/sha 与 blob 一致、两次读取相同；内容是从 0 开始的连续完整行，且含主进程退出后写入的行；调用不因管道被持有而挂起；exec 环境 cgroup 与目录随后删除 | 1395–1673 行，其中主进程退出后 805–991 行；wall 6–7 ms |
| `TestRealE36StdoutFlood` | 100 MB stdout：不挂起，stdout 恰为 1 MiB 且 `stdout_truncated`，stderr 完整，任务按时完成 | 保留 1048576 字节；exec wall 75–83 ms，任务约 0.5 s |
| `TestRealE37ServerKilledDuringExec/rerun` | exec（sleep 8 s）运行中 SIGKILL `cmd/agentbox server` 并重启：旧 exec 环境的 `stopped_at` 早于新 try 环境的建立；旧 try `unknown`；attempt 1 `lost_on_restart`；attempt 2 以同一 call id 重发，新 try 在新环境中 `ok`，任务成功；`verify-invariants --quiescent` 通过 | 旧环境 stopped_at 与新环境建立相隔 1.4–2.0 s |
| `TestRealE37ServerKilledDuringExec/cancelled_not_rerun` | 取消（202）后立即 SIGKILL server 并重启：任务 `cancelled`，只有原来的 1 个 try 且其环境已停止，没有重跑 | 三轮中 try 1 均为 `unknown/server_restart`（server 在取消生效前被杀死，由启动恢复停止环境） |
| `TestRealE38ExecPressure` | 4 个暂停中的任务经各自的 socket 同时发出 8 个 exec（每任务 2，全局 slots 4）：① `/out` 写满后分配内存超过 `memory.max` 96 MiB；② 创建 2000 个文件；③ 其余 6 个 sleep 3 s。断言：① 写满报错且环境内 OOM 如实报告；② ENOSPC、收集 256、其余 `too_many`；③ 同时存活的 exec 环境 ≤ 4，等待者有 `queue_ms`；压力期间 `GET /status`、`GET /tasks/{id}` 每次 < 2 s；全部环境回收，静止时 I1、I11、I12 等通过 | ① `fill EFBIG 64`（64 MiB 时先触及 `RLIMIT_FSIZE`，与 tmpfs 写满同一界限），随后 signal 9、`oom_observed`、`oom_kill_delta 1`；② `files ENOSPC 1023`，收集 256、too_many 767；③ 最多 4 个同时存活，4 个排队 ≥ 100 ms；控制面轮询 124–126 次，最大延迟 2 ms |
| E39（UID 范围复用） | 已在 Plan 15 Task 5 合入：`internal/provider/local` 的 `TestE39UIDRangeReuse`（root）与 `resource` 的回收/隔离用例；本次 root 全量中重跑 | 见 root 全量结果 |

与任务说明的差异：

- E38 ③ 按默认 slots 4 运行，因此是"第 5 个起排队"，而不是说明中的"第 9 个"（说明假设了 8 个 slots）。
- E38 ① 在默认 `--exec-out-bytes 64 MiB` 下先得到 `EFBIG`（`RLIMIT_FSIZE` = `/out` 大小），断言接受 ENOSPC 或 EFBIG。
- `TestRealE34ExecViaGateway` 与 `TestRealE39UIDReuseViaTasks` 未编写：E34 见下节；E39 已由 Task 5 的用例覆盖。

## E34（权限边界）

按项目负责人的决定（2026-10-06）**不另写提权/攻击探针**。E34 记录为由以下已有测试覆盖——**不是穷尽证明，也不是无逃逸证明**：

- seccomp 拒绝探针（`internal/sandbox`：`TestSeccompFilterRules`、`TestSeccompOrchestratorFilterInChildProcess` 等；exec 与 orchestrator 两套配置规则相同）：mount/unshare/setns/pivot_root/CLONE_NEW*、ptrace/process_vm_*（以无效参数调用一次，断言 `EPERM`，不建立跟踪关系）、bpf/io_uring/keyctl/perf_event_open/userfaultfd、非 AF_UNIX socket；
- 能力集与启动判据的回归锁：`TestInitCapsBoundaryIsFinal`（init permitted = effective = {KILL}，bounding = {KILL, SETUID, SETGID, SETPCAP}）、`TestNoPtraceStartDetection`（sandbox 源码不含 ptrace 跟踪调用）；
- §16.2 生产验收 `TestIsolationAcceptance16_2`（含 exec 环境场景：`/in`、`/out`）。

本次 root 全量中的结果：`TestSeccompFilterRules`（exec/orchestrator × amd64/arm64）、`TestSeccompBuildFilterRejectsUnknownInputs`、`TestSeccompSyscallNumbersMatchStdlib`、`TestSeccompOrchestratorFilterInChildProcess`、`TestInitCapsBoundaryIsFinal`、`TestNoPtraceStartDetection` 全部 PASS；`TestIsolationAcceptance16_2` 的 11 个条目全部 PASS（含"07 Gateway socket：编排环境 uid 1000 可访问、exec 环境不可见"与"补充 §4.5 隔离配置"）；`TestE39UIDRangeReuse` PASS。

## 本任务修复的缺陷

- **inspect 的 attempt 行被 exec 环境复制**（`internal/persistence/postgres/apiread.go`）：exec 环境同样记录 `attempt_id`，`Inspect` 以 `environments.attempt_id` 连接 attempt，每个 exec 环境多出一行 attempt（E37 首轮观测到 "lost_on_restart, lost_on_restart, succeeded, succeeded"）。连接改为排除 `kind = 'exec'`（exec 环境见调用 try 的 `env_id`）；`TestExecCleanupCandidateAndInspect` 增加断言（修复前失败、修复后通过）。I2 与 `admitAttempt` 的同类连接有意包含 exec 环境，未改。

## 待联合验收补入

1. 服务器（CVM，经 SSH 隧道）：`agentbox doctor`；同上的 root 序列（串行）；E34 相关套件、16.2、E35–E38 无环境 skip。
2. CI：correctness、linux-integration（零未允许 skip）、python、web。
3. 服务器真实链路：运维 Bearer 提交 sim-worker 任务（`fetch` + 两次 `exec`）并 `inspect`；普通用户在对话中让 Agent 用 `run_python` 计算一个数字（需先把 `RunPython` 注册到对话 Agent 的工具表，见 Plan 15 Task 11 报告）；`verify-invariants --quiescent`。
