# 沙箱化深度研究助手（Go + Python）

**运行在自研 Agent Runtime 上的对话式 DeepResearch 助手。** Go 负责执行与全部安全边界，Python 负责研究逻辑。

[English](README.en.md) · [使用与运维参考](docs/usage.zh-CN.md) · [设计规格](docs/design/2026-10-03-v0.2-first-release-design.md)

用户在对话界面中提问，Agent 直接回答，或开始一轮深度研究：

1. 读取研究 skill。
2. 范围不明确时，问至多 3 个选择题。
3. 写待办清单。
4. 并行研究 2–4 个子主题。
5. 写出带编号引用的报告，引用只指向本轮实际读过的网页。

用户随时可以停止，看到"目前发现"的简短摘要，然后选择继续、立即用已有资料写报告，或之后恢复被取消的一轮。

这个项目真正的重点在下面一层：单主机的 **Agent Runtime**，在 Linux 沙箱中运行 Agent 代码。崩溃和重启不会丢失已提交的进度，API Key 完全不进入沙箱。

## 架构

```
浏览器（Vue 3）/ CLI ── REST + SSE ──► Go 控制面
                                        ├─ 任务与会话状态机、准入、调度
                                        ├─ checkpoint、崩溃恢复、事件续传（Last-Event-ID）
                                        ├─ Gateway：凭据隔离、调用 journal、预算、每轮工具额度、
                                        │           防 SSRF 的出网、exec 调度
                                        ├─ PostgreSQL（唯一事实来源）   Redis（只做缓存）
                                        └─ 沙箱：user/pid/mount/net 命名空间、映射 UID、
                                           只读 rootfs、seccomp、能力集 {KILL}、cgroup v2
                                                  │
                        ┌─────────────────────────┴─────────────────────────┐
             编排沙箱：Python Agent                               exec 沙箱：模型生成的 Python
             （经 Unix socket 访问 Gateway）                      （无网络、无 Gateway、单次）
```

## 实现了什么

| 方面 | 内容 |
|---|---|
| **崩溃安全的执行** | Worker 向宿主提交 checkpoint；Worker 被杀、server 被杀或数据库断连后，从最后一个已提交的 checkpoint 恢复。`agentbox verify-invariants` 按 16 条不变量核对存储状态；故障注入模式在测试中驱动各崩溃路径。 |
| **凭据隔离** | 模型与搜索 Key 只在宿主进程内。沙箱经每个 attempt 独立的 Unix socket Gateway 访问供应商；Gateway 记录每次调用、执行预算，并可在调用中途撤销访问。研究演示（`scripts/demo-m2.sh`）读取沙箱内每个进程的 `/proc/<pid>/environ` 与命令行，确认看不到任何 Key。 |
| **沙箱** | 命名空间、带回收检查的映射 UID 范围、只读 rootfs 模板、seccomp、能力集 `{KILL}`，以及 cgroup v2 的内存、进程数与 CPU 限额；执行树确认清空后才清理。 |
| **会话** | 每个对话是一个长期运行的沙箱进程：空闲 10 分钟冻结（cgroup freezer），1 小时后驱逐，下一条消息从最后提交的会话 checkpoint 恢复。 |
| **Agent** | 工具调用式 Agent：渐进披露的 skill（`read_skill`）、`ask_user`、待办清单、`web_search`、`web_fetch`、`run_python`。每轮 30 次搜索与抓取，由 Gateway 强制，不靠模型自觉。 |
| **并行 sub-run** | 同一轮内 2–4 个子主题并发执行，采用两层账本（task 与 sub-run）；可以取消，停止或崩溃后继续时，已完成的子主题不会重跑。 |
| **模型降级链** | 一个逻辑模型可由按顺序排列的多个 OpenAI 兼容供应商提供：可重试的失败立即转下一个供应商，每次尝试都是 journal 中独立计价的 try；每个供应商一个熔断器，全部熔断时立即返回 `503 model_degraded`；可选对冲请求。重放与指纹不受影响。 |
| **exec 沙箱** | 模型写的 Python 每次在全新、无网络的环境中运行，有独立 UID、CPU 与墙钟配额，并收集输出文件。 |
| **产品层** | 账号（PBKDF2、`HttpOnly` 会话、登录限速）；用户隔离（他人的数据一律 404）；费用、模型与内部 ID 在服务端脱敏；CSP 与 Markdown 安全渲染。 |

## 证据

所有数字都来自有记录的运行，证据文件写明每次运行证明了什么、没有证明什么。

- **测试：**
  - 约 700 个 Go 测试函数，含以 root 在真实沙箱中运行的端到端测试；
  - 约 280 个 Python 测试；
  - 约 170 个 Web 组件测试。
- **CI（6 个作业）：**
  - 非 root 全量（`-race`）；
  - 在 `ubuntu-24.04` 上以 root 在真实沙箱中运行的全量；
  - Python 3.11 与 3.13；
  - Web 的 lint、类型检查与测试；
  - lint 与复杂度报告。
- **真实验收**：在 4 vCPU / 8 GiB 的 Linux 云主机（腾讯云上海）上，使用真实模型（Moonshot：`kimi-k3` 主导、`kimi-k2.6` 执行）与经 Serper 的 Google 搜索结果。记录：[对话助手](docs/evidence/2026-10-06-m4-chat-acceptance.md)、[exec 沙箱](docs/evidence/2026-10-06-m4-exec-hardening.md)、[账号](docs/evidence/2026-10-06-m3-accounts.md)、[备份与恢复](docs/evidence/2026-10-06-backup-restore.md)。
- **模型降级链**（[记录](docs/evidence/2026-10-10-model-fallback.md)，fake upstream、零费用）：主供应商持续 503 时，无降级链的调用 3.7–5.1 s 后失败（3 次 try 加退避），有降级链时约 51 ms 由后备供应商完成（与健康供应商相同）；全部熔断时约 40 µs 返回 `model_degraded`。
- **串行与并行研究对比**（[记录](docs/evidence/2026-10-06-m4-subrun-comparison.md)）。小样本，每组 N = 4：

  | | 串行 | 并行 |
  |---|---|---|
  | 墙钟时间中位数 | 302 s | 184 s |
  | 每次费用中位数 | 0.48 USD | 0.50 USD |
  | 失败 | 0 | 0 |
  | 可定位的引用 | 100% | 100% |

## 快速开始

依赖：Linux（cgroup v2）或 WSL2、Go 1.24+、Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)、Docker（运行 PostgreSQL）、Node.js 24（Web 界面）。

```bash
# 单元测试（不需要数据库；需要 root 的用例会跳过）
go vet ./...
go test ./...

# 依赖数据库的测试
docker compose -f deploy/docker-compose.yml up -d --wait
export AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable'
CI=true go test -count=1 ./internal/persistence/postgres/ ./tests/e2e/...

# Python Worker SDK 与 Agent
(cd worker && uv run pytest -q)

# Web 界面
(cd web && npm ci && npm test && npm run build)
```

**真实沙箱演示**：`sudo bash scripts/demo-m1.sh` 以 root 在全新的数据目录上跑完整条链路：

1. 提交任务；
2. 写 checkpoint；
3. 杀死 Worker 后恢复；
4. 输出结果；
5. SIGKILL server 后重启恢复；
6. 检查不变量与泄漏。

[运行记录](docs/evidence/2026-10-05-m1-demo-run.md)。

**运行完整助手**（沙箱、Gateway、真实模型、对话界面）需要一个模型 Key 和一个搜索 Key。逐步命令与全部 server 标志见[使用参考](docs/usage.zh-CN.md)；演示服务器所用 unit 的模板是 [`deploy/systemd/agentbox-demo.service.example`](deploy/systemd/agentbox-demo.service.example)（复制到 `/etc/systemd/system/`，并在 `/etc/agentbox/agentbox.env` 中设置 `AGENTBOX_PUBLIC_HOST`）。

## 目录

| 路径 | 内容 |
|---|---|
| `cmd/agentbox` | 单一二进制：`server`、`doctor`、`task …`、`user …`、`verify-invariants` |
| `internal/sandbox`、`provider/local`、`cgroup`、`rootfs`、`hostcheck` | 沙箱启动器、init、环境生命周期、宿主自检 |
| `internal/task`、`runner`、`session`、`subrun`、`admission`、`resource` | 控制面：actor、attempt、会话、sub-run、准入 |
| `internal/gateway/{edge,call,upstream,cache}` | Gateway：每 attempt 的 socket、调用 journal 与账本、供应商 adapter、Redis 缓存 |
| `internal/persistence/postgres`、`blob`、`recovery`、`reconcile`、`invariants` | 存储、内容寻址的 blob、启动恢复、不变量检查 |
| `internal/api`、`account` | REST/SSE API（[OpenAPI](api/openapi.yaml)）、账号 |
| `protocol/` | 版本化的 Go ↔ Python Worker 协议与共享 fixtures |
| `worker/` | Python Worker SDK、`chatagent`、工具、`deep-research` skill |
| `web/` | Vue 3 + TypeScript 的对话界面与运维工作台 |
| `tests/e2e` | 端到端测试（进程型 provider 与真实沙箱） |
| `docs/` | [设计](docs/design/)、[证据](docs/evidence/)、[使用参考](docs/usage.zh-CN.md) |

## 能力边界

以下边界在设计中写明，不加掩饰：

- **容器级隔离，共享宿主内核。** 权限边界测试是回归测试，不是无逃逸证明；不应依赖它隔离恶意的多租户代码。
- **单执行主机。** 没有跨主机故障接管。
- **不保证 exactly-once。** 外部调用不保证 exactly-once；结果未知时按保守方式计费，预算可能略有超出。
- **Workspace 不回滚。** workspace 不随 checkpoint 回滚。

## 致谢

参考其设计，代码自行实现：[runc](https://github.com/opencontainers/runc)（re-exec init、降权、`/proc` 掩蔽）、[Tencent Cloud CubeSandbox](https://github.com/tencentcloud/CubeSandbox)（Agent 沙箱能力面）、E2B（沙箱内 daemon 语义）。研究流程参考 gpt-researcher、dzhng/deep-research 与 Anthropic 关于多 Agent 研究系统的文章。

## License

[MIT](LICENSE)
