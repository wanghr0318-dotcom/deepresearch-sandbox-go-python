# 沙箱化深度研究助手（Go + Python）

**运行在自研 Agent Runtime 上的对话式 DeepResearch 助手。** Go 负责执行与全部安全边界，Python 负责研究逻辑。

[English](README.en.md) · [使用与运维参考](docs/usage.zh-CN.md) · [设计规格](docs/design/2026-10-03-v0.2-first-release-design.md) · [术语表（E/I 编号、Plan、§）](docs/glossary.md)

用户在对话界面中提问，Agent 直接回答，或开始一轮深度研究：

1. 读取研究 skill。
2. 范围不明确时，问至多 3 个选择题。
3. 写待办清单。
4. 并行研究 2–4 个子主题。
5. 写出带编号引用的报告，引用只指向本轮实际读过的网页。

用户随时可以停止，看到"目前发现"的简短摘要，然后选择继续、立即用已有资料写报告，或之后恢复被取消的一轮。

这个项目真正的重点在下面一层：单主机的 **Agent Runtime**，在 Linux 沙箱中运行 Agent 代码。崩溃和重启不会丢失已提交的进度，API Key 完全不进入沙箱。

同一个控制面也是一个**多租户模型网关**：org、用户与平台 API key；按 key、用户、org 的限流与月度预算；OpenAI 与 Anthropic 两种格式的模型 API（任一格式的客户端到任一格式的供应商，含流式与工具调用）；用量、账单与对账。

## 架构

```
浏览器（Vue 3）/ CLI / 官方 OpenAI 与 Anthropic SDK
   │  REST + SSE；会话 cookie、运维 token 或平台 key（Bearer abx_live_… / x-api-key）
   ▼
Go 控制面
 ├─ 认证与租户：org → 用户 → key、scope、过期与吊销（PostgreSQL NOTIFY）
 ├─ 限流：rpm、tpm（key / 用户 / org；Redis Lua，不可用时退化为进程内）；并发流在每个 server 进程内计数
 ├─ 月度预算：每个请求或沙箱调用在各层级放一个 hold，结束后按实际费用结算
 ├─ 运行时 API：任务与会话状态机、准入、调度、checkpoint、崩溃恢复、事件续传（Last-Event-ID）
 ├─ 模型 API（--model-api）：/v1/chat/completions、/v1/messages、/v1/models
 │     └─ llmproto（OpenAI ⇄ Anthropic）→ 模型路由（权重、熔断、首字之前换供应商）
 ├─ Gateway：凭据隔离、调用 journal、预算、每轮工具额度、防 SSRF 的出网、exec 调度
 │     └─ 供应商：OpenAI 兼容与 Anthropic 格式（Key 只在宿主进程内）
 ├─ 用量：model_requests 与 hold → usage_daily → 账单与对账
 ├─ PostgreSQL（唯一事实来源）   Redis（缓存与限流，可丢弃）
 └─ 沙箱：user/pid/mount/net 命名空间、映射 UID、只读 rootfs、seccomp、能力集 {KILL}、cgroup v2
          │
   ┌──────┴───────────────────────────────┐
 编排沙箱：Python Agent                 exec 沙箱：模型生成的 Python
 （经 Unix socket 访问 Gateway）         （无网络、无 Gateway、单次）
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
| **模型降级链** | 一个逻辑模型可由多个供应商提供（OpenAI 兼容或 Anthropic 格式；沙箱 Worker 始终看到 OpenAI 格式）：可重试的失败立即转下一个供应商，每次尝试都是 journal 中独立计价的 try（可按缓存读取、写入单价计）；按顺序、平滑加权轮询或首 token 延迟分配流量；每个供应商一个熔断器，全部熔断时立即返回 `503 model_degraded`；可选对冲请求。重放与指纹不受影响。 |
| **exec 沙箱** | 模型写的 Python 每次在全新、无网络的环境中运行，有独立 UID、CPU 与墙钟配额，并收集输出文件。 |
| **工作区工具与 MCP（可选）** | `--workspace-tools`：每轮一个工作区，`write_file` / `read_file` / `list_dir` 与 `exec_shell`（每条命令一个全新的 exec 沙箱，文件在命令之间保留，按 exec 配额记账，路径只能是工作区内的相对路径，符号链接不会保留）。`--mcp-config`：Gateway 作为 MCP 客户端（stdio 或 streamable HTTP）连接运维配置的服务器，只暴露允许清单中的工具，调用记入 journal 并计入每轮工具额度；沙箱不接触 MCP 服务器与其凭据。[设计](docs/design/2026-10-10-shell-file-mcp-design.md)、[记录](docs/evidence/2026-10-10-shell-file-mcp.md)。 |
| **Kubernetes provider（可选）** | `--provider k8s` 把每个环境放进一个 Pod：经 pods/exec 运行镜像内的 helper 作为控制通道，非 root、只读根、丢弃全部 capability、无 ServiceAccount token、deny-all NetworkPolicy；worker 镜像按 digest 固定；预热池让环境就绪从约 1 s 降到约 10 ms。与本地 provider 通过同一套契约一致性测试。 |
| **评测平台** | `agentbox eval`：按 suite（YAML/JSON）经运维 API 批量运行编码/终端任务（检查器在 exec 沙箱中运行）与研究问题，可设并发、重复与 seed；manifest 固定 suite 哈希、服务端构建、模型、Worker 版本与 exec 摘要；导出轨迹 JSONL（事件、调用 journal、账本）；确定性评分（退出码、输出、引用可定位）与可选的有预算 LLM judge；报告与 `eval compare`。可对 fake upstream 零成本运行。 |
| **轨迹诊断** | `agentbox doctor-traces`：读评测运行目录或运行中 server 的一个时间窗口，找出失败与慢的任务，用确定性规则归因（模型 try 挂起、熔断、供应商错误、搜索/抓取被调用期限截断、预算耗尽、exec 超时、退避、对冲浪费、沙箱启动/排队/停止慢），每条附证据（任务与调用 ID、各供应商 try 的计数与延迟分位、延迟占比；观测栈在运行时加上 Tempo 中的 trace 与按 span 汇总的耗时、Loki 中带 trace ID 的失败日志）；在白名单与上下界内提出 server flag 修改，写成补丁与一份实验配置（从不修改运行中的 server）；可选的有预算模型总结。[设计](docs/design/2026-10-10-trace-doctor-design.md)、[记录](docs/evidence/2026-10-10-trace-doctor.md)。 |
| **产品层** | 账号（PBKDF2、`HttpOnly` 会话、登录限速）；用户隔离（他人的数据一律 404）；费用、模型与内部 ID 在服务端脱敏；CSP 与 Markdown 安全渲染。 |
| **多租户与平台 API key** | org → 用户 → key：每个用户属于一个 org（注册时建个人 org），owner 邀请成员、用户接受后才迁入，个人 org 不接受他人；平台 key `abx_live_…` 只存 HMAC（pepper 文件，指纹登记在库中，不一致时拒绝启动），可设 scope（`runtime` / `models`）、过期与吊销，以 `Authorization: Bearer` 或 `x-api-key` 调用运行时 API；校验结果缓存 30 s，吊销经 PostgreSQL `NOTIFY` 立即通知各 server；key 永远不能创建 key 或管理成员。任务与会话记录所属 org 与 key。[设计](docs/design/2026-10-11-model-gateway-design.md)。 |
| **模型 API** | `--model-api` 打开 OpenAI `/v1/chat/completions` 与 Anthropic `/v1/messages`（含流式与工具调用）以及 `/v1/models`，以 `models` scope 的平台 key 调用，经 Gateway 的模型路由（权重、熔断、首字之前换供应商）发往 OpenAI 或 Anthropic 格式的供应商：同格式原样转发，不同格式由 `internal/llmproto` 转换。每个请求按 key、用户、org 扣 rpm、tpm 与并发流名额，按最坏费用在月度预算中放 hold、结束后按实际用量结算（用量未知时按估算全额记为 unknown），PostgreSQL 不可用时 503；降级链文件的 `routing.primary_models` 可把主供应商限制在它自己的模型上；请求与每次尝试记入按月分区的 `model_requests`；指标含首字时间、流完整性与按原因的拒绝。官方 `openai` 与 `anthropic` Python SDK 的兼容性测试在 CI 中运行；`scripts/demo-gateway.sh` 零费用演示全部流程，负载测试与故障注入见下面的证据。 |
| **限额与月度预算** | org、用户、key 三级的每分钟请求数限额（Redis 中一个 Lua 脚本原子地检查所有层级；Redis 不可用或超过 50 ms 时退化为进程内桶，请求照常、指标报告退化），org 的并发沙箱上限在准入时按 PostgreSQL 中的存活环境计数；按 UTC 月的预算：沙箱的每个 Gateway 调用在预留任务预算的同一事务中于各层级放一个 hold，任一层级不足即拒绝，hold 随调用结算转换、超时由 sweep 按全额记为未知。`/v1/limits`、`/v1/budgets` 端点（owner 管理本 org 的用户与 key，不能超过 org 的上限）与 `agentbox limits` / `agentbox budget`；`verify-invariants` 核对 G1、G2。 |
| **用量、账单与对账** | 每分钟把已结算的预算 hold 汇总为按日、org、用户、key、模型与来源（模型 API / 运行时）的 `usage_daily`（按 watermark 增量、幂等，多 server 经 advisory lock 串行）；`GET /v1/usage`（按日、模型、key、用户分组）与 `GET /v1/bills/{月}`（按模型的 token 与费用，已知与未知分开），成员只看自己、owner 看本 org、运维看全部；`agentbox bill reconcile` 从原始 hold 与模型请求重算一个月，与 `usage_daily` 和 `quota_budgets` 逐项比较，有差异时退出码 1。网页新增 API 密钥、用量与成员三页。 |
| **可观测性** | OpenTelemetry 追踪：一次用户操作一条 trace，从 API 请求经任务 actor、attempt、Worker 启动与握手、checkpoint，**进入沙箱**（W3C `traceparent` 经协议交给 Worker，Python SDK 只用标准库转发）再回到 Gateway 的每个模型、搜索、抓取与 exec 调用及其每次重试；Prometheus 指标（结果、启动与停止延迟、上游延迟与错误、费用、工具调用、沙箱与槽位）；JSON 日志带 `trace_id`。默认关闭；`deploy/observability` 提供 Collector、Tempo、Prometheus、Loki、Grafana 本地栈（[设计](docs/design/2026-10-10-observability-design.md)、[演示记录](docs/evidence/2026-10-10-observability.md)）。 |

## 证据

所有数字都来自有记录的运行，证据文件写明每次运行证明了什么、没有证明什么。

- **测试：**
  - 约 1150 个 Go 测试函数，含以 root 在真实沙箱中运行的端到端测试；
  - 约 380 个 Python 测试（含官方 SDK 兼容性测试）；
  - 约 230 个 Web 组件测试。
- **CI（7 个作业）：**
  - 非 root 全量（`-race`）；
  - 在 `ubuntu-24.04` 上以 root 在真实沙箱中运行的全量；
  - Python 3.11 与 3.13；
  - Web 的 lint、类型检查与测试；
  - 官方 `openai` 与 `anthropic` SDK 对模型 API 的兼容性测试；
  - lint 与复杂度报告。
- **真实验收**：在 4 vCPU / 8 GiB 的 Linux 云主机（腾讯云上海）上，使用真实模型（Moonshot：`kimi-k3` 主导、`kimi-k2.6` 执行）与经 Serper 的 Google 搜索结果。记录：[对话助手](docs/evidence/2026-10-06-m4-chat-acceptance.md)、[exec 沙箱](docs/evidence/2026-10-06-m4-exec-hardening.md)、[账号](docs/evidence/2026-10-06-m3-accounts.md)、[备份与恢复](docs/evidence/2026-10-06-backup-restore.md)。
- **模型降级链**（[记录](docs/evidence/2026-10-10-model-fallback.md)，fake upstream、零费用）：主供应商持续 503 时，无降级链的调用 3.7–5.1 s 后失败（3 次 try 加退避），有降级链时约 51 ms 由后备供应商完成（与健康供应商相同）；全部熔断时约 40 µs 返回 `model_degraded`。
- **模型网关**（[记录](docs/evidence/2026-10-12-model-gateway.md)，2026-10-12；fake upstream、零费用；单机 WSL2、CPU 与其他任务共享、PostgreSQL 在 Docker Desktop 中（提交落盘偏慢）、只有一个 server，所以跨 server 的 Redis 全局限流没有测；关键场景各 3 次）：非流式网关开销 P50 约 16 ms、P99 约 25–27 ms（四条路径相同；流式 P99 24.5–35 ms，首字约 6.5 ms；约三分之二是每个请求约 4 次提交的落盘），这些数字没有设限额、限流器不在路径上——三级都设 rpm 与 tpm 时 P50 多约 1–2 ms（两轮各一次运行，8 组配对中 7 组如此）；16 个 org 约 1050 RPS（并发 32，数据库较空闲时；数据库变慢的一次运行中为 440 RPS，吞吐随提交延迟变化），限流器在路径上时没有可见差别；限流放行 / 配置 1.000–1.002；并发 64 下预算超支 0，且 fake upstream 自己的计数与网关的 try、hold、token 完全一致；运行中停止 Redis 0 个客户端错误；负载之后 `bill reconcile` 一致。同一 org 的预算行是热点：单个 org 约 135 RPS，数据库变慢时排队超过 2 s 的存储期限会返回 503（失败关闭）。四个故障场景（kill -9、首字节后失败、流中吊销、熔断）全部通过；经 Moonshot 的 6 次真实调用覆盖四条路径（含 Anthropic 兼容端点）。
- **Kubernetes provider**（[记录](docs/evidence/2026-10-10-k8s-provider.md)，kind 单节点，每组 20 次）：环境就绪 P50 冷启动 978 ms、预热池 11 ms；到 Python 首行输出 1051 ms / 78 ms；契约一致性测试在 fake clientset（CI）与 kind 上均 11/11 通过；`scripts/demo-k8s.sh` 演示 Gateway、取消与 server 崩溃恢复。
- **评测平台**（[记录](docs/evidence/2026-10-10-eval-platform.md)，fake upstream、零模型费用）：真实沙箱中 15 个任务的参考解 15/15 通过；fake 模型 9/15，6 个故意错误的回答全部失败（`wrong_exit_code`×5、`check_timeout`×1），其中两个在导入时 `exit 0` 的奖励作弊也失败——解答在独立进程中运行，检查器须打印随机完成令牌才算通过；并发 1 → 4 墙钟 18.5 s → 10.5 s。
- **轨迹诊断闭环**（[记录](docs/evidence/2026-10-10-trace-doctor.md)，真实沙箱 + fake upstream、零模型费用，每次运行 N = 64）：故意错误配置的 server（主供应商每第 4 个 chat 挂起而没有每 try 超时；调用期限 1 s 短于 1.5 s 的抓取）成功率 56.2%（Wilson 95% [44.1%, 67.7%]）、P95 90.2 s；`doctor-traces` 只读轨迹就把 28 个失败全部归因到两条规则，提出 `--model-try-timeout 2s` 与 `--call-deadline 10s`；以它写出的实验配置重跑同一 suite：100%（[94.3%, 100%]）、P95 15.1 s。故障按每第 4 个请求的确定性节奏注入，Wilson 区间只是保守的核对；更强的证据是脚本两次完整运行得到相同结果。
- **串行与并行研究对比**（[记录](docs/evidence/2026-10-06-m4-subrun-comparison.md)）。小样本，每组 N = 4：

  | | 串行 | 并行 |
  |---|---|---|
  | 墙钟时间中位数 | 302 s | 184 s |
  | 每次费用中位数 | 0.48 USD | 0.50 USD |
  | 失败 | 0 | 0 |
  | 可定位的引用 | 100% | 100% |

## 快速开始

依赖：Linux（cgroup v2）或 WSL2、Go 1.26+、Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)、Docker（运行 PostgreSQL）、Node.js 24（Web 界面）。

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

### 模型 API

`agentbox server` 加 `--model-api` 后，用 `models` scope 的平台 key 以 OpenAI 或 Anthropic 格式调用（[使用参考](docs/usage.zh-CN.md)中的"模型 API"）：

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"kimi-k3","messages":[{"role":"user","content":"你好"}],"max_tokens":200}'
```

```python
import os, anthropic, openai
KEY = os.environ["KEY"]
openai.OpenAI(base_url="http://127.0.0.1:8080/v1", api_key=KEY).chat.completions.create(
    model="kimi-k3", messages=[{"role": "user", "content": "你好"}], max_tokens=200)
anthropic.Anthropic(base_url="http://127.0.0.1:8080", api_key=KEY).messages.create(
    model="kimi-k3", max_tokens=200, messages=[{"role": "user", "content": "你好"}])
```

不需要模型 Key 与 root（fake upstream 作为 OpenAI 与 Anthropic 格式的供应商；需要 docker、Go、python3、curl，`sdk-compat.sh` 与演示另需 uv；未设置 `AGENTBOX_DATABASE_URL` 时脚本自己以 `deploy/docker-compose.yml` 启动 PostgreSQL）：

```bash
AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable' bash scripts/sdk-compat.sh  # 两个 SDK × 两种供应商格式
bash scripts/demo-gateway.sh       # org、邀请、key，两个 SDK 的四条路径，加权路由，429 与预算用尽，吊销，账单与对账，指标
bash scripts/fault-gateway.sh      # kill -9 中途崩溃、首字节后供应商失败、流中吊销 key、熔断与恢复（约 6 分钟）
bash scripts/loadtest-gateway.sh   # 开销、最大 RPS、限流精度、并发预算、行锁等待、停止 Redis（cmd/agentbox-loadtest）
```

### 用量、账单与网页

server 每分钟把已结算的预算汇总为按日用量；运维可在命令行查看账单并对账（[使用参考](docs/usage.zh-CN.md)中的"用量、账单与对账"）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox usage --org acme --group-by day,model --rollup
sudo AGENTBOX_DATABASE_URL=... agentbox bill show 2026-10 --org acme
sudo AGENTBOX_DATABASE_URL=... agentbox bill reconcile 2026-10   # 有差异时退出码 1
```

登录后顶部导航多了三页（文字描述，没有截图）：

- **API 密钥**：密钥表格（名称、权限范围、创建、最近使用、过期、状态）；新建时勾选"模型 API / 运行时"并选择有效期，完整密钥只在一个带复制按钮和黄色警示框的对话框中出现一次；吊销要再点一次确认。
- **用量**：顶部四个数字卡片（费用、其中未知、请求、token），下面并排两张紫色柱状图（每日费用、每日 token），再是按模型与按密钥的表格、组织/本人/每个密钥的预算进度条（紫色已花费、橙色未知、浅紫预留中，右侧显示剩余与上限），最后是本月账单。
- **成员**：收到的邀请（接受、拒绝）、我的组织与"离开组织"；owner 另有成员表（移出需确认）、邀请表单与待接受的邀请。

## 目录

| 路径 | 内容 |
|---|---|
| `cmd/agentbox` | 单一二进制：`server`、`doctor`、`task …`、`user …`、`org …`、`key …`、`limits …`、`budget …`、`usage`、`bill …`、`eval …`、`doctor-traces …`、`verify-invariants` |
| `internal/eval`、`eval/suites` | 评测平台：suite、runner、轨迹、评分、报告与对比（只是运维 API 的客户端）；演示 suite |
| `internal/doctor` | 轨迹诊断：根因规则、白名单内的提议与补丁、Tempo/Prometheus/Loki 查询、可选的模型总结（只用评测文件与公开 API） |
| `internal/sandbox`、`provider/local`、`cgroup`、`rootfs`、`hostcheck` | 沙箱启动器、init、环境生命周期、宿主自检 |
| `internal/provider/k8s`、`cmd/agentbox-podagent`、`deploy/k8s` | Kubernetes provider（每环境一个 Pod、镜像 digest 固定、预热池）、镜像内 helper、worker 镜像与 kind 配置 |
| `internal/task`、`runner`、`session`、`subrun`、`admission`、`resource` | 控制面：actor、attempt、会话、sub-run、准入 |
| `internal/gateway/{edge,call,upstream,cache}` | Gateway：每 attempt 的 socket、调用 journal 与账本、供应商 adapter、Redis 缓存 |
| `internal/gateway/{workspace,mcp}`、`internal/mcpdemo` | 工作区（清单、生命周期、文件操作）、MCP 客户端与 adapter、演示 MCP 服务器（`agentbox mcp-demo-server`） |
| `internal/persistence/postgres`、`blob`、`recovery`、`reconcile`、`invariants` | 存储、内容寻址的 blob、启动恢复、不变量检查 |
| `internal/api`、`account` | REST/SSE API（[OpenAPI](api/openapi.yaml)）、账号 |
| `internal/tenant`、`apikey`、`ratelimit`、`quota` | org 与成员、平台 key（HMAC、缓存与 NOTIFY 失效）、限流（Redis Lua 与进程内退化、并发计数）、月度预算 hold |
| `internal/llmproto`、`modelapi`、`usage` | OpenAI ⇄ Anthropic 的中性格式与编解码（只依赖标准库）、模型 API、用量汇总、账单与对账 |
| `cmd/agentbox-loadtest`、`tests/sdk`、`scripts/*-gateway.sh` | 模型网关的负载工具、官方 SDK 兼容性测试、演示、负载与故障注入脚本 |
| `internal/obs`、`telemetry`、`deploy/observability` | 可观测性：只依赖标准库的埋点接口、OpenTelemetry 与 Prometheus 实现、本地观测栈 |
| `protocol/` | 版本化的 Go ↔ Python Worker 协议与共享 fixtures |
| `worker/` | Python Worker SDK、`chatagent`、工具、`deep-research` skill、评测 Worker `evalworker` |
| `web/` | Vue 3 + TypeScript 的对话界面与运维工作台 |
| `tests/e2e` | 端到端测试（进程型 provider 与真实沙箱） |
| `docs/` | [设计](docs/design/)、[证据](docs/evidence/)、[使用参考](docs/usage.zh-CN.md)、[术语表](docs/glossary.md) |

## 能力边界

以下边界在设计中写明，不加掩饰：

- **容器级隔离，共享宿主内核。** 权限边界测试是回归测试，不是无逃逸证明；不应依赖它隔离恶意的多租户代码。
- **单执行主机。** 没有跨主机故障接管；Kubernetes provider 的 workspace 与 Gateway socket 经节点本地目录共享，目前也只支持单节点。
- **不保证 exactly-once。** 外部调用不保证 exactly-once；结果未知时按保守方式计费，预算可能略有超出。
- **Workspace 不回滚。** workspace 不随 checkpoint 回滚。
- **同一 org 的预算行是热点。** 每个模型请求在预留与结算时各锁一次 org 的月度预算行：测试机上单个 org 约 135 RPS（分散到 16 个 org 约 1050 RPS；都是数据库较空闲时，吞吐随提交延迟变化）；数据库变慢时排队超过存储期限的请求得到 503（失败关闭，不会超支）。分片计数与预分配的预算片是后续工作。
- **Redis 不可用时限流按 server 生效；并发流（`max_streams`）总是按 server 计数**（N 个 server 时实际上限为 N 倍）。预算在 PostgreSQL 中，不受影响；PostgreSQL 不可用时模型 API 返回 503。
- **模型网关的范围。** 每个用户只属于一个 org；主供应商（`--model-base-url`）总是 OpenAI 格式，Anthropic 格式的供应商只能作为后备；没有支付、SSO、多区域，也不支持用户自带供应商 Key。

## 致谢

参考其设计，代码自行实现：[runc](https://github.com/opencontainers/runc)（re-exec init、降权、`/proc` 掩蔽）、[Tencent Cloud CubeSandbox](https://github.com/tencentcloud/CubeSandbox)（Agent 沙箱能力面）、E2B（沙箱内 daemon 语义）。研究流程参考 gpt-researcher、dzhng/deep-research 与 Anthropic 关于多 Agent 研究系统的文章。

## License

[MIT](LICENSE)
