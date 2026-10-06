# go-agentbox

单执行主机上的 Agent Runtime：Go 负责执行与权限边界，Python 负责研究编排。

> **状态：v0.2 内部里程碑 M1、M2、M3 已通过（2026-10-06），并增加了面向用户的账号体系。** `agentbox server` 已能在单台 Linux 主机（含 WSL2）上以真实沙箱（命名空间、映射 UID、只读 rootfs 模板、seccomp、降权）运行 sim-worker 任务，见下文"快速开始"；Plan 2 已验收（真实沙箱：WSL2 6.6 x86_64 本地 + CI ubuntu-24.04 x86_64）；**M1 门槛 2026-10-05、M2 与 M3 门槛 2026-10-06 判定通过**（判定依据与"仅验证可行性"的边界见 [M1](docs/plans/2026-10-03-m1-index.md)、[M2](docs/plans/2026-10-05-m2-index.md)、[M3](docs/plans/2026-10-05-m3-index.md) 计划索引）；M2/M3 的真实模型演示在腾讯云上海 CVM（原生 Linux）上执行。下文"已实现"只列出已经通过验收的部分；"这是什么"描述的是设计目标，每项保证在对应验收通过前都不成立。

---

## 当前状态

| 部分 | 状态 | 说明 |
|---|---|---|
| Worker 协议 v1（task 模式）与 Python Worker SDK、sim-worker（Plan 3） | **已验收** | Go 侧 `internal/protocol` 与 Python 侧 `worker/agentbox_worker` 共用 `protocol/fixtures` |
| PostgreSQL 持久化、BlobStore、安装身份引导（Plan 4） | **已验收** | `internal/persistence`、`internal/blob`、`internal/datadir`、`internal/ownership`；事务语义只在真实 PostgreSQL 上测试 |
| `internal/runtime` 改名为 `internal/sandbox`（Plan 1A） | **已验收** | 纯改名 |
| 控制面：准入、资源 coordinator 与清理、AttemptRunner、task actor 与调度（Plan 5） | **已验收**（控制面层） | `internal/admission`、`resource`、`runner`、`task`；E1、E4、E7、E8、E10 以进程型 fake provider 与真实 sim_worker 验证 |
| 启动恢复、REST/SSE API、CLI、不变量检查、故障注入（Plan 6） | **已验收**（控制面层） | `internal/reconcile`、`recovery`、`api`、`cli`、`app`、`invariants`、`faultinject`；E5、E6、E13–E16 |
| Provider 契约、cgroup 扩展、宿主自检、init 服务循环、环境生命周期（Plan 2 Task 1–8） | **已验收**（Plan 2 联合验收，2026-10-05） | `internal/provider`、`provider/local`、`sandbox`、`cgroup`、`hostcheck`；`agentbox doctor` |
| 沙箱生产启动器、init 环境建立、stage-2 helper、启用 server（Plan 2 Task 9–13） | **已验收**（Plan 2 联合验收，2026-10-05） | `local.NewProcessStarter`；首个切片、E1–E5（E5 为物理回收部分）、E7、E8、E10 与累计运行时限跨重启，在真实沙箱中以 root 通过（`tests/e2e` 的 `TestReal*`） |
| Gateway：每 attempt 的 Unix socket 入口、调用 journal 与 task 层预算、OpenAI 兼容 chat / 搜索 / 抓取 adapter、SSRF 验证 dialer、启动账本转换（M2 Plan 7） | **已验收**（2026-10-05） | `internal/gateway/{edge,call,upstream}`；E11b、E17–E20、E48、I3、I14（journal）；供应商 Key 只在宿主进程内，沙箱中不可见（G3，真实沙箱验证） |
| Worker SDK Gateway 客户端与 DeepResearch 接入（M2 Plan 8 Task 1–5） | **已验收**（以 fake upstream 自动化验收） | `worker/agentbox_worker/gateway.py`、`worker/deepresearch`（零运行时依赖）；计划 → 检索 → 阅读 → 总结 → 报告，checkpoint 可恢复，引用对应已保存证据 blob |
| 真实模型研究演示（M2 Plan 8 Task 6） | **已验收**（2026-10-06，[演示记录](docs/evidence/2026-10-06-m2-demo-run.md)） | `scripts/demo-m2.sh`（见"真实研究演示"）；真实运行默认使用 Moonshot（编排 `kimi-k3`、worker `kimi-k2.6`），需要 `AGENTBOX_MODEL_API_KEY` |
| Redis 共享缓存与调用合并（M3 Plan 9） | **已验收**（2026-10-06） | `internal/gateway/cache`；`--redis-addr`、`--cache` |
| 下载端点与 Vue 工作台（M3 Plan 10） | **已验收**（2026-10-06，[服务器验收](docs/evidence/2026-10-06-m3-server-acceptance.md)） | `GET /tasks/{id}/result`、产物下载；`web/`（Vite + Vue 3），由 `agentbox server --web-dir` 同源提供，见"工作台" |
| 用户账号与 DeepResearch 助手（M3 Plan 11） | **已验收**（2026-10-06，[账号验收](docs/evidence/2026-10-06-m3-accounts.md)） | 开放注册、服务端会话、用户只见自己的研究、内部细节仅运维可见；`internal/account`、`/auth/*`、`POST /research`、`agentbox user` |
| 搜索供应商 serper（Google 结果）与按端点的调用期限 | **已验收**（2026-10-06） | `--search-provider serper`；`--call-deadline`（120 s）、`--model-call-deadline`（300 s） |
| 会话后端：每会话一个长期 incarnation（冻结、驱逐、冷恢复、关闭）、turn 与工具额度（M4 Plan 12） | **已实现**（以脚本化会话 Worker 验收，待 Plan 13 联调） | `internal/session`、会话 API 与整会话 SSE、`--turn-tool-budget`、`--session-idle-freeze`、`--session-evict-after`、`--session-worker-argv`；E28–E33 在真实沙箱中以 root 运行，见"会话"；面向用户的会话 Agent 与聊天界面是 Plan 13 |
| 独立 exec 沙箱与沙箱加固（M4 Plan 15） | **已实现**（WSL2 真实沙箱 root 验收；服务器验收与 CI 待协调者联合验收） | `POST /v1/exec`、`--exec-*` 标志、Worker 工具 `run_python`；E35–E38 在真实沙箱中以 root 通过，E39 由 provider 测试覆盖，见"代码执行沙箱"与[验收记录](docs/evidence/2026-10-06-m4-exec-hardening.md) |
| 对话式研究助手：工具调用式 Agent `chatagent`、deep-research skill、停止/继续/立即写报告/恢复/提问、对话界面（M4 Plan 13） | **已实现**（2026-10-06 在演示服务器上以真实 Kimi + Serper 验收，[验收记录](docs/evidence/2026-10-06-m4-chat-acceptance.md)；待协调者联合验收与浏览器目视检查） | `worker/chatagent`、`worker/agentbox_worker/tools`、`worker/skills/deep-research`、`web/src/views/ChatView.vue`；`scripts/dev/install-worker.sh`；见"对话式助手" |

## 现在可以运行的命令

Go、Docker 与 PostgreSQL 命令已在 WSL2（内核 6.6）上验证；Python 测试已在 Windows（uv）与 CI 的 Linux 作业上验证。没有列出的命令尚不能使用。

**依赖**：Linux（cgroup v2）或 Windows 下的 WSL2；Go 1.24+；Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)；Docker（运行 PostgreSQL）。

```bash
stat -fc %T /sys/fs/cgroup            # 应输出 cgroup2fs
```

**Go 单元测试**（不需要数据库；需要 root 的用例在非 root 下跳过）：

```bash
go vet ./...
go test ./...
```

**PostgreSQL 与数据库测试**（事务语义测试只在真实 PostgreSQL 上运行）：

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
export AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable'
go test ./internal/persistence/postgres/
docker compose -f deploy/docker-compose.yml down
```

`CI=true` 时未设置 `AGENTBOX_TEST_DATABASE_URL` 会使数据库测试失败而不是跳过（CI 即如此运行）。

**控制面端到端测试**（需要上面的 PostgreSQL 与 `python3`；以 `app.Run` 装配全部组件，用进程型测试 provider 运行真实的 `sim_worker`，覆盖崩溃重启恢复、取消竞争、数据库断连等；约 2 分钟）：

```bash
CI=true go test -count=1 ./tests/e2e/...
```

以 root 运行时（WSL 中用 `wsl -u root`，同样设置上面的 `AGENTBOX_TEST_DATABASE_URL`），`TestReal*` 用例另在真实沙箱中重跑首个切片、E1–E5、E7、E8、E10 与累计运行时限（provider/local + 生产启动器；测试会把 `worker/` 复制到 `/opt/agentbox`；非 root 时这些用例跳过）。须以 `CGO_ENABLED=0` 构建（cgo 构建的 stage-2 helper 拒绝启动 workload），约 2.5 分钟：

```bash
CI=true CGO_ENABLED=0 go test -count=1 ./tests/e2e/...      # 以 root 运行
```

**需要 root 的测试**（cgroup、环境生命周期等）：以 root 运行 `go test ./internal/cgroup/ ./internal/rootfs/ ./internal/sandbox/ ./internal/provider/local/`（WSL 中可用 `wsl -u root`）。

**宿主自检**（需要 root）：

```bash
CGO_ENABLED=0 go build -o bin/agentbox ./cmd/agentbox
sudo ./bin/agentbox doctor             # 通过时输出"宿主环境检查通过"
```

## 快速开始：在真实沙箱中运行一个任务

**一键演示**：`sudo bash scripts/demo-m1.sh` 在全新的数据目录与数据库上执行整条链路并逐步打印结果——提交 → checkpoint → 杀死 Worker → 从 checkpoint 恢复 → result → 清理，再 SIGKILL server → 重启恢复 → `verify-invariants --quiescent` → 泄漏检查；任一步失败即非零退出。一次实际运行的完整输出见 [docs/evidence/2026-10-05-m1-demo-run.md](docs/evidence/2026-10-05-m1-demo-run.md)。下面是同一链路的手动步骤。

需要 root（cgroup、命名空间与 UID 映射）、cgroup v2、`python3`（3.11+，沙箱内用宿主的 `/usr`）与上文的 PostgreSQL。配置示例 [`deploy/agentbox.env.example`](deploy/agentbox.env.example) 列出 `agentbox server` 的全部标志与环境变量，下面的命令按原样载入它（数据目录 `/var/lib/agentbox`，监听 `127.0.0.1:8080`）。数据目录与数据库一一绑定（安装身份）：换数据库须换数据目录。

```bash
# 1. PostgreSQL：按上文"PostgreSQL 与数据库测试"的 docker compose 命令启动（连接串见配置示例）

# 2. 构建并自检（生产二进制固定 CGO_ENABLED=0）
CGO_ENABLED=0 go build -o bin/agentbox ./cmd/agentbox
sudo ./bin/agentbox doctor

# 3. 安装 worker 包到默认 rootfs 模板中的 /opt/agentbox（server 启动时检查它存在）
sudo install -d -m 0755 /opt/agentbox
sudo cp -r worker/agentbox_worker worker/sim_worker worker/deepresearch /opt/agentbox/

# 4. 终端 1：启动 server（前台运行，Ctrl-C 停止）
sudo sh -c 'set -a; . deploy/agentbox.env.example; exec ./bin/agentbox server $AGENTBOX_SERVER_FLAGS'
```

```bash
# 5. 终端 2：提交任务、观察事件、查看 attempt 与 checkpoint
set -a; . deploy/agentbox.env.example; set +a
./bin/agentbox status
./bin/agentbox task submit --spec '{"steps":[{"op":"progress","message":"hello from the sandbox"},{"op":"artifact","artifact_id":"report","path":"report.md","content":"# quick start"},{"op":"checkpoint","step_id":"written"}],"summary":"quick start done","outputs":["report"]}'
./bin/agentbox task watch <task_id>      # 事件流：ready → progress → artifact_saved → checkpoint_committed → result → task_terminal
./bin/agentbox task inspect <task_id>    # status succeeded；attempt 的退出码、OOM 诊断、环境清理状态；checkpoint

# 6. 不变量检查（运行中可查 A/B 类；终端 1 Ctrl-C 停止 server 后加 --quiescent）
sudo sh -c 'set -a; . deploy/agentbox.env.example; exec ./bin/agentbox verify-invariants --data-dir /var/lib/agentbox'
sudo sh -c 'set -a; . deploy/agentbox.env.example; exec ./bin/agentbox verify-invariants --quiescent --data-dir /var/lib/agentbox'
```

Worker 在沙箱中以映射 UID 运行，只读看到宿主的 `/usr`、`/etc` 的子集与 `/opt/agentbox`，可写的只有 `/workspace`（宿主 `<data>/workspaces/<task_id>`）与限额 tmpfs。产物按固定版本保存在 `<data>/blobs`（版本与 sha256 见 `task watch` 的 `artifact_saved` 事件），经 API 下载：`GET /tasks/{id}/result` 返回终态任务的结果（固定输出 `(artifact_id, version, sha256)`），`GET /tasks/{id}/artifacts/{artifact_id}/versions/{v}`（或 `?version=N`，省略时为最新版本）返回产物内容，`ETag` 为带引号的 sha256；只有 `visibility = output` 的产物可下载，HTML、SVG 等主动内容一律以 attachment 下载。`agentbox task result <task_id>` 下载结果，`--artifact <artifact_id> [--version N]` 下载产物，两者都按 `ETag` 校验 sha256，不符则报错且不输出。

**Gateway 配置**：每个 attempt 在 `<data>/gateway/<attempt_id>.sock` 有一个 Gateway 入口，挂载到沙箱内的 `/run/agentbox/gateway.sock`（属主为环境映射 uid 1000、0600）；attempt 结束或取消生效时先在数据库撤销访问、再关闭入口。供应商 Key 只从宿主环境变量 `AGENTBOX_MODEL_API_KEY`、`AGENTBOX_SEARCH_API_KEY` 读取，不进入 Worker 的 init、沙箱环境变量与日志；`--worker-env` 的键须在白名单中（如 `PYTHONPATH`）。相关标志（含义与默认值见配置示例）：`--default-budget-micro` / `--budget-cap-micro`（task 层预算，微美元；任务可用 `limits.budget_micro` 指定，超过上限返回 `400 invalid_limits`）、`--model-base-url` / `--model-name` / `--models` / `--model-price-in-micro-per-mtok` / `--model-price-out-micro-per-mtok` / `--model-price`（OpenAI 兼容模型上游；不设 `--model-base-url` 时不提供模型端点；`--model-name` 是请求未指定 `model` 时的默认模型，`--models` 是声明的白名单（须包含默认模型），请求可按调用在其中选择 `model`，白名单外为 `400 unsupported_model`；`--model-price model=IN:OUT` 给出按模型的单价，用于预留估算与结算，只是配置、不代表供应商实际计费）、`--search-provider ddg_lite|tavily|serper|fake`（`serper` 为经 Serper.dev 的 Google 结果：`POST https://google.serper.dev/search`，Key 只放在 `X-API-KEY` 头；`tavily` 与 `serper` 都读同一个 `AGENTBOX_SEARCH_API_KEY`，未设置时 server 以退出码 2 拒绝启动。serper 的请求与响应映射只经本机 fake Serper 服务器的单元测试验证，未用真实 Key 执行）、`--upstream-allow-private`（显式放行的私有上游，例如本机模型服务）。`agentbox task inspect` 除 attempt 与 checkpoint 外列出每个 Gateway 调用及其 try（端点、chat 调用解析后的模型、状态、费用、延迟、上游请求 ID；不含请求与响应正文）。

### 真实研究演示（M2）

`scripts/demo-m2.sh` 在全新的数据目录（`/var/lib/agentbox-demo-m2`）与数据库（`agentbox_demo_m2`）上，以真实沙箱与真实 Gateway 运行一个 DeepResearch 任务（`--worker-argv python3,-m,deepresearch`），逐步打印：提交 → Worker 运行中从宿主读取沙箱内每个进程的 `/proc/<pid>/environ` 与命令行，确认不含 Key 的值与变量名（G3）→ 等待 `task_terminal` → `inspect` 的调用明细（端点、模型、tries、费用、延迟；不含正文）与总费用 → 报告前 40 行与证据列表（报告从 `<data>/blobs` 读取并校验 sha256）→ 环境清理 → `verify-invariants --quiescent` → 泄漏检查 → Key 不出现在 server 日志、事件流与 inspect 输出中。任一步失败即非零退出。

**前提**：与上文快速开始相同（root、cgroup v2、`python3` 3.11+、PostgreSQL；数据库经本机 `psql` 或 docker 新建）；脚本自行构建 `bin/agentbox` 并把 `worker/` 中的 `agentbox_worker`、`sim_worker`、`deepresearch` 安装到 `/opt/agentbox`。

**主/从模型**：DeepResearch 的主 agent（编排：计划与最终报告）与任务内的 worker（各任务的总结）可以用不同的模型。任务 config 的 `orchestrator_model` 与 `worker_model` 决定每次 chat 调用请求体中的 `model`（未配置时不带 `model`，由 Gateway 用 `--model-name` 的默认模型）；两者都须在 server 的 `--models` 白名单中。模型名进入调用指纹，恢复后同一步骤重发同一模型。演示脚本默认编排 `kimi-k3`、worker `kimi-k2.6`，以 `--model-name <worker 模型> --models <白名单>` 启动 server，`inspect` 明细显示每个调用的模型（取自结果 blob 中上游回复的 `model` 字段）并检查路由：plan/report 为编排模型，任务内 chat 为 worker 模型。费用按配置的单价估算与结算，不代表供应商的实际计费。

**变量**（变量名也列在 [`deploy/agentbox.env.example`](deploy/agentbox.env.example)）：`AGENTBOX_DEMO_MODEL_BASE_URL`（默认 `https://api.moonshot.cn/v1`，OpenAI 兼容端点）、`AGENTBOX_DEMO_ORCHESTRATOR_MODEL`（默认 `kimi-k3`）、`AGENTBOX_DEMO_WORKER_MODEL`（默认 `kimi-k2.6`）、`AGENTBOX_DEMO_MODELS`（白名单，默认 `kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3`）、`AGENTBOX_DEMO_MODEL_PRICES`（可选，`model=IN:OUT`，逗号分隔）、`AGENTBOX_DEMO_SEARCH_PROVIDER`（`ddg_lite` 默认、无 Key；`tavily` 需要 `AGENTBOX_SEARCH_API_KEY`；`serper` 需要 `AGENTBOX_SERPER_API_KEY`，脚本把它作为 `AGENTBOX_SEARCH_API_KEY` 交给 server，因此两种 Key 可并存于 `.env`，只读取所选供应商的那一行，并纳入 G3 与日志的 Key 泄漏检查；`serper` 的真实运行未在此执行）、`AGENTBOX_DEMO_TOPIC`（默认一个固定中文题目）、`AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK` / `AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK`、`AGENTBOX_DEMO_BUDGET_MICRO`（可选）。模型 Key 取环境变量 `AGENTBOX_MODEL_API_KEY`，未设置时从仓库根的 `.env`（git 忽略；可用 `AGENTBOX_DEMO_ENV_FILE` 指定）中**只读取这一行**，不 `source` 整个文件；脚本只报告"已设置"，不打印、不写入任何文件，只经环境变量交给 server。

**演练**（fake upstream 扮演模型、搜索与网页，不访问外网，原样回显请求的 `model`；Key 为随机生成的假值；其余流程与真实模式相同，含主/从模型路由检查。已在 WSL2 6.6 x86_64 上以 root 执行，16 步全部通过）：

```bash
sudo AGENTBOX_DEMO_FAKE=1 bash scripts/demo-m2.sh
```

**真实模型**（待执行：默认 Moonshot，需要 `AGENTBOX_MODEL_API_KEY`；运行后证据记录在 `docs/evidence/`）：

```bash
sudo bash scripts/demo-m2.sh
```

**Python Worker SDK 测试**：

```bash
cd worker
uv run pytest -q
```

### 工作台（M3，Vue）

`web/` 是浏览器工作台（Vite + Vue 3 + TypeScript）：任务列表与提交、事件时间线（SSE，断线按 `Last-Event-ID` 续传）、暂停/恢复/取消、产物与版本（预览或下载）、Gateway 调用明细（端点、模型、状态、费用、延迟）与 inspect 原始数据。它只调用上文的 REST API，由 `agentbox server` 以同一 Origin 提供，不需要单独的 Web 服务器。

**构建**（需要 Node.js 24 与 npm；已在 Windows 上以 Node 24.11 执行）。产物在 `web/dist`，不提交到仓库：

```bash
cd web
npm ci
npm run build          # vue-tsc 类型检查 + vite build → web/dist
```

开发检查（与 CI 的 `web` 作业相同）：`npm run lint`、`npm run typecheck`、`npm test`（Vitest），以及 `npm run gen:api`——从 `api/openapi.yaml` 重新生成 `web/src/api/schema.d.ts`，CI 要求生成结果与提交的文件一致。

**启动**（已在演示服务器上执行）：在快速开始第 4 步的 server 命令后加 `--web-dir <仓库>/web/dist`，然后在浏览器中打开 `http://127.0.0.1:8080/`。API 路径（`/status`、`/tasks/...`）仍由 API 处理；其余路径从该目录提供静态文件，无扩展名的未知路径回退到 `index.html`（工作台用 `#/tasks/...` 哈希路由）。演示脚本可设置 `AGENTBOX_DEMO_WEB_DIR=<仓库>/web/dist` 让演示期间的 server 同时提供工作台。

```bash
sudo sh -c 'set -a; . deploy/agentbox.env.example; exec ./bin/agentbox server $AGENTBOX_SERVER_FLAGS --web-dir "$PWD/web/dist"'
```

**token**：打开工作台后在入口页输入 API token（`<data>/api.token` 的内容）。token 默认只保存在页面内存中（刷新即需重新输入），可选择保存到本标签页的 `sessionStorage`；从不写入 `localStorage`、URL 或构建产物，只放在请求的 `Authorization: Bearer` 头中。server 未配置 `api.token` 时（只允许 loopback 监听）可选择"无 token 继续"。

**远程访问**（已在演示服务器上执行）：server 保持监听 `127.0.0.1:8080`，从本机经 SSH 隧道访问，然后在本机浏览器打开 `http://127.0.0.1:8080/`：

```bash
ssh -L 8080:127.0.0.1:8080 ubuntu@<server>
```

**安全说明**：

- 同源：不设置通配 CORS；带 `Origin` 的 API 请求须在允许列表中（默认等于监听地址与 `--allowed-host` 的各主机，可用 `--allowed-origin` 显式指定），`Host` 头须在 `--allowed-host` 中。
- 所有响应带 `Content-Security-Policy`、`X-Content-Type-Options: nosniff` 与 `Referrer-Policy: no-referrer`；访问日志不记录请求头与查询串。
- 报告 Markdown 经 `marked` 渲染后由 DOMPurify 清洗（去除脚本、事件属性、`javascript:` 链接、`iframe`/`object`/`style` 等）再插入页面；`text/html`、`image/svg+xml` 等主动内容只下载、不内联。
- 优先用 SSH 隧道。若直接监听非 loopback 地址，须有 `<data>/api.token`（0600）与 `--allowed-host`，并经 TLS 访问（`--tls-cert`/`--tls-key` 或外部 TLS 终止）；未启用 TLS 时 server 在启动时警告 token 会以明文传输。

### DeepResearch 助手（用户侧，M3 Plan 11）

配置了模型上游（`--model-base-url`）时，server 启用用户账号，浏览器打开站点根路径即是面向用户的研究助手：

1. **注册 / 登录**：用户名 3–32 位字母、数字、`_`、`.`、`-`；密码 8–128 个字符。会话保存在 `HttpOnly; Secure; SameSite=Strict` cookie 中，有效期 7 天。
2. **新研究**：只需输入研究主题。模型、搜索与预算由 server 固定（编排 `--user-orchestrator-model`，默认 `kimi-k3`；worker `--user-worker-model`，默认 `kimi-k2.6`）。每个用户同一时间最多 1 个进行中的研究。
3. **我的研究**：只列出自己的研究；进度按"计划 → 子任务 → 报告"显示；完成后在页面中阅读报告（安全渲染）并下载。

用户看不到 API Key、token、预算、模型、费用或调用明细（事件流与任务视图在服务端按允许列表脱敏）；他人的研究一律返回"不存在"。注册与登录按 IP 限速，登录失败不区分"用户不存在"与"密码错误"；密码以 PBKDF2-SHA256（600,000 次迭代，标准库 `crypto/pbkdf2`）存储。

**运维**：工作台移到 `#/admin`，仍使用 `<data>/api.token`（启用账号时运维调用必须带有效 Bearer token）。账号管理直接连数据库（已在演示服务器上执行）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox user list
sudo AGENTBOX_DATABASE_URL=... agentbox user disable <username>   # 同时吊销其全部会话
sudo AGENTBOX_DATABASE_URL=... agentbox user enable <username>
```

演示服务器的常驻服务见 `deploy/systemd/agentbox-demo.service`（HTTPS 443、自签证书、serper、Redis、4C8G 资源与预算上限）。

### 会话（M4 Plan 12，后端）

会话是用户与研究助手的一段对话：每条消息是一个 turn（一个任务），同一会话的 turn 在同一个长期运行的沙箱进程（incarnation）中依次执行，并共享会话状态（最新的 session checkpoint）与 `/workspace`。

**用户**（登录后，API 见 [OpenAPI](api/openapi.yaml) 的 `/sessions`、`/turns`）：

- **会话列表**：`GET /sessions` 只列出自己的会话；他人的会话与 turn 一律 404。新建 `POST /sessions`，首条消息的前 40 个字符作为标题，可重命名或删除（`DELETE /sessions/{id}`：取消未完成的 turn、停止环境后删除 workspace）。
- **发消息**：`POST /sessions/{id}/messages`。已有进行中的 turn 时为 `409 turn_in_progress`；有已停止（paused）的 turn 时，它被取消但内容全部保留（`status_reason = superseded`），其最新 checkpoint 作为新一轮的上下文（carryover）。
- **停止 / 继续 / 恢复**：`POST /turns/{id}/stop` 暂停运行中的 turn；`/continue` 从停止处继续，`/finish` 让它立即收尾；被取代的 turn 可 `POST /turns/{id}/restore`——在同一会话开新一轮，从它的最后一个 checkpoint 继续，并获得新的工具额度。Agent 提问时 turn 处于 `awaiting_input`（释放执行槽位），`POST /turns/{id}/answer` 回答后同一 turn 从原位置继续。
- **工具额度**：每个 turn 的 `web_search` 与 `web_fetch` 合计至多 `--turn-tool-budget` 次（默认 30；缓存命中与合并也计数，同一调用的重放不计数，server 重启后计数延续），第 31 次得到 `429 tool_budget_exhausted`；`Turn.tool_calls_used/tool_call_limit` 与响应头 `X-Agentbox-Tool-Budget` 给出用量。
- **事件**：`GET /sessions/{id}/events` 是整个会话的 SSE（`id` = 会话内序号，可用 `Last-Event-ID` 续传）；用户视图去掉费用、模型与内部 ID。

**生命周期**：会话空闲 `--session-idle-freeze`（默认 10 min）后冻结（cgroup freezer；冻结前要求 Worker 报告的会话 checkpoint 等于最新已提交者），空闲 `--session-evict-after`（默认 1 h，须大于前者）或内存不足时（按最久未用）驱逐；新消息唤醒：冻结的解冻，驱逐的以最新会话 checkpoint 冷恢复到新的 incarnation（同一 workspace 与 UID 范围）。server 重启时全部会话转为驱逐（保留最新 checkpoint），下一条消息冷恢复。

**运维**：

- `--session-worker-argv`：会话 incarnation 内的 Worker 命令（逗号分隔）。**为空时不启用会话**，会话端点返回 `503 sessions_unavailable`；启用会话需要用户账号（`--model-base-url`）。面向用户的会话 Agent 为 `python3,-m,chatagent`（见下节"对话式助手"）；会话后端的端到端测试使用脚本化的 Go 会话 Worker `tests/e2e/sessionworker`。
- `--turn-tool-budget`（1–1000）、`--session-idle-freeze`（> 0）、`--session-evict-after`（> idle-freeze）：违反时 server 拒绝启动（退出码 2）。
- 运维工作台（`#/admin`，Bearer token）可见全部用户的会话与内部状态；会话的写操作只允许所有者（运维为 403）。
- 不变量检查（`agentbox verify-invariants`）另核对 I9（每个会话至多一个存活 incarnation；冻结的会话最近一次 quiesce 报告的 checkpoint 等于会话指针）与 I10（会话 checkpoint 只由成功的 turn 提交，序号与指针单调）。

会话的端到端测试（PostgreSQL 与上文相同；非 root 用 `agentbox-e2e` 的进程型 provider，root 时另在真实沙箱中运行 E28–E33，测试会把 sessionworker 复制到 `/opt/agentbox`）：

```bash
CI=true go test -count=1 -run 'Session|ToolBudget' ./tests/e2e/
CI=true CGO_ENABLED=0 go test -count=1 -p 1 -timeout 60m -run 'E2[89]|E3[0-3]' ./tests/e2e/   # 以 root 运行
```

### 代码执行沙箱（M4 Plan 15）

编排 Worker 经 Gateway 的 `POST /v1/exec` 请求运行一段 Python（SDK：`ctx.gateway.exec(step_id, code, inputs=[(sha256, path)], wall_ms=...)`；工具 `run_python` 已在 `agentbox_worker.tools` 中提供，对话 Agent 的默认工具表尚未注册它）。server 为**每次**调用新建一个 exec 环境，运行 `python3 -I -B /in/.agentbox/main.py`（cwd `/out`），收集 `/out` 后销毁环境。

**保证与边界**：

- **单次、无状态**：调用之间不保留任何状态；需要上一次的文件时，把它的输出 sha 作为下一次的 `inputs` 显式传入（输入 blob 须已授权到本任务，放在只读 `/in/<path>`）。
- **无网络、无 Gateway**：exec 环境的网络命名空间只有 `lo`；不挂载 Gateway socket、不挂载 `/workspace`、不含 `/opt/agentbox`，`/sys` 与 cgroupfs 不挂载；独立的 user namespace 与 UID 范围，seccomp 与编排环境相同。
- **结果**：`completed`（主进程退出）、`timed_out`（超过 wall，整个执行树被杀死）、`cancelled`（attempt 结束、被替换或任务取消时 exec 总是被终止）、`unknown`（无法确认停止或结果，例如 server 崩溃）。主进程退出后仍在运行的后台进程会被一并停止，`/out` 在执行树清空后才收集；stdout/stderr 各保留前 1 MiB（`*_truncated`）；`/out` 至多收集 256 个普通文件（符号链接、FIFO 与超额文件在 `skipped_outputs` 中报告）。
- **不保证 exactly-once**：`unknown` 的调用在确认此前的 exec 环境已停止后可以重跑，`cancelled` 的调用由新 attempt 以 `X-Agentbox-Retry: true` 重跑（每调用累计 3 次 try）；**重跑可能产生不同结果并消耗额外配额**。用户取消的任务不会重跑。
- **配额与默认值**（server 标志，见 `deploy/agentbox.env.example`）：全局 `--exec-slots 4`（0 关闭 exec）、每任务 `--exec-per-task 2`；每任务 50 次、600 CPU 秒、累计 wall 30 min；单次 wall 默认 60 s（上限 300 s）、内存默认 512 MiB（上限 1 GiB）、`pids.max` 128、1 核、`/tmp` 与 `/out` 各 64 MiB（`/out` 1024 inode；单文件上限 64 MiB）；排队至多 60 s。CPU 按 `1 核 × wall × 1.1` 预留，停止后按实测结算，超额后该任务的后续 exec 被阻止。
- **内存规划**：exec 环境的内存**不计入** `--memory-bytes`（任务环境内存池）；exec 内存总量 = `--exec-slots × --exec-memory-max`（默认 4 × 1 GiB），须与 `--memory-bytes` 一并留出。
- **隔离强度**：容器级，共享宿主内核。权限边界测试（seccomp 拒绝探针、能力集 {KILL} 与无 ptrace 的回归锁）是回归测试，**不是无逃逸证明**；不应在不可信的多租户场景中依赖它隔离恶意代码。
- server 启动时检查 exec 模板（宿主 `/usr/bin/python3` 等系统路径）并计算其摘要（进入调用指纹）；缺失时拒绝启动，可用 `--exec-slots 0` 关闭 exec。

exec 的真实沙箱端到端测试（PostgreSQL 与上文相同；以 root、`CGO_ENABLED=0` 运行，约 1 分钟）：

```bash
CI=true CGO_ENABLED=0 go test -count=1 -p 1 -run 'TestRealExec|TestRealE3[5-8]' ./tests/e2e/   # 以 root 运行
```

### 对话式助手（M4 Plan 13）

启用会话后（`--session-worker-argv python3,-m,chatagent`），登录后打开站点根路径即是对话式研究助手：左栏是会话列表，中间是对话，右侧面板有"进度 / 来源 / 报告"三个标签（窄屏折叠为单列）。

- **新对话**：左栏"＋ 新对话"；首条消息的前 40 个字符作为标题。日常问题由模型直接回答（标注"直接回答"），也可从本会话的记忆（此前的回答与报告）作答。
- **深度研究**：输入框中的"深度研究"开关强制研究（否则由模型决定）。Agent 先读取 deep-research skill，范围不明确时用**提问卡**问一轮（至多 3 个选择题，可选"其他"自填），然后写待办清单、把子主题并行交给各自的执行者检索与阅读，最后写带编号引用 `[n]` 的报告，报告末尾附系统生成的证据列表（引用只指向本轮读过的网页）。每轮 `web_search` + `web_fetch` 合计至多 30 次（第 31 次被 Gateway 拒绝）；额度用尽时用已有资料写报告并标注"已达工具额度"。
- **步骤行**：读取 skill、待办清单、搜索网页、阅读网页等逐行显示、可展开；⟨/⟩ 显示发给模型或工具的请求与服务端脱敏后的响应，不含 Key、费用、模型名与内部调用 ID。
- **停止 / 继续 / 立即写报告**：研究进行中点 ■ 停止，turn 暂停并显示停止卡（进度卡与 2–3 句"目前发现"）；"继续"从停止处接着研究，"立即写报告"（至少一个子主题完成时可用）用已有资料写出标注"部分"的报告。研究进行中不能发新消息（先停止当前研究）。
- **恢复**：停止后直接发新消息，原研究被取消但内容保留（卡片显示"已停止"与"恢复"），其来源与已完成的子主题摘要作为新一轮的上下文；之后点"恢复"会在同一会话开新一轮，从原研究的最后 checkpoint 继续，并获得新的 30 次额度。

**安装与运维**：Worker 包（含 `chatagent` 与 `skills/`）安装到沙箱默认模板中的 `/opt/agentbox`，界面由 `--web-dir` 同源提供；演示服务器的常驻服务 `deploy/systemd/agentbox-demo.service` 即以下列配置运行（另加 TLS、Redis 与资源、预算参数）：

```bash
sudo bash scripts/dev/install-worker.sh /opt/agentbox   # agentbox_worker、deepresearch、chatagent、skills（与 sim_worker）
(cd web && npm ci && npm run build)                     # → web/dist
# AGENTBOX_DATABASE_URL、AGENTBOX_MODEL_API_KEY、AGENTBOX_SEARCH_API_KEY（serper 的 Key）只从环境变量读取
sudo -E ./bin/agentbox server --data-dir /var/lib/agentbox --web-dir web/dist \
  --model-base-url https://api.moonshot.cn/v1 --model-name kimi-k2.6 --models kimi-k2.6,kimi-k3 \
  --search-provider serper --session-worker-argv python3,-m,chatagent
```

编排（路由、计划、报告）用 `--user-orchestrator-model`（默认 `kimi-k3`），子主题与停止摘要用 `--user-worker-model`（默认 `kimi-k2.6`）；两者是推理模型，`--model-max-tokens-cap`（默认 32768）不得低于 16384。用户点"停止"时宿主给 Worker 60 s 写停止卡，超时则终止 incarnation（会话被驱逐，turn 仍从最后 checkpoint 暂停，可继续）。独立 exec（`--exec-slots`，默认 4，演示服务器为 2）已启用，但对话 Agent 尚未提供 `run_python` 工具。

## 文档

| 文档 | 内容 |
|---|---|
| [路线图](docs/design/2026-10-03-roadmap.md) | v0.2 首发（M1–M4）→ v0.3 多节点 → v0.4 MicroVM；发布门槛 |
| [v0.2 首发设计规格](docs/design/2026-10-03-v0.2-first-release-design.md) | 架构、协议、数据模型、Gateway、会话、隔离、验证与发布门槛 |
| [代码组织设计](docs/design/2026-10-03-code-organization.md) | 模块职责、允许依赖、事务用例、所有权、测试布局 |
| [M1 计划索引](docs/plans/2026-10-03-m1-index.md) | M1 各计划、依赖与验收归属 |
| [持久化设计](docs/design/2026-10-04-m1-4-persistence-design.md)、[安装身份修订](docs/design/2026-10-05-installation-identity-amendment.md) | Plan 4 的设计依据 |
| [Provider 契约](docs/design/2026-10-05-provider-contract.md) | 环境生命周期的 Go 接口、错误、并发边界；会话环境的冻结、解冻、进程表与恢复暂存（第 10 节） |
| [Plan 15 exec 与加固](docs/plans/2026-10-06-m4-15-exec-hardening.md)、[exec 加固验收记录](docs/evidence/2026-10-06-m4-exec-hardening.md) | 独立 exec 沙箱的决定 D1–D16、E34–E39 的证据与边界 |
| [M4 计划索引](docs/plans/2026-10-06-m4-index.md)、[Plan 12 会话后端](docs/plans/2026-10-06-m4-12-sessions.md)、[会话后端验收记录](docs/evidence/2026-10-06-m4-sessions.md)、[Plan 13 Agent 与对话界面](docs/plans/2026-10-06-m4-13-agent-chat.md)、[对话式助手真实验收记录](docs/evidence/2026-10-06-m4-chat-acceptance.md) | M4 计划、Plan 12/13 共享契约与状态 |
| [Plan 2](docs/plans/2026-10-05-m1-2-local-provider.md)、[Plan 5](docs/plans/2026-10-05-m1-5-control-plane.md)、[Plan 6](docs/plans/2026-10-05-m1-6-recovery-entry.md) | M1 第 2 批计划、执行中修订与验收记录 |
| [M2 计划索引](docs/plans/2026-10-05-m2-index.md)、[Plan 7 Gateway](docs/plans/2026-10-05-m2-7-gateway.md)、[Plan 8 DeepResearch](docs/plans/2026-10-05-m2-8-deepresearch.md) | M2 计划、执行中修订与验收记录 |
| [M3 工作台验证记录](docs/evidence/2026-10-05-m3-workbench.md) | E26、E27 的自动化证据；浏览器联调待在演示服务器上补入 |
| [REST API（OpenAPI）](api/openapi.yaml) | 任务提交、控制、查询与 SSE 事件流的契约 |
| [Plan 1B spike 记录](docs/experiments/2026-10-05-spike-1b.md)、[§4.6 回写提案](docs/design/2026-10-05-spec-4.6-writeback-proposal.md) | 降权启动序列的实验结论（待审阅） |
| [Worker 协议](protocol/README.md) | 协议 v1 的语义与 fixtures |

历史文档（不可直接执行）：[2026-09-29 设计稿](docs/design/2026-09-29-sandbox-runtime-design.md)、[v0.1 规格](docs/design/2026-10-03-v0.1-reliable-execution-design.md)、[旧 M1 计划](docs/plans/2026-09-29-m1-local-provider.md)。

## 这是什么（设计目标）

```
CLI / Vue ── REST + SSE ──► Go 控制面（任务/会话状态机、准入、事件续传、checkpoint、恢复）
                              ├─ Gateway：凭据隔离、调用 journal、预算、撤销、出网控制、exec 调度
                              ├─ PostgreSQL：唯一元数据存储      Redis：仅缓存
                              └─ Go Runtime：namespace + cgroup + user namespace + seccomp
                                     │
                    ┌────────────────┴────────────────┐
           编排环境：Python Worker（可信）         exec 环境：模型生成的代码
           经 Unix socket 访问 Gateway             无 Gateway 入口、无网络
```

- 长任务 Worker 故障后，从宿主已提交的 checkpoint 恢复。
- 供应商 API Key 不进入沙箱；所有外部访问经 Gateway 记录、计费、可撤销。
- 会话可冻结、驱逐并从已提交状态重建。
- 同一任务内的受限并行子研究共享预算、可取消。
- DeepResearch 与 sim-worker 通过同一套版本化协议接入。

## 能力边界（v0.2 设计）

| 项 | v0.2 |
|---|---|
| 隔离强度 | 容器级，共享宿主内核；权限边界测试是回归测试，不是无逃逸证明 |
| 编排 Worker | 可信平台代码；生成代码在独立 exec 沙箱中运行 |
| 外部调用 | 不保证 exactly-once；结果未知按全额预留保守计费 |
| 预算 | 保守估算，可出现赤字，不是严格上限 |
| 部署 | 单执行主机，无跨主机故障接管 |
| 冻结 | 保留进程与内存；驱逐才释放 |
| Workspace | 不随 checkpoint 回滚 |

完整清单见规格第 1.3 节与第 20 节。

## 参考与致谢

参考其设计，代码自行实现：

- [runc](https://github.com/opencontainers/runc)：re-exec init、降权与 `/proc` 掩蔽
- [Tencent Cloud Cube Sandbox](https://github.com/tencentcloud/CubeSandbox)：Agent 沙箱能力面
- E2B：沙箱内 daemon 与接口语义

## License

待定（v0.2 发布门槛包含 LICENSE、CONTRIBUTING、SECURITY 与版本说明）。
