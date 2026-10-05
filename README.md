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
| 会话、exec 沙箱、sub-run | 未开始 | M4 |

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

## 文档

| 文档 | 内容 |
|---|---|
| [路线图](docs/design/2026-10-03-roadmap.md) | v0.2 首发（M1–M4）→ v0.3 多节点 → v0.4 MicroVM；发布门槛 |
| [v0.2 首发设计规格](docs/design/2026-10-03-v0.2-first-release-design.md) | 架构、协议、数据模型、Gateway、会话、隔离、验证与发布门槛 |
| [代码组织设计](docs/design/2026-10-03-code-organization.md) | 模块职责、允许依赖、事务用例、所有权、测试布局 |
| [M1 计划索引](docs/plans/2026-10-03-m1-index.md) | M1 各计划、依赖与验收归属 |
| [持久化设计](docs/design/2026-10-04-m1-4-persistence-design.md)、[安装身份修订](docs/design/2026-10-05-installation-identity-amendment.md) | Plan 4 的设计依据 |
| [Provider 契约](docs/design/2026-10-05-provider-contract.md) | 环境生命周期的 Go 接口、错误、并发边界 |
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
