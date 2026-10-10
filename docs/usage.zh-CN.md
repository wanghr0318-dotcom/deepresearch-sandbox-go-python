# 使用与运维参考

本文是 [README](../README.md) 的详细补充：构建、测试、运行、各功能的用法与 server 标志。下列命令都已执行过（Go、Docker 与 PostgreSQL 命令在 WSL2 内核 6.6 上，Python 测试在 Windows 与 CI 的 Linux 上，真实模型部分在腾讯云 CVM 上）。

## 环境与测试

**依赖**：Linux（cgroup v2）或 Windows 下的 WSL2；Go 1.25+；Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)；Docker（运行 PostgreSQL）。

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

**一键演示**：`sudo bash scripts/demo-m1.sh` 在全新的数据目录与数据库上执行整条链路并逐步打印结果——提交 → checkpoint → 杀死 Worker → 从 checkpoint 恢复 → result → 清理，再 SIGKILL server → 重启恢复 → `verify-invariants --quiescent` → 泄漏检查；任一步失败即非零退出。一次实际运行的完整输出见 [docs/evidence/2026-10-05-m1-demo-run.md](evidence/2026-10-05-m1-demo-run.md)。下面是同一链路的手动步骤。

需要 root（cgroup、命名空间与 UID 映射）、cgroup v2、`python3`（3.11+，沙箱内用宿主的 `/usr`）与上文的 PostgreSQL。配置示例 [`deploy/agentbox.env.example`](../deploy/agentbox.env.example) 列出 `agentbox server` 的全部标志与环境变量，下面的命令按原样载入它（数据目录 `/var/lib/agentbox`，监听 `127.0.0.1:8080`）。数据目录与数据库一一绑定（安装身份）：换数据库须换数据目录。

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

**Gateway 配置**：每个 attempt 在 `<data>/gateway/<attempt_id>.sock` 有一个 Gateway 入口，挂载到沙箱内的 `/run/agentbox/gateway.sock`（属主为环境映射 uid 1000、0600）；attempt 结束或取消生效时先在数据库撤销访问、再关闭入口。供应商 Key 只从宿主环境变量 `AGENTBOX_MODEL_API_KEY`、`AGENTBOX_SEARCH_API_KEY` 读取，不进入 Worker 的 init、沙箱环境变量与日志；`--worker-env` 的键须在白名单中（如 `PYTHONPATH`）。相关标志（含义与默认值见配置示例）：`--default-budget-micro` / `--budget-cap-micro`（task 层预算，微美元；任务可用 `limits.budget_micro` 指定，超过上限返回 `400 invalid_limits`）、`--model-base-url` / `--model-name` / `--models` / `--model-price-in-micro-per-mtok` / `--model-price-out-micro-per-mtok` / `--model-price`（OpenAI 兼容模型上游；不设 `--model-base-url` 时不提供模型端点；`--model-name` 是请求未指定 `model` 时的默认模型，`--models` 是声明的白名单（须包含默认模型），请求可按调用在其中选择 `model`，白名单外为 `400 unsupported_model`；`--model-price model=IN:OUT` 给出按模型的单价，用于预留估算与结算，只是配置、不代表供应商实际计费）、`--search-provider ddg_lite|tavily|serper|fake`（`serper` 为经 Serper.dev 的 Google 结果：`POST https://google.serper.dev/search`，Key 只放在 `X-API-KEY` 头；`tavily` 与 `serper` 都读同一个 `AGENTBOX_SEARCH_API_KEY`，未设置时 server 以退出码 2 拒绝启动。serper 已在演示服务器上以真实 Key 运行，见 [M3 服务器验收记录](evidence/2026-10-06-m3-server-acceptance.md)）、`--upstream-allow-private`（显式放行的私有上游，例如本机模型服务）。`agentbox task inspect` 除 attempt 与 checkpoint 外列出每个 Gateway 调用及其 try（端点、chat 调用解析后的模型、状态、费用、延迟、上游请求 ID；不含请求与响应正文）。

### 真实研究演示

`scripts/demo-m2.sh` 在全新的数据目录（`/var/lib/agentbox-demo-m2`）与数据库（`agentbox_demo_m2`）上，以真实沙箱与真实 Gateway 运行一个 DeepResearch 任务（`--worker-argv python3,-m,deepresearch`），逐步打印：提交 → Worker 运行中从宿主读取沙箱内每个进程的 `/proc/<pid>/environ` 与命令行，确认不含 Key 的值与变量名（G3）→ 等待 `task_terminal` → `inspect` 的调用明细（端点、模型、tries、费用、延迟；不含正文）与总费用 → 报告前 40 行与证据列表（报告从 `<data>/blobs` 读取并校验 sha256）→ 环境清理 → `verify-invariants --quiescent` → 泄漏检查 → Key 不出现在 server 日志、事件流与 inspect 输出中。任一步失败即非零退出。

**前提**：与上文快速开始相同（root、cgroup v2、`python3` 3.11+、PostgreSQL；数据库经本机 `psql` 或 docker 新建）；脚本自行构建 `bin/agentbox` 并把 `worker/` 中的 `agentbox_worker`、`sim_worker`、`deepresearch` 安装到 `/opt/agentbox`。

**主/从模型**：DeepResearch 的主 agent（编排：计划与最终报告）与任务内的 worker（各任务的总结）可以用不同的模型。任务 config 的 `orchestrator_model` 与 `worker_model` 决定每次 chat 调用请求体中的 `model`（未配置时不带 `model`，由 Gateway 用 `--model-name` 的默认模型）；两者都须在 server 的 `--models` 白名单中。模型名进入调用指纹，恢复后同一步骤重发同一模型。演示脚本默认编排 `kimi-k3`、worker `kimi-k2.6`，以 `--model-name <worker 模型> --models <白名单>` 启动 server，`inspect` 明细显示每个调用的模型（取自结果 blob 中上游回复的 `model` 字段）并检查路由：plan/report 为编排模型，任务内 chat 为 worker 模型。费用按配置的单价估算与结算，不代表供应商的实际计费。

**变量**（变量名也列在 [`deploy/agentbox.env.example`](../deploy/agentbox.env.example)）：`AGENTBOX_DEMO_MODEL_BASE_URL`（默认 `https://api.moonshot.cn/v1`，OpenAI 兼容端点）、`AGENTBOX_DEMO_ORCHESTRATOR_MODEL`（默认 `kimi-k3`）、`AGENTBOX_DEMO_WORKER_MODEL`（默认 `kimi-k2.6`）、`AGENTBOX_DEMO_MODELS`（白名单，默认 `kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3`）、`AGENTBOX_DEMO_MODEL_PRICES`（可选，`model=IN:OUT`，逗号分隔）、`AGENTBOX_DEMO_SEARCH_PROVIDER`（`ddg_lite` 默认、无 Key；`tavily` 需要 `AGENTBOX_SEARCH_API_KEY`；`serper` 需要 `AGENTBOX_SERPER_API_KEY`，脚本把它作为 `AGENTBOX_SEARCH_API_KEY` 交给 server，因此两种 Key 可并存于 `.env`，只读取所选供应商的那一行，并纳入 G3 与日志的 Key 泄漏检查；`serper` 的真实运行见 M3 服务器验收记录）、`AGENTBOX_DEMO_TOPIC`（默认一个固定中文题目）、`AGENTBOX_DEMO_PRICE_IN_MICRO_PER_MTOK` / `AGENTBOX_DEMO_PRICE_OUT_MICRO_PER_MTOK`、`AGENTBOX_DEMO_BUDGET_MICRO`（可选）。模型 Key 取环境变量 `AGENTBOX_MODEL_API_KEY`，未设置时从仓库根的 `.env`（git 忽略；可用 `AGENTBOX_DEMO_ENV_FILE` 指定）中**只读取这一行**，不 `source` 整个文件；脚本只报告"已设置"，不打印、不写入任何文件，只经环境变量交给 server。

**演练**（fake upstream 扮演模型、搜索与网页，不访问外网，原样回显请求的 `model`；Key 为随机生成的假值；其余流程与真实模式相同，含主/从模型路由检查。已在 WSL2 6.6 x86_64 上以 root 执行，16 步全部通过）：

```bash
sudo AGENTBOX_DEMO_FAKE=1 bash scripts/demo-m2.sh
```

**真实模型**（默认 Moonshot，需要 `AGENTBOX_MODEL_API_KEY`；一次实际运行见[演示记录](evidence/2026-10-06-m2-demo-run.md)）：

```bash
sudo bash scripts/demo-m2.sh
```

**Python Worker SDK 测试**：

```bash
cd worker
uv run pytest -q
```

### 工作台

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

### DeepResearch 助手（用户侧）

配置了模型上游（`--model-base-url`）时，server 启用用户账号，浏览器打开站点根路径即是面向用户的研究助手：

1. **注册 / 登录**：用户名 3–32 位字母、数字、`_`、`.`、`-`；注册密码 8–16 位，且至少包含数字、大写字母、小写字母中的两种（登录不校验此规则）。会话保存在 `HttpOnly; Secure; SameSite=Strict` cookie 中，有效期 7 天。
2. **新研究**：只需输入研究主题。模型、搜索与预算由 server 固定（编排 `--user-orchestrator-model`，默认 `kimi-k3`；worker `--user-worker-model`，默认 `kimi-k2.6`）。每个用户同一时间最多 1 个进行中的研究。
3. **我的研究**：只列出自己的研究；进度按"计划 → 子任务 → 报告"显示；完成后在页面中阅读报告（安全渲染）并下载。

用户看不到 API Key、token、预算、模型、费用或调用明细（事件流与任务视图在服务端按允许列表脱敏）；他人的研究一律返回"不存在"。注册与登录按 IP 限速，登录失败不区分"用户不存在"与"密码错误"；密码以 PBKDF2-SHA256（600,000 次迭代，标准库 `crypto/pbkdf2`）存储。

**运维**：工作台移到 `#/admin`，仍使用 `<data>/api.token`（启用账号时运维调用必须带有效 Bearer token）。账号管理直接连数据库（已在演示服务器上执行）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox user list
sudo AGENTBOX_DATABASE_URL=... agentbox user disable <username>   # 同时吊销其全部会话
sudo AGENTBOX_DATABASE_URL=... agentbox user enable <username>
```

演示服务器的常驻服务见 `deploy/systemd/agentbox-demo.service.example`（HTTPS 443、自签证书、serper、Redis、4C8G 资源与预算上限；复制为 `/etc/systemd/system/agentbox-demo.service`，并在 `/etc/agentbox/agentbox.env` 中设置 `AGENTBOX_PUBLIC_HOST`）。

### 会话

会话是用户与研究助手的一段对话：每条消息是一个 turn（一个任务），同一会话的 turn 在同一个长期运行的沙箱进程（incarnation）中依次执行，并共享会话状态（最新的 session checkpoint）与 `/workspace`。

**用户**（登录后，API 见 [OpenAPI](../api/openapi.yaml) 的 `/sessions`、`/turns`）：

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

### 代码执行沙箱

编排 Worker 经 Gateway 的 `POST /v1/exec` 请求运行一段 Python（SDK：`ctx.gateway.exec(step_id, code, inputs=[(sha256, path)], wall_ms=...)`；对话 Agent 以工具 `run_python` 调用它）。server 为**每次**调用新建一个 exec 环境，运行 `python3 -I -B /in/.agentbox/main.py`（cwd `/out`），收集 `/out` 后销毁环境。

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

### 工作区工具与 MCP（可选，默认关闭）

设计见 [2026-10-10-shell-file-mcp-design.md](design/2026-10-10-shell-file-mcp-design.md)。两项都关闭时 turn spec、端点与 Agent 的工具与之前完全相同。

- **`--workspace-tools`**（需要 exec，即 `--exec-slots` > 0）：对话 Agent 多出 `write_file`、`read_file`、`list_dir`、`exec_shell` 四个工具，Gateway 多出 `POST /v1/workspace/{exec,read,write,list}`。每个 turn 一个工作区：文件清单由 Gateway 持有（内容在 BlobStore，状态文件在 `<data>/tool-workspaces/`），文件操作只按清单查找、不碰宿主路径；`exec_shell` 的每条命令在一个全新的 exec 环境中运行（工作区文件只读暂存到 `/in/ws`，由固定的包装脚本复制到 `/out` 后执行 `bash`，结束后 `/out` 中的普通文件成为新版本），与 `run_python` 同样无网络、独立 UID、seccomp、按 exec 配额记账（不计入每轮 30 次工具额度）。进程不在命令之间保留；符号链接、特殊文件与超出 256 个的文件不保留（结果中列出）。路径只能是工作区内的相对路径（`..`、绝对路径、`a//b` 为 400 `invalid_path`）。路径中不能有 `.agentbox` 目录、控制字符或超过 255 字节的文件名（与 exec 暂存一致；命令生成的这类文件不保留，结果中列出原因）。限额：至多 256 个文件、总大小不超过 `--exec-out-bytes` − 1 MiB（留出暂存余量；工作区放入 `/out` 后剩余空间归命令使用）、单次写入 1 MiB、每轮至多 2000 次写入与 256 MiB 写入量（`write_file` 写入的内容永久保存在 BlobStore 中，不会被清理）、单次读取 2000 行 / 256 KiB（流式读取，只读所需窗口）。`write_file` 带 `"delete": true` 可删除文件或整个目录。包装脚本在工作区完整放入执行环境后才在 stdout 开头写出暂存标记；没有标记（如空间不足）时工作区不变，返回 503 `workspace_staging_failed`。生命周期：第一次使用时建立；turn 结束（终态）后由清扫器销毁；空闲超过 `--workspace-idle-timeout`（默认 2 h）后过期（之后的操作为 410 `workspace_expired`）；server 重启后从状态文件恢复，状态文件不可信或文件 blob 缺失时标记丢失（410 `workspace_lost`）。同一 call id 重发的 `exec_shell` 返回已记录的结果，不会再次执行。
- **`--mcp-config <文件>`**：Gateway 作为 MCP 客户端连接运维配置的服务器，Agent 的工具清单中多出允许的工具（名称 `mcp__<server>__<tool>`），调用经 `POST /v1/mcp/call` 以上游类别 `mcp` 记入调用 journal（同一 call id 重放、调用期限、撤销即取消），每次计入每轮工具额度（与搜索、抓取合计）。沙箱不连接 MCP 服务器、看不到其凭据；stdio 服务器只得到配置中的 `env`、`env_from`（启动时从宿主环境变量读取）与 `PATH`，HTTP 服务器的凭据头取自 `headers_from_env`。MCP 服务器与模型、搜索上游同属运维信任的基础设施：stdio 服务器作为 server 的子进程在宿主上运行（不在沙箱中），默认以 server 的用户（通常是 root）运行，参数由模型决定——**只把对恶意参数也安全的工具放进允许清单**。可用 `uid`/`gid` 降权（两者须同时给出）、`dir` 指定工作目录（默认每个服务器一个空的临时目录）；它在自己的进程组中运行，超时或关闭时整组被杀死，server 退出时随之退出。每个 stdio 服务器同一时间只处理一个调用。`arguments` 至多 64 KiB；HTTP 服务器返回 4xx 视为明确拒绝（不重试），5xx 视为结果未知（`initialize` 阶段的 5xx 除外：工具调用尚未发出，可重试）。配置示例（演示服务器只允许 `calculate` 与 `unit_convert`，`echo_env` 不在清单中，Agent 看不到、调用被拒绝 403 `mcp_tool_not_allowed`）：

```json
{"servers": [
  {"name": "calc", "transport": "stdio", "command": ["/usr/local/bin/agentbox", "mcp-demo-server"],
   "allowed_tools": ["calculate", "unit_convert"], "timeout_ms": 10000},
  {"name": "docs", "transport": "http", "url": "https://mcp.internal.example/mcp",
   "headers_from_env": {"Authorization": "DOCS_MCP_AUTH"}, "allowed_tools": ["search_docs"]}
]}
```

```bash
# 在下文"对话式助手"的启动命令上追加两个标志（mcp.json 中 command 写 agentbox 二进制的绝对路径）
sudo -E ./bin/agentbox server --data-dir /var/lib/agentbox --web-dir web/dist \
  --model-base-url https://api.moonshot.cn/v1 --model-name kimi-k2.6 --models kimi-k2.6,kimi-k3 \
  --search-provider serper --session-worker-argv python3,-m,chatagent \
  --workspace-tools --mcp-config /etc/agentbox/mcp.json
```

真实沙箱中的演示轮次（真实的对话 Agent、真实 exec 环境、stdio 演示 MCP 服务器；模型为脚本化的 fake upstream；以 root、`CGO_ENABLED=0` 运行）：

```bash
CI=true CGO_ENABLED=0 go test -count=1 -p 1 -v -run 'TestRealWorkspaceShellMCPDemo' ./tests/e2e/   # 以 root 运行
```

### 对话式助手

启用会话后（`--session-worker-argv python3,-m,chatagent`），登录后打开站点根路径即是对话式研究助手：左栏是会话列表，中间是对话，右侧面板有"进度 / 来源 / 报告"三个标签，默认收起，由对话标题栏的"进度"按钮或报告卡片打开、× 收起。宽屏下三栏之间的分隔线可拖动（或聚焦后用 ←/→ 每次 16 px），宽度记在本浏览器；窄屏（≤ 900 px）为单列，会话列表与面板以抽屉打开。

- **新对话**：左栏"＋ 新对话"；首条消息的前 40 个字符作为标题。日常问题由模型直接回答（标注"直接回答"），也可从本会话的记忆（此前的回答与报告）作答。
- **深度研究**：输入框中的"深度研究"开关强制研究（否则由模型决定）。Agent 先读取 deep-research skill，范围不明确时用**提问卡**问一轮（至多 3 个选择题，可选"其他"自填），然后写待办清单、把子主题并行交给各自的执行者检索与阅读，最后写带编号引用 `[n]` 的报告，报告末尾附系统生成的证据列表（引用只指向本轮读过的网页）。抓取到的人机验证/拦截页不交给模型、不作为来源，步骤行标注"页面被拦截"（仍计入额度）。每轮 `web_search` + `web_fetch` 合计至多 30 次（第 31 次被 Gateway 拒绝）；额度用尽时用已有资料写报告并标注"已达工具额度"。报告生成后全文显示在对话中，其后是报告卡片（"部分""已达工具额度"标记），点击卡片在右侧"报告"标签中打开并可下载；撰写报告期间显示"正在整理资料并撰写报告… N 秒"。
- **步骤行**：读取 skill、待办清单、搜索网页、阅读网页等逐行显示、可展开，按子主题分组；连续的"思考"与连续的"阅读网页"各合并为一行（如"阅读网页 · 5 个网页"），点击展开；一轮结束后整条链折叠为"研究过程 · N 步 · 用时"。⟨/⟩ 显示发给模型或工具的请求与服务端脱敏后的响应，不含 Key、费用、模型名与内部调用 ID。
- **停止 / 继续 / 立即写报告**：研究进行中点输入框内的 ■ 停止，turn 暂停并显示停止卡（进度卡与 2–3 句"目前发现"）；"继续"从停止处接着研究，"立即写报告"（至少一个子主题完成时可用）用已有资料写出标注"部分"的报告。点停止后、停止卡出现前，该轮显示"正在停止… N 秒"（从点停止时起计）。停止时模型若未能及时写出摘要，"目前发现"改为"停止时模型正在处理，未能生成摘要…"（宿主兜底）或一句按进度的说明（如"已完成 1 个子主题、阅读 5 个来源。"），"继续"与"立即写报告"照常可用。停止卡出现后直接发送"继续"（或"继续研究 / 接着 / continue / go on / resume"）等同于点"继续"，不会新建一轮；其他文字照常作为新消息。研究进行中输入框的发送按钮变为 ■（停止），不能发新消息。
- **恢复**：停止后直接发新消息，原研究被取消但内容保留（卡片显示"已停止"与"恢复"），其来源与已完成的子主题摘要作为新一轮的上下文；之后点"恢复"会在同一会话开新一轮，从原研究的最后 checkpoint 继续，并获得新的 30 次额度。

**安装与运维**：Worker 包（含 `chatagent` 与 `skills/`）安装到沙箱默认模板中的 `/opt/agentbox`，界面由 `--web-dir` 同源提供；演示服务器的常驻服务 `deploy/systemd/agentbox-demo.service.example` 即以下列配置运行（另加 TLS、Redis 与资源、预算参数）：

```bash
sudo bash scripts/dev/install-worker.sh /opt/agentbox   # agentbox_worker、deepresearch、chatagent、skills（与 sim_worker）
(cd web && npm ci && npm run build)                     # → web/dist
# AGENTBOX_DATABASE_URL、AGENTBOX_MODEL_API_KEY、AGENTBOX_SEARCH_API_KEY（serper 的 Key）只从环境变量读取
sudo -E ./bin/agentbox server --data-dir /var/lib/agentbox --web-dir web/dist \
  --model-base-url https://api.moonshot.cn/v1 --model-name kimi-k2.6 --models kimi-k2.6,kimi-k3 \
  --search-provider serper --session-worker-argv python3,-m,chatagent
```

编排（路由、计划、报告）用 `--user-orchestrator-model`（默认 `kimi-k3`），子主题与停止摘要用 `--user-worker-model`（默认 `kimi-k2.6`）；两者是推理模型，`--model-max-tokens-cap`（默认 32768）不得低于 16384。用户点"停止"时宿主给 Worker 60 s 写停止卡，超时则终止 incarnation（会话被驱逐，turn 仍从最后 checkpoint 暂停，可继续）。独立 exec（`--exec-slots`，默认 4，演示服务器为 2）已启用，对话 Agent 经 `run_python` 工具使用它。

### 并行研究（sub-run）

用户侧无需任何操作：深度研究的子主题（2–4 个）作为同一 turn 内的 sub-run 并行执行。主 Agent（kimi-k3）负责规划与写报告，每个 sub-run 内的搜索/阅读循环由 kimi-k2.6 驱动。对话中的步骤按子主题分组，右侧"进度"面板（标题栏"进度"按钮打开）中多个子主题同时显示"进行中"。每轮 30 次工具额度由全部 sub-run 共享。停止、继续、立即写报告与恢复的用法不变；已完成的子主题在继续或恢复后不会重跑。

运维侧：

- **inspect。** `GET /tasks/{id}/inspect` 的 `subruns[]`，或工作台 `#/admin` 的任务详情，给出每个 sub-run 的状态、开始/结束时间、取消或失败原因，以及 sub-run 层的预留/已花/unknown 费用（task 层是两层之和的上层）。`calls[].subrun_id` 标出每个调用归属哪个 sub-run。
- **`--worker-subruns`**（默认 true）：向 Worker 协商 `subruns` 扩展。设为 false 时，研究退回到进程内逐个执行子主题。
- **`--subrun-cancel-timeout`**（默认 10s）：宿主取消一个 sub-run 后，等待 Worker 结束它的时限。超时则终止整个 attempt，从 checkpoint 恢复。
- **串并行对比。** 运维可用 `POST /tasks {session_id, spec: {text, deep_research, research: {scheduling, fixed_plan}}}` 在指定会话中创建 turn。驱动与评分脚本见 [m4-gate 标签下的 `experiments/subrun-compare/`](https://github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tree/m4-gate/experiments/subrun-compare)，结果见[对比与验收记录](evidence/2026-10-06-m4-subrun-comparison.md)。演示服务器上每组 N = 4（小样本）：墙钟时间 P50 串行 302 s、并行 184 s；费用中位数 0.48 / 0.50 USD；两组都无失败；引用可定位率都是 100%。

### 模型降级链（多供应商、熔断、对冲）

设计见 [2026-10-10-model-fallback-design.md](design/2026-10-10-model-fallback-design.md)。不给 `--model-fallback-file` 时只有主供应商（`--model-base-url`），行为与之前完全相同。

后备供应商写在一个 JSON 文件里，按顺序尝试；Key 不写在文件里，只给出宿主环境变量名（`key_env`，可省略，用于无鉴权的本机服务）：

```json
{"providers": [
  {"name": "backup", "base_url": "https://api.example.com/v1", "key_env": "AGENTBOX_MODEL_BACKUP_API_KEY",
   "models": {"kimi-k3": "vendor-model-a", "kimi-k2.6": "vendor-model-b"},
   "price": {"in": 2000000, "out": 8000000},
   "model_prices": {"kimi-k3": {"in": 4000000, "out": 16000000}}}
]}
```

- `models` 把逻辑模型（`--model-name` / `--models` 中声明的名字，Worker 请求里写的就是它）映射为该供应商的模型名；不在其中的逻辑模型不由它提供（跳过原因 `model_not_served`）。省略 `models` 表示以同名提供全部声明的模型。
- `price` 必填，`model_prices` 按逻辑模型覆盖；单位与 `--model-price` 相同（每百万 token 的微美元）。转到后备供应商的 try 按它的价格预留与结算。
- 后备供应商的地址同样经出网防护；私有地址须在 `--upstream-allow-private` 中。

在上文的 `agentbox server` 命令后加：

```bash
export AGENTBOX_MODEL_BACKUP_API_KEY=...        # 只从环境变量读取
sudo -E ./bin/agentbox server ... --model-fallback-file /etc/agentbox/model-fallback.json \
  --model-try-timeout 90s --model-breaker-failures 3 --model-breaker-open 30s
```

| 标志 | 默认 | 含义 |
|---|---|---|
| `--model-fallback-file` | 空 | 后备供应商列表（JSON，见上）。为空时没有降级链。 |
| `--model-breaker-failures` | 3 | 供应商连续失败（retryable 或 unknown）多少次后熔断：之后直接跳过它，不花 try。 |
| `--model-breaker-open` | 30s | 熔断打开时长；届满后下一次调用向它放行一个半开试探，成功则恢复，失败则再打开。 |
| `--model-try-timeout` | 0（不设） | 每次模型 try 的超时（需要 `--model-fallback-file`）。不设时挂起的供应商会占满整个 `--model-call-deadline`，无法转到下一个；推理模型的长输出可能需要几十秒，取值应高于正常 p99。超时时请求已发出，按 unknown 结算（估算计入 unknown）。 |
| `--model-hedge-delay` | 0（关闭） | 对冲（需要 `--model-fallback-file`）：try 发出这么久仍未结束时，向下一个就绪的供应商并发发出同一请求，先成功者胜出，另一方被取消。落败方已发出时按估算全额计入 unknown，最坏情况约两倍费用；建议设在主供应商 p95 附近。 |

行为：

- 可重试的失败（连不上、429、5xx、每 try 超时、2xx 但没有 `choices`——链模式下主供应商也按此判定）之后**立即**换下一个供应商，不退避；本轮所有供应商都失败才按 `--model-call-deadline` 内的指数退避重来。一次逻辑调用的 try 总数仍受上限 3 约束（跨供应商累计）。
- 400/401/403/404 等明确拒绝（fatal）不换供应商：请求本身不被接受，换供应商也一样；鉴权失败应修配置，而不是被掩盖。其中 401/403/404 与出站防护拒绝计入该供应商的熔断。
- 后备供应商的地址在启动时按出站防护检查：私有地址与 localhost 须在 `--upstream-allow-private` 中，否则拒绝启动。
- 对冲：任一腿的 ok 或第一条腿的 fatal 才是决定性结果；对冲腿的 fatal（例如后备 Key 配错）不取消第一条腿。只有最终结果决定调用的状态与结果，其余的腿只记账（`hedge_lost` 标记被取消的一方，`error` 保留它自己的错误码）；触发对冲后落败的第一条腿计为一次熔断失败。
- 所有提供该模型的供应商都处于熔断时，调用立即以 `503 model_degraded` 失败（不预留、不访问上游）。Worker 把它当作暂时性错误；对话界面显示"模型服务暂时不可用"。
- 指纹只含逻辑模型，与哪个供应商执行无关；已完成调用的重放直接返回 journal 中的结果，不访问任何供应商，也不受熔断状态影响。

查看每次 try 由谁执行、为什么跳过了别的供应商：

```bash
./bin/agentbox task inspect <task_id> --format text   # "MODEL CALL" 表：TRY、PROVIDER、LATENCY、OUTCOME、HEDGE、SKIPPED
```

`GET /tasks/{id}/inspect` 的 `calls[].tries[]` 带 `provider`、`skipped`（`name:reason`，原因为 `circuit_open`、`tried`、`model_not_served`）、`hedge` 与 `hedge_lost`；单供应商配置下这些字段省略。Gateway 日志的 `gateway: try` 行另带 `route`、`route_skipped`、`hedge`、`breaker`；熔断状态变化记为 `gateway: breaker`（`route`、`from`、`to`），降级记为 `gateway: degraded`。

混沌测试（fake upstream，零费用；测量有无降级链时一次模型调用的延迟，结果见[证据](evidence/2026-10-10-model-fallback.md)；只在设置 `AGENTBOX_CHAOS_RUNS` 时运行，平时由确定性的 `TestRoute*`/`TestHedge*` 覆盖）：

```bash
AGENTBOX_CHAOS_RUNS=5 go test ./internal/gateway/call -run TestChaos -v
### Kubernetes provider（`--provider k8s`，可选）

默认的 `--provider local` 不变。`--provider k8s` 把每个环境放进一个 Kubernetes Pod（设计见 [k8s provider 设计](design/2026-10-10-k8s-provider-design.md)，测量见[证据](evidence/2026-10-10-k8s-provider.md)）：

- **控制通道**：经 pods/exec 运行镜像内的 `agentbox-podagent exec`，它在 workload 启动后先在 stdout 写一行确认（对应本地 provider 的 `start_ack`），再转发 workload 输出；退出码与信号由它记录。`Stop` 把 Pod 的 `activeDeadlineSeconds` 设为 1（kubelet 杀死全部容器，Pod 对象保留到 `Destroy`），并经 `agentbox-podagent shutdown` 快速结束容器。
- **隔离**：非 root（uid 10001）、只读根文件系统、丢弃全部 capability、`RuntimeDefault` seccomp、不挂载 ServiceAccount token、deny-all NetworkPolicy；`--k8s-runtime-class` 可指定 `runtimeClassName`（例如 gVisor）。Pod 的内存、CPU 由任务限额给出（requests = limits）；`pids.max` 由 kubelet 的 `podPidsLimit` 统一限制。
- **workspace 与 Gateway socket**：经节点本地的槽位目录（hostPath）进入 Pod，因此 server 与节点须以同一路径看到 `--data-dir`（kind：`extraMounts`）。单节点设计；多节点需要 RWX 卷与 socket 代理（见设计 §2.2）。
- **镜像固定**：`--k8s-image repo:tag` 在启动时经镜像仓库 API 解析为 digest，Pod 只引用 `repo@sha256:…`；`--k8s-image-strict` 拒绝 tag。
- **预热池**：`--k8s-warm-pool N` 保持 N 个已启动、未分配的 Pod（按默认任务资源规格与镜像 digest）；资源规格相同的环境直接认领，之后异步补足。
- **不支持**：exec 环境（须 `--exec-slots 0`）、`verify-invariants`（只检查本地 provider）。

在 kind 上完整演示（构建镜像、推送到本地仓库、以最小权限的 ServiceAccount 运行 server、任务 A/B/C 与冷/热启动延迟）：

```bash
bash scripts/demo-k8s.sh        # 需要 docker、kind、go、python3、curl；集群与本地镜像仓库会保留
```

worker 镜像单独构建：

```bash
docker build -f deploy/k8s/worker.Dockerfile -t <registry>/agentbox-worker:<tag> .
```

provider 的契约一致性测试在 fake clientset 上随单元测试运行；对真实集群运行（含冷/热启动延迟测量）：

```bash
AGENTBOX_K8S_TEST_KUBECONFIG=<kubeconfig> AGENTBOX_K8S_TEST_IMAGE=<repo@sha256:…> \
  go test -count=1 -v -run Real ./internal/provider/k8s/
```
### 可观测性（追踪、指标与日志）

默认全部关闭，行为与未启用时相同。设计与属性白名单见[可观测性设计](design/2026-10-10-observability-design.md)，一次完整演示的记录见[演示记录](evidence/2026-10-10-observability.md)。

server 标志：

- **`--otlp-endpoint`**：OTLP/HTTP 导出地址（例如 `http://127.0.0.1:4318`）；为空时不追踪。**`--trace-sample-ratio`**（默认 1）：新 trace 的采样比例。
- **`--metrics-listen`**：Prometheus `/metrics` 的监听地址（例如 `127.0.0.1:9464`）。与 API 分开监听、无鉴权，只供运维的 Prometheus 抓取，不得暴露给用户；非 loopback 地址启动时打印警告。
- **`--log-file`**：JSON 日志另外追加写入的文件（0600），供日志采集器读取；带活动 span 的日志行有 `trace_id`、`span_id`。

追踪模型：一次用户操作（`POST /tasks`、resume、会话消息、continue/answer）一条 trace。API 请求 → 运行（`task`/`turn`）→ `attempt` → `env.create`、`worker.run`（`worker.start`、`worker.handshake`、`checkpoint.commit`、`worker.finalize`）、`env.stop` / `session.handoff`；Worker 的每个 Gateway 调用是 `worker.run` 下的 `gateway.call`，每次上游 try 是其下的 `gateway.try`。宿主经协议 `init` / `task_start` 的 `traceparent` 字段把 `worker.run` 交给 Worker，Python SDK（`agentbox_worker.tracecontext`，只用标准库）在每个 Gateway 请求上带 `traceparent` 头；Gateway 只接受属于本 attempt 的 trace 的值，其他值忽略。span 与指标标签只含 ID、数字与有限枚举，不含提示词、查询、URL、Key 或自由文本错误信息。

本地观测栈与演示（以 root 在 WSL2 或 Linux 中运行；fake upstream，不访问外网、没有模型费用）：

```bash
docker compose -f deploy/docker-compose.yml up -d --wait              # PostgreSQL
sudo bash scripts/demo-observability.sh                               # 启动观测栈、运行任务与会话 turn、取回证据
# Grafana 看板 http://127.0.0.1:3000/d/agentbox-overview；Prometheus http://127.0.0.1:9090；Tempo API http://127.0.0.1:3200
docker compose -f deploy/observability/docker-compose.yml down        # 停止观测栈（加 -v 删除数据）
```

手动接入已有的 server：先 `cp deploy/observability/prometheus/targets/agentbox.json.example deploy/observability/prometheus/targets/agentbox.json`（WSL2 / Docker Desktop 经 `host.docker.internal:9464` 抓取；原生 Linux 上把 server 的 `--metrics-listen` 设为 docker 网桥网关地址并相应修改该文件），再以 `AGENTBOX_LOG_DIR=<日志目录> docker compose -f deploy/observability/docker-compose.yml up -d --wait` 启动观测栈，server 加 `--otlp-endpoint http://127.0.0.1:4318 --metrics-listen 127.0.0.1:9464 --log-file <日志目录>/agentbox.log`。

### 评测平台（agentbox eval）

`agentbox eval` 是运维工具：按 suite（`eval/suites/*.yaml|json`）经运维 REST API 批量运行任务，记录轨迹、评分并生成报告，两次运行可对比。设计见 [评测平台设计](design/2026-10-10-eval-platform-design.md)；一次实际运行见 [评测平台演示记录](evidence/2026-10-10-eval-platform.md)。

- **任务**：`coding`（编码/终端任务：沙箱内的 `evalworker` 取得解答——模型经 Gateway 写出，或 `--agent reference` 用 suite 的参考解——再经 `/v1/exec` 在独立 exec 沙箱中运行 suite 的检查器）与 `research`（交给 `deepresearch`，与产品相同的研究 Agent）。server 须以 `--worker-argv python3,-m,evalworker` 启动（`evalworker` 安装到 `/opt/agentbox`：`scripts/dev/install-worker.sh`）。
- **输出**：`<out>/<run-id>/` 下的 `manifest.json`（suite 哈希、seed、并发、`GET /server-info` 给出的服务端构建与模型、Worker 版本、exec 模板摘要）、`trajectories.jsonl`（每个任务一行：事件流、Gateway 调用 journal、attempt、账本、结果与评分；不含提示词与请求/响应正文）、`summary.json` 与 `report.md`（成功率、延迟 P50/P95、费用、工具调用、失败类别）。
- **评分**：任务状态、检查器判定（退出码为 0 且打印了 harness 经管道交给它的随机完成令牌才算通过：解答不在检查器进程内运行：`solution` 是代理模块，调用经管道以 JSON 转发给单独的子进程，检查器与 harness 设为不可 dump，解答读不到令牌；解答的提前退出只结束它自己的进程，判为失败；检查器以 `python -I` 运行、夹具读运行前的快照并在运行后校验（被改动即 `fixtures_tampered`），隔离条件不满足时判为 `harness_unsafe`；因此检查器与解答之间只能传递普通数据（数字、字符串、列表、元组、字典、集合、bytes），不能传函数；检查器超时为 `check_timeout`）、stdout 匹配；研究报告的引用可定位率（每个 `[n]` 对应的证据 sha256 须是本任务一次已完成 `fetch` 调用的结果 blob）与必含词；可选的 LLM judge（默认关闭；`--judge-model`、`--judge-base-url` 与必填的 `--judge-price IN:OUT`，`--judge-budget-usd` 硬预算与 `--judge-max-calls` 硬调用上限，`--judge-required` 使无法评判的任务失败；Key 只读环境变量 `AGENTBOX_JUDGE_API_KEY` 或 `AGENTBOX_MODEL_API_KEY`，不写入任何结果文件）。`compare` 给出两边的样本数 N 与成功率的 Wilson 95% 区间，N < 30 或每任务只重复 1 次时给出警告。
- **先用参考解验证 suite**：夹具只经检查器进程内挂钩的 `open`、`io.open`、`_io.open`（以及经它们的 `pathlib`）从内存快照读取；绕过挂钩的读取（`os.open`、`io.FileIO`、`mmap`、C 扩展、检查器启动的子进程）看到的是磁盘上的内容，其中可能有解答放置的文件。夹具名在检查器目录中不存在，因此这样写的检查对正确解答同样失败：每个 suite 先以 `--agent reference` 运行并确认全部通过，再相信模型的分数。
- **零成本**：`fakeupstream -eval-suite <suite>` 按 suite 的 `fake_reply` 回答编码任务，研究为固定脚本；费用是按配置单价折算的模拟值。

```bash
export AGENTBOX_TOKEN=$(sudo cat /var/lib/agentbox/api.token)     # 或 --data-dir
./bin/agentbox eval run --suite eval/suites/demo.yaml --agent reference --concurrency 4 --out eval-runs
./bin/agentbox eval run --suite eval/suites/demo.yaml --agent model --concurrency 4 --out eval-runs --min-success 0.5
./bin/agentbox eval compare eval-runs/<run-A> eval-runs/<run-B>      # 指标、逐任务与失败类别的变化；--json 机器可读
./bin/agentbox eval report eval-runs/<run-A>                          # 由 trajectories.jsonl 重新生成 summary 与 report
```

一键演示（root；全新数据目录与数据库，真实沙箱 + fake upstream，零模型费用；依次运行参考解、fake 模型并发 4 与并发 1，并输出两份对比）：

```bash
sudo bash scripts/demo-eval.sh
```

**重放与确定性**：同一任务内，每个 Gateway 调用有确定的 call id 与指纹；Worker 崩溃或 server 重启后，恢复的 attempt 重发同一调用时直接得到 journal 中保存的结果，不再调用上游（模型回答、检查器运行与费用都不重复）。不同的评测运行之间不重放：真实模型的两次运行可能不同，因此 manifest 固定 suite、模型与服务端构建，用 fake upstream 得到可重复的结果。
