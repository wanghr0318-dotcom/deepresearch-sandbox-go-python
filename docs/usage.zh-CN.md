# 使用与运维参考

本文是 [README](../README.md) 的详细补充：构建、测试、运行、各功能的用法与 server 标志。下列命令都已执行过（Go、Docker 与 PostgreSQL 命令在 WSL2 内核 6.6 上，Python 测试在 Windows 与 CI 的 Linux 上，真实模型部分在腾讯云 CVM 上）。

## 环境与测试

**依赖**：Linux（cgroup v2）或 Windows 下的 WSL2；Go 1.26+；Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)；Docker（运行 PostgreSQL）。

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

### 多租户：org、成员与平台 API key

启用账号时（`--model-base-url`），每个用户恰属一个 org：注册时建立个人 org `user-<id>`，自己是 owner；升级到 0012 迁移时已有用户同样补建，运维 token 创建的任务与会话归保留 org `operator`。任务与会话记录所属 org，以及经 API key 创建时的 key。个人 org 不接受其他成员。

**平台 API key**：格式 `abx_live_<id>_<secret>`；库中只存 `HMAC-SHA256(pepper, secret)`。pepper 是 server 首次启动时在数据目录中原子创建的 `apikey.pepper`（0600），其指纹登记在数据库中：之后每次启动核对，文件缺失（而库中已有 key 或指纹）或与登记的不一致时 server 拒绝启动（退出码 2），不会另建 pepper 使已发放的 key 全部失效。共享同一数据库的各 server 必须使用同一份文件；备份不含它，须另行保管。完整 key 只在创建时显示一次。调用时用 `Authorization: Bearer abx_live_…` 或 `x-api-key: abx_live_…`（不以 `abx_` 开头的 Bearer 仍按运维 token 判定，不以 `abx_` 开头的 `x-api-key` 被忽略；因此启用账号时运维 token 不能以 `abx_` 开头，否则 server 拒绝启动）。scope：`runtime`（以 key 的用户身份调用会话、研究与自己的任务，登出除外）与 `models`（模型 API，见下文"模型 API"）。无效、已吊销或已过期的 key 为 `401 invalid_api_key`，缺少 scope 为 `403 permission_denied`；**key 永远不能创建或吊销 key、管理成员、邀请或 org**（同样 `403 permission_denied`），泄露的 key 不能铸造新 key。校验结果在内存中缓存 30 s，吊销、成员迁出与停用用户经 PostgreSQL `NOTIFY apikey_changed` 立即通知各 server；`last_used_at` 每个 key 每分钟至多写一次。

**加入与离开 org（须本人同意）**：org 的 owner（登录会话）邀请用户 `POST /v1/orgs/invites {username, role}`（7 天有效，再次邀请即续期）；被邀请者 `GET /v1/orgs/invites` 查看，`POST /v1/orgs/invites/{id}/accept` 接受或 `DELETE /v1/orgs/invites/{id}` 拒绝；owner 可撤销本 org 发出的邀请，邀请不跨 org 可见。只有仍单独留在个人 org 中的用户能接受（接受时其旧 key 同一事务吊销）。成员可以自己离开（`DELETE /v1/orgs/members/<自己>`），owner 可以移出成员；离开或被移出的用户回到其个人 org，其在该 org 的 key 吊销；org 至少保留一个 owner。运维可以直接移入用户（`POST /v1/orgs/members {username, role, org}` 或 `agentbox org add-member`）。

**HTTP 端点**（[OpenAPI](../api/openapi.yaml) 的 `/v1/*`）：`GET /v1/me`（任何已认证的调用者）；`POST /v1/keys`（登录用户为自己建 key，运维须给 `username`）、`GET /v1/keys`（成员看自己的，owner 看 org 的，运维看全部）、`DELETE /v1/keys/{id}`（吊销，幂等）；`/v1/orgs/invites`（见上）；`DELETE /v1/orgs/members/{user}`（owner、本人；运维须给 `?org=`）；`POST /v1/orgs` 与 `POST /v1/orgs/members`（运维；建 org 时指定的 owner 与 org 在同一事务中创建）。

**已知限制**：key 被吊销或用户迁出的瞬间已通过校验的请求、以及 LISTEN 连接断开期间（至多 30 s 缓存期）的请求，仍按 key 原来的 org 记账；用户迁出后，其已有会话中新的 turn 归新 org，而会话本身仍记在旧 org。不存在的 key ID 每次请求读一次数据库（不做负缓存）；`last_used_at` 在请求中同步写入（每 key 每分钟至多一次，上限 2 s）；建 key 与查询用户的错误码可用于确认用户名是否存在。

**运维 CLI**（与 `agentbox user` 一样直接连接数据库；`key create` 读取 server 数据目录中的 pepper）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox org create acme --owner alice     # alice 须仍单独在个人 org 中；与建 org 同一事务
sudo AGENTBOX_DATABASE_URL=... agentbox org add-member acme bob --role member   # 运维直接移入（用户自己则经邀请）
sudo AGENTBOX_DATABASE_URL=... agentbox org list
sudo AGENTBOX_DATABASE_URL=... agentbox key create --user bob --name ci --scopes runtime,models --expires-in 720h --data-dir <server 的数据目录>
sudo AGENTBOX_DATABASE_URL=... agentbox key list --org acme
sudo AGENTBOX_DATABASE_URL=... agentbox key revoke <key id>
```

用 key 调用运行时 API（`$KEY` 为上面打印的完整 key）：

```bash
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:8080/v1/me
curl -s -H "x-api-key: $KEY" -H 'Content-Type: application/json' \
  -d '{"request_id":"r1","topic":"示例主题"}' http://127.0.0.1:8080/research
```

`agentbox verify-invariants` 另核对 G4（每个未吊销 key 的 org 等于其用户的 org）与 G5（带 key 的任务与会话属于该 key 的 org）。

### 限额与月度预算（org、用户、key）

启用账号（多租户）时，每个 org、用户与 key 都可以设**限额**与**月度预算**；一次请求必须同时通过它存在的每个层级。值为 null（或 CLI 的 `none`）表示该层级不限，默认全部不限。

- **限额**（`limits` 表）：`rpm`（每分钟请求数）、`tpm`（每分钟 token 数，模型 API）、`max_sandboxes`（org 的并发沙箱数）、`max_streams`（并发模型流，模型 API）。`tpm` 与 `max_streams` 见下文"模型 API"；其余生效方式是：
  - `rpm`：作用于运行时 API 中新建 turn 或启动沙箱的端点 `POST /sessions`、`POST /research`、`POST /sessions/{id}/messages`、`POST /sessions/{id}/wake`、`POST /turns/{id}/restore`、`/continue`、`/finish`、`/answer`，对 API key 与登录用户按 key、用户、org 三级的令牌桶同时扣减（任一级不足则都不扣），运维 token 不受限。超出时返回 `429 rate_limit_exceeded`（错误体仍为 `{code, message}`），带 `Retry-After`（秒）与 `x-ratelimit-limit-requests`、`x-ratelimit-remaining-requests`、`x-ratelimit-reset-requests`。配置了 `--redis-addr` 时桶在 Redis 中（一个 Lua 脚本原子地检查全部层级），各 server 共享；Redis 出错或超过 50 ms 时退化为每个 server 各自的进程内桶，请求不受影响，指标 `agentbox_ratelimit_degraded` 为 1，5 s 后再试 Redis（本地连接池暂时占满不算 Redis 故障）。
  - `max_sandboxes`（只能设在 org 层级，用户与 key 层级为 `400 unsupported_limit`）：任务申请运行槽位时检查。没有上限的 org 不做任何计数；有上限时按 PostgreSQL 中该 org 存活的任务与会话环境（加上本 server 已准入、尚未建出环境的任务）计数，达到上限的任务按 org 严格先来先到排队（保持 `queued`，指标 `agentbox_org_sandbox_waiting`），本 server 有沙箱归还时立即唤醒队首。已有存活环境的会话 turn 不新增沙箱，不受影响。
- **月度预算**（`quota_budgets`，按 UTC 月）：沙箱经 Gateway 发出的每个模型、搜索、抓取（以及 MCP）调用，在预留任务预算的同一事务中，于任务所属 org、用户与 key 各层级放一个 hold（`quota_holds`）；任一层级不足时调用被拒绝（`402 insufficient_quota`，Worker 按预算耗尽处理），任务预算照常生效且优先判定。hold 随调用的结算同步转换：成功按实际费用计入 spent，可重试/致命失败释放，结果未知按预留全额计入 unknown（重启时的账本转换同样处理）。结算失败（例如对冲中另一条腿的结算出错）而仍为 held 的预留，在调用期限过去 2 分钟后由每分钟一次的任务按重启时账本转换的同一路径转为 unknown（在途的 try 不受影响）。月度预算不足时 Worker 收到的说明是"本月配额已用尽（org/user/key）"。用户层级按 (org, 用户) 记：用户换到另一个 org 后在新 org 中是全新的用户层级，原 org 的行与花费留在原 org。没有配置预算时只多写这些 hold 行。设置的上限从当月起生效，之后的月份沿用；上限不能低于当月已用（含预留）。

**HTTP 端点**：`GET/PUT /v1/limits/{kind}/{id}` 与 `GET/PUT /v1/budgets/{kind}/{id}`，`kind` 为 `org`、`user`、`key`，`id` 分别为 org 名称、用户名、key ID；`GET /v1/budgets/...` 可带 `?period=YYYY-MM`。org 的 owner 可读本 org、读写本 org 的用户与 key（不能超过 org 自己的上限，否则 `400 exceeds_org_limit`）；org 层级只有运维能改；成员不能管理（`403 forbidden`），API key 永远不能（`403 permission_denied`），其他 org 的对象为 404。

```bash
# 运维：给 org acme 设 rpm 与并发沙箱数、月度预算 50 美元
curl -s -X PUT -H "Authorization: Bearer $(cat <data>/api.token)" -H 'Content-Type: application/json' \
  -d '{"rpm":60,"max_sandboxes":4}' http://127.0.0.1:8080/v1/limits/org/acme
curl -s -X PUT -H "Authorization: Bearer $(cat <data>/api.token)" -H 'Content-Type: application/json' \
  -d '{"limit_micro":50000000}' http://127.0.0.1:8080/v1/budgets/org/acme
curl -s -H "Authorization: Bearer $(cat <data>/api.token)" http://127.0.0.1:8080/v1/budgets/org/acme
```

**运维 CLI**（直接连接数据库；`limits set` 只改给出的字段，`none` 清除该字段）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox limits set org acme --rpm 60 --max-sandboxes 4
sudo AGENTBOX_DATABASE_URL=... agentbox limits set key <key id> --rpm 10
sudo AGENTBOX_DATABASE_URL=... agentbox limits show user bob
sudo AGENTBOX_DATABASE_URL=... agentbox budget set org acme --usd 50
sudo AGENTBOX_DATABASE_URL=... agentbox budget set user bob --limit-micro none
sudo AGENTBOX_DATABASE_URL=... agentbox budget show org acme --period 2026-10
```

`agentbox verify-invariants` 另核对 G1（每个预算行的 reserved / spent / unknown 等于其 hold 之和）与 G2（有上限的层级已用不超过上限）；hold 以高于预留的实际费用结算造成的超出单独列为 `G2 [超额，非违反]`，不算违反。

### 用量、账单与对账

启用账号时，server 每分钟把**已结算的预算 hold** 汇总进按日的 `usage_daily`（迁移 0015）：每行是一个 UTC 日、org、用户、key、模型与来源的请求数、输入与输出 token、已知费用（`cost_micro`，settled 的 hold）与未知费用（`unknown_micro`，`charged_unknown` 的 hold，即结果未知时按估算全额计入的部分）。来源 `model_api` 是模型 API 的请求（模型与 token 取自 `model_requests`）；来源 `runtime` 是沙箱经 Gateway 的调用（模型取自调用；搜索、抓取等记为端点名 `search`、`fetch`；运行时调用不记录 token，token 为 0）。被释放（没有发出、或可重试失败）的 hold 不算用量。日期取 hold 的创建时间，与 hold 计入的预算月份一致，所以一个月的用量与该月 `quota_budgets` 的 `spent_micro` / `unknown_micro` 逐分相等。

汇总按 watermark 增量：每次从其 hold 仍可能变化的最早一天起删除并重算——仍为 held 的 hold、期限未过（另加 2 分钟）的 hold，以及已结算却还没有 `model_requests` 行、期限过去不到 10 分钟的模型 API hold 所在的日子，至多为一小时前所在的一天；所以结算之后才写入的 `model_requests` 行，即使请求持续很久，也会在下一次汇总中计入。重跑结果相同；多个 server 的汇总经 PostgreSQL advisory lock 串行，并与对账在同一快照中进行。用量页与账单至多落后一分钟（响应中的 `as_of` 是上次汇总的时间）。

**HTTP 端点**（登录会话或运维 token；API key 一律 `403 permission_denied`）：

- `GET /v1/usage?from=YYYY-MM-DD&to=YYYY-MM-DD&group_by=day,model,key,user,org,source`：默认本月 1 日到今天（UTC，至多 366 天）、按日分组；另可 `?user=`、`?key=` 过滤（owner 也可以按已离开、但在本 org 有用量的用户过滤）；响应的 `filter` 字段回显生效的 org、用户与 key 过滤。成员只看到自己的用量，owner 看到本 org 的，运维看到全部 org（`?org=` 选一个）。看不到的 org、用户、key 与不存在的一样是 404。
- `GET /v1/bills/YYYY-MM`：该月账单，按模型列出请求、输入与输出 token、已知与未知费用及合计，另按来源汇总；owner 为本 org 的账单，成员为自己在 org 中的部分（`username` 字段），运维须加 `?org=`。
- `GET /v1/orgs/members`：org 的成员与角色（owner；运维须加 `?org=`），供"成员"页使用。

```bash
curl -s -H "Authorization: Bearer $(cat <data>/api.token)" 'http://127.0.0.1:8080/v1/usage?org=acme&group_by=day,model'
curl -s -H "Authorization: Bearer $(cat <data>/api.token)" 'http://127.0.0.1:8080/v1/bills/2026-10?org=acme'
```

**运维 CLI**（直接连接数据库，看全部 org；`--rollup` 先汇总一次，适合 server 没在运行时）：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox usage --org acme --from 2026-10-01 --to 2026-10-31 --group-by day,model
sudo AGENTBOX_DATABASE_URL=... agentbox bill show 2026-10 --org acme
sudo AGENTBOX_DATABASE_URL=... agentbox bill reconcile 2026-10
```

`agentbox bill reconcile <YYYY-MM>` 在同一快照中先汇总，再从原始的 `quota_holds`、`model_requests` 与调用记录重算该月，与 `usage_daily`（逐日、org、用户、key、模型、来源）和 `quota_budgets`（每个层级的 spent 与 unknown）比较；一致时退出码 0，有差异时逐条打印（指明 org、用户、key 与模型）并以退出码 1 结束（`--json` 输出报告）。已汇总过的历史日期不会被自动重算，所以其中被改动的数据会作为差异报告出来。

不先汇总的对账（`bill reconcile --no-rollup`，以及 `agentbox verify-invariants --usage-month YYYY-MM`，后者把差异作为 `U1 [对账]` 项加入检查，只读）只比较 watermark 之前的日子的 `usage_daily`——这些日子已是终值；watermark 及之后的日子要等下次汇总，报告中注明"未比较 … 起的 usage_daily"。预算总是比较（原始 hold 与 `quota_budgets` 在任何快照中都精确相等）。所以在运行中的系统上，它不会因为尚未汇总的新用量报警。

**修复对账差异**：先找出原因（例如手工改过的 hold，或部分恢复的数据库），然后从报告中最早的那一天起重算，再对账确认：

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox usage --rollup --from 2026-10-03   # 从该日起删除并重算 usage_daily
sudo AGENTBOX_DATABASE_URL=... agentbox bill reconcile 2026-10             # 退出码 0 即已修复
```

**网页**：登录后顶部导航有三页：

- **API 密钥**（`#/keys`）：列出名称、ID、权限范围、创建时间、最近使用、过期时间与状态（有效、已过期、已吊销）；"新建密钥"选择权限范围（模型 API、运行时）与有效期，创建后在对话框中**只显示一次**完整密钥，带复制按钮与醒目的提示，关闭后无法再看；吊销需要再点一次"确认吊销"。收到组织邀请时页顶有提示。
- **用量**（`#/usage`）：所选日期范围的费用（其中未知）、请求与 token 合计，每日费用与每日 token 的柱状图（SVG，悬停显示数值），按模型与按密钥的明细表，组织、本人与每个密钥的本月预算进度条（已花费、未知、预留中三段与剩余；只有 owner 能看预算），以及本月账单摘要。
- **成员**（`#/org`）：所有人都能看到收到的邀请（接受或拒绝）与自己的组织，并可离开团队组织（需确认；回到个人组织，本组织的密钥被吊销）；owner 另可查看成员与角色、移出成员、按用户名与角色邀请、撤销待接受的邀请。

**已知限制**：

- 用量来自已结算的 hold，进行中的请求（hold 仍为 held）在结算后的下一次汇总才出现；hold 结算失败而由 sweep 转为未知的模型请求没有 `model_requests` 行，记为模型"（未记录）"。
- 运行时调用不记录 token，只有请求数与费用。
- 不先汇总的对账不检查 watermark 及之后的日子（它们会在下次汇总时重算）。

### 模型 API（`--model-api`，可选，默认关闭）

`agentbox server` 加 `--model-api`（需要 `--model-base-url`，即启用账号与多租户）后，同一个监听地址另外提供：

- `POST /v1/chat/completions`：OpenAI Chat Completions 格式；
- `POST /v1/messages`：Anthropic Messages 格式（接受并回显 `anthropic-version` 头）；
- `GET /v1/models`：可用的逻辑模型（OpenAI 列表格式；带 `anthropic-version` 头时为 Anthropic 格式）。

不加 `--model-api` 时这三个路径与之前一样是 404。调用者须有 `models` scope：平台 key 以 `Authorization: Bearer abx_live_…`（OpenAI SDK）或 `x-api-key: abx_live_…`（Anthropic SDK）调用，登录会话与运维 token 也可以。错误一律以调用方的格式返回（OpenAI 的 `{"error":{…}}` 或 Anthropic 的 `{"type":"error","error":{…}}`）：key 无效 401，缺少 scope 403，请求不合法或用了无法表达的字段 400，模型不存在 404，限流或预算用尽 429，供应商失败 502，全部供应商熔断或 PostgreSQL 不可用 503。

**路由**：请求的 `model` 是声明的逻辑模型（`--model-name` / `--models`），经 Gateway 的模型路由（主供应商与 `--model-fallback-file` 中的后备供应商、权重、熔断，见"模型降级链"）发往供应商。客户端格式与供应商格式相同时请求体原样转发（只改 `model`），流式回复按供应商发出的事件逐个转发；不同时经 `internal/llmproto` 转换请求与回复（只有一种格式才有的 `cache_control`、`metadata.user_id` 等被丢弃并计数；thinking、图片、`response_format` 等无法表达的字段使请求只发往同格式的供应商，没有这样的供应商时 `400 unsupported_field`）。第一个字节写给客户端之前，可重试的失败（429、5xx、连接失败、内容之前的 `overloaded_error` 事件）换下一个供应商；之后不能再换，客户端收到其格式的错误事件。

**限流与计费**：每个请求按 key、用户、org 三级扣 `rpm`（每分钟请求数，与运行时 API 共用同一个桶；在读取请求体之前扣）与 `tpm`（估算 token：请求体字节数 ÷ 2 + 1024 + `max_tokens`，结束后按实际用量退还多扣的部分），流式请求另占一个 `max_streams` 并发名额；超出时 `429 rate_limit_exceeded`，带 `Retry-After` 与 `x-ratelimit-{limit,remaining,reset}-{requests,tokens}`（运维 token 不受限）。之后按候选供应商中最高的单价估算最坏费用（输入估算 × 最高输入单价 + `max_tokens` × 最高输出单价；客户端没有给 `max_tokens` 时补 4096；同时给出 `max_tokens` 与 `max_completion_tokens` 时只转发较小的那个），在各层级的月度预算中放一个 hold（`source = model_request`）；任一层级不足时 `429 insufficient_quota`（Anthropic 调用方为 `rate_limit_error`），消息指出是哪一级。请求结束后按实际用量结算（实际费用超过预留时按实际结算，超出由 `verify-invariants` 的 G2 单独报告，并计入 `agentbox_model_hold_overshoot_total`）；用量没有到达（客户端断开、供应商中途失败、流在 `finish_reason` 之后、用量 chunk 之前被切断）时按估算全额记为 `charged_unknown`。PostgreSQL 不可用时没有 hold 就不发出任何模型调用：`503`。

**流与时长**：`--model-api-max-duration`（默认 10 min）限制一个请求（含流）的总时长，超过时流以错误事件结束；向不再读取的客户端写入同样在这个时限（另加 2 s）之后被切断，请求照常结算。请求体上限 `--model-api-max-body`（默认 8 MiB），读取请求体限时 30 s。客户端断开时取消上游请求并照常结算；客户端断开与供应商对没有的模型回答 404 都不计入熔断器，404 时换下一个提供该模型的路由。请求进行中吊销 key 不影响这个请求（由上面的时长上限约束），下一个请求即为 401。

**记录与指标**：每个取得了 hold 的请求在结算之后写入 `model_requests`（客户端与供应商格式、是否转换、状态、用量、费用、首字时间、`stream_complete`、丢弃的字段）与 `model_request_tries`（每次尝试的供应商、结局、状态与耗时），两表按月分区（迁移 0014）；响应头 `x-request-id` 即其 ID。写入 `model_requests` 失败时重试 3 次，仍失败则记日志并计入 `agentbox_model_request_record_failures_total`（hold 已结算）。指标（`--metrics-listen`）：`agentbox_model_requests_total{protocol,model,provider,status}`、`agentbox_model_ttft_seconds`、`agentbox_model_streams_total{complete}`、`agentbox_model_rejections_total{reason}`（rate_limit、quota、auth、unsupported、invalid、unavailable）、`agentbox_model_dropped_fields_total{field}`。`agentbox verify-invariants` 另核对 G3（每个 `model_requests` 行恰好对应一个 hold，且该 hold 已结算）。

```bash
# 在"对话式助手"的启动命令上追加
--model-api --model-api-max-duration 10m --model-api-max-body 8388608

# 为自己建一个 models scope 的 key（或运维：agentbox key create --scopes models ...），再调用
curl -s http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"kimi-k3","messages":[{"role":"user","content":"你好"}],"max_tokens":200}'
curl -sN http://127.0.0.1:8080/v1/messages -H "x-api-key: $KEY" -H 'anthropic-version: 2023-06-01' -H 'Content-Type: application/json' \
  -d '{"model":"kimi-k3","max_tokens":200,"stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

官方 SDK 只需改 `base_url` 与 key：`openai.OpenAI(base_url="http://127.0.0.1:8080/v1", api_key=KEY)`、`anthropic.Anthropic(base_url="http://127.0.0.1:8080", api_key=KEY)`。兼容性测试不需要模型 Key 与 root：`AGENTBOX_TEST_DATABASE_URL=... bash scripts/sdk-compat.sh` 以未修改的 `openai` 与 `anthropic` Python SDK，分别对 fake upstream 的 OpenAI 模式与 Anthropic 模式，跑非流式、流式与工具调用（`tests/sdk`）。

**已知限制**：

- 主供应商（`--model-base-url`）总是 OpenAI 格式；Anthropic 格式的供应商只能作为后备供应商配置。主供应商默认以同名提供全部声明的模型，用降级链文件的 `routing.primary_models` 限制（见"模型降级链"）。
- 原样转发时回复中的 `model` 是供应商的模型名（例如 `kimi-k3`、`claude-x`），不改写为逻辑模型名；转换时为逻辑模型名或供应商在 message_start 中给出的名字。
- 原样转发的流中，供应商在内容之后发出的错误事件按原文转发给客户端（同一格式，可能含供应商的错误消息）；其余错误一律由 Gateway 生成，不回显供应商的错误正文。
- 非流式请求由供应商的流组装：同格式时回复也经过重新编码（不是逐字节转发），`logprobs` 只支持流式请求；`n` 只能为 1。
- 输入 token 按字节数估算（÷ 2 + 1024），只用于限流与预留，结算按供应商报告的实际用量。
- 在 hold 之前就被拒绝的请求（认证、限流、预算）不写 `model_requests`，只计入指标。
- 每个请求在预留与结算时各锁一次 org 的月度预算行：同一个 org 的吞吐受这把锁限制（负载测试中单个 org 的吞吐只有分散到 16 个 org 时的一小部分，见 [证据](evidence/2026-10-12-model-gateway.md)）。分片计数与预分配预算片是后续工作。
- Anthropic 请求的内容块只接受 Anthropic API 文档中的字段：把 SDK 回复对象 `model_dump()` 之后原样放回（新版 SDK 会带 `caller: null` 等字段）得到 `400 unsupported field`；按文档直接放回 `response.content` 可以。

#### 演示、负载测试与故障注入（本机，零模型费用，不需要 root）

三个脚本共用 `scripts/lib/gateway.sh`：以 docker 中的 PostgreSQL（新建自己的库）、`tests/e2e/fakeupstream` 作为 OpenAI 与 Anthropic 格式的供应商、带 `--model-api` 的 `agentbox server`（`--exec-slots 0`，不运行沙箱）。工作目录默认 `/tmp/agentbox-<名字>`（须在 Linux 文件系统上：数据目录的 `api.token` 须为 0600），二进制默认构建到 `bin/`。

```bash
bash scripts/demo-gateway.sh        # 多租户、两个 SDK 的四条路径（流式、工具调用）、加权路由、429 限流与预算、吊销 401、对账、指标（约 1 分钟）
bash scripts/fault-gateway.sh       # kill -9 中途崩溃（约 6 分钟，等 hold 期限）、首字节后供应商失败、流中吊销 key、熔断与恢复
bash scripts/loadtest-gateway.sh    # 网关开销、最大 RPS、限流精度、并发预算超支、org 预算行锁等待、运行中停止 Redis
AGENTBOX_LT_REPEAT=3 bash scripts/loadtest-gateway.sh latency ladder budget lockwait   # 重复关键场景
```

- `demo-gateway.sh` 需要 uv（按 `tests/sdk` 的锁文件安装官方 `openai` 与 `anthropic` SDK）；WSL2 中可用 Windows 的 uv：`AGENTBOX_DEMO_UV=/mnt/c/.../uv.exe`。
- `loadtest-gateway.sh` 另启动一个专用 Redis 容器 `agentbox-loadtest-redis`（127.0.0.1:6390，结束时删除），`redis` 场景停止并重新启动的是它。结果写入工作目录的 `results.jsonl`（每次运行一行 JSON）与 `summary.md`。负载工具也可单独使用：`go run ./cmd/agentbox-loadtest <latency|ladder|ratelimit|budget|steady|lockwait> -h`。
- 测量方法、数字与它们不能证明什么见 [docs/evidence/2026-10-12-model-gateway.md](evidence/2026-10-12-model-gateway.md)。Grafana 仪表盘 "Agentbox model gateway"（`deploy/observability/grafana/dashboards/agentbox-model-gateway.json`）随观测栈加载。

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

#### Anthropic 格式的供应商与路由模式（可选字段）

设计见 [2026-10-11-model-gateway-design.md](design/2026-10-11-model-gateway-design.md) §7.6。下面的字段都可省略；没有它们的旧文件仍然有效，行为与之前完全相同（顺序尝试，指纹与配置摘要不变）。

```json
{"providers": [
  {"name": "claude", "protocol": "anthropic", "base_url": "https://api.anthropic.com",
   "key_env": "AGENTBOX_MODEL_CLAUDE_API_KEY", "anthropic_version": "2023-06-01", "weight": 1,
   "models": {"kimi-k3": "claude-model-id"},
   "price": {"in": 3000000, "out": 15000000, "cache_read": 300000, "cache_write": 3750000}},
  {"name": "backup", "base_url": "https://api.example.com/v1", "key_env": "AGENTBOX_MODEL_BACKUP_API_KEY",
   "weight": 3, "price": {"in": 2000000, "out": 8000000}}
],
 "routing": {"mode": "weighted", "primary_weight": 2}}
```

| 字段 | 默认 | 含义 |
|---|---|---|
| `protocol` | `openai` | 供应商的线路格式。`openai`：请求发往 `base_url + /chat/completions`，Key 放在 `Authorization: Bearer`。`anthropic`：请求发往 `base_url + /v1/messages`（`base_url` 不含 `/v1`），Key 放在 `x-api-key`，并带 `anthropic-version`。 |
| `anthropic_version` | `2023-06-01` | `anthropic-version` 头，形如 `YYYY-MM-DD`；只用于 `protocol: anthropic`。 |
| `weight` | 1 | 路由权重（至少 1），用于 `weighted` 与 `latency` 模式。 |
| `price.cache_read`、`price.cache_write` | 按 `in` 计 | 缓存读取、缓存写入的输入单价（每百万 token 的微美元；`model_prices` 中同样可用）。预留按 `in` 与 `cache_write` 中较高者估算。 |
| `routing.mode` | 有权重时 `weighted`，否则 `ordered` | `ordered`：按文件顺序（主供应商在前），与之前相同。`weighted`：在提供该模型、熔断器就绪（关闭，或打开时长已过、可以半开试探）的供应商中按平滑加权轮询（nginx 算法）选第一个，其余按权重从高到低作为后备；熔断后恢复的供应商由此被试探并重新得到它的份额。`latency`：同 `weighted`，有效权重为权重 ÷ 延迟的 EWMA，不低于最快供应商的 5%，慢的供应商仍有少量探测流量。沙箱调用用整个回复时间，模型 API 的流式调用用首 token 时间（各自一个 EWMA，没有样本时用另一个）；超时与 unknown 记一个惩罚样本（每 try 超时与当前 EWMA 两倍中的较大者）。 |
| `routing.primary_weight` | 1 | 主供应商（`--model-base-url`）的权重。 |
| `routing.primary_models` | 不给出 | 主供应商只提供的逻辑模型及其在该供应商的模型名，例如 `{"kimi-k3": "kimi-k3"}`；空对象 `{}` 表示主供应商不提供任何模型（全部交给后备供应商）。不给出时主供应商以同名提供全部声明的模型（与之前相同，指纹不变）。给出时每个声明的模型须至少由一个路由提供（主供应商的 `primary_models`，或某个后备供应商的 `models`；`models` 为空的后备供应商提供全部模型），否则以退出码 2 拒绝启动。逻辑模型只由某个后备供应商真正提供时（例如 `smart` 只映射到 Claude），应当用它把主供应商限制在自己的模型上，主供应商就不会收到它没有的模型名。 |

- 沙箱里的 Worker 仍发送 OpenAI 格式的请求。转到 Anthropic 供应商时，Gateway 把请求转换为 Anthropic 格式，再把回复转回 OpenAI 格式，Worker 看到的结构与 OpenAI 供应商相同（`choices[0].message`、`tool_calls`、`finish_reason`、`usage.prompt_tokens`）。之前轮次 assistant 消息中的 `reasoning_content` 不发给 Anthropic 供应商。journal、重放、指纹、预留与结算的规则不变；结算按该供应商的价格，缓存 token 按缓存单价计。
- 只有 OpenAI 格式才有的请求字段（例如 `response_format` 为 `json_schema`）不能转换：这样的请求跳过 Anthropic 供应商，跳过原因为 `unsupported_protocol`。
- 熔断、每 try 超时与对冲的语义不变；每次调用的供应商顺序在调用开始时确定，同一调用内的换供应商与对冲沿用它。
- 校验失败（未知的 `protocol` 或 `routing.mode`、`anthropic_version` 用于 openai 供应商或不是 `YYYY-MM-DD`、权重小于 1、`routing.mode` 为 `ordered` 却给出权重、负的缓存单价、`primary_models` 含未声明的模型或空的模型名、某个声明的模型没有任何路由提供）时 `agentbox server` 以退出码 2 拒绝启动。
- 设置了缓存单价时，预留按输入、缓存读取、缓存写入单价中最高者估算输入，并为分段取整多留 1–2 微美元，因此在用量不超过估算假设时，实际费用不会超过预留。
- 转到 Anthropic 供应商时，不符合 `^[a-zA-Z0-9_-]+$` 的工具调用 id（例如 `functions.web_search:0`）被确定性地改写，`tool_use` 与对应的 `tool_result` 保持一致。

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

`GET /tasks/{id}/inspect` 的 `calls[].tries[]` 带 `provider`、`skipped`（`name:reason`，原因为 `circuit_open`、`tried`、`model_not_served`、`unsupported_protocol`）、`hedge` 与 `hedge_lost`；单供应商配置下这些字段省略。Gateway 日志的 `gateway: try` 行另带 `route`、`route_skipped`、`hedge`、`breaker`；熔断状态变化记为 `gateway: breaker`（`route`、`from`、`to`），降级记为 `gateway: degraded`。

混沌测试（fake upstream，零费用；测量有无降级链时一次模型调用的延迟，结果见[证据](evidence/2026-10-10-model-fallback.md)；只在设置 `AGENTBOX_CHAOS_RUNS` 时运行，平时由确定性的 `TestRoute*`/`TestHedge*` 覆盖）：

```bash
AGENTBOX_CHAOS_RUNS=5 go test ./internal/gateway/call -run TestChaos -v
```

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

### 轨迹诊断（agentbox doctor-traces）

`agentbox doctor-traces` 是运维工具：读失败与慢的运行，用确定性规则给出根因（附证据：受影响的服务端任务 ID 与调用 ID、各供应商 try 的计数与延迟分位、延迟占比；观测栈在运行时还有 trace ID 与按 span 名汇总的耗时），并在白名单内提出配置修改。它不修改运行中的 server：提议写成补丁，`--apply-to` 另写一份实验用的 flags 文件。设计见 [轨迹诊断设计](design/2026-10-10-trace-doctor-design.md)；一次实际闭环见 [轨迹诊断演示记录](evidence/2026-10-10-trace-doctor.md)。

- **来源**：`--run <eval 运行目录>`（`trajectories.jsonl` + `manifest.json`），或 `--server <地址> --since 1h`（经运维 API 读取时间窗口内创建的任务：`GET /tasks`、`inspect` 与事件流；token 取 `$AGENTBOX_TOKEN` 或 `--data-dir`）。
- **规则**：模型 try 挂起而没有每 try 超时、熔断/降级、供应商错误、搜索/抓取被调用期限截断、抓取被拒、工具额度与费用预算耗尽、exec 超时、退避占主导、对冲浪费、沙箱启动慢、准入排队、停止慢。一个失败或慢的任务归因于第一条解释它的规则；没有规则能解释的任务列为 unexplained（例如答案错误不是基础设施问题）。
- **可修改的 flag（白名单与上下界）**：`--model-try-timeout`（1 s–2 m 或 0）、`--model-hedge-delay`（200 ms–60 s 或 0）、`--model-breaker-failures`（1–10）、`--model-breaker-open`（5 s–10 m）、`--call-deadline`（5 s–5 m）、`--model-call-deadline`（30 s–15 m）、`--turn-tool-budget`（1–1000）。其他建议（供应商顺序、run slots、suite 预算、skill 文本）只作为 advisory 写进报告与补丁注释。
- **flags 文件**：每行一个 `--name=value`（`#` 注释），即 server 的命令行参数；`--config` 只读，`--apply-to` 另写 `<out>/experiment.flags`，输入文件不变。
- **观测栈（可选）**：`--tempo http://127.0.0.1:3200 --prometheus http://127.0.0.1:9090 --loki http://127.0.0.1:3100`：按 `task.id` 在 Tempo 中找到 trace 并按 span 名汇总耗时、Prometheus 的 try/调用/熔断计数作为交叉核对、Loki 中 WARN/ERROR 日志按消息计数并附 trace ID。不可达时只记一条说明。
- **可选的模型建议（默认关闭）**：`--advisor-model M --advisor-base-url URL --advisor-price IN:OUT [--advisor-budget-usd 0.05]`，Key 只读 `AGENTBOX_DOCTOR_API_KEY` 或 `AGENTBOX_MODEL_API_KEY`。一次调用，只发送诊断结果（ID、计数、延迟、错误码），最坏费用超出预算时不调用；模型的提议同样按白名单与上下界校验，与规则冲突时以规则为准。模型的输入包含规则的提议，因此它的认同不是独立的佐证。回复中的文字在写入任何文件或报告前都被展平为单行，不能新增 flag 行。

```bash
./bin/agentbox doctor-traces --run eval-runs/<run> --config before.flags          # 报告 + proposal.patch
./bin/agentbox doctor-traces --run eval-runs/<run> --apply-to before.flags --out doctor-out   # 另写 doctor-out/experiment.flags
AGENTBOX_TOKEN=... ./bin/agentbox doctor-traces --server http://127.0.0.1:8080 --since 2h --tempo http://127.0.0.1:3200
```

一键闭环演示（root；全新数据目录与数据库，真实沙箱 + 两个 fake upstream，零模型费用；错误配置的 server 运行 suite → doctor 给出修改 → 以修改后的配置再运行 → `eval compare`，按 Wilson 区间判断改进是否超出噪声；`AGENTBOX_DOCTOR_OBS=1` 时同时导出到已运行的观测栈）：

```bash
sudo bash scripts/demo-doctor.sh
```
