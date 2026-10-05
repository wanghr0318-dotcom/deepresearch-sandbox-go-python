# M2 Plan 8：Worker SDK Gateway 客户端、DeepResearch 接入与业务验收

> **执行方式**：有界多 agent（同 Plan 7）；Python 侧任务在 Windows（uv）与 WSL 都可运行测试；真实沙箱用例串行。
> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development。

**Goal:** 让 Python Worker 经 `/run/agentbox/gateway.sock` 使用 Gateway（SDK 客户端），把现有 DeepResearch 的研究算法（计划 → 检索 → 阅读 → 证据 → 报告）接入为 `worker/deepresearch` 包：checkpoint 可恢复、证据以 blob 引用、报告为产物；以 fake upstream 完成自动化业务验收，以真实模型完成可记录的人工演示。

**Architecture:** `agentbox_worker.gateway` 是唯一的 Gateway 客户端（stdlib `http.client` over AF_UNIX），负责 `X-Agentbox-Call-Id` 的确定性生成、重放/冲突/预算错误的类型化异常、结果 blob 引用的收集；`deepresearch` 只依赖 SDK 与标准库，不碰数据库、Redis、子进程与网络；研究状态是带 `schema_version` 的 dataclass，由单一状态所有者在步骤边界 checkpoint。现有项目 `F:\helloagents-deepresearch0527` 只作为**算法与提示词来源**（planner/reporter/prompts），其 FastAPI、Milvus、torch、hello-agents 依赖**不**进入沙箱；它的 `.env`、简历与 `memory.db` 不得读取或发布。

**Tech Stack:** Python ≥ 3.11 标准库（SDK 与 deepresearch 均零运行时依赖）、pytest、ruff TID251、import-linter；Go e2e（Plan 7 的 fake upstream）。

**规格依据：** §5.4（`init.budget_limits`、checkpoint `refs[]`）、§5.5 规则 2（refs 必须已授权到 scope）、§5.6（产物）、§9.3（端点与 headers）、§9.4（`CallDivergence`、`X-Agentbox-Supersedes`）、§9.6（`402 budget_exhausted` 不等于终止）、§16.4 业务验收（自动化：预期研究步骤完成；报告非空且引用均对应已保存证据 blob；中途杀死 Worker 后恢复使用已提交 checkpoint；已完成可重放的调用未再次访问上游）与人工演示；§1.2 G3（Key 不进沙箱）；代码组织 §2.2（`agentbox_worker` 职责、`deepresearch` 禁止项、状态即数据）、§3 规则 6。

## Global Constraints

- **依赖**：`agentbox_worker` 与 `deepresearch` 运行时依赖为空（`pyproject` 保持 `dependencies = []`）；`deepresearch` 不导入 `sqlite3`/`psycopg`/`redis`/`requests`/`httpx`/`socket`（除经 SDK）、不调用 `subprocess`、`os.fork`、`os.system`、`os.exec*`（ruff TID251 + import-linter）；`agentbox_worker` 不导入 `deepresearch`。
- **全部外部访问经 Gateway**：`deepresearch` 只通过 `ctx.gateway`；沙箱 netns 只有 lo，任何直连都会失败——测试以 fake socket 服务器证明没有其他出口。
- **Call ID**：`<subrun_id|root>/<step_id>/<kind>/<n>`，`n` 为该 step 内该 kind 的序号，由 `TaskContext` 持有并随 checkpoint 状态持久化，恢复后续号不重复、不回退；≤ 256 字节。
- **幂等与恢复**：同一 step 内重复发起的相同调用得到重放（Gateway 保证）；`CallDivergence` 不自动换 ID，由 deepresearch 以新 ID + `X-Agentbox-Supersedes` 显式重发并记入状态。
- **大小**：遵守 §5.10（inline state ≤ 256 KiB，超出用 `state_ref`——M2 的研究状态以证据 sha 引用而非正文，保持 inline）。
- **提示词与报告语言**：沿用现有项目的中文提示词；报告 Markdown，引用形如 `[n]`，末尾"证据"列表把 `[n]` 映射到证据 blob 的 sha256 与来源 URL。
- **分支**：`m2-batch`（Plan 7 之后）；推送与合并另行授权。

## 任务与依赖

| 任务 | 内容 | 依赖 |
|---|---|---|
| 1 | SDK Gateway 客户端（Unix socket HTTP、call id、异常、blob 引用）、`sim_worker` 新增 `chat`/`search`/`fetch` 操作 | Plan 7 Task 4（协议已定；可用 fake socket 服务器先行） |
| 2 | `deepresearch` 包：模型、提示词、状态与 `migrate()`、计划/检索/阅读/总结/报告各步骤 | 1 |
| 3 | 研究循环与恢复：步骤边界 checkpoint、证据 refs、`CallDivergence`/`budget_exhausted` 处理、报告产物与 result | 2 |
| 4 | Gateway 真实沙箱冒烟（Plan 7 的真实 e2e 缺口）与 G3 断言 | 1、Plan 7 Task 5 |
| 5 | 自动化业务验收（fake upstream，真实沙箱）：固定题目脚本、杀死 Worker 恢复、重放不打上游、引用对应证据 | 3、Plan 7 Task 6 |
| 6 | 人工演示：`scripts/demo-m2.sh`（真实模型）、README、配置示例、证据记录 | 5 |

Task 1 可与 Plan 7 Task 3–4 并行（只依赖已定的 HTTP 契约与 fake 服务器）。

---

### Task 1：SDK Gateway 客户端与 `sim_worker` 操作

**Files:** Create `worker/agentbox_worker/gateway.py`；Modify `worker/agentbox_worker/runtime.py`（`TaskContext.gateway` 属性、`budget_limits`、call 计数器进入 checkpoint 快照）、`worker/agentbox_worker/errors.py`（异常类型）、`worker/sim_worker/app.py`（`chat`/`search`/`fetch` 操作）、`worker/tests/test_sdk.py`（追加）、`worker/tests/conftest.py`（fake Unix socket Gateway 服务器）。

**Interfaces（Python）：**

```python
# agentbox_worker/gateway.py
class GatewayClient:
    def __init__(self, socket_path: str = "/run/agentbox/gateway.sock", *, call_ids: "CallIds", timeout_s: float = 130.0): ...
    def chat(self, step_id: str, messages: list[dict[str, str]], *, max_tokens: int | None = None, temperature: float | None = None) -> "GatewayResult": ...
    def search(self, step_id: str, query: str, *, max_results: int = 5) -> "GatewayResult": ...
    def fetch(self, step_id: str, url: str) -> "GatewayResult": ...
    def budget(self) -> dict[str, int]: ...
    def read_blob(self, sha256: str) -> bytes: ...
    def retry(self, step_id: str, kind: str, call_id: str, body: dict) -> "GatewayResult": ...  # X-Agentbox-Retry: true
    def supersede(self, step_id: str, kind: str, old_call_id: str, reason: str, body: dict) -> "GatewayResult": ...

@dataclass(frozen=True)
class GatewayResult:
    call_id: str; body: dict; blob_sha256: str | None; replayed: bool; status: int

class CallIds:                      # 单一状态所有者持有；进入 checkpoint 状态
    def next(self, step_id: str, kind: str, subrun_id: str | None = None) -> str   # "<subrun|root>/<step_id>/<kind>/<n>"
    def snapshot(self) -> dict[str, int]; @classmethod def restore(cls, data: dict[str, int]) -> "CallIds"

# errors.py 新增
class GatewayError(Exception): status: int; code: str
class BudgetExhausted(GatewayError)      # 402 budget_exhausted / budget_insufficient_for_request
class CallDivergence(GatewayError)       # 409 fingerprint_mismatch
class CallInProgress(GatewayError)       # 409 call_in_progress（客户端有界等待后重试，默认 30 s 内指数退避）
class AccessRevoked(GatewayError)        # 403 access_revoked / 连接被拒
class CallDeadlineExceeded(GatewayError) # 504
```

**规则：** 所有请求带 `X-Agentbox-Call-Id`（`/v1/budget`、`/blobs` 除外）；响应 `X-Agentbox-Blob` → `blob_sha256`，`X-Agentbox-Replayed: true` → `replayed`；HTTP 层超时略大于 Gateway 期限（130 s）；`call_in_progress` 在客户端有界重试（同 ID）；其他 4xx/5xx 映射为上述异常；`TaskContext.gateway` 懒创建；`TaskContext.budget_limits` 来自 `init`；`CallIds` 快照由 SDK 在 `checkpoint()` 时并入 state 的保留键 `_agentbox.call_ids`（恢复时 `restore`），应用状态不受影响。`sim_worker` 新操作：`{"op":"chat","step_id":..,"messages":[..]}`、`{"op":"search",...}`、`{"op":"fetch",...}`，把 `blob_sha256` 加入后续 checkpoint 的 `refs`，用于 Plan 7/8 的 e2e。

**Tests（pytest，fake Unix socket Gateway 服务器）：** call id 格式与序号单调、快照/恢复后续号；三种端点的请求头与体；`X-Agentbox-Blob`/`Replayed` 解析；409/402/403/504 映射；`call_in_progress` 有界重试后成功；超时；`read_blob`；sim_worker 新操作把 sha 放入 refs；运行时依赖仍为空（`uv tree`/import 检查）。

**验证：** `cd worker && uv run pytest -q && uv run ruff check . && uv run lint-imports`。

---

### Task 2：`deepresearch` 包——模型、提示词、状态与步骤

**Files:** Create `worker/deepresearch/__init__.py`、`models.py`、`prompts.py`、`state.py`、`steps.py`、`worker/tests/test_deepresearch.py`；Modify `worker/pyproject.toml`（packages 增加 `deepresearch`；import-linter 契约：`deepresearch` 禁止 `sqlite3, psycopg, redis, requests, httpx, urllib.request, socket, subprocess, multiprocessing`；`agentbox_worker` 禁止 `deepresearch`；ruff TID251 对 `deepresearch` 禁止 `os.fork/os.system/os.exec*/subprocess`）。

**Interfaces：**

```python
# models.py（移植自现有项目 models.py 的 TodoItem/SummaryState，去掉 note/stream 字段）
@dataclass class ResearchTask: id: int; title: str; intent: str; query: str; status: str = "pending"; summary: str | None = None; evidence: list[str] = field(default_factory=list)  # evidence: 证据 sha256 列表
@dataclass class Evidence: sha256: str; url: str; title: str; snippet: str; call_id: str
@dataclass class ResearchState:
    schema_version: int = 1; topic: str = ""; tasks: list[ResearchTask] = ...; evidence: dict[str, Evidence] = ...; loop_count: int = 0
    report_sha256: str | None = None; failures: list[str] = ...
    def to_json(self) -> dict; @classmethod def from_json(cls, d: dict) -> "ResearchState"
def migrate(d: dict) -> dict  # 按 schema_version 升级；未知版本抛 ValueError

# prompts.py：PLAN_PROMPT（输出 JSON 任务列表 {tasks:[{title,intent,query}]}，≤ max_tasks）、SUMMARIZE_PROMPT（给定任务与证据片段，输出中文摘要并以 [n] 引用）、REPORT_PROMPT（综合各任务摘要，输出 Markdown 报告与"证据"列表）——内容移植自 helloagents 的 planner/CHAIN_RAG_*/reporter 提示词并改为引用编号规则。
# steps.py（纯函数 + Gateway 调用，不含控制流）
def plan(gw: GatewayClient, topic: str, *, max_tasks: int) -> tuple[list[ResearchTask], GatewayResult]
def search_and_fetch(gw: GatewayClient, task: ResearchTask, *, max_results: int, max_fetch: int, step_id: str) -> list[Evidence]
def summarize(gw: GatewayClient, task: ResearchTask, evidence: list[Evidence], *, step_id: str) -> str
def write_report(gw: GatewayClient, state: ResearchState, *, step_id: str) -> str  # Markdown
def parse_tasks(text: str) -> list[ResearchTask]  # 容错 JSON 提取（移植 _extract_json_payload）
```

**规则：** 证据 = `fetch` 结果 blob（`blob_sha256`）加上来源 URL/标题/片段（来自 search 结果）；摘要与报告的提示词只包含证据片段（截断到每条 ≤ 4 KiB）与编号；`parse_tasks` 失败 → 回退为单任务（移植 `create_fallback_task`）；LLM 输出非 JSON 时记 `failures` 不中止。

**Tests：** `migrate` 对 v1 恒等、对未知版本报错；`parse_tasks` 的三种输入（纯 JSON、代码块包裹、散文含 JSON）；各步骤对 fake Gateway 的请求形状（messages 含证据编号、`max_tokens` 显式）；证据 sha 来自 fetch 的 `X-Agentbox-Blob`；状态 JSON 往返；lint 契约通过。

**验证：** 同 Task 1。

---

### Task 3：研究循环、恢复与产物

**Files:** Create `worker/deepresearch/app.py`（`run(ctx)` 与 `__main__`）、`worker/deepresearch/__main__.py`；Modify `worker/tests/test_deepresearch.py`（追加）。

**Interfaces：** `async def run(ctx: TaskContext) -> Result | Paused`；配置来自 `ctx.config`：`{topic, max_tasks=4, max_results=5, max_fetch=3, max_loops=1, report_artifact_id="report"}`；入口 `python3 -m deepresearch`。

**规则（循环与恢复）：**
1. 恢复：`ctx.resume.state` → `migrate` → `ResearchState`；否则 `plan()` → checkpoint `plan`（refs = 计划调用的 blob）。
2. 对每个 `pending` 任务：`search_and_fetch` → `summarize` → 任务置 `done`，证据并入 `state.evidence` → checkpoint `task-<id>`，`refs` = 该任务全部证据 sha + 摘要调用 blob（§5.5 规则 2：全部已由 Gateway 授权到 task scope）。每个 checkpoint 之后检查 `ctx.should_pause()` → 返回 `Paused(checkpoint_id)`；`ctx.cancel_reason` → 以 `WorkerFailure("cancelled")` 结束（SDK 已有语义）。
3. 全部任务完成 → `write_report` → 写 `out_dir/report.md` → `register_artifact("report", "report.md")` → checkpoint `report`（refs = 报告调用 blob）→ `Result(summary=首段, outputs=["report"])`。
4. `CallDivergence`：记录 `failures`，以 `gw.supersede(..., reason="divergence")` 用新 ID 重发一次；再冲突则该任务置 `failed`。
5. `BudgetExhausted`：停止新调用；若至少一个任务 `done` → 用已有摘要生成**不再调用模型**的降级报告（模板拼接）并正常 `Result`，`summary` 标注"预算耗尽，部分结果"；否则 `WorkerFailure("budget_exhausted", retryable=False)`。
6. `CallDeadlineExceeded`/`GatewayError(5xx)`：该任务重试一次（新 call id 由 `n` 递增），再失败置 `failed`，继续其余任务。
7. 任何 checkpoint 之间不保存不可序列化对象；`CallIds` 随状态持久化（Task 1）。

**Tests：** 固定 fake Gateway 脚本（2 个任务、每任务 2 条搜索结果、1 次抓取）：完整运行产生报告与 `outputs=["report"]`，报告中每个 `[n]` 都映射到 `state.evidence` 中的 sha；在 `task-1` checkpoint 后"杀死"（抛出）并以该 checkpoint 恢复 → 不重新计划、任务 1 不重做（fake 计数）、调用 id 继续递增；`should_pause` 在边界返回 `Paused`；`CallDivergence` → supersede；预算耗尽 → 降级报告；`fetch` 失败一次后重试。

**验证：** `uv run pytest -q worker/tests/test_deepresearch.py`；`python3 -m deepresearch` 以 fake init 行启动（协议 fixtures 的 task 模式）。

---

### Task 4：Gateway 真实沙箱冒烟与 G3 断言

**Files:** Modify `tests/e2e/e2e_test.go`（追加 `TestRealGatewayChat`、`TestRealNoCredentialsInSandbox`）、`scripts/demo-m1.sh`（不改）；Create `tests/e2e/fakeupstream` 已由 Plan 7 Task 6 提供。

**规则：** 真实沙箱（provider/local + 生产启动器）中运行 `sim_worker`：`{"op":"chat"}` 经 `/run/agentbox/gateway.sock` → fake upstream 返回脚本化回复 → checkpoint 带该 blob ref → result；断言：socket 在沙箱内属主 uid 1000、0600（init 已校验）；`inspect` 显示 1 条 `completed` 调用、1 次 try；G3：workload 以 `sim_worker` `print` 操作输出 `env` 与 `/proc/self/environ` → 不含 `AGENTBOX_MODEL_API_KEY`/`AGENTBOX_SEARCH_API_KEY` 的值；server 日志与事件表（`grep`）不含 Key。

**验证：** root：`CI=true CGO_ENABLED=0 go test -count=1 -run 'TestRealGateway|TestRealNoCredentials' ./tests/e2e/`。

---

### Task 5：自动化业务验收（fake upstream，真实沙箱）

**Files:** Modify `tests/e2e/e2e_test.go`（追加 `TestRealDeepResearchFixedTopic`、`TestRealDeepResearchKillAndResume`）、`tests/e2e/fakeupstream/fakeupstream.go`（研究脚本：按 messages 中的阶段标记返回计划 JSON / 摘要 / 报告；搜索返回 3 条固定结果；抓取返回固定页面；计数器按 call id 去重）、`.github/workflows/ci.yml`（root 作业把 `worker/deepresearch` 一并复制到 `/opt/agentbox`）、`scripts/ci/check-runner.sh`（无改动）。

**验收条目（§16.4 自动化业务验收）：**
- 预期研究步骤完成：`inspect` 的 checkpoints 为 `plan, task-1, task-2, report`；事件含 `artifact_saved(report)`。
- 报告非空且引用均对应已保存证据 blob：读取 report blob，解析 `[n]` 与"证据"列表的 sha，每个 sha 在 `blobs` 且在 `scope_blobs(task)`。
- 中途杀死 Worker 后恢复使用已提交 checkpoint：在 `task-1` 提交后杀死 → 新 attempt 的 `init.resume.checkpoint_id = task-1`；fake upstream 的计划调用计数仍为 1。
- 已完成可重放的调用未再次访问上游：恢复后任务 1 的 search/fetch/summarize 不再到达 fake upstream（计数不变），`inspect` 显示这些调用 `Replayed`。
- 每条用例结束 `verify-invariants --quiescent`（I3、I14）。

**验证：** root：`CI=true CGO_ENABLED=0 go test -count=1 -run 'TestRealDeepResearch' ./tests/e2e/`（连续三次）；CI root 作业实际运行。

---

### Task 6：人工演示（真实模型）、README 与证据

**Files:** Create `scripts/demo-m2.sh`、`docs/evidence/2026-10-XX-m2-demo-run.md`（运行后填写）；Modify `README.md`（M2 状态、Gateway 配置、运行真实研究任务的步骤）、`deploy/agentbox.env.example`（模型/搜索配置项，Key 只写变量名）。

**规则：** 环境检查：`AGENTBOX_MODEL_API_KEY` 已设置（只检查非空，不打印）、`--model-base-url`/`--model-name` 已给、搜索源（`ddg_lite` 无 Key 或 `tavily` 需 `AGENTBOX_SEARCH_API_KEY`）、PostgreSQL、/opt/agentbox 含 `deepresearch`；全新数据目录与数据库（同 demo-m1 做法）；提交真实研究任务（题目参数化，默认一个固定中文题目）→ `task watch` 到 `task_terminal` → `inspect` 打印调用明细（端点、tries、费用、延迟；不含正文）→ 打印报告前 40 行与证据列表 → `verify-invariants --quiescent` → 泄漏检查 → G3：沙箱内无 Key（复用 Task 4 的检查方式）。失败即非零退出。证据记录须写明：模型与供应商（不写 Key）、题目、费用、调用次数、报告摘录、日期。

**验证：** root 执行 `scripts/demo-m2.sh` 一次（需要用户提供模型 base URL 与模型名；Key 已在 `F:\go-agentbox\.env`），输出写入证据文件；README 步骤逐条执行。

---

## 联合验收

1. 本地：`cd worker && uv run pytest -q && uv run ruff check . && uv run lint-imports`；非 root 全量 Go；root：Task 4、5 的真实沙箱用例连续三次。
2. CI：全部作业通过（python 3.11/3.13、root 作业运行 `TestRealDeepResearch*`）。
3. 范围：diff 只含本计划 Files；运行时依赖仍为空；`deepresearch` 的禁止导入契约通过。
4. 验收归属：§16.4 自动化业务验收；人工演示（证据记录）；G3 断言。**M2 门槛** = Plan 7 + Plan 8 联合验收 + CI。

## 自查记录

- **规格覆盖**：§9.3 headers 与 §9.4 `CallDivergence`/`Supersedes` → Task 1、3；§5.5 规则 2（refs 已授权）→ Task 3（refs 只用 Gateway 返回的 blob）；§5.6 → Task 3（report 产物）；§9.6 `402` 不等于终止 → Task 3 规则 5；§1.2 G3 → Task 4、6；§16.4 业务验收四条 → Task 5；人工演示 → Task 6；代码组织 §2.2/§3 规则 6 → Task 2 契约。
- **不在本计划**：sub-run 并行研究（M4）；Redis 缓存收益（M3）；串并行对比实验（M4）；把 hello-agents/FastAPI/Milvus 栈搬进沙箱（明确不做，见 Architecture）。
- **待审批的取值**：默认 `max_tasks=4, max_results=5, max_fetch=3, max_loops=1`；演示题目参数化；`ddg_lite` 作为无 Key 搜索源。
- **接口新增（中心）**：`TaskContext.gateway`、`TaskContext.budget_limits`、checkpoint state 保留键 `_agentbox.call_ids`（SDK 内部，应用不可见）。
- **占位符**：证据文件名中的 `XX` 在运行当日填写。

## 验收记录（2026-10-05）——状态：Task 1–5 已验收；Task 6（真实模型人工演示）待用户提供模型 base URL 与模型名

### 1. 本地测试结果
- Python：Windows `uv run pytest` 358 passed / 35 skipped（AF_UNIX 用例）、ruff、ruff format、lint-imports（3 份契约，含 `deepresearch` 禁止导入与 TID251）；WSL `AGENTBOX_REQUIRE_UNIX_TESTS=1` 116 passed（含全部 deepresearch 与 SDK socket 用例）。
- 本地 WSL2 6.6 x86_64：`go vet`、`GOOS=windows go build`、真实 PostgreSQL 上 `CI=true go test ./...`、root `tests/e2e` 36 PASS / 0 FAIL / 0 SKIP、无 cgroup 残留。

### 2. 真实沙箱与 CI
- CI run 37312414529（commit a1b19ab）全绿：correctness 含 `-race` 与 golangci-lint、linux-integration 以 root 运行且无非预期 skip、python 3.11/3.13。root 作业实际运行并通过：`TestRealGatewayChat`、`TestRealNoCredentialsInSandbox`、`TestRealDeepResearchFixedTopic`、`TestRealDeepResearchKillAndResume`。
- **G3**：配置了非空测试 Key 时，宿主读取沙箱内每个进程的 `/proc/<pid>/environ` 与命令行、沙箱内 uid 1000 进程的 `env` 与 `/proc/self/environ`、server 日志与整张事件表，均不含 Key 值或 Key 变量名。
- **自动化业务验收（§16.4）**：checkpoint 序列为 `plan, task-1, task-2, report`；报告中每个 `[n]` 映射到 `blobs` 且授权到 `scope_blobs(task)` 的证据 sha；在 `task-1` 提交后杀死 Worker，新 attempt 以 `task-1` 恢复，计划调用计数仍为 1；恢复前后任务 1 的上游计数完全相同（1 次 summarize、1 次 search、3 次 fetch），任务 1 的每个调用只有 attempt 1 的 try，attempt 2 的 checkpoint refs 含任务 1 的 blob。

### 3. 剩余限制
- 研究循环只跑一轮（`max_loops` 已校验但只执行一次）；sub-run 并行研究属 M4。
- 有 Gateway 调用后，checkpoint state 必须是内联 JSON 对象（不能用 `state_ref`，应用不能使用 `_agentbox` 键）。
- 真实模型演示（Task 6）未执行：需要 OpenAI 兼容端点的 base URL 与模型名（Key 已在 `F:\go-agentbox\.env`）。
