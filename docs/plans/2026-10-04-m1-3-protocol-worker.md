# Plan 3：Worker 协议 v1（task 模式）与 Python Worker SDK 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 Worker 协议 v1 的 task 模式契约——Go 侧编解码、校验与事件流检查，Python 侧同规则的 SDK 与 sim-worker——并以同一套跨语言 fixtures 证明两侧一致。

**Architecture:** 协议契约以三种形式存在且互相约束：`protocol/v1/*.schema.json`（格式）、`protocol/README.md`（语义与顺序）、`protocol/fixtures/v1/`（可执行样例）。Go 的 `internal/protocol`（只依赖标准库）与 Python 的 `agentbox_worker.protocol` 各自实现同一组校验规则与错误码，并都必须通过全部 fixtures。SDK 在协议之上提供握手、事件发送、控制消息处理、checkpoint 与产物登记；`sim_worker` 用 SDK 按脚本运行，既用于 SDK 层场景回放，也供后续故障实验使用。session 与 sub-run 扩展属于后续里程碑，本计划不实现。

**Tech Stack:** Go 1.23（`go.mod`，标准库）；Python ≥ 3.11（uv 管理；pytest、jsonschema、ruff、import-linter 仅为开发依赖，运行时零依赖）；GitHub Actions。

## Global Constraints

- **规格依据**：[v0.2 规格](../design/2026-10-03-v0.2-first-release-design.md) §5.1–§5.5、§5.8、§5.9、§5.10、§5.11；[代码组织设计](../design/2026-10-03-code-organization.md) §2、§3、§9。本计划只实现 **task 模式**。
- `internal/protocol` **只依赖 Go 标准库**（代码组织设计 §3.1 规则 3），由本计划的测试自动检查。
- 同一主版本内：**未知字段忽略，未知消息类型是错误**（规格 §5.3）。
- 大小上限（规格 §5.10，UTF-8 字节）：单条 Worker 事件 1 MiB；`init` 1 MiB；其他宿主控制消息 16 KiB；inline state 256 KiB（按紧凑 JSON 计）；每个 checkpoint 的 refs 1024 个；产物路径 4096 字节。
- Python **最低版本 3.11**：不使用 3.12+ 语法；CI 必须在 3.11 上实际运行。
- `agentbox_worker` 只提供运行协议与执行能力，不含研究策略、提示词或 planner；不导入 `sim_worker`（import-linter 检查）。Worker 代码不直接创建子进程（ruff TID251 禁用 `subprocess`、`os.system`、`os.fork`、`os.popen`、`os.exec*`；测试目录除外）。
- 函数级质量约束见代码组织设计 §9；复杂度检查（Go 的 gocognit 等、Python 的 ruff C90）只报告、不阻断。
- **测试布局**（代码组织设计 §9.3）：协议契约以 `protocol/fixtures/` 数据为唯一来源，两侧回放；Go 只有 `internal/protocol/protocol_test.go`；Python 只有 `worker/tests/` 下的 `test_protocol.py`、`test_sdk.py`、`test_process.py` 与辅助模块 `protocol_fixtures.py`。后续任务向已有测试文件**末尾追加**一节（以 `# ---- <关注点> ----` 开头，节首导入本节新增的名字），不新建测试文件。只为契约规则与风险路径写测试。
- **执行环境**：Go 只在 WSL Ubuntu 中运行（`/usr/local/go/bin`）；WSL 无外网，Python 依赖装不进去，因此 **Python 工具链在 Windows 上通过 uv 运行**（uv 0.11、Python 3.13 已就绪），并由 CI 在 Linux 上以 3.11 与 3.13 复核。git 操作在 Git Bash 中进行。工作区为 CRLF：Go 格式检查针对暂存内容；编辑新文件后先 `gofmt -w` / `ruff format`。
- **执行工作区**：`git worktree add ../go-agentbox-m1-3 -b m1-3-protocol-worker m1-local-provider`。下文路径均以此为准：Windows `F:\go-agentbox-m1-3`，WSL `/mnt/f/go-agentbox-m1-3`，Git Bash `/f/go-agentbox-m1-3`。
- 提交信息结尾：`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`。只暂存本任务列出的文件，禁止 `git add -A`。
- 若 pytest 因系统临时目录权限报 `PermissionError`（受限环境中出现过），对所有 pytest 命令追加 `--basetemp=.pytest-tmp`（该目录已在 `.gitignore` 中）。

## 平台覆盖

| 测试 | Windows（uv） | Linux（WSL 或 CI） | 判定依据 |
|---|---|---|---|
| Go 编解码、事件流、fixtures | — | 必跑（WSL、CI） | Linux |
| Python 编解码、事件流、schema、fixtures | 可跑 | CI 3.11 与 3.13 必跑 | 两者都须通过 |
| SDK 运行时与场景回放（内存传输） | 可跑 | CI 必跑 | 两者都须通过 |
| 真实进程端到端（stdin/stdout、退出码、print 改道） | 可跑，仅供参考 | **CI 必跑** | **Linux** |

Unix socket、沙箱内运行不在本计划范围（Plan 2、Plan 5）。

## 子 agent 上下文包

每个实现子 agent 除本任务文本外，必须先读：

| 任务 | 必读 |
|---|---|
| 全部 | 规格 §5.1–§5.5、§5.10；代码组织设计 §2、§3、§9；本计划 Global Constraints 与"错误码与契约约定" |
| Task 1、2 | 规格 §5.3（阶段与终态）、§5.11；已有包风格参考 `internal/cgroup/cgroup.go`（错误包装与中文注释） |
| Task 3、4 | Task 1、2 产出的 `internal/protocol/*.go`（Python 侧必须与之逐条对应） |
| Task 5、6 | 规格 §5.5（checkpoint 规则）、§5.8（宿主裁决）、§5.9（暂停与取消）；Task 3 的 `agentbox_worker/protocol.py` |
| Task 7 | Task 5、6 的 SDK 全部文件；`protocol/fixtures/v1/scenarios/` 已有场景 |
| Task 8 | `.github/workflows/ci.yml` 现有内容 |

**文件所有权**：每个任务只修改其 Files 列出的文件。发现需要改动其他任务的文件时，停止并报告主 agent。

## 派发批次

任务之间有产物依赖，**不能八个任务同时开跑**。按以下批次逐批派发，前一批验收后再派发下一批：

| 批次 | 任务 | 依赖 |
|---|---|---|
| 1 | Task 1 → Task 2（顺序） | — |
| 2 | Task 3 | Task 1、2 的 fixtures 与规则 |
| 3 | Task 4 ‖ Task 5（可并行） | Task 3 |
| 4 | Task 6 | Task 5 |
| 5 | Task 7 | Task 2 的场景格式、Task 6 |
| 6 | Task 8 | Task 7 |

批次 3 若并行执行，Task 4 与 Task 5 **各用独立 worktree**（例如 `../go-agentbox-m1-3-t4`、`../go-agentbox-m1-3-t5`，均从批次 2 完成后的提交建出），由主 agent 依次合入 `m1-3-protocol-worker` 后再开始批次 4。两者不修改同一文件。

## 错误码与契约约定

**消息级错误码**（Go 与 Python 相同）：`malformed_json`、`unknown_type`、`version_mismatch`、`missing_field`、`invalid_field`、`message_too_large`、`state_too_large`、`too_many_refs`、`path_invalid`。

**事件流错误码**：`seq_invalid`、`before_ready`、`duplicate_ready`、`after_terminal`、`handshake_error_misplaced`、`after_handshake_error`。

**判定优先级**（两侧一致）：行长超过任何类型上限中的最大值（1 MiB）→ `message_too_large`，不解析（避免解析超大输入）；行不是 JSON 对象 → `malformed_json`；`type` 缺失、非字符串或不属于该方向 → `unknown_type`；超过该类型的大小上限 → `message_too_large`；字段类型错误 → `invalid_field`；然后按各类型的语义规则依次检查。

**事件流规则**（task 模式）：`seq` 从 1 严格递增；`ready` 之前只允许 `error`（启动失败）；`ready` 只能出现一次；终态提议（`result`、`error`、`paused`）至多一个，其后只允许 `checkpoint_query`；`handshake_error` 只能是第一条且是唯一一条 Worker 消息。

**SDK 错误码**（`error` 事件，`retryable` 是给宿主的建议）：

| code | 含义 | retryable |
|---|---|---|
| `checkpoint_conflict` | 宿主对 checkpoint 返回 `conflict` | false |
| `checkpoint_rejected` | 宿主返回 `rejected` | false |
| `checkpoint_unresolved` | 重试与查询达到上限仍无结果 | true |
| `artifact_rejected` | 宿主拒绝产物 | false |
| `artifact_unresolved` | 等待产物结果超时 | true |
| `unsupported_mode` | init 的 mode 不是 task（`ready` 之前发出） | false |
| `control_protocol_error` | 收到不合法的控制消息 | false |
| `internal_error` | 应用抛出未预期异常 | true |
| 应用自定义 | `WorkerFailure(code, ...)` | 由应用决定 |

**SDK 退出码**：0 = 已发出 `result`/`paused`，或被宿主取消（取消不发终态提议，由宿主按其意图裁决，规格 §5.8）；1 = 已发出 `error`、无法开始，或协议输出通道失效；2 = `handshake_error`。stdin 关闭视为取消。

**发送与退出的生命周期**：

- **seq 的提交点**是"校验通过、开始发送"：此后无论发送成功、失败还是被取消，该 seq 都已用掉。发送失败或在途被取消时结果不确定，输出通道进入失效状态（`TransportBroken`），此后不再发送任何事件，绝不以同一 seq 重发；Worker 以 1 退出且无法再发出 `error`。
- **输入**：读取阶段即按帧长上限（init 上限 1 MiB + 行尾）截断判定，超长帧报 `message_too_large` 并停止读取；读到的行进入线程安全的有界队列（默认 64 条），队列满时读线程阻塞，对宿主形成背压。读线程绑定首次 `receive` 所在的事件循环，该循环关闭后不再读取输入并退出。输入缓冲约为 (64 + 1) 帧：队列中的 64 帧，加上读线程正在读取或等待入队的一帧，另有对象与底层文件缓冲的开销；这是估算，**不是**总内存的硬上限（不能写成"最多 64 MiB"）。
- **进程入口** `main()` 是 SDK 中唯一调用 `os._exit` 的地方：运行 Worker（任何异常都记录并按失败处理）→ 在期限内等待已提交的输出写完 → `os._exit`。期限由环境变量 `AGENTBOX_WORKER_SHUTDOWN_TIMEOUT` 设置（秒，默认 5）。读写线程可能阻塞在 stdin 或 stdout 上且无法取消，正常的解释器收尾会挂起或崩溃（计划演练中实测复现 `Fatal Python error: _enter_buffered_busy`）。
- **该期限只约束协议输出的收尾，不是整个 Worker 的退出期限。** `register_artifact` 在线程池中计算哈希，不能保证有限时间内完成（路径是 FIFO 或读操作阻塞时线程会一直等待且无法取消）；`asyncio.run` 返回前会等待线程池中的线程（演练中在 3.11 与 3.13 上实测），这段等待发生在进入输出收尾之前，不受该期限约束。**最终强制终止由宿主负责。**
- **checkpoint 快照**：`state` 在首次提交前经 JSON 往返生成快照，所有重试复用它。往返会归一化：tuple 变为 list，非字符串键变为字符串；NaN 与 Infinity 被拒绝（`invalid_field`）。

---

### Task 1：Go 消息类型、编解码与校验；消息 fixtures

**风险：高（协议契约）——规格符合性 + 代码质量双评审。**

**Files:**
- Create: `internal/protocol/doc.go`、`limits.go`、`errors.go`、`messages.go`、`validate.go`、`codec.go`
- Create: `internal/protocol/protocol_test.go`（本包唯一的测试文件；Task 2 在末尾追加场景回放）
- Create: `protocol/fixtures/v1/messages.json`
- Create: `scripts/dev/gofmt-staged.sh`

**Interfaces:**
- Consumes: 无。
- Produces（后续任务与计划依赖）：
  - `type Direction int`；`HostToWorker`、`WorkerToHost`
  - `func DecodeLine(dir Direction, line []byte) (Message, error)`；`func EncodeLine(dir Direction, m Message) ([]byte, error)`
  - `type Message interface { MessageType() string; declaredType() string; validate() error }`
  - 消息类型：`*Init`、`*Resume`、`*CheckpointResult`、`*ArtifactResult`、`*Cancel`、`*Pause`、`*Ready`、`*Progress`、`*Artifact`、`*Checkpoint`、`*CheckpointQuery`、`*Paused`、`*Result`、`*ErrorEvent`、`*HandshakeError`；公共头 `HostHeader{Type, V}`、`EventHeader{Type, V, Seq, TS}`、`WorkerInfo{Name, Version}`
  - `type Error struct{ Code, Detail string }`；`func CodeOf(err error) string`；`Code*` 常量
  - 常量 `Version = 1`、`BootstrapVersion = 1`、`Max*`、`Type*`、`Mode*`、`Scope*`、`Visibility*`、`Checkpoint*`、`Artifact*`

- [ ] **Step 1：写入消息 fixtures**

`protocol/fixtures/v1/messages.json`：

```json
{
  "valid": [
    {"name": "init_minimal", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": []}}},
    {"name": "init_resume_inline_state", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-2", "attempt_no": 2, "out_dir": "/workspace/out/a-2", "resume": {"checkpoint_id": "cp-1", "step_id": "s1", "state": {"next_index": 2}}}},
    {"name": "init_resume_state_ref", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-2", "attempt_no": 2, "out_dir": "/workspace/out/a-2", "input_refs": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"], "resume": {"checkpoint_id": "cp-1", "step_id": "s1", "state_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "refs": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}}},
    {"name": "init_unknown_field_ignored", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "future_field": {"x": 1}}},
    {"name": "init_session_mode_envelope", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "session", "session_id": "s-1"}},
    {"name": "checkpoint_result_committed", "direction": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"name": "checkpoint_result_rejected_with_code", "direction": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "rejected", "code": "missing_ref"}},
    {"name": "checkpoint_result_not_found", "direction": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "not_found"}},
    {"name": "artifact_result_saved", "direction": "host", "message": {"type": "artifact_result", "v": 1, "artifact_id": "report", "status": "saved", "version": 1, "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
    {"name": "artifact_result_rejected", "direction": "host", "message": {"type": "artifact_result", "v": 1, "artifact_id": "report", "status": "rejected", "code": "path_invalid"}},
    {"name": "cancel", "direction": "host", "message": {"type": "cancel", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 5000}},
    {"name": "pause", "direction": "host", "message": {"type": "pause", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 5000}},
    {"name": "ready", "direction": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"name": "progress", "direction": "worker", "message": {"type": "progress", "v": 1, "seq": 2, "step_id": "s1", "kind": "step_started", "message": "开始"}},
    {"name": "progress_with_data_and_ts", "direction": "worker", "message": {"type": "progress", "v": 1, "seq": 2, "ts": "2026-10-04T00:00:00.000Z", "kind": "tool_error", "message": "搜索超时", "data": {"count": 3}}},
    {"name": "artifact", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 3, "artifact_id": "report", "path": "report.md", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 12, "media_type": "text/markdown", "visibility": "output"}},
    {"name": "artifact_nested_path", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 3, "artifact_id": "data", "path": "sub/dir/data.json", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 0, "media_type": "application/json", "visibility": "internal"}},
    {"name": "artifact_hidden_segment", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 3, "artifact_id": "cache", "path": ".cache/x", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "application/octet-stream", "visibility": "internal"}},
    {"name": "checkpoint_inline_state", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 4, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"name": "checkpoint_state_ref", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 4, "checkpoint_id": "cp-2", "scope": "task", "step_id": "s2", "state_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "refs": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]}},
    {"name": "checkpoint_query", "direction": "worker", "message": {"type": "checkpoint_query", "v": 1, "seq": 5, "checkpoint_id": "cp-1", "scope": "task"}},
    {"name": "paused", "direction": "worker", "message": {"type": "paused", "v": 1, "seq": 6, "checkpoint_id": "cp-1"}},
    {"name": "result", "direction": "worker", "message": {"type": "result", "v": 1, "seq": 7, "summary": "完成", "outputs": ["report"]}},
    {"name": "error", "direction": "worker", "message": {"type": "error", "v": 1, "seq": 2, "code": "internal_error", "message": "boom", "retryable": true}},
    {"name": "handshake_error", "direction": "worker", "message": {"type": "handshake_error", "bootstrap": 1, "code": "no_common_version"}}
  ],
  "invalid": [
    {"name": "malformed_json_truncated", "direction": "worker", "raw": "{\"type\":\"ready\"", "code": "malformed_json"},
    {"name": "not_an_object", "direction": "worker", "raw": "[1,2]", "code": "malformed_json"},
    {"name": "null_line", "direction": "host", "raw": "null", "code": "malformed_json"},
    {"name": "unknown_type_worker", "direction": "worker", "message": {"type": "telemetry", "v": 1, "seq": 1}, "code": "unknown_type"},
    {"name": "missing_type", "direction": "worker", "message": {"v": 1, "seq": 1}, "code": "unknown_type"},
    {"name": "type_not_string", "direction": "worker", "message": {"type": 5, "v": 1, "seq": 1}, "code": "unknown_type"},
    {"name": "host_type_sent_by_worker", "direction": "worker", "message": {"type": "cancel", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 0}, "code": "unknown_type"},
    {"name": "worker_type_sent_by_host", "direction": "host", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "w", "version": "1"}}, "code": "unknown_type"},
    {"name": "event_version_mismatch", "direction": "worker", "message": {"type": "ready", "v": 2, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "w", "version": "1"}}, "code": "version_mismatch"},
    {"name": "ready_protocol_version_mismatch", "direction": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 2, "mode": "task", "worker": {"name": "w", "version": "1"}}, "code": "version_mismatch"},
    {"name": "seq_zero", "direction": "worker", "message": {"type": "progress", "v": 1, "seq": 0, "kind": "step_started", "message": "x"}, "code": "invalid_field"},
    {"name": "seq_wrong_type", "direction": "worker", "message": {"type": "progress", "v": 1, "seq": "1", "kind": "step_started", "message": "x"}, "code": "invalid_field"},
    {"name": "init_bad_bootstrap", "direction": "host", "message": {"type": "init", "bootstrap": 2, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/o"}, "code": "invalid_field"},
    {"name": "init_empty_protocol_versions", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/o"}, "code": "missing_field"},
    {"name": "init_bad_mode", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "batch", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/o"}, "code": "invalid_field"},
    {"name": "init_missing_task_id", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/o"}, "code": "missing_field"},
    {"name": "init_attempt_no_zero", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 0, "out_dir": "/o"}, "code": "invalid_field"},
    {"name": "init_resume_state_and_state_ref", "direction": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-2", "attempt_no": 2, "out_dir": "/o", "resume": {"checkpoint_id": "cp-1", "step_id": "s1", "state": {}, "state_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, "code": "invalid_field"},
    {"name": "checkpoint_neither_state_nor_ref", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "refs": []}, "code": "invalid_field"},
    {"name": "checkpoint_both_state_and_ref", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {}, "state_ref": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, "code": "invalid_field"},
    {"name": "checkpoint_session_scope", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "session", "step_id": "s1", "state": {}}, "code": "invalid_field"},
    {"name": "checkpoint_bad_ref", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {}, "refs": ["xyz"]}, "code": "invalid_field"},
    {"name": "checkpoint_missing_id", "direction": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "scope": "task", "step_id": "s1", "state": {}}, "code": "missing_field"},
    {"name": "artifact_path_absolute", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "/etc/passwd", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_path_dotdot", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "../in/x", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_path_dot", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "./x", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_path_empty_segment", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "a//b", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_path_trailing_slash", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "a/", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_path_nul", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "a\u0000b", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "path_invalid"},
    {"name": "artifact_uppercase_sha", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "x", "declared_sha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "declared_size": 1, "media_type": "text/plain", "visibility": "output"}, "code": "invalid_field"},
    {"name": "artifact_bad_visibility", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "x", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/plain", "visibility": "public"}, "code": "invalid_field"},
    {"name": "artifact_negative_size", "direction": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "x", "path": "x", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": -1, "media_type": "text/plain", "visibility": "output"}, "code": "invalid_field"},
    {"name": "checkpoint_result_bad_status", "direction": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "ok"}, "code": "invalid_field"},
    {"name": "artifact_result_saved_without_sha", "direction": "host", "message": {"type": "artifact_result", "v": 1, "artifact_id": "report", "status": "saved", "version": 1}, "code": "invalid_field"},
    {"name": "artifact_result_rejected_without_code", "direction": "host", "message": {"type": "artifact_result", "v": 1, "artifact_id": "report", "status": "rejected"}, "code": "missing_field"},
    {"name": "cancel_negative_grace", "direction": "host", "message": {"type": "cancel", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": -1}, "code": "invalid_field"},
    {"name": "error_missing_code", "direction": "worker", "message": {"type": "error", "v": 1, "seq": 2, "message": "boom", "retryable": false}, "code": "missing_field"},
    {"name": "error_retryable_not_bool", "direction": "worker", "message": {"type": "error", "v": 1, "seq": 2, "code": "x", "message": "boom", "retryable": "yes"}, "code": "invalid_field"},
    {"name": "handshake_error_bad_bootstrap", "direction": "worker", "message": {"type": "handshake_error", "bootstrap": 2, "code": "no_common_version"}, "code": "invalid_field"}
  ]
}
```

- [ ] **Step 2：写失败的测试**

`internal/protocol/protocol_test.go`：

```go
package protocol

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// fixtureDir 是跨语言 fixtures 的位置；Python 侧使用同一份文件。
const fixtureDir = "../../protocol/fixtures/v1"

type messageCase struct {
	Name      string          `json:"name"`
	Direction string          `json:"direction"`
	Message   json.RawMessage `json:"message"`
	Raw       string          `json:"raw"`
	Code      string          `json:"code"`
}

func (c messageCase) line() []byte {
	if c.Raw != "" {
		return []byte(c.Raw)
	}
	return c.Message
}

func loadJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("解析 %s: %v", path, err)
	}
}

func parseDirection(t *testing.T, s string) Direction {
	t.Helper()
	switch s {
	case "host":
		return HostToWorker
	case "worker":
		return WorkerToHost
	}
	t.Fatalf("未知方向 %q", s)
	return 0
}

func TestMessageFixtures(t *testing.T) {
	var f struct {
		Valid   []messageCase `json:"valid"`
		Invalid []messageCase `json:"invalid"`
	}
	loadJSON(t, filepath.Join(fixtureDir, "messages.json"), &f)
	if len(f.Valid) == 0 || len(f.Invalid) == 0 {
		t.Fatal("消息 fixtures 为空")
	}

	for _, c := range f.Valid {
		t.Run("valid/"+c.Name, func(t *testing.T) {
			dir := parseDirection(t, c.Direction)
			m, err := DecodeLine(dir, c.line())
			if err != nil {
				t.Fatalf("应当合法：%v", err)
			}
			b, err := EncodeLine(dir, m)
			if err != nil {
				t.Fatalf("重新编码：%v", err)
			}
			if _, err := DecodeLine(dir, b); err != nil {
				t.Fatalf("往返后解码：%v", err)
			}
		})
	}
	for _, c := range f.Invalid {
		t.Run("invalid/"+c.Name, func(t *testing.T) {
			_, err := DecodeLine(parseDirection(t, c.Direction), c.line())
			if got := CodeOf(err); got != c.Code {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", got, c.Code, err)
			}
		})
	}
}

func checkpointLine(state string, refs []string) []byte {
	r, _ := json.Marshal(refs)
	return []byte(`{"type":"checkpoint","v":1,"seq":1,"checkpoint_id":"cp-1","scope":"task","step_id":"s1","state":` + state + `,"refs":` + string(r) + `}`)
}

// TestDecodeLimits 覆盖 §5.10 的大小上限：fixtures 不适合存放这些按常量生成的大消息。
func TestDecodeLimits(t *testing.T) {
	ref := strings.Repeat("a", 64)
	refs := make([]string, MaxRefsPerCheckpoint)
	for i := range refs {
		refs[i] = ref
	}
	cases := []struct {
		name string
		dir  Direction
		line string
		want string // 空表示应当合法
	}{
		{"超长事件", WorkerToHost, `{"type":"progress","v":1,"seq":1,"kind":"x","message":"` + strings.Repeat("a", MaxEventBytes) + `"}`, CodeMessageTooLarge},
		{"非 init 控制消息按 16 KiB 计", HostToWorker, `{"type":"cancel","v":1,"attempt_id":"a-1","grace_ms":0,"reason":"` + strings.Repeat("a", MaxControlBytes) + `"}`, CodeMessageTooLarge},
		{"init 上限是 1 MiB", HostToWorker, `{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"task","task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o","config":"` + strings.Repeat("a", 100<<10) + `"}`, ""},
		{"state 恰好到上限", WorkerToHost, string(checkpointLine(`"`+strings.Repeat("a", MaxInlineStateBytes-2)+`"`, []string{})), ""},
		{"state 超过上限", WorkerToHost, string(checkpointLine(`"`+strings.Repeat("a", MaxInlineStateBytes-1)+`"`, []string{})), CodeStateTooLarge},
		{"state 的空白不计入", WorkerToHost, string(checkpointLine(`{ "k" :  "aaaaaaaaaa" }`, []string{})), ""},
		{"refs 恰好到上限", WorkerToHost, string(checkpointLine(`{}`, refs)), ""},
		{"refs 超过上限", WorkerToHost, string(checkpointLine(`{}`, append(refs, ref))), CodeTooManyRefs},
		{"超过所有上限的行不解析", HostToWorker, strings.Repeat("x", MaxInitBytes+1), CodeMessageTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeLine(c.dir, []byte(c.line)); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}
}

func TestEncodeLine(t *testing.T) {
	cp := &Checkpoint{
		EventHeader:  EventHeader{Type: TypeCheckpoint, V: Version, Seq: 3},
		CheckpointID: "cp-1",
		Scope:        ScopeTask,
		StepID:       "s1",
		State:        json.RawMessage(`{"next_index":1}`),
		Refs:         []string{},
	}
	b, err := EncodeLine(WorkerToHost, cp)
	if err != nil {
		t.Fatalf("EncodeLine: %v", err)
	}
	got, err := DecodeLine(WorkerToHost, b)
	if back, ok := got.(*Checkpoint); err != nil || !ok || back.Seq != 3 || string(back.State) != `{"next_index":1}` {
		t.Fatalf("往返结果不一致：%#v（err = %v）", got, err)
	}

	rejects := []struct {
		name string
		dir  Direction
		m    Message
		want string
	}{
		{"Worker 事件按宿主方向编码", HostToWorker, &Paused{EventHeader: EventHeader{Type: TypePaused, V: Version, Seq: 1}, CheckpointID: "cp-1"}, CodeUnknownType},
		{"type 字段与消息类型不一致", WorkerToHost, &Paused{EventHeader: EventHeader{Type: TypeResult, V: Version, Seq: 1}, CheckpointID: "cp-1"}, CodeInvalidField},
		{"缺少 state 与 state_ref", WorkerToHost, &Checkpoint{EventHeader: EventHeader{Type: TypeCheckpoint, V: Version, Seq: 1}, CheckpointID: "cp-1", Scope: ScopeTask, StepID: "s1"}, CodeInvalidField},
	}
	for _, c := range rejects {
		t.Run(c.name, func(t *testing.T) {
			if _, err := EncodeLine(c.dir, c.m); CodeOf(err) != c.want {
				t.Fatalf("错误码 = %q，期望 %q（err = %v）", CodeOf(err), c.want, err)
			}
		})
	}
}

// TestImportsOnlyStandardLibrary 保证本包只依赖标准库（代码组织设计 §3.1 规则 3）：
// 标准库导入路径的第一段不含 "."。
func TestImportsOnlyStandardLibrary(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("读取目录: %v", err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", e.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("解析 %s: %v", e.Name(), err)
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: %v", e.Name(), err)
			}
			if first := strings.SplitN(path, "/", 2)[0]; strings.Contains(first, ".") {
				t.Errorf("%s 导入了非标准库包 %s", e.Name(), path)
			}
		}
	}
}
```

- [ ] **Step 3：确认测试失败**

```powershell
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && go test -count=1 ./internal/protocol/'
```

Expected: 编译失败，报告 `undefined: DecodeLine` 等。

- [ ] **Step 4：实现**

`internal/protocol/doc.go`：

```go
// Package protocol 实现 Worker 协议 v1（task 模式）的消息类型、编解码、校验与事件流顺序检查。
//
// 规则来源：v0.2 规格 §5；Python 侧的 agentbox_worker.protocol 实现同一组规则，
// 二者共用 protocol/fixtures/v1 下的 fixtures。本包只依赖标准库。
package protocol
```

`internal/protocol/limits.go`：

```go
package protocol

// 协议版本（规格 §5.2、§5.3）。
const (
	// Version 是本包实现的协议主版本。
	Version = 1
	// BootstrapVersion 是 init 引导信封的版本，永不改变。
	BootstrapVersion = 1
)

// 大小上限（规格 §5.10，按 UTF-8 字节计）。
const (
	MaxEventBytes        = 1 << 20
	MaxInitBytes         = 1 << 20
	MaxControlBytes      = 16 << 10
	MaxInlineStateBytes  = 256 << 10
	MaxRefsPerCheckpoint = 1024
	MaxArtifactPathBytes = 4096
)

// maxLineBytes 是任何消息类型上限中的最大值；超过它的行不解析，直接判为过大。
const maxLineBytes = max(MaxEventBytes, MaxInitBytes)
```

`internal/protocol/errors.go`：

```go
package protocol

import (
	"errors"
	"fmt"
)

// 消息级错误码。
const (
	CodeMalformedJSON   = "malformed_json"
	CodeUnknownType     = "unknown_type"
	CodeVersionMismatch = "version_mismatch"
	CodeMissingField    = "missing_field"
	CodeInvalidField    = "invalid_field"
	CodeMessageTooLarge = "message_too_large"
	CodeStateTooLarge   = "state_too_large"
	CodeTooManyRefs     = "too_many_refs"
	CodePathInvalid     = "path_invalid"
)

// Error 是带稳定错误码的协议错误。
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func newError(code, format string, args ...any) error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf 返回 err 的协议错误码；err 不是 *Error 时返回空串。
func CodeOf(err error) string {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}
```

`internal/protocol/messages.go`：

```go
package protocol

import "encoding/json"

// 消息类型（规格 §5.4，task 模式）。
const (
	TypeInit             = "init"
	TypeCheckpointResult = "checkpoint_result"
	TypeArtifactResult   = "artifact_result"
	TypeCancel           = "cancel"
	TypePause            = "pause"

	TypeReady           = "ready"
	TypeProgress        = "progress"
	TypeArtifact        = "artifact"
	TypeCheckpoint      = "checkpoint"
	TypeCheckpointQuery = "checkpoint_query"
	TypePaused          = "paused"
	TypeResult          = "result"
	TypeError           = "error"
	TypeHandshakeError  = "handshake_error"
)

// 枚举取值。
const (
	ModeTask    = "task"
	ModeSession = "session"

	ScopeTask    = "task"
	ScopeSession = "session"

	VisibilityOutput   = "output"
	VisibilityInternal = "internal"

	CheckpointCommitted      = "committed"
	CheckpointConflict       = "conflict"
	CheckpointRejected       = "rejected"
	CheckpointRetryableError = "retryable_error"
	CheckpointNotFound       = "not_found"

	ArtifactSaved    = "saved"
	ArtifactRejected = "rejected"
)

// Message 是一条协议消息。
type Message interface {
	// MessageType 返回该类型消息的 type 取值。
	MessageType() string
	declaredType() string
	validate() error
}

// event 是带 seq 的 Worker 事件（handshake_error 除外）。
type event interface {
	Message
	header() EventHeader
}

// HostHeader 是宿主控制消息（init 除外）的公共字段。
type HostHeader struct {
	Type string `json:"type"`
	V    int    `json:"v"`
}

func (h HostHeader) declaredType() string { return h.Type }

// EventHeader 是 Worker 事件的公共字段。ts 仅用于诊断（规格 §5.3）。
type EventHeader struct {
	Type string `json:"type"`
	V    int    `json:"v"`
	Seq  int64  `json:"seq"`
	TS   string `json:"ts,omitempty"`
}

func (h EventHeader) declaredType() string { return h.Type }
func (h EventHeader) header() EventHeader  { return h }

// Init 是宿主发给 Worker 的第一条消息。type、bootstrap、protocol_versions
// 组成引导信封，永不改变（规格 §5.2）；init 本身不带 v。
type Init struct {
	Type             string          `json:"type"`
	Bootstrap        int             `json:"bootstrap"`
	ProtocolVersions []int           `json:"protocol_versions"`
	Mode             string          `json:"mode"`
	TaskID           string          `json:"task_id"`
	AttemptID        string          `json:"attempt_id"`
	AttemptNo        int             `json:"attempt_no"`
	Traceparent      string          `json:"traceparent,omitempty"`
	Config           json.RawMessage `json:"config,omitempty"`
	ConfigVersion    string          `json:"config_version,omitempty"`
	BudgetLimits     json.RawMessage `json:"budget_limits,omitempty"`
	InputRefs        []string        `json:"input_refs,omitempty"`
	OutDir           string          `json:"out_dir"`
	Resume           *Resume         `json:"resume,omitempty"`
}

// Resume 是恢复时随 init 下发的最近已提交 checkpoint。
type Resume struct {
	CheckpointID string          `json:"checkpoint_id"`
	StepID       string          `json:"step_id"`
	State        json.RawMessage `json:"state,omitempty"`
	StateRef     string          `json:"state_ref,omitempty"`
	Refs         []string        `json:"refs,omitempty"`
}

// CheckpointResult 是宿主对 checkpoint 或 checkpoint_query 的答复。
type CheckpointResult struct {
	HostHeader
	CheckpointID string `json:"checkpoint_id"`
	Scope        string `json:"scope"`
	Status       string `json:"status"`
	Code         string `json:"code,omitempty"`
}

// ArtifactResult 是宿主对产物登记的答复。
type ArtifactResult struct {
	HostHeader
	ArtifactID string `json:"artifact_id"`
	Status     string `json:"status"`
	Version    int    `json:"version,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Code       string `json:"code,omitempty"`
}

// Cancel 要求 Worker 在 grace_ms 内停止。
type Cancel struct {
	HostHeader
	AttemptID string `json:"attempt_id"`
	Reason    string `json:"reason"`
	GraceMS   int64  `json:"grace_ms"`
}

// Pause 要求 Worker 在下一个提交边界暂停（协作式，规格 §5.9）。
type Pause struct {
	HostHeader
	AttemptID string `json:"attempt_id"`
	Reason    string `json:"reason"`
	GraceMS   int64  `json:"grace_ms"`
}

// WorkerInfo 标识 Worker 实现。
type WorkerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Ready 是 Worker 完成握手后的第一条事件。
type Ready struct {
	EventHeader
	ProtocolVersion int        `json:"protocol_version"`
	Mode            string     `json:"mode"`
	Worker          WorkerInfo `json:"worker"`
	Capabilities    []string   `json:"capabilities"`
}

// Progress 是业务进度；可恢复的工具失败以 kind=tool_error 报告。
type Progress struct {
	EventHeader
	StepID  string          `json:"step_id,omitempty"`
	Kind    string          `json:"kind"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Artifact 登记 out_dir 下的一个产物；声明的哈希与大小由宿主验证。
type Artifact struct {
	EventHeader
	ArtifactID     string `json:"artifact_id"`
	Path           string `json:"path"`
	DeclaredSHA256 string `json:"declared_sha256"`
	DeclaredSize   int64  `json:"declared_size"`
	MediaType      string `json:"media_type"`
	Visibility     string `json:"visibility"`
}

// Checkpoint 提议提交一次进度；是否成立由宿主决定（规格 §5.5）。
type Checkpoint struct {
	EventHeader
	CheckpointID string          `json:"checkpoint_id"`
	Scope        string          `json:"scope"`
	StepID       string          `json:"step_id"`
	State        json.RawMessage `json:"state,omitempty"`
	StateRef     string          `json:"state_ref,omitempty"`
	Refs         []string        `json:"refs,omitempty"`
}

// CheckpointQuery 查询某个 checkpoint 的提交结果。
type CheckpointQuery struct {
	EventHeader
	CheckpointID string `json:"checkpoint_id"`
	Scope        string `json:"scope"`
}

// Paused 是暂停的终态提议，checkpoint_id 必须是最新已提交的 checkpoint。
type Paused struct {
	EventHeader
	CheckpointID string `json:"checkpoint_id"`
}

// Result 是成功的终态提议。
type Result struct {
	EventHeader
	Summary string   `json:"summary"`
	Outputs []string `json:"outputs"`
}

// ErrorEvent 是失败的终态提议；retryable 只是给宿主的建议。
type ErrorEvent struct {
	EventHeader
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// HandshakeError 在没有共同协议版本时发出，格式固定（规格 §5.2）。
type HandshakeError struct {
	Type      string `json:"type"`
	Bootstrap int    `json:"bootstrap"`
	Code      string `json:"code"`
}

func (*Init) MessageType() string              { return TypeInit }
func (m *Init) declaredType() string           { return m.Type }
func (*CheckpointResult) MessageType() string  { return TypeCheckpointResult }
func (*ArtifactResult) MessageType() string    { return TypeArtifactResult }
func (*Cancel) MessageType() string            { return TypeCancel }
func (*Pause) MessageType() string             { return TypePause }
func (*Ready) MessageType() string             { return TypeReady }
func (*Progress) MessageType() string          { return TypeProgress }
func (*Artifact) MessageType() string          { return TypeArtifact }
func (*Checkpoint) MessageType() string        { return TypeCheckpoint }
func (*CheckpointQuery) MessageType() string   { return TypeCheckpointQuery }
func (*Paused) MessageType() string            { return TypePaused }
func (*Result) MessageType() string            { return TypeResult }
func (*ErrorEvent) MessageType() string        { return TypeError }
func (*HandshakeError) MessageType() string    { return TypeHandshakeError }
func (m *HandshakeError) declaredType() string { return m.Type }
```

`internal/protocol/validate.go`：

```go
package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
)

func (m *Init) validate() error {
	if m.Bootstrap != BootstrapVersion {
		return newError(CodeInvalidField, "bootstrap=%d，期望 %d", m.Bootstrap, BootstrapVersion)
	}
	if len(m.ProtocolVersions) == 0 {
		return newError(CodeMissingField, "protocol_versions 不能为空")
	}
	if err := oneOf("mode", m.Mode, ModeTask, ModeSession); err != nil {
		return err
	}
	if m.Mode != ModeTask {
		return nil // session 扩展的字段由后续里程碑校验
	}
	if err := firstErr(required("task_id", m.TaskID), required("attempt_id", m.AttemptID), required("out_dir", m.OutDir)); err != nil {
		return err
	}
	if m.AttemptNo < 1 {
		return newError(CodeInvalidField, "attempt_no=%d，必须 ≥ 1", m.AttemptNo)
	}
	if err := checkRefs("input_refs", m.InputRefs, -1); err != nil {
		return err
	}
	if m.Resume == nil {
		return nil
	}
	return m.Resume.validate()
}

func (r *Resume) validate() error {
	if err := firstErr(required("resume.checkpoint_id", r.CheckpointID), required("resume.step_id", r.StepID)); err != nil {
		return err
	}
	if err := checkState(r.State, r.StateRef); err != nil {
		return err
	}
	return checkRefs("resume.refs", r.Refs, MaxRefsPerCheckpoint)
}

func (m *CheckpointResult) validate() error {
	return firstErr(
		checkVersion(m.V),
		required("checkpoint_id", m.CheckpointID),
		oneOf("scope", m.Scope, ScopeTask, ScopeSession),
		oneOf("status", m.Status, CheckpointCommitted, CheckpointConflict, CheckpointRejected, CheckpointRetryableError, CheckpointNotFound),
	)
}

func (m *ArtifactResult) validate() error {
	if err := firstErr(checkVersion(m.V), required("artifact_id", m.ArtifactID), oneOf("status", m.Status, ArtifactSaved, ArtifactRejected)); err != nil {
		return err
	}
	if m.Status == ArtifactRejected {
		return required("code", m.Code)
	}
	if m.Version < 1 {
		return newError(CodeInvalidField, "saved 的 version=%d，必须 ≥ 1", m.Version)
	}
	if !validSHA256(m.SHA256) {
		return newError(CodeInvalidField, "saved 的 sha256 不合法")
	}
	return nil
}

func (m *Cancel) validate() error { return validateStop(m.V, m.AttemptID, m.GraceMS) }
func (m *Pause) validate() error  { return validateStop(m.V, m.AttemptID, m.GraceMS) }

func validateStop(v int, attemptID string, graceMS int64) error {
	if err := firstErr(checkVersion(v), required("attempt_id", attemptID)); err != nil {
		return err
	}
	if graceMS < 0 {
		return newError(CodeInvalidField, "grace_ms=%d，必须 ≥ 0", graceMS)
	}
	return nil
}

func (m *Ready) validate() error {
	if err := checkEvent(m.EventHeader); err != nil {
		return err
	}
	if m.ProtocolVersion != Version {
		return newError(CodeVersionMismatch, "protocol_version=%d，期望 %d", m.ProtocolVersion, Version)
	}
	return firstErr(oneOf("mode", m.Mode, ModeTask, ModeSession), required("worker.name", m.Worker.Name))
}

func (m *Progress) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("kind", m.Kind))
}

func (m *Artifact) validate() error {
	if err := firstErr(checkEvent(m.EventHeader), required("artifact_id", m.ArtifactID)); err != nil {
		return err
	}
	if !validArtifactPath(m.Path) {
		return newError(CodePathInvalid, "产物路径 %q 必须是相对路径，且不含空、. 或 .. 分量", m.Path)
	}
	if !validSHA256(m.DeclaredSHA256) {
		return newError(CodeInvalidField, "declared_sha256 不合法")
	}
	if m.DeclaredSize < 0 {
		return newError(CodeInvalidField, "declared_size=%d，必须 ≥ 0", m.DeclaredSize)
	}
	return firstErr(required("media_type", m.MediaType), oneOf("visibility", m.Visibility, VisibilityOutput, VisibilityInternal))
}

func (m *Checkpoint) validate() error {
	if err := firstErr(
		checkEvent(m.EventHeader),
		required("checkpoint_id", m.CheckpointID),
		required("step_id", m.StepID),
		oneOf("scope", m.Scope, ScopeTask), // session scope 只能经 result 提议写入（规格 §5.5 第 7 条）
	); err != nil {
		return err
	}
	if err := checkState(m.State, m.StateRef); err != nil {
		return err
	}
	return checkRefs("refs", m.Refs, MaxRefsPerCheckpoint)
}

func (m *CheckpointQuery) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("checkpoint_id", m.CheckpointID), oneOf("scope", m.Scope, ScopeTask, ScopeSession))
}

func (m *Paused) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("checkpoint_id", m.CheckpointID))
}

func (m *Result) validate() error {
	if err := checkEvent(m.EventHeader); err != nil {
		return err
	}
	for _, o := range m.Outputs {
		if o == "" {
			return newError(CodeInvalidField, "outputs 中不能有空的 artifact_id")
		}
	}
	return nil
}

func (m *ErrorEvent) validate() error {
	return firstErr(checkEvent(m.EventHeader), required("code", m.Code))
}

func (m *HandshakeError) validate() error {
	if m.Bootstrap != BootstrapVersion {
		return newError(CodeInvalidField, "bootstrap=%d，期望 %d", m.Bootstrap, BootstrapVersion)
	}
	return required("code", m.Code)
}

func checkVersion(v int) error {
	if v != Version {
		return newError(CodeVersionMismatch, "v=%d，期望 %d", v, Version)
	}
	return nil
}

func checkEvent(h EventHeader) error {
	if err := checkVersion(h.V); err != nil {
		return err
	}
	if h.Seq < 1 {
		return newError(CodeInvalidField, "seq=%d，必须 ≥ 1", h.Seq)
	}
	return nil
}

// checkState 检查 state 与 state_ref 必须且只能提供一个；inline state 按紧凑 JSON 计大小。
func checkState(state json.RawMessage, stateRef string) error {
	hasState, hasRef := hasJSONValue(state), stateRef != ""
	if hasState == hasRef {
		return newError(CodeInvalidField, "state 与 state_ref 必须且只能提供一个")
	}
	if hasRef {
		if !validSHA256(stateRef) {
			return newError(CodeInvalidField, "state_ref 不是合法的 sha256")
		}
		return nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, state); err != nil {
		return newError(CodeMalformedJSON, "state: %v", err)
	}
	if compact.Len() > MaxInlineStateBytes {
		return newError(CodeStateTooLarge, "inline state %d 字节，上限 %d", compact.Len(), MaxInlineStateBytes)
	}
	return nil
}

// checkRefs 检查 refs 中每个元素都是 sha256；limit < 0 表示不限个数。
func checkRefs(field string, refs []string, limit int) error {
	if limit >= 0 && len(refs) > limit {
		return newError(CodeTooManyRefs, "%s 有 %d 个，上限 %d", field, len(refs), limit)
	}
	for _, r := range refs {
		if !validSHA256(r) {
			return newError(CodeInvalidField, "%s 中有不合法的 sha256", field)
		}
	}
	return nil
}

func hasJSONValue(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) > 0 && !bytes.Equal(t, []byte("null"))
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// validArtifactPath 检查产物路径：相对、非空、无 NUL、不超过上限、不含空、. 或 .. 分量（规格 §5.6）。
func validArtifactPath(p string) bool {
	if p == "" || len(p) > MaxArtifactPathBytes || strings.ContainsRune(p, 0) || strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func required(field, value string) error {
	if value == "" {
		return newError(CodeMissingField, "%s 不能为空", field)
	}
	return nil
}

// oneOf 检查枚举字段：空值是 missing_field，不在允许范围内是 invalid_field。
func oneOf(field, value string, allowed ...string) error {
	if value == "" {
		return newError(CodeMissingField, "%s 不能为空", field)
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return newError(CodeInvalidField, "%s=%q 不在允许范围 %v 内", field, value, allowed)
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
```

`internal/protocol/codec.go`：

```go
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Direction 是消息的传输方向。
type Direction int

const (
	// HostToWorker 是宿主经 stdin 发给 Worker 的控制消息。
	HostToWorker Direction = iota + 1
	// WorkerToHost 是 Worker 经 stdout 发给宿主的事件。
	WorkerToHost
)

var registry = map[Direction]map[string]func() Message{
	HostToWorker: {
		TypeInit:             func() Message { return &Init{} },
		TypeCheckpointResult: func() Message { return &CheckpointResult{} },
		TypeArtifactResult:   func() Message { return &ArtifactResult{} },
		TypeCancel:           func() Message { return &Cancel{} },
		TypePause:            func() Message { return &Pause{} },
	},
	WorkerToHost: {
		TypeReady:           func() Message { return &Ready{} },
		TypeProgress:        func() Message { return &Progress{} },
		TypeArtifact:        func() Message { return &Artifact{} },
		TypeCheckpoint:      func() Message { return &Checkpoint{} },
		TypeCheckpointQuery: func() Message { return &CheckpointQuery{} },
		TypePaused:          func() Message { return &Paused{} },
		TypeResult:          func() Message { return &Result{} },
		TypeError:           func() Message { return &ErrorEvent{} },
		TypeHandshakeError:  func() Message { return &HandshakeError{} },
	},
}

// DecodeLine 解析并校验一行消息（不含行尾换行符）。
// 未知字段忽略；未知类型、超限、字段类型错误与语义违规均返回 *Error。
func DecodeLine(dir Direction, line []byte) (Message, error) {
	if len(line) > maxLineBytes {
		return nil, newError(CodeMessageTooLarge, "%d 字节，上限 %d", len(line), maxLineBytes)
	}
	if t := bytes.TrimSpace(line); len(t) == 0 || t[0] != '{' {
		return nil, newError(CodeMalformedJSON, "消息必须是 JSON 对象")
	}
	var peek struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &peek); err != nil {
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) && te.Field == "type" {
			return nil, newError(CodeUnknownType, "type 不是字符串")
		}
		return nil, newError(CodeMalformedJSON, "%v", err)
	}
	newMsg, ok := registry[dir][peek.Type]
	if !ok {
		return nil, newError(CodeUnknownType, "%q", peek.Type)
	}
	if err := checkSize(dir, peek.Type, len(line)); err != nil {
		return nil, err
	}
	m := newMsg()
	if err := json.Unmarshal(line, m); err != nil {
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) {
			return nil, newError(CodeInvalidField, "%s: %v", te.Field, err)
		}
		return nil, newError(CodeMalformedJSON, "%v", err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// EncodeLine 校验并编码一条消息（不含行尾换行符）。m 的 type 字段必须与其类型一致。
func EncodeLine(dir Direction, m Message) ([]byte, error) {
	if _, ok := registry[dir][m.MessageType()]; !ok {
		return nil, newError(CodeUnknownType, "%s 不属于该方向", m.MessageType())
	}
	if m.declaredType() != m.MessageType() {
		return nil, newError(CodeInvalidField, "type=%q，期望 %q", m.declaredType(), m.MessageType())
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if err := checkSize(dir, m.MessageType(), len(b)); err != nil {
		return nil, err
	}
	return b, nil
}

// checkSize 按方向与类型检查上限：Worker 事件 MaxEventBytes，init MaxInitBytes，
// 其他宿主控制消息 MaxControlBytes。
func checkSize(dir Direction, typ string, n int) error {
	limit := MaxEventBytes
	switch {
	case dir == HostToWorker && typ == TypeInit:
		limit = MaxInitBytes
	case dir == HostToWorker:
		limit = MaxControlBytes
	}
	if n > limit {
		return newError(CodeMessageTooLarge, "%s %d 字节，上限 %d", typ, n, limit)
	}
	return nil
}
```

`scripts/dev/gofmt-staged.sh`（暂存内容的 gofmt 检查；工作区是 CRLF，直接检查会误报）：

```bash
#!/usr/bin/env bash
# 用法（在 WSL 中，以普通用户运行）：bash scripts/dev/gofmt-staged.sh
# 对 git 暂存区中的每个 .go 文件运行 gofmt；任何失败非零退出。
set -uo pipefail
export PATH="$PATH:/usr/local/go/bin"
cd "$(dirname "$0")/../.." || exit 1
tmp=$(mktemp)
status=0
files=$(git ls-files '*.go') || { echo "git ls-files 失败"; exit 1; }
for f in $files; do
  if ! git show ":$f" > "$tmp"; then
    echo "读取暂存内容失败: $f"; status=1; continue
  fi
  if ! out=$(gofmt -l "$tmp" 2>&1); then
    echo "gofmt 执行失败: $f: $out"; status=1; continue
  fi
  if [ -n "$out" ]; then
    echo "未格式化: $f"; status=1
  fi
done
rm -f "$tmp"
[ "$status" -eq 0 ] && echo GOFMT-OK
exit "$status"
```

- [ ] **Step 5：格式化并运行测试**

```powershell
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && gofmt -w internal/protocol && go vet ./internal/protocol/ && go test -count=1 ./internal/protocol/'
```

Expected: `ok  github.com/wanghr0318-dotcom/go-agentbox/internal/protocol`。`gofmt -w` 只调整空白对齐。

- [ ] **Step 6：提交**

```bash
cd /f/go-agentbox-m1-3 && git add internal/protocol protocol/fixtures/v1/messages.json scripts/dev/gofmt-staged.sh
```

```powershell
wsl -d Ubuntu -- bash /mnt/f/go-agentbox-m1-3/scripts/dev/gofmt-staged.sh
```

Expected: `GOFMT-OK`。

```bash
cd /f/go-agentbox-m1-3 && git commit -m "feat(protocol): Worker 协议 v1 消息类型、编解码与校验，跨语言消息 fixtures

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2：Go 事件流顺序检查；协议层场景 fixtures

**风险：高（协议语义）——双评审。**

**Files:**
- Create: `internal/protocol/stream.go`
- Modify: `internal/protocol/protocol_test.go`（末尾追加场景回放，下方给出完整追加内容）
- Create: `protocol/fixtures/v1/scenarios/` 下 12 个文件（下方给出）

**Interfaces:**
- Consumes: Task 1 的 `DecodeLine`、`Message`、`event`、`CodeOf`、`Type*`。
- Produces: `type WorkerStream struct`；`func (s *WorkerStream) Observe(m Message) error`；错误码 `CodeSeqInvalid`、`CodeBeforeReady`、`CodeDuplicateReady`、`CodeAfterTerminal`、`CodeHandshakeMisplaced`、`CodeAfterHandshakeError`；场景文件格式（Task 4、7 与 Plan 5 沿用）。

**场景文件格式：**

```json
{
  "name": "<与文件名相同>",
  "description": "<一句话>",
  "layers": ["protocol"] 或 ["protocol", "sdk"],
  "lines": [{"from": "host" | "worker", "message": {...}} 或 {"from": ..., "raw": "<原始行>"}],
  "expect": {"stream": "ok"} 或 {"stream": "violation", "violation": "<错误码>", "at": <行号，从 0 起>},
  "sdk": {"exit_code": <int>}   // 仅当 layers 含 "sdk"
}
```

- [ ] **Step 1：写入 12 个协议层场景**

`protocol/fixtures/v1/scenarios/seq_gap.json`：

```json
{
  "name": "seq_gap",
  "description": "seq 缺号是协议错误",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 3, "kind": "step_started", "message": "x"}}
  ],
  "expect": {"stream": "violation", "violation": "seq_invalid", "at": 2}
}
```

`protocol/fixtures/v1/scenarios/artifact_before_ready.json`：

```json
{
  "name": "artifact_before_ready",
  "description": "ready 之前登记产物是协议错误",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "artifact", "v": 1, "seq": 1, "artifact_id": "report", "path": "report.md", "declared_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "declared_size": 1, "media_type": "text/markdown", "visibility": "output"}}
  ],
  "expect": {"stream": "violation", "violation": "before_ready", "at": 1}
}
```

`protocol/fixtures/v1/scenarios/duplicate_ready.json`：

```json
{
  "name": "duplicate_ready",
  "description": "ready 只能出现一次",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 2, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}}
  ],
  "expect": {"stream": "violation", "violation": "duplicate_ready", "at": 2}
}
```

`protocol/fixtures/v1/scenarios/business_event_after_result.json`：

```json
{
  "name": "business_event_after_result",
  "description": "终态提议之后的业务事件是协议违规",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 2, "summary": "完成", "outputs": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 3, "kind": "step_started", "message": "x"}}
  ],
  "expect": {"stream": "violation", "violation": "after_terminal", "at": 3}
}
```

`protocol/fixtures/v1/scenarios/second_terminal_proposal.json`：

```json
{
  "name": "second_terminal_proposal",
  "description": "终态提议至多一个",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 2, "summary": "完成", "outputs": []}},
    {"from": "worker", "message": {"type": "error", "v": 1, "seq": 3, "code": "x", "message": "y", "retryable": false}}
  ],
  "expect": {"stream": "violation", "violation": "after_terminal", "at": 3}
}
```

`protocol/fixtures/v1/scenarios/query_after_result_allowed.json`：

```json
{
  "name": "query_after_result_allowed",
  "description": "终态提议之后仍允许 checkpoint_query",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 3, "summary": "完成", "outputs": []}},
    {"from": "worker", "message": {"type": "checkpoint_query", "v": 1, "seq": 4, "checkpoint_id": "cp-1", "scope": "task"}}
  ],
  "expect": {"stream": "ok"}
}
```

`protocol/fixtures/v1/scenarios/unknown_message_type.json`：

```json
{
  "name": "unknown_message_type",
  "description": "同一主版本内未知消息类型是错误",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "telemetry", "v": 1, "seq": 2}}
  ],
  "expect": {"stream": "violation", "violation": "unknown_type", "at": 2}
}
```

`protocol/fixtures/v1/scenarios/message_after_handshake_error.json`：

```json
{
  "name": "message_after_handshake_error",
  "description": "handshake_error 必须是唯一一条 Worker 消息",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [2], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "handshake_error", "bootstrap": 1, "code": "no_common_version"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}}
  ],
  "expect": {"stream": "violation", "violation": "after_handshake_error", "at": 2}
}
```

`protocol/fixtures/v1/scenarios/error_before_ready_allowed.json`：

```json
{
  "name": "error_before_ready_allowed",
  "description": "启动失败时允许在 ready 之前发出 error",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "session", "session_id": "s-1"}},
    {"from": "worker", "message": {"type": "error", "v": 1, "seq": 1, "code": "unsupported_mode", "message": "不支持的模式：session", "retryable": false}}
  ],
  "expect": {"stream": "ok"}
}
```

`protocol/fixtures/v1/scenarios/seq_duplicate.json`：

```json
{
  "name": "seq_duplicate",
  "description": "seq 重复是协议错误",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 1, "kind": "step_started", "message": "x"}}
  ],
  "expect": {"stream": "violation", "violation": "seq_invalid", "at": 2}
}
```

`protocol/fixtures/v1/scenarios/seq_not_from_one.json`：

```json
{
  "name": "seq_not_from_one",
  "description": "第一条事件的 seq 必须是 1",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 2, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}}
  ],
  "expect": {"stream": "violation", "violation": "seq_invalid", "at": 1}
}
```

`protocol/fixtures/v1/scenarios/handshake_error_after_ready.json`：

```json
{
  "name": "handshake_error_after_ready",
  "description": "handshake_error 只能作为第一条 Worker 消息",
  "layers": ["protocol"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1"}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "handshake_error", "bootstrap": 1, "code": "no_common_version"}}
  ],
  "expect": {"stream": "violation", "violation": "handshake_error_misplaced", "at": 2}
}
```

- [ ] **Step 2：写失败的测试**

事件流规则全部以场景 fixtures 表达，Go 与 Python 回放同一份，不另写 Go 专用的规则表。

在 `internal/protocol/protocol_test.go` **末尾追加**：

```go
type scenarioLine struct {
	From    string          `json:"from"`
	Message json.RawMessage `json:"message"`
	Raw     string          `json:"raw"`
}

type scenario struct {
	Name   string         `json:"name"`
	Lines  []scenarioLine `json:"lines"`
	Expect struct {
		Stream    string `json:"stream"`
		Violation string `json:"violation"`
		At        int    `json:"at"`
	} `json:"expect"`
}

// replayScenario 依次解码每一行，并把 Worker 消息送入 WorkerStream；
// 返回首个违规所在行号与错误码，无违规时返回 (-1, "")。
func replayScenario(t *testing.T, sc scenario) (int, string) {
	t.Helper()
	var s WorkerStream
	for i, l := range sc.Lines {
		dir := parseDirection(t, l.From)
		line := []byte(l.Message)
		if l.Raw != "" {
			line = []byte(l.Raw)
		}
		m, err := DecodeLine(dir, line)
		if err == nil && dir == WorkerToHost {
			err = s.Observe(m)
		}
		if err != nil {
			if code := CodeOf(err); code != "" {
				return i, code
			}
			t.Fatalf("第 %d 行返回了非协议错误：%v", i, err)
		}
	}
	return -1, ""
}

func TestScenarioFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir, "scenarios", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("没有场景 fixtures（err=%v）", err)
	}
	for _, path := range files {
		var sc scenario
		loadJSON(t, path, &sc)
		t.Run(sc.Name, func(t *testing.T) {
			at, code := replayScenario(t, sc)
			switch sc.Expect.Stream {
			case "ok":
				if code != "" {
					t.Fatalf("第 %d 行违规 %s，期望无违规", at, code)
				}
			case "violation":
				if at != sc.Expect.At || code != sc.Expect.Violation {
					t.Fatalf("得到 (%d, %q)，期望 (%d, %q)", at, code, sc.Expect.At, sc.Expect.Violation)
				}
			default:
				t.Fatalf("未知的 expect.stream %q", sc.Expect.Stream)
			}
		})
	}
}
```

- [ ] **Step 3：确认测试失败**

```powershell
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && go test -count=1 ./internal/protocol/'
```

Expected: 编译失败，`undefined: WorkerStream`。

- [ ] **Step 4：实现**

`internal/protocol/stream.go`：

```go
package protocol

// 事件流顺序错误码（规格 §5.3）。
const (
	CodeSeqInvalid          = "seq_invalid"
	CodeBeforeReady         = "before_ready"
	CodeDuplicateReady      = "duplicate_ready"
	CodeAfterTerminal       = "after_terminal"
	CodeHandshakeMisplaced  = "handshake_error_misplaced"
	CodeAfterHandshakeError = "after_handshake_error"
)

type streamPhase int

const (
	phaseAwaitingReady streamPhase = iota
	phaseRunning
	phaseTerminalSent
	phaseHandshakeFailed
)

// WorkerStream 按规格 §5.3 检查 task 模式下 Worker 事件流的顺序：
// seq 从 1 严格递增；ready 之前只允许 error（启动失败）；ready 只能出现一次；
// 终态提议（result、error、paused）至多一个，其后只允许 checkpoint_query；
// handshake_error 只能是第一条且是唯一一条消息。
// Observe 返回错误后，流即视为违规，不应继续使用。
type WorkerStream struct {
	phase   streamPhase
	lastSeq int64
}

// Observe 检查下一条 Worker 消息。
func (s *WorkerStream) Observe(m Message) error {
	if s.phase == phaseHandshakeFailed {
		return newError(CodeAfterHandshakeError, "handshake_error 之后出现 %s", m.MessageType())
	}
	if m.MessageType() == TypeHandshakeError {
		if s.phase != phaseAwaitingReady || s.lastSeq != 0 {
			return newError(CodeHandshakeMisplaced, "handshake_error 必须是第一条消息")
		}
		s.phase = phaseHandshakeFailed
		return nil
	}
	ev, ok := m.(event)
	if !ok {
		return newError(CodeUnknownType, "%s 不是 Worker 事件", m.MessageType())
	}
	if seq := ev.header().Seq; seq != s.lastSeq+1 {
		return newError(CodeSeqInvalid, "seq=%d，期望 %d", seq, s.lastSeq+1)
	}
	s.lastSeq++
	return s.advance(m.MessageType())
}

func (s *WorkerStream) advance(typ string) error {
	switch s.phase {
	case phaseAwaitingReady:
		switch typ {
		case TypeReady:
			s.phase = phaseRunning
		case TypeError:
			s.phase = phaseTerminalSent
		default:
			return newError(CodeBeforeReady, "%s 出现在 ready 之前", typ)
		}
	case phaseRunning:
		if typ == TypeReady {
			return newError(CodeDuplicateReady, "重复的 ready")
		}
		if isTerminal(typ) {
			s.phase = phaseTerminalSent
		}
	case phaseTerminalSent:
		if typ != TypeCheckpointQuery {
			return newError(CodeAfterTerminal, "终态提议之后出现 %s", typ)
		}
	}
	return nil
}

func isTerminal(typ string) bool {
	return typ == TypeResult || typ == TypeError || typ == TypePaused
}
```

- [ ] **Step 5：格式化并运行测试**

```powershell
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && gofmt -w internal/protocol && go vet ./internal/protocol/ && go test -count=1 -v ./internal/protocol/ 2>&1 | grep -E "^(--- FAIL|ok|FAIL)|TestScenarioFixtures/"'
```

Expected: 12 个 `TestScenarioFixtures/<场景名>` 均为 PASS，最后一行 `ok`。

- [ ] **Step 6：提交**

```bash
cd /f/go-agentbox-m1-3 && git add internal/protocol/stream.go internal/protocol/protocol_test.go protocol/fixtures/v1/scenarios
```

```powershell
wsl -d Ubuntu -- bash /mnt/f/go-agentbox-m1-3/scripts/dev/gofmt-staged.sh
```

```bash
cd /f/go-agentbox-m1-3 && git commit -m "feat(protocol): Worker 事件流顺序检查与协议层场景 fixtures

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3：Python 项目、协议编解码与事件流检查（与 Go 逐条对应）

**风险：高（跨语言一致性）——双评审。**

**Files:**
- Create: `worker/pyproject.toml`、`worker/uv.lock`（由 `uv lock` 生成）
- Create: `worker/agentbox_worker/__init__.py`、`worker/agentbox_worker/protocol.py`、`worker/agentbox_worker/stream.py`
- Create: `worker/tests/protocol_fixtures.py`（fixtures 读取辅助）、`worker/tests/test_protocol.py`（协议层测试；Task 4 在末尾追加 schema 一致性测试）
- Modify: `.gitignore`（追加 Python 产物）

**Interfaces:**
- Consumes: Task 1、2 的 fixtures 与规则（Python 必须给出相同错误码）。
- Produces：
  - `agentbox_worker.protocol`：`HOST = "host"`、`WORKER = "worker"`、`VERSION`、`BOOTSTRAP_VERSION`、`MAX_*`、`class ProtocolError(Exception)`（属性 `code`、`detail`）、`decode_line(direction: str, line: bytes | str) -> dict`、`encode_line(direction: str, msg: dict) -> bytes`、`valid_sha256(s: str) -> bool`、`valid_artifact_path(path: str) -> bool`
  - `agentbox_worker.stream.StreamChecker`：`observe(msg: dict) -> None`（违规抛 `ProtocolError`）
  - 测试辅助 `protocol_fixtures`：`FIXTURES: Path`、`load_messages() -> dict`、`load_scenarios() -> list[dict]`、`line_bytes(item: dict) -> bytes`

- [ ] **Step 1：项目文件**

`worker/pyproject.toml`：

```toml
[project]
name = "agentbox-worker"
version = "0.1.0"
description = "go-agentbox 的 Python Worker SDK 与 sim-worker"
requires-python = ">=3.11"
dependencies = []

[dependency-groups]
dev = [
  "pytest>=8.0",
  "jsonschema>=4.23",
  "ruff>=0.6",
  "import-linter>=2.1",
]

[build-system]
requires = ["hatchling>=1.25"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["agentbox_worker", "sim_worker"]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
target-version = "py311"
line-length = 100

[tool.ruff.lint]
# C90（复杂度）不在阻断检查中；CI 另行以 --exit-zero 报告。
select = ["E", "F", "W", "I", "B", "UP", "TID251"]

[tool.ruff.lint.mccabe]
max-complexity = 10

[tool.ruff.lint.flake8-tidy-imports.banned-api]
"subprocess".msg = "Worker 不直接创建子进程；代码执行经 Gateway 的 /v1/exec"
"os.system".msg = "Worker 不直接创建子进程"
"os.popen".msg = "Worker 不直接创建子进程"
"os.fork".msg = "Worker 不直接创建子进程"
"os.execv".msg = "Worker 不直接替换进程映像"
"os.execve".msg = "Worker 不直接替换进程映像"
"os.execvp".msg = "Worker 不直接替换进程映像"
"os.execvpe".msg = "Worker 不直接替换进程映像"
"os.execl".msg = "Worker 不直接替换进程映像"
"os.execle".msg = "Worker 不直接替换进程映像"
"os.execlp".msg = "Worker 不直接替换进程映像"
"os.execlpe".msg = "Worker 不直接替换进程映像"

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["TID251", "E402"]  # 测试文件按关注点分节，各节在节首导入

[tool.importlinter]
root_packages = ["agentbox_worker", "sim_worker"]

[[tool.importlinter.contracts]]
name = "SDK 不依赖具体 Worker"
type = "forbidden"
source_modules = ["agentbox_worker"]
forbidden_modules = ["sim_worker"]
```

`worker/agentbox_worker/__init__.py`（Task 6 会替换为完整版本）：

```python
"""go-agentbox Python Worker SDK：协议 v1（task 模式）。"""
```

在 `.gitignore` 末尾追加：

```gitignore

# Python（worker/）
worker/.venv/
__pycache__/
*.pyc
.pytest_cache/
.pytest-tmp/
.ruff_cache/
```

生成锁文件并安装（Windows PowerShell，需要网络）：

```powershell
cd F:\go-agentbox-m1-3\worker; uv lock; uv sync
```

Expected: 生成 `worker/uv.lock` 与 `worker/.venv`，无报错。

- [ ] **Step 2：写失败的测试**

`worker/tests/protocol_fixtures.py`：

```python
"""读取 protocol/fixtures/v1 下的跨语言 fixtures（Go 侧使用同一份文件）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parents[2] / "protocol" / "fixtures" / "v1"


def load_messages() -> dict[str, list[dict[str, Any]]]:
    return json.loads((FIXTURES / "messages.json").read_text(encoding="utf-8"))


def load_scenarios() -> list[dict[str, Any]]:
    paths = sorted((FIXTURES / "scenarios").glob("*.json"))
    return [json.loads(p.read_text(encoding="utf-8")) for p in paths]


def line_bytes(item: dict[str, Any]) -> bytes:
    """消息 fixture 或场景行对应的原始行。"""
    if "raw" in item:
        return item["raw"].encode("utf-8")
    return json.dumps(item["message"], ensure_ascii=False).encode("utf-8")
```

`worker/tests/test_protocol.py`：

```python
"""协议层：跨语言 fixtures 回放与按常量生成的大小上限（规格 §5.10、§5.11）。"""

import json

import pytest
from protocol_fixtures import line_bytes, load_messages, load_scenarios

from agentbox_worker.protocol import (
    HOST,
    MAX_CONTROL_BYTES,
    MAX_EVENT_BYTES,
    MAX_INIT_BYTES,
    MAX_INLINE_STATE_BYTES,
    MAX_REFS_PER_CHECKPOINT,
    WORKER,
    ProtocolError,
    decode_line,
    encode_line,
)
from agentbox_worker.stream import StreamChecker

MESSAGES = load_messages()
SCENARIOS = load_scenarios()
assert MESSAGES["valid"] and MESSAGES["invalid"] and SCENARIOS, "fixtures 为空"


@pytest.mark.parametrize("case", MESSAGES["valid"], ids=lambda c: c["name"])
def test_valid_message_round_trips(case):
    msg = decode_line(case["direction"], line_bytes(case))
    again = decode_line(case["direction"], encode_line(case["direction"], msg))
    assert again == msg


@pytest.mark.parametrize("case", MESSAGES["invalid"], ids=lambda c: c["name"])
def test_invalid_message_has_expected_code(case):
    with pytest.raises(ProtocolError) as exc:
        decode_line(case["direction"], line_bytes(case))
    assert exc.value.code == case["code"]


def replay_protocol(scenario: dict) -> tuple[int, str]:
    """与 Go 侧 replayScenario 相同：返回首个违规的行号与错误码，无违规时为 (-1, "")。"""
    checker = StreamChecker()
    for index, line in enumerate(scenario["lines"]):
        try:
            msg = decode_line(line["from"], line_bytes(line))
            if line["from"] == "worker":
                checker.observe(msg)
        except ProtocolError as exc:
            return index, exc.code
    return -1, ""


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_scenario_stream(scenario):
    expect = scenario["expect"]
    want = (-1, "") if expect["stream"] == "ok" else (expect["at"], expect["violation"])
    assert replay_protocol(scenario) == want


REF = "a" * 64


def checkpoint_line(state_json: str, refs: list[str]) -> bytes:
    head = '{"type":"checkpoint","v":1,"seq":1,"checkpoint_id":"cp-1","scope":"task","step_id":"s1"'
    return f'{head},"state":{state_json},"refs":{json.dumps(refs)}}}'.encode()


def init_line(config: str) -> bytes:
    init = {
        "type": "init",
        "bootstrap": 1,
        "protocol_versions": [1],
        "mode": "task",
        "task_id": "t",
        "attempt_id": "a",
        "attempt_no": 1,
        "out_dir": "/o",
        "config": config,
    }
    return json.dumps(init).encode()


def progress_line(message: str) -> bytes:
    return f'{{"type":"progress","v":1,"seq":1,"kind":"x","message":"{message}"}}'.encode()


def cancel_line(reason: str) -> bytes:
    return f'{{"type":"cancel","v":1,"attempt_id":"a-1","grace_ms":0,"reason":"{reason}"}}'.encode()


LIMIT_CASES = [
    pytest.param(
        WORKER, progress_line("a" * MAX_EVENT_BYTES), "message_too_large", id="event_over_limit"
    ),
    pytest.param(
        HOST, cancel_line("a" * MAX_CONTROL_BYTES), "message_too_large", id="control_over_limit"
    ),
    pytest.param(HOST, init_line("a" * (100 << 10)), None, id="init_limit_is_1mib"),
    pytest.param(
        WORKER,
        checkpoint_line('"' + "a" * (MAX_INLINE_STATE_BYTES - 2) + '"', []),
        None,
        id="state_at_limit",
    ),
    pytest.param(
        WORKER,
        checkpoint_line('"' + "a" * (MAX_INLINE_STATE_BYTES - 1) + '"', []),
        "state_too_large",
        id="state_over_limit",
    ),
    pytest.param(
        WORKER, checkpoint_line('{ "k" :  "aaaaaaaaaa" }', []), None, id="state_whitespace_ignored"
    ),
    pytest.param(
        WORKER, checkpoint_line("{}", [REF] * MAX_REFS_PER_CHECKPOINT), None, id="refs_at_limit"
    ),
    pytest.param(
        WORKER,
        checkpoint_line("{}", [REF] * (MAX_REFS_PER_CHECKPOINT + 1)),
        "too_many_refs",
        id="refs_over_limit",
    ),
    pytest.param(
        HOST, b"x" * (MAX_INIT_BYTES + 1), "message_too_large", id="oversized_line_not_parsed"
    ),
    pytest.param(
        WORKER,
        b'{"type":"progress","v":1,"seq":1,"kind":"x","message":"y","data":NaN}',
        "malformed_json",
        id="nan_is_malformed",
    ),
]


@pytest.mark.parametrize(("direction", "line", "code"), LIMIT_CASES)
def test_decode_limits(direction, line, code):
    if code is None:
        decode_line(direction, line)
        return
    with pytest.raises(ProtocolError) as exc:
        decode_line(direction, line)
    assert exc.value.code == code


@pytest.mark.parametrize(
    ("direction", "message", "code"),
    [
        pytest.param(
            WORKER,
            {
                "type": "checkpoint",
                "v": 1,
                "seq": 1,
                "checkpoint_id": "c",
                "scope": "task",
                "step_id": "s",
            },
            "invalid_field",
            id="checkpoint_without_state",
        ),
        pytest.param(
            HOST,
            {"type": "paused", "v": 1, "seq": 1, "checkpoint_id": "cp-1"},
            "unknown_type",
            id="wrong_direction",
        ),
    ],
)
def test_encode_rejects(direction, message, code):
    with pytest.raises(ProtocolError) as exc:
        encode_line(direction, message)
    assert exc.value.code == code
```

- [ ] **Step 3：确认测试失败**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q
```

Expected: 收集错误 `ModuleNotFoundError: No module named 'agentbox_worker.protocol'`。

- [ ] **Step 4：实现**

`worker/agentbox_worker/protocol.py`：

```python
"""Worker 协议 v1（task 模式）的消息校验与编解码。

规则与 Go 侧 internal/protocol 逐条对应，二者共用 protocol/fixtures/v1。
消息以 dict 表示；未知字段忽略，未知类型是错误（规格 §5.3）。
判定顺序：非 JSON 对象 → 类型未知 → 超限 → 字段类型错误 → 语义规则。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

HOST = "host"
WORKER = "worker"

VERSION = 1
BOOTSTRAP_VERSION = 1

MAX_EVENT_BYTES = 1 << 20
MAX_INIT_BYTES = 1 << 20
MAX_CONTROL_BYTES = 16 << 10
MAX_INLINE_STATE_BYTES = 256 << 10
MAX_REFS_PER_CHECKPOINT = 1024
MAX_ARTIFACT_PATH_BYTES = 4096
# 任何消息类型上限中的最大值；超过它的行不解析，直接判为过大。
MAX_LINE_BYTES = max(MAX_EVENT_BYTES, MAX_INIT_BYTES)

MODE_TASK = "task"
MODE_SESSION = "session"
SCOPE_TASK = "task"
SCOPE_SESSION = "session"
CHECKPOINT_STATUSES = ("committed", "conflict", "rejected", "retryable_error", "not_found")


class ProtocolError(Exception):
    """带稳定错误码的协议错误。"""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


Pred = Callable[[Any], bool]


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_bool(v: Any) -> bool:
    return isinstance(v, bool)


def _any(_: Any) -> bool:
    return True


def _list_of(pred: Pred) -> Pred:
    return lambda v: isinstance(v, list) and all(x is None or pred(x) for x in v)


def _obj(spec: dict[str, Pred]) -> Pred:
    return lambda v: (
        isinstance(v, dict) and all(v.get(k) is None or pred(v[k]) for k, pred in spec.items())
    )


_STRS = _list_of(_is_str)
_HOST = {"type": _is_str, "v": _is_int}
_EVENT = {"type": _is_str, "v": _is_int, "seq": _is_int, "ts": _is_str}
_STOP = {**_HOST, "attempt_id": _is_str, "reason": _is_str, "grace_ms": _is_int}
_RESUME = {
    "checkpoint_id": _is_str,
    "step_id": _is_str,
    "state": _any,
    "state_ref": _is_str,
    "refs": _STRS,
}

_FIELD_TYPES: dict[str, dict[str, Pred]] = {
    "init": {
        "type": _is_str,
        "bootstrap": _is_int,
        "protocol_versions": _list_of(_is_int),
        "mode": _is_str,
        "task_id": _is_str,
        "attempt_id": _is_str,
        "attempt_no": _is_int,
        "traceparent": _is_str,
        "config": _any,
        "config_version": _is_str,
        "budget_limits": _any,
        "input_refs": _STRS,
        "out_dir": _is_str,
        "resume": _obj(_RESUME),
    },
    "checkpoint_result": {
        **_HOST,
        "checkpoint_id": _is_str,
        "scope": _is_str,
        "status": _is_str,
        "code": _is_str,
    },
    "artifact_result": {
        **_HOST,
        "artifact_id": _is_str,
        "status": _is_str,
        "version": _is_int,
        "sha256": _is_str,
        "code": _is_str,
    },
    "cancel": _STOP,
    "pause": _STOP,
    "ready": {
        **_EVENT,
        "protocol_version": _is_int,
        "mode": _is_str,
        "worker": _obj({"name": _is_str, "version": _is_str}),
        "capabilities": _STRS,
    },
    "progress": {**_EVENT, "step_id": _is_str, "kind": _is_str, "message": _is_str, "data": _any},
    "artifact": {
        **_EVENT,
        "artifact_id": _is_str,
        "path": _is_str,
        "declared_sha256": _is_str,
        "declared_size": _is_int,
        "media_type": _is_str,
        "visibility": _is_str,
    },
    "checkpoint": {
        **_EVENT,
        "checkpoint_id": _is_str,
        "scope": _is_str,
        "step_id": _is_str,
        "state": _any,
        "state_ref": _is_str,
        "refs": _STRS,
    },
    "checkpoint_query": {**_EVENT, "checkpoint_id": _is_str, "scope": _is_str},
    "paused": {**_EVENT, "checkpoint_id": _is_str},
    "result": {**_EVENT, "summary": _is_str, "outputs": _STRS},
    "error": {**_EVENT, "code": _is_str, "message": _is_str, "retryable": _is_bool},
    "handshake_error": {"type": _is_str, "bootstrap": _is_int, "code": _is_str},
}


def _s(msg: dict[str, Any], key: str) -> str:
    value = msg.get(key)
    return value if isinstance(value, str) else ""


def _n(msg: dict[str, Any], key: str) -> int:
    value = msg.get(key)
    return value if _is_int(value) else 0


def _required(field: str, value: str) -> None:
    if value == "":
        raise ProtocolError("missing_field", f"{field} 不能为空")


def _one_of(field: str, value: str, *allowed: str) -> None:
    if value == "":
        raise ProtocolError("missing_field", f"{field} 不能为空")
    if value not in allowed:
        raise ProtocolError("invalid_field", f"{field}={value!r} 不在允许范围 {allowed} 内")


def _check_version(msg: dict[str, Any]) -> None:
    if _n(msg, "v") != VERSION:
        raise ProtocolError("version_mismatch", f"v={_n(msg, 'v')}，期望 {VERSION}")


def _check_event(msg: dict[str, Any]) -> None:
    _check_version(msg)
    if _n(msg, "seq") < 1:
        raise ProtocolError("invalid_field", f"seq={_n(msg, 'seq')}，必须 ≥ 1")


def valid_sha256(s: str) -> bool:
    return len(s) == 64 and all(c in "0123456789abcdef" for c in s)


def valid_artifact_path(path: str) -> bool:
    """相对、非空、无 NUL、不超过上限、不含空、. 或 .. 分量（规格 §5.6）。"""
    if not path or len(path.encode("utf-8")) > MAX_ARTIFACT_PATH_BYTES:
        return False
    if "\x00" in path or path.startswith("/"):
        return False
    return all(segment not in ("", ".", "..") for segment in path.split("/"))


def _check_state(msg: dict[str, Any]) -> None:
    has_state = msg.get("state") is not None
    state_ref = _s(msg, "state_ref")
    if has_state == (state_ref != ""):
        raise ProtocolError("invalid_field", "state 与 state_ref 必须且只能提供一个")
    if state_ref:
        if not valid_sha256(state_ref):
            raise ProtocolError("invalid_field", "state_ref 不是合法的 sha256")
        return
    compact = json.dumps(msg["state"], ensure_ascii=False, separators=(",", ":"))
    size = len(compact.encode("utf-8"))
    if size > MAX_INLINE_STATE_BYTES:
        raise ProtocolError(
            "state_too_large", f"inline state {size} 字节，上限 {MAX_INLINE_STATE_BYTES}"
        )


def _check_refs(field: str, refs: list[Any] | None, limit: int | None) -> None:
    refs = refs or []
    if limit is not None and len(refs) > limit:
        raise ProtocolError("too_many_refs", f"{field} 有 {len(refs)} 个，上限 {limit}")
    for ref in refs:
        if not valid_sha256(ref or ""):
            raise ProtocolError("invalid_field", f"{field} 中有不合法的 sha256")


def _v_init(m: dict[str, Any]) -> None:
    if _n(m, "bootstrap") != BOOTSTRAP_VERSION:
        raise ProtocolError("invalid_field", f"bootstrap={_n(m, 'bootstrap')}，期望 1")
    if not m.get("protocol_versions"):
        raise ProtocolError("missing_field", "protocol_versions 不能为空")
    mode = _s(m, "mode")
    _one_of("mode", mode, MODE_TASK, MODE_SESSION)
    if mode != MODE_TASK:
        return  # session 扩展的字段由后续里程碑校验
    for name in ("task_id", "attempt_id", "out_dir"):
        _required(name, _s(m, name))
    if _n(m, "attempt_no") < 1:
        raise ProtocolError("invalid_field", f"attempt_no={_n(m, 'attempt_no')}，必须 ≥ 1")
    _check_refs("input_refs", m.get("input_refs"), None)
    resume = m.get("resume")
    if resume is None:
        return
    _required("resume.checkpoint_id", _s(resume, "checkpoint_id"))
    _required("resume.step_id", _s(resume, "step_id"))
    _check_state(resume)
    _check_refs("resume.refs", resume.get("refs"), MAX_REFS_PER_CHECKPOINT)


def _v_checkpoint_result(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK, SCOPE_SESSION)
    _one_of("status", _s(m, "status"), *CHECKPOINT_STATUSES)


def _v_artifact_result(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("artifact_id", _s(m, "artifact_id"))
    status = _s(m, "status")
    _one_of("status", status, "saved", "rejected")
    if status == "rejected":
        _required("code", _s(m, "code"))
        return
    if _n(m, "version") < 1:
        raise ProtocolError("invalid_field", f"saved 的 version={_n(m, 'version')}，必须 ≥ 1")
    if not valid_sha256(_s(m, "sha256")):
        raise ProtocolError("invalid_field", "saved 的 sha256 不合法")


def _v_stop(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("attempt_id", _s(m, "attempt_id"))
    if _n(m, "grace_ms") < 0:
        raise ProtocolError("invalid_field", f"grace_ms={_n(m, 'grace_ms')}，必须 ≥ 0")


def _v_ready(m: dict[str, Any]) -> None:
    _check_event(m)
    if _n(m, "protocol_version") != VERSION:
        raise ProtocolError("version_mismatch", f"protocol_version={_n(m, 'protocol_version')}")
    _one_of("mode", _s(m, "mode"), MODE_TASK, MODE_SESSION)
    _required("worker.name", _s(m.get("worker") or {}, "name"))


def _v_progress(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("kind", _s(m, "kind"))


def _v_artifact(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("artifact_id", _s(m, "artifact_id"))
    if not valid_artifact_path(_s(m, "path")):
        raise ProtocolError("path_invalid", f"产物路径 {_s(m, 'path')!r} 不合法")
    if not valid_sha256(_s(m, "declared_sha256")):
        raise ProtocolError("invalid_field", "declared_sha256 不合法")
    if _n(m, "declared_size") < 0:
        raise ProtocolError("invalid_field", "declared_size 必须 ≥ 0")
    _required("media_type", _s(m, "media_type"))
    _one_of("visibility", _s(m, "visibility"), "output", "internal")


def _v_checkpoint(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _required("step_id", _s(m, "step_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK)  # session scope 只能经 result 提议写入
    _check_state(m)
    _check_refs("refs", m.get("refs"), MAX_REFS_PER_CHECKPOINT)


def _v_checkpoint_query(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK, SCOPE_SESSION)


def _v_paused(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))


def _v_result(m: dict[str, Any]) -> None:
    _check_event(m)
    for output in m.get("outputs") or []:
        if not output:
            raise ProtocolError("invalid_field", "outputs 中不能有空的 artifact_id")


def _v_error(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("code", _s(m, "code"))


def _v_handshake_error(m: dict[str, Any]) -> None:
    if _n(m, "bootstrap") != BOOTSTRAP_VERSION:
        raise ProtocolError("invalid_field", f"bootstrap={_n(m, 'bootstrap')}，期望 1")
    _required("code", _s(m, "code"))


_VALIDATORS: dict[str, dict[str, Callable[[dict[str, Any]], None]]] = {
    HOST: {
        "init": _v_init,
        "checkpoint_result": _v_checkpoint_result,
        "artifact_result": _v_artifact_result,
        "cancel": _v_stop,
        "pause": _v_stop,
    },
    WORKER: {
        "ready": _v_ready,
        "progress": _v_progress,
        "artifact": _v_artifact,
        "checkpoint": _v_checkpoint,
        "checkpoint_query": _v_checkpoint_query,
        "paused": _v_paused,
        "result": _v_result,
        "error": _v_error,
        "handshake_error": _v_handshake_error,
    },
}


def _reject_constant(name: str) -> Any:
    raise ValueError(f"不允许的 JSON 常量 {name}")


def _check_size(direction: str, typ: str, size: int) -> None:
    if direction == WORKER:
        limit = MAX_EVENT_BYTES
    elif typ == "init":
        limit = MAX_INIT_BYTES
    else:
        limit = MAX_CONTROL_BYTES
    if size > limit:
        raise ProtocolError("message_too_large", f"{typ} {size} 字节，上限 {limit}")


def _check_types(typ: str, msg: dict[str, Any]) -> None:
    for key, pred in _FIELD_TYPES[typ].items():
        value = msg.get(key)
        if value is not None and not pred(value):
            raise ProtocolError("invalid_field", f"{key} 类型不正确")


def _validator_for(direction: str, typ: Any) -> Callable[[dict[str, Any]], None]:
    validator = _VALIDATORS[direction].get(typ) if isinstance(typ, str) else None
    if validator is None:
        raise ProtocolError("unknown_type", repr(typ))
    return validator


def decode_line(direction: str, line: bytes | str) -> dict[str, Any]:
    """解析并校验一行消息（不含行尾换行符）。"""
    raw = line.encode("utf-8") if isinstance(line, str) else line
    if len(raw) > MAX_LINE_BYTES:
        raise ProtocolError("message_too_large", f"{len(raw)} 字节，上限 {MAX_LINE_BYTES}")
    try:
        msg = json.loads(raw, parse_constant=_reject_constant)
    except ValueError as exc:  # JSONDecodeError 与 UnicodeDecodeError 都是 ValueError
        raise ProtocolError("malformed_json", str(exc)) from exc
    if not isinstance(msg, dict):
        raise ProtocolError("malformed_json", "消息必须是 JSON 对象")
    typ = msg.get("type")
    validator = _validator_for(direction, typ)
    _check_size(direction, typ, len(raw))
    _check_types(typ, msg)
    validator(msg)
    return msg


def encode_line(direction: str, msg: dict[str, Any]) -> bytes:
    """校验并编码一条消息（不含行尾换行符）。"""
    typ = msg.get("type")
    validator = _validator_for(direction, typ)
    _check_types(typ, msg)
    validator(msg)
    try:
        text = json.dumps(msg, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:  # NaN、Infinity 不是合法 JSON
        raise ProtocolError("invalid_field", f"消息含有 JSON 不支持的数值：{exc}") from exc
    line = text.encode("utf-8")
    _check_size(direction, typ, len(line))
    return line
```

`worker/agentbox_worker/stream.py`：

```python
"""task 模式 Worker 事件流的顺序检查，与 Go 侧 protocol.WorkerStream 逐条对应（规格 §5.3）。"""

from __future__ import annotations

from typing import Any

from agentbox_worker.protocol import ProtocolError

_TERMINAL = {"result", "error", "paused"}


class StreamChecker:
    """seq 从 1 严格递增；ready 之前只允许 error；ready 只能出现一次；
    终态提议至多一个，其后只允许 checkpoint_query；handshake_error 只能是唯一一条消息。
    """

    def __init__(self) -> None:
        self._phase = "awaiting_ready"
        self._last_seq = 0

    def observe(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        if self._phase == "handshake_failed":
            raise ProtocolError("after_handshake_error", f"handshake_error 之后出现 {typ}")
        if typ == "handshake_error":
            if self._phase != "awaiting_ready" or self._last_seq != 0:
                raise ProtocolError("handshake_error_misplaced", "handshake_error 必须是第一条消息")
            self._phase = "handshake_failed"
            return
        seq = msg.get("seq")
        if seq != self._last_seq + 1:
            raise ProtocolError("seq_invalid", f"seq={seq}，期望 {self._last_seq + 1}")
        self._last_seq += 1
        self._advance(typ)

    def _advance(self, typ: str) -> None:
        if self._phase == "awaiting_ready":
            if typ == "ready":
                self._phase = "running"
            elif typ == "error":
                self._phase = "terminal_sent"
            else:
                raise ProtocolError("before_ready", f"{typ} 出现在 ready 之前")
        elif self._phase == "running":
            if typ == "ready":
                raise ProtocolError("duplicate_ready", "重复的 ready")
            if typ in _TERMINAL:
                self._phase = "terminal_sent"
        elif typ != "checkpoint_query":
            raise ProtocolError("after_terminal", f"终态提议之后出现 {typ}")
```

- [ ] **Step 5：格式化、静态检查并运行测试**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run ruff format .; uv run ruff check .; uv run pytest -q
```

Expected: `ruff check` 无问题；pytest 全部通过，0 failed（消息 fixtures、协议层场景、大小上限与编码拒绝用例，数量以实际为准）。

- [ ] **Step 6：提交**

```bash
cd /f/go-agentbox-m1-3 && git add .gitignore worker/pyproject.toml worker/uv.lock worker/agentbox_worker/__init__.py worker/agentbox_worker/protocol.py worker/agentbox_worker/stream.py worker/tests/protocol_fixtures.py worker/tests/test_protocol.py && git commit -m "feat(worker): Python 协议编解码与事件流检查，与 Go 共用 fixtures

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4：JSON Schema 与协议说明

**风险：中（格式契约与文档）——单次规格评审。**

**Files:**
- Create: `protocol/v1/task.schema.json`、`protocol/v1/control.schema.json`、`protocol/v1/event.schema.json`
- Create: `protocol/README.md`
- Modify: `worker/tests/test_protocol.py`（末尾追加 schema 一致性测试）

**Interfaces:**
- Consumes: Task 1–3 的 fixtures 与规则。
- Produces: 三个 schema 文件（`$id` 稳定）；schema 与 fixtures 的一致性测试。

**约定**：schema 只表达格式。它必须**接受**全部合法消息 fixtures 与场景中的合法行，**拒绝**全部非原始行（无 `raw`）的非法消息 fixtures。事件流顺序不由 schema 表达。

- [ ] **Step 1：写失败的测试**

在 `worker/tests/test_protocol.py` **末尾追加**（与上文空两行）：

```python
# ---- JSON Schema：与 fixtures 一致 ----

from typing import Any

from jsonschema import Draft202012Validator
from protocol_fixtures import FIXTURES

SCHEMA_NAMES = ("task", "control", "event")


def load_schema(name: str) -> dict[str, Any]:
    path = FIXTURES.parents[1] / "v1" / f"{name}.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


VALIDATORS = {name: Draft202012Validator(load_schema(name)) for name in SCHEMA_NAMES}


def validator_for(direction: str, msg: dict[str, Any]) -> Draft202012Validator:
    if direction == "worker":
        return VALIDATORS["event"]
    return VALIDATORS["task"] if msg.get("type") == "init" else VALIDATORS["control"]


def test_schemas_are_valid_draft_2020_12():
    for name in SCHEMA_NAMES:
        Draft202012Validator.check_schema(load_schema(name))


def valid_items() -> list[tuple[str, str, dict[str, Any]]]:
    items = [(c["name"], c["direction"], c["message"]) for c in MESSAGES["valid"]]
    for scenario in SCENARIOS:
        bad = scenario["expect"].get("at")
        for index, line in enumerate(scenario["lines"]):
            if index != bad and "message" in line:
                items.append((f"{scenario['name']}[{index}]", line["from"], line["message"]))
    return items


@pytest.mark.parametrize("item", valid_items(), ids=lambda i: i[0])
def test_schema_accepts_valid(item):
    _, direction, msg = item
    errors = list(validator_for(direction, msg).iter_errors(msg))
    assert not errors, [e.message for e in errors]


@pytest.mark.parametrize(
    "case", [c for c in MESSAGES["invalid"] if "message" in c], ids=lambda c: c["name"]
)
def test_schema_rejects_invalid(case):
    assert not validator_for(case["direction"], case["message"]).is_valid(case["message"])
```

- [ ] **Step 2：确认测试失败**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q tests/test_protocol.py
```

Expected: 收集错误 `FileNotFoundError`（schema 文件不存在）。

- [ ] **Step 3：写入 schema**

`protocol/v1/task.schema.json`：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/wanghr0318-dotcom/go-agentbox/protocol/v1/task.schema.json",
  "title": "init（宿主 → Worker）",
  "description": "type、bootstrap、protocol_versions 组成引导信封，永不改变；mode=task 时其余字段按 v1 校验。",
  "type": "object",
  "required": ["type", "bootstrap", "protocol_versions", "mode"],
  "properties": {
    "type": {"const": "init"},
    "bootstrap": {"const": 1},
    "protocol_versions": {"type": "array", "minItems": 1, "items": {"type": "integer"}},
    "mode": {"enum": ["task", "session"]},
    "task_id": {"$ref": "#/$defs/nonEmpty"},
    "attempt_id": {"$ref": "#/$defs/nonEmpty"},
    "attempt_no": {"type": "integer", "minimum": 1},
    "traceparent": {"type": "string"},
    "config_version": {"type": "string"},
    "input_refs": {"type": "array", "items": {"$ref": "#/$defs/sha256"}},
    "out_dir": {"$ref": "#/$defs/nonEmpty"},
    "resume": {"$ref": "#/$defs/resume"}
  },
  "if": {"properties": {"mode": {"const": "task"}}},
  "then": {"required": ["task_id", "attempt_id", "attempt_no", "out_dir"]},
  "$defs": {
    "nonEmpty": {"type": "string", "minLength": 1},
    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    "resume": {
      "type": "object",
      "required": ["checkpoint_id", "step_id"],
      "properties": {
        "checkpoint_id": {"$ref": "#/$defs/nonEmpty"},
        "step_id": {"$ref": "#/$defs/nonEmpty"},
        "state_ref": {"$ref": "#/$defs/sha256"},
        "refs": {"type": "array", "maxItems": 1024, "items": {"$ref": "#/$defs/sha256"}}
      },
      "oneOf": [
        {"required": ["state"], "not": {"required": ["state_ref"]}},
        {"required": ["state_ref"], "not": {"required": ["state"]}}
      ]
    }
  }
}
```

`protocol/v1/control.schema.json`：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/wanghr0318-dotcom/go-agentbox/protocol/v1/control.schema.json",
  "title": "宿主 → Worker 控制消息（init 除外）",
  "oneOf": [
    {"$ref": "#/$defs/checkpoint_result"},
    {"$ref": "#/$defs/artifact_result"},
    {"$ref": "#/$defs/cancel"},
    {"$ref": "#/$defs/pause"}
  ],
  "$defs": {
    "nonEmpty": {"type": "string", "minLength": 1},
    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    "checkpoint_result": {
      "type": "object",
      "required": ["type", "v", "checkpoint_id", "scope", "status"],
      "properties": {
        "type": {"const": "checkpoint_result"},
        "v": {"const": 1},
        "checkpoint_id": {"$ref": "#/$defs/nonEmpty"},
        "scope": {"enum": ["task", "session"]},
        "status": {"enum": ["committed", "conflict", "rejected", "retryable_error", "not_found"]},
        "code": {"type": "string"}
      }
    },
    "artifact_result": {
      "type": "object",
      "required": ["type", "v", "artifact_id", "status"],
      "properties": {
        "type": {"const": "artifact_result"},
        "v": {"const": 1},
        "artifact_id": {"$ref": "#/$defs/nonEmpty"},
        "status": {"enum": ["saved", "rejected"]},
        "version": {"type": "integer"},
        "sha256": {"type": "string"},
        "code": {"type": "string"}
      },
      "allOf": [
        {
          "if": {"properties": {"status": {"const": "saved"}}},
          "then": {"required": ["version", "sha256"], "properties": {"version": {"minimum": 1}, "sha256": {"$ref": "#/$defs/sha256"}}}
        },
        {
          "if": {"properties": {"status": {"const": "rejected"}}},
          "then": {"required": ["code"], "properties": {"code": {"minLength": 1}}}
        }
      ]
    },
    "stop": {
      "type": "object",
      "required": ["type", "v", "attempt_id"],
      "properties": {
        "v": {"const": 1},
        "attempt_id": {"$ref": "#/$defs/nonEmpty"},
        "reason": {"type": "string"},
        "grace_ms": {"type": "integer", "minimum": 0}
      }
    },
    "cancel": {"allOf": [{"$ref": "#/$defs/stop"}, {"properties": {"type": {"const": "cancel"}}}]},
    "pause": {"allOf": [{"$ref": "#/$defs/stop"}, {"properties": {"type": {"const": "pause"}}}]}
  }
}
```

`protocol/v1/event.schema.json`：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/wanghr0318-dotcom/go-agentbox/protocol/v1/event.schema.json",
  "title": "Worker → 宿主事件（task 模式）",
  "oneOf": [
    {"$ref": "#/$defs/ready"},
    {"$ref": "#/$defs/progress"},
    {"$ref": "#/$defs/artifact"},
    {"$ref": "#/$defs/checkpoint"},
    {"$ref": "#/$defs/checkpoint_query"},
    {"$ref": "#/$defs/paused"},
    {"$ref": "#/$defs/result"},
    {"$ref": "#/$defs/error"},
    {"$ref": "#/$defs/handshake_error"}
  ],
  "$defs": {
    "nonEmpty": {"type": "string", "minLength": 1},
    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    "artifactPath": {
      "type": "string",
      "pattern": "^(?!/)(?!.*(?:^|/)\\.\\.?(?:/|$))(?!.*//)(?!.*/$)[^\\u0000]{1,4096}$"
    },
    "event": {
      "type": "object",
      "required": ["type", "v", "seq"],
      "properties": {
        "v": {"const": 1},
        "seq": {"type": "integer", "minimum": 1},
        "ts": {"type": "string"}
      }
    },
    "ready": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["protocol_version", "mode", "worker"],
      "properties": {
        "type": {"const": "ready"},
        "protocol_version": {"const": 1},
        "mode": {"enum": ["task", "session"]},
        "worker": {
          "type": "object",
          "required": ["name"],
          "properties": {"name": {"$ref": "#/$defs/nonEmpty"}, "version": {"type": "string"}}
        },
        "capabilities": {"type": "array", "items": {"type": "string"}}
      }
    },
    "progress": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["kind"],
      "properties": {
        "type": {"const": "progress"},
        "step_id": {"type": "string"},
        "kind": {"$ref": "#/$defs/nonEmpty"},
        "message": {"type": "string"}
      }
    },
    "artifact": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["artifact_id", "path", "declared_sha256", "media_type", "visibility"],
      "properties": {
        "type": {"const": "artifact"},
        "artifact_id": {"$ref": "#/$defs/nonEmpty"},
        "path": {"$ref": "#/$defs/artifactPath"},
        "declared_sha256": {"$ref": "#/$defs/sha256"},
        "declared_size": {"type": "integer", "minimum": 0},
        "media_type": {"$ref": "#/$defs/nonEmpty"},
        "visibility": {"enum": ["output", "internal"]}
      }
    },
    "checkpoint": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["checkpoint_id", "scope", "step_id"],
      "properties": {
        "type": {"const": "checkpoint"},
        "checkpoint_id": {"$ref": "#/$defs/nonEmpty"},
        "scope": {"const": "task"},
        "step_id": {"$ref": "#/$defs/nonEmpty"},
        "state_ref": {"$ref": "#/$defs/sha256"},
        "refs": {"type": "array", "maxItems": 1024, "items": {"$ref": "#/$defs/sha256"}}
      },
      "oneOf": [
        {"required": ["state"], "not": {"required": ["state_ref"]}},
        {"required": ["state_ref"], "not": {"required": ["state"]}}
      ]
    },
    "checkpoint_query": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["checkpoint_id", "scope"],
      "properties": {
        "type": {"const": "checkpoint_query"},
        "checkpoint_id": {"$ref": "#/$defs/nonEmpty"},
        "scope": {"enum": ["task", "session"]}
      }
    },
    "paused": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["checkpoint_id"],
      "properties": {"type": {"const": "paused"}, "checkpoint_id": {"$ref": "#/$defs/nonEmpty"}}
    },
    "result": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "properties": {
        "type": {"const": "result"},
        "summary": {"type": "string"},
        "outputs": {"type": "array", "items": {"$ref": "#/$defs/nonEmpty"}}
      }
    },
    "error": {
      "allOf": [{"$ref": "#/$defs/event"}],
      "required": ["code"],
      "properties": {
        "type": {"const": "error"},
        "code": {"$ref": "#/$defs/nonEmpty"},
        "message": {"type": "string"},
        "retryable": {"type": "boolean"}
      }
    },
    "handshake_error": {
      "type": "object",
      "required": ["type", "bootstrap", "code"],
      "properties": {
        "type": {"const": "handshake_error"},
        "bootstrap": {"const": 1},
        "code": {"$ref": "#/$defs/nonEmpty"}
      }
    }
  }
}
```

- [ ] **Step 4：写入协议说明**

`protocol/README.md`：

````markdown
# Worker 协议 v1

宿主（Go 控制面）与 Worker 进程之间的协议：stdin 承载宿主控制消息，stdout 承载 Worker 事件，均为每行一个 JSON 对象（JSONL）；stderr 是自由格式日志。完整语义见 [v0.2 规格](../docs/design/2026-10-03-v0.2-first-release-design.md) §5。本目录当前覆盖 **task 模式**；session 与 sub-run 扩展属于后续里程碑。

## 三份契约

| 文件 | 约束什么 |
|---|---|
| `v1/task.schema.json`、`v1/control.schema.json`、`v1/event.schema.json` | 单条消息的格式（JSON Schema 2020-12） |
| 本文件 | 语义、顺序、错误码、提交规则 |
| `fixtures/v1/` | 可执行样例：Go（`internal/protocol`）与 Python（`agentbox_worker`）都必须通过 |

三者冲突时以规格为准，并在同一提交中修正三者。

## 版本规则

- `init` 的 `type`、`bootstrap: 1`、`protocol_versions` 是引导信封，永不改变。Worker 先只读这三项；没有共同版本时输出 `{"type":"handshake_error","bootstrap":1,"code":"no_common_version"}` 并以退出码 2 结束。
- 协商后每条消息带 `v`；Worker 事件带从 1 严格递增的 `seq`；`ts` 仅用于诊断。
- 同一主版本内：未知字段忽略，**未知消息类型是错误**。破坏性变更进入 v2。

## 消息

| 方向 | type | 必需字段 |
|---|---|---|
| 宿主→Worker | `init` | `bootstrap`、`protocol_versions`、`mode`；task 模式另需 `task_id`、`attempt_id`、`attempt_no ≥ 1`、`out_dir` |
| 宿主→Worker | `checkpoint_result` | `checkpoint_id`、`scope`、`status ∈ {committed, conflict, rejected, retryable_error, not_found}` |
| 宿主→Worker | `artifact_result` | `artifact_id`、`status`；`saved` 另需 `version ≥ 1` 与 `sha256`，`rejected` 另需 `code` |
| 宿主→Worker | `cancel`、`pause` | `attempt_id`；`grace_ms ≥ 0` |
| Worker→宿主 | `ready` | `protocol_version = 1`、`mode`、`worker.name` |
| Worker→宿主 | `progress` | `kind` |
| Worker→宿主 | `artifact` | `artifact_id`、`path`、`declared_sha256`、`media_type`、`visibility ∈ {output, internal}` |
| Worker→宿主 | `checkpoint` | `checkpoint_id`、`scope = task`、`step_id`；`state` 与 `state_ref` 必须且只能有一个 |
| Worker→宿主 | `checkpoint_query` | `checkpoint_id`、`scope` |
| Worker→宿主 | `paused` | `checkpoint_id` |
| Worker→宿主 | `result` | — |
| Worker→宿主 | `error` | `code` |
| Worker→宿主 | `handshake_error` | `bootstrap = 1`、`code`（不带 `v` 与 `seq`） |

**产物路径**：相对、非空、无 NUL、≤ 4096 字节、不含空、`.` 或 `..` 分量。**sha256**：64 个小写十六进制字符。

## 大小上限（UTF-8 字节）

单条 Worker 事件 1 MiB；`init` 1 MiB；其他宿主控制消息 16 KiB；inline state 256 KiB（按紧凑 JSON 计，空白不计）；每个 checkpoint 的 refs 1024 个。

## 判定顺序与错误码

依次检查：非 JSON 对象 → `malformed_json`；`type` 缺失、非字符串或不属于该方向 → `unknown_type`；超限 → `message_too_large`；字段类型错误 → `invalid_field`；然后是各类型的语义规则：`version_mismatch`、`missing_field`、`invalid_field`、`state_too_large`、`too_many_refs`、`path_invalid`。

## 事件流顺序（task 模式）

`seq` 从 1 严格递增（`seq_invalid`）；`ready` 之前只允许 `error`，用于启动失败（`before_ready`）；`ready` 只能出现一次（`duplicate_ready`）；终态提议 `result`、`error`、`paused` 至多一个，其后只允许 `checkpoint_query`（`after_terminal`）；`handshake_error` 只能是第一条（`handshake_error_misplaced`）且是唯一一条（`after_handshake_error`）。

## checkpoint 提交（Worker 视角）

- checkpoint 是否成立由宿主决定；收到 `committed` 之前不得把它当作已提交。
- 同一时刻至多一个在途提交。
- 没有收到结果时，用**同一** `checkpoint_id` 发送 `checkpoint_query`；收到 `retryable_error` 或 `not_found` 时，用同一 `checkpoint_id` 重发（新的 `seq`）。
- `conflict` 与 `rejected` 是确定的失败。
- 暂停是协作式的：到达提交边界、提交成功后发出 `paused`，其 `checkpoint_id` 是最新已提交的 checkpoint。

## SDK 错误码与退出码

`agentbox_worker` 在 `error` 事件中使用：`checkpoint_conflict`、`checkpoint_rejected`（false）、`checkpoint_unresolved`（true）、`artifact_rejected`（false）、`artifact_unresolved`（true）、`unsupported_mode`（false，`ready` 之前）、`control_protocol_error`（false）、`internal_error`（true）；括号内为 `retryable` 建议值。应用可通过 `WorkerFailure` 使用自定义 code。

退出码：0 表示已发出 `result`/`paused`，或被宿主取消（取消不发终态提议，stdin 关闭视为取消）；1 表示已发出 `error`、无法开始，或协议输出通道失效；2 表示 `handshake_error`。

发送与退出：seq 在开始发送时即被占用；发送失败或在途被取消后不再发送任何事件（不会复用 seq）。输入行在读取阶段按帧长上限判定，读到的行进入有界队列以形成背压；缓冲约为队列容量加一帧，另有对象开销，不是总内存的硬上限。进程入口在 `AGENTBOX_WORKER_SHUTDOWN_TIMEOUT` 秒（默认 5）内等待已提交的输出写完后退出；该期限只约束输出收尾，不覆盖 `register_artifact` 的哈希线程（遇到 FIFO 或阻塞读时可能无限等待），最终强制终止由宿主负责。checkpoint 的 `state` 按 JSON 往返归一化后提交（tuple 变为 list，非字符串键变为字符串，拒绝 NaN 与 Infinity）。

## fixtures

- `fixtures/v1/messages.json`：`valid` 中每条必须解码成功并能往返编码；`invalid` 中每条必须得到指定错误码（`raw` 表示原始行）。
- `fixtures/v1/scenarios/*.json`：`lines` 为按时间顺序的双向消息；`expect.stream` 为 `ok` 或 `violation`（含错误码与行号 `at`，从 0 起）。`layers` 含 `sdk` 的场景另由 Python SDK 驱动 sim-worker 逐条复现，`sdk.exit_code` 为期望退出码；回放时 `init.out_dir` 替换为临时目录，比较时忽略 `ts`。
- schema 必须接受全部合法消息与场景中的合法行，拒绝全部非原始行的非法消息。
````

- [ ] **Step 5：运行测试**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q
```

Expected: 全部通过，0 failed。

- [ ] **Step 6：提交**

```bash
cd /f/go-agentbox-m1-3 && git add protocol/v1 protocol/README.md worker/tests/test_protocol.py && git commit -m "docs(protocol): v1 JSON Schema 与协议说明，schema 与 fixtures 一致性测试

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5：SDK 基础设施——传输、事件发送与等待器

**风险：高（发送提交点、取消语义、输入背压与有界关闭）——双评审。**

**Files:**
- Create: `worker/agentbox_worker/errors.py`、`worker/agentbox_worker/transport.py`、`worker/agentbox_worker/outbox.py`
- Create: `worker/tests/test_sdk.py`（SDK 测试；Task 6、7 在末尾追加）

**Interfaces:**
- Consumes: Task 3 的 `encode_line`、`WORKER`、`VERSION`、`BOOTSTRAP_VERSION`、`ProtocolError`。
- Produces：
  - `errors.WorkerFailure(code: str, message: str, *, retryable: bool = False)`（属性 `code`、`message`、`retryable`；code 为空时 `ValueError`）；子类 `CheckpointRejected(code, message)`、`CheckpointUnresolved(message)`、`ArtifactRejected(message)`；`errors.TransportBroken(Exception)`（输出通道失效，不是 `WorkerFailure`）
  - `transport.Transport`（Protocol：`async receive() -> bytes | None`，`None` 表示 EOF，帧超长时抛 `ProtocolError("message_too_large")`；`async send(line: bytes) -> None`，调用方须串行化，失败时抛 `TransportBroken`）
  - `transport.StdioTransport(reader: BinaryIO, writer: BinaryIO, *, queue_size: int = 64)`：`receive`、`send`、`close(timeout: float) -> bool`（期限内等待已提交写出完成，返回是否全部成功写出）、`wait_reader_stopped(timeout: float) -> bool`（诊断与测试用）、属性 `pending_lines`；常量 `MAX_FRAME_BYTES`、`DEFAULT_QUEUE_SIZE`。读线程只使用线程安全队列与 `call_soon_threadsafe`，不创建协程；入队失败（事件循环已关闭）时立即停止读取
  - `transport.MemoryTransport()`：`feed(line: bytes | None)`、`sent: list[bytes]`、`on_send: Callable[[bytes], None] | None`
  - `outbox.Outbox(transport)`：`async emit(body: dict) -> None`（补 `v`、`seq`、`ts`；校验失败抛 `ProtocolError` 且不占用 seq；seq 在开始发送时提交；发送失败或在途取消后进入失效状态，此后一律抛 `TransportBroken`）、`async send_handshake_error(code: str) -> None`、属性 `last_seq`
  - `outbox.Waiters`：`expect(key: str) -> asyncio.Future[dict]`、`deliver(key: str, message: dict) -> bool`

- [ ] **Step 1：写失败的测试**

`worker/tests/test_sdk.py`：

```python
"""Worker SDK：传输与事件发送、运行时、SDK 层场景回放与 sim-worker（规格 §5.3–§5.9）。"""

import asyncio
import io
import json
import threading

import pytest

from agentbox_worker.errors import TransportBroken
from agentbox_worker.outbox import Outbox, Waiters
from agentbox_worker.protocol import ProtocolError
from agentbox_worker.transport import MAX_FRAME_BYTES, MemoryTransport, StdioTransport

# ---- 传输与事件发送 ----


def sent_json(transport: MemoryTransport) -> list[dict]:
    return [json.loads(line) for line in transport.sent]


def test_outbox_assigns_increasing_seq_version_and_ts():
    async def go():
        transport = MemoryTransport()
        outbox = Outbox(transport)
        await outbox.emit({"type": "progress", "kind": "step_started", "message": "a"})
        await outbox.emit({"type": "progress", "kind": "step_finished", "message": "b"})
        return transport, outbox.last_seq

    transport, last_seq = asyncio.run(go())
    events = sent_json(transport)
    assert [e["seq"] for e in events] == [1, 2]
    assert all(e["v"] == 1 and isinstance(e["ts"], str) for e in events)
    assert last_seq == 2


def test_outbox_invalid_event_does_not_consume_seq():
    async def go():
        transport = MemoryTransport()
        outbox = Outbox(transport)
        with pytest.raises(ProtocolError) as exc:
            await outbox.emit({"type": "progress", "message": "缺少 kind"})
        assert exc.value.code == "missing_field"
        await outbox.emit({"type": "progress", "kind": "step_started", "message": "ok"})
        return transport

    assert [e["seq"] for e in sent_json(asyncio.run(go()))] == [1]


def test_waiters_drop_late_and_unexpected_replies():
    async def go():
        waiters = Waiters()
        future = waiters.expect("cp-1")
        assert waiters.deliver("cp-2", {"x": 2}) is False
        assert waiters.deliver("cp-1", {"x": 1}) is True
        assert waiters.deliver("cp-1", {"x": 3}) is False
        return await future

    assert asyncio.run(go()) == {"x": 1}


def test_stdio_transport_round_trip():
    out = io.BytesIO()

    async def go():
        transport = StdioTransport(io.BytesIO(b"first\nsecond\r\n"), out)
        await transport.send(b'{"a":1}')
        await transport.send(b'{"b":2}')
        return [await transport.receive() for _ in range(3)]

    assert asyncio.run(go()) == [b"first", b"second", None]
    assert out.getvalue() == b'{"a":1}\n{"b":2}\n'


class BlockingTransport:
    """send 在 release 之前一直阻塞，用于确定性地模拟"写入中"。"""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.release = asyncio.Event()

    async def receive(self) -> bytes | None:
        return None

    async def send(self, line: bytes) -> None:
        self.sent.append(line)
        await self.release.wait()


class FailingTransport:
    def __init__(self) -> None:
        self.attempts = 0

    async def receive(self) -> bytes | None:
        return None

    async def send(self, line: bytes) -> None:
        self.attempts += 1
        raise OSError("broken pipe")


def test_cancel_during_send_breaks_outbox_without_reusing_seq():
    async def go():
        transport = BlockingTransport()
        outbox = Outbox(transport)
        task = asyncio.create_task(outbox.emit({"type": "progress", "kind": "k", "message": "a"}))
        while not transport.sent:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(TransportBroken):
            await outbox.emit({"type": "progress", "kind": "k", "message": "b"})
        return transport, outbox.last_seq

    transport, last_seq = asyncio.run(go())
    assert last_seq == 1
    assert [json.loads(line)["seq"] for line in transport.sent] == [1]


def test_send_failure_breaks_outbox():
    async def go():
        transport = FailingTransport()
        outbox = Outbox(transport)
        for _ in range(2):
            with pytest.raises(TransportBroken):
                await outbox.emit({"type": "progress", "kind": "k", "message": "a"})
        return transport.attempts

    assert asyncio.run(go()) == 1


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        pytest.param(MAX_FRAME_BYTES - 1, "accepted", id="at_limit"),
        pytest.param(MAX_FRAME_BYTES + 10, "message_too_large", id="over_limit"),
    ],
)
def test_stdio_frame_limit_is_checked_at_read_time(size, expected):
    line = b"a" * size

    async def go():
        transport = StdioTransport(io.BytesIO(line + b"\n"), io.BytesIO())
        try:
            got = await transport.receive()
        except ProtocolError as exc:
            return exc.code, await transport.receive()
        return ("accepted" if got == line else "wrong_line"), await transport.receive()

    assert asyncio.run(go()) == (expected, None)


class TrackingReader:
    """包装 BytesIO：记录 readline 调用次数，第 notify_at 次调用读完时触发 reached。"""

    def __init__(self, data: bytes, notify_at: int) -> None:
        self._inner = io.BytesIO(data)
        self._notify_at = notify_at
        self.calls = 0
        self.reached = threading.Event()

    def readline(self, size: int = -1) -> bytes:
        line = self._inner.readline(size)
        self.calls += 1
        if self.calls >= self._notify_at:
            self.reached.set()
        return line

    def tell(self) -> int:
        return self._inner.tell()


LINES = [f"line-{i}".encode() for i in range(100)]
QUEUE_SIZE = 2
# 取走第一行后，读线程最多再读 QUEUE_SIZE + 1 行：QUEUE_SIZE 行在队列中，1 行等待入队。
# 第 READ_LIMIT 次 readline 只能发生在第一行被取走之后；此后无人消费，读线程必然阻塞在入队上。
READ_LIMIT = QUEUE_SIZE + 2
READ_LIMIT_BYTES = sum(len(line) + 1 for line in LINES[:READ_LIMIT])


def tracking_input() -> TrackingReader:
    return TrackingReader(b"\n".join(LINES) + b"\n", notify_at=READ_LIMIT)


def test_stdio_reader_applies_backpressure():
    reader = tracking_input()

    async def go():
        transport = StdioTransport(reader, io.BytesIO(), queue_size=QUEUE_SIZE)
        first = await transport.receive()
        assert await asyncio.to_thread(reader.reached.wait, 5)
        snapshot = (reader.calls, reader.tell(), transport.pending_lines)
        rest = [await transport.receive() for _ in range(len(LINES))]
        return first, snapshot, rest

    first, snapshot, rest = asyncio.run(go())
    assert snapshot == (READ_LIMIT, READ_LIMIT_BYTES, QUEUE_SIZE)
    assert [first, *rest] == [*LINES, None]


def test_stdio_reader_stops_consuming_after_event_loop_closes():
    reader = tracking_input()
    transport = StdioTransport(reader, io.BytesIO(), queue_size=QUEUE_SIZE)

    async def go():
        first = await transport.receive()
        assert await asyncio.to_thread(reader.reached.wait, 5)
        return first

    assert asyncio.run(go()) == LINES[0]
    assert transport.wait_reader_stopped(5)
    assert (reader.calls, reader.tell()) == (READ_LIMIT, READ_LIMIT_BYTES)
```

- [ ] **Step 2：确认测试失败**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q tests/test_sdk.py
```

Expected: `ModuleNotFoundError: No module named 'agentbox_worker.errors'`。

- [ ] **Step 3：实现**

`worker/agentbox_worker/errors.py`：

```python
"""SDK 与应用共用的失败类型；SDK 把 WorkerFailure 转换为 error 事件（规格 §5.4）。"""

from __future__ import annotations


class WorkerFailure(Exception):
    """任务失败。retryable 只是给宿主的建议，是否重试由宿主决定。"""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        if not code:
            raise ValueError("WorkerFailure 的 code 不能为空")
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable


class CheckpointRejected(WorkerFailure):
    """宿主对 checkpoint 返回 conflict 或 rejected。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=False)


class CheckpointUnresolved(WorkerFailure):
    """重发与查询达到上限仍未得到结果；提交是否成立由宿主记录为准。"""

    def __init__(self, message: str) -> None:
        super().__init__("checkpoint_unresolved", message, retryable=True)


class ArtifactRejected(WorkerFailure):
    """宿主拒绝了登记的产物。"""

    def __init__(self, message: str) -> None:
        super().__init__("artifact_rejected", message, retryable=False)


class TransportBroken(Exception):
    """协议输出通道已不可用：某次发送失败或在途被取消，结果不确定。

    此后不再发送任何事件（不会复用 seq），也无法再发出 error 事件；Worker 以失败退出。
    """
```

`worker/agentbox_worker/transport.py`：

```python
"""按行收发协议消息的传输层。"""

from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable
from typing import BinaryIO, Protocol

from agentbox_worker.errors import TransportBroken
from agentbox_worker.protocol import MAX_INIT_BYTES, ProtocolError

# 一行（含行尾 \r\n）的最大字节数：init 的上限是 1 MiB，其他消息更小，由解码器细分。
MAX_FRAME_BYTES = MAX_INIT_BYTES + 2
DEFAULT_QUEUE_SIZE = 64

_OVERSIZED = object()  # 读线程放入队列的"超长帧"标记
_POLL_SECONDS = 0.05  # 读线程因背压阻塞时检查事件循环是否已关闭的间隔


class Transport(Protocol):
    async def receive(self) -> bytes | None:
        """返回下一行（不含换行符）；None 表示对端已关闭；帧超长时抛出 ProtocolError。"""

    async def send(self, line: bytes) -> None:
        """发送一行（不含换行符）。调用方须串行化；失败时抛出 TransportBroken。"""


def _settle(done: asyncio.Future[None], error: BaseException | None) -> None:
    if done.done():  # 发送方已取消等待
        return
    if error is None:
        done.set_result(None)
    else:
        done.set_exception(TransportBroken(f"写出失败：{error!r}"))


class StdioTransport:
    """基于二进制文件对象的行传输。

    - 读：守护线程以 readline(上限) 读取，超长帧在读取阶段即判定，之后停止读取；
      读到的行进入线程安全的有界队列，队列满时读线程阻塞，对宿主形成背压。读失败按对端关闭处理。
      读线程绑定首次 receive 所在的事件循环；该循环关闭后读线程不再读取输入并退出。
      输入缓冲约为 (queue_size + 1) 帧：队列中的 queue_size 帧，加上读线程正在读取或
      等待入队的一帧；另有对象与底层文件缓冲的开销。这是估算，不是总内存的硬上限。
    - 写：专用守护线程逐行写出并 flush，发送方等待写出完成；写失败后传输进入失败状态。
    - close(timeout)：在期限内等待已提交的写出完成。阻塞的读写线程不会阻止进程退出。
    """

    def __init__(
        self, reader: BinaryIO, writer: BinaryIO, *, queue_size: int = DEFAULT_QUEUE_SIZE
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._lines: queue.Queue[object] = queue.Queue(maxsize=queue_size)
        self._readable = asyncio.Event()
        self._read_thread: threading.Thread | None = None
        self._writes: queue.SimpleQueue[
            tuple[bytes, asyncio.AbstractEventLoop, asyncio.Future[None]] | None
        ] = queue.SimpleQueue()
        self._write_thread: threading.Thread | None = None
        self._failed: BaseException | None = None

    @property
    def pending_lines(self) -> int:
        """已读入、尚未被 receive 取走的行数（诊断用）。"""
        return self._lines.qsize()

    def wait_reader_stopped(self, timeout: float) -> bool:
        """在 timeout 秒内等待读线程结束；返回读线程是否已结束（诊断与测试用）。"""
        if self._read_thread is None:
            return True
        self._read_thread.join(timeout)
        return not self._read_thread.is_alive()

    async def receive(self) -> bytes | None:
        if self._read_thread is None:
            loop = asyncio.get_running_loop()
            self._read_thread = threading.Thread(target=self._read_loop, args=(loop,), daemon=True)
            self._read_thread.start()
        item = await self._next_line()
        if item is _OVERSIZED:
            raise ProtocolError("message_too_large", f"输入行超过 {MAX_FRAME_BYTES} 字节")
        return item  # type: ignore[return-value]

    async def _next_line(self) -> object:
        while True:
            self._readable.clear()  # 先清除再检查，避免漏掉检查与等待之间放入的行
            try:
                return self._lines.get_nowait()
            except queue.Empty:
                await self._readable.wait()

    def _read_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            while raw := self._reader.readline(MAX_FRAME_BYTES + 1):
                item = _OVERSIZED if len(raw) > MAX_FRAME_BYTES else raw.rstrip(b"\r\n")
                if not self._put(loop, item):
                    return  # 事件循环已关闭：不再消费输入
                if item is _OVERSIZED:
                    break  # 帧边界已丢失，不再继续读取
        except (OSError, ValueError):
            pass  # 读失败按对端关闭处理
        self._put(loop, None)

    def _put(self, loop: asyncio.AbstractEventLoop, item: object) -> bool:
        """放入有界队列并唤醒接收方；队列满时阻塞（背压）。返回是否已交付。

        读线程不创建协程，只在放入后以 call_soon_threadsafe 唤醒接收方；
        阻塞期间每 _POLL_SECONDS 检查一次事件循环，循环关闭后返回 False。
        """
        while not loop.is_closed():
            try:
                self._lines.put(item, timeout=_POLL_SECONDS)
            except queue.Full:
                continue
            try:
                loop.call_soon_threadsafe(self._readable.set)
            except RuntimeError:  # 放入后事件循环恰好关闭
                return False
            return True
        return False

    async def send(self, line: bytes) -> None:
        if self._failed is not None:
            raise TransportBroken(f"输出通道已失败：{self._failed!r}")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        if self._write_thread is None:
            self._write_thread = threading.Thread(target=self._write_loop, daemon=True)
            self._write_thread.start()
        self._writes.put((line, loop, done))
        await done

    def _write_loop(self) -> None:
        while (item := self._writes.get()) is not None:
            line, loop, done = item
            error: BaseException | None = None
            try:
                self._writer.write(line + b"\n")
                self._writer.flush()
            except (OSError, ValueError) as exc:  # 宿主关闭了 stdout，或文件已关闭
                error = self._failed = exc
            try:
                loop.call_soon_threadsafe(_settle, done, error)
            except RuntimeError:
                pass  # 事件循环已结束
            if error is not None:
                return

    def close(self, timeout: float) -> bool:
        """在 timeout 秒内等待已提交的写出完成；返回是否全部成功写出。"""
        if self._write_thread is None:
            return self._failed is None
        self._writes.put(None)
        self._write_thread.join(timeout)
        return not self._write_thread.is_alive() and self._failed is None


class MemoryTransport:
    """内存传输，供测试与场景回放使用。"""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.sent: list[bytes] = []
        self.on_send: Callable[[bytes], None] | None = None

    def feed(self, line: bytes | None) -> None:
        """送入一条宿主消息；None 模拟宿主关闭 stdin。"""
        self._queue.put_nowait(line)

    async def receive(self) -> bytes | None:
        return await self._queue.get()

    async def send(self, line: bytes) -> None:
        self.sent.append(line)
        if self.on_send is not None:
            self.on_send(line)
```

`worker/agentbox_worker/outbox.py`：

```python
"""Worker 事件的发送与宿主答复的等待。"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from agentbox_worker.errors import TransportBroken
from agentbox_worker.protocol import BOOTSTRAP_VERSION, VERSION, WORKER, encode_line
from agentbox_worker.transport import Transport


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Outbox:
    """为事件补上 v、严格递增的 seq 与 ts，校验后发出（规格 §5.3）。

    seq 的提交点是"校验通过、开始发送"：此后无论发送成功、失败还是被取消，该 seq 都已用掉。
    发送失败或在途被取消时结果不确定，Outbox 进入失效状态，此后的发送一律抛出
    TransportBroken，绝不以同一 seq 重发。校验失败抛出 ProtocolError，且不占用 seq。
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._seq = 0
        self._lock = asyncio.Lock()
        self._broken: str | None = None

    @property
    def last_seq(self) -> int:
        return self._seq

    async def emit(self, body: dict[str, Any]) -> None:
        async with self._lock:
            self._check_usable()
            message = {**body, "v": VERSION, "seq": self._seq + 1, "ts": _now()}
            line = encode_line(WORKER, message)
            self._seq += 1
            await self._send(line, f"seq={self._seq}")

    async def send_handshake_error(self, code: str) -> None:
        async with self._lock:
            self._check_usable()
            body = {"type": "handshake_error", "bootstrap": BOOTSTRAP_VERSION, "code": code}
            await self._send(encode_line(WORKER, body), "handshake_error")

    def _check_usable(self) -> None:
        if self._broken is not None:
            raise TransportBroken(self._broken)

    async def _send(self, line: bytes, what: str) -> None:
        try:
            await self._transport.send(line)
        except asyncio.CancelledError:
            self._broken = f"{what} 的发送在途被取消，结果不确定"
            raise
        except TransportBroken as exc:
            self._broken = str(exc)
            raise
        except Exception as exc:
            self._broken = f"{what} 发送失败：{exc!r}"
            raise TransportBroken(self._broken) from exc


class Waiters:
    """按键等待宿主答复；没有在等待的答复（迟到或重复）被丢弃。"""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    def expect(self, key: str) -> asyncio.Future[dict[str, Any]]:
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[key] = future
        return future

    def deliver(self, key: str, message: dict[str, Any]) -> bool:
        future = self._pending.pop(key, None)
        if future is None or future.done():
            return False
        future.set_result(message)
        return True
```

- [ ] **Step 4：格式化并运行测试**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run ruff format .; uv run ruff check .; uv run pytest -q
```

Expected: 全部通过。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-3 && git add worker/agentbox_worker/errors.py worker/agentbox_worker/transport.py worker/agentbox_worker/outbox.py worker/tests/test_sdk.py && git commit -m "feat(worker): SDK 传输层、事件发送与答复等待

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6：SDK 运行时——握手、控制消息、checkpoint、产物、暂停与取消

**风险：高（终态与提交语义）——双评审。**

**Files:**
- Create: `worker/agentbox_worker/runtime.py`
- Modify: `worker/agentbox_worker/__init__.py`（替换为下方完整内容）
- Modify: `worker/tests/test_sdk.py`（末尾追加运行时测试）

**Interfaces:**
- Consumes: Task 3 的 `decode_line`、`HOST`、`VERSION`、`BOOTSTRAP_VERSION`、`ProtocolError`、`valid_artifact_path`；Task 5 的 `Outbox`、`Waiters`、`Transport`、`StdioTransport`、`WorkerFailure` 及其子类。
- Produces（Task 7 与后续计划依赖）：
  - `Result(summary: str, outputs: list[str] = [])`、`Paused(checkpoint_id: str)`、`ResumeInfo(checkpoint_id, step_id, state, state_ref, refs)`、`ArtifactRef(artifact_id, version, sha256)`、`Timing(ack_timeout=10.0, max_ack_attempts=5, retry_backoff=1.0, artifact_timeout=300.0)`
  - `TaskContext`：属性 `task_id`、`attempt_id`、`attempt_no`、`config`、`out_dir: Path`、`resume: ResumeInfo | None`、`cancel_reason: str | None`；方法 `should_pause() -> bool`、`async progress(kind, message, *, step_id=None, data=None)`、`async checkpoint(step_id, *, state=None, state_ref=None, refs=()) -> str`、`async register_artifact(artifact_id, path, *, media_type, visibility="output") -> ArtifactRef`
  - `async run_worker(app, transport, *, name, version, capabilities=(), timing=Timing(), new_id=None) -> int`（输出通道失效时返回 `EXIT_FAILURE`，不再尝试发送）
  - `main(app, *, name, version, capabilities=()) -> NoReturn`：进程入口策略见"错误码与契约约定"——异常按失败处理、期限内刷新已提交输出、`os._exit`；Task 7 的五个真实进程测试是其回归测试
  - `checkpoint` 在首次提交前以 JSON 往返生成与调用方对象无关的快照，所有重试复用它；快照按 JSON 归一化（tuple 变为 list，非字符串键变为字符串）；不可序列化（含 NaN、Infinity）时抛 `WorkerFailure("invalid_field")`
  - `register_artifact` 的哈希在线程池中计算，等待不保证有限；关闭期限不覆盖它，最终强制终止由宿主负责
  - 退出码常量 `EXIT_OK = 0`、`EXIT_FAILURE = 1`、`EXIT_HANDSHAKE = 2`；`SHUTDOWN_TIMEOUT_ENV = "AGENTBOX_WORKER_SHUTDOWN_TIMEOUT"`；包根导出 `TransportBroken`

- [ ] **Step 1：写失败的测试**

在 `worker/tests/test_sdk.py` **末尾追加**（与上文空两行）：

```python
# ---- 运行时：握手、控制消息、checkpoint、产物 ----
# 正常路径、查询、重发、冲突、暂停、取消与恢复由 SDK 层场景逐条回放（见下一节），此处不重复。

import hashlib
import itertools
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agentbox_worker import Result, TaskContext, Timing, WorkerFailure, run_worker

FAST = Timing(ack_timeout=0.05, max_ack_attempts=3, retry_backoff=0.01, artifact_timeout=0.5)
INIT = {
    "type": "init",
    "bootstrap": 1,
    "protocol_versions": [1],
    "mode": "task",
    "task_id": "t-1",
    "attempt_id": "a-1",
    "attempt_no": 1,
}

Responder = Callable[[dict[str, Any], MemoryTransport], None]


def run(
    app,
    tmp_path: Path,
    *,
    host_lines: list[bytes | None] = (),
    responder: Responder | None = None,
    init: dict[str, Any] | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    async def go():
        transport = MemoryTransport()
        transport.feed(json.dumps({**INIT, "out_dir": str(tmp_path), **(init or {})}).encode())
        for line in host_lines:
            transport.feed(line)
        if responder is not None:
            transport.on_send = lambda line: responder(json.loads(line), transport)
        counter = itertools.count(1)
        code = await asyncio.wait_for(
            run_worker(
                app,
                transport,
                name="test-worker",
                version="0",
                timing=FAST,
                new_id=lambda: f"cp-{next(counter)}",
            ),
            timeout=5,
        )
        return code, sent_json(transport)

    return asyncio.run(go())


def reply(transport: MemoryTransport, message: dict[str, Any]) -> None:
    transport.feed(json.dumps({"v": 1, **message}).encode())


def checkpoint_reply(status: str) -> Responder:
    def respond(msg: dict[str, Any], transport: MemoryTransport) -> None:
        if msg["type"] == "checkpoint":
            reply(
                transport,
                {
                    "type": "checkpoint_result",
                    "checkpoint_id": msg["checkpoint_id"],
                    "scope": "task",
                    "status": status,
                },
            )

    return respond


def types(events: list[dict[str, Any]]) -> list[str]:
    return [e["type"] for e in events]


async def returns_ok(ctx: TaskContext) -> Result:
    return Result("ok", [])


async def sleeps(ctx: TaskContext) -> Result:
    await asyncio.sleep(10)
    return Result("never", [])


def test_ready_checkpoint_artifact_result(tmp_path):
    content = "# 报告".encode() + b"\n"
    (tmp_path / "report.md").write_bytes(content)

    def respond(msg, transport):
        checkpoint_reply("committed")(msg, transport)
        if msg["type"] == "artifact":
            reply(
                transport,
                {
                    "type": "artifact_result",
                    "artifact_id": msg["artifact_id"],
                    "status": "saved",
                    "version": 2,
                    "sha256": msg["declared_sha256"],
                },
            )

    async def app(ctx):
        checkpoint_id = await ctx.checkpoint("s1", state={"n": 1})
        ref = await ctx.register_artifact("report", "report.md", media_type="text/markdown")
        return Result(f"{checkpoint_id}/v{ref.version}", [ref.artifact_id])

    code, events = run(app, tmp_path, responder=respond)
    assert code == 0
    assert types(events) == ["ready", "checkpoint", "artifact", "result"]
    ready, checkpoint, artifact, result = events
    assert ready["worker"] == {"name": "test-worker", "version": "0"}
    assert checkpoint["checkpoint_id"] == "cp-1" and checkpoint["state"] == {"n": 1}
    assert checkpoint["refs"] == [] and "state_ref" not in checkpoint
    assert artifact["declared_sha256"] == hashlib.sha256(content).hexdigest()
    assert artifact["declared_size"] == len(content) and artifact["visibility"] == "output"
    assert result["summary"] == "cp-1/v2" and result["outputs"] == ["report"]


def test_unsupported_mode_fails_before_ready(tmp_path):
    code, events = run(returns_ok, tmp_path, init={"mode": "session"})
    assert code == 1
    assert types(events) == ["error"]
    assert events[0]["code"] == "unsupported_mode" and events[0]["retryable"] is False


@pytest.mark.parametrize(
    ("raised", "code", "retryable"),
    [
        pytest.param(
            WorkerFailure("bad_input", "输入缺少字段"), "bad_input", False, id="worker_failure"
        ),
        pytest.param(KeyError("boom"), "internal_error", True, id="unexpected_exception"),
    ],
)
def test_app_failure_becomes_error_event(tmp_path, raised, code, retryable):
    async def app(ctx):
        raise raised

    exit_code, events = run(app, tmp_path)
    assert exit_code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == code and events[1]["retryable"] is retryable


def test_stdin_eof_is_treated_as_cancel(tmp_path):
    code, events = run(sleeps, tmp_path, host_lines=[None])
    assert code == 0
    assert types(events) == ["ready"]


def test_invalid_control_message_fails_task(tmp_path):
    code, events = run(sleeps, tmp_path, host_lines=[b'{"type":"bogus","v":1}'])
    assert code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == "control_protocol_error"


def test_only_one_checkpoint_in_flight(tmp_path):
    in_flight = 0
    overlaps: list[str] = []

    def respond(msg, transport):
        nonlocal in_flight
        if msg["type"] != "checkpoint":
            return
        if in_flight:
            overlaps.append(msg["checkpoint_id"])
        in_flight += 1

        def deliver():
            nonlocal in_flight
            in_flight -= 1
            checkpoint_reply("committed")(msg, transport)

        asyncio.get_running_loop().call_later(0.02, deliver)

    async def app(ctx):
        async with asyncio.TaskGroup() as group:
            group.create_task(ctx.checkpoint("s1", state={}))
            group.create_task(ctx.checkpoint("s2", state={}))
        return Result("ok", [])

    code, events = run(app, tmp_path, responder=respond)
    assert code == 0 and overlaps == []
    assert [e["checkpoint_id"] for e in events if e["type"] == "checkpoint"] == ["cp-1", "cp-2"]


def test_unresolved_after_max_attempts(tmp_path):
    async def app(ctx):
        await ctx.checkpoint("s1", state={})
        return Result("never", [])

    code, events = run(app, tmp_path)
    assert code == 1
    assert types(events).count("checkpoint_query") == FAST.max_ack_attempts
    assert events[-1]["code"] == "checkpoint_unresolved" and events[-1]["retryable"] is True


@pytest.mark.parametrize(
    ("state", "code"),
    [
        pytest.param("x" * (256 << 10), "state_too_large", id="oversized"),
        pytest.param({"x": object()}, "invalid_field", id="unserializable"),
    ],
)
def test_bad_state_fails_without_sending(tmp_path, state, code):
    async def app(ctx):
        await ctx.checkpoint("s1", state=state)
        return Result("never", [])

    exit_code, events = run(app, tmp_path)
    assert exit_code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == code


def test_checkpoint_retries_use_snapshot_of_state(tmp_path):
    state = {"n": 1}
    replies = iter(["retryable_error", "committed"])

    def respond(msg, transport):
        if msg["type"] == "checkpoint":
            state["n"] = 99  # 提交后修改调用方对象，不得影响同一 checkpoint 的重试内容
            checkpoint_reply(next(replies))(msg, transport)

    async def app(ctx):
        return Result(await ctx.checkpoint("s1", state=state), [])

    code, events = run(app, tmp_path, responder=respond)
    sent = [e for e in events if e["type"] == "checkpoint"]
    assert code == 0
    assert [e["state"] for e in sent] == [{"n": 1}, {"n": 1}]


@pytest.mark.parametrize(
    ("path", "code", "sent"),
    [
        pytest.param(
            "x.txt", "artifact_rejected", ["ready", "artifact", "error"], id="host_rejects"
        ),
        pytest.param("../in/x", "path_invalid", ["ready", "error"], id="invalid_path_local"),
    ],
)
def test_artifact_failure_fails_task(tmp_path, path, code, sent):
    (tmp_path / "x.txt").write_bytes(b"x")

    def respond(msg, transport):
        if msg["type"] == "artifact":
            reply(
                transport,
                {
                    "type": "artifact_result",
                    "artifact_id": msg["artifact_id"],
                    "status": "rejected",
                    "code": "hash_mismatch",
                },
            )

    async def app(ctx):
        await ctx.register_artifact("x", path, media_type="text/plain")
        return Result("never", [])

    exit_code, events = run(app, tmp_path, responder=respond)
    assert exit_code == 1
    assert types(events) == sent and events[-1]["code"] == code


class BreaksAfterReady(MemoryTransport):
    async def send(self, line: bytes) -> None:
        if self.sent:
            raise OSError("broken pipe")
        await super().send(line)


def test_broken_transport_ends_worker_with_failure(tmp_path):
    async def app(ctx):
        await ctx.progress("step_started", "x")
        return Result("never", [])

    async def go():
        transport = BreaksAfterReady()
        transport.feed(json.dumps({**INIT, "out_dir": str(tmp_path)}).encode())
        code = await asyncio.wait_for(
            run_worker(app, transport, name="w", version="0", timing=FAST), timeout=5
        )
        return code, types(sent_json(transport))

    assert asyncio.run(go()) == (1, ["ready"])
```

- [ ] **Step 2：确认测试失败**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q tests/test_sdk.py
```

Expected: 收集错误 `ImportError: cannot import name 'Result' from 'agentbox_worker'`。

- [ ] **Step 3：实现**

`worker/agentbox_worker/runtime.py`：

```python
"""Worker SDK 运行时：握手、事件发送、控制消息处理、checkpoint 与产物登记。

只提供运行协议与执行能力；研究策略、提示词等业务逻辑不在 SDK 中。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn

from agentbox_worker.errors import (
    ArtifactRejected,
    CheckpointRejected,
    CheckpointUnresolved,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.outbox import Outbox, Waiters
from agentbox_worker.protocol import (
    BOOTSTRAP_VERSION,
    HOST,
    VERSION,
    ProtocolError,
    decode_line,
    valid_artifact_path,
)
from agentbox_worker.transport import StdioTransport, Transport

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_HANDSHAKE = 2

SHUTDOWN_TIMEOUT_ENV = "AGENTBOX_WORKER_SHUTDOWN_TIMEOUT"


@dataclass(frozen=True)
class Result:
    summary: str
    outputs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Paused:
    checkpoint_id: str


@dataclass(frozen=True)
class ResumeInfo:
    checkpoint_id: str
    step_id: str
    state: Any
    state_ref: str | None
    refs: list[str]


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    version: int
    sha256: str


@dataclass(frozen=True)
class Timing:
    ack_timeout: float = 10.0
    max_ack_attempts: int = 5
    retry_backoff: float = 1.0
    artifact_timeout: float = 300.0


App = Callable[["TaskContext"], Awaitable["Result | Paused"]]


def _log(message: str) -> None:
    print(f"agentbox_worker: {message}", file=sys.stderr)


def _resume_info(raw: dict[str, Any] | None) -> ResumeInfo | None:
    if raw is None:
        return None
    return ResumeInfo(
        checkpoint_id=raw["checkpoint_id"],
        step_id=raw["step_id"],
        state=raw.get("state"),
        state_ref=raw.get("state_ref") or None,
        refs=list(raw.get("refs") or []),
    )


def _snapshot(state: Any) -> Any:
    """生成与调用方对象无关的状态快照；同一 checkpoint 的所有重试都使用它（规格 §5.5）。

    快照经 JSON 往返归一化：tuple 变为 list，非字符串键变为字符串，NaN 与 Infinity 被拒绝。
    """
    try:
        return json.loads(json.dumps(state, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise WorkerFailure("invalid_field", f"checkpoint state 无法序列化为 JSON：{exc}") from exc


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


class TaskContext:
    """应用看到的任务上下文。"""

    def __init__(
        self, init: dict[str, Any], outbox: Outbox, timing: Timing, new_id: Callable[[], str]
    ) -> None:
        self.task_id: str = init["task_id"]
        self.attempt_id: str = init["attempt_id"]
        self.attempt_no: int = init["attempt_no"]
        self.config: Any = init.get("config")
        self.out_dir = Path(init["out_dir"])
        self.resume = _resume_info(init.get("resume"))
        self.cancel_reason: str | None = None
        self.control_error: ProtocolError | None = None
        self._outbox = outbox
        self._timing = timing
        self._new_id = new_id
        self._checkpoint_results = Waiters()
        self._artifact_results = Waiters()
        self._checkpoint_lock = asyncio.Lock()
        self._pause_requested = False

    def should_pause(self) -> bool:
        """宿主是否请求了暂停。应用在提交边界检查它（规格 §5.9）。"""
        return self._pause_requested

    async def progress(
        self, kind: str, message: str, *, step_id: str | None = None, data: Any = None
    ) -> None:
        body: dict[str, Any] = {"type": "progress"}
        if step_id is not None:
            body["step_id"] = step_id
        body["kind"] = kind
        body["message"] = message
        if data is not None:
            body["data"] = data
        await self._emit(body)

    async def checkpoint(
        self,
        step_id: str,
        *,
        state: Any = None,
        state_ref: str | None = None,
        refs: Iterable[str] = (),
    ) -> str:
        """提交 checkpoint，宿主确认已提交后返回 checkpoint_id（规格 §5.5）。

        同一时刻至多一个在途提交；结果丢失时用同一 ID 查询，
        retryable_error 或 not_found 时用同一 ID 重发。state 在首次提交前按 JSON 归一化
        生成快照（tuple 变为 list，非字符串键变为字符串），无法序列化或含 NaN、Infinity 时
        抛出 WorkerFailure("invalid_field")；之后修改调用方对象不影响重试内容。
        """
        async with self._checkpoint_lock:
            checkpoint_id = self._new_id()
            body: dict[str, Any] = {
                "type": "checkpoint",
                "checkpoint_id": checkpoint_id,
                "scope": "task",
                "step_id": step_id,
            }
            if state_ref is None:
                body["state"] = _snapshot(state)
            else:
                body["state_ref"] = state_ref
            body["refs"] = list(refs)
            status, code = await self._submit_checkpoint(checkpoint_id, body)
        if status == "committed":
            return checkpoint_id
        detail = f"checkpoint {checkpoint_id}: {status}" + (f" ({code})" if code else "")
        error_code = "checkpoint_conflict" if status == "conflict" else "checkpoint_rejected"
        raise CheckpointRejected(error_code, detail)

    async def _submit_checkpoint(
        self, checkpoint_id: str, body: dict[str, Any]
    ) -> tuple[str, str | None]:
        pending = self._checkpoint_results.expect(checkpoint_id)
        await self._emit(body)
        for _ in range(self._timing.max_ack_attempts):
            try:
                result = await asyncio.wait_for(asyncio.shield(pending), self._timing.ack_timeout)
            except TimeoutError:
                query = {
                    "type": "checkpoint_query",
                    "checkpoint_id": checkpoint_id,
                    "scope": "task",
                }
                await self._emit(query)
                continue
            if result["status"] not in ("retryable_error", "not_found"):
                return result["status"], result.get("code")
            await asyncio.sleep(self._timing.retry_backoff)
            pending = self._checkpoint_results.expect(checkpoint_id)
            await self._emit(body)
        raise CheckpointUnresolved(
            f"checkpoint {checkpoint_id}: {self._timing.max_ack_attempts} 次后仍无确定结果"
        )

    async def register_artifact(
        self, artifact_id: str, path: str, *, media_type: str, visibility: str = "output"
    ) -> ArtifactRef:
        """登记 out_dir 下的产物；宿主保存并校验后返回其版本（规格 §5.6）。

        哈希在线程池中计算，不能保证有限时间内完成：path 若是 FIFO 或读操作阻塞，线程会一直
        等待且无法取消。任务被取消后 asyncio.run 退出时仍会等待该线程，进程停在那里，尚未进入
        main() 的输出收尾；最终强制终止由宿主负责。
        """
        if not valid_artifact_path(path):
            raise WorkerFailure("path_invalid", f"产物路径不合法：{path!r}")
        sha256, size = await asyncio.to_thread(_hash_file, self.out_dir / path)
        pending = self._artifact_results.expect(artifact_id)
        await self._emit(
            {
                "type": "artifact",
                "artifact_id": artifact_id,
                "path": path,
                "declared_sha256": sha256,
                "declared_size": size,
                "media_type": media_type,
                "visibility": visibility,
            }
        )
        try:
            result = await asyncio.wait_for(pending, self._timing.artifact_timeout)
        except TimeoutError as exc:
            message = f"artifact {artifact_id}: 等待结果超时"
            raise WorkerFailure("artifact_unresolved", message, retryable=True) from exc
        if result["status"] == "saved":
            return ArtifactRef(artifact_id, result["version"], result["sha256"])
        raise ArtifactRejected(f"artifact {artifact_id}: rejected ({result.get('code', '')})")

    async def _emit(self, body: dict[str, Any]) -> None:
        try:
            await self._outbox.emit(body)
        except ProtocolError as exc:
            raise WorkerFailure(exc.code, exc.detail) from exc

    def _handle_control(self, msg: dict[str, Any]) -> bool:
        """处理一条宿主控制消息；返回 False 表示收到 cancel，控制循环结束。"""
        typ = msg["type"]
        if typ == "checkpoint_result":
            self._checkpoint_results.deliver(msg["checkpoint_id"], msg)
        elif typ == "artifact_result":
            self._artifact_results.deliver(msg["artifact_id"], msg)
        elif typ == "pause":
            self._pause_requested = True
        elif typ == "cancel":
            self.cancel_reason = msg.get("reason") or "cancel"
            return False
        else:
            raise ProtocolError("control_protocol_error", f"运行中收到 {typ}")
        return True


def _supports_protocol(line: bytes) -> bool:
    """只读引导信封判断是否有共同版本（规格 §5.2）；信封本身不合法时交由完整校验报错。"""
    try:
        envelope = json.loads(line)
    except ValueError:
        return True
    if not isinstance(envelope, dict) or envelope.get("type") != "init":
        return True
    if envelope.get("bootstrap") != BOOTSTRAP_VERSION:
        return True
    versions = envelope.get("protocol_versions")
    return not isinstance(versions, list) or VERSION in versions


async def _read_init(transport: Transport, outbox: Outbox) -> dict[str, Any] | int:
    """读取并校验 init；无法开始时返回退出码。"""
    try:
        line = await transport.receive()
    except ProtocolError as exc:  # 例如输入帧超长
        _log(f"读取 init 失败：{exc}")
        return EXIT_FAILURE
    if line is None:
        _log("未收到 init：stdin 已关闭")
        return EXIT_FAILURE
    if not _supports_protocol(line):
        await outbox.send_handshake_error("no_common_version")
        return EXIT_HANDSHAKE
    try:
        init = decode_line(HOST, line)
    except ProtocolError as exc:
        _log(f"init 不合法：{exc}")
        return EXIT_FAILURE
    if init["type"] != "init":
        _log(f"第一条消息必须是 init，收到 {init['type']}")
        return EXIT_FAILURE
    if init["mode"] != "task":
        await outbox.emit(
            {
                "type": "error",
                "code": "unsupported_mode",
                "message": f"不支持的模式：{init['mode']}",
                "retryable": False,
            }
        )
        return EXIT_FAILURE
    return init


async def run_worker(
    app: App,
    transport: Transport,
    *,
    name: str,
    version: str,
    capabilities: Iterable[str] = (),
    timing: Timing = Timing(),  # noqa: B008  Timing 不可变
    new_id: Callable[[], str] | None = None,
) -> int:
    """运行一个 task 模式 Worker，返回进程退出码。

    协议输出通道失效（TransportBroken）时无法再发出任何事件，直接以 EXIT_FAILURE 结束。
    """
    try:
        return await _run_worker(app, transport, name, version, capabilities, timing, new_id)
    except TransportBroken as exc:
        _log(f"协议输出通道失效：{exc}")
        return EXIT_FAILURE


async def _run_worker(
    app: App,
    transport: Transport,
    name: str,
    version: str,
    capabilities: Iterable[str],
    timing: Timing,
    new_id: Callable[[], str] | None,
) -> int:
    outbox = Outbox(transport)
    init = await _read_init(transport, outbox)
    if isinstance(init, int):
        return init
    ctx = TaskContext(init, outbox, timing, new_id or (lambda: uuid.uuid4().hex))
    await outbox.emit(
        {
            "type": "ready",
            "protocol_version": VERSION,
            "mode": "task",
            "worker": {"name": name, "version": version},
            "capabilities": list(capabilities),
        }
    )
    return await _run_task(app, ctx, transport)


async def _control_loop(ctx: TaskContext, transport: Transport, app_task: asyncio.Task) -> None:
    while True:
        try:
            line = await transport.receive()
            if line is None:
                ctx.cancel_reason = "stdin_closed"
                app_task.cancel()
                return
            keep_going = ctx._handle_control(decode_line(HOST, line))
        except ProtocolError as exc:
            ctx.control_error = exc
            app_task.cancel()
            return
        if not keep_going:
            app_task.cancel()
            return


async def _run_task(app: App, ctx: TaskContext, transport: Transport) -> int:
    app_task = asyncio.create_task(app(ctx))
    control_task = asyncio.create_task(_control_loop(ctx, transport, app_task))
    try:
        outcome = await app_task
    except asyncio.CancelledError:
        if ctx.control_error is not None:
            failure = WorkerFailure("control_protocol_error", str(ctx.control_error))
            return await _emit_failure(ctx, failure)
        if ctx.cancel_reason is None:
            raise
        return EXIT_OK  # 宿主取消：不发终态提议，由宿主按其意图裁决（规格 §5.8）
    except WorkerFailure as exc:
        return await _emit_failure(ctx, exc)
    except Exception as exc:
        failure = WorkerFailure("internal_error", f"{type(exc).__name__}: {exc}", retryable=True)
        return await _emit_failure(ctx, failure)
    finally:
        control_task.cancel()
    return await _emit_outcome(ctx, outcome)


async def _emit_outcome(ctx: TaskContext, outcome: Any) -> int:
    try:
        if isinstance(outcome, Paused):
            await ctx._emit({"type": "paused", "checkpoint_id": outcome.checkpoint_id})
            return EXIT_OK
        if isinstance(outcome, Result):
            body = {"type": "result", "summary": outcome.summary, "outputs": list(outcome.outputs)}
            await ctx._emit(body)
            return EXIT_OK
    except WorkerFailure as exc:
        return await _emit_failure(ctx, exc)
    failure = WorkerFailure("internal_error", f"应用返回了未知结果：{outcome!r}", retryable=True)
    return await _emit_failure(ctx, failure)


async def _emit_failure(ctx: TaskContext, failure: WorkerFailure) -> int:
    await ctx._outbox.emit(
        {
            "type": "error",
            "code": failure.code,
            "message": failure.message,
            "retryable": failure.retryable,
        }
    )
    return EXIT_FAILURE


def _shutdown_timeout() -> float:
    try:
        return float(os.environ.get(SHUTDOWN_TIMEOUT_ENV, "5"))
    except ValueError:
        return 5.0


def main(app: App, *, name: str, version: str, capabilities: Iterable[str] = ()) -> NoReturn:
    """进程入口。stdin/stdout 承载协议；应用的 print 被改到 stderr，避免混入协议流。

    这是 SDK 中唯一调用 os._exit 的地方，退出策略固定为：运行 Worker（任何异常都记录并按
    失败处理）→ 在期限内等待已提交的协议输出写完 → os._exit。不走正常的解释器收尾：读线程
    可能阻塞在 stdin 的 readline 中，写线程可能因宿主停止读取 stdout 而阻塞在 write 中，
    二者都无法取消或 join；正常收尾会挂起，或以 "Fatal Python error: _enter_buffered_busy"
    崩溃。

    期限由环境变量 AGENTBOX_WORKER_SHUTDOWN_TIMEOUT 设置（秒，默认 5），只约束协议输出的
    收尾，不是整个 Worker 的退出期限：asyncio.run 返回前会等待线程池中的线程（例如
    register_artifact 的哈希线程），这段等待不受该期限约束。最终强制终止由宿主负责。
    """
    protocol_out = sys.stdout.buffer
    sys.stdout = sys.stderr
    transport = StdioTransport(sys.stdin.buffer, protocol_out)
    code = EXIT_FAILURE
    try:
        code = asyncio.run(
            run_worker(app, transport, name=name, version=version, capabilities=capabilities)
        )
    except BaseException:  # noqa: B036  进程入口：任何异常都必须走有界退出
        traceback.print_exc()
    finally:
        if not transport.close(_shutdown_timeout()):
            _log("退出时仍有未写出的协议输出（宿主可能已停止读取 stdout）")
        sys.stderr.flush()
        os._exit(code)
```

`worker/agentbox_worker/__init__.py`（完整替换）：

```python
"""go-agentbox Python Worker SDK：协议 v1（task 模式）。

SDK 只提供运行协议与执行能力：握手、事件、checkpoint、产物登记、暂停与取消。
研究策略、提示词等业务逻辑属于具体 Worker。
"""

from agentbox_worker.errors import (
    ArtifactRejected,
    CheckpointRejected,
    CheckpointUnresolved,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.runtime import (
    EXIT_FAILURE,
    EXIT_HANDSHAKE,
    EXIT_OK,
    ArtifactRef,
    Paused,
    Result,
    ResumeInfo,
    TaskContext,
    Timing,
    main,
    run_worker,
)
from agentbox_worker.transport import MemoryTransport, StdioTransport, Transport

__all__ = [
    "EXIT_FAILURE",
    "EXIT_HANDSHAKE",
    "EXIT_OK",
    "ArtifactRef",
    "ArtifactRejected",
    "CheckpointRejected",
    "CheckpointUnresolved",
    "MemoryTransport",
    "Paused",
    "Result",
    "ResumeInfo",
    "StdioTransport",
    "TaskContext",
    "Timing",
    "Transport",
    "TransportBroken",
    "WorkerFailure",
    "main",
    "run_worker",
]
```

- [ ] **Step 4：格式化并运行测试**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run ruff format .; uv run ruff check .; uv run pytest -q
```

Expected: 全部通过。代码中只有两处 `noqa`：`B008`（`Timing` 不可变）与 `B036`（进程入口必须捕获所有异常以走有界退出）；其他 ruff 问题按提示修正，不得用 `noqa` 掩盖。

- [ ] **Step 5：提交**

```bash
cd /f/go-agentbox-m1-3 && git add worker/agentbox_worker/runtime.py worker/agentbox_worker/__init__.py worker/tests/test_sdk.py && git commit -m "feat(worker): SDK 运行时——握手、控制消息、checkpoint、产物、暂停与取消

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7：sim-worker、SDK 层场景回放与真实进程端到端测试

**风险：高（跨语言联合验收）——双评审。**

**Files:**
- Create: `worker/sim_worker/__init__.py`、`worker/sim_worker/app.py`、`worker/sim_worker/__main__.py`
- Create: `protocol/fixtures/v1/scenarios/` 下 9 个 SDK 层场景（下方给出）
- Modify: `worker/tests/test_sdk.py`（末尾追加 SDK 层场景回放与 sim-worker 测试）
- Create: `worker/tests/test_process.py`（真实子进程测试）

**Interfaces:**
- Consumes: Task 6 的 SDK 全部导出；Task 2 的场景格式。
- Produces: `sim_worker.NAME = "sim-worker"`、`sim_worker.VERSION = "0.1.0"`、`sim_worker.app.run(ctx) -> Result | Paused`；`python -m sim_worker` 入口。sim 配置格式：

| op | 字段 | 行为 |
|---|---|---|
| `progress` | `kind`、`message`、`step_id?` | 发出 progress |
| `sleep` | `ms` | 等待 |
| `checkpoint` | `step_id` | 以 `{"next_index": 下一步序号}` 提交；提交后若有暂停请求则返回 `Paused` |
| `allocate_mb` | `mb` | 分配并逐页写入内存，持有至进程结束 |
| `fail` | `code`、`message?`、`retryable?` | 抛出 `WorkerFailure` |
| `exit` | `code` | 立即 `os._exit(code)`，模拟崩溃 |
| `artifact` | `artifact_id`、`path`、`content`、`media_type?`、`visibility?` | 写入 out_dir 后登记 |
| `print` | `message` | `print()`，验证不会混入协议流 |
| `flood` | `count`、`size?`（默认 1000）、`step_id?` | 连续发出 `count` 条 progress，用于制造 stdout 背压 |

顶层另有 `summary`（默认 `"done"`）与 `outputs`（默认 `[]`）。恢复时从 `resume.state.next_index` 继续。

- [ ] **Step 1：写入 9 个 SDK 层场景**

`protocol/fixtures/v1/scenarios/happy_path.json`：

```json
{
  "name": "happy_path",
  "description": "握手、进度、checkpoint 提交、成功结束",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "progress", "step_id": "s1", "kind": "step_started", "message": "开始"}, {"op": "checkpoint", "step_id": "s1"}, {"op": "progress", "step_id": "s2", "kind": "step_finished", "message": "完成"}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 2, "step_id": "s1", "kind": "step_started", "message": "开始"}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 3, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 2}, "refs": []}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 4, "step_id": "s2", "kind": "step_finished", "message": "完成"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 5, "summary": "完成", "outputs": []}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/resume_after_crash.json`：

```json
{
  "name": "resume_after_crash",
  "description": "新 attempt 从已提交 checkpoint 的 next_index 继续",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-2", "attempt_no": 2, "out_dir": "/workspace/out/a-2", "config": {"steps": [{"op": "progress", "step_id": "s1", "kind": "step_started", "message": "开始"}, {"op": "checkpoint", "step_id": "s1"}, {"op": "progress", "step_id": "s2", "kind": "step_finished", "message": "完成"}], "summary": "完成", "outputs": []}, "resume": {"checkpoint_id": "cp-1", "step_id": "s1", "state": {"next_index": 2}}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 2, "step_id": "s2", "kind": "step_finished", "message": "完成"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 3, "summary": "完成", "outputs": []}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/ack_lost_retry_same_result.json`：

```json
{
  "name": "ack_lost_retry_same_result",
  "description": "checkpoint_result 丢失时用同一 checkpoint_id 查询",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "checkpoint", "step_id": "s1"}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "worker", "message": {"type": "checkpoint_query", "v": 1, "seq": 3, "checkpoint_id": "cp-1", "scope": "task"}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 4, "summary": "完成", "outputs": []}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/retryable_error_then_committed.json`：

```json
{
  "name": "retryable_error_then_committed",
  "description": "retryable_error 后用同一 checkpoint_id、新 seq 重发",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "checkpoint", "step_id": "s1"}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "retryable_error"}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 3, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 4, "summary": "完成", "outputs": []}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/checkpoint_conflict.json`：

```json
{
  "name": "checkpoint_conflict",
  "description": "宿主返回 conflict 时任务以 checkpoint_conflict 失败",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "checkpoint", "step_id": "s1"}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "conflict"}},
    {"from": "worker", "message": {"type": "error", "v": 1, "seq": 3, "code": "checkpoint_conflict", "message": "checkpoint cp-1: conflict", "retryable": false}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 1}
}
```

`protocol/fixtures/v1/scenarios/pause_at_boundary.json`：

```json
{
  "name": "pause_at_boundary",
  "description": "收到 pause 后在下一个提交边界暂停，paused 指向最新已提交 checkpoint",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "checkpoint", "step_id": "s1"}, {"op": "progress", "step_id": "s2", "kind": "step_started", "message": "继续"}, {"op": "checkpoint", "step_id": "s2"}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "checkpoint", "v": 1, "seq": 2, "checkpoint_id": "cp-1", "scope": "task", "step_id": "s1", "state": {"next_index": 1}, "refs": []}},
    {"from": "host", "message": {"type": "pause", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 5000}},
    {"from": "host", "message": {"type": "checkpoint_result", "v": 1, "checkpoint_id": "cp-1", "scope": "task", "status": "committed"}},
    {"from": "worker", "message": {"type": "paused", "v": 1, "seq": 3, "checkpoint_id": "cp-1"}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/cancel_during_run.json`：

```json
{
  "name": "cancel_during_run",
  "description": "取消时 Worker 停止且不发终态提议",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "progress", "step_id": "s1", "kind": "step_started", "message": "开始"}, {"op": "sleep", "ms": 60000}], "summary": "完成", "outputs": []}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "progress", "v": 1, "seq": 2, "step_id": "s1", "kind": "step_started", "message": "开始"}},
    {"from": "host", "message": {"type": "cancel", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 5000}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

`protocol/fixtures/v1/scenarios/handshake_no_common_version.json`：

```json
{
  "name": "handshake_no_common_version",
  "description": "没有共同协议版本时只输出 handshake_error",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [2], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": []}}},
    {"from": "worker", "message": {"type": "handshake_error", "bootstrap": 1, "code": "no_common_version"}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 2}
}
```

`protocol/fixtures/v1/scenarios/artifact_registered.json`：

```json
{
  "name": "artifact_registered",
  "description": "写入并登记产物，result 引用其 artifact_id",
  "layers": ["protocol", "sdk"],
  "lines": [
    {"from": "host", "message": {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "task", "task_id": "t-1", "attempt_id": "a-1", "attempt_no": 1, "out_dir": "/workspace/out/a-1", "config": {"steps": [{"op": "artifact", "artifact_id": "report", "path": "report.md", "content": "# 报告\n", "media_type": "text/markdown"}], "summary": "完成", "outputs": ["report"]}}},
    {"from": "worker", "message": {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task", "worker": {"name": "sim-worker", "version": "0.1.0"}, "capabilities": []}},
    {"from": "worker", "message": {"type": "artifact", "v": 1, "seq": 2, "artifact_id": "report", "path": "report.md", "declared_sha256": "f43f2ea1c219e0f4ef34c65f4184e1d8fcecc360ca5b7a477318ec35fc0e107e", "declared_size": 9, "media_type": "text/markdown", "visibility": "output"}},
    {"from": "host", "message": {"type": "artifact_result", "v": 1, "artifact_id": "report", "status": "saved", "version": 1, "sha256": "f43f2ea1c219e0f4ef34c65f4184e1d8fcecc360ca5b7a477318ec35fc0e107e"}},
    {"from": "worker", "message": {"type": "result", "v": 1, "seq": 3, "summary": "完成", "outputs": ["report"]}}
  ],
  "expect": {"stream": "ok"},
  "sdk": {"exit_code": 0}
}
```

- [ ] **Step 2：写失败的测试**

在 `worker/tests/test_sdk.py` **末尾追加**（与上文空两行）：

```python
# ---- SDK 层场景回放与 sim-worker ----
# 用 SDK 驱动 sim-worker 逐条复现 layers 含 "sdk" 的场景：宿主行按顺序送入内存传输；每当
# Worker 发出一行，就与场景中下一条 Worker 行比较（忽略 ts），再送入其后连续的宿主行。
# init.out_dir 替换为临时目录。

from protocol_fixtures import load_scenarios

from agentbox_worker.stream import StreamChecker
from sim_worker import NAME, VERSION
from sim_worker.app import run as sim_app

SCENARIO_TIMING = Timing(
    ack_timeout=0.05, max_ack_attempts=5, retry_backoff=0.01, artifact_timeout=1.0
)
SDK_SCENARIOS = [s for s in load_scenarios() if "sdk" in s["layers"]]
assert len(SDK_SCENARIOS) >= 9, "SDK 层场景缺失"


def replay_sdk(scenario: dict[str, Any], out_dir: str) -> tuple[int, list[str], int]:
    lines = scenario["lines"]
    problems: list[str] = []
    cursor = 0

    async def go() -> int:
        nonlocal cursor
        transport = MemoryTransport()

        def pump() -> None:
            nonlocal cursor
            while cursor < len(lines) and lines[cursor]["from"] == "host":
                message = dict(lines[cursor]["message"])
                if message.get("type") == "init":
                    message["out_dir"] = out_dir
                transport.feed(json.dumps(message, ensure_ascii=False).encode())
                cursor += 1

        def on_send(raw: bytes) -> None:
            nonlocal cursor
            got = json.loads(raw)
            got.pop("ts", None)
            if cursor >= len(lines) or lines[cursor]["from"] != "worker":
                problems.append(f"多出的 Worker 消息：{got}")
                return
            want = lines[cursor]["message"]
            if got != want:
                problems.append(f"第 {cursor} 行不一致：期望 {want}，得到 {got}")
            cursor += 1
            pump()

        transport.on_send = on_send
        pump()
        counter = itertools.count(1)
        code = await asyncio.wait_for(
            run_worker(
                sim_app,
                transport,
                name=NAME,
                version=VERSION,
                timing=SCENARIO_TIMING,
                new_id=lambda: f"cp-{next(counter)}",
            ),
            timeout=10,
        )
        checker = StreamChecker()
        for raw in transport.sent:
            checker.observe(json.loads(raw))
        return code

    code = asyncio.run(go())
    return code, problems, cursor


@pytest.mark.parametrize("scenario", SDK_SCENARIOS, ids=lambda s: s["name"])
def test_sdk_reproduces_scenario(scenario, tmp_path):
    code, problems, cursor = replay_sdk(scenario, str(tmp_path))
    assert problems == []
    assert cursor == len(scenario["lines"]), "场景中仍有未出现的行"
    assert code == scenario["sdk"]["exit_code"]


@pytest.mark.parametrize(
    ("steps", "config", "code", "last"),
    [
        pytest.param(
            [{"op": "fail", "code": "sim_failure", "message": "x", "retryable": True}],
            {},
            1,
            {"code": "sim_failure", "retryable": True},
            id="fail",
        ),
        pytest.param(
            [{"op": "allocate_mb", "mb": 4}],
            {"summary": "分配完成"},
            0,
            {"summary": "分配完成"},
            id="allocate_mb",
        ),
        pytest.param([{"op": "teleport"}], {}, 1, {"code": "sim_bad_config"}, id="unknown_op"),
        pytest.param([], {}, 0, {"summary": "done", "outputs": []}, id="defaults"),
    ],
)
def test_sim_worker_ops(tmp_path, steps, config, code, last):
    exit_code, events = run(sim_app, tmp_path, init={"config": {"steps": steps, **config}})
    assert exit_code == code
    assert events[-1] == {**events[-1], **last}
```

`worker/tests/test_process.py`：

```python
"""以真实子进程运行 `python -m sim_worker`：协议、退出码、print 改道与有界退出。

进程控制属于必须在 Linux 上验证的部分；Windows 上的结果仅供参考（见计划"平台覆盖"）。
"""

import json
import os
import subprocess
import sys
import threading
import time

from agentbox_worker.transport import MAX_FRAME_BYTES

# 固定子进程 stderr 编码：Windows 上默认使用本地代码页（如 GBK），断言中文日志会失败。
ENV = {**os.environ, "AGENTBOX_WORKER_SHUTDOWN_TIMEOUT": "1", "PYTHONIOENCODING": "utf-8"}
CANCEL = b'{"type":"cancel","v":1,"attempt_id":"a-1","reason":"user","grace_ms":0}\n'


def init_line(tmp_path, steps: list[dict]) -> bytes:
    init = {
        "type": "init",
        "bootstrap": 1,
        "protocol_versions": [1],
        "mode": "task",
        "task_id": "t-1",
        "attempt_id": "a-1",
        "attempt_no": 1,
        "out_dir": str(tmp_path),
        "config": {"steps": steps, "summary": "完成", "outputs": []},
    }
    return json.dumps(init).encode() + b"\n"


def start() -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "sim_worker"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=ENV,
    )


def stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None and not stream.closed:
            try:
                stream.close()
            except OSError:
                pass


def test_process_result_and_print_goes_to_stderr(tmp_path):
    proc = start()
    try:
        steps = [
            {"op": "progress", "step_id": "s1", "kind": "step_started", "message": "start"},
            {"op": "print", "message": "printed-marker"},
        ]
        proc.stdin.write(init_line(tmp_path, steps))
        proc.stdin.flush()
        events = [json.loads(line) for line in proc.stdout]
        code = proc.wait(timeout=20)
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
    finally:
        stop(proc)
    assert code == 0
    assert [e["type"] for e in events] == ["ready", "progress", "result"]
    assert "printed-marker" in stderr


def test_process_exits_while_host_keeps_stdin_open(tmp_path):
    proc = start()
    try:
        proc.stdin.write(init_line(tmp_path, []))
        proc.stdin.flush()  # stdin 保持打开，Worker 仍须在结束后退出
        events = [json.loads(line) for line in proc.stdout]
        code = proc.wait(timeout=20)
    finally:
        stop(proc)
    assert code == 0 and events[-1]["type"] == "result"


def test_process_exits_after_cancel_when_host_stops_reading_stdout(tmp_path):
    proc = start()
    try:
        proc.stdin.write(init_line(tmp_path, [{"op": "flood", "count": 5000, "size": 1000}]))
        proc.stdin.flush()
        time.sleep(1.0)  # 不读取 stdout：Worker 写满管道后阻塞在写出上
        proc.stdin.write(CANCEL)
        proc.stdin.flush()
        started = time.monotonic()
        code = proc.wait(timeout=20)
        elapsed = time.monotonic() - started
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
    finally:
        stop(proc)
    assert code == 0
    assert elapsed < 10
    assert "未写出" in stderr


def test_process_fails_fast_when_stdout_is_closed(tmp_path):
    proc = start()
    proc.stdout.close()
    try:
        proc.stdin.write(init_line(tmp_path, [{"op": "flood", "count": 5000, "size": 1000}]))
        proc.stdin.flush()
        code = proc.wait(timeout=20)
    finally:
        stop(proc)
    assert code == 1


def test_process_rejects_oversized_input_line(tmp_path):
    proc = start()

    def feed() -> None:
        try:
            proc.stdin.write(b"a" * (MAX_FRAME_BYTES + 10))
            proc.stdin.flush()
        except OSError:
            pass  # Worker 已在超限后退出

    writer = threading.Thread(target=feed, daemon=True)
    writer.start()
    try:
        code = proc.wait(timeout=20)
        out = proc.stdout.read()
    finally:
        stop(proc)
    assert code == 1
    assert out == b""
```

- [ ] **Step 3：确认测试失败**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run pytest -q tests/test_sdk.py tests/test_process.py
```

Expected: `ModuleNotFoundError: No module named 'sim_worker'`。

- [ ] **Step 4：实现**

`worker/sim_worker/__init__.py`：

```python
"""sim-worker：按 init.config 中的步骤脚本运行，用于协议测试与故障实验。"""

NAME = "sim-worker"
VERSION = "0.1.0"
```

`worker/sim_worker/app.py`：

```python
"""sim-worker 的步骤执行。配置格式见 Plan 3 Task 7；恢复时从 checkpoint 状态的 next_index 继续。"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from agentbox_worker import Paused, Result, TaskContext, WorkerFailure

Op = dict[str, Any]
Held = list[bytearray]
Handler = Callable[[TaskContext, Op, int, Held], Awaitable["Paused | None"]]


async def run(ctx: TaskContext) -> Result | Paused:
    config = ctx.config if isinstance(ctx.config, dict) else {}
    steps: list[Op] = config.get("steps", [])
    held: Held = []
    for index in range(_start_index(ctx), len(steps)):
        op = steps[index]
        handler = _HANDLERS.get(op.get("op", ""))
        if handler is None:
            raise WorkerFailure("sim_bad_config", f"第 {index} 步：未知操作 {op.get('op')!r}")
        outcome = await handler(ctx, op, index, held)
        if outcome is not None:
            return outcome
    return Result(summary=config.get("summary", "done"), outputs=list(config.get("outputs", [])))


def _start_index(ctx: TaskContext) -> int:
    if ctx.resume is None or not isinstance(ctx.resume.state, dict):
        return 0
    return int(ctx.resume.state.get("next_index", 0))


async def _progress(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    await ctx.progress(
        op.get("kind", "step_started"), op.get("message", ""), step_id=op.get("step_id")
    )


async def _sleep(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    await asyncio.sleep(op["ms"] / 1000)


async def _checkpoint(ctx: TaskContext, op: Op, index: int, held: Held) -> Paused | None:
    checkpoint_id = await ctx.checkpoint(op["step_id"], state={"next_index": index + 1})
    return Paused(checkpoint_id) if ctx.should_pause() else None


async def _allocate(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    buffer = bytearray(op["mb"] * 1024 * 1024)
    for offset in range(0, len(buffer), 4096):
        buffer[offset] = 1  # 逐页写入，确保真实占用内存
    held.append(buffer)


async def _fail(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    raise WorkerFailure(op["code"], op.get("message", ""), retryable=bool(op.get("retryable")))


async def _exit(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    sys.stderr.flush()
    os._exit(int(op["code"]))


async def _artifact(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    path = ctx.out_dir / op["path"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(op["content"].encode("utf-8"))
    await ctx.register_artifact(
        op["artifact_id"],
        op["path"],
        media_type=op.get("media_type", "application/octet-stream"),
        visibility=op.get("visibility", "output"),
    )


async def _print(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    print(op["message"])


async def _flood(ctx: TaskContext, op: Op, index: int, held: Held) -> None:
    message = "x" * op.get("size", 1000)
    for _ in range(op["count"]):
        await ctx.progress("flood", message, step_id=op.get("step_id"))


_HANDLERS: dict[str, Handler] = {
    "progress": _progress,
    "sleep": _sleep,
    "checkpoint": _checkpoint,
    "allocate_mb": _allocate,
    "fail": _fail,
    "exit": _exit,
    "artifact": _artifact,
    "print": _print,
    "flood": _flood,
}
```

`worker/sim_worker/__main__.py`：

```python
from agentbox_worker import main
from sim_worker import NAME, VERSION
from sim_worker.app import run

main(run, name=NAME, version=VERSION)
```

- [ ] **Step 5：运行全部 Python 测试**

```powershell
cd F:\go-agentbox-m1-3\worker; uv run ruff format .; uv run ruff check .; uv run lint-imports; uv run pytest -q
```

Expected: ruff 无问题；`lint-imports` 报告 1 个契约 KEPT；pytest 全部通过。

- [ ] **Step 6：Go 侧验证新场景**

```powershell
wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && go test -count=1 -v ./internal/protocol/ 2>&1 | grep -cE "^    --- PASS: TestScenarioFixtures/"; go test -count=1 ./internal/protocol/'
```

Expected: 计数为 21（12 个协议层 + 9 个 SDK 层场景）；最后一行 `ok`。

- [ ] **Step 7：提交**

```bash
cd /f/go-agentbox-m1-3 && git add worker/sim_worker protocol/fixtures/v1/scenarios worker/tests/test_sdk.py worker/tests/test_process.py && git commit -m "feat(worker): sim-worker、SDK 层场景回放与真实进程端到端测试

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8：CI 的 Python 作业

**风险：低（配置）——单次评审。**

**Files:**
- Modify: `.github/workflows/ci.yml`（在 `jobs:` 下追加 `python` 作业，完整内容如下）

**Interfaces:**
- Consumes: Task 3–7 的 `worker/` 项目与测试。
- Produces: CI 作业 `python (3.11)`、`python (3.13)`。

- [ ] **Step 1：追加作业**

在 `.github/workflows/ci.yml` 末尾（`linux-integration` 作业之后）追加：

```yaml
  # Python Worker SDK：最低支持版本 3.11 必须实际覆盖。
  python:
    runs-on: ubuntu-24.04
    strategy:
      fail-fast: false
      matrix:
        python: ["3.11", "3.13"]
    defaults:
      run:
        working-directory: worker
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
        with:
          version: "0.11.14"
      - name: 安装依赖（锁文件必须最新）
        run: uv sync --locked --python ${{ matrix.python }}
      - name: ruff（正确性与禁用 API）
        run: uv run ruff check .
      - name: ruff 格式
        run: uv run ruff format --check .
      - name: 模块依赖契约
        run: uv run lint-imports
      - name: pytest（含真实进程端到端测试）
        run: uv run pytest -q
      - name: 复杂度报告（只报告，不阻断）
        run: uv run ruff check --select C90 --exit-zero .
```

- [ ] **Step 2：本地校验 YAML**

```powershell
cd F:\go-agentbox-m1-3; python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml', encoding='utf-8')); print('YAML-OK')"
```

Expected: 输出 `YAML-OK`（使用 Windows 上已有的 Anaconda `python`）。

- [ ] **Step 3：提交**

```bash
cd /f/go-agentbox-m1-3 && git add .github/workflows/ci.yml && git commit -m "ci: Python 作业（3.11 与 3.13）：ruff、格式、模块依赖契约、pytest、复杂度报告

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## 联合验收（计划完成的判定）

全部任务完成后，主 agent 依次执行：

1. **Go（WSL）**：
   ```powershell
   wsl -d Ubuntu -- bash -c 'export PATH=$PATH:/usr/local/go/bin; cd /mnt/f/go-agentbox-m1-3 && go build ./... && go vet ./... && go test -count=1 ./internal/protocol/ ./tools/...'
   ```
   ```powershell
   wsl -d Ubuntu -- bash /mnt/f/go-agentbox-m1-3/scripts/dev/gofmt-staged.sh
   ```
   Expected: `ok` 与 `GOFMT-OK`。
2. **Python（Windows，可移植部分）**：
   ```powershell
   cd F:\go-agentbox-m1-3\worker; uv sync --locked; uv run ruff check .; uv run ruff format --check .; uv run lint-imports; uv run pytest -q
   ```
   Expected: 全部通过。
3. **跨语言一致性**：Go 的 `TestMessageFixtures`、`TestScenarioFixtures` 与 Python 的 `test_valid_message_round_trips`、`test_invalid_message_has_expected_code`、`test_scenario_stream`、`test_schema_*`、`test_sdk_reproduces_scenario` 全部通过，且读取的是同一批 fixtures 文件（`protocol/fixtures/v1/messages.json` 与 21 个场景）。
4. **CI（Linux，判定依据）**：推送分支并开 Draft PR（`Refs #4`），确认 `correctness`、`complexity-report`、`linux-integration`、`python (3.11)`、`python (3.13)` 全部实际执行并通过；`test_process.py` 的五个真实进程测试（保持 stdin 打开时退出、停止读取 stdout 后取消、stdout 被关闭、输入超长、print 改道）在 Linux 上通过。
5. **范围核对**：`git diff --stat m1-local-provider...HEAD` 只包含本计划 Files 中列出的文件。

计划完成后向项目负责人汇报：实现的接口、测试与 CI 结果、偏离计划之处及原因。合并由项目负责人决定。

---

## 自查记录

- **规格覆盖**：§5.1 通道（SDK 的 stdin/stdout/stderr 处理、print 改道）；§5.2 引导与 `handshake_error`（Task 6、场景 `handshake_no_common_version`）；§5.3 公共规则与阶段（Task 2、3 的事件流检查）；§5.4 task 模式消息（Task 1、3）；§5.5 第 1、4、5 条（state 二选一、一个在途提交、同 ID 查询与重发）；§5.8 由宿主裁决（SDK 取消时不发终态提议）；§5.9 协作式暂停与取消；§5.10 大小上限；§5.11 fixtures（本计划覆盖协议层与 SDK 层场景，宿主层场景随 Plan 5 加入同一目录）。session 与 sub-run 扩展不在本计划范围。
- **评审修正（第二轮）**：seq 提交点与取消语义（Task 5，`test_cancel_during_send_breaks_outbox_without_reusing_seq`）；有界关闭（Task 6 `main`，Task 7 三个真实进程场景）；读取阶段限长与有界背压（Task 5，`test_stdio_frame_limit_is_checked_at_read_time`、`test_stdio_reader_applies_backpressure`，Task 7 `test_process_rejects_oversized_input_line`）；checkpoint 快照（Task 6，`test_checkpoint_retries_use_snapshot_of_state`）。
- **评审修正（第三轮）**：读线程改用线程安全的有界队列，不再从读线程创建协程；`_put` 返回是否交付，事件循环关闭后读线程立即停止读取（`test_stdio_reader_stops_consuming_after_event_loop_closes`，已做变异验证：忽略返回值时读线程在循环关闭后读完全部 100 行，测试失败）；背压测试以同步屏障断言输入读取位置，不再依赖固定 sleep；缓冲上限、快照 JSON 归一化与关闭期限的范围写清；e2e 测试固定子进程 `PYTHONIOENCODING=utf-8`（Windows 默认代码页下断言中文日志会失败）。
- **测试布局（第四轮，按代码组织设计 §9.3）**：Go 由 4 个测试文件、13 个函数合并为 `protocol_test.go` 的 5 个函数；Go 专用的事件流规则表删除，其中场景未覆盖的三条（seq 重复、seq 不从 1 开始、handshake_error 不在首位）改为协议层场景 fixtures，Python 侧随之获得同样覆盖。Python 由 9 个测试文件、64 个函数合并为 `test_protocol.py`（8）、`test_sdk.py`（22）、`test_process.py`（5）共 35 个函数：已由 SDK 层场景逐条回放的运行时用例（正常路径、握手失败、取消、查询、重发、冲突、产物、暂停、恢复）合并为一个正常路径测试；同类错误码改为参数化；删除测试辅助类与简单取值的用例。三轮评审的回归测试全部保留。各任务完成时的中间状态（Task 1、3、5、6）均单独通过 ruff 与 pytest。
- **演练**：本计划的全部代码已在临时目录按计划文本组装并运行，组装结果与验证目录逐文件一致：Go vet/test 通过，21 个场景通过；Python ruff、格式、模块依赖契约通过，pytest 在 Windows 3.13 与 3.11 上各 274 项通过（`-W error::RuntimeWarning`，3.11 连续三次）；Linux（WSL 离线，Python 3.14）上 `test_sdk.py` 与 `test_process.py` 共 42 项通过（连续三次），含全部真实进程测试；`test_protocol.py` 依赖已编译的 jsonschema 依赖，离线 WSL 无法安装，由 Windows 与 CI 覆盖。
- **占位符**：无。
- **类型一致性**：`run_worker(app, transport, *, name, version, capabilities, timing, new_id)`、`Timing(ack_timeout, max_ack_attempts, retry_backoff, artifact_timeout)`、`TaskContext.checkpoint(step_id, *, state, state_ref, refs)`、`register_artifact(artifact_id, path, *, media_type, visibility)` 在 Task 6、7 的测试与实现中一致；Go 的 `DecodeLine`、`EncodeLine`、`WorkerStream.Observe`、`CodeOf` 在 Task 1、2 中一致。
