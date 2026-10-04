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

**产物路径**：相对、非空、无 NUL、≤ 4096 字节、不含空、`.` 或 `..` 分量。JSON Schema 只能按字符计长，不表达 4096 字节上限，该上限由编解码器检查（`path_invalid`）。**sha256**：64 个小写十六进制字符。

## 大小上限（UTF-8 字节）

单条 Worker 事件 1 MiB；`init` 1 MiB；其他宿主控制消息 16 KiB；inline state 256 KiB（按紧凑 JSON 计，空白不计）；每个 checkpoint 的 refs 1024 个。

## 判定顺序与错误码

依次检查：行长超过任何类型上限中的最大值（1 MiB）→ `message_too_large`，不解析；行不是严格的 UTF-8（含 BOM 前缀）、不是 JSON 对象或嵌套过深 → `malformed_json`；`type` 缺失、非字符串或不属于该方向 → `unknown_type`；超过该类型的上限 → `message_too_large`；字段类型错误 → `invalid_field`；然后是各类型的语义规则：`version_mismatch`、`missing_field`、`invalid_field`、`state_too_large`、`too_many_refs`、`path_invalid`。

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
