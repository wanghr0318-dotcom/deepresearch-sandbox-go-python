# Worker 协议 v1

宿主（Go 控制面）与 Worker 进程之间的协议：stdin 承载宿主控制消息，stdout 承载 Worker 事件，均为每行一个 JSON 对象（JSONL）；stderr 是自由格式日志。完整语义见 [v0.2 规格](../docs/design/2026-10-03-v0.2-first-release-design.md) §5。本目录覆盖 **task 模式**、**session 扩展**与 **sub-run 扩展**（见文末两节）。

## 三份契约

| 文件 | 约束什么 |
|---|---|
| `v1/task.schema.json`、`v1/control.schema.json`、`v1/event.schema.json` | task 模式单条消息的格式（JSON Schema 2020-12） |
| `v1/session.schema.json` | session 模式单条消息的格式：宿主消息按 `#/$defs/host`，Worker 事件按 `#/$defs/worker` |
| `v1/subrun.schema.json` | sub-run 扩展：五个 `subrun_*` 消息按 `#/$defs/host` / `#/$defs/worker`；其他消息中的扩展字段按 `#/$defs/fields` 与上面各 schema 叠加 |
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

**traceparent**（`init` 与 `task_start` 的可选字段）：W3C Trace Context 版本 00 的 `traceparent`，标识宿主一侧的 `worker.run` span。Worker 可把它原样作为 Gateway 请求的 `traceparent` 头，使自己发起的调用加入同一条 trace；Gateway 只接受 trace-id 属于本 attempt 的值。格式规则与共享向量见 `fixtures/v1/traceparent.json`（Go 的 `internal/obs.ParseTraceparent` 与 Python 的 `agentbox_worker.tracecontext` 都按它测试）。宿主未启用追踪时不发送该字段。

## 大小上限（UTF-8 字节）

单条 Worker 事件 1 MiB；`init` 1 MiB；其他宿主控制消息 16 KiB；inline state 256 KiB（按紧凑 JSON 计，空白不计）；每个 checkpoint 的 refs 1024 个。

## 判定顺序与错误码

依次检查：行长超过任何类型上限中的最大值（1 MiB）→ `message_too_large`，不解析；行不是严格的 UTF-8（含 BOM 前缀）、不是 JSON 对象，或违反 JSON 结构限制（嵌套超过 64 层；任意层级的重复键，按解码后的键名比较，孤立代理项转义按 U+FFFD 计；数字字面量超过 32 个字符；浮点字面量上溢为无穷或非零值下溢为零）→ `malformed_json`；`type` 缺失（键名区分大小写）、非字符串或不属于该方向 → `unknown_type`；超过该类型的大小上限 → `message_too_large`；`v` 不是整数形式的 1 → `version_mismatch`；与已定义字段只差大小写的键（按消息类型逐层检查，含 `resume`、`worker`）→ `invalid_field`；字段类型错误（整数字段只接受整数形式且在 int64 范围内）→ `invalid_field`；然后按各类型的语义规则依次检查。

| JSON 结构限制（对整行生效，自由格式字段同样受限） | 上限 |
|---|---|
| 对象与数组的嵌套层数（消息顶层对象计 1） | 64 |
| 数字字面量的字符数（含负号、小数点、指数符号与指数正负号） | 32 |
| 重复键（任意层级，按解码后的键名比较；孤立代理项转义按 U+FFFD 计） | 不允许 |
| 浮点字面量 | 不得上溢为无穷；非零值不得下溢为零 |

违反上述限制为 `malformed_json`，在消息类型判定之前检查；编码时同样检查，违反为 `invalid_field`（不发出对端会拒绝的行）。键名区分大小写：在已定义字段的对象中（消息顶层及其结构化子对象），与已定义字段只差大小写的键为 `invalid_field`（按 Unicode 简单大小写折叠比较），其余未知键忽略。整数类型字段只接受整数形式（拒绝 `1.0`、`1e0` 与布尔值），取值在 int64 范围内。

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

## session 扩展（M4 Plan 12）

一个会话 incarnation 是一个长期 Worker 进程，按 `task_start → task_accepted → 终态提议 → task_outcome → task_released` 循环执行多个 task attempt（规格 §5.4、§12.3、§12.4）。宿主按 session 模式编解码（Go：`DecodeSessionLine` / `EncodeSessionLine`）：可用消息是 task 模式的全部消息加上下表的扩展；task 模式的编解码器不认识 session 专属类型（`unknown_type`）。各类型自身的规则通过后，再检查 session 模式的附加规则。

| 方向 | type | 必需字段与规则 |
|---|---|---|
| 宿主→Worker | `init` | `mode = session`、`session_id`、`incarnation_id`；`config?`；`session_resume?{checkpoint_id, state \| staged_state_path, refs?}`：`state` 与 `staged_state_path` 必须且只能有一个，`staged_state_path` 必须是 `/run/agentbox/restore/<sha256>`，**不得带 `state_ref`**（冷恢复不经 Gateway 读取，规格 §12.2），inline state 与 refs 上限同 checkpoint |
| 宿主→Worker | `task_start` | `task_id`、`attempt_id`、`attempt_no ≥ 1`、`out_dir`；可选 `traceparent`、`config`、`config_version`、`budget_limits`、`input_refs`、`base_session_checkpoint_id`（空 = 会话尚无 checkpoint）、`resume`（同 task 模式 init）、`directive`、`restored_from_task_id`、`carryover`；大小上限同 `init`（1 MiB） |
| | `directive` | `kind ∈ {finish_now, answer}`；`answer` 另需 `question_id` 与非空数组 `answers`（元素形状由会话 API 的 `AnswerRequest` 约束）。**不带 directive = 正常开始或继续**；没有 `continue`、`restore` 指令 |
| | `restored_from_task_id` | 仅用于展示；恢复（Restore）即从宿主复制的种子 checkpoint（经 `resume` 下发）继续 |
| | `carryover` | `{task_id, checkpoint_ref: sha256}`：被取代 turn 最新 task checkpoint 的 blob（已授权到会话 scope），Worker 经 Gateway 读取；缺失或损坏时新一轮不带上下文继续 |
| 宿主→Worker | `task_outcome` | `attempt_id`、`verdict ∈ {succeeded, failed, cancelled, paused}`、`committed_session_checkpoint_id?`；可重发（确认丢失时） |
| 宿主→Worker | `quiesce`、`session_close` | `grace_ms ≥ 0` |
| 宿主→Worker | `checkpoint_result`、`artifact_result` | 同 task 模式，另需 `attempt_id` |
| 宿主→Worker | `cancel`、`pause` | 同 task 模式 |
| Worker→宿主 | `ready` | 同 task 模式，另需 `mode = session` 与 `session_ext: 1`（缺失或为 0 → `session_ext_missing`，其他值 → `invalid_field`） |
| Worker→宿主 | `task_accepted`、`task_outcome_query`、`task_released` | `attempt_id` |
| Worker→宿主 | `quiesced` | `session_checkpoint_id`（会话尚无 checkpoint 时为空串） |
| Worker→宿主 | `closed` | — |
| Worker→宿主 | `awaiting_input` | `attempt_id`、`checkpoint_id`、`question_id`：等待用户回答的终态提议；`checkpoint_id` 须为最新已提交 task checkpoint（宿主核验）；裁决 `paused`（原因 `awaiting_input`），回答经下一 attempt 的 `directive: answer` 送回 |
| Worker→宿主 | `progress`、`artifact`、`checkpoint`、`checkpoint_query`、`paused`、`result` | 同 task 模式，另需 `attempt_id`；`result` 可带 `session_state?{checkpoint_id, state \| state_ref, refs?}`（规则同 checkpoint） |
| Worker→宿主 | `error` | 同 task 模式；`ready` 之后必须带 `attempt_id`（由事件流检查），`ready` 之前的启动失败不带 |

task 模式的编解码器忽略 `result.session_state`（仅 session 模式有意义）。

### 事件流（`SessionStream`）

宿主在写出成功后以 `HostSent` 记录自己的消息，以 `Accept` 检查 Worker 事件。`seq` 每 incarnation 从 1 严格递增，不随 task 重新开始。

| 阶段 | 含义 | 允许的 Worker 事件 | 宿主可发 |
|---|---|---|---|
| `awaiting_init` | 尚未发送 init | — | `init` |
| `ready` | 等待 ready | `ready`（→ `idle`）；`error`（启动失败 → `closed`）；`handshake_error`（仅第一条 → `closed`） | — |
| `idle` | 无 attempt | 无（带 attempt_id 的事件 → `wrong_attempt`） | `task_start`（→ `starting`）、`quiesce`（→ `quiescing`）、`session_close` |
| `starting(A)` | 已发 task_start | `task_accepted(A)`（→ `active`） | `cancel`、`pause`、`task_outcome(A)`、`session_close` |
| `active(A)` | attempt 运行中 | 业务事件与 `checkpoint_query`；终态提议 `result`/`error`/`paused`/`awaiting_input`（→ `proposed`） | `checkpoint_result`、`artifact_result`、`cancel`、`pause`、`task_outcome(A)`（取消等无提议的裁决，→ `proposed`）、`session_close` |
| `proposed(A)` | 已有终态提议或裁决 | `checkpoint_query`、`task_outcome_query(A)`；宿主已发 `task_outcome` 后 `task_released(A)`（→ `idle`） | `task_outcome(A)`（可重发）、`checkpoint_result`、`session_close` |
| `quiescing` | 已发 quiesce | `quiesced`（→ `quiesced`） | `session_close` |
| `quiesced` | 已静止 | 无 | `task_start`（→ `starting`）、`session_close` |
| `closing` | 已发 session_close | `closed`（→ `closed`） | — |
| `closed` | incarnation 结束 | 无 | — |

违规码：`seq_invalid`；`before_ready`（init/ready 之前）；`duplicate_ready`；`handshake_error_misplaced`、`after_handshake_error`；`after_terminal`（终态提议或裁决之后的业务事件、第二个终态提议；`session_close` 或 incarnation 结束之后的消息）；`wrong_attempt`（`attempt_id` 不是当前 attempt，含无当前 attempt 时）；`not_idle`（不在 `quiescing` 时出现 `quiesced`；宿主在非空闲时发 `task_start` 或 `quiesce`）；`session_ext_missing`；`unexpected_event`（其余阶段不允许的消息：`task_accepted` 之前的事件、重复的 `task_accepted`、提议之前的 `task_outcome_query`/`task_released`、宿主未发 `task_outcome` 即 `task_released`、未收到 `session_close` 的 `closed`）；ready 之后不带 `attempt_id` 的 `error` 为 `missing_field`。返回违规后该流不应继续使用。

### session fixtures

- `fixtures/v1/session_messages.json`：格式同 `messages.json`，按 session 模式解码；`valid` 中每条重新编码后与原消息语义一致（键序与空白不计）。
- `fixtures/v1/scenarios/session_*.jsonl`：每行一个 JSON 对象；带 `scenario` 键的行开始一个新场景（含 `description` 与 `expect`），其后带 `from` 的行是该场景按时间顺序的消息（`from` 为 `host` 或 `worker`）。宿主消息送入 `HostSent`，Worker 事件送入 `Accept`；`expect.stream = ok` 时另给最终阶段 `expect.phase`，`violation` 时给错误码与行号 `at`（从 0 起，只计消息行）。扩展名为 `.jsonl`，task 模式按 `scenarios/*.json` 读取的测试不受影响。
- Python SDK 的 session 模式（M4 Plan 13）必须通过同一组 fixtures。

## sub-run 扩展（M4 Plan 14）

sub-run 是同一 task/attempt 内的并发研究单元（规格 §13）。扩展经协商启用（规格 §5.2）：宿主在 `init`（task 模式或会话 `init`）中携带 `extensions: ["subruns"]`，Worker 的 `ready` 必须回 `subruns: 1`；未请求时 `ready.subruns` 必须缺省或为 0。task 与 session 两种模式的编解码器都认识下表的消息；session 模式下 Worker 的三种 sub-run 事件另需 `attempt_id`，宿主的两种答复不带 `attempt_id`（针对当前 attempt）。

**sub-run ID**：`^[a-z0-9][a-z0-9_-]{0,31}$` 且不等于 `root`。**summary**：≤ 4096 UTF-8 字节。

| 方向 | type | 必需字段与规则 |
|---|---|---|
| Worker→宿主 | `subrun_start` | `subrun_id`、`parent_step_id`（非空，≤ 256 字节）、`deadline_ms`（1–3 600 000）；`budget_cap_micro?`（≥ 0；缺省表示 sub-run 层只做归属、不设上限）。同 ID 同定义重发是幂等的（恢复时使用） |
| 宿主→Worker | `subrun_started` | `subrun_id`、`status ∈ {started, rejected}`；`rejected` 必须带 `code`（`subrun_limit`、`conflict`（同 ID 不同定义）、`subrun_closed`（该 ID 已是终态）、`invalid_field`、`retryable_error`（Store 暂时故障，以同一定义重试）），`started` 不得带 `code`。**`status`/`code` 是对规格 §5.4（只列 `subrun_id`）的补充** |
| Worker→宿主 | `subrun_end` | `subrun_id`、`status ∈ {succeeded, failed, cancelled}`、`summary`；`succeeded` 只是提议，已提交 checkpoint 的 `subruns[]` 列出 `completed` 后才成立 |
| Worker→宿主 | `subrun_cancel` | `subrun_id`、`reason`（非空自由文本）：编排层放弃 |
| 宿主→Worker | `subrun_cancel_requested` | `subrun_id`、`reason ∈ {deadline, task_cancel, policy}`；Worker 取消后回 `subrun_end{cancelled}` |

扩展字段：

| 消息 | 字段 | 规则 |
|---|---|---|
| `init` | `extensions?` | 字符串数组，每项非空；未知扩展名忽略 |
| `init.resume`、`task_start.resume` | `subruns?[]{subrun_id, status, result_ref?}` | 宿主裁定的状态 `started \| completed \| cancelled \| failed \| timed_out`；至多 4 项、ID 不重复；`completed` 必须带 sha256 `result_ref`，其他状态不得带 |
| `ready` | `subruns?` | 0 或 1 |
| `progress` | `subrun_id?` | 该进度所属 sub-run |
| `checkpoint` | `subruns?[]{subrun_id, status, result_ref?}` | `status ∈ {started, completed, failed, cancelled}`；至多 4 项、ID 不重复；`completed` 必须带 sha256 `result_ref`，其他状态不得带；`result_ref` 与 `refs` 合计 ≤ 1024（`too_many_refs`），且同样须已授权到当前 scope（规格 §5.5 规则 2）；状态转换是否合法由宿主判定（`invalid_transition`） |
| `result` | `subruns?[]{id, status, summary}` | `status ∈ {started, completed, failed, cancelled, timed_out}`；至多 4 项、ID 不重复；只供展示，不改变 sub-run 状态 |

### 事件流

`subrun_*`、带 `subrun_id` 的 `progress`、带 `subruns` 的 `checkpoint`/`result` 都是业务事件：`ready` 之前为 `before_ready`，终态提议之后为 `after_terminal`。另有（task 模式 `WorkerStream` 经 `NegotiateExtensions` 记录 init 的请求；`SessionStream` 从 `HostSent(init)` 读取）：

- `extension_mismatch`：请求了 `subruns` 而 `ready.subruns ≠ 1`，或未请求却回 1。
- `extension_not_negotiated`：未协商时出现上述任何 sub-run 事件或字段。
- `subrun_unknown`：`subrun_end`、`subrun_cancel`、带 `subrun_id` 的 `progress` 引用本 attempt 中尚未 `subrun_start` 的 ID（session 模式下 `subrun_started`/`subrun_cancel_requested` 同理）。已启动的 ID 按 attempt 计：恢复后的 attempt 须先以同一定义重发 `subrun_start` 才能再引用该 sub-run；`checkpoint.subruns[]` 不受此限（可列出恢复前已结束的 sub-run）。`subrun_start` 发出后、`subrun_started` 到达前即可发送该 sub-run 的事件。
- session 模式：没有当前 attempt 时宿主发 `subrun_started`/`subrun_cancel_requested` 为 `wrong_attempt`。

### sub-run fixtures

- `messages.json` 中名称含 `subrun` 的条目（及 `init_with_extensions_*`、`ready_with_subruns`、`progress_*subrun*`、`checkpoint_*subrun*`、`result_*subrun*`）。"ID 不重复" 无法用 JSON Schema 表达，相应非法条目以 `raw` 给出。
- `scenarios/subrun_start_before_ack.json`、`subrun_late_complete_after_cancel.json`（E41）、`subrun_end_without_checkpoint.json`（规格 §5.11）：`expect.subruns` 是宿主侧期望的 sub-run 最终状态，供 runner 测试使用，协议层回放忽略它。
- `scenarios/session_subruns.jsonl`：session 模式下的协商、`attempt_id` 与按 attempt 计的已启动 ID。
