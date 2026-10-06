# M4 对话式助手（Plan 13）真实验收记录

> 日期：2026-10-06（时间为服务器本地时间 UTC+8）。环境：腾讯云上海 CVM（4C8G，Ubuntu 24.04，内核 6.8），常驻服务 `agentbox-demo`（HTTPS 443，自签证书）。
> 模型：Moonshot `kimi-k3`（编排：路由、计划、报告）与 `kimi-k2.6`（子主题执行、停止摘要）；搜索：Serper。
> 所有请求从维护者本机经公网 HTTPS 发出（curl + cookie jar），使用专门注册的测试用户（随机口令只保存在维护者本机）。
> 本文不含 Key、cookie、token、口令或数据库连接串。费用为按配置单价估算的数字，不代表供应商实际计费；用户视图看不到费用，费用取自运维 inspect。

## 部署

- 分支 `m4-p13-t9`（自 `m4-batch` d4cdc75）经 `git bundle` + scp 送到服务器，用 `/etc/profile.d/go.sh` 构建（`CGO_ENABLED=0`）；`agentbox doctor` 通过。
- 迁移前备份：`scripts/backup.sh` → `agentbox-backup-20261006T135605Z.tar.zst.gpg`（SHA-256 `021dbf66…3d9d`），已复制到维护者本机 `backups/`（git 忽略）。回滚副本：`/usr/local/bin/agentbox.m3-bak`、`/opt/agentbox.m3-bak`、`/opt/agentbox-web.m3-bak`、`/root/agentbox-demo.service.m3-bak`。
- 迁移 0006–0008 在启动时应用；`scripts/dev/install-worker.sh /opt/agentbox` 安装 `agentbox_worker`、`deepresearch`、`chatagent`、`skills`（与 `sim_worker`）；web `dist` 在本机构建后装到 `/opt/agentbox-web`。
- 服务标志（`deploy/systemd/agentbox-demo.service`）新增：`--session-worker-argv python3,-m,chatagent --turn-tool-budget 30 --session-idle-freeze 10m --session-evict-after 1h --exec-slots 2 --exec-per-task 2 --exec-memory-max 1073741824`。exec 模板检查通过（日志 `exec 已启用`，镜像摘要已记录）。`--model-max-tokens-cap` 为默认 32768（≥ 16384）；白名单含 kimi-k3 与 kimi-k2.6；搜索供应商 serper；`--worker-subruns` 默认开启。
- 验收中发现并修复 3 个缺陷（见"修复"），每次修复后重新构建并部署；最终服务以默认标志运行，`systemctl is-active` = active，`agentbox verify-invariants` 通过。

## 七项验收

| # | 场景 | 结果 |
|---|---|---|
| 1 | 模糊题目 + 深度研究：一轮提问 → 回答 → 待办 → 并行子主题 → 带引用报告，额度 ≤ 30 | **通过** |
| 2 | 对上一份报告的追问从记忆回答 | **通过** |
| 3 | 停止 → 停止卡 → 继续 → 完成，不重复调用 | **通过**（首次失败，修复缺陷 A、C 后重测通过） |
| 4 | 另一次停止 → 立即写报告（部分） | **通过** |
| 5 | 停止后发新消息（取代）→ 恢复 → 新一轮从原处继续，额度从 0 开始 | **通过** |
| 6 | 冻结 / 驱逐：空闲后新消息唤醒，驱逐后冷恢复且记忆保留 | **通过**（首次发现缺陷 B，修复后重测通过） |
| 7 | 用户隔离；用户可见事件无 Key、费用、模型名 | **通过** |

### 1. 一次触发提问的研究（会话 S1，turn `…bff743`）

- 消息"帮我研究一下储能电池"，`deep_research=true`。事件：`skill_read{deep-research}` → `route{research, forced:true}` → `thinking` → `tool_call ask_user` → `ask_user`（3 题：用途、地区、维度，每题 3 个选项，`allow_other:true`）→ turn `awaiting_input`（22:00:42，开始后 13 s）。
- 回答（一般了解 / 中国市场 / 技术路线对比）后同一 turn 继续：`tool_result ask_user`（"用户的回答：…"）→ `todo_write`（4 个子主题，份额 6/7/7/6）→ **`research_subtopic` 只调用一次** → 4 个 sub-run 同时 `running`，子主题的搜索与抓取交错进行（并行）→ 4 个子主题 `done` 与摘要 → 报告。
- 22:05:21 成功（回答后约 3.5 min）。工具额度 **27/30**（`web_search` 10、`web_fetch` 17，其中 4 次抓取失败：403、404、Bad Gateway，计入额度）。模型调用 26 次（kimi-k3 5、kimi-k2.6 21）；估算费用 0.59 USD；4 个 sub-run 全部 completed。
- 报告"中国储能电池行业研究报告：技术路线全景对比与趋势研判"（14 992 字节）：正文引用 `[2][3][5]–[12]`，全部在证据列表中（列表共 12 项，每项带标题、URL 与结果 blob 的 sha256）。`report_ready{partial:false, tool_budget_reached:false}`。
- ⟨/⟩：`thinking` 事件内联发给模型的请求（system 提示含 skill 目录与工具说明、`tools`、`max_tokens`，无 `model` 字段）；`GET /turns/{id}/raw/{sha}` 返回的模型响应只有 `choices/created/object`（`id`、`model`、`usage` 被服务端去除），`message` 含 `reasoning_content` 与 `tool_calls`；搜索响应为 `query/results`。
- **Kimi 兼容性（计划 Step 4.5）**：多轮工具调用中，带 `tool_calls` 的 assistant 消息以 `content: ""`（或说明文字）发出、不回传 `reasoning_content`，kimi-k3 与 kimi-k2.6 均接受：部署后服务器共 169 次模型 try，没有 4xx；2 次 502 `upstream_unconfirmed` 分别是 sub-run 到期被取消时的在途调用和另一位用户的一次停止摘要（Worker 改用固定句子），与消息格式无关。不提供工具时（写报告、停止摘要）模型直接给出正文。

### 2. 从记忆回答的追问（S1，turn `…a777a4`）

- "根据刚才的报告，简要说说钠离子电池在储能上的主要优势和短板？"，`deep_research=false`。
- `route = answer`，工具额度 **0/30**（无搜索、无 `read_source`），模型调用 2 次（kimi-k3），估算 0.03 USD；回复按"主要优势 / 主要短板 / 一句话总结"组织，引用 `[6]`（上一轮报告中的来源，经会话记忆续编号）。

### 3. 停止 / 继续（S3，turn `…94d725`，"开源大模型许可证对比"）

- 22:25:36 停止（已用 10 次）→ 22:26:15 `turn_stopped` → `paused`。停止卡：`card{subtopics_done 0, subtopics_total 4, sources 1, tool_calls_used 12, tool_call_limit 30, todo[…]}`、`can_finish:false`、`findings`（kimi-k2.6 写的 2 句"目前发现"）。停止到暂停 39 s：4 个子主题的在途抓取（huggingface.co 等在服务器所在网络不可达）各重试 3 次、每次 10 s 超时后才到达工具边界，随后停止摘要调用 18 s。
- 22:32:07 继续 → 22:37:18 成功：额度 30/30（达到上限，报告注明"已达工具额度（30/30）"）。**无重复调用**：30 个计数调用的 `tool_call_id` 各不相同，输入（查询或 URL）无重复。子主题 st4 因 sub-run 的 10 分钟期限（自首次开始计，包含停止期间）超时，报告注明"子主题「Mistral AI 许可证…」不完整（超时）"，`partial:true`。
- 首次尝试（S2，turn `…5af006`）失败：停止后未出现停止卡，宿主在 10 s grace 到期时终止 incarnation，会话被驱逐（`session_state evicted`）；turn 以最后 checkpoint 暂停，"继续"后冷恢复并完成（无重复调用）。原因与修复见缺陷 A。停止卡上的 12/30 与 Gateway 的 10/30 不一致，见缺陷 C。

### 4. 立即写报告（S2，turn `…805e4a`，"数据中心液冷"）

- 第一个子主题完成后停止（22:41:03）→ 22:41:23 停止卡：`subtopics_done 3/3`（停止时 3 个子主题恰好全部完成）、`sources 9`、`tool_calls_used 25`、`can_finish:true`。
- 22:41:41 "立即写报告" → 22:42:36 成功：`report_ready{partial:true, note:"部分研究"}`，报告"2024–2025 年全球数据中心液冷技术市场研究报告"，引用 `[12]–[18]` 均在证据列表中。

### 5. 取代与恢复（S3）

- turn `…b0df6e`（"欧洲热泵补贴"）用到 6 次时停止 → 停止卡（sources 2）→ `paused`。
- 发新消息"先不研究了。顺便问一下：热泵的 COP 是什么意思？"（`deep_research=false`）→ 响应带 `superseded_turn_id`；原 turn `cancelled`、`status_reason = superseded`、`restorable = true`；新 turn `route = answer`、0/30，7 s 完成。
- `POST /turns/…b0df6e/restore` → 新 turn `…59a7be`（`restored_from_turn_id = …b0df6e`，`deep_research = true`）→ 22:49:22 成功：额度从 0 重新计数（首个额度事件 `used 3`，最终 **26/30**），**没有重复原 turn 的任何搜索或抓取**（原 turn 7 次，新 turn 26 次，输入无交集），3 个子主题 completed，报告"2025 年德法英三国住宅热泵安装补贴政策对比报告"，引用均在证据列表中。

### 6. 冻结 / 唤醒 / 驱逐 / 冷恢复（S4）

测试期间临时把服务改为 `--session-idle-freeze 30s --session-evict-after 90s`，结束后恢复默认（10m / 1h；已核对安装的 unit 与仓库一致）。

| 时间 | 操作 | 会话状态 |
|---|---|---|
| 22:56:31 | 驱逐状态下发"我的项目代号是什么？只回答代号。" | `restoring` → `idle`（新 incarnation），回答"青鸟"（记忆来自此前的 turn） |
| 22:57:12 | 空闲 30 s | `frozen` |
| 22:57:13 | 新消息 | thaw → `idle`，回答"青鸟" |
| 22:57:55 | 再次空闲 30 s | `frozen`（修复缺陷 B 之前这里变为 `evicted`，`end_reason = quiesce_failed`） |
| 22:58:49 | 自 idle 起 90 s | `evicted`（`end_reason = evicted`） |
| 22:58:56 | 新消息 | `restoring` → `idle`，回答"青鸟" |

### 7. 用户隔离与脱敏

- 第二个测试用户访问第一个用户的会话：`GET /sessions` 为空列表；`GET /sessions/{id}`、`/turns`、`/events`、`POST /messages`、`DELETE` → 404 `session_not_found`；`POST /turns/{id}/stop`、`/restore` → 404 `turn_not_found`；报告下载与 `GET /tasks/{id}` → 404 `task_not_found`；`raw/{sha}` → 404 `not_found`；`/tasks/{id}/inspect` → 403；未登录 → 401。
- 本次收集的全部用户可见数据（1 175 个会话事件、报告、原始响应，约 4 MB）：不含字段 `model`、`usage`、`cost*`、`call_id`、`attempt_id`、`upstream_request_id`、`internal` 等，不含 `kimi-*` 模型名；在服务器 root shell 中用实际的模型 Key、搜索 Key、数据库连接串与 api.token 逐一搜索，命中文件数均为 0（只输出计数）。

## 修复（均附测试，已部署到服务器后重测）

- **A. 停止卡来不及写（`internal/task/decide.go`）**：会话 turn 的 pause 沿用 10 s grace；Worker 停止时要等子主题到达工具边界再调用模型写"目前发现"，真实 Kimi 下合计常超过 10 s，宿主于是终止 incarnation（会话被驱逐、停止卡丢失）。改为会话 turn 的 pause 默认 60 s（`DefaultSessionPauseGraceMs`；cancel 与显式 grace 不变）。测试 `TestSessionTurnPauseUsesLongerGrace`。
- **B. 唤醒过的会话无法再次冻结（`internal/session/decide.go`）**：thaw 成功后没有清除上一次 quiesce 的结果，第二次空闲时 `stepQuiescing` 跳过 quiesce 并判为 `quiesce_failed` 驱逐。thaw 成功时清除 `Quiesced`。测试 `TestActorRefreezesAfterThaw`（修复前失败，日志与服务器上的现象一致）。
- **C. 失败调用的工具额度多计（`worker/agentbox_worker`、`worker/chatagent/research.py`）**：Gateway 的错误响应同样带 `X-Agentbox-Tool-Budget`，但 Worker 对失败调用一律本地加 1；并行子主题的在途调用已计入先前响应的计数，失败时再加便多计（停止卡 12/30，Gateway 10/30）。`GatewayError.tool_budget` 携带错误响应的计数头，失败时与成功同样以 Gateway 计数为准。测试 `test_failed_call_uses_gateway_count_from_error_response`、`test_gateway_client_parses_tool_budget_header_and_429`（AF_UNIX，WSL）、`test_subbudget_failure_uses_gateway_count_when_present`。

## 费用与调用汇总（运维 inspect）

| turn | 场景 | 状态 | 工具额度 | 模型调用（k3 / k2.6） | 估算费用 USD |
|---|---|---|---|---|---|
| …bff743 | 1 提问研究 | succeeded | 27/30 | 5 / 21 | 0.593 |
| …a777a4 | 2 记忆回答 | succeeded | 0/30 | 2 / 0 | 0.028 |
| …5af006 | 3 首次停止（缺陷 A） | succeeded | 30/30 | 8 / 25 | 0.852 |
| …94d725 | 3 停止/继续 | succeeded | 30/30 | 9 / 26 | 0.667 |
| …805e4a | 4 立即写报告 | succeeded | 25/30 | 5 / 20 | 0.648 |
| …b0df6e | 5 被取代 | cancelled | 7/30 | 2 / 8 | 0.114 |
| …727e11 | 5 取代消息 | succeeded | 0/30 | 1 / 0 | 0.008 |
| …59a7be | 5 恢复 | succeeded | 26/30 | 9 / 18 | 1.020 |

研究 turn 合计约 3.9 USD（另有冻结测试的 5 个短回答，每个约 0.01 USD）。

## 本地联调（Step 1）

- Python（Windows）：`pytest` 3 次各 1 070 passed、72 skipped；`ruff check`、`ruff format --check`、`lint-imports`（7 个契约）通过；WSL（`AGENTBOX_REQUIRE_UNIX_TESTS=1`，不含 `test_protocol.py`）360 passed。
- web：`gen:api` 无差异，lint、typecheck 通过，`vitest` 3 次各 188 passed，build 通过。
- Go（WSL，PostgreSQL + Redis）：`go vet ./...` 与非 root 全量 `go test ./...` 通过；root `./tests/e2e/...` 一次通过（51 个顶层用例，唯一跳过为 `TestCacheBenefit`），包括会话的 E28–E33。
- 未做：以 `python3 -m chatagent` 作为会话 Worker 的 root e2e（现有会话 e2e 使用脚本化的 `sessionworker`）；chatagent 与真实沙箱、Gateway 的联调由本次服务器验收覆盖。CI 待分支推送后确认。

## 观察到的问题（未修复）

- **重放时的额度头是当前值**：Gateway 对已有调用（重放）返回的 `X-Agentbox-Tool-Budget` 是当前计数而非原调用时的计数；Worker 把它写进工具结果的"已用 k/N"行。崩溃恢复后若重放的调用之后已有其他计数调用，重建的提示词会与原来不同，可能触发 `fingerprint_mismatch`。本次验收未出现（停止均在 checkpoint 处）。
- **sub-run 期限包含停止时间**：sub-run 的 10 分钟期限自首次开始计算，停止期间不暂停；停止较久后继续，未完成的子主题可能立即超时（场景 3 的 st4）。
- **被取代 turn 的 sub-run 行**：在 paused 状态被取消的 turn 不经裁决事务，其 sub-run 保持 `started`（不影响运行，`verify-invariants` 通过）。
- **停止耗时**：停止到停止卡 20–39 s（等待在途调用 + 停止摘要）；界面期间显示"停止中"。
- **抓取质量**：部分网页摘录以导航文字开头；PDF（如 nea.gov.cn、dfcfw.com）摘录为空但仍登记为来源；服务器所在网络访问 huggingface.co、developer.meta.com 等超时，错误文字显示为 `tries_exhausted（Too Many Requests）`（HTTP 429 的状态文字），对用户有误导。
- **恢复出的 turn 没有路径标签**：从种子继续的 turn 不再发 `route` 事件，列表中 `route` 为空，界面不显示"深度研究"标签。
- **标题**：模型未写 `# 标题` 时报告标题取用户原文（场景 3 首次的报告标题较长）。
