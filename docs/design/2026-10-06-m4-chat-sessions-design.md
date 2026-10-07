# M4 第一部分：会话与对话式 DeepResearch 助手（Plan 12 + Plan 13）

> 日期：2026-10-06。状态：设计已批准（项目负责人，2026-10-06，逐节确认）。
> 依据：[v0.2 规格](2026-10-03-v0.2-first-release-design.md) §12（会话）、§8（状态机）、§9（Gateway）、§13（sub-run，Plan 14）、§10（exec，Plan 15）；[用户账号设计](2026-10-06-user-accounts-design.md)。
> M4 拆分：**Plan 12 会话后端** + **Plan 13 skill 化的 Agent、工具包与对话界面**（本文）→ Plan 14 受限多 Agent（sub-run）→ Plan 15 独立 exec 与隔离加固 → 发布门槛（全量回归、性能报告）。

## 1. 目标

把面向用户的研究助手改成对话式产品（参考 Claude、ChatGPT、Codex）：

- 左侧会话列表；对话中 AI 回复在左、用户消息在右；研究进度、来源与报告在右侧面板。
- 每轮（用户的一条消息）由 Agent 决定直接回答还是执行深度研究；用户可用"深度研究"开关强制研究。
- Agent 的每一步以可折叠的行展示：读取 skill、思考、向用户提问、待办清单、搜索网页（N 条结果，可逐行展开）、阅读网页；每行可查看原始请求与响应。
- 研究可以停止（显示进度摘要并引导继续）、继续、立即写报告；被新消息取代的研究保留全部内容并可恢复。
- 会话长期保留；空闲时冻结、驱逐以节省内存，新消息时透明恢复。
- 每轮最多 30 次工具调用（`web_search` + `web_fetch`），由 Gateway 强制。

## 2. 已确定的决定

| # | 决定 |
|---|---|
| D1 | 回答还是研究：模型决定，用户可用"深度研究"开关强制研究；界面显示所走的路径 |
| D2 | 停止摘要：由 checkpoint 生成的结构化进度卡（零成本）+ kimi-k2.6 写的 2–3 句"目前发现"（约 0.01 USD）；按钮"继续""立即写报告" |
| D3 | 研究方式：工具调用式 Agent（模型决定何时搜索、读哪个网页、何时结束）；每轮 30 次工具调用上限由 Gateway 强制，第 31 次 `429 tool_budget_exhausted`；Agent 在每个工具结果中看到剩余额度 |
| D4 | 会话保留到用户删除；自动标题；可重命名、删除；空闲 10 分钟冻结，1 小时（或内存压力下按 LRU）驱逐；新消息唤醒 |
| D5 | 已停止的研究遇到新消息：自动取消但保留全部内容，卡片显示"已停止 + 恢复"；其来源与已完成的子主题摘要可作为新一轮的上下文；"恢复"在同一会话中开新一轮，从被取消研究的最后 checkpoint 继续，获得新的 30 次额度 |
| D6 | 会话实现：完整的规格 §12 会话（每会话一个长期 incarnation，冻结/驱逐/恢复），覆盖 E28–E33 |
| D7 | DeepResearch 是一个 **skill**：系统提示列出 skill 目录（名称 + 一句描述）与工具；skill 正文经 `read_skill` 按需加载（渐进披露，同 Claude Code） |
| D8 | 向用户提问：每轮至多一轮、至多 3 个选择题，只在研究范围不明确时（时间、地区、目的、深度）；研究进行中不提问；简单问答不提问 |
| D9 | 工具放在 `worker/agentbox_worker/tools/`（每个工具一个模块、统一接口、注册表生成函数调用 schema）；仓库根的 `tools/` 是 Go 开发工具，不混用 |
| D10 | 界面布局：对话 + 右侧面板（标签：进度 / 来源 / 报告）；窄屏折叠为单列 |

参考：gpt-researcher（规划 → 执行 → 发布）、dzhng/deep-research（广度/深度参数 ↔ 30 次额度）、Anthropic 多 Agent 研究系统（主 Agent 规划、子 Agent 并行、引用核对；Plan 14 实现并行）、Alibaba Tongyi DeepResearch 与 SkyworkAI DeepResearchAgent（分层规划）。

## 3. 架构与数据（Plan 12）

- **session**（规格 §12）：属于一个用户；`title`、状态（`creating → idle ⇄ running`；`idle → quiescing → frozen → evicting → evicted → restoring → idle`；`closing → closed`）、最新 **session checkpoint** 指针（对话记忆：历史消息、各轮报告摘要、已收集来源的索引及其 blob）、workspace。
- **turn** = 一个 session task：`text`、`deep_research`、`route`（`answer|research`）、状态、结果（回复文本 + 报告产物）；`restored_from_task_id`（恢复时）。
- **incarnation**：会话的存活沙箱，同时至多一个；会话 actor 负责创建、冻结、驱逐与恢复；task actor 负责 turn 的 attempt（经会话 actor 授予）。
- 新表：`sessions`、`incarnations`、`session_checkpoints`、`session_progress`（规格表）；`tasks` 增加 `session_id`、`turn_index`、`restored_from_task_id`。
- **工具额度**：Gateway 按 task 计数 `web_search` 与 `web_fetch`（缓存命中、合并也计数），与预算预留在同一事务内（并发与重启下精确）；上限默认 30（`--turn-tool-budget`）；模型调用、`read_skill`、`read_source`、`ask_user`、`todo_write` 不计数。
- **权限**：会话只属于其所有者；用户只见自己的会话；运维在 `#/admin` 可见全部。
- **生命周期参数**：`--session-idle-freeze`（默认 10 min）、`--session-evict-after`（默认 1 h）；冻结前条件、驱逐、唤醒、冷恢复的 state 读取、关闭与 server 重启按规格 §12.2、§12.6。

## 4. Agent 与 skill（Plan 13）

### 4.1 Worker 会话模式

Worker 以会话模式运行（每个 incarnation 一个长期进程，协议 v1 的会话扩展）。每轮：

1. 构造系统提示：角色与规则、**skill 目录**（`deep-research`：一句描述）、**工具列表**、预算规则、提问规则（D8）、会话状态摘要。
2. Agent 循环（编排模型 kimi-k3，函数调用）：模型可直接回答（流式回复），或调用工具。开关打开时系统提示要求先 `read_skill("deep-research")`。
3. 读取 deep-research skill 后按其流程：（必要时）`ask_user` → `todo_write` 计划（2–4 个子主题与 30 次额度分配）→ 逐个子主题由 kimi-k2.6 执行搜索/阅读循环 → 子主题摘要 → kimi-k3 写报告 → 引用核对（每个 `[n]` 对应已保存的证据 blob）。Plan 13 中子主题顺序执行；Plan 14 改为并行 sub-run，流程与界面不变。
4. 每个工具调用后、每个子主题完成后写 checkpoint（停止、继续、崩溃恢复最多损失进行中的一次调用）。
5. 成功时新的会话状态（对话 + 来源索引）成为下一个 session checkpoint，报告固定为产物。

### 4.2 Skill

```
worker/skills/deep-research/
  SKILL.md              # 名称、描述、何时使用、流程、提问规则、额度规则、引用要求
  references/report-template.md
  references/source-quality.md
```

`SKILL.md` 的 front matter（`name`、`description`）进入系统提示中的 skill 目录；正文只在 `read_skill` 时加载。skill 目录可扩展：新增 skill 只需新增目录。

### 4.3 工具包

```
worker/agentbox_worker/tools/
  __init__.py         # ToolRegistry：名称 → 工具；生成函数调用 schema；分发调用
  base.py             # Tool 接口：name、description、JSON schema、run(args, ctx) -> ToolResult
  budget.py           # 本轮额度视图（已用/30、子主题份额），以 Gateway 计数为准
  skills.py           # read_skill(name)
  ask_user.py         # ask_user(questions[])：≤3 个选择题，附"其他"
  todo.py             # todo_write(items[])
  web_search.py       # web_search(query, max_results) → Gateway /v1/search
  web_fetch.py        # web_fetch(url) → Gateway /v1/fetch（结果为证据 blob）
  sources.py          # read_source(source_id)：重读已收集的来源，不访问网络
```

工具不持有任何凭据，只经 SDK 的 Gateway 客户端调用；`tool_budget_exhausted` 时 Agent 直接进入摘要或报告，本轮成功并标注"已达工具额度（30/30）"。

## 5. 停止、继续、恢复与提问

- **停止**：记录 pause 控制；Agent 在下一个提交边界停止并写 checkpoint，turn 为 `paused`；生成停止摘要（D2）；还没有任何发现时文字为"尚无发现，研究在规划阶段被停止"。
- **继续**：恢复 paused turn，从 checkpoint 继续，额度计数延续。
- **立即写报告**（至少一个子主题完成时可用）：恢复并附"现在结束"指令，跳过剩余子主题，用已有材料写报告，turn 成功并标注"部分"。
- **新消息到达时有 paused turn**：该 turn 被取消（裁决事务内清除会话队列阻塞），内容全部保留，卡片显示"已停止 + 恢复"；其来源与已完成的子主题摘要作为新一轮的可用上下文。
- **恢复**：在同一会话新建 turn（`restored_from_task_id`），从被取消 turn 的最后 task checkpoint 继续（该 blob 已授权到本会话 scope），新的 30 次额度。
- **`ask_user`**：turn 进入 `awaiting_input`（一种暂停：保留 checkpoint、释放 run slot）；用户的回答恢复同一 turn 的同一位置；不计入 30 次。
- **删除会话**：拒绝新消息 → 排队与暂停的 turn 取消 → 运行中的 turn 取消并释放 → 环境停止后删除 workspace；已固定的报告保留在运维记录中，从用户视图消失。

## 6. API 与事件

用户侧（会话 cookie），OpenAPI 为唯一契约，前端类型由其生成：

| 端点 | 作用 |
|---|---|
| `POST /sessions`、`GET /sessions` | 新建；侧栏列表（标题、最近活动、状态） |
| `PATCH /sessions/{id}`、`DELETE /sessions/{id}` | 重命名；删除 |
| `POST /sessions/{id}/messages` | `{text, deep_research}` 开始新一轮（处理 D5） |
| `GET /sessions/{id}/turns` | 历史 |
| `GET /sessions/{id}/events` | 整个会话的 SSE 事件流，可按游标续传 |
| `POST /turns/{id}/stop` · `/continue` · `/finish` · `/restore` · `/answer` | 停止；继续；立即写报告；恢复；回答 `ask_user` |

事件（驱动步骤行）：`skill_read`、`thinking`、`ask_user`、`todo_updated`、`tool_call` / `tool_result`（含用于显示的结果预览与原始请求/响应的引用）、`assistant_delta`（流式回复）、`report_ready`、`turn_stopped`（含停止摘要卡）、`budget`（已用/30）、会话状态变化（`frozen`、`evicted`、`restoring`）。

**原始请求/响应（⟨/⟩）**：用户可见工具的输入、输出与发给模型的提示；不可见 Key、费用与内部调用 ID（沿用 Plan 11 的服务端脱敏，运维可见全部）。

## 7. 前端（Plan 13）

- 布局：左侧会话列表（新对话、标题、重命名/删除）；中间对话（用户消息右侧气泡；AI 回复左侧，由步骤行 + 流式文本组成）；右侧面板（进度：待办清单与额度条；来源：编号列表；报告：安全渲染与下载）。
- 步骤行：默认折叠，进行中的一行展开；"搜索网页（N 条结果）"展开后逐行显示标题、站点、摘要，可再折叠；⟨/⟩ 打开原始请求/响应。
- 提问卡：选项按钮 + "其他…"输入；回答后显示"已回答"。
- 停止卡：进度卡 + "目前发现" + 继续 / 立即写报告；被取消的研究显示"已停止 + 恢复"。
- 会话状态提示："正在恢复对话…"。
- 运维工作台仍在 `#/admin`。

## 8. 错误处理

- 模型或供应商失败：重试一次；仍失败则 turn `failed`，显示面向用户的提示（"模型服务暂时不可用，请重试"），会话仍可继续。
- 单个工具调用失败（如网页不可达）：记录并继续，计 1 次额度。
- 额度用尽：进入摘要或报告，turn 成功并标注。
- 唤醒失败：会话转 `evicted` 并按冷恢复重试一次；仍失败则提示"会话暂时无法恢复"，历史仍可阅读。

## 9. 测试与验收

- Go：会话生命周期 E28–E33（冻结、驱逐、恢复、重启、取消与暂停语义）；工具额度在并发与重启下精确；`awaiting_input` 与恢复；恢复（Restore）的种子 checkpoint；会话的用户隔离。
- Python：工具注册表与各工具（对 fake Gateway）；Agent 循环（脚本化的 fake 模型：read_skill → ask_user → todo → search/fetch → report）；额度用尽；循环中途停止与继续；从 checkpoint 恢复。
- Web（Vitest）：会话侧栏、对话气泡、可折叠步骤行、提问卡、面板标签、停止卡与按钮、用户视图不含内部细节。
- 服务器真实验收（Kimi + Serper）：一次触发提问的研究、一次从记忆回答的追问、一次停止/继续、一次恢复；记录为证据。

## 10. 计划与并行

- **Plan 12（会话后端）** 与 **Plan 13（Agent、skill、工具包、对话界面）** 以本文第 6 节的 API 与事件为共享契约并行推进，最后联调。
- Plan 14（sub-run 并行研究）与 Plan 15（独立 exec 与加固）按 v0.2 规格 §13、§10 编写计划；与 Plan 12/13 文件重叠处由协调者排序。
- 运行验收（root、真实沙箱）串行。

## 11. 不在本文内

- 多 Agent 并行（Plan 14）、代码执行沙箱（Plan 15）、性能报告与发布门槛。
- 用量配额（项目负责人不做）、语音、文件上传、分享会话。
