# 串并行对比（M4 Plan 14 Task 12，规格 §16.6）

比较同一研究任务在 `research.scheduling = serial` 与 `parallel` 下的墙钟时间、调用次数、费用、失败率与引用质量。两组的唯一差别是调度：

- **相同拆分**：每个问题的子主题清单写在 `questions.json` 中，以 `research.fixed_plan` 交给 Worker，两组共用。Worker 跳过规划，直接以合成的 `research_subtopic` 调用进入研究阶段。
- **相同记账路径**：`serial` 也走 sub-run，只是逐个启动；两组的提示词、预留与份额分配、call id 都相同（Plan 14 Task 9）。
- **相同条件**：模型由服务端固定（主 Agent kimi-k3、子主题 kimi-k2.6）；每轮 30 次工具额度，单任务预算默认 2 USD；服务端以**不配置 Redis** 的方式运行（共享缓存关闭）。每次运行使用一个新会话，没有对话历史。
- **交替顺序**：第 i 个问题按 serial→parallel 或 parallel→serial 交替运行，以抵消时段差异。两组不并发运行。

## 文件

| 文件 | 作用 |
|---|---|
| `questions.json` | 10 个固定问题（中英各半），每题 3 个子主题，各 8 次份额 |
| `run.py` | 驱动：测试用户建会话 → 运维 `POST /tasks {session_id, spec}` 建 turn → 轮询到终态 → 导出 inspect、任务事件、报告与报告引用的结果 blob → 删除会话 |
| `grade.py` | 引用评分与汇总：可定位率（自动）；支持结论率（kimi-k3 按固定 rubric）；P50/P95 等汇总写 `summary.json` |
| `accept.py` | 服务器真实验收 2–5 的驱动：用户路径的并行研究、停止/继续、立即写报告 |

2026-10-07 的运行结果（每组 N = 4）见 [`docs/evidence/2026-10-06-m4-subrun-comparison.md`](../../docs/evidence/2026-10-06-m4-subrun-comparison.md)。

## 运行（演示服务器上，root）

```bash
# 1. 服务端去掉 --redis-addr 后重启（对比期间关闭共享缓存）
# 2. 测试用户的登录 JSON {"username": ..., "password": ...} 放到 0600 文件
sudo python3 run.py --creds ~/exp/cred.json --out ~/exp/runs --limit 4 --max-usd 4.5
sudo python3 grade.py --runs ~/exp/runs --pairs 8
# 3. 恢复带 --redis-addr 的 unit 并重启
```

- `run.py` 从 `/var/lib/agentbox/api.token` 读取运维 token，不打印；`grade.py` 从 `/etc/agentbox/agentbox.env` 读取模型 Key 一行，不打印。
- `--max-usd`：累计费用（按服务端配置的单价）加上已观察到的单次最高费用超过上限时，不再开始新的运行。
- 已有结果文件的运行会被跳过，中断后重跑会接着做。

## 运维路径

`POST /tasks` 带 `session_id`（规格 §15.1；只对运维开放）。这时 `spec` 只接受 `text`、`deep_research` 与 `research`。服务端按用户消息的同样方式生成 turn spec（模型、额度），再把 `research` 的键覆盖到 `spec.research`。turn 属于会话的所有者，仍受“每个用户同时只有一个进行中的 turn”约束。用户路径从不设置 `scheduling` 与 `fixed_plan`。

## 指标

- **墙钟时间**：turn 创建（`task.created_at`）到 `report_ready` 事件。包括新会话启动环境的时间，两组相同。
- **研究跨度与重叠度**：研究跨度是第一个 sub-run 开始到最后一个 sub-run 结束；重叠度 = Σ sub-run 时长 ÷ 研究跨度（串行约为 1）。
- **调用与费用**：模型调用、`web_search + web_fetch` 次数，以及任务层的 spent 与 unknown（微美元，按配置单价估算），全部取自 inspect。
- **失败**：turn 未成功或没有报告；部分失败是 `report_ready.partial` 或 sub-run 未 completed。
- **可定位率**：正文中的每个 `[n]` 都要在“## 证据”列表中，且其 sha256 是本 turn 可经 `GET /turns/{id}/raw/{sha}` 取到的结果 blob。
- **支持结论率**：按固定种子，每份报告抽取至多 8 个“句子 + 引用”对。证据是该 blob 经 Worker 的 `page_text` 得到、截断到 6 KiB 的正文，即模型实际看到的文字。kimi-k3 按 `grade.py` 中的 RUBRIC 判定。随后人工复核约 20% 的报告，并在证据文档中记录与自动评分的分歧。

## 偏差

- **拆分不是由主 Agent 规划的。** 计划要求“每个问题先由主 Agent 规划一次”，实际是维护者按 deep-research skill 的清单格式事先写定。原因有两点：用户路径无法只做规划而不进入研究，而单独为规划付一次研究费用不合算。两组仍共用同一份拆分。
- **样本量受费用上限限制。** 实际运行的问题数见证据文档。结论只是小样本趋势，不是基准结论。
