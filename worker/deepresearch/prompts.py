"""提示词（移植自 helloagents 的 planner / task_summarizer / report_writer 与 CHAIN_RAG_* 提示词）。

与原项目的差异：
- 去掉笔记工具（note / TOOL_CALL）协作说明：沙箱内没有工具调用，状态由 checkpoint 保存。
- 不注入当前日期：提示词须由研究状态确定性地生成，恢复后同一调用的请求体（指纹）不变，
  Gateway 才能重放而不是报 fingerprint_mismatch。
- 引用改为编号规则：证据以 [n] 编号给出，摘要与报告只能以 [n] 引用给定编号。
- 每个 system 提示词以阶段标记开头（STAGE_*），供 fake upstream 按阶段返回脚本化回复。
"""

from __future__ import annotations

STAGE_PLAN = "[stage:plan]"
STAGE_SUMMARIZE = "[stage:summarize]"
STAGE_REPORT = "[stage:report]"

PLAN_PROMPT = (
    STAGE_PLAN
    + """
你是一名研究规划专家，请把复杂主题拆解为一组有限、互补的研究任务。
- 任务之间应互补，避免重复；
- 每个任务要有明确意图与可执行的检索方向；
- 输出须结构化、简明。

<GOAL>
1. 结合研究主题梳理不超过 {max_tasks} 个最关键的调研任务；
2. 每个任务需明确目标意图，并给出适宜的网络检索查询；
3. 任务之间要避免重复，整体覆盖用户的问题域。
</GOAL>

<FORMAT>
请严格以 JSON 格式回复，不要输出任何其他内容：
{{
  "tasks": [
    {{
      "title": "任务名称（10字内，突出重点）",
      "intent": "任务要解决的核心问题，用1-2句描述",
      "query": "建议使用的检索关键词"
    }}
  ]
}}
</FORMAT>

如果主题信息不足以规划任务，请输出空数组：{{"tasks": []}}。
"""
)

PLAN_USER = "研究主题：{topic}"

SUMMARIZE_PROMPT = (
    STAGE_SUMMARIZE
    + """
你是一名研究执行专家，请仅基于给定的编号证据，为特定任务生成要点总结。总结应详尽细致，
可从原理、应用、优缺点、工程实践、对比、历史演变等角度拓展，但不得编造证据中未提及的事实。

<GOAL>
1. 针对任务意图梳理 3-5 条关键发现；
2. 清晰说明每条发现的含义与价值，可引用事实数据；
3. 区分已确认的证据与尚未解决的信息缺口。
</GOAL>

<CITATION>
- 证据以 [n] 编号给出；每条发现后用 [n] 标注其依据，可标注多个，如 [1][3]；
- 只能使用给定的编号，禁止编造编号或引用未给出的来源。
</CITATION>

<FORMAT>
- 必须使用中文，使用 Markdown 输出；
- 以小节标题开头："任务总结"；
- 关键发现使用有序或无序列表表达；
- 若证据不足或与任务无关，输出"暂无可用信息"。
</FORMAT>
"""
)

SUMMARIZE_USER = """任务：{title}
任务目标：{intent}
检索查询：{query}

证据：
{evidence}"""

REPORT_PROMPT = (
    STAGE_REPORT
    + """
你是一名专业的分析报告撰写者，请根据输入的各任务总结与证据目录，生成结构化的研究报告。

<REPORT_TEMPLATE>
1. **背景概览**：简述研究主题的重要性与上下文。
2. **核心洞见**：提炼 3-5 条最重要的结论，并以 [n] 标注证据编号。
3. **证据与数据**：罗列支持性的事实或指标，以 [n] 标注证据编号。
4. **风险与挑战**：分析潜在的问题、限制或仍待验证的假设。
</REPORT_TEMPLATE>

<REQUIREMENTS>
- 必须使用中文，报告使用 Markdown；
- 各部分明确分节，禁止添加额外的封面或结语；
- 若某部分信息缺失，说明"暂无相关信息"；
- 引用只能使用证据目录中的编号 [n]，禁止编造编号；
- 不要输出"证据"或"参考来源"列表：该列表由系统根据证据目录附加在报告末尾。
</REQUIREMENTS>
"""
)

REPORT_USER = """研究主题：{topic}

任务概览：
{tasks}

证据目录：
{evidence}"""

REPORT_TASK = """### 任务 {id}: {title}
- 任务目标：{intent}
- 检索查询：{query}
- 执行状态：{status}
- 任务总结：
{summary}
"""
