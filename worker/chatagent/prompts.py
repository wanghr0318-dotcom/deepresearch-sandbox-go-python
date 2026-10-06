"""提示词的确定性构造：全部是纯函数，输出只取决于参数（不含时间、随机数）。

恢复后由同一 checkpoint 状态重建的请求体与原调用相同，Gateway 的调用指纹因而稳定。
"""

from __future__ import annotations

from typing import Any

from agentbox_worker.tools import ToolRegistry
from agentbox_worker.tools.skills import SkillCatalog
from agentbox_worker.tools.sources import SourceStore
from agentbox_worker.tools.todo import TodoItem
from chatagent.config import TurnConfig
from chatagent.state import SessionMemory, TurnState, json_size

PROMPT_TURNS = 6  # 系统提示中列出的最近轮次
PROMPT_SOURCES = 60  # 系统提示中列出的既往来源（最近的）
TURN_TEXT_CHARS = 200

_ORCHESTRATOR_RULES = "\n".join(
    [
        "你是 agentbox 的对话助手，同时是研究编排者。根据用户的问题决定直接回答还是开展深度研究。",
        "",
        "## 规则",
        "1. 简单事实、定义、计算、闲聊、写作润色，或能从对话记忆直接回答的问题：直接回答，"
        "不读 skill、不提问；必要时可少量 web_search / web_fetch 核实。",
        '2. 需要多来源核实、比较、趋势或综述的问题：先调用 read_skill("deep-research") '
        "读取研究流程，并严格按其执行（计划 → 逐个 research_subtopic → 写报告）。",
        "3. 工具额度：本轮 web_search + web_fetch 合计 {budget} 次，由系统强制；每个计数工具的"
        "结果末尾显示已用与剩余次数。模型调用、read_skill、read_source、ask_user、todo_write、"
        "research_subtopic 本身不计数。额度用尽后不要再调用工具，直接用已有材料作答。",
        "4. 提问：只有研究范围不明确（时间、地区、目的、深度）时，在读取 skill 之后、写计划之前"
        "用 ask_user 提一轮选择题（至多 3 题）；简单问答不提问；计划写出或开始搜索后不再提问。",
        "5. 引用：只能用 [n] 引用 web_fetch 读过、带编号的来源（含下面列出的既往来源）；"
        "不得编造编号，不写参考文献列表（系统自动附证据列表）。",
        "6. 不再调用工具的回复即本轮的最终回复：直接给出回答，或研究时给出完整报告正文"
        '（Markdown，以 "# 标题" 开头）。',
        "7. 用与用户相同的语言回复。",
    ]
)


def _first_sentence(text: str) -> str:
    head = text.split("。", 1)[0].strip()
    return head + ("。" if "。" in text else "")


def _clip(text: str, n: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[: n - 1] + "…"


def orchestrator_system(
    skills: SkillCatalog,
    registry: ToolRegistry,
    memory: SessionMemory,
    cfg: TurnConfig,
    carry: str | None,
) -> str:
    """编排模型的系统提示：角色与规则；skill 目录；工具列表；会话状态摘要；强制研究说明。"""
    parts = [_ORCHESTRATOR_RULES.format(budget=cfg.tool_budget)]
    parts.append(
        "## Skill 目录（用 read_skill(name) 按需读取正文）\n" + (skills.catalog_text() or "（无）")
    )
    tools = "\n".join(
        f"- {s['function']['name']}：{_first_sentence(s['function']['description'])}"
        for s in registry.schemas()
    )
    parts.append("## 工具\n" + tools)
    parts.append("## 会话状态\n" + session_summary(memory))
    if carry:
        parts.append("## 上一轮被停止的研究的发现（可直接使用，不要重复搜索）\n" + carry)
    if cfg.deep_research:
        parts.append(
            "## 本轮要求\n用户打开了“深度研究”：本轮必须按 deep-research skill 研究"
            "（系统已为你读取 skill 正文，见对话中的 read_skill 结果）。"
        )
    return "\n\n".join(parts)


def session_summary(memory: SessionMemory) -> str:
    """最近轮次的用户问题与回复摘要、报告标题，以及可用的既往来源 "第 k 轮 [n] 标题"。

    来源编号与本轮来源表沿用（adopt）既往来源后的编号一致。"""
    if not memory.turns:
        return "这是会话的第一轮。"
    index = {t.task_id: k for k, t in enumerate(memory.turns, 1)}
    lines = ["最近的轮次："]
    first = max(0, len(memory.turns) - PROMPT_TURNS)
    for k, t in enumerate(memory.turns[first:], first + 1):
        user, said = _clip(t.user, TURN_TEXT_CHARS), _clip(t.reply, TURN_TEXT_CHARS)
        line = f"- 第 {k} 轮：用户问「{user}」；回复：{said}"
        if t.report:
            line += f"；报告《{t.report.get('title', '')}》"
        lines.append(line)
    store = SourceStore()
    store.adopt(memory.sources)
    adopted = store.all()
    if adopted:
        lines.append("既往来源（本轮可用 read_source(n) 重读，也可在本轮直接以 [n] 引用）：")
        for s in adopted[-PROMPT_SOURCES:]:
            k = index.get(s.origin)
            where = f"第 {k} 轮" if k is not None else "之前的轮次"
            lines.append(f"- {where} [{s.n}] {_clip(s.title, 80)}")
    return "\n".join(lines)


def history_messages(memory: SessionMemory, limit_bytes: int = 24 * 1024) -> list[dict[str, str]]:
    """最近轮次的 user/assistant 消息对（按时间顺序），总量 ≤ limit_bytes（保留最新的）。"""
    out: list[dict[str, str]] = []
    size = 0
    for t in reversed(memory.turns):
        pair = [
            {"role": "user", "content": t.user},
            {"role": "assistant", "content": t.reply},
        ]
        n = json_size(pair)
        if size + n > limit_bytes:
            break
        out[:0] = pair
        size += n
    return out


_NO_BRIEF = "（无简报：围绕标题收集关键事实与数据）"


def subtopic_system(item: TodoItem, strategy: str, budget_line: str) -> str:
    """子主题执行者（kimi-k2.6）的系统提示：简报、搜索与阅读策略、额度与摘要要求。"""
    return "\n\n".join(
        [
            "你是研究子主题的执行者。用 web_search 找线索、用 web_fetch 阅读一手页面，"
            "然后写出本子主题的摘要。",
            f"## 子主题 {item.id}：{item.title}\n{item.brief or _NO_BRIEF}",
            "## 搜索与阅读策略\n" + strategy.strip(),
            f"## 额度\n本子主题份额 {item.budget} 次（web_search + web_fetch）。"
            f"{budget_line}\n份额用完或材料已足够时停止调用工具。",
            "## 摘要要求\n不再调用工具的回复即摘要：300–800 字，列出关键事实与数据"
            "（注明时间与口径）、分歧与缺口；每个事实用 [n] 引用 web_fetch 结果给出的来源编号；"
            "没读过的页面不能引用，不得编造编号。",
        ]
    )


def subtopic_user(item: TodoItem) -> str:
    return f"开始研究子主题 {item.id}「{item.title}」。"


SUBTOPIC_CLOSE = "份额或额度已用完（或轮数已到上限）：现在写子主题摘要，不要再调用工具。"


def finish_instruction(reason: str) -> str:
    """收尾指令（user 消息）。reason: "user_finish" | "budget" | "rounds"。"""
    if reason == "user_finish":
        return (
            "现在结束：用户要求立即写报告。不要再调用任何工具；跳过未完成的子主题，"
            "按 deep-research 第 8 节用已有材料写报告，开头注明“部分研究”及未完成的子主题。"
        )
    if reason == "budget":
        return (
            "本轮工具额度已用尽。不要再调用任何工具；立即用已有材料写最终回复"
            "（研究时为完整报告，开头注明“已达工具额度”）。"
        )
    if reason == "rounds":
        return (
            "本轮步骤数已达上限。不要再调用任何工具；现在用已有材料写最终回复（研究时为完整报告）。"
        )
    raise ValueError(f"未知的收尾原因：{reason!r}")


def stop_summary_messages(state: TurnState, topic: str) -> list[dict[str, str]]:
    """停止摘要（Task 5）：只含已完成子主题的摘要与来源标题，要求 2–3 句"目前发现"。"""
    lines = [f"研究主题：{topic}"]
    for item in state.todo:
        sub = state.subtopics.get(item.id)
        if sub is not None and sub.status == "done" and sub.summary:
            lines.append(f"子主题「{item.title}」摘要：{sub.summary}")
    titles = [f"[{s.n}] {s.title}" for s in state.sources.all() if not s.origin]
    if titles:
        lines.append("已阅读的来源：\n" + "\n".join(titles))
    return [
        {
            "role": "system",
            "content": "用 2–3 句中文概括这项被停止的研究目前的发现；只依据给出的材料，不要编造。",
        },
        {"role": "user", "content": "\n\n".join(lines)},
    ]


def strategy_section(skill_body: str) -> str:
    """deep-research SKILL.md 的第 7 节（搜索与阅读策略）；缺失时给出内置的简版。"""
    lines = skill_body.split("\n")
    start = next((i for i, ln in enumerate(lines) if ln.startswith("## 7.")), None)
    if start is None:
        return _DEFAULT_STRATEGY
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    return "\n".join(lines[start + 1 : end]).strip() or _DEFAULT_STRATEGY


_DEFAULT_STRATEGY = """- 先宽后窄：先用概括性查询了解全貌，再针对缺口用具体查询。
- 先看搜索摘要再决定读哪页；只读最可能含一手信息的页面。
- 同一 URL 不读两次；需要重看时用 read_source(n)。
- 搜索结果的摘要不是证据：没读过的页面不能引用。"""


def tool_names(schemas: list[dict[str, Any]]) -> list[str]:
    return [s["function"]["name"] for s in schemas]


# ---- 停止之后的控制（Task 5） ----

FINISH_SKIPPED = "已跳过：用户要求立即写报告"
UNANSWERED_ON_RESTORE = "用户未回答（研究已恢复）：按默认假设继续，并在报告“背景与范围”中写明。"
NO_MATERIAL_REPLY = "尚无可用材料，未生成报告。"


def restore_instruction(budget: int) -> str:
    """恢复种子追加到编排转录末尾的说明（user 消息）。"""
    return f"这是对之前被停止研究的恢复，获得新的 {budget} 次工具额度；不要重复已完成的子主题。"


def answer_text(questions: list[dict[str, Any]], answers: list[Any]) -> str:
    """用户对 ask_user 的回答 → 该提问调用的 tool 消息："用户的回答：\n1. 问题 → 选项"。

    answers 按 openapi Answer：{question_id: 题号 "1"…, choice | other}；顺序按题号。"""
    by_id: dict[str, Any] = {}
    for a in answers:
        if isinstance(a, dict) and isinstance(a.get("question_id"), str):
            by_id.setdefault(a["question_id"], a)
    lines = ["用户的回答："]
    for i, q in enumerate(questions, 1):
        a = by_id.get(str(i))
        if a is None:
            said = "（未回答）"
        elif isinstance(a.get("other"), str) and a["other"].strip():
            said = "其他：" + " ".join(a["other"].split())[:500]
        else:
            said = str(a.get("choice", "")).strip()[:200] or "（未回答）"
        lines.append(f"{i}. {q.get('question', '')} → {said}")
    return "\n".join(lines)


def carryover_lines(state: TurnState) -> str:
    """被取代 turn 的发现：已完成子主题的标题与摘要、来源列表（编号为该 turn 的编号，由调用方
    在并入本轮来源表后改写）。没有任何发现时为空串。"""
    lines: list[str] = []
    for item in state.todo:
        sub = state.subtopics.get(item.id)
        if sub is not None and sub.status == "done" and sub.summary:
            lines.append(f"### 子主题「{item.title}」\n{sub.summary.strip()}")
    srcs = [s for s in state.sources.all() if not s.origin]
    if srcs:
        lines.append(
            "来源（本轮可用 read_source(n) 重读，也可直接以 [n] 引用）：\n"
            + "\n".join(f"- [{s.n}] {_clip(s.title or s.url, 80)}" for s in srcs)
        )
    return "\n\n".join(lines)
