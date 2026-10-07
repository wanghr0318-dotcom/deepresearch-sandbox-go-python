"""research_subtopic 工具与 kimi-k2.6 子主题子循环。

工具本身只做参数与规则检查（计划已写出、是子主题、未被跳过）；实际执行由编排循环拦截
（control="run_subtopic"），因为子循环要异步发事件并写 checkpoint。子循环的工具只有
web_search、web_fetch、read_source；份额或全局额度用尽、或轮数到上限时下一次调用不给工具，
要求"现在写子主题摘要"。子循环中模型不可用 → 该子主题 failed，编排继续。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agentbox_worker.tools import ToolArgsError, ToolContext, ToolResult
from chatagent.model import ModelUnavailable, assistant_message, call_model
from chatagent.prompts import SUBTOPIC_CLOSE, subtopic_system, subtopic_user
from chatagent.state import SubtopicState, TurnState

if TYPE_CHECKING:
    from chatagent.loop import Agent

RUN_SUBTOPIC = "run_subtopic"  # ToolResult.control：由编排循环执行子循环
PREVIEW_MAX_CHARS = 600


class ResearchSubtopic:
    name = "research_subtopic"
    description = (
        "执行计划中的一个子主题（由另一个模型按其简报搜索、阅读并写摘要），返回带 [n] 引用的摘要。"
        "须先 todo_write 写计划；一次执行一个；已完成的子主题不会重复执行。"
        "本身不消耗额度，子主题内部的搜索与阅读各自计数。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "id": {"type": "string", "minLength": 1, "description": "计划中子主题的 id"}
        },
        "required": ["id"],
        "additionalProperties": False,
    }
    counts_budget = False
    PARALLEL_DESCRIPTION = (
        "执行研究计划：运行时把计划中全部未完成的子主题同时交给各自独立的执行者（另一个模型，"
        "按简报搜索、阅读并写摘要），全部结束后一次返回各子主题带 [n] 引用的摘要，失败、超时或"
        "部分完成的子主题会标注“不完整”。须先 todo_write 写计划；id 填计划中任一子主题即可；"
        "已完成的子主题不会重复执行。本身不消耗额度，子主题内部的搜索与阅读各自计数。"
    )

    def __init__(self, state: TurnState, *, parallel: bool = False) -> None:
        self.state = state
        if parallel:  # 协商了 sub-run 扩展：一次调用执行整个研究阶段（chatagent.research）
            self.description = self.PARALLEL_DESCRIPTION

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        sid = args["id"]
        if not ctx.flags.planned:
            return ToolResult(content="不能执行子主题：先用 todo_write 写出计划", ok=False)
        item = self.state.todo_item(sid)
        if item is None:
            known = "、".join(i.id for i in self.state.todo if i.budget > 0) or "无"
            raise ToolArgsError(f"计划中没有 id 为 {sid!r} 的项（子主题：{known}）")
        if item.budget <= 0:
            return ToolResult(content=f"不能执行：{sid} 不是子主题（budget 为 0）", ok=False)
        sub = self.state.subtopics.get(sid)
        if sub is not None and sub.status == "done":
            return ToolResult(
                content=done_content(self.state, item.title, sub),
                preview={"kind": "text", "text": (sub.summary or "")[:PREVIEW_MAX_CHARS]},
            )
        if item.status == "skipped" or (sub is not None and sub.status in ("skipped", "failed")):
            return ToolResult(content=f"子主题 {sid} 已跳过或失败，不再执行", ok=False)
        return ToolResult(content="", control=RUN_SUBTOPIC, data={"id": sid})


def done_content(state: TurnState, title: str, sub: SubtopicState) -> str:
    return f"子主题「{title}」摘要：\n{sub.summary or ''}\n{state.budget.line()}"


async def run_subtopic(agent: Agent, sub_id: str) -> ToolResult:
    """执行（或从 checkpoint 继续）一个子主题，返回给编排模型的 tool 结果。"""
    st = agent.state
    item = st.todo_item(sub_id)
    assert item is not None  # ResearchSubtopic.run 已校验
    step = f"sub-{sub_id}"
    sub = st.subtopics.get(sub_id)
    if sub is None or sub.status == "pending":
        if st.budget.remaining() == 0:
            sub = SubtopicState(sub_id, status="skipped")
            st.subtopics[sub_id] = sub
            item.status = "skipped"
            await agent.emit.todo(step, st.todo)
            await agent.emit.subtopic(sub, item.title)
            text = f"子主题「{item.title}」未执行：本轮工具额度已用尽。"
            return ToolResult(
                content=f"{text}\n{st.budget.line()}",
                ok=False,
                preview={"kind": "text", "text": text},
            )
        sub = SubtopicState(sub_id, status="running")
        sub.messages = [
            {
                "role": "system",
                "content": subtopic_system(item, agent.strategy(), st.budget.line(sub_id)),
            },
            {"role": "user", "content": subtopic_user(item)},
        ]
        st.subtopics[sub_id] = sub
        item.status = "in_progress"
        await agent.emit.todo(step, st.todo)
        await agent.emit.subtopic(sub, item.title)
    elif item.status != "in_progress":  # 恢复种子把进行中的项改回 pending：继续时重新标记
        item.status = "in_progress"
        await agent.emit.todo(step, st.todo)
    st.phase, st.current_subtopic = "subtopic", sub_id
    try:
        summary = await _loop(agent, sub, step)
    except ModelUnavailable:
        sub.status, item.status = "failed", "skipped"
        st.phase, st.current_subtopic = "orchestrating", None
        await agent.emit.todo(step, st.todo)
        await agent.emit.subtopic(sub, item.title)
        await agent.checkpoint("orch")
        text = f"子主题「{item.title}」失败：模型服务暂时不可用；已收集的来源仍可引用。"
        return ToolResult(
            content=f"{text}\n{st.budget.line()}",
            ok=False,
            preview={"kind": "text", "text": text},
        )
    sub.status, sub.summary = "done", summary
    sub.sources = [s.n for s in st.sources.all() if not s.origin and f"/{step}/" in s.call_id]
    item.status = "done"
    st.phase, st.current_subtopic = "orchestrating", None
    await agent.emit.todo(step, st.todo)
    await agent.emit.subtopic(sub, item.title)
    await agent.checkpoint("orch")
    return ToolResult(
        content=done_content(st, item.title, sub),
        preview={"kind": "text", "text": summary[:PREVIEW_MAX_CHARS]},
    )


def _closing(agent: Agent, sub: SubtopicState) -> bool:
    budget = agent.state.budget
    left = budget.share_left(sub.id)
    return (
        sub.closing
        or budget.remaining() == 0
        or left == 0
        or sub.rounds >= agent.cfg.max_subtopic_rounds
    )


async def _loop(agent: Agent, sub: SubtopicState, step: str) -> str:
    st, cfg = agent.state, agent.cfg
    while True:
        pending = agent.pending_calls(sub.messages)
        if pending:
            for tc in pending:
                await agent.execute(step, tc, sub.messages, agent.sub_registry, sub.id)
            continue
        if _closing(agent, sub) and not sub.closing:
            sub.messages.append({"role": "user", "content": SUBTOPIC_CLOSE})
            sub.closing = True
        tools = None if sub.closing else agent.sub_registry.schemas()
        reply = await agent.call(
            call_model,
            agent.gw,
            step,
            sub.messages,
            model=cfg.worker_model,
            max_tokens=cfg.subtopic_max_tokens,
            tools=tools,
        )
        sub.rounds += 1
        st.add_refs([reply.blob])
        await agent.thinking(step, reply)
        sub.messages.append(assistant_message(reply))
        if not reply.tool_calls:
            return reply.content
