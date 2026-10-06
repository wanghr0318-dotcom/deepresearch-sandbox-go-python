"""编排循环（kimi-k3）：回答或研究、ask_user 暂停、额度用尽与收尾、报告。

循环可从任意 checkpoint 重入：待执行的工具调用由转录推出（最后一个 assistant 消息中尚无
tool 结果的调用），提示词与工具参数只由状态确定性地生成。每个工具调用之后、每个子主题完成
之后写 checkpoint；收尾指令在模型调用之前按已提交的状态确定性地追加，故恢复后的请求体不变。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from agentbox_worker.errors import BudgetExhausted, WorkerFailure
from agentbox_worker.runtime import Paused, TaskContext
from agentbox_worker.tools import GatewayLike, ToolContext, ToolRegistry, ToolResult
from agentbox_worker.tools.ask_user import AskUser
from agentbox_worker.tools.run_python import RunPython
from agentbox_worker.tools.skills import RESEARCH_SKILL, ReadSkill, SkillCatalog
from agentbox_worker.tools.sources import ReadSource
from agentbox_worker.tools.todo import TodoItem, TodoWrite
from agentbox_worker.tools.web_fetch import WebFetch
from agentbox_worker.tools.web_search import WebSearch
from chatagent.config import TurnConfig
from chatagent.events import Emitter
from chatagent.model import ModelReply, assistant_message, call_model
from chatagent.prompts import (
    FINISH_SKIPPED,
    NO_MATERIAL_REPLY,
    answer_text,
    finish_instruction,
    history_messages,
    orchestrator_system,
    strategy_section,
)
from chatagent.report import (
    citation_pass,
    publish_report,
    render_report,
    report_summary,
    split_title,
)
from chatagent.research import ResearchPhase
from chatagent.state import SessionMemory, TurnState
from chatagent.stop import can_finish, stop_card, stop_summary
from chatagent.subtopic import RUN_SUBTOPIC, ResearchSubtopic, run_subtopic

ORCH = "orch"
ASK_STEP = "ask"
STOP_STEP = "stop"
PARTIAL_NOTE = "部分研究"
MAX_REFS = 1024
THINKING_MAX_CHARS = 600
SKIPPED_FOR_QUESTION = "已跳过：等待用户回答"
COST_BUDGET_NOTE = "预算耗尽，部分结果"
REPORT_POINTER = "完整报告见右侧「报告」"
FORCED_READ_ID = "forced-read-skill"
FIXED_PLAN_ID = "fixed-plan"
FIXED_RESEARCH_ID = "fixed-research"
# run_python 在回答与研究两条路线都可用；子主题循环（sub_registry）只搜索与阅读
_BASE_TOOLS = ("read_skill", "web_search", "web_fetch", "read_source", "run_python")


@dataclass(frozen=True)
class LoopOutcome:
    kind: str  # "reply" | "report" | "awaiting" | "paused"
    text: str = ""  # reply：回复全文；report：报告 Markdown（已核对）
    checkpoint_id: str = ""  # awaiting / paused
    reply: str = ""  # 本轮对话中的回复文本（turn result 的 summary）
    memo: str = ""  # 进入会话记忆的回复摘要（报告轮为报告摘要段）
    report: dict[str, Any] | None = None  # {"artifact_id","version","title"}
    question_id: str = ""  # awaiting


class _Pause(Exception):
    """commit 后发现宿主的暂停请求：展开到 run() 返回 paused。"""

    def __init__(self, paused: Paused) -> None:
        super().__init__(paused.checkpoint_id)
        self.paused = paused


def pending_calls(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """最后一个 assistant 消息中尚无 tool 结果的工具调用（按原顺序）。"""
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if msg.get("role") == "assistant":
            answered = {m.get("tool_call_id") for m in messages[i + 1 :] if m.get("role") == "tool"}
            return [tc for tc in msg.get("tool_calls") or [] if tc["id"] not in answered]
        if msg.get("role") not in ("tool",):
            return []
    return []


def _args(raw: str) -> Any:
    try:
        return json.loads(raw) if raw.strip() else {}
    except (ValueError, RecursionError):
        return raw


def _tool_count(messages: list[dict[str, Any]]) -> int:
    return sum(1 for m in messages if m.get("role") == "tool")


class Agent:
    def __init__(
        self,
        ctx: TaskContext,
        cfg: TurnConfig,
        state: TurnState,
        memory: SessionMemory,
        *,
        gateway: GatewayLike,
        skills: SkillCatalog,
        emit: Emitter,
        carry: str | None = None,
    ) -> None:
        self.ctx = ctx
        self.cfg = cfg
        self.state = state
        self.memory = memory
        self.gw = gateway
        self.skills = skills
        self.emit = emit
        self.carry = carry
        # 宿主协商了 sub-run 扩展时，子主题作为 sub-run 并行执行（chatagent.research）；
        # 否则按 Plan 13 在进程内逐个执行
        self.subruns = ctx.subruns.negotiated
        # 工具实例每轮新建（无跨轮可变状态）
        self.registry = ToolRegistry(
            [
                ReadSkill(),
                WebSearch(),
                WebFetch(),
                ReadSource(),
                RunPython(),
                AskUser(),
                TodoWrite(),
                ResearchSubtopic(state, parallel=self.subruns),
            ]
        )
        self.sub_registry = ToolRegistry([WebSearch(), WebFetch(), ReadSource()])
        self._strategy: str | None = None

    # ---- 入口 ----

    async def run(
        self, *, finish: bool = False, answer: dict[str, Any] | None = None
    ) -> LoopOutcome:
        """finish：用户要求立即写报告（directive finish_now）；answer：directive answer。"""
        try:
            if finish:
                out = await self._apply_finish()
                if out is not None:
                    return out
            if answer is not None:
                await self._apply_answer(answer)
            return await self._run()
        except _Pause as p:
            return LoopOutcome("paused", checkpoint_id=p.paused.checkpoint_id)
        except BudgetExhausted:
            return await self._cost_budget_exhausted()

    async def commit(self, step_id: str) -> Paused | None:
        """压缩状态 → 写 checkpoint；之后宿主已请求暂停时返回 Paused。"""
        st = self.state
        st.compact()
        await self.ctx.checkpoint(step_id, state=st.to_json(), refs=st.refs[-MAX_REFS:])
        if self.ctx.should_pause():
            return await self._stop()
        return None

    async def _stop(self) -> Paused:
        """宿主请求暂停：停止摘要（模型调用在 "stop" checkpoint 之前，call id 计数器随之保存）
        → "stop" checkpoint → turn_stopped 停止卡。"""
        st = self.state
        st.stops += 1
        findings = await stop_summary(self.ctx, self.gw, st, self.cfg)
        st.stop_findings = findings
        st.compact()
        cp = await self.ctx.checkpoint(STOP_STEP, state=st.to_json(), refs=st.refs[-MAX_REFS:])
        await self.emit.turn_stopped(stop_card(st), findings, can_finish(st))
        return Paused(cp)

    async def checkpoint(self, step_id: str) -> None:
        paused = await self.commit(step_id)
        if paused is not None:
            raise _Pause(paused)

    def strategy(self) -> str:
        if self._strategy is None:
            try:
                body = self.skills.read(RESEARCH_SKILL)
            except (ValueError, OSError):
                body = ""
            self._strategy = strategy_section(body)
        return self._strategy

    pending_calls = staticmethod(pending_calls)

    # ---- 编排 ----

    async def _run(self) -> LoopOutcome:
        st = self.state
        if st.phase == "start":
            await self._start()
        while True:
            pending = pending_calls(st.messages)
            if pending:
                out = await self._run_calls(pending)
                if out is not None:
                    return out
                continue
            if st.restore_note:  # 恢复说明：在下一次编排模型调用之前追加到转录末尾
                st.messages.append({"role": "user", "content": st.restore_note})
                st.restore_note = None
            self._maybe_close()
            tools = None if st.closing else self.registry.schemas(self._available())
            reply = await asyncio.to_thread(
                call_model,
                self.gw,
                ORCH,
                st.messages,
                model=self.cfg.orchestrator_model,
                max_tokens=(
                    self.cfg.report_max_tokens if st.closing else self.cfg.orchestrator_max_tokens
                ),
                tools=tools,
            )
            st.rounds += 1
            st.add_refs([reply.blob])
            await self.thinking(ORCH, reply)
            st.messages.append(assistant_message(reply))
            if not reply.tool_calls:
                return await self._final(reply.content)

    async def _start(self) -> None:
        st, cfg = self.state, self.cfg
        system = orchestrator_system(self.skills, self.registry, self.memory, cfg, self.carry)
        st.messages = [
            {"role": "system", "content": system},
            *history_messages(self.memory),
            {"role": "user", "content": cfg.text},
        ]
        st.phase = "orchestrating"
        if not (cfg.deep_research or cfg.fixed_plan):
            return
        # 深度研究开关：由运行时直接读取 skill，不依赖模型遵守指令
        args = json.dumps({"name": RESEARCH_SKILL}, ensure_ascii=False)
        result = await self._synthetic_call(FORCED_READ_ID, "read_skill", args)
        if not result.ok:
            raise WorkerFailure(
                "skills_unavailable", f"无法读取 {RESEARCH_SKILL}：{result.content}"
            )
        await self._skill_events(ORCH, result)
        if cfg.fixed_plan:
            await self._fixed_plan(cfg.fixed_plan)
        await self.checkpoint(ORCH)

    async def _synthetic_call(self, call_id: str, name: str, args: str) -> ToolResult:
        """运行时代替模型发起的工具调用：assistant 调用消息 + 执行 + tool 结果消息（无事件）。"""
        st = self.state
        call = {"id": call_id, "type": "function", "function": {"name": name, "arguments": args}}
        st.messages.append({"role": "assistant", "content": "", "tool_calls": [call]})
        result = await asyncio.to_thread(
            self.registry.dispatch, name, args, self._tool_ctx(ORCH, st.messages, None)
        )
        st.messages.append(_tool_message(call_id, name, result.content))
        return result

    async def _fixed_plan(self, items: tuple[dict[str, Any], ...]) -> None:
        """research.fixed_plan（串并行对比的运维路径）：跳过规划，按给定清单写计划，并以合成的
        research_subtopic 调用开始研究阶段（由编排循环执行）。"""
        st = self.state
        args = json.dumps({"items": list(items)}, ensure_ascii=False)
        result = await self._synthetic_call(FIXED_PLAN_ID, "todo_write", args)
        if not result.ok:
            raise WorkerFailure("invalid_config", f"research.fixed_plan 不可用：{result.content}")
        await self._after_tool(ORCH, "todo_write", result)
        first = next(i.id for i in st.todo if i.budget > 0)
        call = {
            "id": FIXED_RESEARCH_ID,
            "type": "function",
            "function": {
                "name": "research_subtopic",
                "arguments": json.dumps({"id": first}, ensure_ascii=False),
            },
        }
        st.messages.append({"role": "assistant", "content": "", "tool_calls": [call]})

    def _available(self) -> list[str]:
        names = list(_BASE_TOOLS)
        if self.state.flags.skill_read:
            names += ["ask_user", "todo_write"]
        if self.state.flags.planned:
            names.append("research_subtopic")
        return names

    def _maybe_close(self) -> None:
        """模型调用之前：需要收尾而尚未追加收尾指令时追加（确定性，只取决于已提交的状态）。"""
        st = self.state
        if st.closing is not None:
            return
        reason = None
        if st.finish_requested:
            reason = "user_finish"
        elif st.budget.remaining() == 0:
            reason = "budget"
            note = f"已达工具额度（{st.budget.used}/{st.budget.limit}）"
            if note not in st.notes:
                st.notes.append(note)
        elif st.rounds >= self.cfg.max_orchestrator_rounds:
            reason = "rounds"
        if reason is not None:
            st.messages.append({"role": "user", "content": finish_instruction(reason)})
            st.closing = reason

    async def _run_calls(self, pending: list[dict[str, Any]]) -> LoopOutcome | None:
        st = self.state
        for k, tc in enumerate(pending):
            pq = st.pending_question
            if pq is not None and tc["id"] == pq.get("tool_call_id"):
                return await self._awaiting(re_ask=True)
            result = await self.execute(ORCH, tc, st.messages, self.registry, None)
            if result.control == "await_user":
                data = result.data or {}
                st.pending_question = {
                    "question_id": data["question_id"],
                    "tool_call_id": tc["id"],
                    "questions": data["questions"],
                }
                for rest in pending[k + 1 :]:
                    st.messages.append(
                        _tool_message(rest["id"], rest["function"]["name"], SKIPPED_FOR_QUESTION)
                    )
                return await self._awaiting(re_ask=False)
        return None

    async def _awaiting(self, *, re_ask: bool) -> LoopOutcome:
        pq = self.state.pending_question
        assert pq is not None
        await self.emit.ask_user(ORCH, pq["question_id"], pq["questions"])
        self.state.compact()
        cp = await self.ctx.checkpoint(
            ASK_STEP, state=self.state.to_json(), refs=self.state.refs[-MAX_REFS:]
        )
        return LoopOutcome("awaiting", checkpoint_id=cp, question_id=pq["question_id"])

    # ---- 停止之后的指令 ----

    async def _apply_finish(self) -> LoopOutcome | None:
        """立即写报告：未完成的计划项与子主题 → skipped，待执行的编排工具调用以"已跳过"作答，
        标记部分研究；随后的编排调用带收尾指令且不带工具。没有任何材料时直接回复（不调用模型）。

        幂等：崩溃后的新 attempt 再次带 finish_now 时不重复。"""
        st = self.state
        if not st.finish_requested:
            phase: ToolResult | None = None
            if self.subruns and st.phase_subs is not None:
                phase = await ResearchPhase(self).finish()  # 取消仍在运行的 sub-run
            st.finish_requested, st.partial = True, True
            st.stop_findings = None
            if PARTIAL_NOTE not in st.notes:
                st.notes.append(PARTIAL_NOTE)
            skipped = []
            for item in st.todo:
                if item.status != "done":
                    item.status = "skipped"
                sub = st.subtopics.get(item.id)
                if sub is not None and sub.status in ("pending", "running"):
                    sub.status = "skipped"
                    skipped.append((sub, item.title))
            st.phase, st.current_subtopic = "orchestrating", None
            st.pending_question = None
            for tc in pending_calls(st.messages):
                event_id = f"{ORCH}:{_tool_count(st.messages) + 1}"
                name, raw = tc["function"]["name"], tc["function"]["arguments"]
                result = ToolResult(content=FINISH_SKIPPED, ok=False)
                if name == "research_subtopic" and phase is not None:
                    result, phase = phase, None  # 研究阶段的结果（已完成与部分证据）
                elif name == "research_subtopic":  # 已完成的子主题仍把摘要交给编排模型
                    tctx = self._tool_ctx(ORCH, st.messages, None)
                    result = self.registry.dispatch(name, raw, tctx)
                st.messages.append(_tool_message(tc["id"], name, result.content))
                await self.emit.tool_result(ORCH, event_id, name, result, None, _args(raw))
            if st.todo:
                await self.emit.todo(ORCH, st.todo)
            for sub, title in skipped:
                await self.emit.subtopic(sub, title)
        if self._has_material():
            return None
        st.phase = "done"
        await self.emit.assistant_text(NO_MATERIAL_REPLY)
        return LoopOutcome(
            "reply", text=NO_MATERIAL_REPLY, reply=NO_MATERIAL_REPLY, memo=NO_MATERIAL_REPLY
        )

    async def _apply_answer(self, directive: dict[str, Any]) -> None:
        """用户回答：作为提问调用的 tool 消息恢复同一位置；question_id 须与待答的提问一致。"""
        st = self.state
        qid = directive.get("question_id")
        pq = st.pending_question
        if pq is None and qid in st.answered:  # 崩溃后的新 attempt 再次带同一回答
            return
        if pq is None or pq.get("question_id") != qid:
            want = pq.get("question_id") if pq is not None else None
            raise WorkerFailure("invalid_directive", f"回答的提问 {qid!r} 不是待答的提问 {want!r}")
        text = answer_text(pq.get("questions") or [], directive.get("answers") or [])
        st.messages.append(_tool_message(pq["tool_call_id"], "ask_user", text))
        index = str(qid).rsplit("-", 1)[-1]  # q-<step>-<call_index>：与 tool_call 事件同一 id
        event_id = f"{ORCH}:{index}" if index.isdigit() else f"{ORCH}:{_tool_count(st.messages)}"
        preview = {"kind": "text", "text": text[:THINKING_MAX_CHARS]}
        await self.emit.tool_result(
            ORCH, event_id, "ask_user", ToolResult(content=text, preview=preview), None
        )
        st.pending_question = None
        st.answered.append(str(qid))

    # ---- 工具执行（编排与子主题共用） ----

    def _tool_ctx(
        self, step: str, transcript: list[dict[str, Any]], subtopic_id: str | None
    ) -> ToolContext:
        st = self.state
        return ToolContext(
            gateway=self.gw,
            step_id=step,
            budget=st.budget,
            sources=st.sources,
            flags=st.flags,
            subtopic_id=subtopic_id,
            skills=self.skills,
            call_index=_tool_count(transcript) + 1,  # 确定性：转录中已有的工具结果数 + 1
        )

    async def execute(
        self,
        step: str,
        tc: dict[str, Any],
        transcript: list[dict[str, Any]],
        registry: ToolRegistry,
        subtopic_id: str | None,
    ) -> ToolResult:
        """执行一个工具调用：tool_call 事件 → 执行 → tool 消息 → tool_result 等事件 → checkpoint。

        ask_user 返回 control="await_user" 时不追加 tool 消息、不写 checkpoint（由调用方暂停）。"""
        st = self.state
        name, raw = tc["function"]["name"], tc["function"]["arguments"]
        args = _args(raw)
        tctx = self._tool_ctx(step, transcript, subtopic_id)
        # 事件中的配对 id：模型的工具调用 id 可能跨轮重复（如 kimi 的 "web_search:0"），
        # 改用本轮内唯一且确定的 "<step>:<序号>"；恢复后重发同一调用得到同一 id。
        event_id = f"{step}:{tctx.call_index}"
        await self.emit.tool_call(step, event_id, name, args, subtopic_id)
        if name == "research_subtopic" and registry is self.registry:
            result = registry.dispatch(name, raw, tctx)
            if self.subruns and (result.control == RUN_SUBTOPIC or st.phase_subs is not None):
                phase = await ResearchPhase(self).run()  # 全部子主题作为 sub-run 执行
                if phase is None:  # 各 sub-run 已在工具边界返回：停止
                    raise _Pause(await self._stop())
                result = phase
            elif result.control == RUN_SUBTOPIC:
                result = await run_subtopic(self, (result.data or {})["id"])
        else:
            result = await asyncio.to_thread(registry.dispatch, name, raw, tctx)
        if result.control == "await_user":
            return result
        transcript.append(_tool_message(tc["id"], name, result.content))
        await self.emit.tool_result(step, event_id, name, result, subtopic_id, args)
        await self._after_tool(step, name, result)
        st.add_refs(result.blobs)
        await self.checkpoint(step)
        return result

    async def _after_tool(self, step: str, name: str, result: ToolResult) -> None:
        tool = self.registry.get(name) or self.sub_registry.get(name)
        if tool is not None and tool.counts_budget:
            await self.emit.budget(self.state.budget)
        if not result.ok:
            return
        if name == "todo_write":
            self._adopt_plan(result.data or {})
            await self.emit.todo(step, self.state.todo)
        elif name == "read_skill":
            await self._skill_events(step, result)

    def _adopt_plan(self, data: dict[str, Any]) -> None:
        st = self.state
        items = [TodoItem.from_json(d) for d in data.get("items", [])]
        for item in items:  # 已完成的子主题保持 done（模型改写计划时不回退）
            sub = st.subtopics.get(item.id)
            if sub is not None and sub.status == "done":
                item.status = "done"
        st.todo = items

    async def _skill_events(self, step: str, result: ToolResult) -> None:
        data = result.data or {}
        name = data.get("name", "")
        await self.emit.skill_read(step, name, data.get("description", ""), data.get("file"))
        if name == RESEARCH_SKILL and data.get("file") is None and self.state.route != "research":
            self.state.route = "research"
            await self.emit.route("research", forced=self.cfg.deep_research)

    async def thinking(self, step: str, reply: ModelReply) -> None:
        """推理内容，或带工具调用时的正文 → thinking（≤ 600 字）。"""
        text = reply.reasoning or (reply.content if reply.tool_calls else "")
        if text:
            await self.emit.thinking(
                step, text[:THINKING_MAX_CHARS], reply.call_id, reply.request, reply.blob
            )

    # ---- 收尾 ----

    def _has_material(self) -> bool:
        st = self.state
        done = any(s.status == "done" for s in st.subtopics.values())
        remembered = {s.origin for s in self.memory.sources}  # carryover 的来源也算材料
        return done or any(not s.origin or s.origin not in remembered for s in st.sources.all())

    async def _final(self, content: str) -> LoopOutcome:
        st = self.state
        if st.route is None:
            st.route = "answer"
            await self.emit.route("answer", forced=False)
        if st.route == "research" and self._has_material():
            return await self._report(content)
        st.phase = "done"
        await self.emit.assistant_text(content)
        return LoopOutcome("reply", text=content, reply=content, memo=content)

    async def _report(self, text: str) -> LoopOutcome:
        st = self.state
        st.phase = "reporting"
        body, cited = citation_pass(text, st.sources)
        title, body = split_title(body, default=self.cfg.text)
        notes = list(st.notes)
        markdown = render_report(body, title, st.sources, cited, notes)
        ref = await publish_report(self.ctx, markdown)
        reached = st.closing == "budget" or st.budget.remaining() == 0
        await self.emit.report_ready(ref, title, st.partial, "；".join(notes) or None, reached)
        summary = report_summary(markdown)
        reply = f"{summary}\n\n{REPORT_POINTER}" if summary else REPORT_POINTER
        await self.emit.assistant_text(reply)
        st.phase = "done"
        report = {"artifact_id": ref.artifact_id, "version": ref.version, "title": title}
        return LoopOutcome("report", text=markdown, reply=reply, memo=summary, report=report)

    async def _cost_budget_exhausted(self) -> LoopOutcome:
        """费用预算耗尽（402）：有完成的子主题时用其摘要写部分报告，否则失败。"""
        st = self.state
        done = [
            (item, st.subtopics[item.id])
            for item in st.todo
            if item.id in st.subtopics and st.subtopics[item.id].status == "done"
        ]
        if not done:
            raise WorkerFailure("budget_exhausted", "费用预算已用尽，本轮未能完成")
        st.partial = True
        if COST_BUDGET_NOTE not in st.notes:
            st.notes.append(COST_BUDGET_NOTE)
        body = "\n\n".join(f"## {item.title}\n\n{sub.summary or ''}" for item, sub in done)
        return await self._report(f"# {self.cfg.text}\n\n{body}")


def _tool_message(tool_call_id: str, name: str, content: str) -> dict[str, Any]:
    return {"role": "tool", "tool_call_id": tool_call_id, "name": name, "content": content}
