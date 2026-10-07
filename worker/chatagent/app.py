"""每轮入口：解析 config 与会话记忆，决定从头开始还是从 checkpoint 继续，运行编排循环，
构造 Result（成功时提议新的会话记忆）或 Paused（awaiting_input / 宿主暂停）。

入口决策（resume_mode）：
- fresh：无 resume，新建状态；有 carryover 时经 Gateway 读取被取代 turn 的 checkpoint，其发现进入
  系统提示、来源并入本轮来源表（续编号，摘要中的 [n] 随之改写）；读不到或损坏时不带它继续。
- continue：无 directive 的 resume（继续、崩溃恢复）：从 checkpoint 原处继续，额度沿用。
- finish：directive finish_now；answer：directive answer{question_id, answers}。
- restore：宿主把被取消 turn 的最新 checkpoint 复制为本 task 的种子（restored_from_task_id 仅用于
  展示；种子由另一个 task 写出）→ seed_restore（新额度）后从原处继续。
成功（回复或报告）时提议新的会话记忆；paused、awaiting 与失败不提议（宿主保持 base）。
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    GatewayError,
    WorkerFailure,
)
from agentbox_worker.gateway import GatewayClient
from agentbox_worker.runtime import RESERVED_STATE_KEY, Paused, Result, TaskContext
from agentbox_worker.session import SessionApp
from agentbox_worker.tools import GatewayLike
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.skills import SkillCatalog, default_skills_root
from agentbox_worker.tools.sources import Source, SourceStore
from agentbox_worker.tools.text import renumber_citations, truncate_utf8
from chatagent.config import parse_turn_config
from chatagent.events import Emitter
from chatagent.loop import Agent, LoopOutcome
from chatagent.prompts import UNANSWERED_ON_RESTORE, carryover_lines, restore_instruction
from chatagent.state import SessionMemory, TurnRecord, TurnState

SUMMARY_MAX_BYTES = 60 * 1024  # result.summary（协议上限 64 KiB，留余量）
RESTORE_SHARE_RESERVE = 4  # 恢复时为编排保留的额度（子主题剩余份额缩放到 ≤ limit - 4）


def _log(message: str) -> None:
    print(f"chatagent: {message}", file=sys.stderr)


def _memory(ctx: TaskContext) -> SessionMemory:
    raw = ctx.session.state if ctx.session is not None else None
    try:
        return SessionMemory.from_json(raw)
    except ValueError as exc:  # 记忆损坏不应让会话永久不可用：本轮以空记忆进行
        _log(f"会话记忆不合法，按空记忆继续：{exc}")
        return SessionMemory()


# ---- 入口决策 ----


def resume_mode(ctx: TaskContext) -> str:
    """返回 fresh、continue、finish、answer 或 restore（见模块说明）。"""
    directive = ctx.directive
    if directive is not None:
        kind = directive.get("kind")
        if kind not in ("finish_now", "answer"):
            raise WorkerFailure("invalid_directive", f"未知的指令：{kind!r}")
        if ctx.resume is None:
            raise WorkerFailure("invalid_directive", f"指令 {kind} 需要从 checkpoint 继续")
        return "finish" if kind == "finish_now" else "answer"
    if ctx.resume is None:
        return "fresh"
    raw = ctx.resume.state
    owner = raw.get("task_id") if isinstance(raw, dict) else None
    if ctx.restored_from_task_id and owner != ctx.task_id:
        return "restore"
    return "continue"


def seed_restore(state: TurnState, budget_limit: int) -> TurnState:
    """被取消 turn 的状态 → 本 task 的起点：新额度（未完成子主题的剩余份额按原比例缩放到
    ≤ limit - 4）；stops 保留；进行中 → pending；待答的提问以"未回答"作答并清除；
    finish_requested、partial、notes 清除；恢复说明在下一次编排模型调用之前追加到转录末尾。"""
    old = state.budget
    ended = {sid for sid, s in state.subtopics.items() if s.status in ("done", "skipped", "failed")}
    left = {
        sid: max(0, share - old.spent.get(sid, 0))
        for sid, share in old.shares.items()
        if sid not in ended
    }
    cap, total = max(0, budget_limit - RESTORE_SHARE_RESERVE), sum(left.values())
    if total > cap:
        left = {sid: v * cap // total for sid, v in left.items()}
    state.budget = TurnBudget(limit=budget_limit, shares=left)
    for sid, sub in state.subtopics.items():  # sub-run 的份额同样缩放（新 task 的新额度）
        if sub.subrun_id and sid in left:
            sub.share, sub.exhausted = sub.spent + left[sid], False
    for item in state.todo:
        if item.id in left:
            item.budget = left[item.id]
        if item.status == "in_progress":
            item.status = "pending"
    pq = state.pending_question
    if pq is not None:
        state.messages.append(
            {
                "role": "tool",
                "tool_call_id": pq["tool_call_id"],
                "name": "ask_user",
                "content": UNANSWERED_ON_RESTORE,
            }
        )
        state.pending_question = None
    state.finish_requested = False
    state.partial = False
    state.notes = []
    state.stop_findings = None
    state.restore_note = restore_instruction(budget_limit)
    return state


# ---- carryover（被取代 turn 的上下文，设计 D5） ----


def carryover_context(carry: dict[str, Any] | None) -> tuple[str | None, list[Source]]:
    """carry = {"task_id", "state"}（被取代 turn 最新 checkpoint 的内容）→ (发现文本, 来源)。

    文本含已完成子主题的标题与摘要及来源列表，[n] 为该 turn 的编号（由 adopt_carryover 改写）；
    来源为该 turn 的全部来源，其本轮来源的 origin 改为该 task_id。状态无法解析 → (None, [])。"""
    if not carry:
        return None, []
    raw = carry.get("state")
    if isinstance(raw, dict) and "schema_version" not in raw and isinstance(raw.get("state"), dict):
        raw = raw["state"]  # checkpoint 信封 {"state": …}
    if isinstance(raw, dict):
        raw = {k: v for k, v in raw.items() if k != RESERVED_STATE_KEY}
    try:
        # 并行研究中被取代时，各 sub-run 的新来源尚未并入全局来源表：按计划顺序合并
        state = TurnState.from_json(raw).merged_copy()  # type: ignore[arg-type]
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        _log(f"carryover 状态无法解析，本轮不带上一轮的发现：{exc}")
        return None, []
    origin = str(carry.get("task_id") or "carryover")
    sources = [
        Source(s.n, s.sha256, s.url, s.title, s.excerpt, s.call_id, s.origin or origin)
        for s in state.sources.all()
    ]
    return carryover_lines(state) or None, sources


def adopt_carryover(store: SourceStore, text: str | None, sources: list[Source]) -> str | None:
    """来源并入本轮来源表（续编号、跳过重复），文本中的 [n] 改写为本轮编号。"""
    store.adopt(sources)
    mapping: dict[int, int] = {}
    for s in sources:
        same = next((t for t in store.all() if t.sha256 == s.sha256 or t.url == s.url), None)
        if same is not None:
            mapping[s.n] = same.n
    return renumber_citations(text, mapping) if text else None


async def _read_carryover(ctx: TaskContext, gw: GatewayLike) -> dict[str, Any] | None:
    carry = ctx.carryover
    if not carry:
        return None
    try:
        data = await asyncio.to_thread(gw.read_blob, str(carry.get("checkpoint_ref", "")))
        state = json.loads(data)
    except (GatewayError, ValueError, KeyError, OSError, TypeError) as exc:
        _log(f"carryover 读取失败，本轮不带上一轮的发现：{exc}")
        return None
    return {"task_id": carry.get("task_id", ""), "state": state}


# ---- 状态与结果 ----


def _resumed_state(ctx: TaskContext) -> TurnState:
    assert ctx.resume is not None
    try:
        return TurnState.from_json(ctx.resume.state)
    except ValueError as exc:
        raise WorkerFailure("invalid_state", f"checkpoint 中的轮次状态不合法：{exc}") from exc


async def _state(
    ctx: TaskContext, mode: str, tool_budget: int, memory: SessionMemory, gw: GatewayLike
) -> tuple[TurnState, str | None]:
    """本轮起点状态与 carryover 文本（只在 fresh 时；恢复后系统提示已在转录中）。"""
    if mode != "fresh":
        state = _resumed_state(ctx)
        if mode == "restore":
            state = seed_restore(state, tool_budget)
        state.task_id = ctx.task_id
        state.stop_findings = None
        return state, None
    state = TurnState(budget=TurnBudget(limit=tool_budget), task_id=ctx.task_id)
    state.sources.adopt(memory.sources)  # 既往来源续编号，可被 read_source 与本轮报告引用
    text, sources = carryover_context(await _read_carryover(ctx, gw))
    return state, adopt_carryover(state.sources, text, sources)


def _skills(root: Path | None) -> SkillCatalog:
    try:
        return SkillCatalog.load(root or default_skills_root())
    except (ValueError, OSError) as exc:
        raise WorkerFailure("skills_unavailable", f"无法加载 skill 目录：{exc}") from exc


def _result(ctx: TaskContext, out: LoopOutcome, state: TurnState, memory: SessionMemory) -> Result:
    record = TurnRecord(
        task_id=ctx.task_id,
        user=parse_turn_config(ctx.config).text,
        reply=out.memo,
        route=state.route or "answer",
        report=out.report,
    )
    # 本轮来源与 carryover 的来源（被取代 turn 未提交会话记忆）进入会话记忆，origin=本轮
    remembered = {s.origin for s in memory.sources}
    fresh = [s for s in state.sources.all() if not s.origin or s.origin not in remembered]
    new_memory = memory.with_turn(record, _origin(fresh, ctx.task_id))
    return Result(
        summary=truncate_utf8(out.reply, SUMMARY_MAX_BYTES),
        outputs=[out.report["artifact_id"]] if out.report else [],
        session_state=new_memory.to_json(),
    )


def _origin(sources: list[Source], task_id: str) -> list[Source]:
    return [Source(s.n, s.sha256, s.url, s.title, s.excerpt, s.call_id, task_id) for s in sources]


def turn_gateway(ctx: TaskContext) -> GatewayClient:
    """本轮的 Gateway 客户端。call_in_progress 一直等到本次调用的客户端超时（而非 SDK 缺省的
    30 s）：停止时被放弃的调用仍在 Gateway 侧进行（规格 E19），很快"继续"时同一 ID 的重发应等它
    结束后重放，而不是以 CallInProgress 让本轮失败。sub-run 视图由它派生，沿用同一设置。"""
    gw = ctx.gateway
    gw.in_progress_wait_s = None
    return gw


def make_app(
    *,
    gateway: Callable[[TaskContext], GatewayLike] | None = None,
    skills_root: Path | None = None,
) -> SessionApp:
    """测试注入 ScriptedGateway（其 call_ids 应取 ctx.call_ids）与临时 skills 目录。"""

    async def app(ctx: TaskContext) -> Result | Paused:
        cfg = parse_turn_config(ctx.config)
        mode = resume_mode(ctx)
        memory = _memory(ctx)
        gw = gateway(ctx) if gateway is not None else turn_gateway(ctx)
        try:
            state, carry = await _state(ctx, mode, cfg.tool_budget, memory, gw)
            emit = Emitter(ctx)
            agent = Agent(
                ctx,
                cfg,
                state,
                memory,
                gateway=gw,
                skills=_skills(skills_root),
                emit=emit,
                carry=carry,
            )
            if mode == "restore" and state.route is not None:
                # 源 turn 的 route 事件属于源 turn：恢复出的 turn 补发一次，界面据此显示路径标签
                await emit.route(state.route, forced=False, restored=True)
            answer = ctx.directive if mode == "answer" else None
            out = await agent.run(finish=mode == "finish", answer=answer)
        except WorkerFailure:
            raise
        except AccessRevoked as exc:
            raise WorkerFailure("access_revoked", str(exc)) from exc
        except BudgetExhausted as exc:
            raise WorkerFailure("budget_exhausted", str(exc)) from exc
        except GatewayError as exc:
            raise WorkerFailure(exc.code or "gateway_error", str(exc), retryable=True) from exc
        if out.kind == "awaiting":
            return Paused(out.checkpoint_id, awaiting_input={"question_id": out.question_id})
        if out.kind == "paused":
            return Paused(out.checkpoint_id)
        return _result(ctx, out, state, memory)

    return app


run: SessionApp = make_app()
