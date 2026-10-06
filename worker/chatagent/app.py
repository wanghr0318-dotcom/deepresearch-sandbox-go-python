"""每轮入口：解析 config 与会话记忆，从头开始或从 checkpoint 继续，运行编排循环，
构造 Result（成功时提议新的会话记忆）或 Paused（awaiting_input / 宿主暂停）。

本任务覆盖开始与无 directive 的继续（含崩溃恢复）；directive（finish_now、answer）、
carryover 与恢复种子由 Task 5 接入。
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    GatewayError,
    WorkerFailure,
)
from agentbox_worker.runtime import Paused, Result, TaskContext
from agentbox_worker.session import SessionApp
from agentbox_worker.tools import GatewayLike
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.skills import SkillCatalog, default_skills_root
from agentbox_worker.tools.sources import Source
from agentbox_worker.tools.text import truncate_utf8
from chatagent.config import parse_turn_config
from chatagent.events import Emitter
from chatagent.loop import Agent, LoopOutcome
from chatagent.state import SessionMemory, TurnRecord, TurnState

SUMMARY_MAX_BYTES = 60 * 1024  # result.summary（协议上限 64 KiB，留余量）


def _log(message: str) -> None:
    print(f"chatagent: {message}", file=sys.stderr)


def _memory(ctx: TaskContext) -> SessionMemory:
    raw = ctx.session.state if ctx.session is not None else None
    try:
        return SessionMemory.from_json(raw)
    except ValueError as exc:  # 记忆损坏不应让会话永久不可用：本轮以空记忆进行
        _log(f"会话记忆不合法，按空记忆继续：{exc}")
        return SessionMemory()


def _state(ctx: TaskContext, tool_budget: int, memory: SessionMemory) -> TurnState:
    if ctx.resume is not None:
        try:
            return TurnState.from_json(ctx.resume.state)
        except ValueError as exc:
            raise WorkerFailure("invalid_state", f"checkpoint 中的轮次状态不合法：{exc}") from exc
    state = TurnState(budget=TurnBudget(limit=tool_budget))
    state.sources.adopt(memory.sources)  # 既往来源续编号，可被 read_source 与本轮报告引用
    return state


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
    fresh = [s for s in state.sources.all() if not s.origin]
    new_memory = memory.with_turn(record, _origin(fresh, ctx.task_id))
    return Result(
        summary=truncate_utf8(out.reply, SUMMARY_MAX_BYTES),
        outputs=[out.report["artifact_id"]] if out.report else [],
        session_state=new_memory.to_json(),
    )


def _origin(sources: list[Source], task_id: str) -> list[Source]:
    return [Source(s.n, s.sha256, s.url, s.title, s.excerpt, s.call_id, task_id) for s in sources]


def make_app(
    *,
    gateway: Callable[[TaskContext], GatewayLike] | None = None,
    skills_root: Path | None = None,
) -> SessionApp:
    """测试注入 ScriptedGateway（其 call_ids 应取 ctx.call_ids）与临时 skills 目录。"""

    async def app(ctx: TaskContext) -> Result | Paused:
        cfg = parse_turn_config(ctx.config)
        if ctx.directive is not None:
            kind = ctx.directive.get("kind")
            raise WorkerFailure("unsupported_directive", f"尚不支持的指令：{kind!r}")
        memory = _memory(ctx)
        state = _state(ctx, cfg.tool_budget, memory)
        gw = gateway(ctx) if gateway is not None else ctx.gateway
        agent = Agent(
            ctx,
            cfg,
            state,
            memory,
            gateway=gw,
            skills=_skills(skills_root),
            emit=Emitter(ctx),
        )
        try:
            out = await agent.run()
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
