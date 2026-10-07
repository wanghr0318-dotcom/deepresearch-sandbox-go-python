"""停止：停止卡（零成本，取自状态）与 kimi-k2.6 写的 2–3 句"目前发现"。

停止摘要的模型调用发生在最后一个（"stop"）checkpoint 之前，call id 计数器随之保存；摘要失败或
为空时退回固定句子，停止本身从不失败。没有任何发现（无已完成子主题摘要、无本轮来源）时不调用
模型，给出 NO_FINDINGS。

停止摘要有期限（STOP_DEADLINE_S，自停止路径开始计）：被放弃的在途调用仍占着 Gateway 的每任务
在途槽位时摘要会排队；超过期限即放弃等待，停止卡用确定性的固定句子。被放弃的 stop-<stops> 调用
已占的号随 "stop" checkpoint 保存；之后的停止 stops 递增、用新的 step id，它不会以别的请求体重发。
"""

from __future__ import annotations

import sys
import time
from typing import Any

from agentbox_worker.errors import CallAbandoned, GatewayError, WorkerFailure
from agentbox_worker.runtime import TaskContext
from agentbox_worker.tools import GatewayLike
from chatagent.config import TurnConfig
from chatagent.model import call_model
from chatagent.prompts import stop_summary_messages
from chatagent.state import TurnState

NO_FINDINGS = "尚无发现，研究在规划阶段被停止"
STOP_MAX_TOKENS = 4096
FINDINGS_MAX_CHARS = 600
# 停止路径（回退 + 摘要）的期限（秒）：停止卡的目标是约 30 s 内出现，宿主兜底在 60 s
STOP_DEADLINE_S = 22.0


def _log(message: str) -> None:
    print(f"chatagent: {message}", file=sys.stderr)


def _done(state: TurnState) -> list[str]:
    return [sid for sid, s in state.subtopics.items() if s.status == "done"]


def _fresh_sources(state: TurnState) -> int:
    return sum(1 for s in state.sources.all() if not s.origin)


def _view(state: TurnState) -> TurnState:
    """并行研究中途停止时，各 sub-run 的新来源尚未并入全局来源表：按计划顺序合并的副本。"""
    if any(s.found and not s.merged for s in state.subtopics.values()):
        return state.merged_copy()
    return state


def stop_card(state: TurnState) -> dict[str, Any]:
    """停止卡（契约 L：StopCard）：子主题完成数/总数、本轮来源数、工具额度，附计划清单。"""
    state = _view(state)
    return {
        "subtopics_done": len(_done(state)),
        "subtopics_total": sum(1 for i in state.todo if i.budget > 0),
        "sources": _fresh_sources(state),
        "tool_calls_used": state.budget.used,
        "tool_call_limit": state.budget.limit,
        "todo": [
            {"id": i.id, "title": i.title, "status": i.status, "budget_share": i.budget}
            for i in state.todo
        ],
    }


def can_finish(state: TurnState) -> bool:
    """至少一个子主题已完成（"立即写报告"可用）。"""
    return bool(_done(state))


def findings_messages(state: TurnState, topic: str) -> list[dict[str, str]] | None:
    """无已完成子主题摘要且无本轮来源 → None（不调用模型）；否则为确定性的摘要请求。"""
    state = _view(state)
    has_summary = any(s.status == "done" and s.summary for s in state.subtopics.values())
    if not has_summary and _fresh_sources(state) == 0:
        return None
    return stop_summary_messages(state, topic)


def fallback_findings(state: TurnState) -> str:
    state = _view(state)
    return f"已完成 {len(_done(state))} 个子主题、阅读 {_fresh_sources(state)} 个来源。"


async def stop_summary(
    ctx: TaskContext,
    gw: GatewayLike,
    state: TurnState,
    cfg: TurnConfig,
    *,
    started: float | None = None,
) -> str:
    """kimi-k2.6 写 2–3 句"目前发现"（step_id=stop-<stops>）；失败、为空或超过期限 → 固定句子。

    started：停止路径开始的 time.monotonic()（缺省为现在）；期限 STOP_DEADLINE_S 从它算起。"""
    messages = findings_messages(state, cfg.text)
    if messages is None:
        return NO_FINDINGS
    begin = time.monotonic() if started is None else started
    remaining = STOP_DEADLINE_S - (time.monotonic() - begin)
    try:
        reply = await ctx.run_call_within(
            remaining,
            call_model,
            gw,
            f"stop-{state.stops}",
            messages,
            model=cfg.worker_model,
            max_tokens=STOP_MAX_TOKENS,
            tools=None,
        )
    except CallAbandoned as exc:  # 超过期限：不再等它，停止卡立即写出
        _log(f"停止摘要未能及时完成，使用固定句子：{exc}")
        return fallback_findings(state)
    except (GatewayError, WorkerFailure) as exc:  # 停止从不因摘要失败
        _log(f"停止摘要失败，使用固定句子：{exc}")
        return fallback_findings(state)
    state.add_refs([reply.blob])
    text = reply.content.strip()
    if not text:
        return fallback_findings(state)
    return text if len(text) <= FINDINGS_MAX_CHARS else text[: FINDINGS_MAX_CHARS - 1] + "…"
