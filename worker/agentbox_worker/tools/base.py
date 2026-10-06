"""工具的统一接口：Tool 协议、调用上下文、结果，以及工具所用的 Gateway 能力（GatewayLike）。

工具不持有任何凭据，只经 ctx.gateway（SDK 的 GatewayClient 或测试替身）访问外部。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    GatewayError,
    ToolBudgetExhausted,
)
from agentbox_worker.gateway import GatewayResult

if TYPE_CHECKING:
    from agentbox_worker.tools.budget import TurnBudget
    from agentbox_worker.tools.skills import SkillCatalog
    from agentbox_worker.tools.sources import SourceStore


class GatewayLike(Protocol):
    def chat(
        self,
        step_id: str,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
    ) -> GatewayResult: ...

    def search(
        self, step_id: str, query: str, *, max_results: int = 5, no_cache: bool = False
    ) -> GatewayResult: ...

    def fetch(self, step_id: str, url: str, *, no_cache: bool = False) -> GatewayResult: ...

    def read_blob(self, sha256: str) -> bytes: ...


@dataclass
class TurnFlags:
    skill_read: bool = False  # 已 read_skill("deep-research")
    asked: bool = False  # 本轮已提问
    planned: bool = False  # 已 todo_write
    researching: bool = False  # 已发生 web_search/web_fetch


@dataclass(frozen=True)
class ToolResult:
    content: str  # 回给模型的文本（计数工具末尾附额度行）
    ok: bool = True
    preview: dict[str, Any] | None = None  # 契约 tool_result.preview
    raw: dict[str, Any] | None = None  # {"call_id": …} 或 {"inline": {"request": …, "response": …}}
    blobs: tuple[str, ...] = ()  # 本次得到的 Gateway 结果 blob（进入 refs）
    control: str | None = None  # None | "await_user" | "budget_exhausted"
    data: dict[str, Any] | None = None  # 工具特有的结构化输出（如 todo 快照、提问）


@dataclass
class ToolContext:
    gateway: GatewayLike
    step_id: str  # 本次调用所属步骤（"orch" 或 "sub-<id>"），决定 call id
    budget: TurnBudget
    sources: SourceStore
    flags: TurnFlags
    subtopic_id: str | None = None
    skills: SkillCatalog | None = None
    call_index: int = 0  # 本步骤内第几次工具调用（由 Agent 循环给出；ask_user 的 question_id 用它）


class Tool(Protocol):
    name: str
    description: str
    parameters: dict[str, Any]  # JSON Schema（object）
    counts_budget: bool

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult: ...


class ToolArgsError(ValueError):
    """工具参数不合法：注册表把它转为 ok=False 的结果回给模型，不计额度。"""


def is_fatal(exc: GatewayError) -> bool:
    """单个工具调用不能吞掉的错误：费用预算、访问撤销、调用指纹冲突与 Gateway 连接丢失。"""
    if isinstance(exc, BudgetExhausted | AccessRevoked | CallDivergence):
        return True
    return exc.status == 0 and not isinstance(exc, CallDeadlineExceeded)


def counted_call(
    ctx: ToolContext, call: Callable[[], GatewayResult], action: str
) -> GatewayResult | ToolResult:
    """发起一次计数调用（web_search / web_fetch 共用）。

    返回 GatewayResult 表示成功（已记入额度）；返回 ToolResult 表示未发起或失败：额度已用尽
    （调用前或 429 tool_budget_exhausted）→ control="budget_exhausted"；其余非致命的
    GatewayError（站点不可达、超时、4xx）→ 计 1 次、ok=False，由模型决定下一步。致命错误上抛。
    """
    ctx.flags.researching = True
    if ctx.budget.remaining() == 0:
        return _exhausted(ctx, action)
    try:
        res = call()
    except ToolBudgetExhausted:
        ctx.budget.exhaust()
        return _exhausted(ctx, action)
    except GatewayError as exc:
        if is_fatal(exc):
            raise
        ctx.budget.record_failure(ctx.subtopic_id)
        reason = f"{exc.code}（{exc.message}）" if exc.message else exc.code
        return failed(ctx, f"{action}失败：{reason}")
    ctx.budget.record(res.tool_budget, ctx.subtopic_id)
    return res


def with_budget_line(ctx: ToolContext, text: str) -> str:
    """计数工具的结果文本末尾附额度行（模型据此看到剩余额度）。"""
    return f"{text}\n{ctx.budget.line(ctx.subtopic_id)}"


def failed(ctx: ToolContext, message: str, raw: dict[str, Any] | None = None) -> ToolResult:
    """计数工具的失败结果：内容末尾附额度行。"""
    return ToolResult(
        content=with_budget_line(ctx, message),
        ok=False,
        preview={"kind": "text", "text": message},
        raw=raw,
    )


def _exhausted(ctx: ToolContext, action: str) -> ToolResult:
    message = f"工具额度已用尽，未发起本次{action}；请用已收集的材料写摘要或报告。"
    return ToolResult(
        content=with_budget_line(ctx, message),
        ok=False,
        preview={"kind": "text", "text": message},
        control="budget_exhausted",
    )
