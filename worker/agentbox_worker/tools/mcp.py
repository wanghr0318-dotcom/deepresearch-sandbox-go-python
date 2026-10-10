"""MCP 工具（设计 docs/design/2026-10-10-shell-file-mcp-design.md §3.4）。

服务端配置的 MCP 服务器上允许的工具，经 Gateway 调用（沙箱不连接 MCP 服务器、
不持有其凭据）。每次调用计入本轮工具额度（与 web_search、web_fetch 相同，由
Gateway 强制）。

工具清单在每轮开始时取一次并存入 checkpoint 状态：恢复后发给模型的 tools 与
原来相同（调用指纹稳定）。
参数由 MCP 服务器校验（其 JSON Schema 不一定在本地校验器支持的子集内），本地只要求是对象。
"""

from __future__ import annotations

import json
from typing import Any, Protocol, cast

from agentbox_worker.errors import GatewayError, ToolBudgetExhausted
from agentbox_worker.gateway import GatewayResult
from agentbox_worker.tools.base import (
    ToolContext,
    ToolResult,
    _exhausted,
    failed,
    is_fatal,
    user_error,
    with_budget_line,
)

MAX_TEXT_CHARS = 16_000
PREVIEW_CHARS = 1200
DESCRIPTION_CHARS = 600
_MCP_ERRORS = {
    "mcp_tool_not_allowed": "该工具不在服务端允许的清单中",
    "mcp_server_not_found": "没有这个 MCP 服务器",
    "mcp_error": "MCP 服务器返回了错误",
}


class McpGateway(Protocol):
    def mcp_call(
        self, step_id: str, server: str, tool: str, arguments: dict[str, Any]
    ) -> GatewayResult: ...


def valid_descriptor(d: Any) -> bool:
    return (
        isinstance(d, dict)
        and all(isinstance(d.get(k), str) and d.get(k) for k in ("name", "server", "tool"))
        and str(d["name"]).startswith("mcp__")
        and len(str(d["name"])) <= 64
    )


class McpTool:
    """一个 MCP 工具；name 为 mcp__<server>__<tool>。"""

    counts_budget = True
    validate = False  # 参数交给 MCP 服务器校验（ToolRegistry.dispatch 只要求是 JSON 对象）

    def __init__(self, descriptor: dict[str, Any]) -> None:
        self.name = str(descriptor["name"])
        self.server = str(descriptor["server"])
        self.tool = str(descriptor["tool"])
        desc = str(descriptor.get("description") or f"MCP 工具 {self.server}/{self.tool}")
        self.description = (
            f"[外部工具 {self.server}，经 Gateway 调用，消耗 1 次本轮工具额度] "
            + desc[:DESCRIPTION_CHARS]
        )
        schema = descriptor.get("input_schema")
        self.parameters: dict[str, Any] = (
            schema
            if isinstance(schema, dict) and schema.get("type") == "object"
            else {"type": "object"}
        )

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        if ctx.budget.remaining() == 0:
            return _exhausted(ctx, "外部工具调用")
        gw = cast(McpGateway, ctx.gateway)
        try:
            res = gw.mcp_call(ctx.step_id, self.server, self.tool, args)
        except ToolBudgetExhausted:
            ctx.budget.exhaust()
            return _exhausted(ctx, "外部工具调用")
        except GatewayError as exc:
            if is_fatal(exc):
                raise
            ctx.budget.record_failure(ctx.subtopic_id, exc.tool_budget)
            detail = _MCP_ERRORS.get(exc.code) or user_error(exc.code)
            return failed(
                ctx, f"外部工具 {self.server}/{self.tool} 调用失败：{detail}（{exc.code}）"
            )
        ctx.budget.record(res.tool_budget, ctx.subtopic_id)
        return _result(ctx, self, res)


def _result(ctx: ToolContext, tool: McpTool, res: GatewayResult) -> ToolResult:
    body = res.body if isinstance(res.body, dict) else {}
    texts = [
        str(c.get("text", ""))
        for c in body.get("content") or []
        if isinstance(c, dict) and c.get("type") == "text"
    ]
    others = [
        str(c.get("type"))
        for c in body.get("content") or []
        if isinstance(c, dict) and c.get("type") != "text"
    ]
    text = "\n".join(t for t in texts if t)
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS] + f"\n…（共 {len(text)} 字符，已截断）"
    structured = body.get("structured_content")
    lines = [
        f"{tool.server}/{tool.tool}" + ("（工具报告错误）" if body.get("is_error") else "") + "："
    ]
    lines.append(text or "（无文本结果）")
    if structured is not None and not text:
        lines.append(json.dumps(structured, ensure_ascii=False)[:MAX_TEXT_CHARS])
    if others:
        lines.append(f"（另有非文本内容 {', '.join(others)}，未转交）")
    ok = not body.get("is_error")
    preview = (
        text or json.dumps(structured, ensure_ascii=False) if structured is not None else text
    )[:PREVIEW_CHARS]
    return ToolResult(
        content=with_budget_line(ctx, "\n".join(lines)),
        ok=ok,
        preview={"kind": "text", "text": preview or "（无文本结果）"},
        raw={"call_id": res.call_id},
        blobs=(res.blob_sha256,) if res.blob_sha256 else (),
        data=body,
    )


def mcp_tools(descriptors: list[dict[str, Any]]) -> list[McpTool]:
    """由（checkpoint 中保存的）工具描述建立工具；不合法或重名的描述跳过。"""
    out: list[McpTool] = []
    seen: set[str] = set()
    for d in descriptors:
        if valid_descriptor(d) and d["name"] not in seen:
            seen.add(d["name"])
            out.append(McpTool(d))
    return out
