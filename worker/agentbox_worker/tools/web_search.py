"""web_search：经 Gateway /v1/search 搜索网页（计入本轮工具额度）。"""

from __future__ import annotations

from typing import Any

from agentbox_worker.tools.base import (
    ToolArgsError,
    ToolContext,
    ToolResult,
    counted_call,
    with_budget_line,
)
from agentbox_worker.tools.text import search_hits, site_of

DEFAULT_MAX_RESULTS = 5
PREVIEW_SNIPPET_MAX_CHARS = 300


class WebSearch:
    name = "web_search"
    description = (
        "搜索网页，返回标题、站点、摘要与 URL。每次调用消耗 1 次本轮工具额度；"
        "需要正文时再用 web_fetch 抓取具体页面。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1, "maxLength": 200, "description": "搜索词"},
            "max_results": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
                "description": f"返回条数，缺省 {DEFAULT_MAX_RESULTS}",
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    }
    counts_budget = True

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        query = args["query"].strip()
        if not query:
            raise ToolArgsError("query 不能为空白")
        max_results = args.get("max_results", DEFAULT_MAX_RESULTS)
        out = counted_call(
            ctx, lambda: ctx.gateway.search(ctx.step_id, query, max_results=max_results), "搜索"
        )
        if isinstance(out, ToolResult):
            return out
        results = [{**h, "site": site_of(h["url"])} for h in search_hits(out.body)[:max_results]]
        lines = [
            f"{k}. {r['title']} — {r['site']}\n{r['snippet']}\n{r['url']}"
            for k, r in enumerate(results, start=1)
        ]
        text = "\n\n".join(lines) if lines else "没有搜索结果。"
        preview = {
            "kind": "search",
            "query": query,
            "results": [
                {
                    "title": r["title"],
                    "site": r["site"],
                    "snippet": r["snippet"][:PREVIEW_SNIPPET_MAX_CHARS],
                    "url": r["url"],
                }
                for r in results
            ],
        }
        return ToolResult(
            content=with_budget_line(ctx, text),
            preview=preview,
            raw={"call_id": out.call_id},
            blobs=(out.blob_sha256,) if out.blob_sha256 else (),
        )
