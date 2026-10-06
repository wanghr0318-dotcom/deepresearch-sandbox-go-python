"""web_fetch：经 Gateway /v1/fetch 抓取网页（计入本轮工具额度），成功结果登记为编号来源 [n]。"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any

from agentbox_worker.tools.base import (
    ToolArgsError,
    ToolContext,
    ToolResult,
    counted_call,
    failed,
    with_budget_line,
)
from agentbox_worker.tools.sources import EXCERPT_MAX_BYTES, PREVIEW_MAX_CHARS
from agentbox_worker.tools.text import page_text, site_of, truncate_utf8

MODEL_TEXT_MAX_BYTES = 6 * 1024
URL_MAX_CHARS = 2048
_TITLE_SCAN_MAX_CHARS = 64 * 1024
_HTTP_URL = re.compile(r"https?://\S+", re.IGNORECASE)


class WebFetch:
    name = "web_fetch"
    description = (
        "抓取一个 http(s) 网页的正文，并把它登记为本轮来源 [n]（报告中以 [n] 引用）。"
        "每次调用消耗 1 次本轮工具额度；同一网页重复抓取不新增来源。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "minLength": 8,
                "maxLength": URL_MAX_CHARS,
                "description": "http:// 或 https:// 开头的网址",
            }
        },
        "required": ["url"],
        "additionalProperties": False,
    }
    counts_budget = True

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        url = args["url"].strip()
        if not _HTTP_URL.fullmatch(url) or not site_of(url):
            raise ToolArgsError(f"url 须为 http(s) 网址：{url[:200]!r}")
        out = counted_call(ctx, lambda: ctx.gateway.fetch(ctx.step_id, url), "抓取")
        if isinstance(out, ToolResult):
            return out
        raw = {"call_id": out.call_id}
        body = out.body
        status = body.get("status")
        if body.get("error") == "http_status":  # 目标站点返回 4xx/5xx：不作为来源
            return failed(ctx, f"抓取失败：站点返回 HTTP {status}", raw)
        if out.blob_sha256 is None:
            return failed(ctx, "抓取失败：Gateway 未返回结果 blob", raw)
        text = page_text(body) if body.get("encoding", "utf-8") == "utf-8" else ""
        src, _ = ctx.sources.add(
            sha256=out.blob_sha256,
            url=url,
            title=_title(body) or url,
            excerpt=truncate_utf8(text, EXCERPT_MAX_BYTES),
            call_id=out.call_id,
        )
        shown = truncate_utf8(text, MODEL_TEXT_MAX_BYTES) if text else "（页面没有可读的文本）"
        return ToolResult(
            content=with_budget_line(ctx, f"来源 [{src.n}]：{src.title}\n{src.url}\n{shown}"),
            preview={
                "kind": "fetch",
                "n": src.n,
                "title": src.title,
                "url": src.url,
                "site": site_of(src.url),
                "excerpt": src.excerpt[:PREVIEW_MAX_CHARS],
            },
            raw=raw,
            blobs=(out.blob_sha256,),
        )


def _title(body: dict[str, Any]) -> str:
    """页面标题：抓取结果的 title 字段，否则 HTML 的 <title>；空白折叠为单个空格。"""
    if isinstance(t := body.get("title"), str) and t.strip():
        return " ".join(t.split())
    content = body.get("content")
    if not isinstance(content, str) or "<" not in content:
        return ""
    parser = _TitleParser()
    parser.feed(content[:_TITLE_SCAN_MAX_CHARS])
    parser.close()
    return " ".join("".join(parser.parts).split())[:300]


class _TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._depth = 0
        self._done = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title" and not self._done:
            self._depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "title" and self._depth:
            self._depth -= 1
            self._done = True

    def handle_data(self, data: str) -> None:
        if self._depth and not self._done:
            self.parts.append(data)
