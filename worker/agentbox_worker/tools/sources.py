"""本轮来源表与 read_source 工具。

每次成功抓取（有结果 blob）成为一个编号来源 [n]；同一 blob 或同一 URL 只登记一次。来源保存
摘录（≤ 4 KiB）以便 blob 不可读时退回；read_source 只读 blob，不发起计数调用。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any

from agentbox_worker.errors import GatewayError
from agentbox_worker.tools.base import ToolArgsError, ToolContext, ToolResult
from agentbox_worker.tools.text import page_text, truncate_utf8

EXCERPT_MAX_BYTES = 4096
READ_MAX_BYTES = 12 * 1024
PREVIEW_MAX_CHARS = 600
INLINE_RAW_MAX_BYTES = 16 * 1024


@dataclass
class Source:
    n: int  # 本轮编号（报告 [n]）
    sha256: str  # fetch 结果 blob
    url: str
    title: str
    excerpt: str  # 页面正文摘录 ≤ 4 KiB
    call_id: str
    origin: str = ""  # "" = 本轮；"<task_id>" = 来自之前的轮次或 carryover


class SourceStore:
    def __init__(self) -> None:
        self._items: list[Source] = []

    def add(
        self, *, sha256: str, url: str, title: str, excerpt: str, call_id: str
    ) -> tuple[Source, bool]:
        """登记一个来源；同 sha 或同 url 已存在时返回 (已有来源, False)。"""
        if (old := self._find(sha256, url)) is not None:
            return old, False
        src = Source(
            n=self._next_n(),
            sha256=sha256,
            url=url,
            title=title,
            excerpt=truncate_utf8(excerpt, EXCERPT_MAX_BYTES),
            call_id=call_id,
        )
        self._items.append(src)
        return src, True

    def get(self, n: int) -> Source | None:
        return next((s for s in self._items if s.n == n), None)

    def adopt(self, sources: Iterable[Source]) -> None:
        """引入既有来源（之前的轮次或 carryover）：跳过重复（同 sha 或同 url），其余按给出顺序
        续编号（空表时保持 1, 2, … 的原编号），origin 保持不变。"""
        for s in sources:
            if self._find(s.sha256, s.url) is None:
                self._items.append(
                    Source(self._next_n(), s.sha256, s.url, s.title, s.excerpt, s.call_id, s.origin)
                )

    def all(self) -> list[Source]:
        return list(self._items)

    def to_json(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self._items]

    @classmethod
    def from_json(cls, items: list[dict[str, Any]]) -> SourceStore:
        if not isinstance(items, list):
            raise ValueError("来源表状态不是数组")
        store = cls()
        for d in items:
            src = _source(d)
            if store.get(src.n) is not None or store._find(src.sha256, src.url) is not None:
                raise ValueError(f"来源表状态重复：[{src.n}] {src.url}")
            store._items.append(src)
        return store

    def _find(self, sha256: str, url: str) -> Source | None:
        return next((s for s in self._items if s.sha256 == sha256 or s.url == url), None)

    def _next_n(self) -> int:
        return max((s.n for s in self._items), default=0) + 1


def _source(d: Any) -> Source:
    if not isinstance(d, dict):
        raise ValueError("来源状态项不是对象")
    n = d.get("n")
    if not isinstance(n, int) or isinstance(n, bool) or n < 1:
        raise ValueError(f"来源编号不合法：{n!r}")
    fields = ("sha256", "url", "title", "excerpt", "call_id", "origin")
    values = {k: d.get(k, "") for k in fields}
    if not all(isinstance(v, str) for v in values.values()) or not values["sha256"]:
        raise ValueError(f"来源 [{n}] 的字段不合法")
    return Source(n=n, **values)


class ReadSource:
    """重读已收集的来源全文（≤ 12 KiB）；blob 读取失败时退回摘录。不计额度。"""

    name = "read_source"
    description = (
        "重读本轮已收集的来源 [n] 的正文（最多约 12 KiB），用于核对细节；"
        "不访问网络，不消耗工具额度。"
    )
    parameters: dict[str, Any] = {
        "type": "object",
        "properties": {"n": {"type": "integer", "minimum": 1, "description": "来源编号"}},
        "required": ["n"],
        "additionalProperties": False,
    }
    counts_budget = False

    def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        src = ctx.sources.get(args["n"])
        if src is None:
            known = ", ".join(str(s.n) for s in ctx.sources.all()) or "无"
            raise ToolArgsError(f"没有来源 [{args['n']}]（已有：{known}）")
        body, note = self._body(ctx, src)
        content = f"来源 [{src.n}]：{src.title}\n{src.url}\n{note}{body}"
        return ToolResult(
            content=content,
            preview={"kind": "text", "text": body[:PREVIEW_MAX_CHARS]},
            raw={
                "inline": {
                    "request": dict(args),
                    "response": truncate_utf8(content, INLINE_RAW_MAX_BYTES),
                }
            },
        )

    @staticmethod
    def _body(ctx: ToolContext, src: Source) -> tuple[str, str]:
        try:
            parsed = json.loads(ctx.gateway.read_blob(src.sha256))
            text = page_text(parsed) if isinstance(parsed, dict) else ""
        except (GatewayError, ValueError, KeyError):
            text = ""
        if text:
            return truncate_utf8(text, READ_MAX_BYTES), ""
        return src.excerpt, "（正文不可读，以下为保存的摘录）\n"
