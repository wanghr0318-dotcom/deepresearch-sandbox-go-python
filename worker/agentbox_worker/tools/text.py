"""文本辅助：去掉思考段、按 UTF-8 字节截断、解析搜索与抓取结果、改写引用编号、取站点名。

从 deepresearch.steps 原样移来（行为不变），供 deepresearch 与 Agent 工具共用。只用标准库，
不导入 urllib（工具包的 import 契约）。
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any

_HTML_PARSE_MAX_CHARS = 512 * 1024
_THINK = re.compile(r"<think>.*?</think>|【思考】.*?【/思考】", re.DOTALL | re.IGNORECASE)
_CITE = re.compile(r"\[(\d+(?:\s*[,，、]\s*\d+)*)\]")
_SCHEME = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://")


def strip_thinking(text: str) -> str:
    """去掉推理模型输出中的 <think>…</think> / 【思考】…【/思考】 段。"""
    return _THINK.sub("", text)


def truncate_utf8(text: str, limit: int) -> str:
    """截断到 UTF-8 编码 ≤ limit 字节（不切开字符）；被截断时以 … 结尾且总长仍 ≤ limit。"""
    data = text.encode("utf-8")
    if len(data) <= limit:
        return text
    ellipsis = "…"
    cut = data[: limit - len(ellipsis.encode("utf-8"))]
    return cut.decode("utf-8", "ignore") + ellipsis


def search_hits(body: dict) -> list[dict[str, str]]:
    """搜索响应 {"results": [{url, title, snippet|content|description}]}；跳过非 http(s) 条目。"""
    results = body.get("results")
    if not isinstance(results, list):
        return []
    hits = []
    for item in results:
        if not isinstance(item, dict):
            continue
        url = _text(item.get("url"))
        if not url.lower().startswith(("http://", "https://")):
            continue
        snippet = next(
            (s for k in ("snippet", "content", "description") if (s := _text(item.get(k)))), ""
        )
        # 网页 <title> 原文可能含制表符与换行：折叠为单个空格，证据列表才能一行一条。
        title = " ".join(_text(item.get("title")).split())
        hits.append({"url": url, "title": title or url, "snippet": snippet})
    return hits


def page_text(body: dict) -> str:
    """抓取响应中的正文（text|content|body），HTML 转为纯文本并压缩空白。"""
    raw = next((v for k in ("text", "content", "body") if isinstance(v := body.get(k), str)), "")
    content_type = body.get("content_type")
    is_html = (isinstance(content_type, str) and "html" in content_type.lower()) or raw.lstrip()[
        :1
    ] == "<"
    if is_html:
        parser = _TextExtractor()
        parser.feed(raw[:_HTML_PARSE_MAX_CHARS])
        parser.close()
        raw = " ".join(parser.chunks)
    return " ".join(raw.split())


class _TextExtractor(HTMLParser):
    _SKIP = frozenset({"script", "style", "noscript", "template", "svg", "head"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._SKIP:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth and data.strip():
            self.chunks.append(data)


def renumber_citations(text: str, mapping: dict[int, int]) -> str:
    """把 [n]、[n, m] 形式的引用按 mapping 改写为 [a][b]；不在 mapping 中的编号删除。"""

    def repl(m: re.Match[str]) -> str:
        nums = [int(x) for x in re.split(r"\s*[,，、]\s*", m.group(1))]
        return "".join(f"[{mapping[n]}]" for n in nums if n in mapping)

    return _CITE.sub(repl, text)


def site_of(url: str) -> str:
    """URL 的主机名（小写，去掉端口、用户信息与开头的 "www."）；不是绝对 URL 时为空串。"""
    m = _SCHEME.match(url.strip())
    if m is None:
        return ""
    rest = url.strip()[m.end() :]
    host = re.split(r"[/?#]", rest, maxsplit=1)[0].rpartition("@")[2]
    if host.startswith("["):  # IPv6 字面量
        host = host[: host.find("]") + 1] if "]" in host else host
    else:
        host = host.partition(":")[0]
    host = host.lower().rstrip(".")
    return host.removeprefix("www.")


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""
