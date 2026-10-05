"""研究步骤：计划 → 检索与抓取 → 总结 → 报告。纯函数 + Gateway 调用，不含控制流。

全部外部访问经 GatewayClient（Unix socket）；本模块不打开网络连接、文件、数据库或子进程。
GatewayError 原样上抛，由研究循环（Task 3）决定重试、取代或降级。

提示词只由参数与研究状态确定性地生成（不含时间、随机数），恢复后重发同一调用时请求体不变。
chat 步骤可选接收 model：None 时请求不带 model（Gateway 用服务端默认模型），否则原样写入请求体
（模型名进入调用指纹）。

每个步骤可选接收 calls 列表：本步骤发起的每个 Gateway 调用结果按发起顺序追加其中，供调用方
把结果 blob 放入 checkpoint 的 refs（summarize/write_report 的返回值只有文本）。
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any

from agentbox_worker import GatewayClient, GatewayError, GatewayResult
from deepresearch import prompts
from deepresearch.models import Evidence, ResearchState, ResearchTask

PLAN_STEP_ID = "plan"
EVIDENCE_MAX_BYTES = 4096  # 每条证据进入提示词（及状态）的上限，UTF-8 字节
PLAN_MAX_TOKENS = 1024
SUMMARY_MAX_TOKENS = 1536
REPORT_MAX_TOKENS = 4096
NO_INFO = "暂无可用信息"

# 移植 create_fallback_task；intent 兼作回退标记，研究循环据此把计划失败记入 failures
FALLBACK_TITLE = "基础背景梳理"
FALLBACK_INTENT = "收集主题的核心背景与最新动态"

_HTML_PARSE_MAX_CHARS = 512 * 1024
_THINK = re.compile(r"<think>.*?</think>|【思考】.*?【/思考】", re.DOTALL | re.IGNORECASE)
_FENCE = re.compile(r"```(?:json|JSON)?\s*\n?(.*?)```", re.DOTALL)
_CITE = re.compile(r"\[(\d+(?:\s*[,，、]\s*\d+)*)\]")
_EVIDENCE_HEADING = re.compile(r"^#{1,6}\s*(?:证据|参考来源)(?:列表)?\s*$", re.MULTILINE)


# ---- 计划 ----


def plan(
    gw: GatewayClient,
    topic: str,
    *,
    max_tasks: int,
    model: str | None = None,
    calls: list[GatewayResult] | None = None,
) -> tuple[list[ResearchTask], GatewayResult]:
    """请模型把主题拆成 ≤ max_tasks 个任务；输出无法解析或为空时回退为单任务。"""
    if max_tasks < 1:
        raise ValueError("max_tasks 必须 ≥ 1")
    messages = [
        {"role": "system", "content": prompts.PLAN_PROMPT.format(max_tasks=max_tasks)},
        {"role": "user", "content": prompts.PLAN_USER.format(topic=topic)},
    ]
    res = gw.chat(PLAN_STEP_ID, messages, model=model, max_tokens=PLAN_MAX_TOKENS)
    _record(calls, res)
    tasks = parse_tasks(_chat_text(res))[:max_tasks]
    return (tasks or [fallback_task(topic)]), res


def parse_tasks(text: str) -> list[ResearchTask]:
    """从模型输出中容错提取任务列表（移植 _extract_json_payload）。

    依次尝试：整段 JSON、```json 代码块、正文中第一个可解析的 JSON 对象或数组。
    接受 {"tasks": [...]} 或直接的任务数组；无法提取时返回空列表（由 plan 回退）。
    """
    for payload in _json_candidates(strip_thinking(text)):
        items = payload.get("tasks") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            continue
        dicts = [item for item in items if isinstance(item, dict)]
        if dicts:
            return [_to_task(i, item) for i, item in enumerate(dicts, start=1)]
    return []


def fallback_task(topic: str) -> ResearchTask:
    """计划失败时的最小任务（移植 create_fallback_task）。"""
    return ResearchTask(
        id=1,
        title=FALLBACK_TITLE,
        intent=FALLBACK_INTENT,
        query=f"{topic} 最新进展" if topic else FALLBACK_TITLE,
    )


def is_fallback(tasks: list[ResearchTask]) -> bool:
    return (
        len(tasks) == 1 and tasks[0].title == FALLBACK_TITLE and tasks[0].intent == FALLBACK_INTENT
    )


def _json_candidates(text: str):
    text = text.strip()
    try:
        yield json.loads(text)
    except (ValueError, RecursionError):
        pass
    for block in _FENCE.findall(text):
        try:
            yield json.loads(block.strip())
        except (ValueError, RecursionError):
            pass
    decoder = json.JSONDecoder()
    for m in re.finditer(r"[\[{]", text):
        try:
            value, _ = decoder.raw_decode(text, m.start())
        except (ValueError, RecursionError):
            continue
        if isinstance(value, dict | list):
            yield value


def _to_task(idx: int, item: dict) -> ResearchTask:
    title = _text(item.get("title")) or f"任务{idx}"
    intent = _text(item.get("intent")) or "聚焦主题的关键问题"
    query = _text(item.get("query")) or title
    return ResearchTask(id=idx, title=title, intent=intent, query=query)


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


# ---- 检索与抓取 ----


def search_and_fetch(
    gw: GatewayClient,
    task: ResearchTask,
    *,
    max_results: int,
    max_fetch: int,
    step_id: str,
    calls: list[GatewayResult] | None = None,
) -> list[Evidence]:
    """搜索任务查询，抓取前 max_fetch 个不同的 http(s) 结果；每次成功抓取得到一条证据。

    证据的 sha256 是 fetch 结果 blob（X-Agentbox-Blob），URL/标题/片段来自搜索结果，片段后接
    抓取正文的纯文本摘录，合计截断到 EVIDENCE_MAX_BYTES。没有 blob 的抓取结果不成为证据。
    """
    res = gw.search(step_id, task.query, max_results=max_results)
    _record(calls, res)
    evidence: list[Evidence] = []
    seen_urls: set[str] = set()
    seen_shas: set[str] = set()
    for hit in _search_hits(res.body)[:max_results]:
        if len(seen_urls) >= max_fetch:
            break
        if hit["url"] in seen_urls:
            continue
        seen_urls.add(hit["url"])
        fetched = gw.fetch(step_id, hit["url"])
        _record(calls, fetched)
        sha = fetched.blob_sha256
        if sha is None or sha in seen_shas:
            continue
        seen_shas.add(sha)
        parts = [p for p in (hit["snippet"], _page_text(fetched.body)) if p]
        evidence.append(
            Evidence(
                sha256=sha,
                url=hit["url"],
                title=hit["title"],
                snippet=truncate_utf8("\n\n".join(parts), EVIDENCE_MAX_BYTES),
                call_id=fetched.call_id,
            )
        )
    return evidence


def _search_hits(body: dict) -> list[dict[str, str]]:
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
        hits.append({"url": url, "title": _text(item.get("title")) or url, "snippet": snippet})
    return hits


def _page_text(body: dict) -> str:
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


# ---- 总结 ----


def summarize(
    gw: GatewayClient,
    task: ResearchTask,
    evidence: list[Evidence],
    *,
    step_id: str,
    model: str | None = None,
    calls: list[GatewayResult] | None = None,
) -> str:
    """基于编号证据为任务写摘要；摘要中的 [n] 指 evidence[n-1]。

    调用方应令 task.evidence == [e.sha256 for e in evidence]（同序），write_report 据此把
    各任务的局部编号映射为全局编号。没有证据时不调用模型，直接返回"暂无可用信息"。
    """
    if not evidence:
        return NO_INFO
    user = prompts.SUMMARIZE_USER.format(
        title=task.title,
        intent=task.intent,
        query=task.query,
        evidence=_evidence_block(evidence),
    )
    messages = [
        {"role": "system", "content": prompts.SUMMARIZE_PROMPT},
        {"role": "user", "content": user},
    ]
    res = gw.chat(step_id, messages, model=model, max_tokens=SUMMARY_MAX_TOKENS)
    _record(calls, res)
    return strip_thinking(_chat_text(res)).strip() or NO_INFO


def _evidence_block(evidence: list[Evidence]) -> str:
    items = []
    for n, e in enumerate(evidence, start=1):
        snippet = truncate_utf8(e.snippet, EVIDENCE_MAX_BYTES)
        items.append(f"[{n}] 标题：{e.title}\n来源：{e.url}\n片段：{snippet}")
    return "\n\n".join(items)


# ---- 报告 ----


def write_report(
    gw: GatewayClient,
    state: ResearchState,
    *,
    step_id: str,
    model: str | None = None,
    calls: list[GatewayResult] | None = None,
) -> str:
    """综合各任务摘要写 Markdown 报告，末尾附"证据"列表：[n] → 证据 blob sha256 与来源 URL。

    证据按任务顺序、任务内证据顺序全局编号；各任务摘要的局部 [n] 先映射为全局编号再交给模型。
    模型输出中不在证据列表里的编号被删除，模型自行写出的"证据/参考来源"节被替换为系统生成的列表。
    """
    numbers = _global_numbers(state)
    ordered = sorted(numbers, key=numbers.__getitem__)
    task_blocks = []
    for task in state.tasks:
        local = {i: numbers[sha] for i, sha in enumerate(task.evidence, start=1) if sha in numbers}
        summary = _renumber(task.summary or NO_INFO, local)
        task_blocks.append(
            prompts.REPORT_TASK.format(
                id=task.id,
                title=task.title,
                intent=task.intent,
                query=task.query,
                status=task.status,
                summary=summary,
            )
        )
    catalog = "\n".join(
        f"[{numbers[sha]}] {state.evidence[sha].title} — {state.evidence[sha].url}"
        for sha in ordered
    )
    user = prompts.REPORT_USER.format(
        topic=state.topic, tasks="\n".join(task_blocks), evidence=catalog or "（无）"
    )
    messages = [
        {"role": "system", "content": prompts.REPORT_PROMPT},
        {"role": "user", "content": user},
    ]
    res = gw.chat(step_id, messages, model=model, max_tokens=REPORT_MAX_TOKENS)
    _record(calls, res)
    body = strip_thinking(_chat_text(res))
    heading = _EVIDENCE_HEADING.search(body)
    if heading:
        body = body[: heading.start()]
    body = (
        _renumber(body, {n: n for n in numbers.values()}).strip()
        or "报告生成失败：模型未返回内容。"
    )
    lines = [f"# {state.topic}" if state.topic else "# 研究报告", "", body, "", "## 证据", ""]
    if ordered:
        for sha in ordered:
            ev = state.evidence[sha]
            lines.append(f"- [{numbers[sha]}] {ev.title} — {ev.url} — sha256:{sha}")
    else:
        lines.append("暂无证据。")
    return "\n".join(lines) + "\n"


def _global_numbers(state: ResearchState) -> dict[str, int]:
    """sha256 → 全局编号（从 1 起）：先按任务与任务内顺序，再补未被任务引用的证据。"""
    numbers: dict[str, int] = {}
    order = [sha for task in state.tasks for sha in task.evidence] + list(state.evidence)
    for sha in order:
        if sha in state.evidence and sha not in numbers:
            numbers[sha] = len(numbers) + 1
    return numbers


def _renumber(text: str, mapping: dict[int, int]) -> str:
    """把 [n]、[n, m] 形式的引用按 mapping 改写为 [a][b]；不在 mapping 中的编号删除。"""

    def repl(m: re.Match[str]) -> str:
        nums = [int(x) for x in re.split(r"\s*[,，、]\s*", m.group(1))]
        return "".join(f"[{mapping[n]}]" for n in nums if n in mapping)

    return _CITE.sub(repl, text)


# ---- 公共工具 ----


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


def _chat_text(res: GatewayResult) -> str:
    """OpenAI 兼容响应的 choices[0].message.content。"""
    try:
        content = res.body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise GatewayError(
            res.status, "invalid_chat_response", f"缺少 choices[0].message.content：{exc!r}"
        ) from exc
    if not isinstance(content, str):
        raise GatewayError(res.status, "invalid_chat_response", "message.content 不是字符串")
    return content


def _record(calls: list[GatewayResult] | None, res: GatewayResult) -> None:
    if calls is not None:
        calls.append(res)
