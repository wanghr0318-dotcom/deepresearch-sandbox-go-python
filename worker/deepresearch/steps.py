"""研究步骤：计划 → 检索与抓取 → 总结 → 报告。纯函数 + Gateway 调用，不含控制流。

全部外部访问经 GatewayClient（Unix socket）；本模块不打开网络连接、文件、数据库或子进程。
GatewayError 原样上抛，由研究循环（Task 3）决定重试、取代或降级；例外：单个抓取失败不致命
（search_and_fetch 记录并跳过该来源，见其文档）。

chat 回复去掉思考段后为空（推理模型把 max_tokens 全部用于隐藏推理时常见）一律以 EmptyCompletion
上抛，消息含 finish_reason，从不当作"暂无可用信息"；summarize 先以加倍的 max_tokens 重试一次。
max_tokens 由调用方传入（研究循环取自任务 config），缺省为下面的常量。

提示词只由参数与研究状态确定性地生成（不含时间、随机数），恢复后重发同一调用时请求体不变。
chat 步骤可选接收 model：None 时请求不带 model（Gateway 用服务端默认模型），否则原样写入请求体
（模型名进入调用指纹）。

每个步骤可选接收 calls 列表：本步骤发起的每个 Gateway 调用结果按发起顺序追加其中，供调用方
把结果 blob 放入 checkpoint 的 refs（summarize/write_report 的返回值只有文本）。
"""

from __future__ import annotations

import json
import re
from typing import Any

from agentbox_worker import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    GatewayClient,
    GatewayError,
    GatewayResult,
)
from agentbox_worker.tools.text import (
    page_text,
    renumber_citations,
    search_hits,
    strip_thinking,
    truncate_utf8,
)
from deepresearch import prompts
from deepresearch.models import Evidence, ResearchState, ResearchTask

PLAN_STEP_ID = "plan"
EVIDENCE_MAX_BYTES = 4096  # 每条证据进入提示词（及状态）的上限，UTF-8 字节
# 推理模型（如 kimi）先在隐藏推理上消耗输出 token（usage 中的 reasoning_tokens），之后才写正文；
# 上限过小时正文为空（finish_reason=length）。以下为缺省值，可由任务 config 覆盖。
PLAN_MAX_TOKENS = 4096
SUMMARY_MAX_TOKENS = 8192
REPORT_MAX_TOKENS = 16384
NO_INFO = "暂无可用信息"  # 只表示"没有证据"，从不用来掩盖模型的空回复

# 移植 create_fallback_task；intent 兼作回退标记，研究循环据此把计划失败记入 failures
FALLBACK_TITLE = "基础背景梳理"
FALLBACK_INTENT = "收集主题的核心背景与最新动态"

_FENCE = re.compile(r"```(?:json|JSON)?\s*\n?(.*?)```", re.DOTALL)
_EVIDENCE_HEADING = re.compile(r"^#{1,6}\s*(?:证据|参考来源)(?:列表)?\s*$", re.MULTILINE)


class EmptyCompletion(GatewayError):
    """chat 调用成功（2xx）但去掉思考段后正文为空：步骤失败，不可在任务内以同一请求重做。

    status 为该调用的 HTTP 状态（2xx），故研究循环不把它当作瞬时错误重做。
    """

    def __init__(self, res: GatewayResult, max_tokens: int) -> None:
        self.finish_reason = _finish_reason(res)
        self.call_id = res.call_id
        hint = "：推理耗尽 max_tokens" if self.finish_reason == "length" else ""
        reasoning = _reasoning_tokens(res)
        extra = f"，reasoning_tokens={reasoning}" if reasoning is not None else ""
        super().__init__(
            res.status,
            "empty_completion",
            f"{res.call_id} 模型未返回正文（finish_reason={self.finish_reason}{hint}，"
            f"max_tokens={max_tokens}{extra}）",
        )


# ---- 计划 ----


def plan(
    gw: GatewayClient,
    topic: str,
    *,
    max_tasks: int,
    model: str | None = None,
    max_tokens: int = PLAN_MAX_TOKENS,
    calls: list[GatewayResult] | None = None,
) -> tuple[list[ResearchTask], GatewayResult]:
    """请模型把主题拆成 ≤ max_tasks 个任务；输出无法解析为任务时回退为单任务。

    模型回复为空（EmptyCompletion）不回退，原样上抛：计划失败须可见。
    """
    if max_tasks < 1:
        raise ValueError("max_tasks 必须 ≥ 1")
    messages = [
        {"role": "system", "content": prompts.PLAN_PROMPT.format(max_tasks=max_tasks)},
        {"role": "user", "content": prompts.PLAN_USER.format(topic=topic)},
    ]
    text, res = _complete(gw, PLAN_STEP_ID, messages, model, max_tokens, calls)
    tasks = parse_tasks(text)[:max_tasks]
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
    failures: list[str] | None = None,
) -> list[Evidence]:
    """搜索任务查询，抓取前 max_fetch 个不同的 http(s) 结果；每次成功抓取得到一条证据。

    证据的 sha256 是 fetch 结果 blob（X-Agentbox-Blob），URL/标题/片段来自搜索结果，片段后接
    抓取正文的纯文本摘录，合计截断到 EVIDENCE_MAX_BYTES。没有 blob 的抓取结果不成为证据。

    搜索失败原样上抛。单个抓取失败（目标站点不可达、超时等）不致命：记入 failures 并跳过该来源；
    但若最终没有任何证据，则上抛最后一个抓取错误（由研究循环决定重做或判任务失败）。致命错误
    （预算耗尽、访问撤销、取代后仍指纹冲突、与 Gateway 的连接中断）立即上抛。
    """
    res = gw.search(step_id, task.query, max_results=max_results)
    _record(calls, res)
    evidence: list[Evidence] = []
    seen_urls: set[str] = set()
    seen_shas: set[str] = set()
    last_error: GatewayError | None = None
    for hit in search_hits(res.body)[:max_results]:
        if len(seen_urls) >= max_fetch:
            break
        if hit["url"] in seen_urls:
            continue
        seen_urls.add(hit["url"])
        try:
            fetched = gw.fetch(step_id, hit["url"])
        except GatewayError as exc:
            if _fatal(exc):
                raise
            last_error = exc
            _note(failures, f"{step_id}: 抓取 {hit['url']} 失败（{exc}），跳过该来源")
            continue
        _record(calls, fetched)
        sha = fetched.blob_sha256
        if sha is None or sha in seen_shas:
            continue
        seen_shas.add(sha)
        parts = [p for p in (hit["snippet"], page_text(fetched.body)) if p]
        evidence.append(
            Evidence(
                sha256=sha,
                url=hit["url"],
                title=hit["title"],
                snippet=truncate_utf8("\n\n".join(parts), EVIDENCE_MAX_BYTES),
                call_id=fetched.call_id,
            )
        )
    if not evidence and last_error is not None:
        raise last_error
    return evidence


def _fatal(exc: GatewayError) -> bool:
    """单个抓取不能吞掉的错误：影响整个任务的预算与访问、调用指纹，或与 Gateway 本身的连接。"""
    if isinstance(exc, BudgetExhausted | AccessRevoked | CallDivergence):
        return True
    return exc.status == 0 and not isinstance(exc, CallDeadlineExceeded)


# ---- 总结 ----


def summarize(
    gw: GatewayClient,
    task: ResearchTask,
    evidence: list[Evidence],
    *,
    step_id: str,
    model: str | None = None,
    max_tokens: int = SUMMARY_MAX_TOKENS,
    calls: list[GatewayResult] | None = None,
    failures: list[str] | None = None,
) -> str:
    """基于编号证据为任务写摘要；摘要中的 [n] 指 evidence[n-1]。

    调用方应令 task.evidence == [e.sha256 for e in evidence]（同序），write_report 据此把
    各任务的局部编号映射为全局编号。没有证据时不调用模型，直接返回"暂无可用信息"。

    模型回复为空时记入 failures，并以 2 × max_tokens 重试一次（同一提示词；SDK 计数器分配新
    call id，故不与空回复的调用冲突，恢复后按同样顺序确定性重放）；仍为空则上抛 EmptyCompletion。
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
    try:
        return _complete(gw, step_id, messages, model, max_tokens, calls)[0]
    except EmptyCompletion as exc:
        _note(failures, f"{step_id}: {exc}，以 max_tokens={2 * max_tokens} 重试一次")
    return _complete(gw, step_id, messages, model, 2 * max_tokens, calls)[0]


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
    max_tokens: int = REPORT_MAX_TOKENS,
    calls: list[GatewayResult] | None = None,
) -> str:
    """综合各任务摘要写 Markdown 报告，末尾附"证据"列表：[n] → 证据 blob sha256 与来源 URL。

    证据按任务顺序、任务内证据顺序全局编号；各任务摘要的局部 [n] 先映射为全局编号再交给模型。
    模型输出中不在证据列表里的编号被删除，模型自行写出的"证据/参考来源"节被替换为系统生成的列表。
    模型回复为空时上抛 EmptyCompletion（研究循环据此写降级报告）。
    """
    numbers = _global_numbers(state)
    ordered = sorted(numbers, key=numbers.__getitem__)
    task_blocks = []
    for task in state.tasks:
        local = {i: numbers[sha] for i, sha in enumerate(task.evidence, start=1) if sha in numbers}
        summary = renumber_citations(task.summary or NO_INFO, local)
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
    body = _complete(gw, step_id, messages, model, max_tokens, calls)[0]
    heading = _EVIDENCE_HEADING.search(body)
    if heading:
        body = body[: heading.start()]
    body = (
        renumber_citations(body, {n: n for n in numbers.values()}).strip()
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


# ---- 公共工具 ----

# 文本辅助已移至 agentbox_worker.tools.text；_renumber 是 app.py 使用的原名。
_renumber = renumber_citations


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


def _complete(
    gw: GatewayClient,
    step_id: str,
    messages: list[dict[str, str]],
    model: str | None,
    max_tokens: int,
    calls: list[GatewayResult] | None,
) -> tuple[str, GatewayResult]:
    """发起一次 chat：返回去掉思考段并去掉首尾空白的正文；为空时上抛 EmptyCompletion。

    结果先记入 calls（空回复的 blob 也是本任务得到的结果）。
    """
    res = gw.chat(step_id, messages, model=model, max_tokens=max_tokens)
    _record(calls, res)
    text = strip_thinking(_chat_text(res)).strip()
    if not text:
        raise EmptyCompletion(res, max_tokens)
    return text, res


def _finish_reason(res: GatewayResult) -> str:
    try:
        reason = res.body["choices"][0].get("finish_reason")
    except (KeyError, IndexError, TypeError, AttributeError):
        return "unknown"
    return reason if isinstance(reason, str) and reason else "unknown"


def _reasoning_tokens(res: GatewayResult) -> int | None:
    try:
        value = res.body["usage"]["completion_tokens_details"]["reasoning_tokens"]
    except (KeyError, IndexError, TypeError):
        return None
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _record(calls: list[GatewayResult] | None, res: GatewayResult) -> None:
    if calls is not None:
        calls.append(res)


def _note(failures: list[str] | None, message: str) -> None:
    if failures is not None:
        failures.append(message)
