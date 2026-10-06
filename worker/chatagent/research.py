"""研究阶段：计划中的子主题作为 sub-run 并行（或串行）执行（Plan 14 Task 9）。

主 Agent（kimi-k3）规划并写报告；每个子主题的 kimi-k2.6 搜索 / 阅读循环在自己的 sub-run 中运行
（独立上下文、带归属头的 Gateway 视图、call id 前缀 <subrun_id>/）。只在宿主协商了 sub-run 扩展时
使用；未协商时编排循环按 Plan 13 在进程内逐个执行子主题。

- 触发：编排模型的 research_subtopic 调用（或 fixed_plan 的合成调用）运行整个研究阶段：计划中
  全部未完成的子主题同时启动（scheduling=serial 时逐个启动，其余完全相同），全部结束后一次把
  各子主题的摘要（全局编号）与"不完整"标注交给编排模型。
- 分配（阶段开始时一次，写入状态并提交之后才启动；恢复时以同一定义重发 subrun_start）：读
  /v1/budget，扣除报告预留后平均分给各 sub-run 作 budget_cap_micro；不足以给每个子主题至少一次
  模型调用时把子主题合并为可负担的组数（减少并行度），在报告中注明。工具份额取自计划（软上限），
  30 次额度由 Gateway 按 task 计数。
- 确定性：sub-run 的提示词在分配时生成（与调度无关）；收尾判定只取决于该 sub-run 自己已提交的
  状态；其 call id 计数器随自己的转录一起保存（并发的 checkpoint 不会记下尚未写入转录的在途
  调用）；额度行取 Gateway 计数头（重放时不变）；新来源先在本地编号，阶段结束时按计划顺序并入
  全局来源表。故串行与并行在同样的脚本下得到同样的报告，崩溃恢复后的请求体不变。
- 线程：工具在线程池中运行，只修改本次调用的工作副本（本地来源、份额视图）；task 级已用次数在锁
  内更新；合并回状态、写 tool 消息与 checkpoint 都在事件循环上，checkpoint 不会看到半写的状态。
  提交由单一状态所有者串行进行，并屏蔽 sub-run 的取消（每 attempt 至多一个在途提交）。
- 部分失败（规格 §13.6）：模型不可用 → fail；deadline 取消 → timed_out；sub-run 或 task 费用耗尽
  → 不再发起付费调用，以已收集来源的抽取式摘要 complete（partial）；task 级工具额度用尽 → 各
  sub-run 不再发起工具调用，以已有证据写摘要。
- 停止：各 sub-run 在下一个工具边界返回（宿主侧仍为 started），由编排层写停止摘要与 checkpoint；
  继续时 completed 的从结果产物读取、不重跑，started 的同 ID 重发 subrun_start 后从转录继续。
  立即写报告：对仍在运行的 sub-run cancel(reason="finish_now")。E41：取消与完成竞态时宿主拒绝
  仍列出 completed 的 checkpoint → 等 SDK 处理完取消，按本地状态重建后重提一次。
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from typing import TYPE_CHECKING, Any

from agentbox_worker.errors import BudgetExhausted, CheckpointRejected, GatewayError
from agentbox_worker.gateway import CallIds
from agentbox_worker.protocol import MAX_SUBRUNS_PER_TASK
from agentbox_worker.subruns import SubrunCancelled, SubrunHandle, SubrunRejected
from agentbox_worker.tools import ToolContext, ToolResult
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.sources import EXCERPT_MAX_BYTES, Source, SourceStore
from agentbox_worker.tools.sources import _source as source_from_json
from agentbox_worker.tools.text import truncate_utf8
from agentbox_worker.tools.todo import BRIEF_MAX_CHARS, TITLE_MAX_CHARS, TodoItem
from chatagent.config import TurnConfig
from chatagent.events import Emitter
from chatagent.model import ModelReply, ModelUnavailable, assistant_message, call_model
from chatagent.prompts import SUBTOPIC_CLOSE, subtopic_system, subtopic_user
from chatagent.state import SubtopicState, TurnState

if TYPE_CHECKING:
    from chatagent.loop import Agent

PARENT_STEP = "research"  # subrun_start.parent_step_id，也是阶段级 checkpoint 的 step_id
RESULT_MEDIA_TYPE = "application/json"
FINISH_REASON = "finish_now"
REFS_MAX = 1024  # 与编排循环的 MAX_REFS 相同
THINKING_MAX_CHARS = 600
PREVIEW_MAX_CHARS = 600
EXCERPT_CHARS = 300
E41_WAIT_S = 5.0
EXTRACT_REASON = "费用上限已到，摘要为已收集来源的摘录"
EXTRACT_HEAD = "（费用上限已到：以下为已收集来源的摘录，未经模型总结）"
LIMIT_REASON = f"超出每轮 {MAX_SUBRUNS_PER_TASK} 个子主题 sub-run 的上限，未执行"
NO_MONEY_NOTE = "费用预算不足以执行子主题"
_CANCEL_REASONS = {
    "deadline": "超时",
    "finish_now": "用户要求立即写报告",
    "task_cancel": "任务已取消",
}
_HOST_STATUS_REASONS = {"timed_out": "超时", "cancelled": "已取消", "failed": "失败"}


def _log(message: str) -> None:
    print(f"chatagent: {message}", file=sys.stderr)


def _cancel_reason(reason: str) -> str:
    return _CANCEL_REASONS.get(reason, f"已取消（{reason}）")


# ---- 费用分配 ----


def report_reserve(cfg: TurnConfig) -> int:
    """报告与引用核对的预留：report_reserve_micro，缺省 report_max_tokens × 编排输出单价 × 1.5。"""
    if cfg.report_reserve_micro is not None:
        return cfg.report_reserve_micro
    return -(-cfg.report_max_tokens * cfg.orchestrator_output_micro_per_mtok * 3 // 2_000_000)


def call_estimate(cfg: TurnConfig) -> int:
    """一次 sub-run 模型调用的费用估算（subtopic_max_tokens × worker 输出单价），至少 1。"""
    return max(1, -(-cfg.subtopic_max_tokens * cfg.worker_output_micro_per_mtok // 1_000_000))


def plan_caps(
    n: int, info: dict[str, Any] | None, cfg: TurnConfig
) -> tuple[int, int | None, str | None]:
    """n 个子主题 → (组数 k, 每个 sub-run 的 budget_cap_micro, 报告注记)。

    没有 /v1/budget 或预算无上限 → (n, None, None)：sub-run 层只做归属、不设上限。可用额扣除报告
    预留后不足以给每组至少一次模型调用时减少组数（合并子主题）；一组都给不起 → k = 0。"""
    if n <= 0:
        return 0, None, None
    if not isinstance(info, dict):
        return n, None, None
    limit, available = info.get("limit_micro"), info.get("available_micro")
    if not isinstance(limit, int) or not isinstance(available, int) or limit <= 0:
        return n, None, None
    pool = max(0, available - report_reserve(cfg))
    k = min(n, pool // call_estimate(cfg))
    if k == 0:
        return 0, None, NO_MONEY_NOTE
    note = f"费用预算有限：{n} 个子主题合并为 {k} 组执行" if k < n else None
    return k, pool // k, note


# ---- 线程中工具使用的工作视图 ----


class LocalSources:
    """ToolContext.sources 的 sub-run 视图：读全局来源表（阶段中只读）的前 base 项，新来源登记在
    本地、编号从 base + 1 起；同 sha 或同 URL（全局或本地）只登记一次。"""

    def __init__(self, store: SourceStore, base: int, own: list[Source]) -> None:
        self._store = store
        self._base = base
        self.own = own

    def add(
        self, *, sha256: str, url: str, title: str, excerpt: str, call_id: str
    ) -> tuple[Source, bool]:
        for src in self.all():
            if src.sha256 == sha256 or src.url == url:
                return src, False
        n = max([self._base, *(s.n for s in self.own)]) + 1
        src = Source(n, sha256, url, title, truncate_utf8(excerpt, EXCERPT_MAX_BYTES), call_id)
        self.own.append(src)
        return src, True

    def get(self, n: int) -> Source | None:
        return next((s for s in self.all() if s.n == n), None)

    def all(self) -> list[Source]:
        return [s for s in self._store.all() if s.n <= self._base] + list(self.own)


class SubBudget:
    """ToolContext.budget 的 sub-run 视图（线程安全）：task 级 used/limit 在锁内写入共享的
    TurnBudget；份额与已用次数属于本 sub-run。额度行中的"已用"取本次调用的 Gateway 计数头
    （重放时与原调用相同），没有头时取 task 级视图。"""

    def __init__(self, turn: TurnBudget, lock: threading.Lock, share: int, spent: int) -> None:
        self._turn = turn
        self._lock = lock
        self.share = share
        self.spent = spent
        self.exhausted = False
        self._seen: tuple[int, int] | None = None

    def remaining(self) -> int:
        with self._lock:
            return self._turn.remaining()

    def share_left(self, subtopic_id: str | None = None) -> int:
        return max(0, min(self.share - self.spent, self._counts()[2]))

    def record(self, result_budget: tuple[int, int] | None, subtopic_id: str | None) -> None:
        with self._lock:
            if result_budget is None:
                self._turn.used += 1
            else:
                used, limit = result_budget
                self._turn.used = max(self._turn.used, used)
                self._turn.limit = limit
        self._seen = result_budget
        self.spent += 1

    def record_failure(
        self, subtopic_id: str | None, result_budget: tuple[int, int] | None = None
    ) -> None:
        if result_budget is not None:  # 错误响应带计数头：同成功，以 Gateway 计数为准
            self.record(result_budget, subtopic_id)
            return
        with self._lock:
            self._turn.used += 1
        self.spent += 1

    def exhaust(self) -> None:
        with self._lock:
            self._turn.exhaust()
        self.exhausted = True

    def line(self, subtopic_id: str | None = None) -> str:
        used, limit, left = self._counts()
        return (
            f"工具额度：已用 {used}/{limit}，剩余 {left}"
            f"（本子主题剩余 {max(0, min(self.share - self.spent, left))}）"
        )

    def _counts(self) -> tuple[int, int, int]:
        if self._seen is not None:
            used, limit = self._seen
        else:
            with self._lock:
                used, limit = self._turn.used, self._turn.limit
        return used, limit, max(0, limit - used)


@dataclass(frozen=True)
class _Done:
    summary: str
    partial: bool


_PAUSED = object()  # sub-run 主体在工具边界看到暂停请求


def _args(raw: str) -> Any:
    try:
        return json.loads(raw) if raw.strip() else {}
    except (ValueError, RecursionError):
        return raw


def _first_error(group: BaseExceptionGroup) -> BaseException:
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            return _first_error(exc)
        if not isinstance(exc, asyncio.CancelledError):
            return exc
    return group.exceptions[0]


def _consume(task: asyncio.Future[Any]) -> None:
    """被屏蔽的提交在调用方已被取消时，其异常由这里取走（避免"未取回的异常"告警）。"""
    if not task.cancelled() and task.exception() is not None:
        _log(f"sub-run 被取消后完成的提交失败：{task.exception()!r}")


# ---- 研究阶段 ----


class ResearchPhase:
    def __init__(self, agent: Agent) -> None:
        self.agent = agent
        self.ctx = agent.ctx
        self.st: TurnState = agent.state
        self.cfg: TurnConfig = agent.cfg
        self.mgr = agent.ctx.subruns
        self.lock = threading.Lock()
        self.paused = False

    # ---- 入口 ----

    async def run(self) -> ToolResult | None:
        """运行（或从 checkpoint 继续）研究阶段；暂停时返回 None（各 sub-run 已在工具边界返回）。"""
        if self.st.phase_subs is None:
            await self._allocate()
        groups = self._groups()
        await self._absorb(groups)
        todo = [g for g in groups if self._needs_run(g)]
        if self.cfg.scheduling == "serial":
            for g in todo:
                if self.ctx.should_pause():
                    self.paused = True
                if self.paused:
                    break
                await self._one(g)
        elif todo:
            try:
                async with asyncio.TaskGroup() as tg:
                    for g in todo:
                        tg.create_task(self._one(g))
            except BaseExceptionGroup as group:
                raise _first_error(group) from None
        if self.paused:
            return None
        return await self._close(groups)

    async def finish(self) -> ToolResult:
        """立即写报告：仍在运行的 sub-run 以 cancel(reason="finish_now") 结束；已完成而宿主尚未
        收到结束的补发 complete；用已完成与部分证据结束阶段。"""
        groups = self._groups()
        await self._absorb(groups)
        resumed = self.mgr.resumed()
        for g in groups:
            info = resumed.get(g.subrun_id or "")
            host_started = info is not None and info.status == "started"
            if g.status in ("pending", "running"):
                if host_started:
                    try:
                        h = await self._start(g)
                        await self.mgr.cancel(h, reason=FINISH_REASON)
                    except SubrunRejected as exc:
                        _log(f"{g.subrun_id} 无法取消：{exc.code}")
                ran = g.status == "running" or bool(g.found) or g.rounds > 0
                self._mark(g, "failed" if ran else "skipped", _cancel_reason(FINISH_REASON))
            elif g.status == "done" and host_started:
                h = await self._start(g)
                await self._complete(h, g)
        return await self._close(groups)

    # ---- 分配 ----

    async def _allocate(self) -> None:
        st, cfg = self.st, self.cfg
        runnable = [
            i
            for i in st.todo
            if i.budget > 0
            and (
                st.subtopics.get(i.id) is None
                or st.subtopics[i.id].status in ("pending", "running")
            )
        ]
        used = {s.subrun_id for s in st.subtopics.values() if s.subrun_id}
        slots = max(0, MAX_SUBRUNS_PER_TASK - len(used))
        for item in runnable[slots:]:
            self._skip(item, LIMIT_REASON)
        runnable = runnable[:slots]
        k, cap, note = plan_caps(len(runnable), await self._budget_info(), cfg)
        if note and note not in st.notes:
            st.notes.append(note)
        if k == 0:
            for item in runnable:
                self._skip(item, NO_MONEY_NOTE)
        n = len(runnable)
        groups = [runnable[j * n // k : (j + 1) * n // k] for j in range(k)] if k else []
        base = max((s.n for s in st.sources.all()), default=0)
        phase = []
        for group in groups:
            head, item = group[0], _merged_item(group)
            sub = st.subtopics.get(head.id) or SubtopicState(head.id)
            sub.subrun_id = f"st{head.id}"
            sub.share, sub.cap, sub.deadline_ms = item.budget, cap, cfg.subrun_deadline_ms
            sub.base, sub.members = base, [i.id for i in group[1:]]
            if not sub.messages:  # 提示词在分配时生成：与调度方式无关
                line = SubBudget(st.budget, self.lock, sub.share, sub.spent).line()
                sub.messages = [
                    {
                        "role": "system",
                        "content": subtopic_system(item, self.agent.strategy(), line),
                    },
                    {"role": "user", "content": subtopic_user(item)},
                ]
            st.subtopics[head.id] = sub
            for member in group[1:]:
                st.subtopics[member.id] = SubtopicState(
                    member.id, status="skipped", incomplete=f"并入子主题「{head.title}」"
                )
            phase.append(head.id)
        st.phase_subs, st.phase, st.current_subtopic = phase, "subtopic", None
        await self.commit(PARENT_STEP)
        await self.agent.emit.todo(PARENT_STEP, st.todo)

    async def _budget_info(self) -> dict[str, Any] | None:
        fn: Callable[[], Any] | None = getattr(self.agent.gw, "budget", None)
        if fn is None:
            return None
        try:
            info = await asyncio.to_thread(fn)
        except (GatewayError, OSError, ValueError) as exc:
            _log(f"读取 /v1/budget 失败，sub-run 不设费用上限：{exc}")
            return None
        return info if isinstance(info, dict) else None

    def _skip(self, item: TodoItem, reason: str) -> None:
        self.st.subtopics[item.id] = SubtopicState(item.id, status="skipped", incomplete=reason)
        item.status = "skipped"
        note = f"子主题「{item.title}」未执行：{reason}"
        if note not in self.st.notes:
            self.st.notes.append(note)

    # ---- 恢复 ----

    def _groups(self) -> list[SubtopicState]:
        return [self.st.subtopics[i] for i in self.st.phase_subs or []]

    def _needs_run(self, g: SubtopicState) -> bool:
        if g.status in ("pending", "running"):
            return True
        info = self.mgr.resumed().get(g.subrun_id or "")
        return g.status == "done" and info is not None and info.status == "started"

    async def _absorb(self, groups: list[SubtopicState]) -> None:
        """resume.subruns（宿主裁定）优先：completed 从结果产物读取、不重跑；cancelled、failed、
        timed_out 不重跑，按部分失败处理。"""
        resumed = self.mgr.resumed()
        for g in groups:
            info = resumed.get(g.subrun_id or "")
            if info is None:
                continue
            if info.status == "completed":
                if g.status != "done":
                    await self._load_result(g, info.result_ref)
                if g.status == "done":
                    self.mgr.set_summary(info.subrun_id, g.summary or "")
            elif info.status in _HOST_STATUS_REASONS and g.status != "failed":
                self._mark(g, "failed", _HOST_STATUS_REASONS[info.status])

    async def _load_result(self, g: SubtopicState, ref: str | None) -> None:
        try:
            data = await asyncio.to_thread(self.agent.gw.read_blob, ref or "")
            body = json.loads(data)
            summary, partial = body["summary"], bool(body.get("partial", False))
            found = [asdict(source_from_json(s)) for s in body.get("sources", [])]
        except (GatewayError, ValueError, KeyError, TypeError, OSError) as exc:
            _log(f"{g.subrun_id} 的结果产物不可读：{exc}")
            self._mark(g, "failed", "结果不可读")
            return
        if not isinstance(summary, str):
            self._mark(g, "failed", "结果不可读")
            return
        g.summary, g.partial, g.found, g.result_ref = summary, partial, found, ref
        g.incomplete = EXTRACT_REASON if partial else None
        g.status = "done"
        self._set_items(g, "done")

    # ---- 单个 sub-run ----

    async def _start(self, g: SubtopicState) -> SubrunHandle:
        assert g.subrun_id is not None
        return await self.mgr.start(
            g.subrun_id,
            parent_step_id=PARENT_STEP,
            deadline_ms=g.deadline_ms,
            budget_cap_micro=g.cap,
        )

    async def _one(self, g: SubtopicState) -> None:
        try:
            h = await self._start(g)
        except SubrunRejected as exc:
            await self._failed(g, f"未能启动（{exc.code}）")
            return
        emit = self.agent.emit.for_subrun(h)
        if g.status == "done":  # 已完成而宿主尚未收到结束（崩溃发生在两者之间）
            await self._complete(h, g)
            return
        item = self.st.todo_item(g.id)
        if g.status == "pending" or (item is not None and item.status != "in_progress"):
            g.status = "running"
            self._set_items(g, "in_progress")
            await emit.todo(f"sub-{g.id}", self.st.todo)
            await emit.subtopic(g, self._title(g))

        async def body(handle: SubrunHandle) -> Any:
            return await self._body(handle, emit, g)

        try:
            out = await self.mgr.run(h, body)
        except SubrunCancelled as exc:
            await self._failed(g, _cancel_reason(exc.reason))
            return
        except ModelUnavailable:
            reason = "模型服务暂时不可用"
            try:
                await self.mgr.fail(h, summary=reason)
            except SubrunCancelled as exc:
                reason = _cancel_reason(exc.reason)
            await self._failed(g, reason)
            return
        if out is _PAUSED:
            self.paused = True
            return
        assert isinstance(out, _Done)
        g.summary, g.partial = out.summary, out.partial
        g.incomplete = EXTRACT_REASON if out.partial else None
        await self._complete(h, g)

    async def _complete(self, h: SubrunHandle, g: SubtopicState) -> None:
        """登记 internal 产物 subruns/<id>.json（{summary, sources, partial}），以其 sha 作
        result_ref 发出 subrun_end{succeeded}，之后的 checkpoint 列出 completed。"""
        assert g.subrun_id is not None
        payload = {"summary": g.summary or "", "sources": g.found, "partial": g.partial}
        path = f"subruns/{g.subrun_id}.json"
        target = self.ctx.out_dir / path
        target.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        target.write_text(text, encoding="utf-8", newline="\n")
        ref = await self.ctx.register_artifact(
            f"subrun-{g.subrun_id}", path, media_type=RESULT_MEDIA_TYPE, visibility="internal"
        )
        g.result_ref = ref.sha256
        try:
            await self.mgr.complete(h, summary=g.summary or "", result_ref=ref.sha256)
        except SubrunCancelled as exc:
            await self._failed(g, _cancel_reason(exc.reason))
            return
        g.status = "done"
        self._set_items(g, "done")
        # subrun_end 之后的事件经 root 发出（不再带该 subrun_id）
        await self.agent.emit.todo(f"sub-{g.id}", self.st.todo)
        await self.agent.emit.subtopic(
            g, self._title(g), with_summary=False
        )  # 全局编号的摘要在阶段结束时
        await self.commit(PARENT_STEP)

    async def _failed(self, g: SubtopicState, reason: str) -> None:
        self._mark(g, "failed", reason)
        await self.agent.emit.todo(f"sub-{g.id}", self.st.todo)
        await self.agent.emit.subtopic(g, self._title(g))
        await self.commit(PARENT_STEP)

    def _mark(self, g: SubtopicState, status: str, reason: str) -> None:
        g.status, g.incomplete, g.result_ref = status, reason, None
        self._set_items(g, "skipped")

    def _set_items(self, g: SubtopicState, status: str) -> None:
        for sid in (g.id, *g.members):
            item = self.st.todo_item(sid)
            if item is not None:
                item.status = status

    def _title(self, g: SubtopicState) -> str:
        titles = [i.title for sid in (g.id, *g.members) if (i := self.st.todo_item(sid))]
        return "、".join(titles) or g.id

    # ---- sub-run 主体（kimi-k2.6 的搜索 / 阅读循环） ----

    async def _body(self, h: SubrunHandle, emit: Emitter, g: SubtopicState) -> Any:
        view = h.gateway
        if hasattr(view, "call_ids"):  # 本 sub-run 的计数器随其转录保存（见模块说明）
            view.call_ids = CallIds.restore(g.call_ids)
        step = f"sub-{g.id}"
        while True:
            if self.ctx.should_pause():
                return _PAUSED
            last = g.messages[-1] if g.messages else None
            if last is not None and last["role"] == "assistant" and not last.get("tool_calls"):
                return _Done(last["content"], False)  # 摘要已写出（恢复）
            pending = self.agent.pending_calls(g.messages)
            if pending:
                for tc in pending:
                    if self.ctx.should_pause():
                        return _PAUSED
                    try:
                        await self._tool(view, emit, g, step, tc)
                    except BudgetExhausted:
                        return self._extract(g)
                continue
            if self._closing(g) and not g.closing:
                g.messages.append({"role": "user", "content": SUBTOPIC_CLOSE})
                g.closing = True
            tools = None if g.closing else self.agent.sub_registry.schemas()
            try:
                reply = await asyncio.to_thread(
                    call_model,
                    view,
                    step,
                    [dict(m) for m in g.messages],  # 副本：并发的压缩不影响在途请求体
                    model=self.cfg.worker_model,
                    max_tokens=self.cfg.subtopic_max_tokens,
                    tools=tools,
                )
            except BudgetExhausted:
                return self._extract(g)
            g.rounds += 1
            self.st.add_refs([reply.blob])
            await self._thinking(emit, step, reply)
            g.messages.append(assistant_message(reply))
            self._save_ids(view, g)
            if not reply.tool_calls:
                return _Done(reply.content, False)

    def _closing(self, g: SubtopicState) -> bool:
        """只取决于该 sub-run 自己已提交的状态（恢复后同样的判定）。"""
        return (
            g.closing
            or g.exhausted
            or g.spent >= g.share
            or g.rounds >= self.cfg.max_subtopic_rounds
        )

    async def _tool(
        self, view: Any, emit: Emitter, g: SubtopicState, step: str, tc: dict[str, Any]
    ) -> None:
        st = self.st
        name, raw = tc["function"]["name"], tc["function"]["arguments"]
        args = _args(raw)
        index = sum(1 for m in g.messages if m.get("role") == "tool") + 1
        event_id = f"{step}:{index}"
        await emit.tool_call(step, event_id, name, args, g.id)
        budget = SubBudget(st.budget, self.lock, g.share, g.spent)
        sources = LocalSources(st.sources, g.base, g.found_sources())
        tctx = ToolContext(
            gateway=view,
            step_id=step,
            budget=budget,  # type: ignore[arg-type]
            sources=sources,  # type: ignore[arg-type]
            flags=replace(st.flags),
            subtopic_id=g.id,
            skills=self.agent.skills,
            call_index=index,
        )
        try:
            result = await asyncio.to_thread(self.agent.sub_registry.dispatch, name, raw, tctx)
        except BudgetExhausted:
            failed = ToolResult(content="费用上限已到", ok=False)
            await emit.tool_result(step, event_id, name, failed, g.id, args)
            raise
        # 合并回状态（事件循环上，与 tool 消息、计数器在同一时刻；之间没有 await）
        g.spent = budget.spent
        st.budget.spent[g.id] = g.spent
        g.exhausted = g.exhausted or budget.exhausted or result.control == "budget_exhausted"
        g.found = [asdict(s) for s in sources.own]
        st.flags.researching = True
        g.messages.append(
            {"role": "tool", "tool_call_id": tc["id"], "name": name, "content": result.content}
        )
        self._save_ids(view, g)
        st.add_refs(result.blobs)
        await emit.tool_result(step, event_id, name, result, g.id, args)
        tool = self.agent.sub_registry.get(name)
        if tool is not None and tool.counts_budget:
            await emit.budget(st.budget)
        await self.commit(step)

    @staticmethod
    def _save_ids(view: Any, g: SubtopicState) -> None:
        ids = getattr(view, "call_ids", None)
        if isinstance(ids, CallIds):
            g.call_ids = ids.snapshot()

    async def _thinking(self, emit: Emitter, step: str, reply: ModelReply) -> None:
        text = reply.reasoning or (reply.content if reply.tool_calls else "")
        if text:
            await emit.thinking(
                step, text[:THINKING_MAX_CHARS], reply.call_id, reply.request, reply.blob
            )

    @staticmethod
    def _extract(g: SubtopicState) -> _Done:
        """费用耗尽：不再调用模型，以已收集来源的摘录作摘要（E43"可完成总结"）。"""
        lines = [EXTRACT_HEAD]
        for s in g.found_sources():
            excerpt = " ".join(s.excerpt.split())[:EXCERPT_CHARS]
            lines.append(f"- [{s.n}] {s.title or s.url}：{excerpt}")
        if len(lines) == 1:
            lines.append("（尚未收集到来源）")
        return _Done("\n".join(lines), True)

    # ---- 结束 ----

    async def _close(self, groups: list[SubtopicState]) -> ToolResult:
        """全部 sub-run 已结束：按计划顺序合并来源、标注不完整的子主题，返回给编排模型的结果。"""
        st = self.st
        self._sync()
        for g in groups:
            st.merge_found(g)
        parts: list[str] = []
        for g in groups:
            title = self._title(g)
            if g.status == "done" and not g.incomplete:
                parts.append(f"子主题「{title}」摘要：\n{g.summary or ''}")
                continue
            note = f"子主题「{title}」不完整（{g.incomplete or '未完成'}）"
            if note not in st.notes:
                st.notes.append(note)
            st.partial = True
            text = f"{note}：\n{g.summary}" if g.summary else note
            if g.sources and not g.summary:
                text += "；已收集的来源：" + "".join(f"[{n}]" for n in g.sources)
            parts.append(text)
        if not groups:
            parts.append("没有执行任何子主题：" + "；".join(st.notes or ["无可执行的子主题"]))
        parts.append(st.budget.line())
        content = "\n\n".join(parts)
        st.phase_subs, st.phase, st.current_subtopic = None, "orchestrating", None
        for g in groups:
            await self.agent.emit.subtopic(g, self._title(g))
        await self.agent.emit.todo(PARENT_STEP, st.todo)
        return ToolResult(
            content=content, preview={"kind": "text", "text": content[:PREVIEW_MAX_CHARS]}
        )

    # ---- 提交 ----

    async def commit(self, step: str) -> None:
        """压缩 → checkpoint（SDK 附上 subruns 快照）。屏蔽调用方（sub-run 主体）的取消，
        保证提交不被中途放弃、每 attempt 至多一个在途提交。"""
        task = asyncio.ensure_future(self._commit(step))
        task.add_done_callback(_consume)
        await asyncio.shield(task)

    async def _commit(self, step: str) -> None:
        st = self.st
        for attempt in (1, 2):
            self._sync()
            st.compact()
            before = self.mgr.snapshot()
            try:
                await self.ctx.checkpoint(step, state=st.to_json(), refs=st.refs[-REFS_MAX:])
                return
            except CheckpointRejected:
                completed = any(s["status"] == "completed" for s in before)
                if attempt == 2 or not completed:
                    raise
                # E41：宿主已把某个 sub-run 置为 cancel_requested，拒绝了列出 completed 的
                # checkpoint；等 SDK 处理完取消（快照不再列出 completed）后按本地状态重建
                await self._await_change(before)

    async def _await_change(self, before: list[dict[str, Any]]) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + E41_WAIT_S
        while self.mgr.snapshot() == before and loop.time() < deadline:
            await asyncio.sleep(0.01)

    def _sync(self) -> None:
        """宿主在完成之后取消了 sub-run（竞态）：本地已完成的子主题改为不完整。"""
        statuses = {e["id"]: e["status"] for e in self.mgr.result_entries()}
        for g in self._groups():
            status = statuses.get(g.subrun_id or "")
            if g.status == "done" and status in _HOST_STATUS_REASONS:
                self._mark(g, "failed", _HOST_STATUS_REASONS[status])


def _merged_item(group: list[TodoItem]) -> TodoItem:
    """一组子主题（费用不足时合并）→ 一个 sub-run 的简报：标题与简报拼接，份额相加。"""
    if len(group) == 1:
        return group[0]
    title = "、".join(i.title for i in group)[:TITLE_MAX_CHARS]
    brief = "\n\n".join(f"【{i.title}】{i.brief}" for i in group)[:BRIEF_MAX_CHARS]
    return TodoItem(
        id=group[0].id,
        title=title,
        status="pending",
        budget=sum(i.budget for i in group),
        brief=brief,
    )
