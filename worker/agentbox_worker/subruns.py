"""sub-run 扩展的 SDK 侧：SubrunManager（规格 §5.4 sub-run 扩展、§5.5 规则 4、§13）。

每个 attempt（session 模式下即每个 turn）一个 SubrunManager，由 TaskContext.subruns 提供。
它是各 sub-run 状态的单一所有者：checkpoint 的 subruns[] 与 result.subruns[] 都由它生成，
因此并发的 sub-run 同时结束时，下一个 checkpoint 看到的是一致的快照。

协议要点：
- start 发出 subrun_start 并等待 subrun_started；rejected → SubrunRejected（retryable_error
  按同一定义有界重发，等待超时同样以同一定义重发：宿主把同 ID 同定义的重发视为同一个 sub-run）。
- 恢复后的 attempt 必须重新 subrun_start（同一定义）才能再为该 sub-run 发送 subrun_end、
  subrun_cancel 或带 subrun_id 的 progress；resume.subruns 中已终态的 ID 在本地直接拒绝
  （subrun_closed），不发消息。
- 一旦宿主发来 subrun_cancel_requested（或编排层 cancel），该 sub-run 唯一会再发出的 subrun_end
  是 cancelled：主体任务被取消并等待结束后发出。即使主体已结束、甚至 subrun_end{succeeded}
  已经发出（与宿主的取消竞态），也照样发出 subrun_end{cancelled}，否则宿主的 T_subrun_cancel
  会终止整个 attempt；本地状态随之改为 cancelled（deadline 时为 timed_out），不再在 checkpoint
  中宣称 completed（宿主会以 invalid_transition 拒绝，E41）。
- 暂停：SDK 不强行停止 sub-run。编排层在工具调用边界检查 ctx.should_pause()，在途调用经
  ctx.run_call 发起时被放弃等待（CallAbandoned）；各 sub-run 主体返回"未完成"，管理器保持其
  started，编排层提交 checkpoint 后返回 Paused。
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from agentbox_worker.errors import WorkerFailure
from agentbox_worker.protocol import (
    MAX_PARENT_STEP_ID_BYTES,
    MAX_SUBRUN_DEADLINE,
    MAX_SUBRUN_SUMMARY,
    MAX_SUBRUNS_PER_TASK,
    ProtocolError,
    valid_subrun_id,
)

T = TypeVar("T")

Emit = Callable[[dict[str, Any]], Awaitable[None]]
CancelTask = Callable[[asyncio.Task[Any]], Awaitable[None]]

# 宿主可在 subrun_started.rejected 中给出的临时故障码：以同一定义重发
RETRYABLE_REJECTION = "retryable_error"
TERMINAL = ("completed", "cancelled", "failed", "timed_out")


class SubrunRejected(WorkerFailure):
    """subrun_start 被拒绝。code ∈ subrun_limit | conflict | subrun_closed | invalid_field
    （宿主给出，或 SDK 在本地判定而未发消息）。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=False)


class SubrunCancelled(Exception):
    """在 sub-run 协程之外表示"该 sub-run 已被宿主或编排层取消"；subrun_end{cancelled} 已发出。

    reason 取自 subrun_cancel_requested.reason（deadline、task_cancel、policy）或编排层
    cancel 时给出的原因。"""

    def __init__(self, subrun_id: str, reason: str) -> None:
        super().__init__(f"sub-run {subrun_id} 已取消：{reason}")
        self.subrun_id = subrun_id
        self.reason = reason


@dataclass(frozen=True)
class SubrunInfo:
    subrun_id: str
    status: str  # started | completed | cancelled | failed | timed_out（宿主裁定）
    result_ref: str | None


class SubrunHandle:
    """一个本 attempt 中已 subrun_start 并得到 started 的 sub-run。

    gateway 是该 sub-run 的 Gateway 视图（带归属头、call id 前缀、在途上限），sub-run 主体
    的所有 Gateway 调用都应经它发出；它没有创建 sub-run 的能力。
    """

    def __init__(self, manager: SubrunManager, subrun_id: str, gateway: Any) -> None:
        self.subrun_id = subrun_id
        self.gateway = gateway
        self._manager = manager
        self._task: asyncio.Task[Any] | None = None  # run() 中的主体任务
        self._cancel_reason: str | None = None
        self._cancel_end_sent = False
        self._lock = asyncio.Lock()  # subrun_end 的发送互斥（完成与取消竞态）

    def cancelled(self) -> bool:
        """是否已被取消（宿主 subrun_cancel_requested 或编排层 cancel）。"""
        return self._cancel_reason is not None

    @property
    def cancel_reason(self) -> str | None:
        return self._cancel_reason

    def gateway_close(self) -> None:
        """关闭 Gateway 视图：此后的请求在本地失败（替身 Gateway 没有 close 时忽略）。"""
        close = getattr(self.gateway, "close", None)
        if callable(close):
            close()

    async def progress(
        self, kind: str, message: str, *, step_id: str | None = None, data: Any = None
    ) -> None:
        """发出带 subrun_id 的 progress 事件。"""
        body: dict[str, Any] = {"type": "progress"}
        if step_id is not None:
            body["step_id"] = step_id
        body["kind"] = kind
        body["message"] = message
        if data is not None:
            body["data"] = data
        body["subrun_id"] = self.subrun_id
        await self._manager._emit(body)


@dataclass
class _Entry:
    status: str  # SUBRUN_STATES 之一（本地视角）
    result_ref: str | None = None
    summary: str = ""
    handle: SubrunHandle | None = None  # 本 attempt 中已 started 时存在
    starting: bool = False


def _truncate(text: str, limit: int = MAX_SUBRUN_SUMMARY) -> str:
    """按 UTF-8 字节截断（替换孤立代理项），保证 summary 满足协议上限。"""
    return str(text).encode("utf-8", "replace")[:limit].decode("utf-8", "ignore")


def _log(message: str) -> None:
    print(f"agentbox_worker: {message}", file=sys.stderr)


def parse_resumed(raw: Any) -> dict[str, SubrunInfo]:
    """resume.subruns[]（协议层已校验）→ {subrun_id: SubrunInfo}。"""
    resumed: dict[str, SubrunInfo] = {}
    for item in raw or []:
        sid = item["subrun_id"]
        resumed[sid] = SubrunInfo(sid, item["status"], item.get("result_ref") or None)
    return resumed


class SubrunManager:
    """单一状态所有者：记录各 sub-run 的状态与 result_ref，生成 checkpoint 的 subruns[]
    一致快照（规格 §5.5 规则 4）与 result.subruns[]。

    由 TaskContext 构造；emit 是带 attempt_id 标记的事件发送，cancel_task 在没有在途发送时
    取消任务（避免取消落在发送中途使输出通道失效），gateway_factory 为 sub-run ID 生成
    Gateway 视图（测试与替身 Gateway 可替换它）。
    """

    def __init__(
        self,
        *,
        negotiated: bool,
        resumed: dict[str, SubrunInfo],
        emit: Emit,
        cancel_task: CancelTask,
        gateway_factory: Callable[[str], Any],
        ack_timeout: float,
        max_ack_attempts: int,
        retry_backoff: float,
    ) -> None:
        self.negotiated = negotiated
        self.gateway_factory = gateway_factory
        self._resumed = dict(resumed)
        self._entries: dict[str, _Entry] = {
            sid: _Entry(info.status, info.result_ref) for sid, info in resumed.items()
        }
        self._emit = emit
        self._cancel_task = cancel_task
        self._ack_timeout = ack_timeout
        self._max_ack_attempts = max_ack_attempts
        self._retry_backoff = retry_backoff
        self._started: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._sent_start: set[str] = set()  # 本 attempt 中发出过 subrun_start 的 ID
        self._background: set[asyncio.Task[Any]] = set()

    # ---- 查询 ----

    def resumed(self) -> dict[str, SubrunInfo]:
        """本 attempt 的 resume.subruns（宿主裁定的状态）；没有恢复时为空。"""
        return dict(self._resumed)

    def snapshot(self) -> list[dict[str, Any]]:
        """checkpoint 的 subruns[]：started / completed+result_ref / failed / cancelled。

        timed_out（宿主裁定，checkpoint 无此取值）列为 cancelled，宿主保持 timed_out。"""
        entries = []
        for sid, entry in self._entries.items():
            status = "cancelled" if entry.status == "timed_out" else entry.status
            item: dict[str, Any] = {"subrun_id": sid, "status": status}
            if status == "completed" and entry.result_ref:
                item["result_ref"] = entry.result_ref
            entries.append(item)
        return entries

    def result_entries(self) -> list[dict[str, Any]]:
        """result.subruns[]：每个 sub-run 的 {id, status, summary}。"""
        return [
            {"id": sid, "status": entry.status, "summary": entry.summary}
            for sid, entry in self._entries.items()
        ]

    def set_summary(self, subrun_id: str, summary: str) -> None:
        """为 result.subruns[] 设置摘要（例如恢复的已完成 sub-run 由编排层从其结果中取得）。"""
        self._entry(subrun_id).summary = _truncate(summary)

    # ---- 生命周期 ----

    async def start(
        self,
        subrun_id: str,
        *,
        parent_step_id: str,
        deadline_ms: int,
        budget_cap_micro: int | None = None,
    ) -> SubrunHandle:
        """发出 subrun_start 并等待 subrun_started；rejected → SubrunRejected。

        未协商 subruns 扩展 → WorkerFailure("subruns_not_negotiated")。字段不合法、ID 已终态
        或超出每 task 4 个的上限时在本地拒绝，不发消息。始终未得到答复 → WorkerFailure
        ("subrun_unresolved", retryable)。"""
        if not self.negotiated:
            raise WorkerFailure("subruns_not_negotiated", "宿主未在 init 中请求 subruns 扩展")
        body = self._start_body(subrun_id, parent_step_id, deadline_ms, budget_cap_micro)
        entry = self._entries.get(subrun_id)
        if entry is not None and entry.status in TERMINAL:
            raise SubrunRejected("subrun_closed", f"sub-run {subrun_id} 已是 {entry.status}")
        if entry is not None and (entry.handle is not None or entry.starting):
            raise WorkerFailure("invalid_field", f"sub-run {subrun_id} 在本 attempt 中已启动")
        if entry is None and len(self._entries) >= MAX_SUBRUNS_PER_TASK:
            limit = f"每个 task 至多 {MAX_SUBRUNS_PER_TASK} 个 sub-run"
            raise SubrunRejected("subrun_limit", limit)
        if entry is None:
            entry = self._entries[subrun_id] = _Entry("started")
        entry.starting = True
        try:
            answer = await self._await_started(subrun_id, body)
        except BaseException:
            entry.starting = False
            if subrun_id not in self._resumed and entry.handle is None:
                # 未得到 started：不计入本地状态（宿主侧若已登记，恢复时会经 resume 告知）
                del self._entries[subrun_id]
            raise
        entry.starting = False
        if answer["status"] != "started":
            if subrun_id not in self._resumed:
                del self._entries[subrun_id]
            code = answer.get("code") or "rejected"
            raise SubrunRejected(code, f"sub-run {subrun_id} 被拒绝：{code}")
        entry.status = "started"
        entry.handle = SubrunHandle(self, subrun_id, self.gateway_factory(subrun_id))
        return entry.handle

    async def run(self, handle: SubrunHandle, body: Callable[[SubrunHandle], Awaitable[T]]) -> T:
        """在独立的 asyncio.Task 中运行 body(handle) 并返回其结果。

        收到 subrun_cancel_requested（或编排层 cancel）→ 取消该任务并等待其结束 → 发出
        subrun_end{cancelled} → 抛出 SubrunCancelled。调用方自身被取消时同样先取消并等待主体。
        body 抛出的异常原样传播（由编排层决定 fail）。"""
        self._own(handle)
        if handle._task is not None:
            raise WorkerFailure("invalid_field", f"sub-run {handle.subrun_id} 的主体已在运行")
        if handle.cancelled():
            await self._finish_cancel(handle)
            raise SubrunCancelled(handle.subrun_id, handle._cancel_reason or "")
        task = asyncio.create_task(body(handle))
        handle._task = task
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError:
            await self._stop(task)
            raise
        if handle.cancelled():
            await self._finish_cancel(handle)
            raise SubrunCancelled(handle.subrun_id, handle._cancel_reason or "")
        return task.result()

    async def complete(self, handle: SubrunHandle, *, summary: str, result_ref: str) -> None:
        """发出 subrun_end{succeeded}，记录 result_ref：下一个 checkpoint 列出 completed。

        已被取消时改为（确保）发出 subrun_end{cancelled} 并抛出 SubrunCancelled。"""
        entry = self._own(handle)
        summary = _truncate(summary)
        async with handle._lock:
            if not handle.cancelled():
                self._require_started(entry, handle)
                await self._end(handle, "succeeded", summary)
                # 先发出 end 再记为 completed：此后的 checkpoint 才会列出 completed
                entry.status, entry.result_ref, entry.summary = "completed", result_ref, summary
                handle.gateway_close()
                return
        await self._finish_cancel(handle)
        raise SubrunCancelled(handle.subrun_id, handle._cancel_reason or "")

    async def fail(self, handle: SubrunHandle, *, summary: str) -> None:
        """发出 subrun_end{failed}；已被取消时改为 cancelled 并抛出 SubrunCancelled。"""
        entry = self._own(handle)
        summary = _truncate(summary)
        async with handle._lock:
            if not handle.cancelled():
                self._require_started(entry, handle)
                await self._end(handle, "failed", summary)
                entry.status, entry.result_ref, entry.summary = "failed", None, summary
                handle.gateway_close()
                return
        await self._finish_cancel(handle)
        raise SubrunCancelled(handle.subrun_id, handle._cancel_reason or "")

    async def cancel(self, handle: SubrunHandle, *, reason: str) -> None:
        """编排层放弃该 sub-run：发出 subrun_cancel，取消主体并等待其结束，发出
        subrun_end{cancelled}。已是 cancelled/failed/timed_out 时无操作。"""
        entry = self._own(handle)
        if not reason:
            raise ValueError("cancel 的 reason 不能为空")
        if handle.cancelled():
            await self._finish_cancel(handle)
            return
        if entry.status in ("cancelled", "failed", "timed_out"):
            return
        handle._cancel_reason = reason
        await self._emit({"type": "subrun_cancel", "subrun_id": handle.subrun_id, "reason": reason})
        await self._finish_cancel(handle)

    # ---- 宿主控制消息（由 TaskContext._handle_control 同步调用） ----

    def handle_control(self, msg: dict[str, Any]) -> None:
        """处理 subrun_started 与 subrun_cancel_requested；不合法时抛出 ProtocolError。"""
        typ, sid = msg["type"], msg["subrun_id"]
        if not self.negotiated:
            raise ProtocolError("control_protocol_error", f"未协商 subruns 扩展却收到 {typ}")
        entry = self._entries.get(sid)
        if typ == "subrun_started":
            future = self._started.get(sid)
            if future is not None and not future.done():
                future.set_result(msg)
            elif sid not in self._sent_start:
                detail = f"未发出 subrun_start 却收到 {sid} 的答复"
                raise ProtocolError("control_protocol_error", detail)
            return  # 重发产生的迟到答复：丢弃
        if entry is None or entry.handle is None:
            raise ProtocolError(
                "control_protocol_error", f"subrun_cancel_requested 引用了本 attempt 未启动的 {sid}"
            )
        handle = entry.handle
        if handle.cancelled() or entry.status in ("cancelled", "failed", "timed_out"):
            return  # 重复的取消，或已以 failed 结束（宿主按 cancel_requested + end{failed} 处理）
        handle._cancel_reason = msg["reason"]
        task = handle._task
        if task is not None and not task.done():
            # run() 正在等待主体：取消主体，由 run() 发出 subrun_end{cancelled}
            self._spawn(self._cancel_task(task))
        else:
            # 没有在运行的主体（尚未 run、已返回，或已发出 end{succeeded}）：直接结束
            self._spawn(self._finish_cancel(handle))

    # ---- 收尾（由运行时在终态提议之前调用） ----

    async def settle(self, *, abandon: bool) -> None:
        """等待后台发送（取消引起的 subrun_end{cancelled}）结束，保证它们先于终态提议发出。

        abandon=True（attempt 被宿主取消或裁决）时直接取消后台任务与仍在运行的主体。"""
        if abandon:
            for entry in self._entries.values():
                if entry.handle is not None and entry.handle._task is not None:
                    entry.handle._task.cancel()
            for task in list(self._background):
                task.cancel()
        while pending := [t for t in self._background if not t.done()]:
            done = await asyncio.gather(*pending, return_exceptions=True)
            for result in done:
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    _log(f"sub-run 后台发送失败：{result!r}")
        for entry in self._entries.values():
            if entry.handle is not None:
                entry.handle.gateway_close()

    # ---- 内部 ----

    def _start_body(
        self, subrun_id: str, parent_step_id: str, deadline_ms: int, cap: int | None
    ) -> dict[str, Any]:
        problem = None
        if not isinstance(subrun_id, str) or not valid_subrun_id(subrun_id):
            problem = f"subrun_id={subrun_id!r} 须匹配 ^[a-z0-9][a-z0-9_-]{{0,31}}$ 且不是 root"
        elif not parent_step_id or len(parent_step_id.encode("utf-8")) > MAX_PARENT_STEP_ID_BYTES:
            problem = f"parent_step_id 须非空且不超过 {MAX_PARENT_STEP_ID_BYTES} 字节"
        elif isinstance(deadline_ms, bool) or not isinstance(deadline_ms, int):
            problem = "deadline_ms 必须是整数"
        elif not 1 <= deadline_ms <= MAX_SUBRUN_DEADLINE:
            problem = f"deadline_ms={deadline_ms}，须在 1–{MAX_SUBRUN_DEADLINE} 之间"
        elif cap is not None and (isinstance(cap, bool) or not isinstance(cap, int) or cap < 0):
            problem = f"budget_cap_micro={cap!r}，须为 ≥ 0 的整数"
        if problem is not None:
            raise SubrunRejected("invalid_field", problem)
        body: dict[str, Any] = {
            "type": "subrun_start",
            "subrun_id": subrun_id,
            "parent_step_id": parent_step_id,
            "deadline_ms": deadline_ms,
        }
        if cap is not None:
            body["budget_cap_micro"] = cap
        return body

    async def _await_started(self, subrun_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """发送并等待答复；超时与 retryable_error 时以同一定义重发，共用重试上限。"""
        retries = 0
        try:
            while True:
                future = asyncio.get_running_loop().create_future()
                self._started[subrun_id] = future
                self._sent_start.add(subrun_id)
                await self._emit(body)
                try:
                    answer = await asyncio.wait_for(future, self._ack_timeout)
                except TimeoutError:
                    answer = None
                if answer is not None and answer.get("code") != RETRYABLE_REJECTION:
                    return answer
                if retries == self._max_ack_attempts:
                    break
                retries += 1
                if answer is not None:
                    await asyncio.sleep(self._retry_backoff)
        finally:
            self._started.pop(subrun_id, None)
        raise WorkerFailure(
            "subrun_unresolved",
            f"sub-run {subrun_id}：{self._max_ack_attempts} 次重发后仍未得到 subrun_started",
            retryable=True,
        )

    async def _end(self, handle: SubrunHandle, status: str, summary: str) -> None:
        await self._emit(
            {
                "type": "subrun_end",
                "subrun_id": handle.subrun_id,
                "status": status,
                "summary": summary,
            }
        )

    async def _finish_cancel(self, handle: SubrunHandle) -> None:
        """取消并等待主体结束，发出 subrun_end{cancelled}（每个 sub-run 至多一次）。"""
        async with handle._lock:
            if handle._cancel_end_sent:
                return
            task = handle._task
            if task is not None and not task.done():
                await self._stop(task)
            reason = handle._cancel_reason or "cancelled"
            handle.gateway_close()
            await self._end(handle, "cancelled", _truncate(f"cancelled: {reason}"))
            handle._cancel_end_sent = True
            entry = self._entries[handle.subrun_id]
            entry.status = "timed_out" if reason == "deadline" else "cancelled"
            entry.result_ref = None
            entry.summary = _truncate(f"cancelled: {reason}")

    async def _stop(self, task: asyncio.Task[Any]) -> None:
        """在没有在途发送时取消任务并等待它结束。"""
        if not task.done():
            await self._cancel_task(task)
        with contextlib.suppress(BaseException):
            await asyncio.wait({task})

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _entry(self, subrun_id: str) -> _Entry:
        entry = self._entries.get(subrun_id)
        if entry is None:
            raise WorkerFailure("invalid_field", f"未知的 sub-run {subrun_id!r}")
        return entry

    def _own(self, handle: SubrunHandle) -> _Entry:
        entry = self._entries.get(handle.subrun_id)
        if handle._manager is not self or entry is None or entry.handle is not handle:
            raise WorkerFailure("invalid_field", f"sub-run {handle.subrun_id} 不属于本 attempt")
        return entry

    @staticmethod
    def _require_started(entry: _Entry, handle: SubrunHandle) -> None:
        if entry.status != "started":
            raise WorkerFailure(
                "invalid_field", f"sub-run {handle.subrun_id} 已是 {entry.status}，不能再结束"
            )
