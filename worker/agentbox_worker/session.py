"""Worker SDK 的 session 模式：一个长期进程按 task_start → task_accepted → 终态提议 →
task_outcome → task_released 循环执行多个 task attempt（规格 §5.4 session 扩展、§12.3–12.4）。

- 握手：init.mode = session（session_id、incarnation_id）；session_resume 带 state 时直接采用，
  带 staged_state_path 时经 load_staged 读取；ready 确认 mode = session 与 session_ext: 1。
- 每个 attempt 以新的 TaskContext 运行应用；应用返回 Result（可带 session_state 提议）或
  Paused（可带 awaiting_input，协调者裁定 A）。宿主 cancel 或提前裁决时不发终态提议。
- 提议之后等待 task_outcome，超时以 task_outcome_query 查询；裁决 succeeded 且提交的正是
  本次提议的 session checkpoint 时采用新状态，否则丢弃工作副本。应用任务结束（取消时等待其
  结束）之后才发 task_released，不留后台任务。
- 事件 seq 每 incarnation 从 1 严格递增（共用一个 Outbox）。
- 无法继续时的退出码：宿主违反协议或 task_outcome 始终未到 → 1；stdin 在空闲时关闭 → 0，
  在 attempt 进行中关闭 → 取消该 attempt 后 1；session_close → closed 后 0。
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

from agentbox_worker.errors import TransportBroken, WorkerFailure
from agentbox_worker.outbox import Outbox
from agentbox_worker.protocol import (
    HOST,
    MODE_SESSION,
    SESSION_EXT_VERSION,
    VERSION,
    ProtocolError,
    decode_session_line,
    parse_json,
)
from agentbox_worker.runtime import (
    EXIT_FAILURE,
    EXIT_OK,
    Paused,
    Result,
    TaskContext,
    Timing,
    _await_app,
    _error_body,
    _log,
    _outcome_body,
    _read_init,
    _run_main,
    _snapshot,
)
from agentbox_worker.transport import Transport


@dataclass(frozen=True)
class SessionInfo:
    """attempt 开始时已提交的会话状态。state 是本 attempt 的独立副本，只读：应用的修改不会
    进入已提交状态；新状态只能经 Result.session_state 提议。"""

    session_id: str
    incarnation_id: str
    checkpoint_id: str | None  # 最新已提交的会话 checkpoint（尚无时为 None）
    state: Any  # 已提交的会话状态（尚无时为 None）


SessionApp = Callable[[TaskContext], Awaitable[Result | Paused]]


def read_staged_state(path: str) -> Any:
    """读取冷恢复时宿主暂存的会话状态文件（规格 §12.2）：严格 UTF-8 的 JSON，结构限制同协议。"""
    try:
        return parse_json(Path(path).read_bytes())
    except ProtocolError as exc:
        raise ValueError(f"{path} 不是合法的 JSON：{exc.detail}") from exc


_EOF = object()  # stdin 已关闭
_TIMEOUT = object()  # 等待期间没有宿主消息


class _Inbox:
    """宿主消息的读取。至多一个在途 receive，等待超时或与应用并发等待时不取消它，
    因而不会丢消息，也保留传输层的背压。读到的行按 session 模式解码；解码失败返回
    ProtocolError 对象，stdin 关闭返回 _EOF。"""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._pending: asyncio.Task[Any] | None = None

    def pending(self) -> asyncio.Task[Any]:
        if self._pending is None:
            self._pending = asyncio.create_task(self._receive())
        return self._pending

    def take(self) -> Any:
        """取走已完成的 receive 结果。"""
        task, self._pending = self.pending(), None
        return task.result()

    async def get(self, timeout: float | None = None) -> Any:
        done, _ = await asyncio.wait({self.pending()}, timeout=timeout)
        return self.take() if done else _TIMEOUT

    async def close(self) -> None:
        if self._pending is not None:
            self._pending.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pending
            self._pending = None

    async def _receive(self) -> Any:
        try:
            line = await self._transport.receive()
            if line is None:
                return _EOF
            return decode_session_line(HOST, line)
        except ProtocolError as exc:
            return exc


def _violation(detail: str) -> int:
    _log(f"宿主违反 session 协议：{detail}")
    return EXIT_FAILURE


class _Session:
    def __init__(
        self,
        app: SessionApp,
        outbox: Outbox,
        inbox: _Inbox,
        timing: Timing,
        new_id: Callable[[], str],
        info: SessionInfo,
    ) -> None:
        self.app = app
        self.outbox = outbox
        self.inbox = inbox
        self.timing = timing
        self.new_id = new_id
        self.info = info

    async def run(self) -> int:
        quiesced = False
        while True:
            item = await self.inbox.get()
            if item is _EOF:
                return EXIT_OK
            if isinstance(item, ProtocolError):
                return _violation(f"空闲时收到不合法的消息：{item}")
            typ = item["type"]
            if typ == "task_start":
                quiesced = False
                code = await self._turn(item)
                if code is not None:
                    return code
            elif typ == "quiesce" and not quiesced:
                checkpoint_id = self.info.checkpoint_id or ""  # 尚无会话 checkpoint 时为空串
                await self.outbox.emit({"type": "quiesced", "session_checkpoint_id": checkpoint_id})
                quiesced = True
            elif typ == "session_close":
                return await self._close()
            else:
                return _violation(f"{'静止' if quiesced else '空闲'}时收到 {typ}")

    async def _close(self) -> int:
        await self.outbox.emit({"type": "closed"})
        return EXIT_OK

    async def _emit_error(self, attempt_id: str, failure: WorkerFailure) -> None:
        await self.outbox.emit({**_error_body(failure), "attempt_id": attempt_id})

    async def _turn(self, start: dict[str, Any]) -> int | None:
        """执行一个 attempt 直到释放；返回 None 表示回到空闲，否则为进程退出码。"""
        attempt_id = start["attempt_id"]
        session = SessionInfo(
            self.info.session_id,
            self.info.incarnation_id,
            self.info.checkpoint_id,
            copy.deepcopy(self.info.state),
        )
        try:
            ctx: TaskContext | None = TaskContext(
                start, self.outbox, self.timing, self.new_id, session=session
            )
            failure = None
        except WorkerFailure as exc:  # resume 中的 SDK 保留状态损坏：无法续号，不能安全开始
            ctx, failure = None, exc
        await self.outbox.emit({"type": "task_accepted", "attempt_id": attempt_id})
        if ctx is None:
            assert failure is not None
            await self._emit_error(attempt_id, failure)
            return await self._settle(attempt_id, None, proposed=True)
        return await self._run_app(ctx)

    async def _run_app(self, ctx: TaskContext) -> int | None:
        app_task = asyncio.create_task(self.app(ctx))
        control = _TurnControl(ctx, app_task, self.outbox)
        try:
            while not app_task.done():
                receive = self.inbox.pending()
                await asyncio.wait({app_task, receive}, return_when=asyncio.FIRST_COMPLETED)
                if receive.done():
                    await control.handle(self.inbox.take())
            kind, value = await _await_app(ctx, app_task)
        finally:  # 结构化并发：任何路径离开本 attempt 之前，应用任务都已结束
            if not app_task.done():
                app_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await app_task
        if control.stop == "close":
            return await self._close()
        if control.stop == "eof":
            _log(f"attempt {ctx.attempt_id} 进行中 stdin 已关闭：取消并退出")
            return EXIT_FAILURE
        if control.outcome is not None:  # 宿主已提前裁决（例如取消）：不再发终态提议
            if ctx.control_error is not None:
                return _violation(str(ctx.control_error))
            return await self._release(ctx.attempt_id, control.outcome, None)
        if ctx.control_error is not None:
            await self._emit_error(ctx.attempt_id, value)
            return EXIT_FAILURE
        if kind == "cancelled":  # 宿主 cancel：不发终态提议，等待裁决（不查询）
            return await self._settle(ctx.attempt_id, None, proposed=False)
        proposal = None
        if kind == "failure":
            await self._emit_error(ctx.attempt_id, value)
        else:
            try:
                body = _outcome_body(ctx, value)
                await ctx._emit(body)
                proposal = body.get("session_state")
            except WorkerFailure as exc:
                await self._emit_error(ctx.attempt_id, exc)
        return await self._settle(ctx.attempt_id, proposal, proposed=True)

    async def _settle(
        self, attempt_id: str, proposal: dict[str, Any] | None, *, proposed: bool
    ) -> int | None:
        outcome = await self._await_outcome(attempt_id, query=proposed)
        if isinstance(outcome, int):
            return outcome
        return await self._release(attempt_id, outcome, proposal)

    async def _await_outcome(self, attempt_id: str, *, query: bool) -> dict[str, Any] | int:
        """等待本 attempt 的 task_outcome。已有终态提议时，每次超时发 task_outcome_query，
        至多 max_ack_attempts 次；没有提议（宿主取消）时只等待同样长的时间而不查询
        （提议之前的查询是协议违规）。仍无结果 → 退出 1，由宿主销毁 incarnation。"""
        waited = 0
        while True:
            item = await self.inbox.get(self.timing.ack_timeout)
            if item is _TIMEOUT:
                if waited == self.timing.max_ack_attempts:
                    _log(f"attempt {attempt_id}：{waited} 次查询后仍未收到 task_outcome")
                    return EXIT_FAILURE
                waited += 1
                if query:
                    await self.outbox.emit({"type": "task_outcome_query", "attempt_id": attempt_id})
                continue
            if item is _EOF:
                _log(f"attempt {attempt_id} 等待裁决时 stdin 已关闭")
                return EXIT_FAILURE
            if isinstance(item, ProtocolError):
                return _violation(f"等待裁决时收到不合法的消息：{item}")
            typ = item["type"]
            if typ == "session_close":
                return await self._close()
            if item.get("attempt_id") != attempt_id:
                return _violation(f"等待 {attempt_id} 的裁决时收到 {typ}")
            if typ == "task_outcome":
                return item
            if typ not in ("checkpoint_result", "artifact_result", "cancel", "pause"):
                return _violation(f"等待裁决时收到 {typ}")
            # 迟到的答复或控制消息：attempt 已结束，忽略

    async def _release(
        self, attempt_id: str, outcome: dict[str, Any], proposal: dict[str, Any] | None
    ) -> None:
        committed = outcome.get("committed_session_checkpoint_id") or ""
        if (
            outcome["verdict"] == "succeeded"
            and proposal is not None
            and committed == proposal["checkpoint_id"]
        ):
            self.info = SessionInfo(
                self.info.session_id, self.info.incarnation_id, committed, proposal["state"]
            )
        elif committed and committed != (self.info.checkpoint_id or ""):
            _log(
                f"attempt {attempt_id}：宿主提交的会话 checkpoint {committed} 不是本 Worker 的提议"
            )
        await self.outbox.emit({"type": "task_released", "attempt_id": attempt_id})
        return None


class _TurnControl:
    """attempt 运行期间的宿主消息分发。stop 为 "close" / "eof" 时应用已被取消；
    outcome 为宿主在提议之前发出的裁决（应用已被取消）。"""

    def __init__(self, ctx: TaskContext, app_task: asyncio.Task[Any], outbox: Outbox) -> None:
        self.ctx = ctx
        self.app_task = app_task
        self.outbox = outbox
        self.stop: str | None = None
        self.outcome: dict[str, Any] | None = None

    async def handle(self, item: Any) -> None:
        ctx = self.ctx
        if item is _EOF:
            self._stop("eof")
            return
        if isinstance(item, ProtocolError):
            await self._fail(item)
            return
        typ = item["type"]
        if typ == "session_close":
            self._stop("close")
        elif typ == "task_outcome" and item["attempt_id"] == ctx.attempt_id:
            self.outcome = item
            self._stop(None)
        elif typ in ("checkpoint_result", "artifact_result", "pause", "cancel"):
            try:
                keep_going = ctx._handle_control(item)
            except ProtocolError as exc:
                await self._fail(exc)
                return
            if not keep_going:
                self.app_task.cancel()
        else:
            await self._fail(ProtocolError("control_protocol_error", f"运行中收到 {typ}"))

    def _stop(self, stop: str | None) -> None:
        if stop is not None:
            self.stop = stop
        self.ctx.cancel_reason = self.ctx.cancel_reason or stop or "task_outcome"
        self.app_task.cancel()

    async def _fail(self, exc: ProtocolError) -> None:
        if self.ctx.control_error is None:
            self.ctx.control_error = exc
        # 等在途发送结束再取消：取消落在发送中途会使输出通道失效，error 事件就发不出去
        await self.outbox.cancel_when_idle(self.app_task)


def _session_info(init: dict[str, Any], load_staged: Callable[[str], Any]) -> SessionInfo:
    resume = init.get("session_resume")
    if resume is None:
        return SessionInfo(init["session_id"], init["incarnation_id"], None, None)
    if resume.get("state") is not None:
        state = resume["state"]
    else:
        path = resume["staged_state_path"]
        try:
            state = load_staged(path)
        except Exception as exc:  # 文件缺失、不可读、不是 JSON，或自定义读取器的任何失败
            message = f"无法读取暂存的会话状态 {path}：{type(exc).__name__}: {exc}"
            raise WorkerFailure("invalid_field", message) from exc
    return SessionInfo(
        init["session_id"], init["incarnation_id"], resume["checkpoint_id"], _snapshot(state)
    )


async def run_session_worker(
    app: SessionApp,
    transport: Transport,
    *,
    name: str,
    version: str,
    capabilities: Iterable[str] = (),
    timing: Timing = Timing(),  # noqa: B008  Timing 不可变
    new_id: Callable[[], str] | None = None,
    load_staged: Callable[[str], Any] | None = None,
) -> int:
    """运行一个 session 模式 Worker，返回进程退出码。

    load_staged 读取 session_resume.staged_state_path，缺省为 read_staged_state；失败时在
    ready 之前发出 error{code: invalid_field} 并返回 1。协议输出通道失效时返回 1。
    """
    try:
        return await _run_session(
            app,
            transport,
            name,
            version,
            capabilities,
            timing,
            new_id or (lambda: uuid.uuid4().hex),
            load_staged or read_staged_state,
        )
    except TransportBroken as exc:
        _log(f"协议输出通道失效：{exc}")
        return EXIT_FAILURE


async def _run_session(
    app: SessionApp,
    transport: Transport,
    name: str,
    version: str,
    capabilities: Iterable[str],
    timing: Timing,
    new_id: Callable[[], str],
    load_staged: Callable[[str], Any],
) -> int:
    outbox = Outbox(transport, session=True)
    init = await _read_init(transport, outbox, MODE_SESSION)
    if isinstance(init, int):
        return init
    try:
        info = _session_info(init, load_staged)
    except WorkerFailure as exc:  # 启动失败：不属于任何 attempt
        await outbox.emit(_error_body(exc))
        return EXIT_FAILURE
    await outbox.emit(
        {
            "type": "ready",
            "protocol_version": VERSION,
            "mode": MODE_SESSION,
            "worker": {"name": name, "version": version},
            "capabilities": list(capabilities),
            "session_ext": SESSION_EXT_VERSION,
        }
    )
    inbox = _Inbox(transport)
    try:
        return await _Session(app, outbox, inbox, timing, new_id, info).run()
    finally:
        await inbox.close()


def main_session(
    app: SessionApp, *, name: str, version: str, capabilities: Iterable[str] = ()
) -> NoReturn:
    """session 模式的进程入口；输出收尾与退出策略同 runtime.main。"""
    _run_main(
        lambda transport: run_session_worker(
            app, transport, name=name, version=version, capabilities=capabilities
        )
    )
