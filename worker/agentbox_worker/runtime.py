"""Worker SDK 运行时：握手、事件发送、控制消息处理、checkpoint 与产物登记。

只提供运行协议与执行能力；研究策略、提示词等业务逻辑不在 SDK 中。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

from agentbox_worker.errors import (
    ArtifactRejected,
    CheckpointRejected,
    CheckpointUnresolved,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.gateway import DEFAULT_SOCKET_PATH, CallIds, GatewayClient, SubrunGateway
from agentbox_worker.outbox import Outbox, Waiters
from agentbox_worker.protocol import (
    BOOTSTRAP_VERSION,
    HOST,
    MODE_SESSION,
    MODE_TASK,
    SUBRUNS_EXT_VERSION,
    VERSION,
    ProtocolError,
    decode_line,
    parse_json,
    requests_subruns,
    valid_artifact_path,
)
from agentbox_worker.subruns import SubrunManager, parse_resumed
from agentbox_worker.transport import StdioTransport, Transport

if TYPE_CHECKING:
    from agentbox_worker.session import SessionInfo

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_HANDSHAKE = 2

SHUTDOWN_TIMEOUT_ENV = "AGENTBOX_WORKER_SHUTDOWN_TIMEOUT"
DEFAULT_SHUTDOWN_TIMEOUT = 5.0
MAX_SHUTDOWN_TIMEOUT = 3600.0  # 超过线程等待的可表示范围会抛异常，取一个足够大的上限

# error 事件中 code 与 message 的上限（UTF-8 字节），保证事件远小于 1 MiB 的事件上限
MAX_ERROR_CODE_BYTES = 256
MAX_ERROR_MESSAGE_BYTES = 8 << 10

# Gateway socket 路径的覆盖（测试与沙箱外演练用）；缺省为沙箱内的挂载点
GATEWAY_SOCKET_ENV = "AGENTBOX_GATEWAY_SOCKET"
# checkpoint state 中由 SDK 保留的键：{"_agentbox": {"call_ids": {...}}}，应用看不到也不能写
RESERVED_STATE_KEY = "_agentbox"
# result.session_state 的上限（紧凑 JSON 的 UTF-8 字节）：低于协议的 256 KiB，留出余量
MAX_SESSION_STATE_BYTES = 192 << 10


@dataclass(frozen=True)
class Result:
    summary: str
    outputs: list[str] = field(default_factory=list)
    # 新的会话状态提议（规格 §12.3）；仅 session 模式写入 result，task 模式忽略。None = 不提议
    session_state: Any = None


@dataclass(frozen=True)
class Paused:
    checkpoint_id: str
    # {"question_id": str}：等待用户回答（协调者裁定 A），以 awaiting_input 终态提议发出；
    # 仅 session 模式可用，task 模式下给出时任务以 invalid_field 失败
    awaiting_input: dict[str, Any] | None = None


@dataclass(frozen=True)
class ResumeInfo:
    checkpoint_id: str
    step_id: str
    state: Any
    state_ref: str | None
    refs: list[str]


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    version: int
    sha256: str


@dataclass(frozen=True)
class Timing:
    ack_timeout: float = 10.0
    max_ack_attempts: int = 5
    retry_backoff: float = 1.0
    artifact_timeout: float = 300.0


App = Callable[["TaskContext"], Awaitable["Result | Paused"]]


def _log(message: str) -> None:
    print(f"agentbox_worker: {message}", file=sys.stderr)


def _resume_info(raw: dict[str, Any] | None) -> tuple[ResumeInfo | None, CallIds]:
    """解析 init.resume；从 state 中取出 SDK 保留键（call id 计数器），应用只看到其余部分。"""
    if raw is None:
        return None, CallIds()
    state = raw.get("state")
    call_ids = CallIds()
    if isinstance(state, dict) and RESERVED_STATE_KEY in state:
        state = dict(state)
        reserved = state.pop(RESERVED_STATE_KEY)
        saved = reserved.get("call_ids") if isinstance(reserved, dict) else None
        try:
            call_ids = CallIds.restore(saved if isinstance(saved, dict) else {})
        except ValueError as exc:
            raise WorkerFailure(
                "invalid_field", f"checkpoint 中的 call id 计数器不合法：{exc}"
            ) from exc
    info = ResumeInfo(
        checkpoint_id=raw["checkpoint_id"],
        step_id=raw["step_id"],
        state=state,
        state_ref=raw.get("state_ref") or None,
        refs=list(raw.get("refs") or []),
    )
    return info, call_ids


def _with_call_ids(state: Any, call_ids: dict[str, int]) -> Any:
    """把 call id 计数器并入 state 快照的保留键；尚未发起任何 Gateway 调用时 state 原样不变。"""
    if isinstance(state, dict) and RESERVED_STATE_KEY in state:
        raise WorkerFailure(
            "invalid_field", f"checkpoint state 不能使用保留键 {RESERVED_STATE_KEY}"
        )
    if not call_ids:
        return state
    if not isinstance(state, dict):
        raise WorkerFailure(
            "invalid_field",
            "已发起 Gateway 调用后 checkpoint state 必须是 JSON 对象（以保存 call id 计数器）",
        )
    return {**state, RESERVED_STATE_KEY: {"call_ids": call_ids}}


def _snapshot(state: Any) -> Any:
    """生成与调用方对象无关的状态快照；同一 checkpoint 的所有重试都使用它（规格 §5.5）。

    快照经 JSON 往返归一化：tuple 变为 list，非字符串键变为字符串，NaN 与 Infinity 被拒绝。
    """
    try:
        return json.loads(json.dumps(state, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, RecursionError) as exc:
        raise WorkerFailure("invalid_field", f"checkpoint state 无法序列化为 JSON：{exc}") from exc


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


class TaskContext:
    """应用看到的任务上下文。

    task 模式由 init 构造；session 模式由 task_start 构造并带 session（该 attempt 开始时
    已提交的会话状态）。session 模式下 directive、carryover、restored_from_task_id、
    base_session_checkpoint_id 原样取自 task_start（缺省为 None），发出的 task 相关事件
    自动带 attempt_id；task 模式下这些字段均为 None。
    """

    def __init__(
        self,
        init: dict[str, Any],
        outbox: Outbox,
        timing: Timing,
        new_id: Callable[[], str],
        *,
        session: SessionInfo | None = None,
        subruns: bool | None = None,
    ) -> None:
        """subruns：是否已协商 sub-run 扩展；None 时取 init.extensions（task 模式）。
        session 模式由会话 init 决定，经此参数传入（task_start 不携带 extensions）。"""
        self.session = session
        start = init if session is not None else {}
        # {"kind": "finish_now"} | {"kind": "answer", "question_id", "answers"}；None = 开始或继续
        self.directive: dict[str, Any] | None = start.get("directive")
        # {"task_id", "checkpoint_ref"}：被取代 turn 最新 task checkpoint 的 blob（协调者裁定 D）
        self.carryover: dict[str, Any] | None = start.get("carryover")
        # 仅用于展示：恢复即从 resume（宿主复制的种子 checkpoint）继续（协调者裁定 C）
        self.restored_from_task_id: str | None = start.get("restored_from_task_id") or None
        self.base_session_checkpoint_id: str | None = (
            start.get("base_session_checkpoint_id") or None
        )
        self.task_id: str = init["task_id"]
        self.attempt_id: str = init["attempt_id"]
        self.attempt_no: int = init["attempt_no"]
        self.config: Any = init.get("config")
        self.out_dir = Path(init["out_dir"])
        self.resume, self._call_ids = _resume_info(init.get("resume"))
        limits = init.get("budget_limits")
        self.budget_limits: dict[str, int] = dict(limits) if isinstance(limits, dict) else {}
        self._gateway: GatewayClient | None = None
        self.cancel_reason: str | None = None
        self.control_error: ProtocolError | None = None
        self._outbox = outbox
        self._timing = timing
        self._new_id = new_id
        self._checkpoint_results = Waiters()
        self._artifact_results = Waiters()
        self._checkpoint_lock = asyncio.Lock()
        self._pause_requested = False
        negotiated = requests_subruns(init.get("extensions")) if subruns is None else subruns
        # 本 attempt（session 模式下即本 turn）的 sub-run 状态所有者
        self.subruns = SubrunManager(
            negotiated=negotiated,
            resumed=parse_resumed((init.get("resume") or {}).get("subruns")),
            emit=self._emit,
            cancel_task=outbox.cancel_when_idle,
            gateway_factory=lambda subrun_id: SubrunGateway.of(self.gateway, subrun_id),
            ack_timeout=timing.ack_timeout,
            max_ack_attempts=timing.max_ack_attempts,
            retry_backoff=timing.retry_backoff,
        )

    @property
    def gateway(self) -> GatewayClient:
        """Gateway 客户端，首次访问时创建；与 checkpoint 共用同一个 call id 计数器。

        客户端方法是同步阻塞的，应用应经 asyncio.to_thread 调用。
        """
        if self._gateway is None:
            path = os.environ.get(GATEWAY_SOCKET_ENV) or DEFAULT_SOCKET_PATH
            self._gateway = GatewayClient(path, call_ids=self._call_ids)
        return self._gateway

    @property
    def call_ids(self) -> CallIds:
        """本 attempt 的 call id 计数器（resume 时已从快照续号）；Gateway 客户端与 checkpoint
        共用它，测试中注入的替身 Gateway 也应使用它。"""
        return self._call_ids

    def should_pause(self) -> bool:
        """宿主是否请求了暂停。应用在提交边界检查它（规格 §5.9）。"""
        return self._pause_requested

    async def progress(
        self, kind: str, message: str, *, step_id: str | None = None, data: Any = None
    ) -> None:
        body: dict[str, Any] = {"type": "progress"}
        if step_id is not None:
            body["step_id"] = step_id
        body["kind"] = kind
        body["message"] = message
        if data is not None:
            body["data"] = data
        await self._emit(body)

    async def checkpoint(
        self,
        step_id: str,
        *,
        state: Any = None,
        state_ref: str | None = None,
        refs: Iterable[str] = (),
    ) -> str:
        """提交 checkpoint，宿主确认已提交后返回 checkpoint_id（规格 §5.5）。

        同一时刻至多一个在途提交；结果丢失时用同一 ID 查询，
        retryable_error 或 not_found 时用同一 ID 重发。state 在首次提交前按 JSON 归一化
        生成快照（tuple 变为 list，非字符串键变为字符串），无法序列化或含 NaN、Infinity 时
        抛出 WorkerFailure("invalid_field")；之后修改调用方对象不影响重试内容。

        已发起过 Gateway 调用时，SDK 把 call id 计数器并入 state 的保留键 _agentbox.call_ids
        （state 须为对象，不能用 state_ref），恢复时取出并续号；应用的 resume.state 不含该键。

        存在 sub-run 时自动附加 subruns=self.subruns.snapshot()（与 state 同一时刻生成）。

        state、call id 计数器与 subruns[] 都在调用时（等待在途提交之前、无 await）一并取快照：
        排队期间 sub-run 完成或发起新调用都不进入本 checkpoint，否则宿主会看到 state 里仍在进行的
        sub-run 已 completed，或计数器越过 state 里没有的调用（恢复后以新 id 重发）。锁按 FIFO
        交接，快照顺序即提交顺序。
        """
        snap: dict[str, Any] = {"scope": "task", "step_id": step_id}
        call_ids = self._call_ids.snapshot()
        if state_ref is None:
            snap["state"] = _snapshot(_with_call_ids(state, call_ids))
        elif call_ids:
            raise WorkerFailure(
                "invalid_field",
                "已发起 Gateway 调用后不能用 state_ref 提交（call id 计数器无处保存）",
            )
        else:
            snap["state_ref"] = state_ref
        snap["refs"] = list(refs)
        subruns = self.subruns.snapshot()
        if subruns:
            snap["subruns"] = subruns
        async with self._checkpoint_lock:
            checkpoint_id = self._new_id()
            body: dict[str, Any] = {"type": "checkpoint", "checkpoint_id": checkpoint_id, **snap}
            status, code = await self._submit_checkpoint(checkpoint_id, body)
        if status == "committed":
            return checkpoint_id
        detail = f"checkpoint {checkpoint_id}: {status}" + (f" ({code})" if code else "")
        error_code = "checkpoint_conflict" if status == "conflict" else "checkpoint_rejected"
        raise CheckpointRejected(error_code, detail)

    async def _submit_checkpoint(
        self, checkpoint_id: str, body: dict[str, Any]
    ) -> tuple[str, str | None]:
        query = {"type": "checkpoint_query", "checkpoint_id": checkpoint_id, "scope": "task"}
        limit = self._timing.max_ack_attempts
        retries = 0  # 查询与重发共用上限；每次发送之后都等待一次结果
        pending = self._checkpoint_results.expect(checkpoint_id)
        await self._emit(body)
        while True:
            try:
                result = await asyncio.wait_for(asyncio.shield(pending), self._timing.ack_timeout)
            except TimeoutError:
                if retries == limit:
                    break
                retries += 1
                await self._emit(query)
                continue
            if result["status"] not in ("retryable_error", "not_found"):
                return result["status"], result.get("code")
            if retries == limit:
                break
            retries += 1
            await asyncio.sleep(self._timing.retry_backoff)
            pending = self._checkpoint_results.expect(checkpoint_id)
            await self._emit(body)
        raise CheckpointUnresolved(
            f"checkpoint {checkpoint_id}: {limit} 次查询或重发后仍无确定结果"
        )

    async def register_artifact(
        self, artifact_id: str, path: str, *, media_type: str, visibility: str = "output"
    ) -> ArtifactRef:
        """登记 out_dir 下的产物；宿主保存并校验后返回其版本（规格 §5.6）。

        哈希在线程池中计算，不能保证有限时间内完成：path 若是 FIFO 或读操作阻塞，线程会一直
        等待且无法取消。任务被取消后 asyncio.run 退出时仍会等待该线程，进程停在那里，尚未进入
        main() 的输出收尾；最终强制终止由宿主负责。
        """
        if not valid_artifact_path(path):
            raise WorkerFailure("path_invalid", f"产物路径不合法：{path!r}")
        sha256, size = await asyncio.to_thread(_hash_file, self.out_dir / path)
        pending = self._artifact_results.expect(artifact_id)
        await self._emit(
            {
                "type": "artifact",
                "artifact_id": artifact_id,
                "path": path,
                "declared_sha256": sha256,
                "declared_size": size,
                "media_type": media_type,
                "visibility": visibility,
            }
        )
        try:
            result = await asyncio.wait_for(pending, self._timing.artifact_timeout)
        except TimeoutError as exc:
            message = f"artifact {artifact_id}: 等待结果超时"
            raise WorkerFailure("artifact_unresolved", message, retryable=True) from exc
        if result["status"] == "saved":
            return ArtifactRef(artifact_id, result["version"], result["sha256"])
        raise ArtifactRejected(f"artifact {artifact_id}: rejected ({result.get('code', '')})")

    def _tag(self, body: dict[str, Any]) -> dict[str, Any]:
        """session 模式下为 task 相关事件补上 attempt_id。"""
        return body if self.session is None else {**body, "attempt_id": self.attempt_id}

    async def _emit(self, body: dict[str, Any]) -> None:
        try:
            await self._outbox.emit(self._tag(body))
        except ProtocolError as exc:
            raise WorkerFailure(exc.code, exc.detail) from exc

    def _handle_control(self, msg: dict[str, Any]) -> bool:
        """处理一条宿主控制消息；返回 False 表示收到 cancel，控制循环结束。

        session 模式下控制消息的 attempt_id 必须是本 attempt，否则为 control_protocol_error。
        """
        typ = msg["type"]
        if typ in ("subrun_started", "subrun_cancel_requested"):
            # 不带 attempt_id：针对本 attempt 中已 subrun_start 的 sub-run
            self.subruns.handle_control(msg)
            return True
        if self.session is not None and msg.get("attempt_id") != self.attempt_id:
            raise ProtocolError(
                "control_protocol_error",
                f"收到 attempt_id={msg.get('attempt_id')!r} 的 {typ}，当前为 {self.attempt_id}",
            )
        if typ == "checkpoint_result":
            self._checkpoint_results.deliver(msg["checkpoint_id"], msg)
        elif typ == "artifact_result":
            self._artifact_results.deliver(msg["artifact_id"], msg)
        elif typ == "pause":
            self._pause_requested = True
        elif typ == "cancel":
            self.cancel_reason = msg.get("reason") or "cancel"
            return False
        else:
            raise ProtocolError("control_protocol_error", f"运行中收到 {typ}")
        return True


def _supports_protocol(line: bytes) -> bool:
    """只读引导信封判断是否有共同版本（规格 §5.2）；信封本身不合法时交由完整校验报错。"""
    try:  # 与 decode_line 使用同一套 JSON 结构限制
        envelope = parse_json(line)
    except ProtocolError:
        return True
    if not isinstance(envelope, dict) or envelope.get("type") != "init":
        return True
    if envelope.get("bootstrap") != BOOTSTRAP_VERSION:
        return True
    versions = envelope.get("protocol_versions")
    return not isinstance(versions, list) or VERSION in versions


def _peek_mode(line: bytes) -> Any:
    try:
        envelope = parse_json(line)
    except ProtocolError:
        return None
    return envelope.get("mode") if isinstance(envelope, dict) else None


async def _unsupported_mode(outbox: Outbox, mode: Any) -> int:
    await outbox.emit(
        {
            "type": "error",
            "code": "unsupported_mode",
            "message": f"不支持的模式：{mode}",
            "retryable": False,
        }
    )
    return EXIT_FAILURE


async def _read_init(
    transport: Transport, outbox: Outbox, mode: str = MODE_TASK
) -> dict[str, Any] | int:
    """读取并校验 init；无法开始时返回退出码。

    init 按 task 模式编解码器校验（它同样校验 session 分支）；mode 不是本 Worker 的模式时
    发出 unsupported_mode——即使另一模式的字段不完整，也先报告模式不受支持。
    """
    try:
        line = await transport.receive()
    except ProtocolError as exc:  # 例如输入帧超长
        _log(f"读取 init 失败：{exc}")
        return EXIT_FAILURE
    if line is None:
        _log("未收到 init：stdin 已关闭")
        return EXIT_FAILURE
    if not _supports_protocol(line):
        await outbox.send_handshake_error("no_common_version")
        return EXIT_HANDSHAKE
    try:
        init = decode_line(HOST, line)
    except ProtocolError as exc:
        peeked = _peek_mode(line)
        if peeked in (MODE_TASK, MODE_SESSION) and peeked != mode:
            return await _unsupported_mode(outbox, peeked)
        _log(f"init 不合法：{exc}")
        return EXIT_FAILURE
    if init["type"] != "init":
        _log(f"第一条消息必须是 init，收到 {init['type']}")
        return EXIT_FAILURE
    if init["mode"] != mode:
        return await _unsupported_mode(outbox, init["mode"])
    return init


async def run_worker(
    app: App,
    transport: Transport,
    *,
    name: str,
    version: str,
    capabilities: Iterable[str] = (),
    timing: Timing = Timing(),  # noqa: B008  Timing 不可变
    new_id: Callable[[], str] | None = None,
) -> int:
    """运行一个 task 模式 Worker，返回进程退出码。

    协议输出通道失效（TransportBroken）时无法再发出任何事件，直接以 EXIT_FAILURE 结束。
    """
    try:
        return await _run_worker(app, transport, name, version, capabilities, timing, new_id)
    except TransportBroken as exc:
        _log(f"协议输出通道失效：{exc}")
        return EXIT_FAILURE


async def _run_worker(
    app: App,
    transport: Transport,
    name: str,
    version: str,
    capabilities: Iterable[str],
    timing: Timing,
    new_id: Callable[[], str] | None,
) -> int:
    outbox = Outbox(transport)
    init = await _read_init(transport, outbox)
    if isinstance(init, int):
        return init
    try:
        ctx = TaskContext(init, outbox, timing, new_id or (lambda: uuid.uuid4().hex))
    except WorkerFailure as exc:  # resume 中的 SDK 保留状态损坏：无法续号，不能安全开始
        await outbox.emit(
            {"type": "error", "code": exc.code, "message": exc.message, "retryable": False}
        )
        return EXIT_FAILURE
    ready: dict[str, Any] = {
        "type": "ready",
        "protocol_version": VERSION,
        "mode": "task",
        "worker": {"name": name, "version": version},
        "capabilities": list(capabilities),
    }
    if requests_subruns(init.get("extensions")):
        ready["subruns"] = SUBRUNS_EXT_VERSION  # 扩展确认：当且仅当宿主请求（规格 §5.2）
    await outbox.emit(ready)
    return await _run_task(app, ctx, transport)


async def _control_loop(ctx: TaskContext, transport: Transport, app_task: asyncio.Task) -> None:
    while True:
        try:
            line = await transport.receive()
            if line is None:
                ctx.cancel_reason = "stdin_closed"
                app_task.cancel()
                return
            keep_going = ctx._handle_control(decode_line(HOST, line))
        except ProtocolError as exc:
            ctx.control_error = exc
            # 等在途发送结束再取消：取消落在发送中途会使输出通道失效，error 事件就发不出去
            await ctx._outbox.cancel_when_idle(app_task)
            return
        if not keep_going:
            app_task.cancel()
            return


async def _run_task(app: App, ctx: TaskContext, transport: Transport) -> int:
    app_task = asyncio.create_task(app(ctx))
    control_task = asyncio.create_task(_control_loop(ctx, transport, app_task))
    try:
        kind, value = await _await_app(ctx, app_task)
        # 取消引起的 subrun_end{cancelled} 须先于终态提议发出；控制循环此时仍在运行
        await ctx.subruns.settle(abandon=kind == "cancelled" or ctx.control_error is not None)
    finally:
        control_task.cancel()
    if kind == "cancelled":
        return EXIT_OK  # 宿主取消：不发终态提议，由宿主按其意图裁决（规格 §5.8）
    if kind == "failure":
        return await _emit_failure(ctx, value)
    return await _emit_outcome(ctx, value)


async def _await_app(ctx: TaskContext, app_task: asyncio.Task[Any]) -> tuple[str, Any]:
    """等待应用结束并归类（task 与 session 模式共用）：

    ("outcome", 应用返回值) | ("failure", WorkerFailure) | ("cancelled", None)（宿主取消）。
    控制协议错误（含发生在应用最后一次发送期间的）归为 control_protocol_error 失败；
    不是由宿主取消或控制错误引起的取消原样向上传播。
    """
    try:
        outcome = await app_task
    except asyncio.CancelledError:
        if ctx.control_error is not None:
            return "failure", WorkerFailure("control_protocol_error", str(ctx.control_error))
        if ctx.cancel_reason is None:
            raise
        return "cancelled", None
    except WorkerFailure as exc:
        return "failure", exc
    except Exception as exc:
        failure = WorkerFailure("internal_error", f"{type(exc).__name__}: {exc}", retryable=True)
        return "failure", failure
    if ctx.control_error is not None:  # 控制错误发生在应用最后一次发送期间：同样以错误结束
        return "failure", WorkerFailure("control_protocol_error", str(ctx.control_error))
    return "outcome", outcome


def _session_state_snapshot(state: Any) -> Any:
    snap = _snapshot(state)
    size = len(json.dumps(snap, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    if size > MAX_SESSION_STATE_BYTES:
        raise WorkerFailure(
            "state_too_large", f"session_state {size} 字节，上限 {MAX_SESSION_STATE_BYTES}"
        )
    return snap


def _outcome_body(ctx: TaskContext, outcome: Any) -> dict[str, Any]:
    """把应用返回值转换为终态提议的事件体；不合法时抛出 WorkerFailure。

    session 模式下 Result.session_state 归一化为快照并以新的 checkpoint_id 提议；
    Paused.awaiting_input 发出 awaiting_input 提议（协调者裁定 A）。
    """
    if isinstance(outcome, Paused):
        if outcome.awaiting_input is None:
            return {"type": "paused", "checkpoint_id": outcome.checkpoint_id}
        if ctx.session is None:
            raise WorkerFailure("invalid_field", "awaiting_input 只能用于 session 模式")
        ask = outcome.awaiting_input
        question_id = ask.get("question_id") if isinstance(ask, dict) else None
        if not isinstance(question_id, str) or not question_id:
            raise WorkerFailure("invalid_field", "awaiting_input.question_id 必须是非空字符串")
        return {
            "type": "awaiting_input",
            "checkpoint_id": outcome.checkpoint_id,
            "question_id": question_id,
        }
    if isinstance(outcome, Result):
        body = {"type": "result", "summary": outcome.summary, "outputs": list(outcome.outputs)}
        subruns = ctx.subruns.result_entries()
        if subruns:
            body["subruns"] = subruns
        if ctx.session is not None and outcome.session_state is not None:
            body["session_state"] = {
                "checkpoint_id": ctx._new_id(),
                "state": _session_state_snapshot(outcome.session_state),
            }
        return body
    raise WorkerFailure("internal_error", f"应用返回了未知结果：{outcome!r}", retryable=True)


async def _emit_outcome(ctx: TaskContext, outcome: Any) -> int:
    try:
        await ctx._emit(_outcome_body(ctx, outcome))
    except WorkerFailure as exc:
        return await _emit_failure(ctx, exc)
    return EXIT_OK


def _safe_text(value: object, limit: int) -> str:
    """转为字符串、替换孤立代理项并按 UTF-8 字节截断，保证 error 事件一定可以编码。"""
    return str(value).encode("utf-8", "replace")[:limit].decode("utf-8", "ignore")


async def _emit_failure(ctx: TaskContext, failure: WorkerFailure) -> int:
    # 应用可能传入非字符串的 code/message 或非布尔的 retryable：一律规范化后再发送
    await ctx._outbox.emit(ctx._tag(_error_body(failure)))
    return EXIT_FAILURE


def _error_body(failure: WorkerFailure) -> dict[str, Any]:
    return {
        "type": "error",
        "code": _safe_text(failure.code, MAX_ERROR_CODE_BYTES),
        "message": _safe_text(failure.message, MAX_ERROR_MESSAGE_BYTES),
        "retryable": bool(failure.retryable),
    }


def _shutdown_timeout() -> float:
    """读取收尾期限；非数值、负数、NaN 或超过上限时使用默认值。"""
    try:
        value = float(os.environ.get(SHUTDOWN_TIMEOUT_ENV, DEFAULT_SHUTDOWN_TIMEOUT))
    except ValueError:
        return DEFAULT_SHUTDOWN_TIMEOUT
    if 0 <= value <= MAX_SHUTDOWN_TIMEOUT:  # NaN 的比较为假
        return value
    return DEFAULT_SHUTDOWN_TIMEOUT


def main(app: App, *, name: str, version: str, capabilities: Iterable[str] = ()) -> NoReturn:
    """进程入口。stdin/stdout 承载协议；应用的 print 被改到 stderr，避免混入协议流。

    这是 SDK 中唯一调用 os._exit 的地方，退出策略固定为：运行 Worker（任何异常都记录并按
    失败处理）→ 在期限内等待已提交的协议输出写完 → os._exit。不走正常的解释器收尾：读线程
    可能阻塞在 stdin 的 readline 中，写线程可能因宿主停止读取 stdout 而阻塞在 write 中，
    二者都无法取消或 join；正常收尾会挂起，或以 "Fatal Python error: _enter_buffered_busy"
    崩溃。

    期限由环境变量 AGENTBOX_WORKER_SHUTDOWN_TIMEOUT 设置（秒，默认 5），只约束协议输出的
    收尾，不是整个 Worker 的退出期限：asyncio.run 返回前会等待线程池中的线程（例如
    register_artifact 的哈希线程），这段等待不受该期限约束。最终强制终止由宿主负责。
    """
    _run_main(
        lambda transport: run_worker(
            app, transport, name=name, version=version, capabilities=capabilities
        )
    )


def _run_main(start: Callable[[Transport], Awaitable[int]]) -> NoReturn:
    """main 与 session.main_session 共用的进程入口（退出策略见 main 的说明）。"""
    protocol_out = sys.stdout.buffer
    sys.stdout = sys.stderr
    transport = StdioTransport(sys.stdin.buffer, protocol_out)
    code = EXIT_FAILURE
    try:
        code = asyncio.run(start(transport))
    except BaseException:  # noqa: B036  进程入口：任何异常都必须走有界退出
        traceback.print_exc()
    finally:
        try:
            if not transport.close(_shutdown_timeout()):
                _log("退出时仍有未写出的协议输出（宿主可能已停止读取 stdout）")
            sys.stderr.flush()
        finally:  # 收尾本身失败（如 stderr 已关闭）也必须以既定退出码退出
            os._exit(code)
