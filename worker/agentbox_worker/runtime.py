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
from typing import Any, NoReturn

from agentbox_worker.errors import (
    ArtifactRejected,
    CheckpointRejected,
    CheckpointUnresolved,
    TransportBroken,
    WorkerFailure,
)
from agentbox_worker.gateway import DEFAULT_SOCKET_PATH, CallIds, GatewayClient
from agentbox_worker.outbox import Outbox, Waiters
from agentbox_worker.protocol import (
    BOOTSTRAP_VERSION,
    HOST,
    VERSION,
    ProtocolError,
    decode_line,
    parse_json,
    valid_artifact_path,
)
from agentbox_worker.transport import StdioTransport, Transport

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


@dataclass(frozen=True)
class Result:
    summary: str
    outputs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Paused:
    checkpoint_id: str


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
    """应用看到的任务上下文。"""

    def __init__(
        self, init: dict[str, Any], outbox: Outbox, timing: Timing, new_id: Callable[[], str]
    ) -> None:
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

    @property
    def gateway(self) -> GatewayClient:
        """Gateway 客户端，首次访问时创建；与 checkpoint 共用同一个 call id 计数器。

        客户端方法是同步阻塞的，应用应经 asyncio.to_thread 调用。
        """
        if self._gateway is None:
            path = os.environ.get(GATEWAY_SOCKET_ENV) or DEFAULT_SOCKET_PATH
            self._gateway = GatewayClient(path, call_ids=self._call_ids)
        return self._gateway

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
        """
        async with self._checkpoint_lock:
            checkpoint_id = self._new_id()
            body: dict[str, Any] = {
                "type": "checkpoint",
                "checkpoint_id": checkpoint_id,
                "scope": "task",
                "step_id": step_id,
            }
            call_ids = self._call_ids.snapshot()
            if state_ref is None:
                body["state"] = _snapshot(_with_call_ids(state, call_ids))
            elif call_ids:
                raise WorkerFailure(
                    "invalid_field",
                    "已发起 Gateway 调用后不能用 state_ref 提交（call id 计数器无处保存）",
                )
            else:
                body["state_ref"] = state_ref
            body["refs"] = list(refs)
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

    async def _emit(self, body: dict[str, Any]) -> None:
        try:
            await self._outbox.emit(body)
        except ProtocolError as exc:
            raise WorkerFailure(exc.code, exc.detail) from exc

    def _handle_control(self, msg: dict[str, Any]) -> bool:
        """处理一条宿主控制消息；返回 False 表示收到 cancel，控制循环结束。"""
        typ = msg["type"]
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


async def _read_init(transport: Transport, outbox: Outbox) -> dict[str, Any] | int:
    """读取并校验 init；无法开始时返回退出码。"""
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
        _log(f"init 不合法：{exc}")
        return EXIT_FAILURE
    if init["type"] != "init":
        _log(f"第一条消息必须是 init，收到 {init['type']}")
        return EXIT_FAILURE
    if init["mode"] != "task":
        await outbox.emit(
            {
                "type": "error",
                "code": "unsupported_mode",
                "message": f"不支持的模式：{init['mode']}",
                "retryable": False,
            }
        )
        return EXIT_FAILURE
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
    await outbox.emit(
        {
            "type": "ready",
            "protocol_version": VERSION,
            "mode": "task",
            "worker": {"name": name, "version": version},
            "capabilities": list(capabilities),
        }
    )
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
        outcome = await app_task
    except asyncio.CancelledError:
        if ctx.control_error is not None:
            failure = WorkerFailure("control_protocol_error", str(ctx.control_error))
            return await _emit_failure(ctx, failure)
        if ctx.cancel_reason is None:
            raise
        return EXIT_OK  # 宿主取消：不发终态提议，由宿主按其意图裁决（规格 §5.8）
    except WorkerFailure as exc:
        return await _emit_failure(ctx, exc)
    except Exception as exc:
        failure = WorkerFailure("internal_error", f"{type(exc).__name__}: {exc}", retryable=True)
        return await _emit_failure(ctx, failure)
    finally:
        control_task.cancel()
    if ctx.control_error is not None:  # 控制错误发生在应用最后一次发送期间：同样以错误结束
        failure = WorkerFailure("control_protocol_error", str(ctx.control_error))
        return await _emit_failure(ctx, failure)
    return await _emit_outcome(ctx, outcome)


async def _emit_outcome(ctx: TaskContext, outcome: Any) -> int:
    try:
        if isinstance(outcome, Paused):
            await ctx._emit({"type": "paused", "checkpoint_id": outcome.checkpoint_id})
            return EXIT_OK
        if isinstance(outcome, Result):
            body = {"type": "result", "summary": outcome.summary, "outputs": list(outcome.outputs)}
            await ctx._emit(body)
            return EXIT_OK
    except WorkerFailure as exc:
        return await _emit_failure(ctx, exc)
    failure = WorkerFailure("internal_error", f"应用返回了未知结果：{outcome!r}", retryable=True)
    return await _emit_failure(ctx, failure)


def _safe_text(value: object, limit: int) -> str:
    """转为字符串、替换孤立代理项并按 UTF-8 字节截断，保证 error 事件一定可以编码。"""
    return str(value).encode("utf-8", "replace")[:limit].decode("utf-8", "ignore")


async def _emit_failure(ctx: TaskContext, failure: WorkerFailure) -> int:
    # 应用可能传入非字符串的 code/message 或非布尔的 retryable：一律规范化后再发送
    await ctx._outbox.emit(
        {
            "type": "error",
            "code": _safe_text(failure.code, MAX_ERROR_CODE_BYTES),
            "message": _safe_text(failure.message, MAX_ERROR_MESSAGE_BYTES),
            "retryable": bool(failure.retryable),
        }
    )
    return EXIT_FAILURE


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
    protocol_out = sys.stdout.buffer
    sys.stdout = sys.stderr
    transport = StdioTransport(sys.stdin.buffer, protocol_out)
    code = EXIT_FAILURE
    try:
        code = asyncio.run(
            run_worker(app, transport, name=name, version=version, capabilities=capabilities)
        )
    except BaseException:  # noqa: B036  进程入口：任何异常都必须走有界退出
        traceback.print_exc()
    finally:
        try:
            if not transport.close(_shutdown_timeout()):
                _log("退出时仍有未写出的协议输出（宿主可能已停止读取 stdout）")
            sys.stderr.flush()
        finally:  # 收尾本身失败（如 stderr 已关闭）也必须以既定退出码退出
            os._exit(code)
