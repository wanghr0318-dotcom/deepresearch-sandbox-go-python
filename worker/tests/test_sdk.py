"""Worker SDK：传输与事件发送、运行时、SDK 层场景回放与 sim-worker（规格 §5.3–§5.9）。"""

import asyncio
import io
import json
import threading

import pytest

from agentbox_worker.errors import TransportBroken
from agentbox_worker.outbox import Outbox, Waiters
from agentbox_worker.protocol import ProtocolError
from agentbox_worker.transport import MAX_FRAME_BYTES, MemoryTransport, StdioTransport

# ---- 传输与事件发送 ----


def sent_json(transport: MemoryTransport) -> list[dict]:
    return [json.loads(line) for line in transport.sent]


def test_outbox_assigns_increasing_seq_version_and_ts():
    async def go():
        transport = MemoryTransport()
        outbox = Outbox(transport)
        await outbox.emit({"type": "progress", "kind": "step_started", "message": "a"})
        await outbox.emit({"type": "progress", "kind": "step_finished", "message": "b"})
        return transport, outbox.last_seq

    transport, last_seq = asyncio.run(go())
    events = sent_json(transport)
    assert [e["seq"] for e in events] == [1, 2]
    assert all(e["v"] == 1 and isinstance(e["ts"], str) for e in events)
    assert last_seq == 2


def test_outbox_invalid_event_does_not_consume_seq():
    async def go():
        transport = MemoryTransport()
        outbox = Outbox(transport)
        with pytest.raises(ProtocolError) as exc:
            await outbox.emit({"type": "progress", "message": "缺少 kind"})
        assert exc.value.code == "missing_field"
        await outbox.emit({"type": "progress", "kind": "step_started", "message": "ok"})
        return transport

    assert [e["seq"] for e in sent_json(asyncio.run(go()))] == [1]


def test_waiters_drop_late_and_unexpected_replies():
    async def go():
        waiters = Waiters()
        future = waiters.expect("cp-1")
        assert waiters.deliver("cp-2", {"x": 2}) is False
        assert waiters.deliver("cp-1", {"x": 1}) is True
        assert waiters.deliver("cp-1", {"x": 3}) is False
        return await future

    assert asyncio.run(go()) == {"x": 1}


def test_stdio_transport_round_trip():
    out = io.BytesIO()

    async def go():
        transport = StdioTransport(io.BytesIO(b"first\nsecond\r\n"), out)
        await transport.send(b'{"a":1}')
        await transport.send(b'{"b":2}')
        return [await transport.receive() for _ in range(3)]

    assert asyncio.run(go()) == [b"first", b"second", None]
    assert out.getvalue() == b'{"a":1}\n{"b":2}\n'


class BlockingTransport:
    """send 在 release 之前一直阻塞，用于确定性地模拟"写入中"。"""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.release = asyncio.Event()

    async def receive(self) -> bytes | None:
        return None

    async def send(self, line: bytes) -> None:
        self.sent.append(line)
        await self.release.wait()


class FailingTransport:
    def __init__(self) -> None:
        self.attempts = 0

    async def receive(self) -> bytes | None:
        return None

    async def send(self, line: bytes) -> None:
        self.attempts += 1
        raise OSError("broken pipe")


def test_cancel_during_send_breaks_outbox_without_reusing_seq():
    async def go():
        transport = BlockingTransport()
        outbox = Outbox(transport)
        task = asyncio.create_task(outbox.emit({"type": "progress", "kind": "k", "message": "a"}))
        while not transport.sent:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(TransportBroken):
            await outbox.emit({"type": "progress", "kind": "k", "message": "b"})
        return transport, outbox.last_seq

    transport, last_seq = asyncio.run(go())
    assert last_seq == 1
    assert [json.loads(line)["seq"] for line in transport.sent] == [1]


def test_send_failure_breaks_outbox():
    async def go():
        transport = FailingTransport()
        outbox = Outbox(transport)
        for _ in range(2):
            with pytest.raises(TransportBroken):
                await outbox.emit({"type": "progress", "kind": "k", "message": "a"})
        return transport.attempts

    assert asyncio.run(go()) == 1


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        pytest.param(MAX_FRAME_BYTES - 1, "accepted", id="at_limit"),
        pytest.param(MAX_FRAME_BYTES + 10, "message_too_large", id="over_limit"),
    ],
)
def test_stdio_frame_limit_is_checked_at_read_time(size, expected):
    line = b"a" * size

    async def go():
        transport = StdioTransport(io.BytesIO(line + b"\n"), io.BytesIO())
        try:
            got = await transport.receive()
        except ProtocolError as exc:
            return exc.code, await transport.receive()
        return ("accepted" if got == line else "wrong_line"), await transport.receive()

    assert asyncio.run(go()) == (expected, None)


class TrackingReader:
    """包装 BytesIO：记录 readline 调用次数，第 notify_at 次调用读完时触发 reached。"""

    def __init__(self, data: bytes, notify_at: int) -> None:
        self._inner = io.BytesIO(data)
        self._notify_at = notify_at
        self.calls = 0
        self.reached = threading.Event()

    def readline(self, size: int = -1) -> bytes:
        line = self._inner.readline(size)
        self.calls += 1
        if self.calls >= self._notify_at:
            self.reached.set()
        return line

    def tell(self) -> int:
        return self._inner.tell()


LINES = [f"line-{i}".encode() for i in range(100)]
QUEUE_SIZE = 2
# 取走第一行后，读线程最多再读 QUEUE_SIZE + 1 行：QUEUE_SIZE 行在队列中，1 行等待入队。
# 第 READ_LIMIT 次 readline 只能发生在第一行被取走之后；此后无人消费，读线程必然阻塞在入队上。
READ_LIMIT = QUEUE_SIZE + 2
READ_LIMIT_BYTES = sum(len(line) + 1 for line in LINES[:READ_LIMIT])


def tracking_input() -> TrackingReader:
    return TrackingReader(b"\n".join(LINES) + b"\n", notify_at=READ_LIMIT)


def test_stdio_reader_applies_backpressure():
    reader = tracking_input()

    async def go():
        transport = StdioTransport(reader, io.BytesIO(), queue_size=QUEUE_SIZE)
        first = await transport.receive()
        assert await asyncio.to_thread(reader.reached.wait, 5)
        snapshot = (reader.calls, reader.tell(), transport.pending_lines)
        rest = [await transport.receive() for _ in range(len(LINES))]
        return first, snapshot, rest

    first, snapshot, rest = asyncio.run(go())
    assert snapshot == (READ_LIMIT, READ_LIMIT_BYTES, QUEUE_SIZE)
    assert [first, *rest] == [*LINES, None]


def test_stdio_reader_stops_consuming_after_event_loop_closes():
    reader = tracking_input()
    transport = StdioTransport(reader, io.BytesIO(), queue_size=QUEUE_SIZE)

    async def go():
        first = await transport.receive()
        assert await asyncio.to_thread(reader.reached.wait, 5)
        return first

    assert asyncio.run(go()) == LINES[0]
    assert transport.wait_reader_stopped(5)
    assert (reader.calls, reader.tell()) == (READ_LIMIT, READ_LIMIT_BYTES)


# ---- 运行时：握手、控制消息、checkpoint、产物 ----
# 正常路径、查询、重发、冲突、暂停、取消与恢复由 SDK 层场景逐条回放（见下一节），此处不重复。

import hashlib
import itertools
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agentbox_worker import Result, TaskContext, Timing, WorkerFailure, run_worker
from agentbox_worker.runtime import MAX_ERROR_MESSAGE_BYTES, SHUTDOWN_TIMEOUT_ENV, _shutdown_timeout

FAST = Timing(ack_timeout=0.05, max_ack_attempts=3, retry_backoff=0.01, artifact_timeout=0.5)
INIT = {
    "type": "init",
    "bootstrap": 1,
    "protocol_versions": [1],
    "mode": "task",
    "task_id": "t-1",
    "attempt_id": "a-1",
    "attempt_no": 1,
}

Responder = Callable[[dict[str, Any], MemoryTransport], None]


def run(
    app,
    tmp_path: Path,
    *,
    host_lines: list[bytes | None] = (),
    responder: Responder | None = None,
    init: dict[str, Any] | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    async def go():
        transport = MemoryTransport()
        transport.feed(json.dumps({**INIT, "out_dir": str(tmp_path), **(init or {})}).encode())
        for line in host_lines:
            transport.feed(line)
        if responder is not None:
            transport.on_send = lambda line: responder(json.loads(line), transport)
        counter = itertools.count(1)
        code = await asyncio.wait_for(
            run_worker(
                app,
                transport,
                name="test-worker",
                version="0",
                timing=FAST,
                new_id=lambda: f"cp-{next(counter)}",
            ),
            timeout=5,
        )
        return code, sent_json(transport)

    return asyncio.run(go())


def reply(transport: MemoryTransport, message: dict[str, Any]) -> None:
    transport.feed(json.dumps({"v": 1, **message}).encode())


def checkpoint_reply(status: str) -> Responder:
    def respond(msg: dict[str, Any], transport: MemoryTransport) -> None:
        if msg["type"] == "checkpoint":
            reply(
                transport,
                {
                    "type": "checkpoint_result",
                    "checkpoint_id": msg["checkpoint_id"],
                    "scope": "task",
                    "status": status,
                },
            )

    return respond


def types(events: list[dict[str, Any]]) -> list[str]:
    return [e["type"] for e in events]


async def returns_ok(ctx: TaskContext) -> Result:
    return Result("ok", [])


async def sleeps(ctx: TaskContext) -> Result:
    await asyncio.sleep(10)
    return Result("never", [])


def test_ready_checkpoint_artifact_result(tmp_path):
    content = "# 报告".encode() + b"\n"
    (tmp_path / "report.md").write_bytes(content)

    def respond(msg, transport):
        checkpoint_reply("committed")(msg, transport)
        if msg["type"] == "artifact":
            reply(
                transport,
                {
                    "type": "artifact_result",
                    "artifact_id": msg["artifact_id"],
                    "status": "saved",
                    "version": 2,
                    "sha256": msg["declared_sha256"],
                },
            )

    async def app(ctx):
        checkpoint_id = await ctx.checkpoint("s1", state={"n": 1})
        ref = await ctx.register_artifact("report", "report.md", media_type="text/markdown")
        return Result(f"{checkpoint_id}/v{ref.version}", [ref.artifact_id])

    code, events = run(app, tmp_path, responder=respond)
    assert code == 0
    assert types(events) == ["ready", "checkpoint", "artifact", "result"]
    ready, checkpoint, artifact, result = events
    assert ready["worker"] == {"name": "test-worker", "version": "0"}
    assert checkpoint["checkpoint_id"] == "cp-1" and checkpoint["state"] == {"n": 1}
    assert checkpoint["refs"] == [] and "state_ref" not in checkpoint
    assert artifact["declared_sha256"] == hashlib.sha256(content).hexdigest()
    assert artifact["declared_size"] == len(content) and artifact["visibility"] == "output"
    assert result["summary"] == "cp-1/v2" and result["outputs"] == ["report"]


def test_unsupported_mode_fails_before_ready(tmp_path):
    code, events = run(returns_ok, tmp_path, init={"mode": "session"})
    assert code == 1
    assert types(events) == ["error"]
    assert events[0]["code"] == "unsupported_mode" and events[0]["retryable"] is False


@pytest.mark.parametrize(
    ("raised", "code", "retryable"),
    [
        pytest.param(
            WorkerFailure("bad_input", "输入缺少字段"), "bad_input", False, id="worker_failure"
        ),
        pytest.param(KeyError("boom"), "internal_error", True, id="unexpected_exception"),
    ],
)
def test_app_failure_becomes_error_event(tmp_path, raised, code, retryable):
    async def app(ctx):
        raise raised

    exit_code, events = run(app, tmp_path)
    assert exit_code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == code and events[1]["retryable"] is retryable


def test_stdin_eof_is_treated_as_cancel(tmp_path):
    code, events = run(sleeps, tmp_path, host_lines=[None])
    assert code == 0
    assert types(events) == ["ready"]


def test_invalid_control_message_fails_task(tmp_path):
    code, events = run(sleeps, tmp_path, host_lines=[b'{"type":"bogus","v":1}'])
    assert code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == "control_protocol_error"


class HoldsProgress(MemoryTransport):
    """progress 的发送要等宿主的第二条消息（第一条是 init）被读出之后才完成。"""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.received = 0

    async def receive(self) -> bytes | None:
        line = await super().receive()
        self.received += 1
        if self.received == 2:
            asyncio.get_running_loop().call_soon(self.release.set)
        return line

    async def send(self, line: bytes) -> None:
        await super().send(line)
        if json.loads(line)["type"] == "progress":
            await self.release.wait()


async def progress_then_sleep(ctx: TaskContext) -> Result:
    await ctx.progress("step_started", "x")
    await asyncio.sleep(10)
    return Result("never", [])


async def progress_then_return(ctx: TaskContext) -> Result:
    await ctx.progress("step_started", "x")
    return Result("done", [])


@pytest.mark.parametrize("app", [progress_then_sleep, progress_then_return])
def test_control_error_waits_for_inflight_send(tmp_path, app):
    async def go():
        transport = HoldsProgress()
        transport.feed(json.dumps({**INIT, "out_dir": str(tmp_path)}).encode())
        transport.feed(b'{"type":"bogus","v":1}')
        code = await asyncio.wait_for(
            run_worker(app, transport, name="w", version="0", timing=FAST), timeout=5
        )
        return code, sent_json(transport)

    code, events = asyncio.run(go())
    assert code == 1
    assert types(events) == ["ready", "progress", "error"]
    assert events[-1]["code"] == "control_protocol_error"


@pytest.mark.parametrize(
    ("raised", "message"),
    [
        pytest.param(
            WorkerFailure("bad_input", "x" * (1 << 20)),
            "x" * MAX_ERROR_MESSAGE_BYTES,
            id="oversized_message",
        ),
        pytest.param(ValueError("坏文件名 \udcff"), "ValueError: 坏文件名 ?", id="lone_surrogate"),
        pytest.param(WorkerFailure("bad_input", 42, retryable=1), "42", id="non_string_fields"),
    ],
)
def test_failure_message_is_made_encodable(tmp_path, raised, message):
    async def app(ctx):
        raise raised

    code, events = run(app, tmp_path)
    assert code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["message"] == message


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("2.5", 2.5), ("0", 0.0), ("inf", 5.0), ("nan", 5.0), ("-1", 5.0), ("1e300", 5.0), ("x", 5.0)],
)
def test_shutdown_timeout_falls_back_on_unusable_values(monkeypatch, raw, expected):
    monkeypatch.setenv(SHUTDOWN_TIMEOUT_ENV, raw)
    assert _shutdown_timeout() == expected


def test_only_one_checkpoint_in_flight(tmp_path):
    in_flight = 0
    overlaps: list[str] = []

    def respond(msg, transport):
        nonlocal in_flight
        if msg["type"] != "checkpoint":
            return
        if in_flight:
            overlaps.append(msg["checkpoint_id"])
        in_flight += 1

        def deliver():
            nonlocal in_flight
            in_flight -= 1
            checkpoint_reply("committed")(msg, transport)

        asyncio.get_running_loop().call_later(0.02, deliver)

    async def app(ctx):
        async with asyncio.TaskGroup() as group:
            group.create_task(ctx.checkpoint("s1", state={}))
            group.create_task(ctx.checkpoint("s2", state={}))
        return Result("ok", [])

    code, events = run(app, tmp_path, responder=respond)
    assert code == 0 and overlaps == []
    assert [e["checkpoint_id"] for e in events if e["type"] == "checkpoint"] == ["cp-1", "cp-2"]


def test_unresolved_after_max_attempts(tmp_path):
    async def app(ctx):
        await ctx.checkpoint("s1", state={})
        return Result("never", [])

    code, events = run(app, tmp_path)
    assert code == 1
    assert types(events).count("checkpoint_query") == FAST.max_ack_attempts
    assert events[-1]["code"] == "checkpoint_unresolved" and events[-1]["retryable"] is True


def test_reply_to_last_query_is_awaited(tmp_path):
    queries = 0

    def respond(msg, transport):
        nonlocal queries
        if msg["type"] != "checkpoint_query":
            return
        queries += 1
        if queries == FAST.max_ack_attempts:
            reply(
                transport,
                {
                    "type": "checkpoint_result",
                    "checkpoint_id": msg["checkpoint_id"],
                    "scope": "task",
                    "status": "committed",
                },
            )

    async def app(ctx):
        return Result(await ctx.checkpoint("s1", state={}), [])

    code, events = run(app, tmp_path, responder=respond)
    assert code == 0
    assert types(events).count("checkpoint_query") == FAST.max_ack_attempts
    assert events[-1]["type"] == "result"


@pytest.mark.parametrize(
    ("state", "code"),
    [
        pytest.param("x" * (256 << 10), "state_too_large", id="oversized"),
        pytest.param({"x": object()}, "invalid_field", id="unserializable"),
    ],
)
def test_bad_state_fails_without_sending(tmp_path, state, code):
    async def app(ctx):
        await ctx.checkpoint("s1", state=state)
        return Result("never", [])

    exit_code, events = run(app, tmp_path)
    assert exit_code == 1
    assert types(events) == ["ready", "error"]
    assert events[1]["code"] == code


def test_checkpoint_retries_use_snapshot_of_state(tmp_path):
    state = {"n": 1}
    replies = iter(["retryable_error", "committed"])

    def respond(msg, transport):
        if msg["type"] == "checkpoint":
            state["n"] = 99  # 提交后修改调用方对象，不得影响同一 checkpoint 的重试内容
            checkpoint_reply(next(replies))(msg, transport)

    async def app(ctx):
        return Result(await ctx.checkpoint("s1", state=state), [])

    code, events = run(app, tmp_path, responder=respond)
    sent = [e for e in events if e["type"] == "checkpoint"]
    assert code == 0
    assert [e["state"] for e in sent] == [{"n": 1}, {"n": 1}]


@pytest.mark.parametrize(
    ("path", "code", "sent"),
    [
        pytest.param(
            "x.txt", "artifact_rejected", ["ready", "artifact", "error"], id="host_rejects"
        ),
        pytest.param("../in/x", "path_invalid", ["ready", "error"], id="invalid_path_local"),
    ],
)
def test_artifact_failure_fails_task(tmp_path, path, code, sent):
    (tmp_path / "x.txt").write_bytes(b"x")

    def respond(msg, transport):
        if msg["type"] == "artifact":
            reply(
                transport,
                {
                    "type": "artifact_result",
                    "artifact_id": msg["artifact_id"],
                    "status": "rejected",
                    "code": "hash_mismatch",
                },
            )

    async def app(ctx):
        await ctx.register_artifact("x", path, media_type="text/plain")
        return Result("never", [])

    exit_code, events = run(app, tmp_path, responder=respond)
    assert exit_code == 1
    assert types(events) == sent and events[-1]["code"] == code


class BreaksAfterReady(MemoryTransport):
    async def send(self, line: bytes) -> None:
        if self.sent:
            raise OSError("broken pipe")
        await super().send(line)


def test_broken_transport_ends_worker_with_failure(tmp_path):
    async def app(ctx):
        await ctx.progress("step_started", "x")
        return Result("never", [])

    async def go():
        transport = BreaksAfterReady()
        transport.feed(json.dumps({**INIT, "out_dir": str(tmp_path)}).encode())
        code = await asyncio.wait_for(
            run_worker(app, transport, name="w", version="0", timing=FAST), timeout=5
        )
        return code, types(sent_json(transport))

    assert asyncio.run(go()) == (1, ["ready"])


# ---- SDK 层场景回放与 sim-worker ----
# 用 SDK 驱动 sim-worker 逐条复现 layers 含 "sdk" 的场景：宿主行按顺序送入内存传输；每当
# Worker 发出一行，就与场景中下一条 Worker 行比较（忽略 ts），再送入其后连续的宿主行。
# init.out_dir 替换为临时目录。

from protocol_fixtures import load_scenarios

from agentbox_worker.stream import StreamChecker
from sim_worker import NAME, VERSION
from sim_worker.app import run as sim_app

SCENARIO_TIMING = Timing(
    ack_timeout=0.05, max_ack_attempts=5, retry_backoff=0.01, artifact_timeout=1.0
)
SDK_SCENARIOS = [s for s in load_scenarios() if "sdk" in s["layers"]]
assert len(SDK_SCENARIOS) >= 9, "SDK 层场景缺失"


def replay_sdk(scenario: dict[str, Any], out_dir: str) -> tuple[int, list[str], int]:
    lines = scenario["lines"]
    problems: list[str] = []
    cursor = 0

    async def go() -> int:
        nonlocal cursor
        transport = MemoryTransport()

        def pump() -> None:
            nonlocal cursor
            while cursor < len(lines) and lines[cursor]["from"] == "host":
                message = dict(lines[cursor]["message"])
                if message.get("type") == "init":
                    message["out_dir"] = out_dir
                transport.feed(json.dumps(message, ensure_ascii=False).encode())
                cursor += 1

        def on_send(raw: bytes) -> None:
            nonlocal cursor
            got = json.loads(raw)
            got.pop("ts", None)
            if cursor >= len(lines) or lines[cursor]["from"] != "worker":
                problems.append(f"多出的 Worker 消息：{got}")
                return
            want = lines[cursor]["message"]
            if got != want:
                problems.append(f"第 {cursor} 行不一致：期望 {want}，得到 {got}")
            cursor += 1
            pump()

        transport.on_send = on_send
        pump()
        counter = itertools.count(1)
        code = await asyncio.wait_for(
            run_worker(
                sim_app,
                transport,
                name=NAME,
                version=VERSION,
                timing=SCENARIO_TIMING,
                new_id=lambda: f"cp-{next(counter)}",
            ),
            timeout=10,
        )
        checker = StreamChecker()
        for raw in transport.sent:
            checker.observe(json.loads(raw))
        return code

    code = asyncio.run(go())
    return code, problems, cursor


@pytest.mark.parametrize("scenario", SDK_SCENARIOS, ids=lambda s: s["name"])
def test_sdk_reproduces_scenario(scenario, tmp_path):
    code, problems, cursor = replay_sdk(scenario, str(tmp_path))
    assert problems == []
    assert cursor == len(scenario["lines"]), "场景中仍有未出现的行"
    assert code == scenario["sdk"]["exit_code"]


@pytest.mark.parametrize(
    ("steps", "config", "code", "last"),
    [
        pytest.param(
            [{"op": "fail", "code": "sim_failure", "message": "x", "retryable": True}],
            {},
            1,
            {"code": "sim_failure", "retryable": True},
            id="fail",
        ),
        pytest.param(
            [{"op": "allocate_mb", "mb": 4}],
            {"summary": "分配完成"},
            0,
            {"summary": "分配完成"},
            id="allocate_mb",
        ),
        pytest.param([{"op": "teleport"}], {}, 1, {"code": "sim_bad_config"}, id="unknown_op"),
        pytest.param([], {}, 0, {"summary": "done", "outputs": []}, id="defaults"),
    ],
)
def test_sim_worker_ops(tmp_path, steps, config, code, last):
    exit_code, events = run(sim_app, tmp_path, init={"config": {"steps": steps, **config}})
    assert exit_code == code
    assert events[-1] == {**events[-1], **last}
