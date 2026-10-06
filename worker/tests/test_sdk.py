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
NESTED_5000: object = 0  # 超过 Python 递归上限，json.dumps 无法序列化
for _ in range(5000):
    NESTED_5000 = [NESTED_5000]
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


def test_deeply_nested_init_fails_without_crashing(tmp_path):
    async def go():
        transport = MemoryTransport()
        head = json.dumps({**INIT, "out_dir": str(tmp_path)})[:-1]
        transport.feed(f'{head},"config":{"[" * 100_000}{"]" * 100_000}}}'.encode())
        code = await run_worker(returns_ok, transport, name="w", version="0", timing=FAST)
        return code, sent_json(transport)

    assert asyncio.run(go()) == (1, [])


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
        pytest.param(NESTED_5000, "invalid_field", id="too_deep_to_serialize"),
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


def test_sim_worker_exec_log_records_each_executed_step(tmp_path):
    # 恢复时只记录从 next_index 起实际执行的步骤，最后记录 result；未设置 exec_log 时不写文件。
    log = tmp_path / "exec.log"
    steps = [{"op": "progress", "message": "a"}, {"op": "progress", "message": "b"}]
    resume = {"checkpoint_id": "cp-0", "step_id": "s0", "state": {"next_index": 1}}
    for attempt_no, init_resume in ((1, None), (2, resume)):
        init = {
            "attempt_id": f"a-{attempt_no}",
            "attempt_no": attempt_no,
            "resume": init_resume,
            "config": {"steps": steps, "exec_log": str(log)},
        }
        code, _ = run(sim_app, tmp_path, init=init)
        assert code == 0
    records = [json.loads(line) for line in log.read_text().splitlines()]
    assert [(r["attempt_no"], r["index"], r["op"]) for r in records] == [
        (1, 0, "progress"),
        (1, 1, "progress"),
        (1, 2, "result"),
        (2, 1, "progress"),
        (2, 2, "result"),
    ]
    assert all(r["task_id"] == "t-1" and r["pid"] > 0 for r in records)


# ---- Gateway 客户端（规格 §9.3、§9.4；契约见 Plan 7 Task 4）----
# 依赖 Unix socket 的测试使用 conftest 的 fake_gateway；没有 AF_UNIX 的平台跳过（见 conftest）。

import ast
import sys
import time
import tomllib

from conftest import FakeGateway, Reply, error_reply

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    CallDeadlineExceeded,
    CallDivergence,
    CallInProgress,
    GatewayError,
)
from agentbox_worker.gateway import CallIds, GatewayClient
from agentbox_worker.runtime import GATEWAY_SOCKET_ENV

WORKER_ROOT = Path(__file__).resolve().parent.parent


def client(gw: FakeGateway, call_ids: CallIds | None = None, **attrs: float) -> GatewayClient:
    c = GatewayClient(gw.socket_path, call_ids=call_ids or CallIds(), timeout_s=5)
    c.in_progress_backoff_s = 0.01
    for name, value in attrs.items():
        setattr(c, name, value)
    return c


def test_call_ids_format_sequence_and_restore():
    ids = CallIds()
    assert [ids.next("s1", "search") for _ in range(3)] == [
        "root/s1/search/1",
        "root/s1/search/2",
        "root/s1/search/3",
    ]
    assert ids.next("s1", "chat") == "root/s1/chat/1"  # 每个 step 内每个 kind 独立计数
    assert ids.next("s2", "search") == "root/s2/search/1"
    assert ids.next("s1", "search", subrun_id="sr-7") == "sr-7/s1/search/1"
    snap = ids.snapshot()
    assert snap == {
        "root/s1/search": 3,
        "root/s1/chat": 1,
        "root/s2/search": 1,
        "sr-7/s1/search": 1,
    }
    restored = CallIds.restore(json.loads(json.dumps(snap)))  # 经 JSON 往返，如同 checkpoint
    assert restored.next("s1", "search") == "root/s1/search/4"  # 续号，不重复、不回退
    assert restored.next("s9", "fetch") == "root/s9/fetch/1"
    assert ids.snapshot() == snap  # 快照与原计数器互不影响
    with pytest.raises(ValueError):
        ids.next("x" * 300, "chat")  # > 256 字节
    assert "root/" + "x" * 300 + "/chat" not in ids.snapshot()  # 失败不占号
    for bad in ({"root/s/chat": -1}, {"root/s/chat": "1"}, {"root/s/chat": True}):
        with pytest.raises(ValueError):
            CallIds.restore(bad)


def test_runtime_dependencies_stay_empty():
    pyproject = tomllib.loads((WORKER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["dependencies"] == []
    for path in (WORKER_ROOT / "agentbox_worker").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            for name in names:
                top = name.split(".")[0]
                assert top in sys.stdlib_module_names or top == "agentbox_worker", (path, name)


def test_three_endpoints_send_call_id_and_body(fake_gateway):
    gw = client(fake_gateway)
    messages = [{"role": "user", "content": "你好"}]
    chat = gw.chat("plan", messages, max_tokens=64, temperature=0.2)
    search = gw.search("task-1", "agent sandbox", max_results=3)
    fetch = gw.fetch("task-1", "https://example.com/a")
    chat2 = gw.chat("plan", messages)
    got = [
        (r.method, r.path, r.headers.get("x-agentbox-call-id"), r.json())
        for r in fake_gateway.requests
    ]
    assert got == [
        (
            "POST",
            "/v1/chat/completions",
            "root/plan/chat/1",
            {"messages": messages, "max_tokens": 64, "temperature": 0.2},
        ),
        (
            "POST",
            "/v1/search",
            "root/task-1/search/1",
            {"query": "agent sandbox", "max_results": 3},
        ),
        ("POST", "/v1/fetch", "root/task-1/fetch/1", {"url": "https://example.com/a"}),
        ("POST", "/v1/chat/completions", "root/plan/chat/2", {"messages": messages}),
    ]
    for r in fake_gateway.requests:
        assert r.headers["content-type"] == "application/json"
        assert not {"x-agentbox-retry", "x-agentbox-supersedes", "x-agentbox-subrun"} & set(
            r.headers
        )
    for res, kind in ((chat, "chat"), (search, "search"), (fetch, "fetch"), (chat2, "chat")):
        assert res.status == 200 and res.replayed is False
        assert res.blob_sha256 == fake_gateway.calls[res.call_id]
        assert res.body["kind"] == kind and res.body["call_id"] == res.call_id


def test_chat_model_is_written_only_when_given(fake_gateway):
    gw = client(fake_gateway)
    messages = [{"role": "user", "content": "x"}]
    gw.chat("plan", messages, model="kimi-k3", max_tokens=8)
    gw.chat("task-1", messages, model=None)
    bodies = [r.json() for r in fake_gateway.requests]
    assert bodies == [
        {"messages": messages, "model": "kimi-k3", "max_tokens": 8},
        {"messages": messages},
    ]


def test_retry_replay_and_supersede_headers(fake_gateway):
    gw = client(fake_gateway)
    first = gw.search("s1", "q")
    again = gw.retry("s1", "search", first.call_id, {"query": "q", "max_results": 5})
    assert again.replayed is True and again.call_id == first.call_id
    assert again.blob_sha256 == first.blob_sha256 and again.body == first.body
    assert fake_gateway.requests[1].headers["x-agentbox-retry"] == "true"
    new = gw.supersede(
        "s1", "search", first.call_id, "divergence", {"query": "q2", "max_results": 5}
    )
    assert new.call_id == "root/s1/search/2" and new.replayed is False
    headers = fake_gateway.requests[2].headers
    assert headers["x-agentbox-call-id"] == "root/s1/search/2"
    assert headers["x-agentbox-supersedes"] == first.call_id
    assert headers["x-agentbox-supersede-reason"] == "divergence"
    assert "x-agentbox-retry" not in headers


def test_no_cache_sends_cache_directive_only_when_asked(fake_gateway):
    gw = client(fake_gateway)
    gw.search("s1", "q", no_cache=True)
    gw.fetch("s1", "https://example.com/a", no_cache=True)
    gw.search("s1", "q")
    gw.fetch("s1", "https://example.com/a")
    gw.retry("s1", "fetch", "root/s1/fetch/1", {"url": "https://example.com/a"}, no_cache=True)
    got = [r.headers.get("x-agentbox-cache") for r in fake_gateway.requests]
    assert got == ["no-cache", "no-cache", None, None, "no-cache"]
    assert fake_gateway.requests[4].headers["x-agentbox-retry"] == "true"
    # 指令不改变请求体与调用 ID 序列
    assert [r.json() for r in fake_gateway.requests[:2]] == [
        {"query": "q", "max_results": 5},
        {"url": "https://example.com/a"},
    ]
    assert [r.headers["x-agentbox-call-id"] for r in fake_gateway.requests[:4]] == [
        "root/s1/search/1",
        "root/s1/fetch/1",
        "root/s1/search/2",
        "root/s1/fetch/2",
    ]


@pytest.mark.parametrize(
    ("reply", "exc_type", "code"),
    [
        (error_reply(402, "budget_exhausted"), BudgetExhausted, "budget_exhausted"),
        (
            error_reply(402, "budget_insufficient_for_request"),
            BudgetExhausted,
            "budget_insufficient_for_request",
        ),
        (error_reply(409, "fingerprint_mismatch"), CallDivergence, "fingerprint_mismatch"),
        (error_reply(403, "access_revoked"), AccessRevoked, "access_revoked"),
        (
            error_reply(504, "call_deadline_exceeded"),
            CallDeadlineExceeded,
            "call_deadline_exceeded",
        ),
        (error_reply(409, "cancel_requested"), GatewayError, "cancel_requested"),
        (error_reply(429, "tries_exhausted"), GatewayError, "tries_exhausted"),
        (error_reply(501, "not_implemented"), GatewayError, "not_implemented"),
        (Reply(500, b"boom"), GatewayError, "http_500"),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_error_responses_map_to_typed_exceptions(fake_gateway, reply, exc_type, code):
    fake_gateway.replies.append(reply)
    with pytest.raises(GatewayError) as caught:
        client(fake_gateway).chat("s1", [{"role": "user", "content": "x"}])
    assert type(caught.value) is exc_type
    assert (caught.value.status, caught.value.code) == (reply.status, code)
    assert len(fake_gateway.requests) == 1  # 只有 call_in_progress 在客户端重试


def test_call_in_progress_is_retried_with_same_id(fake_gateway):
    fake_gateway.replies += [error_reply(409, "call_in_progress")] * 2
    res = client(fake_gateway).fetch("s1", "https://example.com")
    assert res.status == 200 and res.call_id == "root/s1/fetch/1"
    ids = [r.headers["x-agentbox-call-id"] for r in fake_gateway.requests]
    assert ids == ["root/s1/fetch/1"] * 3


def test_call_in_progress_wait_is_bounded(fake_gateway):
    fake_gateway.replies += [error_reply(409, "call_in_progress")] * 50
    gw = client(fake_gateway, in_progress_wait_s=0.1, in_progress_backoff_s=0.02)
    started = time.monotonic()
    with pytest.raises(CallInProgress):
        gw.fetch("s1", "https://example.com")
    assert time.monotonic() - started < 1
    assert 2 <= len(fake_gateway.requests) < 10


def test_client_timeout_is_call_deadline_exceeded(fake_gateway):
    fake_gateway.delay = 5
    gw = client(fake_gateway)
    gw.timeout_s = 0.2
    started = time.monotonic()
    with pytest.raises(CallDeadlineExceeded) as caught:
        gw.chat("s1", [{"role": "user", "content": "x"}])
    assert caught.value.code == "client_timeout" and caught.value.status == 0
    assert time.monotonic() - started < 2


def test_default_timeout_exceeds_gateway_deadline(monkeypatch):
    # Gateway 期限：模型调用默认 300 s，搜索与抓取 120 s；
    # 客户端超时须长于最长者，由 Gateway 先给出 504
    monkeypatch.delenv("AGENTBOX_GATEWAY_TIMEOUT_S", raising=False)
    assert GatewayClient(call_ids=CallIds()).timeout_s == 330.0


def test_timeout_override(monkeypatch):
    # 宿主调高 --model-call-deadline 时经 --worker-env 设置 AGENTBOX_GATEWAY_TIMEOUT_S；
    # 构造参数优先于它
    monkeypatch.setenv("AGENTBOX_GATEWAY_TIMEOUT_S", "630")
    assert GatewayClient(call_ids=CallIds()).timeout_s == 630.0
    assert GatewayClient(call_ids=CallIds(), timeout_s=5).timeout_s == 5
    for bad in ("0", "-1", "abc", "nan", "inf", "1e300"):
        monkeypatch.setenv("AGENTBOX_GATEWAY_TIMEOUT_S", bad)
        with pytest.raises(ValueError, match="AGENTBOX_GATEWAY_TIMEOUT_S"):
            GatewayClient(call_ids=CallIds())


def test_read_blob_and_budget(fake_gateway):
    gw = client(fake_gateway)
    sha = fake_gateway.put_blob("证据正文".encode())
    assert gw.read_blob(sha) == "证据正文".encode()
    assert gw.budget() == fake_gateway.budget
    with pytest.raises(GatewayError) as caught:
        gw.read_blob("0" * 64)
    assert (caught.value.status, caught.value.code) == (404, "not_found")
    fake_gateway.replies.append(Reply(200, b"tampered"))
    with pytest.raises(GatewayError) as caught:
        gw.read_blob(sha)
    assert caught.value.code == "blob_corrupt"
    with pytest.raises(ValueError):
        gw.read_blob("../etc/passwd")
    # 只读端点不携带 call id
    assert all("x-agentbox-call-id" not in r.headers for r in fake_gateway.requests)


def test_missing_socket_is_access_revoked(fake_gateway):
    gw = GatewayClient(fake_gateway.socket_path + ".gone", call_ids=CallIds(), timeout_s=1)
    with pytest.raises(AccessRevoked) as caught:
        gw.budget()
    assert caught.value.code == "connection_refused"


# ---- TaskContext：gateway、budget_limits、call id 计数器随 checkpoint 持久化 ----


def test_context_budget_limits_and_lazy_gateway(tmp_path, monkeypatch):
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, "/tmp/x.sock")
    seen = {}

    async def app(ctx):
        seen["before"] = ctx._gateway
        seen["gateway"] = ctx.gateway
        seen["same"] = ctx.gateway is seen["gateway"]
        seen["limits"] = ctx.budget_limits
        return Result("ok", [])

    code, _ = run(app, tmp_path, init={"budget_limits": {"budget_micro": 5000}})
    assert code == 0
    assert seen["before"] is None and seen["same"] is True
    assert seen["gateway"].socket_path == "/tmp/x.sock"
    assert seen["limits"] == {"budget_micro": 5000}
    monkeypatch.delenv(GATEWAY_SOCKET_ENV)

    async def default_path(ctx):
        return Result(f"{ctx.gateway.socket_path}|{ctx.budget_limits}", [])

    code, events = run(default_path, tmp_path)
    assert events[-1]["summary"] == "/run/agentbox/gateway.sock|{}"


def test_checkpoint_persists_call_ids_under_reserved_key(tmp_path):
    async def app(ctx):
        await ctx.checkpoint("s0", state={"a": 1})  # 尚无调用：state 原样
        ctx.gateway.call_ids.next("s1", "chat")
        ctx.gateway.call_ids.next("s1", "chat")
        state = {"a": 2}
        await ctx.checkpoint("s1", state=state)
        assert state == {"a": 2}  # 应用对象不被修改
        return Result("ok", [])

    code, events = run(app, tmp_path, responder=checkpoint_reply("committed"))
    assert code == 0
    states = [e["state"] for e in events if e["type"] == "checkpoint"]
    assert states == [{"a": 1}, {"a": 2, "_agentbox": {"call_ids": {"root/s1/chat": 2}}}]


def test_resume_restores_call_ids_and_hides_reserved_key(tmp_path):
    resume = {
        "checkpoint_id": "cp-0",
        "step_id": "s1",
        "state": {"a": 2, "_agentbox": {"call_ids": {"root/s1/chat": 2}}},
        "refs": [],
    }
    seen = {}

    async def app(ctx):
        seen["state"] = ctx.resume.state
        seen["next"] = ctx.gateway.call_ids.next("s1", "chat")
        await ctx.checkpoint("s2", state={"a": 3})
        return Result("ok", [])

    code, events = run(
        app, tmp_path, init={"resume": resume}, responder=checkpoint_reply("committed")
    )
    assert code == 0
    assert seen == {"state": {"a": 2}, "next": "root/s1/chat/3"}
    checkpoint = next(e for e in events if e["type"] == "checkpoint")
    assert checkpoint["state"] == {"a": 3, "_agentbox": {"call_ids": {"root/s1/chat": 3}}}


@pytest.mark.parametrize(
    ("kwargs", "after_call"),
    [
        pytest.param({"state": {"_agentbox": 1}}, False, id="reserved_key_from_app"),
        pytest.param({"state": [1, 2]}, True, id="non_object_state_after_call"),
        pytest.param({"state_ref": "a" * 64}, True, id="state_ref_after_call"),
    ],
)
def test_checkpoint_rejects_states_that_cannot_carry_call_ids(tmp_path, kwargs, after_call):
    async def app(ctx):
        if after_call:
            ctx.gateway.call_ids.next("s1", "chat")
        await ctx.checkpoint("s1", **kwargs)
        return Result("never", [])

    code, events = run(app, tmp_path)
    assert code == 1
    assert types(events) == ["ready", "error"] and events[1]["code"] == "invalid_field"


def test_corrupt_reserved_state_on_resume_fails_before_ready(tmp_path):
    resume = {
        "checkpoint_id": "cp-0",
        "step_id": "s1",
        "state": {"_agentbox": {"call_ids": {"x": -3}}},
    }
    code, events = run(returns_ok, tmp_path, init={"resume": resume})
    assert code == 1
    assert types(events) == ["error"] and events[0]["code"] == "invalid_field"


# ---- sim-worker 的 Gateway 操作 ----


def test_sim_worker_gateway_ops_put_blobs_into_checkpoint_refs(fake_gateway, tmp_path, monkeypatch):
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, fake_gateway.socket_path)
    steps = [
        {"op": "chat", "step_id": "s1", "messages": [{"role": "user", "content": "题目"}]},
        {"op": "search", "step_id": "s1", "query": "agent", "max_results": 2},
        {"op": "fetch", "step_id": "s1", "url": "https://example.com/a"},
        {"op": "checkpoint", "step_id": "s1"},
        {"op": "chat", "step_id": "s1", "messages": [{"role": "user", "content": "再问"}]},
        {"op": "checkpoint", "step_id": "s2"},
    ]
    code, events = run(
        sim_app,
        tmp_path,
        init={"config": {"steps": steps}},
        responder=checkpoint_reply("committed"),
    )
    assert code == 0 and events[-1]["type"] == "result"
    shas = list(fake_gateway.calls.values())
    assert [r.headers["x-agentbox-call-id"] for r in fake_gateway.requests] == [
        "root/s1/chat/1",
        "root/s1/search/1",
        "root/s1/fetch/1",
        "root/s1/chat/2",
    ]
    first, second = [e for e in events if e["type"] == "checkpoint"]
    assert first["refs"] == shas[:3] and second["refs"] == shas
    assert first["state"] == {
        "next_index": 4,
        "_agentbox": {"call_ids": {"root/s1/chat": 1, "root/s1/search": 1, "root/s1/fetch": 1}},
    }
    assert fake_gateway.requests[1].json() == {"query": "agent", "max_results": 2}

    # 从第一个 checkpoint 恢复：refs 延续，call id 续号（重做的第 4 步得到与原来相同的 ID → 重放）
    resume = {
        "checkpoint_id": first["checkpoint_id"],
        "step_id": "s1",
        "state": first["state"],
        "refs": first["refs"],
    }
    code, events = run(
        sim_app,
        tmp_path,
        init={"attempt_no": 2, "resume": resume, "config": {"steps": steps}},
        responder=checkpoint_reply("committed"),
    )
    assert code == 0
    assert fake_gateway.requests[-1].headers["x-agentbox-call-id"] == "root/s1/chat/2"
    (last,) = [e for e in events if e["type"] == "checkpoint"]
    assert last["refs"] == shas


def test_sim_worker_gateway_error_fails_task(fake_gateway, tmp_path, monkeypatch):
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, fake_gateway.socket_path)
    fake_gateway.replies.append(error_reply(402, "budget_exhausted"))
    steps = [{"op": "search", "step_id": "s1", "query": "q"}]
    code, events = run(sim_app, tmp_path, init={"config": {"steps": steps}})
    assert code == 1
    assert events[-1]["type"] == "error" and events[-1]["code"] == "budget_exhausted"


# ---- sub-run：SubrunManager 与 sub-run Gateway 视图（Plan 14 Task 8） ----
# 宿主由 responder 脚本化：checkpoint 一律 committed，subrun_start 按用例作答；
# 取消经 subrun_cancel_requested 注入。Worker 事件流再按协商了 subruns 的流规则回放一遍。

from agentfakes import SessionHost

from agentbox_worker import (
    SubrunCancelled,
    SubrunGateway,
    SubrunInfo,
    SubrunRejected,
)
from agentbox_worker.gateway import _Response
from agentbox_worker.stream import SessionStreamChecker

SUBRUN_INIT = {"extensions": ["subruns"]}
REF1 = "1" * 64
REF2 = "2" * 64
START_ARGS = {"parent_step_id": "plan", "deadline_ms": 600_000}


def subrun_host(
    *,
    started: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
    on: Responder | None = None,
) -> Responder:
    """checkpoint → committed；subrun_start → started(msg) 的答复（None 不答复），缺省 started；
    on(msg, transport) 在自动作答之后调用，用于注入 subrun_cancel_requested。"""

    def respond(msg: dict[str, Any], transport: MemoryTransport) -> None:
        checkpoint_reply("committed")(msg, transport)
        if msg["type"] == "subrun_start":
            answer = {"status": "started"} if started is None else started(msg)
            if answer is not None:
                reply(
                    transport,
                    {"type": "subrun_started", "subrun_id": msg["subrun_id"], **answer},
                )
        if on is not None:
            on(msg, transport)

    return respond


def cancel_requested(transport: MemoryTransport, subrun_id: str, reason: str = "deadline") -> None:
    reply(
        transport,
        {"type": "subrun_cancel_requested", "subrun_id": subrun_id, "reason": reason},
    )


def check_stream(events: list[dict[str, Any]]) -> None:
    checker = StreamChecker()
    checker.negotiate(["subruns"])
    for event in events:
        checker.observe(event)


def of_type(events: list[dict[str, Any]], typ: str) -> list[dict[str, Any]]:
    return [e for e in events if e["type"] == typ]


def first_of(events: list[dict[str, Any]], typ: str) -> dict[str, Any]:
    return next(e for e in events if e["type"] == typ)


def strip(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if k not in ("v", "seq", "ts")}


async def sleeps_in_subrun(handle) -> str:
    await asyncio.sleep(10)
    return "never"


@pytest.mark.parametrize(
    ("extensions", "expected"),
    [
        pytest.param(["subruns"], 1, id="requested"),
        pytest.param(None, None, id="absent"),
        pytest.param(["other"], None, id="other_extension"),
    ],
)
def test_ready_acknowledges_subruns_only_when_requested(tmp_path, extensions, expected):
    init = {} if extensions is None else {"extensions": extensions}
    code, events = run(returns_ok, tmp_path, init=init)
    assert code == 0
    assert events[0]["type"] == "ready" and events[0].get("subruns") == expected


@pytest.mark.parametrize("requested", [True, False])
def test_session_ready_acknowledges_subruns_only_when_requested(requested):
    host = SessionHost()
    if requested:
        host.init["extensions"] = ["subruns"]
    host.close_when_idle()
    code, events = host.run(returns_ok)
    assert code == 0
    assert events[0]["type"] == "ready"
    assert events[0].get("subruns") == (1 if requested else None)


def test_start_sends_subrun_start_and_blocks_until_started(tmp_path):
    holder: dict[str, Any] = {}
    seen: dict[str, Any] = {}

    def on(msg, transport):
        holder["transport"] = transport

    async def app(ctx):
        task = asyncio.create_task(ctx.subruns.start("st1", **START_ARGS, budget_cap_micro=5000))
        await asyncio.sleep(0.02)  # 短于 ack_timeout：宿主尚未答复
        seen["blocked"] = not task.done()
        started = {"type": "subrun_started", "subrun_id": "st1", "status": "started"}
        reply(holder["transport"], started)
        handle = await task
        seen["id"] = handle.subrun_id
        seen["cancelled"] = handle.cancelled()
        return Result("ok")

    responder = subrun_host(started=lambda m: None, on=on)
    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=responder)
    assert code == 0, events
    assert seen == {"blocked": True, "id": "st1", "cancelled": False}
    assert strip(of_type(events, "subrun_start")[0]) == {
        "type": "subrun_start",
        "subrun_id": "st1",
        "parent_step_id": "plan",
        "deadline_ms": 600_000,
        "budget_cap_micro": 5000,
    }
    check_stream(events)


def test_start_without_cap_omits_budget_cap(tmp_path):
    async def app(ctx):
        await ctx.subruns.start("st1", **START_ARGS)
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0
    assert "budget_cap_micro" not in of_type(events, "subrun_start")[0]


@pytest.mark.parametrize(
    "reject_code", ["subrun_limit", "conflict", "subrun_closed", "invalid_field"]
)
def test_rejected_start_raises_subrun_rejected(tmp_path, reject_code):
    async def app(ctx):
        try:
            await ctx.subruns.start("st1", **START_ARGS)
        except SubrunRejected as exc:
            assert isinstance(exc, WorkerFailure)
            return Result(f"rejected:{exc.code}")
        return Result("started")

    responder = subrun_host(started=lambda m: {"status": "rejected", "code": reject_code})
    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=responder)
    assert code == 0
    assert events[-1]["summary"] == f"rejected:{reject_code}"
    assert len(of_type(events, "subrun_start")) == 1


def test_retryable_rejection_resends_same_definition(tmp_path):
    answers = [{"status": "rejected", "code": "retryable_error"}, {"status": "started"}]

    async def app(ctx):
        await ctx.subruns.start("st1", **START_ARGS)
        return Result("ok")

    responder = subrun_host(started=lambda m: answers.pop(0))
    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=responder)
    assert code == 0, events
    starts = [strip(e) for e in of_type(events, "subrun_start")]
    assert len(starts) == 2 and starts[0] == starts[1]


def test_unanswered_start_is_resent_then_unresolved(tmp_path):
    async def app(ctx):
        await ctx.subruns.start("st1", **START_ARGS)
        return Result("ok")

    responder = subrun_host(started=lambda m: None)
    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=responder)
    assert code == 1
    starts = of_type(events, "subrun_start")
    assert len(starts) == FAST.max_ack_attempts + 1
    assert len({json.dumps(strip(s), sort_keys=True) for s in starts}) == 1
    assert events[-1]["type"] == "error" and events[-1]["code"] == "subrun_unresolved"
    assert events[-1]["retryable"] is True


def test_start_without_negotiation_fails_locally(tmp_path):
    async def app(ctx):
        await ctx.subruns.start("st1", **START_ARGS)
        return Result("ok")

    code, events = run(app, tmp_path, responder=subrun_host())
    assert code == 1
    assert types(events) == ["ready", "error"]
    assert events[-1]["code"] == "subruns_not_negotiated"


@pytest.mark.parametrize(
    ("subrun_id", "args"),
    [
        pytest.param("root", START_ARGS, id="root_id"),
        pytest.param("ST1", START_ARGS, id="bad_id"),
        pytest.param("st1", {"parent_step_id": "", "deadline_ms": 1}, id="empty_parent"),
        pytest.param("st1", {"parent_step_id": "p" * 257, "deadline_ms": 1}, id="long_parent"),
        pytest.param("st1", {"parent_step_id": "p", "deadline_ms": 0}, id="zero_deadline"),
        pytest.param("st1", {"parent_step_id": "p", "deadline_ms": 3_600_001}, id="long_deadline"),
        pytest.param("st1", {**START_ARGS, "budget_cap_micro": -1}, id="negative_cap"),
    ],
)
def test_invalid_start_is_rejected_locally(tmp_path, subrun_id, args):
    async def app(ctx):
        try:
            await ctx.subruns.start(subrun_id, **args)
        except SubrunRejected as exc:
            return Result(exc.code)
        return Result("started")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0
    assert events[-1]["summary"] == "invalid_field"
    assert of_type(events, "subrun_start") == []


def test_fifth_logical_subrun_is_rejected_locally(tmp_path):
    async def app(ctx):
        for n in range(1, 5):
            await ctx.subruns.start(f"st{n}", **START_ARGS)
        try:
            await ctx.subruns.start("st5", **START_ARGS)
        except SubrunRejected as exc:
            return Result(exc.code)
        return Result("started")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0
    assert events[-1]["summary"] == "subrun_limit"
    assert [e["subrun_id"] for e in of_type(events, "subrun_start")] == ["st1", "st2", "st3", "st4"]


def recording_send(recorded: list[tuple[str, dict[str, str]]]):
    def _send(self, method, path, payload, headers, **_):  # exec 另传 timeout_s
        recorded.append((path, dict(headers)))
        return _Response(200, {}, b"{}")

    return _send


def test_subrun_gateway_tags_requests_and_continues_call_ids(tmp_path, monkeypatch):
    recorded: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(GatewayClient, "_send", recording_send(recorded))
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, "/tmp/x.sock")
    resume = {
        "checkpoint_id": "cp-0",
        "step_id": "s0",
        "state": {"_agentbox": {"call_ids": {"st1/s1/search": 2}}},
        "refs": [],
        "subruns": [{"subrun_id": "st1", "status": "started"}],
    }
    seen: dict[str, Any] = {}

    async def app(ctx):
        st1 = await ctx.subruns.start("st1", **START_ARGS)
        st2 = await ctx.subruns.start("st2", **START_ARGS)
        for gateway in (st1.gateway, st2.gateway, st1.gateway, ctx.gateway):
            await asyncio.to_thread(gateway.search, "s1", "q")
        await asyncio.to_thread(st1.gateway.budget)
        seen["no_recursion"] = not any(
            hasattr(st1.gateway, name) for name in ("start_subrun", "for_subrun", "subruns")
        )
        try:
            st1.gateway.retry("s1", "search", "st2/s1/search/1", {})
        except ValueError:
            seen["foreign_retry"] = "refused"
        await ctx.checkpoint("s2", state={})
        return Result("ok")

    init = {**SUBRUN_INIT, "resume": resume}
    code, events = run(app, tmp_path, init=init, responder=subrun_host())
    assert code == 0, events
    assert seen == {"no_recursion": True, "foreign_retry": "refused"}
    calls = [(h.get("X-Agentbox-Call-Id"), h.get("X-Agentbox-Subrun")) for _, h in recorded]
    assert calls == [
        ("st1/s1/search/3", "st1"),  # 随 checkpoint 恢复续号
        ("st2/s1/search/1", "st2"),  # 两个 sub-run 的计数相互独立
        ("st1/s1/search/4", "st1"),
        ("root/s1/search/1", None),  # root 请求不带 sub-run 头
        (None, "st1"),  # 只读端点同样带归属头
    ]
    checkpoint = of_type(events, "checkpoint")[0]
    assert checkpoint["state"]["_agentbox"]["call_ids"] == {
        "st1/s1/search": 4,
        "st2/s1/search": 1,
        "root/s1/search": 1,
    }
    assert checkpoint["subruns"] == [
        {"subrun_id": "st1", "status": "started"},
        {"subrun_id": "st2", "status": "started"},
    ]
    check_stream(events)


def test_subrun_gateway_limits_in_flight_calls_to_two(monkeypatch):
    lock = threading.Lock()
    state = {"now": 0, "max": 0}
    release = threading.Event()

    def _send(self, method, path, payload, headers):
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
        release.wait(5)
        with lock:
            state["now"] -= 1
        return _Response(200, {}, b"{}")

    monkeypatch.setattr(GatewayClient, "_send", _send)
    gateway = SubrunGateway("/x.sock", call_ids=CallIds(), subrun_id="st1", timeout_s=5)
    threads = [threading.Thread(target=gateway.search, args=("s", f"q{i}")) for i in range(4)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 2
    while state["now"] < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.1)  # 若没有上限，第三、四个请求会在这段时间内进入
    assert state["now"] == 2
    release.set()
    for thread in threads:
        thread.join(5)
    assert state["max"] == 2


def test_subrun_gateway_rejects_invalid_id():
    for bad in ("root", "", "A", "x" * 33):
        with pytest.raises(ValueError):
            SubrunGateway("/x.sock", call_ids=CallIds(), subrun_id=bad, timeout_s=5)


def test_cancel_requested_cancels_body_and_ends_cancelled(tmp_path, monkeypatch):
    recorded: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(GatewayClient, "_send", recording_send(recorded))
    seen: dict[str, Any] = {}

    def on(msg, transport):
        if msg["type"] == "progress" and msg.get("subrun_id") == "st1":
            cancel_requested(transport, "st1", "deadline")

    async def body(handle):
        await handle.progress("tool_call", "searching")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen["body_cancelled"] = True
            raise

    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        try:
            await ctx.subruns.run(handle, body)
        except SubrunCancelled as exc:
            seen["reason"] = (exc.subrun_id, exc.reason)
        seen["cancelled"] = handle.cancelled()
        try:
            handle.gateway.search("s1", "late")
        except GatewayError as exc:
            seen["late_call"] = exc.code
        await ctx.checkpoint("after", state={})
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host(on=on))
    assert code == 0, events
    assert seen == {
        "body_cancelled": True,
        "reason": ("st1", "deadline"),
        "cancelled": True,
        "late_call": "subrun_closed",
    }
    assert recorded == []  # 已关闭的视图不再发出请求
    progress = of_type(events, "progress")[0]
    assert progress["subrun_id"] == "st1" and progress["kind"] == "tool_call"
    (end,) = of_type(events, "subrun_end")
    assert end["subrun_id"] == "st1" and end["status"] == "cancelled"
    assert of_type(events, "checkpoint")[0]["subruns"] == [
        {"subrun_id": "st1", "status": "cancelled"}
    ]
    result = events[-1]
    assert result["type"] == "result"
    assert [(e["id"], e["status"]) for e in result["subruns"]] == [("st1", "timed_out")]
    check_stream(events)


def test_cancel_requested_racing_completion_still_ends_cancelled(tmp_path):
    def on(msg, transport):
        if msg["type"] == "subrun_end" and msg["status"] == "succeeded":
            cancel_requested(transport, "st1", "deadline")  # 宿主已置 cancel_requested

    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        value = await ctx.subruns.run(handle, lambda h: asyncio.sleep(0, result="found"))
        await ctx.subruns.complete(handle, summary=value, result_ref=REF1)
        await asyncio.sleep(0.05)  # 让控制循环处理 subrun_cancel_requested
        await ctx.checkpoint("after", state={})
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host(on=on))
    assert code == 0, events
    assert [e["status"] for e in of_type(events, "subrun_end")] == ["succeeded", "cancelled"]
    assert of_type(events, "checkpoint")[0]["subruns"] == [
        {"subrun_id": "st1", "status": "cancelled"}
    ]  # 不再宣称 completed（否则 E41 invalid_transition）
    assert [(e["id"], e["status"]) for e in events[-1]["subruns"]] == [("st1", "timed_out")]
    check_stream(events)


def test_cancel_requested_between_run_and_complete(tmp_path):
    def on(msg, transport):
        if msg["type"] == "progress" and msg["kind"] == "between":
            cancel_requested(transport, "st1", "deadline")

    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        await ctx.subruns.run(handle, lambda h: asyncio.sleep(0, result="x"))
        await ctx.progress("between", "body finished")
        await asyncio.sleep(0.05)
        try:
            await ctx.subruns.complete(handle, summary="x", result_ref=REF1)
        except SubrunCancelled:
            return Result("cancelled-before-complete")
        return Result("completed")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host(on=on))
    assert code == 0, events
    assert events[-1]["summary"] == "cancelled-before-complete"
    assert [e["status"] for e in of_type(events, "subrun_end")] == ["cancelled"]
    check_stream(events)


def test_cancel_requested_without_running_body_ends_before_result(tmp_path):
    def on(msg, transport):
        if msg["type"] == "progress" and msg["kind"] == "idle":
            cancel_requested(transport, "st1", "deadline")

    async def app(ctx):
        await ctx.subruns.start("st1", **START_ARGS)
        await ctx.progress("idle", "not running any body")
        await asyncio.sleep(0.05)
        return Result("ok")  # 结束前 SDK 仍须先发 subrun_end{cancelled}

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host(on=on))
    assert code == 0, events
    assert types(events)[-2:] == ["subrun_end", "result"]
    assert events[-2]["status"] == "cancelled"
    check_stream(events)


def test_orchestrator_cancel_sends_cancel_then_end(tmp_path):
    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        task = asyncio.create_task(ctx.subruns.run(handle, sleeps_in_subrun))
        await asyncio.sleep(0.01)
        await ctx.subruns.cancel(handle, reason="enough_evidence")
        try:
            await task
        except SubrunCancelled as exc:
            return Result(f"cancelled:{exc.reason}")
        return Result("not cancelled")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0, events
    assert events[-1]["summary"] == "cancelled:enough_evidence"
    assert [strip(e) for e in events if e["type"] in ("subrun_cancel", "subrun_end")] == [
        {"type": "subrun_cancel", "subrun_id": "st1", "reason": "enough_evidence"},
        {
            "type": "subrun_end",
            "subrun_id": "st1",
            "status": "cancelled",
            "summary": "cancelled: enough_evidence",
        },
    ]
    assert [(e["id"], e["status"]) for e in events[-1]["subruns"]] == [("st1", "cancelled")]
    check_stream(events)


def test_concurrent_completions_are_both_in_next_checkpoint(tmp_path):
    async def app(ctx):
        handles = [await ctx.subruns.start(sid, **START_ARGS) for sid in ("st1", "st2")]

        async def one(handle, ref):
            value = await ctx.subruns.run(handle, lambda h: asyncio.sleep(0, result=h.subrun_id))
            await ctx.subruns.complete(handle, summary=f"{value} done", result_ref=ref)

        await asyncio.gather(one(handles[0], REF1), one(handles[1], REF2))
        await ctx.checkpoint("merge", state={"n": 1})
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0, events
    assert of_type(events, "checkpoint")[0]["subruns"] == [
        {"subrun_id": "st1", "status": "completed", "result_ref": REF1},
        {"subrun_id": "st2", "status": "completed", "result_ref": REF2},
    ]
    assert events[-1]["subruns"] == [
        {"id": "st1", "status": "completed", "summary": "st1 done"},
        {"id": "st2", "status": "completed", "summary": "st2 done"},
    ]
    assert [e["status"] for e in of_type(events, "subrun_end")] == ["succeeded", "succeeded"]
    check_stream(events)


def test_failed_subrun_cannot_restart_in_same_turn(tmp_path):
    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        await ctx.subruns.fail(handle, summary="搜索全部失败")
        try:
            await ctx.subruns.start("st1", **START_ARGS)
        except SubrunRejected as exc:
            return Result(exc.code)
        return Result("restarted")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0
    assert events[-1]["summary"] == "subrun_closed"
    assert len(of_type(events, "subrun_start")) == 1
    (end,) = of_type(events, "subrun_end")
    assert (end["status"], end["summary"]) == ("failed", "搜索全部失败")
    assert events[-1]["subruns"] == [{"id": "st1", "status": "failed", "summary": "搜索全部失败"}]


def test_resumed_subruns_are_visible_and_terminal_ones_closed(tmp_path):
    resume = {
        "checkpoint_id": "cp-0",
        "step_id": "s0",
        "state": {},
        "refs": [],
        "subruns": [
            {"subrun_id": "st1", "status": "completed", "result_ref": REF1},
            {"subrun_id": "st2", "status": "started"},
            {"subrun_id": "st3", "status": "timed_out"},
        ],
    }
    seen: dict[str, Any] = {}

    async def app(ctx):
        seen["resumed"] = ctx.subruns.resumed()
        for sid in ("st1", "st3"):
            try:
                await ctx.subruns.start(sid, **START_ARGS)
            except SubrunRejected as exc:
                seen[sid] = exc.code
        await ctx.subruns.start("st2", **START_ARGS)  # 恢复后须重发 subrun_start
        await ctx.checkpoint("s1", state={})
        return Result("ok")

    init = {**SUBRUN_INIT, "resume": resume}
    code, events = run(app, tmp_path, init=init, responder=subrun_host())
    assert code == 0, events
    assert seen == {
        "resumed": {
            "st1": SubrunInfo("st1", "completed", REF1),
            "st2": SubrunInfo("st2", "started", None),
            "st3": SubrunInfo("st3", "timed_out", None),
        },
        "st1": "subrun_closed",
        "st3": "subrun_closed",
    }
    assert [e["subrun_id"] for e in of_type(events, "subrun_start")] == ["st2"]
    assert of_type(events, "checkpoint")[0]["subruns"] == [
        {"subrun_id": "st1", "status": "completed", "result_ref": REF1},
        {"subrun_id": "st2", "status": "started"},
        {"subrun_id": "st3", "status": "cancelled"},  # checkpoint 无 timed_out；宿主保持 timed_out
    ]
    assert [(e["id"], e["status"]) for e in events[-1]["subruns"]] == [
        ("st1", "completed"),
        ("st2", "started"),
        ("st3", "timed_out"),
    ]
    check_stream(events)


def test_checkpoint_and_result_omit_subruns_when_none(tmp_path):
    async def app(ctx):
        await ctx.checkpoint("s1", state={})
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0
    assert "subruns" not in of_type(events, "checkpoint")[0]
    assert "subruns" not in events[-1]


@pytest.mark.parametrize(
    ("init", "message"),
    [
        pytest.param(
            {},
            {"type": "subrun_started", "subrun_id": "st1", "status": "started"},
            id="not_negotiated",
        ),
        pytest.param(
            SUBRUN_INIT,
            {"type": "subrun_cancel_requested", "subrun_id": "st9", "reason": "deadline"},
            id="unknown_subrun",
        ),
    ],
)
def test_unexpected_subrun_control_is_protocol_error(tmp_path, init, message):
    line = json.dumps({"v": 1, **message}).encode()
    code, events = run(sleeps, tmp_path, init=init, host_lines=[line])
    assert code == 1
    assert events[-1]["type"] == "error" and events[-1]["code"] == "control_protocol_error"


def test_session_mode_subrun_flow():
    def on_event(event, host):
        if event["type"] == "subrun_start":
            started = {"subrun_id": event["subrun_id"], "status": "started"}
            host.send({"type": "subrun_started", "v": 1, **started})
        if event["type"] == "progress" and event.get("subrun_id") == "st2":
            cancel = {"subrun_id": "st2", "reason": "deadline"}
            host.send({"type": "subrun_cancel_requested", "v": 1, **cancel})

    host = SessionHost(on_event=on_event)
    host.init["extensions"] = ["subruns"]
    host.start_task("t-1", "a-1")
    host.close_when_idle()

    async def body(handle):
        await handle.progress("tool_call", "working")
        await asyncio.sleep(10)

    async def app(ctx):
        st1 = await ctx.subruns.start("st1", **START_ARGS)
        st2 = await ctx.subruns.start("st2", **START_ARGS)
        await ctx.subruns.run(st1, lambda h: asyncio.sleep(0))
        await ctx.subruns.complete(st1, summary="done", result_ref=REF1)
        try:
            await ctx.subruns.run(st2, body)
        except SubrunCancelled:
            pass
        await ctx.checkpoint("merge", state={})
        return Result("ok")

    code, events = host.run(app)
    assert code == 0, events
    subrun_events = [e for e in events if e["type"].startswith("subrun_")]
    assert all(e["attempt_id"] == "a-1" for e in subrun_events)
    assert [(e["type"], e["subrun_id"], e.get("status")) for e in subrun_events] == [
        ("subrun_start", "st1", None),
        ("subrun_start", "st2", None),
        ("subrun_end", "st1", "succeeded"),
        ("subrun_end", "st2", "cancelled"),
    ]
    assert first_of(events, "checkpoint")["subruns"] == [
        {"subrun_id": "st1", "status": "completed", "result_ref": REF1},
        {"subrun_id": "st2", "status": "cancelled"},
    ]
    assert [(e["id"], e["status"]) for e in first_of(events, "result")["subruns"]] == [
        ("st1", "completed"),
        ("st2", "timed_out"),
    ]
    checker = SessionStreamChecker()
    for side, msg in host.transcript:
        if side == "host":
            checker.host_sent(msg)
        else:
            checker.observe(msg)


def test_subrun_gateway_over_unix_socket(fake_gateway, tmp_path, monkeypatch):
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, fake_gateway.socket_path)
    messages = [{"role": "user", "content": "hi"}]

    async def app(ctx):
        handle = await ctx.subruns.start("st1", **START_ARGS)
        await asyncio.to_thread(handle.gateway.chat, "s1", messages)
        await asyncio.to_thread(ctx.gateway.chat, "s1", messages)
        return Result("ok")

    code, events = run(app, tmp_path, init=SUBRUN_INIT, responder=subrun_host())
    assert code == 0, events
    sub, root = fake_gateway.requests
    assert sub.headers["x-agentbox-subrun"] == "st1"
    assert sub.headers["x-agentbox-call-id"] == "st1/s1/chat/1"
    assert "x-agentbox-subrun" not in root.headers
    assert root.headers["x-agentbox-call-id"] == "root/s1/chat/1"


# ---- exec：GatewayClient.exec() → POST /v1/exec（规格 §10；Plan 15 Task 9） ----

from agentbox_worker.gateway import (
    EXEC_DEFAULT_WALL_MS,
    EXEC_QUEUE_TIMEOUT_S,
    exec_timeout_s,
)

EXEC_RESULT = {"status": "completed", "exit_code": 0, "stdout": "1\n", "stderr": ""}


def exec_gateway(gw: FakeGateway) -> None:
    """fake Gateway 的 /v1/exec：返回固定结果与结果 blob（同一 call id 再次到达时带重放头）。"""
    seen: set[str] = set()
    sha = gw.put_blob(json.dumps(EXEC_RESULT).encode())

    def intercept(req):
        if req.path != "/v1/exec":
            return None
        call_id = req.headers.get("x-agentbox-call-id", "")
        headers = {"X-Agentbox-Blob": sha}
        if call_id in seen:
            headers["X-Agentbox-Replayed"] = "true"
        seen.add(call_id)
        return Reply(200, EXEC_RESULT, headers)

    gw.interceptor = intercept


def test_exec_sends_body_headers_and_call_ids(fake_gateway):
    exec_gateway(fake_gateway)
    gw = client(fake_gateway)
    first = gw.exec("analyze", "print(1)")
    inputs = [("a" * 64, "data/a.csv"), ("b" * 64, "b.txt")]
    second = gw.exec("analyze", "print(2)", inputs=inputs, wall_ms=1500, memory_bytes=1 << 20)
    third = gw.exec("analyze", "print(1)", retry=True)
    got = [
        (r.method, r.path, r.headers.get("x-agentbox-call-id"), r.json())
        for r in fake_gateway.requests
    ]
    assert got == [
        ("POST", "/v1/exec", "root/analyze/exec/1", {"language": "python3", "code": "print(1)"}),
        (
            "POST",
            "/v1/exec",
            "root/analyze/exec/2",
            {
                "language": "python3",
                "code": "print(2)",
                "inputs": [
                    {"sha256": "a" * 64, "path": "data/a.csv"},
                    {"sha256": "b" * 64, "path": "b.txt"},
                ],
                "limits": {"wall_ms": 1500, "memory_bytes": 1 << 20},
            },
        ),
        ("POST", "/v1/exec", "root/analyze/exec/3", {"language": "python3", "code": "print(1)"}),
    ]
    retry_headers = [r.headers.get("x-agentbox-retry") for r in fake_gateway.requests]
    assert retry_headers == [None, None, "true"]
    for r in fake_gateway.requests:
        assert r.headers["content-type"] == "application/json"
        assert not {"x-agentbox-cache", "x-agentbox-supersedes", "x-agentbox-subrun"} & set(
            r.headers
        )
    assert first.body == EXEC_RESULT and first.status == 200 and first.replayed is False
    assert first.blob_sha256 == second.blob_sha256 and first.call_id == "root/analyze/exec/1"
    assert third.call_id == "root/analyze/exec/3"


def test_exec_call_ids_continue_after_checkpoint_restore(fake_gateway):
    exec_gateway(fake_gateway)
    ids = CallIds()
    gw = client(fake_gateway, ids)
    gw.exec("s1", "x = 1")
    gw.exec("s1", "x = 2")
    snap = json.loads(json.dumps(ids.snapshot()))  # 如同经 checkpoint 往返
    assert snap == {"root/s1/exec": 2}
    resumed = client(fake_gateway, CallIds.restore(snap))
    assert resumed.exec("s1", "x = 3").call_id == "root/s1/exec/3"
    # 恢复后重做同一步（快照早于该步）得到同一 call id；retry=True 允许重跑已取消的 exec
    replay = client(fake_gateway, CallIds.restore({"root/s1/exec": 1}))
    res = replay.exec("s1", "x = 2", retry=True)
    assert res.call_id == "root/s1/exec/2" and res.replayed is True
    assert fake_gateway.requests[-1].headers["x-agentbox-retry"] == "true"


@pytest.mark.parametrize(
    ("reply", "exc_type"),
    [
        (error_reply(402, "exec_quota_exhausted"), BudgetExhausted),
        (error_reply(402, "exec_cpu_exhausted"), BudgetExhausted),
        (error_reply(402, "exec_blocked"), BudgetExhausted),
        # 输入未授权是请求参数问题，不是访问撤销：不得映射为 AccessRevoked（应用据它终止）
        (error_reply(403, "input_not_authorized"), GatewayError),
        (error_reply(403, "access_revoked"), AccessRevoked),
        (error_reply(409, "exec_cancelled"), GatewayError),
        (error_reply(409, "fingerprint_mismatch"), CallDivergence),
        (error_reply(502, "exec_start_failed"), GatewayError),
        (error_reply(502, "exec_unknown"), GatewayError),
        (error_reply(503, "exec_env_unavailable"), GatewayError),
        (error_reply(504, "exec_queue_timeout"), CallDeadlineExceeded),
        (error_reply(504, "call_deadline_exceeded"), CallDeadlineExceeded),
        (error_reply(400, "unsupported_field"), GatewayError),
    ],
    ids=lambda v: v.body["error"]["code"] if isinstance(v, Reply) else None,
)
def test_exec_errors_map_to_typed_exceptions(fake_gateway, reply, exc_type):
    fake_gateway.replies.append(reply)
    with pytest.raises(GatewayError) as caught:
        client(fake_gateway).exec("s1", "print(1)")
    assert type(caught.value) is exc_type
    assert (caught.value.status, caught.value.code) == (reply.status, reply.body["error"]["code"])
    assert len(fake_gateway.requests) == 1


def test_exec_call_in_progress_is_retried_with_same_id(fake_gateway):
    exec_gateway(fake_gateway)
    fake_gateway.replies += [error_reply(409, "call_in_progress")] * 2
    res = client(fake_gateway).exec("s1", "print(1)")
    assert res.body == EXEC_RESULT
    assert [r.headers["x-agentbox-call-id"] for r in fake_gateway.requests] == [
        "root/s1/exec/1"
    ] * 3


def test_exec_timeout_covers_queue_wall_and_grace(monkeypatch):
    # 调用期限 = 排队上限 + 生效 wall + 30 s（§9.7）；客户端再加余量，且不短于普通调用的超时
    assert exec_timeout_s(None) == exec_timeout_s(EXEC_DEFAULT_WALL_MS)
    assert exec_timeout_s(300_000) > EXEC_QUEUE_TIMEOUT_S + 300 + 30
    seen: list[float | None] = []

    def _send(self, method, path, payload, headers, *, timeout_s=None):
        seen.append(timeout_s)
        return _Response(200, {}, b"{}")

    monkeypatch.setattr(GatewayClient, "_send", _send)
    gw = GatewayClient("/x.sock", call_ids=CallIds(), timeout_s=5)
    gw.exec("s", "pass", wall_ms=300_000)
    gw.exec("s", "pass")
    big = GatewayClient("/x.sock", call_ids=CallIds(), timeout_s=10_000)
    big.exec("s", "pass")
    assert seen == [exec_timeout_s(300_000), exec_timeout_s(None), 10_000]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"code": b"print(1)"},
        {"inputs": [("a" * 64,)]},
        {"inputs": [("a" * 64, 1)]},
        {"wall_ms": 0},
        {"memory_bytes": True},
    ],
)
def test_exec_rejects_malformed_arguments_locally(kwargs):
    gw = GatewayClient("/x.sock", call_ids=CallIds(), timeout_s=5)
    args = {"code": "print(1)", **kwargs}
    with pytest.raises((TypeError, ValueError)):
        gw.exec("s", args.pop("code"), **args)
    assert gw.call_ids.snapshot() == {}  # 本地拒绝不占号


def test_subrun_gateway_exec_is_tagged(monkeypatch):
    recorded: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(GatewayClient, "_send", recording_send(recorded))
    gw = SubrunGateway("/x.sock", call_ids=CallIds(), subrun_id="st1", timeout_s=5)
    gw.exec("s1", "print(1)")
    ((path, headers),) = recorded
    assert path == "/v1/exec"
    assert headers["X-Agentbox-Call-Id"] == "st1/s1/exec/1"
    assert headers["X-Agentbox-Subrun"] == "st1"
