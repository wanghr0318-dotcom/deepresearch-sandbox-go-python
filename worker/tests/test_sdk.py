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
