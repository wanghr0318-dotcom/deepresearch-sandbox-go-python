"""Worker 事件的发送与宿主答复的等待。"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from agentbox_worker.errors import TransportBroken
from agentbox_worker.protocol import BOOTSTRAP_VERSION, VERSION, WORKER, encode_line
from agentbox_worker.transport import Transport


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Outbox:
    """为事件补上 v、严格递增的 seq 与 ts，校验后发出（规格 §5.3）。

    seq 的提交点是"校验通过、开始发送"：此后无论发送成功、失败还是被取消，该 seq 都已用掉。
    发送失败或在途被取消时结果不确定，Outbox 进入失效状态，此后的发送一律抛出
    TransportBroken，绝不以同一 seq 重发。校验失败抛出 ProtocolError，且不占用 seq。
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._seq = 0
        self._lock = asyncio.Lock()
        self._broken: str | None = None

    @property
    def last_seq(self) -> int:
        return self._seq

    async def emit(self, body: dict[str, Any]) -> None:
        async with self._lock:
            self._check_usable()
            message = {**body, "v": VERSION, "seq": self._seq + 1, "ts": _now()}
            line = encode_line(WORKER, message)
            self._seq += 1
            await self._send(line, f"seq={self._seq}")

    async def send_handshake_error(self, code: str) -> None:
        async with self._lock:
            self._check_usable()
            body = {"type": "handshake_error", "bootstrap": BOOTSTRAP_VERSION, "code": code}
            await self._send(encode_line(WORKER, body), "handshake_error")

    async def cancel_when_idle(self, task: asyncio.Task[Any]) -> None:
        """等当前在途发送结束后取消 task，使取消不会落在发送中途而令通道失效。

        task 的取消在它下一次恢复执行时生效；若它正等待发送锁，会在取得锁之前收到取消。
        """
        async with self._lock:
            task.cancel()

    def _check_usable(self) -> None:
        if self._broken is not None:
            raise TransportBroken(self._broken)

    async def _send(self, line: bytes, what: str) -> None:
        try:
            await self._transport.send(line)
        except asyncio.CancelledError:
            self._broken = f"{what} 的发送在途被取消，结果不确定"
            raise
        except TransportBroken as exc:
            self._broken = str(exc)
            raise
        except Exception as exc:
            self._broken = f"{what} 发送失败：{exc!r}"
            raise TransportBroken(self._broken) from exc


class Waiters:
    """按键等待宿主答复；没有在等待的答复（迟到或重复）被丢弃。"""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    def expect(self, key: str) -> asyncio.Future[dict[str, Any]]:
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[key] = future
        return future

    def deliver(self, key: str, message: dict[str, Any]) -> bool:
        future = self._pending.pop(key, None)
        if future is None or future.done():
            return False
        future.set_result(message)
        return True
