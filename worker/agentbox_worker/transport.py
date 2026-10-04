"""按行收发协议消息的传输层。"""

from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable
from typing import BinaryIO, Protocol

from agentbox_worker.errors import TransportBroken
from agentbox_worker.protocol import MAX_INIT_BYTES, ProtocolError

# 一行（含行尾 \r\n）的最大字节数：init 的上限是 1 MiB，其他消息更小，由解码器细分。
MAX_FRAME_BYTES = MAX_INIT_BYTES + 2
DEFAULT_QUEUE_SIZE = 64

_OVERSIZED = object()  # 读线程放入队列的"超长帧"标记
_POLL_SECONDS = 0.05  # 读线程因背压阻塞时检查事件循环是否已关闭的间隔


class Transport(Protocol):
    async def receive(self) -> bytes | None:
        """返回下一行（不含换行符）；None 表示对端已关闭；帧超长时抛出 ProtocolError。"""

    async def send(self, line: bytes) -> None:
        """发送一行（不含换行符）。调用方须串行化；失败时抛出 TransportBroken。"""


def _settle(done: asyncio.Future[None], error: BaseException | None) -> None:
    if done.done():  # 发送方已取消等待
        return
    if error is None:
        done.set_result(None)
    else:
        done.set_exception(TransportBroken(f"写出失败：{error!r}"))


class StdioTransport:
    """基于二进制文件对象的行传输。

    - 读：守护线程以 readline(上限) 读取，超长帧在读取阶段即判定，之后停止读取；
      读到的行进入线程安全的有界队列，队列满时读线程阻塞，对宿主形成背压。读失败按对端关闭处理。
      读线程绑定首次 receive 所在的事件循环；该循环关闭后读线程不再读取输入并退出。
      输入缓冲约为 (queue_size + 1) 帧：队列中的 queue_size 帧，加上读线程正在读取或
      等待入队的一帧；另有对象与底层文件缓冲的开销。这是估算，不是总内存的硬上限。
    - 写：专用守护线程逐行写出并 flush，发送方等待写出完成；写失败后传输进入失败状态。
    - close(timeout)：在期限内等待已提交的写出完成。阻塞的读写线程不会阻止进程退出。
    """

    def __init__(
        self, reader: BinaryIO, writer: BinaryIO, *, queue_size: int = DEFAULT_QUEUE_SIZE
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._lines: queue.Queue[object] = queue.Queue(maxsize=queue_size)
        self._readable = asyncio.Event()
        self._read_thread: threading.Thread | None = None
        self._writes: queue.SimpleQueue[
            tuple[bytes, asyncio.AbstractEventLoop, asyncio.Future[None]] | None
        ] = queue.SimpleQueue()
        self._write_thread: threading.Thread | None = None
        self._failed: BaseException | None = None

    @property
    def pending_lines(self) -> int:
        """已读入、尚未被 receive 取走的行数（诊断用）。"""
        return self._lines.qsize()

    def wait_reader_stopped(self, timeout: float) -> bool:
        """在 timeout 秒内等待读线程结束；返回读线程是否已结束（诊断与测试用）。"""
        if self._read_thread is None:
            return True
        self._read_thread.join(timeout)
        return not self._read_thread.is_alive()

    async def receive(self) -> bytes | None:
        if self._read_thread is None:
            loop = asyncio.get_running_loop()
            self._read_thread = threading.Thread(target=self._read_loop, args=(loop,), daemon=True)
            self._read_thread.start()
        item = await self._next_line()
        if item is _OVERSIZED:
            raise ProtocolError("message_too_large", f"输入行超过 {MAX_FRAME_BYTES} 字节")
        return item  # type: ignore[return-value]

    async def _next_line(self) -> object:
        while True:
            self._readable.clear()  # 先清除再检查，避免漏掉检查与等待之间放入的行
            try:
                return self._lines.get_nowait()
            except queue.Empty:
                await self._readable.wait()

    def _read_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            while raw := self._reader.readline(MAX_FRAME_BYTES + 1):
                item = _OVERSIZED if len(raw) > MAX_FRAME_BYTES else raw.rstrip(b"\r\n")
                if not self._put(loop, item):
                    return  # 事件循环已关闭：不再消费输入
                if item is _OVERSIZED:
                    break  # 帧边界已丢失，不再继续读取
        except (OSError, ValueError):
            pass  # 读失败按对端关闭处理
        self._put(loop, None)

    def _put(self, loop: asyncio.AbstractEventLoop, item: object) -> bool:
        """放入有界队列并唤醒接收方；队列满时阻塞（背压）。返回是否已交付。

        读线程不创建协程，只在放入后以 call_soon_threadsafe 唤醒接收方；
        阻塞期间每 _POLL_SECONDS 检查一次事件循环，循环关闭后返回 False。
        """
        while not loop.is_closed():
            try:
                self._lines.put(item, timeout=_POLL_SECONDS)
            except queue.Full:
                continue
            try:
                loop.call_soon_threadsafe(self._readable.set)
            except RuntimeError:  # 放入后事件循环恰好关闭
                return False
            return True
        return False

    async def send(self, line: bytes) -> None:
        if self._failed is not None:
            raise TransportBroken(f"输出通道已失败：{self._failed!r}")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()
        if self._write_thread is None:
            self._write_thread = threading.Thread(target=self._write_loop, daemon=True)
            self._write_thread.start()
        self._writes.put((line, loop, done))
        await done

    def _write_loop(self) -> None:
        while (item := self._writes.get()) is not None:
            line, loop, done = item
            error: BaseException | None = None
            try:
                self._writer.write(line + b"\n")
                self._writer.flush()
            except (OSError, ValueError) as exc:  # 宿主关闭了 stdout，或文件已关闭
                error = self._failed = exc
            try:
                loop.call_soon_threadsafe(_settle, done, error)
            except RuntimeError:
                pass  # 事件循环已结束
            if error is not None:
                return

    def close(self, timeout: float) -> bool:
        """在 timeout 秒内等待已提交的写出完成；返回是否全部成功写出。"""
        if self._write_thread is None:
            return self._failed is None
        self._writes.put(None)
        self._write_thread.join(timeout)
        return not self._write_thread.is_alive() and self._failed is None


class MemoryTransport:
    """内存传输，供测试与场景回放使用。"""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.sent: list[bytes] = []
        self.on_send: Callable[[bytes], None] | None = None

    def feed(self, line: bytes | None) -> None:
        """送入一条宿主消息；None 模拟宿主关闭 stdin。"""
        self._queue.put_nowait(line)

    async def receive(self) -> bytes | None:
        return await self._queue.get()

    async def send(self, line: bytes) -> None:
        self.sent.append(line)
        if self.on_send is not None:
            self.on_send(line)
