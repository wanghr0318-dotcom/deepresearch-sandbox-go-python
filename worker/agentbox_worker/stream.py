"""task 模式 Worker 事件流的顺序检查，与 Go 侧 protocol.WorkerStream 逐条对应（规格 §5.3）。"""

from __future__ import annotations

from typing import Any

from agentbox_worker.protocol import ProtocolError

_TERMINAL = {"result", "error", "paused"}


class StreamChecker:
    """seq 从 1 严格递增；ready 之前只允许 error；ready 只能出现一次；
    终态提议至多一个，其后只允许 checkpoint_query；handshake_error 只能是唯一一条消息。
    """

    def __init__(self) -> None:
        self._phase = "awaiting_ready"
        self._last_seq = 0

    def observe(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        if self._phase == "handshake_failed":
            raise ProtocolError("after_handshake_error", f"handshake_error 之后出现 {typ}")
        if typ == "handshake_error":
            if self._phase != "awaiting_ready" or self._last_seq != 0:
                raise ProtocolError("handshake_error_misplaced", "handshake_error 必须是第一条消息")
            self._phase = "handshake_failed"
            return
        seq = msg.get("seq")
        if seq != self._last_seq + 1:
            raise ProtocolError("seq_invalid", f"seq={seq}，期望 {self._last_seq + 1}")
        self._last_seq += 1
        self._advance(typ)

    def _advance(self, typ: str) -> None:
        if self._phase == "awaiting_ready":
            if typ == "ready":
                self._phase = "running"
            elif typ == "error":
                self._phase = "terminal_sent"
            else:
                raise ProtocolError("before_ready", f"{typ} 出现在 ready 之前")
        elif self._phase == "running":
            if typ == "ready":
                raise ProtocolError("duplicate_ready", "重复的 ready")
            if typ in _TERMINAL:
                self._phase = "terminal_sent"
        elif typ != "checkpoint_query":
            raise ProtocolError("after_terminal", f"终态提议之后出现 {typ}")
