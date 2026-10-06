"""Worker 事件流的顺序检查，与 Go 侧 protocol.WorkerStream、protocol.SessionStream 逐条对应
（规格 §5.3、§5.4 session 扩展）。"""

from __future__ import annotations

from typing import Any

from agentbox_worker.protocol import (
    SUBRUNS_EXT_VERSION,
    ProtocolError,
    check_session_ext,
    requests_subruns,
)

_TERMINAL = {"result", "error", "paused"}
_SESSION_TERMINAL = _TERMINAL | {"awaiting_input"}


class _SubrunTracker:
    """sub-run 扩展的事件流状态（同 Go 的 subrunTracker）：是否已协商，以及本 attempt 中
    已 subrun_start 的 ID。checkpoint 的 subruns[] 可以列出恢复前已结束、本 attempt 未启动
    的 sub-run。"""

    def __init__(self) -> None:
        self.negotiated = False
        self.known: set[str] = set()

    def check_ready(self, msg: dict[str, Any]) -> None:
        subruns = msg.get("subruns") or 0
        if (subruns == SUBRUNS_EXT_VERSION) != self.negotiated:
            raise ProtocolError(
                "extension_mismatch",
                f"ready.subruns={subruns}，宿主请求 subruns 扩展：{self.negotiated}",
            )

    def observe(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        if typ in ("checkpoint", "result"):
            self._require_negotiated(bool(msg.get("subruns")), f"{typ}.subruns")
            return
        if typ not in ("subrun_start", "subrun_end", "subrun_cancel", "progress"):
            return
        subrun_id = msg.get("subrun_id") or ""
        if not subrun_id:
            return
        self._require_negotiated(True, typ)
        if typ == "subrun_start":
            self.known.add(subrun_id)
        else:
            self._require_known(subrun_id, typ)

    def host_sent(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        self._require_negotiated(True, typ)
        self._require_known(msg.get("subrun_id") or "", typ)

    def _require_negotiated(self, uses: bool, what: str) -> None:
        if uses and not self.negotiated:
            raise ProtocolError("extension_not_negotiated", f"未协商 subruns 扩展却出现 {what}")

    def _require_known(self, subrun_id: str, what: str) -> None:
        if subrun_id not in self.known:
            raise ProtocolError(
                "subrun_unknown", f"{what} 引用了尚未 subrun_start 的 sub-run {subrun_id!r}"
            )


class StreamChecker:
    """seq 从 1 严格递增；ready 之前只允许 error；ready 只能出现一次；
    终态提议至多一个，其后只允许 checkpoint_query；handshake_error 只能是唯一一条消息。
    """

    def __init__(self) -> None:
        self._phase = "awaiting_ready"
        self._last_seq = 0
        self._subruns = _SubrunTracker()

    def negotiate(self, extensions: list[Any] | None) -> None:
        """记录宿主在 init 中请求的扩展；须在 ready 之前调用，未调用视为未请求任何扩展。"""
        self._subruns.negotiated = requests_subruns(extensions)

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
        self._advance(msg, typ)

    def _advance(self, msg: dict[str, Any], typ: str) -> None:
        if self._phase == "awaiting_ready":
            if typ == "ready":
                self._subruns.check_ready(msg)
                self._phase = "running"
            elif typ == "error":
                self._phase = "terminal_sent"
            else:
                raise ProtocolError("before_ready", f"{typ} 出现在 ready 之前")
        elif self._phase == "running":
            if typ == "ready":
                raise ProtocolError("duplicate_ready", "重复的 ready")
            self._subruns.observe(msg)
            if typ in _TERMINAL:
                self._phase = "terminal_sent"
        elif typ != "checkpoint_query":
            raise ProtocolError("after_terminal", f"终态提议之后出现 {typ}")


# SessionStreamChecker 的阶段（同 Go 的 Phase* 常量）
PHASE_AWAITING_INIT = "awaiting_init"
PHASE_READY = "ready"
PHASE_IDLE = "idle"
PHASE_STARTING = "starting"
PHASE_ACTIVE = "active"
PHASE_PROPOSED = "proposed"
PHASE_QUIESCING = "quiescing"
PHASE_QUIESCED = "quiesced"
PHASE_CLOSING = "closing"
PHASE_CLOSED = "closed"


def _err(code: str, detail: str) -> ProtocolError:
    return ProtocolError(code, detail)


class SessionStreamChecker:
    """session 模式的事件流状态机：宿主消息经 host_sent 推进阶段，Worker 事件经 observe
    检查 seq（每 incarnation 从 1 严格递增）、attempt_id 与阶段。抛出 ProtocolError 后
    该检查器不应继续使用。消息须已按 session 模式解码（decode_session_line）。
    """

    def __init__(self) -> None:
        self._phase = PHASE_AWAITING_INIT
        self._current = ""
        self._outcome_sent = False
        self._last_seq = 0
        self._handshake = False
        self._subruns = _SubrunTracker()  # 协商结果来自 init，已启动的 ID 按 attempt 清空

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def current(self) -> str:
        """当前 attempt_id（无则为空串）。"""
        return self._current

    def host_sent(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        if self._phase == PHASE_AWAITING_INIT:
            if typ != "init":
                raise _err("unexpected_event", f"init 之前发送了 {typ}")
            if msg.get("mode") != "session":
                raise _err("invalid_field", f"session 流的 init.mode={msg.get('mode')!r}")
            self._subruns.negotiated = requests_subruns(msg.get("extensions"))
            self._phase = PHASE_READY
            return
        if self._phase in (PHASE_CLOSING, PHASE_CLOSED):
            raise _err("after_terminal", f"session_close 或 incarnation 结束之后发送了 {typ}")
        if typ == "init":
            raise _err("unexpected_event", "重复的 init")
        if typ == "task_start":
            if self._phase not in (PHASE_IDLE, PHASE_QUIESCED):
                raise _err("not_idle", f"{self._phase} 阶段不能发送 task_start")
            self._phase, self._current, self._outcome_sent = (
                PHASE_STARTING,
                msg["attempt_id"],
                False,
            )
            self._subruns.known = set()
        elif typ == "quiesce":
            if self._phase != PHASE_IDLE:
                raise _err("not_idle", f"{self._phase} 阶段不能发送 quiesce")
            self._phase = PHASE_QUIESCING
        elif typ == "session_close":
            if self._phase == PHASE_READY:
                raise _err("unexpected_event", "ready 之前发送了 session_close")
            self._phase, self._current = PHASE_CLOSING, ""
        elif typ == "task_outcome":
            self._check_current(msg.get("attempt_id") or "")
            self._phase, self._outcome_sent = PHASE_PROPOSED, True
        elif typ in ("cancel", "pause", "checkpoint_result", "artifact_result"):
            self._check_current(msg.get("attempt_id") or "")
        elif typ in ("subrun_started", "subrun_cancel_requested"):
            # 不带 attempt_id：针对当前 attempt 中已 subrun_start 的 sub-run
            if not self._current:
                raise _err("wrong_attempt", f"{self._phase} 阶段没有当前 attempt，不能发送 {typ}")
            self._subruns.host_sent(msg)
        else:
            raise _err("unknown_type", f"{typ} 不是 session 模式的宿主消息")

    def observe(self, msg: dict[str, Any]) -> None:
        typ = msg["type"]
        if self._handshake:
            raise _err("after_handshake_error", f"handshake_error 之后出现 {typ}")
        if typ == "handshake_error":
            if self._phase != PHASE_READY or self._last_seq != 0:
                raise _err("handshake_error_misplaced", "handshake_error 必须是第一条消息")
            self._phase, self._handshake = PHASE_CLOSED, True
            return
        seq = msg.get("seq")
        if seq != self._last_seq + 1:
            raise _err("seq_invalid", f"seq={seq}，期望 {self._last_seq + 1}")
        self._last_seq += 1
        self._advance(msg, typ, msg.get("attempt_id") or "")

    def _advance(self, msg: dict[str, Any], typ: str, attempt_id: str) -> None:
        if self._before_attempt_phases(msg, typ):
            return
        if typ == "ready":
            raise _err("duplicate_ready", "重复的 ready")
        if typ == "closed":
            raise _err("unexpected_event", "未收到 session_close 却出现 closed")
        if typ == "quiesced":
            if self._phase != PHASE_QUIESCING:
                raise _err("not_idle", f"{self._phase} 阶段出现 quiesced")
            self._phase = PHASE_QUIESCED
            return
        if self._phase == PHASE_QUIESCING:
            raise _err("wrong_attempt", f"quiesce 之后出现 {typ}(attempt_id={attempt_id!r})")
        if not attempt_id:  # 其余事件都属于某个 attempt
            raise _err("missing_field", "attempt_id 不能为空")
        self._check_current(attempt_id)
        self._advance_attempt(msg, typ)

    def _before_attempt_phases(self, msg: dict[str, Any], typ: str) -> bool:
        """处理 init/ready 之前与 close 之后的阶段；返回是否已处理。"""
        if self._phase == PHASE_AWAITING_INIT:
            raise _err("before_ready", f"init 之前出现 {typ}")
        if self._phase == PHASE_READY:
            if typ == "ready":
                check_session_ext(msg)
                self._subruns.check_ready(msg)
                self._phase = PHASE_IDLE
            elif typ == "error":  # 启动失败
                self._phase = PHASE_CLOSED
            else:
                raise _err("before_ready", f"{typ} 出现在 ready 之前")
            return True
        if self._phase == PHASE_CLOSING:
            if typ != "closed":
                raise _err("after_terminal", f"session_close 之后出现 {typ}")
            self._phase = PHASE_CLOSED
            return True
        if self._phase == PHASE_CLOSED:
            raise _err("after_terminal", f"incarnation 结束之后出现 {typ}")
        return False

    def _advance_attempt(self, msg: dict[str, Any], typ: str) -> None:
        if self._phase == PHASE_STARTING:
            if typ != "task_accepted":
                raise _err("unexpected_event", f"task_accepted 之前出现 {typ}")
            self._phase = PHASE_ACTIVE
        elif self._phase == PHASE_ACTIVE:
            self._subruns.observe(msg)
            if typ in _SESSION_TERMINAL:
                self._phase = PHASE_PROPOSED
            elif typ in ("task_accepted", "task_outcome_query", "task_released"):
                raise _err("unexpected_event", f"终态提议之前出现 {typ}")
        elif self._phase == PHASE_PROPOSED:
            if typ == "task_released":
                if not self._outcome_sent:
                    raise _err("unexpected_event", "宿主尚未发送 task_outcome 即出现 task_released")
                self._phase, self._current, self._outcome_sent = PHASE_IDLE, "", False
            elif typ not in ("checkpoint_query", "task_outcome_query"):
                raise _err("after_terminal", f"终态提议或裁决之后出现 {typ}")

    def _check_current(self, attempt_id: str) -> None:
        if not self._current or attempt_id != self._current:
            raise _err(
                "wrong_attempt",
                f"attempt_id={attempt_id!r}，当前为 {self._current!r}（{self._phase}）",
            )
