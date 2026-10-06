"""Worker 协议 v1 的消息校验与编解码：task 模式与 session 扩展。

规则与 Go 侧 internal/protocol 逐条对应，二者共用 protocol/fixtures/v1。
消息以 dict 表示；未知字段忽略，未知类型是错误（规格 §5.3）。
判定顺序：行长 → UTF-8 → JSON 结构限制 → 消息类型 → 类型上限 → 版本 → 键名大小写
→ 字段类型 → 语义规则；session 模式（decode_session_line）最后再检查 session 附加规则。
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from typing import Any

HOST = "host"
WORKER = "worker"

VERSION = 1
BOOTSTRAP_VERSION = 1

MAX_EVENT_BYTES = 1 << 20
MAX_INIT_BYTES = 1 << 20
MAX_CONTROL_BYTES = 16 << 10
MAX_INLINE_STATE_BYTES = 256 << 10
MAX_REFS_PER_CHECKPOINT = 1024
MAX_ARTIFACT_PATH_BYTES = 4096
# 任何消息类型上限中的最大值；超过它的行不解析，直接判为过大。
MAX_LINE_BYTES = max(MAX_EVENT_BYTES, MAX_INIT_BYTES)

# JSON 结构限制（规格 §5.10）：对整行生效，自由格式字段同样受限；违反时为 malformed_json。
MAX_NESTING_DEPTH = 64  # 对象与数组的最大嵌套层数，消息顶层对象计 1
MAX_NUMBER_LITERAL_BYTES = 32  # 数字字面量的最大字符数，含负号、小数点、指数符号与指数正负号

MODE_TASK = "task"
MODE_SESSION = "session"
SCOPE_TASK = "task"
SCOPE_SESSION = "session"
CHECKPOINT_STATUSES = ("committed", "conflict", "rejected", "retryable_error", "not_found")

# session 扩展（规格 §5.4；协调者裁定 A、B：没有 continue/restore 指令，awaiting_input 是终态提议）
SESSION_EXT_VERSION = 1
STAGED_STATE_DIR = "/run/agentbox/restore/"
DIRECTIVE_KINDS = ("finish_now", "answer")
VERDICTS = ("succeeded", "failed", "cancelled", "paused")


class ProtocolError(Exception):
    """带稳定错误码的协议错误。"""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


Pred = Callable[[Any], bool]


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


_INT64_MIN, _INT64_MAX = -(1 << 63), (1 << 63) - 1


def _is_int(v: Any) -> bool:
    """与 Go 一致：整数字段必须落在 int64 范围内。"""
    return isinstance(v, int) and not isinstance(v, bool) and _INT64_MIN <= v <= _INT64_MAX


def _is_bool(v: Any) -> bool:
    return isinstance(v, bool)


def _any(_: Any) -> bool:
    return True


def _list_of(pred: Pred) -> Pred:
    return lambda v: isinstance(v, list) and all(x is None or pred(x) for x in v)


class _Obj:
    """嵌套对象字段的类型谓词；保留字段定义，供键名大小写检查逐层使用。"""

    def __init__(self, spec: dict[str, Pred]) -> None:
        self.spec = spec

    def __call__(self, v: Any) -> bool:
        return isinstance(v, dict) and all(
            v.get(k) is None or pred(v[k]) for k, pred in self.spec.items()
        )


def _obj(spec: dict[str, Pred]) -> Pred:
    return _Obj(spec)


_STRS = _list_of(_is_str)

# sub-run 扩展（规格 §5.4 sub-run 扩展、§13；M4 Plan 14），与 Go 的 internal/protocol/subrun.go 对应
EXTENSION_SUBRUNS = "subruns"
SUBRUNS_EXT_VERSION = 1
MAX_SUBRUN_ID_BYTES = 32
MAX_SUBRUN_DEADLINE = 3_600_000  # ms
MAX_SUBRUN_SUMMARY = 4096  # subrun_end.summary 与 result.subruns[].summary，UTF-8 字节
MAX_SUBRUNS_PER_TASK = 4
MAX_PARENT_STEP_ID_BYTES = 256
SUBRUN_STARTED_STATUSES = ("started", "rejected")
SUBRUN_END_STATUSES = ("succeeded", "failed", "cancelled")
SUBRUN_CANCEL_REASONS = ("deadline", "task_cancel", "policy")
CHECKPOINT_SUBRUN_STATUSES = ("started", "completed", "failed", "cancelled")
# 宿主裁定的状态（resume.subruns[]）与 result.subruns[] 另含 timed_out
SUBRUN_STATES = ("started", "completed", "cancelled", "failed", "timed_out")
_SUBRUN_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")

_CHECKPOINT_SUBRUN = {"subrun_id": _is_str, "status": _is_str, "result_ref": _is_str}
_RESULT_SUBRUN = {"id": _is_str, "status": _is_str, "summary": _is_str}

_HOST = {"type": _is_str, "v": _is_int}
# attempt_id 在 session 模式下由 task 相关事件携带；task 模式不要求，但类型同样检查（与 Go 一致）
_EVENT = {"type": _is_str, "v": _is_int, "seq": _is_int, "ts": _is_str, "attempt_id": _is_str}
_STOP = {**_HOST, "attempt_id": _is_str, "reason": _is_str, "grace_ms": _is_int}
_GRACE = {**_HOST, "grace_ms": _is_int}
_RESUME = {
    "checkpoint_id": _is_str,
    "step_id": _is_str,
    "state": _any,
    "state_ref": _is_str,
    "refs": _STRS,
    "subruns": _list_of(_obj(_CHECKPOINT_SUBRUN)),
}
_SESSION_RESUME = {
    "checkpoint_id": _is_str,
    "state": _any,
    "staged_state_path": _is_str,
    "refs": _STRS,
    "state_ref": _is_str,  # 只用于拒绝：session_resume 不携带 state_ref
}
_SESSION_STATE = {"checkpoint_id": _is_str, "state": _any, "state_ref": _is_str, "refs": _STRS}

_FIELD_TYPES: dict[str, dict[str, Pred]] = {
    "init": {
        "type": _is_str,
        "bootstrap": _is_int,
        "protocol_versions": _list_of(_is_int),
        "mode": _is_str,
        "task_id": _is_str,
        "attempt_id": _is_str,
        "attempt_no": _is_int,
        "traceparent": _is_str,
        "config": _any,
        "config_version": _is_str,
        "budget_limits": _any,
        "input_refs": _STRS,
        "out_dir": _is_str,
        "resume": _obj(_RESUME),
        "session_id": _is_str,
        "incarnation_id": _is_str,
        "session_resume": _obj(_SESSION_RESUME),
        "extensions": _STRS,
    },
    "checkpoint_result": {
        **_HOST,
        "attempt_id": _is_str,
        "checkpoint_id": _is_str,
        "scope": _is_str,
        "status": _is_str,
        "code": _is_str,
    },
    "artifact_result": {
        **_HOST,
        "attempt_id": _is_str,
        "artifact_id": _is_str,
        "status": _is_str,
        "version": _is_int,
        "sha256": _is_str,
        "code": _is_str,
    },
    "cancel": _STOP,
    "pause": _STOP,
    "task_start": {
        **_HOST,
        "task_id": _is_str,
        "attempt_id": _is_str,
        "attempt_no": _is_int,
        "traceparent": _is_str,
        "config": _any,
        "config_version": _is_str,
        "budget_limits": _any,
        "input_refs": _STRS,
        "out_dir": _is_str,
        "base_session_checkpoint_id": _is_str,
        "resume": _obj(_RESUME),
        "directive": _obj({"kind": _is_str, "question_id": _is_str, "answers": _any}),
        "restored_from_task_id": _is_str,
        "carryover": _obj({"task_id": _is_str, "checkpoint_ref": _is_str}),
    },
    "task_outcome": {
        **_HOST,
        "attempt_id": _is_str,
        "verdict": _is_str,
        "committed_session_checkpoint_id": _is_str,
    },
    "quiesce": _GRACE,
    "session_close": _GRACE,
    "ready": {
        **_EVENT,
        "protocol_version": _is_int,
        "mode": _is_str,
        "worker": _obj({"name": _is_str, "version": _is_str}),
        "capabilities": _STRS,
        "session_ext": _is_int,
        "subruns": _is_int,
    },
    "task_accepted": _EVENT,
    "task_outcome_query": _EVENT,
    "task_released": _EVENT,
    "quiesced": {**_EVENT, "session_checkpoint_id": _is_str},
    "closed": _EVENT,
    "awaiting_input": {**_EVENT, "checkpoint_id": _is_str, "question_id": _is_str},
    "progress": {
        **_EVENT,
        "step_id": _is_str,
        "kind": _is_str,
        "message": _is_str,
        "data": _any,
        "subrun_id": _is_str,
    },
    "artifact": {
        **_EVENT,
        "artifact_id": _is_str,
        "path": _is_str,
        "declared_sha256": _is_str,
        "declared_size": _is_int,
        "media_type": _is_str,
        "visibility": _is_str,
    },
    "checkpoint": {
        **_EVENT,
        "checkpoint_id": _is_str,
        "scope": _is_str,
        "step_id": _is_str,
        "state": _any,
        "state_ref": _is_str,
        "refs": _STRS,
        "subruns": _list_of(_obj(_CHECKPOINT_SUBRUN)),
    },
    "checkpoint_query": {**_EVENT, "checkpoint_id": _is_str, "scope": _is_str},
    "paused": {**_EVENT, "checkpoint_id": _is_str},
    "result": {
        **_EVENT,
        "summary": _is_str,
        "outputs": _STRS,
        "session_state": _obj(_SESSION_STATE),
        "subruns": _list_of(_obj(_RESULT_SUBRUN)),
    },
    "error": {**_EVENT, "code": _is_str, "message": _is_str, "retryable": _is_bool},
    "handshake_error": {"type": _is_str, "bootstrap": _is_int, "code": _is_str},
    "subrun_start": {
        **_EVENT,
        "subrun_id": _is_str,
        "parent_step_id": _is_str,
        "budget_cap_micro": _is_int,
        "deadline_ms": _is_int,
    },
    "subrun_started": {**_HOST, "subrun_id": _is_str, "status": _is_str, "code": _is_str},
    "subrun_end": {**_EVENT, "subrun_id": _is_str, "status": _is_str, "summary": _is_str},
    "subrun_cancel": {**_EVENT, "subrun_id": _is_str, "reason": _is_str},
    "subrun_cancel_requested": {**_HOST, "subrun_id": _is_str, "reason": _is_str},
}


def _s(msg: dict[str, Any], key: str) -> str:
    value = msg.get(key)
    return value if isinstance(value, str) else ""


def _n(msg: dict[str, Any], key: str) -> int:
    value = msg.get(key)
    return value if _is_int(value) else 0


def _required(field: str, value: str) -> None:
    if value == "":
        raise ProtocolError("missing_field", f"{field} 不能为空")


def _one_of(field: str, value: str, *allowed: str) -> None:
    if value == "":
        raise ProtocolError("missing_field", f"{field} 不能为空")
    if value not in allowed:
        raise ProtocolError("invalid_field", f"{field}={value!r} 不在允许范围 {allowed} 内")


def _check_version(msg: dict[str, Any]) -> None:
    """v 必须是整数形式的 1（不接受 1.0、布尔值或缺失），否则为 version_mismatch。"""
    v = msg.get("v")
    if not (_is_int(v) and v == VERSION):
        raise ProtocolError("version_mismatch", f"v={v!r}，期望 {VERSION}")


def _check_event(msg: dict[str, Any]) -> None:
    _check_version(msg)
    if _n(msg, "seq") < 1:
        raise ProtocolError("invalid_field", f"seq={_n(msg, 'seq')}，必须 ≥ 1")


def valid_sha256(s: str) -> bool:
    return len(s) == 64 and all(c in "0123456789abcdef" for c in s)


def valid_artifact_path(path: str) -> bool:
    """相对、非空、无 NUL、不超过上限、不含空、. 或 .. 分量（规格 §5.6）。"""
    if not path or len(path.encode("utf-8", "surrogatepass")) > MAX_ARTIFACT_PATH_BYTES:
        return False
    if "\x00" in path or path.startswith("/"):
        return False
    return all(segment not in ("", ".", "..") for segment in path.split("/"))


def _check_state(msg: dict[str, Any]) -> None:
    has_state = msg.get("state") is not None
    state_ref = _s(msg, "state_ref")
    if has_state == (state_ref != ""):
        raise ProtocolError("invalid_field", "state 与 state_ref 必须且只能提供一个")
    if state_ref:
        if not valid_sha256(state_ref):
            raise ProtocolError("invalid_field", "state_ref 不是合法的 sha256")
        return
    _check_inline_state(msg["state"])


def _check_inline_state(state: Any) -> None:
    compact = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    # 孤立代理项按 3 字节计，与 Go 替换成的 U+FFFD 相同
    size = len(compact.encode("utf-8", "surrogatepass"))
    if size > MAX_INLINE_STATE_BYTES:
        raise ProtocolError(
            "state_too_large", f"inline state {size} 字节，上限 {MAX_INLINE_STATE_BYTES}"
        )


def _check_refs(field: str, refs: list[Any] | None, limit: int | None) -> None:
    refs = refs or []
    if limit is not None and len(refs) > limit:
        raise ProtocolError("too_many_refs", f"{field} 有 {len(refs)} 个，上限 {limit}")
    for ref in refs:
        if not valid_sha256(ref or ""):
            raise ProtocolError("invalid_field", f"{field} 中有不合法的 sha256")


def _v_init(m: dict[str, Any]) -> None:
    if _n(m, "bootstrap") != BOOTSTRAP_VERSION:
        raise ProtocolError("invalid_field", f"bootstrap={_n(m, 'bootstrap')}，期望 1")
    if not m.get("protocol_versions"):
        raise ProtocolError("missing_field", "protocol_versions 不能为空")
    mode = _s(m, "mode")
    _one_of("mode", mode, MODE_TASK, MODE_SESSION)
    _check_extensions(m.get("extensions"))
    if mode == MODE_SESSION:
        _v_session_init(m)
        return
    _check_attempt_fields(m)


def _check_attempt_fields(m: dict[str, Any]) -> None:
    """task 模式 init 与 task_start 共用的 attempt 字段规则（含 input_refs 与 resume）。"""
    for name in ("task_id", "attempt_id", "out_dir"):
        _required(name, _s(m, name))
    if _n(m, "attempt_no") < 1:
        raise ProtocolError("invalid_field", f"attempt_no={_n(m, 'attempt_no')}，必须 ≥ 1")
    _check_refs("input_refs", m.get("input_refs"), None)
    resume = m.get("resume")
    if resume is None:
        return
    _required("resume.checkpoint_id", _s(resume, "checkpoint_id"))
    _required("resume.step_id", _s(resume, "step_id"))
    _check_state(resume)
    _check_refs("resume.refs", resume.get("refs"), MAX_REFS_PER_CHECKPOINT)
    _check_subrun_entries(
        "resume.subruns", resume.get("subruns"), "subrun_id", SUBRUN_STATES, with_ref=True
    )


def _v_session_init(m: dict[str, Any]) -> None:
    _required("session_id", _s(m, "session_id"))
    _required("incarnation_id", _s(m, "incarnation_id"))
    resume = m.get("session_resume")
    if resume is None:
        return
    _required("session_resume.checkpoint_id", _s(resume, "checkpoint_id"))
    if _s(resume, "state_ref"):
        raise ProtocolError(
            "invalid_field", "session_resume 不携带 state_ref，冷恢复以 staged_state_path 读取"
        )
    path = _s(resume, "staged_state_path")
    if (resume.get("state") is not None) == (path != ""):
        raise ProtocolError(
            "invalid_field", "session_resume 的 state 与 staged_state_path 必须且只能提供一个"
        )
    if path:
        if not (path.startswith(STAGED_STATE_DIR) and valid_sha256(path[len(STAGED_STATE_DIR) :])):
            raise ProtocolError(
                "invalid_field", f"staged_state_path 必须是 {STAGED_STATE_DIR}<sha256>"
            )
    else:
        _check_inline_state(resume["state"])
    _check_refs("session_resume.refs", resume.get("refs"), MAX_REFS_PER_CHECKPOINT)


def _v_task_start(m: dict[str, Any]) -> None:
    _check_version(m)
    _check_attempt_fields(m)
    directive = m.get("directive")
    if directive is not None:
        _v_directive(directive)
    carryover = m.get("carryover")
    if carryover is not None:
        _required("carryover.task_id", _s(carryover, "task_id"))
        _required("carryover.checkpoint_ref", _s(carryover, "checkpoint_ref"))
        if not valid_sha256(_s(carryover, "checkpoint_ref")):
            raise ProtocolError("invalid_field", "carryover.checkpoint_ref 不是合法的 sha256")


def _v_directive(d: dict[str, Any]) -> None:
    kind = _s(d, "kind")
    _one_of("directive.kind", kind, *DIRECTIVE_KINDS)
    if kind != "answer":
        return
    _required("directive.question_id", _s(d, "question_id"))
    answers = d.get("answers")
    if answers is None:
        raise ProtocolError("missing_field", "directive.answers 不能为空")
    if not isinstance(answers, list) or not answers:
        raise ProtocolError("invalid_field", "directive.answers 必须是非空数组")


def _v_task_outcome(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("attempt_id", _s(m, "attempt_id"))
    _one_of("verdict", _s(m, "verdict"), *VERDICTS)


def _v_grace(m: dict[str, Any]) -> None:
    _check_version(m)
    if _n(m, "grace_ms") < 0:
        raise ProtocolError("invalid_field", f"grace_ms={_n(m, 'grace_ms')}，必须 ≥ 0")


def _v_attempt_event(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("attempt_id", _s(m, "attempt_id"))


def _v_awaiting_input(m: dict[str, Any]) -> None:
    _v_attempt_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _required("question_id", _s(m, "question_id"))


def _v_checkpoint_result(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK, SCOPE_SESSION)
    _one_of("status", _s(m, "status"), *CHECKPOINT_STATUSES)


def _v_artifact_result(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("artifact_id", _s(m, "artifact_id"))
    status = _s(m, "status")
    _one_of("status", status, "saved", "rejected")
    if status == "rejected":
        _required("code", _s(m, "code"))
        return
    if _n(m, "version") < 1:
        raise ProtocolError("invalid_field", f"saved 的 version={_n(m, 'version')}，必须 ≥ 1")
    if not valid_sha256(_s(m, "sha256")):
        raise ProtocolError("invalid_field", "saved 的 sha256 不合法")


def _v_stop(m: dict[str, Any]) -> None:
    _check_version(m)
    _required("attempt_id", _s(m, "attempt_id"))
    if _n(m, "grace_ms") < 0:
        raise ProtocolError("invalid_field", f"grace_ms={_n(m, 'grace_ms')}，必须 ≥ 0")


def _v_ready(m: dict[str, Any]) -> None:
    _check_event(m)
    if _n(m, "protocol_version") != VERSION:
        raise ProtocolError("version_mismatch", f"protocol_version={_n(m, 'protocol_version')}")
    _one_of("mode", _s(m, "mode"), MODE_TASK, MODE_SESSION)
    _required("worker.name", _s(m.get("worker") or {}, "name"))
    if _n(m, "subruns") not in (0, SUBRUNS_EXT_VERSION):
        raise ProtocolError("invalid_field", f"subruns={_n(m, 'subruns')}，只能是 0 或 1")


def _v_progress(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("kind", _s(m, "kind"))
    subrun_id = _s(m, "subrun_id")
    if subrun_id and not valid_subrun_id(subrun_id):
        raise ProtocolError("invalid_field", f"subrun_id={subrun_id!r} 不合法")


def _v_artifact(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("artifact_id", _s(m, "artifact_id"))
    if not valid_artifact_path(_s(m, "path")):
        raise ProtocolError("path_invalid", f"产物路径 {_s(m, 'path')!r} 不合法")
    if not valid_sha256(_s(m, "declared_sha256")):
        raise ProtocolError("invalid_field", "declared_sha256 不合法")
    if _n(m, "declared_size") < 0:
        raise ProtocolError("invalid_field", "declared_size 必须 ≥ 0")
    _required("media_type", _s(m, "media_type"))
    _one_of("visibility", _s(m, "visibility"), "output", "internal")


def _v_checkpoint(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _required("step_id", _s(m, "step_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK)  # session scope 只能经 result 提议写入
    _check_state(m)
    _check_refs("refs", m.get("refs"), MAX_REFS_PER_CHECKPOINT)
    entries = _check_subrun_entries(
        "subruns", m.get("subruns"), "subrun_id", CHECKPOINT_SUBRUN_STATUSES, with_ref=True
    )
    total = len(m.get("refs") or []) + sum(1 for e in entries if _s(e, "result_ref"))
    if total > MAX_REFS_PER_CHECKPOINT:
        raise ProtocolError(
            "too_many_refs",
            f"refs 与 subruns[].result_ref 合计 {total} 个，上限 {MAX_REFS_PER_CHECKPOINT}",
        )


def _v_checkpoint_query(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))
    _one_of("scope", _s(m, "scope"), SCOPE_TASK, SCOPE_SESSION)


def _v_paused(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("checkpoint_id", _s(m, "checkpoint_id"))


def _v_result(m: dict[str, Any]) -> None:
    _check_event(m)
    for output in m.get("outputs") or []:
        if not output:
            raise ProtocolError("invalid_field", "outputs 中不能有空的 artifact_id")
    for entry in _check_subrun_entries(
        "result.subruns", m.get("subruns"), "id", SUBRUN_STATES, with_ref=False
    ):
        _check_summary("result.subruns[].summary", _s(entry, "summary"))


# ---- sub-run 扩展 ----


def valid_subrun_id(subrun_id: str) -> bool:
    """^[a-z0-9][a-z0-9_-]{0,31}$ 且不等于 root（同 Go 的 validSubrunID）。"""
    return subrun_id != "root" and _SUBRUN_ID.fullmatch(subrun_id) is not None


def _check_subrun_id(field: str, subrun_id: str) -> None:
    _required(field, subrun_id)
    if not valid_subrun_id(subrun_id):
        raise ProtocolError(
            "invalid_field",
            f"{field}={subrun_id!r} 须匹配 ^[a-z0-9][a-z0-9_-]{{0,31}}$ 且不是 root",
        )


def _utf8_len(s: str) -> int:
    return len(s.encode("utf-8", "surrogatepass"))  # 孤立代理项按 3 字节计，同 Go 的 U+FFFD


def _check_summary(field: str, summary: str) -> None:
    if _utf8_len(summary) > MAX_SUBRUN_SUMMARY:
        raise ProtocolError(
            "invalid_field", f"{field} {_utf8_len(summary)} 字节，上限 {MAX_SUBRUN_SUMMARY}"
        )


def _check_subrun_entries(
    field: str, items: list[Any] | None, id_key: str, statuses: tuple[str, ...], *, with_ref: bool
) -> list[dict[str, Any]]:
    """subruns[]：至多 4 项、ID 合法且不重复、status 在允许范围内；with_ref 时 completed 必须带
    合法 sha256 的 result_ref，其他状态不得带。返回各项（null 项按空对象处理，同 Go 的零值）。"""
    entries = [item if isinstance(item, dict) else {} for item in items or []]
    if len(entries) > MAX_SUBRUNS_PER_TASK:
        raise ProtocolError(
            "invalid_field", f"{field} 有 {len(entries)} 项，上限 {MAX_SUBRUNS_PER_TASK}"
        )
    seen: set[str] = set()
    for entry in entries:
        subrun_id, status = _s(entry, id_key), _s(entry, "status")
        _check_subrun_id(f"{field}[].id", subrun_id)
        _one_of(f"{field}[].status", status, *statuses)
        if subrun_id in seen:
            raise ProtocolError("invalid_field", f"{field} 中 ID {subrun_id!r} 重复")
        seen.add(subrun_id)
        if not with_ref:
            continue
        ref = _s(entry, "result_ref")
        if status == "completed":
            if not valid_sha256(ref):
                raise ProtocolError(
                    "invalid_field",
                    f"{field} 中 completed 的 {subrun_id!r} 必须带合法 sha256 的 result_ref",
                )
        elif ref:
            raise ProtocolError(
                "invalid_field", f"{field} 中 {status} 的 {subrun_id!r} 不得带 result_ref"
            )
    return entries


def _check_extensions(extensions: list[Any] | None) -> None:
    """init.extensions 每项非空（未知扩展名忽略，便于前向兼容）。"""
    if any(not e for e in extensions or []):
        raise ProtocolError("invalid_field", "extensions 中不能有空串")


def requests_subruns(extensions: list[Any] | None) -> bool:
    """init.extensions 是否请求了 sub-run 扩展。"""
    return EXTENSION_SUBRUNS in (extensions or [])


def _v_subrun_start(m: dict[str, Any]) -> None:
    _check_event(m)
    _check_subrun_id("subrun_id", _s(m, "subrun_id"))
    parent = _s(m, "parent_step_id")
    _required("parent_step_id", parent)
    if _utf8_len(parent) > MAX_PARENT_STEP_ID_BYTES:
        raise ProtocolError(
            "invalid_field",
            f"parent_step_id {_utf8_len(parent)} 字节，上限 {MAX_PARENT_STEP_ID_BYTES}",
        )
    deadline = _n(m, "deadline_ms")
    if not 1 <= deadline <= MAX_SUBRUN_DEADLINE:
        raise ProtocolError(
            "invalid_field", f"deadline_ms={deadline}，须在 1–{MAX_SUBRUN_DEADLINE} 之间"
        )
    cap = m.get("budget_cap_micro")
    if cap is not None and cap < 0:
        raise ProtocolError("invalid_field", f"budget_cap_micro={cap}，必须 ≥ 0")


def _v_subrun_started(m: dict[str, Any]) -> None:
    _check_version(m)
    _check_subrun_id("subrun_id", _s(m, "subrun_id"))
    status = _s(m, "status")
    _one_of("status", status, *SUBRUN_STARTED_STATUSES)
    if status == "rejected" and not _s(m, "code"):
        raise ProtocolError("invalid_field", "rejected 的 subrun_started 必须带 code")
    if status == "started" and _s(m, "code"):
        raise ProtocolError("invalid_field", "started 的 subrun_started 不得带 code")


def _v_subrun_end(m: dict[str, Any]) -> None:
    _check_event(m)
    _check_subrun_id("subrun_id", _s(m, "subrun_id"))
    _one_of("status", _s(m, "status"), *SUBRUN_END_STATUSES)
    _check_summary("summary", _s(m, "summary"))


def _v_subrun_cancel(m: dict[str, Any]) -> None:
    _check_event(m)
    _check_subrun_id("subrun_id", _s(m, "subrun_id"))
    _required("reason", _s(m, "reason"))


def _v_subrun_cancel_requested(m: dict[str, Any]) -> None:
    _check_version(m)
    _check_subrun_id("subrun_id", _s(m, "subrun_id"))
    _one_of("reason", _s(m, "reason"), *SUBRUN_CANCEL_REASONS)


def _v_error(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("code", _s(m, "code"))


def _v_handshake_error(m: dict[str, Any]) -> None:
    if _n(m, "bootstrap") != BOOTSTRAP_VERSION:
        raise ProtocolError("invalid_field", f"bootstrap={_n(m, 'bootstrap')}，期望 1")
    _required("code", _s(m, "code"))


_VALIDATORS: dict[str, dict[str, Callable[[dict[str, Any]], None]]] = {
    HOST: {
        "init": _v_init,
        "checkpoint_result": _v_checkpoint_result,
        "artifact_result": _v_artifact_result,
        "cancel": _v_stop,
        "pause": _v_stop,
        "subrun_started": _v_subrun_started,
        "subrun_cancel_requested": _v_subrun_cancel_requested,
    },
    WORKER: {
        "subrun_start": _v_subrun_start,
        "subrun_end": _v_subrun_end,
        "subrun_cancel": _v_subrun_cancel,
        "ready": _v_ready,
        "progress": _v_progress,
        "artifact": _v_artifact,
        "checkpoint": _v_checkpoint,
        "checkpoint_query": _v_checkpoint_query,
        "paused": _v_paused,
        "result": _v_result,
        "error": _v_error,
        "handshake_error": _v_handshake_error,
    },
}

# session 模式可用的消息：task 模式的全部消息加上 session 扩展
_SESSION_VALIDATORS: dict[str, dict[str, Callable[[dict[str, Any]], None]]] = {
    HOST: {
        **_VALIDATORS[HOST],
        "task_start": _v_task_start,
        "task_outcome": _v_task_outcome,
        "quiesce": _v_grace,
        "session_close": _v_grace,
    },
    WORKER: {
        **_VALIDATORS[WORKER],
        "task_accepted": _v_attempt_event,
        "task_outcome_query": _v_attempt_event,
        "task_released": _v_attempt_event,
        "quiesced": _check_event,
        "closed": _check_event,
        "awaiting_input": _v_awaiting_input,
    },
}

# session 模式下必须带 attempt_id 的 task 模式消息；error 的 attempt_id 由事件流按阶段检查
# （ready 之前的启动失败不属于任何 attempt）
_SESSION_ATTEMPT_REQUIRED = frozenset(
    (
        "checkpoint_result",
        "artifact_result",
        "progress",
        "artifact",
        "checkpoint",
        "checkpoint_query",
        "paused",
        "result",
        "subrun_start",
        "subrun_end",
        "subrun_cancel",
    )
)


def check_session_ext(m: dict[str, Any]) -> None:
    """session 模式的 ready 必须是 mode=session 且确认 session_ext: 1。"""
    _one_of("mode", _s(m, "mode"), MODE_SESSION)
    ext = _n(m, "session_ext")
    if ext == 0:
        raise ProtocolError(
            "session_ext_missing", f"session 模式的 ready 必须带 session_ext: {SESSION_EXT_VERSION}"
        )
    if ext != SESSION_EXT_VERSION:
        raise ProtocolError("invalid_field", f"session_ext={ext}，期望 {SESSION_EXT_VERSION}")


def _session_rules(typ: str, m: dict[str, Any]) -> None:
    """task 模式消息在 session 模式下的附加规则：各类型自身规则之后检查
    （同 Go 的 validateSessionMode）。"""
    if typ == "init":
        _one_of("mode", _s(m, "mode"), MODE_SESSION)
    elif typ == "ready":
        check_session_ext(m)
    elif typ in _SESSION_ATTEMPT_REQUIRED:
        _required("attempt_id", _s(m, "attempt_id"))
        state = m.get("session_state") if typ == "result" else None
        if state is not None:
            _required("session_state.checkpoint_id", _s(state, "checkpoint_id"))
            _check_state(state)
            _check_refs("session_state.refs", state.get("refs"), MAX_REFS_PER_CHECKPOINT)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"不允许的 JSON 常量 {name}")


# 展开写法加占有量词（Python ≥ 3.11）：对未闭合的字符串也是线性时间，不会回溯爆炸
_JSON_STRING = re.compile(r'"[^"\\]*+(?:\\.[^"\\]*+)*+"')
_BRACKET = re.compile(r"[\[\]{}]")


def _check_depth(text: str) -> None:
    """在构建任何对象之前，按原始文本计算对象与数组的嵌套层数（字符串内的括号不计）。"""
    depth = 0
    for bracket in _BRACKET.finditer(_JSON_STRING.sub('""', text)):
        if bracket.group() in "[{":
            depth += 1
            if depth > MAX_NESTING_DEPTH:
                raise ValueError(f"嵌套超过 {MAX_NESTING_DEPTH} 层")
        else:
            depth -= 1


_LONE_SURROGATE = re.compile("[\ud800-\udfff]")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """按解码后的键名比较：seq 与其转义写法 \\u0073eq 是同一个键。

    json 已把成对的代理项合并为一个字符，剩下的都是孤立代理项；与 Go 一致，每个按 U+FFFD 比较。
    """
    obj: dict[str, Any] = {}
    seen: set[str] = set()
    for key, value in pairs:
        normalized = _LONE_SURROGATE.sub("\ufffd", key)
        if normalized in seen:
            raise ValueError(f"重复的键 {key!r}")
        seen.add(normalized)
        obj[key] = value
    return obj


def _check_literal(lit: str) -> None:
    if len(lit) > MAX_NUMBER_LITERAL_BYTES:
        raise ValueError(f"数字字面量 {len(lit)} 个字符，上限 {MAX_NUMBER_LITERAL_BYTES}")


def _parse_int(lit: str) -> int:
    _check_literal(lit)
    return int(lit)


def _parse_float(lit: str) -> float:
    """浮点字面量不得上溢为无穷，非零值不得下溢为零（与 Go 的 strconv.ParseFloat 结果一致）。"""
    _check_literal(lit)
    value = float(lit)
    if math.isinf(value):
        raise ValueError(f"数字 {lit} 超出 float64 范围")
    mantissa = re.split("[eE]", lit)[0]
    if value == 0.0 and re.search("[1-9]", mantissa):
        raise ValueError(f"数字 {lit} 下溢为零")
    return value


def parse_json(raw: bytes) -> Any:
    """按协议的 JSON 结构限制解析一行：严格 UTF-8，深度、重复键与数字字面量受限。"""
    try:
        text = raw.decode("utf-8")
        _check_depth(text)
        return json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_int=_parse_int,
            parse_float=_parse_float,
            parse_constant=_reject_constant,
        )
    except (ValueError, RecursionError) as exc:  # 含非法 UTF-8、BOM 与语法错误
        raise ProtocolError("malformed_json", str(exc)) from exc


def _check_size(direction: str, typ: str, size: int) -> None:
    if direction == WORKER:
        limit = MAX_EVENT_BYTES
    elif typ in ("init", "task_start"):  # task_start 携带 config 与 resume.state，同 init
        limit = MAX_INIT_BYTES
    else:
        limit = MAX_CONTROL_BYTES
    if size > limit:
        raise ProtocolError("message_too_large", f"{typ} {size} 字节，上限 {limit}")


def _check_types(typ: str, msg: dict[str, Any]) -> None:
    for key, pred in _FIELD_TYPES[typ].items():
        value = msg.get(key)
        if value is not None and not pred(value):
            raise ProtocolError("invalid_field", f"{key} 类型不正确")


_FOLD = str.maketrans({"\u212a": "k", "\u017f": "s"})


def _fold(key: str) -> str:
    """与 Go 的 strings.EqualFold 对 ASCII 字段名的判定一致：ASCII 大小写，加 U+212A 与 U+017F。"""
    return "".join(c.lower() if c.isascii() else c for c in key.translate(_FOLD))


def _check_key_case(spec: dict[str, Pred], obj: dict[str, Any]) -> None:
    """拒绝与已定义字段只差大小写的键；按消息类型逐层检查，键按字典序检查。"""
    for key in sorted(obj):
        pred = spec.get(key)
        if pred is not None:
            if isinstance(pred, _Obj) and isinstance(obj[key], dict):
                _check_key_case(pred.spec, obj[key])
            continue
        folded = _fold(key)
        for name in spec:
            if folded == name:
                raise ProtocolError("invalid_field", f"键 {key!r} 与字段 {name!r} 只差大小写")


def _validator_for(
    direction: str, typ: Any, session: bool = False
) -> Callable[[dict[str, Any]], None]:
    registry = _SESSION_VALIDATORS if session else _VALIDATORS
    validator = registry[direction].get(typ) if isinstance(typ, str) else None
    if validator is None:
        raise ProtocolError("unknown_type", repr(typ))
    return validator


def decode_line(direction: str, line: bytes | str) -> dict[str, Any]:
    """解析并校验一行 task 模式消息（不含行尾换行符）。行必须是严格的 UTF-8，不含 BOM。

    task 模式不认识 session 专属类型（unknown_type）；session 模式使用 decode_session_line。
    """
    return _decode(direction, line, session=False)


def decode_session_line(direction: str, line: bytes | str) -> dict[str, Any]:
    """按 session 模式解析并校验一行消息：判定顺序同 decode_line，最后检查 session 附加规则。"""
    return _decode(direction, line, session=True)


def encode_line(direction: str, msg: dict[str, Any]) -> bytes:
    """校验并编码一条 task 模式消息（不含行尾换行符）。"""
    return _encode(direction, msg, session=False)


def encode_session_line(direction: str, msg: dict[str, Any]) -> bytes:
    """按 session 模式校验并编码一条消息。"""
    return _encode(direction, msg, session=True)


def _decode(direction: str, line: bytes | str, *, session: bool) -> dict[str, Any]:
    try:
        raw = line.encode("utf-8") if isinstance(line, str) else line
    except UnicodeEncodeError as exc:  # str 中含孤立代理项字符
        raise ProtocolError("malformed_json", "行不是合法的 UTF-8") from exc
    if len(raw) > MAX_LINE_BYTES:
        raise ProtocolError("message_too_large", f"{len(raw)} 字节，上限 {MAX_LINE_BYTES}")
    msg = parse_json(raw)
    if not isinstance(msg, dict):
        raise ProtocolError("malformed_json", "消息必须是 JSON 对象")
    typ = msg.get("type")
    validator = _validator_for(direction, typ, session)
    _check_size(direction, typ, len(raw))
    spec = _FIELD_TYPES[typ]
    if "v" in spec:
        _check_version(msg)
    _check_key_case(spec, msg)
    _check_types(typ, msg)
    validator(msg)
    if session:
        _session_rules(typ, msg)
    return msg


def _encode(direction: str, msg: dict[str, Any], *, session: bool) -> bytes:
    typ = msg.get("type")
    validator = _validator_for(direction, typ, session)
    _check_types(typ, msg)
    validator(msg)
    if session:
        _session_rules(typ, msg)
    try:
        text = json.dumps(msg, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (ValueError, RecursionError) as exc:  # NaN、Infinity 不是合法 JSON；嵌套过深无法序列化
        raise ProtocolError("invalid_field", f"消息含有 JSON 不支持的数值：{exc}") from exc
    try:
        line = text.encode("utf-8")
    except UnicodeEncodeError as exc:  # 字符串中含孤立代理项
        raise ProtocolError("invalid_field", "消息含有孤立代理项，无法编码为 UTF-8") from exc
    try:  # 编码结果必须能通过对端的 JSON 结构限制（深度、数字字面量等）
        parse_json(line)
    except ProtocolError as exc:
        raise ProtocolError("invalid_field", f"消息违反 JSON 结构限制：{exc.detail}") from exc
    _check_size(direction, typ, len(line))
    return line
