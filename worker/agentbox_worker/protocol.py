"""Worker 协议 v1（task 模式）的消息校验与编解码。

规则与 Go 侧 internal/protocol 逐条对应，二者共用 protocol/fixtures/v1。
消息以 dict 表示；未知字段忽略，未知类型是错误（规格 §5.3）。
判定顺序：行长 → UTF-8 → JSON 结构限制 → 消息类型 → 类型上限 → 版本 → 键名大小写
→ 字段类型 → 语义规则。
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
_HOST = {"type": _is_str, "v": _is_int}
_EVENT = {"type": _is_str, "v": _is_int, "seq": _is_int, "ts": _is_str}
_STOP = {**_HOST, "attempt_id": _is_str, "reason": _is_str, "grace_ms": _is_int}
_RESUME = {
    "checkpoint_id": _is_str,
    "step_id": _is_str,
    "state": _any,
    "state_ref": _is_str,
    "refs": _STRS,
}

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
    },
    "checkpoint_result": {
        **_HOST,
        "checkpoint_id": _is_str,
        "scope": _is_str,
        "status": _is_str,
        "code": _is_str,
    },
    "artifact_result": {
        **_HOST,
        "artifact_id": _is_str,
        "status": _is_str,
        "version": _is_int,
        "sha256": _is_str,
        "code": _is_str,
    },
    "cancel": _STOP,
    "pause": _STOP,
    "ready": {
        **_EVENT,
        "protocol_version": _is_int,
        "mode": _is_str,
        "worker": _obj({"name": _is_str, "version": _is_str}),
        "capabilities": _STRS,
    },
    "progress": {**_EVENT, "step_id": _is_str, "kind": _is_str, "message": _is_str, "data": _any},
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
    },
    "checkpoint_query": {**_EVENT, "checkpoint_id": _is_str, "scope": _is_str},
    "paused": {**_EVENT, "checkpoint_id": _is_str},
    "result": {**_EVENT, "summary": _is_str, "outputs": _STRS},
    "error": {**_EVENT, "code": _is_str, "message": _is_str, "retryable": _is_bool},
    "handshake_error": {"type": _is_str, "bootstrap": _is_int, "code": _is_str},
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
    compact = json.dumps(msg["state"], ensure_ascii=False, separators=(",", ":"))
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
    if mode != MODE_TASK:
        return  # session 扩展的字段由后续里程碑校验
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


def _v_progress(m: dict[str, Any]) -> None:
    _check_event(m)
    _required("kind", _s(m, "kind"))


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
    },
    WORKER: {
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


def _reject_constant(name: str) -> Any:
    raise ValueError(f"不允许的 JSON 常量 {name}")


_JSON_STRING = re.compile(r'"(?:[^"\\]+|\\.)*"')
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


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """按解码后的键名比较：seq 与其转义写法 \\u0073eq 是同一个键。"""
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"重复的键 {key!r}")
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
    elif typ == "init":
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


def _validator_for(direction: str, typ: Any) -> Callable[[dict[str, Any]], None]:
    validator = _VALIDATORS[direction].get(typ) if isinstance(typ, str) else None
    if validator is None:
        raise ProtocolError("unknown_type", repr(typ))
    return validator


def decode_line(direction: str, line: bytes | str) -> dict[str, Any]:
    """解析并校验一行消息（不含行尾换行符）。行必须是严格的 UTF-8，不含 BOM。"""
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
    validator = _validator_for(direction, typ)
    _check_size(direction, typ, len(raw))
    spec = _FIELD_TYPES[typ]
    if "v" in spec:
        _check_version(msg)
    _check_key_case(spec, msg)
    _check_types(typ, msg)
    validator(msg)
    return msg


def encode_line(direction: str, msg: dict[str, Any]) -> bytes:
    """校验并编码一条消息（不含行尾换行符）。"""
    typ = msg.get("type")
    validator = _validator_for(direction, typ)
    _check_types(typ, msg)
    validator(msg)
    try:
        text = json.dumps(msg, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:  # NaN、Infinity 不是合法 JSON
        raise ProtocolError("invalid_field", f"消息含有 JSON 不支持的数值：{exc}") from exc
    try:
        line = text.encode("utf-8")
    except UnicodeEncodeError as exc:  # 字符串中含孤立代理项
        raise ProtocolError("invalid_field", "消息含有孤立代理项，无法编码为 UTF-8") from exc
    _check_size(direction, typ, len(line))
    return line
