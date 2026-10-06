"""协议层：跨语言 fixtures 回放与按常量生成的大小上限（规格 §5.10、§5.11）。"""

import json
import time

import pytest
from protocol_fixtures import line_bytes, load_messages, load_scenarios

from agentbox_worker.protocol import (
    HOST,
    MAX_CONTROL_BYTES,
    MAX_EVENT_BYTES,
    MAX_INIT_BYTES,
    MAX_INLINE_STATE_BYTES,
    MAX_REFS_PER_CHECKPOINT,
    WORKER,
    ProtocolError,
    decode_line,
    encode_line,
)
from agentbox_worker.stream import StreamChecker

MESSAGES = load_messages()
SCENARIOS = load_scenarios()
assert MESSAGES["valid"] and MESSAGES["invalid"] and SCENARIOS, "fixtures 为空"


@pytest.mark.parametrize("case", MESSAGES["valid"], ids=lambda c: c["name"])
def test_valid_message_round_trips(case):
    msg = decode_line(case["direction"], line_bytes(case))
    again = decode_line(case["direction"], encode_line(case["direction"], msg))
    assert again == msg


@pytest.mark.parametrize("case", MESSAGES["invalid"], ids=lambda c: c["name"])
def test_invalid_message_has_expected_code(case):
    with pytest.raises(ProtocolError) as exc:
        decode_line(case["direction"], line_bytes(case))
    assert exc.value.code == case["code"]


def replay_protocol(scenario: dict) -> tuple[int, str]:
    """与 Go 侧 replayScenario 相同：返回首个违规的行号与错误码，无违规时为 (-1, "")。"""
    checker = StreamChecker()
    for index, line in enumerate(scenario["lines"]):
        try:
            msg = decode_line(line["from"], line_bytes(line))
            if line["from"] == "worker":
                checker.observe(msg)
            elif msg["type"] == "init":
                checker.negotiate(msg.get("extensions"))  # sub-run 扩展协商（M4 Plan 14）
        except ProtocolError as exc:
            return index, exc.code
    return -1, ""


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_scenario_stream(scenario):
    expect = scenario["expect"]
    want = (-1, "") if expect["stream"] == "ok" else (expect["at"], expect["violation"])
    assert replay_protocol(scenario) == want


REF = "a" * 64

NESTED_70: object = 0
for _ in range(70):
    NESTED_70 = [NESTED_70]
NESTED_5000: object = 0
for _ in range(5000):
    NESTED_5000 = [NESTED_5000]


def checkpoint_line(state_json: str, refs: list[str]) -> bytes:
    head = '{"type":"checkpoint","v":1,"seq":1,"checkpoint_id":"cp-1","scope":"task","step_id":"s1"'
    return f'{head},"state":{state_json},"refs":{json.dumps(refs)}}}'.encode()


def init_line(config: str) -> bytes:
    init = {
        "type": "init",
        "bootstrap": 1,
        "protocol_versions": [1],
        "mode": "task",
        "task_id": "t",
        "attempt_id": "a",
        "attempt_no": 1,
        "out_dir": "/o",
        "config": config,
    }
    return json.dumps(init).encode()


def progress_line(message: str) -> bytes:
    return f'{{"type":"progress","v":1,"seq":1,"kind":"x","message":"{message}"}}'.encode()


def cancel_line(reason: str) -> bytes:
    return f'{{"type":"cancel","v":1,"attempt_id":"a-1","grace_ms":0,"reason":"{reason}"}}'.encode()


LIMIT_CASES = [
    pytest.param(
        WORKER, progress_line("a" * MAX_EVENT_BYTES), "message_too_large", id="event_over_limit"
    ),
    pytest.param(
        HOST, cancel_line("a" * MAX_CONTROL_BYTES), "message_too_large", id="control_over_limit"
    ),
    pytest.param(HOST, init_line("a" * (100 << 10)), None, id="init_limit_is_1mib"),
    pytest.param(
        WORKER,
        checkpoint_line('"' + "a" * (MAX_INLINE_STATE_BYTES - 2) + '"', []),
        None,
        id="state_at_limit",
    ),
    pytest.param(
        WORKER,
        checkpoint_line('"' + "a" * (MAX_INLINE_STATE_BYTES - 1) + '"', []),
        "state_too_large",
        id="state_over_limit",
    ),
    pytest.param(
        WORKER, checkpoint_line('{ "k" :  "aaaaaaaaaa" }', []), None, id="state_whitespace_ignored"
    ),
    pytest.param(
        WORKER, checkpoint_line("{}", [REF] * MAX_REFS_PER_CHECKPOINT), None, id="refs_at_limit"
    ),
    pytest.param(
        WORKER,
        checkpoint_line("{}", [REF] * (MAX_REFS_PER_CHECKPOINT + 1)),
        "too_many_refs",
        id="refs_over_limit",
    ),
    pytest.param(
        HOST, b"x" * (MAX_INIT_BYTES + 1), "message_too_large", id="oversized_line_not_parsed"
    ),
    pytest.param(
        WORKER,
        b'{"type":"progress","v":1,"seq":1,"kind":"x","message":"\xff"}',
        "malformed_json",
        id="invalid_utf8",
    ),
    pytest.param(
        WORKER, checkpoint_line('"\\ud800"', []), None, id="lone_surrogate_escape_accepted"
    ),
]


@pytest.mark.parametrize(("direction", "line", "code"), LIMIT_CASES)
def test_decode_limits(direction, line, code):
    if code is None:
        decode_line(direction, line)
        return
    with pytest.raises(ProtocolError) as exc:
        decode_line(direction, line)
    assert exc.value.code == code


@pytest.mark.parametrize(
    ("direction", "message", "code"),
    [
        pytest.param(
            WORKER,
            {
                "type": "checkpoint",
                "v": 1,
                "seq": 1,
                "checkpoint_id": "c",
                "scope": "task",
                "step_id": "s",
            },
            "invalid_field",
            id="checkpoint_without_state",
        ),
        pytest.param(
            HOST,
            {"type": "paused", "v": 1, "seq": 1, "checkpoint_id": "cp-1"},
            "unknown_type",
            id="wrong_direction",
        ),
        pytest.param(
            WORKER,
            {"type": "progress", "v": 1, "seq": 1, "kind": "x", "message": "\ud800"},
            "invalid_field",
            id="lone_surrogate",
        ),
        pytest.param(
            WORKER,
            {"type": "progress", "v": 1, "seq": 1, "kind": "x", "message": "y", "data": 10**40},
            "invalid_field",
            id="number_literal_too_long",
        ),
        pytest.param(
            WORKER,
            {"type": "progress", "v": 1, "seq": 1, "kind": "x", "message": "y", "data": NESTED_70},
            "invalid_field",
            id="nesting_too_deep",
        ),
        pytest.param(
            WORKER,
            {
                "type": "progress",
                "v": 1,
                "seq": 1,
                "kind": "x",
                "message": "y",
                "data": NESTED_5000,
            },
            "invalid_field",
            id="too_deep_to_serialize",
        ),
    ],
)
def test_encode_rejects(direction, message, code):
    with pytest.raises(ProtocolError) as exc:
        encode_line(direction, message)
    assert exc.value.code == code


def test_unterminated_string_is_rejected_in_linear_time():
    """深度预扫描的正则不得回溯爆炸：被截断的行（例如宿主写到一半退出）应立即被拒绝。"""
    line = b'{"type":"progress","v":1,"seq":1,"kind":"x","message":"' + b"a b" * 10
    started = time.monotonic()
    with pytest.raises(ProtocolError) as exc:
        decode_line(WORKER, line)
    assert exc.value.code == "malformed_json"
    assert time.monotonic() - started < 1.0


# ---- JSON Schema：与 fixtures 一致 ----

from typing import Any

from jsonschema import Draft202012Validator
from protocol_fixtures import FIXTURES

SCHEMA_NAMES = ("task", "control", "event")


def load_schema(name: str) -> dict[str, Any]:
    path = FIXTURES.parents[1] / "v1" / f"{name}.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


VALIDATORS = {name: Draft202012Validator(load_schema(name)) for name in SCHEMA_NAMES}

# sub-run 扩展：五个消息按 subrun.schema.json 的 host/worker，其他消息的扩展字段按其 fields 叠加
SUBRUN_SCHEMA = load_schema("subrun")
SUBRUN_TYPES = frozenset(
    ("subrun_start", "subrun_started", "subrun_end", "subrun_cancel", "subrun_cancel_requested")
)


def subrun_validator(ref: str) -> Draft202012Validator:
    schema = {k: v for k, v in SUBRUN_SCHEMA.items() if k != "oneOf"}
    return Draft202012Validator({**schema, "$ref": f"#/$defs/{ref}"})


SUBRUN_VALIDATORS = {ref: subrun_validator(ref) for ref in ("host", "worker", "fields")}


class Combined:
    """依次应用多个 validator（基础 schema 与 sub-run 扩展字段）。"""

    def __init__(self, *validators: Draft202012Validator) -> None:
        self.validators = validators

    def iter_errors(self, msg: dict[str, Any]) -> list[Any]:
        return [e for v in self.validators for e in v.iter_errors(msg)]

    def is_valid(self, msg: dict[str, Any]) -> bool:
        return not self.iter_errors(msg)


def with_subrun_fields(direction: str, msg: dict[str, Any], base: Draft202012Validator) -> Combined:
    if msg.get("type") in SUBRUN_TYPES:
        return Combined(SUBRUN_VALIDATORS["host" if direction == HOST else "worker"])
    return Combined(base, SUBRUN_VALIDATORS["fields"])


def validator_for(direction: str, msg: dict[str, Any]) -> Combined:
    if direction == "worker":
        base = VALIDATORS["event"]
    else:
        base = VALIDATORS["task"] if msg.get("type") == "init" else VALIDATORS["control"]
    return with_subrun_fields(direction, msg, base)


def test_schemas_are_valid_draft_2020_12():
    for name in (*SCHEMA_NAMES, "subrun"):
        Draft202012Validator.check_schema(load_schema(name))


def valid_items() -> list[tuple[str, str, dict[str, Any]]]:
    # raw 行表达的是 schema 无法描述的原始文本约束，由编解码器负责
    items = [(c["name"], c["direction"], c["message"]) for c in MESSAGES["valid"] if "message" in c]
    for scenario in SCENARIOS:
        bad = scenario["expect"].get("at")
        for index, line in enumerate(scenario["lines"]):
            if index != bad and "message" in line:
                items.append((f"{scenario['name']}[{index}]", line["from"], line["message"]))
    return items


@pytest.mark.parametrize("item", valid_items(), ids=lambda i: i[0])
def test_schema_accepts_valid(item):
    _, direction, msg = item
    errors = list(validator_for(direction, msg).iter_errors(msg))
    assert not errors, [e.message for e in errors]


@pytest.mark.parametrize(
    "case", [c for c in MESSAGES["invalid"] if "message" in c], ids=lambda c: c["name"]
)
def test_schema_rejects_invalid(case):
    assert not validator_for(case["direction"], case["message"]).is_valid(case["message"])


# ---- session 扩展：与 Go 共用的 fixtures（M4 Plan 12/13）----

from protocol_fixtures import load_session_messages, load_session_scenarios

from agentbox_worker.protocol import decode_session_line, encode_session_line
from agentbox_worker.stream import SessionStreamChecker

SESSION_MESSAGES = load_session_messages()
SESSION_SCENARIOS = load_session_scenarios()
assert SESSION_MESSAGES["valid"] and SESSION_MESSAGES["invalid"] and SESSION_SCENARIOS


@pytest.mark.parametrize("case", SESSION_MESSAGES["valid"], ids=lambda c: c["name"])
def test_session_valid_message_round_trips(case):
    msg = decode_session_line(case["direction"], line_bytes(case))
    encoded = encode_session_line(case["direction"], msg)
    assert json.loads(encoded) == json.loads(line_bytes(case))


@pytest.mark.parametrize("case", SESSION_MESSAGES["invalid"], ids=lambda c: c["name"])
def test_session_invalid_message_has_expected_code(case):
    with pytest.raises(ProtocolError) as exc:
        decode_session_line(case["direction"], line_bytes(case))
    assert exc.value.code == case["code"]


def replay_session(scenario: dict) -> tuple[int, str, SessionStreamChecker]:
    """与 Go 侧 replaySession 相同：宿主消息送入 host_sent，Worker 事件送入 observe。"""
    checker = SessionStreamChecker()
    for index, line in enumerate(scenario["lines"]):
        try:
            msg = decode_session_line(line["from"], line_bytes(line))
            if line["from"] == HOST:
                checker.host_sent(msg)
            else:
                checker.observe(msg)
        except ProtocolError as exc:
            return index, exc.code, checker
    return -1, "", checker


@pytest.mark.parametrize("scenario", SESSION_SCENARIOS, ids=lambda s: s["name"])
def test_session_scenario_stream(scenario):
    expect = scenario["expect"]
    at, code, checker = replay_session(scenario)
    if expect["stream"] == "ok":
        assert (at, code) == (-1, "")
        assert checker.phase == expect["phase"]
    else:
        assert (at, code) == (expect["at"], expect["violation"])


@pytest.mark.parametrize(
    ("decode", "direction", "line", "code"),
    [
        pytest.param(
            decode_line,
            WORKER,
            b'{"type":"task_accepted","v":1,"seq":2,"attempt_id":"a-1"}',
            "unknown_type",
            id="task_mode_rejects_task_accepted",
        ),
        pytest.param(
            decode_line,
            HOST,
            b'{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,"out_dir":"/o"}',
            "unknown_type",
            id="task_mode_rejects_task_start",
        ),
        pytest.param(
            decode_session_line,
            HOST,
            b'{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,'
            b'"out_dir":"/o","config":"' + b"a" * (100 << 10) + b'"}',
            None,
            id="task_start_limit_is_1mib",
        ),
        pytest.param(
            decode_session_line,
            HOST,
            b'{"type":"task_start","v":1,"task_id":"t","attempt_id":"a","attempt_no":1,'
            b'"out_dir":"/o","config":"' + b"a" * MAX_INIT_BYTES + b'"}',
            "message_too_large",
            id="task_start_over_1mib",
        ),
        pytest.param(
            decode_session_line,
            HOST,
            b'{"type":"task_outcome","v":1,"attempt_id":"a","verdict":"failed","pad":"'
            + b"a" * MAX_CONTROL_BYTES
            + b'"}',
            "message_too_large",
            id="task_outcome_is_a_control_message",
        ),
        pytest.param(
            decode_session_line,
            HOST,
            b'{"type":"init","bootstrap":1,"protocol_versions":[1],"mode":"session",'
            b'"session_id":"s","incarnation_id":"i","session_resume":{"checkpoint_id":"sc",'
            b'"state":{},"Staged_State_Path":"x"}}',
            "invalid_field",
            id="session_resume_key_case",
        ),
    ],
)
def test_session_codec_boundaries(decode, direction, line, code):
    if code is None:
        decode(direction, line)
        return
    with pytest.raises(ProtocolError) as exc:
        decode(direction, line)
    assert exc.value.code == code


def test_session_encode_applies_session_rules():
    cr = {"type": "checkpoint_result", "v": 1, "checkpoint_id": "c", "scope": "task"}
    cr["status"] = "committed"
    with pytest.raises(ProtocolError) as exc:
        encode_session_line(HOST, cr)
    assert exc.value.code == "missing_field"
    encode_line(HOST, cr)  # task 模式下 attempt_id 可省略


SESSION_SCHEMA = json.loads(
    (FIXTURES.parents[1] / "v1" / "session.schema.json").read_text(encoding="utf-8")
)


def session_validator(direction: str, msg: dict[str, Any]) -> Combined:
    side = "host" if direction == HOST else "worker"
    schema = {k: v for k, v in SESSION_SCHEMA.items() if k != "oneOf"}  # 顶层接受二者之一
    base = Draft202012Validator({**schema, "$ref": f"#/$defs/{side}"})
    return with_subrun_fields(direction, msg, base)


def session_valid_items() -> list[tuple[str, str, dict[str, Any]]]:
    items = [(c["name"], c["direction"], c["message"]) for c in SESSION_MESSAGES["valid"]]
    for scenario in SESSION_SCENARIOS:
        bad = scenario["expect"].get("at")
        for index, line in enumerate(scenario["lines"]):
            if index != bad and "message" in line:
                items.append((f"{scenario['name']}[{index}]", line["from"], line["message"]))
    return items


def test_session_schema_is_valid_draft_2020_12():
    Draft202012Validator.check_schema(SESSION_SCHEMA)


@pytest.mark.parametrize("item", session_valid_items(), ids=lambda i: i[0])
def test_session_schema_accepts_valid(item):
    _, direction, msg = item
    errors = list(session_validator(direction, msg).iter_errors(msg))
    assert not errors, [e.message for e in errors]


@pytest.mark.parametrize(
    "case", [c for c in SESSION_MESSAGES["invalid"] if "message" in c], ids=lambda c: c["name"]
)
def test_session_schema_rejects_invalid(case):
    assert not session_validator(case["direction"], case["message"]).is_valid(case["message"])


# ---- sub-run 扩展（M4 Plan 14）：与 Go 的 TestSubrun* 对应 ----


def test_subrun_scenarios_present():
    """规格 §5.11 要求的三个 sub-run 场景存在且期望符合计划。"""
    by_name = {s["name"]: s["expect"] for s in SCENARIOS}
    assert by_name["subrun_start_before_ack"]["violation"] == "subrun_unknown"
    assert by_name["subrun_late_complete_after_cancel"]["stream"] == "ok"
    assert by_name["subrun_end_without_checkpoint"]["stream"] == "ok"


def _cp_line(refs: list[str], subruns: list[dict[str, Any]]) -> bytes:
    msg = {"type": "checkpoint", "v": 1, "seq": 1, "checkpoint_id": "c", "scope": "task"}
    msg.update(step_id="s", state={}, refs=refs, subruns=subruns)
    return json.dumps(msg).encode()


def _end_line(summary: str) -> bytes:
    msg = {"type": "subrun_end", "v": 1, "seq": 1, "subrun_id": "st1", "status": "failed"}
    return json.dumps({**msg, "summary": summary}, ensure_ascii=False).encode()


def _start_line(parent: str) -> bytes:
    msg = {"type": "subrun_start", "v": 1, "seq": 1, "subrun_id": "st1", "deadline_ms": 1}
    return json.dumps({**msg, "parent_step_id": parent}, ensure_ascii=False).encode()


_DONE = [{"subrun_id": "st1", "status": "completed", "result_ref": REF}]


@pytest.mark.parametrize(
    ("line", "code"),
    [
        pytest.param(
            _cp_line([REF] * (MAX_REFS_PER_CHECKPOINT - 1), _DONE), None, id="refs_at_limit"
        ),
        pytest.param(
            _cp_line([REF] * MAX_REFS_PER_CHECKPOINT, _DONE), "too_many_refs", id="refs_over_limit"
        ),
        pytest.param(
            _cp_line([REF] * MAX_REFS_PER_CHECKPOINT, [{"subrun_id": "st1", "status": "started"}]),
            None,
            id="non_completed_not_counted",
        ),
        pytest.param(_end_line("a" * 4096), None, id="summary_at_limit"),
        pytest.param(_end_line("é" * 2048 + "a"), "invalid_field", id="summary_bytes_over"),
        pytest.param(_start_line("p" * 256), None, id="parent_at_limit"),
        pytest.param(_start_line("é" * 128 + "p"), "invalid_field", id="parent_bytes_over"),
    ],
)
def test_subrun_limits(line, code):
    if code is None:
        decode_line(WORKER, line)
        return
    with pytest.raises(ProtocolError) as exc:
        decode_line(WORKER, line)
    assert exc.value.code == code


def test_subrun_encode_direction_and_session_rules():
    started = {"type": "subrun_started", "v": 1, "subrun_id": "st1", "status": "started"}
    with pytest.raises(ProtocolError) as exc:
        encode_line(WORKER, started)
    assert exc.value.code == "unknown_type"
    encode_line(HOST, started)
    start = {"type": "subrun_start", "v": 1, "seq": 1, "subrun_id": "st1"}
    start.update(parent_step_id="p", deadline_ms=1)
    with pytest.raises(ProtocolError) as exc:
        encode_session_line(WORKER, start)
    assert exc.value.code == "missing_field"  # session 模式的 sub-run 事件须带 attempt_id
    encode_session_line(WORKER, {**start, "attempt_id": "a-1"})
    encode_session_line(HOST, started)


READY = {"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task"}
READY.update(worker={"name": "w"}, capabilities=[])


def _ev(typ: str, seq: int, subrun_id: str, **extra: Any) -> dict[str, Any]:
    return {"type": typ, "v": 1, "seq": seq, "subrun_id": subrun_id, **extra}


def _start(seq, sid):
    return _ev("subrun_start", seq, sid, parent_step_id="p", deadline_ms=1000)


def _end(seq, sid):
    return _ev("subrun_end", seq, sid, status="succeeded", summary="ok")


def _cancel(seq, sid):
    return _ev("subrun_cancel", seq, sid, reason="enough")


def _progress(seq, sid):
    return _ev("progress", seq, sid, kind="tool_call", message="m")


def _cp(seq):
    msg = {"type": "checkpoint", "v": 1, "seq": seq, "checkpoint_id": "c", "scope": "task"}
    return {
        **msg,
        "step_id": "s",
        "state": {},
        "subruns": [{"subrun_id": "st9", "status": "started"}],
    }


def _result(seq):
    return {"type": "result", "v": 1, "seq": seq, "summary": "s", "outputs": []}


_READY_EXT = {**READY, "subruns": 1}

STREAM_CASES = [
    (
        "full_flow",
        True,
        [
            _READY_EXT,
            _start(2, "st1"),
            _progress(3, "st1"),
            _cancel(4, "st1"),
            _end(5, "st1"),
            _result(6),
        ],
        -1,
        "",
    ),
    ("checkpoint_lists_unstarted", True, [_READY_EXT, _cp(2)], -1, ""),
    ("before_ready", True, [_start(1, "st1")], 0, "before_ready"),
    (
        "after_terminal",
        True,
        [_READY_EXT, _start(2, "st1"), _result(3), _end(4, "st1")],
        3,
        "after_terminal",
    ),
    ("ready_missing_ack", True, [READY], 0, "extension_mismatch"),
    ("ready_unrequested_ack", False, [_READY_EXT], 0, "extension_mismatch"),
    ("start_not_negotiated", False, [READY, _start(2, "st1")], 1, "extension_not_negotiated"),
    ("progress_not_negotiated", False, [READY, _progress(2, "st1")], 1, "extension_not_negotiated"),
    ("checkpoint_not_negotiated", False, [READY, _cp(2)], 1, "extension_not_negotiated"),
    ("end_unknown", True, [_READY_EXT, _start(2, "st1"), _end(3, "st2")], 2, "subrun_unknown"),
    ("cancel_unknown", True, [_READY_EXT, _cancel(2, "st1")], 1, "subrun_unknown"),
    ("progress_unknown", True, [_READY_EXT, _progress(2, "st1")], 1, "subrun_unknown"),
]


@pytest.mark.parametrize(
    ("negotiated", "lines", "at", "code"),
    [c[1:] for c in STREAM_CASES],
    ids=[c[0] for c in STREAM_CASES],
)
def test_subrun_worker_stream(negotiated, lines, at, code):
    checker = StreamChecker()
    checker.negotiate(["future_ext", "subruns"] if negotiated else ["future_ext"])
    got = (-1, "")
    for index, msg in enumerate(lines):
        decode_line(WORKER, json.dumps(msg).encode())
        try:
            checker.observe(msg)
        except ProtocolError as exc:
            got = (index, exc.code)
            break
    assert got == (at, code)


def test_subrun_session_host_messages():
    checker = SessionStreamChecker()
    init = {"type": "init", "bootstrap": 1, "protocol_versions": [1], "mode": "session"}
    init.update(session_id="s-1", incarnation_id="i-1", extensions=["subruns"])
    checker.host_sent(init)
    cancel = {"type": "subrun_cancel_requested", "v": 1, "subrun_id": "st1", "reason": "deadline"}
    with pytest.raises(ProtocolError) as exc:  # 没有当前 attempt
        checker.host_sent(cancel)
    assert exc.value.code == "wrong_attempt"
    checker.observe({**READY, "mode": "session", "session_ext": 1, "subruns": 1})
    task_start = {"type": "task_start", "v": 1, "task_id": "t", "attempt_id": "a-1"}
    checker.host_sent({**task_start, "attempt_no": 1, "out_dir": "/o"})
    checker.observe({"type": "task_accepted", "v": 1, "seq": 2, "attempt_id": "a-1"})
    checker.observe({**_start(3, "st1"), "attempt_id": "a-1"})
    checker.host_sent({"type": "subrun_started", "v": 1, "subrun_id": "st1", "status": "started"})
    checker.host_sent(cancel)
    with pytest.raises(ProtocolError) as exc:
        checker.host_sent({**cancel, "subrun_id": "st2"})
    assert exc.value.code == "subrun_unknown"
