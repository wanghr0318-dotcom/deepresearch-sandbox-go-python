"""协议层：跨语言 fixtures 回放与按常量生成的大小上限（规格 §5.10、§5.11）。"""

import json

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
        except ProtocolError as exc:
            return index, exc.code
    return -1, ""


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_scenario_stream(scenario):
    expect = scenario["expect"]
    want = (-1, "") if expect["stream"] == "ok" else (expect["at"], expect["violation"])
    assert replay_protocol(scenario) == want


REF = "a" * 64


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
        WORKER,
        b'{"type":"progress","v":1,"seq":1,"kind":"x","message":"y","data":'
        + b"[" * 100_000
        + b"]" * 100_000
        + b"}",
        "malformed_json",
        id="nesting_too_deep",
    ),
    pytest.param(
        WORKER, checkpoint_line('"\\ud800"', []), None, id="lone_surrogate_escape_accepted"
    ),
    pytest.param(
        WORKER,
        b'{"type":"progress","v":1,"seq":1,"kind":"x","message":"y","data":NaN}',
        "malformed_json",
        id="nan_is_malformed",
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
    ],
)
def test_encode_rejects(direction, message, code):
    with pytest.raises(ProtocolError) as exc:
        encode_line(direction, message)
    assert exc.value.code == code


# ---- JSON Schema：与 fixtures 一致 ----

from typing import Any

from jsonschema import Draft202012Validator
from protocol_fixtures import FIXTURES

SCHEMA_NAMES = ("task", "control", "event")


def load_schema(name: str) -> dict[str, Any]:
    path = FIXTURES.parents[1] / "v1" / f"{name}.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


VALIDATORS = {name: Draft202012Validator(load_schema(name)) for name in SCHEMA_NAMES}


def validator_for(direction: str, msg: dict[str, Any]) -> Draft202012Validator:
    if direction == "worker":
        return VALIDATORS["event"]
    return VALIDATORS["task"] if msg.get("type") == "init" else VALIDATORS["control"]


def test_schemas_are_valid_draft_2020_12():
    for name in SCHEMA_NAMES:
        Draft202012Validator.check_schema(load_schema(name))


def valid_items() -> list[tuple[str, str, dict[str, Any]]]:
    items = [(c["name"], c["direction"], c["message"]) for c in MESSAGES["valid"]]
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
