"""SDK session 模式：多轮 task、task_outcome、awaiting_input、session_state、quiesce 与 close
（规格 §5.4 session 扩展、§12.3–12.4；协调者裁定 A–D）。"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from agentfakes import FAST_TIMING, SessionHost

from agentbox_worker import (
    MemoryTransport,
    Paused,
    Result,
    TaskContext,
    WorkerFailure,
    run_worker,
)
from agentbox_worker.protocol import ProtocolError
from agentbox_worker.session import SessionInfo, read_staged_state, run_session_worker
from agentbox_worker.stream import SessionStreamChecker

STAGED = "/run/agentbox/restore/" + "a" * 64
REF = "b" * 64


def types(events: list[dict[str, Any]]) -> list[str]:
    return [e["type"] for e in events]


def assert_valid_transcript(host: SessionHost) -> None:
    """整个双向转录按 session 事件流规则合法（宿主消息与 Worker 事件交错回放）。"""
    checker = SessionStreamChecker()
    for side, msg in host.transcript:
        if side == "host":
            checker.host_sent(msg)
        else:
            checker.observe(msg)


def first(events: list[dict[str, Any]], typ: str) -> dict[str, Any]:
    return next(e for e in events if e["type"] == typ)


async def counter_app(ctx: TaskContext) -> Result:
    n = (ctx.session.state or {}).get("n", 0) + 1
    await ctx.checkpoint("step", state={"n": n})
    return Result(summary=f"turn {n}", session_state={"n": n})


# ---- 多轮与会话状态 ----


def test_two_turns_share_one_process_and_adopt_committed_state():
    host = SessionHost(session_id="s-1", incarnation_id="i-1")
    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append(ctx.session.state)
        return await counter_app(ctx)

    host.start_task("t-1", "a-1", config={"text": "hi"})
    host.start_task("t-2", "a-2", config={"text": "again"})
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert seen == [None, {"n": 1}]  # 第二轮看到第一轮成功后提交的状态
    assert types(events)[:2] == ["ready", "task_accepted"]
    assert types(events).count("task_released") == 2 and types(events)[-1] == "closed"
    ready = events[0]
    assert ready["mode"] == "session" and ready["session_ext"] == 1
    first_turn = events[: types(events).index("task_released")]
    assert all(e["attempt_id"] == "a-1" for e in first_turn if e["type"] != "ready")
    result = first(events, "result")
    assert result["session_state"]["state"] == {"n": 1}
    assert [e["seq"] for e in events] == list(range(1, len(events) + 1))
    assert_valid_transcript(host)


def test_session_info_carries_ids_and_committed_checkpoint():
    host = SessionHost(session_id="s-9", incarnation_id="i-9")
    infos: list[SessionInfo] = []

    async def app(ctx: TaskContext):
        infos.append(ctx.session)
        return await counter_app(ctx)

    host.start_task("t-1", "a-1")
    host.start_task("t-2", "a-2", base_session_checkpoint_id="placeholder")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    proposed = first(events, "result")["session_state"]["checkpoint_id"]
    assert [(i.session_id, i.incarnation_id) for i in infos] == [("s-9", "i-9")] * 2
    assert [i.checkpoint_id for i in infos] == [None, proposed]


def test_failed_turn_discards_working_state():
    host = SessionHost(verdicts={"a-1": "failed"})
    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append(ctx.session.state)
        return await counter_app(ctx)

    host.start_task("t-1", "a-1")
    host.start_task("t-2", "a-2")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert seen == [None, None]  # 第一轮提议了 {"n":1}，但裁决 failed：工作副本被丢弃
    assert types(events).count("task_released") == 2
    assert_valid_transcript(host)


def test_app_cannot_corrupt_committed_state_in_place():
    host = SessionHost(verdicts={"a-2": "failed"})
    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append(json.loads(json.dumps(ctx.session.state)))
        if ctx.session.state is not None:
            ctx.session.state["n"] = 99  # 应用违规原地修改；不得影响已提交状态
            raise WorkerFailure("bad", "失败")
        return Result("one", session_state={"n": 1})

    for i in (1, 2, 3):
        host.start_task(f"t-{i}", f"a-{i}")
    host.close_when_idle()
    code, _ = host.run(app)
    assert code == 0
    assert seen == [None, {"n": 1}, {"n": 1}]


def test_app_error_is_a_proposal_and_session_continues():
    async def app(ctx: TaskContext):
        if ctx.task_id == "t-1":
            raise KeyError("boom")
        return Result("ok")

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.start_task("t-2", "a-2")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    error = first(events, "error")
    assert error["attempt_id"] == "a-1" and error["code"] == "internal_error"
    assert "session_state" not in first(events, "result")  # 未提议会话状态
    assert_valid_transcript(host)


def test_oversized_session_state_fails_the_turn():
    async def app(ctx: TaskContext):
        return Result("big", session_state={"x": "a" * (192 << 10)})

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert "result" not in types(events)
    assert first(events, "error")["code"] == "state_too_large"
    assert_valid_transcript(host)


# ---- awaiting_input（裁定 A） ----


def test_awaiting_input_proposal_without_host_pause_request():
    async def app(ctx: TaskContext):
        cp = await ctx.checkpoint("ask", state={"q": 1})
        return Paused(cp, awaiting_input={"question_id": "q-1"})

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    ask = first(events, "awaiting_input")
    checkpoint = first(events, "checkpoint")
    assert ask["attempt_id"] == "a-1" and ask["question_id"] == "q-1"
    assert ask["checkpoint_id"] == checkpoint["checkpoint_id"]
    assert "paused" not in types(events)
    assert host.outcome_for(ask)["verdict"] == "paused"
    assert_valid_transcript(host)


@pytest.mark.parametrize("ask", [{}, {"question_id": ""}, {"question_id": 3}, "q-1"])
def test_awaiting_input_needs_question_id(ask):
    async def app(ctx: TaskContext):
        return Paused("cp-1", awaiting_input=ask)

    host = SessionHost()
    host.start_task("t-1", "a-1")
    code, events = host.run(app)
    assert code == 0
    assert first(events, "error")["code"] == "invalid_field"


def test_plain_pause_request_still_proposes_paused():
    async def app(ctx: TaskContext):
        while not ctx.should_pause():
            await asyncio.sleep(0.001)
        return Paused(await ctx.checkpoint("s", state={}))

    def pause_after_accept(event, host):
        if event["type"] == "task_accepted":
            host.send(
                {"type": "pause", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 0}
            )

    host = SessionHost(on_event=pause_after_accept)
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert first(events, "paused")["attempt_id"] == "a-1"
    assert_valid_transcript(host)


# ---- run_call：宿主暂停时放弃在途阻塞调用的等待（stop-fix F1） ----

import threading
import time

from agentbox_worker import CallAbandoned
from agentbox_worker.gateway import CallIds

PAUSE_A1 = {"type": "pause", "v": 1, "attempt_id": "a-1", "reason": "user", "grace_ms": 0}


def test_run_call_abandons_blocked_call_on_pause_and_fences_its_call_ids():
    gate, entered = threading.Event(), threading.Event()
    seen: dict[str, Any] = {}

    def blocking(ids: CallIds) -> str:
        first_id = ids.next("s", "chat")  # 进入时已占号（如同 Gateway 客户端发出请求）
        entered.set()
        gate.wait(5)
        try:
            ids.next("s", "chat")  # 被放弃之后不能再占号（不会再发出新请求）
        except CallAbandoned:
            seen["fenced"] = True
        return first_id

    async def app(ctx: TaskContext):
        before = ctx.call_ids.snapshot()
        started = time.monotonic()
        try:
            await ctx.run_call(blocking, ctx.call_ids)
        except CallAbandoned:
            seen["elapsed"] = time.monotonic() - started
        assert ctx.call_ids.snapshot() == {"root/s/chat": 1}  # 被放弃的调用已占号
        ctx.rewind_call_ids(before)  # 回到一致点：恢复后被放弃的调用以同一 ID 重发
        with pytest.raises(CallAbandoned):  # 已请求暂停：不再开始新的调用
            await ctx.run_call(lambda: "never")
        return Paused(await ctx.checkpoint("s", state={}))

    def hook(event, host):
        if event["type"] == "task_accepted":
            loop = asyncio.get_running_loop()

            def pause_when_entered() -> None:
                entered.wait(5)
                loop.call_soon_threadsafe(host.send, PAUSE_A1)

            threading.Thread(target=pause_when_entered, daemon=True).start()

    host = SessionHost(on_event=hook)
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert seen["elapsed"] < 2
    assert first(events, "paused")["attempt_id"] == "a-1"
    assert "_agentbox" not in first(events, "checkpoint")["state"]  # 计数器已回退为空
    gate.set()
    deadline = time.monotonic() + 5
    while "fenced" not in seen and time.monotonic() < deadline:
        time.sleep(0.01)
    assert seen.get("fenced") is True
    assert_valid_transcript(host)


def test_run_call_returns_result_and_propagates_errors_without_pause():
    async def app(ctx: TaskContext):
        assert await ctx.run_call(lambda a, b=0: a + b, 1, b=2) == 3
        with pytest.raises(ValueError):
            await ctx.run_call(_raise_value_error)
        return Result(summary="ok")

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0 and first(events, "result")["summary"] == "ok"


def _raise_value_error() -> None:
    raise ValueError("boom")


def test_call_ids_rewind_restores_snapshot_in_place():
    ids = CallIds()
    ids.next("s", "chat")
    snap = ids.snapshot()
    ids.next("s", "chat")
    ids.next("t", "exec")
    ids.rewind(snap)
    assert ids.snapshot() == {"root/s/chat": 1}
    assert ids.next("s", "chat") == "root/s/chat/2"
    ids.advance({"root/s/chat": 5, "root/u/fetch": 1})  # 跳过被放弃调用已占的号；不回退
    ids.advance({"root/s/chat": 3})
    assert ids.snapshot() == {"root/s/chat": 5, "root/u/fetch": 1}
    assert ids.next("s", "chat") == "root/s/chat/6"


# ---- task_outcome 丢失与查询 ----


def test_task_outcome_lost_is_queried_then_released():
    host = SessionHost(drop_first_outcome=True)
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(counter_app)
    assert code == 0
    kinds = types(events)
    assert kinds.index("result") < kinds.index("task_outcome_query") < kinds.index("task_released")
    sent = [m["type"] for side, m in host.transcript if side == "host"]
    assert sent.count("task_outcome") == 1  # 第一条未送达；查询后重发的才是送达的那条
    assert_valid_transcript(host)


def test_task_outcome_never_arrives_exits_failure():
    host = SessionHost(drop_first_outcome=True, answer_outcome_queries=False)
    host.start_task("t-1", "a-1")
    code, events = host.run(counter_app)
    assert code == 1
    assert types(events).count("task_outcome_query") == FAST_TIMING.max_ack_attempts
    assert "task_released" not in types(events)


# ---- directive、carryover、恢复 ----


def test_directive_and_carryover_reach_the_app_and_resume_restores_call_ids():
    seen: dict[str, Any] = {}

    async def app(ctx: TaskContext):
        seen.update(
            directive=ctx.directive,
            carryover=ctx.carryover,
            restored=ctx.restored_from_task_id,
            base=ctx.base_session_checkpoint_id,
            resume=ctx.resume.state,
            call=ctx.call_ids.next("x", "chat"),
        )
        return Result("ok")

    directive = {"kind": "answer", "question_id": "q-1", "answers": [{"index": 0}]}
    carryover = {"task_id": "t-0", "checkpoint_ref": REF}
    resume = {
        "checkpoint_id": "seed-1",
        "step_id": "x",
        "state": {"plan": 1, "_agentbox": {"call_ids": {"root/x/chat": 4}}},
    }
    host = SessionHost()
    host.start_task(
        "t-1",
        "a-1",
        attempt_no=2,
        directive=directive,
        carryover=carryover,
        restored_from_task_id="t-0",
        resume=resume,
    )
    host.close_when_idle()
    code, _ = host.run(app)
    assert code == 0
    assert seen["directive"] == directive and seen["carryover"] == carryover
    assert seen["restored"] == "t-0" and seen["base"] is None
    assert seen["resume"] == {"plan": 1}  # SDK 保留键对应用不可见
    assert seen["call"] == "root/x/chat/5"  # 从快照续号


def test_no_directive_means_start_or_continue():
    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append((ctx.directive, ctx.carryover, ctx.restored_from_task_id))
        return Result("ok")

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    assert host.run(app)[0] == 0
    assert seen == [(None, None, None)]


def test_each_turn_has_its_own_call_id_counter():
    calls: list[str] = []

    async def app(ctx: TaskContext):
        calls.append(ctx.call_ids.next("plan", "chat"))
        return Result("ok")

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.start_task("t-2", "a-2")
    host.close_when_idle()
    assert host.run(app)[0] == 0
    assert calls == ["root/plan/chat/1", "root/plan/chat/1"]


# ---- quiesce 与 close ----


def test_quiesce_reports_committed_checkpoint_and_close_exits():
    host = SessionHost()
    host.quiesce()
    host.start_task("t-1", "a-1")  # 静止后由 task_start 唤醒
    host.quiesce()
    host.close_when_idle()
    code, events = host.run(counter_app)
    assert code == 0
    quiesced = [e for e in events if e["type"] == "quiesced"]
    assert quiesced[0]["session_checkpoint_id"] == ""  # 尚无会话 checkpoint
    committed = first(events, "result")["session_state"]["checkpoint_id"]
    assert quiesced[1]["session_checkpoint_id"] == committed
    assert types(events)[-1] == "closed" and "attempt_id" not in events[-1]
    assert_valid_transcript(host)


def test_session_close_during_turn_cancels_and_closes():
    async def app(ctx: TaskContext):
        await asyncio.sleep(10)
        return Result("never")

    def close_after_accept(event, host):
        if event["type"] == "task_accepted":
            host.send({"type": "session_close", "v": 1, "grace_ms": 0})

    host = SessionHost(on_event=close_after_accept)
    host.start_task("t-1", "a-1")
    code, events = host.run(app)
    assert code == 0
    assert types(events) == ["ready", "task_accepted", "closed"]
    assert_valid_transcript(host)


def test_stdin_close_when_idle_exits_ok():
    host = SessionHost()
    code, events = host.run(counter_app)
    assert (code, types(events)) == (0, ["ready"])


def test_stdin_close_during_turn_cancels_and_fails():
    cancelled: list[bool] = []

    async def app(ctx: TaskContext):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return Result("never")

    def eof_after_accept(event, host):
        if event["type"] == "task_accepted":
            host.send(None)

    host = SessionHost(on_event=eof_after_accept)
    host.start_task("t-1", "a-1")
    code, events = host.run(app)
    assert code == 1 and cancelled == [True]
    assert types(events) == ["ready", "task_accepted"]


# ---- 宿主取消与控制错误 ----


def test_host_cancel_sends_no_proposal_then_released():
    async def app(ctx: TaskContext):
        await asyncio.sleep(10)
        return Result("never")

    def cancel_after_accept(event, host):
        if event["type"] == "task_accepted":
            attempt = event["attempt_id"]
            host.send(
                {"type": "cancel", "v": 1, "attempt_id": attempt, "reason": "x", "grace_ms": 0}
            )
            host.send(
                {"type": "task_outcome", "v": 1, "attempt_id": attempt, "verdict": "cancelled"}
            )

    host = SessionHost(on_event=cancel_after_accept)
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert types(events) == ["ready", "task_accepted", "task_released", "closed"]
    assert_valid_transcript(host)


def test_task_outcome_without_proposal_stops_the_turn():
    async def app(ctx: TaskContext):
        await asyncio.sleep(10)
        return Result("never")

    def outcome_after_accept(event, host):
        if event["type"] == "task_accepted":
            host.send({"type": "task_outcome", "v": 1, "attempt_id": "a-1", "verdict": "cancelled"})

    host = SessionHost(on_event=outcome_after_accept)
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0
    assert types(events) == ["ready", "task_accepted", "task_released", "closed"]
    assert_valid_transcript(host)


@pytest.mark.parametrize(
    "control",
    [
        pytest.param(
            {"type": "pause", "v": 1, "attempt_id": "a-x", "reason": "u", "grace_ms": 0},
            id="pause_other_attempt",
        ),
        pytest.param(
            {
                "type": "checkpoint_result",
                "v": 1,
                "attempt_id": "a-x",
                "checkpoint_id": "c",
                "scope": "task",
                "status": "committed",
            },
            id="checkpoint_result_other_attempt",
        ),
        pytest.param(
            {
                "type": "task_start",
                "v": 1,
                "task_id": "t-2",
                "attempt_id": "a-2",
                "attempt_no": 1,
                "out_dir": "/o",
            },
            id="task_start_while_running",
        ),
        pytest.param({"type": "quiesce", "v": 1, "grace_ms": 0}, id="quiesce_while_running"),
        pytest.param({"type": "bogus", "v": 1}, id="unknown_type"),
    ],
)
def test_control_for_other_attempt_is_protocol_error(control):
    async def app(ctx: TaskContext):
        await asyncio.sleep(10)
        return Result("never")

    def inject(event, host):
        if event["type"] == "task_accepted":
            host.send(control)

    host = SessionHost(on_event=inject)
    host.start_task("t-1", "a-1")
    code, events = host.run(app)
    assert code == 1
    assert types(events) == ["ready", "task_accepted", "error"]
    error = events[-1]
    assert error["code"] == "control_protocol_error" and error["attempt_id"] == "a-1"


@pytest.mark.parametrize(
    "msg",
    [
        pytest.param({"type": "pause", "v": 1, "attempt_id": "a-1", "reason": "u", "grace_ms": 0}),
        pytest.param({"type": "task_outcome", "v": 1, "attempt_id": "a-1", "verdict": "failed"}),
    ],
    ids=["control_when_idle", "outcome_when_idle"],
)
def test_unexpected_message_when_idle_exits_failure(msg):
    def inject(event, host):
        if event["type"] == "ready":
            host.send(msg)

    host = SessionHost(on_event=inject)
    host.start_task("t-1", "a-1")  # 不会被执行：宿主先违规
    code, events = host.run(counter_app)
    assert code == 1
    assert types(events) == ["ready"]  # 空闲时没有 attempt，无法发出 error


# ---- 握手与冷恢复 ----


def test_inline_session_resume_is_adopted():
    host = SessionHost(session_resume={"checkpoint_id": "sc-1", "state": {"n": 5}, "refs": [REF]})
    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append((ctx.session.checkpoint_id, ctx.session.state))
        return Result("ok")

    host.start_task("t-1", "a-1", base_session_checkpoint_id="sc-1")
    host.close_when_idle()
    assert host.run(app)[0] == 0
    assert seen == [("sc-1", {"n": 5})]


def test_staged_session_resume_is_loaded(tmp_path):
    staged = tmp_path / "state.json"
    staged.write_text(json.dumps({"n": 7, "名": "值"}), encoding="utf-8")
    asked: list[str] = []

    def load(path: str) -> Any:
        asked.append(path)
        return read_staged_state(str(staged))

    seen: list[Any] = []

    async def app(ctx: TaskContext):
        seen.append(ctx.session.state)
        return Result("ok")

    host = SessionHost(session_resume={"checkpoint_id": "sc-1", "staged_state_path": STAGED})
    host.start_task("t-1", "a-1", base_session_checkpoint_id="sc-1")
    host.close_when_idle()
    code, _ = host.run(app, load_staged=load)
    assert code == 0
    assert asked == [STAGED] and seen == [{"n": 7, "名": "值"}]


@pytest.mark.parametrize(
    "content", [None, b"not json", b"\xff\xfe"], ids=["missing", "bad", "utf8"]
)
def test_bad_staged_state_fails_before_ready(tmp_path, content):
    staged = tmp_path / "state.json"
    if content is not None:
        staged.write_bytes(content)
    host = SessionHost(session_resume={"checkpoint_id": "sc-1", "staged_state_path": STAGED})
    host.start_task("t-1", "a-1")
    code, events = host.run(counter_app, load_staged=lambda _: read_staged_state(str(staged)))
    assert code == 1
    assert types(events) == ["error"]
    assert events[0]["code"] == "invalid_field" and "attempt_id" not in events[0]
    assert_valid_transcript(host)


def run_raw(app: Any, worker: Any, init: dict[str, Any]) -> tuple[int, list[dict[str, Any]]]:
    async def go():
        transport = MemoryTransport()
        transport.feed(json.dumps(init).encode())
        transport.feed(None)
        code = await asyncio.wait_for(
            worker(app, transport, name="w", version="0", timing=FAST_TIMING), timeout=5
        )
        return code, [json.loads(line) for line in transport.sent]

    return asyncio.run(go())


TASK_INIT = {
    "type": "init",
    "bootstrap": 1,
    "protocol_versions": [1],
    "mode": "task",
    "task_id": "t-1",
    "attempt_id": "a-1",
    "attempt_no": 1,
    "out_dir": "/o",
}


def test_session_worker_rejects_task_mode():
    code, events = run_raw(counter_app, run_session_worker, TASK_INIT)
    assert code == 1
    assert types(events) == ["error"] and events[0]["code"] == "unsupported_mode"


def test_task_worker_rejects_session_mode():
    init = {**SessionHost().init}
    code, events = run_raw(counter_app, run_worker, init)
    assert code == 1
    assert types(events) == ["error"] and events[0]["code"] == "unsupported_mode"


def test_corrupt_call_ids_in_resume_fail_the_turn():
    resume = {"checkpoint_id": "c", "step_id": "s", "state": {"_agentbox": {"call_ids": {"k": -1}}}}
    host = SessionHost()
    host.start_task("t-1", "a-1", resume=resume)
    host.close_when_idle()
    code, events = host.run(counter_app)
    assert code == 0
    assert types(events)[:3] == ["ready", "task_accepted", "error"]
    assert events[2]["code"] == "invalid_field"
    assert_valid_transcript(host)


# ---- task 模式不受影响 ----


def run_task(app: Any) -> tuple[int, list[dict[str, Any]]]:
    return run_raw(app, run_worker, TASK_INIT)


def test_task_mode_unchanged_and_rejects_session_fields():
    async def with_state(ctx: TaskContext):
        assert ctx.session is None and ctx.directive is None and ctx.carryover is None
        return Result("ok", session_state={"n": 1})

    code, events = run_task(with_state)
    assert code == 0
    assert types(events) == ["ready", "result"]
    assert "session_state" not in events[1] and "attempt_id" not in events[1]
    assert "session_ext" not in events[0]

    async def asks(ctx: TaskContext):
        return Paused("cp-1", awaiting_input={"question_id": "q-1"})

    code, events = run_task(asks)
    assert code == 1
    assert types(events) == ["ready", "error"] and events[1]["code"] == "invalid_field"


# ---- SessionStreamChecker ----


def host_msg(typ: str, **fields: Any) -> dict[str, Any]:
    return {"type": typ, "v": 1, **fields}


def ev(seq: int, typ: str, **fields: Any) -> dict[str, Any]:
    return {"type": typ, "v": 1, "seq": seq, **fields}


def started_checker() -> SessionStreamChecker:
    checker = SessionStreamChecker()
    checker.host_sent(SessionHost().init)
    checker.observe(ev(1, "ready", mode="session", session_ext=1))
    checker.host_sent(host_msg("task_start", attempt_id="a-1"))
    checker.observe(ev(2, "task_accepted", attempt_id="a-1"))
    return checker


def test_session_stream_checker_rules():
    checker = started_checker()
    checker.observe(ev(3, "result", attempt_id="a-1"))
    with pytest.raises(ProtocolError) as exc:
        checker.observe(ev(4, "progress", attempt_id="a-1"))
    assert exc.value.code == "after_terminal"

    checker = started_checker()
    checker.observe(ev(3, "result", attempt_id="a-1"))
    checker.observe(ev(4, "task_outcome_query", attempt_id="a-1"))  # 终态后允许查询
    checker.host_sent(host_msg("task_outcome", attempt_id="a-1", verdict="succeeded"))
    checker.observe(ev(5, "task_released", attempt_id="a-1"))  # 终态后允许释放
    assert (checker.phase, checker.current) == ("idle", "")

    checker = started_checker()
    with pytest.raises(ProtocolError) as exc:
        checker.observe(ev(3, "task_accepted", attempt_id="a-1"))  # 无终态时第二次 accepted
    assert exc.value.code == "unexpected_event"

    checker = started_checker()
    with pytest.raises(ProtocolError) as exc:
        checker.observe(ev(3, "error", code="x"))  # ready 之后的 error 必须带 attempt_id
    assert exc.value.code == "missing_field"


def test_run_call_within_abandons_on_timeout_even_while_paused():
    gate, entered = threading.Event(), threading.Event()
    seen: dict[str, Any] = {}

    def blocking(ids: CallIds) -> str:
        first_id = ids.next("stop-1", "chat")
        entered.set()
        gate.wait(5)
        try:
            ids.next("stop-1", "chat")
        except CallAbandoned:
            seen["fenced"] = True
        return first_id

    async def app(ctx: TaskContext):
        # 不受暂停影响（停止摘要在暂停之后发起），只受期限约束
        assert await ctx.run_call_within(5, lambda a: a * 2, 21) == 42
        started = time.monotonic()
        with pytest.raises(CallAbandoned):
            await ctx.run_call_within(0.2, blocking, ctx.call_ids)
        seen["elapsed"] = time.monotonic() - started
        with pytest.raises(ValueError):
            await ctx.run_call_within(5, _raise_value_error)
        assert ctx.call_ids.snapshot() == {"root/stop-1/chat": 1}
        return Result(summary="ok")

    host = SessionHost()
    host.start_task("t-1", "a-1")
    host.close_when_idle()
    code, events = host.run(app)
    assert code == 0 and first(events, "result")["summary"] == "ok"
    assert entered.is_set() and seen["elapsed"] < 2
    gate.set()
    deadline = time.monotonic() + 5
    while "fenced" not in seen and time.monotonic() < deadline:
        time.sleep(0.01)
    assert seen.get("fenced") is True
