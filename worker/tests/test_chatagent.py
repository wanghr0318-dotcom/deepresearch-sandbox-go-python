"""chatagent：编排循环、kimi-k2.6 子主题循环、引用核对与报告、状态压缩、崩溃恢复的确定性。

全部经 SessionHost（MemoryTransport 上的脚本化宿主）+ ScriptedGateway + ScriptedModel 在进程内
运行，不依赖 AF_UNIX（Windows 可运行）。
"""

from __future__ import annotations

import ast
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
from agentfakes import ScriptedGateway, ScriptedModel, SessionHost, call, calls, reply

from agentbox_worker import GatewayError, ToolBudgetExhausted
from chatagent import TurnState, make_app, parse_turn_config
from chatagent.report import citation_pass, render_report, report_summary
from chatagent.state import OMITTED_SOURCE, STATE_LIMIT_BYTES, json_size

WORKER_ROOT = Path(__file__).resolve().parent.parent
SKILLS = WORKER_ROOT / "skills"
ALLOWED_MESSAGE_KEYS = {"role", "content", "name", "tool_call_id", "tool_calls"}
FORBIDDEN_IMPORTS = {
    "sqlite3",
    "psycopg",
    "redis",
    "requests",
    "httpx",
    "urllib",
    "subprocess",
    "multiprocessing",
    "socket",
}

_OUT: dict[str, Path] = {}


@pytest.fixture(autouse=True)
def _out_root(tmp_path: Path) -> None:
    _OUT["root"] = tmp_path


# ---- 脚本与事件辅助 ----


def todo(item_id: str, title: str, budget: int, brief: str = "") -> dict[str, Any]:
    return {
        "id": item_id,
        "title": title,
        "status": "pending",
        "budget": budget,
        "brief": brief or f"{title}的简报",
    }


def page(url: str, text: str | None = None) -> dict[str, Any]:
    return {
        "url": url,
        "status": 200,
        "title": f"页面 {url}",
        "content": text if text is not None else f"{url} 的正文：关键数据与结论。",
        "content_type": "text/plain",
    }


def default_search(query: str, max_results: int) -> dict[str, Any]:
    urls = [f"https://s.example/{query}/{i}" for i in range(1, 3)]
    return {"results": [{"url": u, "title": f"结果 {u}", "snippet": "摘要"} for u in urls]}


class Run:
    def __init__(self, code: int, events: list[dict[str, Any]], gw: ScriptedGateway) -> None:
        self.code, self.events, self.gw = code, events, gw


def make_gateway(model: ScriptedModel, **kw: Any) -> ScriptedGateway:
    return ScriptedGateway(
        chat=model,
        search=kw.get("search") or default_search,
        fetch=kw.get("fetch") or page,
        tool_budget=kw.get("tool_budget"),
    )


def run_turn(
    model: ScriptedModel,
    config: dict[str, Any] | None = None,
    *,
    gw: ScriptedGateway | None = None,
    host: SessionHost | None = None,
    start: bool = True,
    **kw: Any,
) -> tuple[int, list[dict[str, Any]], ScriptedGateway]:
    """在一个 SessionHost 上运行（默认一轮 t-1/a-1）；同一 task 的各 attempt 共用一个
    ScriptedGateway（同一 call id 重放），其 call id 计数器取自 ctx。"""
    gw = gw or make_gateway(model, **kw)
    host = host or SessionHost()
    if start:
        start_task(host, "t-1", "a-1", config or {"text": "问题"})

    def factory(ctx: Any) -> ScriptedGateway:
        gw.call_ids = ctx.call_ids
        return gw

    code, events = host.run(make_app(gateway=factory, skills_root=SKILLS), timeout=20)
    return code, events, gw


def start_task(
    host: SessionHost, task_id: str, attempt_id: str, config: Any, **fields: Any
) -> None:
    out = _OUT["root"] / attempt_id
    host.start_task(task_id, attempt_id, config=config, out_dir=str(out), **fields)


def progress(events: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [e for e in events if e["type"] == "progress" and e["kind"] == kind]


def kinds(events: list[dict[str, Any]]) -> list[str]:
    return [e["kind"] for e in events if e["type"] == "progress"]


def result(events: list[dict[str, Any]]) -> dict[str, Any]:
    return next(e for e in events if e["type"] == "result")


def checkpoints(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in events if e["type"] == "checkpoint"]


def artifact_text(events: list[dict[str, Any]], artifact_id: str) -> str:
    ev = next(e for e in events if e["type"] == "artifact" and e["artifact_id"] == artifact_id)
    return (_OUT["root"] / ev["attempt_id"] / ev["path"]).read_text(encoding="utf-8")


def chat_models(gw: ScriptedGateway) -> dict[str, Any]:
    return {c.step_id: c.body.get("model") for c in gw.calls if c.kind == "chat"}


def without_sdk_key(state: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in state.items() if k != "_agentbox"}


def research_model() -> ScriptedModel:
    return ScriptedModel(
        orch=[
            call("read_skill", name="deep-research"),
            call("todo_write", items=[todo("1", "材料", 8), todo("2", "产业", 8)]),
            call("research_subtopic", id="1"),
            call("research_subtopic", id="2"),
            reply("## 摘要\n- 电解质路线分化 [1]\n- 量产在 2027 年前后 [2][9]"),
        ],
        sub={
            "1": [
                call("web_search", query="固态电解质"),
                call("web_fetch", url="https://a.example/1"),
                reply("硫化物路线领先 [1]", reasoning="先看材料路线"),
            ],
            "2": [call("web_fetch", url="https://b.example/1"), reply("量产时间 [2]")],
        },
    )


RESEARCH_CONFIG = {
    "text": "固态电池现状",
    "orchestrator_model": "kimi-k3",
    "worker_model": "kimi-k2.6",
}


# ---- 回答与研究 ----


def test_simple_question_is_answered_directly_without_skill():
    model = ScriptedModel(orch=[reply("北京是中国的首都。")])
    code, events, gw = run_turn(model, config={"text": "中国首都是哪里？"})
    assert code == 0
    assert kinds(events) == ["route", "assistant_delta"]
    assert progress(events, "route")[0]["data"] == {"route": "answer", "forced": False}
    delta = progress(events, "assistant_delta")[0]["data"]
    assert delta["text"] == "北京是中国的首都。" and delta["final"] is True and delta["message_id"]
    res = result(events)
    assert res["summary"] == "北京是中国的首都。" and res["outputs"] == []
    assert res["session_state"]["state"]["turns"][-1]["route"] == "answer"
    assert gw.counts() == {"chat": 1}
    body = model.bodies("orch")[0]
    assert {s["function"]["name"] for s in body["tools"]} == {
        "read_skill",
        "web_search",
        "web_fetch",
        "read_source",
    }
    assert "deep-research" in body["messages"][0]["content"]  # skill 目录在系统提示中


def test_research_flow_skill_todo_subtopics_report_with_checked_citations():
    model = research_model()
    code, events, gw = run_turn(model, config=RESEARCH_CONFIG)
    assert code == 0
    assert chat_models(gw) == {"orch": "kimi-k3", "sub-1": "kimi-k2.6", "sub-2": "kimi-k2.6"}
    report = artifact_text(events, "report")
    assert "[9]" not in report and "[1]" in report and "[2]" in report
    assert re.findall(r"^- \[(\d+)\] .* — sha256:[0-9a-f]{64}$", report, re.M) == ["1", "2"]
    ready = progress(events, "report_ready")[0]["data"]
    assert ready["partial"] is False and ready["tool_budget_reached"] is False
    assert ready["artifact_id"] == "report" and ready["title"] == "固态电池现状"
    assert [c["step_id"] for c in checkpoints(events)].count("sub-1") == 2  # 每次工具调用后一个
    assert all(r in gw.blobs for c in checkpoints(events) for r in c["refs"])
    res = result(events)
    assert res["outputs"] == ["report"]
    assert "电解质路线分化 [1]" in res["summary"] and "完整报告见右侧「报告」" in res["summary"]
    assert progress(events, "route")[0]["data"] == {"route": "research", "forced": False}
    subs = [(e["data"]["id"], e["data"]["status"]) for e in progress(events, "subtopic")]
    assert subs == [("1", "running"), ("1", "done"), ("2", "running"), ("2", "done")]
    assert progress(events, "subtopic")[1]["data"]["summary"] == "硫化物路线领先 [1]"
    todo_events = progress(events, "todo_updated")
    assert todo_events[0]["data"]["items"][0] == {
        "id": "1",
        "title": "材料",
        "status": "pending",
        "budget_share": 8,
    }
    assert [i["status"] for i in todo_events[-1]["data"]["items"]] == ["done", "done"]
    # 子主题的工具调用带 subtopic_id；搜索与抓取的 ⟨/⟩ 指向结果 blob，本地工具不带 raw
    fetch_result = next(
        e["data"] for e in progress(events, "tool_result") if e["data"]["tool"] == "web_fetch"
    )
    assert fetch_result["subtopic_id"] == "1" and fetch_result["step_id"] == "sub-1"
    assert fetch_result["raw"]["response_ref"] in gw.blobs
    assert fetch_result["raw"]["request"] == {"url": "https://a.example/1"}
    assert fetch_result["preview"]["kind"] == "fetch" and fetch_result["preview"]["n"] == 1
    local = [
        e["data"] for e in progress(events, "tool_result") if e["data"]["tool"] == "read_skill"
    ]
    assert local and all("raw" not in d for d in local)
    thinking = progress(events, "thinking")
    assert thinking and thinking[0]["data"]["raw"]["response_ref"] in gw.blobs
    # 子主题摘要回给编排模型
    rs = [m for m in model.bodies("orch")[3]["messages"] if m.get("name") == "research_subtopic"]
    assert rs[-1]["content"].startswith("子主题「材料」摘要：\n硫化物路线领先 [1]")


def test_deep_research_toggle_reads_skill_before_first_model_call():
    model = ScriptedModel(orch=[reply("暂时没有材料。")])
    cfg = {"text": "储能电池市场", "deep_research": True}
    code, events, gw = run_turn(model, config=cfg)
    assert code == 0
    assert kinds(events)[:2] == ["skill_read", "route"]
    assert progress(events, "route")[0]["data"] == {"route": "research", "forced": True}
    assert progress(events, "skill_read")[0]["data"]["name"] == "deep-research"
    first = model.bodies("orch")[0]["messages"]
    assert first[-2]["tool_calls"][0]["function"]["name"] == "read_skill"
    assert first[-1]["role"] == "tool" and "深度研究" in first[-1]["content"]
    assert "本轮必须按 deep-research skill 研究" in first[0]["content"]
    # skill 已读：ask_user、todo_write 可用
    names = {s["function"]["name"] for s in model.bodies("orch")[0]["tools"]}
    assert {"ask_user", "todo_write"} <= names and "research_subtopic" not in names
    assert result(events)["summary"] == "暂时没有材料。"  # 没有材料：不写报告


def test_tool_results_show_remaining_budget_and_budget_events_follow():
    model = ScriptedModel(
        orch=[
            call("web_search", query="q"),
            call("web_fetch", url="https://a.example/1"),
            calls(("web_fetch", {"url": "https://a.example/2"}), ("read_source", {"n": 1})),
            reply("答案 [1]"),
        ]
    )
    code, events, gw = run_turn(model, tool_budget=lambda k: (k, 30))
    assert code == 0
    last = model.bodies("orch")[-1]["messages"]
    counted = [m for m in last if m.get("name") in ("web_search", "web_fetch")]
    assert len(counted) == 3
    for k, m in enumerate(counted, 1):
        assert f"已用 {k}/30，剩余 {30 - k}" in m["content"]
    seq = [(e["kind"], e["data"].get("tool")) for e in events if e["type"] == "progress"]
    for i, (kind, tool) in enumerate(seq):
        if kind == "tool_result" and tool in ("web_search", "web_fetch"):
            assert seq[i + 1][0] == "budget"
    assert [e["data"] for e in progress(events, "budget")] == [
        {"used": k, "limit": 30} for k in (1, 2, 3)
    ]
    assert result(events)["outputs"] == []  # 回答路径：不写报告


def test_budget_exhausted_writes_report_and_marks_turn():
    def fetch(url: str) -> dict[str, Any]:
        if url.endswith("/3"):
            raise ToolBudgetExhausted(429, "tool_budget_exhausted", "本轮额度已用尽")
        return page(url)

    model = ScriptedModel(
        orch=[
            call("read_skill", name="deep-research"),
            call("todo_write", items=[todo("1", "材料", 8), todo("2", "产业", 8)]),
            call("research_subtopic", id="1"),
            reply("# 固态电池\n\n## 摘要\n- 路线 [1][2]"),
        ],
        sub={
            "1": [
                call("web_fetch", url="https://a.example/1"),
                call("web_fetch", url="https://a.example/2"),
                call("web_fetch", url="https://a.example/3"),
                reply("摘要 [1][2]"),
            ]
        },
    )
    code, events, gw = run_turn(
        model, config=RESEARCH_CONFIG, fetch=fetch, tool_budget=lambda k: (k, 30)
    )
    assert code == 0
    assert "tools" not in model.bodies("sub-1")[-1]
    last_orch = model.bodies("orch")[-1]
    assert "tools" not in last_orch
    assert "工具额度已用尽" in last_orch["messages"][-1]["content"]
    report = artifact_text(events, "report")
    assert "> 已达工具额度（30/30）" in report
    ready = progress(events, "report_ready")[0]["data"]
    assert ready["tool_budget_reached"] is True and ready["note"] == "已达工具额度（30/30）"
    exhausted = [e["data"] for e in progress(events, "tool_result") if not e["data"]["ok"]]
    assert exhausted and "额度" in exhausted[0]["error"]
    assert result(events)["outputs"] == ["report"]
    assert [c.body["url"] for c in gw.calls if c.kind == "fetch"][-1] == "https://a.example/3"


def test_empty_completion_retries_with_double_max_tokens_then_fails_turn():
    model = ScriptedModel(
        orch=[reply("", finish_reason="length"), reply("<think>只有思考</think>")]
    )
    code, events, gw = run_turn(model)
    assert code == 0  # session 继续；本轮以 error 提议
    assert [b["max_tokens"] for b in model.bodies("orch")] == [16384, 32768]
    err = next(e for e in events if e["type"] == "error")
    assert err["code"] == "model_unavailable"
    assert err["message"] == "模型服务暂时不可用，请重试" and err["retryable"] is False
    assert "result" not in [e["type"] for e in events]


def test_model_5xx_retried_once():
    model = ScriptedModel(orch=[GatewayError(502, "upstream_error", "bad gateway"), reply("好的")])
    code, events, gw = run_turn(model)
    assert code == 0
    ids = [c.call_id for c in gw.calls if c.kind == "chat"]
    assert ids == ["root/orch/chat/1", "root/orch/chat/2"]
    assert [e["data"]["text"] for e in progress(events, "assistant_delta")] == ["好的"]
    assert result(events)["summary"] == "好的"


def test_model_failing_twice_fails_turn_with_user_message():
    model = ScriptedModel(
        orch=[GatewayError(503, "upstream_error"), GatewayError(504, "call_deadline_exceeded")]
    )
    code, events, gw = run_turn(model)
    err = next(e for e in events if e["type"] == "error")
    assert err["code"] == "model_unavailable" and err["retryable"] is False


def test_unreachable_page_is_recorded_and_research_continues():
    def fetch(url: str) -> dict[str, Any]:
        if "down" in url:
            raise GatewayError(502, "fetch_failed", "连接被重置")
        return page(url)

    model = ScriptedModel(
        orch=[
            call("read_skill", name="deep-research"),
            call("todo_write", items=[todo("1", "材料", 8), todo("2", "产业", 8)]),
            call("research_subtopic", id="1"),
            reply("## 摘要\n- 结论 [1]"),
        ],
        sub={
            "1": [
                call("web_fetch", url="https://down.example/x"),
                call("web_fetch", url="https://a.example/1"),
                reply("结论 [1]"),
            ]
        },
    )
    code, events, gw = run_turn(model, fetch=fetch)
    assert code == 0
    results = [
        e["data"] for e in progress(events, "tool_result") if e["data"]["tool"] == "web_fetch"
    ]
    assert results[0]["ok"] is False and "fetch_failed" in results[0]["error"]
    assert results[1]["ok"] is True and results[1]["preview"]["n"] == 1
    assert progress(events, "budget")[0]["data"] == {"used": 1, "limit": 30}
    done = [e["data"] for e in progress(events, "subtopic") if e["data"]["status"] == "done"]
    assert done and done[0]["id"] == "1"
    assert result(events)["outputs"] == ["report"]


def test_assistant_messages_are_gateway_compatible():
    model = research_model()
    run_turn(model, config=RESEARCH_CONFIG)
    assert len(model.requests) == 10
    for _, body in model.requests:
        msgs = body["messages"]
        open_ids: set[str] = set()
        for m in msgs:
            assert set(m) <= ALLOWED_MESSAGE_KEYS, m
            assert isinstance(m["content"], str)
            assert "reasoning_content" not in m
            if m["role"] == "assistant":
                open_ids = {tc["id"] for tc in m.get("tool_calls") or []}
                for tc in m.get("tool_calls") or []:
                    assert set(tc) == {"id", "type", "function"}
                    assert isinstance(tc["function"]["arguments"], str)
            elif m["role"] == "tool":
                assert m["tool_call_id"] in open_ids
        assert "<think>" not in json.dumps(body, ensure_ascii=False)


# ---- 提问 ----


QUESTIONS = [
    {"question": "关注哪个地区？", "options": ["中国", "全球"]},
    {"question": "研究用途？", "options": ["投资", "一般了解"]},
]


def test_ask_user_pauses_turn_awaiting_input_without_counting_budget():
    model = ScriptedModel(
        orch=[
            call("read_skill", name="deep-research"),
            calls(("ask_user", {"questions": QUESTIONS}), ("web_search", {"query": "x"})),
        ]
    )
    code, events, gw = run_turn(model, config={"text": "储能电池市场"})
    assert code == 0
    proposal = next(e for e in events if e["type"] == "awaiting_input")
    assert proposal["question_id"] == "q-orch-2"
    ask = progress(events, "ask_user")[0]["data"]
    assert ask["question_id"] == "q-orch-2" and ask["step_id"] == "orch"
    assert [q["id"] for q in ask["questions"]] == ["1", "2"]
    assert all(q["allow_other"] is True for q in ask["questions"])
    assert ask["questions"][0]["options"] == ["中国", "全球"]
    assert gw.counts() == {"chat": 2}  # 没有搜索或抓取
    last = checkpoints(events)[-1]
    assert last["step_id"] == "ask" and proposal["checkpoint_id"] == last["checkpoint_id"]
    state = last["state"]
    assert state["pending_question"]["question_id"] == "q-orch-2"
    assert state["budget"]["used"] == 0
    tool_msgs = [m for m in state["messages"] if m["role"] == "tool"]
    assert (
        tool_msgs[-1]["name"] == "web_search" and tool_msgs[-1]["content"] == "已跳过：等待用户回答"
    )
    assert "result" not in [e["type"] for e in events]


def test_ask_user_after_todo_write_is_refused():
    model = ScriptedModel(
        orch=[
            call("read_skill", name="deep-research"),
            call("todo_write", items=[todo("1", "材料", 8), todo("2", "产业", 8)]),
            call("ask_user", questions=QUESTIONS),
            reply("按默认假设继续。"),
        ]
    )
    code, events, gw = run_turn(model)
    asks = [e["data"] for e in progress(events, "tool_result") if e["data"]["tool"] == "ask_user"]
    assert asks[0]["ok"] is False and "计划" in asks[0]["error"]
    assert not progress(events, "ask_user")
    assert result(events)["summary"] == "按默认假设继续。"


# ---- 状态、压缩与恢复 ----


def test_state_compaction_keeps_checkpoint_under_limit_and_is_deterministic():
    urls = [f"https://big.example/{i}" for i in range(20)]
    model = ScriptedModel(orch=[*(call("web_fetch", url=u) for u in urls), reply("完成")])
    code, events, gw = run_turn(model, fetch=lambda url: page(url, "字" * 2100 + url))
    assert code == 0
    cps = checkpoints(events)
    assert len(cps) == 20
    assert all(json_size(without_sdk_key(c["state"])) <= STATE_LIMIT_BYTES for c in cps)
    assert json_size(without_sdk_key(cps[-1]["state"])) > 150 * 1024  # 压缩是必要的
    contents = [m["content"] for m in cps[-1]["state"]["messages"] if m["role"] == "tool"]
    assert contents[0] == OMITTED_SOURCE and contents[-1] != OMITTED_SOURCE
    # 之后的提示词使用压缩后的状态
    assert OMITTED_SOURCE in json.dumps(model.bodies("orch")[-1], ensure_ascii=False)
    # 同一状态两次压缩结果相同
    a = TurnState.from_json(without_sdk_key(cps[-1]["state"]))
    b = TurnState.from_json(without_sdk_key(cps[-1]["state"]))
    a.compact(100 * 1024)
    b.compact(100 * 1024)
    assert a.to_json() == b.to_json() and json_size(a.to_json()) <= 100 * 1024


def test_state_round_trips_and_rejects_bad_versions():
    model = research_model()
    _, events, _ = run_turn(model, config=RESEARCH_CONFIG)
    for cp in checkpoints(events):
        raw = without_sdk_key(cp["state"])
        assert TurnState.from_json(raw).to_json() == raw
    with pytest.raises(ValueError):
        TurnState.from_json({**raw, "schema_version": 2})
    with pytest.raises(ValueError):
        TurnState.from_json({**raw, "phase": "flying"})


def test_crash_resume_replays_same_call_ids_and_request_bodies():
    model = research_model()
    code, events, gw = run_turn(model, config=RESEARCH_CONFIG)
    assert code == 0
    first_calls = {c.call_id: c.body for c in gw.calls}
    report = artifact_text(events, "report")
    n_requests = len(model.requests)
    for cp in [c for c in checkpoints(events) if c["step_id"] in ("sub-1", "orch")]:
        before = len(gw.calls)
        host = SessionHost()
        resume = {
            "checkpoint_id": cp["checkpoint_id"],
            "step_id": cp["step_id"],
            "state": cp["state"],
            "refs": cp["refs"],
        }
        start_task(host, "t-1", "a-2", RESEARCH_CONFIG, attempt_no=2, resume=resume)
        code2, events2, _ = run_turn(model, gw=gw, host=host, start=False)
        assert code2 == 0 and len(gw.calls) > before
        assert len(model.requests) == n_requests  # 全部是同一 call id 的重放，没有新调用
        snapshot = cp["state"]["_agentbox"]["call_ids"]
        for c in gw.calls[before:]:
            assert c.call_id in first_calls and c.body == first_calls[c.call_id]
            prefix, n = c.call_id.rsplit("/", 1)
            assert int(n) > snapshot.get(prefix, 0)  # 已记录的调用不再发起
        assert artifact_text(events2, "report") == report
        assert result(events2)["outputs"] == ["report"]


def test_follow_up_turn_sees_previous_report_in_memory():
    model1 = research_model()
    model2 = ScriptedModel(orch=[call("read_source", n=1), reply("上一轮的来源 [1] 说明了路线。")])
    shared: dict[str, bytes] = {}
    gws: dict[str, ScriptedGateway] = {}

    def factory(ctx: Any) -> ScriptedGateway:
        if ctx.task_id not in gws:
            gw = make_gateway(model1 if ctx.task_id == "t-1" else model2)
            gw.blobs = shared
            gws[ctx.task_id] = gw
        gws[ctx.task_id].call_ids = ctx.call_ids
        return gws[ctx.task_id]

    host = SessionHost()
    start_task(host, "t-1", "a-1", RESEARCH_CONFIG)
    start_task(host, "t-2", "a-2", {"text": "第一个来源讲了什么？"})
    code, events = host.run(make_app(gateway=factory, skills_root=SKILLS), timeout=20)
    assert code == 0
    system = model2.bodies("orch")[0]["messages"][0]["content"]
    assert "报告《固态电池现状》" in system and "第 1 轮 [1]" in system
    history = model2.bodies("orch")[0]["messages"][1:-1]
    assert [m["role"] for m in history] == ["user", "assistant"]
    src1 = next(s for s in checkpoints(events)[-1]["state"]["sources"] if s["n"] == 1)
    assert src1["sha256"] in gws["t-2"].blob_reads
    tool = [m for m in model2.bodies("orch")[1]["messages"] if m.get("name") == "read_source"]
    assert "a.example/1 的正文" in tool[0]["content"]
    results = [e for e in events if e["type"] == "result"]
    assert len(results) == 2
    memory = results[1]["session_state"]["state"]
    assert [t["task_id"] for t in memory["turns"]] == ["t-1", "t-2"]
    assert memory["turns"][0]["report"]["title"] == "固态电池现状"
    assert {s["origin"] for s in memory["sources"]} == {"t-1"}


# ---- 纯函数 ----


def test_parse_turn_config_validates_and_defaults():
    cfg = parse_turn_config({"text": "  你好 ", "orchestrator_model": None, "tool_budget": 12})
    assert cfg.text == "你好" and cfg.tool_budget == 12 and cfg.orchestrator_model is None
    for bad in (
        None,
        {},
        {"text": " "},
        {"text": "x" * 4001},
        {"text": "x", "tool_budget": 0},
        {"text": "x", "deep_research": "yes"},
        {"text": "x", "worker_model": ""},
    ):
        with pytest.raises(Exception) as exc:
            parse_turn_config(bad)
        assert getattr(exc.value, "code", None) == "invalid_config"


def test_invalid_config_fails_turn():
    code, events, _ = run_turn(ScriptedModel(), config={"text": ""})
    assert next(e for e in events if e["type"] == "error")["code"] == "invalid_config"


def test_citation_pass_removes_unknown_numbers_and_model_reference_sections():
    from agentbox_worker.tools.sources import SourceStore

    store = SourceStore()
    store.add(sha256="a" * 64, url="https://a/1", title="A", excerpt="", call_id="c1")
    store.add(sha256="b" * 64, url="https://b/1", title="B", excerpt="", call_id="c2")
    text = (
        "<think>草稿</think>结论 [2][7]，另见 [1, 5]。\n\n## 参考来源\n- [1] A\n\n## 局限\n少 [3]"
    )
    body, cited = citation_pass(text, store)
    assert cited == [2, 1]
    assert "[7]" not in body and "[5]" not in body and "[3]" not in body
    assert "参考来源" not in body and "## 局限" in body and "草稿" not in body
    md = render_report(body, "标题", store, cited, ["部分研究"])
    assert md.startswith("# 标题\n\n> 部分研究\n\n")
    assert re.findall(r"^- \[(\d+)\]", md, re.M) == ["2", "1"]
    assert report_summary("# T\n\n> 注\n\n首段文字。\n\n## 细节") == "首段文字。"


# ---- 依赖约束 ----


BANNED_CALLS = re.compile(r"^os\.(system|popen|fork|exec\w*|spawn\w*|posix_spawn\w*)$")


def test_chatagent_imports_only_stdlib_and_sdk():
    pyproject = tomllib.loads((WORKER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["dependencies"] == []
    assert "chatagent" in pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    contracts = pyproject["tool"]["importlinter"]["contracts"]
    forbidden = {
        m for c in contracts if c["source_modules"] == ["chatagent"] for m in c["forbidden_modules"]
    }
    assert forbidden == FORBIDDEN_IMPORTS
    sdk = next(c for c in contracts if c["source_modules"] == ["agentbox_worker"])
    assert "chatagent" in sdk["forbidden_modules"]
    for path in (WORKER_ROOT / "chatagent").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            elif isinstance(node, ast.Attribute):
                assert not BANNED_CALLS.match(ast.unparse(node)), (path, ast.unparse(node))
                continue
            else:
                continue
            for name in names:
                top = name.split(".")[0]
                assert top not in FORBIDDEN_IMPORTS, (path, name)
                assert top in sys.stdlib_module_names or top in ("agentbox_worker", "chatagent"), (
                    path,
                    name,
                )


def test_event_tool_call_ids_are_unique_even_when_model_ids_repeat():
    def kimi_call(name: str, **args: Any) -> dict[str, Any]:
        item = call(name, **args)
        item["message"]["tool_calls"][0]["id"] = f"{name}:0"  # kimi 的 id 每轮从 0 编号
        return item

    model = ScriptedModel(
        orch=[
            kimi_call("web_fetch", url="https://a.example/1"),
            kimi_call("web_fetch", url="https://a.example/2"),
            reply("完成 [1][2]"),
        ]
    )
    code, events, gw = run_turn(model)
    assert code == 0
    call_ids = [e["data"]["tool_call_id"] for e in progress(events, "tool_call")]
    result_ids = [e["data"]["tool_call_id"] for e in progress(events, "tool_result")]
    assert call_ids == result_ids == ["orch:1", "orch:2"]
    tool_msgs = [m for m in model.bodies("orch")[-1]["messages"] if m["role"] == "tool"]
    assert [m["tool_call_id"] for m in tool_msgs] == ["web_fetch:0", "web_fetch:0"]
