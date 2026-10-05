"""deepresearch 包：状态与迁移、任务解析、各步骤对 Gateway 的请求形状、lint 契约。

依赖 Unix socket 的测试使用 conftest 的 fake_gateway（没有 AF_UNIX 的平台跳过，见 conftest）；
其余为纯进程内测试。
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
from conftest import FakeGateway

from agentbox_worker import CallIds, GatewayClient, GatewayResult
from deepresearch import (
    EVIDENCE_MAX_BYTES,
    Evidence,
    ResearchState,
    ResearchTask,
    is_fallback,
    migrate,
    parse_tasks,
    plan,
    prompts,
    search_and_fetch,
    steps,
    summarize,
    write_report,
)

WORKER_ROOT = Path(__file__).resolve().parent.parent


def sha(tag: str) -> str:
    return hashlib.sha256(tag.encode()).hexdigest()


def sample_state() -> ResearchState:
    ev = [
        Evidence(sha("a"), "https://a.example/x", "A 标题", "片段 A", "root/task-1/fetch/1"),
        Evidence(sha("b"), "https://b.example/y", "B 标题", "片段 B", "root/task-1/fetch/2"),
        Evidence(sha("c"), "https://c.example/z", "C 标题", "片段 C", "root/task-2/fetch/1"),
    ]
    return ResearchState(
        topic="固态电池",
        tasks=[
            ResearchTask(
                1,
                "材料",
                "电解质路线",
                "固态电解质",
                "done",
                "发现一 [2]；发现二 [1, 2]",
                [ev[0].sha256, ev[1].sha256],
            ),
            ResearchTask(
                2,
                "产业",
                "量产进度",
                "固态电池 量产",
                "done",
                "发现三 [1]；越界 [5]",
                [ev[2].sha256],
            ),
        ],
        evidence={e.sha256: e for e in ev},
        loop_count=1,
        failures=["plan: 非 JSON"],
    )


# ---- 状态与迁移 ----


def test_migrate_v1_is_identity_and_unknown_versions_fail():
    d = sample_state().to_json()
    assert migrate(d) is d
    for bad in (
        {**d, "schema_version": 2},
        {**d, "schema_version": 0},
        {**d, "schema_version": "1"},
        {**d, "schema_version": True},
        {k: v for k, v in d.items() if k != "schema_version"},
        [d],
    ):
        with pytest.raises(ValueError):
            migrate(bad)


def test_state_json_round_trip_and_validation():
    state = sample_state()
    d = json.loads(json.dumps(state.to_json(), ensure_ascii=False))  # 如同经 checkpoint
    assert "_agentbox" not in d  # SDK 保留键不被应用状态占用
    assert ResearchState.from_json(migrate(d)) == state
    assert ResearchState.from_json(ResearchState().to_json()) == ResearchState()
    wrong_key = json.loads(json.dumps(d))
    wrong_key["evidence"][sha("x")] = wrong_key["evidence"].pop(sha("a"))
    bad_status = json.loads(json.dumps(d))
    bad_status["tasks"][0]["status"] = "running"
    for bad in (wrong_key, bad_status, {**d, "schema_version": 2}, {**d, "loop_count": -1}):
        with pytest.raises(ValueError):
            ResearchState.from_json(bad)


# ---- 任务解析 ----

TASKS_JSON = (
    '{"tasks": [{"title": "背景", "intent": "梳理背景", "query": "q1"}, {"title": "现状"}]}'
)


@pytest.mark.parametrize(
    "text",
    [
        TASKS_JSON,
        f"好的，计划如下：\n```json\n{TASKS_JSON}\n```\n请确认。",
        f"<think>先想想 {{草稿}}</think>说明：任务见 {{下文}}。{TASKS_JSON} 以上共两项。",
    ],
    ids=["pure-json", "fenced", "prose"],
)
def test_parse_tasks_tolerant_extraction(text):
    tasks = parse_tasks(text)
    assert [(t.id, t.title, t.intent, t.query) for t in tasks] == [
        (1, "背景", "梳理背景", "q1"),
        (2, "现状", "聚焦主题的关键问题", "现状"),  # 缺省字段按原项目规则补齐
    ]
    assert all(t.status == "pending" and t.evidence == [] for t in tasks)


def test_parse_tasks_returns_empty_on_garbage():
    assert parse_tasks("抱歉，我无法规划。") == []
    assert parse_tasks('{"tasks": []}') == []
    assert parse_tasks('{"tasks": "x"} [1, 2]') == []
    assert parse_tasks("[" * 5000) == []  # 过深嵌套（RecursionError）同样回退，不中止


def test_truncate_utf8_never_splits_characters():
    text = "证据" * 3000
    out = steps.truncate_utf8(text, EVIDENCE_MAX_BYTES)
    assert len(out.encode("utf-8")) <= EVIDENCE_MAX_BYTES and out.endswith("…")
    assert steps.truncate_utf8("短", EVIDENCE_MAX_BYTES) == "短"


# ---- 步骤与 fake Gateway ----


class Script:
    """fake Gateway 的研究脚本：chat 按 system 提示词中的阶段标记回复，search/fetch 按请求回复。"""

    def __init__(self) -> None:
        self.chat: dict[str, str] = {}
        self.search: list[dict[str, Any]] = []
        self.pages: dict[str, dict[str, Any]] = {}

    def __call__(self, kind: str, body: dict) -> dict:
        if kind == "chat":
            system = body["messages"][0]["content"]
            stage = next(s for s in self.chat if system.startswith(s))
            return {"choices": [{"message": {"role": "assistant", "content": self.chat[stage]}}]}
        if kind == "search":
            return {"query": body["query"], "results": self.search[: body["max_results"]]}
        return {"url": body["url"], "status": 200, **self.pages[body["url"]]}


@pytest.fixture
def research(fake_gateway: FakeGateway) -> tuple[FakeGateway, GatewayClient, Script]:
    script = Script()
    fake_gateway.responder = script
    return (
        fake_gateway,
        GatewayClient(fake_gateway.socket_path, call_ids=CallIds(), timeout_s=5),
        script,
    )


def billed(gw: FakeGateway) -> list[tuple[str, str, Any]]:
    return [(r.path, r.headers["x-agentbox-call-id"], r.json()) for r in gw.requests]


def test_plan_request_shape_truncation_and_replay(research):
    fake, gw, script = research
    many = [{"title": f"t{i}", "intent": "i", "query": f"q{i}"} for i in range(6)]
    script.chat[prompts.STAGE_PLAN] = json.dumps({"tasks": many})
    calls: list[GatewayResult] = []
    tasks, res = plan(gw, "固态电池", max_tasks=3, calls=calls)
    assert [t.query for t in tasks] == ["q0", "q1", "q2"]
    assert calls == [res] and res.blob_sha256 == fake.calls[res.call_id]
    ((path, call_id, body),) = billed(fake)
    assert (path, call_id) == ("/v1/chat/completions", "root/plan/chat/1")
    assert body["max_tokens"] == steps.PLAN_MAX_TOKENS
    system, user = body["messages"]
    assert system["role"] == "system" and system["content"].startswith(prompts.STAGE_PLAN)
    assert "不超过 3 个" in system["content"]
    assert user == {"role": "user", "content": "研究主题：固态电池"}
    # 提示词确定性：恢复后（计数器回到快照）同一调用得到重放，而不是指纹冲突
    again = GatewayClient(fake.socket_path, call_ids=CallIds(), timeout_s=5)
    tasks2, res2 = plan(again, "固态电池", max_tasks=3)
    assert res2.replayed and res2.call_id == res.call_id and tasks2 == tasks
    assert fake.requests[1].body == fake.requests[0].body


def test_plan_falls_back_on_non_json(research):
    fake, gw, script = research
    script.chat[prompts.STAGE_PLAN] = "我认为应该先了解背景。"
    tasks, _ = plan(gw, "固态电池", max_tasks=4)
    assert is_fallback(tasks)
    assert tasks[0].query == "固态电池 最新进展"


def test_search_and_fetch_evidence_comes_from_fetch_blobs(research):
    fake, gw, script = research
    script.search = [
        {"url": "https://a.example/1", "title": "A", "snippet": "搜索片段 A"},
        {"url": "ftp://skip.example/", "title": "非 http", "content": "x"},
        {"url": "https://a.example/1", "title": "A 重复", "content": "重复"},
        {"url": "https://b.example/2", "title": "B", "content": "搜索片段 B"},
        {"url": "https://c.example/3", "title": "C", "content": "超出 max_fetch"},
    ]
    script.pages = {
        "https://a.example/1": {
            "content_type": "text/html; charset=utf-8",
            "text": "<html><head><title>t</title><script>evil()</script></head>"
            "<body><p>正文&amp;内容</p></body></html>",
        },
        "https://b.example/2": {"content_type": "text/plain", "text": "长" * 10_000},
    }
    task = ResearchTask(1, "材料", "电解质", "固态电解质")
    calls: list[GatewayResult] = []
    evidence = search_and_fetch(gw, task, max_results=5, max_fetch=2, step_id="task-1", calls=calls)
    reqs = billed(fake)
    assert [(p, c) for p, c, _ in reqs] == [
        ("/v1/search", "root/task-1/search/1"),
        ("/v1/fetch", "root/task-1/fetch/1"),
        ("/v1/fetch", "root/task-1/fetch/2"),
    ]
    assert reqs[0][2] == {"query": "固态电解质", "max_results": 5}
    assert [r[2] for r in reqs[1:]] == [
        {"url": "https://a.example/1"},
        {"url": "https://b.example/2"},
    ]
    assert [c.call_id for c in calls] == [c for _, c, _ in reqs]
    # 证据 sha 即 fetch 响应的 X-Agentbox-Blob（fake 记录的该调用结果 blob）
    assert [e.sha256 for e in evidence] == [
        fake.calls["root/task-1/fetch/1"],
        fake.calls["root/task-1/fetch/2"],
    ]
    assert [e.call_id for e in evidence] == ["root/task-1/fetch/1", "root/task-1/fetch/2"]
    assert [(e.url, e.title) for e in evidence] == [
        ("https://a.example/1", "A"),
        ("https://b.example/2", "B"),
    ]
    assert evidence[0].snippet == "搜索片段 A\n\n正文&内容"  # HTML 转纯文本，script/head 被丢弃
    assert evidence[1].snippet.startswith("搜索片段 B\n\n长")
    assert len(evidence[1].snippet.encode("utf-8")) <= EVIDENCE_MAX_BYTES


def test_summarize_prompt_numbers_evidence_and_bounds_snippets(research):
    fake, gw, script = research
    script.chat[prompts.STAGE_SUMMARIZE] = "<think>草稿</think>## 任务总结\n- 发现 [1][2]"
    big = "片" * 5000  # 未截断的长片段（如旧状态）进入提示词前也被截断
    evidence = [
        Evidence(sha("a"), "https://a.example/1", "A", "片段 A", "c1"),
        Evidence(sha("b"), "https://b.example/2", "B", big, "c2"),
    ]
    task = ResearchTask(1, "材料", "电解质路线", "固态电解质")
    summary = summarize(gw, task, evidence, step_id="task-1")
    assert summary == "## 任务总结\n- 发现 [1][2]"
    ((path, call_id, body),) = billed(fake)
    assert (path, call_id) == ("/v1/chat/completions", "root/task-1/chat/1")
    assert body["max_tokens"] == steps.SUMMARY_MAX_TOKENS
    system, user = (m["content"] for m in body["messages"])
    assert system.startswith(prompts.STAGE_SUMMARIZE)
    assert "任务：材料" in user and "任务目标：电解质路线" in user
    assert "[1] 标题：A\n来源：https://a.example/1\n片段：片段 A" in user
    assert "[2] 标题：B\n来源：https://b.example/2\n片段：" in user
    snippet2 = user.split("[2] 标题：B\n来源：https://b.example/2\n片段：", 1)[1]
    assert len(snippet2.encode("utf-8")) <= EVIDENCE_MAX_BYTES
    assert sha("a") not in user  # 提示词只含编号与片段，不含 blob 引用
    # 没有证据：不调用模型
    assert summarize(gw, task, [], step_id="task-2") == steps.NO_INFO
    assert len(fake.requests) == 1


def test_write_report_renumbers_citations_and_appends_evidence_list(research):
    fake, gw, script = research
    script.chat[prompts.STAGE_REPORT] = (
        "## 核心洞见\n- 结论 [1]，另见 [3, 9]\n\n## 参考来源\n- [1] 模型编造的来源"
    )
    state = sample_state()
    calls: list[GatewayResult] = []
    report = write_report(gw, state, step_id="report", calls=calls)
    ((path, call_id, body),) = billed(fake)
    assert (path, call_id) == ("/v1/chat/completions", "root/report/chat/1")
    assert body["max_tokens"] == steps.REPORT_MAX_TOKENS and len(calls) == 1
    system, user = (m["content"] for m in body["messages"])
    assert system.startswith(prompts.STAGE_REPORT)
    # 任务 2 的局部 [1] → 全局 [3]；越界编号删除；任务 1 的 [1, 2] → [1][2]
    assert "发现一 [2]；发现二 [1][2]" in user
    assert "发现三 [3]；越界 " in user and "[5]" not in user
    assert "[3] C 标题 — https://c.example/z" in user
    assert "片段" not in user.split("证据目录：", 1)[1]  # 目录只含编号、标题与 URL
    # 报告：模型自写的来源节被替换；不存在的编号被删除；每个 [n] 都映射到状态中的证据 sha
    assert "模型编造" not in report and "[9]" not in report
    assert report.startswith("# 固态电池\n") and "- 结论 [1]，另见 [3]" in report
    listing = dict(re.findall(r"^- \[(\d+)\] .* — sha256:([0-9a-f]{64})$", report, re.MULTILINE))
    assert listing == {"1": sha("a"), "2": sha("b"), "3": sha("c")}
    body_part = report.split("\n## 证据\n", 1)[0]
    assert {n for n in re.findall(r"\[(\d+)\]", body_part)} <= set(listing)
    assert all(state.evidence[s].url in report for s in listing.values())


# ---- lint 契约 ----

FORBIDDEN_IMPORTS = {
    "sqlite3", "psycopg", "redis", "requests", "httpx", "urllib", "socket",
    "subprocess", "multiprocessing",
}  # fmt: skip
BANNED_CALLS = re.compile(r"^os\.(system|popen|fork|forkpty|exec\w*|spawn\w*|posix_spawnp?)$")


def test_deepresearch_imports_only_stdlib_and_sdk():
    pyproject = tomllib.loads((WORKER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["dependencies"] == []
    contracts = pyproject["tool"]["importlinter"]["contracts"]
    forbidden = {
        m
        for c in contracts
        if c["source_modules"] == ["deepresearch"]
        for m in c["forbidden_modules"]
    }
    assert forbidden == FORBIDDEN_IMPORTS
    sdk = next(c for c in contracts if c["source_modules"] == ["agentbox_worker"])
    assert "deepresearch" in sdk["forbidden_modules"]
    for path in (WORKER_ROOT / "deepresearch").glob("*.py"):
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
                assert top in sys.stdlib_module_names or top in (
                    "agentbox_worker",
                    "deepresearch",
                ), (path, name)


# ---- 研究循环（app.run）：经 SDK 运行时 + fake Gateway ----

import asyncio
import itertools
from collections.abc import Callable

from conftest import Recorded, Reply, error_reply

from agentbox_worker import MemoryTransport, TaskContext, Timing, run_worker
from agentbox_worker.runtime import GATEWAY_SOCKET_ENV
from deepresearch import app as dr_app

LOOP_TIMING = Timing(ack_timeout=1.0, max_ack_attempts=2, retry_backoff=0.01, artifact_timeout=2)
LOOP_INIT = {
    "type": "init",
    "bootstrap": 1,
    "protocol_versions": [1],
    "mode": "task",
    "task_id": "t-1",
    "attempt_id": "a-1",
    "attempt_no": 1,
}
PLAN_TASKS = [
    {"title": "材料", "intent": "电解质路线", "query": "固态电解质"},
    {"title": "产业", "intent": "量产进度", "query": "固态电池 量产"},
]
EVIDENCE_LINE = re.compile(r"^- \[(\d+)\] .* — sha256:([0-9a-f]{64})$", re.MULTILINE)


class LoopScript:
    """2 个任务；每个查询 2 条搜索结果（max_fetch=1 时只抓取第 1 条）；chat 按阶段标记回复。"""

    def __init__(self) -> None:
        self.chat = {
            prompts.STAGE_PLAN: json.dumps({"tasks": PLAN_TASKS}, ensure_ascii=False),
            prompts.STAGE_SUMMARIZE: "## 任务总结\n- 关键发现 [1]",
            prompts.STAGE_REPORT: "## 核心洞见\n本研究综述了固态电池 [1]，量产仍有挑战 [2]。",
        }

    def __call__(self, kind: str, body: dict) -> dict:
        if kind == "chat":
            system = body["messages"][0]["content"]
            stage = next(s for s in self.chat if system.startswith(s))
            return {"choices": [{"message": {"role": "assistant", "content": self.chat[stage]}}]}
        if kind == "search":
            query = body["query"]
            slug = hashlib.sha256(query.encode()).hexdigest()[:8]
            results = [
                {
                    "url": f"https://{slug}.example/{i}",
                    "title": f"{query} {i}",
                    "snippet": f"片段 {i}",
                }
                for i in (1, 2)
            ]
            return {"query": query, "results": results[: body["max_results"]]}
        url = body["url"]
        return {"url": url, "status": 200, "content_type": "text/plain", "text": f"{url} 的正文"}


def ok_host(pause_at: str | None = None) -> Callable[[dict, MemoryTransport], None]:
    """宿主：checkpoint 一律 committed，产物一律 saved；pause_at 的 checkpoint 之前先送 pause。"""

    def respond(msg: dict, transport: MemoryTransport) -> None:
        def send(m: dict) -> None:
            transport.feed(json.dumps({"v": 1, **m}).encode())

        if msg["type"] == "checkpoint":
            if msg["step_id"] == pause_at:
                send({"type": "pause", "attempt_id": "a-1", "reason": "user", "grace_ms": 0})
            send(
                {
                    "type": "checkpoint_result",
                    "checkpoint_id": msg["checkpoint_id"],
                    "scope": "task",
                    "status": "committed",
                }
            )
        elif msg["type"] == "artifact":
            send(
                {
                    "type": "artifact_result",
                    "artifact_id": msg["artifact_id"],
                    "status": "saved",
                    "version": 1,
                    "sha256": msg["declared_sha256"],
                }
            )

    return respond


class Killed(Exception):
    pass


def kill_after(step_id: str):
    """在 step_id 的 checkpoint 提交之后抛出，模拟 Worker 在该边界被杀死。"""

    async def app(ctx: TaskContext):
        commit = ctx.checkpoint

        async def checkpoint(step: str, **kwargs):
            checkpoint_id = await commit(step, **kwargs)
            if step == step_id:
                raise Killed(step)
            return checkpoint_id

        ctx.checkpoint = checkpoint
        return await dr_app.run(ctx)

    return app


def run_loop(tmp_path, *, app=dr_app.run, config=None, resume=None, pause_at=None):
    config = {"topic": "固态电池", "max_fetch": 1, **(config or {})}

    async def go():
        transport = MemoryTransport()
        init = {**LOOP_INIT, "out_dir": str(tmp_path), "config": config}
        if resume is not None:
            init = {**init, "attempt_no": 2, "resume": resume}
        transport.feed(json.dumps(init).encode())
        host = ok_host(pause_at)
        transport.on_send = lambda line: host(json.loads(line), transport)
        counter = itertools.count(1)
        code = await asyncio.wait_for(
            run_worker(
                app,
                transport,
                name=dr_app.NAME,
                version=dr_app.VERSION,
                timing=LOOP_TIMING,
                new_id=lambda: f"cp-{next(counter)}",
            ),
            timeout=20,
        )
        return code, [json.loads(line) for line in transport.sent]

    return asyncio.run(go())


@pytest.fixture
def loop_gw(fake_gateway: FakeGateway, monkeypatch) -> FakeGateway:
    monkeypatch.setenv(GATEWAY_SOCKET_ENV, fake_gateway.socket_path)
    fake_gateway.responder = LoopScript()
    return fake_gateway


def checkpoints(events: list[dict]) -> dict[str, dict]:
    return {e["step_id"]: e for e in events if e["type"] == "checkpoint"}


def call_ids(gw: FakeGateway, start: int = 0) -> list[str]:
    return [r.headers["x-agentbox-call-id"] for r in gw.requests[start:] if r.method == "POST"]


def resume_from(cp: dict) -> dict:
    return {
        "checkpoint_id": cp["checkpoint_id"],
        "step_id": cp["step_id"],
        "state": cp["state"],
        "refs": cp["refs"],
    }


def assert_refs_are_returned_blobs(gw: FakeGateway, events: list[dict]) -> None:
    """§5.5 规则 2：每个 checkpoint 的 refs 都是本任务经 Gateway 调用得到的结果 blob。"""
    returned = set(gw.calls.values())
    for cp in checkpoints(events).values():
        assert set(cp["refs"]) <= returned, cp["step_id"]
        assert len(cp["refs"]) == len(set(cp["refs"]))


def test_loop_full_run_produces_report_with_mapped_citations(loop_gw, tmp_path):
    code, events = run_loop(tmp_path)
    assert code == 0, events[-1]
    assert [e["type"] for e in events] == [
        "ready", "checkpoint", "checkpoint", "checkpoint", "artifact", "checkpoint", "result",
    ]  # fmt: skip
    result = events[-1]
    assert result["outputs"] == ["report"]
    assert result["summary"] == "本研究综述了固态电池 [1]，量产仍有挑战 [2]。"
    assert call_ids(loop_gw) == [
        "root/plan/chat/1",
        "root/task-1/search/1", "root/task-1/fetch/1", "root/task-1/chat/1",
        "root/task-2/search/1", "root/task-2/fetch/1", "root/task-2/chat/1",
        "root/report/chat/1",
    ]  # fmt: skip
    cps = checkpoints(events)
    assert list(cps) == ["plan", "task-1", "task-2", "report"]
    blob = loop_gw.calls
    # refs 累积：计划 → 各任务证据与摘要 → 报告，首次出现顺序
    plan_refs = [blob["root/plan/chat/1"]]
    task1_refs = plan_refs + [blob["root/task-1/fetch/1"], blob["root/task-1/chat/1"]]
    # 两个任务的摘要回复相同 → 摘要 blob 相同，按首次出现去重
    task2_refs = list(
        dict.fromkeys(task1_refs + [blob["root/task-2/fetch/1"], blob["root/task-2/chat/1"]])
    )
    assert len(task2_refs) == 4
    assert cps["plan"]["refs"] == plan_refs
    assert cps["task-1"]["refs"] == task1_refs
    assert cps["task-2"]["refs"] == task2_refs
    assert cps["report"]["refs"] == task2_refs + [blob["root/report/chat/1"]]
    assert_refs_are_returned_blobs(loop_gw, events)
    # 状态内联，带 SDK 的 call id 计数器；证据只存 sha 引用
    final = cps["report"]["state"]
    assert final["_agentbox"]["call_ids"]["root/report/chat"] == 1
    state = ResearchState.from_json(migrate({k: v for k, v in final.items() if k != "_agentbox"}))
    assert [t.status for t in state.tasks] == ["done", "done"]
    assert state.report_sha256 == blob["root/report/chat/1"]
    assert list(state.evidence) == [blob["root/task-1/fetch/1"], blob["root/task-2/fetch/1"]]
    # 报告：每个 [n] 都映射到 state.evidence 中的 sha
    artifact = next(e for e in events if e["type"] == "artifact")
    report = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert artifact["path"] == "report.md" and artifact["media_type"] == "text/markdown"
    assert artifact["declared_sha256"] == hashlib.sha256(report.encode()).hexdigest()
    listing = dict(EVIDENCE_LINE.findall(report))
    body_part = report.split("\n## 证据\n", 1)[0]
    cited = set(re.findall(r"\[(\d+)\]", body_part))
    assert cited == {"1", "2"} and cited <= set(listing)
    assert set(listing.values()) == set(state.evidence)


def test_loop_resume_after_kill_skips_plan_and_done_tasks(loop_gw, tmp_path):
    code, events = run_loop(tmp_path, app=kill_after("task-1"))
    assert code == 1 and events[-1]["type"] == "error"  # 被"杀死"
    cp1 = checkpoints(events)["task-1"]
    assert cp1["state"]["_agentbox"]["call_ids"] == {
        "root/plan/chat": 1, "root/task-1/search": 1, "root/task-1/fetch": 1,
        "root/task-1/chat": 1,
    }  # fmt: skip
    before = len(loop_gw.requests)
    code, events = run_loop(tmp_path, resume=resume_from(cp1))
    assert code == 0 and events[-1]["type"] == "result"
    # 不重新计划、任务 1 不重做；任务 2 与报告照常
    assert call_ids(loop_gw, before) == [
        "root/task-2/search/1", "root/task-2/fetch/1", "root/task-2/chat/1",
        "root/report/chat/1",
    ]  # fmt: skip
    assert sum(1 for c in call_ids(loop_gw) if c.startswith("root/plan/")) == 1
    assert sum(1 for c in call_ids(loop_gw) if c.startswith("root/task-1/")) == 3
    cps = checkpoints(events)
    assert list(cps) == ["task-2", "report"]
    # 计数器从恢复的快照继续：已有的 plan/task-1 序号保留，新步骤从 1 起
    assert cps["report"]["state"]["_agentbox"]["call_ids"] == {
        **cp1["state"]["_agentbox"]["call_ids"],
        "root/task-2/search": 1, "root/task-2/fetch": 1, "root/task-2/chat": 1,
        "root/report/chat": 1,
    }  # fmt: skip
    assert_refs_are_returned_blobs(loop_gw, events)
    state = cps["report"]["state"]
    assert [t["status"] for t in state["tasks"]] == ["done", "done"]
    assert len(state["evidence"]) == 2


def test_loop_resume_retry_continues_call_ids(loop_gw, tmp_path):
    """恢复后的任务失败重做时，新 ID 在计数器之上递增，不回退、不复用失败的 ID。"""
    code, events = run_loop(tmp_path, app=kill_after("task-1"))
    cp1 = checkpoints(events)["task-1"]
    failed = set()

    def flaky(req: Recorded) -> Reply | None:
        call_id = req.headers.get("x-agentbox-call-id", "")
        if call_id == "root/task-2/fetch/1" and call_id not in failed:
            failed.add(call_id)
            return error_reply(504, "call_deadline_exceeded")
        return None

    loop_gw.interceptor = flaky
    before = len(loop_gw.requests)
    code, events = run_loop(tmp_path, resume=resume_from(cp1))
    assert code == 0
    assert call_ids(loop_gw, before) == [
        "root/task-2/search/1", "root/task-2/fetch/1",
        "root/task-2/search/2", "root/task-2/fetch/2", "root/task-2/chat/1",
        "root/report/chat/1",
    ]  # fmt: skip


def test_loop_resume_at_report_rebuilds_without_model_calls(loop_gw, tmp_path):
    code, events = run_loop(tmp_path)
    original = (tmp_path / "report.md").read_text(encoding="utf-8")
    report_cp = checkpoints(events)["report"]
    (tmp_path / "report.md").unlink()
    before = len(loop_gw.requests)
    code, events = run_loop(tmp_path, resume=resume_from(report_cp))
    assert code == 0
    assert [e["type"] for e in events] == ["ready", "artifact", "result"]
    assert call_ids(loop_gw, before) == []  # 只读取了报告 blob
    report_blob = report_cp["state"]["report_sha256"]
    assert [r.path for r in loop_gw.requests[before:]] == [f"/blobs/{report_blob}"]
    assert (tmp_path / "report.md").read_text(encoding="utf-8") == original
    assert events[-1]["outputs"] == ["report"]


def test_loop_pauses_at_checkpoint_boundary(loop_gw, tmp_path):
    code, events = run_loop(tmp_path, pause_at="plan")
    assert code == 0
    assert [e["type"] for e in events] == ["ready", "checkpoint", "paused"]
    assert events[-1]["checkpoint_id"] == events[1]["checkpoint_id"]
    assert call_ids(loop_gw) == ["root/plan/chat/1"]
    # 从暂停点恢复，继续任务 1
    code, events = run_loop(tmp_path, resume=resume_from(events[1]), pause_at="task-1")
    assert [e["type"] for e in events] == ["ready", "checkpoint", "paused"]
    assert events[1]["step_id"] == "task-1"
    assert call_ids(loop_gw)[1:] == [
        "root/task-1/search/1", "root/task-1/fetch/1", "root/task-1/chat/1",
    ]  # fmt: skip


def test_loop_divergence_supersedes_with_new_id(loop_gw, tmp_path):
    def diverge(req: Recorded) -> Reply | None:
        if req.headers.get("x-agentbox-call-id") == "root/task-1/search/1":
            return error_reply(409, "fingerprint_mismatch")
        return None

    loop_gw.interceptor = diverge
    code, events = run_loop(tmp_path)
    assert code == 0 and events[-1]["type"] == "result"
    assert call_ids(loop_gw)[1:5] == [
        "root/task-1/search/1", "root/task-1/search/2", "root/task-1/fetch/1",
        "root/task-1/chat/1",
    ]  # fmt: skip
    superseding = loop_gw.requests[2]
    assert superseding.headers["x-agentbox-supersedes"] == "root/task-1/search/1"
    assert superseding.headers["x-agentbox-supersede-reason"] == "divergence"
    assert superseding.json() == loop_gw.requests[1].json()
    state = checkpoints(events)["task-1"]["state"]
    assert state["tasks"][0]["status"] == "done"
    assert any("root/task-1/search/1" in f and "取代" in f for f in state["failures"])
    assert_refs_are_returned_blobs(loop_gw, events)


def test_loop_second_divergence_fails_only_that_task(loop_gw, tmp_path):
    def diverge(req: Recorded) -> Reply | None:
        if req.headers.get("x-agentbox-call-id", "").startswith("root/task-1/search/"):
            return error_reply(409, "fingerprint_mismatch")
        return None

    loop_gw.interceptor = diverge
    code, events = run_loop(tmp_path)
    assert code == 0 and events[-1]["outputs"] == ["report"]
    cps = checkpoints(events)
    assert cps["task-1"]["refs"] == cps["plan"]["refs"]  # 失败任务不增加 refs
    assert [t["status"] for t in cps["report"]["state"]["tasks"]] == ["failed", "done"]
    assert [c for c in call_ids(loop_gw) if c.startswith("root/task-1/")] == [
        "root/task-1/search/1",
        "root/task-1/search/2",
    ]


def test_loop_budget_exhausted_writes_degraded_report(loop_gw, tmp_path):
    def broke(req: Recorded) -> Reply | None:
        if req.headers.get("x-agentbox-call-id", "").startswith("root/task-2/"):
            return error_reply(402, "budget_exhausted")
        return None

    loop_gw.interceptor = broke
    code, events = run_loop(tmp_path)
    assert code == 0
    result = events[-1]
    assert result["type"] == "result" and result["outputs"] == ["report"]
    assert result["summary"].startswith(dr_app.BUDGET_NOTE)
    assert call_ids(loop_gw)[-1] == "root/task-2/search/1"  # 之后不再发起调用（没有报告调用）
    cps = checkpoints(events)
    assert list(cps) == ["plan", "task-1", "report"]
    assert cps["report"]["refs"] == cps["task-1"]["refs"]  # 降级报告没有新 blob
    state = cps["report"]["state"]
    assert [t["status"] for t in state["tasks"]] == ["done", "skipped"]
    assert state["report_sha256"] is None
    report = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "关键发现 [1]" in report and "未完成（skipped）" in report
    assert dict(EVIDENCE_LINE.findall(report)) == {"1": loop_gw.calls["root/task-1/fetch/1"]}


def test_loop_budget_exhausted_before_any_task_fails(loop_gw, tmp_path):
    def broke(req: Recorded) -> Reply | None:
        return error_reply(402, "budget_exhausted") if req.method == "POST" else None

    loop_gw.interceptor = broke
    code, events = run_loop(tmp_path)
    assert code == 1
    error = events[-1]
    assert error["type"] == "error" and error["code"] == "budget_exhausted"
    assert error["retryable"] is False


def test_loop_fetch_failure_retries_task_once(loop_gw, tmp_path):
    failed = set()

    def flaky(req: Recorded) -> Reply | None:
        call_id = req.headers.get("x-agentbox-call-id", "")
        if call_id == "root/task-1/fetch/1" and call_id not in failed:
            failed.add(call_id)
            return error_reply(502, "upstream_error")
        return None

    loop_gw.interceptor = flaky
    code, events = run_loop(tmp_path)
    assert code == 0 and events[-1]["type"] == "result"
    assert call_ids(loop_gw)[1:6] == [
        "root/task-1/search/1", "root/task-1/fetch/1",
        "root/task-1/search/2", "root/task-1/fetch/2", "root/task-1/chat/1",
    ]  # fmt: skip
    cps = checkpoints(events)
    assert cps["task-1"]["refs"][1] == loop_gw.calls["root/task-1/fetch/2"]
    assert any("task-1" in f and "重做" in f for f in cps["task-1"]["state"]["failures"])


def test_loop_fetch_failing_twice_fails_task_and_continues(loop_gw, tmp_path):
    def timeout(req: Recorded) -> Reply | None:
        if req.headers.get("x-agentbox-call-id", "").startswith("root/task-1/fetch/"):
            return error_reply(504, "call_deadline_exceeded")
        return None

    loop_gw.interceptor = timeout
    code, events = run_loop(tmp_path)
    assert code == 0 and events[-1]["outputs"] == ["report"]
    state = checkpoints(events)["report"]["state"]
    assert [t["status"] for t in state["tasks"]] == ["failed", "done"]
    assert [c for c in call_ids(loop_gw) if c.startswith("root/task-1/fetch/")] == [
        "root/task-1/fetch/1",
        "root/task-1/fetch/2",
    ]


@pytest.mark.parametrize(
    "config",
    [{"topic": ""}, {"topic": "x", "max_tasks": 0}, {"topic": "x", "max_fetch": True}, "x"],
)
def test_parse_config_rejects_bad_values(config):
    with pytest.raises(dr_app.WorkerFailure) as info:
        dr_app.parse_config(config)
    assert info.value.code == "invalid_config"
    assert dr_app.parse_config({"topic": " t "}) == dr_app.Config(topic="t")


def test_first_paragraph_skips_headings():
    md = "# 主题\n\n## 核心洞见\n第一段 [1]\n续行\n\n第二段"
    assert dr_app.first_paragraph(md) == "第一段 [1]\n续行"


def test_loop_refs_are_cumulative_and_reproduced_after_resume(loop_gw, tmp_path):
    code, events = run_loop(tmp_path)
    assert code == 0
    full = checkpoints(events)
    order = list(full)
    for earlier, later in zip(order, order[1:], strict=False):  # 每步只增不减，前者是后者的前缀
        assert full[later]["refs"][: len(full[earlier]["refs"])] == full[earlier]["refs"]
    report_refs = set(full["report"]["refs"])
    assert set(full["report"]["state"]["evidence"]) <= report_refs
    assert full["report"]["state"]["report_sha256"] in report_refs
    # 在 task-1 后杀死并恢复：下一个 checkpoint（task-2）得到与未中断运行相同的累积集合
    code, events = run_loop(tmp_path, app=kill_after("task-1"))
    cp1 = checkpoints(events)["task-1"]
    assert cp1["refs"] == full["task-1"]["refs"]
    code, events = run_loop(tmp_path, resume=resume_from(cp1))
    assert code == 0
    resumed = checkpoints(events)
    assert resumed["task-2"]["refs"] == full["task-2"]["refs"]
    assert resumed["report"]["refs"] == full["report"]["refs"]
    # 即使 resume.refs 为空（如旧 checkpoint），状态中的证据 sha 仍进入累积 refs
    code, events = run_loop(tmp_path, resume={**resume_from(cp1), "refs": []})
    assert set(cp1["state"]["evidence"]) <= set(checkpoints(events)["task-2"]["refs"])
