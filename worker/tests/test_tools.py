"""agentbox_worker.tools：注册表与参数校验、工具额度视图、web_search / web_fetch / read_source、
Gateway 客户端的函数调用与额度头。

工具测试使用进程内的 ScriptedGateway（Windows 可运行）；只有 Gateway 客户端头部解析一项使用
conftest 的 AF_UNIX fake（没有 AF_UNIX 的平台跳过）。
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from agentfakes import ScriptedGateway
from conftest import FakeGateway, Reply, error_reply

from agentbox_worker.errors import (
    AccessRevoked,
    BudgetExhausted,
    GatewayError,
    ToolBudgetExhausted,
)
from agentbox_worker.gateway import CallIds, GatewayClient
from agentbox_worker.tools import ToolRegistry, text, validate_args
from agentbox_worker.tools.base import ToolArgsError, ToolContext, TurnFlags
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.sources import ReadSource, Source, SourceStore
from agentbox_worker.tools.web_fetch import WebFetch
from agentbox_worker.tools.web_search import WebSearch


def hits(n: int) -> dict[str, Any]:
    sites = "abcdefghij"
    return {
        "results": [
            {
                "url": f"https://www.{sites[i]}.example/p{i}",
                "title": f"标题 {i + 1}",
                "snippet": f"摘要 {i + 1}",
            }
            for i in range(n)
        ]
    }


def page(title: str, body: str) -> dict[str, Any]:
    html = f"<html><head><title>{title}</title></head><body><p>{body}</p></body></html>"
    return {"url": "", "status": 200, "content_type": "text/html", "content": html}


def tool_ctx(gw: ScriptedGateway | None = None, **kw: Any) -> ToolContext:
    return ToolContext(
        gateway=gw or ScriptedGateway(),
        step_id="orch",
        budget=kw.pop("budget", TurnBudget()),
        sources=kw.pop("sources", SourceStore()),
        flags=TurnFlags(),
        **kw,
    )


# ---- 注册表与参数校验 ----


def test_registry_schemas_and_dispatch_validation():
    reg = ToolRegistry([WebSearch(), WebFetch(), ReadSource()])
    assert [s["function"]["name"] for s in reg.schemas()] == [
        "web_search",
        "web_fetch",
        "read_source",
    ]
    assert reg.schemas()[0]["type"] == "function"
    assert [s["function"]["name"] for s in reg.schemas(["read_source", "web_search"])] == [
        "web_search",
        "read_source",
    ]
    ctx = tool_ctx()
    bad = reg.dispatch("web_search", '{"query": ""}', ctx)
    assert not bad.ok and "参数" in bad.content and ctx.budget.used == 0
    assert not reg.dispatch("nope", "{}", ctx).ok
    assert not reg.dispatch("web_search", "not json", ctx).ok
    assert not reg.dispatch("web_fetch", '{"url": "file:///etc/passwd"}', ctx).ok
    assert ctx.gateway.calls == [] and ctx.budget.used == 0
    with pytest.raises(ValueError):
        ToolRegistry([WebSearch(), WebSearch()])


def test_validate_args_subset():
    schema = {
        "type": "object",
        "properties": {
            "q": {"type": "string", "minLength": 1, "maxLength": 3},
            "n": {"type": "integer", "minimum": 1, "maximum": 10},
            "mode": {"type": "string", "enum": ["a", "b"]},
            "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 2},
            "flag": {"type": "boolean"},
        },
        "required": ["q"],
        "additionalProperties": False,
    }
    validate_args(schema, {"q": "ab", "n": 3, "mode": "a", "tags": ["x"], "flag": True})
    for bad in (
        [],
        {},
        {"q": "abcd"},
        {"q": "a", "n": True},
        {"q": "a", "n": 0},
        {"q": "a", "n": 1.5},
        {"q": "a", "mode": "c"},
        {"q": "a", "tags": []},
        {"q": "a", "tags": [1]},
        {"q": "a", "tags": ["1", "2", "3"]},
        {"q": "a", "flag": "yes"},
        {"q": "a", "extra": 1},
    ):
        with pytest.raises(ToolArgsError):
            validate_args(schema, bad)


# ---- 额度视图 ----


def test_turn_budget_header_local_count_shares_and_json():
    b = TurnBudget(limit=30, shares={"1": 4})
    b.record(None, "1")
    assert (b.used, b.spent) == (1, {"1": 1})
    b.record((7, 30), "1")
    assert b.used == 7 and b.remaining() == 23 and b.share_left("1") == 2
    b.record((5, 30), None)  # 单调不减
    assert b.used == 7
    b.record_failure("1")
    assert b.used == 8 and b.share_left("1") == 1 and b.share_left("2") is None
    assert b.line("1") == "工具额度：已用 8/30，剩余 22（本子主题剩余 1）"
    assert b.line() == "工具额度：已用 8/30，剩余 22"
    assert TurnBudget.from_json(json.loads(json.dumps(b.to_json()))) == b
    b.exhaust()
    assert b.remaining() == 0 and b.used == 30
    with pytest.raises(ValueError):
        TurnBudget.from_json({"limit": "30", "used": 0})


# ---- web_search ----


def test_search_counts_budget_from_gateway_header_and_shows_remaining():
    gw = ScriptedGateway(search=lambda q, n: hits(3), tool_budget=(7, 30))
    ctx = tool_ctx(gw)
    r = ToolRegistry([WebSearch()]).dispatch(
        "web_search", '{"query":"固态电池","max_results":3}', ctx
    )
    assert r.ok and ctx.budget.used == 7 and "剩余 23" in r.content
    assert r.content.splitlines()[-1] == "工具额度：已用 7/30，剩余 23"
    assert "1. 标题 1 — a.example\n摘要 1\nhttps://www.a.example/p0" in r.content
    assert r.preview["kind"] == "search" and r.preview["query"] == "固态电池"
    assert [x["site"] for x in r.preview["results"]] == ["a.example", "b.example", "c.example"]
    assert r.raw == {"call_id": "root/orch/search/1"}
    assert r.blobs and r.blobs[0] in gw.blobs
    assert ctx.flags.researching and r.control is None
    assert gw.calls[0].body == {"query": "固态电池", "max_results": 3}


def test_search_without_header_counts_locally_and_defaults_max_results():
    gw = ScriptedGateway(search=lambda q, n: hits(n))
    ctx = tool_ctx(gw, subtopic_id="2", budget=TurnBudget(shares={"2": 3}))
    r = ToolRegistry([WebSearch()]).dispatch("web_search", '{"query":"x"}', ctx)
    assert gw.calls[0].body["max_results"] == 5 and len(r.preview["results"]) == 5
    assert ctx.budget.used == 1 and ctx.budget.spent == {"2": 1}
    assert r.content.endswith("工具额度：已用 1/30，剩余 29（本子主题剩余 2）")


# ---- web_fetch ----


def test_fetch_creates_numbered_source_from_blob_and_dedupes():
    long_body = "固态电池" * 2000  # 远超 4 KiB
    gw = ScriptedGateway(fetch=lambda url: page("硫化物 路线", long_body))
    ctx = tool_ctx(gw, subtopic_id="1")
    reg = ToolRegistry([WebFetch()])
    r1 = reg.dispatch("web_fetch", '{"url":"https://www.a.example/1"}', ctx)
    assert r1.ok and r1.content.startswith("来源 [1]：硫化物 路线\nhttps://www.a.example/1\n")
    src = ctx.sources.get(1)
    assert src is not None and src.title == "硫化物 路线" and src.url == "https://www.a.example/1"
    assert len(src.excerpt.encode("utf-8")) <= 4096 and src.excerpt.startswith("固态电池")
    assert src.call_id == "root/orch/fetch/1" and src.sha256 in gw.blobs
    assert r1.blobs == (src.sha256,)
    assert r1.preview == {
        "kind": "fetch",
        "n": 1,
        "title": "硫化物 路线",
        "url": "https://www.a.example/1",
        "site": "a.example",
        "excerpt": src.excerpt[:600],
    }
    body_part = r1.content.split("\n", 2)[2].rsplit("\n", 1)[0]
    assert len(body_part.encode("utf-8")) <= 6 * 1024 + 4
    r2 = reg.dispatch("web_fetch", '{"url":"https://www.a.example/1"}', ctx)
    assert r2.ok and r2.content.startswith("来源 [1]：") and len(ctx.sources.all()) == 1
    assert ctx.budget.used == 2 and ctx.budget.spent == {"1": 2}


def test_fetch_failure_is_recorded_and_counted_and_loop_continues():
    def fail(url: str) -> Any:
        raise GatewayError(502, "upstream_error", "站点不可达")

    gw = ScriptedGateway(fetch=fail)
    ctx = tool_ctx(gw)
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", '{"url":"https://a.example/x"}', ctx)
    assert not r.ok and r.content.startswith("抓取失败：") and r.control is None
    assert ctx.budget.used == 1 and ctx.sources.all() == []
    assert r.content.endswith("工具额度：已用 1/30，剩余 29")


def test_failed_call_uses_gateway_count_from_error_response():
    # 真实验收：并行子主题的在途抓取已计入先前响应的头（8/30），之后它们失败时本地再各加 1 会多计。
    def fail(url: str) -> Any:
        raise GatewayError(429, "tries_exhausted", "Too Many Requests", tool_budget=(10, 30))

    gw = ScriptedGateway(fetch=fail)
    ctx = tool_ctx(gw)
    ctx.budget.used = 8
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", '{"url":"https://a.example/x"}', ctx)
    assert not r.ok and ctx.budget.used == 10
    assert r.content.endswith("工具额度：已用 10/30，剩余 20")


def test_fetch_of_http_error_page_is_not_a_source():
    gw = ScriptedGateway(
        fetch=lambda url: {"status": 404, "error": "http_status", "content": "nope"},
        tool_budget=(4, 30),
    )
    ctx = tool_ctx(gw)
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", '{"url":"https://a.example/x"}', ctx)
    assert not r.ok and "404" in r.content and ctx.sources.all() == []
    assert ctx.budget.used == 4 and r.raw == {"call_id": "root/orch/fetch/1"}


def test_tool_budget_exhausted_maps_to_control_and_no_more_calls():
    def exhausted(url: str) -> Any:
        raise ToolBudgetExhausted(429, "tool_budget_exhausted")

    gw = ScriptedGateway(fetch=exhausted, search=lambda q, n: hits(1))
    ctx = tool_ctx(gw)
    reg = ToolRegistry([WebSearch(), WebFetch()])
    r = reg.dispatch("web_fetch", '{"url":"https://a.example/x"}', ctx)
    assert not r.ok and r.control == "budget_exhausted"
    assert ctx.budget.used == ctx.budget.limit == 30
    r2 = reg.dispatch("web_search", '{"query":"x"}', ctx)
    assert r2.control == "budget_exhausted" and not r2.ok
    assert gw.counts() == {"fetch": 1}
    assert "已用 30/30" in r2.content


def test_fatal_gateway_errors_propagate():
    for exc in (
        BudgetExhausted(402, "budget_exhausted"),
        AccessRevoked(403, "access_revoked"),
        GatewayError(0, "connection_lost"),
    ):

        def boom(q: str, n: int, exc: Exception = exc) -> Any:
            raise exc

        ctx = tool_ctx(ScriptedGateway(search=boom))
        with pytest.raises(type(exc)):
            ToolRegistry([WebSearch()]).dispatch("web_search", '{"query":"x"}', ctx)


# ---- read_source 与来源表 ----


def test_read_source_never_touches_network_and_falls_back_to_excerpt():
    gw = ScriptedGateway()
    sha = gw.put_blob(json.dumps(page("T", "全文 " * 10)).encode())
    store = SourceStore()
    store.add(sha256=sha, url="https://a.example/1", title="T", excerpt="摘录", call_id="c1")
    ctx = tool_ctx(gw, sources=store)
    reg = ToolRegistry([ReadSource()])
    full = reg.dispatch("read_source", '{"n":1}', ctx)
    assert full.ok and "全文" in full.content and full.raw["inline"]["request"] == {"n": 1}
    gw.read_blob_error = AccessRevoked(403, "access_revoked")
    r = reg.dispatch("read_source", '{"n":1}', ctx)
    assert r.ok and "摘录" in r.content and "全文" not in r.content
    assert gw.calls == [] and ctx.budget.used == 0 and "工具额度" not in r.content
    missing = reg.dispatch("read_source", '{"n":9}', ctx)
    assert not missing.ok


def test_source_store_dedupe_adopt_and_json():
    store = SourceStore()
    s1, new1 = store.add(sha256="a" * 64, url="https://x/1", title="1", excerpt="e", call_id="c")
    s2, new2 = store.add(sha256="a" * 64, url="https://x/other", title="", excerpt="", call_id="d")
    assert new1 and not new2 and s2 is s1 and s1.n == 1
    _, dup_url = store.add(sha256="b" * 64, url="https://x/1", title="", excerpt="", call_id="e")
    assert not dup_url
    store.adopt(
        [
            Source(1, "c" * 64, "https://y/1", "y", "e", "c9", origin="t-1"),
            Source(2, "a" * 64, "https://x/1", "dup", "e", "c9", origin="t-1"),
        ]
    )
    assert [(s.n, s.url, s.origin) for s in store.all()] == [
        (1, "https://x/1", ""),
        (2, "https://y/1", "t-1"),
    ]
    again = SourceStore.from_json(json.loads(json.dumps(store.to_json())))
    assert again.all() == store.all()
    s3, _ = again.add(sha256="d" * 64, url="https://z/1", title="z", excerpt="", call_id="f")
    assert s3.n == 3


# ---- Gateway 客户端 ----


def test_chat_body_with_tools_and_unchanged_without():
    msgs = [{"role": "user", "content": "x"}]
    assert GatewayClient.chat_body(msgs) == {"messages": msgs}
    body = GatewayClient.chat_body([], tools=[{"type": "function"}], tool_choice="auto")
    assert body["tools"] == [{"type": "function"}] and body["tool_choice"] == "auto"


def test_gateway_client_parses_tool_budget_header_and_429(fake_gateway: FakeGateway):
    gw = GatewayClient(fake_gateway.socket_path, call_ids=CallIds(), timeout_s=10)
    fake_gateway.replies.append(Reply(200, {"results": []}, {"X-Agentbox-Tool-Budget": "3/30"}))
    assert gw.search("orch", "x").tool_budget == (3, 30)
    fake_gateway.replies.append(Reply(200, {"results": []}, {"X-Agentbox-Tool-Budget": "x/y"}))
    assert gw.search("orch", "x").tool_budget is None
    fake_gateway.replies.append(Reply(200, {"results": []}))
    assert gw.search("orch", "x").tool_budget is None
    fake_gateway.replies.append(error_reply(429, "tool_budget_exhausted"))
    with pytest.raises(ToolBudgetExhausted):
        gw.fetch("orch", "https://a.example/")
    fake_gateway.replies.append(error_reply(429, "rate_limited"))
    with pytest.raises(GatewayError) as info:
        gw.fetch("orch", "https://a.example/")
    assert not isinstance(info.value, ToolBudgetExhausted) and info.value.tool_budget is None
    fake_gateway.replies.append(
        Reply(429, {"error": {"code": "tries_exhausted"}}, {"X-Agentbox-Tool-Budget": "10/30"})
    )
    with pytest.raises(GatewayError) as info:
        gw.fetch("orch", "https://a.example/")
    assert info.value.code == "tries_exhausted" and info.value.tool_budget == (10, 30)
    gw.chat("orch", [{"role": "user", "content": "x"}], tools=[{"type": "function"}])
    assert fake_gateway.requests[-1].json()["tools"] == [{"type": "function"}]


# ---- text 从 deepresearch.steps 移来 ----


def test_text_helpers_moved_without_behavior_change():
    from deepresearch import steps

    assert steps.strip_thinking is text.strip_thinking
    assert steps.truncate_utf8 is text.truncate_utf8
    assert text.renumber_citations("a [1, 3] b [2]", {1: 1, 2: 2}) == "a [1] b [2]"
    assert text.site_of("https://www.Example.com:8443/a?b") == "example.com"
    assert text.site_of("not a url") == ""
    assert text.page_text({"content": "<p>a<script>x</script> b</p>"}) == "a b"


# ---- run_python：经 Gateway /v1/exec 运行代码（M4 Plan 15 Task 11） ----

import hashlib

from agentbox_worker.errors import CallDeadlineExceeded, CallDivergence
from agentbox_worker.gateway import GatewayResult
from agentbox_worker.tools import RunPython

SHA_A = "a" * 64
SHA_OUT = hashlib.sha256(b"out").hexdigest()


class ExecGateway(ScriptedGateway):
    """ScriptedGateway 加 exec：记录 (step_id, code, inputs, wall_ms)；返回脚本结果或抛出脚本异常。

    （agentfakes.ScriptedGateway 不含 exec；本任务不改共享替身。）
    """

    def __init__(self, result: dict[str, Any] | BaseException) -> None:
        super().__init__()
        self.result = result
        self.execs: list[tuple[str, str, list[tuple[str, str]], int | None]] = []

    def exec(self, step_id, code, *, inputs=(), wall_ms=None, memory_bytes=None, retry=False):
        assert memory_bytes is None and retry is False
        self.execs.append((step_id, code, [tuple(i) for i in inputs], wall_ms))
        if isinstance(self.result, BaseException):
            raise self.result
        n = len(self.execs)
        data = json.dumps(self.result).encode()
        sha = hashlib.sha256(data).hexdigest()
        return GatewayResult(f"root/{step_id}/exec/{n}", self.result, sha, False, 200)


def exec_result(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "status": "completed",
        "exit": {"code": 0, "signal": 0},
        "diag": {"oom_kill_delta": 0, "oom_observed": False, "cpu_usage_usec": 1200},
        "stdout": "42\n",
        "stdout_truncated": False,
        "stdout_invalid_utf8": False,
        "stderr": "",
        "stderr_truncated": False,
        "stderr_invalid_utf8": False,
        "outputs": [],
        "skipped_outputs": [],
        "queue_ms": 3,
        "wall_ms": 120,
        "limits": {"wall_ms": 60000, "memory_bytes": 536870912},
        "image_digest": "d" * 64,
    }
    return {**base, **kw}


RUN_PYTHON_PARAMETERS = {
    "type": "object",
    "properties": {
        "code": {"type": "string", "maxLength": 262144},
        "inputs": {
            "type": "array",
            "maxItems": 64,
            "items": {
                "type": "object",
                "properties": {
                    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    "path": {"type": "string"},
                },
                "required": ["sha256", "path"],
                "additionalProperties": False,
            },
        },
        "timeout_s": {"type": "integer", "minimum": 1, "maximum": 300},
    },
    "required": ["code"],
    "additionalProperties": False,
}


def test_run_python_schema_from_registry_and_not_counted():
    (schema,) = ToolRegistry([RunPython()]).schemas()
    assert schema == {
        "type": "function",
        "function": {
            "name": "run_python",
            "description": RunPython.description,
            "parameters": RUN_PYTHON_PARAMETERS,
        },
    }
    assert "/in" in RunPython.description and "/out" in RunPython.description
    assert RunPython.counts_budget is False


def test_run_python_passes_code_inputs_and_timeout_without_using_tool_budget():
    gw = ExecGateway(exec_result())
    ctx = tool_ctx(gw)
    reg = ToolRegistry([RunPython()])
    args = {
        "code": "print(6*7)",
        "inputs": [{"sha256": SHA_A, "path": "data/a.csv"}],
        "timeout_s": 5,
    }
    r = reg.dispatch("run_python", json.dumps(args), ctx)
    assert r.ok and r.control is None
    r2 = reg.dispatch("run_python", '{"code": "print(1)"}', ctx)
    assert r2.ok
    assert gw.execs == [
        ("orch", "print(6*7)", [(SHA_A, "data/a.csv")], 5000),
        ("orch", "print(1)", [], None),
    ]
    assert ctx.budget.used == 0 and ctx.flags.researching is False
    # schema 违规（sha 非小写十六进制、超时越界、未知字段）→ 参数错误，不发起调用
    for bad in (
        {"code": "x", "inputs": [{"sha256": "A" * 64, "path": "a"}]},
        {"code": "x", "inputs": [{"sha256": SHA_A + "\n", "path": "a"}]},
        {"code": "x", "timeout_s": 301},
        {"code": "x", "argv": ["sh"]},
    ):
        out = reg.dispatch("run_python", json.dumps(bad), ctx)
        assert not out.ok and out.content.startswith("工具参数错误")
    assert len(gw.execs) == 2


def test_run_python_result_text_truncates_streams_and_lists_outputs():
    head = "H" * 4096
    middle = "m" * 3000
    tail = "T" * 1024
    body = exec_result(
        exit={"code": 1, "signal": 0},
        stdout=head + middle + tail,
        stdout_truncated=True,
        stderr="e" * 1000 + "E" * 2048,
        stderr_invalid_utf8=True,
        diag={"oom_kill_delta": 1, "oom_observed": True, "cpu_usage_usec": 5},
        outputs=[{"path": "plot.png", "sha256": SHA_OUT, "size": 2048}],
        skipped_outputs=[{"path": "link", "reason": "symlink"}],
    )
    gw = ExecGateway(body)
    r = ToolRegistry([RunPython()]).dispatch("run_python", '{"code": "x"}', tool_ctx(gw))
    c = r.content
    assert "completed" in c and "退出码 1" in c
    assert head in c and tail in c and "m" * 10 not in c
    assert "省略 3000 字节" in c
    assert "E" * 2048 in c and "e" * 10 not in c
    assert "省略 1000 字节" in c
    assert "plot.png" in c and SHA_OUT in c and "2048" in c
    assert "link" in c and "symlink" in c
    assert "1 MiB" in c  # Gateway 已截断 stdout 的提示
    assert "U+FFFD" in c  # stderr 含非法 UTF-8 的提示
    assert "内存" in c and "536870912" in c  # OOM 提示带内存上限
    assert r.ok is False  # 非零退出：运行本身完成，但对模型与界面是失败
    assert r.data == body
    assert r.raw == {"call_id": "root/orch/exec/1"}
    result_sha = hashlib.sha256(json.dumps(body).encode()).hexdigest()
    assert r.blobs == (result_sha, SHA_OUT)


def test_run_python_clips_on_utf8_byte_boundaries():
    # 多字节字符跨越 4 KiB 边界：按字节截取，不产生半个字符
    stdout = "中" * 3000  # 9000 字节
    r = ToolRegistry([RunPython()]).dispatch(
        "run_python", '{"code": "x"}', tool_ctx(ExecGateway(exec_result(stdout=stdout)))
    )
    assert "�" not in r.content
    kept = r.content.replace("中间省略", "").count("中")
    assert 1364 + 341 - 2 <= kept <= 1365 + 341  # 4096//3 + 1024//3


def test_run_python_short_streams_untouched_and_timed_out_explained():
    ok = ToolRegistry([RunPython()]).dispatch(
        "run_python", '{"code": "print(42)"}', tool_ctx(ExecGateway(exec_result()))
    )
    assert ok.ok and "42\n" in ok.content and "省略" not in ok.content
    assert "退出码 0" in ok.content and "输出文件：无" in ok.content
    body = exec_result(
        status="timed_out",
        exit={"code": -1, "signal": 9},
        limits={"wall_ms": 5000, "memory_bytes": 536870912},
    )
    r = ToolRegistry([RunPython()]).dispatch(
        "run_python", '{"code": "while 1: pass", "timeout_s": 5}', tool_ctx(ExecGateway(body))
    )
    assert not r.ok and "timed_out" in r.content and "5000 ms" in r.content
    assert "信号 9" in r.content
    assert r.preview == {
        "kind": "text",
        "text": "timed_out（运行超过时限 5000 ms 被终止，信号 9）\n42",
    }


def test_run_python_preview_for_step_row_is_bounded():
    # 步骤行预览（面向用户）：退出码、stdout 开头（≤ 2000 字符）、失败时 stderr 末尾、输出文件名
    body = exec_result(
        exit={"code": 2, "signal": 0},
        stdout="行\n" * 1500,
        stderr="x" * 1000 + "Traceback END\n",
        outputs=[{"path": "a.csv", "sha256": SHA_OUT, "size": 3}],
    )
    r = ToolRegistry([RunPython()]).dispatch(
        "run_python", '{"code": "x"}', tool_ctx(ExecGateway(body))
    )
    assert r.preview is not None and r.preview["kind"] == "text"
    text = r.preview["text"]
    assert text.startswith("退出码 2\n行\n行")
    assert "（共 2999 字符，已截断）" in text
    assert text.count("x") == 600 - len("Traceback END") and text.endswith("输出文件：a.csv")
    assert len(text) < 3000


@pytest.mark.parametrize(
    ("exc", "phrase"),
    [
        (BudgetExhausted(402, "exec_quota_exhausted", "count"), "次数"),
        (BudgetExhausted(402, "exec_cpu_exhausted"), "CPU"),
        (BudgetExhausted(402, "exec_wall_exhausted"), "运行时间"),
        (BudgetExhausted(402, "exec_blocked"), "已停止"),
        (GatewayError(403, "input_not_authorized"), "输入文件"),
        (GatewayError(400, "unsupported_field", "inputs[0].path"), "inputs[0].path"),
        (CallDeadlineExceeded(504, "exec_queue_timeout"), "排队"),
        (CallDeadlineExceeded(0, "client_timeout"), "client_timeout"),
        (GatewayError(409, "exec_cancelled"), "取消"),
        (GatewayError(502, "exec_unknown"), "exec_unknown"),
        (GatewayError(503, "exec_env_unavailable"), "exec_env_unavailable"),
    ],
    ids=lambda v: v.code if isinstance(v, GatewayError) else None,
)
def test_run_python_gateway_errors_become_tool_failures(exc, phrase):
    ctx = tool_ctx(ExecGateway(exc))
    r = ToolRegistry([RunPython()]).dispatch("run_python", '{"code": "x"}', ctx)
    assert not r.ok and r.control is None and r.blobs == ()
    assert phrase in r.content and exc.code in r.content
    assert ctx.budget.used == 0


@pytest.mark.parametrize(
    "exc",
    [
        BudgetExhausted(402, "budget_exhausted"),  # 费用预算（非 exec 配额）仍终止本轮
        AccessRevoked(403, "access_revoked"),
        CallDivergence(409, "fingerprint_mismatch"),
        GatewayError(0, "connection_lost"),
    ],
    ids=lambda v: v.code,
)
def test_run_python_fatal_gateway_errors_propagate(exc):
    with pytest.raises(type(exc)):
        ToolRegistry([RunPython()]).dispatch(
            "run_python", '{"code": "x"}', tool_ctx(ExecGateway(exc))
        )


# ---- 抓取质量与错误文案（M4 验收修复） ----

NAV_PAGE = """<html><head><title>储能新闻</title><style>.x{}</style></head><body>
<header><a href="/">首页</a><a href="/news">新闻中心</a></header>
<nav class="top"><ul><li><a href="/a">产品</a></li><li><a href="/b">关于我们</a></li></ul></nav>
<div class="breadcrumb"><a href="/">首页</a> &gt; <a href="/n">新闻</a></div>
<div id="cookie-banner">本站使用 Cookie，继续浏览即表示同意。</div>
<div role="navigation"><a href="/x">快速链接</a></div>
<article>
<h1>储能电池装机量创新高</h1>
<p>今年上半年，国内新型储能新增装机规模达到新的高点，其中锂离子电池仍占主导地位，占比超过九成。</p>
<p>业内人士认为，随着电芯价格下降与电力市场化改革推进，独立储能电站的收益模式正逐步清晰。</p>
<form><input name="q"><button>搜索</button></form>
</article>
<aside class="related"><a href="/r1">相关阅读一</a><a href="/r2">相关阅读二</a></aside>
<div class="ad-slot">广告：限时优惠</div>
<footer>版权所有 © 某网站 京ICP备</footer>
<script>var nav = "菜单";</script>
</body></html>"""


def html_body(html: str) -> dict[str, Any]:
    return {"status": 200, "content_type": "text/html; charset=utf-8", "content": html}


def test_page_text_drops_site_chrome_and_prefers_article():
    out = text.page_text(html_body(NAV_PAGE))
    assert out.startswith("储能电池装机量创新高 今年上半年")
    assert "电力市场化改革" in out
    for chrome in (
        "首页",
        "产品",
        "Cookie",
        "快速链接",
        "相关阅读",
        "广告",
        "版权所有",
        "菜单",
        "搜索",
    ):
        assert chrome not in out, chrome
    assert text.page_text(html_body(NAV_PAGE)) == out  # 确定性


def test_page_text_main_and_role_main_win_over_surroundings():
    html = (
        "<body><div class='menu'><a href='/1'>栏目一</a></div>"
        "<main><p>正文第一段，讲的是钠离子电池的成本结构与量产进度。</p></main>"
        "<div id='sidebar'><p>热门文章列表</p></div></body>"
    )
    assert text.page_text(html_body(html)) == "正文第一段，讲的是钠离子电池的成本结构与量产进度。"
    html2 = (
        "<body><div><a href='/1'>栏目一</a> <a href='/2'>栏目二</a> <a href='/3'>栏目三</a></div>"
        "<div role='main'><p>主体内容：液流电池适合长时储能。</p></div></body>"
    )
    assert text.page_text(html_body(html2)) == "主体内容：液流电池适合长时储能。"


def test_page_text_without_landmarks_takes_densest_text_block():
    para = "固态电池的电解质分为氧化物、硫化物与聚合物三条路线，各有优劣。" * 3
    html = (
        "<body><div class='wrap'>"
        "<div><a href='/1'>新闻</a> <a href='/2'>产品</a> "
        "<a href='/3'>联系我们</a> <a href='/4'>招聘</a></div>"
        f"<div class='c'><h2>路线对比</h2><p>{para}</p><p>{para}</p></div>"
        "<div><p>相关推荐：点此查看</p></div>"
        "</div></body>"
    )
    out = text.page_text(html_body(html))
    assert out.startswith("路线对比 固态电池")
    assert "联系我们" not in out and "相关推荐" not in out


def test_page_text_short_or_plain_pages_unchanged():
    assert text.page_text({"content": "<p>a<script>x</script> b</p>"}) == "a b"
    assert text.page_text({"content": "纯文本  正文\n第二行"}) == "纯文本 正文 第二行"
    # 整页只有被当作导航的结构时，退回全页文本而不是空串
    only_nav = "<body><div class='sidebar-layout'><p>唯一的一段正文。</p></div></body>"
    assert text.page_text(html_body(only_nav)) == "唯一的一段正文。"
    # 大量未闭合的标签：深度有界，不触发递归上限
    assert text.page_text(html_body("<div>" * 5000 + "深处的文字")) == "深处的文字"
    assert text.page_text(html_body("<p>行内<b>加粗</b>不切开</p>")) == "行内加粗不切开"


PDF_BODY = {
    "status": 200,
    "content_type": "application/pdf",
    "encoding": "base64",
    "content": "JVBERi0xLjcKJcfsj6IK",
}
SEARCH_CONTENT = (
    "1. 2025 年储能产业报告 — nea.gov.cn\n全国新型储能装机规模达 7376 万千瓦。\n"
    "https://www.nea.gov.cn/r.pdf\n\n"
    "2. 无摘要 — b.example\n\nhttps://b.example/x.pdf\n"
    "工具额度：已用 1/30，剩余 29"
)


def test_search_snippets_parse_from_web_search_tool_messages():
    from agentbox_worker.tools.web_search import search_snippets

    msgs = [
        {"role": "user", "content": "1. 假的 — x\n摘要\nhttps://x.example/"},
        {"role": "tool", "tool_call_id": "c1", "name": "web_search", "content": SEARCH_CONTENT},
    ]
    assert search_snippets(msgs) == {
        "https://www.nea.gov.cn/r.pdf": (
            "2025 年储能产业报告",
            "全国新型储能装机规模达 7376 万千瓦。",
        ),
        "https://b.example/x.pdf": ("无摘要", ""),
    }


def test_fetched_pdf_without_snippet_is_not_registered_as_source():
    gw = ScriptedGateway(fetch=lambda url: PDF_BODY)
    ctx = tool_ctx(gw)
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", '{"url":"https://b.example/x.pdf"}', ctx)
    assert r.ok and ctx.sources.all() == []
    assert "PDF（未提取正文）" in r.content and "来源 [" not in r.content
    assert r.preview is not None and "n" not in r.preview
    assert r.preview["excerpt"] == "PDF（未提取正文）" and r.blobs  # 结果 blob 仍进入 refs
    assert ctx.budget.used == 1


def test_fetched_pdf_with_search_snippet_uses_snippet_as_excerpt():
    gw = ScriptedGateway(fetch=lambda url: PDF_BODY)
    url = "https://www.nea.gov.cn/r.pdf"
    ctx = tool_ctx(
        gw, snippets={url: ("2025 年储能产业报告", "全国新型储能装机规模达 7376 万千瓦。")}
    )
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", json.dumps({"url": url}), ctx)
    src = ctx.sources.get(1)
    assert r.ok and src is not None
    assert src.title == "2025 年储能产业报告 · PDF（未提取正文）"
    assert src.excerpt == "全国新型储能装机规模达 7376 万千瓦。"
    assert "仅有搜索摘要" in r.content and r.preview is not None and r.preview["n"] == 1


@pytest.mark.parametrize(
    ("status", "code", "message", "phrase"),
    [
        (429, "tries_exhausted", "Too Many Requests", "网页无法访问或超时"),
        (502, "upstream_unreachable", "Bad Gateway", "网页无法访问或超时"),
        (502, "upstream_unconfirmed", "Bad Gateway", "网页无法访问或超时"),
        (504, "call_deadline_exceeded", "Gateway Timeout", "网页无法访问或超时"),
        (0, "client_timeout", "", "网页无法访问或超时"),
        (429, "upstream_rate_limited", "Too Many Requests", "请求过多"),
        (503, "upstream_unavailable", "Service Unavailable", "网站暂时不可用"),
        (403, "egress_blocked", "Forbidden", "不允许访问该网址"),
        (400, "invalid_url", "Bad Request", "网址无效"),
        (502, "too_many_redirects", "Bad Gateway", "重定向次数过多"),
        (502, "response_too_large", "Bad Gateway", "内容过大"),
        (402, "subrun_budget_exhausted", "Payment Required", "已达费用额度"),
        (418, "something_new", "I'm a teapot", "暂时无法完成（something_new）"),
    ],
)
def test_fetch_errors_map_to_chinese_user_messages(status, code, message, phrase):
    from agentbox_worker.errors import CallDeadlineExceeded as Deadline

    cls = Deadline if code in ("call_deadline_exceeded", "client_timeout") else GatewayError

    def fail(url: str) -> Any:
        raise cls(status, code, message)

    ctx = tool_ctx(ScriptedGateway(fetch=fail))
    r = ToolRegistry([WebFetch()]).dispatch("web_fetch", '{"url":"https://a.example/x"}', ctx)
    assert not r.ok and r.preview is not None
    shown = r.preview["text"]
    assert shown.startswith(f"抓取失败：{phrase}")
    assert not message or message not in r.content  # HTTP 状态文字不说明原因，不显示
    assert code == "something_new" or code not in shown


def test_search_timeout_message_names_the_search_service():
    def fail(query: str, n: int) -> Any:
        raise GatewayError(429, "tries_exhausted", "Too Many Requests")

    ctx = tool_ctx(ScriptedGateway(search=fail))
    r = ToolRegistry([WebSearch()]).dispatch("web_search", '{"query":"储能"}', ctx)
    assert not r.ok and r.preview == {"kind": "text", "text": "搜索失败：搜索服务无法访问或超时"}
