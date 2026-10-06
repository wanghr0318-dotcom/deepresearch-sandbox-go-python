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
    assert not isinstance(info.value, ToolBudgetExhausted)
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
