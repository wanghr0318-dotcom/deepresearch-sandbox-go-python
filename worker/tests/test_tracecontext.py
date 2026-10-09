"""W3C traceparent：与 Go 侧的共享向量（protocol/fixtures/v1/traceparent.json）
以及 Gateway 客户端发送的头。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import FakeGateway

from agentbox_worker.gateway import CallIds, GatewayClient, SubrunGateway
from agentbox_worker.tracecontext import parse_traceparent, valid_traceparent

VECTORS = json.loads(
    (
        Path(__file__).resolve().parents[2] / "protocol" / "fixtures" / "v1" / "traceparent.json"
    ).read_text(encoding="utf-8")
)
TP = VECTORS["valid"][0]["value"]


@pytest.mark.parametrize("case", VECTORS["valid"])
def test_valid_vectors(case):
    tp = parse_traceparent(case["value"])
    assert tp is not None
    assert (tp.trace_id, tp.parent_id, tp.sampled) == (
        case["trace_id"],
        case["parent_id"],
        case["sampled"],
    )
    assert valid_traceparent(case["value"]) == case["value"]


@pytest.mark.parametrize("value", [*VECTORS["invalid"], None, 1, b"00-x"])
def test_invalid_vectors(value):
    assert parse_traceparent(value) is None
    assert valid_traceparent(value) is None


def test_client_sends_traceparent_on_every_request(fake_gateway: FakeGateway):
    gw = GatewayClient(fake_gateway.socket_path, call_ids=CallIds(), timeout_s=5, traceparent=TP)
    gw.search("s1", "q")
    gw.budget()
    sub = SubrunGateway.of(gw, "sr-1")
    sub.fetch("s1", "https://example.com/")
    assert [r.headers.get("traceparent") for r in fake_gateway.requests] == [TP, TP, TP]
    assert fake_gateway.requests[2].headers.get("x-agentbox-subrun") == "sr-1"


@pytest.mark.parametrize("value", [None, "garbage", VECTORS["invalid"][5]])
def test_client_omits_invalid_or_missing_traceparent(fake_gateway: FakeGateway, value):
    gw = GatewayClient(fake_gateway.socket_path, call_ids=CallIds(), timeout_s=5, traceparent=value)
    gw.search("s1", "q")
    assert "traceparent" not in fake_gateway.requests[0].headers


def test_task_context_passes_host_traceparent_to_gateway(tmp_path):
    from test_sdk import run

    from agentbox_worker.runtime import Result

    seen = {}

    async def app(ctx):
        seen["ctx"] = ctx.traceparent
        seen["gateway"] = ctx.gateway.traceparent
        return Result("ok", [])

    code, _ = run(app, tmp_path, init={"traceparent": TP})
    assert code == 0
    assert seen == {"ctx": TP, "gateway": TP}

    async def untraced(ctx):
        seen["untraced"] = ctx.gateway.traceparent
        return Result("ok", [])

    code, _ = run(untraced, tmp_path / "b", init={"traceparent": "00-bad"})
    assert code == 0 and seen["untraced"] is None
