"""工作区工具与 MCP 工具（设计 2026-10-10-shell-file-mcp）。

工具与编排循环使用进程内的 ScriptedGateway（Windows 可运行）；GatewayClient 的端点
与头部使用 conftest 的 AF_UNIX fake（没有 AF_UNIX 的平台跳过）。
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from agentfakes import ScriptedGateway, ScriptedModel, SessionHost, call, reply
from conftest import FakeGateway, Reply, error_reply
from test_chatagent import (  # _out_root：自动夹具（start_task 的输出目录）
    SKILLS,
    _out_root,  # noqa: F401
    checkpoints,
    progress,
    result,
    start_task,
)

from agentbox_worker.errors import AccessRevoked, BudgetExhausted, GatewayError
from agentbox_worker.gateway import CallIds, GatewayClient
from agentbox_worker.tools import ToolRegistry
from agentbox_worker.tools.base import ToolContext, TurnFlags
from agentbox_worker.tools.budget import TurnBudget
from agentbox_worker.tools.mcp import McpTool, mcp_tools
from agentbox_worker.tools.sources import SourceStore
from agentbox_worker.tools.workspace import ExecShell, ListDir, ReadFile, WriteFile
from chatagent import TurnState, make_app, parse_turn_config

CALC = {
    "name": "mcp__calc__calculate",
    "server": "calc",
    "tool": "calculate",
    "description": "Evaluate an arithmetic expression.",
    "input_schema": {
        "type": "object",
        "properties": {"expression": {"type": "string"}},
        "required": ["expression"],
    },
}


def tool_ctx(gw: ScriptedGateway, budget: TurnBudget | None = None) -> ToolContext:
    return ToolContext(
        gateway=gw,
        step_id="orch",
        budget=budget or TurnBudget(),
        sources=SourceStore(),
        flags=TurnFlags(),
    )


class Workspace:
    """测试用的工作区：ScriptedGateway 的 workspace 与 shell 脚本（只模拟本测试用到的命令）。"""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def op(self, op: str, body: dict[str, Any]) -> dict[str, Any]:
        path = body.get("path", "")
        if ".." in path.split("/") or path.startswith("/"):
            raise GatewayError(400, "invalid_path")
        ws = {"version": "v", "files": len(self.files), "bytes": 0}
        if op == "write":
            created = path not in self.files
            self.files[path] = body["content"]
            ws["files"] = len(self.files)
            size = len(body["content"].encode())
            return {
                "path": path,
                "size": size,
                "sha256": "0" * 64,
                "created": created,
                "workspace": ws,
            }
        if op == "read":
            if path not in self.files:
                raise GatewayError(404, "not_found")
            lines = self.files[path].splitlines(keepends=True)
            return {
                "path": path,
                "content": self.files[path],
                "start_line": 1,
                "end_line": len(lines),
                "total_lines": len(lines),
                "truncated": False,
                "binary": False,
                "size": len(self.files[path]),
                "sha256": "0" * 64,
            }
        entries = [
            {"name": p, "type": "file", "size": len(c)} for p, c in sorted(self.files.items())
        ]
        return {"path": path, "entries": entries, "workspace": ws}

    def shell(self, body: dict[str, Any]) -> dict[str, Any]:
        cmd = body["command"]
        before = set(self.files)
        if cmd.startswith("python3 -m unittest"):
            ok = "return a + b" in self.files.get("calc.py", "")
            self.files["test-report.txt"] = "OK\n" if ok else "FAILED\n"
            out, err, code = (
                ("", "Ran 1 test\n\nOK\n", 0) if ok else ("", "FAILED (failures=1)\n", 1)
            )
        else:
            out, err, code = "", f"unknown command {cmd}\n", 127
        added = sorted(set(self.files) - before)
        return {
            "status": "completed",
            "exit": {"code": code, "signal": 0},
            "stdout": out,
            "stderr": err,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "wall_ms": 412,
            "queue_ms": 0,
            "limits": {"wall_ms": 60000},
            "changes": {"added": added, "modified": [], "deleted": []},
            "skipped_outputs": [],
            "workspace": {"version": "v", "files": len(self.files), "bytes": 10},
        }


def calc(body: dict[str, Any]) -> dict[str, Any]:
    expr = body["arguments"]["expression"]
    value = eval(expr, {"__builtins__": {}})  # noqa: S307  测试脚本中的固定表达式
    return {
        "server": "calc",
        "tool": "calculate",
        "is_error": False,
        "content": [{"type": "text", "text": f"{expr} = {value}"}],
        "structured_content": {"value": value},
    }


# ---- 工具 ----


def test_exec_shell_result_and_errors():
    ws = Workspace()
    ws.files["calc.py"] = "def add(a, b):\n    return a + b\n"
    gw = ScriptedGateway(shell=ws.shell)
    res = ExecShell().run({"command": "python3 -m unittest -v", "timeout_s": 30}, tool_ctx(gw))
    assert res.ok and res.raw == {"call_id": "root/orch/workspace_exec/1"} and res.blobs
    assert "退出码 0" in res.content and "新增：test-report.txt" in res.content
    assert "Ran 1 test" in res.content and "工作区：" in res.content
    assert gw.calls[0].body == {"command": "python3 -m unittest -v", "timeout_ms": 30000}
    # 非零退出码：ok=False，预览带 stderr
    ws.files["calc.py"] = "def add(a, b):\n    return a - b\n"
    res = ExecShell().run({"command": "python3 -m unittest"}, tool_ctx(gw))
    assert not res.ok and "FAILED" in res.preview["text"]
    # exec 配额用尽：提示不要再调用；访问撤销等致命错误上抛
    quota = ScriptedGateway(
        shell=lambda b: (_ for _ in ()).throw(BudgetExhausted(402, "exec_quota_exhausted"))
    )
    res = ExecShell().run({"command": "ls"}, tool_ctx(quota))
    assert not res.ok and "不要再调用 exec_shell" in res.content
    lost = ScriptedGateway(
        shell=lambda b: (_ for _ in ()).throw(GatewayError(410, "workspace_lost"))
    )
    res = ExecShell().run({"command": "ls"}, tool_ctx(lost))
    assert not res.ok and "无法恢复" in res.content
    revoked = ScriptedGateway(
        shell=lambda b: (_ for _ in ()).throw(AccessRevoked(403, "access_revoked"))
    )
    with pytest.raises(AccessRevoked):
        ExecShell().run({"command": "ls"}, tool_ctx(revoked))


def test_file_tools():
    ws = Workspace()
    gw = ScriptedGateway(workspace=ws.op)
    res = WriteFile().run({"path": "src/a.py", "content": "print(1)\n"}, tool_ctx(gw))
    assert res.ok and "已新建 src/a.py" in res.content
    assert res.raw["inline"]["request"] == {"path": "src/a.py", "content_bytes": 9}
    res = ReadFile().run({"path": "src/a.py"}, tool_ctx(gw))
    assert res.ok and res.content.endswith("print(1)\n") and "第 1–1 行" in res.content
    res = ListDir().run({}, tool_ctx(gw))
    assert res.ok and "src/a.py" in res.content
    res = ReadFile().run({"path": "../etc/passwd"}, tool_ctx(gw))
    assert not res.ok and "路径不合法" in res.content
    res = ReadFile().run({"path": "missing"}, tool_ctx(gw))
    assert not res.ok and "不存在" in res.content
    assert [op for op, _ in gw.file_ops] == ["write", "read", "list", "read", "read"]
    assert gw.calls == []  # 文件操作不是计费调用、不占 call id


def test_mcp_tool_counts_budget_and_reports_errors():
    gw = ScriptedGateway(mcp=calc, tool_budget=lambda k: (k, 3))
    budget = TurnBudget(limit=3)
    tool = McpTool(CALC)
    assert tool.name == "mcp__calc__calculate" and tool.counts_budget and not tool.validate
    assert "消耗 1 次本轮工具额度" in tool.description
    res = tool.run({"expression": "6*7"}, tool_ctx(gw, budget))
    assert res.ok and "6*7 = 42" in res.content and "已用 1/3" in res.content
    assert gw.calls[0].kind == "mcp" and gw.calls[0].call_id == "root/orch/mcp/1"
    assert gw.calls[0].body == {
        "server": "calc",
        "tool": "calculate",
        "arguments": {"expression": "6*7"},
    }
    # 工具报告的错误：ok=False，内容给模型
    err = ScriptedGateway(
        mcp=lambda b: {
            "server": "calc",
            "tool": "calculate",
            "is_error": True,
            "content": [{"type": "text", "text": "division by zero"}],
        }
    )
    res = tool.run({"expression": "1/0"}, tool_ctx(err))
    assert not res.ok and "division by zero" in res.content
    # Gateway 拒绝（不在允许清单）：计 1 次、ok=False
    denied = ScriptedGateway(
        mcp=lambda b: (_ for _ in ()).throw(GatewayError(403, "mcp_tool_not_allowed"))
    )
    b2 = TurnBudget(limit=3)
    res = tool.run({"expression": "1"}, tool_ctx(denied, b2))
    assert not res.ok and "不在服务端允许的清单中" in res.content and b2.used == 1
    # 额度已用尽：不发起
    spent = TurnBudget(limit=1, used=1)
    gw3 = ScriptedGateway(mcp=calc)
    res = tool.run({"expression": "1"}, tool_ctx(gw3, spent))
    assert res.control == "budget_exhausted" and gw3.calls == []


def test_registry_passes_mcp_arguments_without_local_schema_check():
    reg = ToolRegistry(mcp_tools([CALC, {"name": "bad"}, CALC]))
    assert reg.names() == ["mcp__calc__calculate"]
    gw = ScriptedGateway(mcp=calc)
    # 本地校验器不支持的 schema 关键字也不影响：参数交给 MCP 服务器
    res = reg.dispatch("mcp__calc__calculate", json.dumps({"expression": "2+3"}), tool_ctx(gw))
    assert res.ok and "2+3 = 5" in res.content
    res = reg.dispatch("mcp__calc__calculate", "[1, 2]", tool_ctx(gw))
    assert not res.ok and "参数须为 JSON 对象" in res.content
    with pytest.raises(ValueError):
        reg.register(McpTool(CALC))


def test_turn_config_and_state():
    cfg = parse_turn_config({"text": "hi"})
    assert not cfg.workspace_tools and not cfg.mcp_tools
    cfg = parse_turn_config({"text": "hi", "tools": {"workspace": True, "mcp": True}})
    assert cfg.workspace_tools and cfg.mcp_tools
    with pytest.raises(Exception, match="config.tools.workspace"):
        parse_turn_config({"text": "hi", "tools": {"workspace": "yes"}})
    st = TurnState()
    assert "mcp_tools" not in st.to_json()  # 未启用时状态与之前相同
    st.mcp_tools = [CALC]
    back = TurnState.from_json(json.loads(json.dumps(st.to_json())))
    assert back.mcp_tools == [CALC]


# ---- 编排循环：一个完整的演示轮次 ----


def demo_model() -> ScriptedModel:
    test_src = (
        "import unittest\nfrom calc import add\n\n"
        "class T(unittest.TestCase):\n    def test_add(self):\n"
        "        self.assertEqual(add(2, 3), 5)\n"
    )
    return ScriptedModel(
        orch=[
            call("write_file", path="calc.py", content="def add(a, b):\n    return a + b\n"),
            call("write_file", path="test_calc.py", content=test_src),
            call("exec_shell", command="python3 -m unittest -v"),
            call("read_file", path="test-report.txt"),
            call("mcp__calc__calculate", expression="(2 + 3) * 7"),
            reply("测试通过（OK），add(2, 3) = 5；外部计算器算得 (2 + 3) * 7 = 35。"),
        ]
    )


def test_demo_turn_writes_runs_reads_and_calls_mcp():
    ws = Workspace()
    model = demo_model()
    gw = ScriptedGateway(
        chat=model,
        shell=ws.shell,
        workspace=ws.op,
        mcp_list=[CALC],
        mcp=calc,
        tool_budget=lambda k: (k, 30),
    )
    host = SessionHost()
    start_task(
        host,
        "t-1",
        "a-1",
        {"text": "写一个 add 函数并测试", "tools": {"workspace": True, "mcp": True}},
    )

    def factory(ctx: Any) -> ScriptedGateway:
        gw.call_ids = ctx.call_ids
        return gw

    code, events = host.run(make_app(gateway=factory, skills_root=SKILLS), timeout=20)
    assert code == 0
    first = model.bodies("orch")[0]
    names = {s["function"]["name"] for s in first["tools"]}
    assert {"exec_shell", "read_file", "write_file", "list_dir", "mcp__calc__calculate"} <= names
    system = first["messages"][0]["content"]
    assert "## 工作区（写代码、跑测试）" in system and "## 外部工具（MCP）" in system
    # 工具调用顺序与 Gateway 调用：两个文件写入、一次工作区命令、一次读取、一次 MCP 调用
    results = progress(events, "tool_result")
    assert [r["data"]["tool"] for r in results] == [
        "write_file",
        "write_file",
        "exec_shell",
        "read_file",
        "mcp__calc__calculate",
    ]
    assert all(r["data"]["ok"] for r in results)
    assert [c.kind for c in gw.calls if c.kind != "chat"] == ["workspace_exec", "mcp"]
    assert [op for op, _ in gw.file_ops] == ["write", "write", "read"]
    # ⟨/⟩：工作区命令与 MCP 调用带原始请求与响应 blob；文件操作不带
    raws = {r["data"]["tool"]: r["data"].get("raw") for r in results}
    assert raws["exec_shell"]["request"] == {"command": "python3 -m unittest -v"}
    assert raws["exec_shell"]["response_ref"] and raws["read_file"] is None
    assert raws["mcp__calc__calculate"]["request"] == {
        "server": "calc",
        "tool": "calculate",
        "arguments": {"expression": "(2 + 3) * 7"},
    }
    # MCP 调用计入工具额度（额度事件），工作区工具不计
    budgets = progress(events, "budget")
    assert [b["data"]["used"] for b in budgets] == [1]
    # 清单保存在 checkpoint 中（恢复后 tools 不变）
    state = checkpoints(events)[-1]["state"]
    assert state["mcp_tools"][0]["name"] == "mcp__calc__calculate"
    assert "35" in result(events)["summary"]


def test_default_turn_has_no_workspace_or_mcp_tools():
    model = ScriptedModel(orch=[reply("好。")])
    gw = ScriptedGateway(chat=model, mcp_list=[CALC])
    host = SessionHost()
    start_task(host, "t-1", "a-1", {"text": "你好"})

    def factory(ctx: Any) -> ScriptedGateway:
        gw.call_ids = ctx.call_ids
        return gw

    code, events = host.run(make_app(gateway=factory, skills_root=SKILLS), timeout=20)
    assert code == 0
    body = model.bodies("orch")[0]
    names = {s["function"]["name"] for s in body["tools"]}
    assert names == {"read_skill", "web_search", "web_fetch", "read_source", "run_python"}
    assert "工作区" not in body["messages"][0]["content"]


def test_mcp_list_unavailable_means_no_mcp_tools():
    model = ScriptedModel(orch=[reply("好。")])
    gw = ScriptedGateway(chat=model, mcp_list=GatewayError(404, "endpoint_not_configured"))
    host = SessionHost()
    start_task(host, "t-1", "a-1", {"text": "你好", "tools": {"mcp": True}})

    def factory(ctx: Any) -> ScriptedGateway:
        gw.call_ids = ctx.call_ids
        return gw

    code, _ = host.run(make_app(gateway=factory, skills_root=SKILLS), timeout=20)
    assert code == 0
    names = {s["function"]["name"] for s in model.bodies("orch")[0]["tools"]}
    assert not any(n.startswith("mcp__") for n in names)


# ---- GatewayClient（AF_UNIX） ----


def test_gateway_client_workspace_and_mcp_endpoints(fake_gateway: FakeGateway):
    def intercept(req):
        if req.path == "/v1/workspace/read":
            return Reply(200, {"path": "a", "content": "x"})
        if req.path == "/v1/workspace/write":
            return error_reply(400, "invalid_path")
        if req.path == "/v1/mcp/tools":
            return Reply(200, {"tools": [CALC, "junk"]})
        if req.path in ("/v1/workspace/exec", "/v1/mcp/call"):
            sha = fake_gateway.put_blob(b"{}")
            return Reply(
                200, {"ok": True}, {"X-Agentbox-Blob": sha, "X-Agentbox-Tool-Budget": "4/30"}
            )
        return None

    fake_gateway.interceptor = intercept
    gw = GatewayClient(fake_gateway.socket_path, call_ids=CallIds(), timeout_s=5)
    assert gw.workspace("read", {"path": "a"}) == {"path": "a", "content": "x"}
    with pytest.raises(GatewayError) as e:
        gw.workspace("write", {"path": "../x", "content": ""})
    assert e.value.code == "invalid_path"
    with pytest.raises(ValueError):
        gw.workspace("delete", {})
    assert gw.mcp_tools() == [CALC]
    r = gw.workspace_exec("orch", "ls -la", timeout_ms=5000)
    m = gw.mcp_call("orch", "calc", "calculate", {"expression": "1"})
    assert r.call_id == "root/orch/workspace_exec/1" and m.call_id == "root/orch/mcp/1"
    assert m.tool_budget == (4, 30)
    got = [(q.method, q.path, q.headers.get("x-agentbox-call-id")) for q in fake_gateway.requests]
    assert got == [
        ("POST", "/v1/workspace/read", None),
        ("POST", "/v1/workspace/write", None),
        ("GET", "/v1/mcp/tools", None),
        ("POST", "/v1/workspace/exec", "root/orch/workspace_exec/1"),
        ("POST", "/v1/mcp/call", "root/orch/mcp/1"),
    ]
    assert fake_gateway.requests[3].json() == {"command": "ls -la", "timeout_ms": 5000}
    assert fake_gateway.requests[4].json() == {
        "server": "calc",
        "tool": "calculate",
        "arguments": {"expression": "1"},
    }
