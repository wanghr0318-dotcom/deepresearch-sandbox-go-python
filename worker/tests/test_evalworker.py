"""evalworker: config validation, code extraction, the exec harness and the coding flow.

The harness is the code the Gateway runs in the exec sandbox; here it is run with the local
interpreter (tests may create processes) to check exit-code propagation, fixtures and timeouts.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from agentbox_worker import Result, WorkerFailure
from agentbox_worker.errors import GatewayError
from agentbox_worker.gateway import GatewayResult
from evalworker import NAME, VERSION
from evalworker.app import eval_kind
from evalworker.coding import (
    CHECK_TIMEOUT_EXIT,
    STAGE_CODE,
    check_timeout_s,
    exec_info,
    extract_code,
    harness,
    messages,
    parse_config,
    run_coding,
)

BASE = {
    "kind": "coding",
    "task_id": "add",
    "prompt": "Write add(a, b).",
    "check": "from solution import add\nassert add(2, 3) == 5\nprint('PASS')\n",
}


def test_parse_config_and_errors() -> None:
    cfg = parse_config({**BASE, "files": {"data.csv": "a\n"}, "wall_ms": 20000, "model": "m"})
    assert cfg.agent == "model" and cfg.files == {"data.csv": "a\n"} and cfg.wall_ms == 20000
    bad = [
        "x",
        {**BASE, "prompt": ""},
        {**BASE, "agent": "oracle"},
        {**BASE, "agent": "reference"},  # reference missing
        {**BASE, "files": {"../x": "1"}},
        {**BASE, "files": {"solution.py": "1"}},
        {**BASE, "wall_ms": 0},
        {**BASE, "max_tokens": True},
    ]
    for raw in bad:
        with pytest.raises(WorkerFailure):
            parse_config(raw)
    assert NAME == "evalworker" and "deepresearch" in VERSION
    assert eval_kind({"eval": {"kind": "coding"}}) == "coding"
    assert eval_kind({"topic": "x"}) is None


def test_extract_code_and_messages() -> None:
    assert extract_code("Here:\n```python\ndef f():\n    return 1\n```\nthanks") == (
        "def f():\n    return 1\n",
        "code_block",
    )
    assert extract_code("```\nx = 1\n```")[1] == "code_block"
    assert extract_code("x = 1") == ("x = 1\n", "raw")
    msgs = messages(parse_config({**BASE, "files": {"b.csv": "", "a.csv": ""}}))
    assert msgs[0]["content"].startswith(STAGE_CODE)
    assert msgs[1]["content"].startswith("[eval-task:add]\nWrite add")
    assert "a.csv, b.csv" in msgs[1]["content"]
    assert "solution import" not in json.dumps(msgs)  # the checker is not shown to the agent
    assert (
        check_timeout_s(None) == 50 and check_timeout_s(10_000) == 7 and check_timeout_s(1000) == 1
    )


def run_harness(code: str, tmp: Path) -> subprocess.CompletedProcess[str]:
    script = tmp / "main.py"
    script.write_text(code, encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-I", "-B", str(script)],
        cwd=tmp,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_harness_pass_fail_fixtures_timeout(tmp_path: Path) -> None:
    cfg = parse_config({**BASE, "files": {"nums.txt": "1\n2\n"}})
    ok = run_harness(harness("def add(a, b):\n    return a + b\n", cfg), tmp_path)
    assert ok.returncode == 0 and "PASS" in ok.stdout
    bad = run_harness(harness("def add(a, b):\n    return a - b\n", cfg), tmp_path)
    assert bad.returncode == 1 and "AssertionError" in bad.stderr
    fixture = parse_config(
        {
            **BASE,
            "check": "print(sum(int(x) for x in open('nums.txt')))",
            "files": {"nums.txt": "1\n2\n"},
        }
    )
    assert run_harness(harness("", fixture), tmp_path).stdout.strip() == "3"
    slow = parse_config(
        {
            **BASE,
            "check": "import time\nprint('started', flush=True)\ntime.sleep(30)",
            "wall_ms": 4000,
        }
    )
    res = run_harness(harness("", slow), tmp_path)
    assert res.returncode == CHECK_TIMEOUT_EXIT and "timed out" in res.stderr


def test_exec_info_caps_and_signal() -> None:
    body = {
        "status": "completed",
        "exit": {"code": 0, "signal": 0},
        "stdout": "x" * 40_000,
        "stderr": "",
        "wall_ms": 12,
        "queue_ms": 1,
        "image_digest": "sha256:ab",
    }
    info = exec_info(GatewayResult("root/check/exec/1", body, "f" * 64, False, 200))
    assert info["exit_code"] == 0 and info["stdout_truncated"] and len(info["stdout"]) == 16 << 10
    killed = exec_info(
        GatewayResult(
            "c", {"status": "completed", "exit": {"code": 0, "signal": 9}}, None, False, 200
        )
    )
    assert killed["signal"] == 9 and "exit_code" not in killed


class FakeGW:
    def __init__(self, reply: str | None = None, exec_error: GatewayError | None = None) -> None:
        self.reply = reply
        self.exec_error = exec_error
        self.calls: list[tuple[str, Any]] = []

    def chat(self, step_id: str, msgs: list[dict[str, str]], **kw: Any) -> GatewayResult:
        self.calls.append(("chat", (step_id, msgs, kw)))
        body = {"choices": [{"message": {"content": self.reply}}]}
        return GatewayResult(f"root/{step_id}/chat/1", body, "a" * 64, False, 200)

    def exec(self, step_id: str, code: str, **kw: Any) -> GatewayResult:
        self.calls.append(("exec", (step_id, code, kw)))
        if self.exec_error:
            raise self.exec_error
        body = {
            "status": "completed",
            "exit": {"code": 0, "signal": 0},
            "stdout": "PASS\n",
            "stderr": "",
        }
        return GatewayResult(f"root/{step_id}/exec/1", body, "b" * 64, False, 200)


class FakeCtx:
    def __init__(self, gw: FakeGW, out: Path) -> None:
        self.gateway = gw
        self.out_dir = out
        self.artifacts: list[tuple[str, str, str]] = []
        self.progressed: list[str] = []

    async def progress(self, kind: str, message: str, *, step_id: str | None = None) -> None:
        self.progressed.append(message)

    async def register_artifact(self, artifact_id: str, path: str, *, media_type: str) -> None:
        self.artifacts.append((artifact_id, path, media_type))


def test_run_coding_model_agent(tmp_path: Path) -> None:
    gw = FakeGW(reply="```python\ndef add(a, b):\n    return a + b\n```")
    ctx = FakeCtx(gw, tmp_path)
    res = asyncio.run(run_coding(ctx, {**BASE, "model": "m1", "wall_ms": 9000}))  # type: ignore[arg-type]
    assert isinstance(res, Result) and res.outputs == ["eval", "solution"]
    kind, (step, msgs, kw) = gw.calls[0]
    assert kind == "chat" and step == "solve" and kw["model"] == "m1"
    kind, (step, code, kw) = gw.calls[1]
    assert kind == "exec" and step == "check" and kw["wall_ms"] == 9000 and "return a + b" in code
    art = json.loads((tmp_path / "eval.json").read_text(encoding="utf-8"))
    assert art["schema"] == "agentbox.eval.coding/v1" and art["extract"] == "code_block"
    assert art["exec"]["exit_code"] == 0 and art["exec"]["stdout"] == "PASS\n"
    assert (tmp_path / "solution.py").read_text(encoding="utf-8").startswith("def add")
    assert [a[0] for a in ctx.artifacts] == ["solution", "eval"]


def test_run_coding_reference_skips_model_and_maps_errors(tmp_path: Path) -> None:
    gw = FakeGW()
    ctx = FakeCtx(gw, tmp_path)
    asyncio.run(
        run_coding(ctx, {**BASE, "agent": "reference", "reference": "def add(a, b): return a + b"})
    )  # type: ignore[arg-type]
    assert [c[0] for c in gw.calls] == ["exec"]
    gw = FakeGW(exec_error=GatewayError(404, "endpoint_not_configured", "exec off"))
    with pytest.raises(WorkerFailure) as info:
        asyncio.run(
            run_coding(FakeCtx(gw, tmp_path), {**BASE, "agent": "reference", "reference": "x"})
        )  # type: ignore[arg-type]
    assert info.value.code == "endpoint_not_configured" and not info.value.retryable
