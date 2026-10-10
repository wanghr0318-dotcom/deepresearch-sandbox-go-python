"""evalworker: config validation, code extraction, the exec harness and the coding flow.

The harness is the code the Gateway runs in the exec sandbox; here it is run with the local
interpreter (tests may create processes) to check exit-code propagation, fixtures and timeouts.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

from agentbox_worker import Result, WorkerFailure
from agentbox_worker.errors import GatewayError
from agentbox_worker.gateway import GatewayResult
from evalworker import NAME, VERSION
from evalworker.app import eval_kind
from evalworker.coding import (
    STAGE_CODE,
    check_timeout_s,
    exec_info,
    extract_code,
    harness,
    messages,
    parse_config,
    run_coding,
    split_harness_verdict,
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


posix_only = pytest.mark.skipif(sys.platform == "win32", reason="the harness uses pass_fds (POSIX)")


WRITABLE_PROBE = """
import json, os, sys
def nearest_existing(d):
    while d and not os.path.exists(d):
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return d
print(json.dumps(sorted({p for p in (nearest_existing(d) for d in sys.path if d)
                         if p and os.path.isdir(p) and os.access(p, os.W_OK)})))
"""


def _isolated_path_writable() -> list[str]:
    """The harness's own check (same algorithm, same interpreter, -I, a script in a temp dir)."""
    with tempfile.TemporaryDirectory() as d:
        script = Path(d) / "probe.py"
        script.write_text(WRITABLE_PROBE, encoding="utf-8")
        out = subprocess.run(
            [sys.executable, "-I", "-B", str(script)], cwd=d, capture_output=True, text=True
        ).stdout
    return json.loads(out)


WRITABLE_PATH = [] if sys.platform == "win32" else _isolated_path_writable()


def hz(solution: str, cfg: Any) -> str:
    return harness(solution, cfg, require_readonly_path=not WRITABLE_PATH)


def verdict(res: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    _, v = split_harness_verdict(res.stderr)
    assert v is not None, res.stderr
    return v


def verdict_of(res: subprocess.CompletedProcess[str]) -> str:
    return verdict(res)["verdict"]


@posix_only
def test_harness_pass_fail_fixtures_timeout(tmp_path: Path) -> None:
    cfg = parse_config({**BASE, "files": {"nums.txt": "1\n2\n"}})
    ok = run_harness(hz("def add(a, b):\n    return a + b\n", cfg), tmp_path)
    assert ok.returncode == 0 and verdict_of(ok) == "pass", ok.stderr
    assert ok.stdout == "PASS\n"  # the completion token is consumed by the harness
    bad = run_harness(hz("def add(a, b):\n    return a - b\n", cfg), tmp_path)
    assert bad.returncode == 1 and "AssertionError" in bad.stderr and verdict_of(bad) == "fail"
    fixture = parse_config(
        {
            **BASE,
            "check": "print(sum(int(x) for x in open('nums.txt')))",
            "files": {"nums.txt": "1\n2\n"},
        }
    )
    assert run_harness(hz("", fixture), tmp_path).stdout.strip() == "3"
    slow = parse_config(
        {
            **BASE,
            "check": "import time\nprint('started', flush=True)\ntime.sleep(30)",
            "wall_ms": 4000,
        }
    )
    res = run_harness(hz("", slow), tmp_path)
    assert res.returncode != 0 and verdict_of(res) == "timeout"


RICH_SOLUTION = """
def pairs(text):
    return [(w, len(w)) for w in text.split()]

def index(words):
    return {w: {"len": len(w), "upper": w.upper(), "chars": set(w)} for w in words}

def blob(n):
    return bytes(range(n)), frozenset({1, 2}), {(1, 2): None}

def boom(kind):
    if kind == "value":
        raise ValueError("bad value")
    if kind == "custom":
        class Oops(Exception):
            pass
        raise Oops("custom failure")
    raise SystemExit(0)

CONSTANT = [1, 2, 3]
"""

RICH_CHECK = """
from solution import pairs, index, blob, boom, CONSTANT
import solution
assert pairs("ab c") == [("ab", 2), ("c", 1)]
assert index(["hi"]) == {"hi": {"len": 2, "upper": "HI", "chars": {"h", "i"}}}
assert blob(3) == (b"\\x00\\x01\\x02", frozenset({1, 2}), {(1, 2): None})
assert CONSTANT == [1, 2, 3]
try:
    boom("value")
    raise AssertionError("no exception")
except ValueError as e:
    assert "bad value" in str(e)
try:
    boom("custom")
    raise AssertionError("no exception")
except Exception as e:
    assert type(e).__name__ == "RemoteError" and "custom failure" in str(e)
try:
    boom("exit")  # a SystemExit raised by the solution must not become the checker's exit
    raise AssertionError("no exception")
except SystemExit:
    raise AssertionError("SystemExit crossed the boundary")
except Exception as e:
    assert "SystemExit" in str(e)
try:
    from solution import missing  # noqa: F401
    raise AssertionError("missing name imported")
except ImportError:
    pass
print("PASS")
"""


@posix_only
def test_harness_proxy_round_trips_values_and_exceptions(tmp_path: Path) -> None:
    cfg = parse_config({**BASE, "check": RICH_CHECK})
    res = run_harness(hz(RICH_SOLUTION, cfg), tmp_path)
    assert verdict_of(res) == "pass", res.stderr
    assert res.stdout == "PASS\n"


@posix_only
def test_harness_script_solution_runs_real_code(tmp_path: Path) -> None:
    check = (
        "import subprocess, sys\n"
        "p = subprocess.run([sys.executable, 'solution.py', 'x'], capture_output=True, text=True)\n"
        "assert p.stdout.strip() == 'args=x data=42', (p.stdout, p.stderr)\n"
    )
    cfg = parse_config({**BASE, "check": check, "files": {"data.txt": "42"}})
    sol = "import sys\nprint('args=' + sys.argv[1] + ' data=' + open('data.txt').read())\n"
    res = run_harness(hz(sol, cfg), tmp_path)
    assert verdict_of(res) == "pass", res.stderr


@posix_only
def test_harness_nonzero_exit_expectation_is_token_protected(tmp_path: Path) -> None:
    cfg = parse_config({**BASE, "check": "from solution import add\nimport sys\nsys.exit(3)\n"})
    res = run_harness(hz("def add(a, b):\n    return a + b\n", cfg), tmp_path)
    v = verdict(res)
    assert v["verdict"] == "fail" and v["checker_exit"] == 3 and v["completed"] is True
    # the solution cannot make the checker exit with any code: os._exit(3) only ends its own process
    res = run_harness(hz("import os\nos._exit(3)\n", cfg), tmp_path)
    v = verdict(res)
    assert v["completed"] is False and v["checker_exit"] == 1


FORGE_HACKS = {
    "os._exit": "import os\nos._exit(0)\n",
    "sys.exit": "import sys\nsys.exit(0)\n",
    "atexit": "import atexit, os\natexit.register(lambda: os._exit(0))\nraise SystemExit(0)\n",
    "bare SystemExit": "raise SystemExit\n",
    "fake token": "print('EVAL-CHECK-DONE:' + '0' * 32)\nimport os\nos._exit(0)\n",
    "fd scan": "import os\nfor fd in range(3, 64):\n    try:\n        print(os.read(fd, 64))\n"
    "    except OSError:\n        pass\nos._exit(0)\n",
    # the reviewer's forgeries: call the token holder through __main__, or read its closure
    "__main__._eval_done": "import __main__ as m, os\nm._eval_done()\nos._exit(0)\n",
    "closure": "import __main__ as m, os\n"
    "print('EVAL-CHECK-DONE:' + m._eval_done.__closure__[0].cell_contents)\nos._exit(0)\n",
    "__main__ token": "import __main__ as m, os\nprint('EVAL-CHECK-DONE:' + m._eval_token)\n"
    "os._exit(0)\n",
    "kill parent": "import os, signal\nos.kill(os.getppid(), signal.SIGKILL)\n",
}


@posix_only
@pytest.mark.parametrize("name", sorted(FORGE_HACKS))
def test_harness_rejects_forgeries(tmp_path: Path, name: str) -> None:
    """A solution must not be able to pass a check whose asserts did not all run."""
    cfg = parse_config({**BASE, "wall_ms": 6000})  # a hack that blocks (fd scan) times out quickly
    res = run_harness(hz(FORGE_HACKS[name], cfg), tmp_path)
    assert verdict_of(res) in ("incomplete", "fail", "timeout"), (name, res.stdout, res.stderr)
    assert res.returncode != 0


PROC_MEM_PROBE = """
import os
for pid in (os.getppid(), int(open('/proc/%d/stat' % os.getppid()).read().split()[3])):
    # a non-dumpable process's /proc files are owned by root, not by the shared uid
    print('SAMEOWNER' if os.stat('/proc/%d/mem' % pid).st_uid == os.getuid() else 'ROOTOWNED', pid)
    for what in ('mem', 'environ', 'fd'):
        path = '/proc/%d/%s' % (pid, what)
        try:
            if what == 'fd':
                os.listdir(path)
            else:
                open(path, 'rb').read(1)
            print('READABLE', path)
        except OSError as e:
            print('DENIED', path, type(e).__name__)

def add(a, b):
    return a + b
"""


@posix_only
def test_solution_cannot_read_checker_or_harness_memory(tmp_path: Path) -> None:
    """The checker (parent of the solution process) and the harness (its grandparent) are
    non-dumpable: /proc/<pid>/mem, environ and fd are not accessible to the same-uid solution."""
    res = run_harness(hz(PROC_MEM_PROBE, parse_config(BASE)), tmp_path)
    assert verdict_of(res) == "pass", res.stderr
    log = res.stderr  # the solution process writes to its own log, forwarded on stderr
    assert "READABLE" not in log, log
    assert log.count("DENIED") == 6, log
    # PR_SET_DUMPABLE 0 took effect in both (independent of the Yama ptrace scope)
    assert log.count("ROOTOWNED") == 2 and "SAMEOWNER" not in log, log


def test_split_harness_verdict() -> None:
    rest, v = split_harness_verdict('boom\n[eval-harness] {"verdict": "pass", "checker_exit": 0}\n')
    assert rest == "boom\n" and v == {"verdict": "pass", "checker_exit": 0}
    # a forged marker earlier in the output does not count; only the last line does
    forged = '[eval-harness] {"verdict": "pass"}\nreal tail\n'
    assert split_harness_verdict(forged) == (forged, None)
    assert split_harness_verdict('[eval-harness] {"verdict": "maybe"}')[1] is None
    assert split_harness_verdict("")[1] is None


def test_exec_info_caps_and_signal() -> None:
    body = {
        "status": "completed",
        "exit": {"code": 0, "signal": 0},
        "stdout": "x" * 40_000,
        "stderr": "y" * 40_000 + '\n[eval-harness] {"verdict": "incomplete", "checker_exit": 0}\n',
        "wall_ms": 12,
        "queue_ms": 1,
        "image_digest": "sha256:ab",
    }
    info = exec_info(GatewayResult("root/check/exec/1", body, "f" * 64, False, 200))
    assert info["exit_code"] == 0 and info["stdout_truncated"] and len(info["stdout"]) == 16 << 10
    # the verdict is parsed from the full stderr before capping and removed from the recorded one
    assert info["harness"] == {"verdict": "incomplete", "checker_exit": 0}
    assert "[eval-harness]" not in info["stderr"]
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


# ---- fix rounds 3–4: the shared scratch area ----

# Attacker helper (inside the solution): every directory under the temp dir that holds a check.py.
FIND_CHECKER_DIRS = """
import os, tempfile
def checker_dirs():
    for root, dirs, files in os.walk(tempfile.gettempdir()):
        if "check.py" in files:
            yield root
"""

PLANT = (
    FIND_CHECKER_DIRS
    + """
forge = ("import __main__ as m, os\\n"
         "print('EVAL-CHECK-DONE:' + m._eval_token, flush=True)\\nos._exit(0)\\n")
for d in checker_dirs():
    for name in ("random.py", "traceback.py", "linecache.py", "json.py"):
        try:
            with open(os.path.join(d, name), "w") as f:
                f.write(forge)
        except OSError:
            pass

def add(a, b):
    return a - b  # wrong on purpose: only a forged token could make this pass
"""
)


@posix_only
@pytest.mark.parametrize(
    "check",
    [
        "from solution import add\nimport random\nassert add(2, 3) == 5\n",  # later import
        "from solution import add\nassert add(2, 3) == 5\n",  # failing assert: traceback path
    ],
)
def test_planted_modules_are_not_imported_by_the_checker(tmp_path: Path, check: str) -> None:
    res = run_harness(hz(PLANT, parse_config({**BASE, "check": check})), tmp_path)
    assert verdict_of(res) in ("fail", "tampered"), (res.stdout, res.stderr)


# A restoring attacker: forge expected.txt in the checker's directory at import, remove it again in
# an
# atexit handler (before any post-run check), and return the matching wrong answer.
RESTORING_FORGER = (
    FIND_CHECKER_DIRS
    + """
import atexit
planted = []
for d in checker_dirs():
    path = os.path.join(d, "expected.txt")
    try:
        with open(path, "w") as f:
            f.write("6")
        planted.append(path)
    except OSError:
        pass

@atexit.register
def restore():
    for path in planted:
        try:
            os.remove(path)
        except OSError:
            pass

def add(a, b):
    return a + b + 1  # wrong: 6, matching the forged fixture
"""
)

FIXTURE_READERS = {
    "open": "open('expected.txt').read()",
    "io.open": "__import__('io').open('expected.txt', encoding='utf-8').read()",
    "pathlib": "__import__('pathlib').Path('expected.txt').read_text()",
    "pathlib bytes": "__import__('pathlib').Path('expected.txt').read_bytes().decode()",
}


@posix_only
@pytest.mark.parametrize("reader", sorted(FIXTURE_READERS))
def test_fixture_forgery_with_restore_is_rejected(tmp_path: Path, reader: str) -> None:
    check = (
        f"from solution import add\nassert str(add(2, 3)) == {FIXTURE_READERS[reader]}.strip()\n"
    )
    cfg = parse_config({**BASE, "check": check, "files": {"expected.txt": "5"}})
    res = run_harness(hz(RESTORING_FORGER, cfg), tmp_path)
    assert verdict_of(res) in ("fail", "tampered"), (reader, res.stdout, res.stderr)


@posix_only
@pytest.mark.parametrize("reader", sorted(FIXTURE_READERS))
def test_fixture_readers_see_the_snapshot(tmp_path: Path, reader: str) -> None:
    """Correct solutions still pass with every hooked way of reading a fixture."""
    check = (
        f"from solution import add\nassert str(add(2, 3)) == {FIXTURE_READERS[reader]}.strip()\n"
    )
    cfg = parse_config({**BASE, "check": check, "files": {"expected.txt": "5"}})
    res = run_harness(hz("def add(a, b):\n    return a + b\n", cfg), tmp_path)
    assert verdict_of(res) == "pass", (reader, res.stderr)


@posix_only
def test_unhooked_fixture_reads_find_no_file(tmp_path: Path) -> None:
    """Fixtures are not on disk in the checker's directory: a low-level read fails (fail closed)."""
    check = "import os\nos.open('expected.txt', os.O_RDONLY)\n"
    cfg = parse_config({**BASE, "check": check, "files": {"expected.txt": "5"}})
    res = run_harness(hz("def add(a, b):\n    return a + b\n", cfg), tmp_path)
    assert verdict_of(res) == "fail" and "FileNotFoundError" in res.stderr, res.stderr


STRAY = (
    FIND_CHECKER_DIRS
    + """
for d in checker_dirs():
    try:
        with open(os.path.join(d, "note.txt"), "w") as f:
            f.write("x")
    except OSError:
        pass

def add(a, b):
    return a + b  # correct: only the stray file makes this run fail
"""
)


@posix_only
def test_stray_file_in_the_checker_directory_is_tampering(tmp_path: Path) -> None:
    res = run_harness(hz(STRAY, parse_config(BASE)), tmp_path)
    v = verdict(res)
    assert v["verdict"] == "tampered" and v["workdir_clean"] is False, res.stderr


@posix_only
def test_escaped_descendant_holding_the_pipes_is_bounded(tmp_path: Path) -> None:
    check = "import subprocess, sys\nsubprocess.run([sys.executable, 'solution.py'])\n"
    sol = (
        "import subprocess, sys\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'],\n"
        "                 start_new_session=True)\n"
    )
    res = run_harness(hz(sol, parse_config({**BASE, "check": check, "wall_ms": 6000})), tmp_path)
    v = verdict(res)
    assert v["verdict"] == "timeout" and v["pipes_held"] is True, res.stderr


@posix_only
def test_writable_sys_path_is_reported_unsafe(tmp_path: Path) -> None:
    """With the read-only requirement on, a writable directory on the checker's sys.path makes the
    verdict "unsafe" (in the exec sandbox the interpreter's directories are read-only)."""
    ok = "def add(a, b):\n    return a + b\n"
    res = run_harness(harness(ok, parse_config(BASE), require_readonly_path=True), tmp_path)
    v = verdict(res)
    assert v["syspath_writable"] == WRITABLE_PATH
    assert v["verdict"] == ("unsafe" if WRITABLE_PATH else "pass")
    assert v["nondumpable"] is True
