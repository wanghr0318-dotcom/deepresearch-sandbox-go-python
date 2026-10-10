"""Coding/terminal eval tasks: obtain a solution, run the suite's checker in the exec sandbox.

The checker never runs in this process: it runs through the Gateway's /v1/exec in a fresh exec
environment (no network, no Gateway, no /workspace). Judgement is left to `agentbox eval`, which
reads the `eval` artifact written here (schema agentbox.eval.coding/v1).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from agentbox_worker import Result, TaskContext, WorkerFailure
from agentbox_worker.errors import GatewayError
from agentbox_worker.gateway import GatewayResult
from evalworker import isolation

ARTIFACT_SCHEMA = "agentbox.eval.coding/v1"
STAGE_CODE = "[stage:code]"
SOLVE_STEP = "solve"
CHECK_STEP = "check"
EVAL_ARTIFACT = "eval"
SOLUTION_ARTIFACT = "solution"
OUTPUT_CAP = 16 << 10  # bytes of checker stdout/stderr kept in the artifact
DEFAULT_MAX_TOKENS = 8192
DEFAULT_CHECK_TIMEOUT_S = 50  # exec default wall is 60 s

SYSTEM_PROMPT = (
    STAGE_CODE + " You are a careful Python coding agent. Write the complete contents of a Python "
    "module named solution.py that solves the task. Use only the Python standard library. "
    "Reply with exactly one ```python fenced code block and nothing else."
)

_FILE_NAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_FENCE = re.compile(r"```[ \t]*(?:python3?|py)?[ \t]*\n(.*?)```", re.DOTALL | re.IGNORECASE)


@dataclass(frozen=True)
class CodingConfig:
    task_id: str
    prompt: str
    check: str
    agent: str = "model"
    reference: str | None = None
    files: dict[str, str] = field(default_factory=dict)
    wall_ms: int | None = None
    model: str | None = None
    max_tokens: int = DEFAULT_MAX_TOKENS


def parse_config(raw: Any) -> CodingConfig:
    """Validates config.eval of a coding task; invalid configs fail the task (not retryable)."""
    if not isinstance(raw, dict):
        raise WorkerFailure("invalid_config", "config.eval must be an object")

    def text(key: str, required: bool = True) -> str | None:
        value = raw.get(key)
        if value is None and not required:
            return None
        if not isinstance(value, str) or not value.strip():
            raise WorkerFailure("invalid_config", f"config.eval.{key} must be a non-empty string")
        return value

    agent = raw.get("agent", "model")
    if agent not in ("model", "reference"):
        raise WorkerFailure(
            "invalid_config", f"config.eval.agent must be model or reference: {agent!r}"
        )
    files = raw.get("files") or {}
    if not isinstance(files, dict) or not all(
        isinstance(k, str) and _FILE_NAME.match(k) and isinstance(v, str) for k, v in files.items()
    ):
        raise WorkerFailure("invalid_config", "config.eval.files must map plain file names to text")
    if {"solution.py", "check.py"} & files.keys() or any(k.startswith("_eval") for k in files):
        raise WorkerFailure(
            "invalid_config", "config.eval.files cannot use solution.py, check.py or _eval* names"
        )
    wall = raw.get("wall_ms")
    if wall is not None and (isinstance(wall, bool) or not isinstance(wall, int) or wall <= 0):
        raise WorkerFailure("invalid_config", "config.eval.wall_ms must be a positive integer")
    max_tokens = raw.get("max_tokens", DEFAULT_MAX_TOKENS)
    if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or max_tokens <= 0:
        raise WorkerFailure("invalid_config", "config.eval.max_tokens must be a positive integer")
    reference = text("reference", required=agent == "reference")
    return CodingConfig(
        task_id=text("task_id") or "",
        prompt=text("prompt") or "",
        check=text("check") or "",
        agent=agent,
        reference=reference,
        files=dict(files),
        wall_ms=wall,
        model=text("model", required=False),
        max_tokens=max_tokens,
    )


def extract_code(reply: str) -> tuple[str, str]:
    """The first fenced code block of the model reply, else the whole reply. Returns (code, how)."""
    match = _FENCE.search(reply)
    if match:
        return match.group(1).rstrip() + "\n", "code_block"
    return reply.strip() + "\n", "raw"


def messages(cfg: CodingConfig) -> list[dict[str, str]]:
    user = f"[eval-task:{cfg.task_id}]\n{cfg.prompt.strip()}"
    if cfg.files:
        user += "\n\nFiles in the working directory: " + ", ".join(sorted(cfg.files))
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]


def check_timeout_s(wall_ms: int | None) -> int:
    """Checker timeout in the exec: a few seconds below the exec wall, so the harness can report."""
    if wall_ms is None:
        return DEFAULT_CHECK_TIMEOUT_S
    return max(1, wall_ms // 1000 - 3)


# The exec harness (see the design doc §3.2 and evalworker.isolation).
#
# Passing needs a completion token that only the checker can print. The harness generates a random
# nonce and hands it to the checker through an inherited pipe (never argv or env). The checker runs
# with `python -I` (no writable directory on sys.path) in a directory that holds only check.py, the
# proxy `solution.py` and the codec; the fixtures are not on disk there at all. Its prelude marks it
# non-dumpable (failure => exit 97, "unsafe"), reads and closes the token pipe, records hashes of
# its
# three files, loads the codec and the proxy by path, and replaces builtins.open, io.open (which
# pathlib uses) and _io.open process-wide with a hook that serves the fixture names from an
# in-memory snapshot; any other read of a fixture path finds no file. Reads that bypass the hook
# (os.open, io.FileIO, mmap, C extensions, subprocesses) see whatever is on disk, including files
# the solution planted: such checks also fail for correct solutions, so validate every suite with
# --agent reference before trusting model scores. The check code runs inside a wrapper; after its
# last
# statement the checker lists its own directory, and any extra entry, symlink or changed file means
# no token (exit 98, "tampered"); otherwise it prints the token and exits with the check's exit code
# (sys.exit(n) inside the check included). The solution runs in a child process in its own directory
# under a separate parent (see isolation). Verdicts: "unsafe" (non-dumpable failed or a sys.path
# directory is writable), "timeout", "tampered", "pass" (exit 0 + token as the last stdout line,
# found
# in the uncapped output), "incomplete" (exit 0 without it), otherwise "fail". It is reported out of
# band as the last stderr line "[eval-harness] {json}"; output is capped before forwarding.
_HARNESS = r"""import ctypes, json, os, secrets, shutil, signal, subprocess, sys, tempfile
try:
    nondumpable = ctypes.CDLL(None).prctl(4, 0, 0, 0, 0) == 0  # PR_SET_DUMPABLE 0
except Exception:
    nondumpable = False
FIXTURES = json.loads({fixtures!r})
SOLUTION = json.loads({solution!r})
CHECK = json.loads({check!r})
PROXY = json.loads({proxy!r})
SERVER = json.loads({server!r})
CODEC = json.loads({codec!r})
CAP = {cap}
TIMEOUT = {timeout}
REQUIRE_RO_PATH = {require_ro_path}


def nearest_existing(d):
    while d and not os.path.exists(d):
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return d


# The checker runs with -I like this harness: no directory on its sys.path (or, for entries that do
# not exist, their nearest existing parent) may be writable by us.
writable = sorted({{p for p in (nearest_existing(d) for d in sys.path if d)
                   if p and os.path.isdir(p) and os.access(p, os.W_OK)}})
# Separate, unpredictable parents for the checker and the solution (defence in depth).
work_parent, sol_parent = tempfile.mkdtemp(prefix="eval-"), tempfile.mkdtemp(prefix="eval-")
work = tempfile.mkdtemp(dir=work_parent)
sol = tempfile.mkdtemp(dir=sol_parent)


def write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


for name, content in FIXTURES.items():  # fixtures exist on disk only in the solution's directory
    write(os.path.join(sol, name), content)
for d in (work, sol):
    write(os.path.join(d, "_eval_codec.py"), CODEC)
write(os.path.join(sol, "solution.py"), SOLUTION)
write(os.path.join(sol, "_eval_server.py"), SERVER)
write(os.path.join(work, "solution.py"), PROXY.replace("__EVAL_SOL_DIR__", repr(sol)))
nonce = secrets.token_hex(16)
token = ("EVAL-CHECK-DONE:" + nonce).encode()
r, w = os.pipe()
os.write(w, nonce.encode())
os.close(w)
prelude = (
    "import builtins as _eval_b, ctypes as _eval_ct, hashlib as _eval_h\n"
    "import importlib.util as _eval_iu\n"
    "import _io as _eval_rio\n"
    "import io as _eval_io, json as _eval_json, os as _eval_os, sys as _eval_sys\n"
    "try:\n"
    "    _eval_ok = _eval_ct.CDLL(None).prctl(4, 0, 0, 0, 0) == 0\n"
    "except Exception:\n"
    "    _eval_ok = False\n"
    "if not _eval_ok:\n"
    "    _eval_os._exit(97)\n"
    "_eval_token = _eval_os.read(%d, 64).decode()\n"
    "_eval_os.close(%d)\n"
    "_eval_dir = %r\n"
    "_eval_real_open = _eval_io.open\n"
    "def _eval_sha(n):\n"
    "    with _eval_real_open(_eval_os.path.join(_eval_dir, n), 'rb') as f:\n"
    "        return _eval_h.sha256(f.read()).hexdigest()\n"
    "_eval_files = {{n: _eval_sha(n) for n in ('_eval_codec.py', 'check.py', 'solution.py')}}\n"
    "_eval_fix = {{_eval_os.path.join(_eval_dir, n): c.encode('utf-8')\n"
    "             for n, c in _eval_json.loads(%r).items()}}\n"
    "def _eval_open(file, mode='r', buffering=-1, encoding=None, errors=None, newline=None,\n"
    "               closefd=True, opener=None):\n"
    "    if not isinstance(file, int):\n"
    "        try:\n"
    "            p = _eval_os.path.abspath(_eval_os.fsdecode(file))\n"
    "        except TypeError:\n"
    "            p = None\n"
    "        if p in _eval_fix:\n"
    "            if any(c in mode for c in 'wax+'):\n"
    "                raise PermissionError('fixture %%s is read-only' %% p)\n"
    "            data = _eval_io.BytesIO(_eval_fix[p])\n"
    "            if 'b' in mode:\n"
    "                return data\n"
    "            return _eval_io.TextIOWrapper(data, encoding=encoding or 'utf-8', errors=errors,\n"
    "                                          newline=newline)\n"
    "    return _eval_real_open(file, mode, buffering, encoding, errors, newline, closefd,\n"
    "                           opener)\n"
    "def _eval_load(name, path):\n"
    "    spec = _eval_iu.spec_from_file_location(name, path)\n"
    "    mod = _eval_iu.module_from_spec(spec)\n"
    "    _eval_sys.modules[name] = mod\n"
    "    spec.loader.exec_module(mod)\n"
    "_eval_load('_eval_codec', _eval_os.path.join(_eval_dir, '_eval_codec.py'))\n"
    "_eval_b.open = _eval_io.open = _eval_rio.open = _eval_open\n"
    "_eval_load('solution', _eval_os.path.join(_eval_dir, 'solution.py'))\n"
    % (r, r, work, json.dumps(FIXTURES)))
wrapper = (
    "_eval_code = 0\n"
    "try:\n"
    "    exec(compile(%r, 'check.py', 'exec'),\n"
    "         {{'__name__': '__main__', '__builtins__': _eval_b}})\n"
    "except SystemExit as _eval_e:\n"
    "    _c = _eval_e.code\n"
    "    _eval_code = _c if isinstance(_c, int) else (0 if _c is None else 1)\n"
    "_eval_sys.stdout.flush()\n"
    "def _eval_clean():\n"
    "    if sorted(_eval_os.listdir(_eval_dir)) != sorted(_eval_files):\n"
    "        return False\n"
    "    return all(not _eval_os.path.islink(_eval_os.path.join(_eval_dir, n)) and\n"
    "               _eval_sha(n) == h for n, h in _eval_files.items())\n"
    "if not _eval_clean():\n"
    "    _eval_os._exit(98)\n"
    "print('EVAL-CHECK-DONE:' + _eval_token, flush=True)\n"
    "_eval_sys.exit(_eval_code)\n" % CHECK)
write(os.path.join(work, "check.py"), prelude + wrapper)
p = subprocess.Popen([sys.executable, "-I", "-B", "check.py"], cwd=work, pass_fds=(r,),
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
os.close(r)
timed_out = held = False
try:
    out, err = p.communicate(timeout=TIMEOUT)
except subprocess.TimeoutExpired:
    timed_out = True
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        out, err = p.communicate(timeout=5)
    except subprocess.TimeoutExpired:  # an escaped (setsid) descendant holds the pipes
        held = True
        out, err = b"", b"[eval-harness] checker output pipes held open after kill\n"
try:
    os.killpg(p.pid, signal.SIGKILL)  # leftovers (the solution process, its children)
except OSError:
    pass
rc = p.returncode if p.returncode is not None and p.returncode >= 0 else 128 - (p.returncode or -9)
lines = out.rstrip(b"\n").split(b"\n")
done = lines[-1] == token
if done:
    out = b"\n".join(lines[:-1]) + (b"\n" if len(lines) > 1 else b"")
clean = not (rc == 98 and not done)
if not nondumpable or (rc == 97 and not done) or (REQUIRE_RO_PATH and writable):
    verdict = "unsafe"
elif timed_out:
    verdict = "timeout"
elif not clean:
    verdict = "tampered"
elif rc == 0:
    verdict = "pass" if done else "incomplete"
else:
    verdict = "fail"
sys.stdout.write(out[:CAP].decode("utf-8", "replace"))
sys.stderr.write(err[:CAP].decode("utf-8", "replace"))
try:
    with open(os.path.join(sol, "_eval_child.log"), "rb") as f:
        child = f.read(CAP)
    if child:
        sys.stderr.write("\n[solution process output]\n" + child.decode("utf-8", "replace"))
except OSError:
    pass
for d in (work_parent, sol_parent):
    shutil.rmtree(d, ignore_errors=True)
result = {{"verdict": verdict, "checker_exit": rc, "completed": done, "timeout_s": TIMEOUT,
          "nondumpable": nondumpable and rc != 97, "syspath_writable": writable,
          "workdir_clean": clean, "pipes_held": held}}
sys.stderr.write("\n[eval-harness] " + json.dumps(result) + "\n")
sys.exit(0 if verdict == "pass" else (rc or 1))
"""

HARNESS_MARKER = "[eval-harness] "
_VERDICTS = ("pass", "fail", "incomplete", "timeout", "tampered", "unsafe")


def harness(solution: str, cfg: CodingConfig, *, require_readonly_path: bool = True) -> str:
    """Python code for /v1/exec (see _HARNESS). require_readonly_path=False is for tests on hosts
    whose interpreter directories are writable by the test user (never in the exec sandbox)."""
    dumps = json.dumps
    return _HARNESS.format(
        fixtures=dumps(dict(cfg.files), ensure_ascii=False, sort_keys=True),
        solution=dumps(solution, ensure_ascii=False),
        check=dumps(cfg.check, ensure_ascii=False),
        proxy=dumps(isolation.PROXY_SRC, ensure_ascii=False),
        server=dumps(isolation.SERVER_SRC, ensure_ascii=False),
        codec=dumps(isolation.CODEC_SRC, ensure_ascii=False),
        cap=OUTPUT_CAP * 4,
        timeout=check_timeout_s(cfg.wall_ms),
        require_ro_path=require_readonly_path,
    )


def split_harness_verdict(stderr: str) -> tuple[str, dict[str, Any] | None]:
    """Removes the harness's last "[eval-harness] {json}" stderr line; returns (rest, verdict)."""
    text = stderr.rstrip("\n")
    head, _, last = text.rpartition("\n")
    if not last.startswith(HARNESS_MARKER):
        return stderr, None
    try:
        verdict = json.loads(last[len(HARNESS_MARKER) :])
    except ValueError:
        return stderr, None
    if not isinstance(verdict, dict) or verdict.get("verdict") not in _VERDICTS:
        return stderr, None
    return head.rstrip("\n") + ("\n" if head else ""), verdict


def _cap(text: Any) -> tuple[str, bool]:
    if not isinstance(text, str):
        return "", False
    data = text.encode("utf-8")
    if len(data) <= OUTPUT_CAP:
        return text, False
    return data[:OUTPUT_CAP].decode("utf-8", "ignore"), True


def exec_info(res: GatewayResult) -> dict[str, Any]:
    """The checker run as recorded in the eval artifact (from the exec result body)."""
    body = res.body if isinstance(res.body, dict) else {}
    out, out_cut = _cap(body.get("stdout"))
    raw_err = body.get("stderr") if isinstance(body.get("stderr"), str) else ""
    rest, verdict = split_harness_verdict(raw_err)
    err, _ = _cap(rest)
    info: dict[str, Any] = {
        "call_id": res.call_id,
        "status": body.get("status", "unknown"),
        "stdout": out,
        "stderr": err,
        "stdout_truncated": out_cut or bool(body.get("stdout_truncated")),
        "wall_ms": body.get("wall_ms", 0),
        "queue_ms": body.get("queue_ms", 0),
        "image_digest": body.get("image_digest", ""),
    }
    if verdict is not None:
        info["harness"] = verdict
    exit_ = body.get("exit")
    if isinstance(exit_, dict):
        if exit_.get("signal"):
            info["signal"] = exit_["signal"]
        else:
            info["exit_code"] = exit_.get("code", 0)
    return info


def chat_text(res: GatewayResult) -> str:
    try:
        content = res.body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise WorkerFailure(
            "invalid_chat_response", f"no choices[0].message.content: {exc!r}"
        ) from exc
    if not isinstance(content, str):
        raise WorkerFailure("invalid_chat_response", "message.content is not a string")
    return content


async def run_coding(ctx: TaskContext, raw: Any) -> Result:
    cfg = parse_config(raw)
    gw = ctx.gateway
    try:
        if cfg.agent == "reference":
            solution, how = (cfg.reference or "").rstrip() + "\n", "reference"
        else:
            await ctx.progress("step_started", "solving", step_id=SOLVE_STEP)
            res = await asyncio.to_thread(
                lambda: gw.chat(
                    SOLVE_STEP, messages(cfg), model=cfg.model, max_tokens=cfg.max_tokens
                )
            )
            solution, how = extract_code(chat_text(res))
        await ctx.progress("step_started", "checking in the exec sandbox", step_id=CHECK_STEP)
        checked = await asyncio.to_thread(
            lambda: gw.exec(CHECK_STEP, harness(solution, cfg), wall_ms=cfg.wall_ms)
        )
    except GatewayError as exc:
        raise WorkerFailure(
            exc.code, str(exc), retryable=exc.status == 0 or exc.status >= 500
        ) from exc
    info = exec_info(checked)
    artifact = {
        "schema": ARTIFACT_SCHEMA,
        "task_id": cfg.task_id,
        "agent": cfg.agent,
        "extract": how,
        "solution_sha256": hashlib.sha256(solution.encode("utf-8")).hexdigest(),
        "solution_bytes": len(solution.encode("utf-8")),
        "exec": info,
    }
    (ctx.out_dir / "solution.py").write_text(solution, encoding="utf-8", newline="\n")
    (ctx.out_dir / "eval.json").write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n"
    )
    await ctx.register_artifact(SOLUTION_ARTIFACT, "solution.py", media_type="text/x-python")
    await ctx.register_artifact(EVAL_ARTIFACT, "eval.json", media_type="application/json")
    verdict = info.get("exit_code")
    summary = (
        f"{cfg.task_id}: checker {info['status']}, exit {verdict if verdict is not None else '-'}"
    )
    return Result(summary=summary, outputs=[EVAL_ARTIFACT, SOLUTION_ARTIFACT])
