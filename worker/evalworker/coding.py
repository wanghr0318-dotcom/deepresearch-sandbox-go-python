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
    if {"solution.py", "check.py"} & files.keys():
        raise WorkerFailure(
            "invalid_config", "config.eval.files cannot override solution.py/check.py"
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


# The exec harness. Passing needs a completion token that the checker prints after its last
# statement: the harness creates a random nonce and hands it to the checker through an inherited
# pipe (never argv or env); the checker's first line reads and closes that pipe before the solution
# is imported. A solution that exits early (os._exit(0), sys.exit(0), atexit tricks) therefore
# never produces the token, and the check is "incomplete", not passed. Limit: checker and solution
# share a process, so a solution that deliberately inspects the checker's memory could still forge
# the token; full isolation would need separate processes. The verdict is reported out of band as
# the last stderr line "[eval-harness] {json}"; checker output is capped before it is forwarded so
# that line is never cut by the exec output limit.
_HARNESS = r"""import json, os, secrets, subprocess, sys, tempfile
FILES = json.loads({files!r})
CHECK = json.loads({check!r})
CAP = {cap}
work = tempfile.mkdtemp(prefix="eval-")
for name, content in FILES.items():
    with open(os.path.join(work, name), "w", encoding="utf-8") as f:
        f.write(content)
nonce = secrets.token_hex(16)
token = "EVAL-CHECK-DONE:" + nonce
r, w = os.pipe()
os.write(w, nonce.encode())
os.close(w)
prelude = ("import os as _eval_os; _eval_done = (lambda t: lambda: "
           "print('EVAL-CHECK-DONE:' + t, flush=True))"
           "(_eval_os.read(%d, 64).decode()); _eval_os.close(%d)\n" % (r, r))
with open(os.path.join(work, "check.py"), "w", encoding="utf-8") as f:
    f.write(prelude + CHECK + "\n_eval_done()\n")
def cap(b):
    return (b or b"")[:CAP].decode("utf-8", "replace")
verdict, rc = "fail", 1
try:
    p = subprocess.run([sys.executable, "-E", "-s", "-B", "check.py"], cwd=work,
                       capture_output=True, timeout={timeout}, pass_fds=(r,))
    out, err = cap(p.stdout), cap(p.stderr)
    rc = p.returncode if p.returncode >= 0 else 128 - p.returncode
    lines = out.rstrip("\n").split("\n")
    done = lines[-1] == token
    if done:
        out = "\n".join(lines[:-1]) + ("\n" if len(lines) > 1 else "")
    verdict = "pass" if rc == 0 and done else ("incomplete" if rc == 0 else "fail")
except subprocess.TimeoutExpired as e:
    out, err, verdict = cap(e.stdout), cap(e.stderr), "timeout"
sys.stdout.write(out)
sys.stderr.write(err)
sys.stderr.write("\n[eval-harness] " + json.dumps({{"verdict": verdict, "checker_exit": rc,
                                                    "timeout_s": {timeout}}}) + "\n")
sys.exit(0 if verdict == "pass" else (rc or 1))
"""

HARNESS_MARKER = "[eval-harness] "


def harness(solution: str, cfg: CodingConfig) -> str:
    """Python code for /v1/exec: writes solution.py and the fixtures into a scratch directory, wraps
    the checker with the completion-token prelude/epilogue, runs it with a timeout and reports the
    verdict out of band (see _HARNESS)."""
    files = dict(cfg.files)
    files["solution.py"] = solution
    return _HARNESS.format(
        files=json.dumps(files, ensure_ascii=False, sort_keys=True),
        check=json.dumps(cfg.check, ensure_ascii=False),
        cap=OUTPUT_CAP * 4,
        timeout=check_timeout_s(cfg.wall_ms),
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
    if not isinstance(verdict, dict) or verdict.get("verdict") not in (
        "pass",
        "fail",
        "incomplete",
        "timeout",
    ):
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
