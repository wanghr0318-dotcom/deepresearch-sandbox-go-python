"""Verify the persisted exec_shell result and an independent file in a real KindExec Pod.

Use the arguments of scripts/run-cloud-agent-csv.py with the CSV fixture and --scripted.
"""
import hashlib
import json
from pathlib import Path
import runpy
import shlex

ROOT = Path(__file__).resolve().parents[2]
BASE = runpy.run_path(str(ROOT / "scripts/run-cloud-agent-csv.py"))
CAP = 1 << 20
EXCESS = 4096
ARTIFACT = "sum.txt"
EXPECTED = b"499500\n"
SHELL_MARKER = b"\x00agentbox:staged\n"


def upstream():
    original = (ROOT / "eval/fixtures/csv-first-row/repo/records.py").read_text()
    steps = BASE["csv_steps"](original)
    program = (
        "import pathlib,sys; "
        f"pathlib.Path({ARTIFACT!r}).write_text(str(sum(range(1000)))+'\\n'); "
        f"sys.stdout.write('x'*{CAP + EXCESS}); "
        "sys.stderr.write('calculation complete\\n')"
    )
    steps.append(("exec_shell", {"command": "python3 -I -c " + shlex.quote(program)}))
    return BASE["tool_upstream"](lambda body: steps)


def verify(directory):
    task = json.loads((directory / "solve-inspect.json").read_bytes())
    call, result = BASE["last_exec"](task, directory / "data")
    output = next(o for o in result["outputs"] if o["path"] == ARTIFACT)
    actual = BASE["read_blob"](directory / "data", output["sha256"])
    grade = json.loads((directory / "result.json").read_bytes())
    retained = len(result["stdout"].encode())
    if (result["status"] != "completed" or result["exit"]["code"] != 0
            or not result["workspace_staged"] or not result["stdout_truncated"]
            or result["stderr_truncated"] or result["stderr"] != "calculation complete\n"
            or result["stdout"] != "x" * (CAP-len(SHELL_MARKER))
            or actual != EXPECTED or output["size"] != len(EXPECTED)
            or grade["verdict"] != "solved"):
        raise ValueError(f"Unexpected output metadata or grade: {retained=}, {output=}, {actual=}")
    return {"layer": "real K3s exec_shell", "call_id": call["call_id"],
            "generated_stdout_bytes": CAP+EXCESS, "capture_limit_per_stream": CAP,
            "staging_marker_bytes": len(SHELL_MARKER), "retained_stdout_bytes": retained,
            "stdout_truncated": result["stdout_truncated"], "stderr_truncated": result["stderr_truncated"],
            "exit": result["exit"], "artifact_bytes": len(actual),
            "artifact_sha256": hashlib.sha256(actual).hexdigest(),
            "candidate_sha256": grade["candidate_sha256"], "checks": grade["cases"],
            "pods": grade["pods"], "remaining_resources": grade["remaining_resources"]}


if __name__ == "__main__":
    path = BASE["main"](upstream=upstream)
    (path / "output-proof.json").write_text(json.dumps(verify(path), indent=2) + "\n")
