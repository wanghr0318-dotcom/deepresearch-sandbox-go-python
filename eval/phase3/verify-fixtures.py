"""Validate the trusted phase-3 fixture baselines and reference patches."""

import json
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

EVAL = Path(__file__).resolve().parents[1]


def main():
    manifest = json.loads((EVAL / "phase3/tasks.json").read_text(encoding="utf-8"))
    failed = False
    for task in manifest["order"]:
        fixture = EVAL / "fixtures" / task["id"]
        oracle = runpy.run_path(str(fixture / "verify.py"))
        for scenario in ("baseline", "reference"):
            with TemporaryDirectory(prefix="agentbox-fixture-") as work:
                repo = Path(work) / "repo"
                shutil.copytree(fixture / "repo", repo)
                if scenario == "reference":
                    subprocess.run(
                        ["git", "apply", str(fixture / "patches/correct.patch")],
                        cwd=repo,
                        check=True,
                    )
                result = subprocess.run(
                    [sys.executable, "-I", "-B", "-c", oracle["CHECKER"]],
                    cwd=repo,
                    input=json.dumps(oracle["CASES"]),
                    text=True,
                    capture_output=True,
                    check=True,
                    timeout=10,
                )
            cases = json.loads(result.stdout)
            expected_names = {case[0] for case in oracle["CASES"]}
            if set(cases) != expected_names or not all(isinstance(v, bool) for v in cases.values()):
                raise ValueError(f"invalid checker result for {task['id']}")
            passed = all(cases.values())
            print(json.dumps({**task, "scenario": scenario, "passed": passed, "cases": cases}))
            failed |= passed != (scenario == "reference")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
