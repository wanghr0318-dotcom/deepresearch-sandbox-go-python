"""Run frozen coding fixtures through the existing cloud acceptance runner and classify each invocation."""

import argparse
from collections import Counter
import json
from pathlib import Path
import runpy
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
RECOGNIZED = {"solved", "scored_unsolved", "parse_abort", "infra_abort", "other_abort"}


def classify(path, expected_cases):
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, UnicodeError, json.JSONDecodeError):
        return "other_abort", {}
    if not isinstance(report, dict):
        return "other_abort", {}
    verdict = report.get("verdict", report.get("failure_category"))
    if verdict == "infrastructure_abort":
        verdict = "infra_abort"
    if verdict not in RECOGNIZED:
        verdict = "other_abort"
    if verdict in {"solved", "scored_unsolved"}:
        cases = report.get("cases")
        if cases is None and verdict == "scored_unsolved" and report.get("reason") == "candidate_exception":
            exit_status = report.get("grade_exit")
            code = exit_status.get("code") if isinstance(exit_status, dict) else None
            if (report.get("grade_status") == "completed" and report.get("grade_result")
                    and type(code) is int and code != 0):
                return verdict, report
        if (not isinstance(cases, dict) or not cases or set(cases) != expected_cases
                or not all(isinstance(x, bool) for x in cases.values())):
            return "other_abort", report
        if (verdict == "solved") != all(cases.values()):
            return "other_abort", report
    return verdict, report


def run_suite(evidence, repeat, runner_args):
    manifest = json.loads((ROOT / "eval/phase3/tasks.json").read_text(encoding="utf-8"))
    fixtures = ROOT / "eval/fixtures"
    expected = {
        task["id"]: {case[0] for case in runpy.run_path(str(fixtures / task["id"] / "verify.py"))["CASES"]}
        for task in manifest["order"]
    }
    evidence.mkdir(mode=0o700, parents=True)
    runner = ROOT / "scripts/run-cloud-agent-csv.py"
    start = time.monotonic()
    counts = Counter({category: 0 for category in RECOGNIZED})
    with (evidence / "trajectories.jsonl").open("w", encoding="utf-8") as index:
        for attempt in range(1, repeat + 1):
            for task in manifest["order"]:
                name = f"{attempt:03d}-{task['id']}"
                directory = evidence / name
                fixture = fixtures / task["id"]
                argv = [sys.executable, "-B", str(runner), *runner_args, "--fixture", str(fixture),
                        "--evidence", str(directory)]
                with (evidence / f"{name}.log").open("w", encoding="utf-8") as log:
                    result = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT)
                verdict, report = classify(directory / "result.json", expected[task["id"]])
                row = {
                    "task_id": task["id"], "task_group": task["group"], "repetition": attempt,
                    "exit_code": result.returncode, "verdict": verdict, "evidence": name,
                    **{field: report[field] for field in (
                        "session_id", "turn_id", "candidate_sha256", "agent_status", "cases", "runtime_ms",
                        "model_calls", "parse_errors", "prompt_tokens", "completion_tokens",
                        "reason", "grade_result", "grade_status", "grade_exit",
                    ) if field in report},
                }
                index.write(json.dumps(row, ensure_ascii=False) + "\n")
                index.flush()
                counts[verdict] += 1
    elapsed = time.monotonic() - start
    scored = counts["solved"] + counts["scored_unsolved"]
    summary = {
        "submitted": sum(counts.values()), "scored": scored, "counts": dict(sorted(counts.items())),
        "wall_seconds_including_setup_and_grading": round(elapsed, 3),
        "scored_per_wall_hour_including_setup_and_grading": round(scored * 3600 / elapsed, 3),
    }
    (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("runner_args", nargs=argparse.REMAINDER, help="acceptance runner arguments after --")
    args = parser.parse_args()
    runner_args = args.runner_args[1:] if args.runner_args[:1] == ["--"] else args.runner_args
    if args.repeat < 1 or not runner_args or "--scripted" in runner_args:
        parser.error("provide a positive repeat and real-model runner arguments after --")
    run_suite(args.evidence.resolve(), args.repeat, runner_args)


if __name__ == "__main__":
    main()
