"""Regression coverage for the phase-3 runner's result contract."""

import json
from pathlib import Path
import runpy
from tempfile import TemporaryDirectory
import unittest


classify = runpy.run_path(str(Path(__file__).with_name("run-suite.py")))["classify"]


class ClassificationTest(unittest.TestCase):
    def verdict(self, report):
        with TemporaryDirectory() as work:
            path = Path(work) / "result.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            return classify(path, {"first", "second"})[0]

    def test_runner_infrastructure_abort_uses_report_category(self):
        self.assertEqual(self.verdict({"verdict": "infrastructure_abort"}), "infra_abort")

    def test_candidate_exception_is_scored_only_with_grade_evidence(self):
        report = {
            "verdict": "scored_unsolved",
            "reason": "candidate_exception",
            "grade_status": "completed",
            "grade_result": "sha256:fixture",
            "grade_exit": {"code": 1},
        }
        self.assertEqual(self.verdict(report), "scored_unsolved")
        del report["grade_result"]
        self.assertEqual(self.verdict(report), "other_abort")

    def test_solved_requires_all_expected_cases(self):
        report = {"verdict": "solved", "cases": {"first": True}}
        self.assertEqual(self.verdict(report), "other_abort")
        report["cases"]["other"] = True
        self.assertEqual(self.verdict(report), "other_abort")
        report["cases"] = {"first": True, "second": True}
        self.assertEqual(self.verdict(report), "solved")
