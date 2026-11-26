CASES = (
    ("positive_tie", ["1.00", "0.005"], "1.01"),
    ("negative_tie", ["-1.005"], "-1.01"),
    ("round_after_sum", ["0.004", "0.004"], "0.01"),
    ("empty", [], "0.00"),
)

CHECKER = """
import json
import runpy
import sys

solve = runpy.run_path("records.py")["invoice_total"]
cases = json.load(sys.stdin)
json.dump({name: solve(data) == expected for name, data, expected in cases}, sys.stdout)
"""
