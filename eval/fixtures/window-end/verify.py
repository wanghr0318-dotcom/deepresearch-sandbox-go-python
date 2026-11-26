CASES = (
    ("three_windows", [[1, 2, 3, 4], 2], [1.5, 2.5, 3.5]),
    ("exact_window", [[4, 6], 2], [5.0]),
    ("width_one", [[-2, 8], 1], [-2.0, 8.0]),
    ("incomplete", [[1], 2], []),
)

CHECKER = """
import json
import runpy
import sys

solve = runpy.run_path("records.py")["moving_averages"]
cases = json.load(sys.stdin)
json.dump({name: solve(*data) == expected for name, data, expected in cases}, sys.stdout)
"""
