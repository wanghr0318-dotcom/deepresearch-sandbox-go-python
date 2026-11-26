ROWS = [{"time": 7, "id": "b"}, {"time": 9, "id": "a"}, {"time": 7, "id": "c"}, {"time": 7, "id": "a"}]
CASES = (
    ("after_first_tie", [ROWS, [7, "a"], 2], [ROWS[0], ROWS[2]]),
    ("after_late_tie", [ROWS, [7, "b"], 2], [ROWS[2], ROWS[1]]),
    ("first_page", [ROWS, None, 1], [ROWS[3]]),
    ("after_last", [ROWS, [9, "a"], 3], []),
)

CHECKER = """
import json
import runpy
import sys

solve = runpy.run_path("records.py")["next_page"]
cases = json.load(sys.stdin)
json.dump({name: solve(*data) == expected for name, data, expected in cases}, sys.stdout)
"""
