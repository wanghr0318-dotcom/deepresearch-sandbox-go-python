CASES = (
    ("mixed", [{"label": " TEA ", "quantity": 2}, {"label": "coffee", "quantity": 1}], {"beverages": 3}),
    ("canonical", [{"label": "pens", "quantity": 4}], {"stationery": 4}),
    ("fallback", [{"label": "paints", "quantity": 2}], {"other": 2}),
    ("blank", [], {}),
)

CHECKER = """
import json
import runpy
import sys

sys.path.insert(0, ".")
solve = runpy.run_path("records.py")["count_groups"]
cases = json.load(sys.stdin)
json.dump({name: solve(data) == expected for name, data, expected in cases}, sys.stdout)
"""
