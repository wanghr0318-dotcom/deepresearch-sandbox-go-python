CASES = (
    ("mixed_units", [{"sensor": "a", "value": 1200, "unit": "mV"}, {"sensor": "a", "value": 1, "unit": "V"}], {"a": 2.2}),
    ("signed", [{"sensor": "a", "value": -250, "unit": "mV"}, {"sensor": "b", "value": 1, "unit": "V"}], {"a": -0.25, "b": 1}),
    ("unknown", [{"sensor": "a", "value": 2, "unit": "mA"}], "ValueError"),
    ("empty", [], {}),
)

CHECKER = """
import json
import runpy
import sys

sys.path.insert(0, ".")
solve = runpy.run_path("records.py")["sensor_totals"]

def check(data, expected):
    try:
        return solve(data) == expected
    except ValueError:
        return expected == "ValueError"

cases = json.load(sys.stdin)
json.dump({name: check(data, expected) for name, data, expected in cases}, sys.stdout)
"""
