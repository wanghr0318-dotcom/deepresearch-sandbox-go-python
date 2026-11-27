CASES = (
    ("gross", "gross", 23),
    ("refunded", "refunded", 6),
    ("net", "net", 17),
    ("consistent", "consistent", True),
)

CHECKER = """
import json
import runpy
import sys
cases = json.load(sys.stdin)
audit = runpy.run_path("audit.py")
report = runpy.run_path("records.py")
values = {"gross": audit["GROSS"], "refunded": audit["REFUNDED"], "net": report["NET"],
          "consistent": list(report["AUDIT"]) == [audit["GROSS"], audit["REFUNDED"]]}
print(json.dumps({name: values[key] == expected for name, key, expected in cases}))
"""
