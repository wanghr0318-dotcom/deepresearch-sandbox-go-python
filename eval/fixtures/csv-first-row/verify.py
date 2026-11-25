"""Exercise the trusted checker against three independent workspaces."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory


CASES = (
    ("single_row", "name,city\nAda,London\n", [{"name": "Ada", "city": "London"}]),
    (
        "ordered_rows",
        "name,city\nAda,London\nGrace,New York\n",
        [{"name": "Ada", "city": "London"}, {"name": "Grace", "city": "New York"}],
    ),
    ("header_only", "name,city\n", []),
    ("quoted_comma", 'name,city\nAda,"London, UK"\n', [{"name": "Ada", "city": "London, UK"}]),
)

CHECKER = """
import json
import runpy
import sys

read_records = runpy.run_path("records.py")["read_records"]
cases = json.load(sys.stdin)
json.dump(
    {name: read_records(source) == expected for name, source, expected in cases},
    sys.stdout,
)
"""


def verify_fixture(fixture: Path, patch: Path | None) -> dict[str, bool]:
    with TemporaryDirectory(prefix="agentbox-csv-") as temp:
        workspace = Path(temp) / "repo"
        shutil.copytree(fixture / "repo", workspace)
        if patch is not None:
            subprocess.run(["git", "apply", str(patch)], cwd=workspace, check=True)
        result = subprocess.run(
            [sys.executable, "-I", "-c", CHECKER],
            cwd=workspace,
            input=json.dumps(CASES),
            text=True,
            capture_output=True,
            check=True,
            timeout=10,
        )
        return json.loads(result.stdout)


def main() -> int:
    fixture = Path(__file__).resolve().parent
    scenarios = (
        ("baseline", None, (False, False, True, False)),
        ("correct", fixture / "patches" / "correct.patch", (True, True, True, True)),
        ("incorrect", fixture / "patches" / "incorrect.patch", (True, True, True, False)),
    )
    failed = False
    for name, patch, expected in scenarios:
        results = verify_fixture(fixture, patch)
        matches = results == dict(zip((case[0] for case in CASES), expected))
        print(json.dumps({"scenario": name, "passed": all(results.values()), "cases": results}))
        failed |= not matches
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
