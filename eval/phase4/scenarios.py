"""Deterministic model/tool scenarios for the phase-4 recovery probe."""
import ast
from pathlib import Path
import runpy

HERE = Path(__file__).resolve().parent
BASE = runpy.run_path(str(HERE.parents[1] / "scripts/run-cloud-agent-csv.py"))
FIXTURES = {
    "csv-first-row": HERE.parent / "fixtures/csv-first-row",
    "catalog-alias": HERE.parent / "fixtures/catalog-alias",
    "reconciliation": HERE / "fixtures/reconciliation",
}
SOURCES = {name: {p.name: p.read_bytes().decode() for p in sorted((path / "repo").glob("*.py"))}
           for name, path in FIXTURES.items()}
TITLES = {path.joinpath("task.md").read_text(encoding="utf8").splitlines()[0]: name
          for name, path in FIXTURES.items()}


def identify(text):
    return next(name for title, name in TITLES.items() if title in text)


def read(path):
    return "read_file", {"path": path}


def write(path, content):
    return "write_file", {"path": path, "content": content}


def shell(command):
    return "exec_shell", {"command": command}


def assignments(content):
    tree = ast.parse(content.partition("\n")[2])
    return {stmt.targets[0].id: ast.literal_eval(stmt.value) for stmt in tree.body
            if isinstance(stmt, ast.Assign)}


def steps_for(body):
    messages = body["messages"]
    case = identify("\n".join(m["content"] for m in messages if m["role"] == "user"))
    history = [m["content"] for m in messages if m["role"] == "tool"]
    source = SOURCES[case]
    if case == "csv-first-row":
        return BASE["csv_steps"](source["records.py"])
    if case == "catalog-alias":
        patch = source["records.py"].replace('group_for(row["label"])', 'group_for(row["label"].strip().lower())')
        return [read("catalog.py"), read("records.py"), write("records.py", patch),
                shell("""python3 -c 'from records import count_groups; assert count_groups([{"label":" TEA ","quantity":2}]) == {"beverages":2}'""")]
    steps = [read("records.py"), read("sales.py"), read("returns_data.py")]
    if len(history) >= 3:
        sales = sum(assignments(history[1])["SALES"])
        refunds = sum(assignments(history[2])["RETURNS"])
        steps.extend([write("audit.py", f"GROSS = {sales}\nREFUNDED = {refunds}\n"), read("audit.py")])
    if len(history) >= 5:
        audit = assignments(history[4])
        gross, refunds = audit["GROSS"], audit["REFUNDED"]
        steps.extend([write("records.py", f"NET = {gross-refunds}\nAUDIT = ({gross}, {refunds})\n"),
                      shell("python3 -c 'from audit import GROSS, REFUNDED; from records import NET, AUDIT; assert AUDIT == (GROSS, REFUNDED) and NET == GROSS-REFUNDED'")])
    return steps


# A checkpoint immediately after the first file mutation, before the next dependent tool call.
WRITE_INDEX = {"csv-first-row": 1, "catalog-alias": 2, "reconciliation": 3}
CAPTURE_FILES = {"csv-first-row": (), "catalog-alias": ("catalog.py",),
                 "reconciliation": ("sales.py", "returns_data.py", "audit.py")}
