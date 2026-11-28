"""Project a completed control-partition trial into the existing dashboard input format."""
import argparse
import json
import os
import tempfile
from pathlib import Path


def load(path):
    return json.loads(path.read_bytes())


def project(folder):
    boundary = load(folder / "boundary.json")
    writes = load(folder / "durable-writes.json")
    old = boundary["old"]
    turn, attempt = old["turn"], old["attempt"]
    old_rows = [a for a in writes["attempts"] if a["attempt_id"] == attempt]
    if len(old_rows) != 1 or not boundary.get("finished"):
        raise ValueError(f"Missing original attempt or final evidence in {folder}")
    batch = load(folder / "batch.json")
    if batch.get("before", {}).get(turn, {}).get("attempt") != attempt:
        raise ValueError(f"Batch and boundary identities disagree in {folder}")
    start = boundary["windows"][0]["cut"]
    samples = [json.loads(line) for line in (folder / "pg.jsonl").read_bytes().splitlines() if line.strip()]
    samples.extend([boundary.get("before", {}), boundary.get("pg_end", {}), writes.get("pg", {})])
    for window in boundary["windows"]:
        samples.extend(window.get(k, {}) for k in ("before", "after", "interruption_gap_pg"))
    samples = sorted((s for s in samples if s.get("owner") and s.get("pg_epoch")), key=lambda s: s["pg_epoch"])
    deadlines = [s for s in samples if s["owner"] == attempt and s.get("expiry") is not None]
    data = {"start": start, "original": {"turn": turn}, "samples": []}
    if deadlines:
        data["samples"].append({"owner": attempt, "expires": deadlines[-1]["expiry"]})
    successors = {a["attempt_id"] for a in writes["attempts"] if a["attempt_no"] > old_rows[0]["attempt_no"]}
    first_observed = next((s for s in samples if s["owner"] in successors and s["pg_epoch"] >= start), None)
    if first_observed:
        data["A_takeover"] = {"at": first_observed["pg_epoch"]}
    fences = [e for e in writes["environments"] if e.get("env_id") == old_rows[0]["env_id"] and e.get("fenced") is not None]
    if len(fences) > 1:
        raise ValueError(f"Ambiguous environment fence in {folder}")
    fence_record = None
    if fences:
        env = fences[0]
        fence_record = {"pg": [], "fences": [f"{env['env_id']}|{env['fenced']}"]}
    return data, fence_record


def write_new_evidence(target, data):
    if target.exists():
        if load(target) != data:
            raise ValueError(f"Different dashboard input already exists at {target}")
        return
    # Publish a complete file without overwriting existing experimental evidence.
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False, mode="w", encoding="utf-8") as stream:
        temp = Path(stream.name)
        json.dump(data, stream, separators=(",", ":"))
        stream.write("\n")
    try:
        try:
            os.link(temp, target)
        except FileExistsError:
            if load(target) != data:
                raise
    finally:
        temp.unlink()


def write_projection(folder):
    partition, fence = project(folder)
    if fence:
        write_new_evidence(folder.with_suffix(".jsonl"), fence)
    write_new_evidence(folder / "partition.json", partition)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rounds", nargs="+", type=Path, help="completed boundary trial directories")
    args = parser.parse_args()
    for folder in args.rounds:
        write_projection(folder)
        print(folder / "partition.json")