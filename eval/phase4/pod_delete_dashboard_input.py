"""Create a dashboard batch projection from a single completed Pod-deletion trial."""
import argparse
import json
import os
import tempfile
from pathlib import Path


def project(source):
    fault = json.loads((source / "fault.json").read_bytes())
    result = json.loads((source / "result.json").read_bytes())
    turn = fault["turn"]
    if turn != result["turn_id"]:
        raise ValueError("Fault and grade refer to different tasks")
    checkpoint, = [c for c in fault["checkpoints"] if c["checkpoint_id"] == fault["checkpoint_id"]]
    old = checkpoint["attempt_id"]
    pods = {p["uid"]: p for p in result["pods"]}
    if (fault["uid"] == fault["resumed_uid"] or pods[fault["uid"]]["node"] != fault["node"]
            or pods[fault["resumed_uid"]]["node"] != fault["node"]):
        raise ValueError("Replacement on the original node is unconfirmed")
    events = []
    with (source / "events.sse").open(encoding="utf-8") as stream:
        for line in stream:
            if not line.startswith("data:"):
                continue
            event = json.loads(line[5:])
            if event.get("turn_id") != turn:
                continue
            inner = event.get("internal") or {}
            data = event.get("data") or {}
            if inner.get("type") != "attempt_created" and event["type"] != "tool_result":
                continue
            # Raw tool outputs may be large and may contain task content.
            event = {k: event[k] for k in ("ts", "turn_id", "type")}
            event["internal"] = {k: inner[k] for k in ("type", "attempt_id") if k in inner}
            event["data"] = {k: data[k] for k in ("tool", "ok") if k in data}
            events.append(event)
    created = [e["internal"]["attempt_id"] for e in events if e["internal"].get("type") == "attempt_created"]
    if len(created) != 2 or created[0] != old or created[1] == old:
        raise ValueError("Expected one original attempt and one successor")
    fixture = result["fixture"]
    if not fixture or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in fixture):
        raise ValueError("Invalid fixture directory")
    child = "01-" + fixture
    batch = {"submitted": [turn], "before": {turn: {"attempt": old, "node": fault["node"]}},
             "fault_requested_at": fault["delete_utc_epoch"], "tasks": [{"turn": turn, "directory": child}]}
    grade = {k: result[k] for k in ("turn_id", "session_id", "verdict", "cases") if k in result}
    return {"batch.json": json.dumps(batch), child+"/result.json": json.dumps(grade),
            child+"/events.sse": "".join("data: "+json.dumps(e)+"\n" for e in events)}


def write_projection(source, destination):
    files = project(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as scratch:
        ready = Path(scratch) / "ready"
        for name, data in files.items():
            path = ready / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data.encode())
        if not destination.exists():
            try:
                os.rename(ready, destination)
            except OSError:
                if not destination.is_dir():
                    raise
        if any((destination / k).read_bytes() != v.encode() for k, v in files.items()):
            raise ValueError(f"Conflicting projection already exists at {destination}")
    return destination / "batch.json"


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("source", type=Path)
    cli.add_argument("destination", type=Path)
    args = cli.parse_args()
    write_projection(args.source, args.destination)
