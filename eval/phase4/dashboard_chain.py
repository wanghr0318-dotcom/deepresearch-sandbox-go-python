"""Project a multi-fault run onto separate, evidence-backed recovery intervals."""
import json
from pathlib import Path


def source_paths(root):
    return sorted(root.glob("p4*/*/chain-summary.json"))


def source_records(folder, read_events, admission):
    def load(name):
        return json.loads((folder / name).read_bytes())

    evidence = load("fault.json")
    summary = load("chain-summary.json")
    grade = load("result.json")
    yield "fault.json", {
        "turn": evidence["turn"],
        "faults": [{"at": event["at"]} for event in evidence["faults"]],
        **{key: {"node": evidence[key]["node"]} for key in ("first", "second")},
        **{key: {"at": evidence[key]["at"],
                 "task": {"current_attempt_id": evidence[key]["task"]["current_attempt_id"]}}
           for key in ("before_stop", "before_start", "after_b_ready") if key in evidence},
    }
    yield "chain-summary.json", {"attempts": [{k: a[k] for k in ("attempt_no", "attempt_id")}
                                              for a in summary["attempts"]]}
    yield "result.json", {k: grade[k] for k in ("turn_id", "fixture", "session_id", "verdict", "cases")}
    events = []
    for event in read_events(folder / "events.sse"):
        inner = event.get("internal") or {}
        data = event.get("data") or {}
        if inner.get("type") == "attempt_created" or event["type"] == "tool_result":
            events.append({k: event[k] for k in ("turn_id", "ts", "type")} |
                          {"internal": {k: inner.get(k) for k in ("attempt_id", "type")},
                           "data": {k: data.get(k) for k in ("ok", "tool")}})
    yield "events.sse", "\n".join("data: " + json.dumps(e) for e in events)
    gate = folder / "gateway.log"
    yield "gateway.log", "\n".join(m.group(0) for m in admission.finditer(gate.read_text(errors="replace"))) if gate.is_file() else ""


def task_row(folder: Path, read_events, admission, timestamp):
    records = dict(source_records(folder, read_events, admission))
    grade = records["result.json"]
    fault = records["fault.json"]
    turn = fault["turn"]
    attempts = sorted(records["chain-summary.json"]["attempts"], key=lambda a: a["attempt_no"])
    if grade["turn_id"] != turn or len(attempts) != 3 or len(fault["faults"]) != 2:
        raise ValueError("inconsistent chain attempt, fault or grade identities")
    cases = grade["cases"]
    row = {"node": fault["first"]["node"], "task": grade["fixture"], "verdict": grade["verdict"], "hops": []}
    if isinstance(cases, dict) and cases:
        row.update(total=len(cases), passed=sum(v is True for v in cases.values()))

    events = [json.loads(line[6:]) for line in records["events.sse"].splitlines()]
    grants = [(int(q)/1000, int(a)/1000) for session, q, a in admission.findall(records["gateway.log"])
              if session == grade["session_id"]]
    for i, node in enumerate(("first", "second")):
        start = fault["faults"][i]["at"]
        end = fault["faults"][i+1]["at"] if i+1 < len(fault["faults"]) else float("inf")
        successor = attempts[i+1]["attempt_id"]
        hop = {"fault": start, "node": fault[node]["node"]}
        for event in events:
            inner = event["internal"]
            at = timestamp(event["ts"])
            if event["turn_id"] != turn or inner["attempt_id"] != successor or not start <= at < end:
                continue
            if inner["type"] == "attempt_created":
                hop.setdefault("new_attempt", at-start)
            if event["type"] == "tool_result" and event["data"] == {"ok": True, "tool": "exec_shell"}:
                hop.setdefault("exec_ok", at-start)
        samples = [fault[k]["at"] for k in ("before_stop", "before_start", "after_b_ready")
                   if k in fault and fault[k]["task"]["current_attempt_id"] == successor and start <= fault[k]["at"] < end]
        if samples:
            hop["owner_observed"] = min(samples)-start
        matching = [(q, a) for q, a in grants if start <= q <= a < end]
        if len(matching) == 1:
            q, a = matching[0]
            hop.update(admit=a-start, wait=a-q)
            if "exec_ok" in hop and hop["exec_ok"] >= hop["admit"]:
                hop["recovery"] = hop["exec_ok"]-hop["admit"]
        row["hops"].append(hop)
    return turn, row
