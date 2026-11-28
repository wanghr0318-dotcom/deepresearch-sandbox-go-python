"""Calculate per-container CPU core equivalents from cgroup-v2 cumulative deltas."""
import argparse
from collections import defaultdict
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path


def epoch(value):
    return datetime.fromisoformat(value).timestamp()


def task_windows(task):
    windows = []
    pending = {}
    for event in task["events"]:
        kind, name = event["type"], event["tool"]
        if kind == "tool_call":
            pending[name] = epoch(event["ts"])
        elif kind == "tool_result" and name in pending:
            windows.append(("tool:" + name, pending.pop(name), epoch(event["ts"])))
    for call in task["calls"]:
        if call["endpoint"] == "/v1/chat/completions":
            start = epoch(call["created_at"])
            attempt = call["tries"][0]
            windows.append(("model_rpc_estimate", start, start + attempt["latency_ms"] / 1000))
    return windows


def detail(rows, windows):
    totals = defaultdict(lambda: dict(seconds=0., cpu_usec=0, intervals=0))
    peaks = []
    for a, b in zip(rows, rows[1:]):
        dt = (b["mono_ns"] - a["mono_ns"]) / 1e9
        du = b["usage_usec"] - a["usage_usec"]
        if dt <= 0 or du < 0:
            raise ValueError("non-monotonic cgroup clock or counter")
        matches = [s for s, lo, hi in windows if lo <= a["epoch_ns"]/1e9 and b["epoch_ns"]/1e9 <= hi]
        intersects = any(lo < b["epoch_ns"]/1e9 and hi > a["epoch_ns"]/1e9 for _,lo,hi in windows)
        phase = matches[0] if len(matches) == 1 else ("boundary_or_overlap" if intersects else "outside_marked_windows")
        total = totals[phase]
        total["seconds"] += dt
        total["cpu_usec"] += du
        total["intervals"] += 1
        peaks.append((du/1e6/dt, dt))
    for entry in totals.values():
        entry["mean_cores"] = round(entry["cpu_usec"]/1e6/entry["seconds"], 6)
        entry["seconds"] = round(entry["seconds"], 6)
    a, b = rows[0], rows[-1]
    duration = (b["mono_ns"]-a["mono_ns"])/1e9
    highest = max(peaks, default=(None,None))
    return dict(observed_seconds=duration, samples=len(rows), usage_usec=b["usage_usec"]-a["usage_usec"],
                mean_cores=round((b["usage_usec"]-a["usage_usec"])/1e6/duration,6) if duration else None,
                highest_interval_cores=highest[0], highest_interval_seconds=highest[1],
                nr_throttled=b["nr_throttled"]-a["nr_throttled"],
                throttled_usec=b["throttled_usec"]-a["throttled_usec"], phases=totals)


def summarize(folder):
    data = json.loads((folder / "events.json").read_bytes())
    batch = json.loads((folder / "batch.json").read_bytes())
    pods = {v["uid"]: v for v in data["pods"]}
    by_env = {v["labels"]["agentbox.io/env"]: uid for uid,v in pods.items()}
    sessions, names, windows = {}, {}, {}
    for name,t in data["tasks"].items():
        if t["turn"] in batch["before"]:
            sessions[batch["before"][t["turn"]]["uid"]] = name
        windows[name] = task_windows(t)
        for key, run_id, calls in [("solve",t["turn"],t["calls"]),("grade",t["grade_task"],t["grade_calls"])]:
            for c in calls:
                if key == "grade":
                    for attempt in c["tries"]:
                        uid = by_env.get(attempt["env_id"])
                        if uid and "gateway-relay" in pods[uid]["requests"]:
                            names[uid] = (name, "grade_session")
                if c["endpoint"] == "/v1/exec":
                    for attempt in c["tries"]:
                        h = hashlib.sha256(chr(0).join((run_id,c["call_id"],str(attempt["try_no"]))).encode()).hexdigest()[:24]
                        names[by_env["exec-"+h]] = (name,key + "_exec")
    samples = defaultdict(list)
    markers = defaultdict(dict)
    for path in sorted(folder.glob("?-cpu.jsonl.gz")):
        with gzip.open(path, "rt") as stream:
            for line in stream:
                row=json.loads(line)
                if row["uid"] in pods:
                    samples[row["cid"]].append(row)
                    markers[row["uid"]].update(row["markers"])
    items=[]
    for rows in samples.values():
        head = rows[0]
        uid = head["uid"]
        name, kind = (sessions[uid], "solve_session") if uid in sessions else names.get(uid, (None,"grade_or_unmapped_session"))
        periods = list(windows.get(name, [])) if kind=="solve_session" else []
        if {"fault-ready","fault-reference"} <= markers[uid].keys():
            periods.append(("checkpoint_barrier", markers[uid]["fault-ready"]/1e9, markers[uid]["fault-reference"]/1e9))
        info=pods[uid]
        solve_cpu = None
        if kind=="solve_session":
            events=data["tasks"][name]["events"]
            first = next(e for e in events if e["type"]=="session_state")
            last = next(e for e in events if e["type"]=="turn_result")
            start, finish = epoch(first["ts"]), epoch(last["ts"])
            active = [r for r in rows if start <= r["epoch_ns"]/1e9 <= finish]
            if len(active)>1:
                solve_cpu=detail(active, [])["mean_cores"]
        items.append(dict(uid=uid,node=info["node"],pod=info["name"],task=name,kind=kind,container=head["container"],
                          request=info["requests"].get(head["container"]), solve_window_mean_cores=solve_cpu, **detail(rows,periods)))
    return sorted(items,key=lambda x:(x["task"] or "",x["kind"],x["uid"],x["container"]))


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder",type=Path)
    args=parser.parse_args()
    print(json.dumps(summarize(args.folder),indent=2))
