"""Collect phase-four evidence into a durable, bounded experiment-only metric set."""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import dashboard_chain

ROOT = Path(os.environ.get("PHASE4_RUN_ROOT", "/var/tmp"))
STATE = Path(os.environ.get("PHASE4_STATE", "/var/lib/agentbox-phase4-monitor"))
admission = re.compile(r"recovery_admitted session_id=(\S+) queued_ms=(\d+) admitted_ms=(\d+)")


def read(path, default=None):
    if path.is_file():
        return json.loads(path.read_bytes())
    return default


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def atomic(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(data)
    temp.replace(path)


def pg_history(folder, batch):
    """Read database observations using the identity of the pre-fault attempt."""
    old = batch.get("before", {})
    deadlines, fences = {}, {}
    paired = folder.with_suffix(".jsonl")
    if paired.is_file():
        envs = {}
        for events in folder.glob("[0-9]*-*/events.sse"):
            for event in read_events(events):
                inner = event.get("internal", {})
                task = event.get("turn_id")
                if inner.get("type") == "attempt_created" and task in old and inner.get("attempt_id") == old[task]["attempt"]:
                    envs[inner["payload"]["env_id"]] = task
        for line in paired.read_text().splitlines():
            sample = json.loads(line)
            for record in sample.get("pg", []):
                task, owner, status, expiry, _ = record.split("|")
                if expiry and old.get(task, {}).get("attempt") == owner and status == "queued":
                    deadlines[task] = float(expiry)
            for record in sample.get("fences", []):
                env, at = record.split("|")
                task = envs.get(env)
                if task and at:
                    fences[task] = float(at)
    observations = folder/"lease-observations.log"
    if observations.is_file():
        for line in observations.read_text().splitlines():
            fields = line.split("|")
            if len(fields) == 8 and fields[0] in old and fields[1] == "1":
                task, _, _, outcome, status, expiry, fenced, _ = fields
                if outcome == "control_lost" and status == "queued" and expiry:
                    deadlines[task] = timestamp(expiry)
                if fenced:
                    fences[task] = timestamp(fenced)
    return deadlines, fences


def update_round(path, batch, rows, *, sample_pg=True):
    name = path.parent.relative_to(ROOT).as_posix()
    folder = path.parent
    fault = batch.get("fault_requested_at")
    partition = read(folder / "partition.json", {}) or {}
    fault = partition.get("start", fault)
    old = partition.get("original", {})
    grants = {}
    log = folder / "gateway.log"
    if log.exists():
        for sid, queued, admitted in admission.findall(log.read_text(errors="replace")):
            grants[sid] = int(queued) / 1000, int(admitted) / 1000
    infos = {x.get("turn"): x for x in batch.get("tasks", []) if x.get("turn")}
    for sub in folder.glob("[0-9]*-*"):
        result = read(sub / "result.json", {})
        turn = result.get("turn_id")
        if not turn:
            turn = next((e.get("turn_id") for e in read_events(sub / "events.sse")
                         if e.get("type") == "turn_created"), None)
        if turn in batch.get("submitted", []):
            infos.setdefault(turn, {}).update(directory=sub.name)
    table = rows.setdefault(name, {})
    deadlines, fences = pg_history(folder, batch)
    for turn in batch.get("submitted", []):
        info = infos.get(turn, {})
        row = table.setdefault(turn, {})
        row["task"] = info.get("directory", row.get("task", turn))
        before = batch.get("before", {}).get(turn, {})
        row["node"] = before.get("node", "unknown")
        row["verdict"] = info.get("verdict", row.get("verdict", "pending"))
        if not info.get("directory"):
            continue
        sub = folder / info["directory"]
        result = read(sub / "result.json", {})
        checks = result.get("cases") or info.get("cases")
        if isinstance(checks, dict) and checks:
            row.update(passed=sum(v is True for v in checks.values()), total=len(checks))
        row["verdict"] = result.get("verdict") or row["verdict"]
        if fault is None:
            continue
        row["fault"] = fault
        first = before.get("attempt")
        for event in read_events(sub / "events.sse"):
            inner = event.get("internal", {})
            if event.get("turn_id") != turn or not first or inner.get("attempt_id") in (None, first):
                continue
            at = timestamp(event["ts"])
            if inner.get("type") == "attempt_created":
                row.setdefault("new_attempt", at - fault)
            if event.get("type") == "tool_result" and event["data"].get("ok") and event["data"].get("tool") == "exec_shell":
                row.setdefault("exec_ok", at - fault)
        if result.get("session_id") in grants:
            q, a = grants[result["session_id"]]
            row.update(wait=a-q, admit=a-fault)
        if turn == old.get("turn"):
            samples = [x for x in partition.get("samples", []) if x.get("owner") == first and x.get("expires")]
            if samples:
                row["expiry"] = samples[-1]["expires"] - fault
            if partition.get("A_takeover"):
                row["owner_observed"] = partition["A_takeover"]["at"] - fault
        if turn in deadlines:
            row["expiry"] = deadlines[turn]-fault
        if turn in fences:
            row["fenced"] = fences[turn]-fault
        if "admit" in row and "exec_ok" in row:
            row["recovery"] = row["exec_ok"] - row["admit"]
    # Capture old lease deadlines while an active stop trial is running.
    # The task row is reused by the new owner, so join on the original attempt.
    ns = batch.get("namespace", "")
    if sample_pg and batch.get("fault_requested_at") and time.time()-fault < 600 and not (folder/"summary.json").exists() and re.fullmatch(r"agentbox-csv-[a-z0-9]+", ns):
        sql = """SELECT t.task_id, a.attempt_id, extract(epoch FROM t.lease_expires_at),
                        t.current_attempt_id, extract(epoch FROM e.fenced_at),
                        extract(epoch FROM clock_timestamp())
                 FROM tasks t JOIN attempts a ON a.task_id = t.task_id
                 LEFT JOIN environments e ON e.attempt_id = a.attempt_id"""
        cmd = ["k3s","kubectl","-n","agentbox-cvm-acceptance","exec","postgres","--","psql","-XAt","-U","agentbox","-d","csv_"+ns.split("-")[-1],"-c",sql]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
        if proc.returncode:
            print("PG sample unavailable for "+name, file=sys.stderr)
        else:
            journal = STATE/"pg"/name/"observations.jsonl"
            journal.parent.mkdir(parents=True, exist_ok=True)
            with journal.open("a") as stream:
                stream.write(json.dumps(proc.stdout.splitlines())+"\n")
    journal = STATE/"pg"/name/"observations.jsonl"
    if journal.exists() and fault is not None:
        for sample in journal.read_text().splitlines():
            for line in json.loads(sample):
                task, attempt, expiry, current, fenced, observed = line.split("|")
                if task in table and batch["before"].get(task, {}).get("attempt") == attempt:
                    if expiry and attempt == current:
                        table[task]["expiry"] = float(expiry)-fault
                    if fenced:
                        table[task]["fenced"] = float(fenced)-fault
                    if current and current != attempt:
                        table[task].setdefault("owner_observed", float(observed)-fault)


def read_events(path):
    if path.is_file():
        for line in path.read_text(errors="replace").splitlines():
            if line.startswith("data:"):
                try:
                    yield json.loads(line[5:])
                except json.JSONDecodeError:
                    continue


def render(rows):
    lines = []
    for run, tasks in sorted(rows.items()):
        for turn,row in sorted(tasks.items()):
            labels = dict(round=run, task=row["task"], turn=turn, node=row["node"], verdict=row["verdict"], item=run+" / "+row["task"])
            def metric(name, value, **extra):
                pairs = ",".join(k+"="+json.dumps(v, ensure_ascii=False) for k,v in (labels|extra).items())
                lines.append(f"agentbox_phase4_{name}{{{pairs}}} {value:.6f}")
            metric("task_info", 1)
            for index, interval in enumerate(row.get("hops", [row]), start=1):
                hop = {}
                if "hops" in row:
                    hop = dict(hop=str(index), node=interval["node"], item=labels["item"]+f" / hop {index}")
                for key in ("expiry","fenced","new_attempt","owner_observed","exec_ok","admit"):
                    if key in interval:
                        metric("event_offset_seconds",interval[key],event=key,**hop)
                        metric("event_timestamp_seconds",interval["fault"]+interval[key],event=key,**hop)
                for key in ("wait","recovery"):
                    if key in interval:
                        metric("duration_seconds",interval[key],stage=key,**hop)
            for key in ("passed","total"):
                if key in row:
                    metric("grade_cases",row[key],count=key)
    return "\n".join(lines)+"\n"


def main(*, sample_pg=True):
    STATE.mkdir(parents=True, exist_ok=True)
    cache = STATE/"rounds.json"
    rows = read(cache, {})
    paths = sorted(set(ROOT.glob("p4*/batch.json")) | set(ROOT.glob("p4*/*/batch.json")))
    for path in paths:
        try:
            update_round(path, read(path), rows, sample_pg=sample_pg)
        except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
            print(f"{path}: {exc}",file=sys.stderr)
    for marker in dashboard_chain.source_paths(ROOT):
        folder = marker.parent
        try:
            turn, row = dashboard_chain.task_row(folder, read_events, admission, timestamp)
            rows[folder.relative_to(ROOT).as_posix()] = {turn: row}
        except (KeyError, ValueError, OSError) as exc:
            print(f"{marker}: {exc}", file=sys.stderr)
    atomic(cache, json.dumps(rows,ensure_ascii=False))
    public = STATE/"public"
    public.mkdir(exist_ok=True)
    atomic(public/"metrics",render(rows))


if __name__ == "__main__":
    main()
