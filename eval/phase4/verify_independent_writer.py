"""Cross-check B's HTTP attempts against A's ledger for one reconnect trial."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path


OPERATIONS = ("renew", "checkpoint", "file_head", "final_result")


def parse_time(value):
    value = value.replace("Z", "+00:00")
    value = re.sub(r"[.](\d+)", lambda m: "." + m[1][:6].ljust(6, "0"), value)
    return datetime.fromisoformat(value)


def read_rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def index_unique(rows, key):
    result = {}
    for row in rows:
        value = row[key]
        if not value or value in result:
            raise ValueError(f"empty or duplicate {key}: {value!r}")
        result[value] = row
    return result


def verify(writer, server):
    identities = {(r["b_host"], r["boot_id"], r["pid"], r["start_ticks"]) for r in writer}
    if len(identities) != 1:
        raise ValueError("writer identity changed")
    times = [r["monotonic_s"] for r in writer]
    if times != sorted(times):
        raise ValueError("writer monotonic clock out of order")
    lifecycle = [r for r in writer if r["event"] in ("start", "bound", "disconnected", "successor", "complete")]
    if [r["event"] for r in lifecycle] != ["start", "bound", "disconnected", "successor", "complete"]:
        raise ValueError("incomplete writer lifecycle")
    old, new = lifecycle[1]["old_attempt"], lifecycle[3]["new_attempt"]
    if not old or not new or old == new:
        raise ValueError("missing distinct old and new attempts")
    transitions = index_unique((r for r in server if r["event"] != "write"), "event")
    initial, last = transitions["old_owner"], transitions["last_old_renew"]
    expired, before, final = transitions["expired"], transitions["new_owner"], transitions["final"]
    if initial["owner"] != old or last["owner"] != old or expired["owner"] != old or before["owner"] != new:
        raise ValueError("adapter owner sequence inconsistent with B witness")
    expiry = parse_time(last["lease_expiry"])
    eval_time = parse_time(expired["pg_clock"])
    ttl = (expiry - parse_time(last["pg_clock"])).total_seconds()
    if eval_time < expiry or not 119 <= ttl <= 120:
        raise ValueError(f"real 120s old lease expiry unverified, observed {ttl}s remaining")
    if not eval_time <= parse_time(before["pg_clock"]) <= parse_time(final["pg_clock"]):
        raise ValueError("successor snapshots out of order on the database clock")
    state_fields = ("owner", "checkpoint", "head", "content_sha256", "result_ref", "lease_expiry")
    if any(not before[k] or before[k] != final[k] for k in state_fields):
        raise ValueError("new owner snapshot incomplete or changed")
    starts = index_unique((r for r in writer if r["event"] == "request_start"), "operation_id")
    ends = index_unique((r for r in writer if r["event"] == "request_end"), "operation_id")
    if starts.keys() != ends.keys():
        raise ValueError("unmatched writer request starts and ends")
    reached = index_unique((r for r in server if r["event"] == "write"), "operation_id")
    other = [r for k, r in reached.items() if k not in starts]
    if any(r["phase"] != "old" or r["accepted"] or parse_time(r["at"]) >= parse_time(before["at"]) for r in other):
        raise ValueError("unexpected external write during the measured round")

    counts = {op: dict(attempts=0, connection_failures=0, entry_rejections=0,
                       pg_rejections=0, accepted=0, unknown=0, reconnected_attempts=0) for op in OPERATIONS}
    for request_id, start in starts.items():
        end = ends[request_id]
        op = start["op"]
        if (end["op"], end["stage"], start["old_attempt"], end["old_attempt"]) != (
            op, start["stage"], old, old
        ) or end["monotonic_s"] < start["monotonic_s"]:
            raise ValueError(f"mismatched response for {request_id}")
        count = counts[op]
        count["attempts"] += 1
        if start["stage"] == "reconnected":
            if start["monotonic_s"] < lifecycle[3]["monotonic_s"]:
                raise ValueError("reconnect write precedes B's successor observation")
            count["reconnected_attempts"] += 1
        arrived = reached.get(request_id)
        if end["outcome"] == "connection_failure":
            if arrived or end["status"] is not None:
                raise ValueError(f"connection error with server receipt: {request_id}")
            count["connection_failures"] += 1
            continue
        if not arrived or end["status"] is None:
            count["unknown"] += 1
            continue
        if not parse_time(before["at"]) <= parse_time(arrived["at"]) <= parse_time(final["at"]):
            raise ValueError(f"write outside the successor snapshot window: {request_id}")
        if (arrived["operation"], arrived["layer"], arrived["code"], arrived["status"], arrived["accepted"],
            arrived["attempt"], arrived["phase"]) != (
                op, end["layer"], end["code"], end["status"], end["accepted"], old, "new"
        ):
            raise ValueError(f"adapter reply differs from B for {request_id}")
        if 200 <= end["status"] < 300:
            count["accepted"] += 1
        elif end["status"] == 409 and end["layer"] == "pg" and end["code"] == "stale_attempt":
            count["pg_rejections"] += 1
        elif end["status"] == 409 and end["layer"] == "gateway_workspace_access" and end["code"] == "not_current_attempt":
            count["entry_rejections"] += 1
        else:
            count["unknown"] += 1
    for count in counts.values():
        count["reconnect_denial_verified"] = (
            count["reconnected_attempts"] > 0 and count["accepted"] == count["unknown"] == 0
            and count["entry_rejections"] + count["pg_rejections"] == count["reconnected_attempts"]
        )
    beats = [r["monotonic_s"] for r in writer if r["event"] == "heartbeat"]
    return dict(old_attempt=old, new_attempt=new, operation_counts=counts,
                preflight_adapter_writes=[{k: r[k] for k in ("operation", "phase", "code", "accepted")} for r in other],
                successor_unchanged={k: final[k] for k in state_fields}, lease_remaining_after_last_renew_s=ttl,
                elapsed_last_renew_to_successor_s=(parse_time(before["pg_clock"]) - parse_time(last["pg_clock"])).total_seconds(),
                heartbeat_samples=len(beats),
                max_heartbeat_gap_s=max((b - a for a, b in zip(beats, beats[1:])), default=None))


def verify_transport(rows, nodes, writer, result):
    ordered = ["confirmed_old_writer_before_partition", "data_forward_closed",
               "start_signalled_with_data_path_closed",
               "confirmed_new_pg_owner_and_same_B_writer_before_restore",
               "restore_requested_after_independent_verification"]
    checks = [r for r in rows if r["event"] in ordered]
    if [r["event"] for r in checks] != ordered:
        raise ValueError("missing, duplicated or out-of-order transport milestones")
    times = [parse_time(r["utc"]) for r in checks]
    if times != sorted(times):
        raise ValueError("controller timestamps out of order")
    ssh_checks = [parse_time(r["utc"]) for r in rows if r["event"] == "B_management_SSH_reachable_during_cut"]
    if not any(times[1] <= t < times[-1] for t in ssh_checks):
        raise ValueError("missing B management SSH witness during the cut")
    before, after = checks[0], checks[3]
    for row, owner in ((before, "old_attempt"), (after, "new_attempt")):
        if row["pg"]["owner"] != result[owner]:
            raise ValueError("B and independently read PG owner disagree")
        witness = row["writer"]
        for field, source in (("host", "b_host"), ("boot_id", "boot_id"),
                              ("pid", "pid"), ("start_ticks", "start_ticks")):
            if str(witness[field]).lower() != str(writer[0][source]).lower():
                raise ValueError(f"independent B witness differs on {field}")
    if any(nodes["nodes"].get(host) != "True" for host in ("vm-0-6-ubuntu", "vm-0-13-ubuntu")):
        raise ValueError("final A/B Ready not verified")
    return dict(final_nodes=nodes["nodes"], restore_authorized_utc=checks[-1]["utc"],
                pg_successor_confirmed_utc=after["utc"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("writer", type=Path)
    parser.add_argument("adapter", type=Path)
    parser.add_argument("transport", type=Path)
    parser.add_argument("nodes", type=Path)
    args = parser.parse_args()
    writer = read_rows(args.writer)
    result = verify(writer, read_rows(args.adapter))
    result.update(verify_transport(read_rows(args.transport), json.loads(args.nodes.read_bytes()), writer, result))
    print(json.dumps(result, indent=2))
