"""Report each interrupted control link relative to an observed PostgreSQL lease renewal."""
import argparse
import json
from pathlib import Path

from collections import defaultdict
import gzip


def pid_samples(folder):
    samples, identities = defaultdict(list), defaultdict(set)
    with gzip.open(folder / "b-witness.jsonl.gz", "rt") as stream:
        for line in stream:
            obs = json.loads(line)
            if obs["event"] == "bound":
                identities[obs["uid"]].add(tuple(obs[k] for k in ("container","worker","start","boot")))
            elif obs["event"] == "pid":
                samples[obs["uid"]].append(obs)
    return samples, identities


def alive(samples, epoch):
    if epoch is None:
        return None
    before = next((s for s in reversed(samples) if s["at"] <= epoch), None)
    after = next((s for s in samples if s["at"] >= epoch), None)
    if before and after and after["at"]-before["at"] <= 2:
        if before["alive"] is not None and before["alive"] == after["alive"]:
            return before["alive"]
    return None


def read(p):
    return json.loads(p.read_bytes())


def summarize(path, witnesses):
    boundary = read(path/"boundary.json")
    ledger = read(path/"durable-writes.json")
    placement = read(path/"placement-score.json")
    w, = boundary["windows"]
    grant = w["confirmed_renewal"]
    old = boundary["old"]["attempt"]
    assert grant["owner"] == old and w["previous_grant"]["expiry"] < grant["expiry"]
    assert w["probe_before"]["rc"] == 0 and w["probe_cut"]["rc"] != 0
    anchor = grant["expiry"] - boundary["ttl"]
    restore = w["restore"]
    samples = [json.loads(line) for line in (path/"pg.jsonl").read_bytes().splitlines()]
    old_expiries = [s["expiry"] for s in samples if s.get("owner") == old and s.get("expiry") and s["pg_epoch"] <= restore]
    last = max([grant["expiry"], *old_expiries]) - boundary["ttl"]
    attempts = sorted(ledger["attempts"],key=lambda a:a["attempt_no"])
    assert attempts[0]["attempt_id"] == old
    revoked = next((a["revoked"] for a in ledger["access"] if a["attempt_id"]==old),None)
    successor = attempts[1]["created"] if len(attempts)>1 else None
    state, ids = witnesses
    uid = boundary["old"]["uid"]
    ident = next(iter(ids[uid])) if len(ids[uid]) == 1 else None
    worker = [e for e in state[uid] if ident and e["worker"]==ident[1] and e["start"]==ident[2]]
    cases = [c for c in placement["grading"] if c.get("cases") and c.get("source_intact") is True]
    event_attempts = [v["attempt_id"] for v in ledger["events"] if v["type"]=="attempt_created"]
    if [a["attempt_id"] for a in attempts]!=event_attempts or attempts[-1]["attempt_id"] != ledger["pg"]["owner"]:
        raise ValueError("attempt ledger and PG ownership disagree")
    new_nodes=[]
    for successor_row in attempts[1:]:
        names={v["witness"]["pod"] for v in placement["restore_witnesses"] if v["witness"]["attempt"]==successor_row["attempt_id"]}
        nodes={p["node"] for p in placement["pods"] if p["pod"] in names and p.get("node")}
        new_nodes.append(next(iter(nodes)) if len(nodes)==1 else None)
    environment=next((e for e in ledger.get("environments",[]) if e["env_id"]==attempts[0]["env_id"]),{})
    stopped, fenced = environment.get("stopped"), environment.get("fenced")
    if successor is None:
        retirement = None
    elif stopped is not None and stopped <= successor:
        retirement = "stopped"
    elif fenced is not None and fenced <= successor:
        retirement = "fenced"
    else:
        retirement = "unverified"
    old_events=[v for v in ledger["events"] if v["attempt_id"]==old and v["source"]=="worker" and v["type"]=="result"]
    overlap=None
    if len(attempts)==1 and ledger["pg"]["status"] in ("succeeded","failed","cancelled"):
        overlap=0
    if len(attempts)==2 and revoked is not None and successor is not None:
        overlap=round(max(0,revoked-successor),3)
    def since_renewal(epoch):
        return None if epoch is None else round(epoch-last,3)

    return {"round":path.name,"task":boundary["old"]["turn"],"uid":uid,"old":old,
            "target_seconds":w["seconds"],"original_anchor_pg":anchor,"last_confirmed_renewal_pg":last,
            "renewed_again":last>anchor+0.001,"cut_after_anchor_s":since_renewal(w["cut"]),
            "restore_after_last_renewal_s":since_renewal(restore),"actual_cut_s":round(w["elapsed"],3),
            "pg_clock_sample_rtt_ms":round(w["clock_sample_rtt"]*1000,2),"owner_changed":len(attempts)>1,
            "revoke_after_last_renewal_s":since_renewal(revoked),"successor_after_last_renewal_s":since_renewal(successor),
            "old_outcome":attempts[0].get("outcome_class"),"old_process_identity":ident,
            "alive_at_restore":alive(worker,restore),"alive_at_handoff":alive(worker,successor),
            "old_node":boundary["old"]["node"],"successor_nodes":new_nodes,
            "old_env_retirement_at_grant":retirement,"old_env_stopped_after_last_renewal_s":since_renewal(stopped),
            "old_env_fenced_after_last_renewal_s":since_renewal(fenced),
            "grant_overlap_s":overlap,"old_worker_results_after_revoke":sum(v["ts"]>revoked for v in old_events) if revoked else None,
            "old_checkpoints_after_revoke":sum(c["attempt_id"]==old and c["committed"]>revoked for c in ledger["checkpoints"]) if revoked else None,
            "dual_write_count":None,"harmful_false_takeover":None,
            "score":[sum(v is True for v in cases[0]["cases"].values()),len(cases[0]["cases"])] if len(cases)==1 else None}


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("folder",type=Path)
    folder=p.parse_args().folder
    w=pid_samples(folder)
    print(json.dumps([summarize(d.parent,w) for d in sorted(folder.glob("*/placement-score.json"))],indent=2))
