"""Verify the exported PG, Pod and application witnesses of a two-hop task."""
import argparse
from datetime import datetime
import json
from pathlib import Path


def verify(folder, source_node, target_node):
    def read(name):
        return json.loads((folder/name).read_bytes())

    def require(test, message):
        if not test:
            raise ValueError(message)

    chain = read("chain-summary.json")
    fault = read("fault.json")
    grants = read("grants.json")
    result = read("result.json")
    first, second = fault["first"], fault["second"]
    prior = first["record"]["prior"]
    listeners = chain["restores"]
    require(len(listeners)==2, "two application restore witnesses required")
    old = [prior["cpu-3"],second["record"]]
    require(old[0]["checkpoint"] != old[1]["checkpoint"], "same checkpoint used twice")
    keys = ("turn","checkpoint","messages","file_sha256","tool_ids")
    for before,after in zip(old,listeners):
        require(all(before[k]==after[k] for k in keys), "checkpoint, message, file or tool witness differs")
    require(prior["cpu-3"]["file_sha256"]!=first["record"]["uncommitted"]["file_sha256"], "candidate did not differ")
    require(old[1]["file_sha256"]==first["record"]["uncommitted"]["file_sha256"], "replayed update differs")

    turn = result["turn_id"]
    require(turn==fault["turn"] and all(s["turn"]==turn for s in [*old,*listeners]),"task changed")
    expected_attempts=[old[0]["attempt"],listeners[0]["attempt"],listeners[1]["attempt"]]
    require(old[1]["attempt"]==expected_attempts[1], "second checkpoint owner mismatch")
    require(len(set(expected_attempts))==3, "attempt reused")
    for ledger in (grants,chain["attempts"]):
        require([r["attempt_id"] for r in ledger]==expected_attempts and [r["attempt_no"] for r in ledger]==[1,2,3],
                "attempt identities or numbering differ between ledgers")
    for a in grants:
        require(a["state"]=="revoked" and a["revoked_at"],"final grant not revoked")
    for a,b in zip(grants,grants[1:]):
        require(datetime.fromisoformat(a["revoked_at"])<=datetime.fromisoformat(b["created_at"]),
                "successor existed before old grant was revoked")
    for stage,signature in [("before_stop",old[0]),("before_start",old[1]),("after_b_ready",old[1])]:
        snapshot=fault[stage]
        latest=max(snapshot["checkpoints"],key=lambda r:r["commit_seq"])
        require(snapshot["task"]["current_attempt_id"]==signature["attempt"],"wrong owner at "+stage)
        require(latest["checkpoint_id"]==signature["checkpoint"] and latest["attempt_id"]==signature["attempt"],
                "wrong last checkpoint at "+stage)

    by_name={p["name"]:p for p in chain["pods"]}
    pods=[by_name[name] for name in [first["pod"],listeners[0]["pod"],listeners[1]["pod"]]]
    require(pods[0]["uid"]==first["uid"] and pods[1]["uid"]==second["uid"], "marker and Kubernetes UID differ")
    require([p["node"] for p in pods]==[source_node,target_node,source_node] and source_node!=target_node,
            "wrong three-node placement sequence")
    require(len({p["uid"] for p in pods})==3 and all(p["deleted"] for p in pods),"session Pod reused or retained")
    require(len(fault["faults"])==2 and fault["faults"][1]["uid"]==pods[1]["uid"],"wrong second fault target")
    require(result["remaining_resources"]==[] and chain["remaining_resources"]==[],"resources left in test namespace")
    require(result["verdict"]=="solved" and len(chain["cases"])==4 and all(chain["cases"].values()),"grading failed")
    return {"task":turn,"attempts":expected_attempts,"pod_uids":[p["uid"] for p in pods],
            "nodes":[p["node"] for p in pods],"restored_checkpoints":[r["checkpoint"] for r in listeners],
            "grants_ordered":True,"four_cases_passed":True,"remaining_resources":[]}


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("folder",type=Path)
    p.add_argument("source_node")
    p.add_argument("target_node")
    a=p.parse_args()
    print(json.dumps(verify(a.folder,a.source_node,a.target_node),indent=2))
