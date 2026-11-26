"""Sample one K3s node for a bounded benchmark window; JSONL times are UTC."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from time import monotonic, sleep


def kubectl(*args):
    return json.loads(subprocess.check_output(["sudo", "-n", "k3s", "kubectl", *args], timeout=10))


def sample(node, namespace):
    scope = ["-A"] if namespace == "*" else ["-n", namespace]
    listing = kubectl("get", "pods", *scope, "-l", "agentbox.io/managed=true", "-o", "json")
    metrics = kubectl("get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary")
    by_uid = {p["podRef"]["uid"]: p for p in metrics["pods"]}
    pods = []
    for p in listing["items"]:
        if p["spec"].get("nodeName", node) != node:
            continue
        meta = p["metadata"]
        stat = by_uid.get(meta["uid"], {})
        cond = {c["type"]:c["lastTransitionTime"] for c in p["status"].get("conditions", [])
                if c["status"] == "True" and c["type"] in ("PodScheduled", "Ready")}
        cpu, mem = stat.get("cpu", {}), stat.get("memory", {})
        pods.append({"name":meta["name"], "uid":meta["uid"], "namespace":meta["namespace"], "created":meta["creationTimestamp"],
                     "containers":[c["name"] for c in p["spec"]["containers"]], "conditions":cond,
                     "cpu_ns":cpu.get("usageCoreNanoSeconds"), "cpu_at":cpu.get("time"),
                     "working_set_bytes":mem.get("workingSetBytes"), "memory_at":mem.get("time")})
    return {"time":datetime.now(timezone.utc).isoformat(), "node":node, "namespace":namespace, "pods":pods}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("node")
    parser.add_argument("namespace")
    parser.add_argument("report", type=Path)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--interval", type=float, default=2)
    args = parser.parse_args()
    if args.seconds <= 0 or args.interval <= 0:
        parser.error("seconds and interval must be positive")
    with args.report.open("x", encoding="utf-8") as output:
        deadline = monotonic() + args.seconds
        while monotonic() < deadline:
            print(json.dumps(sample(args.node, args.namespace)), file=output, flush=True)
            sleep(min(args.interval, max(0, deadline-monotonic())))
