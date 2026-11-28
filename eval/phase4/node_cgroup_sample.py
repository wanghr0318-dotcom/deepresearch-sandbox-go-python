"""Sample cgroup v2 counters for the Kubernetes experiment containers on this node."""
import argparse
import json
from pathlib import Path
import time

TASKS = Path("/run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io")
CGROUP = Path("/sys/fs/cgroup")


def container(task):
    config = json.loads((task / "config.json").read_bytes())
    a = config.get("annotations", {})
    ns = a.get("io.kubernetes.cri.sandbox-namespace", "")
    if not ns.startswith("agentbox-csv-") or a.get("io.kubernetes.cri.container-type") != "container":
        return None
    pid = int((task / "init.pid").read_text())
    relative = next(x[3:] for x in Path(f"/proc/{pid}/cgroup").read_text().splitlines() if x.startswith("0::"))
    if task.name not in relative:
        return None
    identity = {"cid": task.name, "container": a["io.kubernetes.cri.container-name"], "namespace": ns,
                "pod": a["io.kubernetes.cri.sandbox-name"], "uid": a["io.kubernetes.cri.sandbox-uid"]}
    return identity, CGROUP / relative.lstrip("/") / "cpu.stat",Path(f"/proc/{pid}/root/tmp")


def run(output, seconds, interval):
    known = {}
    deadline = time.monotonic() + seconds
    with output.open("x", buffering=1) as out:
        while time.monotonic() < deadline and not output.with_suffix(".stop").exists():
            children = list(TASKS.iterdir())
            known = {p:v for p,v in known.items() if p in children}
            for p in children:
                try:
                    if p not in known:
                        known[p] = container(p)
                    if not known[p]:
                        continue
                    identity, stat, tmp = known[p]
                    data = dict(line.split() for line in stat.read_text().splitlines())
                    markers = {}
                    if identity["container"] == "sandbox":
                        for n in ("fault-ready","fault-reference"):
                            try:
                                markers[n]= (tmp / n).stat().st_mtime_ns
                            except FileNotFoundError:
                                pass
                    row = {**identity,"epoch_ns":time.time_ns(),"mono_ns":time.monotonic_ns(),"markers":markers}
                    for k in ("usage_usec","nr_throttled","throttled_usec"):
                        row[k] = int(data[k]) if k in data else None
                    out.write(json.dumps(row,separators=(",",":"))+"\n")
                except (FileNotFoundError, ProcessLookupError, KeyError, ValueError, StopIteration):
                    continue
            time.sleep(interval)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("output",type=Path)
    p.add_argument("--seconds",type=int,default=330)
    p.add_argument("--interval",type=float,default=.075)
    args=p.parse_args()
    run(args.output,args.seconds,args.interval)
