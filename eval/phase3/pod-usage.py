"""Join kubelet counters to the exact Pod UIDs recorded by the grading runner."""
import argparse
import json
from datetime import datetime
from pathlib import Path


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def summarize(samples, results):
    pods = {p["uid"]: p for result in results for p in result.get("pods", [])}
    cpu = {uid: {} for uid in pods}
    memory = {uid: {} for uid in pods}
    for frame in samples:
        for sample in frame["pods"]:
            uid = sample["uid"]
            if uid not in pods:
                continue
            if sample.get("cpu_ns") is not None and sample.get("cpu_at"):
                cpu[uid][timestamp(sample["cpu_at"])] = sample["cpu_ns"]
            if sample.get("working_set_bytes") is not None and sample.get("memory_at"):
                memory[uid][timestamp(sample["memory_at"])] = sample["working_set_bytes"]

    rows = []
    for uid, meta in pods.items():
        # A cumulative counter contains the early CPU usage even if the first read was late.
        counters = list(cpu[uid].values())
        points = sorted(memory[uid].items())
        observed_seconds = sum(b[0] - a[0] for a, b in zip(points, points[1:]))
        integral = sum((b[0]-a[0])*(a[1]+b[1])/2 for a,b in zip(points, points[1:]))
        rows.append({
            "uid": uid, "containers": meta["containers"],
            "ready_s": timestamp(meta["ready"])-timestamp(meta["created"]) if meta.get("ready") else None,
            "cpu_samples": len(counters), "memory_samples": len(points),
            "cpu_s_through_last_sample": max(counters)/1e9 if counters else None,
            "memory_interval_s": observed_seconds,
            "observed_memory_gib_s": integral/2**30 if len(points)>=2 else None,
        })
    return {"pods": rows, "expected_pods": len(rows),
            "pods_with_cpu": sum(bool(r["cpu_samples"]) for r in rows),
            "pods_with_memory_interval": sum(r["memory_samples"]>=2 for r in rows)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    parser.add_argument("results", type=Path, nargs="+")
    args = parser.parse_args()
    with args.samples.open(encoding="utf8") as stream:
        report = summarize((json.loads(line) for line in stream),
                           [json.loads(p.read_text(encoding="utf8")) for p in args.results])
    print(json.dumps(report, indent=2))
