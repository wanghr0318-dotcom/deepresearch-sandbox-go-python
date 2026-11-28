"""Archive only the source fields used by the phase-four dashboard."""
import argparse
import gzip
import json
from pathlib import Path

import dashboard_chain
import dashboard_metrics
from dashboard_metrics import admission, read, read_events


def pick(obj, *keys):
    return {key: obj[key] for key in keys if key in obj}


def records(root, journal_root, paths):
    for path in sorted(paths):
        folder = path.parent
        run = folder.relative_to(root).as_posix()
        if path.name == "chain-summary.json":
            for name, value in dashboard_chain.source_records(folder, read_events, admission):
                yield run+"/"+name, value if isinstance(value, str) else json.dumps(value)
            continue
        batch = read(path)
        batch_info = pick(batch, "namespace", "submitted", "fault_requested_at")
        batch_info["before"] = {k: pick(v, "attempt", "node", "pod") for k, v in batch.get("before", {}).items()}
        batch_info["tasks"] = [pick(v, "turn", "directory", "verdict", "cases") for v in batch.get("tasks", [])]
        yield run+"/batch.json", json.dumps(batch_info)
        for child in sorted(folder.glob("[0-9]*-*")):
            prefix = run+"/"+child.name+"/"
            result = read(child/"result.json")
            if result:
                yield prefix+"result.json", json.dumps(pick(result, "turn_id", "cases", "verdict", "session_id"))
            events = []
            for event in read_events(child/"events.sse"):
                typ = event.get("type")
                inner = event.get("internal", {})
                if typ not in ("turn_created", "tool_result") and inner.get("type") != "attempt_created":
                    continue
                e = pick(event, "type", "ts", "turn_id")
                e["internal"] = pick(inner, "type", "attempt_id")
                if inner.get("type") == "attempt_created":
                    e["internal"]["payload"] = pick(inner.get("payload", {}), "env_id")
                e["data"] = pick(event.get("data", {}), "tool", "ok")
                events.append("data: "+json.dumps(e))
            if events:
                yield prefix+"events.sse", "\n".join(events)
        log = folder/"gateway.log"
        if log.exists():
            yield run+"/gateway.log", "\n".join(line for line in log.read_text(errors="replace").splitlines() if admission.search(line))
        partition = read(folder/"partition.json")
        if partition:
            fields = pick(partition,"start")
            fields["original"] = pick(partition.get("original", {}),"turn")
            fields["samples"] = [pick(s,"owner","expires") for s in partition.get("samples",[])]
            if partition.get("A_takeover"):
                fields["A_takeover"] = pick(partition["A_takeover"],"at")
            yield run+"/partition.json", json.dumps(fields)
        paired = folder.with_suffix(".jsonl")
        if paired.is_file():
            lines = [json.dumps(pick(json.loads(line),"pg","fences")) for line in paired.read_text().splitlines()]
            yield run+".jsonl", "\n".join(lines)
        log = folder/"lease-observations.log"
        if log.is_file():
            yield run+"/lease-observations.log", log.read_text()
        journal = journal_root/run/"observations.jsonl"
        if journal.is_file():
            yield "pg/"+run+"/observations.jsonl", journal.read_text()


def archive(root, journal, target, paths):
    tmp = target.with_suffix(".tmp")
    with gzip.open(tmp,"wt",encoding="utf-8") as stream:
        for path,data in records(root,journal,paths):
            stream.write(json.dumps([path,data], ensure_ascii=True)+"\n")
    tmp.replace(target)


def restore(source, target):
    target = target.resolve()
    with gzip.open(source,"rt",encoding="utf-8") as stream:
        for line in stream:
            name, data = json.loads(line)
            path = (target/name).resolve()
            if target not in path.parents:
                raise ValueError("Invalid archive path")
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(data,encoding="utf-8")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    sub = cli.add_subparsers(dest="command",required=True)
    save = sub.add_parser("archive")
    save.add_argument("run_root",type=Path)
    save.add_argument("journal_root",type=Path)
    save.add_argument("output",type=Path)
    save.add_argument("--per-round",action="store_true")
    load = sub.add_parser("restore")
    load.add_argument("source",type=Path)
    load.add_argument("output",type=Path)
    replay = sub.add_parser("replay")
    replay.add_argument("source", type=Path)
    replay.add_argument("output", type=Path, help="New directory for clean offline reconstruction")
    replay.add_argument("--html-template", type=Path)
    args = cli.parse_args()
    if args.command == "archive":
        paths = sorted(set(args.run_root.glob("p4*/batch.json")) | set(args.run_root.glob("p4*/*/batch.json")))
        paths.extend(dashboard_chain.source_paths(args.run_root))
        if args.per_round:
            args.output.mkdir(parents=True,exist_ok=True)
            for path in paths:
                name = "__".join(path.parent.relative_to(args.run_root).parts)
                archive(args.run_root,args.journal_root,args.output/(name+".jsonl.gz"),[path])
        else:
            archive(args.run_root,args.journal_root,args.output,paths)
    elif args.command == "restore":
        restore(args.source,args.output)
    else:
        args.output.mkdir(parents=True, exist_ok=False)
        inputs = args.output / "inputs"
        state = args.output / "state"
        restore(args.source, inputs)
        state.mkdir()
        if (inputs / "pg").is_dir():
            (inputs / "pg").rename(state / "pg")
        dashboard_metrics.ROOT = inputs
        dashboard_metrics.STATE = state
        dashboard_metrics.main(sample_pg=False)
        if args.html_template:
            template = args.html_template.read_text(encoding="utf-8")
            rows = (state / "rounds.json").read_text(encoding="utf-8")
            script = '<script type="application/json" id="offline-rows">' + rows.replace("<", r"\u003c") + '</script>'
            if "<script>" not in template:
                raise ValueError("missing script insertion point in HTML template")
            (args.output / "task-dashboard.html").write_text(template.replace("<script>", script+"<script>", 1), encoding="utf-8")
