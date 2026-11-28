"""One Gateway and independent sessions sharing a single B-to-A fault injection."""
import argparse
import concurrent.futures
from copy import copy
from contextlib import ExitStack
import json
from datetime import datetime
from pathlib import Path
import queue
import os
import subprocess
import sys
import time
import threading

from scenarios import BASE, CAPTURE_FILES, FIXTURES, SOURCES, TITLES, WRITE_INDEX, steps_for


def worker_command(real_model=False):
    setup = f"""
import hashlib, json
from pathlib import Path
from chatagent.loop import Agent
titles = {TITLES!r}
sources = {SOURCES!r}
indices = {WRITE_INDEX!r}
def scenario(ctx):
    return next(name for title, name in titles.items() if title in ctx.config["text"])
def witness(ctx, messages, checkpoint):
    serialized = json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return {{"turn": ctx.task_id, "attempt": ctx.attempt_id, "checkpoint": checkpoint,
            "messages_sha256": hashlib.sha256(serialized).hexdigest()}}
"""
    if real_model:
        setup += """
from chatagent.events import Emitter
original_tool_result = Emitter.tool_result
async def note_write(emitter, step, event_id, name, result, subtopic_id, args=None):
    await original_tool_result(emitter, step, event_id, name, result, subtopic_id, args)
    emitter.m1_write_event = event_id if (name == "write_file" and result.ok
        and (args or {}).get("path") == "records.py") else None
Emitter.tool_result = note_write
def injection_point(agent):
    return getattr(agent.emit, "m1_write_event", None)
"""
    else:
        setup += """
def injection_point(agent):
    messages = agent.state.messages
    expected = f"cpu-{indices[scenario(agent.ctx)]}"
    return expected if messages and messages[-1].get("tool_call_id") == expected else None
"""
    setup += """
original_save = Agent.save
async def save_and_hold(agent, step):
    cp = await original_save(agent, step)
    state, ctx = agent.state, agent.ctx
    call_id = injection_point(agent)
    if ctx.resume is None and call_id:
        marker = dict(witness(ctx, state.messages, cp), tool_event_id=call_id)
        path = Path("/tmp/fault-ready")
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(marker))
        tmp.replace(path)
        while not Path("/tmp/fault-reference").exists():
            await asyncio.sleep(.15)
    return cp
Agent.save = save_and_hold
original_run = Agent.run
async def record_resume(agent, *args, **kwargs):
    ctx = agent.ctx
    if ctx.resume is not None:
        import socket
        data = witness(ctx, agent.state.messages, ctx.resume.checkpoint_id)
        data["pod"] = socket.gethostname()
        await ctx.progress("phase4_restored", "recovery witness", data=data)
    return await original_run(agent, *args, **kwargs)
Agent.run = record_resume
"""
    return BASE["seed_command"]({}, setup, "sources[scenario(ctx)]")


def collect_markers(kube, marker, turns, expected_node, deadline, seen):
    while len(seen) < len(turns):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{marker}: missing {sorted(turns - seen.keys())}")
        pods = json.loads(subprocess.check_output(kube + ["get", "pods", "-o", "json"], timeout=15))
        for pod in pods["items"]:
            meta, spec = pod["metadata"], pod["spec"]
            if (expected_node and spec.get("nodeName") != expected_node) or pod["status"]["phase"] != "Running":
                continue
            if not any(c["name"] == "gateway-relay" for c in spec["containers"]):
                continue
            get = subprocess.run(kube + ["exec", meta["name"], "-c", "sandbox", "--", "cat", "/tmp/" + marker],
                                 capture_output=True, text=True, timeout=10)
            if get.returncode:
                continue
            record = json.loads(get.stdout)
            task = record["turn"]
            if task in turns and task not in seen:
                seen[task] = {**record, "node": spec["nodeName"], "pod": meta["name"], "uid": meta["uid"],
                              "observed_at": time.time()}
        time.sleep(.25)


def sample(stop, kube, node, path):
    with path.open("x", encoding="utf8") as log:
        while not stop.is_set():
            row = {"at": time.time()}
            try:
                stats = json.loads(subprocess.check_output(
                    ["k3s", "kubectl", "get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"],
                    timeout=10))["node"]
                listing = json.loads(subprocess.check_output(kube + ["get", "pods", "-o", "json"], timeout=10))
                row.update(cpu_nanocores=stats["cpu"].get("usageNanoCores"),
                           memory_bytes=stats["memory"].get("workingSetBytes"),
                           unscheduled=sum(not pod["spec"].get("nodeName") for pod in listing["items"]))
            except (subprocess.SubprocessError, KeyError, ValueError) as exc:
                row["error"] = type(exc).__name__
            log.write(json.dumps(row) + "\n")
            log.flush()
            stop.wait(2)


def accept_batch(opts, args, source, data_dir, url, namespace, unused):
    submitted = queue.Queue()
    resume_wait = threading.Event()
    abort = threading.Event()
    cases = list(FIXTURES)
    batch = {"mode": opts.mode, "namespace": namespace, "injections": 0, "session_recovery_slots": opts.session_recovery_slots,
             "lease_ttl": opts.lease_ttl if opts.mode == "node-loss" else None,
             "source_node": opts.source_node, "target_node": opts.target_node,
             "submitted": [], "before": {}, "tasks": []}
    evidence = args.evidence / "batch.json"
    grading_slot = threading.Lock()
    solves_finished = threading.Barrier(opts.tasks)
    def save():
        evidence.write_bytes(BASE["encode"](batch))

    def run(index):
        case = cases[index % len(cases)]
        child = copy(args)
        child.evidence = args.evidence / f"{index+1:02}-{case}"
        child.evidence.mkdir()
        child.fixture = FIXTURES[case]
        def registered(_, address, turn, operator, ns):
            submitted.put(turn)
            resume_wait.wait()
            if abort.is_set():
                raise RuntimeError("batch aborted before recovery observation")
        reached_grading = False
        def before_grade():
            nonlocal reached_grading
            reached_grading = True
            solves_finished.wait(timeout=args.timeout)
            grading.enter_context(grading_slot)
        with ExitStack() as grading:
            try:
                result = BASE["run_acceptance"](child, SOURCES[case], data_dir, url, namespace, registered,
                                                capture_files=CAPTURE_FILES[case], before_grade=before_grade)
            finally:
                if not resume_wait.is_set() or abort.is_set():
                    solves_finished.abort()
                elif not reached_grading:
                    solves_finished.wait(timeout=args.timeout)
        intact = all(result.get("captured_files", {}).get(name) == BASE["digest"](text.encode())
                     for name, text in SOURCES[case].items() if name != "records.py")
        return {"turn": result["turn_id"], "case": case, "verdict": result["verdict"],
                "cases": result.get("cases"), "source_intact": intact, "directory": child.evidence.name}

    kube = ["k3s", "kubectl", "-n", namespace]
    deadline = time.monotonic() + args.timeout
    stop = threading.Event()
    sampler = threading.Thread(target=sample, args=(stop, kube, opts.target_node or opts.source_node,
                                                    args.evidence / "node-samples.jsonl"), daemon=True)
    sampler.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=opts.tasks) as pool:
        futures = [pool.submit(run, i) for i in range(opts.tasks)]
        try:
            while len(batch["submitted"]) < opts.tasks:
                try:
                    batch["submitted"].append(submitted.get(timeout=1))
                except queue.Empty:
                    for future in futures:
                        if future.done() and future.exception():
                            raise future.exception()
                    if time.monotonic() >= deadline:
                        raise TimeoutError("batch submissions did not reach barrier")
            turns = set(batch["submitted"])
            if len(turns) != opts.tasks:
                raise RuntimeError("batch reused a task")
            collect_markers(kube, "fault-ready", turns, None if opts.allow_spillover else opts.source_node,
                            deadline, batch["before"])
            token = (data_dir / "api.token").read_text().strip()
            for turn, old in batch["before"].items():
                run_info = BASE["inspect_task"](args.agentbox, url, turn, token)
                if not any(c["checkpoint_id"] == old["checkpoint"] and c["attempt_id"] == old["attempt"]
                           for c in run_info["checkpoints"]):
                    raise RuntimeError(f"{turn}: uncommitted checkpoint at barrier")
                if opts.real_model:
                    pg = ["k3s", "kubectl", "-n", "agentbox-cvm-acceptance", "exec", "-i", "postgres", "--",
                          "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "agentbox", "-d",
                          "csv_" + namespace.split("-")[-1], "-v", "cp=" + old["checkpoint"],
                          "-v", "old=" + old["attempt"], "-v", "task=" + turn]
                    query = """SELECT json_build_object('checkpoint',c.checkpoint_id,'attempt',c.attempt_id,
                        'turn',c.scope_id,'committed_at',c.committed_at,'verified_at',clock_timestamp(),
                        'lease_expires_at',t.lease_expires_at) FROM checkpoints c JOIN tasks t
                        ON t.task_id=c.scope_id AND t.current_attempt_id=c.attempt_id WHERE
                        c.scope_kind='task' AND c.scope_id=:'task' AND c.checkpoint_id=:'cp'
                        AND c.attempt_id=:'old' AND t.status='running' AND t.lease_expires_at>now();
                    """
                    receipt = subprocess.check_output(pg, input=query, text=True, timeout=15).strip()
                    old["pg_before_stop"] = json.loads(receipt)
                    save()
            if opts.mode == "node-loss" or opts.check_egress:
                # Check the actual worker network namespace before triggering the real fault.
                host, port = args.relay_listen.rsplit(":", 1)
                pg_ip = subprocess.check_output(["k3s", "kubectl", "-n", "agentbox-cvm-acceptance", "get", "pod",
                    "postgres", "-o", "jsonpath={.status.podIP}"], text=True, timeout=10).strip()
                for old in batch["before"].values():
                    subprocess.run(kube + ["exec", old["pod"], "-c", "sandbox", "--", "python3", "-c",
                        "import socket,sys\n"
                        "socket.create_connection((sys.argv[1],int(sys.argv[2])),3).close()\n"
                        "for target in [('169.254.0.23',80),(sys.argv[3],5432)]:\n"
                        " try: socket.create_connection(target,2)\n"
                        " except OSError: continue\n"
                        " sys.exit('unexpected worker egress to ' + repr(target))", host, port, pg_ip],
                        check=True, timeout=10)
                batch["worker_egress_probe"] = "relay reachable; CVM metadata TCP/80 and PG TCP/5432 blocked"
            if opts.mode == "reference":
                for old in batch["before"].values():
                    subprocess.run(kube + ["exec", old["pod"], "-c", "sandbox", "--",
                                           "touch", "/tmp/fault-reference"], check=True, timeout=10)
            else:
                if not any(old["node"] == opts.source_node for old in batch["before"].values()):
                    raise RuntimeError("no checkpoint on the designated fault node")
                batch["fault_requested_at"] = time.time()
                save()
                # Dedicated operator-provided control helper; no shell and no kubectl Pod deletion.
                receipt = subprocess.check_output([str(opts.stop_instance), opts.instance_id, opts.source_node],
                                                  timeout=args.timeout, text=True)
                batch["power_receipt"] = json.loads(receipt)
                if (batch["power_receipt"].get("instance_id") != opts.instance_id or
                    batch["power_receipt"].get("state") != "STOPPED"):
                    raise RuntimeError("control plane did not confirm the requested instance stopped")
                batch["injections"] = 1
                save()
                for old in batch["before"].values():
                    if old["node"] != opts.source_node:
                        subprocess.run(kube + ["exec", old["pod"], "-c", "sandbox", "--",
                                               "touch", "/tmp/fault-reference"], check=True, timeout=15)

            resume_wait.set()
            for index, future in enumerate(futures):
                try:
                    batch["tasks"].append(future.result())
                except Exception as exc:
                    batch["tasks"].append({"directory": f"{index+1:02}-{cases[index % len(cases)]}",
                                           "error": str(exc)})
                save()
        except Exception as exc:
            batch["error"] = str(exc)
            raise
        finally:
            abort.set()
            resume_wait.set()
            stop.set()
            sampler.join(25)
            save()
    return {"batch_evidence": evidence.name}


def verify(evidence):
    batch = json.loads((evidence / "batch.json").read_bytes())
    affected = {t for t, old in batch["before"].items() if old["node"] == batch["source_node"]}
    recovery = {"injections": batch["injections"], "affected": len(affected) if batch["injections"] else 0,
                "recovered": 0, "graded_tasks": 0, "passed_checks": 0, "executed_checks": 0, "turns": {}}
    pods = {p["name"]: p for p in json.loads((evidence / "result.json").read_bytes())["pods"]}
    errors = []
    for task in batch["tasks"]:
        if "error" in task:
            errors.append(f'{task["directory"]}: {task["error"]}')
            continue
        turn = task["turn"]
        if task["cases"]:
            recovery["graded_tasks"] += 1
            recovery["passed_checks"] += sum(task["cases"].values())
            recovery["executed_checks"] += len(task["cases"])
        if task["verdict"] != "solved" or not task["source_intact"]:
            errors.append(f"{turn}: grading or input integrity failed")
        if batch["mode"] == "node-loss" and turn in affected:
            old = batch["before"][turn]
            result_dir = evidence / task["directory"]
            result = json.loads((result_dir / "result.json").read_bytes())
            run = json.loads((result_dir / "solve-inspect.json").read_bytes())
            matching = [line for line in (result_dir / "events.sse").read_text("utf8").splitlines()
                        if line.startswith("data: ")]
            events = [json.loads(line[6:]) for line in matching]
            restored = [e for e in events if e.get("internal", {}).get("payload", {}).get("kind") == "phase4_restored"]
            if len(restored) != 1:
                errors.append(f"{turn}: expected exactly one restored chatagent, got {len(restored)}")
                continue
            event = restored[0]
            new = event["internal"]["payload"]["data"]
            pod = pods[new["pod"]]
            identity_ok = (new["turn"] == turn and new["attempt"] == event["internal"]["attempt_id"]
                           and new["attempt"] != old["attempt"] and pod["node"] == batch["target_node"]
                           and pod["uid"] != old["uid"] and old["node"] == batch["source_node"])
            state_ok = old["checkpoint"] == new["checkpoint"] and old["messages_sha256"] == new["messages_sha256"]
            if not identity_ok or not state_ok:
                errors.append(f"{turn}: replacement identity or checkpoint mismatch")
            after = [e for e in events if e["type"] == "tool_result" and e["data"]["ok"]
                     and e["seq"] > event["seq"] and e["internal"].get("attempt_id") == new["attempt"]]
            if after:
                recovery["turns"][turn] = {"old": old, "restored": new, "node": pod["node"],
                    "pod_uid": pod["uid"], "tool": after[0]["data"]["tool"],
                    "fault_to_next_ok_s": datetime.fromisoformat(after[0]["ts"]).timestamp() - batch["fault_requested_at"]}
            else:
                errors.append(f"{turn}: no effective tool after recovery")
            if not any(call["first_attempt_id"] == new["attempt"] and call["call_id"] == result.get("producer_exec")
                       for call in run["calls"]):
                errors.append(f"{turn}: missing resumed exec")
            elif after and identity_ok and state_ok:
                recovery["recovered"] += 1
    if not batch["tasks"] or len(batch["tasks"]) != len(batch["submitted"]):
        errors.append("incomplete batch")
    rows = [json.loads(line) for line in (evidence / "node-samples.jsonl").read_text().splitlines()]
    for field in ("cpu_nanocores", "memory_bytes", "unscheduled"):
        recovery["peak_" + field] = max((r[field] for r in rows if r.get(field) is not None), default=None)
    recovery["missing_samples"] = sum("error" in r for r in rows)
    recovery["errors"] = errors
    (evidence / "summary.json").write_bytes(BASE["encode"](recovery))
    if errors:
        raise RuntimeError("; ".join(errors))


if __name__ == "__main__":
    cli = argparse.ArgumentParser(add_help=False)
    cli.add_argument("--mode", choices=["reference", "node-loss"], required=True)
    cli.add_argument("--tasks", choices=[1, 3, 5, 10], type=int, default=5)
    cli.add_argument("--session-recovery-slots", type=int, default=0)
    cli.add_argument("--source-node", required=True)
    cli.add_argument("--target-node")
    cli.add_argument("--instance-id")
    cli.add_argument("--stop-instance", type=Path)
    cli.add_argument("--check-egress", action="store_true", help="verify worker network isolation in a reference run")
    cli.add_argument("--allow-spillover", action="store_true", help="also count checkpoints on other nodes; only source-node turns are affected")
    cli.add_argument("--lease-ttl", default="30s", help="attempt lease duration for the node-loss run")
    cli.add_argument("--real-model", action="store_true", help="use the supplied model URL and a successful workspace write as the M1 checkpoint")
    options, remaining = cli.parse_known_args()
    if options.real_model and (options.tasks != 1 or not any(s == "--model-url" or s.startswith("--model-url=") for s in remaining)):
        cli.error("--real-model requires one task and an explicit --model-url")
    if options.mode == "node-loss" and (not options.target_node or options.target_node == options.source_node or
        not options.instance_id or not options.stop_instance or not options.stop_instance.is_absolute()):
        cli.error("node-loss requires distinct nodes, instance ID and absolute control-helper path")
    flags = ["--k8s-preferred-node", options.source_node, "--session-recovery-slots", str(options.session_recovery_slots)]
    if options.mode == "node-loss":
        required = ("AGENTBOX_COS_BUCKET_URL", "AGENTBOX_COS_PREFIX", "AGENTBOX_COS_CVM_ROLE")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            cli.error("missing COS settings: " + ", ".join(missing))
        flags += ["--allow-session-lease-fence", "--attempt-lease-ttl", options.lease_ttl]
    sys.argv = [sys.argv[0], *remaining, "--fixture", str(FIXTURES["csv-first-row"])]
    if not options.real_model:
        sys.argv.append("--scripted")
    directory = BASE["main"](worker_command=lambda _: worker_command(options.real_model),
                             acceptance=lambda *args: accept_batch(options, *args),
                             upstream=lambda: BASE["tool_upstream"](steps_for), run_slots=options.tasks,
                             gateway_flags=flags)
    verify(directory)
