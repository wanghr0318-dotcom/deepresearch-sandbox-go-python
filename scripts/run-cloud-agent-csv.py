#!/usr/bin/env python3
"""Run one fixture through a fresh CVM Gateway, chatagent session and KindExec grader."""

import argparse
import base64
import hashlib
import http.cookiejar
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import runpy
import secrets
import socket
import subprocess
import threading
import time
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener


def encode(data):
    return json.dumps(data, ensure_ascii=False).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def seed_command(files):
    # A test entry point in the existing worker image. Base64 keeps argv free of commas.
    payload = f"""import asyncio
from agentbox_worker import main_session
from chatagent.app import run
async def seeded(ctx):
    for path, content, sha in {[(p, s, digest(s.encode())) for p, s in files.items()]!r}:
        result = await asyncio.to_thread(ctx.gateway.workspace, 'write', {{'path': path, 'content': content}})
        if result['sha256'] != sha:
            raise ValueError(f'fixture checksum mismatch: {{path}}')
    return await run(ctx)
main_session(seeded, name='chatagent', version='0.1.0')
"""
    source = base64.b64encode(payload.encode()).decode()
    return f"python3,-c,exec(__import__('base64').b64decode('{source}'))"


def scripted_upstream(original):
    repair = original.replace("    next(reader, None)\n", "")
    steps = [
        ("read_file", {"path": "records.py"}),
        ("write_file", {"path": "records.py", "content": repair}),
        ("exec_shell", {"command": "python3 -I -c 'import runpy; assert len(runpy.run_path(\"records.py\")[\"read_records\"](\"name,city\\nAda,London\\n\")) == 1'"}),
    ]

    class Model(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            tool_ids = [m["tool_call_id"] for m in body["messages"] if m["role"] == "tool"]
            step = len(tool_ids)
            valid = tool_ids == [f"cpu-{i}" for i in range(step)] and step <= len(steps)
            message = {"role": "assistant", "content": "CSV smoke finished."}
            finish = "stop"
            if valid and step < len(steps):
                name, arguments = steps[step]
                valid = name in [t["function"]["name"] for t in body["tools"]]
                message = {"role": "assistant", "content": None, "tool_calls": [
                    {"id": f"cpu-{step}", "type": "function",
                     "function": {"name": name, "arguments": json.dumps(arguments)}}]}
                finish = "tool_calls"
            output = encode({
                "id": f"cpu-response-{step}", "object": "chat.completion", "model": body["model"],
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40},
            } if valid else {"error": {"message": "unexpected CPU rehearsal transcript"}})
            self.send_response(200 if valid else 400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(output)

    return HTTPServer(("127.0.0.1", 0), Model)


def inspect_task(binary, url, task, token):
    return json.loads(subprocess.check_output(
        [str(binary), "task", "inspect", "--addr", url, task],
        env={**os.environ, "AGENTBOX_TOKEN": token}, timeout=10))


def read_blob(data_dir, sha):
    path = data_dir / "blobs" / "sha256" / sha[:2] / sha[2:]
    data = path.read_bytes()
    if digest(data) != sha:
        raise ValueError(f"SHA-256 mismatch for {sha}")
    return data


def last_exec(run, data_dir):
    calls = [c for c in run["calls"] if "/workspace_exec/" in c["call_id"] and c.get("result_ref")]
    if not calls:
        return None, {}
    call = calls[-1]
    return call, json.loads(read_blob(data_dir, call["result_ref"]))


def wait_task(binary, url, task, token, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = inspect_task(binary, url, task, token)
        if current["task"]["status"] in ("succeeded", "failed", "cancelled"):
            return current
        time.sleep(1)
    raise TimeoutError(f"task {task} timed out")


def wait_http(url, gateway):
    opener = build_opener()
    for _ in range(30):
        if gateway.poll() is not None:
            raise RuntimeError("Gateway exited; see gateway.log")
        try:
            opener.open(url + "/status", timeout=1).close()
            return
        except URLError:
            time.sleep(1)
    raise TimeoutError("Gateway startup timed out")


def run_acceptance(args, source, data_dir, url, namespace):
    results = args.evidence
    owner = build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
    operator = (data_dir / "api.token").read_text().strip()

    def api(method, path, data=None, *, admin=False):
        headers = {"Content-Type": "application/json"}
        if admin:
            headers["Authorization"] = f"Bearer {operator}"
        request = Request(url + path, data=None if data is None else encode(data),
                          headers=headers, method=method)
        with owner.open(request, timeout=15) as response:
            return json.load(response)

    login = {"username": "csv-" + secrets.token_hex(6), "password": "C" + secrets.token_hex(6) + "9"}
    api("POST", "/auth/register", login)
    session = api("POST", "/sessions", {"request_id": "create", "title": args.fixture.name})["session_id"]
    fixture = args.fixture
    report = {"fixture": fixture.name, "namespace": namespace, "session_id": session, "model": args.model,
              "source_sha256": {p: digest(text.encode()) for p, text in source.items()},
              "upstream": "scripted_cpu" if args.scripted else "configured_model", "verdict": "other"}
    try:
        start = time.monotonic()
        prompt = fixture.joinpath("task.md").read_text(encoding="utf-8")
        prompt += ("\nThe contents of repo/ are already staged at the workspace root. The tool path "
                   "for the target is exactly records.py. Use read_file on records.py first, write "
                   "your change to records.py, then run a public smoke test with exec_shell.")
        prompt = prompt.strip()
        report["prompt_sha256"] = digest(prompt.encode())
        (results / "prompt.txt").write_bytes(prompt.encode())
        turn = api("POST", f"/sessions/{session}/messages",
                   {"request_id": "solve", "deep_research": False, "text": prompt})["turn_id"]
        report["turn_id"] = turn
        solved = wait_task(args.agentbox, url, turn, operator, args.timeout)
        report["runtime_ms"] = round((time.monotonic() - start) * 1000)
        (results / "solve-inspect.json").write_bytes(encode(solved))
        # A closed per-turn workflow leaves the session event stream open. Resume until its turn ends.
        request = Request(url + f"/sessions/{session}/events",
                          headers={"Authorization": f"Bearer {operator}"})
        with owner.open(request, timeout=10) as stream, (results / "events.sse").open("wb") as saved:
            events = []
            for line in stream:
                saved.write(line)
                if line.startswith(b"data: "):
                    event = json.loads(line[6:])
                    events.append(event)
                    if event["type"] == "turn_status" and event["data"]["status"] in ("succeeded", "failed", "cancelled"):
                        break
        tool_calls = [e["data"] for e in events if e["type"] == "tool_call"]
        report["tools"] = [entry["tool"] for entry in tool_calls]
        report["agent_status"] = solved["task"]["status"]
        report["agent_reason"] = solved["task"]["status_reason"]
        model_calls = [c for c in solved["calls"] if c["endpoint"] == "/v1/chat/completions" and c.get("result_ref")]
        report["model_calls"] = []
        for c in model_calls:
            body = json.loads(read_blob(data_dir, c["result_ref"]))
            report["model_calls"].append({"call_id": c["call_id"], "result_ref": c["result_ref"],
                                          "configured": c["model"], "served": body.get("model"),
                                          "usage": body.get("usage"), "tries": c["tries"]})
        tool_results = [e["data"] for e in events if e["type"] == "tool_result"]
        report["tool_argument_errors"] = sum(
            not e["ok"] and e.get("error", "").startswith("工具参数错误：") for e in tool_results
        )
        if not model_calls:
            report.update(verdict="infrastructure_abort", reason="no_completed_model_response")
            return report
        if any(c["served"] != args.model for c in report["model_calls"]):
            report["reason"] = "served_model_mismatch"
            return report
        tool_ok = {e["tool_call_id"]: e["ok"] for e in tool_results}
        required = ["read_file", "write_file", "exec_shell"]
        for entry in tool_calls:
            target_ok = entry["tool"] == "exec_shell" or entry["input"].get("path") == "records.py"
            if required and required[0] == entry["tool"] and target_ok and tool_ok.get(entry["tool_call_id"]):
                required.pop(0)
        report["public_workflow_ok"] = not required
        call, outcome = last_exec(solved, data_dir)
        if call is None:
            report["reason"] = "no_completed_tool_result"
            if report["tool_argument_errors"]:
                report["verdict"] = "parse_abort"
            return report
        captured = next((i for i, e in enumerate(tool_results)
                         if (e.get("raw") or {}).get("response_ref") == call["result_ref"]), None)
        latest = {e["tool_call_id"]: e for e in tool_calls}
        if captured is None or any(
            e["ok"] and e["tool"] == "write_file" and
            latest[e["tool_call_id"]]["input"].get("path") == "records.py"
            for e in tool_results[captured + 1:]
        ):
            report["reason"] = "candidate_snapshot_missing_or_stale"
            return report
        if outcome["status"] != "completed":
            report.update(verdict="infrastructure_abort", reason=outcome["status"])
            return report
        candidate_meta = next((o for o in outcome.get("outputs", []) if o["path"] == "records.py"), None)
        if candidate_meta is None:
            report["reason"] = "records_py_missing_from_snapshot"
            return report
        sha = candidate_meta["sha256"]
        candidate = read_blob(data_dir, sha)
        if len(candidate) != candidate_meta["size"]:
            raise ValueError("candidate size does not match KindExec output")
        (results / "records.py").write_bytes(candidate)
        report.update(candidate_sha256=sha, candidate_bytes=len(candidate), producer_exec=call["call_id"],
                      producer_result=call["result_ref"])
        oracle = runpy.run_path(str(fixture / "verify.py"))
        checker = oracle["CHECKER"].replace("cases = json.load(sys.stdin)",
                                           "cases = " + repr(oracle["CASES"]))
        spec = {"steps": [
            {"op": "workspace_exec", "step_id": "grade", "command": "python3 -I checker.py",
             "wall_ms": 8000, "files": {**source, "records.py": candidate.decode(), "checker.py": checker}},
            {"op": "artifact", "artifact_id": "report", "path": "report.txt", "content": "Graded candidate."}],
            "outputs": ["report"], "summary": "Independent CSV candidate grading"}
        spec_path = results / "grade-spec.json"
        spec_path.write_bytes(encode(spec))
        grade_task = json.loads(subprocess.check_output(
            [str(args.agentbox), "task", "submit", "--addr", url, "--spec", "@" + str(spec_path)],
            env={**os.environ, "AGENTBOX_TOKEN": operator}, timeout=10))["task_id"]
        graded = wait_task(args.agentbox, url, grade_task, operator, args.timeout)
        (results / "grade-inspect.json").write_bytes(encode(graded))
        grade_call, grade_outcome = last_exec(graded, data_dir)
        if grade_call is None:
            report.update(verdict="infrastructure_abort", reason="grade_no_output")
            return report
        report.update(grade_task=grade_task, grade_result=grade_call["result_ref"],
                      grade_status=grade_outcome["status"], grade_exit=grade_outcome.get("exit"))
        if graded["task"]["status"] != "succeeded" or grade_outcome["status"] != "completed":
            report.update(verdict="infrastructure_abort", reason="grade_incomplete")
            return report
        after = next((o for o in grade_outcome.get("outputs", []) if o["path"] == "records.py"), None)
        if after is None or after["sha256"] != sha:
            report["reason"] = "candidate_changed_during_grade"
            return report
        if grade_outcome["exit"]["code"] != 0:
            report.update(verdict="scored_unsolved", reason="candidate_exception",
                          stderr=grade_outcome["stderr"])
            return report
        checks = json.loads(grade_outcome["stdout"])
        if set(checks) != {name for name, *_ in oracle["CASES"]} or not all(
            isinstance(value, bool) for value in checks.values()
        ):
            raise ValueError("grading did not return one judgement per fixture case")
        report["cases"] = checks
        report["verdict"] = "solved" if all(checks.values()) else "scored_unsolved"
    finally:
        (results / "result.json").write_bytes(encode(report) + b"\n")
        api("DELETE", f"/sessions/{session}")
    return report


def summarize_pods(path):
    pods = {}
    for line in path.read_text().splitlines():
        if not line:
            continue
        event = json.loads(line)
        pod = event["object"]
        meta, spec = pod["metadata"], pod["spec"]
        uid = meta["uid"]
        status = pod["status"]
        entry = pods.setdefault(uid, {
            "name": meta["name"], "uid": uid, "created": meta["creationTimestamp"],
            "containers": [c["name"] for c in spec["containers"]],
            "host_path": sum("hostPath" in volume for volume in spec.get("volumes", [])),
        })
        entry["node"] = spec.get("nodeName")
        ready = next((c["lastTransitionTime"] for c in status.get("conditions", [])
                      if c["type"] == "Ready" and c["status"] == "True"), None)
        if ready:
            entry.setdefault("ready", ready)
        if event["type"] == "DELETED":
            entry["deleted"] = True
    return list(pods.values())


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--fixture", type=Path, required=True)
    cli.add_argument("--agentbox", type=Path, required=True)
    cli.add_argument("--worker-image", required=True)
    cli.add_argument("--relay-listen", required=True)
    cli.add_argument("--model", required=True)
    models = cli.add_mutually_exclusive_group(required=True)
    models.add_argument("--model-url")
    models.add_argument("--scripted", action="store_true")
    cli.add_argument("--evidence", type=Path, required=True)
    cli.add_argument("--timeout", type=int, default=240)
    args = cli.parse_args()
    args.agentbox = args.agentbox.resolve()
    args.evidence = args.evidence.resolve()
    args.evidence.mkdir(mode=0o700)
    source = {p.relative_to(args.fixture / "repo").as_posix(): p.read_bytes().decode()
              for p in sorted(args.fixture.joinpath("repo").rglob("*.py"))}
    model = scripted_upstream(source["records.py"]) if args.scripted else None
    if model:
        threading.Thread(target=model.serve_forever, daemon=True).start()
        args.model_url = f"http://127.0.0.1:{model.server_port}/v1"
    run_id = secrets.token_hex(5)
    namespace = "agentbox-csv-" + run_id
    database = "csv_" + run_id
    kubectl = ["k3s", "kubectl"]
    postgres = kubectl + ["-n", "agentbox-cvm-acceptance", "exec", "postgres", "--"]
    subprocess.run(kubectl + ["create", "namespace", namespace], check=True)
    subprocess.run(postgres + ["createdb", "-U", "agentbox", database], check=True)
    pw = subprocess.check_output(postgres + ["printenv", "POSTGRES_PASSWORD"], text=True).strip()
    ip = subprocess.check_output(kubectl + ["-n", "agentbox-cvm-acceptance", "get", "pod", "postgres",
                                            "-o", "jsonpath={.status.podIP}"], text=True)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{listener.getsockname()[1]}"
    url = "http://" + address
    state = args.evidence / "data"
    state.mkdir(mode=0o700)
    (state / "api.token").write_text(secrets.token_hex(32))
    (state / "api.token").chmod(0o600)
    env = {**os.environ, "AGENTBOX_DATABASE_URL": f"postgres://agentbox:{pw}@{ip}:5432/{database}?sslmode=disable"}
    args_for_gateway = [
        "--data-dir", str(state), "--listen", address, "--provider", "k8s",
        "--k8s-kubeconfig", "/etc/rancher/k3s/k3s.yaml", "--k8s-namespace", namespace,
        "--k8s-image", args.worker_image, "--k8s-gateway-listen", args.relay_listen,
        "--workspace-tools", "--run-slots", "1", "--exec-slots", "1", "--exec-per-task", "1",
        "--default-memory-bytes", "268435456", "--memory-bytes", "1073741824",
        "--worker-argv", "python3,-m,sim_worker", "--session-worker-argv", seed_command(source),
        "--model-base-url", args.model_url, "--upstream-allow-private", urlsplit(args.model_url).netloc,
        "--model-name", args.model, "--user-orchestrator-model", args.model, "--user-worker-model", args.model,
    ]
    with (args.evidence / "gateway.log").open("wb") as log:
        gateway = subprocess.Popen([str(args.agentbox), "server", *args_for_gateway],
                                   env=env, stdout=log, stderr=subprocess.STDOUT)
        pod_cmd = kubectl + ["-n", namespace, "get", "pods", "-l", "agentbox.io/managed=true"]
        with (args.evidence / "pods.jsonstream").open("wb") as pod_log:
            watch = subprocess.Popen(pod_cmd + ["--watch", "--output-watch-events", "-o", "json"],
                                     stdout=pod_log)
            report = None
            try:
                wait_http(url, gateway)
                report = run_acceptance(args, source, state, url, namespace)
                remaining_cmd = kubectl + ["-n", namespace, "get", "pods,secret",
                                           "-l", "agentbox.io/managed=true", "-o", "name"]
                for _ in range(12):
                    remaining = subprocess.check_output(remaining_cmd, text=True).split()
                    if not remaining:
                        break
                    time.sleep(1)
                report["remaining_resources"] = remaining
            finally:
                gateway.terminate()
                gateway.wait(timeout=30)
                watch.terminate()
                watch.wait(timeout=5)
                subprocess.run(postgres + ["dropdb", "-U", "agentbox", database], check=True)
                subprocess.run(kubectl + ["delete", "namespace", namespace, "--wait=false"], check=True)
            if report:
                report["pods"] = summarize_pods(args.evidence / "pods.jsonstream")
                report["worker_image"] = args.worker_image
                (args.evidence / "result.json").write_bytes(encode(report) + b"\n")
                print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
