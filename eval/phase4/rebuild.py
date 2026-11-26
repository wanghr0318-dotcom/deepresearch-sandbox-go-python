"""Crash the fresh chatagent Pod after its write tool checkpoint, then grade its continued turn."""

from pathlib import Path
import json
import runpy
import subprocess
import sys
import time

base = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/run-cloud-agent-csv.py"))
HOLD = """
from chatagent.loop import Agent
import chatagent.app as app
import hashlib, json
from pathlib import Path
def witness(messages):
    encoded = json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "roles": [m["role"] for m in messages],
            "tool_results": [m["tool_call_id"] for m in messages if m["role"] == "tool"]}
restore = app._resumed_state
def observed_restore(ctx):
    state = restore(ctx)
    Path("/tmp/phase4-resumed").write_text(json.dumps(witness(state.messages)))
    return state
app._resumed_state = observed_restore
original_save = Agent.save
async def save_and_hold(agent, step):
    checkpoint = await original_save(agent, step)
    messages = agent.state.messages
    if agent.ctx.resume is None and messages and messages[-1].get("tool_call_id") == "cpu-1":
        Path("/tmp/phase4-ready").write_text(json.dumps({"checkpoint_id": checkpoint, **witness(messages)}))
        await asyncio.Event().wait()
    return checkpoint
Agent.save = save_and_hold
"""


def inject(args, url, turn, token, namespace):
    kubectl = ["k3s", "kubectl", "-n", namespace]
    def witness(filename, exclude=None):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            pods = json.loads(subprocess.check_output(kubectl + ["get", "pods", "-o", "json"]))["items"]
            for pod in pods:
                name = pod["metadata"]["name"]
                if name == exclude or pod["status"]["phase"] != "Running":
                    continue
                result = subprocess.run(kubectl + ["exec", name, "-c", "sandbox", "--", "cat", "/tmp/" + filename],
                                        capture_output=True, text=True, timeout=8)
                if result.returncode == 0:
                    return pod, json.loads(result.stdout)
            time.sleep(.25)
        raise TimeoutError(f"Pod witness {filename} timed out")

    old, committed = witness("phase4-ready")
    name = old["metadata"]["name"]
    checkpoint_id = committed.pop("checkpoint_id")
    before = base["inspect_task"](args.agentbox, url, turn, token)
    if not any(cp["checkpoint_id"] == checkpoint_id for cp in before["checkpoints"]):
        raise ValueError("write checkpoint not present in the durable journal")
    evidence = {"turn": turn, "pod": name, "uid": old["metadata"]["uid"], "node": old["spec"]["nodeName"],
                "checkpoint_id": checkpoint_id, "checkpoints": before["checkpoints"], "attempts": before["attempts"],
                "committed_messages": committed, "delete_utc_epoch": time.time()}
    path = args.evidence / "fault.json"
    path.write_bytes(base["encode"](evidence))
    subprocess.run(kubectl + ["delete", "pod", name, "--grace-period=0", "--force", "--wait=true"],
                   check=True, timeout=20)
    new, restored = witness("phase4-resumed", exclude=name)
    evidence.update(resumed_messages=restored, resumed_pod=new["metadata"]["name"], resumed_uid=new["metadata"]["uid"])
    path.write_bytes(base["encode"](evidence))
    if committed != restored or restored["tool_results"] != ["cpu-0", "cpu-1"]:
        raise ValueError("message transcript changed across Pod replacement")

def verify(directory):
    def load(name):
        return json.loads((directory / name).read_bytes())

    fault, run, result = (load(name) for name in ("fault.json", "solve-inspect.json", "result.json"))
    attempts = run["attempts"]
    if [a["outcome_class"] for a in attempts] != ["crashed_signal", "succeeded"]:
        raise ValueError(f"unexpected attempt outcomes: {attempts}")
    first, second = [a["attempt_id"] for a in attempts]
    checkpoint = next(cp for cp in run["checkpoints"] if cp["checkpoint_id"] == fault["checkpoint_id"])
    calls = [c for c in run["calls"] if c["call_id"] == result["producer_exec"]]
    if checkpoint["attempt_id"] != first or len(calls) != 1 or calls[0]["first_attempt_id"] != second:
        raise ValueError("checkpoint and resumed exec must belong to different attempts")
    if result["tools"] != ["read_file", "write_file", "exec_shell"] or not result["public_workflow_ok"]:
        raise ValueError("invalid tool sequence across recovery")
    if result["verdict"] != "solved" or result["remaining_resources"]:
        raise ValueError("grading or resource cleanup failed")
    if result["source_sha256"]["records.py"] == result["candidate_sha256"]:
        raise ValueError("restored file still has the initial content")
    print("PASS: committed write, Pod loss, resumed exec and independent grading")


if __name__ == "__main__":
    if "--scripted" not in sys.argv:
        raise SystemExit("the recovery fixture requires --scripted")
    directory = base["main"](worker_command=lambda files: base["seed_command"](files, HOLD), after_submit=inject)
    verify(directory)
