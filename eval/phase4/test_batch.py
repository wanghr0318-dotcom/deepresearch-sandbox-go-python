"""Exercise the deterministic tool turns against isolated real files without a GPU."""
import json
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from urllib.request import Request, urlopen

from scenarios import BASE, FIXTURES, SOURCES, steps_for
from batch import verify, worker_command
from stop_worker import stop_worker
from unittest.mock import patch


class FixtureTests(unittest.TestCase):
    def test_worker_entrypoint_compiles(self):
        import base64
        import re
        for command in (BASE["seed_command"](SOURCES["csv-first-row"]), worker_command()):
            encoded = re.search(r"b64decode\('([^']+)'\)", command).group(1)
            compile(base64.b64decode(encoded), "<worker>", "exec")

    def test_stopping_wrong_cvm_never_invokes_stop(self):
        node = {"metadata": {"labels": {}}, "status": {"addresses": [{"type": "InternalIP", "address": "10.0.0.2"}]}}
        cvm = {"Response": {"InstanceSet": [{"InstanceId": "ins-b", "PrivateIpAddresses": ["10.0.0.3"]}]}}
        with patch("subprocess.check_output", side_effect=[json.dumps(node), json.dumps(cvm)]) as calls:
            with self.assertRaisesRegex(ValueError, "internal IP"):
                stop_worker("ins-b", "worker-b", "ap-shanghai")
            self.assertEqual(calls.call_count, 2)
            self.assertIn("DescribeInstances", calls.call_args.args[0])

    def test_recovery_requires_new_node_transcript_and_effective_tool(self):
        old = dict(node="B", uid="old", attempt="a1", checkpoint="c1", messages_sha256="hash")
        new = dict(pod="worker", turn="t", attempt="a2", checkpoint="c1", messages_sha256="hash")
        resumed = {"seq": 4, "type": "internal", "internal": {"attempt_id": "a2",
            "payload": {"kind": "phase4_restored", "data": new}}}
        ok = {"seq": 5, "type": "tool_result", "ts": "2026-11-27T00:00:00+00:00",
              "data": {"ok": True, "tool": "exec_shell"}, "internal": {"attempt_id": "a2", "payload": {}}}
        task = dict(directory="run", turn="t", cases={"correct": True}, verdict="solved", source_intact=True)
        for node, events, expected in (("A", [resumed, ok], 1), ("B", [resumed, ok], 0), ("A", [resumed], 0)):
            with self.subTest(node=node, event_count=len(events)), TemporaryDirectory() as tmp:
                root = Path(tmp)
                out = root / "run"
                out.mkdir()
                (root / "batch.json").write_text(json.dumps(dict(injections=1, before={"t": old}, tasks=[task],
                    submitted=["t"], mode="node-loss", source_node="B", target_node="A", fault_requested_at=0)))
                (root / "result.json").write_text(json.dumps({"pods": [dict(name="worker", node=node, uid="new")]}))
                (out / "result.json").write_text('{"producer_exec":"exec1"}')
                (out / "solve-inspect.json").write_text('{"calls":[{"first_attempt_id":"a2","call_id":"exec1"}]}')
                (root / "node-samples.jsonl").write_text('{}')
                (out / "events.sse").write_text("".join("data: " + json.dumps(e) + "\n" for e in events))
                if expected:
                    verify(root)
                else:
                    with self.assertRaises(RuntimeError):
                        verify(root)
                result = json.loads((root / "summary.json").read_bytes())
                self.assertEqual((result["injections"], result["recovered"], result["passed_checks"]), (1, expected, 1))

    def test_three_reference_traces(self):
        server = BASE["tool_upstream"](steps_for)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for case in FIXTURES:
                with self.subTest(case=case), TemporaryDirectory() as tmp:
                    work = Path(tmp)
                    for name, content in SOURCES[case].items():
                        (work / name).write_bytes(content.encode())
                    messages = [{"role": "user", "content": (FIXTURES[case] / "task.md").read_text("utf8")}]
                    names = ["read_file", "write_file", "exec_shell"]
                    tool_sequence = []
                    for step in range(12):
                        body = {"model": "phase4-cpu", "messages": messages,
                                "tools": [{"function": {"name": n}} for n in names]}
                        req = Request(f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                                      data=BASE["encode"](body))
                        with urlopen(req, timeout=3) as response:
                            answer = json.load(response)["choices"][0]["message"]
                        if "tool_calls" not in answer:
                            break
                        messages.append(answer)
                        call = answer["tool_calls"][0]
                        name = call["function"]["name"]
                        args = json.loads(call["function"]["arguments"])
                        tool_sequence.append((name, args.get("path")))
                        if name == "read_file":
                            result = "file content\n" + (work / args["path"]).read_text("utf8")
                        elif name == "write_file":
                            (work / args["path"]).write_bytes(args["content"].encode())
                            result = "write complete"
                        else:
                            # Split the Linux probe command without invoking a shell on the dev host.
                            import shlex
                            command = shlex.split(args["command"])
                            subprocess.run([sys.executable, *command[1:]], cwd=work, check=True, timeout=8)
                            result = "exit 0"
                        messages.append({"role": "tool", "tool_call_id": call["id"], "content": result})
                    else:
                        self.fail("unterminated scripted model")
                    trusted = runpy.run_path(str(FIXTURES[case] / "verify.py"))
                    checked = subprocess.run([sys.executable, "-I", "-c", trusted["CHECKER"]], cwd=work,
                                             text=True, input=json.dumps(trusted["CASES"]), capture_output=True,
                                             check=True, timeout=8)
                    self.assertEqual(list(json.loads(checked.stdout).values()), [True] * 4)
                    if case == "reconciliation":
                        self.assertLess(tool_sequence.index(("write_file", "audit.py")),
                                        tool_sequence.index(("read_file", "audit.py")))
                    for name, content in SOURCES[case].items():
                        if name != "records.py":
                            self.assertEqual((work / name).read_bytes(), content.encode())
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()


if __name__ == "__main__":
    unittest.main()
