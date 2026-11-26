import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import sys
import time

root = Path(sys.argv[1])
fixture = runpy.run_path(sys.argv[2])
prior = json.loads((root / "inspect.json").read_text())
cli = [str(root / "agentbox"), "task"]
addr = ["--addr", "http://127.0.0.1:18181"]

def blob(base, digest):
    data = (base / "blobs/sha256" / digest[:2] / digest[2:]).read_bytes()
    assert hashlib.sha256(data).hexdigest() == digest
    return data

def result(base, call):
    return json.loads(blob(base, call["result_ref"]))

for case in ("correct", "incorrect"):
    source = next(c for c in prior["calls"] if f"/{case}/workspace_exec/" in c["call_id"])
    entry = next(o for o in result(root / "data", source)["outputs"] if o["path"] == "records.py")
    candidate = blob(root / "data", entry["sha256"])
    assert len(candidate) == entry["size"]
    checker = fixture["CHECKER"].replace("cases = json.load(sys.stdin)", "cases = " + repr(fixture["CASES"]))
    step = dict(op="workspace_exec",step_id="grade",command="python3 -I checker.py",wall_ms=8000,
                files={"records.py":candidate.decode(),"checker.py":checker})
    spec = {"steps":[step,dict(op="artifact",artifact_id="report",path="report.txt",content="CSV grading finished.")],"outputs":["report"],"summary":"CPU candidate regrade"}
    path = Path(case + "-spec.json")
    path.write_text(json.dumps(spec))
    task = json.loads(subprocess.check_output(cli+["submit",*addr,"--spec","@"+str(path)]))["task_id"]
    for i in range(90):
        inspect = json.loads(subprocess.check_output(cli+["inspect",*addr,task]))
        if inspect["task"]["status"] in ("succeeded","failed","cancelled"):
            break
        time.sleep(1)
    assert inspect["task"]["status"] == "succeeded", (task,inspect["task"]["status"])
    call = next(c for c in inspect["calls"] if "/grade/workspace_exec/" in c["call_id"])
    scored = result(Path("data"),call)
    assert scored["status"] == "completed" and scored["exit"]["code"] == 0
    final = next(o for o in scored["outputs"] if o["path"] == "records.py")
    assert blob(Path("data"),final["sha256"]) == candidate
    checks = json.loads(scored["stdout"])
    expected = {name:case == "correct" or name != "quoted_comma" for name,*_ in fixture["CASES"]}
    assert checks == expected
    evidence = dict(scenario=case,producer_task=prior["task"]["task_id"],producer_call=source["call_id"],
                    producer_result=source["result_ref"],candidate_sha256=entry["sha256"],size=len(candidate),
                    grader_task=task,grader_result=call["result_ref"],graded_sha256=final["sha256"],
                    cases=checks,wall_ms=scored["wall_ms"])
    Path(case+".json").write_text(json.dumps(evidence,indent=2)+"\n")
    print(json.dumps(evidence),flush=True)