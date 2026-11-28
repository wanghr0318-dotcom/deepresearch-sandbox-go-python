"""Run from the control-plane host with working k3s, tccli and configured COS credentials.

Supply the two Kubernetes node names and the worker CVM ID. The worker is powered
off once; the control-plane node stays online. Use a new evidence directory.

Example (shell variables supplied by the operator):

    python3 eval/phase4/chain_cloud.py --source-node "$WORKER_NODE" \
      --target-node "$CONTROL_NODE" --instance-id "$WORKER_CVM_ID" \
      --agentbox "$GATEWAY_BIN" --worker-image "$LOCAL_WORKER_IMAGE_DIGEST" \
      --relay-listen "$CONTROL_PRIVATE_IP:$RELAY_PORT" --model phase4-cpu \
      --evidence "$NEW_RUN_DIR" --timeout 500

TENCENTCLOUD_REGION and the gateway's configured artifact-store environment
must be in the runner's environment. The safety check matches CVM private IP
and ID to the worker, rejects control-plane nodes and polls both nodes plus the
worker kubelet after restoring power. Rerun just the restoration via:

    python3 eval/phase4/stop_worker.py "$WORKER_CVM_ID" "$WORKER_NODE" \
      --restore-with-peer "$CONTROL_NODE"
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from scenarios import BASE, FIXTURES, SOURCES
from stop_worker import restore_worker, stop_worker
from verify_chain import verify

WORKER = """
import json
from pathlib import Path
from chatagent.loop import Agent

def sha(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",",":")).encode()).hexdigest()

async def signature(agent, cp):
    doc = await asyncio.to_thread(agent.ctx.gateway.workspace, "read", {"path":"records.py"})
    return dict(attempt=agent.ctx.attempt_id,turn=agent.ctx.task_id,checkpoint=cp,
                messages=sha(agent.state.messages),file_sha256=doc["sha256"],
                tool_ids=[m["tool_call_id"] for m in agent.state.messages if m["role"]=="tool"])

def marker(name,value):
    p=Path("/tmp/chain-"+name)
    t=p.with_suffix(".tmp")
    t.write_text(json.dumps(value))
    t.replace(p)

original_run=Agent.run
async def run_probe(agent,*args,**kwargs):
    if agent.ctx.resume is not None:
        sig=await signature(agent,agent.ctx.resume.checkpoint_id)
        await agent.ctx.progress("chain_restore","loaded committed task",data={**sig,"pod":__import__("socket").gethostname()})
        marker("restore",sig)
    return await original_run(agent,*args,**kwargs)
Agent.run=run_probe
original_save=Agent.save
async def save_probe(agent,step):
    idx=agent.state.messages[-1].get("tool_call_id")
    if idx=="cpu-4" and agent.ctx.resume is None:
        # The fourth content update has run; withhold its task checkpoint before the PG write.
        marker("first",dict(prior=agent.versions,uncommitted=await signature(agent,None)))
        await asyncio.Event().wait()
    cp=await original_save(agent,step)
    if idx in {"cpu-1","cpu-2","cpu-3","cpu-4"}:
        sig=await signature(agent,cp)
        if not hasattr(agent,"versions"):agent.versions={}
        agent.versions[idx]=sig
        if idx=="cpu-4" and agent.ctx.resume is not None:
            marker("second",sig)
            await asyncio.Event().wait()
    return cp
Agent.save=save_probe
"""

def steps(_):
    old=SOURCES["csv-first-row"]["records.py"]
    read,write,smoke=BASE["csv_steps"](old)
    versions=[("write_file",{"path":"records.py","content":write[1]["content"]+f"\n# experiment version {i}\n"}) for i in range(1,5)]
    return [read,*versions,smoke]


def inject(opts,args,url,turn,token,ns):
    kube=["k3s","kubectl","--request-timeout=8s","-n",ns]
    evidence={"turn":turn,"namespace":ns,"faults":[]}
    def save():
        (args.evidence/"fault.json").write_bytes(BASE["encode"](evidence))
    def witness(name,node):
        until=time.monotonic()+args.timeout
        while time.monotonic()<until:
            listing=json.loads(subprocess.check_output(kube+["get","pods","-o","json"],timeout=14))
            for pod in listing["items"]:
                if pod["spec"].get("nodeName")!=node or pod["status"]["phase"]!="Running":
                    continue
                m=pod["metadata"]
                res=subprocess.run(kube+["exec",m["name"],"-c","sandbox","--","cat","/tmp/chain-"+name],capture_output=True,timeout=15)
                if res.returncode==0:
                    return dict(node=node,uid=m["uid"],pod=m["name"],record=json.loads(res.stdout),at=time.time())
            time.sleep(.4)
        raise TimeoutError(f"{node}: {name}")
    def verify_owner(attempt,cp):
        s=BASE["inspect_task"](args.agentbox,url,turn,token)
        assert s["task"]["current_attempt_id"]==attempt
        item=max(s["checkpoints"],key=lambda c:c["commit_seq"])
        assert item["checkpoint_id"]==cp and item["attempt_id"]==attempt
        return {"task":s["task"],"checkpoints":s["checkpoints"],"at":time.time()}
    first=witness("first",opts.source_node)
    states=first["record"]["prior"]
    assert set(states)=={"cpu-1","cpu-2","cpu-3"} and len({v["file_sha256"] for v in states.values()})==3
    c3=states["cpu-3"]
    assert first["record"]["uncommitted"]["file_sha256"]!=c3["file_sha256"]
    evidence.update(first=first,before_stop=verify_owner(c3["attempt"],c3["checkpoint"]))
    evidence["faults"].append({"operation":"B soft stop","at":time.time()})
    save()
    try:
        evidence["stop_receipt"]=stop_worker(opts.instance_id,opts.source_node,os.environ["TENCENTCLOUD_REGION"])
        save()
        second=witness("second",opts.target_node)
        evidence["second"]=second
        resumed=witness("restore",opts.target_node)
        evidence["first_restore"]=resumed
        save()
        c4=second["record"]
        evidence["before_start"]=verify_owner(c4["attempt"],c4["checkpoint"])
        save()
    finally:
        restored=restore_worker(opts.instance_id, opts.source_node, opts.target_node, os.environ["TENCENTCLOUD_REGION"])
        (args.evidence/"restoration.json").write_bytes(BASE["encode"](restored))
    assert second["uid"]==resumed["uid"] and second["uid"]!=first["uid"]
    evidence["after_b_ready"]=verify_owner(c4["attempt"],c4["checkpoint"])
    pod=json.loads(subprocess.check_output(kube+["get","pod",second["pod"],"-o","json"]))
    assert pod["metadata"]["uid"]==second["uid"] and pod["spec"]["nodeName"]==opts.target_node
    evidence["faults"].append({"operation":"delete only A experiment session Pod","uid":second["uid"],"at":time.time()})
    save()
    subprocess.run(kube+["delete","pod",second["pod"],"--wait=true","--grace-period=0","--force"],check=True,timeout=25)
    # Last attempt may finish in seconds. Its own PG-backed progress event is collected after grading.


def grant_snapshot(args, ns, turn):
    cmd=["k3s","kubectl","-n","agentbox-cvm-acceptance","exec","-i","postgres","--","psql",
         "-XAt","-v","ON_ERROR_STOP=1","-U","agentbox","-d","csv_"+ns.split("-")[-1],"-v","turn="+turn]
    sql="""SELECT json_agg(row_to_json(x)) FROM (SELECT a.attempt_no,a.attempt_id,
      a.created_at, aa.state,aa.revoked_at,aa.reason FROM attempts a JOIN attempt_access aa
      USING(task_id,attempt_id) WHERE a.task_id=:'turn' ORDER BY a.attempt_no) x;"""
    return json.loads(subprocess.check_output(cmd,input=sql,text=True,timeout=15))

def main():
    parser=argparse.ArgumentParser(add_help=False)
    for key in ["source-node","target-node","instance-id"]:
        parser.add_argument("--"+key,required=True)
    opts,remaining=parser.parse_known_args()
    sys.argv=[sys.argv[0],*remaining,"--scripted","--fixture",str(FIXTURES["csv-first-row"])]
    flags=["--k8s-preferred-node",opts.source_node,"--session-recovery-slots","2","--allow-session-lease-fence","--attempt-lease-ttl","120s"]
    def acceptance(args,source,data_dir,url,ns,after_submit):
        report=BASE["run_acceptance"](args,source,data_dir,url,ns,after_submit)
        grants=grant_snapshot(args,ns,report["turn_id"])
        (args.evidence/"grants.json").write_bytes(BASE["encode"](grants))
        return report
    out=BASE["main"](worker_command=lambda files: BASE["seed_command"](files,WORKER),
                     acceptance=acceptance,after_submit=lambda *a:inject(opts,*a),upstream=lambda:BASE["tool_upstream"](steps),gateway_flags=flags)
    report=json.loads((out/"result.json").read_bytes())
    run=json.loads((out/"solve-inspect.json").read_bytes())
    evidence=json.loads((out/"fault.json").read_bytes())
    events=[json.loads(l[6:]) for l in (out/"events.sse").read_text().splitlines() if l.startswith("data: ")]
    restores=[e["internal"]["payload"]["data"] for e in events if e.get("internal",{}).get("payload",{}).get("kind")=="chain_restore"]
    actuals=[evidence["first"]["record"]["prior"]["cpu-3"],evidence["second"]["record"]]
    checks={}
    for i,(old,new) in enumerate(zip(actuals,restores),1):
        checks[f"hop{i}"]={k:old[k]==new[k] for k in ("turn","checkpoint","messages","file_sha256","tool_ids")}
    proof={"restores":restores,"checks":checks,"attempts":run["attempts"],"cases":report["cases"],
           "remaining_resources":report["remaining_resources"],"pods":report["pods"]}
    (out/"chain-summary.json").write_bytes(BASE["encode"](proof))
    assert len(restores)==2 and len(run["attempts"])==3 and all(all(x.values()) for x in checks.values())
    assert report["verdict"]=="solved" and not report["remaining_resources"]
    verify(out, opts.source_node, opts.target_node)


if __name__=="__main__":
    main()
