#!/usr/bin/env python3
"""Power operations restricted to the CVM matching a designated Kubernetes worker."""
import argparse
from datetime import datetime, timezone
import json
import os
import subprocess
import time


KUBECTL = ["k3s", "kubectl", "--request-timeout=6s"]
CONTROL_LABELS = ("node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master")


class WorkerCVM:
    def __init__(self, instance_id, node_name, region):
        self.instance_id = instance_id
        self.node_name = node_name
        self.region = region
        node = json.loads(subprocess.check_output(KUBECTL + ["get","node",node_name,"-o","json"], timeout=15))
        if any(key in node["metadata"]["labels"] for key in CONTROL_LABELS):
            raise ValueError("refusing the cluster control-plane node")
        self.addresses = {a["address"] for a in node["status"]["addresses"] if a["type"]=="InternalIP"}

    def call(self, action, **options):
        args = ["tccli","cvm",action,"--region",self.region,"--InstanceIds",json.dumps([self.instance_id])]
        for key,value in options.items():
            args.extend(["--"+key,value])
        response = json.loads(subprocess.check_output(args,timeout=30))
        return response.get("Response",response)

    def state(self):
        instance, = self.call("DescribeInstances")["InstanceSet"]
        if instance["InstanceId"] != self.instance_id or not self.addresses.intersection(instance["PrivateIpAddresses"]):
            raise ValueError("CVM ID or internal IP differs from the chosen K3s worker")
        return instance["InstanceState"]


def stop_worker(instance_id, node_name, region):
    worker = WorkerCVM(instance_id,node_name,region)
    if worker.state() != "RUNNING":
        raise ValueError("worker instance must be RUNNING")
    rid=worker.call("StopInstances",StopType="SOFT",StoppedMode="STOP_CHARGING")["RequestId"]
    until=time.monotonic()+180
    while time.monotonic()<until:
        if worker.state()=="STOPPED":
            return {"instance_id":instance_id,"node":node_name,"region":region,
                    "request_id":rid,"stop_type":"SOFT","state":"STOPPED"}
        time.sleep(2)
    raise TimeoutError(f"CVM did not confirm STOPPED, request {rid}")


def restore_worker(instance_id, node_name, peer_name, region, timeout=420):
    """Start only the previously verified worker and observe both nodes and the worker kubelet."""
    if peer_name==node_name:
        raise ValueError("source worker must differ from peer")
    worker=WorkerCVM(instance_id,node_name,region)
    initial=worker.state()
    receipt=None
    start_sent=False
    until=time.monotonic()+timeout
    state=initial
    while time.monotonic()<until:
        if state=="RUNNING":
            break
        if state=="STOPPED" and not start_sent:
            start_sent=True
            try:
                receipt=worker.call("StartInstances")["RequestId"]
            except (subprocess.CalledProcessError,subprocess.TimeoutExpired):
                # A response can be lost after the start was accepted. Reconcile by state.
                if worker.state() not in {"STARTING","RUNNING"}:
                    raise
        elif state not in {"STOPPED","STOPPING","STARTING","REBOOTING"}:
            raise RuntimeError(f"unexpected worker state {state}")
        time.sleep(2)
        state=worker.state()
    if state!="RUNNING":
        raise TimeoutError(f"worker still {state}, start request {receipt}")
    while time.monotonic()<until:
        try:
            result=json.loads(subprocess.check_output(KUBECTL+["get","nodes","-o","json"],timeout=12))
            nodes={n["metadata"]["name"]:next((c["status"] for c in n["status"]["conditions"]
                                               if c["type"]=="Ready"),"Unknown") for n in result["items"]}
            if all(nodes.get(n)=="True" for n in (node_name,peer_name)):
                health=subprocess.check_output(KUBECTL+["get","--raw",f"/api/v1/nodes/{node_name}/proxy/healthz"],
                                               timeout=12,stderr=subprocess.DEVNULL).strip()
                if health==b"ok" and worker.state()=="RUNNING":
                    return {"initial":initial,"final":"RUNNING","start_request":receipt,
                            "nodes":nodes,"kubelet":"ok","verified":datetime.now(timezone.utc).isoformat()}
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired):
            pass
        time.sleep(2)
    raise TimeoutError(f"worker running but Ready and kubelet not confirmed, start request {receipt}")


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance_id")
    parser.add_argument("worker")
    parser.add_argument("--restore-with-peer",metavar="NODE",help="restore the worker then check its peer and kubelet")
    args=parser.parse_args()
    region=os.environ["TENCENTCLOUD_REGION"]
    if args.restore_with_peer:
        result=restore_worker(args.instance_id,args.worker,args.restore_with_peer,region)
    else:
        result=stop_worker(args.instance_id,args.worker,region)
    print(json.dumps(result))
