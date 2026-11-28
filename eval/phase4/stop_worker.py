#!/usr/bin/env python3
"""Stop only the CVM matching the designated worker node, and emit the cloud receipt."""
import argparse
import json
import os
import subprocess
import time


def stop_worker(instance_id, node_name, region):
    node = json.loads(subprocess.check_output(["k3s", "kubectl", "get", "node", node_name, "-o", "json"],
                                               timeout=20))
    if any(key in node["metadata"]["labels"] for key in ("node-role.kubernetes.io/control-plane",
                                                         "node-role.kubernetes.io/master")):
        raise ValueError("refusing the cluster control-plane node")
    addresses = {a["address"] for a in node["status"]["addresses"] if a["type"] == "InternalIP"}
    def call(action, **extra):
        cmd = ["tccli", "cvm", action, "--region", region, "--InstanceIds", json.dumps([instance_id])]
        for key, value in extra.items():
            cmd.extend(["--" + key, value])
        result = json.loads(subprocess.check_output(cmd, timeout=30))
        return result.get("Response", result)

    def describe():
        row, = call("DescribeInstances")["InstanceSet"]
        if row["InstanceId"] != instance_id or not addresses.intersection(row["PrivateIpAddresses"]):
            raise ValueError("CVM ID or internal IP differs from the chosen K3s worker")
        return row

    if describe()["InstanceState"] != "RUNNING":
        raise ValueError("worker instance must be RUNNING")
    rid = call("StopInstances", StopType="SOFT", StoppedMode="STOP_CHARGING")["RequestId"]
    until = time.monotonic() + 180
    while time.monotonic() < until:
        row = describe()
        if row["InstanceState"] == "STOPPED":
            return {"instance_id": instance_id, "state": "STOPPED", "node": node_name,
                    "region": region, "request_id": rid, "stop_type": "SOFT"}
        time.sleep(2)
    raise TimeoutError(f"CVM did not confirm STOPPED, request {rid}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance_id")
    parser.add_argument("worker")
    args = parser.parse_args()
    print(json.dumps(stop_worker(args.instance_id, args.worker, os.environ["TENCENTCLOUD_REGION"])))
