# Phase 4 cloud recovery probe

Run from the checkout on Tencent CVM A. One Gateway and one PostgreSQL database serve all sessions in a run. The scripted upstream drives the real Agent tool protocol on CPU. The three tasks cover a CSV patch, a two-file catalog repair, and reconciliation across input ledgers with an intermediate file read back on the next turn.

## Prepare the two nodes

1. Confirm A's actual instance ID, private address and K3s control-plane identity after starting A. Place B in the same VPC. Select the worker's actual Kubernetes node name and the matching Tencent instance ID. Kubernetes hostname labels must equal the node names used below.
2. Build the Linux agentbox binary from this checkout. Make the same digest-pinned worker image available on both nodes. Confirm both nodes are Ready and schedulable. Confirm PostgreSQL in namespace agentbox-cvm-acceptance accepts connections from the Gateway. The pre-existing single-node acceptance fixture provisions this PostgreSQL instance.
3. Make the Gateway TCP relay reachable from managed worker Pods on A and B via A's private address. Keep both K3s traffic and the relay on the VPC. The Gateway owns the relay listener and creates per-Pod socket credentials. Give A a role scoped to the experiment COS bucket/prefix. Worker Pods must not receive Tencent credentials. Use the cluster's enforced NetworkPolicy and verify relay-only egress on **both** nodes before enabling lease fencing. The batch additionally tests the B Pod's actual relay connection, CVM metadata and PostgreSQL ports before requesting shutdown. These checks cover those three destinations.
4. Use an approved operator profile for Tencent's CLI on A, separate from the Pods. The included stop helper reads only the specified instance and stops it after checking its private address against B. It refuses control-plane nodes and records the CVM request ID and final state. Set TENCENTCLOUD_REGION to ap-shanghai. This helper requests a graceful whole-instance shutdown. Run the node-loss command only during the approved B outage window. B must be restarted and Ready before the next node-loss run.
5. Set AGENTBOX_COS_BUCKET_URL, AGENTBOX_COS_CVM_ROLE and an experiment-specific AGENTBOX_COS_PREFIX. Record the Git commit and worker image digest. Before the B run, establish the relay egress prerequisites on A and B using the same worker image and policy. Reserve at least five scheduling slots on B for the simultaneous barrier. With a 4-core, 8-GiB node, check actual CPU/memory requests and existing Pods first.

## Run on A

Use an empty output directory name for each run. Run with the same account and K3s permissions as the previous cloud acceptance. The shared driver starts its own Gateway and creates a fresh database and namespace. Supply actual values for A, B, A_IP, B_ID, BIN and WORKER_IMAGE.

```bash
k3s kubectl get nodes -o wide -L kubernetes.io/hostname
k3s kubectl -n agentbox-cvm-acceptance get pod postgres -o wide
common=(--source-node "$B" --target-node "$A" --agentbox "$BIN"
        --worker-image "$WORKER_IMAGE" --relay-listen "$A_IP:19446"
        --model phase4-cpu --timeout 900)
python3 eval/phase4/batch.py --mode reference --tasks 3 "${common[@]}" --evidence /var/tmp/p4-ref
python3 eval/phase4/batch.py --mode node-loss --tasks 5 "${common[@]}" --instance-id "$B_ID" \
  --stop-instance "$PWD/eval/phase4/stop_worker.py" --evidence /var/tmp/p4-loss
```

The reference pass releases each checkpoint barrier on B. The node-loss pass enables a 30-second database lease and session fencing, waits for each registered task's first file mutation and durable checkpoint, and records B Pod UID and actual node before requesting B shutdown. A remains running. The monitoring window starts before the request. Detection, scheduler convergence and startup may overlap. Workers on A emit an internal recovery witness then continue immediately. The first successful tool result on the same replacement attempt ends the measured interval.

## Inspect the evidence

Read summary.json, batch.json, node-samples.jsonl and the per-task result.json, solve-inspect.json and event stream. Cross-check old/new Pod UID, nodeName, checkpoint ID, message hash, successor attempt, successful tool journal and independent checker cases. Compare tasks submitted, tasks affected, tasks recovered and grading checks separately. One B shutdown affecting five sessions contributes one injection. Node samples measure the whole A node. The count of unscheduled Pods applies to the experiment namespace. Keep raw data/ and the Kubernetes watch private because they may contain tokens or Pod launch configuration. Review and redact evidence before publishing.

The B shutdown takes place after a committed checkpoint. For a missing checkpoint upload, a pre-commit failure or a lost acknowledgement, independently record the affected component, the last durable checkpoint and the result of replay. Because the Gateway, PG and COS writer run outside B, shutting down B alone cannot guarantee a particular COS-to-PG timing window. An application injection must be reported separately from a CVM outage. A second takeover and old-worker revocation require additional trials. Confirm B rejoins on its own instance ID and check remaining Pods and authorization after B returns.

## Local checkpoint chain

On Linux with a disposable PostgreSQL admin DSN in AGENTBOX_TEST_DATABASE_URL, run the command below. The PostgreSQL test fixture creates its own short-lived database. The test uses real coordinator authorization, real workspace reads/writes, real PG head selection and separate empty local blob stores. The archive is a filesystem test implementation that interrupts a selected upload after one byte. The checkpoint state contains a deterministic tool cursor. The batch records the complete Agent message hash separately.

```bash
go test ./internal/persistence/postgres -run '^TestColdWorkspaceCheckpointChain$' -v -count=1
```

One case exercises two cold restorations. After C1, C2 and C3 complete, a partial C4 upload causes attempt 2 to load C3 from PG and read C3 from the restored file. Attempt 2 successfully publishes C4. After a second simulated fault, attempt 3 loads C4 and its first read returns C4. The [local run log](../../docs/evidence/2026-11-27-phase4-local/cold-chain.log) records these assertions. Cross-node Pod scheduling, real COS and cloud outage timing are measured by their respective cloud trials.
