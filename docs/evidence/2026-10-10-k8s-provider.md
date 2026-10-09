# Evidence: Kubernetes provider, digest-pinned worker image, warm pool (2026-10-10)

Design: [docs/design/2026-10-10-k8s-provider-design.md](../design/2026-10-10-k8s-provider-design.md).

## Environment

- Developer workstation: Windows 11, WSL2 (kernel 6.6.87.2), 24 logical CPUs, 15 GiB RAM visible to WSL.
- Docker Desktop 29.4.2 (WSL2 backend). kind v0.30.0, node image `kindest/node:v1.34.0` (Kubernetes 1.34,
  single node, default CNI kindnet).
- Local registry `registry:2` (`kind-registry:5000` in the cluster).
- Worker image built from `deploy/k8s/worker.Dockerfile`. The server pinned the `:demo` tag to
  `sha256:a59acf41a0cb5f9edf854005285f3ad24821ad51aed7bb3d8c16b3c184d9df57` at startup.
- Pod limits: 256 MiB memory and 1 CPU in the demo; 256 MiB and 0.5 CPU in the integration test.

These are single-machine numbers on a developer laptop. They are not measured on a production cluster.

## 1. Contract conformance

The provider runs the same `internal/provider/providertest` suite as the local provider. It passes in two
setups:

| Target | How | Result |
|---|---|---|
| Fake clientset + fake kubelet + fake helper | `go test ./internal/provider/k8s/` (runs in CI) | 11/11 subtests pass |
| Real kind cluster, real image | `go test -run Real ./internal/provider/k8s/` with `AGENTBOX_K8S_TEST_*` | 11/11 subtests pass in 16.2 s |

Subtests covered:
- Create, then List reports the environment as complete.
- Idempotent Create.
- `ErrConflict`.
- Residue gives `ErrIncomplete`, and Stop → Destroy → Create rebuilds it.
- A foreign Pod is neither claimed nor deleted.
- StartExec echoes output and the exit code (3).
- StartExec on an unknown environment gives `ErrNotFound`.
- Stop terminates the execution and closes the gate.
- Stop while a start is in flight.
- Destroy requires Stop first and verifies the Pod is gone.
- OpenOutputs errors.

The exec-environment case is skipped because this provider does not support exec environments. The real
cluster was not tested under `-race`, because the WSL environment has no gcc. CI runs the fake-cluster suite
under `-race`.

## 2. Start latency: cold vs warm

### a) Provider level

`TestStartLatencyOnRealCluster`: 20 sequential starts per path. "Ready" is the time `Create` takes to return
a Pod that accepts `StartExec`. "First output" adds `StartExec` of `python3 -c "print('ok')"` and reading its
first line.

| Path | n | Ready P50 | Ready P95 | First output P50 | First output P95 |
|---|---|---|---|---|---|
| cold (new Pod) | 20 | 978 ms | 987 ms | 1051 ms | 1110 ms |
| warm (claimed from the pool, size 2) | 20 | 11 ms | 16 ms | 78 ms | 104 ms |

### b) End to end through the server

`scripts/demo-k8s.sh` step 10 measures this. Each sample is one `sim_worker` task. "Env ready" comes from the
provider's log line `k8s: environment ready`. "Submit→terminal" is wall time measured by the script, from
`task submit` to the task's terminal status. That includes admission, Gateway binding, Worker start, the
protocol handshake and polling at 0.2 s.

| Path | n | Env ready P50 / P95 | Submit→terminal P50 / P95 |
|---|---|---|---|
| warm (default task resources = pool profile) | 20 | 9 / 12 ms | 438 / 690 ms |
| cold (`memory_max` differs from the pool profile) | 20 | 1620 / 1761 ms | 2155 / 2392 ms |

The cold "env ready" figure is higher through the server than in the provider test. In the demo, warm Pods
are being refilled while cold Pods start, and the kubelet starts them at the same time.

### Caveats

- **Warm is the best case.** Every warm sample waited for a ready warm Pod before starting, so the pool
  was never exhausted. When more Creates arrive at once than the pool holds, the extra ones are cold
  starts. They are counted as `path=cold`; `TestBurstOnRealCluster` measures that case: N concurrent
  Creates against a pool of 2. Measured on kind with N = 6: 2 Creates claimed warm Pods (P50 11 ms) and 4
  started cold (P50 2989 ms, P95 3017 ms). Overall P50 was 2985 ms: concurrent cold starts on one node are
  slower than the sequential cold starts above.
- **Cold does not include an image pull.** The image was already on the node, so the cold numbers are
  scheduling, container start and readiness only. A first start on a node without the image adds the
  pull time.
- **Scale.** Single node, sequential samples, laptop hardware.

## 3. Pod isolation, checked inside a running sandbox

Step 8 of the demo. A task is running in a claimed Pod, and the script runs a probe through `kubectl exec`:

```
image           kind-registry:5000/agentbox-worker@sha256:a59acf41…
pod security    {"runAsGroup": 10001, "runAsNonRoot": true, "runAsUser": 10001, "seccompProfile": {"type": "RuntimeDefault"}}
container sec   {"allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}, "privileged": false, "readOnlyRootFilesystem": true}
resources       {"cpu": "1", "memory": "256Mi"}
uid/gid         10001 10001
rootfs          read-only (Read-only file system)
SA token        absent
gateway socket  True
egress 1.1.1.1 80          blocked (TimeoutError)
egress kind-registry 5000  blocked (Temporary failure in name resolution)
egress 10.96.0.10 53       blocked (TimeoutError)
capabilities    0000000000000000
```

The server itself uses a ServiceAccount token from `deploy/k8s/sandbox.yaml`. That token is limited to Pods,
`pods/exec` and NetworkPolicies in `agentbox-sandbox`. As that identity,
`kubectl auth can-i create pods -n default` returns `no`.

**Admission policy** (`deploy/k8s/admission-policy.yaml`, kind 1.34, demo step 4). These are probe Pods submitted
as the server's ServiceAccount with `--dry-run=server`:

```
compliant (provider shape)              admitted
hostPath /                              DENIED: only emptyDir volumes and hostPath volumes of the form <slot dir>/abx-<12 hex>/(workspace|run) are allowed
hostPath below a slot (symlink-shaped)  DENIED: (same rule)
container runAsNonRoot: false           DENIED: containers must be unprivileged: … no runAsNonRoot/runAsUser/seccomp override
container seccomp Unconfined            DENIED: (same rule)
projected ServiceAccount token volume   DENIED: only emptyDir volumes and hostPath volumes … are allowed
```

With the policy active, the server's real Pods (warm and cold) were admitted, and all 11 demo steps passed. The
policy protects against a leaked token. It does not protect against a compromised server process that runs as
root on the node; see design §2.4.

Known gap, observed during design experiments: kindnet's NetworkPolicy implementation does not filter
traffic to host-network endpoints. A sandbox Pod could still open a TCP connection to the API server's node
address. It has no ServiceAccount token, so only anonymous discovery endpoints answer. With Calico or
Cilium, the deny-all policy also covers that path. A `runtimeClassName` (for example gVisor) is supported
(`--k8s-runtime-class`) but was **not** exercised: gVisor is not installed on kind.

## 4. Behaviour through the full server (`scripts/demo-k8s.sh`, run of 2026-10-10)

All 11 steps passed; exit code 0. Full log: kept by the author, not committed.

- **Gateway and workspace (task A).**
  - What ran: `/v1/search` and `/v1/chat/completions` through the attempt's Gateway socket, which is
    hard-linked into the Pod's slot. Both calls completed with `tries=1`.
  - Workspace: the artifact written to `/workspace/out/<attempt>/report.md` reached the host through the
    slot.
  - Result: `task result --artifact report` passed the sha256 check.
  - Pod path: the environment was served by a warm Pod (9 ms).
- **Stop (task B).**
  - Action: `task cancel` was sent while the Worker was running.
  - Result: the task reached `cancelled` 0.44 s after the cancel.
  - Cleanup: the Pod was stopped through `activeDeadlineSeconds` plus `podagent shutdown`, then deleted by
    the cleanup loop.
- **Crash recovery (task C).**
  - Action: the server container got SIGKILL after checkpoint `s1`. The orphan Pod kept running.
  - Restart: after the restart, startup recovery listed and scanned the Pods by label, then stopped and
    destroyed the orphan.
  - Attempts: attempt 1 was `lost_on_restart`; attempt 2 resumed from `s1` and `succeeded`.
- **Shutdown.** SIGTERM gave exit code 0. No Pod bound to an environment was left; the 2 warm Pods were
  left for reuse.

## What this does not show

- Multi-node behaviour: the workspace and Gateway socket use a node-local hostPath (design §2.2).
- Resistance to a malicious workload beyond the Pod security settings above. Isolation is container-level,
  with a shared kernel, unless a sandboxed runtime class is used.
- Exec environments (`/v1/exec`): they are not supported by this provider.
- Long-running stability, or the pool under concurrent bursts larger than its size. Those Creates fall back
  to cold starts, which are counted as `path=cold`.
