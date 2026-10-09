# Design: Kubernetes provider, OCI worker image, warm pool

> Status: implemented on branch `s3-k8s-provider`; measurements in `docs/evidence/2026-10-10-k8s-provider.md`. Contract: `docs/design/2026-10-05-provider-contract.md`.
> The local provider (`internal/provider/local`) stays the default; nothing changes unless the server is
> started with `--provider k8s`.

## 1. Goals and non-goals

Goals

- A second implementation of `provider.Provider` (`internal/provider/k8s`) that runs each environment in
  **one Pod**, passes the same contract conformance suite (`internal/provider/providertest`) as the local
  provider, and is selected by a single server flag (`--provider k8s`).
- Pod-level isolation: non-root, read-only root filesystem, all capabilities dropped, `RuntimeDefault`
  seccomp, no service-account token, no service links, deny-all NetworkPolicy, optional
  `runtimeClassName` (e.g. gVisor `runsc`).
- **Image pinning**: the worker image is referenced by digest. A tag is resolved to a digest once at
  startup; in strict mode a tag is refused. The pinned reference is exposed for run manifests.
- **Warm pool**: N pre-started, unassigned Pods per (image digest, resource profile); `Create` claims
  one and refills asynchronously; cold vs warm start latency is measured.
- A reproducible local demo on `kind` (`scripts/demo-k8s.sh`).

Non-goals

- Multi-node scheduling of the shared workspace (see §4: the workspace and the Gateway socket are shared
  through a node-local hostPath; on a multi-node cluster this needs a RWX volume or a socket proxy).
- `exec` environments (`/v1/exec`, kind `exec`): `Create` rejects them; run the server with
  `--exec-slots 0` under `--provider k8s`.
- Per-environment UID ranges: all sandboxes run as one fixed non-root UID; pods are isolated by
  namespaces, not by UID (`ReclaimUIDFiles`/`UIDFiles` therefore report nothing).
- Registry authentication for digest resolution (anonymous registries only).

## 2. Architecture

```
agentbox server (container on the docker host, or a Pod)          kind node (kubelet + containerd)
 ├─ app / runner / gateway  (unchanged)                            ┌ Pod abx-<rand>  (one per env)
 ├─ provider/k8s ──client-go──► kube-apiserver ──────────────────► │  container "sandbox": worker image@sha256
 │    Create  = claim warm Pod | create Pod, wait Ready           │   PID 1: agentbox-podagent init
 │    StartExec = pods/exec stream: podagent exec -- argv ───────►│   podagent exec → python3 -m <worker>
 │    Stop    = patch activeDeadlineSeconds=1 → phase Failed      │   /workspace      ← hostPath slot/workspace
 │    Destroy = delete Pod, verify NotFound                       │   /run/agentbox   ← hostPath slot/run
 └─ data dir /var/lib/agentbox-k8s  ◄──── same host path ────────►└  /tmp, /run/agentbox-exec ← emptyDir (memory)
```

### 2.1 Control channel: `pods/exec` + an in-image helper

The local provider talks to its sandbox init over a Unix socket and passes stdin/stdout/stderr pipes as
file descriptors. A Pod has no such channel, so `StartExec` uses the Kubernetes exec API (the same
SPDY/WebSocket stream as `kubectl exec`) to run a small static helper baked into the image:

`agentbox-podagent exec --id <exec_id> [--env K=V]… [--dir D] [--nofile N] -- argv…`

- It starts the workload in its own process group, applies `RLIMIT_NOFILE`, writes a pid file to
  `/run/agentbox-exec/<id>.pid`, and **then writes one acknowledgement line** `ABX-STARTED <pid>` on
  stdout before relaying the workload's stdout (piped through the helper so the ack is strictly first).
  A failed `exec` produces `ABX-START-ERR <reason>` and exit code 127. The provider consumes this line;
  callers see exactly the workload's stdout. This is the k8s equivalent of `start_ack` / `start_err`.
- stdin and stderr are inherited; closing `Stdin()` closes the remote stdin stream.
- On exit the helper writes `/run/agentbox-exec/<id>.status` (`{code, signal}`) and exits with the code
  (or `128+signal`). The exec API only reports an exit code, so for codes ≥ 128 the provider reads the
  status file (`podagent status`) to distinguish "killed by signal N" from "exited with 128+N".
- `Terminate(grace)` runs `podagent kill --id <id> --grace-ms <ms>` (SIGTERM to the process group,
  SIGKILL after the grace period).
- A broken stream without an exit status is `ErrControlLost`, as in the local provider.

Alternatives considered: (a) an in-pod agent serving gRPC/TCP — needs a network path from the control
plane into every Pod, an auth scheme and a NetworkPolicy hole; (b) attach to the container's main
process — only one process per container. The exec API reuses kube-apiserver authn/authz and needs no
inbound network access to the Pod, so it is the simplest correct option.

`Freeze`/`Thaw`/`Procs`/`ResourceDiag` are also helper subcommands: `freeze` sends SIGSTOP to every
process in the container except PID 1, re-lists `/proc` and stops newly forked processes, and succeeds only
when two consecutive passes see the same set of processes, all in state `T` (a stopped process cannot fork,
so a child forked between a listing and its parent's stop is caught by the next pass); `thaw` sends
SIGCONT; `procs` lists the container's pids; `diag` reads `cpu.stat` and `memory.events` of the
container's own cgroup (cgroup namespace). An OOM that kills the container (kubelet sets
`memory.oom.group`) is reported from the container status (`OOMKilled`). Pids are container-namespace
pids, which is all `Procs` is used for (baseline comparison).

**Trust boundary.** The helper runs with the same UID and in the same PID namespace as the untrusted
workload. Everything it produces inside the Pod — pid and status files in `/run/agentbox-exec`, the ack
line, `freeze`/`procs`/`diag` answers — can be tampered with by the workload, which can also signal or kill
helper processes. Helper output is therefore advisory and only affects the workload's own attempt (its
reported exit status, CPU accounting, whether a freeze is confirmed). Every safety decision relies on
API-server facts: container state and exit for `Stop`/`Destroy`, `OOMKilled` from the container status,
`activeDeadlineSeconds` for killing, Pod deletion (verified NotFound) for cleanup. A workload that defeats
`freeze` gains only CPU time in its own Pod; it cannot escape `Stop`.

### 2.2 Workspace and Gateway socket: node-local shared hostPath ("slots")

The runner reads artifacts from the host workspace while the worker runs, and the Gateway identifies an
attempt by the per-attempt Unix socket it accepts on. Both therefore need a filesystem shared between the
control plane and the Pod. The provider requires a data directory that is the **same path** for the
server and the node (on kind: an `extraMounts` entry; the server runs in a container that bind-mounts the
same docker-host path).

Every Pod gets a slot directory `<data>/k8s-slots/<pod>/{workspace,run}` mounted as hostPath at
`/workspace` and `/run/agentbox`. Because a warm Pod's volumes are fixed before its environment is known,
the environment is bound to the slot at claim time:

- **Gateway socket**: `link(2)` the Gateway's socket into `slot/run/gateway.sock` and chown it to the
  sandbox UID. Connecting through a hard link reaches the same socket inode, so the Gateway's per-attempt
  identity is unchanged; revoking the socket (closing the listener) revokes access through the link.
- **Workspace**: the app created `<data>/workspaces/<id>` (a plain directory, or a symlink left by a
  previous environment). Its entries are moved (same-filesystem `rename`) into `slot/workspace`, and the
  workspace path is atomically replaced by a symlink to the slot. The host side keeps using the same path;
  the next environment for the same task/session moves the entries on. A host-only back-reference file
  (`<slot>/workspace.ref`, not mounted into the Pod) records the workspace path. `Destroy` keeps a slot's
  workspace while that path is still a symlink to it; a periodic slot GC (every minute, slots older than
  10 min) removes slots whose Pod is gone and whose workspace path no longer points at them — e.g. after
  a session is closed and its workspace directory (with the symlink) is deleted. The Pod annotation
  `agentbox.io/workspace-sha256` carries only a hash of the host path.
- **Session restore files** (`Mounts.RestoreDir`): hard-linked into `slot/run/restore/`; the provider
  removes them on `Stop`. Semantics differ from the local provider's read-only bind of the directory: a
  file the host deletes from `RestoreDir` stays visible in the Pod (the hard link keeps the inode) until
  `Stop`, and the Pod sees the files present at `Create` time, not files added later.

Trade-off: this is single-node. On a multi-node cluster the same contract would be met by a RWX volume for
workspaces and a socket proxy for the Gateway (an in-pod Unix listener that forwards over an authenticated
connection), at the cost of an authenticated network path out of the sandbox. With the slot design the
NetworkPolicy can deny **all** egress, because the only way out of the Pod is the Unix socket.

### 2.3 Object model and ownership

Namespace (default `agentbox-sandbox`). Labels/annotations:

| key | value | meaning |
|---|---|---|
| `agentbox.io/managed` | `true` | every Pod the provider creates (Scan selector) |
| `agentbox.io/install` | install id | owning installation |
| `agentbox.io/pool` | pool key | warm pool membership (digest + profile hash) |
| `agentbox.io/state` | `warm` \| `assigned` | warm = unassigned |
| `agentbox.io/env` | env id (or `h-<sha256>` if not a valid label value) | environment binding |
| annotation `agentbox.io/owner` | `{"install_id","env_id","spec_hash","kind"}` | the owner.json equivalent |

`owner` is written in the same API call that binds the Pod to the environment (Pod creation for a cold
start, an `Update` with a `resourceVersion` precondition for a warm claim — so two creators can never
claim the same warm Pod). Mapping of the contract:

| contract | local provider | k8s provider |
|---|---|---|
| env dir + owner.json | `<data>/envs/<id>/owner.json` | Pod labels + `agentbox.io/owner` annotation |
| init ready | control conn open | Pod `Ready` + in-process state (gate open) |
| `Stop` authoritative check | cgroup absent or `populated 0` | Pod absent, phase `Succeeded`/`Failed`, or every container `terminated` (restartPolicy Never; the phase follows ~2 s later) |
| kill | `cgroup.kill` | patch `spec.activeDeadlineSeconds=1` → kubelet kills all containers, Pod object stays; plus a best-effort `podagent shutdown` exec (SIGTERM to PID 1) that ends the container in ~0.3 s |
| `Destroy` layers | mounts → cgroup → dir | Pod deleted (verified NotFound) → slot `run/` removed |
| `Scan` | dirs, mounts, cgroups, listeners | all `agentbox.io/managed` Pods in the namespace; warm Pods of this install are pool capacity, not environments, and are not reported |
| execution gate | in-process | identical (copied semantics) |

`Stop` uses `activeDeadlineSeconds` rather than an exec-based kill because it only needs the API server and
the kubelet — it works when the helper, the container or the exec path is wedged. PID 1 (`podagent init`)
SIGKILLs every process in the container on SIGTERM so termination is fast. The kubelet only acts on the deadline at its next pod sync (3–5 s on kind), so `Stop` also execs `podagent shutdown` as a fast path; with it, Stop completes in about 1 s on kind.

Idempotency and residue follow the contract: a bound Pod of this install that is not ready in this
process (interrupted create, previous server process, stopped) is `ErrIncomplete`; a different
`spec_hash` is `ErrConflict`; a Pod with the env label but a missing/corrupt/foreign owner annotation is
`ErrForeign` and is never deleted. Freeze is supported via SIGSTOP (see 2.1). `OpenOutputs` exists only for
exec environments, which this provider does not create, so it returns `ErrNotFound`/an error per contract.

### 2.4 Pod spec

- `restartPolicy: Never`, `automountServiceAccountToken: false`, `enableServiceLinks: false`,
  `terminationGracePeriodSeconds: 2`, optional `runtimeClassName`.
- Pod `securityContext`: `runAsNonRoot`, `runAsUser/runAsGroup 10001`, `seccompProfile: RuntimeDefault`.
- Container `securityContext`: `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`,
  `capabilities.drop: [ALL]`, `privileged: false`.
- Resources: `requests = limits`: memory = `Limits.MemoryMax`, cpu = `CPUQuotaUs/100000` cores. `/tmp` is a
  memory-backed `emptyDir` with `sizeLimit = TmpBytes` (counts against the memory limit, like the local
  tmpfs). `PidsMax` has no per-Pod field; it is the kubelet's `podPidsLimit`. **Deployment
  prerequisite:** set `podPidsLimit` on the sandbox nodes (`deploy/k8s/kind-config.yaml` sets 512);
  without it a fork bomb is bounded only by the node. `NoFile` is applied by the helper.
- NetworkPolicy `agentbox-sandbox-deny-all` (created at startup) selects `agentbox.io/managed=true` with
  `policyTypes: [Ingress, Egress]` and no rules.
- **Admission policy** (`deploy/k8s/admission-policy.yaml`, a ValidatingAdmissionPolicy bound to the
  namespace). The hostPath slot volumes rule out Pod Security "baseline", so the namespace cannot enforce
  PSS, and `pods: create` alone would let the server's ServiceAccount mount any host path or run privileged
  Pods. The policy applies to *every* Pod in the namespace (not only labelled ones). It requires:
  - hostPath only below the slot prefix, and no `..`;
  - no host namespaces, no hostPort, no ServiceAccount token, no init or ephemeral containers;
  - `runAsNonRoot` with a non-zero UID and `RuntimeDefault` seccomp;
  - per container: no privilege escalation, not privileged, read-only root, drop `ALL` and add nothing.

  `scripts/demo-k8s.sh` applies it and checks that a violating Pod is rejected. RBAC is limited to
  Pods (no `deletecollection`), `pods/exec`, and `create` on NetworkPolicies.

  **Residual risk:** a compromised control plane holding the ServiceAccount token can still create
  compliant Pods with any image, mount *another* environment's slot (the prefix is shared), and exec into
  any sandbox Pod in the namespace. Closing that would need per-environment admission (for example a
  policy parameter binding slot paths to Pod names) or a node-side volume plugin.

### 2.5 Image pinning

`--k8s-image` accepts `repo[:tag]` or `repo@sha256:<64 hex>`.

- Digest reference → used verbatim.
- Tag + `--k8s-image-strict` → startup error ("mutable tag refused").
- Tag otherwise → resolved once at startup with the registry API (`HEAD /v2/<repo>/manifests/<tag>`,
  `Docker-Content-Digest`; OCI index / manifest list / manifest accepted; `--k8s-registry-plain-http` for a
  local registry), then pinned as `repo@sha256:…`. Pods never reference the tag.
- `Provider.Image()` returns the pinned reference; the server logs it at startup and every Pod carries it
  as annotation `agentbox.io/image`, so an eval run manifest can record exactly which image ran.

### 2.6 Warm pool

- Pool key = `sha256(image digest, memory, cpu, tmp, runtimeClass)[:16]`; the profile is the server's
  default task limits. A `Create` whose limits match the profile claims a warm Pod; any other spec, an
  empty pool, or a failed claim falls back to a cold start (counted).
- Claim: list `state=warm, pool=<key>` Pods that are `Ready`, `Update` the first one with the owner
  annotation, env label and `state=assigned` under the listed `resourceVersion`; on conflict try the next.
- A background loop keeps `N` warm Pods (counting pending ones), deletes warm Pods of other pool keys
  (e.g. after an image change) and failed warm Pods, and refills immediately after each claim. Warm Pods
  are single-use: a Pod is destroyed with its environment, never returned to the pool. A warm Pod that is
  not Ready after `WarmReadyTimeout` (default 3 min: Pending, ImagePullBackOff, unschedulable) is logged
  with its waiting reason and replaced.
- Resource accounting: warm Pods hold their requests (memory, CPU) on the node while idle; they are **not**
  counted in the server's admission capacity (`--memory-bytes`, run slots), which only counts assigned
  environments. Size the node for `run slots + pool size` Pods of the pool profile.
- Metrics: every `Create` records `path=warm|cold` and the latency to "ready for StartExec";
  `Provider.Stats()` returns P50/P95 per path, and the server logs each start.

## 3. Failure handling

| failure | behaviour |
|---|---|
| API server unavailable | operations return wrapped errors (transient host errors for the coordinator) |
| Pod never becomes Ready (pull error, unschedulable) | `Create` waits until ctx deadline (default 2 min), returns the ctx error; the bound Pod is residue (`ErrIncomplete`) and is stopped/destroyed by the coordinator |
| Pod fails while running (OOM, node loss) | the exec stream breaks → `ErrControlLost`; `ResourceDiag.OOMObserved` from container status |
| control plane restarts | gates are not rebuilt; `List` reports existing Pods as incomplete; recovery stops and destroys them (contract §5) |
| warm Pod dies before claim | the refill loop deletes and replaces it; a claim only considers `Ready` Pods |
| conflicting claim | optimistic concurrency via `resourceVersion`; loser tries the next Pod |

## 4. Testing

- Unit: the full `providertest` suite against a fake clientset (`k8s.io/client-go/kubernetes/fake`) plus a
  fake kubelet (marks Pods Running/Ready, honours `activeDeadlineSeconds`) and an in-process fake of the
  exec helper; warm-pool claim/refill/conflict, image reference parsing and digest resolution against an
  `httptest` registry, Pod spec security fields, owner/label classification. Runs in CI.
- Helper: `cmd/agentbox-podagent` unit tests (ack line ordering, exit/signal status, kill, rlimit).
- Integration (opt-in, `AGENTBOX_K8S_TEST_KUBECONFIG` + `AGENTBOX_K8S_TEST_IMAGE`): the same conformance
  suite against a real cluster, plus a cold-vs-warm latency measurement.
- Demo: `scripts/demo-k8s.sh` — kind cluster + local registry, image build/push, server in a container
  with `--provider k8s`, fake upstream, tasks including a stop and a server crash/restart, printed latency.

## 5. Demonstration

`docs/evidence/2026-10-10-k8s-provider.md` records the measured cold/warm P50/P95, the conformance result
on kind, the pod security checks (`id`, read-only root, egress blocked) and the stop/crash-recovery runs.
