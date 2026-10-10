# Sandboxed DeepResearch Assistant (Go + Python)

**A chat-style DeepResearch assistant running on a self-built agent runtime.** Go owns execution and every security boundary. Python owns the research logic.

[简体中文](README.md) · [Usage and operations reference (中文)](docs/usage.zh-CN.md) · [Design spec (中文)](docs/design/2026-10-03-v0.2-first-release-design.md)

A user asks a question in a chat UI. The agent either answers directly or starts a deep research turn:

1. It reads its research skill.
2. If the scope is unclear, it asks up to three multiple-choice questions.
3. It writes a todo list.
4. It researches 2–4 sub-topics in parallel.
5. It writes a report with numbered citations to the pages it actually read.

The user can stop at any point, see a short summary of what was found, and then continue, have the report written from what exists so far, or restore a cancelled turn later.

Underneath is the part this project is really about: a single-host **agent runtime** that runs untrusted-ish agent code in Linux sandboxes. It survives crashes and restarts without losing committed work, and it keeps API keys out of the sandbox entirely.

## Architecture

```
Browser (Vue 3) / CLI ── REST + SSE ──► Go control plane
                                         ├─ task & session state machines, admission, scheduling
                                         ├─ checkpoints, crash recovery, event replay (Last-Event-ID)
                                         ├─ Gateway: credential isolation, call journal, budgets,
                                         │           per-turn tool quota, SSRF-safe egress, exec scheduling
                                         ├─ PostgreSQL (only source of truth)   Redis (cache only)
                                         └─ Sandbox: user/pid/mount/net namespaces, mapped UIDs,
                                            read-only rootfs, seccomp, capabilities {KILL}, cgroup v2
                                                   │
                         ┌─────────────────────────┴─────────────────────────┐
              Orchestration sandbox: Python agent                 Exec sandbox: model-written Python
              (talks to the Gateway over a Unix socket)           (no network, no Gateway, one-shot)
```

## What it does

| Area | What is implemented |
|---|---|
| **Crash-safe execution** | Workers commit checkpoints to the host. A killed worker, a killed server or a lost database connection resumes from the last committed checkpoint. `agentbox verify-invariants` checks the stored state against 16 invariants; a fault-injection mode drives the crash paths in tests. |
| **Credential isolation** | Model and search API keys live only in the host process. The sandbox reaches providers through a per-attempt Unix-socket Gateway that journals every call, enforces budgets and can revoke access mid-call. The research demo (`scripts/demo-m2.sh`) reads `/proc/<pid>/environ` and the command line of every sandbox process to show that no key is visible. |
| **Sandbox** | Namespaces, mapped UID ranges with reuse checks, a read-only rootfs template, seccomp, capability set `{KILL}`, and cgroup v2 limits for memory, pids and CPU. Execution trees are confirmed empty before cleanup. |
| **Sessions** | Each chat session is one long-lived sandbox process. It is frozen after 10 idle minutes (cgroup freezer), evicted after 1 hour, and restored on the next message from its last committed session checkpoint. |
| **Agent** | A tool-calling agent with progressive-disclosure skills (`read_skill`), `ask_user`, a todo list, `web_search`, `web_fetch` and `run_python`. Each turn gets 30 search and fetch calls, enforced by the Gateway rather than the model. |
| **Workspace tools and MCP (optional)** | `--workspace-tools`: a per-turn workspace with `write_file` / `read_file` / `list_dir` and `exec_shell` (each command in a fresh exec sandbox, files persist between commands, exec quota accounting, paths confined to the workspace, symlinks never kept). `--mcp-config`: the Gateway is the MCP client (stdio or streamable HTTP) for operator-configured servers, exposes only allowlisted tools, journals each call and counts it against the turn's tool budget; the sandbox never touches MCP servers or their credentials. [Design](docs/design/2026-10-10-shell-file-mcp-design.md), [record](docs/evidence/2026-10-10-shell-file-mcp.md). |
| **Parallel sub-runs** | 2–4 sub-topics run concurrently inside one turn under a two-level ledger (task and sub-run). They can be cancelled, and they resume after stop or crash without re-running finished sub-topics. |
| **Model fallback** | A logical model can be served by an ordered chain of OpenAI-compatible providers: retryable failures fail over to the next provider immediately, and every provider attempt is a separately priced try in the journal. Per-provider circuit breakers; when all are open the Gateway answers `503 model_degraded` at once; optional hedged requests. Replay and fingerprints are unaffected. |
| **Exec sandbox** | Model-written Python runs in a fresh, network-less environment per call, with separate UIDs, CPU and wall quotas and output collection. |
| **Kubernetes provider (optional)** | `--provider k8s` runs each environment in one Pod: the control channel is an in-image helper reached through pods/exec; non-root, read-only root, all capabilities dropped, no ServiceAccount token, deny-all NetworkPolicy; the worker image is pinned by digest; a warm pool cuts environment start from ~1 s to ~10 ms. It passes the same contract conformance suite as the local provider. |
| **Evaluation platform** | `agentbox eval` runs a suite (YAML/JSON) of coding/terminal tasks (checkers run in the exec sandbox) and research questions through the operator API, with concurrency, repetitions and a seed; a manifest pins the suite hash, server build, models, worker version and exec digest; trajectories (events, call journal, ledger) are exported as JSONL; deterministic graders (exit code, output, locatable citations) plus an optional budgeted LLM judge; reports and `eval compare`. Runs against the fake upstream at zero cost. |
| **Product** | Accounts (PBKDF2, `HttpOnly` sessions, rate-limited login), per-user isolation (other users' data always returns 404), server-side redaction of costs, models and internal IDs, a CSP and sanitized Markdown rendering. |
| **Observability** | OpenTelemetry tracing with one trace per user action: API request → task actor → attempt → worker start, handshake and checkpoints → **into the sandbox** (the W3C `traceparent` is handed to the worker over the protocol; the stdlib-only Python SDK forwards it) → every model, search, fetch and exec call and each retry at the Gateway. Prometheus metrics (outcomes, start and stop latency, upstream latency and errors, cost, tool calls, sandboxes and slots); JSON logs carry `trace_id`. Off by default; `deploy/observability` runs Collector, Tempo, Prometheus, Loki and Grafana locally ([design](docs/design/2026-10-10-observability-design.md), [demo run](docs/evidence/2026-10-10-observability.md)). |

## Evidence

All numbers come from recorded runs. The evidence files say exactly what each run did and did not show.

- **Tests:**
  - about 700 Go test functions, including real-sandbox end-to-end tests run as root;
  - about 280 Python tests;
  - about 170 web component tests.
- **CI (6 jobs):**
  - non-root suite with `-race`;
  - root suite in real sandboxes on `ubuntu-24.04`;
  - Python 3.11 and 3.13;
  - web lint, typecheck and tests;
  - lint and a complexity report.
- **Real acceptance** on a 4 vCPU / 8 GiB Linux VM (Tencent Cloud, Shanghai) with real models (Moonshot `kimi-k3` lead, `kimi-k2.6` workers) and Google results via Serper: [chat assistant](docs/evidence/2026-10-06-m4-chat-acceptance.md), [exec sandbox](docs/evidence/2026-10-06-m4-exec-hardening.md), [accounts](docs/evidence/2026-10-06-m3-accounts.md), [backup and restore](docs/evidence/2026-10-06-backup-restore.md).
- **Model fallback** ([record](docs/evidence/2026-10-10-model-fallback.md), fake upstream, zero cost): with the primary answering 503, a call without fallback fails after 3.7–5.1 s (3 tries plus backoff); with fallback it completes on the backup in about 51 ms (same as a healthy provider); with every breaker open the Gateway answers `model_degraded` in about 40 µs.
- **Kubernetes provider** ([record](docs/evidence/2026-10-10-k8s-provider.md), single-node kind, 20 runs per path): environment ready P50 978 ms cold vs. 11 ms from the warm pool; first Python output 1051 ms vs. 78 ms; the conformance suite passes 11/11 on a fake clientset (CI) and on kind; `scripts/demo-k8s.sh` shows the Gateway path, cancel and server crash recovery.
- **Evaluation platform** ([record](docs/evidence/2026-10-10-eval-platform.md), fake upstream, no model spend): in the real sandbox the reference solutions pass 15/15; the fake model passes 9/15 and all 6 deliberately wrong answers fail (`wrong_exit_code`×5, `check_timeout`×1), including two reward hacks that `exit 0` at import — the solution runs in a separate process and a check passes only if the checker prints a random completion token; concurrency 1 → 4 cuts wall clock 18.9 s → 10.5 s.
- **Serial vs. parallel research** ([record](docs/evidence/2026-10-06-m4-subrun-comparison.md)). Small sample, N = 4 per mode:

  | | Serial | Parallel |
  |---|---|---|
  | Median wall time | 302 s | 184 s |
  | Median cost per run | 0.48 USD | 0.50 USD |
  | Failures | 0 | 0 |
  | Locatable citations | 100% | 100% |

## Quick start

Requirements: Linux with cgroup v2 (or WSL2), Go 1.24+, Python 3.11+ with [uv](https://docs.astral.sh/uv/), Docker for PostgreSQL, and Node.js 24 for the web UI.

```bash
# Unit tests (no database; tests that need root are skipped)
go vet ./...
go test ./...

# Database-backed tests
docker compose -f deploy/docker-compose.yml up -d --wait
export AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable'
CI=true go test -count=1 ./internal/persistence/postgres/ ./tests/e2e/...

# Python worker SDK and agent
(cd worker && uv run pytest -q)

# Web UI
(cd web && npm ci && npm test && npm run build)
```

**Real-sandbox demo.** `sudo bash scripts/demo-m1.sh` runs the whole chain on a fresh data directory, as root:

1. submit a task;
2. checkpoint;
3. kill the worker and recover;
4. produce the result;
5. SIGKILL the server and restart-recover;
6. check invariants and leaks.

[Recorded output](docs/evidence/2026-10-05-m1-demo-run.md).

**Running the full assistant** (sandbox, Gateway, real models, chat UI) takes a model key and a search key. Step-by-step commands and every server flag are in the [usage reference](docs/usage.zh-CN.md). [`deploy/systemd/agentbox-demo.service.example`](deploy/systemd/agentbox-demo.service.example) is a template of the unit the demo server runs (copy it to `/etc/systemd/system/` and set `AGENTBOX_PUBLIC_HOST` in `/etc/agentbox/agentbox.env`).

## Repository layout

| Path | Contents |
|---|---|
| `cmd/agentbox` | Single binary: `server`, `doctor`, `task …`, `user …`, `eval …`, `verify-invariants` |
| `internal/eval`, `eval/suites` | Evaluation platform: suites, runner, trajectories, graders, reports and compare (an operator-API client only); demo suite |
| `internal/sandbox`, `provider/local`, `cgroup`, `rootfs`, `hostcheck` | Sandbox launcher, init, environment lifecycle, host self-check |
| `internal/provider/k8s`, `cmd/agentbox-podagent`, `deploy/k8s` | Kubernetes provider (one Pod per environment, digest-pinned image, warm pool), in-image helper, worker image and kind config |
| `internal/task`, `runner`, `session`, `subrun`, `admission`, `resource` | Control plane: actors, attempts, sessions, sub-runs, admission |
| `internal/gateway/{edge,call,upstream,cache}` | Gateway: per-attempt socket, call journal and ledger, provider adapters, Redis cache |
| `internal/gateway/{workspace,mcp}`, `internal/mcpdemo` | Workspaces (manifest, lifecycle, file operations), MCP client and adapter, demo MCP server (`agentbox mcp-demo-server`) |
| `internal/persistence/postgres`, `blob`, `recovery`, `reconcile`, `invariants` | Storage, content-addressed blobs, startup recovery, invariant checker |
| `internal/api`, `account` | REST/SSE API ([OpenAPI](api/openapi.yaml)), accounts |
| `internal/obs`, `telemetry`, `deploy/observability` | Observability: stdlib-only instrumentation facade, OpenTelemetry and Prometheus implementation, local stack |
| `protocol/` | Versioned Go ↔ Python worker protocol and shared fixtures |
| `worker/` | Python worker SDK, `chatagent`, tools, the `deep-research` skill, the eval worker `evalworker` |
| `web/` | Vue 3 + TypeScript chat UI and operator workbench |
| `tests/e2e` | End-to-end tests (process provider and real sandbox) |
| `docs/` | [Design](docs/design/), [evidence](docs/evidence/), [usage](docs/usage.zh-CN.md) |

## Limits

These are stated in the design and not hidden:

- **Container-level isolation on a shared kernel.** The privilege-boundary tests are regression tests, not a proof of no escape. Don't rely on this to contain hostile multi-tenant code.
- **Single execution host.** There is no cross-host failover; the Kubernetes provider shares workspaces and Gateway sockets through a node-local directory, so it is single-node for now.
- **No exactly-once.** External calls are not exactly-once. Unknown outcomes are charged conservatively, and budgets can overshoot slightly.
- **Workspace not rolled back.** The workspace is not rolled back with checkpoints.

## Acknowledgements

Designs studied, code written from scratch: [runc](https://github.com/opencontainers/runc) (re-exec init, privilege drop, `/proc` masking), [Tencent Cloud CubeSandbox](https://github.com/tencentcloud/CubeSandbox) (agent sandbox surface), E2B (in-sandbox daemon semantics). The research flow draws on gpt-researcher, dzhng/deep-research and Anthropic's write-up on multi-agent research.

## License

[MIT](LICENSE)
