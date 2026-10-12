# Sandboxed DeepResearch Assistant (Go + Python)

**A chat-style DeepResearch assistant running on a self-built agent runtime.** Go owns execution and every security boundary. Python owns the research logic.

[简体中文](README.md) · [Usage and operations reference (中文)](docs/usage.zh-CN.md) · [Design spec (中文)](docs/design/2026-10-03-v0.2-first-release-design.md) · [Glossary (E/I codes, Plan N, §)](docs/glossary.md)

A user asks a question in a chat UI. The agent either answers directly or starts a deep research turn:

1. It reads its research skill.
2. If the scope is unclear, it asks up to three multiple-choice questions.
3. It writes a todo list.
4. It researches 2–4 sub-topics in parallel.
5. It writes a report with numbered citations to the pages it actually read.

The user can stop at any point, see a short summary of what was found, and then continue, have the report written from what exists so far, or restore a cancelled turn later.

Underneath is the part this project is really about: a single-host **agent runtime** that runs untrusted-ish agent code in Linux sandboxes. It survives crashes and restarts without losing committed work, and it keeps API keys out of the sandbox entirely.

The same control plane is also a **multi-tenant model gateway**: orgs, users and platform API keys; rate limits and monthly budgets per key, user and org; a model API in both OpenAI and Anthropic format (a client in either format to a provider in either format, with streaming and tool calls); usage, bills and reconciliation.

## Architecture

```
Browser (Vue 3) / CLI / official OpenAI and Anthropic SDKs
   │  REST + SSE; session cookie, operator token or platform key (Bearer abx_live_… / x-api-key)
   ▼
Go control plane
 ├─ auth and tenancy: org → user → key, scopes, expiry and revocation (PostgreSQL NOTIFY)
 ├─ rate limits: rpm, tpm (key / user / org; Redis Lua, in-process when Redis fails); concurrent streams counted per server
 ├─ monthly budgets: every request or sandbox call holds money at each level, settled at the actual cost
 ├─ runtime API: task & session state machines, admission, scheduling, checkpoints, crash recovery,
 │                event replay (Last-Event-ID)
 ├─ model API (--model-api): /v1/chat/completions, /v1/messages, /v1/models
 │     └─ llmproto (OpenAI ⇄ Anthropic) → model routes (weights, breakers, failover before the first byte)
 ├─ Gateway: credential isolation, call journal, budgets, per-turn tool quota, SSRF-safe egress,
 │     │      exec scheduling
 │     └─ providers: OpenAI-compatible and Anthropic-format (keys stay in the host process)
 ├─ usage: model_requests and holds → usage_daily → bills and reconciliation
 ├─ PostgreSQL (only source of truth)   Redis (cache and rate limits, disposable)
 └─ sandbox: user/pid/mount/net namespaces, mapped UIDs, read-only rootfs, seccomp, capabilities {KILL},
    cgroup v2
          │
   ┌──────┴────────────────────────────────────┐
 Orchestration sandbox: Python agent         Exec sandbox: model-written Python
 (talks to the Gateway over a Unix socket)   (no network, no Gateway, one-shot)
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
| **Model fallback** | A logical model can be served by several providers, OpenAI-compatible or Anthropic-format (sandbox workers always see the OpenAI format): retryable failures fail over to the next provider immediately, and every provider attempt is a separately priced try in the journal (with optional cache read/write prices). Traffic goes in order, by smooth weighted round-robin, or by time to first token. Per-provider circuit breakers; when all are open the Gateway answers `503 model_degraded` at once; optional hedged requests. Replay and fingerprints are unaffected. |
| **Exec sandbox** | Model-written Python runs in a fresh, network-less environment per call, with separate UIDs, CPU and wall quotas and output collection. |
| **Kubernetes provider (optional)** | `--provider k8s` runs each environment in one Pod: the control channel is an in-image helper reached through pods/exec; non-root, read-only root, all capabilities dropped, no ServiceAccount token, deny-all NetworkPolicy; the worker image is pinned by digest; a warm pool cuts environment start from ~1 s to ~10 ms. It passes the same contract conformance suite as the local provider. |
| **Evaluation platform** | `agentbox eval` runs a suite (YAML/JSON) of coding/terminal tasks (checkers run in the exec sandbox) and research questions through the operator API, with concurrency, repetitions and a seed; a manifest pins the suite hash, server build, models, worker version and exec digest; trajectories (events, call journal, ledger) are exported as JSONL; deterministic graders (exit code, output, locatable citations) plus an optional budgeted LLM judge; reports and `eval compare`. Runs against the fake upstream at zero cost. |
| **Trace doctor** | `agentbox doctor-traces` reads an eval run directory or a time window of a running server, finds failed and slow tasks, and attributes them to root causes with deterministic rules (hung model tries, open breakers, provider errors, search/fetch calls cut by the call deadline, budget exhaustion, exec timeouts, backoff, hedge waste, slow sandbox start / queueing / stops), each with evidence (task and call ids, per-provider try counts and latency percentiles, latency shares; with the observability stack also the Tempo trace and per-span time, and failed-try log lines with trace ids from Loki). It proposes changes to an allowlist of server flags with bounds as a patch and an experiment config — it never touches a running server; an optional budgeted model writes a summary. [Design](docs/design/2026-10-10-trace-doctor-design.md), [record](docs/evidence/2026-10-10-trace-doctor.md). |
| **Product** | Accounts (PBKDF2, `HttpOnly` sessions, rate-limited login), per-user isolation (other users' data always returns 404), server-side redaction of costs, models and internal IDs, a CSP and sanitized Markdown rendering. |
| **Tenancy and platform API keys** | Org → user → key: every user belongs to one org (a personal org at sign-up) whose owners invite members; a user moves only by accepting, and personal orgs take no one else. Platform keys `abx_live_…` are stored only as an HMAC under a pepper file (its fingerprint is recorded in the database and a mismatch refuses to start), carry scopes (`runtime` / `models`), expiry and revocation, and call the runtime API as `Authorization: Bearer` or `x-api-key`. Verified keys are cached for 30 s; revocation reaches every server at once through PostgreSQL `NOTIFY`. A key can never mint keys or manage members. Tasks and sessions record their org and key. [Design](docs/design/2026-10-11-model-gateway-design.md). |
| **Model API** | `--model-api` serves OpenAI `/v1/chat/completions` and Anthropic `/v1/messages` (streaming and tool calls) and `/v1/models` to platform keys with the `models` scope, through the Gateway's model routes (weights, breakers, failover before the first byte) to OpenAI- or Anthropic-format providers: same format is passed through, the other is converted by `internal/llmproto`. Each request takes rpm, tpm and a stream slot at the key, user and org level, reserves its worst-case cost as a hold in the monthly budgets and settles the actual cost afterwards (unknown usage is charged in full as unknown); PostgreSQL down means 503; `routing.primary_models` in the fallback file restricts the primary to its own models. Requests and every try are recorded in `model_requests` (partitioned by month); metrics include time to first token, stream completeness and rejections by reason. Compatibility tests with the official `openai` and `anthropic` Python SDKs run in CI; `scripts/demo-gateway.sh` shows the whole flow at zero cost, and the load test and fault injection are under Evidence below. |
| **Limits and monthly budgets** | Requests-per-minute limits at the org, user and key level (one Redis Lua script checks every level atomically; when Redis fails or takes over 50 ms the limiter falls back to in-process buckets, requests keep working and a metric reports the degradation); an org's concurrent sandboxes are counted in PostgreSQL at admission. Budgets per UTC month: every Gateway call of a sandbox places a hold at each level in the same transaction as its task reservation and is refused when any level is short; holds follow the call's settlement, and a sweep charges expired ones in full as unknown. `/v1/limits` and `/v1/budgets` endpoints (owners manage their org's users and keys, never above the org's own limit) and `agentbox limits` / `agentbox budget`; `verify-invariants` checks G1 and G2. |
| **Usage, bills and reconciliation** | Every minute, settled budget holds are rolled up into `usage_daily` per day, org, user, key, model and source (model API / runtime), incrementally from a watermark and idempotently (servers serialise on an advisory lock). `GET /v1/usage` (grouped by day, model, key, user) and `GET /v1/bills/{month}` (per-model tokens and cost, known and unknown apart): members see their own, owners their org's, the operator everything. `agentbox bill reconcile` recomputes a month from the raw holds and model requests, compares it with `usage_daily` and `quota_budgets`, and exits 1 on any difference. The web app gains API keys, usage and members pages. |
| **Observability** | OpenTelemetry tracing with one trace per user action: API request → task actor → attempt → worker start, handshake and checkpoints → **into the sandbox** (the W3C `traceparent` is handed to the worker over the protocol; the stdlib-only Python SDK forwards it) → every model, search, fetch and exec call and each retry at the Gateway. Prometheus metrics (outcomes, start and stop latency, upstream latency and errors, cost, tool calls, sandboxes and slots); JSON logs carry `trace_id`. Off by default; `deploy/observability` runs Collector, Tempo, Prometheus, Loki and Grafana locally ([design](docs/design/2026-10-10-observability-design.md), [demo run](docs/evidence/2026-10-10-observability.md)). |

## Evidence

All numbers come from recorded runs. The evidence files say exactly what each run did and did not show.

- **Tests:**
  - about 1150 Go test functions, including real-sandbox end-to-end tests run as root;
  - about 380 Python tests (including the official-SDK compatibility tests);
  - about 230 web component tests.
- **CI (7 jobs):**
  - non-root suite with `-race`;
  - root suite in real sandboxes on `ubuntu-24.04`;
  - Python 3.11 and 3.13;
  - web lint, typecheck and tests;
  - compatibility of the model API with the official `openai` and `anthropic` SDKs;
  - lint and a complexity report.
- **Real acceptance** on a 4 vCPU / 8 GiB Linux VM (Tencent Cloud, Shanghai) with real models (Moonshot `kimi-k3` lead, `kimi-k2.6` workers) and Google results via Serper: [chat assistant](docs/evidence/2026-10-06-m4-chat-acceptance.md), [exec sandbox](docs/evidence/2026-10-06-m4-exec-hardening.md), [accounts](docs/evidence/2026-10-06-m3-accounts.md), [backup and restore](docs/evidence/2026-10-06-backup-restore.md).
- **Model fallback** ([record](docs/evidence/2026-10-10-model-fallback.md), fake upstream, zero cost): with the primary answering 503, a call without fallback fails after 3.7–5.1 s (3 tries plus backoff); with fallback it completes on the backup in about 51 ms (same as a healthy provider); with every breaker open the Gateway answers `model_degraded` in about 40 µs.
- **Model gateway** ([record](docs/evidence/2026-10-12-model-gateway.md), 2026-10-12; fake upstreams, zero cost; one WSL2 machine with the CPU shared with other work, PostgreSQL in Docker Desktop (slow commit flushes), and one server, so Redis-global limiting across servers was not exercised; key runs 3×): non-streaming gateway overhead P50 about 16 ms and P99 about 25–27 ms on all four paths (streaming P99 24.5–35 ms, time to first token about 6.5 ms; about two thirds is flushing the ~4 commits per request). Those numbers have no limits set, so the rate limiter is not on the path; with rpm and tpm set at all three levels P50 is about 1–2 ms higher. About 1050 RPS over 16 orgs (concurrency 32, with the database less loaded; 440 RPS in one run where the database had slowed down: throughput tracks commit latency), with no visible difference when the limiter is on the path. Rate limiter allowed / configured 1.000–1.002; zero budget overspend at concurrency 64, and the fake upstreams' own request and token counts match the gateway's tries, holds and recorded tokens exactly; zero client errors with Redis stopped mid-run; `bill reconcile` consistent after the load. One org's budget row is a hot lock: about 135 RPS for a single org, and when the database slows down, queued requests that pass the 2 s store deadline get 503 (fail closed). Four fault scenarios (kill -9, provider failure after the first byte, key revoked mid-stream, breaker) pass; 6 real calls through Moonshot cover all four paths (including its Anthropic-compatible endpoint).
- **Kubernetes provider** ([record](docs/evidence/2026-10-10-k8s-provider.md), single-node kind, 20 runs per path): environment ready P50 978 ms cold vs. 11 ms from the warm pool; first Python output 1051 ms vs. 78 ms; the conformance suite passes 11/11 on a fake clientset (CI) and on kind; `scripts/demo-k8s.sh` shows the Gateway path, cancel and server crash recovery.
- **Evaluation platform** ([record](docs/evidence/2026-10-10-eval-platform.md), fake upstream, no model spend): in the real sandbox the reference solutions pass 15/15; the fake model passes 9/15 and all 6 deliberately wrong answers fail (`wrong_exit_code`×5, `check_timeout`×1), including two reward hacks that `exit 0` at import — the solution runs in a separate process and a check passes only if the checker prints a random completion token; concurrency 1 → 4 cuts wall clock 18.5 s → 10.5 s.
- **Trace doctor closed loop** ([record](docs/evidence/2026-10-10-trace-doctor.md), real sandbox + fake upstream, no model spend, N = 64 per run): a deliberately mis-configured server (the primary provider hangs on every 4th chat request and there is no per-try timeout; a 1 s call deadline against 1.5 s fetches) passes 56.2 % (Wilson 95 % [44.1 %, 67.7 %]), P95 90.2 s; `doctor-traces` attributes all 28 failures to two rules from the trajectories alone and proposes `--model-try-timeout 2s` and `--call-deadline 10s`; rerunning the same suite with the experiment config it wrote: 100 % ([94.3 %, 100 %]), P95 15.1 s. The faults follow a deterministic every-4th-request schedule, so the Wilson intervals are a conservative sanity check; the stronger evidence is that two complete runs gave the same totals.
- **Serial vs. parallel research** ([record](docs/evidence/2026-10-06-m4-subrun-comparison.md)). Small sample, N = 4 per mode:

  | | Serial | Parallel |
  |---|---|---|
  | Median wall time | 302 s | 184 s |
  | Median cost per run | 0.48 USD | 0.50 USD |
  | Failures | 0 | 0 |
  | Locatable citations | 100% | 100% |

## Quick start

Requirements: Linux with cgroup v2 (or WSL2), Go 1.26+, Python 3.11+ with [uv](https://docs.astral.sh/uv/), Docker for PostgreSQL, and Node.js 24 for the web UI.

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

### Model API

Start `agentbox server` with `--model-api` and call it in OpenAI or Anthropic format with a platform key that has the `models` scope (see "模型 API" in the [usage reference](docs/usage.zh-CN.md)):

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json'   -d '{"model":"kimi-k3","messages":[{"role":"user","content":"hello"}],"max_tokens":200}'
```

```python
import os, anthropic, openai
KEY = os.environ["KEY"]
openai.OpenAI(base_url="http://127.0.0.1:8080/v1", api_key=KEY).chat.completions.create(
    model="kimi-k3", messages=[{"role": "user", "content": "hello"}], max_tokens=200)
anthropic.Anthropic(base_url="http://127.0.0.1:8080", api_key=KEY).messages.create(
    model="kimi-k3", max_tokens=200, messages=[{"role": "user", "content": "hello"}])
```

No model key and no root needed (fakeupstream plays the OpenAI- and Anthropic-format providers; needs docker, Go, python3 and curl, plus uv for `sdk-compat.sh` and the demo; without `AGENTBOX_DATABASE_URL` the scripts start PostgreSQL from `deploy/docker-compose.yml` themselves):

```bash
AGENTBOX_TEST_DATABASE_URL='postgres://agentbox:agentbox@127.0.0.1:5432/agentbox?sslmode=disable' bash scripts/sdk-compat.sh  # both SDKs × both provider formats
bash scripts/demo-gateway.sh       # org, invite, keys, both SDKs on all four paths, weighted routing, 429 and exhausted budget, revocation, bill and reconcile, metrics
bash scripts/fault-gateway.sh      # kill -9 mid-stream, provider failure after the first byte, key revoked mid-stream, breaker and recovery (about 6 min)
bash scripts/loadtest-gateway.sh   # overhead, max RPS, rate-limit accuracy, concurrent budgets, row-lock wait, Redis stopped (cmd/agentbox-loadtest)
```

### Usage, bills and the web pages

The server rolls settled budgets up into daily usage every minute; the operator can print a bill and reconcile a month from the command line (see "用量、账单与对账" in the [usage reference](docs/usage.zh-CN.md)):

```bash
sudo AGENTBOX_DATABASE_URL=... agentbox usage --org acme --group-by day,model --rollup
sudo AGENTBOX_DATABASE_URL=... agentbox bill show 2026-10 --org acme
sudo AGENTBOX_DATABASE_URL=... agentbox bill reconcile 2026-10   # exits 1 on a difference
```

Signed-in users get three more pages in the top bar (described in words; no screenshots):

- **API keys**: a table of keys (name, scopes, created, last used, expires, status). Creating one takes "model API / runtime" checkboxes and an expiry; the full key appears exactly once, in a dialog with a copy button and a yellow warning. Revoking asks for a second click.
- **Usage**: four number tiles (cost, of which unknown, requests, tokens), two purple bar charts side by side (daily cost, daily tokens), tables by model and by key, budget bars for the org, the user and each key (purple spent, orange unknown, light purple reserved, with remaining and limit), and this month's bill.
- **Members**: received invites (accept, decline), your org and "leave org"; owners also get the member table (removal asks for confirmation), an invite form and pending invites.

## Repository layout

| Path | Contents |
|---|---|
| `cmd/agentbox` | Single binary: `server`, `doctor`, `task …`, `user …`, `org …`, `key …`, `limits …`, `budget …`, `usage`, `bill …`, `eval …`, `doctor-traces …`, `verify-invariants` |
| `internal/eval`, `eval/suites` | Evaluation platform: suites, runner, trajectories, graders, reports and compare (an operator-API client only); demo suite |
| `internal/doctor` | Trace doctor: root-cause rules, allowlisted proposals and patches, Tempo/Prometheus/Loki queries, optional model summary (eval files and public APIs only) |
| `internal/sandbox`, `provider/local`, `cgroup`, `rootfs`, `hostcheck` | Sandbox launcher, init, environment lifecycle, host self-check |
| `internal/provider/k8s`, `cmd/agentbox-podagent`, `deploy/k8s` | Kubernetes provider (one Pod per environment, digest-pinned image, warm pool), in-image helper, worker image and kind config |
| `internal/task`, `runner`, `session`, `subrun`, `admission`, `resource` | Control plane: actors, attempts, sessions, sub-runs, admission |
| `internal/gateway/{edge,call,upstream,cache}` | Gateway: per-attempt socket, call journal and ledger, provider adapters, Redis cache |
| `internal/gateway/{workspace,mcp}`, `internal/mcpdemo` | Workspaces (manifest, lifecycle, file operations), MCP client and adapter, demo MCP server (`agentbox mcp-demo-server`) |
| `internal/persistence/postgres`, `blob`, `recovery`, `reconcile`, `invariants` | Storage, content-addressed blobs, startup recovery, invariant checker |
| `internal/api`, `account` | REST/SSE API ([OpenAPI](api/openapi.yaml)), accounts |
| `internal/tenant`, `apikey`, `ratelimit`, `quota` | Orgs and members, platform keys (HMAC, cache with NOTIFY eviction), rate limits (Redis Lua with in-process fallback, concurrency counters), monthly budget holds |
| `internal/llmproto`, `modelapi`, `usage` | Neutral OpenAI ⇄ Anthropic format and codecs (stdlib only), the model API, usage rollup, bills and reconciliation |
| `cmd/agentbox-loadtest`, `tests/sdk`, `scripts/*-gateway.sh` | Model gateway load tool, official-SDK compatibility tests, demo, load-test and fault-injection scripts |
| `internal/obs`, `telemetry`, `deploy/observability` | Observability: stdlib-only instrumentation facade, OpenTelemetry and Prometheus implementation, local stack |
| `protocol/` | Versioned Go ↔ Python worker protocol and shared fixtures |
| `worker/` | Python worker SDK, `chatagent`, tools, the `deep-research` skill, the eval worker `evalworker` |
| `web/` | Vue 3 + TypeScript chat UI and operator workbench |
| `tests/e2e` | End-to-end tests (process provider and real sandbox) |
| `docs/` | [Design](docs/design/), [evidence](docs/evidence/), [usage](docs/usage.zh-CN.md), [glossary](docs/glossary.md) |

## Limits

These are stated in the design and not hidden:

- **Container-level isolation on a shared kernel.** The privilege-boundary tests are regression tests, not a proof of no escape. Don't rely on this to contain hostile multi-tenant code.
- **Single execution host.** There is no cross-host failover; the Kubernetes provider shares workspaces and Gateway sockets through a node-local directory, so it is single-node for now.
- **No exactly-once.** External calls are not exactly-once. Unknown outcomes are charged conservatively, and budgets can overshoot slightly.
- **Workspace not rolled back.** The workspace is not rolled back with checkpoints.
- **One org's budget row is a hot lock.** Every model request locks its org's monthly budget row when it reserves and again when it settles: about 135 RPS for a single org on the test machine (about 1050 RPS over 16 orgs; both with the database less loaded, as throughput tracks commit latency); when the database slows down, queued requests that pass the store deadline get 503 (fail closed, never overspent). Sharded counters and pre-allocated budget slices are future work.
- **Rate limits are per server while Redis is down, and concurrent streams (`max_streams`) are always counted per server** (with N servers the effective limit is N times). Budgets live in PostgreSQL and are unaffected; with PostgreSQL down the model API answers 503.
- **Model gateway scope.** One org per user; the primary provider (`--model-base-url`) is always OpenAI-format, so Anthropic-format providers can only be fallbacks; no payment, SSO, multiple regions or bring-your-own provider keys.

## Acknowledgements

Designs studied, code written from scratch: [runc](https://github.com/opencontainers/runc) (re-exec init, privilege drop, `/proc` masking), [Tencent Cloud CubeSandbox](https://github.com/tencentcloud/CubeSandbox) (agent sandbox surface), E2B (in-sandbox daemon semantics). The research flow draws on gpt-researcher, dzhng/deep-research and Anthropic's write-up on multi-agent research.

## License

[MIT](LICENSE)
