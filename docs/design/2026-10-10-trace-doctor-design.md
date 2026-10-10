# Trace doctor: reading failed and slow runs, proposing safe fixes, measuring the result

> Date: 2026-10-10. Status: implemented on branch `s6b-trace-doctor` (sub-project S6b of the agent-infra
> gap program). Builds on observability (`2026-10-10-observability-design.md`), the eval platform
> (`2026-10-10-eval-platform-design.md`) and model fallback (`2026-10-10-model-fallback-design.md`).

## 1. Problem

The server already records everything needed to explain a bad run: the Gateway journal (every model,
search, fetch and exec call with every upstream try, its provider route, outcome, latency, cost), the
operator event stream (task created, attempt created, worker ready, control accepted, terminal), and —
with observability on — OpenTelemetry traces, Prometheus metrics and trace-correlated logs. What is
missing is the loop an operator runs by hand after a bad night: *find* the failed and slow runs, *explain*
them with evidence, *change* a configuration knob, and *prove* with a measured before/after that the
change helped. `agentbox doctor-traces` automates that loop for the failure modes this system actually
has, and keeps every step reviewable.

## 2. Goals

1. **Find** failed and slow tasks/turns in an eval run directory (`trajectories.jsonl` + `manifest.json`)
   or in a time window of a running server (operator REST API), optionally enriched from Tempo, Prometheus
   and Loki when the observability stack is running.
2. **Classify** each failed or slow task with **deterministic rules** (no model needed), each finding with
   evidence: counts, affected server task ids and call ids, per-provider try statistics, latency shares,
   and trace ids when Tempo is available.
3. **Propose** concrete, *safe* configuration changes from a fixed allowlist with bounds, as a reviewable
   patch. Changes are never applied to the running server; `--apply-to` writes a *new* config file for an
   experiment run only.
4. **Optional agentic step** (off by default, hard budget): a model reads the findings (metadata only) and
   writes a short diagnosis and may suggest values from the same allowlist; its suggestions pass the same
   validation as the rules' and are labelled as model-suggested.
5. **Closed loop with numbers**: `scripts/demo-doctor.sh` runs a suite against a deliberately
   mis-configured server (fake upstreams with injected faults, zero model cost), runs the doctor, starts a
   second server with the proposed config, reruns the suite and prints `agentbox eval compare` with N and
   Wilson intervals.

## 3. Non-goals

- No automatic reconfiguration of a production server, no feedback controller. A human (or a CI job with
  a review step) decides whether an experiment's result justifies changing the production config.
- No reading of prompts, model output, fetched pages or worker logs. The doctor sees what the operator
  inspect API, the event stream and the observability attribute allowlist expose: ids, enumerations,
  numbers. Its model step therefore never sees user content either.
- No new server endpoint and no server-side change: default server behaviour is untouched.
- No statistical significance testing beyond what `eval compare` already reports (N, Wilson 95%
  intervals, a warning for small N).

## 4. Architecture

```
 eval run dir ──────────────┐                        ┌── Tempo  /api/search, /api/traces/{id}  (optional)
 (trajectories.jsonl,       │                        ├── Prometheus /api/v1/query               (optional)
  manifest.json)            ▼                        ├── Loki   /loki/api/v1/query_range        (optional)
 server window ──► internal/doctor: Load → Analyze ──┤
 (GET /tasks, inspect,       │   records   rules      └── advisor (optional OpenAI-compatible model, budgeted)
  events; operator token)    ▼
                 findings.json, findings.md, proposal.patch, [experiment.flags]
```

| Package / file | Responsibility | Depends on |
|---|---|---|
| `internal/doctor` | loading, per-task timing breakdown, rules, proposals, server-flags files and patches, observability API clients, advisor, rendering, CLI (`Main`) | stdlib, `internal/eval` (trajectory schema, REST client) — **no** server internals (archtest `TestDoctorUsesPublicSurfacesOnly`) |
| `internal/eval` | gains `Client.ListTasks` (`GET /tasks`, keyset pages) | — |
| `cmd/agentbox/main.go` | dispatches `doctor-traces` to `doctor.Main` | — |
| `tests/e2e/fakeupstream` | periodic faults (`SetEvery`; `-every chat:4:hang`) for the demo | — |
| `scripts/demo-doctor.sh`, `eval/suites/doctor.yaml` | the closed-loop demonstration | — |

The doctor is a *client* of public surfaces: the eval files (a documented schema) and the operator REST
API, plus the HTTP query APIs of Tempo, Prometheus and Loki. It shares the eval package's types so a
trajectory written by `agentbox eval run` and a record built from a live server are the same structure.

## 5. Inputs

- `--run DIR`: an eval run directory. Every trajectory is a task with its events, journal (calls and
  tries), attempts, budget, outcome and failure category.
- `--server URL --since 1h [--until T]`: lists tasks newest-first (`GET /tasks`, keyset pages, bounded by
  `--max-tasks`, default 500) and keeps those created in the window; for each it reads `inspect` and the
  event stream and builds the same record (outcome = task status; no graders).
- `--config FILE`: the server's flags file (one `--flag=value` per line, `#` comments) — the configuration
  the run was made with. Optional; with it, rules use the actual values (e.g. the current
  `--call-deadline`) and the patch is a diff of this file.
- `--tempo URL`, `--prometheus URL`, `--loki URL` (all optional, e.g. the stack in
  `deploy/observability/`): see §8.

## 6. Analysis

### 6.1 Per-task timing breakdown

For every task the doctor derives, from events and the journal:

| Component | Source |
|---|---|
| `queue` | `task_created` → first `attempt_created` (admission wait) |
| `start` | each `attempt_created` → that attempt's worker `ready` (sandbox start + worker boot) |
| `model`, `search`, `fetch`, `exec`, `other_calls` | sum of try latencies per endpoint kind |
| `backoff_est` | for each extra try on the *same* provider route: the Gateway's backoff formula (2 s · 2^k, cap 60 s) |
| `stop` | `control_accepted` (pause/cancel) → the next terminal / paused status |
| `latency` | client-measured submit → terminal (eval) or created → terminal (server window) |

Try latencies of parallel calls overlap (research sub-runs), so the shares are "busy time / wall time"
and may sum to more than 100 %; the report says so.

A task is **failed** when its outcome verdict is not `pass` (server window: status not `succeeded`). A task
is **slow** when its latency is at least `--slow-factor` (default 3) × the median latency of the passing
tasks of the same kind, or above `--slow-ms` when given.

### 6.2 Rules (deterministic)

Each rule inspects all records, emits at most one finding (with evidence aggregated over the tasks it
explains) and labels the failed/slow tasks it explains with its cause. A task explained by several rules
gets the first in this order (the order is "closest to the root cause first"):

| Rule id | Fires when | Evidence | Proposal |
|---|---|---|---|
| `model_try_hang` | model tries that did not finish `ok` and ran ≥ max(10 s, 10 × the route's median ok latency), or are still in flight | per route: hung tries, their latency, ok p50/p99, tasks that timed out | `--model-try-timeout = clamp(4 × p99(ok model tries), 2 s, 120 s)` — only in chain mode (the flag applies to routed adapters); single provider: advisory "configure a fallback chain" |
| `model_upstream_errors` | model tries with outcome retryable/fatal/unknown (excluding hangs and hedge losers) ≥ 5 % of a route's tries; explains failed tasks whose model call failed, and slow tasks whose calls survived failed tries (failover or retry cost) | per route and error code: counts, error rate, failed-try time as a share of the slow tasks' latency | advisory: chain order (healthiest first) / add a fallback provider / a hedge when the primary still fails ≥ 20 %; `--model-breaker-failures` lowered to 2 when a route fails > 50 % |
| `breaker_degraded` | calls failed `model_degraded`, or tries skipped a route as `circuit_open` | counts per route, affected tasks | advisory (provider health); none applied |
| `fetch_deadline` | search/fetch calls that did not complete and whose tries were cut by the call deadline: tries at ≥ 85 % of `--call-deadline` (from the config, or its default), or — without the config — ≥ 3 failed tries that all stop within 15 % of the same latency; or the explicit `call_deadline_exceeded` code. (Observed on a real server: a cut fetch leaves the call `unknown` without a fail reason and its try `unknown / upstream_unconfirmed` just below the deadline.) | counts per kind, cut-off latency, ok tries and p99 per kind | `--call-deadline = clamp(max(4 × p99(ok search/fetch tries), 10 × current), 10 s, 120 s)` |
| `fetch_blocked` | fetch tries rejected by policy or target (`egress_blocked`, `upstream_rejected`, `too_many_redirects`, `invalid_url`, HTTP 403/429 from the target) | counts per code | advisory: skill text (prefer primary sources, avoid sites that challenge bots); never applied |
| `tool_budget_exhausted` | calls failed `tool_budget_exhausted` | counts, tasks | `--turn-tool-budget` raised by 50 % (bounded 1000) for turns; advisory for tasks (`research.max_fetch`) |
| `cost_budget_exhausted` | calls failed `budget_exhausted` / `budget_insufficient_for_request` | counts, spent vs limit | advisory: raise `limits.budget_micro` in the suite or lower `max_tokens` |
| `exec_timeout` | exec results `timeout`, checker `timeout`, or `exec_queue_timeout` | counts, wall vs limit | advisory: suite `wall_ms`; `--exec-slots` when queueing |
| `retry_backoff` | estimated backoff ≥ 30 % of a failed/slow task's latency | tasks, backoff seconds, provider | advisory: a fallback route (immediate failover instead of backoff) |
| `hedge_waste` | hedge legs exist and fewer than 20 % of them won, or lost legs were charged | legs, wins, losses, charged unknown micro-USD | `--model-hedge-delay` doubled (or advisory: turn it off) |
| `sandbox_start` | p95 of `start` ≥ 10 s or ≥ 30 % of slow tasks' latency | p50/p95, worst tasks | advisory: warm pool (k8s provider), template size |
| `admission_queue` | p95 of `queue` ≥ 5 s and ≥ 30 % of slow tasks' latency | p50/p95 | advisory: `--run-slots` (capacity dependent) |
| `stop_latency` | p95 of `stop` ≥ 10 s | p50/p95, worst tasks | advisory |

Failed or slow tasks that no rule explains are listed as `unexplained` with their eval category (e.g.
`wrong_output` — a wrong answer is not an infrastructure problem). The report states how many failed tasks
are explained, so a doctor that explains nothing says so.

### 6.3 Proposals and the allowlist

Only these server flags can be changed by `--apply-to` (bounds enforced for rule and model proposals):

| Flag | Bounds |
|---|---|
| `--model-try-timeout` | 1 s – 120 s |
| `--model-hedge-delay` | 0 (off) or 200 ms – 60 s |
| `--model-breaker-failures` | 1 – 10 |
| `--model-breaker-open` | 5 s – 10 min |
| `--call-deadline` | 5 s – 300 s |
| `--model-call-deadline` | 30 s – 900 s |
| `--turn-tool-budget` | 1 – 1000 |

Everything else (chain order, run slots, suite limits, skill text) is an **advisory** proposal: it appears
in the findings and as a comment in the patch, and is never written. When two findings propose the same flag,
the larger value wins (every allowlisted flag is a timeout, delay, threshold or budget for which the larger value is the more permissive one, so the experiment does not create new failures by tightening something). The patch
(`proposal.patch`) is a unified diff of the flags file; `--apply-to FILE` reads `FILE` and writes
`<out>/experiment.flags` (the input is never modified). Values for flags that are absent from the file are
appended with a comment naming the finding.

### 6.4 Optional model step (`--advisor-model`)

Off by default. With `--advisor-model M --advisor-base-url URL --advisor-price IN:OUT` (key from
`AGENTBOX_DOCTOR_API_KEY`, else `AGENTBOX_MODEL_API_KEY`; environment only, never printed or written), the
doctor sends **one** chat request: a system prompt with the allowlist and bounds, and the findings JSON
(ids, counts, latencies, codes — no user content). The reply must be JSON
`{"summary": "...", "proposals": [{"flag": "...", "value": "...", "reason": "..."}]}`. Proposals outside the
allowlist or bounds are dropped and listed as rejected; accepted ones are merged *after* the rule proposals
(rules win on conflict) and labelled `source: model`. The worst-case cost (prompt estimate + `max_tokens`)
must fit `--advisor-budget-usd` (default 0.05) before the call is made; the actual cost from `usage` is
reported. `--advisor-max-tokens` (default 1024) caps the reply; reasoning models spend output tokens before the
answer, so an empty reply cut at the cap is reported as such. Any failure (budget, transport, format) leaves the deterministic report intact.

## 7. Outputs

`--out DIR` (default `<run>/doctor` or `./doctor-<timestamp>`):

- `findings.json` (schema `agentbox.doctor.findings/v1`): source, totals (tasks, failed, slow, explained),
  failed/slow tasks by cause, findings with evidence and proposals, slowest tasks with breakdowns,
  observability enrichment, advisor usage.
- `findings.md`: the same for humans (also printed to stdout).
- `proposal.patch`: unified diff of the flags file (only with `--config`/`--apply-to`); advisory proposals
  as `#` comment lines.
- `experiment.flags`: only with `--apply-to`.

Exit code: 0 when the analysis ran (findings or not), 1 on input errors, 2 on usage errors.

## 8. Observability enrichment (optional)

- **Tempo**: for up to 3 example tasks per finding and the 5 slowest tasks, `GET /api/search` with the tag
  `task.id=<id>` (the run span carries it) yields the trace ids; for the slowest tasks
  `GET /api/traces/<id>` (OTLP JSON) gives span durations, aggregated by span name (`gateway.try`,
  `worker.start`, `env.create`, …) into latency shares. The trace id goes into the finding's examples, so
  an operator can open it in Grafana.
- **Prometheus**: instant queries over the analysed window: try outcomes per provider
  (`agentbox_upstream_tries_total`), breaker transitions, `agentbox_gateway_calls_total` by result; shown
  as a table beside the journal-derived numbers (a cross-check: two independent paths should agree).
- **Loki**: WARN/ERROR lines by message, and Gateway tries that did not succeed (`gateway: try` lines, which
  are INFO) by endpoint, route, outcome and code, each with sample `trace_id`s — a log line leads to its trace.

The window is the span of the analysed tasks padded by 10 s (more would pull in the next run when runs follow
each other closely). Prometheus counts are a sampled cross-check: `increase()` cannot see what a fresh process
counted before its first scrape or after its last one, so they can undercount; the journal and the logs are
exact.

Every enrichment is best effort: an unreachable endpoint adds a note, never an error.

## 9. Safety

- Operator-only: the server source uses the operator token (`AGENTBOX_TOKEN` or `--token-file`), sent
  only in the `Authorization` header; it never appears in outputs (test).
- The advisor key comes only from the environment and is never written (test asserts it is absent from
  every output file).
- The doctor never writes the input config and never talks to the server except to read.
- Proposals are bounded by the allowlist; the model cannot introduce a flag (e.g. `--upstream-allow-private`)
  or an out-of-range value.

## 10. Testing

- Unit: breakdown from synthetic trajectories (queue/start/stop/backoff), each rule positive and negative,
  cause precedence, proposal merging and bounds, flags-file parse/format/diff/apply, advisor with an
  `httptest` endpoint (accepted, rejected, over-budget, malformed reply, key never recorded), Tempo /
  Prometheus / Loki clients against canned responses, server-window loader against a fake API,
  CLI usage errors and exit codes.
- Archtest: `internal/doctor` depends only on stdlib, `internal/eval` and what that already allows.
- Python: `_io.open` fixture hook (eval follow-up, §11).
- End to end: `scripts/demo-doctor.sh` with the real sandbox and two fake upstreams; numbers in
  `docs/evidence/2026-10-10-trace-doctor.md`.

## 11. Demonstration (`sudo bash scripts/demo-doctor.sh`)

1. Two fake upstream processes: *primary* answers model calls but every 4th chat request hangs
   (`-every chat:4:hang`), and serves search and fetch with 1.5 s fetch latency; *backup* is healthy.
2. Server A ("before"): fallback chain primary → backup, **no** `--model-try-timeout` (a hung primary try
   holds the call until the eval timeout), and `--call-deadline 1s` (shorter than the fetch latency).
3. `agentbox eval run` of `eval/suites/doctor.yaml` (6 coding tasks whose fake replies are correct, plus 2
   research questions) with `--repeat 8 --concurrency 8 --seed 7`: N = 64 per run.
4. `agentbox doctor-traces --run <A> --apply-to before.flags` → findings, `proposal.patch`,
   `experiment.flags`.
5. Server B with `experiment.flags`; the same suite and seed.
6. `agentbox eval compare A B` — success rate with N and Wilson intervals, latency P50/P95, and per-task
   changes. If the intervals overlap the script says the change is within noise.

The fault schedule is deterministic per request count, not per task, so which task hits a hang depends on
interleaving; the *rate* is fixed, which is what the comparison measures.

## 12. Limits

- The doctor sees only metadata. It can tell that fetches hit the call deadline, not *why* a site is slow.
- Proposed values are heuristics from observed latency distributions; a run with few successful tries
  gives a weak estimate (the report shows N for every percentile).
- Causality is not proven by one experiment: the before/after compare shows the effect of the whole
  experiment config. Rerun with one change at a time to attribute effects (the doctor prints each change
  with the finding that motivated it so this is easy to do).
- Without Tempo the evidence uses server task ids and call ids; `agentbox task inspect <task>` shows the same
  journal.
