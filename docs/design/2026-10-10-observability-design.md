# Observability: tracing, metrics and log correlation (2026-10-10)

Status: implemented on branch `s1-observability`.

## 1. Goals

1. **One trace per task / turn**, from the API request that created it, through the task actor's attempts
   (admission, environment, worker start, handshake, checkpoints, finalize), into the sandbox, and back out
   through every Gateway call the worker makes (model / search / fetch / exec, including each upstream try,
   retries, cache hits, coalescing and budget rejections).
2. **Prometheus metrics** an operator can alert on: task/turn outcomes, attempt start latency, stop latency,
   upstream latency and errors by provider/model/status, tool calls per turn, cost in micro-USD, sandbox
   environment counts, cleanup backlog, run/exec slot usage, API request rate/latency.
3. **Log correlation**: JSON logs written while a span is active carry `trace_id` / `span_id`, so Grafana can
   jump from a log line to its trace and back.
4. **Off by default**: with no `--otlp-endpoint` and no `--metrics-listen` the server behaves exactly as before.
   No exporter, no listener, no submit/stop registry entries, no request-context wrapping in the API; instrumented
   call sites cost a nil check plus the attribute slice built at the call site (~30 ns, 2 small allocations,
   measured).
5. **A reproducible local stack** (`deploy/observability/`) and a demo script that drives real tasks and turns
   (with a stop and a failure) through the fake upstream, at zero model cost.

## 2. Non-goals

- No OpenTelemetry dependency inside the sandbox. The Python worker SDK stays stdlib-only; it does not export
  spans, it only forwards the W3C `traceparent` it was given.
- No OTel metrics pipeline (metrics are Prometheus pull); no OTel logs pipeline (logs are shipped from the log
  file by Alloy into Loki).
- No accepting of `traceparent` from API clients (end users are untrusted; every API request starts a new
  trace). This can be added behind a flag later.
- No per-task or per-user metric labels (cardinality); per-task drill-down is what traces are for.

## 3. Architecture

```
            cmd/agentbox (flags)                      internal/telemetry (OTel SDK + OTLP/HTTP, Prometheus)
                   │ telemetry.Setup(cfg) ───────────────────────────────┐
                   ▼                                                     │ implements
   internal/api ─ internal/app ─ internal/task ─ internal/runner         ▼
        │              │               │              │           internal/obs  (stdlib-only facade:
        └──────────────┴───────────────┴──────────────┴── import ──  Start/Span/Traceparent/Recorder)
   internal/gateway/edge ─ internal/gateway/call ─ internal/session ─────┘
```

- `internal/obs` is a **stdlib-only facade**: `Start(ctx, name, attrs...)`, `Span{SetAttrs, Event, Fail, End}`,
  `Traceparent(ctx)`, `WithRemoteParent(ctx, tp)`, `Carry(dst, src)`, `IDs(ctx)`, a W3C `traceparent` parser,
  a `Recorder` interface for metric events, a bounded submit registry, and an `slog.Handler` wrapper. The
  default implementation is a no-op. Domain packages (task, session, runner, gateway/call, gateway/edge, api)
  import only `obs`. This keeps existing architecture rules intact: `gateway/call` must not depend on any
  third-party module, `task`/`session` must not import `net/http` (the OTel propagation package does).
  A new archtest rule pins `obs` to the standard library and forbids OTel/Prometheus imports anywhere except
  `internal/telemetry` and `cmd/agentbox`.
- `internal/telemetry` implements `obs` with the OpenTelemetry Go SDK (batch span processor, OTLP/HTTP
  exporter, parent-based ratio sampler, W3C TraceContext) and `prometheus/client_golang` (own registry, Go and
  process collectors, `/metrics` on a separate listener). `Setup` installs the implementation with
  `obs.Set` and returns a shutdown function that flushes spans.
- `cmd/agentbox server` gets three flags: `--otlp-endpoint` (e.g. `http://127.0.0.1:4318`; empty = tracing off),
  `--trace-sample-ratio` (default 1), `--metrics-listen` (e.g. `127.0.0.1:9464`; empty = metrics off; a
  non-loopback address prints a warning – the endpoint has no authentication and must never be exposed to users).
  `--log-file` (optional) additionally appends the JSON log to a file so a shipper can read it.

## 4. Trace model

| Span | Where | Parent | Key attributes |
|---|---|---|---|
| `HTTP <METHOD> <route>` | `api.Handler.ServeHTTP` | new root | `http.method`, `http.route` (pattern, not path), `http.status_code` |
| `task` / `turn` (run span) | task actor, one per *run*: first effect of the run → the verdict that stops it running (paused, awaiting_input, terminal; fault retries stay in the run) | the API request that started the run — create, resume, continue or answer (submit registry), else new root | `task.id`, `session.id`, `task.kind`, `task.run`, `task.status` |
| `attempt` | task actor, attempt committed → verdict committed | task span | `attempt.id`, `attempt.no`, `env.id`, `outcome.class`, `attempt.status` |
| `admission.acquire` | actor slot request (starts the run) | run span | — (its duration is the queueing time) |
| `env.create` / `env.stop` / `session.handoff` | actor async effects | attempt span | `env.id`, `stopped`, `recorded` |
| `worker.run` | `runner.Run` / `Incarnation.RunTask` | attempt span | `worker.mode`, `task.id`, `attempt.id`, `attempt.no`, `env.id`, `outcome.class`, `kill.reason`, `control` |
| `worker.start` | `StartExec` | worker.run | — |
| `worker.handshake` | init/task_start sent → ready/task_accepted | worker.run | — |
| `checkpoint.commit` | `processor.commitCheckpoint` | worker.run | `checkpoint.status` |
| `worker.finalize` | process exit / proposal → outcome classified | worker.run | `outcome.class` |
| `gateway.call` | `call.Coordinator.Invoke` / `Exec` | worker-sent `traceparent` (validated) or the bound attempt | `gateway.kind`, `gateway.provider`, `gateway.model`, `call.id`, `call.result` (`completed`, `replayed`, `cache_hit`, `coalesced`, or the stable rejection code), `subrun.id` |
| `gateway.try` | each upstream try — one per leg, started inside the leg right before `Coordinator.do` (hedge legs are siblings) | gateway.call | `try.no`, `http.status_code`, `try.outcome` (ok, retryable, fatal, unknown, `hedge_lost`, `aborted` — the last two are not failures); routed kinds add `route`, `route_skipped`, `hedge`, `breaker` (at start) and `breaker_after`. Backoff is the gap between sibling tries. The `gateway.call` span carries the winning leg's `gateway.route` and `hedge`. |
| `session.<op>` | session actor ops (start, quiesce, freeze, thaw, release, destroy) | none (session-scoped root) | `session.id`, `incarnation.id`, `env.id` |

**Propagation into the sandbox.** Protocol v1 already reserves an optional string `traceparent` on `init` and
`task_start` (unknown-field rules make it backward-compatible; old workers ignore it). The runner fills it with
the `worker.run` span context. The Python SDK (`agentbox_worker.tracecontext`, stdlib only) validates it with
the W3C rules (version `00`, 32-hex trace-id and 16-hex parent-id not all zero, 2-hex flags; anything else is
dropped) and the Gateway client sends it as the `traceparent` header on every request (including sub-run views).

**Untrusted header handling.** The worker is untrusted, so its `traceparent` header only *selects* a parent and is
never adopted as is. The runner records the traceparent it handed to the attempt's worker (`obs.ExpectWorker`).
The edge uses the host's recorded value — with the host's sampling flags — only when the header parses, its
trace-id equals the bound attempt's trace and its parent-id equals that worker.run span; otherwise the header is
ignored and the call is parented to the bound attempt span. A worker therefore cannot attach spans to other traces,
invent parents, or change the sampling decision.

**Async boundaries.** Gateway calls continue in the coordinator's own context after the request returns (journal
semantics); the span context is carried over explicitly (`obs.Carry`) so the call and its tries stay in the trace.
The task actor runs asynchronously after the API request; the API records `traceparent` + submit time for the
task id in a bounded in-memory registry (≤ 4096 entries, 1 h TTL). After a restart the registry is empty and
the run span becomes a new root — a deliberate best-effort limit.

**One user action = one trace.** A paused task can wait for days; keeping one span open across the pause would
produce unbounded spans that never export. Each run of a task therefore gets its own run span and trace, rooted at
the request that caused it (POST /tasks, POST /tasks/{id}/resume, turn continue/answer). Effects that complete
after the verdict (the session handoff of a turn) stay under the ended attempt span. Every committed status that
stops the task running ends the run — a verdict, ApplyControl (queued → paused / cancelled without an attempt) and a
turn failed while queued — and drops submissions noted before it (stale). Effects that do not start a run (stopping
an attempt handed over by recovery) never open one. `task.run` counts runs within one actor lifetime (it restarts at
1 after a server restart).

**Spans under ended parents.** Some work legitimately outlives its parent span: the session handoff after the
verdict, and Gateway tries of a call whose caller left (the call continues to its deadline, journal semantics). The
`gateway.call` span then ends with `call.result=detached`; the eventual settlement is recorded as a
`gateway.detached_settlement` child carrying the final result. Tempo shows such children past the parent's end.

## 5. Metrics

All names are prefixed `agentbox_`. Labels are bounded enumerations only.

| Metric | Type | Labels | Source |
|---|---|---|---|
| `http_requests_total`, `http_request_duration_seconds` | counter, histogram | `route` (registered pattern or `other`), `method`, `code` | API |
| `tasks_finished_total` | counter | `kind` (task/turn), `status` (succeeded/failed/cancelled) | committed terminal status (verdict, ApplyControl, failed queued turn) |
| `task_runs_ended_total` | counter | `kind`, `status` (paused — incl. awaiting_input — or terminal) | end of each run |
| `attempts_finished_total` | counter | `kind`, `outcome_class` (closed set from runner.Classify) | actor |
| `attempt_ready_seconds` | histogram | `kind` | attempt created → worker ready |
| `task_start_seconds` | histogram | `kind` | API submit → first worker ready (submit→ready) |
| `stop_seconds` | histogram | `kind`, `desired` (pause/cancel) | API accepted the pause/cancel/stop → paused/cancelled committed (actor observation time if the request was accepted before this process started) |
| `gateway_calls_total` | counter | `kind`, `result` (completed, replayed, cache_hit, coalesced, detached, or a stable Gateway code such as `model_degraded`, `tries_exhausted`, `budget_insufficient_for_request`) | call coordinator |
| `upstream_tries_total`, `upstream_try_duration_seconds` | counter, histogram | `kind`, `provider` (route), `model`, `status`, `outcome` (incl. `hedge_lost`, `aborted`) | each upstream try / hedge leg |
| `cost_micro_usd_total` | counter | `kind`, `provider` (the leg's route), `model` | actual cost of `ok` settlements, priced per route (`costOn`), including hedge legs that were not the result |
| `breaker_transitions_total` | counter | `kind`, `route`, `to` | circuit breaker state changes of model routes |
| `breaker_state` | gauge | `kind`, `route` | scrape-time breaker state (0 closed, 1 half_open, 2 open) |
| `tool_calls_per_task` | histogram | `kind` | search/fetch/exec calls of a task over all its runs (replays excluded), observed at its terminal status |
| `sandbox_envs` | gauge | `kind` (task/session/exec) | DB: environments not yet stopped (cached 5 s) |
| `sandbox_cleanup_backlog` | gauge | — | DB: stopped, cleanup not done |
| `run_slots_in_use`, `run_slots_capacity`, `admission_queued`, `memory_reserved_bytes` | gauge | — | admission snapshot |
| `exec_slots_in_use`, `exec_slots_capacity`, `exec_queued` | gauge | — | exec gate snapshot |

`model` comes from the server's declared allowlist (requests outside it are rejected before metrics); `provider`
from the configured adapters; `status` is the numeric HTTP status of the try (bounded in practice: the codes upstreams return, plus 0 for transport failures); `result` is a closed set of
Gateway codes.

## 6. Attribute allowlist (security)

Allowed in spans, metric labels and correlated logs: opaque IDs (`task.id`, `attempt.id`, `env.id`,
`session.id`, `incarnation.id`, `subrun.id`, `call.id`, `checkpoint.id`), numbers (`attempt.no`, `try.no`,
durations, sizes, cost), closed enumerations (status, outcome class, kill reason, Gateway codes, HTTP method,
route pattern, kind, provider, declared model name).

Never recorded: prompts, model output, search queries, fetch URLs, worker stdout/stderr, checkpoint state,
API tokens / cookies / provider keys, request paths with user-supplied segments (the route *pattern* is used),
free-text error messages (only stable error codes). A unit test asserts that a span recorded for a real Gateway
call contains only allowlisted attribute keys.

## 7. Failure handling

- Exporter unreachable: the batch processor drops spans after its queue fills; requests are never blocked. Setup
  does not fail if the collector is down.
- Shutdown flushes with a 5 s timeout.
- `/metrics` listener failure at startup is a startup error (operator asked for it explicitly).
- DB-backed gauges use a 5 s cache and a 2 s query timeout; on error the previous value is kept and the error
  is logged at debug level.

## 8. Testing

- `obs`: traceparent parser vectors (W3C spec examples, invalid versions, all-zero IDs, uppercase), no-op
  behaviour, registry bounds/TTL, log handler adds IDs only when a span is active.
- `telemetry`: in-memory span exporter; parent/child relationships across `Carry` and `WithRemoteParent`;
  Prometheus registry gather assertions; attribute allowlist test.
- `edge`: header accepted only when trace-id matches; mismatched/garbage header ignored.
- `call`: with a recording tracer, a retried call yields one `gateway.call` with two `gateway.try` children;
  a cache hit records `call.result=cache_hit`; metrics recorder receives try/cost events.
- `runner`: init / task_start carry the `worker.run` traceparent when tracing is on, and omit it when off
  (default behaviour unchanged, existing fixtures pass).
- Python: `tracecontext` vectors identical to Go's; GatewayClient sends the header when given a valid value,
  never otherwise; sub-run view inherits it.
- Archtest: `obs` is stdlib-only; OTel/Prometheus only in `telemetry` and `cmd/agentbox`.
- End to end: `scripts/demo-observability.sh` against the compose stack; evidence in
  `docs/evidence/2026-10-10-observability.md` (trace JSON exported from Tempo, PromQL results).

## 9. Demonstration

`scripts/demo-observability.sh` starts `deploy/observability/docker-compose.yml` (OTel Collector → Tempo,
Prometheus scraping the server, Loki + Alloy tailing the server log, Grafana with provisioned datasources and an
"Agentbox overview" dashboard), runs the server with the fake upstream and tracing + metrics on, drives tasks
(success, a stopped one, a failing one) and prints the Grafana URL, a trace id to open, and PromQL results.
