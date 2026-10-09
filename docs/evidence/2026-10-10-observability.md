# Observability demo run (2026-10-10)

What this run shows: one trace per user action, running from the API request through the task actor and the
sandbox into every Gateway call. It also shows the Prometheus metrics, and logs in Loki that link to their traces.
The run covered a successful task, a paused-and-resumed task, a failing task and two session turns (one of them
stopped).

What it does **not** show: production load, real-model latency, or overhead under concurrency. The upstream is
the fake upstream, with fixed latencies of chat 300 ms, search 80 ms and fetch 50 ms. Its second chat call returns
503. Costs come from the configured demo price table: input 1 and output 2 USD per million tokens.

Environment: WSL2 (kernel 6.6, mirrored networking) on an Intel Core Ultra 9 275HX, plus Docker Desktop 29.4.2.
PostgreSQL 16 ran from `deploy/docker-compose.yml`. The observability stack ran from
`deploy/observability/docker-compose.yml`: OTel Collector 0.112.0, Tempo 2.6.1, Prometheus 2.55.1, Loki 3.2.1,
Alloy 1.4.3 and Grafana 11.3.0.

Command (as root, in WSL):

```bash
sudo bash scripts/demo-observability.sh       # all 16 steps passed
```

## What was run

| # | Action | Result |
|---|---|---|
| A | `POST /tasks` DeepResearch topic; 2nd chat call → upstream 503 | succeeded, 1 attempt, Gateway retried (try 2 ok) |
| B | `POST /tasks`, `POST /tasks/{id}/pause` after the first checkpoint, then `/resume` | paused → succeeded; 2 runs, 2 traces |
| C | `POST /tasks` with `limits.budget_micro = 10` | failed (`worker_error`): first chat call rejected 402 `budget_insufficient_for_request` |
| T1 | register user, `POST /sessions`, `POST /sessions/{id}/messages` | turn succeeded |
| T2 | second message (`deep_research: true`), `POST /turns/{id}/stop` while running | turn paused |

## Trace of task A (exported from Tempo: [trace-task-a.json](2026-10-10-observability/trace-task-a.json))

Rendered with `scripts/dev/span-tree.py`; search/fetch calls abbreviated:

```
39 spans
POST /tasks  23.3 ms  http.route=/tasks http.status_code=201
  task  3614.2 ms  task.kind=task task.status=succeeded
    admission.acquire  0.0 ms
    attempt  3588.1 ms  attempt.no=1 outcome.class=succeeded
      env.create  131.1 ms
      worker.run  3434.2 ms  worker.mode=task outcome.class=succeeded
        worker.start  12.3 ms
        worker.handshake  92.1 ms
        gateway.call  367.5 ms  gateway.kind=chat call.result=completed cost.actual_micro=407
          gateway.try  305.8 ms  try.no=1 try.outcome=ok
        checkpoint.commit  12.8 ms  checkpoint.status=committed
        gateway.call  115.2 ms  gateway.kind=search call.result=completed           (+3 fetch calls, ~90 ms each)
        gateway.call  1408.1 ms  gateway.kind=chat call.result=completed cost.actual_micro=530
          gateway.try  0.6 ms  http.status_code=503 try.no=1 try.outcome=retryable ERROR(upstream_unavailable)
          gateway.try  301.6 ms  http.status_code=200 try.no=2 try.outcome=ok
        checkpoint.commit  12.2 ms
        ...                                                                         (search + 3 fetch, 2 chat, 2 checkpoints)
        worker.finalize  17.8 ms
      env.stop  18.4 ms
```

Points to note:

- **The trace crosses the sandbox boundary.** The `gateway.call` spans are children of `worker.run`. The Gateway
  edge only knows the `attempt` span, from the `Bind` context. So the calls could only land under `worker.run` if
  the Python worker forwarded the `traceparent` it received in `init` as a header, and the edge accepted it after
  checking that its trace-id belongs to the attempt.
- **The retry is visible.** The second chat call shows a failed try (503, 0.6 ms) and a successful try (301.6 ms).
  The gap between them, about 1.1 s, is the Gateway's backoff.
- **Where time goes in startup:** creating the environment took 131 ms. Starting the worker took 12 ms, and the
  protocol handshake took 92 ms (Python import and init).
- **Attribute keys in this trace:** IDs (`task.id`, `attempt.id`, `env.id`, `call.id`, `checkpoint.id`), numbers,
  and closed enumerations. There is no prompt, query, URL or key. The demo's last step checks every exported trace
  for the API key, the topic text, `Bearer ` and the upstream URL, and passed.

Task B is two traces. One is rooted at `POST /tasks` (run 1 → `task.status=paused`, `control=pause`). The other is
rooted at `POST /tasks/{id}/resume` (run 2, attempt 2 → succeeded). Task C:

```
POST /tasks  →  task (failed, ERROR worker_error)  →  attempt  →  worker.run
  gateway.call  13.5 ms  http.status_code=402 call.result=budget_insufficient_for_request ERROR(...)
```

Session turns run in a warm incarnation, so they have no `env.create` or `worker.start` spans:

```
POST /sessions/{id}/messages  13.0 ms
  turn  639.6 ms  task.kind=turn task.status=succeeded
    attempt  372.0 ms
      worker.run  358.5 ms  worker.mode=session
        worker.handshake  6.9 ms        (task_start → task_accepted)
        gateway.call  332.2 ms  gateway.kind=chat
      session.handoff  16.0 ms
```

## Prometheus (instant queries after the run; full output: [promql.txt](2026-10-10-observability/promql.txt))

| Query | Result |
|---|---|
| `sum by (kind, status) (agentbox_tasks_finished_total)` | task: succeeded 2, paused 1, failed 1; turn: succeeded 1, paused 1 |
| `histogram_quantile(0.5, … agentbox_attempt_ready_seconds_bucket)` | task 0.175 s, turn 0.025 s (bucket-interpolated) |
| `histogram_quantile(0.95, … agentbox_task_start_seconds_bucket)` (submit→ready) | task 0.45 s, turn 0.475 s |
| mean `agentbox_stop_seconds` (stop observed → paused verdict) | task 0.66 s, turn 1.05 s |
| `sum by (kind, status, outcome) (agentbox_upstream_tries_total)` | chat 200 ok 10, chat 503 retryable 1, search 4, fetch 12 |
| `histogram_quantile(0.95, … agentbox_upstream_try_duration_seconds_bucket)` | chat 0.486 s, search 0.098 s, fetch 0.098 s |
| `sum by (kind, result) (agentbox_gateway_calls_total)` | chat completed 10, chat budget_insufficient_for_request 1, search 4, fetch 12 |
| `sum by (kind, model) (agentbox_cost_micro_usd_total)` | chat fake-model 10 520 micro-USD |
| mean `agentbox_tool_calls_per_task` | task 4, turn 0 |
| `agentbox_sandbox_envs` / `agentbox_sandbox_cleanup_backlog` | session 1 (idle incarnation), task 0, exec 0 / 0 |
| `sum by (route, code) (agentbox_http_requests_total)` | route label is the pattern, e.g. `/tasks/{id}` 55, `/sessions/{id}/turns` 21 |

This run predates review fix round 1, and three of the rows above reflect the old semantics:

- At the time, `tasks_finished_total` also counted paused runs. It now counts only terminal statuses, and pauses
  go to `task_runs_ended_total`.
- `stop_seconds` started when the actor observed the stop. It now starts when the API accepted the request, which
  adds the actor's notification delay (milliseconds).
- `tool_calls_per_task` was observed per run. It now covers a task's whole life and is observed at its terminal
  status.

A cold sandbox (attempt-ready p50 175 ms) and a warm session incarnation (25 ms) differ by about 7× in start
latency. This is the number that motivates a warm pool.

## Loki

`{job="agentbox"}` held 2 556 lines from the run. `{job="agentbox"} |= "8e3fa409b4d97d93b82d112f5aa25cbf"` (task A's
trace) returned 14 lines: the Gateway try log lines and the API access log line. Each carries the `trace_id` and
`span_id` of the span that was active. Grafana is provisioned so that the "TraceID" derived field on a log line
opens the trace in Tempo, and Tempo's trace-to-logs link opens the matching Loki query. These links come from the
provisioned configuration; they were **not** clicked through in a browser (see "How this was verified").

## How this was verified

- **Traces:** fetched from Tempo's HTTP API (`/api/search` with TraceQL on `span.task.id`, then `/api/traces/<id>`)
  and rendered with `scripts/dev/span-tree.py`. The demo's last step scans the exported JSON for secrets.
- **Metrics:** instant queries against Prometheus's HTTP API (`/api/v1/query`); the output is in `promql.txt`.
- **Logs:** Loki's HTTP API (`/loki/api/v1/query_range`), counting lines that contain task A's trace id.
- **Dashboard:** 23 of the 24 queries of the provisioned "Agentbox overview" dashboard were run through Grafana's
  `/api/ds/query` and returned data. Two gauge panels were empty after the server had stopped, which is expected.
  The traces table is not covered: Grafana runs TraceQL search in the browser, and its backend rejects that query
  type. Its TraceQL was checked directly against Tempo instead.
- **Not done:** no panel, trace view, or log↔trace link was viewed in a browser, and no screenshots were taken.

## Overhead when off

`go test -bench 'OffPath|SpanOn' -benchtime 2s ./internal/obs/ ./internal/telemetry/` on the same host. Each
iteration is one span with three attributes, one metric event and one traceparent lookup:

| Configuration | ns/op | allocs/op |
|---|---|---|
| off (default; no tracer, no recorder) | 29.6 | 2 (the variadic attribute slice) |
| on (OTel SDK, batch exporter, Prometheus counter) | 1 580 | 16 |

A Gateway call costs hundreds of milliseconds, so either figure is negligible. With observability off, the
server's behaviour and the worker protocol are unchanged: `init` and `task_start` carry no `traceparent`, and no
extra listener is opened.

## Known limits

- The submit registry that links an API request to the actor's run lives in memory. After a server restart the run
  span becomes a new root.
- Session-actor operations, such as starting, freezing or evicting an incarnation, are their own traces, keyed by
  `session.id`. A turn whose incarnation had to be started first shows that gap as untraced time inside the `turn`
  span (here 640 ms turn vs. 372 ms attempt).
- `sandbox_envs` and `cleanup_backlog` come from a `GROUP BY` over the `environments` table, cached for 5 s. That
  table grows with history, so a larger installation needs an index or a maintained counter.
- On native Linux, the Prometheus container reaches the host's `/metrics` through the docker bridge. The demo
  script handles this; a manual setup must set `--metrics-listen` to the bridge gateway.
