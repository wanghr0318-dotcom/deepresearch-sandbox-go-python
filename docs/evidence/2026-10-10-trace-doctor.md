# Trace doctor: a measured closed loop (2026-10-10)

> Run: `sudo AGENTBOX_DOCTOR_OBS=1 bash scripts/demo-doctor.sh` on WSL2 (cgroup v2, Python 3.14, PostgreSQL in
> Docker, the observability stack of `deploy/observability` running). Real sandbox, real Gateway, real exec
> sandbox; two `fakeupstream` processes stand in for the model providers and the web, so the eval runs cost
> nothing. Design: [trace doctor design](../design/2026-10-10-trace-doctor-design.md).

## Result in three lines

- **Before** (deliberately mis-configured server): 36/64 tasks pass — success 56.2 %, Wilson 95 % [44.1 %, 67.7 %];
  latency P95 90.2 s; wall clock 208.4 s.
- **Doctor**: 28 of 28 failures explained by two rules (16 `model_try_hang`, 12 `fetch_deadline`); it proposed
  `--model-try-timeout 0s → 2s` and `--call-deadline 1s → 10s`, written as `proposal.patch` and `experiment.flags`.
- **After** (same suite, seed, repetitions and concurrency, server started from `experiment.flags`): 64/64 —
  success 100 %, Wilson 95 % [94.3 %, 100 %]; P95 15.1 s; wall clock 42.2 s. The intervals do not overlap, so
  the improvement is beyond sampling noise.

## Setup

- Suite `eval/suites/doctor.yaml`: 6 coding tasks (one model call, one checker run in the exec sandbox) and 2
  research questions (plan, two searches, six fetches, summaries, report). Every scripted answer is correct, so a
  failure is an infrastructure failure. `--repeat 8 --concurrency 8 --seed 7` → N = 64 per run.
- Fake upstreams: **primary** answers model calls in 200 ms but every 4th chat request hangs
  (`fakeupstream -every chat:4:hang`), and serves search and fetch with 1.5 s fetch latency; **backup** answers model
  calls in 400 ms and never fails.
- The "before" flags file (paths and ports abbreviated):

```
--run-slots=8
--model-base-url=http://127.0.0.1:<primary>/v1
--model-fallback-file=<out>/fallback.json        # one provider: backup
--search-provider=fake
--search-base-url=http://127.0.0.1:<primary>
# fetches take 1.5 s at the fake site, but the search/fetch call deadline is 1 s
--call-deadline=1s
# no --model-try-timeout: a hung primary try holds the model call until the eval timeout
--otlp-endpoint=http://127.0.0.1:4318
--metrics-listen=127.0.0.1:9464
--log-file=<observability>/logs/agentbox-doctor.log
```

The doctor is not told about the injected faults: it reads `trajectories.jsonl` (the Gateway journal and the
event stream of each task), the flags file, and Tempo/Prometheus/Loki.

## Run A (before)

| | value |
|---|---|
| passed | 36 / 64 (coding 36/48, research 0/16) |
| failure categories | `timeout` 16, `task_failed:worker_error` 12 |
| latency P50 / P95 / max | 1.04 s / 90.2 s / 90.2 s |
| wall clock | 208.4 s |

## Doctor output (abridged from `findings.md`)

```
64 tasks: 28 failed, 0 slow (not failed); 28 of 28 explained by a rule.

| Cause          | Failed | Slow |
| model_try_hang |     16 |    0 |
| fetch_deadline |     12 |    0 |

1. Model tries hang without a per-try timeout (model_try_hang, critical)
16 model tries in 16 calls ran ≥ 10× the provider's median latency (or never finished) without succeeding;
ok tries took p50 201 ms / p99 205 ms.
- counts: hung_calls=16, hung_tries.primary=16, ok_tries.primary=48, tasks_with_hung_tries=16
- latency ms: hung_max.primary=89757, ok_p50.primary=201, ok_p99.primary=205
- shares: hung_try_time_of_affected_task_latency=99.3%
- --model-try-timeout is default 0s
- example: is-prime#2 task_f7801eb9… call root/solve/chat/1 trace 4d81b741c7907e8d41dc7f88ac3dfde7
  — try 1 on primary: unknown after 59714 ms (upstream_unconfirmed)
- Proposal: --model-try-timeout: default 0s → 2s (4 × p99 of ok model tries (205 ms), bounded to [2s, 2m])

2. Search/fetch calls hit the call deadline (fetch_deadline, critical)
144 search/fetch calls were cut by the call deadline (cut tries p50 990 ms, max 992 ms — the cut-off, not the
time the target needs); ok fetch tries: 0.
- counts: calls_deadline.fetch=144, ok_tries.fetch=0, ok_tries.search=48, tasks=12
- --call-deadline is 1s; the tries were cut at ≈ 85–100 % of --call-deadline
- example: sodium-ion-storage#4 task_37586b90… call root/task-1/fetch/1 trace 6ede2a82c4981cbbbabf4cbd4996f3bc
  — fetch call unknown: try 1 unknown upstream_unconfirmed after 990 ms
- Proposal: --call-deadline: 1s → 10s (max(4 × p99 of ok search/fetch tries, 10 × the current deadline),
  bounded to [10s, 2m])
```

Note what the journal actually records for a deadline cut: the call stays `unknown` with no fail reason and its
try is `unknown / upstream_unconfirmed` at ≈ 990 ms. A first version of the rule looked for
`call_deadline_exceeded` and missed all 144; the rule now recognises tries cut at 85–100 % of the configured
deadline (or, without the config, failed tries that all stop at the same latency).

**Traces (Tempo).** For the five slowest tasks the doctor found the run's trace by `task.id` and summed span
durations by name, e.g. for `sodium-ion-storage#1` (trace `333440c1a708815b9ee636804c5b878c`):

```
task 90035 ms (100%); attempt 90021 ms; worker.run 89876 ms; gateway.call 89767 ms; gateway.try 89757 ms (100%);
env.create 131 ms (0%); worker.handshake 83 ms; worker.start 22 ms
```

— a single upstream try is the whole task; sandbox start and handshake are noise.

**Logs (Loki)** agree exactly with the journal: `gateway: try /v1/fetch outcome=unknown code=upstream_unconfirmed`
144 lines, `gateway: try /v1/chat/completions route=primary outcome=unknown code=upstream_unconfirmed` 16 lines,
each with sample trace ids.

**Metrics (Prometheus)** are a sampled cross-check and undercount: `fetch result=call_deadline_exceeded` 132,
`chat result=detached` 13, tasks finished 51 of 64. `increase()` cannot see what a fresh server process counted
before its first scrape (5 s interval; the fast coding tasks finish in about 1 s) nor after its last one. The
doctor shows these numbers beside the journal's rather than relying on them.

## The proposal

`proposal.patch` (applies cleanly with `patch(1)`; the script checks that the result equals `experiment.flags`):

```diff
@@ -13,8 +13,10 @@
 # fetches take 1.5 s at the fake site, but the search/fetch call deadline is 1 s
---call-deadline=1s
+--call-deadline=10s
 # no --model-try-timeout: a hung primary try holds the model call until the eval timeout
 --otlp-endpoint=http://127.0.0.1:4318
 --metrics-listen=127.0.0.1:9464
 --log-file=<observability>/logs/agentbox-doctor.log
+# doctor (model_try_hang, rule): 4 × p99 of ok model tries (205 ms), bounded to [2s, 2m]: a hung try fails over to the next provider after 2s
+--model-try-timeout=2s
```

## Run B (after) and the comparison

`agentbox eval compare A B`:

```
Sample: N = 64 (A) vs 64 (B) task runs; success 95% CI A 44.1%–67.7%, B 94.3%–100.0%.

| Metric              | A       | B       | Δ        |
| success_rate        | 56.2%   | 100.0%  | +43.8%   |
| latency_p50         | 1.0s    | 1.0s    | -0.0s    |
| latency_p95         | 90.2s   | 15.1s   | -75.1s   |
| wall_clock          | 208.4s  | 42.2s   | -166.2s  |
| cost_per_run        | $0.0002 | $0.0006 | +$0.0005 |
| model_calls_per_run | 1       | 1.75    | +0.75    |
| extra_tries_per_run | 0       | 0.44    | +0.44    |
| citations_locatable | 0.0%    | 100.0%  | +100.0%  |

| Task                | A   | B   | Change    |
| add                 | 8/8 | 8/8 | unchanged |
| count-vowels        | 6/8 | 8/8 | fixed     |
| dedupe              | 8/8 | 8/8 | unchanged |
| flatten             | 5/8 | 8/8 | fixed     |
| is-prime            | 4/8 | 8/8 | fixed     |
| reverse-words       | 5/8 | 8/8 | fixed     |
| sodium-ion-storage  | 0/8 | 8/8 | fixed     |
| solid-state-battery | 0/8 | 8/8 | fixed     |
```

Reading it honestly:

- The two changes act on disjoint task kinds, which separates their effects without a third run: coding tasks
  make no fetches, so their 36/48 → 48/48 is the try timeout; 12 of the 16 research failures were fetch
  deadlines and 4 were hangs, and both are fixed (0/16 → 16/16).
- The faults are still there in B. The primary still hangs on every 4th chat request; each hang now costs a 2 s
  try and an immediate failover (`extra_tries_per_run` 0.44; affected coding tasks take ≈ 3.2 s instead of 1 s).
  Cost per run triples (simulated prices): research tasks now run to completion, and every cut try is charged its
  estimate as unknown.
- Running the doctor on run B explains all 7 remaining slow tasks with `model_upstream_errors` (primary fails
  25 % of tries; failed-try time is 61.8 % of those tasks' latency) and suggests — as an advisory, not applied —
  a hedge or a healthier primary as the next experiment.
- The fault schedule is per request count, so which tasks hit a hang depends on interleaving; a second complete
  run of the script gave the same totals (36/64 → 64/64).

## Optional model step

One call (`--advisor-model kimi-k2.6 --advisor-base-url https://api.moonshot.cn/v1`) with the same run A findings.
Its summary:

> 28 of 64 tasks failed, split between model tries hanging indefinitely on the primary provider (16 tasks) and
> fetch calls being cut by the 1-second call deadline (12 tasks). Hung model tries lasted up to ~90 seconds,
> while healthy model tries had a p99 of only 205 ms. All 144 fetch tries were truncated at about 990 ms with zero
> successful fetch tries, showing the 1-second deadline is too short. […]

It proposed the same two values (`--model-try-timeout 2s`, `--call-deadline 10s`); both passed the allowlist and
bounds, and since the rules already proposed those flags the rule values stand. Budget accounting at an assumed
conservative price (4 / 16 USD per million input / output tokens): 0.039 USD for this call, 0.11 USD for the four
calls made while developing the step (one HTTP 400, which went away once the request stopped sending `temperature: 0`; one reply cut at a
1 500-token cap before any content, because the model reasons first, which led to `--advisor-max-tokens` and a
clearer error; one earlier run whose summary misread the cut-off latency as the time a fetch needs, which led to
per-kind evidence and a prompt line on cut-off latencies). The model never sees prompts, pages or answers — only
ids, counts, latencies and codes — and its key was never written (checked against every output file).

## Limits

- The scenario is constructed: the faults are injected at a known rate. What the demo shows is that the doctor
  finds them from the journal alone and that its proposals measurably fix them, not that every real incident has
  a two-flag fix.
- Proposed values are heuristics (4 × p99 of ok tries; 10 × a deadline that cuts every try). With few ok tries
  the estimate is weak; the report shows the sample sizes.
- Both proposals were applied together. Here the per-task table separates them; in general, apply one change per
  experiment to attribute effects.
- Prometheus counts lag the journal (see above); traces and logs agree with it exactly.
