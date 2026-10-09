# Evidence: agent evaluation platform (`agentbox eval`) — local zero-cost run

> Date: 2026-10-10. Design: [eval platform design](../design/2026-10-10-eval-platform-design.md).
> Command: `sudo AGENTBOX_DEMO_PREBUILT=1 bash scripts/demo-eval.sh` (binaries cross-built from commit
> `c798bc6` so the manifest carries the VCS revision). All 13 steps passed.

## Setup

| Item | Value |
|---|---|
| Host | WSL2, kernel 6.6.87.2-microsoft-standard-WSL2 x86_64, cgroup v2, run as root |
| Server | `agentbox server --worker-argv python3,-m,evalworker`, real sandbox (provider/local, production launcher), exec sandbox on (4 slots), PostgreSQL 16 in Docker, fresh data dir and database |
| Upstream | `tests/e2e/fakeupstream -eval-suite eval/suites/demo.yaml`: model, search and pages are local; coding tasks are answered with the suite's `fake_reply`, research with a fixed script. **No model was called; costs are simulated** (configured price 1 / 2 USD per M tokens in / out × fake token counts). |
| Suite | `eval/suites/demo.yaml`, sha256 `9460a93d95e9…`: 10 coding/terminal tasks, 3 research questions |
| Pinned in every manifest | server build `vcs.revision c798bc63dcca`, `vcs.modified false`, Go 1.27.1; models default `kimi-k2.6`, declared `kimi-k2.6, kimi-k3`; search `fake`; worker argv `python3 -m evalworker`; worker `evalworker@0.1.0+deepresearch.0.1.0` (from the `ready` events); exec image digest `7720bf3acbcf…` |

## Results

Three runs of the same suite, then two comparisons:

| Run | Agent | Concurrency | Pass | Latency P50 / P95 | Wall | Simulated cost | Model / tool / exec calls |
|---|---|---|---|---|---|---|---|
| A | reference | 4 | 13/13 (100%) | 0.84 s / 1.02 s | 3.1 s | $0.0063 | 12 / 24 / 10 |
| B | model (fake) | 4 | 9/13 (69.2%) | 0.82 s / 9.67 s | 12.2 s | $0.0090 | 22 / 24 / 10 |
| C | model (fake) | 1 | 9/13 (69.2%) | 0.81 s / 9.68 s | 18.3 s | $0.0090 | 22 / 24 / 10 |

- **Run A validates the harness:** every reference solution passes its checker in the exec sandbox, every
  research report passes the citation grader (18/18 citations resolve to a completed `fetch` result blob of
  the same task). A coding task — sandbox start, worker, checker in a fresh exec environment, two artifacts —
  takes 0.6–0.8 s end to end.
- **Run B finds the four deliberately wrong answers, each with its own category** (from `trajectories.jsonl`,
  the `eval` artifact written by the worker):

  | Task | What the fake model answered | Checker in the exec sandbox | Category |
  |---|---|---|---|
  | `binary-search` | `while lo < hi` (misses one-element ranges) | exit 1, `AssertionError: (0, -1)` | `wrong_exit_code` |
  | `csv-region-totals` | does not skip the CSV header | exit 1, `script exited 1` | `wrong_exit_code` |
  | `dedupe-lines` | prose, no code block (`extract: raw`) | exit 1 | `wrong_exit_code` |
  | `primes-fast` | O(n²) trial division | killed by the harness after 9 s, exit 124 (exec wall 9.0 s of 12 s) | `check_timeout` |

- **Compare A → B** (`eval-runs/compare-reference-vs-model.md`): success −30.8 points, P95 latency +8.6 s
  (the timed-out checker), model calls per run +83% (the reference agent makes no coding model calls),
  4 tasks `regressed`, 9 `unchanged`; manifest difference: only `agent`.
- **Compare C → B** (concurrency 1 → 4): identical outcomes (determinism with the fake upstream), wall clock
  18.3 s → 12.2 s (−33.6%); per-task latency unchanged — the run is bounded by the 9.7 s timed-out task.

After the runs: server SIGTERM exit 0; `verify-invariants --quiescent` passed; no environment directories,
processes or cgroups left; the operator token does not appear in any run file. Fake upstream counters:
chat 56 (code 20, plan 9, summarize 18, report 9), search 18, fetch 54.

## What a trajectory contains

`trajectories.jsonl` (≈ 10 KiB per task run) has, per task run: outcome and grades, metrics (ledger cost,
calls per endpoint, tries, attempts, models), the full operator event stream (`task_created` →
`attempt_created` → worker `ready` → progress/checkpoint/artifact → `task_terminal`), the Gateway journal
(`root/solve/chat/1`, `root/check/exec/1`, `root/task-1/fetch/2`… with state, tries, cost, `result_ref`),
attempts, checkpoints, the ledger, the pinned result and the `eval` artifact or the report. No prompts,
request or response bodies or credentials (the inspect API never returns them).

## What this does not show

- **No real model.** The fake model's answers are scripted; the numbers measure the platform, not an agent.
  A real-model run was not possible from this host (WSL2 has no outbound internet); on a host with a model
  key the same command is `agentbox eval run --suite eval/suites/demo.yaml --agent model` against a server
  configured with the real `--model-base-url`.
- **Research latency is not realistic:** the fake upstream answers instantly; real research takes minutes
  (see the [sub-run comparison](2026-10-06-m4-subrun-comparison.md)).
- N = 1 per task and run; the demo is about the harness, not statistical claims.
