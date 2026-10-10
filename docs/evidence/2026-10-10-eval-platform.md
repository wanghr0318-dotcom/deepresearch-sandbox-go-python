# Evidence: agent evaluation platform (`agentbox eval`) — local zero-cost run

> Date: 2026-10-10. Design: [eval platform design](../design/2026-10-10-eval-platform-design.md).
> Command: `sudo AGENTBOX_DEMO_PREBUILT=1 bash scripts/demo-eval.sh` (binaries cross-built from commit
> `e7e13c7` so the manifest carries the VCS revision). All 13 steps passed.
> **Harness version:** fix round 4 — the checker runs with `python -I` in a directory that holds only
> its own three files (fixtures are served from memory through hooked `builtins.open`/`io.open`, and the
> directory is verified before the token is printed); the solution runs in a separate process and
> directory under another parent behind a JSON proxy; the checker and the harness are non-dumpable
> (design §3.2). Earlier recorded runs (exit code only; token in-process; round 3) are superseded.

## Setup

| Item | Value |
|---|---|
| Host | WSL2, kernel 6.6.87.2-microsoft-standard-WSL2 x86_64, cgroup v2, run as root |
| Server | `agentbox server --worker-argv python3,-m,evalworker`, real sandbox (provider/local, production launcher), exec sandbox on (4 slots), PostgreSQL 16 in Docker, fresh data dir and database |
| Upstream | `tests/e2e/fakeupstream -eval-suite eval/suites/demo.yaml`: model, search and pages are local; coding tasks are answered with the suite's `fake_reply`, research with a fixed script. **No model was called; costs are simulated** (configured price 1 / 2 USD per M tokens in / out × fake token counts). |
| Suite | `eval/suites/demo.yaml`, sha256 `54c4aa158d4d…`: 12 coding/terminal tasks, 3 research questions |
| Pinned in every manifest | server build `vcs.revision e7e13c7`, `vcs.modified false`, Go 1.27.1; models default `kimi-k2.6`, declared `kimi-k2.6, kimi-k3`; search `fake`; `upstream_fingerprint 3a5f7d152bfd90a6`; worker argv `python3 -m evalworker`; worker `evalworker@0.1.0+deepresearch.0.1.0` (from the `ready` events); exec image digest `7720bf3acbcf…`; run nonce (e.g. `0b5b8bbe49a395df`) |

## Results

Three runs of the same suite, then two comparisons (N = 15 task runs per run, one repetition each):

| Run | Agent | Concurrency | Pass (95% Wilson CI) | Latency P50 / P95 | Wall | Simulated cost | Model / tool / exec calls |
|---|---|---|---|---|---|---|---|
| A | reference | 4 | 15/15 (79.6–100%) | 0.81 s / 1.03 s | 3.3 s | $0.0063 | 12 / 24 / 12 |
| B | model (fake) | 4 | 9/15 (35.7–80.2%) | 0.81 s / 9.66 s | 10.5 s | $0.0093 | 24 / 24 / 12 |
| C | model (fake) | 1 | 9/15 (35.7–80.2%) | 0.61 s / 9.66 s | 18.5 s | $0.0093 | 24 / 24 / 12 |

- **Isolation held in the real exec sandbox:** all 36 checker runs reported `nondumpable: true`,
  `syspath_writable: []` (no directory on the `-I` interpreter path, nor the nearest existing parent of
  a missing entry, is writable by the sandbox uid — the exec template is read-only) and
  `workdir_clean: true`.
- **Run A validates the harness:** every reference solution passes its checker (through the proxy, with
  the completion token); every research report passes the citation grader (18/18 citations resolve to a
  completed `fetch` result blob of the same task). A coding task — sandbox start, worker, checker plus
  solution process in a fresh exec environment, two artifacts — takes 0.6–0.8 s end to end (exec wall
  59–152 ms).
- **Run B finds the six deliberately wrong answers** (from `trajectories.jsonl`; `harness` is the
  out-of-band verdict in the worker's `eval` artifact):

  | Task | What the fake model answered | Harness verdict | Category |
  |---|---|---|---|
  | `clamp` | `import os; os._exit(0)` (reward hack) | `fail`: `ImportError: solution process exited during import` | `wrong_exit_code` |
  | `median` | `import sys; sys.exit(0)` (reward hack) | `fail`: `ImportError: importing the solution failed: SystemExit: 0` | `wrong_exit_code` |
  | `binary-search` | `while lo < hi` (misses one-element ranges) | `fail`, `AssertionError: (0, -1)` | `wrong_exit_code` |
  | `csv-region-totals` | does not skip the CSV header | `fail`, `script exited 1` | `wrong_exit_code` |
  | `dedupe-lines` | prose, no code block (`extract: raw`) | `fail` | `wrong_exit_code` |
  | `primes-fast` | O(n²) trial division | `timeout` after 9 s (exec wall 9.0 s of 12 s) | `check_timeout` |

  The two hacks have no stdout expectation. With exit-code-only grading they would have passed; now the
  solution runs in its own process, so exiting only ends that process and the checker's import fails.
- **Compare A → B** (`eval-runs/compare-reference-vs-model.md`): success 100% → 60% (intervals
  79.6–100% vs 35.7–80.2%, non-overlapping), P95 latency +8.6 s (the timed-out checker), model calls per run
  ×2 (the reference agent makes no coding model calls), 6 tasks `regressed`, 9 `unchanged`; manifest
  difference: only `agent`. The report warns that N = 15 < 30 and one repetition per task.
- **Compare C → B** (concurrency 1 → 4): identical outcomes (determinism with the fake upstream), wall clock
  18.5 s → 10.5 s (−43.4%); the run is bounded by the ~9.7 s timed-out task.

After the runs: server SIGTERM exit 0; `verify-invariants --quiescent` passed; no environment directories,
processes or cgroups left; the operator token does not appear in any run file.

The forgeries found in review (in-process `__main__._eval_done()` / closure / `__main__._eval_token`,
planted `random.py` / `traceback.py`, fixture forgeries via `open`, `io.open` and `pathlib` including a
forger that restores the file in `atexit`, stray files in the checker's directory, `/proc/<pid>/mem`,
`environ` and `fd` of the checker and the harness, an escaped `setsid` descendant holding the pipes) are
regression tests in `worker/tests/test_evalworker.py`, run on Linux (WSL here, Ubuntu in CI); none passes.

## What a trajectory contains

`trajectories.jsonl` (≈ 10 KiB per task run) has, per task run: outcome and grades, metrics (ledger cost,
calls per endpoint, tries, attempts, models, provider routes), the full operator event stream
(`task_created` → `attempt_created` → worker `ready` → progress/checkpoint/artifact → `task_terminal`), the
Gateway journal (`root/solve/chat/1`, `root/check/exec/1`, `root/task-1/fetch/2`… with state, tries, cost,
`result_ref`), attempts, checkpoints, the ledger, the pinned result and the `eval` artifact or the report.
No prompts, request or response bodies or credentials: that exclusion relies on the operator inspect API
and event stream, which carry metadata only; the token and the judge key are asserted absent by tests.

## What this does not show

- **No real model.** The fake model's answers are scripted; the numbers measure the platform, not an agent.
  A real-model run was not possible from this host (WSL2 has no outbound internet); on a host with a model
  key the same command is `agentbox eval run --suite eval/suites/demo.yaml --agent model` against a server
  configured with the real `--model-base-url`. The LLM judge was exercised only against a test endpoint.
- **Isolation limits** (design §3.2): only plain data crosses the checker/solution boundary; a process
  with `CAP_SYS_PTRACE` or a kernel bug would defeat the memory isolation (the exec sandbox has neither
  the capability nor ptrace); reads in the checker that bypass the `open` hook (`os.open`, `io.FileIO`,
  `mmap`, C extensions, subprocesses) see whatever is on disk, including files the solution planted —
  for fixture names they normally find no file, so such a check fails for correct solutions too: validate
  every suite with `--agent reference` before trusting model scores; full closure needs uid/mount
  separation in the exec layer.
- **Research latency is not realistic:** the fake upstream answers instantly and uses one fixed research
  script for every topic; real research takes minutes (see the
  [sub-run comparison](2026-10-06-m4-subrun-comparison.md)).
- N = 15 per run with one repetition per task; the demo is about the harness, not statistical claims.
