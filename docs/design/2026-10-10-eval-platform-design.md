# Agent evaluation platform (`agentbox eval`)

> Date: 2026-10-10. Status: design for sub-project S2 of the agent-infra gap program.
> Scope approved by the project owner: batch evaluation of the research agent and of coding/terminal
> tasks against a running server, with a pinned run manifest, trajectory capture, graders, reports and
> run-to-run comparison.

## 1. Goals

1. **Batch runs.** `agentbox eval run` executes a *suite* (YAML or JSON in `eval/suites/`) against a running
   server through the existing operator REST API, with configurable concurrency and repetitions.
2. **Reproducible and comparable.** Every run writes a *manifest* that pins what was evaluated: suite hash,
   seed, server build (module version, VCS revision), default and declared models, search provider, worker
   command and worker name/version (from the worker's `ready` event), exec template digest, eval CLI build.
   Two runs are comparable when their suite hashes match; `compare` prints what else differs.
3. **Two task kinds.**
   - `research`: a DeepResearch question run as a task (same `deepresearch` agent the product uses).
   - `coding`: a terminal/coding task. An eval worker inside the task sandbox obtains a solution (from the
     model through the Gateway, or a reference solution for harness validation) and runs the suite's
     checker against it **inside the independent exec sandbox** (`POST /v1/exec`): no network, fresh
     environment per check.
4. **Trajectory capture.** One JSONL record per task run: the operator event stream, the Gateway call
   journal (calls and tries: endpoint, model, state, latency, cost; never bodies or prompts), attempts,
   checkpoints, sub-runs, the budget ledger, the pinned result, the eval artifact and the grades.
5. **Graders.** Deterministic: task status, exit code, stdout match (exact / contains / regex), citation
   locatability (every `[n]` in a research report resolves to an evidence entry whose sha256 is the
   result blob of a completed `fetch` call of the same task), required terms. Optional LLM judge
   (OpenAI-compatible, off by default, hard budget).
6. **Reports.** `summary.json` + `report.md` per run: success rate (overall, per kind, per task),
   latency P50/P95, cost, tool calls, attempts, failure categories. `agentbox eval compare A B` shows
   per-metric and per-task deltas (used later by the "trace doctor" for before/after evidence).
7. **Zero-cost mode.** Everything runs against the test fake upstream (`tests/e2e/fakeupstream`), which
   can load a suite and answer each coding task with the suite's scripted `fake_reply`.

## 2. Non-goals

- No new end-user surface. The eval CLI is operator tooling; it uses the operator Bearer token and
  operator-only endpoints. Users never see eval runs differently from other operator tasks.
- The chat/session path is not driven: session writes are owner-only by design (operator gets 403), so
  research questions use the task path with the same `deepresearch` agent. Driving sessions would require
  creating eval user accounts and is left for later.
- No distributed runner and no result database: a run is a directory of files.
- No exactly-once across runs. Determinism across *runs* needs the fake upstream (or a model that is
  deterministic); determinism *within* a task under crashes comes from the Gateway journal (§8).

## 3. Architecture

```
eval/suites/demo.yaml ─► agentbox eval run ──REST (operator token)──► agentbox server
                              │                                     │ POST /tasks (spec = suite task)
                              │  SSE /tasks/{id}/events (wait+log)  │ task sandbox: python3 -m evalworker
                              │  GET /tasks/{id}/inspect            │   coding: chat → /v1/exec checker
                              │  GET /tasks/{id}/result, artifacts  │   research: deepresearch agent
                              │  GET /server-info (manifest)        │ Gateway journal, ledger, events
                              ▼
               runs/<run_id>/{manifest.json, trajectories.jsonl, summary.json, report.md}
                              ▼
                   agentbox eval compare runs/A runs/B
```

Packages:

| Package | Responsibility | Depends on |
|---|---|---|
| `internal/eval` | suite parsing and hashing, REST client, runner, trajectory records, graders, LLM judge, report, compare | stdlib, `internal/jcs`, `gopkg.in/yaml.v3` — **no** server internals (archtest) |
| `cmd/agentbox` (`eval.go`) | `agentbox eval run|report|compare` flags, token loading, exit codes | `internal/eval` |
| `internal/api`, `internal/app` | new operator-only `GET /server-info` (manifest hook) | — |
| `worker/evalworker` | eval worker: dispatches `coding` tasks to the coding harness and everything else to `deepresearch` | `agentbox_worker`, `deepresearch` |
| `tests/e2e/fakeupstream` | `[stage:code]` replies scripted per eval task (`SetCodeReply`, `-eval-suite`) | `internal/eval` (test-only binary) |

### 3.1 Server hook: `GET /server-info`

Operator-only (Bearer; with accounts enabled the audience is admin; available in every mode). Returns
build info (`runtime/debug.ReadBuildInfo`: module version, `vcs.revision`, `vcs.time`, `vcs.modified`, Go
version), `config_version`, task template, worker argv, the default model and declared model list, search
provider, whether sessions/exec are enabled and the exec template digest. No secrets, keys, URLs with
credentials or prices. The route is additive; default behaviour of all existing endpoints is unchanged.

### 3.2 Eval worker (`python3 -m evalworker`)

The server runs one task worker command (`--worker-argv`). For eval the operator starts the server with
`--worker-argv python3,-m,evalworker`, which:

- for `config.eval.kind == "coding"` runs the coding harness (below);
- otherwise delegates to `deepresearch.app.run` unchanged (research questions).

Coding harness, per task:

1. **Solve** (step `solve`): `agent: model` sends one chat call — system prompt starting with
   `[stage:code]`, user message starting with `[eval-task:<id>]` followed by the task prompt — and
   extracts the first fenced code block (or the whole reply). `agent: reference` uses the suite's
   reference solution without a model call (harness validation, like a "gold patch" run).
2. **Check** (step `check`): one `/v1/exec` call whose code is a small harness that writes the solution
   and the task's fixture files into a scratch directory, runs the suite's checker (`python3 -I check.py`)
   with a timeout, forwards its stdout/stderr and exits with its exit code.
3. **Report**: writes the output artifacts `solution` (the code) and `eval` (JSON: agent, exec status,
   exit code/signal, stdout/stderr (capped 16 KiB each), wall/queue ms, exec image digest).

Judgement is not done in the worker: the CLI grades from the `eval` artifact, so a trajectory can be
re-graded offline.

## 4. Suite format

```yaml
name: demo
version: 1
defaults: {timeout: 5m, limits: {budget_micro: 200000}}
tasks:
  - id: fizzbuzz
    kind: coding
    prompt: "Write fizzbuzz(n) returning a list of strings …"
    reference: |
      def fizzbuzz(n): ...
    files: {"input.csv": "a,b\n1,2\n"}          # fixtures next to solution.py
    check: |                                       # Python; exit code 0 = pass
      from solution import fizzbuzz
      assert fizzbuzz(3) == ["1","2","Fizz"]
    fake_reply: "```python\n...\n```"              # what the fake upstream answers (zero-cost mode)
    expect: {exit_code: 0, stdout_contains: ["ok"]}
    tags: [python, algorithms]
  - id: solid-state
    kind: research
    topic: "固态电池的技术路线与量产进展"
    research: {max_tasks: 2}                       # deepresearch config overrides
    expect: {min_citations: 1, min_locatable_ratio: 1.0, must_mention: ["电解质"]}
    judge: {rubric: "…"}                           # only used with --judge
```

Validation: unique non-empty ids (`[A-Za-z0-9._-]{1,64}`), known kind, coding tasks need `prompt` and
`check`, research tasks need `topic`, unknown fields are rejected. **Suite hash** = sha256 of the JCS
canonical JSON of the parsed suite (format-independent: the same suite in YAML and JSON hashes equal).

## 5. Runner

- Work items = tasks × repetitions; order shuffled with `--seed` (default 1) so runs are reproducible and
  interleaving is controlled. `--concurrency N` workers.
- Each item: `POST /tasks` with `request_id = eval-<run_id>-<task>-<rep>` (idempotent on CLI retry),
  spec = `{eval: {...}}` (coding) or `{topic, ..., eval: {...}}` (research), `limits` from the suite
  (task overrides defaults). The CLI flag `--agent model|reference` and `--model` (passed as
  `config.eval.model` / `orchestrator_model`/`worker_model`) select the agent under test.
- Wait by reading the SSE event stream from the beginning until `task_terminal` (reconnects with
  `Last-Event-ID`); the events are also the trajectory. Per-item `timeout` cancels the task
  (`POST /tasks/{id}/cancel`) and records `timeout`.
- Then `inspect`, `result` and the needed artifacts are fetched; graders run; one JSONL line is appended
  (mutex-serialised) to `trajectories.jsonl`.
- Infra errors (HTTP 5xx, connection refused) are retried with backoff by the client; persistent ones mark
  the item `infra_error` without aborting the run.

## 6. Trajectory record (`trajectories.jsonl`, schema `agentbox.eval.trajectory/v1`)

One JSON object per line:

| Field | Content |
|---|---|
| `schema`, `run_id`, `suite`, `task_id`, `kind`, `rep`, `server_task_id` | identity |
| `submitted_at`, `terminal_at`, `latency_ms` | client-measured wall clock (submit → `task_terminal`) |
| `status`, `status_reason` | final task status |
| `outcome` | `pass` / `fail` / `error` and `category` (see §7) |
| `metrics` | `cost_micro` (ledger spent), `unknown_micro`, `calls` per endpoint, `tool_calls`, `model_calls`, `exec_calls`, `attempts`, `tries`, `models` used |
| `grades[]` | `{grader, pass, score, detail}` |
| `events[]` | operator event stream: `task_seq, source, type, ts, attempt_id, payload` |
| `calls[]` | journal: `call_id, endpoint, model, subrun_id, state, tries_used, cost_charged_micro, result_ref, fail_reason, tries[] {try_no, state, outcome, latency_ms, cost_micro, error, queue_ms, wall_ms}` |
| `attempts[]`, `checkpoints[]`, `subruns[]`, `budget` | as in `GET /tasks/{id}/inspect` |
| `result` | pinned result (summary, outputs) |
| `artifacts` | the `eval` artifact JSON (coding) or the report text (research, capped 64 KiB) |

No request/response bodies, prompts or credentials are recorded (the inspect API never returns them; the
token is never written).

## 7. Graders and failure categories

| Grader | Applies to | Pass when |
|---|---|---|
| `task_status` | all | task `succeeded` |
| `exit_code` | coding | exec `completed` and exit code = `expect.exit_code` (default 0) |
| `stdout` | coding | `stdout_equals` (trimmed) / every `stdout_contains` / `stdout_regex` hold |
| `citations` | research | ≥ `min_citations` citations and locatable ratio ≥ `min_locatable_ratio` (default 1.0) |
| `must_mention` | research | report contains every term |
| `llm_judge` | any task with `judge`, only with `--judge-model` | judge score ≥ threshold |

Outcome = `pass` if every applicable grader passes. Category of the first failing check, in order:
`timeout`, `infra_error` (API failure, exec unavailable), `task_failed:<status_reason>`, `exec_timeout`,
`exec_<status>`, `wrong_exit_code`, `wrong_output`, `no_citations`, `unlocatable_citations`,
`missing_terms`, `judge_below_threshold`.

**LLM judge.** OpenAI-compatible `POST <base>/chat/completions` from the CLI (outside the sandbox),
key from `AGENTBOX_JUDGE_API_KEY` or `AGENTBOX_MODEL_API_KEY` (environment only; never logged).
Prompt = rubric + task + answer (report or solution, capped), reply must be JSON `{"score":0..1,"reason":…}`.
Budget: `--judge-budget-usd` (default 0.20) with configured prices; before each call the worst-case cost
(prompt estimate + max_tokens) is reserved; when it does not fit, remaining judgements are `skipped`
(not failed). Judge spend is reported separately in the summary.

## 8. Cost control and replay

- **Fake upstream**: `fakeupstream -eval-suite eval/suites/demo.yaml` answers `[stage:code]` chats with the
  task's `fake_reply` and research stages with the fixed research script; search and fetch are local.
  Costs are then *simulated* (configured price × fake token counts).
- **Budgets**: every task carries `limits.budget_micro` (suite default); the server's ledger refuses calls
  beyond it. The judge has its own budget.
- **Journal replay**: within a task, every Gateway call has a deterministic call id
  (`<step>/<kind>/<n>`) and a fingerprint. After a worker crash or server restart, a resumed attempt that
  re-issues the same call gets the stored result from the journal instead of a new upstream call, so the
  model answer, the checker run and the cost are not repeated (`trajectory.calls[].tries` shows replays as
  a single try). Across separate eval runs nothing is replayed: a rerun against a real model may differ,
  which is exactly why the manifest pins the models and the suite.

## 9. Reports and compare

`summary.json` (schema `agentbox.eval.summary/v1`): counts, success rate (+ per kind, per task pass@k
style `passes/runs`), latency P50/P95/max (nearest-rank), cost total/mean, tool/model/exec calls, attempts,
failure categories, judge spend. `report.md` renders the same with a per-task table.

`compare A B`: manifest differences (suite hash mismatch is a warning), metric deltas (B − A, relative),
per-task status changes (fixed / regressed / unchanged). Output Markdown to stdout, `--json` for machines.
Exit code 0 (comparison is informational).

## 10. Failure handling

- Server unreachable at start → exit 1 before submitting anything.
- `/server-info` missing (older server) → manifest records `server_info: unavailable`, run continues.
- A task that cannot be created (400) → item `error/infra_error` with the API error code.
- Ctrl-C → the runner cancels in-flight server tasks it created, writes partial outputs and exits 130.

## 11. Testing

- Suite: YAML/JSON parsing, validation errors, defaults, hash equality across formats.
- Runner: against an `httptest` fake server implementing the used API subset (create, SSE, inspect,
  result, artifacts, cancel, server-info): concurrency bound, repetitions, seed order, timeout → cancel,
  transient 503 retry, JSONL content (no token).
- Graders: exit code, stdout modes, citation parsing (locatable and not), judge with fake endpoint and
  budget exhaustion.
- Report/compare: percentiles, categories, deltas, Markdown golden fragments.
- Worker: `evalworker` coding harness with fake Gateway (pytest), exec harness script behaviour.
- API: `/server-info` route in OpenAPI, operator-only, no secrets.
- End-to-end evidence: a real local run with the real sandbox and the fake upstream
  (`scripts/demo-eval.sh`), numbers in `docs/evidence/2026-10-10-eval-platform.md`.

## 12. Demonstration

`sudo bash scripts/demo-eval.sh` starts PostgreSQL-backed server with `evalworker` and the fake upstream,
runs the demo suite (10 coding + 3 research) twice — `--agent reference` (harness validation: all coding
tasks must pass) and `--agent model` (the fake model, whose scripted answers include deliberate bugs) — and
prints the compare table. Zero model cost.
