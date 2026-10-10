# Glossary of internal references

Code comments, tests and design documents in this repository use short identifiers from the design process.
This page explains what they point to. The authoritative tables live in the
[v0.2 release design](design/2026-10-03-v0.2-first-release-design.md) (in Chinese; "规格" in comments means
this document).

## §N — section of a design document

- In Go/Python comments, **"规格 §N" / "spec §N"** without a document name refers to the
  [v0.2 release design](design/2026-10-03-v0.2-first-release-design.md). Frequently cited sections:

  | § | Topic |
  |---|---|
  | §4.5–§4.6 | Sandbox isolation profile and the privilege-dropping start sequence (re-exec init) |
  | §5 | Worker protocol v1 (handshake, checkpoints, artifacts, verdicts) |
  | §7 | Transactions, lock order, retries, single-owner execution host |
  | §8 | State machines (task, attempt, environment, sub-run, session) |
  | §9 | Gateway (edge binding, access check, call journal, the two commit points, budgets, egress guard) |
  | §10 | Exec sandbox for model-written code |
  | §11 | Redis cache and request coalescing |
  | §12 | Sessions (freeze / evict / restore, workspace) |
  | §13 | Bounded multi-agent sub-runs |
  | §14 | Startup recovery and error classification |
  | §16 | Verification: invariants (§16.3), fault experiments (§16.4), measurements (§16.5) |

- **"design §N"** or **"<topic> design §N"** refers to the design document named next to it, for example
  `docs/design/2026-10-10-model-fallback-design.md §4.5` (hedged requests).
- **"code organization §9"** is [code organization](design/2026-10-03-code-organization.md) §9 (function-level
  quality rules and the lint configuration).

## I1–I16 — invariants

Properties checked by `agentbox verify-invariants` and by tests (`internal/invariants`). The full table is
spec §16.3. Each invariant is tagged **[A]** always holds, **[B]** holds within a deadline (otherwise an
occupancy record and an alert must exist), or **[Q]** holds when quiescent (after an experiment and cleanup).
Examples: **I1** a cleaned-up environment has no mounts, cgroups, listeners or directories left; **I3** each
budget ledger's `reserved` equals the sum of its held reservations, and every reservation is in exactly one bucket; **I11** live environments have disjoint
UID ranges; **I14** a recorded call result never changes; **I16** each `request_id` maps to exactly one resource.

## E1–E49 — fault experiments

Numbered fault-injection and recovery experiments from spec §16.4 (the table states the injected fault and the
expected outcome). Tests that implement one carry its number in the test name or comment, e.g. **E1** kill the
worker after the second checkpoint (resume from checkpoint 2), **E17** upstream 429 / hang / cut-off response
(≤ 3 tries per call, unknown charged once), **E18** SSRF attempts (metadata IP, DNS rebinding, redirect to a
private address) are blocked, **E28** evict an idle session and restore it from its checkpoint. Variants carry
a letter (**E11a**, **E11b**).

## M1–M4 and "Plan N" — milestones and implementation plans

The v0.2 release was built in four milestones ([roadmap](design/2026-10-03-roadmap.md) §3, §7): **M1** sandbox
runtime and control plane, **M2** real research through the Gateway, **M3** cache, web workbench and user
accounts, **M4** chat sessions, skill-based agent and multi-agent sub-runs. Each milestone was executed as
numbered implementation plans; the plan files themselves are not part of the repository, so a "Plan N"
reference only says in which step a rule was introduced:

| Plan | Scope |
|---|---|
| 1A / 1B | Local provider core; 1B is the privilege-drop start-sequence spike ([experiment](experiments/2026-10-05-spike-1b.md)) |
| 2 | Real sandbox (namespaces, mounts, seccomp, cgroups) on Linux |
| 3 | Worker protocol and Python worker SDK |
| 4 | PostgreSQL persistence and BlobStore |
| 5, 6 | Control plane: task / attempt state machines, runner, recovery |
| 7, 8 | Gateway, budgets and call journal; real DeepResearch (M2) |
| 9 | Redis cache and request coalescing (M3) |
| 10 | Operator web workbench |
| 11 | User accounts |
| 12 | Session backend (M4) |
| 13 | Skill-based chat agent, tools and chat UI |
| 14 | Bounded multi-agent sub-runs |
| 15 | Separate exec sandbox and isolation hardening |

## D1–D13 — product / contract decisions

Numbered decisions recorded in a design document's decision table; the number is local to that document — for
example the [chat sessions design](design/2026-10-06-m4-chat-sessions-design.md) (D1: answer vs. research is
decided by the model, with a "deep research" switch to force research) and the
[provider contract](design/2026-10-05-provider-contract.md).

## Other short names

- **Tx1 / Tx2** — transactions of the Gateway's call flow (`internal/gateway/call`, spec §9.4–§9.5): Tx1
  (`BeginCall`) registers the call in the journal or finds the existing record; Tx2 (`ReserveTry`) allocates a
  try number and reserves budget before the request is sent upstream (or `CompleteFromCache` for a cache hit).
- **try / leg** — one attempt to send a call upstream; with hedging, the two concurrent tries of one call are
  its legs.
- **attempt** — one run of a task's worker; a crash or fault starts a new attempt from the last checkpoint.
- **sub-run** — a bounded child research run started by the agent (spec §13).
- **slot** — in the Kubernetes provider, the node-local directory shared by the server and one Pod
  ([k8s provider design](design/2026-10-10-k8s-provider-design.md)).
