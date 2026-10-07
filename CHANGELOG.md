# Changelog

## v0.2.0 — 2026-10-07

First public release.

- **Runtime:** Go control plane with task and session state machines, checkpoints and crash recovery, invariant checker, fault injection.
- **Sandbox:** namespaces, mapped UID ranges, read-only rootfs template, seccomp, capabilities `{KILL}`, cgroup v2 limits; separate network-less exec sandbox for model-written Python.
- **Gateway:** per-attempt Unix socket, call journal, task and sub-run budgets, per-turn tool quota (30), SSRF-safe egress, Redis cache and call coalescing.
- **Agent:** Python worker SDK and versioned protocol; chat agent with skills, ask-user, todo list, web search/fetch, `run_python`; parallel sub-runs; challenge / bot-check pages are dropped instead of becoming sources.
- **Product:** accounts (registration passwords 8–16 characters with at least two of digits / upper / lower case), sessions (freeze / evict / restore), chat UI with resizable three-pane layout (progress / sources / report panel, collapsed by default), merged and auto-folded step chain, full report rendered in the conversation with a report card, stop / continue / write-now / restore; operator workbench.
- **Evidence:** real-model acceptance on a 4 vCPU / 8 GiB Linux VM; serial vs. parallel comparison (N = 4 per mode).
