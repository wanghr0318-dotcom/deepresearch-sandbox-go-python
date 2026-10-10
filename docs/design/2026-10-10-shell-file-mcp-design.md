# Workspace tools (shell + files) and MCP through the Gateway

> Date: 2026-10-10. Status: approved scope (project owner); this document is the design of record for the implementation.
> Builds on: [v0.2 spec](2026-10-03-v0.2-first-release-design.md) §9 (Gateway), §10 (exec, Plan 15); [chat sessions](2026-10-06-m4-chat-sessions-design.md) (turn = task, tool budget D3, tools D9).

## 1. Goals

1. **Workspace tools.** Today the agent has one execution primitive, `run_python`: one-shot, stateless, a fresh exec sandbox per call. Coding-style work ("write a file, run its test, read the failure, fix it") needs state that survives between tool calls. Add a per-turn **workspace** with four tools:
   - `exec_shell(command, timeout_s)` — `bash -c` in the sandbox, returns exit code, stdout/stderr (capped and truncated like exec), and the list of files the command changed;
   - `read_file(path, start_line, max_lines)`, `write_file(path, content)`, `list_dir(path)`.
2. **Same isolation and accounting as exec.** Every `exec_shell` runs in an exec environment (no network, own UID range, seccomp, cgroup CPU/memory/pids, `/out` tmpfs size = disk quota, 1 MiB stream caps), goes through the exec slots and the task's exec quota (count / CPU / wall), and is journaled as a call with idempotent replay.
3. **Clear lifecycle.** A workspace is created on first use, destroyed when its turn reaches a terminal state or after an idle timeout, and after a server crash it is either recovered (durable head) or reported as lost — never silently replaced by an empty one.
4. **MCP.** The agent can use tools from operator-configured MCP servers (stdio or streamable HTTP). The sandbox never talks to MCP servers: the Gateway lists (`tools/list`) and calls (`tools/call`) them, filters by an allowlist, applies timeouts, the per-turn tool budget and the call journal. A demo MCP server ships in the repo and is used in tests.
5. **Off by default.** Without `--workspace-tools` and `--mcp-config` the server, the worker and the UI behave exactly as before (same turn spec bytes, same tools, `/v1/exec` unchanged).

## 2. Non-goals

- Long-lived processes inside the workspace (background servers, REPL state). Processes do **not** persist between `exec_shell` calls; files do (§3.1 explains why).
- An interactive terminal / PTY.
- MCP resources, prompts, sampling, elicitation, notifications; MCP servers that need OAuth. Only `tools/list` and `tools/call`.
- Sandboxing MCP server processes: an MCP server is operator-trusted infrastructure, in the same trust class as the model and search upstreams (it runs on the host next to the Gateway, configured by the operator, and gets only the environment the operator lists).
- Workspaces for deep-research sub-runs (sub-topic agents keep their search/read tool set).

## 3. Architecture

```
 Worker (session sandbox, no network)                    Gateway (host)
 ┌──────────────────────────┐   unix socket   ┌─────────────────────────────────────────────┐
 │ exec_shell / read_file / │ ──────────────▶ │ edge: /v1/workspace/{exec,read,write,list}  │
 │ write_file / list_dir    │                 │   └─ workspace.Manager (per task)            │
 │ mcp__<server>__<tool>    │                 │        manifest ⇄ <data>/tool-workspaces/    │
 └──────────────────────────┘                 │        exec → call.Coordinator.ExecShell ───┼──▶ fresh exec env
                                              │ edge: GET /v1/mcp/tools, POST /v1/mcp/call   │    (/in/ws ro, /out rw)
                                              │   └─ call.Coordinator.Invoke(kind "mcp")     │
                                              │        └─ mcp adapter ── stdio / HTTP ───────┼──▶ MCP servers
                                              └─────────────────────────────────────────────┘
```

### 3.1 Workspace = persistent file tree + ephemeral processes

Decision: **extend the exec environment, not a new kind of sandbox.** A workspace is a *manifest* held by the Gateway — `path → {sha256, size, executable}` — whose file contents live in the content-addressed BlobStore. `exec_shell` runs one exec environment per command:

1. The Gateway stages the manifest's files into the exec environment's read-only `/in/ws/` (mode 0444, or 0555 for executables) and the command into `/in/.agentbox/cmd.sh`.
2. A fixed wrapper (`/bin/bash --noprofile --norc -c …`, not caller-controlled) copies `/in/ws/.` into `/out` (the writable tmpfs, preserving mode bits), `cd /out`, then runs `bash --noprofile --norc /in/.agentbox/cmd.sh`. Environment: the exec environment's fixed variables plus `WORKSPACE=/out`.
3. After the command exits (or hits its wall limit), the existing exec path stops the whole tree, then collects the stable regular files in `/out` (openat2 `RESOLVE_BENEATH|NO_SYMLINKS|NO_XDEV`, ≤ 256 files). That snapshot — with each file's executable bit read by `fstat` on the already-opened FD — becomes the new head.

Why this instead of a long-lived sandbox per workspace:

- **No new isolation surface.** It reuses the audited exec environment (Plan 15: E34–E39, §16.2 isolation acceptance) unchanged; there is no long-running workload process that could outlive its turn, and the stop/collect/cleanup paths (incl. stop-blocked and crash recovery) are the existing ones.
- **Journal-replayable.** The call fingerprint covers the command, the input manifest, the image digest and limits; a retried call id replays the stored result instead of re-running the command, exactly like `/v1/exec`.
- **Bounded cost.** Disk is bounded by the `/out` tmpfs (`--exec-out-bytes`, default 64 MiB, also `RLIMIT_FSIZE`), memory by `--exec-memory-max`, and nothing is held between commands except blobs.
- Cost: one environment creation per command (measured in the evidence, §9) and no process state between commands. Coding-agent harnesses that use a stateless Bash tool (each command in a fresh shell, files persist) show this is the right trade-off for an agent workload.

Symlinks, FIFOs and devices created by a command are **not** carried into the next version (collection skips them and the response lists them as `skipped`); empty directories are not represented. Files whose names are not valid workspace paths (under the reserved `.agentbox/`, with control characters, …) are skipped with reason `invalid_path`, so the next command can always be staged. This is what makes the file API escape-proof (§5).

**Staging signal.** The wrapper writes a fixed marker (`\0agentbox:staged\n`) to stdout after the copy succeeded and before it `exec`s the command. The marker is positionally unforgeable — the command can only write after it — so its presence at the very start of stdout is the wrapper's positive "staged OK" signal; `ExecShell` strips it and reports `workspace_staged`. The Manager applies the `/out` snapshot **only** when it is present; otherwise (copy failed, e.g. `/out` full, or the wall limit hit during staging) the head is unchanged and the call returns `503 workspace_staging_failed` (the exec call itself is journaled and counted). To make a full workspace always fit, workspace bytes are capped at `--exec-out-bytes` − 1 MiB (256 files × 4 KiB block rounding); the command gets whatever is left of the `/out` tmpfs.

### 3.2 Where the head lives (lifecycle and crash recovery)

`workspace.Manager` (package `internal/gateway/workspace`) keeps one workspace per **task** (= chat turn). Its state is a small JSON file `<data>/tool-workspaces/<sha256(task_id)[:32]>.json` (not `<data>/workspaces`, which holds the task environments' `/workspace` mounts), written atomically (temp file + fsync + rename + fsync dir):

```json
{"task_id": "...", "state": "active|expired|lost", "reason": "", "version": "<sha256 of canonical manifest>",
 "files": {"src/app.py": {"sha256": "…", "size": 120, "exec": false}}, "created_at": "...", "used_at": "...",
 "ops": 7, "applied": [{"call_id": "orch/workspace_exec/3", "result_sha256": "…"}]}
```

| Event | Effect |
|---|---|
| first operation of a task | state file created (`active`, empty manifest) |
| `exec_shell` completed or timed out | head ← `/out` snapshot; call id + response blob appended to `applied` (last 32) |
| `exec_shell` cancelled / unknown / rejected (quota, queue timeout, …) | head unchanged |
| `write_file` | head ← head + file (blob put first, then the state file) |
| task terminal (succeeded / failed / cancelled), checked by the sweeper every 30 s and once at startup | state file deleted (**destroyed**) |
| no operation for `--workspace-idle-timeout` (default 2 h; longer than the session evict-after so that a paused or `awaiting_input` turn keeps its workspace) | manifest dropped, state `expired` kept as a tombstone |
| server crash / restart | state file is read lazily on next access → **recovered**; unreadable or inconsistent file, or a manifest blob missing from the BlobStore → state `lost` |
| operation on an `expired` / `lost` workspace | `410 workspace_expired` / `410 workspace_lost`; the worker tells the model that the files are gone |
| request that was waiting for the workspace while the sweeper destroyed it | `410 workspace_destroyed` (tombstone; it does not recreate the workspace) |
| any accepted operation, reads included | refreshes the idle clock (in memory; written to the state file with the next change, and at most once a minute by operations that change nothing, so a restart keeps it) |
| state file or blob store unreadable (I/O error) | the request fails (500); the workspace is **not** marked lost — only unparsable/inconsistent state or a missing blob is |

Idempotency across crashes: the worker re-sends an in-flight `exec_shell` with the same call id after a restart. If the head was already advanced by that call (`applied` contains the id), the Manager returns the stored response without running anything; otherwise the call goes to the coordinator, which replays from the journal or re-runs it (the input manifest is unchanged, so the fingerprint matches). Operations on one workspace are serialized by a per-task mutex; the Manager waits for an `exec_shell` to settle even if the HTTP client disconnected, so a result is never lost between "exec completed" and "head advanced".

The blobs referenced by a workspace are ordinary Gateway blobs (exec outputs are authorized to the task scope by `SettleExec`, as for `/v1/exec`); destroying a workspace drops only the manifest. **The BlobStore is never garbage-collected**, so `write_file` (permanent host-disk writes) is metered per task: at most 2000 write operations and 256 MiB written (`429 workspace_write_quota`; counters in the state file). Exec outputs are bounded by the exec quota.

An applied call id replays only for the same request (sha256 of command and timeout stored with it); a different request under that id is `409 fingerprint_mismatch`.

### 3.3 Accounting

- `exec_shell` is an **exec call**: `calls.endpoint = /v1/exec` (so `ReserveExec`, `SettleExec`, the invariants, `inspect` and the operator UI need no change), adapter version `exec-shell/1` and endpoint `/v1/workspace/exec` inside the fingerprint, so a shell call can never be confused with a `/v1/exec` call. It consumes the exec slots and the task's exec quota (count 50, CPU, wall by default), **not** the per-turn web tool budget — the same rule as `run_python`.
- `read_file` / `write_file` / `list_dir` are Gateway-local metadata operations (no sandbox, no upstream): not billed, not counted in the tool budget, bounded by size limits (§4.3), logged with latency.
- MCP `tools/call` **counts against the per-turn tool budget** (it is an external tool like search/fetch): `/v1/mcp` is added to the tool endpoints in `BeginCall`, so the 31st external tool call is `429 tool_budget_exhausted`. Price 0 (no money reservation beyond the zero estimate). `tools/list` is free.

### 3.4 MCP through the Gateway

Operator config (`--mcp-config /etc/agentbox/mcp.json`; absent = MCP off):

```json
{"servers": [
  {"name": "calc", "transport": "stdio", "command": ["/usr/local/bin/agentbox", "mcp-demo-server"],
   "env": {"LANG": "C.UTF-8"}, "allowed_tools": ["calculate", "unit_convert"], "timeout_ms": 10000},
  {"name": "docs", "transport": "http", "url": "https://mcp.internal.example/mcp",
   "headers_from_env": {"Authorization": "DOCS_MCP_AUTH"}, "allowed_tools": ["search_docs"]}
]}
```

- `name`: `[a-z0-9_]{1,32}`; tools are exposed to the agent as `mcp__<server>__<tool>`. `allowed_tools` is required and non-empty: tools not listed are never shown and calls to them are rejected (`403 mcp_tool_not_allowed`) before anything is sent.
- Credentials stay on the host: stdio servers get only `env` plus `env_from` (values read from the server's own environment at start), HTTP servers get `headers_from_env`. None of it enters the journal, the fingerprint, error texts or events.
- **stdio**: the Gateway starts the process (argv as given, no shell), speaks newline-delimited JSON-RPC 2.0, performs `initialize` + `notifications/initialized`, and serializes requests per server (one call at a time; a slow call delays the next). A dead process, or one whose call timed out, is killed with its whole process group (own group via `Setpgid`, `Pdeathsig SIGKILL` so it dies with the Gateway) and restarted on the next request; since calls are serialized, that kill aborts no other call. Working directory: `dir` from the config or a fresh empty temp directory (removed on close); optional `uid`/`gid` (Linux; both or neither — one alone would keep the Gateway's other id, often root's group). Stderr is logged (truncated). **The server runs on the host as that user — root by default — with arguments chosen by the model: allowlist only tools that are safe with hostile arguments.**
- **Streamable HTTP**: `POST` JSON-RPC with `Accept: application/json, text/event-stream`; accepts a JSON body or an SSE stream (first response with the matching id); keeps `Mcp-Session-Id` from `initialize`; no redirects; response cap 1 MiB.
- **`GET /v1/mcp/tools`** returns `{"tools":[{"name":"mcp__calc__calculate","server":"calc","tool":"calculate","description":…,"input_schema":…}]}`: the allowlisted intersection of each server's `tools/list`, fetched once per server and cached until the server process is restarted. A server that fails to list is omitted (and logged).
- **`POST /v1/mcp/call`** `{"server","tool","arguments"}` with `X-Agentbox-Call-Id`: goes through `call.Coordinator.Invoke` as upstream kind `mcp` (adapter in `internal/gateway/mcp`). This gives the call journal (`calls`/`call_tries`, result blob, `inspect`), idempotent replay by call id, the call deadline, cancellation on attempt revocation, the tool budget and the per-attempt concurrency limits for free. Outcome mapping: transport failure before the request is written → `retryable`; after it was written without a response → `unknown` (not retried automatically: `tools/call` is not idempotent); JSON-RPC error → `fatal` (`502 mcp_error`); a tool result with `isError: true` is a successful call whose content tells the model what went wrong. The response is `{"server","tool","is_error","content":[…],"structured_content":…}` (text content capped at 256 KiB for the model).
- Determinism: the worker fetches the MCP tool list once per turn and stores it in the turn state (checkpoint), so a resumed turn sends the same `tools` to the model (same chat fingerprint) even if a server changed its list meanwhile.

### 3.5 Worker and UI

- The server's turn spec gains `"tools": {"workspace": true, "mcp": true}` **only when enabled** (default spec bytes unchanged). The chat agent registers `exec_shell`, `read_file`, `write_file`, `list_dir` when `workspace` is on, and one tool per MCP tool when `mcp` is on, available in both the answer and the research route (orchestrator only).
- The system prompt gains a short "workspace" section (when to use files + shell vs. `run_python`; paths are relative to the workspace root; processes do not persist; exec quota) and an "external tools" section listing MCP tools (each MCP call costs one unit of the turn's tool budget).
- Events: `tool_call` / `tool_result` as for other tools; `exec_shell` and MCP calls carry `raw{request, response_ref}` (⟨/⟩) because they are journaled Gateway calls; file operations carry their request inline.
- Web: new step row kinds — `shell` ("运行命令", first line of the command, exit code + output like 运行代码), `file` ("读取文件 · path", "写入文件 · path", "列出目录 · path"), `mcp` ("外部工具 · server/tool").

## 4. Interfaces

### 4.1 Gateway endpoints (worker socket)

| Endpoint | Body | Response |
|---|---|---|
| `POST /v1/workspace/exec` (call id required) | `{"command": str ≤ 64 KiB, "timeout_ms"?: int}` | `{"status","exit","stdout",…(exec result fields)…,"changes":{"added","modified","deleted"},"skipped":[…],"workspace":{"version","files","bytes"}}` |
| `POST /v1/workspace/read` | `{"path", "start_line"? ≥ 1, "max_lines"? ≤ 2000}` | `{"path","content","start_line","end_line","total_lines" (null unless the end was reached),"truncated","binary","size","sha256"}` — streamed: only the window is read and checked for binary content |
| `POST /v1/workspace/write` | `{"path", "content" ≤ 1 MiB UTF-8, "executable"?: bool}` or `{"path", "delete": true}` (a file, or every file under a directory; any existing manifest key can be deleted) | `{"path","size","sha256","created","workspace":{…}}` / `{"path","deleted":[…],"workspace":{…}}` |
| `POST /v1/workspace/list` | `{"path"?: "" = root, "recursive"?: bool}` | `{"path","entries":[{"name","type":"file|dir","size"}],"workspace":{…}}` |
| `GET /v1/mcp/tools` | — | see §3.4 |
| `POST /v1/mcp/call` (call id required) | `{"server","tool","arguments": object}` | see §3.4 |

All workspace endpoints are `404 endpoint_not_configured` when `--workspace-tools` is off (and when exec is off); MCP endpoints when `--mcp-config` is absent (`/v1/mcp/call` validates its headers — call id, sub-run prefix — first, then answers `404 endpoint_not_configured` because no `mcp` adapter is registered). MCP `arguments` are capped at 64 KiB (`413 mcp_arguments_too_large`). HTTP answers from an MCP server: 4xx → fatal `upstream_rejected` (408/429 → retryable), 5xx → unknown (a 5xx to `initialize` → retryable: the tool call was not sent yet). Read/write/list do the §9.2 access check (revoked attempt → 403) before touching the workspace.

### 4.2 Go

- `call.(*Coordinator).ExecShell(ctx, call.ShellInvoke{TaskID, AttemptID, CallID, SubrunID, Command, WallMs, Files []ShellFile, Retry})` — shares the exec pipeline (`runExec`) with a `shell` variant: different fingerprint, trusted inputs (from the Gateway's manifest, so the worker-facing `input_not_authorized` check is skipped), the wrapper argv, executable bits in outputs.
- `workspace.Manager{Exec, Read, Write, List, Sweep, Destroy}`, `workspace.Config{Dir, Execer, Blobs, Access, Tasks, IdleTimeout, MaxFiles, MaxBytes, Logger}`.
- `mcp.LoadConfig(path)`, `mcp.Hub` (clients per server, cached lists), `mcp.Adapter` (implements `upstream.Adapter`, kind `mcp`), `mcp/demo` (the demo server), `agentbox mcp-demo-server` (stdio).
- `edge.Config.Workspace` and `edge.Config.MCP` (optional; nil = 404).

### 4.3 Limits

| Limit | Value |
|---|---|
| files per workspace | 256 (= `/out` collection cap); `write_file` beyond it → `409 workspace_full` |
| bytes per workspace | `--exec-out-bytes` − 1 MiB staging headroom (default 63 MiB); enforced on `write_file` (`409 workspace_full`). An `exec_shell` snapshot is kept even when it is larger (bounded by the `/out` tmpfs, `--exec-out-bytes`); `workspace.bytes` in the response shows the size |
| path | relative, `path.Clean`-stable, ≤ 512 bytes, ≤ 32 segments, ≤ 255 bytes per segment, no `.`/`..`, no NUL or other Unicode control characters (Cc, including C1), none of the invisible format characters that make a name display differently (bidi controls, BOM, soft hyphen, zero-width space; zero-width joiners and emoji tag characters are allowed), no leading `/`, no `\`, not under the reserved `.agentbox/` (exactly what exec staging can stage); state files written by earlier versions are read with the earlier character rule, so an upgrade does not mark their workspaces lost |
| `write_file` content | ≤ 1 MiB; per task ≤ 2000 writes and ≤ 256 MiB written |
| `read_file` | ≤ 2000 lines and ≤ 256 KiB per call |
| `exec_shell` command | ≤ 64 KiB; `timeout_ms` capped by `--exec-wall-max` (default from exec) |
| MCP result for the model | text ≤ 256 KiB; transport response ≤ 1 MiB; per-call timeout `timeout_ms` (default 30 s, ≤ 120 s) |

## 5. Security

- **No host paths.** File operations never touch the host filesystem by path: they are lookups and updates of the manifest (a map keyed by validated relative paths) plus content-addressed blob reads. There is nothing a `..`, an absolute path or a symlink could resolve against. Path validation rejects `..`, absolute paths, NUL, backslashes and non-canonical forms (`a//b`, `./a`) with `400 invalid_path`.
- **Symlink escapes.** A command can create `ln -s /etc/passwd x` inside its sandbox, but symlinks are skipped at collection (openat2 without following links), so `x` does not exist in the next version; `read_file x` is `404 not_found`. Hard links to files outside `/out` are impossible across the tmpfs mount boundary (EXDEV). Tests cover each case at the Manager level (fake environment) and in the root e2e test (real sandbox).
- **Inside the sandbox** the command has exactly the exec environment's view and limits (no network, seccomp, own UIDs, read-only `/in`, tmpfs `/out` and `/tmp`).
- **MCP**: allowlist enforced in the Gateway before any I/O; server credentials only on the host; the sandbox sees tool names, schemas and results only.

## 6. Failure handling

| Failure | Behaviour |
|---|---|
| exec rejected (quota, queue timeout, start failure) | error passed through (`402 exec_*`, `504 exec_queue_timeout`, …); head unchanged |
| command times out | `status: timed_out`; files written so far become the head (like a real shell); response says so |
| `/out` full / too many files | ENOSPC/EFBIG inside the command; files beyond 256 are listed in `skipped` (`too_many`) and are not kept |
| client disconnects during `exec_shell` | Manager still waits and applies; retry with the same call id returns the stored response |
| server crash during `exec_shell` | head unchanged; the journaled call becomes `unknown`/rerun on retry (Plan 15 E37 path) |
| state file corrupt or blob missing | workspace `lost` → `410 workspace_lost` |
| MCP server down | `tools/list`: server omitted; `tools/call`: `502 upstream_unreachable` (retryable) or `502 upstream_unconfirmed` (unknown) |
| MCP tool not allowlisted / unknown server | `403 mcp_tool_not_allowed` / `404 mcp_server_not_found`, nothing sent |

## 7. Testing

- `internal/gateway/call`: shell variant with the fake exec environments — argv/env, staging modes, fingerprint ≠ `/v1/exec`, trusted inputs, executable bits, replay, quota pass-through; existing exec tests unchanged.
- `internal/gateway/workspace`: lifecycle (create, apply, write/read/list, idle expiry, terminal destroy, restart recovery, corrupt state → lost), idempotent `applied` replay, path validation table, symlink/too_many snapshot handling, concurrency (serialized ops).
- `internal/gateway/mcp`: config validation; stdio client against the demo server (helper process); HTTP client against an in-process streamable-HTTP server (JSON and SSE replies, session id); adapter outcome mapping; allowlist; credentials never in errors.
- `internal/gateway/edge`: routes, 404 when not configured, access check before file ops.
- `internal/persistence/postgres`: `/v1/mcp` counts against the tool budget.
- `internal/app`: flags/config validation, turn spec unchanged by default and carrying `tools` when enabled, wiring with the fake provider.
- Worker: tool unit tests (fake gateway), registry gating by turn config, prompts, events (raw refs), state round-trip of the MCP tool list, a scripted full turn (write → exec → read → MCP).
- Web: step rows for `shell`, `file`, `mcp`.
- Root e2e (`tests/e2e`, real sandbox): workspace shell end-to-end (write a test file, run it, read output; symlink escape blocked; network blocked; persistence across commands) and a chat turn with the real Python chat agent against a scripted fake upstream plus the demo MCP server.

## 8. Defaults and flags

| Flag | Default | Meaning |
|---|---|---|
| `--workspace-tools` | `false` | enable workspace endpoints and tools (requires exec, i.e. `--exec-slots` > 0) |
| `--workspace-idle-timeout` | `2h` | idle expiry of a workspace |
| `--mcp-config` | empty | MCP server config file; empty = MCP off |

## 9. Demonstration

`docs/evidence/2026-10-10-shell-file-mcp.md`: a chat turn driven by a scripted fake upstream in which the agent writes `calc.py` and `test_calc.py`, runs `python3 -m unittest` with `exec_shell`, reads the output, calls `mcp__calc__calculate`, and answers; escape attempts (`../`, absolute path, symlink to `/etc/passwd`, network) are blocked; measured latencies of `write_file`, `read_file`, `list_dir`, `exec_shell` (environment create → collect) and MCP `tools/call`.
