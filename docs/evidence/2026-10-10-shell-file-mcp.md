# Workspace shell/file tools and MCP through the Gateway — acceptance record

> Date 2026-10-10. Branch `s5-shell-file-mcp` (PR #28). Design: [2026-10-10-shell-file-mcp-design.md](../design/2026-10-10-shell-file-mcp-design.md).
> The demo turn ran as root in a real sandbox on the CI `linux-integration` runner (GitHub Actions `ubuntu-24.04`, cgroup v2), not on the project's server. The model is a scripted fake upstream (no real-model spend). No keys, tokens or host addresses appear here.

## What was run

`TestRealWorkspaceShellMCPDemo` (`tests/e2e/workspace_test.go`), started with:

```bash
CI=true CGO_ENABLED=0 go test -count=1 -p 1 -v -run 'TestRealWorkspaceShellMCPDemo' ./tests/e2e/   # as root
```

- `cmd/agentbox server` with the production sandbox starter, `--session-worker-argv python3,-m,chatagent --workspace-tools --mcp-config <file>`; the MCP config lists one stdio server, `agentbox mcp-demo-server`, with `allowed_tools: ["calculate", "unit_convert"]` (its third tool, `echo_env`, is not allowlisted).
- A registered user creates a session and sends one message. The real Python chat agent runs in the session sandbox. The fake upstream answers each orchestrator model call with the next scripted tool call (chosen by the number of tool results already in the request), then the final answer:

| # | Tool call | Result |
|---|---|---|
| 1 | `write_file calc.py` | ok (file op, no sandbox) |
| 2 | `write_file tests/test_calc.py` | ok |
| 3 | `exec_shell "PYTHONPATH=. python3 -m unittest discover -s tests -v 2>&1 \| tee test-output.txt"` | exit 0; `test-output.txt` added to the workspace |
| 4 | `read_file test-output.txt` | `Ran 1 test … OK` |
| 5 | `exec_shell` escape attempts: `ln -s /etc/passwd leak; ln -s / root-link`, TCP connect to `1.1.1.1:80`, `/proc/self/status` Uid/Seccomp, `id -u` | `NET-BLOCKED`; uid ≠ 0; both symlinks reported in `skipped_outputs` (`symlink`) and **not** kept |
| 6 | `read_file ../../etc/passwd` | rejected: `400 invalid_path` (tool error to the model, turn continues) |
| 7 | `read_file leak` | `404 not_found` — the symlink never became a workspace file |
| 8 | `list_dir recursive` | `calc.py`, `test-output.txt`, `tests/test_calc.py` |
| 9 | `mcp__calc__calculate {"expression": "(2 + 3) * 7"}` | `(2 + 3) * 7 = 35` via the Gateway's stdio MCP client |
| — | final answer | "测试通过（Ran 1 test, OK）；逃逸尝试均被阻止；外部计算器给出 (2 + 3) * 7 = 35。" |

Assertions in the test (all passed):

- the turn succeeded; one `tool_result` per step in this order with the `ok` values above;
- `exec_shell` and the MCP call carry `raw.response_ref` (⟨/⟩ in the UI); file operations do not;
- journal: exactly two `/v1/exec` calls (`…/workspace_exec/1`, `…/workspace_exec/2`), each `completed` in **its own** exec environment, both environments later `cleanup_state = done`; exactly one `/v1/mcp` call, `completed`, `tool_budget_used = 1` (counted against the turn's 30);
- after the turn ended the sweeper destroyed the workspace (`<data>/tool-workspaces` empty);
- `agentbox verify-invariants --quiescent` passed at the end.

Logged outputs (CI run 37969261941):

```text
test output read back:
  test_add (test_calc.AddTest.test_add) ... ok
  ----------------------------------------------------------------------
  Ran 1 test in 0.000s
  OK
escape command output (exec_shell #2, preview):
  退出码 0
  NET-BLOCKED
  Uid:    1000    1000    1000    1000
  Seccomp:        2
  whoami=1000
escape reads: "读取 ../../etc/passwd 失败：路径不合法…（invalid_path）。" / "读取 leak 失败：文件或目录不存在（not_found）。"
list_dir: calc.py 32 B, test-output.txt 142 B, tests/test_calc.py 185 B — 3 files, 359 bytes (no leak, no root-link)
```

## Numbers (CI runs 37968026508 and 37969261941, `linux-integration`)

| Measure | Value |
|---|---|
| whole turn (message accepted → `succeeded`, 10 model calls, 9 tools) | 0.99 s (both runs) |
| `write_file` (Gateway, incl. blob put and fsync'd state file) | 2 ms, 2 ms |
| `read_file` | 0–1 ms |
| `list_dir` | 0–1 ms |
| `exec_shell` end to end in the Gateway (create exec env → stage workspace → run → stop → collect `/out` → cleanup → settle → apply) | 169 ms (unittest), 118 ms (escape command) — identical in both runs |
| … of which the command's wall time inside the sandbox | 83–85 ms (unittest; 73–75 ms CPU), 36 ms (escape; 58–61 ms CPU) |
| exec queue time | 0 ms (slots free) |
| MCP `tools/call` try latency (journal) | 0 ms (< 1 ms resolution) |
| MCP stdio round trip, local (`TestStdioListAndCall`, WSL2, 3 runs) | spawn + `initialize` + `tools/list` 2.0–2.4 ms; `tools/call` 109–123 µs |

Local root run (WSL2, kernel 6.6, D: drive over 9p; same test plus `TestRealExec|TestRealE3[5-8]`, all PASS): turn 1.38 s; `write_file` 28 / 18 ms (fsync on the slower disk); `read_file` 0 ms; `list_dir` 1 ms; `exec_shell` end to end 250 / 246 ms with command wall 50 / 36 ms.

The per-command overhead of a fresh exec environment is about 80–90 ms on the CI runner (end-to-end minus wall time); that is the price of "one sandbox per command, nothing long-lived" (design §3.1).

## Other evidence

- Non-root Go suite with PostgreSQL + Redis (`CI=true go test -count=1 ./...`, WSL2): all packages ok. New/changed packages: `internal/gateway/{call,workspace,mcp,edge}`, `internal/app` (`TestWorkspaceAndMCPWiring`: real wrapper script via the fake provider, HTTP MCP with session ids, allowlist rejection `403 mcp_tool_not_allowed`, sweeper destroys the terminal task's workspace), `internal/persistence/postgres` (`TestToolBudgetCountsMCP`), `internal/archtest`.
- Escape attempts at the Gateway level (`internal/gateway/workspace`): `../etc/passwd`, `/etc/passwd`, `a/../../x`, `a\..\x`, `./x`, `x/`, `a//b`, `..`, `.` → `400 invalid_path` for write/read/list without creating the workspace; symlinks from a command are reported and not kept.
- MCP credentials: the stdio server sees only `PATH`, `env` and `env_from` (a Gateway env var not listed is not visible); an HTTP server's credential header and URL query never appear in error texts.
- Worker: 1214 pytest cases in Linux (`AGENTBOX_REQUIRE_UNIX_TESTS=1`), incl. a scripted full turn (write → exec → read → MCP) and the default turn having exactly the previous five tools. Web: 232 vitest cases.
- CI: all six jobs green on the PR (correctness with `-race`, linux-integration as root with zero unallowed skips, python 3.11/3.13, web, complexity report).

## Not shown

- No real-model run: the model is scripted; real-model behaviour (when the agent chooses the workspace) is not measured here.
- No server (CVM) run; the root run is the CI runner.
- The complexity report flags several new functions (`workspace.Manager.Exec`/`Sweep`, MCP config validation, the stdio read loop) above the cognitive-complexity threshold; it is a report, not a gate.
