//go:build linux

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// Workspace tools and MCP through the Gateway (design docs/design/2026-10-10-shell-file-mcp-design.md): a real chat
// turn — cmd/agentbox with the production starter, the real Python chat agent (python3 -m chatagent) in a session
// sandbox, workspace commands in real exec environments, and the demo MCP server as a stdio child of the Gateway
// (`agentbox mcp-demo-server`). The model is the fake upstream, scripted by the number of tool results in the
// request: the agent writes calc.py and its test, runs the test with exec_shell, reads the output, tries to escape
// (symlink to /etc/passwd, network, ../ paths), and calls the MCP calculator.

const wsCalc = "def add(a, b):\n    return a + b\n"

const wsTest = `import unittest
from calc import add


class AddTest(unittest.TestCase):
    def test_add(self):
        self.assertEqual(add(2, 3), 5)


if __name__ == "__main__":
    unittest.main()
`

// wsEscape is one command trying the classic escapes from inside the workspace sandbox.
const wsEscape = `ln -s /etc/passwd leak; ln -s / root-link
python3 -c "import socket; socket.create_connection(('1.1.1.1', 80), 3)" >/dev/null 2>&1 && echo NET-OK || echo NET-BLOCKED
cat /proc/self/status | grep -E '^(Uid|Seccomp):'
echo whoami=$(id -u)`

type wsStep struct {
	tool string
	args map[string]any
}

var wsScript = []wsStep{
	{"write_file", map[string]any{"path": "calc.py", "content": wsCalc}},
	{"write_file", map[string]any{"path": "tests/test_calc.py", "content": wsTest}},
	{"exec_shell", map[string]any{"command": "PYTHONPATH=. python3 -m unittest discover -s tests -v 2>&1 | tee test-output.txt"}},
	{"read_file", map[string]any{"path": "test-output.txt"}},
	{"exec_shell", map[string]any{"command": wsEscape}},
	{"read_file", map[string]any{"path": "../../etc/passwd"}},
	{"read_file", map[string]any{"path": "leak"}},
	{"list_dir", map[string]any{"recursive": true}},
	{"mcp__calc__calculate", map[string]any{"expression": "(2 + 3) * 7"}},
}

const wsFinal = "测试通过（Ran 1 test, OK）；逃逸尝试均被阻止；外部计算器给出 (2 + 3) * 7 = 35。"

// wsChat scripts the orchestrator: the k-th model call (k = number of tool results so far) makes the k-th tool call,
// then the final answer. Other model calls (none expected) get the default reply.
func wsChat(body []byte) json.RawMessage {
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &req) != nil || len(req.Messages) == 0 || !strings.Contains(req.Messages[0].Content, "## 工作区") {
		return nil
	}
	k := 0
	for _, m := range req.Messages {
		if m.Role == "tool" {
			k++
		}
	}
	if k >= len(wsScript) {
		b, _ := json.Marshal(map[string]any{"role": "assistant", "content": wsFinal})
		return b
	}
	args, _ := json.Marshal(wsScript[k].args)
	b, _ := json.Marshal(map[string]any{"role": "assistant", "content": "", "tool_calls": []map[string]any{{
		"id": fmt.Sprintf("call_%d", k), "type": "function",
		"function": map[string]any{"name": wsScript[k].tool, "arguments": string(args)}}}})
	return b
}

func TestRealWorkspaceShellMCPDemo(t *testing.T) {
	s := newRealSys(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	fu.SetChatHook(wsChat)
	u, err := url.Parse(fu.URL())
	if err != nil {
		t.Fatal(err)
	}
	mcpCfg := filepath.Join(t.TempDir(), "mcp.json")
	cfg := fmt.Sprintf(`{"servers":[{"name":"calc","transport":"stdio","command":[%q,"mcp-demo-server"],`+
		`"allowed_tools":["calculate","unit_convert"],"timeout_ms":10000}]}`, s.bin)
	if err := os.WriteFile(mcpCfg, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	ss := &sessSys{sysHarness: s, real: true, flags: []string{
		"--model-base-url", fu.URL() + "/v1", "--model-name", fakeupstream.Model,
		"--user-orchestrator-model", fakeupstream.Model, "--user-worker-model", fakeupstream.Model,
		"--search-provider", upstream.SearchFake, "--search-base-url", fu.URL(), "--upstream-allow-private", u.Host,
		"--session-worker-argv", "python3,-m,chatagent",
		"--workspace-tools", "--mcp-config", mcpCfg,
	}}
	t.Cleanup(ss.dumpOnFailure)
	srv := ss.startS()
	alice := ss.user("alice")
	sid := alice.createSession("ws-1")
	start := time.Now()
	r := alice.message(sid, "m-1", "写一个 add 函数和单元测试并运行，再用外部计算器算 (2+3)*7")
	v := alice.waitTurn(sid, r.TurnID, "成功", turnIs("succeeded"))
	turnDur := time.Since(start)
	if !strings.Contains(v.Summary, "35") {
		t.Fatalf("turn summary %q", v.Summary)
	}

	// Events: one tool_result per scripted step, in order; read_file escapes fail; the rest succeed.
	evs := alice.sse(sid, hasEventType("turn_result"))
	type tr struct {
		Tool    string `json:"tool"`
		OK      bool   `json:"ok"`
		Error   string `json:"error"`
		Preview struct {
			Text string `json:"text"`
		} `json:"preview"`
		Raw *struct {
			ResponseRef string `json:"response_ref"`
		} `json:"raw"`
	}
	var results []tr
	for _, e := range evs {
		var ev struct {
			Type string `json:"type"`
			Data tr     `json:"data"`
		}
		if json.Unmarshal(e, &ev) == nil && ev.Type == "tool_result" {
			results = append(results, ev.Data)
		}
	}
	if len(results) != len(wsScript) {
		t.Fatalf("tool results %d, want %d: %+v", len(results), len(wsScript), results)
	}
	for i, st := range wsScript {
		got := results[i]
		wantOK := !(st.tool == "read_file" && (i == 5 || i == 6))
		if got.Tool != st.tool || got.OK != wantOK {
			t.Fatalf("step %d: %+v, want tool %s ok=%v", i, got, st.tool, wantOK)
		}
		journaled := st.tool == "exec_shell" || strings.HasPrefix(st.tool, "mcp__")
		if journaled != (got.Raw != nil && got.Raw.ResponseRef != "") {
			t.Fatalf("step %d (%s): raw %+v", i, st.tool, got.Raw)
		}
	}
	testOut := results[3].Preview.Text
	if !strings.Contains(testOut, "Ran 1 test") || !strings.Contains(testOut, "OK") {
		t.Fatalf("test output read back: %q", testOut)
	}
	escape := results[4].Preview.Text
	t.Logf("test output read back:
%s", testOut)
	t.Logf("escape command output:
%s", escape)
	t.Logf("escape reads: %q / %q; list_dir: %q", results[5].Error, results[6].Error, results[7].Preview.Text)
	if !strings.Contains(escape, "NET-BLOCKED") || strings.Contains(escape, "NET-OK") || strings.Contains(escape, "whoami=0") {
		t.Fatalf("escape command output: %q", escape)
	}
	if !strings.Contains(results[5].Error, "invalid_path") || !strings.Contains(results[6].Error, "not_found") {
		t.Fatalf("escape reads: %q / %q", results[5].Error, results[6].Error)
	}
	if !strings.Contains(results[8].Preview.Text, "35") {
		t.Fatalf("mcp result %q", results[8].Preview.Text)
	}

	// Journal: two exec calls (endpoint /v1/exec, each in its own exec environment, stopped and cleaned), one MCP call
	// counted against the tool budget.
	type callRow struct {
		id, endpoint, state, env string
		toolUsed                 *int64
	}
	var calls []callRow
	rows := pgRows(t, ss.dsn, `SELECT c.call_id, c.endpoint, c.state, COALESCE(t.env_id, ''), c.tool_budget_used
		FROM calls c LEFT JOIN call_tries t ON t.task_id = c.task_id AND t.call_id = c.call_id
		WHERE c.task_id = $1 AND c.endpoint <> '/v1/chat/completions' ORDER BY c.created_at`, r.TurnID)
	for _, row := range rows {
		c := callRow{id: row[0].(string), endpoint: row[1].(string), state: row[2].(string), env: row[3].(string)}
		if row[4] != nil {
			n := row[4].(int64)
			c.toolUsed = &n
		}
		calls = append(calls, c)
	}
	var execEnvs []string
	mcpCalls := 0
	for _, c := range calls {
		switch c.endpoint {
		case "/v1/exec":
			if c.state != "completed" || c.env == "" || !strings.Contains(c.id, "/workspace_exec/") {
				t.Fatalf("workspace exec call %+v", c)
			}
			execEnvs = append(execEnvs, c.env)
		case "/v1/mcp":
			if c.state != "completed" || c.toolUsed == nil || *c.toolUsed != 1 {
				t.Fatalf("mcp call %+v", c)
			}
			mcpCalls++
		default:
			t.Fatalf("unexpected call %+v", c)
		}
	}
	if len(execEnvs) != 2 || execEnvs[0] == execEnvs[1] || mcpCalls != 1 {
		t.Fatalf("calls %+v", calls)
	}
	eventually(t, "exec environments cleaned", func() bool {
		for _, env := range execEnvs {
			var state string
			ss.row("SELECT cleanup_state FROM environments WHERE env_id = $1", []any{env}, &state)
			if state != "done" {
				return false
			}
		}
		return true
	})
	// The workspace of the finished turn is destroyed by the sweeper.
	eventuallyWithin(t, "workspace destroyed", 90*time.Second, 200*time.Millisecond, func() bool {
		ents, err := os.ReadDir(filepath.Join(s.dir, "tool-workspaces"))
		return err == nil && len(ents) == 0
	})

	// Numbers: per-operation latency from the Gateway's workspace log lines, exec queue/wall/CPU from call_tries,
	// MCP call latency from its try.
	lat := map[string][]int64{}
	// The server logs JSON: {"msg":"gateway: workspace","op":"read",…,"latency_ms":3}.
	re := regexp.MustCompile(`"msg":"gateway: workspace","op":"(\w+)".*"latency_ms":(\d+)`)
	for _, line := range strings.Split(srv.logs.tail(8<<20), "\n") {
		if m := re.FindStringSubmatch(line); m != nil {
			n, _ := strconv.ParseInt(m[2], 10, 64)
			lat[m[1]] = append(lat[m[1]], n)
		}
	}
	ops := slices.Collect(func(yield func(string) bool) {
		for k := range lat {
			if !yield(k) {
				return
			}
		}
	})
	sort.Strings(ops)
	for _, op := range ops {
		t.Logf("workspace %s latency_ms %v", op, lat[op])
	}
	for _, row := range pgRows(t, ss.dsn, `SELECT t.call_id, COALESCE(t.queue_ms, -1), COALESCE(t.wall_ms, -1), COALESCE(t.cpu_usec, -1),
			t.latency_ms FROM call_tries t JOIN calls c USING (task_id, call_id)
		WHERE t.task_id = $1 AND c.endpoint IN ('/v1/exec', '/v1/mcp') ORDER BY c.created_at`, r.TurnID) {
		t.Logf("try %v queue_ms %v wall_ms %v cpu_usec %v latency_ms %v", row[0], row[1], row[2], row[3], row[4])
	}
	t.Logf("turn %s succeeded in %s; summary %q", r.TurnID, turnDur, v.Summary)
	alice.closeSession(sid)
	ss.end()
}

// pgRows runs a query and returns all rows as value slices.
func pgRows(t *testing.T, dsn, sql string, args ...any) [][]any {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
