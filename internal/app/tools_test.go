package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/mcp"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/mcpdemo"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/fake"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// Workspace tools and MCP (design 2026-10-10-shell-file-mcp; tools.go).

func TestToolsConfigAndTurnSpec(t *testing.T) {
	bad := &mcp.Config{Servers: []mcp.ServerConfig{{Name: "x", Transport: "stdio"}}}
	for _, tc := range []struct {
		mut  func(*Config)
		want string
	}{
		{func(c *Config) { c.WorkspaceTools = true }, "--workspace-tools"},
		{func(c *Config) {
			c.Exec = ExecConfig{Slots: 2}
			c.WorkspaceTools = true
			c.WorkspaceIdleTimeout = -time.Second
		},
			"--workspace-idle-timeout"},
		{func(c *Config) { c.MCP = bad }, "--mcp-config"},
	} {
		c := testConfig()
		tc.mut(&c)
		if err := Run(context.Background(), c, Deps{}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Run = %v, want config error (%s)", err, tc.want)
		}
	}
	c := testConfig()
	c.UserOrchestratorModel, c.UserWorkerModel = "kimi-k3", "kimi-k2.6"
	c.Exec = ExecConfig{Slots: 2}
	c.WorkspaceTools = true
	c = c.withDefaults()
	spec, _, err := c.turnSpec("hi", false)
	if err != nil || !strings.HasSuffix(string(spec), `,"tools":{"workspace":true,"mcp":false}}`) {
		t.Fatalf("turn spec with workspace tools = %s, %v", spec, err)
	}
	if c.WorkspaceIdleTimeout != DefaultWorkspaceIdleTimeout {
		t.Fatalf("idle default = %v", c.WorkspaceIdleTimeout)
	}
	c.WorkspaceTools, c.MCP = false, &mcp.Config{}
	spec, _, _ = c.turnSpec("hi", false)
	if !strings.HasSuffix(string(spec), `,"tools":{"workspace":false,"mcp":true}}`) {
		t.Fatalf("turn spec with mcp = %s", spec)
	}
}

// toolsWorld: a task-mode worker that exercises the Gateway's MCP and workspace endpoints over its socket; the
// fake provider runs workspace commands with the real wrapper script on the host (paths /in and /out mapped to the
// fake environment's directories) — this checks the wrapper's staging/copy logic without a sandbox.
type toolsWorld struct {
	h       *harness
	replies chan string
}

func (w *toolsWorld) program(ctx context.Context, spec provider.ExecSpec, stdin io.Reader, stdout, stderr io.Writer) provider.ExitStatus {
	if spec.Dir == "/out" { // a workspace command (exec environment)
		out := w.h.prov.OutDir(spec.ExecID)
		in := filepath.Join(filepath.Dir(out), "in")
		script := spec.Argv[len(spec.Argv)-1]
		script = strings.ReplaceAll(script, "/in/", in+"/")
		script = strings.ReplaceAll(script, "/out", out)
		cmd := exec.CommandContext(ctx, "/bin/bash", "--noprofile", "--norc", "-c", script)
		cmd.Stdout, cmd.Stderr, cmd.Env = stdout, stderr, spec.Env
		err := cmd.Run()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return provider.ExitStatus{Code: ee.ExitCode()}
		}
		if err != nil {
			return provider.ExitStatus{Code: 127}
		}
		return provider.ExitStatus{Code: 0}
	}
	line, err := bufio.NewReader(stdin).ReadBytes('\n')
	if err != nil {
		return provider.ExitStatus{Code: 2}
	}
	var init protocol.Init
	if err := json.Unmarshal(line, &init); err != nil {
		return provider.ExitStatus{Code: 2}
	}
	emit := func(m map[string]any) {
		b, _ := json.Marshal(m)
		_, _ = stdout.Write(append(b, '\n'))
	}
	emit(map[string]any{"type": "ready", "v": 1, "seq": 1, "protocol_version": 1, "mode": "task",
		"worker": map[string]any{"name": "app-tools-test", "version": "0"}, "capabilities": []string{}})
	sock := filepath.Join(gatewayDir(w.h.dir), init.AttemptID+".sock")
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
	call := func(method, path, callID, body string) {
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, _ := http.NewRequestWithContext(ctx, method, "http://gateway"+path, rd)
		if callID != "" {
			req.Header.Set("X-Agentbox-Call-Id", callID)
		}
		resp, err := client.Do(req)
		if err != nil {
			w.replies <- fmt.Sprintf("%s error %v", path, err)
			return
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		w.replies <- fmt.Sprintf("%s %d %s", path, resp.StatusCode, b)
	}
	call("GET", "/v1/mcp/tools", "", "")
	call("POST", "/v1/mcp/call", "root/orch/mcp/1", `{"server":"calc","tool":"calculate","arguments":{"expression":"2+3"}}`)
	call("POST", "/v1/mcp/call", "root/orch/mcp/2", `{"server":"calc","tool":"echo_env","arguments":{"name":"HOME"}}`)
	call("POST", "/v1/workspace/write", "", `{"path":"src/a.txt","content":"hello\n"}`)
	call("POST", "/v1/workspace/exec", "root/orch/workspace_exec/1",
		`{"command":"tr a-z A-Z < src/a.txt > b.txt && ln -s /etc/passwd leak && echo ok && test -w src/a.txt"}`)
	call("POST", "/v1/workspace/read", "", `{"path":"b.txt"}`)
	call("POST", "/v1/workspace/read", "", `{"path":"leak"}`)
	call("POST", "/v1/workspace/read", "", `{"path":"../etc/passwd"}`)
	call("POST", "/v1/workspace/list", "", `{"recursive":true}`)
	emit(map[string]any{"type": "result", "v": 1, "seq": 2, "summary": "done", "outputs": []string{}})
	return provider.ExitStatus{Code: 0}
}

func (w *toolsWorld) next(t *testing.T, prefix string) string {
	t.Helper()
	select {
	case r := <-w.replies:
		if !strings.HasPrefix(r, prefix+" ") {
			t.Fatalf("reply %q, want prefix %q", r, prefix)
		}
		return strings.TrimPrefix(r, prefix+" ")
	case <-time.After(60 * time.Second):
		t.Fatalf("worker did not reach %s", prefix)
	}
	return ""
}

func TestWorkspaceAndMCPWiring(t *testing.T) {
	if _, err := os.Stat("/bin/bash"); err != nil {
		t.Skip("needs /bin/bash")
	}
	mcpSrv := httptest.NewServer(mcpdemo.Handler(false, true))
	defer mcpSrv.Close()
	h := newHarness(t)
	w := &toolsWorld{h: h, replies: make(chan string, 16)}
	h.prov = fake.New(w.program)
	h.execDigest = func() (string, error) { return "digest-exec-test", nil }
	cfg := execTestConfig()
	cfg.WorkspaceTools = true
	cfg.workspaceSweep = 50 * time.Millisecond
	cfg.MCP = &mcp.Config{Servers: []mcp.ServerConfig{{Name: "calc", Transport: mcp.TransportHTTP, URL: mcpSrv.URL,
		AllowedTools: []string{"calculate"}}}}
	h.start(cfg, task.SystemClock())
	base := h.waitAddr()
	id := submit(t, base, "tools-1", `{}`)

	if r := w.next(t, "/v1/mcp/tools"); !strings.HasPrefix(r, "200 ") || !strings.Contains(r, `"name":"mcp__calc__calculate"`) ||
		strings.Contains(r, "echo_env") {
		t.Fatalf("tools = %s", r)
	}
	if r := w.next(t, "/v1/mcp/call"); !strings.HasPrefix(r, "200 ") || !strings.Contains(r, `"text":"2+3 = 5"`) {
		t.Fatalf("mcp call = %s", r)
	}
	if r := w.next(t, "/v1/mcp/call"); !strings.HasPrefix(r, "403 ") || !strings.Contains(r, "mcp_tool_not_allowed") {
		t.Fatalf("not allowlisted = %s", r)
	}
	if r := w.next(t, "/v1/workspace/write"); !strings.HasPrefix(r, "200 ") || !strings.Contains(r, `"created":true`) {
		t.Fatalf("write = %s", r)
	}
	r := w.next(t, "/v1/workspace/exec")
	if !strings.HasPrefix(r, "200 ") || !strings.Contains(r, `"stdout":"ok\n"`) || !strings.Contains(r, `"added":["b.txt"]`) ||
		!strings.Contains(r, `"path":"leak","reason":"symlink"`) || !strings.Contains(r, `"exit":{"code":0`) {
		t.Fatalf("exec = %s", r)
	}
	if r := w.next(t, "/v1/workspace/read"); !strings.Contains(r, `"content":"HELLO\n"`) {
		t.Fatalf("read b.txt = %s", r)
	}
	if r := w.next(t, "/v1/workspace/read"); !strings.HasPrefix(r, "404 ") {
		t.Fatalf("read symlink = %s", r)
	}
	if r := w.next(t, "/v1/workspace/read"); !strings.HasPrefix(r, "400 ") || !strings.Contains(r, "invalid_path") {
		t.Fatalf("read escape = %s", r)
	}
	if r := w.next(t, "/v1/workspace/list"); !strings.Contains(r, `"name":"b.txt"`) || !strings.Contains(r, `"name":"src/a.txt"`) {
		t.Fatalf("list = %s", r)
	}
	eventually(t, "task succeeded", nil, func() bool { s, _ := h.taskStatus(id); return s == "succeeded" })

	// Journal: the MCP call is a /v1/mcp call that counted against the tool budget; the workspace command is an exec call.
	var mcpEndpoint, execEndpoint string
	h.queryRow("SELECT endpoint FROM calls WHERE task_id = $1 AND call_id = 'root/orch/mcp/1'", []any{id}, &mcpEndpoint)
	h.queryRow("SELECT endpoint FROM calls WHERE task_id = $1 AND call_id = 'root/orch/workspace_exec/1'", []any{id}, &execEndpoint)
	var used int64
	h.queryRow("SELECT tool_calls_used FROM budgets WHERE task_id = $1", []any{id}, &used)
	if mcpEndpoint != "/v1/mcp" || execEndpoint != "/v1/exec" || used != 1 {
		t.Fatalf("journal: mcp %q exec %q tool_calls_used %d", mcpEndpoint, execEndpoint, used)
	}
	// The terminal task's workspace is destroyed by the sweeper.
	deadline := time.Now().Add(30 * time.Second)
	for {
		ents, err := os.ReadDir(workspacesDir(h.dir))
		if err == nil && len(ents) == 0 {
			break
		}
		if time.Now().After(deadline) {
			var names []string
			for _, e := range ents {
				b, _ := os.ReadFile(filepath.Join(workspacesDir(h.dir), e.Name()))
				names = append(names, e.Name()+"="+string(b))
			}
			t.Fatalf("workspace not destroyed: %v %v", names, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}
