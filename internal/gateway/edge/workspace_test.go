package edge

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/mcp"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
)

// ==== Workspace tools and MCP (design 2026-10-10-shell-file-mcp) ====

type fakeWorkspace struct {
	mu  sync.Mutex
	ops []string
	got []workspace.Request
	res call.Result
}

func (f *fakeWorkspace) rec(op string, r workspace.Request) (call.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
	f.got = append(f.got, r)
	if f.res.Status != 0 {
		return f.res, nil
	}
	return call.Result{Status: 200, Body: []byte(`{"op":"` + op + `"}`), BlobSHA256: map[bool]string{true: testSHA}[op == "exec"]}, nil
}

func (f *fakeWorkspace) Exec(_ context.Context, r workspace.Request) (call.Result, error) {
	return f.rec("exec", r)
}
func (f *fakeWorkspace) Read(_ context.Context, r workspace.Request) (call.Result, error) {
	return f.rec("read", r)
}
func (f *fakeWorkspace) Write(_ context.Context, r workspace.Request) (call.Result, error) {
	return f.rec("write", r)
}
func (f *fakeWorkspace) List(_ context.Context, r workspace.Request) (call.Result, error) {
	return f.rec("list", r)
}

type fakeMCP []mcp.Tool

func (f fakeMCP) Tools(context.Context) []mcp.Tool { return f }

func TestWorkspaceEndpointsNotConfigured(t *testing.T) {
	calls := &fakeCalls{}
	e := newEdge(t, Config{}, calls)
	p := bind(t, e, "a1", "e1")
	for _, path := range []string{"/v1/workspace/exec", "/v1/workspace/read", "/v1/workspace/write", "/v1/workspace/list"} {
		r := do(t, p, "POST", path, strings.NewReader(`{}`), callHdr("root/orch/workspace_exec/1"))
		if r.status != 404 || errCode(t, r) != call.CodeEndpointNotConfigured {
			t.Errorf("%s: %d %s", path, r.status, r.body)
		}
	}
	if r := do(t, p, "GET", "/v1/mcp/tools", nil, nil); r.status != 404 || errCode(t, r) != call.CodeEndpointNotConfigured {
		t.Errorf("/v1/mcp/tools: %d %s", r.status, r.body)
	}
}

func TestWorkspaceEndpoints(t *testing.T) {
	ws := &fakeWorkspace{}
	e := newEdge(t, Config{MaxBody: 1024, Workspace: ws}, &fakeCalls{})
	p := bind(t, e, "a1", "e1")

	r := do(t, p, "POST", "/v1/workspace/exec", strings.NewReader(`{"command":"ls"}`), map[string]string{
		HeaderCallID: "st1/orch/workspace_exec/1", HeaderSubrun: "st1", HeaderRetry: "true"})
	if r.status != 200 || string(r.body) != `{"op":"exec"}` || r.header.Get(HeaderBlob) != testSHA {
		t.Fatalf("exec: %d %v %s", r.status, r.header, r.body)
	}
	// exec requires a call id; file operations do not.
	if r := do(t, p, "POST", "/v1/workspace/exec", strings.NewReader(`{"command":"ls"}`), nil); r.status != 400 ||
		errCode(t, r) != CodeMissingCallID {
		t.Fatalf("exec without call id: %d %s", r.status, r.body)
	}
	if r := do(t, p, "POST", "/v1/workspace/exec", strings.NewReader(`{}`), map[string]string{HeaderCallID: "x", HeaderCache: "no-cache"}); r.status != 400 {
		t.Fatalf("exec with cache header: %d", r.status)
	}
	for _, op := range []string{"read", "write", "list"} {
		r := do(t, p, "POST", "/v1/workspace/"+op, strings.NewReader(`{"path":"a"}`), nil)
		if r.status != 200 || string(r.body) != `{"op":"`+op+`"}` || r.header.Get(HeaderBlob) != "" {
			t.Errorf("%s: %d %v %s", op, r.status, r.header, r.body)
		}
	}
	if r := do(t, p, "GET", "/v1/workspace/read", nil, nil); r.status != 405 {
		t.Errorf("GET read: %d", r.status)
	}
	if r := do(t, p, "POST", "/v1/workspace/write", strings.NewReader(strings.Repeat("x", 2048)), nil); r.status != 413 {
		t.Errorf("oversized write: %d", r.status)
	}
	ws.mu.Lock()
	got := append([]workspace.Request(nil), ws.got...)
	ws.mu.Unlock()
	if len(got) != 4 || got[0].TaskID != "t1" || got[0].AttemptID != "a1" || got[0].CallID != "st1/orch/workspace_exec/1" ||
		got[0].SubrunID != "st1" || !got[0].Retry || string(got[0].Body) != `{"command":"ls"}` || got[1].CallID != "" {
		t.Fatalf("requests %+v", got)
	}
	// Rejections pass through with their status and code.
	ws.res = call.Result{Status: 410, Code: workspace.CodeWorkspaceLost}
	if r := do(t, p, "POST", "/v1/workspace/list", strings.NewReader(`{}`), nil); r.status != 410 || errCode(t, r) != workspace.CodeWorkspaceLost {
		t.Fatalf("lost: %d %s", r.status, r.body)
	}
}

func TestMCPEndpoints(t *testing.T) {
	calls := &fakeCalls{}
	tools := fakeMCP{{Name: "mcp__calc__calculate", Server: "calc", Tool: "calculate", Description: "d",
		InputSchema: json.RawMessage(`{"type":"object"}`)}}
	e := newEdge(t, Config{MCP: tools}, calls)
	p := bind(t, e, "a1", "e1")
	r := do(t, p, "GET", "/v1/mcp/tools", nil, nil)
	if r.status != 200 || !strings.Contains(string(r.body), `"name":"mcp__calc__calculate"`) ||
		!strings.Contains(string(r.body), `"input_schema":{"type":"object"}`) {
		t.Fatalf("tools: %d %s", r.status, r.body)
	}
	// Access denied → no list.
	calls.mu.Lock()
	calls.access = call.Result{Status: 403, Code: "access_revoked"}
	calls.mu.Unlock()
	if r := do(t, p, "GET", "/v1/mcp/tools", nil, nil); r.status != 403 {
		t.Fatalf("revoked: %d", r.status)
	}
	calls.mu.Lock()
	calls.access = call.Result{}
	calls.mu.Unlock()
	// tools/call goes through Invoke as kind mcp (journal, budget, replay).
	body := `{"server":"calc","tool":"calculate","arguments":{"expression":"1+1"}}`
	if r := do(t, p, "POST", "/v1/mcp/call", strings.NewReader(body), callHdr("root/orch/mcp/1")); r.status != 200 {
		t.Fatalf("call: %d %s", r.status, r.body)
	}
	invokes, _, _, _ := calls.snapshot()
	if len(invokes) != 1 || invokes[0].Kind != mcp.KindMCP || invokes[0].CallID != "root/orch/mcp/1" || string(invokes[0].Body) != body {
		t.Fatalf("invokes %+v", invokes)
	}
	var _ upstream.Kind = invokes[0].Kind
}
