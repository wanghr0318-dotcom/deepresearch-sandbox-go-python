package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/mcpdemo"
)

// The test binary doubles as the demo stdio server (helper-process pattern).
func TestMain(m *testing.M) {
	if os.Getenv("MCPDEMO_HELPER") == "1" {
		if f := os.Getenv("MCPDEMO_REPORT"); f != "" { // process-tree test: start a child, report its pid and our cwd
			reportHelper(f)
		}
		if err := mcpdemo.ServeStdio(os.Stdin, os.Stdout); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func stdioCfg(allowed ...string) ServerConfig {
	return ServerConfig{Name: "calc", Transport: TransportStdio, Command: []string{os.Args[0]},
		Env: map[string]string{"MCPDEMO_HELPER": "1"}, AllowedTools: allowed, TimeoutMs: 5000}
}

func newHub(t *testing.T, servers ...ServerConfig) *Hub {
	t.Helper()
	h, err := NewHub(Config{Servers: servers}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

func textOf(r CallResult) string {
	var b strings.Builder
	for _, c := range r.Content {
		b.WriteString(c.Text)
	}
	return b.String()
}

func TestConfigValidation(t *testing.T) {
	good := `{"servers":[{"name":"calc","transport":"stdio","command":["/bin/x"],"allowed_tools":["calculate"]},
		{"name":"docs","transport":"http","url":"https://mcp.example/mcp","headers_from_env":{"Authorization":"DOCS_TOKEN"},"allowed_tools":["search"]}]}`
	if _, err := ParseConfig([]byte(good)); err != nil {
		t.Fatalf("good config: %v", err)
	}
	bad := map[string]string{
		"no servers":       `{"servers":[]}`,
		"unknown field":    `{"servers":[{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["t"],"shell":true}]}`,
		"bad name":         `{"servers":[{"name":"A-B","transport":"stdio","command":["x"],"allowed_tools":["t"]}]}`,
		"dup name":         `{"servers":[{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["t"]},{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["t"]}]}`,
		"no allowlist":     `{"servers":[{"name":"a","transport":"stdio","command":["x"]}]}`,
		"bad tool":         `{"servers":[{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["a.b"]}]}`,
		"name too long":    `{"servers":[{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["` + strings.Repeat("t", 57) + `"]}]}`,
		"stdio no cmd":     `{"servers":[{"name":"a","transport":"stdio","allowed_tools":["t"]}]}`,
		"http bad url":     `{"servers":[{"name":"a","transport":"http","url":"ftp://x","allowed_tools":["t"]}]}`,
		"http userinfo":    `{"servers":[{"name":"a","transport":"http","url":"https://u:p@x/","allowed_tools":["t"]}]}`,
		"http with cmd":    `{"servers":[{"name":"a","transport":"http","url":"https://x/","command":["y"],"allowed_tools":["t"]}]}`,
		"bad transport":    `{"servers":[{"name":"a","transport":"ws","allowed_tools":["t"]}]}`,
		"bad env_from":     `{"servers":[{"name":"a","transport":"stdio","command":["x"],"env_from":{"A":"$B"},"allowed_tools":["t"]}]}`,
		"negative timeout": `{"servers":[{"name":"a","transport":"stdio","command":["x"],"allowed_tools":["t"],"timeout_ms":-1}]}`,
	}
	for name, b := range bad {
		if _, err := ParseConfig([]byte(b)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if d := (ServerConfig{TimeoutMs: 999_999}).Timeout(); d != MaxTimeout {
		t.Errorf("timeout cap = %v", d)
	}
}

func TestStdioListAndCall(t *testing.T) {
	h := newHub(t, stdioCfg("calculate", "unit_convert"))
	start := time.Now()
	tools := h.Tools(context.Background()) // starts the process, initialize, tools/list
	listDur := time.Since(start)
	if len(tools) != 2 || tools[0].Name != "mcp__calc__calculate" || tools[1].Name != "mcp__calc__unit_convert" ||
		!strings.Contains(string(tools[0].InputSchema), "expression") {
		t.Fatalf("tools = %+v", tools)
	}
	start = time.Now()
	r, uerr := h.Call(context.Background(), "calc", "calculate", json.RawMessage(`{"expression":"(1.5 + 2) * 4 ^ 2"}`))
	t.Logf("stdio: spawn+initialize+tools/list %v; tools/call %v", listDur, time.Since(start))
	if uerr != nil || r.IsError || textOf(r) != "(1.5 + 2) * 4 ^ 2 = 56" || string(r.StructuredContent) != `{"value":56}` {
		t.Fatalf("calculate = %+v %v", r, uerr)
	}
	r, uerr = h.Call(context.Background(), "calc", "calculate", json.RawMessage(`{"expression":"1/0"}`))
	if uerr != nil || !r.IsError || textOf(r) != "division by zero" {
		t.Fatalf("tool error = %+v %v", r, uerr)
	}
	// Not allowlisted: rejected without contacting the server.
	if _, uerr = h.Call(context.Background(), "calc", "echo_env", json.RawMessage(`{"name":"PATH"}`)); uerr == nil ||
		uerr.Code != CodeToolNotAllowed || uerr.Status != 403 {
		t.Fatalf("echo_env = %v", uerr)
	}
	if _, uerr = h.Call(context.Background(), "nope", "x", nil); uerr == nil || uerr.Code != CodeServerNotFound {
		t.Fatalf("unknown server = %v", uerr)
	}
	// The process dies: the next call starts it again and re-initializes.
	st := h.servers["calc"].client.tr.(*stdioTransport)
	st.mu.Lock()
	_ = st.cmd.Process.Kill()
	dead := st.dead
	st.mu.Unlock()
	<-dead
	r, uerr = h.Call(context.Background(), "calc", "unit_convert", json.RawMessage(`{"value":10,"from":"km","to":"mi"}`))
	if uerr != nil || !strings.HasPrefix(textOf(r), "10 km = 6.21371192") {
		t.Fatalf("after restart = %+v %v", r, uerr)
	}
}

// The server process sees only PATH, env and env_from — not the Gateway's environment.
func TestStdioEnvironment(t *testing.T) {
	t.Setenv("S5_TEST_SECRET", "s3cr3t")
	t.Setenv("S5_NOT_PASSED", "leak")
	cfg := stdioCfg("echo_env")
	cfg.EnvFrom = map[string]string{"TOKEN": "S5_TEST_SECRET"}
	h := newHub(t, cfg)
	get := func(name string) string {
		r, uerr := h.Call(context.Background(), "calc", "echo_env", json.RawMessage(`{"name":"`+name+`"}`))
		if uerr != nil {
			t.Fatal(uerr)
		}
		return textOf(r)
	}
	if v := get("TOKEN"); v != "s3cr3t" {
		t.Fatalf("env_from: %q", v)
	}
	if v := get("S5_NOT_PASSED"); v != "" {
		t.Fatalf("gateway env leaked: %q", v)
	}
}

func TestJSONRPCErrorIsFatal(t *testing.T) {
	h := newHub(t, stdioCfg("no_such_tool"))
	_, uerr := h.Call(context.Background(), "calc", "no_such_tool", json.RawMessage(`{}`))
	if uerr == nil || uerr.Outcome != upstream.OutcomeFatal || uerr.Code != CodeMCPError {
		t.Fatalf("got %v", uerr)
	}
}

func httpCfg(url string, allowed ...string) ServerConfig {
	return ServerConfig{Name: "web", Transport: TransportHTTP, URL: url, AllowedTools: allowed, TimeoutMs: 2000}
}

func TestHTTPJSONAndSSE(t *testing.T) {
	for _, sse := range []bool{false, true} {
		srv := httptest.NewServer(mcpdemo.Handler(sse, true))
		h := newHub(t, httpCfg(srv.URL+"/mcp", "calculate"))
		tools := h.Tools(context.Background())
		if len(tools) != 1 || tools[0].Name != "mcp__web__calculate" {
			t.Fatalf("sse=%v tools %+v", sse, tools)
		}
		r, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"2^10"}`))
		if uerr != nil || textOf(r) != "2^10 = 1024" {
			t.Fatalf("sse=%v call %+v %v", sse, r, uerr)
		}
		srv.Close()
	}
}

// An expired session (404 with a session id) makes the client initialize again, transparently.
func TestHTTPSessionExpiry(t *testing.T) {
	var cur atomic.Pointer[http.Handler]
	first := mcpdemo.Handler(false, true)
	cur.Store(&first)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { (*cur.Load()).ServeHTTP(w, r) }))
	defer srv.Close()
	h := newHub(t, httpCfg(srv.URL, "calculate"))
	if _, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"1+1"}`)); uerr != nil {
		t.Fatal(uerr)
	}
	fresh := mcpdemo.Handler(false, true) // server restarted: old session unknown
	cur.Store(&fresh)
	r, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"2+2"}`))
	if uerr != nil || textOf(r) != "2+2 = 4" {
		t.Fatalf("after expiry %+v %v", r, uerr)
	}
}

func TestHTTPOutcomes(t *testing.T) {
	// Unreachable (nothing sent) → retryable.
	srv := httptest.NewServer(mcpdemo.Handler(false, false))
	url := srv.URL
	srv.Close()
	h := newHub(t, httpCfg(url, "calculate"))
	_, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"1"}`))
	if uerr == nil || uerr.Outcome != upstream.OutcomeRetryable || uerr.Code != upstream.CodeUpstreamUnreachable {
		t.Fatalf("unreachable = %v", uerr)
	}
	// Sent but no answer within the timeout → unknown (not retried automatically).
	release := make(chan struct{})
	demo := mcpdemo.Handler(false, false)
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		if strings.Contains(buf.String(), `"tools/call"`) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		r.Body = http.NoBody
		r2 := r.Clone(r.Context())
		r2.Body = ioNop(buf.Bytes())
		demo.ServeHTTP(w, r2)
	}))
	defer hang.Close()
	defer close(release)
	cfg := httpCfg(hang.URL, "calculate")
	cfg.TimeoutMs = 200
	h2 := newHub(t, cfg)
	start := time.Now()
	_, uerr = h2.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"1"}`))
	if uerr == nil || uerr.Outcome != upstream.OutcomeUnknown || uerr.Code != upstream.CodeUpstreamUnconfirmed {
		t.Fatalf("hang = %v", uerr)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("timeout not applied: %v", d)
	}
}

// Header credentials never appear in error texts.
func TestHTTPCredentialsNotInErrors(t *testing.T) {
	t.Setenv("S5_MCP_AUTH", "Bearer top-secret-token")
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		http.Error(w, "Bearer top-secret-token is invalid", http.StatusInternalServerError)
	}))
	defer srv.Close()
	cfg := httpCfg(srv.URL+"?key=also-secret", "calculate")
	cfg.HeadersFromEnv = map[string]string{"Authorization": "S5_MCP_AUTH"}
	h := newHub(t, cfg)
	_, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{}`))
	if uerr == nil {
		t.Fatal("expected an error")
	}
	if s := uerr.Error(); strings.Contains(s, "top-secret") || strings.Contains(s, "also-secret") {
		t.Fatalf("credential in error: %s", s)
	}
	if got.Load() != "Bearer top-secret-token" {
		t.Fatalf("header not sent: %v", got.Load())
	}
}

func TestAdapter(t *testing.T) {
	h := newHub(t, stdioCfg("calculate"))
	a := NewAdapter(h)
	if a.Kind() != KindMCP || a.Provider() != "mcp" || a.Version() != "mcp/1" {
		t.Fatal("identity")
	}
	for name, tc := range map[string]struct {
		body   string
		status int
		code   string
	}{
		"unknown field":  {`{"server":"calc","tool":"calculate","arguments":{},"x":1}`, 400, upstream.CodeUnsupportedField},
		"missing tool":   {`{"server":"calc"}`, 400, upstream.CodeInvalidRequest},
		"args not obj":   {`{"server":"calc","tool":"calculate","arguments":[1]}`, 400, upstream.CodeInvalidRequest},
		"unknown server": {`{"server":"zzz","tool":"calculate"}`, 404, CodeServerNotFound},
		"not allowed":    {`{"server":"calc","tool":"echo_env","arguments":{"name":"HOME"}}`, 403, CodeToolNotAllowed},
	} {
		_, _, err := a.Resolve([]byte(tc.body))
		ue, ok := err.(*upstream.Error)
		if !ok || ue.Status != tc.status || ue.Code != tc.code {
			t.Errorf("%s: %v", name, err)
		}
	}
	resolved, defaults, err := a.Resolve([]byte(`{"server":"calc","tool":"calculate"}`))
	if err != nil || len(defaults) != 0 || string(resolved) != `{"server":"calc","tool":"calculate","arguments":{}}` {
		t.Fatalf("resolve %s %v", resolved, err)
	}
	if est, _ := a.Estimate(resolved); est != 0 {
		t.Fatal("estimate must be 0")
	}
	resolved, _, _ = a.Resolve([]byte(`{"server":"calc","tool":"calculate","arguments":{ "expression" : "6*7" }}`))
	resp, uerr := a.Do(context.Background(), resolved)
	if uerr != nil {
		t.Fatal(uerr)
	}
	var cr CallResult
	if err := json.Unmarshal(resp.Body, &cr); err != nil || cr.Server != "calc" || cr.Tool != "calculate" || textOf(cr) != "6*7 = 42" ||
		resp.Usage.Requests != 1 {
		t.Fatalf("do %s %v", resp.Body, err)
	}
}

func TestDemoEvaluate(t *testing.T) {
	for expr, want := range map[string]float64{"1+2*3": 7, "(1+2)*3": 9, "2^3^2": 512, "-2^2": -4, "7 % 4": 3, "1.5*2": 3, "10/4": 2.5} {
		if v, err := mcpdemo.Evaluate(expr); err != nil || v != want {
			t.Errorf("%s = %v %v, want %v", expr, v, err, want)
		}
	}
	for _, expr := range []string{"", "1+", "(1", "1/0", "abc", "1 2"} {
		if _, err := mcpdemo.Evaluate(expr); err == nil {
			t.Errorf("%q accepted", expr)
		}
	}
}

func ioNop(b []byte) io.ReadCloser { return io.NopCloser(bytes.NewReader(b)) }

func TestHTTPStatusClassification(t *testing.T) {
	for _, tc := range []struct {
		status  int
		outcome upstream.Outcome
		code    string
	}{
		{401, upstream.OutcomeFatal, upstream.CodeUpstreamRejected},
		{403, upstream.OutcomeFatal, upstream.CodeUpstreamRejected},
		{400, upstream.OutcomeFatal, upstream.CodeUpstreamRejected},
		{429, upstream.OutcomeRetryable, upstream.CodeUpstreamRateLimited},
		{500, upstream.OutcomeUnknown, upstream.CodeUpstreamUnconfirmed},
	} {
		demo := mcpdemo.Handler(false, false)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r.Body)
			if strings.Contains(buf.String(), `"tools/call"`) {
				w.WriteHeader(tc.status)
				return
			}
			r.Body = ioNop(buf.Bytes())
			demo.ServeHTTP(w, r)
		}))
		h := newHub(t, httpCfg(srv.URL, "calculate"))
		_, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{"expression":"1"}`))
		if uerr == nil || uerr.Outcome != tc.outcome || uerr.Code != tc.code {
			t.Errorf("HTTP %d: %v", tc.status, uerr)
		}
		srv.Close()
	}
	// A refusal at initialize (401) is fatal too, not "not sent".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	h := newHub(t, httpCfg(srv.URL, "calculate"))
	if _, uerr := h.Call(context.Background(), "web", "calculate", json.RawMessage(`{}`)); uerr == nil ||
		uerr.Outcome != upstream.OutcomeFatal {
		t.Fatalf("401 at initialize: %v", uerr)
	}
}

func TestArgumentsCap(t *testing.T) {
	a := NewAdapter(newHub(t, stdioCfg("calculate")))
	big := `{"server":"calc","tool":"calculate","arguments":{"expression":"` + strings.Repeat("1", MaxArgumentsBytes) + `"}}`
	_, _, err := a.Resolve([]byte(big))
	if ue, ok := err.(*upstream.Error); !ok || ue.Code != CodeArgumentsTooLarge || ue.Status != 413 {
		t.Fatalf("big arguments: %v", err)
	}
}

func TestConfigDirUIDValidation(t *testing.T) {
	for name, b := range map[string]string{
		"relative dir": `{"servers":[{"name":"a","transport":"stdio","command":["x"],"dir":"rel","allowed_tools":["t"]}]}`,
		"http uid":     `{"servers":[{"name":"a","transport":"http","url":"https://x/","uid":1000,"allowed_tools":["t"]}]}`,
	} {
		if _, err := ParseConfig([]byte(b)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := ParseConfig([]byte(`{"servers":[{"name":"a","transport":"stdio","command":["x"],"dir":"/srv/mcp","uid":1000,"gid":1000,"allowed_tools":["t"]}]}`)); err != nil {
		t.Fatalf("valid stdio dir/uid: %v", err)
	}
}
