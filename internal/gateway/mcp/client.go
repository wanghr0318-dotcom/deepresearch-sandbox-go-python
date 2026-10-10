package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// ProtocolVersion is the MCP revision this client speaks.
const ProtocolVersion = "2025-06-18"

// MaxMessageBytes bounds one JSON-RPC message from a server.
const MaxMessageBytes = 1 << 20

// rpcError is a JSON-RPC error object.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message) }

// message is any JSON-RPC 2.0 message.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// transportError classifies a failure: Sent is false when the request certainly did not reach the server.
type transportError struct {
	Sent bool
	Err  error
}

func (e *transportError) Error() string { return e.Err.Error() }
func (e *transportError) Unwrap() error { return e.Err }

func notSent(err error) error { return &transportError{Sent: false, Err: err} }
func sent(err error) error    { return &transportError{Sent: true, Err: err} }

// wasSent reports whether err may have happened after the request was delivered (unknown outcome).
func wasSent(err error) bool {
	var te *transportError
	if errors.As(err, &te) {
		return te.Sent
	}
	var re *rpcError
	return errors.As(err, &re) // the server answered with an error: it was delivered
}

// transport carries JSON-RPC requests to one server.
type transport interface {
	// request sends a request and returns its result (or *rpcError / *transportError).
	request(ctx context.Context, id int64, method string, params any) (json.RawMessage, error)
	notify(ctx context.Context, method string, params any) error
	close() error
}

func encodeRequest(id int64, method string, params any) ([]byte, error) {
	m := map[string]any{"jsonrpc": "2.0", "method": method}
	if id != 0 {
		m["id"] = id
	}
	if params != nil {
		m["params"] = params
	}
	return json.Marshal(m)
}

func idMatches(raw json.RawMessage, id int64) bool {
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&n) != nil {
		return false
	}
	return n.String() == fmt.Sprint(id)
}

// ---- stdio ----

// stdioTransport runs the server as a child process (argv, no shell) and speaks newline-delimited JSON-RPC over its
// stdin/stdout. Requests are serialized: one call at a time per server, so a slow call delays the others. A process
// that died, or whose request timed out (state unknown), is killed together with its whole process group — which
// also aborts nothing else, since nothing else is in flight on it — and restarted on the next request.
//
// Containment (Linux): own process group (Setpgid; the group is killed), Pdeathsig SIGKILL (dies with the
// Gateway), optional uid/gid from the config, working directory from the config or a fresh empty temp directory.
// The server still runs on the host as that user (root by default) with arguments chosen by the model: only allowlist
// tools that are safe with hostile arguments.
type stdioTransport struct {
	cfg ServerConfig
	log *slog.Logger
	dir string // working directory created for the server when the config has none (removed on close)

	mu    sync.Mutex // serializes requests and protects the fields below
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan []byte // stdout lines; closed when the process's stdout ends
	dead  chan struct{}
}

func newStdio(cfg ServerConfig, log *slog.Logger) *stdioTransport {
	return &stdioTransport{cfg: cfg, log: log}
}

// environ is the child's whole environment: PATH (unless given), env, and env_from resolved from the host.
func (t *stdioTransport) environ() []string {
	env := map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin"}
	for k, v := range t.cfg.Env {
		env[k] = v
	}
	for k, from := range t.cfg.EnvFrom {
		env[k] = os.Getenv(from)
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

func (t *stdioTransport) startLocked() error {
	if t.cmd != nil {
		select {
		case <-t.dead:
			t.cmd = nil
		default:
			return nil
		}
	}
	cmd := exec.Command(t.cfg.Command[0], t.cfg.Command[1:]...)
	cmd.Env = t.environ()
	dir, err := t.workDir()
	if err != nil {
		return err
	}
	cmd.Dir = dir
	if err := contain(cmd, t.cfg); err != nil {
		return err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	lines, dead := make(chan []byte, 16), make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64<<10), MaxMessageBytes)
		for sc.Scan() {
			lines <- append([]byte(nil), sc.Bytes()...)
		}
		close(lines)
	}()
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 4<<10), 64<<10)
		for sc.Scan() {
			line := sc.Text()
			if len(line) > 512 {
				line = line[:512] + "…"
			}
			t.log.Info("gateway: mcp server stderr", "server", t.cfg.Name, "line", line)
		}
	}()
	go func() {
		err := cmd.Wait()
		t.log.Info("gateway: mcp server exited", "server", t.cfg.Name, "err", err)
		close(dead)
	}()
	t.cmd, t.stdin, t.lines, t.dead = cmd, stdin, lines, dead
	t.log.Info("gateway: mcp server started", "server", t.cfg.Name, "pid", cmd.Process.Pid)
	return nil
}

// workDir is the configured directory, or an empty temp directory made once per transport.
func (t *stdioTransport) workDir() (string, error) {
	if t.cfg.Dir != "" {
		return t.cfg.Dir, nil
	}
	if t.dir == "" {
		d, err := os.MkdirTemp("", "agentbox-mcp-"+t.cfg.Name+"-")
		if err != nil {
			return "", err
		}
		if err := chownTo(d, t.cfg); err != nil {
			_ = os.Remove(d)
			return "", err
		}
		t.dir = d
	}
	return t.dir, nil
}

// killLocked stops the process and its process group (state unknown after a timeout or protocol error).
func (t *stdioTransport) killLocked() {
	if t.cmd == nil {
		return
	}
	killTree(t.cmd) // already exiting is fine
	_ = t.stdin.Close()
	<-t.dead
	go func(ch <-chan []byte) { // let the stdout reader finish
		for range ch {
		}
	}(t.lines)
	t.cmd = nil
}

func (t *stdioTransport) write(b []byte) error {
	_, err := t.stdin.Write(append(b, '\n'))
	return err
}

func (t *stdioTransport) request(ctx context.Context, id int64, method string, params any) (json.RawMessage, error) {
	b, err := encodeRequest(id, method, params)
	if err != nil {
		return nil, notSent(err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.startLocked(); err != nil {
		return nil, notSent(fmt.Errorf("start server: %w", err))
	}
	if err := t.write(b); err != nil {
		t.killLocked()
		return nil, notSent(fmt.Errorf("write request: %w", err))
	}
	for {
		select {
		case <-ctx.Done():
			t.killLocked()
			return nil, sent(fmt.Errorf("no response: %w", ctx.Err()))
		case line, ok := <-t.lines:
			if !ok {
				t.killLocked()
				return nil, sent(errors.New("server closed its output"))
			}
			var m message
			if err := json.Unmarshal(line, &m); err != nil {
				t.killLocked()
				return nil, sent(fmt.Errorf("invalid message: %w", err))
			}
			if m.Method != "" { // a server request or notification
				t.handleServerRequest(m)
				continue
			}
			if !idMatches(m.ID, id) {
				continue // a stale response
			}
			if m.Error != nil {
				return nil, m.Error
			}
			return m.Result, nil
		}
	}
}

// handleServerRequest answers requests from the server (ping → {}, anything else → method not found); notifications
// are ignored.
func (t *stdioTransport) handleServerRequest(m message) {
	if len(m.ID) == 0 {
		return
	}
	reply := map[string]any{"jsonrpc": "2.0", "id": m.ID}
	if m.Method == "ping" {
		reply["result"] = map[string]any{}
	} else {
		reply["error"] = rpcError{Code: -32601, Message: "method not found"}
	}
	if rb, err := json.Marshal(reply); err == nil {
		_ = t.write(rb) // best effort
	}
}

func (t *stdioTransport) notify(_ context.Context, method string, params any) error {
	b, err := encodeRequest(0, method, params)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.startLocked(); err != nil {
		return err
	}
	return t.write(b)
}

// running reports whether the process is running (otherwise the client initializes again).
func (t *stdioTransport) running() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cmd == nil {
		return false
	}
	select {
	case <-t.dead:
		return false
	default:
		return true
	}
}

func (t *stdioTransport) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.killLocked()
	if t.dir != "" {
		_ = os.RemoveAll(t.dir) // shutdown: best effort
		t.dir = ""
	}
	return nil
}

// ---- streamable HTTP ----

// httpTransport speaks the MCP streamable HTTP transport: POST of one JSON-RPC message, answered by a JSON body or an
// SSE stream; Mcp-Session-Id from initialize is echoed; no redirects.
type httpTransport struct {
	cfg    ServerConfig
	client *http.Client

	mu      sync.Mutex
	session string
}

func newHTTP(cfg ServerConfig) *httpTransport {
	return &httpTransport{cfg: cfg, client: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &http.Transport{Proxy: nil, ResponseHeaderTimeout: MaxTimeout, MaxIdleConnsPerHost: 4},
	}}
}

func (t *httpTransport) post(ctx context.Context, body []byte, id int64) (json.RawMessage, error) {
	wrote := false
	var mu sync.Mutex
	trace := &httptrace.ClientTrace{WroteHeaders: func() { mu.Lock(); wrote = true; mu.Unlock() }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, t.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, notSent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", ProtocolVersion)
	for h, from := range t.cfg.HeadersFromEnv {
		req.Header.Set(h, os.Getenv(from))
	}
	t.mu.Lock()
	if t.session != "" {
		req.Header.Set("Mcp-Session-Id", t.session)
	}
	t.mu.Unlock()
	resp, err := t.client.Do(req)
	if err != nil {
		mu.Lock()
		w := wrote
		mu.Unlock()
		// Errors never include header values (credentials): only the URL host and the transport error class.
		err = fmt.Errorf("POST %s: %w", redact(t.cfg.URL), unwrapURL(err))
		if w {
			return nil, sent(err)
		}
		return nil, notSent(err)
	}
	defer resp.Body.Close()
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.mu.Lock()
		t.session = sid
		t.mu.Unlock()
	}
	if id == 0 { // notification: 202 Accepted (or 200) with no body
		if resp.StatusCode/100 != 2 {
			return nil, sent(fmt.Errorf("notification: HTTP %d", resp.StatusCode))
		}
		return nil, nil
	}
	if resp.StatusCode == http.StatusNotFound && req.Header.Get("Mcp-Session-Id") != "" {
		t.mu.Lock()
		t.session = ""
		t.mu.Unlock()
		return nil, &sessionExpired{}
	}
	if resp.StatusCode/100 != 2 {
		return nil, &httpStatusError{Status: resp.StatusCode}
	}
	return decodeResponse(resp, id)
}

// httpStatusError is a non-2xx answer to a request: the server answered, so the outcome is known to be a refusal
// for 4xx (fatal; 408/429 retryable) and unknown for 5xx (it may have acted).
type httpStatusError struct{ Status int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("HTTP %d", e.Status) }

// decodeResponse reads the JSON-RPC response with the given id from a JSON body or an SSE stream (≤ 1 MiB).
func decodeResponse(resp *http.Response, id int64) (json.RawMessage, error) {
	limited := io.LimitReader(resp.Body, MaxMessageBytes+1)
	ct, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	var m *message
	var err error
	if ct == "text/event-stream" {
		m, err = readSSE(limited, id)
	} else {
		var b []byte
		b, err = io.ReadAll(limited)
		if err == nil && len(b) > MaxMessageBytes {
			err = errors.New("response too large")
		}
		if err == nil {
			m = &message{}
			err = json.Unmarshal(b, m)
		}
	}
	if err != nil {
		return nil, sent(err)
	}
	if !idMatches(m.ID, id) {
		return nil, sent(errors.New("response id mismatch"))
	}
	if m.Error != nil {
		return nil, m.Error
	}
	return m.Result, nil
}

// sessionExpired asks the client to initialize again (the request was not processed).
type sessionExpired struct{}

func (*sessionExpired) Error() string { return "mcp session expired" }

// readSSE returns the first JSON-RPC response with the given id from an SSE stream.
func readSSE(r io.Reader, id int64) (*message, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), MaxMessageBytes)
	var data strings.Builder
	flush := func() (*message, bool) {
		defer data.Reset()
		if data.Len() == 0 {
			return nil, false
		}
		var m message
		if json.Unmarshal([]byte(data.String()), &m) != nil || m.Method != "" || !idMatches(m.ID, id) {
			return nil, false
		}
		return &m, true
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if m, ok := flush(); ok {
				return m, nil
			}
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if m, ok := flush(); ok {
		return m, nil
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("event stream ended without a response")
}

func (t *httpTransport) request(ctx context.Context, id int64, method string, params any) (json.RawMessage, error) {
	b, err := encodeRequest(id, method, params)
	if err != nil {
		return nil, notSent(err)
	}
	return t.post(ctx, b, id)
}

func (t *httpTransport) notify(ctx context.Context, method string, params any) error {
	b, err := encodeRequest(0, method, params)
	if err != nil {
		return err
	}
	_, err = t.post(ctx, b, 0)
	return err
}

func (t *httpTransport) close() error {
	t.client.CloseIdleConnections()
	return nil
}

func redact(raw string) string {
	if i := strings.Index(raw, "?"); i >= 0 {
		raw = raw[:i]
	}
	return raw
}

// unwrapURL drops *url.Error's quoting of the full URL (which may carry a query string).
func unwrapURL(err error) error {
	type unwrapper interface{ Unwrap() error }
	if u, ok := err.(unwrapper); ok && strings.HasPrefix(err.Error(), "Post ") {
		if inner := u.Unwrap(); inner != nil {
			return inner
		}
	}
	return err
}

// ---- client ----

// client is the MCP session with one server: initialize once (again after a stdio restart or an expired HTTP
// session), then tools/list and tools/call.
type client struct {
	cfg ServerConfig
	tr  transport

	mu     sync.Mutex
	nextID int64
	inited bool
}

func newClient(cfg ServerConfig, log *slog.Logger) *client {
	c := &client{cfg: cfg}
	if cfg.Transport == TransportStdio {
		c.tr = newStdio(cfg, log)
	} else {
		c.tr = newHTTP(cfg)
	}
	return c
}

func (c *client) id() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID
}

func (c *client) ensureInit(ctx context.Context) error {
	c.mu.Lock()
	inited := c.inited
	c.mu.Unlock()
	if st, ok := c.tr.(*stdioTransport); ok && inited && !st.running() {
		inited = false
	}
	if inited {
		return nil
	}
	params := map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": "agentbox-gateway", "version": "1"}}
	if _, err := c.tr.request(ctx, c.id(), "initialize", params); err != nil {
		var hse *httpStatusError
		if errors.As(err, &hse) && hse.Status < 500 {
			return err // the server refused the session (e.g. 401): classified by status
		}
		// A 5xx (or any other failure) at initialize: the server may be broken, but the tool call itself was
		// never sent, so the call is retryable rather than unknown. The status error is not wrapped (%v): classify
		// would otherwise treat it as a 5xx answer to the call (unknown).
		if hse != nil {
			return notSent(fmt.Errorf("initialize: %v", err))
		}
		return notSent(fmt.Errorf("initialize: %w", err)) // nothing of the actual request was sent yet
	}
	if err := c.tr.notify(ctx, "notifications/initialized", nil); err != nil {
		return notSent(fmt.Errorf("initialized: %w", err))
	}
	c.mu.Lock()
	c.inited = true
	c.mu.Unlock()
	return nil
}

func (c *client) do(ctx context.Context, method string, params any) (json.RawMessage, error) {
	for attempt := 0; ; attempt++ {
		if err := c.ensureInit(ctx); err != nil {
			return nil, err
		}
		res, err := c.tr.request(ctx, c.id(), method, params)
		var se *sessionExpired
		if errors.As(err, &se) && attempt == 0 {
			c.mu.Lock()
			c.inited = false
			c.mu.Unlock()
			continue
		}
		if errors.As(err, &se) {
			return nil, notSent(err)
		}
		if err != nil {
			var te *transportError
			if errors.As(err, &te) && !te.Sent {
				c.mu.Lock()
				c.inited = false // a broken connection or restarted process: initialize again next time
				c.mu.Unlock()
			}
		}
		return res, err
	}
}

// ToolInfo is a tool as listed by a server.
type ToolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
}

func (c *client) listTools(ctx context.Context) ([]ToolInfo, error) {
	var all []ToolInfo
	cursor := ""
	for page := 0; page < 10; page++ {
		var params any
		if cursor != "" {
			params = map[string]any{"cursor": cursor}
		}
		raw, err := c.do(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var r struct {
			Tools      []ToolInfo `json:"tools"`
			NextCursor string     `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("tools/list result: %w", err)
		}
		all = append(all, r.Tools...)
		if r.NextCursor == "" {
			return all, nil
		}
		cursor = r.NextCursor
	}
	return all, nil
}

func (c *client) callTool(ctx context.Context, tool string, args json.RawMessage) (json.RawMessage, error) {
	return c.do(ctx, "tools/call", map[string]any{"name": tool, "arguments": args})
}

func (c *client) close() error { return c.tr.close() }
