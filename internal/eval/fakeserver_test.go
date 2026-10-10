package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// plan is how the fake server behaves for one created task.
type plan struct {
	status, reason string
	delay          time.Duration // time until task_terminal (cancel ends it early)
	artifacts      map[string][]byte
	calls          []Call
	budget         *Budget
	cutOnce        bool // the first SSE connection drops after the first event (reconnect test)
}

type fakeTask struct {
	id       string
	spec     map[string]any
	limits   json.RawMessage
	plan     plan
	created  time.Time
	cancel   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	status   string
	terminal chan struct{}
	conns    int
}

// fakeServer implements the subset of the agentbox API the evaluator uses.
type fakeServer struct {
	t     *testing.T
	srv   *httptest.Server
	token string
	plan  func(spec map[string]any) plan

	mu        sync.Mutex
	tasks     map[string]*fakeTask
	byRequest map[string]string
	order     []string
	running   int
	maxRun    int
	fail503   int // the next N POST /tasks answer 503
	noInfo    bool
}

func newFakeServer(t *testing.T, planFn func(spec map[string]any) plan) *fakeServer {
	f := &fakeServer{t: t, token: "secret-token-123", plan: planFn, tasks: map[string]*fakeTask{}, byRequest: map[string]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) client() *Client {
	return &Client{Base: f.srv.URL, Token: f.token, Sleep: func(context.Context, time.Duration) {}}
}

func writeJ(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		writeJ(w, 401, map[string]any{"error": map[string]string{"code": "unauthorized"}})
		return
	}
	p := r.URL.Path
	switch {
	case p == "/status":
		writeJ(w, 200, map[string]string{"mode": "normal"})
	case p == "/server-info":
		if f.noInfo {
			writeJ(w, 404, map[string]any{"error": map[string]string{"code": "not_found"}})
			return
		}
		writeJ(w, 200, map[string]any{"build": map[string]string{"vcs.revision": "abc123"}, "models": map[string]any{"default": "fake-model"}})
	case p == "/tasks" && r.Method == http.MethodPost:
		f.create(w, r)
	default:
		parts := strings.Split(strings.TrimPrefix(p, "/tasks/"), "/")
		f.mu.Lock()
		ft := f.tasks[parts[0]]
		f.mu.Unlock()
		if ft == nil {
			writeJ(w, 404, map[string]any{"error": map[string]string{"code": "task_not_found"}})
			return
		}
		switch {
		case len(parts) == 2 && parts[1] == "events":
			f.events(w, r, ft)
		case len(parts) == 2 && parts[1] == "cancel":
			ft.once.Do(func() { close(ft.cancel) })
			writeJ(w, 200, map[string]any{"task_id": ft.id})
		case len(parts) == 2 && parts[1] == "inspect":
			f.inspect(w, ft)
		case len(parts) == 2 && parts[1] == "result":
			f.result(w, ft)
		case len(parts) == 5 && parts[1] == "artifacts" && parts[3] == "versions":
			b, ok := ft.plan.artifacts[parts[2]]
			if !ok {
				writeJ(w, 404, map[string]any{"error": map[string]string{"code": "artifact_not_found"}})
				return
			}
			_, _ = w.Write(b)
		default:
			writeJ(w, 404, map[string]any{"error": map[string]string{"code": "not_found"}})
		}
	}
}

func (f *fakeServer) create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RequestID string          `json:"request_id"`
		Spec      map[string]any  `json:"spec"`
		Limits    json.RawMessage `json:"limits"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJ(w, 400, map[string]any{"error": map[string]string{"code": "invalid_request"}})
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail503 > 0 {
		f.fail503--
		writeJ(w, 503, map[string]any{"error": map[string]string{"code": "unavailable"}})
		return
	}
	if id, ok := f.byRequest[body.RequestID]; ok {
		writeJ(w, 201, map[string]string{"task_id": id})
		return
	}
	id := fmt.Sprintf("t%03d", len(f.tasks)+1)
	ft := &fakeTask{id: id, spec: body.Spec, limits: body.Limits, plan: f.plan(body.Spec), created: time.Now(),
		cancel: make(chan struct{}), status: "running", terminal: make(chan struct{})}
	f.tasks[id] = ft
	f.byRequest[body.RequestID] = id
	f.order = append(f.order, specTaskID(body.Spec))
	f.running++
	f.maxRun = max(f.maxRun, f.running)
	go func() {
		select {
		case <-time.After(ft.plan.delay):
			ft.mu.Lock()
			ft.status = ft.plan.status
			ft.mu.Unlock()
		case <-ft.cancel:
			ft.mu.Lock()
			ft.status, ft.plan.reason = "cancelled", "cancel requested"
			ft.mu.Unlock()
		}
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
		close(ft.terminal)
	}()
	writeJ(w, 201, map[string]string{"task_id": id})
}

func specTaskID(spec map[string]any) string {
	if ev, ok := spec["eval"].(map[string]any); ok {
		s, _ := ev["task_id"].(string)
		return s
	}
	return ""
}

func (f *fakeServer) events(w http.ResponseWriter, r *http.Request, ft *fakeTask) {
	last, _ := strconv.Atoi(r.Header.Get("Last-Event-ID"))
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	fl := w.(http.Flusher)
	emit := func(seq int, typ, payload string) {
		if seq <= last {
			return
		}
		fmt.Fprintf(w, "id: %d\nevent: %s\ndata: {\"task_seq\":%d,\"source\":\"host\",\"type\":%q,\"payload\":%s,\"ts\":\"2026-10-10T00:00:00Z\"}\n\n", seq, typ, seq, typ, payload)
		fl.Flush()
	}
	ft.mu.Lock()
	ft.conns++
	cut := ft.plan.cutOnce && ft.conns == 1
	ft.mu.Unlock()
	emit(1, "task_created", "{}")
	if cut {
		return // drop the connection: the client must reconnect with Last-Event-ID 1
	}
	emit(2, "ready", `{"worker":{"name":"evalworker","version":"0.1.0"}}`)
	fmt.Fprint(w, ": heartbeat\n\n")
	fl.Flush()
	select {
	case <-ft.terminal:
	case <-r.Context().Done():
		return
	}
	ft.mu.Lock()
	st, reason := ft.status, ft.plan.reason
	ft.mu.Unlock()
	emit(3, "task_terminal", fmt.Sprintf(`{"task_status":%q,"status_reason":%q}`, st, reason))
}

func (f *fakeServer) inspect(w http.ResponseWriter, ft *fakeTask) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	calls := ft.plan.calls
	if calls == nil {
		calls = []Call{}
	}
	writeJ(w, 200, map[string]any{
		"task":     map[string]any{"task_id": ft.id, "status": ft.status, "status_reason": ft.plan.reason, "attempts_total": 1, "created_at": ft.created},
		"attempts": []any{map[string]any{"attempt_no": 1}}, "checkpoints": []any{}, "calls": calls, "subruns": []any{},
		"budget": ft.plan.budget,
	})
}

func (f *fakeServer) result(w http.ResponseWriter, ft *fakeTask) {
	select {
	case <-ft.terminal:
	default:
		writeJ(w, 409, map[string]any{"error": map[string]string{"code": "task_not_terminal"}})
		return
	}
	if len(ft.plan.artifacts) == 0 {
		writeJ(w, 409, map[string]any{"error": map[string]string{"code": "not_ready"}})
		return
	}
	var outs []PinnedOutput
	for id := range ft.plan.artifacts {
		outs = append(outs, PinnedOutput{ArtifactID: id, Version: 1, SHA256: "x"})
	}
	writeJ(w, 200, Result{Summary: "done", Outputs: outs})
}
