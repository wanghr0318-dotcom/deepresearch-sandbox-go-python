package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

func TestTempoPromLoki(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/search":
			if !strings.Contains(r.URL.Query().Get("q"), `span.task.id = "task_a"`) {
				_, _ = w.Write([]byte(`{"traces":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"traces":[{"traceID":"bb","startTimeUnixNano":"200"},{"traceID":"ab12","startTimeUnixNano":"100"}]}`))
		case "/api/traces/" + strings.Repeat("0", 28) + "ab12":
			_, _ = w.Write([]byte(`{"batches":[{"scopeSpans":[{"spans":[
				{"name":"POST /tasks","startTimeUnixNano":"0","endTimeUnixNano":"10000000"},
				{"name":"task","startTimeUnixNano":"5000000","endTimeUnixNano":"1000000000"},
				{"name":"gateway.try","startTimeUnixNano":"100000000","endTimeUnixNano":"700000000"},
				{"name":"gateway.try","startTimeUnixNano":"700000000","endTimeUnixNano":"800000000"}]}]}]}`))
		case "/api/v1/query":
			if !strings.Contains(r.URL.Query().Get("query"), "agentbox_") {
				http.Error(w, "bad", 400)
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"provider":"primary","outcome":"unknown"},"value":[1,"3"]},
				{"metric":{"provider":"backup","outcome":"ok"},"value":[1,"0"]}]}}`))
		case "/loki/api/v1/query_range":
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"values":[
				["1","{\"level\":\"WARN\",\"msg\":\"gateway: breaker\",\"trace_id\":\"t1\"}"],
				["2","{\"level\":\"WARN\",\"msg\":\"gateway: breaker\",\"trace_id\":\"t2\"}"],
				["3","{\"level\":\"ERROR\",\"msg\":\"worker failed\"}"],
				["4","{\"level\":\"INFO\",\"msg\":\"gateway: try\",\"endpoint\":\"/v1/fetch\",\"provider\":\"http_get\",\"outcome\":\"unknown\",\"code\":\"upstream_unconfirmed\",\"trace_id\":\"t3\"}"],
				["5","{\"level\":\"INFO\",\"msg\":\"gateway: try\",\"endpoint\":\"/v1/fetch\",\"outcome\":\"ok\"}"],
				["6","not json"]]}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	c := srv.Client()
	id, err := TempoTraceID(ctx, c, srv.URL, "task_a", t0, t0.Add(time.Hour))
	if err != nil || id != strings.Repeat("0", 28)+"ab12" {
		t.Fatalf("trace id %q %v", id, err)
	}
	spans, err := TempoSpanShares(ctx, c, srv.URL, id)
	if err != nil || len(spans) != 3 || spans[0].Name != "task" || spans[1].Name != "gateway.try" || spans[1].Count != 2 || spans[1].TotalMs != 700 || spans[1].Share != 0.7 {
		t.Fatalf("spans %+v %v", spans, err)
	}
	rows, err := PromInstant(ctx, c, srv.URL, "sum(agentbox_x)", t0)
	if err != nil || len(rows) != 1 || rows[0].Labels["provider"] != "primary" || rows[0].Value != 3 {
		t.Fatalf("prom %+v %v", rows, err)
	}
	logs, truncated, err := LokiFailures(ctx, c, srv.URL, `{job="agentbox"}`, t0, t0.Add(time.Hour))
	if err != nil || truncated || len(logs) != 3 || logs[0].Message != "gateway: breaker" || logs[0].Count != 2 || len(logs[0].TraceIDs) != 2 ||
		logs[1].Message != "gateway: try /v1/fetch route=http_get outcome=unknown code=upstream_unconfirmed" || logs[1].TraceIDs[0] != "t3" {
		t.Fatalf("loki %+v %v", logs, err)
	}

	// Enrich wires them into the report; an unreachable endpoint is only a note.
	trs := hangRun()
	for _, tr := range trs {
		tr.ServerTaskID = "task_a"
		tr.SubmittedAt = t0
	}
	rep := Analyze(Source{}, trs, Options{})
	Enrich(ctx, rep, trs, ObsConfig{Tempo: srv.URL, Prometheus: srv.URL, Loki: "http://127.0.0.1:1", HTTP: c})
	o := rep.Observability
	if o == nil || o.TraceIDs != 1 || len(o.Metrics) != len(promQueries) || len(o.Notes) != 1 || !strings.Contains(o.Notes[0], "loki") {
		t.Fatalf("observability %+v", o)
	}
	if rep.Slowest[0].TraceID == "" || len(rep.Slowest[0].Spans) == 0 || rep.Findings[0].Evidence.Examples[0].TraceID == "" {
		t.Fatalf("trace ids not attached: %+v", rep.Slowest[0])
	}
	if md := RenderMarkdown(rep); !strings.Contains(md, "gateway.try ×2 700 ms (70%)") {
		t.Fatalf("markdown spans:\n%s", md)
	}
}

func TestLoadServerWindow(t *testing.T) {
	now := t0.Add(2 * time.Hour)
	events := func(id string, terminal bool) string {
		evs := []map[string]any{
			{"task_seq": 1, "source": "host", "type": "task_created", "ts": now.Add(-10 * time.Minute)},
			{"task_seq": 2, "source": "host", "type": "attempt_created", "ts": now.Add(-10*time.Minute + time.Second)},
		}
		if terminal {
			evs = append(evs, map[string]any{"task_seq": 3, "source": "host", "type": "task_terminal", "ts": now.Add(-9 * time.Minute)})
		}
		var b strings.Builder
		for _, e := range evs {
			d, _ := json.Marshal(e)
			fmt.Fprintf(&b, "id: %v\nevent: %v\ndata: %s\n\n", e["task_seq"], e["type"], d)
		}
		return b.String()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "no", 401)
			return
		}
		p := r.URL.Path
		switch {
		case p == "/tasks" && r.URL.Query().Get("after") == "":
			fmt.Fprintf(w, `{"tasks":[{"task_id":"t_run","status":"running","created_at":%q},{"task_id":"t_fail","status":"failed","created_at":%q}],"next":"p2"}`,
				now.Add(-5*time.Minute).Format(time.RFC3339Nano), now.Add(-10*time.Minute).Format(time.RFC3339Nano))
		case p == "/tasks":
			fmt.Fprintf(w, `{"tasks":[{"task_id":"t_old","status":"succeeded","created_at":%q}],"next":"p3"}`, now.Add(-3*time.Hour).Format(time.RFC3339Nano))
		case strings.HasSuffix(p, "/inspect"):
			id := strings.Split(p, "/")[2]
			st := map[string]string{"t_run": "running", "t_fail": "failed"}[id]
			fmt.Fprintf(w, `{"task":{"task_id":%q,"status":%q,"status_reason":"worker_failed"},"calls":[{"call_id":"c","endpoint":"/v1/fetch","state":"failed","fail_reason":"call_deadline_exceeded","tries":[{"try_no":1,"state":"settled","outcome":"unknown","latency_ms":1000,"error":"call_deadline_exceeded"}]}]}`, id, st)
		case strings.HasSuffix(p, "/events"):
			id := strings.Split(p, "/")[2]
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, events(id, id == "t_fail"))
			if id == "t_run" {
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := &eval.Client{Base: srv.URL, Token: "tok", Retries: 1, Sleep: func(context.Context, time.Duration) {}}
	src, trs, notes, err := LoadServer(context.Background(), c, Window{Since: now.Add(-time.Hour), EventsTimeout: 200 * time.Millisecond})
	if err != nil || len(trs) != 2 || len(notes) != 0 {
		t.Fatalf("load %v %d %v", err, len(trs), notes)
	}
	if src.Kind != "server" || trs[1].ServerTaskID != "t_fail" || trs[1].Outcome.Verdict != eval.VerdictFail || trs[1].LatencyMs != 60_000 {
		t.Fatalf("trajectory %+v", trs[1])
	}
	if trs[0].Outcome.Verdict != eval.VerdictPass || !strings.HasPrefix(trs[0].Outcome.Category, "not_terminal") {
		t.Fatalf("running task %+v", trs[0].Outcome)
	}
	rep := Analyze(src, trs, Options{})
	if f := findRule(rep, "fetch_deadline"); f == nil || f.Failed != 1 {
		t.Fatalf("server-window analysis: %v", ruleIDs(rep))
	}
}

func TestAdvisor(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var gotAuth, gotBody string
	reply := `{"summary":"The primary hangs.","proposals":[{"flag":"--model-try-timeout","value":"3s","reason":"cut hangs"},{"flag":"--model-hedge-delay","value":"2s","reason":"tail"},{"flag":"--upstream-allow-private","value":"evil:80","reason":"x"},{"flag":"--call-deadline","value":"1h","reason":"y"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		gotAuth, gotBody = r.Header.Get("Authorization"), string(b)
		content, _ := json.Marshal("```json\n" + reply + "\n```")
		fmt.Fprintf(w, `{"choices":[{"message":{"content":%s}}],"usage":{"prompt_tokens":2000,"completion_tokens":300}}`, content)
	}))
	defer srv.Close()
	cfg, _ := ParseFlags([]byte("--model-fallback-file=/x\n"))
	rep := Analyze(Source{}, hangRun(), Options{Config: cfg})
	ad := &Advisor{BaseURL: srv.URL, Model: "m", APIKey: "sk-secret", PriceInMicroPerMTok: 1_000_000, PriceOutMicroPerMTok: 4_000_000, BudgetMicro: 50_000}
	ar := ad.Run(context.Background(), rep, cfg)
	if !ar.Called || ar.Error != "" || ar.SpentMicro != 2000+1200 || ar.Summary != "The primary hangs." {
		t.Fatalf("advisor %+v", ar)
	}
	mu.Lock()
	auth, body := gotAuth, gotBody
	mu.Unlock()
	if auth != "Bearer sk-secret" || !strings.Contains(body, "model_try_hang") || !strings.Contains(body, "--model-try-timeout") {
		t.Fatalf("request auth %q body %.200s", auth, body)
	}
	if len(ar.Accepted) != 2 || len(ar.Rejected) != 2 {
		t.Fatalf("accepted %+v rejected %+v", ar.Accepted, ar.Rejected)
	}
	ApplyAdvisor(rep, ar)
	var got []string
	for _, p := range rep.Proposals {
		if p.Kind == KindApply {
			got = append(got, p.Flag+"="+p.To+"/"+p.Source)
		}
	}
	// the rule's try timeout (2s) wins over the model's 3s; the model's hedge delay is added.
	if strings.Join(got, " ") != "--model-try-timeout=2s/rule --model-hedge-delay=2s/model" || len(rep.Rejected) != 2 {
		t.Fatalf("proposals %v rejected %+v", got, rep.Rejected)
	}
	// Over budget: not called.
	ad.BudgetMicro = 10
	if ar := ad.Run(context.Background(), rep, cfg); ar.Called || !strings.Contains(ar.Error, "exceeds the budget") || calls.Load() != 1 {
		t.Fatalf("over budget %+v", ar)
	}
	// Malformed reply: error, deterministic report intact.
	mu.Lock()
	reply = "no json here"
	mu.Unlock()
	ad.BudgetMicro = 50_000
	if ar := ad.Run(context.Background(), rep, cfg); !ar.Called || ar.Error == "" || len(ar.Accepted) != 0 {
		t.Fatalf("malformed %+v", ar)
	}
}

func writeRun(t *testing.T, dir string, trs []*eval.Trajectory) {
	t.Helper()
	m := eval.Manifest{Schema: eval.SchemaManifest, RunID: "before", Suite: eval.SuiteRef{Name: "doctor"}, Agent: "model"}
	mb, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), mb, 0o644); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	for _, tr := range trs {
		d, _ := json.Marshal(tr)
		b.Write(append(d, '\n'))
	}
	if err := os.WriteFile(filepath.Join(dir, "trajectories.jsonl"), b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMainRunDirApplyTo(t *testing.T) {
	dir := t.TempDir()
	writeRun(t, dir, hangRun())
	cfgPath := filepath.Join(dir, "before.flags")
	if err := os.WriteFile(cfgPath, []byte("# before\n--model-fallback-file=/x.json\n--listen=127.0.0.1:8091\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	advisor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"summary\":\"ok\",\"proposals\":[]}"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`))
	}))
	defer advisor.Close()
	out := filepath.Join(dir, "out")
	var stdout, stderr bytes.Buffer
	env := map[string]string{envModelKey: "sk-never-written"}
	code := Main([]string{"--run", dir, "--apply-to", cfgPath, "--out", out,
		"--advisor-model", "m", "--advisor-base-url", advisor.URL, "--advisor-price", "1000000:2000000"},
		&stdout, &stderr, func(k string) string { return env[k] })
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Model tries hang") {
		t.Fatalf("stdout:\n%s", stdout.String())
	}
	exp, err := os.ReadFile(filepath.Join(out, "experiment.flags"))
	if err != nil || !strings.Contains(string(exp), "--model-try-timeout=2s\n") || !strings.Contains(string(exp), "--listen=127.0.0.1:8091\n") {
		t.Fatalf("experiment.flags %q %v", exp, err)
	}
	if b, _ := os.ReadFile(cfgPath); strings.Contains(string(b), "try-timeout") {
		t.Fatal("input config modified")
	}
	for _, name := range []string{"findings.json", "findings.md", "proposal.patch", "experiment.flags"} {
		b, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(b), "sk-never-written") {
			t.Fatalf("%s contains the advisor key", name)
		}
	}
	var rep Report
	b, _ := os.ReadFile(filepath.Join(out, "findings.json"))
	if err := json.Unmarshal(b, &rep); err != nil || rep.Schema != SchemaFindings || rep.Advisor == nil || !rep.Advisor.Called || rep.Source.RunID != "before" {
		t.Fatalf("findings.json %v %+v", err, rep.Advisor)
	}
}

func TestMainUsage(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--run", "a", "--server", "b"},
		{"--run", "a", "--config", "x", "--apply-to", "y"},
		{"--run", "a", "--advisor-model", "m"},
		{"--run", "a", "--slow-factor", "0.5"},
		{"--run", "a", "extra"},
	} {
		var out, errb bytes.Buffer
		if code := Main(args, &out, &errb, func(string) string { return "" }); code != ExitUsage {
			t.Errorf("%v: exit %d", args, code)
		}
	}
	var out, errb bytes.Buffer
	if code := Main([]string{"--run", t.TempDir()}, &out, &errb, func(string) string { return "" }); code != ExitError {
		t.Errorf("missing run files: exit %d", code)
	}
	if code := Main([]string{"--server", "http://127.0.0.1:1"}, &out, &errb, func(string) string { return "" }); code != ExitError || !strings.Contains(errb.String(), "no operator token") {
		t.Errorf("no token: exit %d %s", code, errb.String())
	}
}

func TestParseWindow(t *testing.T) {
	w, err := parseWindow("90m", "", t0)
	if err != nil || !w.Since.Equal(t0.Add(-90*time.Minute)) || !w.Until.IsZero() {
		t.Fatalf("%+v %v", w, err)
	}
	w, err = parseWindow("2026-10-10T10:00:00Z", "2026-10-10T11:00:00Z", t0)
	if err != nil || w.Until.Sub(w.Since) != time.Hour {
		t.Fatalf("%+v %v", w, err)
	}
	if _, err := parseWindow("yesterday", "", t0); err == nil {
		t.Fatal("bad since accepted")
	}
}
