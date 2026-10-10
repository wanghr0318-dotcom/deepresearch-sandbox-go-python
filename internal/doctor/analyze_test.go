package doctor

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

// tb builds a synthetic trajectory.
type tb struct{ tr *eval.Trajectory }

func newTask(id string, rep int, latencyMs int64, pass bool) *tb {
	v, cat := eval.VerdictPass, ""
	if !pass {
		v, cat = eval.VerdictFail, "timeout"
	}
	tr := &eval.Trajectory{Schema: eval.SchemaTrajectory, TaskID: id, Rep: rep, Kind: "coding",
		ServerTaskID: fmt.Sprintf("task_%s_%d", id, rep), LatencyMs: latencyMs, Status: "succeeded",
		Outcome: eval.Outcome{Verdict: v, Category: cat}}
	if !pass {
		tr.Status = "cancelled"
	}
	return &tb{tr}
}

func (b *tb) kind(k string) *tb { b.tr.Kind = k; return b }

func (b *tb) event(src, typ string, at time.Duration, payload string) *tb {
	ev := eval.Event{Source: src, Type: typ, TS: t0.Add(at)}
	if payload != "" {
		ev.Payload = json.RawMessage(payload)
	}
	b.tr.Events = append(b.tr.Events, ev)
	return b
}

func (b *tb) call(endpoint, id, state, fail string, tries ...eval.Try) *tb {
	for i := range tries {
		if tries[i].TryNo == 0 {
			tries[i].TryNo = int64(i + 1)
		}
		if tries[i].State == "" {
			tries[i].State = "settled"
		}
	}
	b.tr.Calls = append(b.tr.Calls, eval.Call{CallID: id, Endpoint: endpoint, State: state, FailReason: fail, Tries: tries})
	return b
}

const (
	epChat  = "/v1/chat/completions"
	epFetch = "/v1/fetch"
	epExec  = "/v1/exec"
)

func ok(provider string, ms int64) eval.Try {
	return eval.Try{Outcome: "ok", LatencyMs: ms, Provider: provider}
}

func ruleIDs(r *Report) []string {
	var ids []string
	for _, f := range r.Findings {
		ids = append(ids, f.Rule)
	}
	return ids
}

func findRule(r *Report, id string) *Finding {
	for i := range r.Findings {
		if r.Findings[i].Rule == id {
			return &r.Findings[i]
		}
	}
	return nil
}

func TestBreakdown(t *testing.T) {
	b := newTask("a", 1, 9000, true).
		event("host", "task_created", 0, "").
		event("host", "attempt_created", 2*time.Second, "").
		event("worker", "ready", 2500*time.Millisecond, "").
		event("host", "control_accepted", 5*time.Second, `{"desired":"cancel"}`).
		event("host", "task_terminal", 8*time.Second, "").
		call(epChat, "c1", "completed", "", eval.Try{Outcome: "retryable", LatencyMs: 100}, eval.Try{Outcome: "retryable", LatencyMs: 100}, ok("", 300)).
		call(epFetch, "f1", "completed", "", ok("", 40)).
		call(epExec, "e1", "completed", "", ok("", 700))
	bd, starts := breakdown(b.tr)
	want := Breakdown{QueueMs: 2000, StartMs: 500, ModelMs: 500, FetchMs: 40, ExecMs: 700, BackoffEstMs: 2000 + 4000, StopMs: 3000}
	if bd != want || len(starts) != 1 || starts[0] != 500 {
		t.Fatalf("breakdown %+v starts %v, want %+v", bd, starts, want)
	}
}

func TestBackoffEstimateChain(t *testing.T) {
	// A → B (immediate failover) → A (new round: backoff 2 s) ; hedge legs never add backoff.
	c := eval.Call{Tries: []eval.Try{{TryNo: 1, Provider: "a"}, {TryNo: 2, Provider: "b"}, {TryNo: 3, Provider: "b", Hedge: true}, {TryNo: 4, Provider: "a"}}}
	if got := backoffEstimate(c); got != 2000 {
		t.Fatalf("backoff = %d", got)
	}
}

// hangRun: 8 coding tasks; every 4th model call hangs on the primary for 40 s and the task times out.
func hangRun() []*eval.Trajectory {
	var trs []*eval.Trajectory
	for i := 1; i <= 8; i++ {
		if i%4 == 0 {
			trs = append(trs, newTask("add", i, 40_500, false).
				call(epChat, "solve/chat/1", "in_progress", "", eval.Try{Outcome: "unknown", LatencyMs: 40_000, Provider: "primary", Error: "cancel_requested"}).tr)
			continue
		}
		trs = append(trs, newTask("add", i, 900, true).
			call(epChat, "solve/chat/1", "completed", "", ok("primary", 300+int64(i))).
			call(epExec, "check/exec/1", "completed", "", ok("", 400)).tr)
	}
	return trs
}

func TestModelHangRuleProposesTryTimeout(t *testing.T) {
	cfg, _ := ParseFlags([]byte("--model-fallback-file=/x.json\n--call-deadline=1s\n"))
	r := Analyze(Source{Kind: "eval_run"}, hangRun(), Options{Config: cfg, Now: func() time.Time { return t0 }})
	f := findRule(r, "model_try_hang")
	if f == nil {
		t.Fatalf("no model_try_hang finding: %v", ruleIDs(r))
	}
	if f.Severity != SevCritical || f.Failed != 2 || f.Evidence.Counts["hung_tries"] != 2 || f.Evidence.Counts["hung_tries.primary"] != 2 {
		t.Fatalf("finding %+v", f)
	}
	if len(f.Proposals) != 1 || f.Proposals[0].Flag != "--model-try-timeout" || f.Proposals[0].To != "2s" || f.Proposals[0].From != "default 0s" {
		t.Fatalf("proposals %+v", f.Proposals)
	}
	if len(f.Evidence.Examples) != 2 || f.Evidence.Examples[0].ServerTaskID != "task_add_4" || f.Evidence.Examples[0].CallID != "solve/chat/1" {
		t.Fatalf("examples %+v", f.Evidence.Examples)
	}
	if r.Totals.Failed != 2 || r.Totals.Explained != 2 || len(r.Causes) != 1 || r.Causes[0].Cause != "model_try_hang" {
		t.Fatalf("totals %+v causes %+v", r.Totals, r.Causes)
	}
	// The hung tries are not double-counted as provider errors.
	if findRule(r, "model_upstream_errors") != nil {
		t.Fatalf("hangs counted as provider errors: %v", ruleIDs(r))
	}
	md := RenderMarkdown(r)
	for _, want := range []string{"Model tries hang", "`--model-try-timeout`: default 0s → **2s**", "task_add_4"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q:\n%s", want, md)
		}
	}
}

func TestModelHangSingleProviderIsAdvisory(t *testing.T) {
	trs := hangRun()
	for _, tr := range trs {
		for i := range tr.Calls {
			for j := range tr.Calls[i].Tries {
				tr.Calls[i].Tries[j].Provider = ""
			}
		}
	}
	r := Analyze(Source{}, trs, Options{})
	f := findRule(r, "model_try_hang")
	if f == nil || len(f.Proposals) != 1 || f.Proposals[0].Kind != KindAdvisory {
		t.Fatalf("finding %+v", f)
	}
	if len(r.Notes) == 0 || !strings.Contains(r.Notes[0], "single provider") {
		t.Fatalf("notes %v", r.Notes)
	}
}

func TestFetchDeadlineRule(t *testing.T) {
	// As observed on a real server: a fetch cut by the call deadline settles unknown/upstream_unconfirmed just
	// below the deadline, and the call stays unknown without a fail reason.
	cutTry := func(ms int64) eval.Try {
		return eval.Try{Outcome: "unknown", LatencyMs: ms, Error: "upstream_unconfirmed"}
	}
	var trs []*eval.Trajectory
	for i := 1; i <= 4; i++ {
		trs = append(trs, newTask("solid", i, 6000, false).kind("research").
			call(epFetch, "task-1/fetch/1", "unknown", "", cutTry(985+int64(i))).
			call(epFetch, "task-1/fetch/2", "unknown", "", cutTry(990)).tr)
	}
	trs = append(trs, newTask("solid", 5, 3000, true).kind("research").call(epFetch, "task-1/fetch/1", "completed", "", ok("", 900)).tr)
	cfg, _ := ParseFlags([]byte("--call-deadline=1s\n"))
	r := Analyze(Source{}, trs, Options{Config: cfg})
	f := findRule(r, "fetch_deadline")
	if f == nil || f.Failed != 4 || f.Evidence.Counts["calls_deadline.fetch"] != 8 || f.Evidence.LatencyMs["cut_try_max"] != 990 {
		t.Fatalf("finding %+v (%v)", f, ruleIDs(r))
	}
	if len(f.Proposals) != 1 || f.Proposals[0].To != "10s" || f.Proposals[0].From != "1s" {
		t.Fatalf("proposals %+v", f.Proposals)
	}
	// Without the config the deadline is inferred from the cluster of cut tries.
	r = Analyze(Source{}, trs, Options{})
	if p := findRule(r, "fetch_deadline").Proposals[0]; p.To != "10s" || !strings.Contains(p.From, "inferred") {
		t.Fatalf("proposal without config %+v", p)
	}
	// With a generous configured deadline, the same short failures are not deadline cuts.
	cfg, _ = ParseFlags([]byte("--call-deadline=2m\n"))
	if f := findRule(Analyze(Source{}, trs, Options{Config: cfg}), "fetch_deadline"); f != nil {
		t.Fatalf("fired with a 2m deadline: %+v", f)
	}
	// Scattered failure latencies are not a deadline signature.
	var scattered []*eval.Trajectory
	for i, ms := range []int64{100, 400, 990} {
		scattered = append(scattered, newTask("s", i+1, 2000, false).call(epFetch, "f", "unknown", "", cutTry(ms)).tr)
	}
	if f := findRule(Analyze(Source{}, scattered, Options{}), "fetch_deadline"); f != nil {
		t.Fatalf("fired on scattered failures: %+v", f)
	}
	// The explicit code is recognised too.
	coded := []*eval.Trajectory{newTask("c", 1, 2000, false).call(epFetch, "f", "failed", codeCallDeadline).tr}
	if f := findRule(Analyze(Source{}, coded, Options{}), "fetch_deadline"); f == nil || f.Failed != 1 {
		t.Fatalf("coded deadline: %+v", f)
	}
}

func TestModelErrorsAndBreaker(t *testing.T) {
	var trs []*eval.Trajectory
	for i := 1; i <= 6; i++ {
		trs = append(trs, newTask("x", i, 800, true).
			call(epChat, "c", "completed", "", eval.Try{Outcome: "retryable", Error: "upstream_unavailable", Provider: "primary", LatencyMs: 50}, ok("backup", 200)).tr)
	}
	trs = append(trs, newTask("x", 7, 100, false).call(epChat, "c", "failed", codeModelDegraded).tr)
	trs[0].Calls[0].Tries[1].Skipped = "primary:circuit_open"
	cfg, _ := ParseFlags([]byte("--model-breaker-failures=5\n"))
	r := Analyze(Source{}, trs, Options{Config: cfg})
	br := findRule(r, "breaker_degraded")
	if br == nil || br.Failed != 1 || br.Evidence.Counts["skipped_circuit_open.primary"] != 1 {
		t.Fatalf("breaker %+v", br)
	}
	me := findRule(r, "model_upstream_errors")
	if me == nil || me.Severity != SevInfo || me.Evidence.Shares["error_rate.primary"] != 1 {
		t.Fatalf("errors %+v", me)
	}
	var flags []string
	for _, p := range me.Proposals {
		flags = append(flags, p.Flag+p.Text)
	}
	if len(me.Proposals) != 2 || me.Proposals[0].Flag != "--model-breaker-failures" || me.Proposals[0].To != "2" {
		t.Fatalf("proposals %v", flags)
	}
}

func TestOtherRules(t *testing.T) {
	in := []*eval.Trajectory{
		newTask("tb", 1, 2000, false).call("/v1/search", "s", "failed", codeToolBudget).tr,
		newTask("cb", 1, 2000, false).call(epChat, "c", "failed", codeBudgetShort).tr,
		newTask("ex", 1, 2000, false).call(epExec, "e", "failed", "", eval.Try{Outcome: "retryable", Error: codeExecQueueTimeout}).tr,
		newTask("fb", 1, 2000, false).call(epFetch, "f", "failed", "upstream_rejected", eval.Try{Outcome: "fatal", Error: "upstream_rejected"}).tr,
		// slow because of a slow sandbox start
		newTask("st", 1, 20_000, true).event("host", "attempt_created", 0, "").event("worker", "ready", 15*time.Second, "").tr,
		newTask("st", 2, 1500, true).tr,
		newTask("st", 3, 1500, true).tr,
	}
	r := Analyze(Source{}, in, Options{})
	for _, id := range []string{"tool_budget_exhausted", "cost_budget_exhausted", "exec_timeout", "fetch_blocked", "sandbox_start"} {
		f := findRule(r, id)
		if f == nil || f.Failed+f.Slow != 1 {
			t.Errorf("%s: %+v (rules %v)", id, f, ruleIDs(r))
		}
	}
	if r.Totals.Failed != 4 || r.Totals.Slow != 1 || r.Totals.Explained != 5 {
		t.Fatalf("totals %+v", r.Totals)
	}
	if p := findRule(r, "tool_budget_exhausted").Proposals[0]; p.Flag != "--turn-tool-budget" || p.To != "45" {
		t.Fatalf("tool budget proposal %+v", p)
	}
}

func TestHedgeWasteAndUnexplained(t *testing.T) {
	var trs []*eval.Trajectory
	for i := 1; i <= 4; i++ {
		trs = append(trs, newTask("h", i, 900, true).call(epChat, "c", "completed", "",
			ok("primary", 800), eval.Try{Outcome: "unknown", Provider: "backup", Hedge: true, HedgeLost: true, LatencyMs: 300}).tr)
	}
	wrong := newTask("w", 1, 900, false).tr
	wrong.Outcome.Category = "wrong_output"
	trs = append(trs, wrong)
	cfg, _ := ParseFlags([]byte("--model-hedge-delay=500ms\n"))
	r := Analyze(Source{}, trs, Options{Config: cfg})
	h := findRule(r, "hedge_waste")
	if h == nil || h.Evidence.Counts["hedge_legs"] != 4 || h.Evidence.Counts["hedge_wins"] != 0 || h.Proposals[0].To != "1s" {
		t.Fatalf("hedge %+v", h)
	}
	if len(r.Unexplained) != 1 || r.Unexplained[0].Category != "wrong_output" || r.Totals.Explained != 0 {
		t.Fatalf("unexplained %+v totals %+v", r.Unexplained, r.Totals)
	}
}

func TestHealthyRunHasNoFindings(t *testing.T) {
	var trs []*eval.Trajectory
	for i := 1; i <= 5; i++ {
		trs = append(trs, newTask("ok", i, 1000+int64(i)*10, true).call(epChat, "c", "completed", "", ok("", 200)).tr)
	}
	r := Analyze(Source{}, trs, Options{})
	if len(r.Findings) != 0 || r.Totals.Failed != 0 || r.Totals.Slow != 0 || len(r.Proposals) != 0 {
		t.Fatalf("report %+v", r)
	}
	if !strings.Contains(RenderMarkdown(r), "No rule fired.") {
		t.Fatal("markdown")
	}
}

// TestDoctorSuite: the demo suite parses, and every coding task has a scripted (correct) fake reply, so failures in
// a zero-cost run are infrastructure failures.
func TestDoctorSuite(t *testing.T) {
	s, err := eval.LoadSuite("../../eval/suites/doctor.yaml")
	if err != nil {
		t.Fatal(err)
	}
	coding := 0
	for _, task := range s.Tasks {
		if task.Kind == eval.KindCoding {
			coding++
			if task.FakeReply == "" || task.Reference == "" {
				t.Errorf("%s: needs fake_reply and reference", task.ID)
			}
		}
	}
	if coding != 6 || len(s.Tasks) != 8 {
		t.Fatalf("tasks %d coding %d", len(s.Tasks), coding)
	}
}
