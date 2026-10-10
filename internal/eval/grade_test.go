package eval

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func intp(v int) *int           { return &v }
func strp(v string) *string     { return &v }
func floatp(v float64) *float64 { return &v }
func codingTask(e Expect) *Task {
	return &Task{ID: "c", Kind: KindCoding, Prompt: "p", Check: "c", Expect: e}
}
func researchTask(e Expect) *Task { return &Task{ID: "r", Kind: KindResearch, Topic: "t", Expect: e} }

func TestGradeCoding(t *testing.T) {
	art := func(status string, exit *int, stdout string, verdict string, checkerExit int) *CodingArtifact {
		ex := &ExecInfo{Status: status, ExitCode: exit, Stdout: stdout, Signal: 9}
		if verdict != "" {
			ex.Harness = &HarnessVerdict{Verdict: verdict, CheckerExit: checkerExit, TimeoutS: 9,
				Completed: verdict == "pass" || (verdict == "fail" && checkerExit == 3)}
		}
		return &CodingArtifact{Exec: ex}
	}
	ok := func(stdout string) *CodingArtifact { return art("completed", intp(0), stdout, "pass", 0) }
	cases := []struct {
		name   string
		task   *Task
		status string
		art    *CodingArtifact
		want   Outcome
	}{
		{"pass", codingTask(Expect{}), "succeeded", ok(""), Outcome{Verdict: VerdictPass}},
		{"checker failed", codingTask(Expect{}), "succeeded", art("completed", intp(1), "", "fail", 1), Outcome{VerdictFail, "wrong_exit_code"}},
		{"expected nonzero", codingTask(Expect{ExitCode: intp(3)}), "succeeded", art("completed", intp(3), "", "fail", 3), Outcome{Verdict: VerdictPass}},
		{"expected nonzero without token", codingTask(Expect{ExitCode: intp(4)}), "succeeded", art("completed", intp(4), "", "fail", 4), Outcome{VerdictFail, "wrong_exit_code"}},
		{"exec timeout", codingTask(Expect{}), "succeeded", art("timed_out", nil, "", "", 0), Outcome{VerdictFail, "exec_timeout"}},
		{"check timeout", codingTask(Expect{}), "succeeded", art("completed", intp(1), "", "timeout", 1), Outcome{VerdictFail, "check_timeout"}},
		{"unsafe", codingTask(Expect{}), "succeeded", art("completed", intp(1), "", "unsafe", 97), Outcome{VerdictFail, "harness_unsafe"}},
		{"tampered", codingTask(Expect{}), "succeeded", art("completed", intp(0), "", "tampered", 0), Outcome{VerdictFail, "fixtures_tampered"}},
		{"early exit", codingTask(Expect{}), "succeeded", art("completed", intp(1), "", "incomplete", 0), Outcome{VerdictFail, "check_incomplete"}},
		{"exit 0 without verdict", codingTask(Expect{}), "succeeded", art("completed", intp(0), "", "", 0), Outcome{VerdictFail, "no_check_result"}},
		{"signal", codingTask(Expect{}), "succeeded", art("completed", nil, "", "", 0), Outcome{VerdictFail, "exec_signal"}},
		{"unknown", codingTask(Expect{}), "succeeded", art("unknown", nil, "", "", 0), Outcome{VerdictFail, "exec_unknown"}},
		{"no artifact", codingTask(Expect{}), "succeeded", nil, Outcome{VerdictFail, "no_check_result"}},
		{"task failed", codingTask(Expect{}), "failed", nil, Outcome{VerdictFail, "task_failed"}},
		{"equals", codingTask(Expect{StdoutEquals: strp("42")}), "succeeded", ok(" 42\n"), Outcome{Verdict: VerdictPass}},
		{"equals miss", codingTask(Expect{StdoutEquals: strp("42")}), "succeeded", ok("41"), Outcome{VerdictFail, "wrong_output"}},
		{"contains", codingTask(Expect{StdoutContains: []string{"a", "b"}}), "succeeded", ok("b a"), Outcome{Verdict: VerdictPass}},
		{"regex miss", codingTask(Expect{StdoutRegex: `^\d+$`}), "succeeded", ok("x"), Outcome{VerdictFail, "wrong_output"}},
	}
	for _, c := range cases {
		got := Decide(GradeCoding(c.task, c.status, "", c.art))
		if got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
	}
	gs := GradeCoding(codingTask(Expect{}), "failed", "worker_error", nil)
	if Decide(gs).Category != "task_failed:worker_error" {
		t.Fatalf("category %+v", Decide(gs))
	}
}

func TestAnalyzeCitations(t *testing.T) {
	report := "# T\n\n结论 [1][2]，又见 [2] 与 [4]（数据截至 [2024]）。\n\n## 证据\n\n" +
		"- [1] A — http://a — sha256:" + sha1 + "\n" +
		"- [2] B — http://b — sha256:" + sha2 + "\n" +
		"- [3] C — http://c — sha256:" + strings.Repeat("3", 64) + "\n"
	st := AnalyzeCitations(report, map[string]bool{sha1: true})
	if len(st.Cited) != 3 || st.Locatable != 1 || st.Evidence != 3 || len(st.Dangling) != 2 {
		t.Fatalf("stats %+v", st)
	}
	if r := st.Ratio(); r < 0.33 || r > 0.34 {
		t.Fatalf("ratio %v", r)
	}
	if (CitationStats{}).Ratio() != 1 {
		t.Fatal("empty ratio")
	}
	// evidence-list numbers are not counted as body citations
	st = AnalyzeCitations("no cites\n\n## Evidence\n- [1] x sha256:"+sha1, nil)
	if len(st.Cited) != 0 || st.Evidence != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestGradeResearch(t *testing.T) {
	calls := []Call{
		{Endpoint: "/v1/fetch", State: "completed", ResultRef: sha1},
		{Endpoint: "/v1/fetch", State: "failed", ResultRef: sha2},
	}
	rep := "固态电解质 [1] 与 [2]\n## 证据\n- [1] a sha256:" + sha1 + "\n- [2] b sha256:" + sha2 + "\n"
	gs, st := GradeResearch(researchTask(Expect{}), "succeeded", "", rep, calls)
	if Decide(gs).Category != "unlocatable_citations" || st.Locatable != 1 {
		t.Fatalf("%+v %+v", Decide(gs), st)
	}
	gs, _ = GradeResearch(researchTask(Expect{MinLocatableRatio: floatp(0.5), MustMention: []string{"电解质", "钠"}}), "succeeded", "", rep, calls)
	if o := Decide(gs); o.Category != "missing_terms" {
		t.Fatalf("%+v", o)
	}
	gs, _ = GradeResearch(researchTask(Expect{MinCitations: intp(3), MinLocatableRatio: floatp(0)}), "succeeded", "", rep, calls)
	if o := Decide(gs); o.Category != "no_citations" {
		t.Fatalf("%+v", o)
	}
	gs, _ = GradeResearch(researchTask(Expect{}), "succeeded", "", "", nil)
	if o := Decide(gs); o.Category != "no_report" {
		t.Fatalf("%+v", o)
	}
}

func TestJudge(t *testing.T) {
	var n atomic.Int32
	var sawKey atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		sawKey.Store(r.Header.Get("Authorization") == "Bearer judge-key")
		var req struct {
			Model    string `json:"model"`
			Messages []struct{ Content string }
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		score := `{"score": 0.9, "reason": "good"}`
		if strings.Contains(req.Messages[1].Content, "BAD") {
			score = "Sure! ```json\n{\"score\": 0.2, \"reason\": \"weak\"}\n```"
		}
		if strings.Contains(req.Messages[1].Content, "GARBAGE") {
			score = "no json here"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]string{"content": score}}},
			"usage":   map[string]int{"prompt_tokens": 1000, "completion_tokens": 100},
		})
	}))
	defer srv.Close()
	j := &Judge{BaseURL: srv.URL, Model: "judge", APIKey: "judge-key", MaxTokens: 100,
		PriceInMicroPerMTok: 1_000_000, PriceOutMicroPerMTok: 2_000_000, BudgetMicro: 5000}
	task := &Task{ID: "x", Kind: KindCoding, Prompt: "p", Judge: &JudgeSpec{Rubric: "r"}}
	if g := j.Grade(context.Background(), task, "fine answer"); !g.Pass || g.Score != 0.9 || !sawKey.Load() {
		t.Fatalf("grade %+v", g)
	}
	if g := j.Grade(context.Background(), task, "BAD answer"); g.Pass || g.Category != "judge_below_threshold" {
		t.Fatalf("grade %+v", g)
	}
	if g := j.Grade(context.Background(), task, "GARBAGE"); !g.Skipped {
		t.Fatalf("garbage reply should be skipped: %+v", g)
	}
	// each call settles 1000 in + 100×2 out = 1200 µ$; the third (malformed) settled its worst case.
	st := j.Stats()
	if st.Calls != 3 || st.SpentMicro < 3600 {
		t.Fatalf("stats %+v", st)
	}
	before := n.Load()
	g := j.Grade(context.Background(), task, strings.Repeat("long ", 2000))
	if !g.Skipped || n.Load() != before || j.Stats().Skipped != 1 {
		t.Fatalf("budget not enforced: %+v", g)
	}
	if st := j.Stats(); st.Errors != 1 {
		t.Fatalf("the malformed reply must count as an error: %+v", st)
	}
}

// okJudgeServer always answers score 0.9 with fixed usage and counts its calls.
func okJudgeServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]string{"content": `{"score": 0.9, "reason": "ok"}`}}},
			"usage":   map[string]int{"prompt_tokens": 10, "completion_tokens": 10},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// TestJudgeCallLimitAndRequired: the call limit binds even when prices would allow more; with Required a
// judgement that cannot be made fails the task instead of being skipped; errors are counted.
func TestJudgeCallLimitAndRequired(t *testing.T) {
	srv, n := okJudgeServer(t)
	task := &Task{ID: "x", Kind: KindCoding, Prompt: "p", Judge: &JudgeSpec{Rubric: "r"}}
	j := &Judge{BaseURL: srv.URL, Model: "m", PriceInMicroPerMTok: 1, PriceOutMicroPerMTok: 1, BudgetMicro: 1 << 40, MaxCalls: 2}
	for i := range 2 {
		if g := j.Grade(context.Background(), task, "a"); !g.Pass {
			t.Fatalf("call %d: %+v", i, g)
		}
	}
	if g := j.Grade(context.Background(), task, "a"); !g.Skipped || n.Load() != 2 || !strings.Contains(g.Detail, "call limit") {
		t.Fatalf("call limit not enforced: %+v (calls %d)", g, n.Load())
	}
	if st := j.Stats(); st.Calls != 2 || st.Skipped != 1 || st.MaxCalls != 2 {
		t.Fatalf("stats %+v", st)
	}

	req := &Judge{BaseURL: srv.URL, Model: "m", PriceInMicroPerMTok: 1, PriceOutMicroPerMTok: 1, BudgetMicro: 1 << 40, MaxCalls: 1, Required: true}
	_ = req.Grade(context.Background(), task, "a")
	g := req.Grade(context.Background(), task, "a")
	if g.Skipped || g.Pass || g.Category != "judge_unavailable" || Decide([]Grade{g}).Category != "judge_unavailable" {
		t.Fatalf("required judge: %+v", g)
	}
	down := &Judge{BaseURL: "http://127.0.0.1:1", Model: "m", PriceInMicroPerMTok: 1, PriceOutMicroPerMTok: 1, BudgetMicro: 1 << 40, Required: true}
	if g := down.Grade(context.Background(), task, "a"); g.Category != "judge_unavailable" || down.Stats().Errors != 1 {
		t.Fatalf("unreachable required judge: %+v %+v", g, down.Stats())
	}
}
