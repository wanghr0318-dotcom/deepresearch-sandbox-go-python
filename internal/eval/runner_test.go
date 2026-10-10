package eval

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const sha1 = "1111111111111111111111111111111111111111111111111111111111111111"
const sha2 = "2222222222222222222222222222222222222222222222222222222222222222"

const runnerSuite = `
name: rs
defaults: {timeout: 5s, limits: {budget_micro: 1000}}
tasks:
  - id: ok
    kind: coding
    prompt: p
    reference: "def f(): pass"
    check: c
    expect: {stdout_contains: ["PASS"]}
  - id: bug
    kind: coding
    prompt: p
    reference: "def f(): pass"
    check: c
  - id: slow
    kind: coding
    prompt: p
    reference: "def f(): pass"
    check: c
    timeout: 300ms
  - id: res
    kind: research
    topic: "固态电池"
    research: {max_tasks: 2}
    expect: {must_mention: ["电解质"]}
`

func codingArt(exit int, stdout string) []byte {
	verdict := "pass"
	if exit != 0 {
		verdict = "fail"
	}
	b, _ := json.Marshal(CodingArtifact{Schema: "agentbox.eval.coding/v1", Agent: "reference",
		Exec: &ExecInfo{Status: "completed", ExitCode: &exit, Stdout: stdout, ImageDigest: "sha256:abcdef0123456789",
			Harness: &HarnessVerdict{Verdict: verdict, CheckerExit: exit, Completed: exit == 0}}})
	return b
}

func testPlan(spec map[string]any) plan {
	cost := &Budget{LimitMicro: 1000, SpentMicro: 120}
	chat := Call{CallID: "root/solve/chat/1", Endpoint: "/v1/chat/completions", Model: "fake-model", State: "completed", TriesUsed: 1,
		CostChargedMicro: 100, Tries: []Try{ // S4 fallback chain: a hedged try on the backup route wins
			{TryNo: 1, State: "settled", LatencyMs: 40, Provider: "primary", HedgeLost: true},
			{TryNo: 2, State: "settled", LatencyMs: 30, Provider: "backup", Hedge: true, Skipped: "primary:hedged"}}}
	exec := Call{CallID: "root/check/exec/1", Endpoint: "/v1/exec", State: "completed", TriesUsed: 2,
		Tries: []Try{{TryNo: 1, State: "failed"}, {TryNo: 2, State: "completed", LatencyMs: 300}}}
	switch specTaskID(spec) {
	case "ok":
		return plan{status: "succeeded", delay: 20 * time.Millisecond, cutOnce: true, budget: cost, calls: []Call{chat, exec},
			artifacts: map[string][]byte{"eval": codingArt(0, "PASS 3/3\n"), "solution": []byte("def f(): pass")}}
	case "bug":
		return plan{status: "succeeded", delay: 10 * time.Millisecond, budget: cost, calls: []Call{chat, exec},
			artifacts: map[string][]byte{"eval": codingArt(1, "AssertionError\n"), "solution": []byte("x")}}
	case "slow":
		return plan{status: "succeeded", delay: time.Hour}
	}
	report := "## 核心洞见\n电解质路线 [1][2]，以及 [3]。\n\n## 证据\n\n- [1] a — http://x/1 — sha256:" + sha1 +
		"\n- [2] b — http://x/2 — sha256:" + sha2 + "\n"
	return plan{status: "succeeded", delay: 10 * time.Millisecond, budget: &Budget{SpentMicro: 500},
		calls: []Call{
			{CallID: "root/plan/chat/1", Endpoint: "/v1/chat/completions", Model: "kimi", State: "completed", TriesUsed: 1},
			{CallID: "root/t1/search/1", Endpoint: "/v1/search", State: "completed", TriesUsed: 1},
			{CallID: "root/t1/fetch/1", Endpoint: "/v1/fetch", State: "completed", TriesUsed: 1, ResultRef: sha1},
			{CallID: "root/t1/fetch/2", Endpoint: "/v1/fetch", State: "completed", TriesUsed: 1, ResultRef: sha2},
		},
		artifacts: map[string][]byte{"report": []byte(report)}}
}

func TestRunEndToEndAgainstFakeServer(t *testing.T) {
	fs := newFakeServer(t, testPlan)
	fs.fail503 = 1 // the first create is retried
	s, err := ParseSuite([]byte(runnerSuite), true)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	var log strings.Builder
	dir, sum, err := Run(context.Background(), Options{Suite: s, SuitePath: "eval/suites/rs.yaml", Client: fs.client(),
		OutDir: out, RunID: "r1", Concurrency: 2, Repetitions: 2, Seed: 7, Agent: AgentReference, Log: &log})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Runs != 8 || sum.Passed != 2 {
		t.Fatalf("runs %d passed %d; categories %v\n%s", sum.Runs, sum.Passed, sum.Categories, log.String())
	}
	// research: citation [3] has no evidence → unlocatable (default ratio 1.0)
	if sum.Categories["wrong_exit_code"] != 2 || sum.Categories["timeout"] != 2 || sum.Categories["unlocatable_citations"] != 2 {
		t.Fatalf("categories %v", sum.Categories)
	}
	if fs.maxRun > 2 {
		t.Fatalf("concurrency exceeded: %d", fs.maxRun)
	}
	if sum.ExtraTries != 4 { // ok+bug × 2 reps, one extra exec try each
		t.Fatalf("extra tries %d", sum.ExtraTries)
	}
	for _, f := range []string{"manifest.json", "trajectories.jsonl", "summary.json", "report.md"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "trajectories.jsonl"))
	if strings.Contains(string(raw), fs.token) {
		t.Fatal("token leaked into trajectories")
	}
	lr, err := LoadRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(lr.Trajectories) != 8 {
		t.Fatalf("trajectories %d", len(lr.Trajectories))
	}
	m := lr.Manifest
	if m.Suite.SHA256 == "" || len(m.Server.Info) == 0 || len(m.Workers) != 1 || m.Workers[0] != "evalworker@0.1.0" {
		t.Fatalf("manifest %+v", m)
	}
	if len(m.ExecDigests) != 1 || len(m.ModelsUsed) != 2 {
		t.Fatalf("provenance %v %v", m.ExecDigests, m.ModelsUsed)
	}
	if strings.Join(m.ProvidersUsed, ",") != "backup,primary" {
		t.Fatalf("providers used %v", m.ProvidersUsed)
	}
	for _, tr := range lr.Trajectories {
		if tr.TaskID == "ok" && tr.Outcome.Verdict == VerdictPass {
			if len(tr.Events) != 3 || tr.Events[2].Type != "task_terminal" { // reconnect deduplicated
				t.Fatalf("events %+v", tr.Events)
			}
			if tr.Metrics.ExecCalls != 1 || tr.Metrics.ModelCalls != 1 || tr.Metrics.CostMicro != 120 || tr.Metrics.HedgeTries != 1 ||
				tr.Calls[0].Tries[1].Provider != "backup" {
				t.Fatalf("metrics %+v", tr.Metrics)
			}
		}
		if tr.TaskID == "slow" && tr.Status != "cancelled" {
			t.Fatalf("slow task status %s", tr.Status)
		}
		if tr.TaskID == "res" && (tr.Citations == nil || tr.Citations.Locatable != 2 || len(tr.Citations.Cited) != 3) {
			t.Fatalf("citations %+v", tr.Citations)
		}
	}
	// the reference solution is sent only with --agent reference; requests are idempotent per (run, task, rep)
	if len(fs.tasks) != 8 {
		t.Fatalf("server tasks %d", len(fs.tasks))
	}
	for _, ft := range fs.tasks {
		ev := ft.spec["eval"].(map[string]any)
		if ev["kind"] == KindCoding && ev["reference"] == nil {
			t.Fatal("reference missing in reference mode")
		}
		if ev["kind"] == KindResearch && (ft.spec["topic"] != "固态电池" || ft.spec["max_tasks"] != float64(2)) {
			t.Fatalf("research spec %v", ft.spec)
		}
		if !strings.Contains(string(ft.limits), "1000") {
			t.Fatalf("limits %s", ft.limits)
		}
	}
}

func TestRunSeedOrderReproducible(t *testing.T) {
	order := func(seed int64) []string {
		fs := newFakeServer(t, func(map[string]any) plan { return plan{status: "succeeded"} })
		s, _ := ParseSuite([]byte(runnerSuite), true)
		s.Tasks = s.Tasks[:2]
		_, _, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), RunID: "o",
			Repetitions: 3, Seed: seed, Agent: AgentModel})
		if err != nil {
			t.Fatal(err)
		}
		return fs.order
	}
	a, b, c := order(5), order(5), order(6)
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatalf("same seed, different order: %v %v", a, b)
	}
	if strings.Join(a, ",") == strings.Join(c, ",") {
		t.Logf("seeds 5 and 6 gave the same order (possible but unlikely): %v", a)
	}
}

func TestRunModelAgentOmitsReferenceAndPassesModel(t *testing.T) {
	fs := newFakeServer(t, func(map[string]any) plan { return plan{status: "failed", reason: "worker_error"} })
	s, _ := ParseSuite([]byte(runnerSuite), true)
	_, sum, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), RunID: "m",
		Tasks: []string{"ok", "res"}, Agent: AgentModel, Model: "kimi-k2.6"})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Runs != 2 || sum.Categories["task_failed:worker_error"] != 2 {
		t.Fatalf("summary %+v", sum.Categories)
	}
	for _, ft := range fs.tasks {
		ev := ft.spec["eval"].(map[string]any)
		if ev["reference"] != nil {
			t.Fatal("reference leaked to the model agent")
		}
		if ev["kind"] == KindCoding && ev["model"] != "kimi-k2.6" {
			t.Fatalf("model %v", ev["model"])
		}
		if ev["kind"] == KindResearch && ft.spec["orchestrator_model"] != "kimi-k2.6" {
			t.Fatalf("research models %v", ft.spec)
		}
	}
}

func TestRunErrors(t *testing.T) {
	fs := newFakeServer(t, func(map[string]any) plan { return plan{status: "succeeded"} })
	s, _ := ParseSuite([]byte(runnerSuite), true)
	if _, _, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), Tasks: []string{"nope"}}); err == nil {
		t.Fatal("unknown task accepted")
	}
	if _, _, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), Agent: "oracle"}); err == nil {
		t.Fatal("unknown agent accepted")
	}
	bad := fs.client()
	bad.Token = "wrong"
	if _, _, err := Run(context.Background(), Options{Suite: s, Client: bad, OutDir: t.TempDir()}); err == nil || strings.Contains(err.Error(), "wrong") {
		t.Fatalf("unauthorized: %v", err)
	}
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, "dup"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: out, RunID: "dup"}); err == nil {
		t.Fatal("existing run dir accepted")
	}
	fs.noInfo = true
	dir, _, err := Run(context.Background(), Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), RunID: "ni", Tasks: []string{"ok"}})
	if err != nil {
		t.Fatal(err)
	}
	lr, _ := LoadRun(dir)
	if lr.Manifest.Server.Note == "" {
		t.Fatal("missing server-info note")
	}
}

func TestRunInterruptedCancelsServerTasks(t *testing.T) {
	fs := newFakeServer(t, func(map[string]any) plan { return plan{status: "succeeded", delay: time.Hour} })
	s, _ := ParseSuite([]byte(runnerSuite), true)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	dir, _, err := Run(ctx, Options{Suite: s, Client: fs.client(), OutDir: t.TempDir(), RunID: "int", Tasks: []string{"ok"}, Concurrency: 1})
	if err == nil {
		t.Fatal("interrupted run returned nil error")
	}
	lr, lerr := LoadRun(dir)
	if lerr != nil || len(lr.Trajectories) != 1 || lr.Trajectories[0].Status != "cancelled" {
		t.Fatalf("partial run: %v %+v", lerr, lr)
	}
}
