package eval

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	xs := []int64{50, 10, 40, 20, 30}
	if Percentile(xs, 50) != 30 || Percentile(xs, 95) != 50 || Percentile(xs, 100) != 50 || Percentile(xs, 1) != 10 {
		t.Fatal("nearest rank")
	}
	if Percentile(nil, 50) != 0 {
		t.Fatal("empty")
	}
	var big []int64
	for i := int64(1); i <= 100; i++ {
		big = append(big, i)
	}
	if Percentile(big, 95) != 95 || Percentile(big, 50) != 50 {
		t.Fatal("1..100")
	}
	if xs[0] != 50 {
		t.Fatal("input mutated")
	}
}

func traj(task, kind string, pass bool, cat string, latency, cost int64) *Trajectory {
	o := Outcome{Verdict: VerdictPass}
	if !pass {
		o = Outcome{Verdict: VerdictFail, Category: cat}
	}
	return &Trajectory{TaskID: task, Kind: kind, Outcome: o, LatencyMs: latency,
		Metrics: Metrics{CostMicro: cost, ModelCalls: 1, ToolCalls: 2, Calls: map[string]int{"chat": 1, "fetch": 2}, Tries: 4, Attempts: 1}}
}

func writeRun(t *testing.T, dir string, m *Manifest, trajs []*Trajectory) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), m); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, tr := range trajs {
		j, _ := json.Marshal(tr)
		b.Write(j)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "trajectories.jsonl"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSummarizeAndMarkdown(t *testing.T) {
	start := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	m := &Manifest{RunID: "a", Suite: SuiteRef{Name: "s", SHA256: strings.Repeat("a", 64)}, Agent: "model", Concurrency: 2,
		StartedAt: start, FinishedAt: start.Add(90 * time.Second)}
	trajs := []*Trajectory{
		traj("t1", "coding", true, "", 1000, 10),
		traj("t1", "coding", false, "wrong_output", 3000, 10),
		traj("t2", "research", false, "timeout", 9000, 500),
		{TaskID: "t3", Kind: "coding", Outcome: Outcome{Verdict: VerdictError, Category: "infra_error"}, LatencyMs: 10},
	}
	trajs[2].Citations = &CitationStats{Cited: []int{1, 2}, Locatable: 1}
	trajs[2].Report = "r"
	s := Summarize(m, trajs)
	if s.Runs != 4 || s.Passed != 1 || s.Failed != 2 || s.Errors != 1 || s.SuccessRate != 0.25 {
		t.Fatalf("%+v", s)
	}
	if s.WallMs != 90000 || s.LatencyMs.P50 != 1000 || s.LatencyMs.P95 != 9000 || s.CostMicro != 520 {
		t.Fatalf("lat %+v cost %d", s.LatencyMs, s.CostMicro)
	}
	if s.ExtraTries != 3 || s.Categories["infra_error"] != 1 || s.ByKind["coding"].Runs != 3 || s.Citations.Ratio != 0.5 {
		t.Fatalf("%+v %+v", s.Categories, s.Citations)
	}
	if len(s.ByTask) != 3 || s.ByTask[0].TaskID != "t1" || s.ByTask[0].Passed != 1 {
		t.Fatalf("by task %+v", s.ByTask[0])
	}
	md := RenderMarkdown(s)
	for _, want := range []string{"| Success rate | 25.0% |", "| timeout | 1 |", "| t1 | coding | 1/2 |", "Citations locatable | 1 / 2"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q\n%s", want, md)
		}
	}
	dir := t.TempDir()
	if err := WriteReports(dir, s); err != nil {
		t.Fatal(err)
	}
}

func TestCompare(t *testing.T) {
	root := t.TempDir()
	info := func(rev string) json.RawMessage {
		return json.RawMessage(`{"build":{"vcs.revision":"` + rev + `"},"models":{"declared":["a","b"]}}`)
	}
	ma := &Manifest{RunID: "A", Suite: SuiteRef{Name: "s", SHA256: "h1"}, Agent: "model", Seed: 1, Concurrency: 1, Server: ServerRef{Info: info("r1")}}
	mb := &Manifest{RunID: "B", Suite: SuiteRef{Name: "s", SHA256: "h1"}, Agent: "model", Seed: 1, Concurrency: 4, Server: ServerRef{Info: info("r2")}}
	writeRun(t, filepath.Join(root, "A"), ma, []*Trajectory{
		traj("fixme", "coding", false, "wrong_output", 2000, 100),
		traj("stable", "coding", true, "", 1000, 100),
		traj("breaks", "coding", true, "", 1000, 100),
		traj("gone", "coding", true, "", 1000, 100),
	})
	writeRun(t, filepath.Join(root, "B"), mb, []*Trajectory{
		traj("fixme", "coding", true, "", 1000, 50),
		traj("stable", "coding", true, "", 1000, 50),
		traj("breaks", "coding", false, "exec_timeout", 5000, 50),
		traj("new", "coding", true, "", 1000, 50),
	})
	a, err := LoadRun(filepath.Join(root, "A"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := LoadRun(filepath.Join(root, "B"))
	c := Compare(a, b)
	if !c.SameSuite || c.Counts["fixed"] != 1 || c.Counts["regressed"] != 1 || c.Counts["unchanged"] != 1 || c.Counts["only_a"] != 1 || c.Counts["only_b"] != 1 {
		t.Fatalf("counts %v", c.Counts)
	}
	diffs := strings.Join(c.Differences, "\n")
	if !strings.Contains(diffs, "concurrency: 1 → 4") || !strings.Contains(diffs, "server.build.vcs.revision: r1 → r2") || strings.Contains(diffs, "models.declared") {
		t.Fatalf("diffs %s", diffs)
	}
	var cost MetricDelta
	for _, m := range c.Metrics {
		if m.Name == "cost_per_run" {
			cost = m
		}
	}
	if cost.A != 100 || cost.B != 50 || cost.Relative != -0.5 {
		t.Fatalf("cost %+v", cost)
	}
	md := RenderCompare(c)
	for _, want := range []string{"| fixme | 0/1 | 1/1 |", "| breaks | 1/1 | 0/1 |", "regressed", "| cost_per_run | $0.0001 | $0.0001 |", "| exec_timeout | 0 | 1 |"} {
		if !strings.Contains(md, want) {
			t.Errorf("compare lacks %q\n%s", want, md)
		}
	}
	mb.Suite.SHA256 = "h2"
	writeRun(t, filepath.Join(root, "C"), mb, nil)
	cc, _ := LoadRun(filepath.Join(root, "C"))
	if c2 := Compare(a, cc); c2.SameSuite || !strings.Contains(RenderCompare(c2), "Warning") {
		t.Fatal("suite mismatch not flagged")
	}
	if _, err := LoadRun(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing run loaded")
	}
}
