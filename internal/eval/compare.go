package eval

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Comparison is the result of `agentbox eval compare A B` (B relative to A).
type Comparison struct {
	A, B        string         `json:"-"`
	RunA        string         `json:"run_a"`
	RunB        string         `json:"run_b"`
	SameSuite   bool           `json:"same_suite"`
	Differences []string       `json:"manifest_differences"`
	Metrics     []MetricDelta  `json:"metrics"`
	Tasks       []TaskDelta    `json:"tasks"`
	Categories  []CountDelta   `json:"failure_categories"`
	Counts      map[string]int `json:"task_changes"` // fixed / regressed / unchanged / only_a / only_b
}

// MetricDelta is one metric of both runs.
type MetricDelta struct {
	Name     string  `json:"name"`
	A        float64 `json:"a"`
	B        float64 `json:"b"`
	Delta    float64 `json:"delta"`
	Relative float64 `json:"relative,omitempty"` // (B − A) / A; 0 when A is 0
	Unit     string  `json:"unit"`
	Better   string  `json:"better"` // higher | lower
}

// TaskDelta is the per-task change.
type TaskDelta struct {
	TaskID string  `json:"task_id"`
	PassA  string  `json:"pass_a"`
	PassB  string  `json:"pass_b"`
	RateA  float64 `json:"rate_a"`
	RateB  float64 `json:"rate_b"`
	P50A   int64   `json:"p50_ms_a"`
	P50B   int64   `json:"p50_ms_b"`
	Change string  `json:"change"` // fixed | regressed | improved | worse | unchanged | only_a | only_b
}

// CountDelta is a failure category count in both runs.
type CountDelta struct {
	Category string `json:"category"`
	A        int    `json:"a"`
	B        int    `json:"b"`
}

// Compare compares run B against run A.
func Compare(a, b *LoadedRun) *Comparison {
	sa, sb := a.Summary(), b.Summary()
	c := &Comparison{A: a.Dir, B: b.Dir, RunA: sa.RunID, RunB: sb.RunID,
		SameSuite: a.Manifest.Suite.SHA256 == b.Manifest.Suite.SHA256, Counts: map[string]int{}}
	c.Differences = manifestDiff(a.Manifest, b.Manifest)

	metric := func(name, unit, better string, va, vb float64) {
		d := MetricDelta{Name: name, A: va, B: vb, Delta: vb - va, Unit: unit, Better: better}
		if va != 0 {
			d.Relative = (vb - va) / va
		}
		c.Metrics = append(c.Metrics, d)
	}
	metric("success_rate", "ratio", "higher", sa.SuccessRate, sb.SuccessRate)
	metric("latency_p50", "ms", "lower", float64(sa.LatencyMs.P50), float64(sb.LatencyMs.P50))
	metric("latency_p95", "ms", "lower", float64(sa.LatencyMs.P95), float64(sb.LatencyMs.P95))
	metric("wall_clock", "ms", "lower", float64(sa.WallMs), float64(sb.WallMs))
	metric("cost_per_run", "micro_usd", "lower", perRun(sa.CostMicro, sa.Runs), perRun(sb.CostMicro, sb.Runs))
	metric("model_calls_per_run", "calls", "lower", perRun(int64(sa.ModelCalls), sa.Runs), perRun(int64(sb.ModelCalls), sb.Runs))
	metric("tool_calls_per_run", "calls", "lower", perRun(int64(sa.ToolCalls), sa.Runs), perRun(int64(sb.ToolCalls), sb.Runs))
	metric("extra_tries", "tries", "lower", float64(sa.ExtraTries), float64(sb.ExtraTries))
	if sa.Citations != nil || sb.Citations != nil {
		var ra, rb float64
		if sa.Citations != nil {
			ra = sa.Citations.Ratio
		}
		if sb.Citations != nil {
			rb = sb.Citations.Ratio
		}
		metric("citations_locatable", "ratio", "higher", ra, rb)
	}

	ta, tb := map[string]*TaskAgg{}, map[string]*TaskAgg{}
	ids := map[string]bool{}
	for _, t := range sa.ByTask {
		ta[t.TaskID] = t
		ids[t.TaskID] = true
	}
	for _, t := range sb.ByTask {
		tb[t.TaskID] = t
		ids[t.TaskID] = true
	}
	for _, id := range sortedKeys(ids) {
		x, y := ta[id], tb[id]
		d := TaskDelta{TaskID: id}
		switch {
		case x == nil:
			d.Change = "only_b"
		case y == nil:
			d.Change = "only_a"
		}
		if x != nil {
			d.PassA, d.RateA, d.P50A = fmt.Sprintf("%d/%d", x.Passed, x.Runs), rate(x.Passed, x.Runs), x.LatencyMs.P50
		}
		if y != nil {
			d.PassB, d.RateB, d.P50B = fmt.Sprintf("%d/%d", y.Passed, y.Runs), rate(y.Passed, y.Runs), y.LatencyMs.P50
		}
		if d.Change == "" {
			switch {
			case d.RateA < 1 && d.RateB == 1:
				d.Change = "fixed"
			case d.RateA == 1 && d.RateB < 1:
				d.Change = "regressed"
			case d.RateB > d.RateA:
				d.Change = "improved"
			case d.RateB < d.RateA:
				d.Change = "worse"
			default:
				d.Change = "unchanged"
			}
		}
		c.Counts[d.Change]++
		c.Tasks = append(c.Tasks, d)
	}
	cats := map[string]bool{}
	for k := range sa.Categories {
		cats[k] = true
	}
	for k := range sb.Categories {
		cats[k] = true
	}
	for _, k := range sortedKeys(cats) {
		c.Categories = append(c.Categories, CountDelta{Category: k, A: sa.Categories[k], B: sb.Categories[k]})
	}
	return c
}

func perRun(total int64, runs int) float64 {
	if runs == 0 {
		return 0
	}
	return float64(total) / float64(runs)
}

// manifestDiff lists the pinned fields that differ between two manifests.
func manifestDiff(a, b *Manifest) []string {
	var out []string
	add := func(name, va, vb string) {
		if va != vb {
			out = append(out, fmt.Sprintf("%s: %s → %s", name, orDash(va), orDash(vb)))
		}
	}
	add("suite.sha256", short(a.Suite.SHA256), short(b.Suite.SHA256))
	add("agent", a.Agent, b.Agent)
	add("model", a.Model, b.Model)
	add("seed", fmt.Sprint(a.Seed), fmt.Sprint(b.Seed))
	add("concurrency", fmt.Sprint(a.Concurrency), fmt.Sprint(b.Concurrency))
	add("repetitions", fmt.Sprint(a.Repetitions), fmt.Sprint(b.Repetitions))
	add("workers", strings.Join(a.Workers, ","), strings.Join(b.Workers, ","))
	add("models_used", strings.Join(a.ModelsUsed, ","), strings.Join(b.ModelsUsed, ","))
	add("exec_digests", shortList(a.ExecDigests), shortList(b.ExecDigests))
	ia, ib := flatInfo(a.Server.Info), flatInfo(b.Server.Info)
	keys := map[string]bool{}
	for k := range ia {
		keys[k] = true
	}
	for k := range ib {
		keys[k] = true
	}
	for _, k := range sortedKeys(keys) {
		add("server."+k, ia[k], ib[k])
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "–"
	}
	return s
}

func shortList(xs []string) string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = short(strings.TrimPrefix(x, "sha256:"))
	}
	return strings.Join(out, ",")
}

// flatInfo flattens server-info JSON into dotted keys (arrays joined) for diffing.
func flatInfo(raw json.RawMessage) map[string]string {
	out := map[string]string{}
	if len(bytes.TrimSpace(raw)) == 0 {
		return out
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return out
	}
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, x := range t {
				p := k
				if prefix != "" {
					p = prefix + "." + k
				}
				walk(p, x)
			}
		case []any:
			parts := make([]string, len(t))
			for i, x := range t {
				parts[i] = fmt.Sprint(x)
			}
			out[prefix] = strings.Join(parts, ",")
		default:
			out[prefix] = fmt.Sprint(t)
		}
	}
	walk("", v)
	return out
}

// RenderCompare renders a comparison as Markdown.
func RenderCompare(c *Comparison) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Eval compare: %s → %s\n\n", c.RunA, c.RunB)
	if !c.SameSuite {
		b.WriteString("> **Warning:** the runs used different suites (suite hash differs); task deltas may not be comparable.\n\n")
	}
	if len(c.Differences) > 0 {
		b.WriteString("Manifest differences:\n\n")
		for _, d := range c.Differences {
			fmt.Fprintf(&b, "- %s\n", d)
		}
		b.WriteString("\n")
	}
	b.WriteString("| Metric | A | B | Δ | Δ% |\n|---|---|---|---|---|\n")
	for _, m := range c.Metrics {
		rel := "–"
		if m.A != 0 {
			rel = fmt.Sprintf("%+.1f%%", m.Relative*100)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", m.Name, fmtMetric(m.A, m.Unit), fmtMetric(m.B, m.Unit), signed(m.Delta, m.Unit), rel)
	}
	b.WriteString("\n| Task | A | B | P50 A | P50 B | Change |\n|---|---|---|---|---|---|\n")
	for _, t := range c.Tasks {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", t.TaskID, orDash(t.PassA), orDash(t.PassB), secs(t.P50A), secs(t.P50B), t.Change)
	}
	if len(c.Categories) > 0 {
		b.WriteString("\n| Failure category | A | B |\n|---|---|---|\n")
		for _, k := range c.Categories {
			fmt.Fprintf(&b, "| %s | %d | %d |\n", k.Category, k.A, k.B)
		}
	}
	changes := make([]string, 0, len(c.Counts))
	for _, k := range []string{"fixed", "improved", "regressed", "worse", "unchanged", "only_a", "only_b"} {
		if n := c.Counts[k]; n > 0 {
			changes = append(changes, fmt.Sprintf("%s %d", k, n))
		}
	}
	fmt.Fprintf(&b, "\nTask changes: %s\n", strings.Join(changes, ", "))
	return b.String()
}

func fmtMetric(v float64, unit string) string {
	switch unit {
	case "ratio":
		return fmt.Sprintf("%.1f%%", v*100)
	case "ms":
		return fmt.Sprintf("%.1fs", v/1000)
	case "micro_usd":
		return fmt.Sprintf("$%.4f", v/1e6)
	}
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%.2f", v)
}

func signed(v float64, unit string) string {
	s := fmtMetric(v, unit)
	if v > 0 {
		return "+" + s
	}
	return s
}
