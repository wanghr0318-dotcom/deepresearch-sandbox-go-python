package eval

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Summary is summary.json: the aggregate of one run.
type Summary struct {
	Schema      string    `json:"schema"`
	RunID       string    `json:"run_id"`
	Suite       string    `json:"suite"`
	SuiteSHA256 string    `json:"suite_sha256"`
	Agent       string    `json:"agent"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	WallMs      int64     `json:"wall_ms"`
	Concurrency int       `json:"concurrency"`

	Runs        int     `json:"runs"`
	Passed      int     `json:"passed"`
	Failed      int     `json:"failed"`
	Errors      int     `json:"errors"`
	SuccessRate float64 `json:"success_rate"`

	LatencyMs  Percentiles      `json:"latency_ms"`
	CostMicro  int64            `json:"cost_micro"`
	ModelCalls int              `json:"model_calls"`
	ToolCalls  int              `json:"tool_calls"`
	ExecCalls  int              `json:"exec_calls"`
	Attempts   int64            `json:"attempts"`
	ExtraTries int64            `json:"extra_tries"` // tries beyond one per call (Gateway retries)
	Categories map[string]int   `json:"failure_categories"`
	ByKind     map[string]*Agg  `json:"by_kind"`
	ByTask     []*TaskAgg       `json:"by_task"`
	Judge      *JudgeStats      `json:"judge,omitempty"`
	Citations  *CitationSummary `json:"citations,omitempty"`
}

// Percentiles of a sample (nearest-rank).
type Percentiles struct {
	P50  int64 `json:"p50"`
	P95  int64 `json:"p95"`
	Max  int64 `json:"max"`
	Mean int64 `json:"mean"`
}

// Agg is an aggregate over a group of runs.
type Agg struct {
	Runs        int         `json:"runs"`
	Passed      int         `json:"passed"`
	SuccessRate float64     `json:"success_rate"`
	LatencyMs   Percentiles `json:"latency_ms"`
	CostMicro   int64       `json:"cost_micro"`
}

// TaskAgg is the aggregate of one suite task across repetitions.
type TaskAgg struct {
	TaskID     string         `json:"task_id"`
	Kind       string         `json:"kind"`
	Runs       int            `json:"runs"`
	Passed     int            `json:"passed"`
	LatencyMs  Percentiles    `json:"latency_ms"`
	CostMicro  int64          `json:"cost_micro"`
	ToolCalls  int            `json:"tool_calls"`
	Categories map[string]int `json:"failure_categories,omitempty"`
}

// CitationSummary aggregates research citations.
type CitationSummary struct {
	Reports   int     `json:"reports"`
	Cited     int     `json:"cited"`
	Locatable int     `json:"locatable"`
	Ratio     float64 `json:"locatable_ratio"`
}

// Percentile returns the nearest-rank p-th percentile (0 < p ≤ 100) of xs.
func Percentile(xs []int64, p float64) int64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]int64(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(math.Ceil(p / 100 * float64(len(s))))
	rank = max(1, min(rank, len(s)))
	return s[rank-1]
}

func percentiles(xs []int64) Percentiles {
	if len(xs) == 0 {
		return Percentiles{}
	}
	var sum int64
	for _, x := range xs {
		sum += x
	}
	return Percentiles{P50: Percentile(xs, 50), P95: Percentile(xs, 95), Max: Percentile(xs, 100), Mean: sum / int64(len(xs))}
}

func rate(passed, runs int) float64 {
	if runs == 0 {
		return 0
	}
	return math.Round(float64(passed)/float64(runs)*10000) / 10000
}

// Summarize aggregates trajectories.
func Summarize(m *Manifest, trajs []*Trajectory) *Summary {
	s := &Summary{Schema: SchemaSummary, RunID: m.RunID, Suite: m.Suite.Name, SuiteSHA256: m.Suite.SHA256, Agent: m.Agent,
		StartedAt: m.StartedAt, FinishedAt: m.FinishedAt, Concurrency: m.Concurrency,
		Categories: map[string]int{}, ByKind: map[string]*Agg{}}
	if !m.FinishedAt.IsZero() {
		s.WallMs = m.FinishedAt.Sub(m.StartedAt).Milliseconds()
	}
	var all []int64
	kindLat := map[string][]int64{}
	tasks := map[string]*TaskAgg{}
	taskLat := map[string][]int64{}
	var cites CitationSummary
	for _, t := range trajs {
		s.Runs++
		pass := t.Outcome.Verdict == VerdictPass
		switch t.Outcome.Verdict {
		case VerdictPass:
			s.Passed++
		case VerdictFail:
			s.Failed++
		default:
			s.Errors++
		}
		if !pass {
			s.Categories[t.Outcome.Category]++
		}
		all = append(all, t.LatencyMs)
		s.CostMicro += t.Metrics.CostMicro
		s.ModelCalls += t.Metrics.ModelCalls
		s.ToolCalls += t.Metrics.ToolCalls
		s.ExecCalls += t.Metrics.ExecCalls
		s.Attempts += t.Metrics.Attempts
		calls := 0
		for _, n := range t.Metrics.Calls {
			calls += n
		}
		if extra := t.Metrics.Tries - int64(calls); extra > 0 {
			s.ExtraTries += extra
		}

		k := s.ByKind[t.Kind]
		if k == nil {
			k = &Agg{}
			s.ByKind[t.Kind] = k
		}
		k.Runs++
		k.CostMicro += t.Metrics.CostMicro
		kindLat[t.Kind] = append(kindLat[t.Kind], t.LatencyMs)

		ta := tasks[t.TaskID]
		if ta == nil {
			ta = &TaskAgg{TaskID: t.TaskID, Kind: t.Kind}
			tasks[t.TaskID] = ta
		}
		ta.Runs++
		ta.CostMicro += t.Metrics.CostMicro
		ta.ToolCalls += t.Metrics.ToolCalls
		taskLat[t.TaskID] = append(taskLat[t.TaskID], t.LatencyMs)
		if pass {
			k.Passed++
			ta.Passed++
		} else {
			if ta.Categories == nil {
				ta.Categories = map[string]int{}
			}
			ta.Categories[t.Outcome.Category]++
		}
		if t.Citations != nil && t.Report != "" {
			cites.Reports++
			cites.Cited += len(t.Citations.Cited)
			cites.Locatable += t.Citations.Locatable
		}
	}
	s.SuccessRate = rate(s.Passed, s.Runs)
	s.LatencyMs = percentiles(all)
	for kind, k := range s.ByKind {
		k.SuccessRate = rate(k.Passed, k.Runs)
		k.LatencyMs = percentiles(kindLat[kind])
	}
	for id, ta := range tasks {
		ta.LatencyMs = percentiles(taskLat[id])
		s.ByTask = append(s.ByTask, ta)
	}
	sort.Slice(s.ByTask, func(i, j int) bool {
		if s.ByTask[i].Kind != s.ByTask[j].Kind {
			return s.ByTask[i].Kind < s.ByTask[j].Kind
		}
		return s.ByTask[i].TaskID < s.ByTask[j].TaskID
	})
	if cites.Reports > 0 {
		cites.Ratio = 1
		if cites.Cited > 0 {
			cites.Ratio = math.Round(float64(cites.Locatable)/float64(cites.Cited)*10000) / 10000
		}
		s.Citations = &cites
	}
	return s
}

// WriteReports writes summary.json and report.md into dir.
func WriteReports(dir string, s *Summary) error {
	if err := writeJSONFile(filepath.Join(dir, "summary.json"), s); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(RenderMarkdown(s)), 0o644); err != nil {
		return fmt.Errorf("eval: %w", err)
	}
	return nil
}

func usd(micro int64) string { return fmt.Sprintf("$%.4f", float64(micro)/1e6) }

func secs(ms int64) string { return fmt.Sprintf("%.1fs", float64(ms)/1000) }

func pct(r float64) string { return fmt.Sprintf("%.1f%%", r*100) }

// RenderMarkdown renders a summary as a Markdown report.
func RenderMarkdown(s *Summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Eval run %s\n\n", s.RunID)
	fmt.Fprintf(&b, "- Suite: `%s` (sha256 `%s`)\n", s.Suite, short(s.SuiteSHA256))
	fmt.Fprintf(&b, "- Agent: %s; concurrency %d; wall %s\n\n", s.Agent, s.Concurrency, secs(s.WallMs))
	b.WriteString("| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Runs | %d (pass %d, fail %d, error %d) |\n", s.Runs, s.Passed, s.Failed, s.Errors)
	fmt.Fprintf(&b, "| Success rate | %s |\n", pct(s.SuccessRate))
	fmt.Fprintf(&b, "| Latency P50 / P95 / max | %s / %s / %s |\n", secs(s.LatencyMs.P50), secs(s.LatencyMs.P95), secs(s.LatencyMs.Max))
	fmt.Fprintf(&b, "| Cost (ledger) | %s |\n", usd(s.CostMicro))
	fmt.Fprintf(&b, "| Model / tool / exec calls | %d / %d / %d |\n", s.ModelCalls, s.ToolCalls, s.ExecCalls)
	fmt.Fprintf(&b, "| Attempts / extra tries | %d / %d |\n", s.Attempts, s.ExtraTries)
	if s.Citations != nil {
		fmt.Fprintf(&b, "| Citations locatable | %d / %d (%s) |\n", s.Citations.Locatable, s.Citations.Cited, pct(s.Citations.Ratio))
	}
	if s.Judge != nil {
		fmt.Fprintf(&b, "| Judge (%s) | %d calls, %d skipped, %s of %s |\n", s.Judge.Model, s.Judge.Calls, s.Judge.Skipped, usd(s.Judge.SpentMicro), usd(s.Judge.Budget))
	}
	b.WriteString("\n## By kind\n\n| Kind | Runs | Success | P50 | P95 | Cost |\n|---|---|---|---|---|---|\n")
	for _, k := range sortedAggKeys(s.ByKind) {
		a := s.ByKind[k]
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s |\n", k, a.Runs, pct(a.SuccessRate), secs(a.LatencyMs.P50), secs(a.LatencyMs.P95), usd(a.CostMicro))
	}
	if len(s.Categories) > 0 {
		b.WriteString("\n## Failure categories\n\n| Category | Count |\n|---|---|\n")
		for _, c := range sortedCountKeys(s.Categories) {
			fmt.Fprintf(&b, "| %s | %d |\n", c, s.Categories[c])
		}
	}
	b.WriteString("\n## Tasks\n\n| Task | Kind | Pass | P50 | Cost | Tool calls | Failures |\n|---|---|---|---|---|---|---|\n")
	for _, t := range s.ByTask {
		var fails []string
		for _, c := range sortedCountKeys(t.Categories) {
			fails = append(fails, fmt.Sprintf("%s×%d", c, t.Categories[c]))
		}
		fmt.Fprintf(&b, "| %s | %s | %d/%d | %s | %s | %d | %s |\n", t.TaskID, t.Kind, t.Passed, t.Runs, secs(t.LatencyMs.P50), usd(t.CostMicro), t.ToolCalls, strings.Join(fails, ", "))
	}
	return b.String()
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func sortedAggKeys(m map[string]*Agg) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedCountKeys orders by count descending, then name.
func sortedCountKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if m[out[i]] != m[out[j]] {
			return m[out[i]] > m[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}

// LoadedRun is a run directory read back from disk.
type LoadedRun struct {
	Dir          string
	Manifest     *Manifest
	Trajectories []*Trajectory
}

// LoadRun reads manifest.json and trajectories.jsonl of a run directory.
func LoadRun(dir string) (*LoadedRun, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("eval: %w", err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("eval: manifest: %w", err)
	}
	f, err := os.Open(filepath.Join(dir, "trajectories.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("eval: %w", err)
	}
	defer f.Close()
	lr := &LoadedRun{Dir: dir, Manifest: &m}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 256<<20)
	for n := 1; sc.Scan(); n++ {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var t Trajectory
		if err := json.Unmarshal(sc.Bytes(), &t); err != nil {
			return nil, fmt.Errorf("eval: trajectories.jsonl line %d: %w", n, err)
		}
		lr.Trajectories = append(lr.Trajectories, &t)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("eval: trajectories.jsonl: %w", err)
	}
	return lr, nil
}

// Summary recomputes the run summary from the stored trajectories.
func (lr *LoadedRun) Summary() *Summary { return Summarize(lr.Manifest, lr.Trajectories) }
