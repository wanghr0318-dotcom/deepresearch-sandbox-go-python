package doctor

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

// Options configure an analysis.
type Options struct {
	// SlowFactor: a task is slow when its latency ≥ SlowFactor × the median latency of the passing tasks of its
	// kind (default 3).
	SlowFactor float64
	// SlowMs: additionally, a task is slow when its latency ≥ SlowMs (0 = off).
	SlowMs int64
	// Config is the flags file the run was made with (optional).
	Config *Flags
	Now    func() time.Time
}

func (o Options) withDefaults() Options {
	if o.SlowFactor <= 0 {
		o.SlowFactor = 3
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// record is one analysed task.
type record struct {
	tr     *eval.Trajectory
	bd     Breakdown
	failed bool
	slow   bool
	cause  string
	// attempt start latencies of this task
	starts []int64
}

// analysis is the state shared by the rules.
type analysis struct {
	o       Options
	recs    []*record
	chain   bool // some model try carries a provider route (fallback chain configured)
	cfg     *Flags
	explain map[string][2]int // rule -> failed, slow
	hangThr map[string]int64  // per route, see hungTry
}

// Analyze classifies the given trajectories and returns the report (without observability or advisor parts).
func Analyze(src Source, trs []*eval.Trajectory, o Options) *Report {
	o = o.withDefaults()
	a := &analysis{o: o, cfg: o.Config, explain: map[string][2]int{}}
	for _, tr := range trs {
		r := &record{tr: tr, failed: tr.Outcome.Verdict != eval.VerdictPass}
		r.bd, r.starts = breakdown(tr)
		a.recs = append(a.recs, r)
		for _, c := range tr.Calls {
			for _, t := range c.Tries {
				if t.Provider != "" && kindOf(c.Endpoint) == "chat" {
					a.chain = true
				}
			}
		}
	}
	thresholds := a.markSlow()
	rep := &Report{Schema: SchemaFindings, GeneratedAt: o.Now().UTC(), Source: src, Causes: []CauseCount{},
		Findings: []Finding{}, Proposals: []Proposal{}, Slowest: []TaskSummary{}, Unexplained: []TaskSummary{}}
	var all []Proposal
	for _, rl := range rules {
		f := rl.run(a)
		if f == nil {
			continue
		}
		f.Rule = rl.id
		counts := a.explain[rl.id]
		f.Failed, f.Slow = counts[0], counts[1]
		for i := range f.Proposals {
			f.Proposals[i].Rule, f.Proposals[i].Source = rl.id, "rule"
		}
		all = append(all, f.Proposals...)
		rep.Findings = append(rep.Findings, *f)
	}
	rep.Proposals = MergeProposals(all)
	rep.Totals = Totals{Tasks: len(a.recs), SlowThresholdMs: thresholds}
	causes := map[string]*CauseCount{}
	var causeOrder []string
	for _, r := range a.recs {
		switch {
		case r.failed:
			rep.Totals.Failed++
		case r.slow:
			rep.Totals.Slow++
		default:
			continue
		}
		cause := r.cause
		if cause == "" {
			cause = "unexplained"
			rep.Unexplained = append(rep.Unexplained, summary(r))
		} else {
			rep.Totals.Explained++
		}
		cc := causes[cause]
		if cc == nil {
			cc = &CauseCount{Cause: cause}
			causes[cause] = cc
			causeOrder = append(causeOrder, cause)
		}
		if r.failed {
			cc.Failed++
		} else {
			cc.Slow++
		}
	}
	for _, c := range causeOrder {
		rep.Causes = append(rep.Causes, *causes[c])
	}
	sort.SliceStable(rep.Causes, func(i, j int) bool {
		return rep.Causes[i].Failed+rep.Causes[i].Slow > rep.Causes[j].Failed+rep.Causes[j].Slow
	})
	sorted := append([]*record(nil), a.recs...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].tr.LatencyMs > sorted[j].tr.LatencyMs })
	for _, r := range sorted[:min(5, len(sorted))] {
		rep.Slowest = append(rep.Slowest, summary(r))
	}
	if len(rep.Unexplained) > 20 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("%d unexplained tasks; the first 20 are listed", len(rep.Unexplained)))
		rep.Unexplained = rep.Unexplained[:20]
	}
	if !a.chain {
		rep.Notes = append(rep.Notes, "no model try carries a provider route: the server runs without a fallback chain (single provider)")
	}
	return rep
}

// mark attributes a failed or slow task to a rule unless an earlier rule already explained it.
func (a *analysis) mark(r *record, rule string) bool {
	if (!r.failed && !r.slow) || r.cause != "" {
		return false
	}
	r.cause = rule
	c := a.explain[rule]
	if r.failed {
		c[0]++
	} else {
		c[1]++
	}
	a.explain[rule] = c
	return true
}

// markSlow flags slow tasks and returns the threshold per kind.
func (a *analysis) markSlow() map[string]int64 {
	byKind := map[string][]int64{}
	allByKind := map[string][]int64{}
	for _, r := range a.recs {
		allByKind[r.tr.Kind] = append(allByKind[r.tr.Kind], r.tr.LatencyMs)
		if !r.failed {
			byKind[r.tr.Kind] = append(byKind[r.tr.Kind], r.tr.LatencyMs)
		}
	}
	th := map[string]int64{}
	for k, all := range allByKind {
		base := byKind[k]
		if len(base) == 0 {
			base = all
		}
		t := int64(math.Ceil(a.o.SlowFactor * float64(pct(base, 50))))
		if a.o.SlowMs > 0 && (t == 0 || a.o.SlowMs < t) {
			t = a.o.SlowMs
		}
		th[k] = max(t, 1000) // sub-second tasks are never "slow"
	}
	for _, r := range a.recs {
		r.slow = !r.failed && r.tr.LatencyMs >= th[r.tr.Kind]
	}
	return th
}

func summary(r *record) TaskSummary {
	return TaskSummary{TaskID: r.tr.TaskID, Rep: r.tr.Rep, ServerTaskID: r.tr.ServerTaskID, Kind: r.tr.Kind,
		Status: r.tr.Status, Verdict: r.tr.Outcome.Verdict, Category: r.tr.Outcome.Category, Cause: r.cause,
		LatencyMs: r.tr.LatencyMs, Breakdown: r.bd}
}

func example(r *record, callID, note string) Example {
	return Example{TaskID: r.tr.TaskID, Rep: r.tr.Rep, ServerTaskID: r.tr.ServerTaskID, CallID: callID, Note: note}
}

// kindOf maps a Gateway endpoint to a call kind.
func kindOf(endpoint string) string {
	switch {
	case strings.Contains(endpoint, "chat/completions"):
		return "chat"
	case strings.HasSuffix(endpoint, "/search"):
		return "search"
	case strings.HasSuffix(endpoint, "/fetch"):
		return "fetch"
	case strings.HasSuffix(endpoint, "/exec") || strings.Contains(endpoint, "workspace"):
		return "exec"
	}
	return "other"
}

// breakdown derives the timing components of one task from its events and journal.
func breakdown(tr *eval.Trajectory) (Breakdown, []int64) {
	var bd Breakdown
	var created, pendingAttempt, stopAt time.Time
	var starts []int64
	firstAttempt := true
	for _, ev := range tr.Events {
		switch {
		case ev.Source == "host" && ev.Type == "task_created":
			created = ev.TS
		case ev.Source == "host" && ev.Type == "attempt_created":
			if firstAttempt && !created.IsZero() {
				bd.QueueMs = ms(ev.TS.Sub(created))
			}
			firstAttempt = false
			pendingAttempt = ev.TS
		case ev.Source == "worker" && ev.Type == "ready":
			if !pendingAttempt.IsZero() {
				d := ms(ev.TS.Sub(pendingAttempt))
				bd.StartMs += d
				starts = append(starts, d)
				pendingAttempt = time.Time{}
			}
		case ev.Source == "host" && ev.Type == "control_accepted":
			var p struct {
				Desired string `json:"desired"`
			}
			if json.Unmarshal(ev.Payload, &p) == nil && (p.Desired == "pause" || p.Desired == "cancel") && stopAt.IsZero() {
				stopAt = ev.TS
			}
		case ev.Source == "host" && (ev.Type == "task_terminal" || ev.Type == "control_applied"):
			if !stopAt.IsZero() && stoppedEvent(ev) {
				bd.StopMs += ms(ev.TS.Sub(stopAt))
				stopAt = time.Time{}
			}
		}
	}
	for _, c := range tr.Calls {
		k := kindOf(c.Endpoint)
		var sum int64
		for _, t := range c.Tries {
			sum += t.LatencyMs
		}
		switch k {
		case "chat":
			bd.ModelMs += sum
		case "search":
			bd.SearchMs += sum
		case "fetch":
			bd.FetchMs += sum
		case "exec":
			bd.ExecMs += sum
		default:
			bd.OtherCallsMs += sum
		}
		bd.BackoffEstMs += backoffEstimate(c)
	}
	return bd, starts
}

// stoppedEvent reports whether a host event ends a requested stop (task terminal, or paused).
func stoppedEvent(ev eval.Event) bool {
	if ev.Type == "task_terminal" {
		return true
	}
	var p struct {
		Status string `json:"status"`
	}
	return json.Unmarshal(ev.Payload, &p) == nil && (p.Status == "paused" || p.Status == "cancelled")
}

// backoffEstimate estimates the backoff the Gateway slept between the tries of one call: a new round starts when
// a provider route repeats (single provider: every retry), and round k sleeps min(2 s · 2^(k-1), 60 s).
func backoffEstimate(c eval.Call) int64 {
	tries := append([]eval.Try(nil), c.Tries...)
	sort.Slice(tries, func(i, j int) bool { return tries[i].TryNo < tries[j].TryNo })
	var total int64
	round := 0
	seen := map[string]bool{}
	for _, t := range tries {
		if t.Hedge {
			continue
		}
		if seen[t.Provider] {
			round++
			total += min(int64(2000)<<(round-1), 60_000)
			seen = map[string]bool{}
		}
		seen[t.Provider] = true
	}
	return total
}

func ms(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return d.Milliseconds()
}

// pct returns the p-th percentile (nearest rank) of xs (0 for none).
func pct(xs []int64, p float64) int64 {
	if len(xs) == 0 {
		return 0
	}
	return eval.Percentile(xs, p)
}

func share(part, whole int64) float64 {
	if whole <= 0 {
		return 0
	}
	return math.Round(float64(part)/float64(whole)*1000) / 1000
}
