package doctor

import (
	"fmt"
	"strings"
)

// RenderMarkdown renders the report for humans (findings.md, also printed by the CLI).
func RenderMarkdown(r *Report) string {
	var b strings.Builder
	b.WriteString("# Trace doctor report\n\n")
	switch r.Source.Kind {
	case "eval_run":
		fmt.Fprintf(&b, "Source: eval run `%s` (suite `%s`, agent `%s`)", r.Source.RunID, r.Source.Suite, r.Source.Agent)
	default:
		fmt.Fprintf(&b, "Source: server %s, tasks created %s – %s", r.Source.Server, r.Source.Since, nz(r.Source.Until, "now"))
	}
	if r.Source.Config != "" {
		fmt.Fprintf(&b, "; config `%s`", r.Source.Config)
	}
	b.WriteString("\n\n")
	t := r.Totals
	fmt.Fprintf(&b, "**%d tasks: %d failed, %d slow (not failed); %d of %d explained by a rule.**\n\n",
		t.Tasks, t.Failed, t.Slow, t.Explained, t.Failed+t.Slow)
	if len(t.SlowThresholdMs) > 0 {
		var parts []string
		for _, k := range sortedKeys(t.SlowThresholdMs) {
			parts = append(parts, fmt.Sprintf("%s ≥ %d ms", k, t.SlowThresholdMs[k]))
		}
		fmt.Fprintf(&b, "Slow threshold: %s.\n\n", strings.Join(parts, ", "))
	}
	if len(r.Causes) > 0 {
		b.WriteString("| Cause | Failed | Slow |\n|---|---:|---:|\n")
		for _, c := range r.Causes {
			fmt.Fprintf(&b, "| %s | %d | %d |\n", c.Cause, c.Failed, c.Slow)
		}
		b.WriteString("\n")
	}
	if len(r.Findings) == 0 {
		b.WriteString("No rule fired.\n\n")
	}
	for i, f := range r.Findings {
		fmt.Fprintf(&b, "## %d. %s (`%s`, %s)\n\n%s\n\n", i+1, f.Title, f.Rule, f.Severity, f.Summary)
		if f.Failed+f.Slow > 0 {
			fmt.Fprintf(&b, "Explains %d failed and %d slow tasks.\n\n", f.Failed, f.Slow)
		}
		renderEvidence(&b, f.Evidence)
		for _, p := range f.Proposals {
			fmt.Fprintf(&b, "- Proposal: %s\n", proposalText(p))
		}
		if len(f.Proposals) > 0 {
			b.WriteString("\n")
		}
	}
	if len(r.Proposals) > 0 {
		b.WriteString("## Proposed changes (merged)\n\n")
		for _, p := range r.Proposals {
			fmt.Fprintf(&b, "- %s\n", proposalText(p))
		}
		b.WriteString("\n")
	}
	if len(r.Rejected) > 0 {
		b.WriteString("Rejected (outside the allowlist or its bounds):\n\n")
		for _, p := range r.Rejected {
			fmt.Fprintf(&b, "- %s %s=%s — %s\n", p.Source, p.Flag, p.To, p.Reason)
		}
		b.WriteString("\n")
	}
	if len(r.Slowest) > 0 {
		b.WriteString("## Slowest tasks\n\n| Task | Server task | Verdict | Cause | Latency ms | queue | start | model | search | fetch | exec | backoff≈ | stop | trace |\n|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
		for _, s := range r.Slowest {
			d := s.Breakdown
			fmt.Fprintf(&b, "| %s | %s | %s %s | %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s |\n",
				taskLabel(s), s.ServerTaskID, s.Verdict, s.Category, nz(s.Cause, "-"), s.LatencyMs, d.QueueMs, d.StartMs,
				d.ModelMs, d.SearchMs, d.FetchMs, d.ExecMs, d.BackoffEstMs, d.StopMs, nz(s.TraceID, "-"))
		}
		b.WriteString("\nCall columns are summed try latencies; parallel calls overlap.\n\n")
		for _, s := range r.Slowest {
			if len(s.Spans) == 0 {
				continue
			}
			fmt.Fprintf(&b, "Trace %s (%s) by span name: ", s.TraceID, taskLabel(s))
			var parts []string
			for _, sp := range s.Spans {
				parts = append(parts, fmt.Sprintf("%s ×%d %d ms (%.0f%%)", sp.Name, sp.Count, sp.TotalMs, sp.Share*100))
			}
			b.WriteString(strings.Join(parts, "; ") + "\n\n")
		}
	}
	if len(r.Unexplained) > 0 {
		b.WriteString("## Unexplained failed/slow tasks\n\n")
		for _, s := range r.Unexplained {
			fmt.Fprintf(&b, "- %s (%s): %s %s, %d ms\n", taskLabel(s), s.ServerTaskID, s.Verdict, nz(s.Category, s.Status), s.LatencyMs)
		}
		b.WriteString("\n")
	}
	if o := r.Observability; o != nil {
		b.WriteString("## Observability\n\n")
		fmt.Fprintf(&b, "Trace ids found in Tempo: %d.\n\n", o.TraceIDs)
		for _, m := range o.Metrics {
			fmt.Fprintf(&b, "%s (`%s`):\n\n", m.Title, m.Query)
			for _, row := range m.Rows {
				var ls []string
				for _, k := range sortedKeys(row.Labels) {
					ls = append(ls, k+"="+row.Labels[k])
				}
				fmt.Fprintf(&b, "- {%s} %.0f\n", strings.Join(ls, ","), row.Value)
			}
			b.WriteString("\n")
		}
		for _, l := range o.Logs {
			fmt.Fprintf(&b, "- log `%s`: %d lines; trace ids %s\n", l.Message, l.Count, strings.Join(l.TraceIDs, ", "))
		}
		for _, n := range o.Notes {
			fmt.Fprintf(&b, "- note: %s\n", n)
		}
		b.WriteString("\n")
	}
	if a := r.Advisor; a != nil {
		b.WriteString("## Advisor (model)\n\n")
		fmt.Fprintf(&b, "Model `%s` at %s; called: %v; spent %d of %d micro-USD.\n\n", a.Model, a.Host, a.Called, a.SpentMicro, a.BudgetMicro)
		if a.Error != "" {
			fmt.Fprintf(&b, "Error: %s\n\n", a.Error)
		}
		if a.Summary != "" {
			b.WriteString(a.Summary + "\n\n")
		}
	}
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "Note: %s\n\n", n)
	}
	return b.String()
}

func taskLabel(s TaskSummary) string {
	if s.TaskID == "" {
		return s.ServerTaskID
	}
	if s.Rep > 0 {
		return fmt.Sprintf("%s#%d", s.TaskID, s.Rep)
	}
	return s.TaskID
}

func proposalText(p Proposal) string {
	src := p.Rule
	if p.Source == "model" {
		src = "model"
	}
	if p.Kind == KindApply {
		return fmt.Sprintf("`%s`: %s → **%s** (%s; %s)", p.Flag, p.From, p.To, src, p.Reason)
	}
	return fmt.Sprintf("advisory (%s): %s", src, p.Text)
}

func renderEvidence(b *strings.Builder, e Evidence) {
	if len(e.Counts) > 0 {
		var parts []string
		for _, k := range sortedKeys(e.Counts) {
			parts = append(parts, fmt.Sprintf("%s=%d", k, e.Counts[k]))
		}
		fmt.Fprintf(b, "- counts: %s\n", strings.Join(parts, ", "))
	}
	if len(e.LatencyMs) > 0 {
		var parts []string
		for _, k := range sortedKeys(e.LatencyMs) {
			parts = append(parts, fmt.Sprintf("%s=%d", k, e.LatencyMs[k]))
		}
		fmt.Fprintf(b, "- latency ms: %s\n", strings.Join(parts, ", "))
	}
	if len(e.Shares) > 0 {
		var parts []string
		for _, k := range sortedKeys(e.Shares) {
			parts = append(parts, fmt.Sprintf("%s=%.1f%%", k, e.Shares[k]*100))
		}
		fmt.Fprintf(b, "- shares: %s\n", strings.Join(parts, ", "))
	}
	for _, d := range e.Detail {
		fmt.Fprintf(b, "- %s\n", d)
	}
	for _, x := range e.Examples {
		label := x.ServerTaskID
		if x.TaskID != "" {
			label = fmt.Sprintf("%s#%d %s", x.TaskID, x.Rep, x.ServerTaskID)
		}
		line := "- example: " + label
		if x.CallID != "" {
			line += " call " + x.CallID
		}
		if x.TraceID != "" {
			line += " trace " + x.TraceID
		}
		if x.Note != "" {
			line += " — " + x.Note
		}
		b.WriteString(line + "\n")
	}
	b.WriteString("\n")
}
