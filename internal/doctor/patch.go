package doctor

import (
	"fmt"
	"sort"
	"strings"
)

// MergeProposals collapses apply proposals to one per flag (the larger value wins: every allowlisted flag is a
// timeout, delay, threshold or budget for which the larger value is the more permissive one, so an experiment
// does not create new failures by tightening something) and keeps every distinct advisory.
func MergeProposals(ps []Proposal) []Proposal {
	byFlag := map[string]Proposal{}
	var order []string
	var adv []Proposal
	seen := map[string]bool{}
	for _, p := range ps {
		if p.Kind != KindApply {
			if key := p.Rule + "\x00" + p.Text; !seen[key] {
				seen[key] = true
				adv = append(adv, p)
			}
			continue
		}
		prev, ok := byFlag[p.Flag]
		if !ok {
			byFlag[p.Flag] = p
			order = append(order, p.Flag)
			continue
		}
		s, _ := specFor(p.Flag)
		a, errA := s.parseValue(prev.To)
		b, errB := s.parseValue(p.To)
		if errA == nil && errB == nil && b > a {
			p.Reason = p.Reason + "; also: " + prev.Reason
			byFlag[p.Flag] = p
		}
	}
	out := make([]Proposal, 0, len(order)+len(adv))
	for _, f := range order {
		out = append(out, byFlag[f])
	}
	return append(out, adv...)
}

// Experiment returns a copy of cfg with every apply proposal written into it.
func Experiment(cfg *Flags, ps []Proposal) *Flags {
	out := cfg.Clone()
	for _, p := range ps {
		if p.Kind != KindApply {
			continue
		}
		out.Set(p.Flag, p.To, fmt.Sprintf("doctor (%s, %s): %s", p.Rule, p.Source, p.Reason))
	}
	return out
}

// Patch renders proposal.patch: advisory proposals as leading # comment lines (ignored by patch(1)), then a
// unified diff from the current flags file to the experiment config.
func Patch(name string, cfg *Flags, ps []Proposal) string {
	var b strings.Builder
	b.WriteString("# agentbox doctor-traces proposal. Review before use; never apply to production unmeasured.\n")
	for _, p := range ps {
		if p.Kind == KindAdvisory {
			fmt.Fprintf(&b, "# advisory (%s, not applied): %s\n", p.Rule, oneLine(p.Text))
		}
	}
	exp := Experiment(cfg, ps)
	diff := UnifiedDiff("a/"+name, "b/"+name, string(cfg.Bytes()), string(exp.Bytes()))
	if diff == "" {
		b.WriteString("# no allowlisted flag to change\n")
		return b.String()
	}
	b.WriteString(diff)
	return b.String()
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// UnifiedDiff returns a unified diff (3 lines of context) between two texts, or "" when they are equal.
func UnifiedDiff(aName, bName, a, b string) string {
	if a == b {
		return ""
	}
	al, bl := splitLines(a), splitLines(b)
	ops := lcsOps(al, bl)
	// Group ops into hunks with 3 lines of context.
	const ctx = 3
	type hunk struct{ start, end int } // indexes into ops
	var hunks []hunk
	for i, op := range ops {
		if op.kind == ' ' {
			continue
		}
		lo, hi := max(0, i-ctx), min(len(ops), i+ctx+1)
		if n := len(hunks); n > 0 && lo <= hunks[n-1].end {
			hunks[n-1].end = hi
		} else {
			hunks = append(hunks, hunk{lo, hi})
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "--- %s\n+++ %s\n", aName, bName)
	for _, h := range hunks {
		aStart, bStart, aLen, bLen := -1, -1, 0, 0
		for _, op := range ops[h.start:h.end] {
			if op.kind != '+' {
				if aStart < 0 {
					aStart = op.ai
				}
				aLen++
			}
			if op.kind != '-' {
				if bStart < 0 {
					bStart = op.bi
				}
				bLen++
			}
		}
		if aStart < 0 {
			aStart = ops[h.start].ai - 1
		}
		if bStart < 0 {
			bStart = ops[h.start].bi - 1
		}
		fmt.Fprintf(&out, "@@ -%s +%s @@\n", hunkRange(aStart, aLen), hunkRange(bStart, bLen))
		for _, op := range ops[h.start:h.end] {
			out.WriteByte(op.kind)
			out.WriteString(op.text)
			out.WriteByte('\n')
		}
	}
	return out.String()
}

func hunkRange(start, n int) string {
	if n == 1 {
		return fmt.Sprint(start + 1)
	}
	if n == 0 {
		return fmt.Sprintf("%d,0", start+1)
	}
	return fmt.Sprintf("%d,%d", start+1, n)
}

func splitLines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

type diffOp struct {
	kind   byte // ' ', '-', '+'
	text   string
	ai, bi int // line index in a / b (the position the op refers to)
}

// lcsOps computes an edit script with a longest-common-subsequence table (flags files are small).
func lcsOps(a, b []string) []diffOp {
	n, m := len(a), len(b)
	t := make([][]int, n+1)
	for i := range t {
		t[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				t[i][j] = t[i+1][j+1] + 1
			} else {
				t[i][j] = max(t[i+1][j], t[i][j+1])
			}
		}
	}
	var ops []diffOp
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			ops = append(ops, diffOp{' ', a[i], i, j})
			i++
			j++
		case i < n && (j == m || t[i+1][j] >= t[i][j+1]):
			ops = append(ops, diffOp{'-', a[i], i, j})
			i++
		default:
			ops = append(ops, diffOp{'+', b[j], i, j})
			j++
		}
	}
	return ops
}

// sortedKeys returns the keys of a map in order.
func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
