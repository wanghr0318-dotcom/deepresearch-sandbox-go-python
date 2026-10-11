package usage

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"
)

// Key is the dimensions of one usage_daily row.
type Key struct {
	Day    string `json:"day"` // YYYY-MM-DD
	OrgID  int64  `json:"org_id"`
	UserID int64  `json:"user_id"` // 0: no user (operator)
	KeyID  string `json:"key_id"`  // "": no key
	Model  string `json:"model"`
	Source Source `json:"source"`
}

// Row is one usage_daily row, or the same aggregate recomputed from the raw holds.
type Row struct {
	Key
	Totals
}

// Diff is a usage_daily row that differs from its recomputation (either side may be all zero: missing).
type Diff struct {
	Key
	Raw    Totals `json:"raw"`    // recomputed from quota_holds and model_requests
	Rollup Totals `json:"rollup"` // stored in usage_daily
}

// BudgetRow is one level's month in quota_budgets.
type BudgetRow struct {
	Level        string // org, user, key
	ScopeID      string // as quota.Target.ID: org ID, "<org ID>/<user ID>", key ID
	SpentMicro   int64
	UnknownMicro int64
}

// BudgetDiff is a level whose budget does not match the recomputed usage.
type BudgetDiff struct {
	Level        string `json:"level"`
	ScopeID      string `json:"scope_id"`
	RawCost      int64  `json:"raw_cost_micro"`    // Σ known cost of the level's holds
	RawUnknown   int64  `json:"raw_unknown_micro"` // Σ unknown cost of the level's holds
	SpentMicro   int64  `json:"spent_micro"`
	UnknownMicro int64  `json:"unknown_micro"`
	Missing      bool   `json:"missing"` // the level has usage but no budget row
}

// Report is the result of reconciling a month.
type Report struct {
	Month   string       `json:"month"`
	Raw     Totals       `json:"raw"`    // the month recomputed from the raw holds
	Rollup  Totals       `json:"rollup"` // the month in usage_daily
	Usage   []Diff       `json:"usage_differences"`
	Budgets []BudgetDiff `json:"budget_differences"`
	// Names for printing (filled by the store).
	OrgNames  map[int64]string `json:"-"`
	Usernames map[int64]string `json:"-"`
}

// OK reports whether there is no difference.
func (r Report) OK() bool { return len(r.Usage) == 0 && len(r.Budgets) == 0 }

func keyLess(a, b Key) int {
	return cmp.Or(cmp.Compare(a.Day, b.Day), cmp.Compare(a.OrgID, b.OrgID), cmp.Compare(a.UserID, b.UserID),
		cmp.Compare(a.KeyID, b.KeyID), cmp.Compare(a.Model, b.Model), cmp.Compare(a.Source, b.Source))
}

// Compare reconciles a month: raw (recomputed) rows against the stored usage_daily rows, and the raw rows summed per
// budget level against the month's budget rows.
func Compare(month time.Time, raw, daily []Row, budgets []BudgetRow) Report {
	rep := Report{Month: Month(month).Format("2006-01"), Usage: []Diff{}, Budgets: []BudgetDiff{}}
	byKey := map[Key]*Diff{}
	get := func(k Key) *Diff {
		d := byKey[k]
		if d == nil {
			d = &Diff{Key: k}
			byKey[k] = d
		}
		return d
	}
	type level struct{ kind, id string }
	levels := map[level]*BudgetDiff{}
	at := func(kind, id string) *BudgetDiff {
		l := levels[level{kind, id}]
		if l == nil {
			l = &BudgetDiff{Level: kind, ScopeID: id, Missing: true}
			levels[level{kind, id}] = l
		}
		return l
	}
	for _, r := range raw {
		get(r.Key).Raw.Add(r.Totals)
		rep.Raw.Add(r.Totals)
		org := strconv.FormatInt(r.OrgID, 10)
		ls := []*BudgetDiff{at("org", org)}
		if r.UserID != 0 {
			ls = append(ls, at("user", org+"/"+strconv.FormatInt(r.UserID, 10)))
		}
		if r.KeyID != "" {
			ls = append(ls, at("key", r.KeyID))
		}
		for _, l := range ls {
			l.RawCost += r.CostMicro
			l.RawUnknown += r.UnknownMicro
		}
	}
	for _, r := range daily {
		get(r.Key).Rollup.Add(r.Totals)
		rep.Rollup.Add(r.Totals)
	}
	for _, d := range byKey {
		if d.Raw != d.Rollup {
			rep.Usage = append(rep.Usage, *d)
		}
	}
	slices.SortFunc(rep.Usage, func(a, b Diff) int { return keyLess(a.Key, b.Key) })
	for _, b := range budgets {
		l := at(b.Level, b.ScopeID)
		l.Missing, l.SpentMicro, l.UnknownMicro = false, b.SpentMicro, b.UnknownMicro
	}
	for _, l := range levels {
		if l.Missing && l.RawCost == 0 && l.RawUnknown == 0 {
			continue // e.g. a level whose only holds were released
		}
		if l.Missing || l.RawCost != l.SpentMicro || l.RawUnknown != l.UnknownMicro {
			rep.Budgets = append(rep.Budgets, *l)
		}
	}
	order := map[string]int{"org": 0, "user": 1, "key": 2}
	slices.SortFunc(rep.Budgets, func(a, b BudgetDiff) int {
		return cmp.Or(cmp.Compare(order[a.Level], order[b.Level]), cmp.Compare(a.ScopeID, b.ScopeID))
	})
	return rep
}

func (r Report) orgName(id int64) string {
	if n := r.OrgNames[id]; n != "" {
		return n
	}
	return "#" + strconv.FormatInt(id, 10)
}

func (r Report) username(id int64) string {
	if id == 0 {
		return "-"
	}
	if n := r.Usernames[id]; n != "" {
		return n
	}
	return "#" + strconv.FormatInt(id, 10)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func fmtTotals(t Totals) string {
	return fmt.Sprintf("requests=%d prompt=%d completion=%d cost=%d unknown=%d", t.Requests, t.PromptTokens, t.CompletionTokens,
		t.CostMicro, t.UnknownMicro)
}

// Lines returns one human-readable line per difference (org, user, key and model named).
func (r Report) Lines() []string {
	var out []string
	for _, d := range r.Usage {
		out = append(out, fmt.Sprintf("usage_daily %s org=%s user=%s key=%s model=%s source=%s：重算 {%s}，汇总 {%s}", d.Day,
			r.orgName(d.OrgID), r.username(d.UserID), orDash(d.KeyID), ModelLabel(d.Model), d.Source, fmtTotals(d.Raw), fmtTotals(d.Rollup)))
	}
	for _, b := range r.Budgets {
		if b.Missing {
			out = append(out, fmt.Sprintf("quota_budgets %s:%s：缺少预算行，重算 cost=%d unknown=%d", b.Level, b.ScopeID, b.RawCost, b.RawUnknown))
			continue
		}
		out = append(out, fmt.Sprintf("quota_budgets %s:%s：spent=%d unknown=%d，重算 cost=%d unknown=%d", b.Level, b.ScopeID,
			b.SpentMicro, b.UnknownMicro, b.RawCost, b.RawUnknown))
	}
	return out
}

// WriteReport writes r as text (used by the CLI).
func WriteReport(w io.Writer, r Report) {
	fmt.Fprintf(w, "对账 %s：重算 %s（已知 %s，未知 %s，%d 次请求）；usage_daily %s\n", r.Month, USD(r.Raw.TotalMicro()),
		USD(r.Raw.CostMicro), USD(r.Raw.UnknownMicro), r.Raw.Requests, USD(r.Rollup.TotalMicro()))
	for _, l := range r.Lines() {
		fmt.Fprintln(w, "差异 "+l)
	}
	if r.OK() {
		fmt.Fprintln(w, "一致：usage_daily 与 quota_budgets 都与原始记录相符")
	}
}
