package usage

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

// BillLine is one model's usage in a bill.
type BillLine struct {
	Model string `json:"model"`
	Totals
	TotalMicro int64 `json:"total_micro"`
}

// SourceLine is one source's usage in a bill.
type SourceLine struct {
	Source Source `json:"source"`
	Totals
	TotalMicro int64 `json:"total_micro"`
}

// Bill is a month of usage for an org (or, for a member, for that member in the org).
type Bill struct {
	Month   string       `json:"month"` // YYYY-MM
	Models  []BillLine   `json:"models"`
	Sources []SourceLine `json:"sources"`
	Total   BillLine     `json:"total"` // Model is empty
}

// NewBill builds a bill from rows grouped by model and source. Lines are ordered by total cost (highest first),
// then by name.
func NewBill(month time.Time, rows []Group) Bill {
	b := Bill{Month: Month(month).Format("2006-01"), Models: []BillLine{}, Sources: []SourceLine{}}
	models := map[string]*BillLine{}
	sources := map[Source]*SourceLine{}
	for _, r := range rows {
		m := models[r.Model]
		if m == nil {
			m = &BillLine{Model: r.Model}
			models[r.Model] = m
		}
		m.Add(r.Totals)
		s := sources[r.Source]
		if s == nil {
			s = &SourceLine{Source: r.Source}
			sources[r.Source] = s
		}
		s.Add(r.Totals)
		b.Total.Add(r.Totals)
	}
	for _, m := range models {
		m.TotalMicro = m.Totals.TotalMicro()
		b.Models = append(b.Models, *m)
	}
	for _, s := range sources {
		s.TotalMicro = s.Totals.TotalMicro()
		b.Sources = append(b.Sources, *s)
	}
	slices.SortFunc(b.Models, func(x, y BillLine) int {
		return cmp.Or(cmp.Compare(y.TotalMicro, x.TotalMicro), cmp.Compare(x.Model, y.Model))
	})
	slices.SortFunc(b.Sources, func(x, y SourceLine) int { return cmp.Compare(x.Source, y.Source) })
	b.Total.TotalMicro = b.Total.Totals.TotalMicro()
	return b
}

// USD formats micro-dollars as dollars with six decimals ("$1.234567").
func USD(micro int64) string {
	sign := ""
	if micro < 0 {
		sign, micro = "-", -micro
	}
	return fmt.Sprintf("%s$%d.%06d", sign, micro/1_000_000, micro%1_000_000)
}

// ModelLabel is how a model name is shown: an empty name (a request whose record is missing) is "(未记录)".
func ModelLabel(m string) string {
	if m == "" {
		return "(未记录)"
	}
	return m
}

// WriteBill writes b as a text table (used by the CLI).
func WriteBill(w io.Writer, title string, b Bill) {
	fmt.Fprintf(w, "%s %s\n", title, b.Month)
	fmt.Fprintf(w, "%-28s %10s %14s %14s %16s %16s %16s\n", "模型", "请求", "输入 token", "输出 token", "已知费用", "未知费用", "合计")
	for _, l := range b.Models {
		fmt.Fprintf(w, "%-28s %10d %14d %14d %16s %16s %16s\n", ModelLabel(l.Model), l.Requests, l.PromptTokens, l.CompletionTokens,
			USD(l.CostMicro), USD(l.UnknownMicro), USD(l.TotalMicro))
	}
	fmt.Fprintf(w, "%-28s %10d %14d %14d %16s %16s %16s\n", "合计", b.Total.Requests, b.Total.PromptTokens, b.Total.CompletionTokens,
		USD(b.Total.CostMicro), USD(b.Total.UnknownMicro), USD(b.Total.TotalMicro))
	var parts []string
	for _, s := range b.Sources {
		parts = append(parts, string(s.Source)+"="+USD(s.TotalMicro)+"（"+strconv.FormatInt(s.Requests, 10)+" 次）")
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "按来源：%s\n", strings.Join(parts, "，"))
	}
}
