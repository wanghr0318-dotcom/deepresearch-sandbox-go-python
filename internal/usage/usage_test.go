package usage

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseDims(t *testing.T) {
	got, err := ParseDims("model, day,key")
	if err != nil || !slices.Equal(got, []Dim{DimDay, DimKey, DimModel}) {
		t.Fatalf("ParseDims = %v, %v", got, err)
	}
	if got, err := ParseDims(""); err != nil || !slices.Equal(got, []Dim{DimDay}) {
		t.Fatalf("empty = %v, %v", got, err)
	}
	for _, bad := range []string{"week", "day,day", "day,", "model;key"} {
		if _, err := ParseDims(bad); err == nil {
			t.Errorf("ParseDims(%q) accepted", bad)
		}
	}
}

func TestQueryValidate(t *testing.T) {
	d := func(s string) time.Time { v, _ := ParseDay(s); return v }
	for _, c := range []struct {
		from, to string
		ok       bool
	}{
		{"2026-10-01", "2026-10-01", true},
		{"2026-10-01", "2026-10-31", true},
		{"2026-10-02", "2026-10-01", false},
		{"2025-10-01", "2026-09-30", true}, // 365 days
		{"2025-10-01", "2026-10-02", false},
	} {
		if err := (Query{From: d(c.from), To: d(c.to)}).Validate(); (err == nil) != c.ok {
			t.Errorf("%s..%s: %v", c.from, c.to, err)
		}
	}
	if (Query{}).Validate() == nil {
		t.Fatal("zero range accepted")
	}
}

func TestMonthHelpers(t *testing.T) {
	m, err := ParseMonth("2026-02")
	if err != nil {
		t.Fatal(err)
	}
	from, to := MonthRange(m)
	if from.Format("2006-01-02") != "2026-02-01" || to.Format("2006-01-02") != "2026-02-28" {
		t.Fatalf("MonthRange = %v %v", from, to)
	}
	if _, err := ParseMonth("2026-13"); err == nil {
		t.Fatal("bad month accepted")
	}
	loc := time.FixedZone("x", 8*3600)
	if got := Month(time.Date(2026, 11, 1, 3, 0, 0, 0, loc)); got.Format("2006-01-02") != "2026-10-01" {
		t.Fatalf("Month is UTC: %v", got)
	}
}

func TestNewBill(t *testing.T) {
	m, _ := ParseMonth("2026-10")
	b := NewBill(m, []Group{
		{Model: "smart", Source: SourceModelAPI, Totals: Totals{Requests: 2, PromptTokens: 10, CompletionTokens: 5, CostMicro: 100}},
		{Model: "smart", Source: SourceRuntime, Totals: Totals{Requests: 1, CostMicro: 50, UnknownMicro: 7}},
		{Model: "search", Source: SourceRuntime, Totals: Totals{Requests: 3, CostMicro: 300}},
		{Model: "", Source: SourceModelAPI, Totals: Totals{Requests: 1, UnknownMicro: 9}},
	})
	if b.Month != "2026-10" || len(b.Models) != 3 || b.Models[0].Model != "search" || b.Models[1].Model != "smart" ||
		b.Models[1].TotalMicro != 157 || b.Models[1].Requests != 3 || b.Models[1].PromptTokens != 10 {
		t.Fatalf("models = %+v", b.Models)
	}
	if b.Total.CostMicro != 450 || b.Total.UnknownMicro != 16 || b.Total.TotalMicro != 466 || b.Total.Requests != 7 {
		t.Fatalf("total = %+v", b.Total)
	}
	if len(b.Sources) != 2 || b.Sources[0].Source != SourceModelAPI || b.Sources[0].TotalMicro != 109 || b.Sources[1].TotalMicro != 357 {
		t.Fatalf("sources = %+v", b.Sources)
	}
	var sb strings.Builder
	WriteBill(&sb, "账单", b)
	if !strings.Contains(sb.String(), "(未记录)") || !strings.Contains(sb.String(), "$0.000466") {
		t.Fatalf("WriteBill:\n%s", sb.String())
	}
	if e := NewBill(m, nil); e.Models == nil || e.Sources == nil || e.Total.TotalMicro != 0 {
		t.Fatalf("empty bill = %+v", e)
	}
}

func TestUSD(t *testing.T) {
	for in, want := range map[int64]string{0: "$0.000000", 1: "$0.000001", 1_500_000: "$1.500000", -2: "-$0.000002"} {
		if got := USD(in); got != want {
			t.Errorf("USD(%d) = %s", in, got)
		}
	}
}

// Compare finds a usage_daily row that differs from its recomputation, rows only on one side, and budget levels whose
// spent or unknown differ or that are missing; equal data is OK.
func TestCompare(t *testing.T) {
	m, _ := ParseMonth("2026-10")
	k := Key{Day: "2026-10-03", OrgID: 7, UserID: 3, KeyID: "k_x", Model: "smart", Source: SourceModelAPI}
	raw := []Row{{Key: k, Totals: Totals{Requests: 2, CostMicro: 100, UnknownMicro: 10}}}
	budgets := []BudgetRow{{"org", "7", 100, 10}, {"user", "7/3", 100, 10}, {"key", "k_x", 100, 10}}
	if r := Compare(m, raw, raw, budgets); !r.OK() || r.Raw != r.Rollup {
		t.Fatalf("equal data: %+v", r)
	}
	// An injected difference in usage_daily, plus a row only in usage_daily.
	k2 := k
	k2.Day = "2026-10-04"
	daily := []Row{{Key: k, Totals: Totals{Requests: 2, CostMicro: 101, UnknownMicro: 10}}, {Key: k2, Totals: Totals{Requests: 1}}}
	r := Compare(m, raw, daily, budgets)
	if r.OK() || len(r.Usage) != 2 || r.Usage[0].Rollup.CostMicro != 101 || r.Usage[1].Raw != (Totals{}) || len(r.Budgets) != 0 {
		t.Fatalf("usage diff: %+v", r)
	}
	// Budgets: spent differs at the key level, the user row is missing; an unrelated zero row is fine.
	r = Compare(m, raw, raw, []BudgetRow{{"org", "7", 100, 10}, {"key", "k_x", 99, 10}, {"org", "8", 0, 0}})
	if len(r.Budgets) != 2 || r.Budgets[0].Level != "user" || !r.Budgets[0].Missing || r.Budgets[1].Level != "key" ||
		r.Budgets[1].SpentMicro != 99 {
		t.Fatalf("budget diffs: %+v", r.Budgets)
	}
	r.OrgNames, r.Usernames = map[int64]string{7: "acme"}, map[int64]string{3: "alice"}
	lines := strings.Join(r.Lines(), "\n")
	for _, want := range []string{"quota_budgets user:7/3：缺少预算行", "quota_budgets key:k_x：spent=99"} {
		if !strings.Contains(lines, want) {
			t.Errorf("lines miss %q:\n%s", want, lines)
		}
	}
	r = Compare(m, raw, daily, budgets)
	r.OrgNames, r.Usernames = map[int64]string{7: "acme"}, map[int64]string{3: "alice"}
	if l := r.Lines()[0]; !strings.Contains(l, "org=acme user=alice key=k_x model=smart") {
		t.Fatalf("line = %s", l)
	}
	// A budget with spend but no usage at all.
	if r := Compare(m, nil, nil, []BudgetRow{{"org", "9", 5, 0}}); len(r.Budgets) != 1 || r.Budgets[0].SpentMicro != 5 {
		t.Fatalf("budget without usage: %+v", r.Budgets)
	}
}
