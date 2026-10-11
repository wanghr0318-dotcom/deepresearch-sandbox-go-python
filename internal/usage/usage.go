// Package usage is the usage accounting of the model gateway (design 2026-10-11 §4, §8): a periodic rollup turns
// settled budget holds into daily rows (usage_daily, one per UTC day, org, user, key, model and source), bills sum a
// month of those rows for an org, and reconciliation recomputes a month from the raw holds and model requests and
// compares it with usage_daily and with the monthly budgets.
//
// Money comes from the holds: a settled hold's charge is known cost (cost_micro), a charged_unknown hold's charge is
// unknown cost (unknown_micro), exactly as quota_budgets counts spent_micro and unknown_micro — so the three agree
// to the micro-dollar. Models and tokens come from the model API request linked to a hold (source model_api) or
// from the sandbox call (source runtime; runtime calls do not record tokens). Released holds are not usage.
//
// internal/persistence/postgres implements Store.
package usage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Source is where usage came from.
type Source string

const (
	SourceRuntime  Source = "runtime"   // calls made by sandboxes (sessions, research tasks) through the internal Gateway
	SourceModelAPI Source = "model_api" // /v1/chat/completions and /v1/messages
)

// Dim is a dimension usage can be grouped by.
type Dim string

const (
	DimDay    Dim = "day"
	DimModel  Dim = "model"
	DimKey    Dim = "key"
	DimUser   Dim = "user"
	DimOrg    Dim = "org"
	DimSource Dim = "source"
)

// AllDims lists the dimensions in their canonical order (the order of grouped rows and of output columns).
var AllDims = []Dim{DimDay, DimOrg, DimUser, DimKey, DimModel, DimSource}

// ParseDims parses a comma-separated group_by value ("day,model"). Empty means by day. Unknown and repeated
// dimensions are errors. The result is in canonical order.
func ParseDims(s string) ([]Dim, error) {
	if strings.TrimSpace(s) == "" {
		return []Dim{DimDay}, nil
	}
	seen := map[Dim]bool{}
	for _, part := range strings.Split(s, ",") {
		d := Dim(strings.TrimSpace(part))
		if !slices.Contains(AllDims, d) {
			return nil, fmt.Errorf("usage: unknown group_by %q (want day, model, key, user, org or source)", part)
		}
		if seen[d] {
			return nil, fmt.Errorf("usage: group_by %q repeated", d)
		}
		seen[d] = true
	}
	var out []Dim
	for _, d := range AllDims {
		if seen[d] {
			out = append(out, d)
		}
	}
	return out, nil
}

// Totals are the measures of a set of usage rows.
type Totals struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	CostMicro        int64 `json:"cost_micro"`    // known cost (settled holds)
	UnknownMicro     int64 `json:"unknown_micro"` // charged at the estimate because the outcome was unknown
}

// Add adds o to t.
func (t *Totals) Add(o Totals) {
	t.Requests += o.Requests
	t.PromptTokens += o.PromptTokens
	t.CompletionTokens += o.CompletionTokens
	t.CostMicro += o.CostMicro
	t.UnknownMicro += o.UnknownMicro
}

// TotalMicro is known plus unknown cost: what the month's budgets were charged.
func (t Totals) TotalMicro() int64 { return t.CostMicro + t.UnknownMicro }

// Query selects usage_daily rows. From and To are UTC days, both inclusive. OrgID 0 means every org (operator
// only); UserID 0 and KeyID "" mean every user and key of the org.
type Query struct {
	From, To time.Time
	OrgID    int64
	UserID   int64
	KeyID    string
	GroupBy  []Dim
}

// MaxDays bounds a query's range.
const MaxDays = 366

// ErrRange reports an invalid day range.
var ErrRange = errors.New("usage: invalid day range")

// Validate checks the range: From ≤ To and at most MaxDays days.
func (q Query) Validate() error {
	if q.From.IsZero() || q.To.IsZero() || q.To.Before(q.From) || q.To.Sub(q.From) >= MaxDays*24*time.Hour {
		return ErrRange
	}
	return nil
}

// Group is one row of a grouped query: the dimensions that were grouped by are set, the others are zero.
type Group struct {
	Day      string `json:"day,omitempty"` // YYYY-MM-DD
	OrgID    int64  `json:"org_id,omitempty"`
	OrgName  string `json:"org,omitempty"`
	UserID   int64  `json:"-"`
	Username string `json:"username,omitempty"`
	KeyID    string `json:"key_id,omitempty"`
	KeyName  string `json:"key_name,omitempty"`
	Model    string `json:"model,omitempty"`
	Source   Source `json:"source,omitempty"`
	Totals
}

// Status is the state of the rollup.
type Status struct {
	Watermark time.Time // first day the next rollup recomputes (zero before the first run)
	LastRunAt time.Time // zero before the first run
}

// RollupResult reports one rollup run.
type RollupResult struct {
	From      time.Time // first day recomputed
	Rows      int64     // usage_daily rows written
	Watermark time.Time // first day the next run recomputes
}

// Store is the persistence of usage (implemented by internal/persistence/postgres).
type Store interface {
	// Rollup recomputes usage_daily from the raw holds for every day from the watermark (or from, when it is
	// earlier and non-zero) up to now, in one transaction, and moves the watermark. It is idempotent and safe to
	// run concurrently (runs are serialised) and repeatedly.
	Rollup(ctx context.Context, from time.Time) (RollupResult, error)
	// Usage returns the grouped totals of the rows q selects, ordered by the grouped dimensions.
	Usage(ctx context.Context, q Query) ([]Group, error)
	// Reconcile recomputes month (its first day, UTC) from the raw holds and model requests and compares it with
	// usage_daily and with quota_budgets, in one snapshot. With rollupFirst the rollup runs first in the same snapshot.
	Reconcile(ctx context.Context, month time.Time, rollupFirst bool) (Report, error)
	// Status returns the rollup state.
	Status(ctx context.Context) (Status, error)
	// UserHasUsage reports whether userID has any usage_daily row in orgID (a former member's history stays the org's).
	UserHasUsage(ctx context.Context, orgID, userID int64) (bool, error)
}

// Month returns the first day of t's UTC month.
func Month(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// ParseMonth parses YYYY-MM.
func ParseMonth(s string) (time.Time, error) {
	m, err := time.Parse("2006-01", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("usage: month %q: want YYYY-MM", s)
	}
	return m, nil
}

// ParseDay parses YYYY-MM-DD as a UTC day.
func ParseDay(s string) (time.Time, error) {
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("usage: day %q: want YYYY-MM-DD", s)
	}
	return d, nil
}

// MonthRange returns the first and the last day of month.
func MonthRange(month time.Time) (from, to time.Time) {
	from = Month(month)
	return from, from.AddDate(0, 1, -1)
}
