// Package quota is the monthly money budgets of the model gateway (design 2026-10-11 §6.2): one budget per level
// (org, user, key) per UTC month; a hold reserves the same amount at every level of its principal in one PostgreSQL
// transaction (locked in the fixed order org → user → key), and is later settled, released, or — when its deadline
// passes without a settlement — swept to charged_unknown at its full amount.
//
// PostgreSQL is the source of truth; internal/persistence/postgres implements Holder and Store. A NULL limit means
// unlimited at that level. Runtime calls made by sandboxes place their holds inside the task ledger's own reservation
// transaction (source task_call), so a call never runs without both.
package quota

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// Level is a budget and limit level.
type Level string

// The levels, in lock order.
const (
	LevelOrg  Level = "org"
	LevelUser Level = "user"
	LevelKey  Level = "key"
)

// ParseLevel parses "org", "user" or "key".
func ParseLevel(s string) (Level, error) {
	switch l := Level(s); l {
	case LevelOrg, LevelUser, LevelKey:
		return l, nil
	}
	return "", fmt.Errorf("quota: unknown level %q (want org, user or key)", s)
}

// Scope is the principal a hold is charged to: the org always, the user and key when present (UserID 0 and KeyID ""
// mean absent, e.g. for the operator). OrgID 0 is the operator's reserved org "operator".
type Scope struct {
	OrgID, UserID int64
	KeyID         string
}

// ScopeOf returns the scope a request by p is charged to.
func ScopeOf(p tenant.Principal) Scope {
	return Scope{OrgID: p.OrgID, UserID: p.UserID, KeyID: p.KeyID}
}

// Target is one level of a scope.
type Target struct {
	Level Level
	ID    string // org and user: decimal ID; key: key ID
}

// Targets returns the scope's levels in lock order (org, user, key). The org must be resolved (OrgID ≠ 0).
func (s Scope) Targets() []Target {
	out := []Target{{LevelOrg, strconv.FormatInt(s.OrgID, 10)}}
	if s.UserID != 0 {
		out = append(out, Target{LevelUser, strconv.FormatInt(s.UserID, 10)})
	}
	if s.KeyID != "" {
		out = append(out, Target{LevelKey, s.KeyID})
	}
	return out
}

// Source is what a hold pays for.
type Source string

const (
	SourceTaskCall     Source = "task_call"     // a Gateway call made by a sandbox (ref: task/call#try)
	SourceModelRequest Source = "model_request" // a model API request (ref: model_requests.id)
)

// HoldID identifies a hold.
type HoldID string

// Hold states.
const (
	StateHeld           = "held"
	StateSettled        = "settled"
	StateReleased       = "released"
	StateChargedUnknown = "charged_unknown"
)

// InsufficientError reports that the budget at Level cannot cover the amount; nothing was reserved.
type InsufficientError struct {
	Level     Level
	ID        string
	Limit     int64 // the level's monthly limit
	Available int64 // limit − spent − reserved − unknown (may be negative)
	Amount    int64
}

func (e *InsufficientError) Error() string {
	return fmt.Sprintf("quota: insufficient %s budget (%s %s: available %d of %d, need %d)", e.Level, e.Level, e.ID,
		e.Available, e.Limit, e.Amount)
}

// ErrInsufficient matches every *InsufficientError with errors.Is.
var ErrInsufficient = errors.New("quota: insufficient budget")

// Is makes errors.Is(err, ErrInsufficient) true.
func (e *InsufficientError) Is(target error) bool { return target == ErrInsufficient }

// Holder reserves, settles and releases holds (design §11).
type Holder interface {
	// Reserve reserves amountMicro at every level of s for this month in one transaction, or returns
	// *InsufficientError{Level} and changes nothing.
	Reserve(ctx context.Context, s Scope, source Source, ref string, amountMicro int64, deadline time.Time) (HoldID, error)
	// Settle moves the hold's amount from reserved to spent (chargedMicro; may exceed the amount) or, when unknown,
	// to unknown (chargedMicro, at least the amount), at every level. Settling a hold that is no longer held is a no-op.
	Settle(ctx context.Context, id HoldID, chargedMicro int64, unknown bool) error
	// Release returns the reservation (no try was sent). Releasing a hold that is no longer held is a no-op.
	Release(ctx context.Context, id HoldID) error
}

// Period returns the budget period of t: the first day of its UTC month.
func Period(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// Budget is one level's budget for one period. Limit nil means unlimited.
type Budget struct {
	Level         Level
	ID            string
	Period        time.Time
	LimitMicro    *int64
	ReservedMicro int64
	SpentMicro    int64
	UnknownMicro  int64
}

// Used returns spent + reserved + unknown.
func (b Budget) Used() int64 { return b.SpentMicro + b.ReservedMicro + b.UnknownMicro }

// Limits is the rate and concurrency limits of one level (the limits table). nil fields mean no limit at that level.
type Limits struct {
	RPM          *int64 `json:"rpm"`
	TPM          *int64 `json:"tpm"`
	MaxSandboxes *int64 `json:"max_sandboxes"`
	MaxStreams   *int64 `json:"max_streams"`
}

// Empty reports whether no field is set.
func (l Limits) Empty() bool {
	return l.RPM == nil && l.TPM == nil && l.MaxSandboxes == nil && l.MaxStreams == nil
}

// Exceeding returns the name of the first field of l that is above the same field of parent (both set), or "".
// A level cannot be given more than its org allows (design §8).
func (l Limits) Exceeding(parent Limits) string {
	for _, f := range []struct {
		name       string
		own, outer *int64
	}{{"rpm", l.RPM, parent.RPM}, {"tpm", l.TPM, parent.TPM}, {"max_sandboxes", l.MaxSandboxes, parent.MaxSandboxes},
		{"max_streams", l.MaxStreams, parent.MaxStreams}} {
		if f.own != nil && f.outer != nil && *f.own > *f.outer {
			return f.name
		}
	}
	return ""
}

// Store is the management side of quotas and limits (implemented by internal/persistence/postgres). Targets are
// resolved IDs (Target.ID as in Scope.Targets).
type Store interface {
	Holder
	// Sweep turns held holds whose deadline has passed into charged_unknown at their full amount (at most limit per
	// call) and returns how many it converted. Runtime-call holds whose task reservation is still held are left to
	// the task ledger (they follow its settlement or restart conversion).
	Sweep(ctx context.Context, limit int) (int, error)
	// GetLimits returns the limits of t (all nil when none are set).
	GetLimits(ctx context.Context, t Target) (Limits, error)
	// PutLimits replaces the limits of t (all nil deletes the row).
	PutLimits(ctx context.Context, t Target, l Limits) error
	// LimitsFor returns the limits of every level of s, keyed by level (levels without a row are absent).
	LimitsFor(ctx context.Context, s Scope) (map[Level]Limits, error)
	// GetBudget returns t's budget for period (a row that does not exist yet shows the limit it would be created
	// with and zero usage).
	GetBudget(ctx context.Context, t Target, period time.Time) (Budget, error)
	// PutBudget sets t's monthly limit (nil = unlimited) for period; later months created afterwards carry it forward.
	// A limit below the period's current usage is rejected with *BelowUsageError.
	PutBudget(ctx context.Context, t Target, period time.Time, limitMicro *int64) (Budget, error)
	// OperatorOrg returns the ID of the reserved org "operator".
	OperatorOrg(ctx context.Context) (int64, error)
}

// BelowUsageError reports a budget limit below the period's current usage.
type BelowUsageError struct{ Used, Limit int64 }

func (e *BelowUsageError) Error() string {
	return fmt.Sprintf("quota: limit %d is below this month's usage %d", e.Limit, e.Used)
}
