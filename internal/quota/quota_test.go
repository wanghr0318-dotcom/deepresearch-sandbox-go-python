package quota

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

func TestTargetsInLockOrder(t *testing.T) {
	got := Scope{OrgID: 7, UserID: 3, KeyID: "k_x"}.Targets()
	want := []Target{{LevelOrg, "7"}, {LevelUser, "3"}, {LevelKey, "k_x"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%v", got)
	}
	if got := (Scope{OrgID: 1}).Targets(); len(got) != 1 || got[0] != (Target{LevelOrg, "1"}) {
		t.Fatalf("operator / org only: %v", got)
	}
	if s := ScopeOf(tenant.Principal{OrgID: 2, UserID: 5, KeyID: "k_y"}); s != (Scope{OrgID: 2, UserID: 5, KeyID: "k_y"}) {
		t.Fatalf("%+v", s)
	}
}

func TestPeriodIsUTCMonth(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	// 2026-11-01 05:00 in UTC+8 is still October in UTC.
	if p := Period(time.Date(2026, 11, 1, 5, 0, 0, 0, loc)); p != time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("%v", p)
	}
}

func TestParseLevelAndLimits(t *testing.T) {
	for _, s := range []string{"org", "user", "key"} {
		if l, err := ParseLevel(s); err != nil || string(l) != s {
			t.Fatal(s, err)
		}
	}
	if _, err := ParseLevel("team"); err == nil {
		t.Fatal("unknown level")
	}
	n := func(v int64) *int64 { return &v }
	org := Limits{RPM: n(100), MaxStreams: n(2)}
	if f := (Limits{RPM: n(101)}).Exceeding(org); f != "rpm" {
		t.Fatalf("%q", f)
	}
	if f := (Limits{RPM: n(100), TPM: n(1 << 40), MaxStreams: nil}).Exceeding(org); f != "" {
		t.Fatalf("equal, unset at the org, or unset here is fine: %q", f)
	}
	if !(Limits{}).Empty() || (Limits{MaxSandboxes: n(0)}).Empty() {
		t.Fatal("Empty")
	}
}

func TestInsufficientError(t *testing.T) {
	var err error = &InsufficientError{Level: LevelKey, ID: "k_x", Limit: 10, Available: 3, Amount: 5}
	if !errors.Is(err, ErrInsufficient) {
		t.Fatal("errors.Is")
	}
	var ie *InsufficientError
	if !errors.As(fmt.Errorf("wrapped: %w", err), &ie) || ie.Level != LevelKey {
		t.Fatal("errors.As")
	}
}
