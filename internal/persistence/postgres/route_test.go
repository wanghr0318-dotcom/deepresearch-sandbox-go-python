package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// Model fallback (docs/design/2026-10-10-model-fallback-design.md): the per-try provider/skipped/hedge
// audit columns, the hedge reservation rule and the provider-aware ReserveTry idempotency.

func reserveRoute(s *Store, taskID, callID string, est int64, provider, skipped string, hedge bool) (call.Try, error) {
	return s.ReserveTry(context.Background(), call.ReserveTryRequest{TaskID: taskID, CallID: callID, AttemptID: "att-" + taskID,
		EnvID: "env-" + taskID, EstimateMicro: est, MaxTries: 3, Provider: provider, Skipped: skipped, Hedge: hedge})
}

// TestTryRouteColumnsRoundTrip: provider/skipped/hedge are written by ReserveTry and read back by LoadCall
// and Inspect; a try made without a provider reads as empty/false (single-provider configuration).
func TestTryRouteColumnsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	if n := count(t, s, "SELECT count(*) FROM schema_migrations WHERE version = 11"); n != 1 {
		t.Fatal("migration 0011 not applied")
	}
	gwFixture(t, s, "t1", 10_000)
	beginCall(t, s, "t1", "c1")
	tr, err := reserveRoute(s, "t1", "c1", 100, "primary", "", false)
	if err != nil {
		t.Fatal(err)
	}
	settle(t, s, call.Settlement{Try: tr, Outcome: "retryable", Error: "upstream_unavailable"})
	tr, err = reserveRoute(s, "t1", "c1", 200, "backup", "primary:tried", false)
	if err != nil {
		t.Fatal(err)
	}
	settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 50, ResultSHA256: strings.Repeat("ab", 32), ResultSize: 3})
	beginCall(t, s, "t1", "c2")
	tr = mustReserve(t, s, "t1", "c2", 10)
	settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 5, ResultSHA256: strings.Repeat("cd", 32), ResultSize: 3})

	_, tries, err := s.LoadCall(ctx, "t1", "c1")
	if err != nil || len(tries) != 2 {
		t.Fatalf("LoadCall: %+v %v", tries, err)
	}
	if tries[0].Provider != "primary" || tries[0].Skipped != "" || tries[1].Provider != "backup" ||
		tries[1].Skipped != "primary:tried" || tries[0].Hedge || tries[1].Hedge {
		t.Fatalf("tries %+v", tries)
	}
	_, tries, _ = s.LoadCall(ctx, "t1", "c2")
	if len(tries) != 1 || tries[0].Provider != "" || tries[0].Skipped != "" || tries[0].Hedge {
		t.Fatalf("single-provider try %+v", tries)
	}
	in, err := s.Inspect(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range in.Calls {
		if c.CallID == "c1" {
			found = len(c.Tries) == 2 && c.Tries[1].Provider == "backup" && c.Tries[1].Skipped == "primary:tried"
		}
	}
	if !found {
		t.Fatalf("inspect does not show the route columns: %+v", in.Calls)
	}
}

// TestHedgeReservation: while one try is held, a second concurrent try is allowed only for an explicit
// hedge request for a different provider, at most two legs; normal requests still get call_in_progress.
// The idempotency match includes the provider, so the hedge is not mistaken for a retry of the primary
// reservation, and a retried hedge request returns the hedge try. Settling the loser first and the winner
// last leaves the call completed with the winner's result; I3 holds throughout.
func TestHedgeReservation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 10_000)
	beginCall(t, s, "t1", "c1")
	a, err := reserveRoute(s, "t1", "c1", 100, "primary", "", false)
	if err != nil {
		t.Fatal(err)
	}
	// Same provider and estimate: indistinguishable from a retry of the primary reservation → the primary try
	// (idempotent), never a second leg; with another estimate it is refused.
	if same, err := reserveRoute(s, "t1", "c1", 100, "primary", "", true); err != nil || same.ReservationID != a.ReservationID {
		t.Fatalf("same-provider hedge: %+v %v", same, err)
	}
	_, err = reserveRoute(s, "t1", "c1", 120, "primary", "", true)
	expectRejected(t, err, persistence.CodeCallInProgress)
	// Not a hedge request, other provider: call_in_progress as before.
	_, err = reserveRoute(s, "t1", "c1", 100, "backup", "", false)
	expectRejected(t, err, persistence.CodeCallInProgress)
	// Same estimate, other provider, hedge: a new try (not the primary's try returned by idempotency).
	b, err := reserveRoute(s, "t1", "c1", 100, "backup", "", true)
	if err != nil || b.TryNo != 2 || b.ReservationID == a.ReservationID {
		t.Fatalf("hedge reservation: %+v %v", b, err)
	}
	checkI3(t, s)
	// Retrying the hedge request returns the hedge try (idempotent), no third reservation.
	b2, err := reserveRoute(s, "t1", "c1", 100, "backup", "", true)
	if err != nil || b2.TryNo != 2 || b2.ReservationID != b.ReservationID {
		t.Fatalf("retried hedge: %+v %v", b2, err)
	}
	// A third leg is refused.
	_, err = reserveRoute(s, "t1", "c1", 100, "third", "", true)
	expectRejected(t, err, persistence.CodeCallInProgress)
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 10_000, ReservedMicro: 200})
	// Loser (primary, already sent) settled first as unknown, winner last as ok.
	settle(t, s, call.Settlement{Try: a, Outcome: "unknown", Error: "hedge_lost"})
	rec := settle(t, s, call.Settlement{Try: b, Outcome: "ok", ActualMicro: 40, ResultSHA256: strings.Repeat("ef", 32), ResultSize: 3})
	if rec.State != call.StateCompleted || rec.ResultRef != strings.Repeat("ef", 32) || !rec.PossibleExternalDuplicate {
		t.Fatalf("call after hedge %+v", rec)
	}
	expectBudget(t, s, "t1", call.Budget{LimitMicro: 10_000, SpentMicro: 40, UnknownMicro: 100})
	_, tries, _ := s.LoadCall(ctx, "t1", "c1")
	if len(tries) != 2 || !tries[1].Hedge || tries[0].Hedge {
		t.Fatalf("tries %+v", tries)
	}
	if vs, err := s.DBViolations(ctx); err != nil || len(vs) != 0 {
		t.Fatalf("invariants: %+v %v", vs, err)
	}
}

// TestHedgeCrashLedger: a crash while both legs are held converts both reservations to unknown and the
// call to unknown (the next request makes a new try), exactly like a single crashed try.
func TestHedgeCrashLedger(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	gwFixture(t, s, "t1", 10_000)
	beginCall(t, s, "t1", "c1")
	if _, err := reserveRoute(s, "t1", "c1", 100, "primary", "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := reserveRoute(s, "t1", "c1", 150, "backup", "", true); err != nil {
		t.Fatal(err)
	}
	conv, err := s.ConvertLedger(ctx)
	if err != nil || conv.Reservations != 2 || conv.UnknownMicro != 250 || conv.Calls != 1 {
		t.Fatalf("ConvertLedger %+v %v", conv, err)
	}
	checkI3(t, s)
	rec, tries, _ := s.LoadCall(ctx, "t1", "c1")
	if rec.State != call.StateUnknown || len(tries) != 2 || tries[0].Outcome != "unknown" || tries[1].Outcome != "unknown" {
		t.Fatalf("after conversion %+v %+v", rec, tries)
	}
	tr, err := reserveRoute(s, "t1", "c1", 100, "backup", "primary:circuit_open", false)
	if err != nil || tr.TryNo != 3 {
		t.Fatalf("new try after restart: %+v %v", tr, err)
	}
	rec = settle(t, s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 10, ResultSHA256: strings.Repeat("01", 32), ResultSize: 3})
	if rec.State != call.StateCompleted {
		t.Fatalf("%+v", rec)
	}
}
