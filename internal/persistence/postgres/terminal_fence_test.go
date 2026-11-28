package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
)

func TestTerminalProposalRequiresActiveCurrentAttempt(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	fixture(t, s, "first")
	fixture(t, s, "repeat")
	accepted := runner.TerminalProposal{AttemptID: "att-repeat", Kind: "result", Ref: "seq:7"}
	for i := 0; i < 2; i++ {
		got, err := s.RecordTerminalProposal(ctx, accepted)
		if err != nil || got != accepted {
			t.Fatalf("active submission %d: %+v, %v", i+1, got, err)
		}
	}
	for _, id := range []string{"att-first", "att-repeat"} {
		if err := s.RevokeAttemptAccess(ctx, id, "handoff"); err != nil {
			t.Fatal(err)
		}
		if n := count(t, s, `SELECT count(*) FROM tasks t JOIN attempts a ON a.attempt_id=t.current_attempt_id
            JOIN attempt_access aa USING (attempt_id) WHERE a.attempt_id=$1 AND aa.state='revoked'
            AND a.verdict_hash IS NULL AND t.lease_expires_at > clock_timestamp()`, id); n != 1 {
			t.Fatal("invalid pre-adjudication fixture", id)
		}
	}
	for _, p := range []runner.TerminalProposal{{AttemptID: "att-first", Kind: "result", Ref: "seq:2"}, accepted} {
		_, err := s.RecordTerminalProposal(ctx, p)
		expectRejected(t, err, persistence.CodeStaleAttempt)
	}
	if _, err := s.GetTerminalProposal(ctx, "att-first"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("first proposal after rejection: %v", err)
	}
	if got, err := s.GetTerminalProposal(ctx, "att-repeat"); err != nil || got != accepted {
		t.Fatalf("previous proposal: %+v, %v", got, err)
	}
}

func TestAttemptFenceHoldsAuthorizationUntilCommit(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	fixture(t, s, "during")
	authorized, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer authorized.Rollback(ctx)
	if err := lockTaskShared(ctx, authorized, "during"); err != nil {
		t.Fatal(err)
	}
	if err := fenceAttempt(ctx, authorized, "during", "att-during"); err != nil {
		t.Fatal(err)
	}
	// A non-blocking update lock needs to conflict while the writer is authorized.
	var state string
	err = s.pool.QueryRow(ctx, `SELECT state FROM attempt_access WHERE attempt_id='att-during' FOR UPDATE NOWAIT`).Scan(&state)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
		t.Fatalf("expected lock_not_available, got %v", err)
	}
	if err := authorized.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAttemptAccess(ctx, "att-during", "handoff"); err != nil {
		t.Fatal(err)
	}
}
