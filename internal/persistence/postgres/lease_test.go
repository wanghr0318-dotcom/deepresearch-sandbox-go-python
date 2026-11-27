package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/session"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

func TestAttemptLeaseAndPortableCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{AttemptLeaseTTL: time.Minute})
	fixture(t, s, "t1")
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, AttemptID: "att-t1", StepID: "step", State: []byte(`{"cursor":1}`)}
	for i := 1; i <= 3; i++ {
		cp.CheckpointID = fmt.Sprintf("C%d", i)
		cp.WorkspaceManifestSHA256 = strings.Repeat(fmt.Sprint(i), 64)
		if _, err := s.CommitCheckpoint(ctx, cp); err != nil {
			t.Fatal(err)
		}
	}
	altered := cp
	altered.WorkspaceManifestSHA256 = strings.Repeat("0", 64)
	if _, err := s.CommitCheckpoint(ctx, altered); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	if err := s.RenewAttemptLease(ctx, "t1", "att-t1"); err != nil {
		t.Fatal(err)
	}
	// Force expiry on the DB clock; never depend on wall-clock sleeps.
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE task_id='t1'"); err != nil {
		t.Fatal(err)
	}
	if expired, err := s.ExpiredAttemptLease(ctx, "t1", "att-t1"); err != nil || !expired {
		t.Fatalf("expired: %t %v", expired, err)
	}
	err := s.RenewAttemptLease(ctx, "t1", "att-t1")
	expectRejected(t, err, persistence.CodeStaleAttempt)
	_, err = s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: cp.AttemptID, Kind: "result", Ref: "late"})
	expectRejected(t, err, persistence.CodeStaleAttempt)
	_, err = s.CommitCheckpoint(ctx, cp)
	expectRejected(t, err, persistence.CodeStaleAttempt)
	cp.CheckpointID = "C4"
	_, err = s.CommitCheckpoint(ctx, cp)
	expectRejected(t, err, persistence.CodeStaleAttempt)
	got, err := s.LoadTask(ctx, "t1")
	if err != nil || got.Latest.CheckpointID != "C3" || got.Latest.WorkspaceManifestSHA256 != cp.WorkspaceManifestSHA256 {
		t.Fatalf("restore %+v: %v", got.Latest, err)
	}
	facts, err := s.CheckAccess(ctx, "t1", "att-t1", "")
	if err != nil || facts.Current {
		t.Fatalf("expired access: %+v %v", facts, err)
	}
	v := verdict("t1", 1)
	v.TaskStatus = "succeeded"
	v.TaskStatusReason = "succeeded"
	_, err = s.FinalizeAttempt(ctx, v)
	expectRejected(t, err, persistence.CodeLeaseExpired)
	// Control-plane fault adjudication is still permitted. Two claimants race
	// after confirmed cleanup; the production CreateAttempt transaction picks one.
	v.TaskStatus = "queued"
	v.TaskStatusReason = "worker_error"
	if _, err = s.FinalizeAttempt(ctx, v); err != nil {
		t.Fatal(err)
	}
	stopEnv(t, s, "env-t1")
	out := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			_, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: fmt.Sprintf("replacement-%d", i), AttemptNo: 2, EnvID: fmt.Sprintf("env-%d", i), Retry: task.RetryFault})
			out <- err
		}(i)
	}
	wins := 0
	for i := 0; i < 2; i++ {
		if err := <-out; err == nil {
			wins++
		} else if !errors.Is(err, persistence.ErrConflict) && !errors.Is(err, persistence.ErrRejected) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("replacement winners: %d", wins)
	}
	err = s.RenewAttemptLease(ctx, "t1", "att-t1")
	expectRejected(t, err, persistence.CodeStaleAttempt)
}

func TestFencedSessionCanMoveBeforePhysicalCleanup(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{AttemptLeaseTTL: time.Minute})
	alice := mustUser(t, s, "alice")
	liveSession(t, s, "s1", alice)
	mustTurn(t, s, turnReq("s1", "ta", alice))
	a := turnAttempt("s1", "ta", 1)
	if _, err := s.CreateAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeAttempt(ctx, turnVerdictFor(a, 1, "queued", "worker_crashed")); err != nil {
		t.Fatal(err)
	}
	checkFence := func(want bool) {
		t.Helper()
		got, err := s.FenceExpiredSession(ctx, a.EnvID, a.AttemptID)
		if err != nil || got != want {
			t.Fatalf("fenced %v want %v: %v", got, want, err)
		}
	}
	checkFence(false)
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE task_id='ta'"); err != nil {
		t.Fatal(err)
	}
	checkFence(false)
	if err := s.RevokeAttemptAccess(ctx, a.AttemptID, "expired"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "INSERT INTO environments(env_id,kind,attempt_id,status) VALUES('child','exec',$1,'running')", a.AttemptID); err != nil {
		t.Fatal(err)
	}
	checkFence(false)
	stopEnv(t, s, "child")
	checkFence(true)
	checkFence(true)
	if _, err := s.EndIncarnation(ctx, a.IncarnationID, "lease_fenced"); err != nil {
		t.Fatal(err)
	}
	st, err := s.LoadSession(ctx, "s1")
	if err != nil || !st.PendingFencedStop {
		t.Fatalf("session pending stop %v: %v", st.PendingFencedStop, err)
	}
	if _, err := s.CreateIncarnation(ctx, session.NewIncarnation{IncarnationID: "inc2", SessionID: "s1", EnvID: "senv2", FromRowVersion: st.RowVersion}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetIncarnationStatus(ctx, "inc2", []string{session.IncStarting}, session.IncIdle); err != nil {
		t.Fatal(err)
	}
	replacement := task.NewAttempt{TaskID: "ta", AttemptID: "att2-ta", AttemptNo: 2, EnvID: "senv2", Retry: task.RetryFault, SessionID: "s1", IncarnationID: "inc2"}
	if _, err := s.CreateAttempt(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	candidates, err := s.ListCleanupCandidates(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range candidates {
		if c.EnvID == a.EnvID {
			found = c.StoppedAt == nil && c.FencedAt != nil
		}
	}
	if !found {
		t.Fatalf("missing fenced sandbox %+v", candidates)
	}
	stopEnv(t, s, a.EnvID)
	st, err = s.LoadSession(ctx, "s1")
	if err != nil || st.PendingFencedStop {
		t.Fatalf("still pending %v: %v", st.PendingFencedStop, err)
	}
}

// Offsets use the PostgreSQL clock. At T we place the expiry at the current
// statement timestamp; the first subsequent observer must see the expired side.
func TestLeaseBoundaryOnDatabaseClock(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	s := newStore(t, Options{AttemptLeaseTTL: ttl})
	for _, tc := range []struct {
		id      string
		elapsed time.Duration
		expired bool
	}{{"half", ttl / 2, false}, {"edge", ttl, true}, {"after", 3 * ttl / 2, true}} {
		t.Run(tc.id, func(t *testing.T) {
			fixture(t, s, tc.id)
			if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp()+$2::bigint*interval '1 microsecond' WHERE task_id=$1", tc.id, (ttl - tc.elapsed).Microseconds()); err != nil {
				t.Fatal(err)
			}
			owner := "att-" + tc.id
			expired, err := s.ExpiredAttemptLease(ctx, tc.id, owner)
			if err != nil || expired != tc.expired {
				t.Fatalf("expired=%v want=%v: %v", expired, tc.expired, err)
			}
			err = s.RenewAttemptLease(ctx, tc.id, owner)
			if tc.expired {
				expectRejected(t, err, persistence.CodeStaleAttempt)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
