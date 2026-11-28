package postgres

import (
	"runtime"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
)

// A synthetic client reuses the previous attempt ID after the real store
// has committed its replacement. This exercises application methods, not HTTP
// authentication, network partition timing or survival of the original worker.
func TestReconnectedOldAttemptWrites(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace publication requires directory fsync")
	}
	ctx := t.Context()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	const id, old, current = "t1", "att-t1", "att2-t1"
	fixture(t, s, id)
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE task_id=$1", id); err != nil {
		t.Fatal(err)
	}
	retryWithNewAttempt(t, s, id)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	services := chainServices{s}
	coord, err := call.New(call.Config{Store: s, Blobs: blobs, Events: services})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Close)
	archive, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := workspace.New(workspace.Config{Dir: t.TempDir(), Blobs: blobs, Access: coord, Exec: coord, Tasks: services, Archive: &chainArchive{objects: archive}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	writeFile := func(attempt, body string) call.Result {
		t.Helper()
		r, err := m.Write(ctx, workspace.Request{TaskID: id, AttemptID: attempt, Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	head := func() string {
		t.Helper()
		h, err := m.Snapshot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	if r := writeFile(current, `{"path":"cursor.txt","content":"successor"}`); r.Status != 200 {
		t.Fatalf("new owner write: %+v", r)
	}
	wantHead := head()
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: id}, AttemptID: current, CheckpointID: "current", StepID: "step", State: []byte(`{"owner":"new"}`), WorkspaceManifestSHA256: wantHead}
	if _, err := s.CommitCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: current, Kind: "result", Ref: "successor-result"}); err != nil {
		t.Fatal(err)
	}
	lease := func() time.Time {
		t.Helper()
		var until time.Time
		if err := s.pool.QueryRow(ctx, "SELECT lease_expires_at FROM tasks WHERE task_id=$1 AND current_attempt_id=$2", id, current).Scan(&until); err != nil {
			t.Fatal(err)
		}
		return until
	}
	before, expires := snapshot(t, s, id), lease()
	t.Run("renew", func(t *testing.T) {
		expectRejected(t, s.RenewAttemptLease(ctx, id, old), persistence.CodeStaleAttempt)
		t.Log("attempts=1 rejected=1 accepted=0 layer=pg code=stale_attempt")
	})
	t.Run("checkpoint", func(t *testing.T) {
		cp.AttemptID, cp.CheckpointID, cp.State = old, "late", []byte(`{"owner":"old"}`)
		_, err := s.CommitCheckpoint(ctx, cp)
		expectRejected(t, err, persistence.CodeStaleAttempt)
		t.Log("attempts=1 rejected=1 accepted=0 layer=pg code=stale_attempt")
	})
	t.Run("file_head", func(t *testing.T) {
		r := writeFile(old, `{"path":"cursor.txt","content":"late-writer"}`)
		if r.Code != persistence.CodeAccessRevoked && r.Code != persistence.CodeNotCurrentAttempt {
			t.Fatalf("old writer response: %+v", r)
		}
		t.Logf("attempts=1 rejected=1 accepted=0 layer=gateway_workspace_access status=%d code=%s", r.Status, r.Code)
	})
	t.Run("final_result", func(t *testing.T) {
		_, err := s.RecordTerminalProposal(ctx, runner.TerminalProposal{AttemptID: old, Kind: "result", Ref: "late-result"})
		expectRejected(t, err, persistence.CodeStaleAttempt)
		t.Log("attempts=1 rejected=1 accepted=0 layer=pg code=stale_attempt")
	})
	p, err := s.GetTerminalProposal(ctx, current)
	if err != nil || p.Kind != "result" || p.Ref != "successor-result" || snapshot(t, s, id) != before || head() != wantHead || !expires.Equal(lease()) {
		t.Fatalf("successor state changed: %+v err=%v", p, err)
	}
	if count(t, s, "SELECT count(*) FROM attempts WHERE attempt_id=$1 AND terminal_proposal IS NOT NULL", old) != 0 {
		t.Fatal("old attempt acquired a terminal proposal")
	}
	t.Logf("successor=%s checkpoint=current head=%s result=successor-result lease_unchanged=true", current, wantHead)
}
