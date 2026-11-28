package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

type handoffAtPublication struct {
	*call.Coordinator
	before func()
}

func (a *handoffAtPublication) PublishWorkspace(ctx context.Context, task, attempt, subrun string, publish func() error) (call.Result, error) {
	if a.before != nil {
		before := a.before
		a.before = nil
		before()
	}
	return a.Coordinator.PublishWorkspace(ctx, task, attempt, subrun, publish)
}

type settledWorkspaceExec struct {
	calls    int
	response call.Result
}

func (e *settledWorkspaceExec) ExecShell(context.Context, call.ShellInvoke) (call.Result, error) {
	e.calls++
	return e.response, nil
}

func TestWorkspaceHeadFencedAtPublication(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace publication requires directory fsync")
	}
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	ctx := t.Context()
	for _, op := range []string{"write", "delete", "exec"} {
		t.Run(op, func(t *testing.T) {
			fixture(t, s, op)
			objects, err := blob.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			services := chainServices{s}
			coord, err := call.New(call.Config{Store: s, Blobs: objects, Events: services})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(coord.Close)
			// Real blob and file manager; the executor has already produced a settled response.
			payload := []byte(`{"status":"completed","workspace_staged":true,"outputs":[]}`)
			ref, err := objects.Put(ctx, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			exec := &settledWorkspaceExec{response: call.Result{Status: 200, Body: payload, BlobSHA256: ref.SHA256}}
			access := &handoffAtPublication{Coordinator: coord}
			dir := t.TempDir()
			now := time.Now()
			m, err := workspace.New(workspace.Config{Dir: dir, Blobs: objects, Tasks: services, Exec: exec, Access: access, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(m.Close)
			r := workspace.Request{TaskID: op, AttemptID: "att-" + op, Body: []byte(`{"path":"data","content":"original"}`)}
			if res, err := m.Write(ctx, r); err != nil || res.Status != 200 {
				t.Fatalf("seed: %+v %v", res, err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.json"))
			if err != nil || len(files) != 1 {
				t.Fatal(files, err)
			}
			readHead := func() []byte {
				t.Helper()
				data, err := os.ReadFile(files[0])
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			seed := readHead()
			var injected bool
			access.before = func() {
				staged, err := filepath.Glob(filepath.Join(dir, ".tmp-*"))
				if err != nil || len(staged) != 1 {
					t.Fatalf("staged local replacement: %v %v", staged, err)
				}
				if op == "exec" && exec.calls != 1 {
					t.Fatal("shell result has not settled")
				}
				if err := s.RevokeAttemptAccess(ctx, r.AttemptID, "handoff"); err != nil {
					t.Fatal(err)
				}
				retryWithNewAttempt(t, s, op)
				injected = true
			}
			request := m.Write
			switch op {
			case "write":
				r.Body = []byte(`{"path":"data","content":"new"}`)
			case "delete":
				r.Body = []byte(`{"path":"data","delete":true}`)
			case "exec":
				request = m.Exec
				r.CallID = "shell-1"
				r.Body = []byte(`{"command":"fixture"}`)
			}
			res, err := request(ctx, r)
			if err != nil || res.Status != 403 || res.Code != persistence.CodeAccessRevoked || !injected {
				t.Fatalf("late publish: %+v %v injected=%t", res, err, injected)
			}
			if !bytes.Equal(seed, readHead()) {
				t.Fatal("stale request changed head, receipts or write counters")
			}
			staged, err := filepath.Glob(filepath.Join(dir, ".tmp-*"))
			if err != nil || len(staged) != 0 {
				t.Fatalf("staged temp remained: %v %v", staged, err)
			}
			r.AttemptID = "att2-" + op
			if res, err := request(ctx, r); err != nil || res.Status != 200 {
				t.Fatalf("successor: %+v %v", res, err)
			}
			if bytes.Equal(seed, readHead()) {
				t.Fatal("successor did not publish")
			}
			if op == "exec" {
				replay, err := request(ctx, r)
				if err != nil || !replay.Replayed || exec.calls != 2 {
					t.Fatalf("successor receipt: replay=%t executor calls=%d err=%v", replay.Replayed, exec.calls, err)
				}
			}
			r.Body = []byte(`{"path":"data"}`)
			out, err := m.Read(ctx, r)
			if err != nil {
				t.Fatal(err)
			}
			if op == "write" {
				var got struct{ Content string }
				if err := json.Unmarshal(out.Body, &got); err != nil || out.Status != 200 || got.Content != "new" {
					t.Fatalf("new head: %+v %v", out, err)
				}
			} else if out.Status != 404 {
				t.Fatalf("removed file still exists: %+v", out)
			}
			t.Logf("operation=%s old=403/%s original_state_identical=true successor=200", op, res.Code)
		})
	}
}

func TestWorkspacePublicationLocksAndAdmission(t *testing.T) {
	ctx := t.Context()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second})
	fixture(t, s, "order")
	n := 0
	err := s.WithWorkspaceWrite(ctx, "order", "att-order", "", func() error {
		n++
		for _, q := range []string{
			"SELECT 1 FROM tasks WHERE task_id='order' FOR UPDATE NOWAIT",
			"SELECT 1 FROM task_control WHERE task_id='order' FOR UPDATE NOWAIT",
			"SELECT 1 FROM attempt_access WHERE attempt_id='att-order' FOR UPDATE NOWAIT",
		} {
			var i int
			err := s.pool.QueryRow(ctx, q).Scan(&i)
			var locked *pgconn.PgError
			if !errors.As(err, &locked) || locked.Code != "55P03" {
				t.Fatalf("expected conflicting write lock: %v", err)
			}
		}
		return nil
	})
	if err != nil || n != 1 {
		t.Fatalf("authorized publication: calls=%d err=%v", n, err)
	}
	noPublish := func() error { t.Fatal("denied callback executed"); return nil }
	expectRejected(t, s.WithWorkspaceWrite(ctx, "order", "att-order", "unregistered", noPublish), persistence.CodeSubrunClosed)
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE task_id='order'"); err != nil {
		t.Fatal(err)
	}
	expectRejected(t, s.WithWorkspaceWrite(ctx, "order", "att-order", "", noPublish), persistence.CodeNotCurrentAttempt)
	if err := s.RevokeAttemptAccess(ctx, "att-order", "handoff"); err != nil {
		t.Fatal(err)
	}
	expectRejected(t, s.WithWorkspaceWrite(ctx, "order", "att-order", "", noPublish), persistence.CodeAccessRevoked)
	fixture(t, s, "cancel")
	if _, err := s.pool.Exec(ctx, "UPDATE task_control SET desired='cancel' WHERE task_id='cancel'"); err != nil {
		t.Fatal(err)
	}
	expectRejected(t, s.WithWorkspaceWrite(ctx, "cancel", "att-cancel", "", noPublish), persistence.CodeCancelRequested)
}

func TestWorkspacePublicationOrdersConcurrentRevocation(t *testing.T) {
	ctx := t.Context()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second, LockTimeout: 5 * time.Second})
	fixture(t, s, "ordered")
	revoked := make(chan error, 1)
	err := s.WithWorkspaceWrite(ctx, "ordered", "att-ordered", "", func() error {
		go func() { revoked <- s.RevokeAttemptAccess(ctx, "att-ordered", "handoff") }()
		waitLockWait(t, s)
		select {
		case err := <-revoked:
			t.Fatalf("revocation completed during publication: %v", err)
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-revoked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revocation stayed blocked after publication")
	}
	expectRejected(t, s.WithWorkspaceWrite(ctx, "ordered", "att-ordered", "", func() error { t.Fatal("late publish"); return nil }), persistence.CodeAccessRevoked)
}

func TestWorkspaceLeaseExpiresWhileAcquiringAccess(t *testing.T) {
	ctx := t.Context()
	s := newStore(t, Options{AttemptLeaseTTL: 120 * time.Second, LockTimeout: 5 * time.Second})
	fixture(t, s, "near-expiry")
	hold, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(hold)
	if _, err := hold.Exec(ctx, "SELECT 1 FROM attempt_access WHERE attempt_id='att-near-expiry' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET lease_expires_at=clock_timestamp()+interval '1 second' WHERE task_id='near-expiry'"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.WithWorkspaceWrite(ctx, "near-expiry", "att-near-expiry", "", func() error { return errors.New("expired callback executed") })
	}()
	waitLockWait(t, s)
	if _, err := s.pool.Exec(ctx, "SELECT pg_sleep(GREATEST(0,extract(epoch FROM lease_expires_at-clock_timestamp()))::float8) FROM tasks WHERE task_id='near-expiry'"); err != nil {
		t.Fatal(err)
	}
	if err := hold.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	expectRejected(t, <-done, persistence.CodeNotCurrentAttempt)
}
