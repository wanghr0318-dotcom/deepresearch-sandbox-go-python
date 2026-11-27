package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// The archive is independent of every attempt's data directory. On the selected
// upload it consumes actual bytes and fails before publishing a complete object.
type chainArchive struct {
	objects   *blob.Local
	interrupt bool
}

func (a *chainArchive) Get(_ context.Context, sha string) (io.ReadCloser, error) {
	return a.objects.Open(sha)
}
func (a *chainArchive) Put(ctx context.Context, sha string, body io.Reader, size int64) error {
	if a.interrupt {
		_, err := io.CopyN(io.Discard, body, 1)
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	got, err := a.objects.Put(ctx, body)
	if err == nil && (got.SHA256 != sha || got.Size != size) {
		return errors.New("invalid archived object")
	}
	return err
}

type chainServices struct{ store *Store }

func (v chainServices) TaskTerminal(ctx context.Context, id string) (bool, error) {
	s, err := v.store.LoadTask(ctx, id)
	return task.IsTerminal(s.Status), err
}
func (chainServices) ReplayDivergence(context.Context, string, string, string, string) error {
	return errors.New("unexpected call replay divergence")
}

func TestColdWorkspaceCheckpointChain(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace atomic state publication requires directory fsync; run on Linux")
	}
	ctx := t.Context()
	s := newStore(t, Options{})
	fixture(t, s, "t1")
	objects, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	archive := &chainArchive{objects: objects}
	cold := func() *workspace.Manager {
		t.Helper()
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
		m, err := workspace.New(workspace.Config{Dir: t.TempDir(), Blobs: blobs, Access: coord, Exec: coord, Tasks: services, Archive: archive})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		return m
	}
	m := cold()
	attempt := "att-t1"
	write := func(value string) {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"path": "cursor.txt", "content": value})
		r, err := m.Write(ctx, workspace.Request{TaskID: "t1", AttemptID: attempt, Body: body})
		if err != nil || r.Status != 200 {
			t.Fatalf("write %s: %+v %v", value, r, err)
		}
	}
	publish := func(id string) error {
		sha, err := m.Snapshot(ctx, "t1")
		if err != nil {
			return err
		}
		_, err = s.CommitCheckpoint(ctx, runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "t1"}, AttemptID: attempt,
			CheckpointID: id, StepID: "step", State: json.RawMessage(fmt.Sprintf("{\"last_tool\":%q}", id)), WorkspaceManifestSHA256: sha})
		return err
	}
	for _, id := range []string{"C1", "C2", "C3"} {
		write(id)
		if err := publish(id); err != nil {
			t.Fatal(err)
		}
	}
	write("C4")
	archive.interrupt = true
	if err := publish("C4"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial C4 should fail: %v", err)
	}
	t.Log("C1,C2,C3 committed; C4 upload interrupted after reading one byte")
	archive.interrupt = false
	env := "env-t1"
	for _, step := range []struct {
		no   int64
		head string
	}{{2, "C3"}, {3, "C4"}} {
		v := verdict("t1", 1)
		v.AttemptID, v.TaskStatus = attempt, "queued"
		if _, err := s.FinalizeAttempt(ctx, v); err != nil {
			t.Fatal(err)
		}
		stopEnv(t, s, env)
		attempt = fmt.Sprintf("att-%d", step.no)
		env = fmt.Sprintf("env-%d", step.no)
		if _, err := s.CreateAttempt(ctx, task.NewAttempt{TaskID: "t1", AttemptID: attempt, AttemptNo: step.no, EnvID: env, Retry: task.RetryFault}); err != nil {
			t.Fatal(err)
		}
		st, err := s.LoadTask(ctx, "t1")
		if err != nil || st.Latest == nil || st.Latest.CheckpointID != step.head {
			t.Fatalf("unexpected head: %+v %v", st.Latest, err)
		}
		var transcript struct {
			LastTool string `json:"last_tool"`
		}
		if err := json.Unmarshal(st.Latest.State, &transcript); err != nil || transcript.LastTool != step.head {
			t.Fatalf("lost transcript cursor: %+v %v", transcript, err)
		}
		m = cold()
		if err = m.Restore(ctx, "t1", st.Latest.WorkspaceManifestSHA256); err != nil {
			t.Fatal(err)
		}
		r, err := m.Read(ctx, workspace.Request{TaskID: "t1", AttemptID: attempt, Body: []byte(`{"path":"cursor.txt"}`)})
		var got struct{ Content string }
		if err != nil || r.Status != 200 || json.Unmarshal(r.Body, &got) != nil || got.Content != step.head {
			t.Fatalf("next read: %+v %v", r, err)
		}
		t.Logf("attempt=%d pg_head=%s next_read=%s", step.no, st.Latest.CheckpointID, got.Content)
		if step.no == 2 {
			write("C4")
			if err := publish("C4"); err != nil {
				t.Fatal(err)
			}
		}
	}
}
