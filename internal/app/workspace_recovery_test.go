package app

import (
	"context"
	"errors"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

type archiveProbe struct {
	snapshots int
	restored  string
	fail      error
}

func (p *archiveProbe) Snapshot(context.Context, string) (string, error) {
	p.snapshots++
	return "v2", p.fail
}
func (p *archiveProbe) Restore(_ context.Context, _ string, sha string) error {
	p.restored = sha
	return nil
}
func (p *archiveProbe) ResetUncommitted(context.Context, string) error {
	p.restored = "reset"
	return nil
}

type recordingCP struct {
	runner.Store
	head    string
	commits int
}

func (p *recordingCP) QueryCheckpoint(context.Context, runner.Scope, string) (runner.CommittedCheckpoint, error) {
	if p.head == "" {
		return runner.CommittedCheckpoint{}, persistence.ErrNotFound
	}
	return runner.CommittedCheckpoint{WorkspaceManifestSHA256: p.head}, nil
}
func (p *recordingCP) CommitCheckpoint(_ context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	p.commits++
	p.head = c.WorkspaceManifestSHA256
	return runner.CommittedCheckpoint{WorkspaceManifestSHA256: p.head}, nil
}
func TestPortableCheckpointPublication(t *testing.T) {
	ctx := context.Background()
	db := &recordingCP{}
	archive := &archiveProbe{fail: errors.New("upload interrupted")}
	store := snapshotStore{db, archive}
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "task", ID: "task-one"}, CheckpointID: "cp2"}
	if _, err := store.CommitCheckpoint(ctx, cp); err == nil || db.commits != 0 {
		t.Fatal("upload failure published a checkpoint")
	}
	archive.fail = nil
	if _, err := store.CommitCheckpoint(ctx, cp); err != nil || db.head != "v2" {
		t.Fatal("missing publication", err)
	}
	// Lose the acknowledgement and replay after the archive has become unavailable.
	archive.fail = errors.New("archive unavailable")
	if _, err := store.CommitCheckpoint(ctx, cp); err != nil || archive.snapshots != 2 || db.commits != 2 {
		t.Fatal("replay did not reuse committed manifest", err)
	}
	spec := task.RunSpec{AttemptNo: 2}
	if err := restoreAttemptWorkspace(ctx, archive, spec); err != nil || archive.restored != "reset" {
		t.Fatal("retry before first checkpoint", err)
	}
	spec.Task.Latest = &task.LatestCheckpoint{WorkspaceManifestSHA256: db.head}
	if err := restoreAttemptWorkspace(ctx, archive, spec); err != nil || archive.restored != "v2" {
		t.Fatal("restore did not use committed head", err)
	}
	if err := restoreAttemptWorkspace(ctx, nil, spec); err == nil {
		t.Fatal("portable checkpoint with missing archive accepted")
	}
}
