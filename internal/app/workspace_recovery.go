package app

import (
	"context"
	"errors"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

type portableWorkspace interface {
	Snapshot(context.Context, string) (string, error)
	Restore(context.Context, string, string) error
	ResetUncommitted(context.Context, string) error
}

type snapshotStore struct {
	runner.Store
	workspace portableWorkspace
}

func (s snapshotStore) CommitCheckpoint(ctx context.Context, c runner.Checkpoint) (runner.CommittedCheckpoint, error) {
	old, err := s.Store.QueryCheckpoint(ctx, c.Scope, c.CheckpointID)
	if err == nil {
		c.WorkspaceManifestSHA256 = old.WorkspaceManifestSHA256
		return s.Store.CommitCheckpoint(ctx, c)
	}
	if !errors.Is(err, persistence.ErrNotFound) {
		return runner.CommittedCheckpoint{}, err
	}
	manifest, err := s.workspace.Snapshot(ctx, c.Scope.ID)
	if err != nil {
		return runner.CommittedCheckpoint{}, err
	}
	c.WorkspaceManifestSHA256 = manifest
	return s.Store.CommitCheckpoint(ctx, c)
}

func restoreAttemptWorkspace(ctx context.Context, workspace portableWorkspace, s task.RunSpec) error {
	if latest := s.Task.Latest; latest != nil {
		if latest.WorkspaceManifestSHA256 == "" {
			return nil // Earlier checkpoints refer to the original host workspace.
		}
		if workspace == nil {
			return errors.New("checkpoint requires a workspace archive")
		}
		return workspace.Restore(ctx, s.Task.TaskID, latest.WorkspaceManifestSHA256)
	}
	if workspace != nil && s.AttemptNo > 1 {
		return workspace.ResetUncommitted(ctx, s.Task.TaskID)
	}
	return nil
}

type attemptLeases interface {
	RenewAttemptLease(context.Context, string, string) error
	ExpiredAttemptLease(context.Context, string, string) (bool, error)
}

// A host timer alone does not attest worker liveness. EnvProcs reaches the bound
// sandbox before the database checks whether the same attempt can still renew.
func leaseProbe(leases attemptLeases, procs func(context.Context, string) ([]int, error), budget time.Duration) func(context.Context, string, string, string) error {
	return func(ctx context.Context, envID, taskID, attemptID string) error {
		probeCtx, cancel := context.WithTimeout(ctx, budget)
		pids, err := procs(probeCtx, envID)
		cancel()
		if err == nil && len(pids) > 0 {
			err = leases.RenewAttemptLease(ctx, taskID, attemptID)
			var rejected *persistence.RejectedError
			if errors.As(err, &rejected) && rejected.Code == persistence.CodeStaleAttempt {
				return persistence.ErrConflict
			}
			return err
		}
		expired, err := leases.ExpiredAttemptLease(ctx, taskID, attemptID)
		if err != nil {
			return err
		}
		if expired {
			return persistence.ErrConflict
		}
		return nil
	}
}
