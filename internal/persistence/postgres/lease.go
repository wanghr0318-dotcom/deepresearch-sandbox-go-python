package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// RenewAttemptLease extends only the live current grant. An expired owner cannot resurrect itself.
func (s *Store) RenewAttemptLease(ctx context.Context, taskID, attemptID string) error {
	return s.run(ctx, "RenewAttemptLease", attemptID, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockTask(ctx, tx, taskID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks t SET lease_expires_at = clock_timestamp() + $3::bigint * interval '1 microsecond'
   WHERE t.task_id=$1 AND t.current_attempt_id=$2 AND t.lease_expires_at>clock_timestamp()
   AND $3::bigint > 0 AND EXISTS (SELECT 1 FROM attempt_access aa WHERE aa.attempt_id=$2 AND aa.state='active')
   AND t.status IN ('running','pausing','cancelling')`, taskID, attemptID, s.opt.AttemptLeaseTTL.Microseconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return rejectf(persistence.CodeStaleAttempt, "cannot renew lease for attempt %s", attemptID)
		}
		return nil
	})
}

// ExpiredAttemptLease reports a definitive loss of this grant or its expiry on the database clock.
func (s *Store) ExpiredAttemptLease(ctx context.Context, taskID, attemptID string) (bool, error) {
	var expired bool
	err := s.read(ctx, "ExpiredAttemptLease", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT current_attempt_id IS DISTINCT FROM $2 OR COALESCE(lease_expires_at<=clock_timestamp(),false)
    FROM tasks WHERE task_id=$1`, taskID, attemptID).Scan(&expired)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("task %s", taskID)
		}
		return err
	})
	return expired, err
}

// FenceExpiredSession logically retires one isolated incarnation after the host
// has revoked its relay. A fresh grant, pending external call, or unresolved
// child sandbox prevents retirement. Physical stop and UID release remain separate.
func (s *Store) FenceExpiredSession(ctx context.Context, envID, attemptID string) (bool, error) {
	var fenced bool
	err := s.run(ctx, "FenceExpiredSession", envID, func(ctx context.Context, tx pgx.Tx) error {
		var taskID string
		err := tx.QueryRow(ctx, `SELECT t.task_id FROM tasks t JOIN attempts a ON a.task_id=t.task_id
			WHERE a.attempt_id=$1 AND a.env_id=$2 FOR UPDATE OF t`, attemptID, envID).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			fenced = false
			return nil
		}
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE environments e SET fenced_at=COALESCE(e.fenced_at,clock_timestamp())
			FROM attempts a JOIN tasks t ON a.task_id=t.task_id JOIN attempt_access aa ON aa.attempt_id=a.attempt_id
			WHERE e.env_id=$1 AND a.attempt_id=$2 AND t.task_id=$3 AND t.current_attempt_id=a.attempt_id
			AND e.kind='session' AND e.session_id=t.session_id AND e.env_id=a.env_id AND e.stopped_at IS NULL
			AND aa.state='revoked' AND t.lease_expires_at<=clock_timestamp()
			AND a.verdict_hash IS NOT NULL AND t.status='queued'
			AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.task_id=t.task_id AND c.state IN ('resolving','in_flight'))
			AND NOT EXISTS (SELECT 1 FROM environments child WHERE child.attempt_id=a.attempt_id AND child.kind='exec' AND child.stopped_at IS NULL)`, envID, attemptID, taskID)
		fenced = err == nil && tag.RowsAffected() == 1
		return err
	})
	return fenced, err
}
