package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
)

// ListCleanupCandidates 返回待清理的环境（实现 resource.Store）：stopped_at 已记录、清理未完成、
// 所属 attempt 已有判决、next_retry_at 已到或为空；按停止时间排序。
func (s *Store) ListCleanupCandidates(ctx context.Context, now time.Time, limit int) ([]resource.Environment, error) {
	if limit < 1 {
		return nil, invalidf("ListCleanupCandidates 的 limit 必须为正")
	}
	var out []resource.Environment
	err := s.read(ctx, "ListCleanupCandidates", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT e.env_id, e.kind, COALESCE(e.attempt_id, ''), e.status, e.stopped_at, e.cleanup_state,
				e.cleanup_tries, e.cleanup_error, e.next_retry_at
			FROM environments e JOIN attempts a ON a.attempt_id = e.attempt_id
			WHERE e.stopped_at IS NOT NULL AND e.cleanup_state <> 'done' AND a.verdict_hash IS NOT NULL
				AND (e.next_retry_at IS NULL OR e.next_retry_at <= $1)
			ORDER BY e.stopped_at, e.env_id LIMIT $2`, now, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (resource.Environment, error) {
			var e resource.Environment
			err := r.Scan(&e.EnvID, &e.Kind, &e.AttemptID, &e.Status, &e.StoppedAt, &e.CleanupState,
				&e.CleanupTries, &e.CleanupError, &e.NextRetryAt)
			return e, err
		})
		return err
	})
	return out, err
}

// RecordQuarantine 记录隔离资源（实现 resource.Store；规格 §14.1 扫描表）；按路径幂等，不覆盖已有记录。
func (s *Store) RecordQuarantine(ctx context.Context, q resource.Quarantine) error {
	if q.Layer == "" || q.Path == "" || q.Reason == "" {
		return invalidf("RecordQuarantine 缺少 layer、path 或 reason")
	}
	return s.run(ctx, "RecordQuarantine", q.Path, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO quarantined_resources (resource_path, kind, observed_owner, reason)
			VALUES ($1, $2, $3, $4) ON CONFLICT (resource_path) DO NOTHING`, q.Path, q.Layer, q.ObservedOwner, q.Reason)
		return err
	})
}
