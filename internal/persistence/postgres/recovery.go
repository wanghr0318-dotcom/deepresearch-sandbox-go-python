package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/recovery"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/session"
)

var _ recovery.Store = (*Store)(nil)

// LoadRecoveryFacts 读取启动恢复需要的数据库事实（实现 recovery.Store）。在同一事务中读取；启动恢复在 actor 与
// API 启动之前运行，此时没有其他写者。
func (s *Store) LoadRecoveryFacts(ctx context.Context) (recovery.Facts, error) {
	var f recovery.Facts
	err := s.run(ctx, "LoadRecoveryFacts", "", func(ctx context.Context, tx pgx.Tx) error {
		f = recovery.Facts{}
		var err error
		if f.Tasks, err = loadTaskFacts(ctx, tx); err != nil {
			return err
		}
		if f.Environments, err = loadEnvFacts(ctx, tx); err != nil {
			return err
		}
		if f.PendingIntents, err = loadOpenIntents(ctx, tx); err != nil {
			return err
		}
		f.UnreleasedRanges, err = loadUnreleasedRanges(ctx, tx)
		return err
	})
	return f, err
}

func loadTaskFacts(ctx context.Context, tx pgx.Tx) ([]recovery.TaskFact, error) {
	rows, err := tx.Query(ctx, `SELECT t.task_id, t.status, c.desired, c.control_version, t.not_before, t.run_time_persisted_at,
			a.attempt_id, a.status, a.env_id, a.verdict_hash IS NOT NULL, COALESCE(a.terminal_proposal, '')
		FROM tasks t JOIN task_control c USING (task_id)
		LEFT JOIN attempts a ON a.attempt_id = t.current_attempt_id
		WHERE t.status NOT IN ('succeeded', 'failed', 'cancelled')
		ORDER BY t.created_at, t.task_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (recovery.TaskFact, error) {
		var t recovery.TaskFact
		var attemptID, status, envID *string
		var hasVerdict *bool
		var proposal string
		if err := r.Scan(&t.TaskID, &t.Status, &t.Desired, &t.ControlVersion, &t.NotBefore, &t.RunTimePersistedAt,
			&attemptID, &status, &envID, &hasVerdict, &proposal); err != nil {
			return t, err
		}
		if attemptID != nil {
			t.CurrentAttempt = &recovery.AttemptFact{AttemptID: *attemptID, Status: *status, EnvID: *envID,
				HasVerdict: *hasVerdict, ProposalKind: proposal}
		}
		return t, nil
	})
}

func loadEnvFacts(ctx context.Context, tx pgx.Tx) ([]recovery.EnvFact, error) {
	rows, err := tx.Query(ctx, `SELECT env_id, COALESCE(attempt_id, ''), stopped_at, cleanup_state FROM environments
		WHERE stopped_at IS NULL OR cleanup_state <> 'done' ORDER BY created_at, env_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (recovery.EnvFact, error) {
		var e recovery.EnvFact
		err := r.Scan(&e.EnvID, &e.AttemptID, &e.StoppedAt, &e.CleanupState)
		return e, err
	})
}

func loadOpenIntents(ctx context.Context, tx pgx.Tx) ([]resource.Intent, error) {
	rows, err := tx.Query(ctx, `SELECT intent_id, env_id, kind, name, state FROM resource_intents
		WHERE state IN ('pending', 'acquired') ORDER BY intent_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (resource.Intent, error) {
		var i resource.Intent
		err := r.Scan(&i.IntentID, &i.EnvID, &i.Kind, &i.Name, &i.State)
		return i, err
	})
}

func loadUnreleasedRanges(ctx context.Context, tx pgx.Tx) ([]resource.UIDRange, error) {
	rows, err := tx.Query(ctx, `SELECT u.uid_range_id, u.base, u.size, u.state, u.owner_id, u.allocation_id
		FROM uid_ranges u JOIN environments e ON e.env_id = u.owner_id
		WHERE u.state = 'assigned' AND e.stopped_at IS NOT NULL AND e.cleanup_state = 'done'
		ORDER BY u.base`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (resource.UIDRange, error) {
		var u resource.UIDRange
		err := r.Scan(&u.UIDRangeID, &u.Base, &u.Size, &u.State, &u.OwnerID, &u.AllocationID)
		return u, err
	})
}

// EvictSessionsOnRestart 是重启时的会话驱逐（实现 recovery.Store；规格 §12.6、§14.1 第 7 步），单事务：未关闭会话的
// 非 ended incarnation 置为 ended{lost_on_restart}（当前的清空 current_incarnation_id）；status ∉ {creating, evicted,
// closing, closed} 的会话置为 evicted 并追加 session_state{evicted}。启动恢复在 actor 与 API 启动之前运行。
// 锁顺序：sessions（按 session_id）→ incarnations → session_event_seq。
func (s *Store) EvictSessionsOnRestart(ctx context.Context) ([]recovery.EvictedSession, error) {
	var out []recovery.EvictedSession
	err := s.run(ctx, "EvictSessionsOnRestart", "", func(ctx context.Context, tx pgx.Tx) error {
		out = nil
		rows, err := tx.Query(ctx, `SELECT session_id, status, row_version FROM sessions WHERE status <> 'closed'
			ORDER BY session_id FOR UPDATE`)
		if err != nil {
			return err
		}
		type row struct {
			id, status string
			rv         int64
		}
		open, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
			var x row
			err := r.Scan(&x.id, &x.status, &x.rv)
			return x, err
		})
		if err != nil {
			return err
		}
		for _, x := range open {
			rows, err := tx.Query(ctx, `UPDATE incarnations SET status = 'ended', ended_at = now(), end_reason = $2
				WHERE session_id = $1 AND status <> 'ended' RETURNING incarnation_id, env_id`, x.id, session.EndLostOnRestart)
			if err != nil {
				return err
			}
			ended, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (recovery.EvictedSession, error) {
				e := recovery.EvictedSession{SessionID: x.id}
				err := r.Scan(&e.IncarnationID, &e.EnvID)
				return e, err
			})
			if err != nil {
				return err
			}
			evict := !slices.Contains([]string{session.StatusCreating, session.StatusEvicted, session.StatusClosing}, x.status)
			if !evict {
				if len(ended) > 0 {
					if _, err := tx.Exec(ctx, "UPDATE sessions SET current_incarnation_id = NULL WHERE session_id = $1", x.id); err != nil {
						return err
					}
				}
				out = append(out, ended...)
				continue
			}
			if _, err := tx.Exec(ctx, `UPDATE sessions SET status = 'evicted', row_version = row_version + 1, current_incarnation_id = NULL,
				idle_since = NULL, frozen_since = NULL WHERE session_id = $1`, x.id); err != nil {
				return err
			}
			if _, err := appendSessionEvent(ctx, tx, x.id, fmt.Sprintf("%s:%d", session.StatusEvicted, x.rv+1),
				statePayload(session.StatusEvicted)); err != nil {
				return err
			}
			if len(ended) == 0 {
				ended = []recovery.EvictedSession{{SessionID: x.id}}
			}
			out = append(out, ended...)
		}
		return nil
	})
	return out, err
}

// RevokeAllActive 在单个事务中撤销全部 active 的 attempt 访问（实现 recovery.Store；规格 §14.1 第 3 步）。
// 重跑时已撤销的行不再计入，结果仍是"全部已撤销"。
func (s *Store) RevokeAllActive(ctx context.Context, reason string) (int, error) {
	var n int
	err := s.run(ctx, "RevokeAllActive", "", func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE attempt_access SET state = 'revoked', revoked_at = now(), reason = $1
			WHERE state = 'active'`, reason)
		n = int(tag.RowsAffected())
		return err
	})
	return n, err
}

// AccountUnrecordedRunTime 把上次持久化（无则 attempt 创建时间）到 until 的区间全额计入运行时间并把
// 持久化时间推进到 until（实现 recovery.Store；规格 §14.4）。until 不晚于已持久化时间时不变，因而幂等。
func (s *Store) AccountUnrecordedRunTime(ctx context.Context, taskID, attemptID string, until time.Time) error {
	if taskID == "" || attemptID == "" {
		return invalidf("AccountUnrecordedRunTime 缺少 task_id 或 attempt_id")
	}
	return s.run(ctx, "AccountUnrecordedRunTime", taskID+"@"+attemptID, func(ctx context.Context, tx pgx.Tx) error {
		var current *string
		err := tx.QueryRow(ctx, "SELECT current_attempt_id FROM tasks WHERE task_id = $1 FOR UPDATE", taskID).Scan(&current)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		if err != nil {
			return err
		}
		if current == nil || *current != attemptID {
			return rejectf(persistence.CodeStaleAttempt, "attempt %s 不是任务 %s 的当前 attempt", attemptID, taskID)
		}
		_, err = tx.Exec(ctx, `UPDATE tasks t SET
				run_time_ms = t.run_time_ms + GREATEST(0, (EXTRACT(EPOCH FROM ($3::timestamptz - COALESCE(t.run_time_persisted_at, a.created_at))) * 1000)::bigint),
				run_time_persisted_at = $3
			FROM attempts a
			WHERE t.task_id = $1 AND a.attempt_id = $2 AND COALESCE(t.run_time_persisted_at, a.created_at) < $3`,
			taskID, attemptID, until)
		return err
	})
}
