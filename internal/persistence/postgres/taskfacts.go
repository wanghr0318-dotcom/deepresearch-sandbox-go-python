package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// LoadTask 一次读取 Decide 需要的任务事实（实现 task.Store）：单个查询，含最新已提交 checkpoint 的完整内容。
func (s *Store) LoadTask(ctx context.Context, taskID string) (task.TaskState, error) {
	st := task.TaskState{TaskID: taskID}
	err := s.read(ctx, "LoadTask", func(ctx context.Context, q queryer) error {
		var cpID, stepID, stateRef *string
		var state, refs []byte
		err := q.QueryRow(ctx, `SELECT t.status, t.status_reason, COALESCE(t.current_attempt_id, ''), t.spec_json, t.limits_json,
				t.config_version, c.desired, c.control_version, t.applied_control_version, t.attempts_total,
				t.fault_retries_used, t.max_fault_retries, t.oom_retries_used, t.run_time_ms, t.not_before,
				cp.checkpoint_id, cp.step_id, cp.state_inline, cp.state_ref, cp.refs_json
			FROM tasks t JOIN task_control c USING (task_id) JOIN task_progress p USING (task_id)
			LEFT JOIN checkpoints cp ON cp.scope_kind = 'task' AND cp.scope_id = t.task_id AND cp.checkpoint_id = p.latest_checkpoint_id
			WHERE t.task_id = $1`, taskID).Scan(&st.Status, &st.StatusReason, &st.CurrentAttemptID, &st.Spec, &st.Limits,
			&st.ConfigVersion, &st.Desired, &st.ControlVersion, &st.AppliedControlVersion, &st.AttemptsTotal,
			&st.FaultRetriesUsed, &st.MaxFaultRetries, &st.OOMRetriesUsed, &st.RunTimeMs, &st.NotBefore,
			&cpID, &stepID, &state, &stateRef, &refs)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		if err != nil || cpID == nil {
			return err
		}
		latest := &task.LatestCheckpoint{CheckpointID: *cpID, StepID: *stepID, State: state}
		if stateRef != nil {
			latest.StateRef = *stateRef
		}
		if err := json.Unmarshal(refs, &latest.Refs); err != nil {
			return err
		}
		st.Latest = latest
		return nil
	})
	return st, err
}

// ListActiveTasks 返回非终态任务的 ID，按创建时间排序（实现 task.Store）。
func (s *Store) ListActiveTasks(ctx context.Context) ([]string, error) {
	var out []string
	err := s.read(ctx, "ListActiveTasks", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT task_id FROM tasks WHERE status NOT IN ('succeeded', 'failed', 'cancelled')
			ORDER BY created_at, task_id`)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}

// PersistRunTime 把累计运行时间推进到 totalMs，取较大者（实现 task.Store；规格 §14.4）。单调写入使
// 提交结果未知后的重跑不会重复累计。只接受当前 attempt。
func (s *Store) PersistRunTime(ctx context.Context, taskID, attemptID string, totalMs int64) (int64, error) {
	if taskID == "" || attemptID == "" || totalMs < 0 {
		return 0, invalidf("PersistRunTime 缺少 task_id、attempt_id，或累计时间为负")
	}
	var out int64
	err := s.run(ctx, "PersistRunTime", taskID+"@"+attemptID, func(ctx context.Context, tx pgx.Tx) error {
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
		return tx.QueryRow(ctx, `UPDATE tasks SET run_time_ms = GREATEST(run_time_ms, $2), run_time_persisted_at = now()
			WHERE task_id = $1 RETURNING run_time_ms`, taskID, totalMs).Scan(&out)
	})
	return out, err
}

// RevokeAttemptAccess 把 attempt 的访问置为 revoked（实现 task.Store）；已撤销时返回成功（幂等）。
func (s *Store) RevokeAttemptAccess(ctx context.Context, attemptID, reason string) error {
	if attemptID == "" {
		return invalidf("RevokeAttemptAccess 缺少 attempt_id")
	}
	return s.run(ctx, "RevokeAttemptAccess", attemptID, func(ctx context.Context, tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM attempt_access WHERE attempt_id = $1)", attemptID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return notFoundf("attempt %s 的访问记录", attemptID)
		}
		_, err := tx.Exec(ctx, `UPDATE attempt_access SET state = 'revoked', revoked_at = now(), reason = $2
			WHERE attempt_id = $1 AND state = 'active'`, attemptID, reason)
		return err
	})
}
