package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/api"
)

// ListTasks 按创建时间倒序 keyset 分页（实现 api.Store）。
func (s *Store) ListTasks(ctx context.Context, after string, limit int) ([]api.TaskView, string, error) {
	if limit < 1 {
		return nil, "", invalidf("ListTasks 的 limit 必须为正")
	}
	var out []api.TaskView
	err := s.read(ctx, "ListTasks", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT t.task_id, t.status, t.status_reason, COALESCE(t.current_attempt_id, ''), c.desired,
				c.control_version, t.applied_control_version, t.attempts_total
			FROM tasks t JOIN task_control c USING (task_id)
			WHERE $1 = '' OR (t.created_at, t.task_id) < (SELECT created_at, task_id FROM tasks WHERE task_id = $1)
			ORDER BY t.created_at DESC, t.task_id DESC LIMIT $2`, after, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.TaskView, error) {
			var v api.TaskView
			err := r.Scan(&v.TaskID, &v.Status, &v.StatusReason, &v.CurrentAttemptID, &v.Desired,
				&v.ControlVersion, &v.AppliedControlVersion, &v.AttemptsTotal)
			return v, err
		})
		return err
	})
	if err != nil || len(out) < limit {
		return out, "", err
	}
	return out, out[len(out)-1].TaskID, nil
}

// Inspect 读取任务的诊断时间线（实现 api.Store；规格 §14.6）。
func (s *Store) Inspect(ctx context.Context, taskID string) (api.Inspection, error) {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return api.Inspection{}, err
	}
	in := api.Inspection{Task: task}
	err = s.read(ctx, "Inspect", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT a.attempt_id, a.status, a.outcome_class, a.attempt_no, a.exit_code, a.exit_signal,
				a.oom_kill_delta, a.platform_killed, COALESCE(e.env_id, ''), COALESCE(e.status, ''), COALESCE(e.cleanup_state, ''),
				e.stopped_at, COALESCE(e.cleanup_tries, 0), COALESCE(e.cleanup_error, '')
			FROM attempts a LEFT JOIN environments e ON e.attempt_id = a.attempt_id
			WHERE a.task_id = $1 ORDER BY a.attempt_no`, taskID)
		if err != nil {
			return err
		}
		in.Attempts, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.AttemptView, error) {
			var v api.AttemptView
			err := r.Scan(&v.AttemptID, &v.Status, &v.OutcomeClass, &v.AttemptNo, &v.ExitCode, &v.ExitSignal,
				&v.OOMKillDelta, &v.PlatformKilled, &v.EnvID, &v.EnvStatus, &v.CleanupState,
				&v.StoppedAt, &v.CleanupTries, &v.CleanupError)
			return v, err
		})
		if err != nil {
			return err
		}
		rows, err = q.Query(ctx, `SELECT checkpoint_id, step_id, attempt_id, commit_seq, committed_at FROM checkpoints
			WHERE scope_kind = 'task' AND scope_id = $1 ORDER BY commit_seq`, taskID)
		if err != nil {
			return err
		}
		in.Checkpoints, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.CheckpointView, error) {
			var v api.CheckpointView
			err := r.Scan(&v.CheckpointID, &v.StepID, &v.AttemptID, &v.CommitSeq, &v.CommittedAt)
			return v, err
		})
		if err != nil {
			return err
		}
		in.Calls, err = inspectCalls(ctx, q, taskID)
		return err
	})
	return in, err
}

// inspectCalls 读取任务的 Gateway 调用与每次 try 的审计元数据（§9.9；不含正文）。调用按登记时间排序，
// try 按 try_no 排序。
func inspectCalls(ctx context.Context, q queryer, taskID string) ([]api.CallView, error) {
	rows, err := q.Query(ctx, `SELECT call_id, endpoint, state, source, first_attempt_id, upstream_request_id,
			COALESCE(result_ref, ''), fail_reason, COALESCE(supersedes_call_id, ''), COALESCE(supersede_reason, ''),
			tries_used, cost_charged, possible_external_duplicate, created_at, deadline_at
		FROM calls WHERE task_id = $1 ORDER BY created_at, call_id`, taskID)
	if err != nil {
		return nil, err
	}
	calls, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.CallView, error) {
		var v api.CallView
		err := r.Scan(&v.CallID, &v.Endpoint, &v.State, &v.Source, &v.FirstAttemptID, &v.UpstreamRequestID,
			&v.ResultRef, &v.FailReason, &v.SupersedesCallID, &v.SupersedeReason,
			&v.TriesUsed, &v.CostChargedMicro, &v.PossibleExternalDuplicate, &v.CreatedAt, &v.DeadlineAt)
		return v, err
	})
	if err != nil || len(calls) == 0 {
		return calls, err
	}
	rows, err = q.Query(ctx, `SELECT call_id, try_no, attempt_id, COALESCE(env_id, ''), state, outcome, latency_ms, cost_micro, error
		FROM call_tries WHERE task_id = $1 ORDER BY call_id, try_no`, taskID)
	if err != nil {
		return nil, err
	}
	type tryRow struct {
		callID string
		try    api.TryView
	}
	tries, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (tryRow, error) {
		var t tryRow
		err := r.Scan(&t.callID, &t.try.TryNo, &t.try.AttemptID, &t.try.EnvID, &t.try.State, &t.try.Outcome,
			&t.try.LatencyMs, &t.try.CostMicro, &t.try.Error)
		return t, err
	})
	if err != nil {
		return nil, err
	}
	idx := make(map[string]int, len(calls))
	for i, c := range calls {
		idx[c.CallID] = i
	}
	for _, t := range tries {
		if i, ok := idx[t.callID]; ok {
			calls[i].Tries = append(calls[i].Tries, t.try)
		}
	}
	return calls, nil
}
