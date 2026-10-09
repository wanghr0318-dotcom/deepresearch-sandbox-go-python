package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
)

// ListTasks 按创建时间倒序 keyset 分页（实现 api.Store）。
func (s *Store) ListTasks(ctx context.Context, after string, limit int) ([]api.TaskView, string, error) {
	return s.listTasks(ctx, "ListTasks", 0, after, limit)
}

// ListTasksByOwner 同 ListTasks，只含 owner_user_id = ownerID 的独立研究任务（实现 api.Accounts）；会话的 turn
// 属于会话视图，不出现在"我的研究"列表中（M4 Plan 12）。
func (s *Store) ListTasksByOwner(ctx context.Context, ownerID int64, after string, limit int) ([]api.TaskView, string, error) {
	if ownerID <= 0 {
		return nil, "", invalidf("ListTasksByOwner 的 ownerID 必须为正")
	}
	return s.listTasks(ctx, "ListTasksByOwner", ownerID, after, limit)
}

// listTasks 是 ListTasks 与 ListTasksByOwner 的共同实现：owner 为 0 时不按归属过滤。
func (s *Store) listTasks(ctx context.Context, op string, owner int64, after string, limit int) ([]api.TaskView, string, error) {
	if limit < 1 {
		return nil, "", invalidf("%s 的 limit 必须为正", op)
	}
	var out []api.TaskView
	err := s.read(ctx, op, func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT t.task_id, t.status, t.status_reason, COALESCE(t.current_attempt_id, ''), c.desired,
				c.control_version, t.applied_control_version, t.attempts_total, t.created_at, COALESCE(t.spec_json->>'topic', '')
			FROM tasks t JOIN task_control c USING (task_id)
			WHERE ($3::bigint = 0 OR (t.owner_user_id = $3::bigint AND t.session_id IS NULL))
				AND ($1 = '' OR (t.created_at, t.task_id) < (SELECT created_at, task_id FROM tasks WHERE task_id = $1))
			ORDER BY t.created_at DESC, t.task_id DESC LIMIT $2`, after, limit, owner)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.TaskView, error) {
			var v api.TaskView
			err := r.Scan(&v.TaskID, &v.Status, &v.StatusReason, &v.CurrentAttemptID, &v.Desired,
				&v.ControlVersion, &v.AppliedControlVersion, &v.AttemptsTotal, &v.CreatedAt, &v.Topic)
			v.CreatedAt = v.CreatedAt.UTC()
			return v, err
		})
		return err
	})
	if err != nil || len(out) < limit {
		return out, "", err
	}
	return out, out[len(out)-1].TaskID, nil
}

// Inspect 读取任务的诊断时间线（实现 api.Store；规格 §14.6）。attempt 行的环境是它的任务环境：exec 环境同样记录
// attempt_id，但出现在调用的 try 中（TryView.EnvID），不参与 attempt 的连接（否则每个 exec 环境复制一行 attempt）。
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
			FROM attempts a LEFT JOIN environments e ON e.attempt_id = a.attempt_id AND e.kind <> 'exec'
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
		if in.Calls, err = inspectCalls(ctx, q, taskID); err != nil {
			return err
		}
		if in.Budget, err = inspectBudget(ctx, q, taskID); err != nil {
			return err
		}
		in.Subruns, err = inspectSubruns(ctx, q, taskID)
		return err
	})
	return in, err
}

// inspectBudget 读取 task 层账本（两层费用的总额）；没有账本行时为 nil。
func inspectBudget(ctx context.Context, q queryer, taskID string) (*api.BudgetView, error) {
	var b api.BudgetView
	err := q.QueryRow(ctx, `SELECT limit_micro, reserved_micro, spent_micro, unknown_micro, tool_call_limit, tool_calls_used
		FROM budgets WHERE task_id = $1`, taskID).
		Scan(&b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro, &b.ToolCallLimit, &b.ToolCallsUsed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// inspectSubruns 读取任务的 sub-run（按启动时间、ID 排序，同 ListSubruns）及其 sub-run 层账本与调用数（规格 §15.4）。
func inspectSubruns(ctx context.Context, q queryer, taskID string) ([]api.SubrunView, error) {
	rows, err := q.Query(ctx, `SELECT s.subrun_id, s.parent_step_id, s.status, s.started_at, s.ended_at, s.deadline_at,
			s.cancel_reason, s.failure_reason, b.cap_micro, COALESCE(b.reserved_micro, 0), COALESCE(b.spent_micro, 0),
			COALESCE(b.unknown_micro, 0),
			(SELECT count(*) FROM calls c WHERE c.task_id = s.task_id AND c.subrun_id = s.subrun_id)
		FROM subruns s LEFT JOIN subrun_budgets b ON b.task_id = s.task_id AND b.subrun_id = s.subrun_id
		WHERE s.task_id = $1 ORDER BY s.started_at, s.subrun_id`, taskID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.SubrunView, error) {
		var v api.SubrunView
		err := r.Scan(&v.SubrunID, &v.ParentStepID, &v.Status, &v.StartedAt, &v.EndedAt, &v.DeadlineAt,
			&v.CancelReason, &v.FailureReason, &v.CapMicro, &v.ReservedMicro, &v.SpentMicro, &v.UnknownMicro, &v.Calls)
		return v, err
	})
}

// TaskResult 读取任务状态与固定的结果 tasks.result_json（实现 api.Store；规格 §5.6）。
func (s *Store) TaskResult(ctx context.Context, taskID string) (api.ResultView, error) {
	var v api.ResultView
	err := s.read(ctx, "TaskResult", func(ctx context.Context, q queryer) error {
		var result []byte
		err := q.QueryRow(ctx, "SELECT status, result_json FROM tasks WHERE task_id = $1", taskID).Scan(&v.Status, &result)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		v.Result = result
		return err
	})
	return v, err
}

// PinnedArtifact 读取任务中一个产物的版本（实现 api.Store）：version 为 0 时取最新版本。只有
// visibility = output 的版本可下载（internal 版本视为不存在；inspect 的元数据不受影响）。授权按
// scope_blobs(task)：blob 未授权到该任务的版本视为不存在。
func (s *Store) PinnedArtifact(ctx context.Context, taskID, artifactID string, version int64) (api.ArtifactView, bool, error) {
	if version < 0 {
		return api.ArtifactView{}, false, invalidf("PinnedArtifact 的 version 不能为负")
	}
	v := api.ArtifactView{ArtifactID: artifactID}
	var found bool
	err := s.read(ctx, "PinnedArtifact", func(ctx context.Context, q queryer) error {
		var exists bool
		if err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM tasks WHERE task_id = $1)", taskID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return notFoundf("任务 %s", taskID)
		}
		// 先选版本（最新或指定），再核对可见性与授权：最新版本为 internal 或未授权时不回退到更早的版本。
		var visible bool
		err := q.QueryRow(ctx, `SELECT a.version, a.sha256, a.size, a.media_type,
				a.visibility = 'output' AND EXISTS (SELECT 1 FROM scope_blobs sb
					WHERE sb.scope_kind = 'task' AND sb.scope_id = a.task_id AND sb.sha256 = a.sha256)
			FROM artifacts a
			WHERE a.task_id = $1 AND a.artifact_id = $2 AND ($3::bigint = 0 OR a.version = $3::bigint)
			ORDER BY a.version DESC LIMIT 1`, taskID, artifactID, version).Scan(&v.Version, &v.SHA256, &v.Size, &v.MediaType, &visible)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		found = err == nil && visible
		return err
	})
	if err != nil || !found {
		return api.ArtifactView{}, false, err
	}
	return v, true, nil
}

// inspectCalls 读取任务的 Gateway 调用与每次 try 的审计元数据（§9.9；不含正文）。调用按登记时间排序，
// try 按 try_no 排序。
func inspectCalls(ctx context.Context, q queryer, taskID string) ([]api.CallView, error) {
	rows, err := q.Query(ctx, `SELECT call_id, endpoint, state, source, first_attempt_id, upstream_request_id,
			COALESCE(result_ref, ''), fail_reason, COALESCE(supersedes_call_id, ''), COALESCE(supersede_reason, ''),
			tries_used, cost_charged, possible_external_duplicate, created_at, deadline_at, model, COALESCE(subrun_id, '')
		FROM calls WHERE task_id = $1 ORDER BY created_at, call_id`, taskID)
	if err != nil {
		return nil, err
	}
	calls, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.CallView, error) {
		var v api.CallView
		err := r.Scan(&v.CallID, &v.Endpoint, &v.State, &v.Source, &v.FirstAttemptID, &v.UpstreamRequestID,
			&v.ResultRef, &v.FailReason, &v.SupersedesCallID, &v.SupersedeReason,
			&v.TriesUsed, &v.CostChargedMicro, &v.PossibleExternalDuplicate, &v.CreatedAt, &v.DeadlineAt, &v.Model, &v.SubrunID)
		return v, err
	})
	if err != nil || len(calls) == 0 {
		return calls, err
	}
	rows, err = q.Query(ctx, `SELECT call_id, try_no, attempt_id, COALESCE(env_id, ''), state, outcome, latency_ms, cost_micro, error,
			queue_ms, wall_ms, cpu_usec, exec_started_at, provider, skipped, hedge
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
			&t.try.LatencyMs, &t.try.CostMicro, &t.try.Error,
			&t.try.QueueMs, &t.try.WallMs, &t.try.CPUUsec, &t.try.ExecStartedAt, &t.try.Provider, &t.try.Skipped, &t.try.Hedge)
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
