package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// LoadTask 一次读取 Decide 需要的任务事实（实现 task.Store）：单个查询，含最新已提交 checkpoint 的完整内容。
// 会话 turn 另读取会话字段、resume_directive 与会话 base checkpoint 的完整内容：base 为 tasks.base_session_checkpoint_id，
// 尚未创建 attempt（base 为空）时取会话当前指针。
func (s *Store) LoadTask(ctx context.Context, taskID string) (task.TaskState, error) {
	st := task.TaskState{TaskID: taskID}
	err := s.read(ctx, "LoadTask", func(ctx context.Context, q queryer) error {
		var cp, scp checkpointCols
		var sessionID, base, restored *string
		var turnIndex *int64
		err := q.QueryRow(ctx, `SELECT t.status, t.status_reason, COALESCE(t.current_attempt_id, ''), t.spec_json, t.limits_json,
				t.config_version, c.desired, c.control_version, t.applied_control_version, t.attempts_total,
				t.fault_retries_used, t.max_fault_retries, t.oom_retries_used, t.run_time_ms, t.not_before,
				cp.checkpoint_id, cp.step_id, cp.state_inline, cp.state_ref, cp.refs_json,
				t.session_id, t.base_session_checkpoint_id, t.restored_from_task_id, t.turn_index, t.resume_directive,
				scp.checkpoint_id, scp.step_id, scp.state_inline, scp.state_ref, scp.refs_json
			FROM tasks t JOIN task_control c USING (task_id) JOIN task_progress p USING (task_id)
			LEFT JOIN checkpoints cp ON cp.scope_kind = 'task' AND cp.scope_id = t.task_id AND cp.checkpoint_id = p.latest_checkpoint_id
			LEFT JOIN session_progress sp ON sp.session_id = t.session_id
			LEFT JOIN checkpoints scp ON scp.scope_kind = 'session' AND scp.scope_id = t.session_id
				AND scp.checkpoint_id = COALESCE(t.base_session_checkpoint_id, sp.latest_checkpoint_id)
			WHERE t.task_id = $1`, taskID).Scan(&st.Status, &st.StatusReason, &st.CurrentAttemptID, &st.Spec, &st.Limits,
			&st.ConfigVersion, &st.Desired, &st.ControlVersion, &st.AppliedControlVersion, &st.AttemptsTotal,
			&st.FaultRetriesUsed, &st.MaxFaultRetries, &st.OOMRetriesUsed, &st.RunTimeMs, &st.NotBefore,
			&cp.id, &cp.step, &cp.state, &cp.stateRef, &cp.refs,
			&sessionID, &base, &restored, &turnIndex, &st.Directive,
			&scp.id, &scp.step, &scp.state, &scp.stateRef, &scp.refs)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		if err != nil {
			return err
		}
		st.SessionID, st.BaseSessionCheckpointID, st.RestoredFromTaskID = strOr(sessionID, ""), strOr(base, ""), strOr(restored, "")
		if turnIndex != nil {
			st.TurnIndex = *turnIndex
		}
		if st.Latest, err = cp.latest(); err != nil {
			return err
		}
		if st.SessionLatest, err = scp.latest(); err != nil {
			return err
		}
		st.Subruns, err = selectSubrunStates(ctx, q, taskID)
		return err
	})
	return st, err
}

// selectSubrunStates 读取任务全部 sub-run 的状态（按 subrun_id 排序，与 rebindSubrunsTx 的返回一致）。
func selectSubrunStates(ctx context.Context, q queryer, taskID string) ([]task.SubrunState, error) {
	rows, err := q.Query(ctx, "SELECT subrun_id, status, COALESCE(result_ref, '') FROM subruns WHERE task_id = $1 ORDER BY subrun_id", taskID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (task.SubrunState, error) {
		var s task.SubrunState
		err := row.Scan(&s.SubrunID, &s.Status, &s.ResultRef)
		return s, err
	})
}

// checkpointCols 是 LEFT JOIN 读出的一行 checkpoint（不存在时 id 为 nil）。
type checkpointCols struct {
	id, step, stateRef *string
	state, refs        []byte
}

func (c checkpointCols) latest() (*task.LatestCheckpoint, error) {
	if c.id == nil {
		return nil, nil
	}
	l := &task.LatestCheckpoint{CheckpointID: *c.id, StepID: *c.step, State: c.state, StateRef: strOr(c.stateRef, "")}
	if err := json.Unmarshal(c.refs, &l.Refs); err != nil {
		return nil, err
	}
	return l, nil
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
		_, err := tx.Exec(ctx, `UPDATE attempt_access SET state = 'revoked', revoked_at = clock_timestamp(), reason = $2
			WHERE attempt_id = $1 AND state = 'active'`, attemptID, reason)
		return err
	})
}

// LookupAttempt 返回 attempt 所属的任务与环境（实现 task.Store；Gateway 入口绑定时核对归属）。
func (s *Store) LookupAttempt(ctx context.Context, attemptID string) (string, string, error) {
	if attemptID == "" {
		return "", "", invalidf("LookupAttempt 缺少 attempt_id")
	}
	var taskID, envID string
	err := s.read(ctx, "LookupAttempt", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, "SELECT task_id, env_id FROM attempts WHERE attempt_id = $1", attemptID).Scan(&taskID, &envID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		return err
	})
	return taskID, envID, err
}

// AppendHostEvent 追加一条 host 事件（实现 task.Store；例如 Gateway 的 replay_divergence，§9.4）：与其他
// host 事件同一路径（lockEventSeq → appendHostEvent 分配 task_seq）。event_key 由类型、attempt 与 payload
// 的哈希确定，提交结果未知后的重跑以同一身份幂等；内容完全相同的两次报告视为同一事件。
func (s *Store) AppendHostEvent(ctx context.Context, taskID, attemptID, typ string, payload json.RawMessage) error {
	if taskID == "" || typ == "" {
		return invalidf("AppendHostEvent 缺少 task_id 或事件类型")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return invalidf("host 事件 %s 的 payload 须为 JSON 对象", typ)
	}
	sum := sha256.Sum256(payload)
	key := typ + ":" + attemptID + ":" + hex.EncodeToString(sum[:16])
	return s.run(ctx, "AppendHostEvent", taskID+"/"+key, func(ctx context.Context, tx pgx.Tx) error {
		if err := lockEventSeq(ctx, tx, taskID); err != nil {
			return err
		}
		_, err := appendHostEvent(ctx, tx, hostEvent{taskID: taskID, key: key, attemptID: attemptID, typ: typ, payload: payload})
		return err
	})
}
