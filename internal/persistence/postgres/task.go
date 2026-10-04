package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

var _ task.Store = (*Store)(nil)

// CreateAttempt 创建 attempt、attempt_access（active）与任务环境记录，并把任务从 queued 推进到 running
// （实现 task.Store）。同一 attempt_id 已存在时只比较内容并返回原结果；新建时在同一事务内检查准入
// 前置条件（规格 §8.1）：任务处于 queued、desired = run、旧执行的环境均已确认停止。
// 锁顺序：tasks → task_control → task_event_seq → attempts → attempt_access → environments。
func (s *Store) CreateAttempt(ctx context.Context, a task.NewAttempt) (task.Attempt, error) {
	if a.TaskID == "" || a.AttemptID == "" || a.EnvID == "" || a.AttemptNo < 1 {
		return task.Attempt{}, invalidf("CreateAttempt 缺少 task_id、attempt_id、env_id 或 attempt_no")
	}
	switch a.Retry {
	case task.RetryNone, task.RetryFault, task.RetryOOM:
	default:
		return task.Attempt{}, invalidf("CreateAttempt 的重试类别 %q 未定义", a.Retry)
	}
	var out task.Attempt
	err := s.run(ctx, "CreateAttempt", a.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		var status, desired string
		err := tx.QueryRow(ctx, "SELECT status FROM tasks WHERE task_id = $1 FOR UPDATE", a.TaskID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", a.TaskID)
		}
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR SHARE", a.TaskID).Scan(&desired); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, a.TaskID); err != nil {
			return err
		}
		existing, err := selectAttempt(ctx, tx, a.AttemptID, true)
		switch {
		case err == nil:
			if existing.TaskID != a.TaskID || existing.AttemptNo != a.AttemptNo || existing.EnvID != a.EnvID {
				return conflictf("attempt %s 已存在且内容不同", a.AttemptID)
			}
			out = existing
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		var taken string
		err = tx.QueryRow(ctx, "SELECT attempt_id FROM attempts WHERE task_id = $1 AND attempt_no = $2", a.TaskID, a.AttemptNo).Scan(&taken)
		if err == nil {
			return conflictf("任务 %s 的第 %d 次 attempt 已是 %s", a.TaskID, a.AttemptNo, taken)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var envTaken bool // 否则 INSERT 的 23505 会被当作可重试错误一直重试到期限
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM environments WHERE env_id = $1)", a.EnvID).Scan(&envTaken); err != nil {
			return err
		}
		if envTaken {
			return conflictf("环境 %s 已存在", a.EnvID)
		}
		if err := admitAttempt(ctx, tx, a.TaskID, status, desired); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ($1, $2, $3, $4, 'starting')",
			a.AttemptID, a.TaskID, a.AttemptNo, a.EnvID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempt_access (attempt_id, task_id, state) VALUES ($1, $2, 'active')", a.AttemptID, a.TaskID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO environments (env_id, kind, attempt_id, status) VALUES ($1, 'task', $2, 'creating')",
			a.EnvID, a.AttemptID); err != nil {
			return err
		}
		// 重试计数只在新建 attempt 的同一事务中递增；重放走上面的身份分支，不重复递增（规格 §14.2）。
		tag, err := tx.Exec(ctx, `UPDATE tasks SET current_attempt_id = $2, attempts_total = attempts_total + 1, status = 'running',
			fault_retries_used = fault_retries_used + CASE WHEN $3 = 'fault' THEN 1 ELSE 0 END,
			oom_retries_used = oom_retries_used + CASE WHEN $3 = 'oom' THEN 1 ELSE 0 END,
			row_version = row_version + 1 WHERE task_id = $1 AND status = 'queued'`, a.TaskID, a.AttemptID, string(a.Retry))
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 { // 持有任务行锁时不会发生；保留 CAS 作为最后一道检查
			return rejectf(persistence.CodeNotRunnable, "任务 %s 已不在 queued", a.TaskID)
		}
		payload, _ := json.Marshal(map[string]any{"attempt_no": a.AttemptNo, "env_id": a.EnvID})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: a.TaskID, key: "attempt_created:" + a.AttemptID,
			attemptID: a.AttemptID, typ: "attempt_created", payload: payload}); err != nil {
			return err
		}
		out = task.Attempt{AttemptID: a.AttemptID, TaskID: a.TaskID, AttemptNo: a.AttemptNo, EnvID: a.EnvID, Status: "starting"}
		return nil
	})
	return out, err
}

// admitAttempt 检查创建 attempt 的准入前置条件（规格 §8.1）。旧环境的 stopped_at 只会从空变为
// 非空，因此不加锁读取至多得到偏保守的"未停止"。
func admitAttempt(ctx context.Context, tx pgx.Tx, taskID, status, desired string) error {
	if status != "queued" || desired != "run" {
		return rejectf(persistence.CodeNotRunnable, "任务 %s 处于 %s、desired = %s", taskID, status, desired)
	}
	var running int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN environments e ON e.attempt_id = a.attempt_id
		WHERE a.task_id = $1 AND e.stopped_at IS NULL`, taskID).Scan(&running); err != nil {
		return err
	}
	if running > 0 {
		return rejectf(persistence.CodePreviousNotStopped, "任务 %s 有 %d 个旧环境尚未确认停止", taskID, running)
	}
	return nil
}

// GetAttempt 读取 attempt（实现 task.Store）。
func (s *Store) GetAttempt(ctx context.Context, attemptID string) (task.Attempt, error) {
	var out task.Attempt
	err := s.read(ctx, "GetAttempt", func(ctx context.Context, q queryer) error {
		a, err := selectAttempt(ctx, q, attemptID, false)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", attemptID)
		}
		out = a
		return err
	})
	return out, err
}

func selectAttempt(ctx context.Context, q queryer, attemptID string, forUpdate bool) (task.Attempt, error) {
	sql := "SELECT attempt_id, task_id, attempt_no, env_id, status, outcome_class, verdict_hash FROM attempts WHERE attempt_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var a task.Attempt
	err := q.QueryRow(ctx, sql, attemptID).Scan(&a.AttemptID, &a.TaskID, &a.AttemptNo, &a.EnvID, &a.Status, &a.OutcomeClass, &a.VerdictHash)
	return a, err
}

// controlTransition 是规格 §8.1 中控制意图引起的任务状态转换：cancel 使 queued、paused 直接
// cancelled，使执行中的任务进入 cancelling；pause 使 queued 进入 paused，使执行中的任务进入
// pausing；run（resume）使 paused 回到 queued。其余组合不是合法转换。
func controlTransition(status, desired string) (next string, ok bool) {
	switch desired {
	case "cancel":
		switch status {
		case "queued", "paused":
			return "cancelled", true
		case "running", "pausing", "cancelling":
			return "cancelling", true
		}
	case "pause":
		switch status {
		case "queued":
			return "paused", true
		case "running", "pausing":
			return "pausing", true
		case "paused":
			return "paused", true
		}
	case "run":
		switch status {
		case "paused", "queued":
			return "queued", true
		case "running":
			return "running", true
		}
	}
	return "", false
}

// ApplyControl 以 applied_control_version 的 CAS 应用控制意图（实现 task.Store）。持有任务锁后
// 依次核对：已应用到不低于目标的版本时返回当前状态（重放）；目标版本尚未被接受为冲突；目标版本
// 已被更新的控制取代为 control_changed（调用方应用最新版本）；终态任务为 task_ended；调用方给出的
// 状态必须是 controlTransition 对当前状态与 desired 的结果。写入以来源状态 CAS（规格 §8.1）。
// 锁顺序：tasks → task_control → task_event_seq。
func (s *Store) ApplyControl(ctx context.Context, c task.ApplyControl) (task.ControlState, error) {
	if c.TaskID == "" || c.ControlVersion < 1 || c.Status == "" {
		return task.ControlState{}, invalidf("ApplyControl 缺少 task_id、control_version 或 status")
	}
	var out task.ControlState
	err := s.run(ctx, "ApplyControl", fmt.Sprintf("%s@%d", c.TaskID, c.ControlVersion), func(ctx context.Context, tx pgx.Tx) error {
		st, err := selectControlState(ctx, tx, c.TaskID, true)
		if err != nil {
			return err
		}
		switch {
		case st.AppliedControlVersion >= c.ControlVersion:
			out = st
			return nil
		case c.ControlVersion > st.ControlVersion:
			return conflictf("任务 %s 的控制版本 %d 尚未被接受（当前 %d）", c.TaskID, c.ControlVersion, st.ControlVersion)
		case c.ControlVersion < st.ControlVersion:
			return rejectf(persistence.CodeControlChanged, "任务 %s 的控制版本 %d 已被 %d 取代", c.TaskID, c.ControlVersion, st.ControlVersion)
		case st.Status == "succeeded" || st.Status == "failed" || st.Status == "cancelled":
			return rejectf(persistence.CodeTaskEnded, "任务 %s 已是 %s", c.TaskID, st.Status)
		}
		if next, ok := controlTransition(st.Status, st.Desired); !ok || c.Status != next {
			return invalidf("任务 %s 处于 %s、desired = %s 时不能转换为 %s", c.TaskID, st.Status, st.Desired, c.Status)
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks SET applied_control_version = $2, status = $3, status_reason = $4,
			row_version = row_version + 1 WHERE task_id = $1 AND status = $5`, c.TaskID, c.ControlVersion, c.Status, c.StatusReason, st.Status)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 { // 持有任务行锁时不会发生；保留 CAS 作为最后一道检查
			return conflictf("任务 %s 已不在 %s", c.TaskID, st.Status)
		}
		if err := lockEventSeq(ctx, tx, c.TaskID); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"control_version": c.ControlVersion, "status": c.Status})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: c.TaskID, key: fmt.Sprintf("control_applied:%d", c.ControlVersion),
			typ: "control_applied", payload: payload}); err != nil {
			return err
		}
		st.AppliedControlVersion, st.Status = c.ControlVersion, c.Status
		out = st
		return nil
	})
	return out, err
}

// GetControlState 读取任务的控制状态（实现 task.Store）。
func (s *Store) GetControlState(ctx context.Context, taskID string) (task.ControlState, error) {
	var out task.ControlState
	err := s.read(ctx, "GetControlState", func(ctx context.Context, q queryer) error {
		st, err := selectControlState(ctx, q, taskID, false)
		out = st
		return err
	})
	return out, err
}

// selectControlState 读取任务的控制状态；forUpdate 时先锁 tasks 再锁 task_control（锁顺序）。
func selectControlState(ctx context.Context, q queryer, taskID string, forUpdate bool) (task.ControlState, error) {
	st := task.ControlState{TaskID: taskID}
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	err := q.QueryRow(ctx, "SELECT status, applied_control_version FROM tasks WHERE task_id = $1"+lock, taskID).
		Scan(&st.Status, &st.AppliedControlVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return st, err
	}
	err = q.QueryRow(ctx, "SELECT desired, control_version FROM task_control WHERE task_id = $1"+lock, taskID).
		Scan(&st.Desired, &st.ControlVersion)
	return st, err
}

// verdictAllowed 是规格 §8.1 最终裁决对 desired 的约束：cancel → cancelled；pause → paused；
// run → 按 §5.8 与 §14.3 裁决为 succeeded、failed，或故障重试回到 queued。
func verdictAllowed(desired, taskStatus string) bool {
	switch desired {
	case "cancel":
		return taskStatus == "cancelled"
	case "pause":
		return taskStatus == "paused"
	default:
		return taskStatus == "succeeded" || taskStatus == "failed" || taskStatus == "queued"
	}
}

// verdictHash 是完整判决内容的哈希；FromStatus 是 CAS 来源，不属于判决内容（设计 §2.4）。
func verdictHash(v task.Verdict) ([]byte, error) {
	content := v
	content.FromStatus = ""
	b, err := json.Marshal(content) // 字段顺序固定；RawMessage 被压缩为规范形式
	if err != nil {
		return nil, invalidf("判决无法编码: %v", err)
	}
	return contentHash(b), nil
}

// FinalizeAttempt 提交判决、任务状态与终态 host 事件（实现 task.Store）。完整判决内容一致
// 才返回原结果；已有不同判决为冲突。新判决在持有任务锁后核对最新事实（规格 §8.1）：
// 提交者必须仍是当前 attempt，且任务仍处于 running、pausing 或 cancelling（stale_attempt）；
// 判决依据的控制版本必须是最新（control_changed，调用方按最新控制重算）；任务状态必须与 desired
// 相符；attempt 必须处于 FromStatus。判决同时把 applied_control_version 推进到该控制版本。
// 锁顺序：tasks → task_control → task_event_seq → attempts。
func (s *Store) FinalizeAttempt(ctx context.Context, v task.Verdict) (task.Attempt, error) {
	if v.AttemptID == "" || v.TaskID == "" || v.ControlVersion < 1 || v.FromStatus == "" || v.AttemptStatus == "" ||
		v.TaskStatus == "" || v.EventType == "" {
		return task.Attempt{}, invalidf("FinalizeAttempt 缺少必填字段")
	}
	if v.NotBefore != nil && v.TaskStatus != "queued" {
		return task.Attempt{}, invalidf("not_before 只用于回到 queued 的故障重试，任务状态为 %s", v.TaskStatus)
	}
	hash, err := verdictHash(v)
	if err != nil {
		return task.Attempt{}, err
	}
	var out task.Attempt
	err = s.run(ctx, "FinalizeAttempt", v.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		var current *string
		var status string
		err := tx.QueryRow(ctx, "SELECT current_attempt_id, status FROM tasks WHERE task_id = $1 FOR UPDATE", v.TaskID).Scan(&current, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", v.TaskID)
		}
		if err != nil {
			return err
		}
		var desired string
		var controlVersion int64
		if err := tx.QueryRow(ctx, "SELECT desired, control_version FROM task_control WHERE task_id = $1 FOR SHARE",
			v.TaskID).Scan(&desired, &controlVersion); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, v.TaskID); err != nil {
			return err
		}
		a, err := selectAttempt(ctx, tx, v.AttemptID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("attempt %s", v.AttemptID)
		}
		if err != nil {
			return err
		}
		switch {
		case a.TaskID != v.TaskID:
			return conflictf("attempt %s 不属于任务 %s", v.AttemptID, v.TaskID)
		case a.VerdictHash != nil && bytes.Equal(a.VerdictHash, hash):
			out = a
			return nil
		case a.VerdictHash != nil:
			return conflictf("attempt %s 已有不同的判决", v.AttemptID)
		case current == nil || *current != v.AttemptID:
			return rejectf(persistence.CodeStaleAttempt, "attempt %s 不是任务 %s 的当前 attempt", v.AttemptID, v.TaskID)
		case status != "running" && status != "pausing" && status != "cancelling":
			return rejectf(persistence.CodeStaleAttempt, "任务 %s 处于 %s，不接受判决", v.TaskID, status)
		case controlVersion != v.ControlVersion:
			return rejectf(persistence.CodeControlChanged, "判决依据控制版本 %d，当前为 %d（desired = %s）",
				v.ControlVersion, controlVersion, desired)
		case !verdictAllowed(desired, v.TaskStatus):
			return invalidf("desired = %s 时任务不能裁决为 %s", desired, v.TaskStatus)
		case a.Status != v.FromStatus:
			return conflictf("attempt %s 处于 %s，不是 %s", v.AttemptID, a.Status, v.FromStatus)
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET status = $2, outcome_class = $3, exit_code = $4, exit_signal = $5,
			oom_kill_delta = $6, platform_killed = $7, verdict_hash = $8 WHERE attempt_id = $1`,
			v.AttemptID, v.AttemptStatus, v.OutcomeClass, v.ExitCode, v.ExitSignal, v.OOMKillDelta, v.PlatformKilled, hash); err != nil {
			return err
		}
		// 判决已按 controlVersion 裁决，一并推进 applied_control_version，actor 不再对已裁决的任务应用控制。
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, status_reason = $3, result_json = $4,
			applied_control_version = GREATEST(applied_control_version, $5), not_before = $6, row_version = row_version + 1
			WHERE task_id = $1`, v.TaskID, v.TaskStatus, v.TaskStatusReason, nullJSON(v.Result), controlVersion, v.NotBefore); err != nil {
			return err
		}
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: v.TaskID, key: "attempt_finalized:" + v.AttemptID,
			attemptID: v.AttemptID, typ: v.EventType, payload: []byte(v.EventPayload)}); err != nil {
			return err
		}
		a.Status, a.OutcomeClass, a.VerdictHash = v.AttemptStatus, v.OutcomeClass, hash
		out = a
		return nil
	})
	return out, err
}
