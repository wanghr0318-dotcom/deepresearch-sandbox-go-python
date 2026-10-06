package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/runner"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

var _ task.Store = (*Store)(nil)

// CreateAttempt 创建 attempt、attempt_access（active）与任务环境记录，并把任务从 queued 推进到 running
// （实现 task.Store）。同一 attempt_id 已存在时只比较内容并返回原结果；新建时在同一事务内检查准入
// 前置条件（规格 §8.1）：任务处于 queued、desired = run、旧执行的环境均已确认停止。
// 锁顺序：tasks → task_control → task_event_seq → subruns → attempts → attempt_access → environments。新建时同事务
// 重新绑定 sub-run（rebindSubrunsTx，规格 §13.5），结果随 Attempt.Subruns 返回。
func (s *Store) CreateAttempt(ctx context.Context, a task.NewAttempt) (task.Attempt, error) {
	if a.TaskID == "" || a.AttemptID == "" || a.EnvID == "" || a.AttemptNo < 1 {
		return task.Attempt{}, invalidf("CreateAttempt 缺少 task_id、attempt_id、env_id 或 attempt_no")
	}
	switch a.Retry {
	case task.RetryNone, task.RetryFault, task.RetryOOM:
	default:
		return task.Attempt{}, invalidf("CreateAttempt 的重试类别 %q 未定义", a.Retry)
	}
	if (a.SessionID == "") != (a.IncarnationID == "") {
		return task.Attempt{}, invalidf("CreateAttempt 的 session_id 与 incarnation_id 须同时提供")
	}
	var out task.Attempt
	err := s.run(ctx, "CreateAttempt", a.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		var turn *sessionTurn
		if a.SessionID != "" { // 会话 turn：锁顺序从 sessions 开始（§8.1）
			t, err := lockSessionTurn(ctx, tx, a.SessionID, a.IncarnationID)
			if err != nil {
				return err
			}
			turn = &t
		}
		var status, desired string
		var taskSession *string
		err := tx.QueryRow(ctx, "SELECT status, session_id FROM tasks WHERE task_id = $1 FOR UPDATE", a.TaskID).Scan(&status, &taskSession)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", a.TaskID)
		}
		if err != nil {
			return err
		}
		if (taskSession == nil && a.SessionID != "") || (taskSession != nil && *taskSession != a.SessionID) {
			return invalidf("任务 %s 的会话与请求的会话 %q 不符", a.TaskID, a.SessionID)
		}
		if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR SHARE", a.TaskID).Scan(&desired); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, a.TaskID); err != nil {
			return err
		}
		if err := lockTaskSubrunsTx(ctx, tx, a.TaskID); err != nil {
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
		sessionEnv := ""
		if turn != nil {
			if a.EnvID != turn.incEnvID {
				return invalidf("会话 turn 的 attempt 须在 incarnation %s 的环境 %s 中运行，得到 %s", a.IncarnationID, turn.incEnvID, a.EnvID)
			}
			sessionEnv = turn.incEnvID
		} else {
			var envTaken bool // 否则 INSERT 的 23505 会被当作可重试错误一直重试到期限
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM environments WHERE env_id = $1)", a.EnvID).Scan(&envTaken); err != nil {
				return err
			}
			if envTaken {
				return conflictf("环境 %s 已存在", a.EnvID)
			}
		}
		if err := admitAttempt(ctx, tx, a.TaskID, status, desired, sessionEnv); err != nil {
			return err
		}
		if turn != nil {
			if err := turn.admit(a.TaskID, a.IncarnationID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempts (attempt_id, task_id, attempt_no, env_id, status) VALUES ($1, $2, $3, $4, 'starting')",
			a.AttemptID, a.TaskID, a.AttemptNo, a.EnvID); err != nil {
			return err
		}
		// sub-run 在新 attempt 行插入之后重新绑定（bound_attempt_id 外键）；行锁已在上面按锁顺序取得（规格 §13.5）。
		subruns, err := rebindSubrunsTx(ctx, tx, a.TaskID, a.AttemptID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO attempt_access (attempt_id, task_id, state) VALUES ($1, $2, 'active')", a.AttemptID, a.TaskID); err != nil {
			return err
		}
		if turn == nil { // 会话 turn 在 incarnation 的会话环境中运行，不另建任务环境
			if _, err := tx.Exec(ctx, "INSERT INTO environments (env_id, kind, attempt_id, status) VALUES ($1, 'task', $2, 'creating')",
				a.EnvID, a.AttemptID); err != nil {
				return err
			}
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
		if turn != nil {
			if err := turn.start(ctx, tx, a.TaskID, a.IncarnationID); err != nil {
				return err
			}
		}
		payload, _ := json.Marshal(map[string]any{"attempt_no": a.AttemptNo, "env_id": a.EnvID})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: a.TaskID, key: "attempt_created:" + a.AttemptID,
			attemptID: a.AttemptID, typ: "attempt_created", payload: payload}); err != nil {
			return err
		}
		out = task.Attempt{AttemptID: a.AttemptID, TaskID: a.TaskID, AttemptNo: a.AttemptNo, EnvID: a.EnvID, Status: "starting"}
		for _, r := range subruns {
			out.Subruns = append(out.Subruns, task.SubrunState{SubrunID: r.SubrunID, Status: r.Status, ResultRef: r.ResultRef})
		}
		return nil
	})
	return out, err
}

// lockTaskSubrunsTx 按锁顺序（task_event_seq → subruns → attempts）以 FOR UPDATE 锁住任务的全部 sub-run 行（按
// subrun_id 升序）。创建 attempt 与最终裁决先取得它们，之后的 rebindSubrunsTx / closeOpenSubrunsTx 不再取新锁。
func lockTaskSubrunsTx(ctx context.Context, tx pgx.Tx, taskID string) error {
	_, err := tx.Exec(ctx, "SELECT 1 FROM subruns WHERE task_id = $1 ORDER BY subrun_id FOR UPDATE", taskID)
	return err
}

// admitAttempt 检查创建 attempt 的准入前置条件（规格 §8.1）。旧环境的 stopped_at 只会从空变为
// 非空，因此不加锁读取至多得到偏保守的"未停止"。会话 turn 的旧 attempt 在会话环境中运行（无任务环境记录）：
// 在 sessionEnv（授予的 incarnation 的环境）中的旧 attempt 是正常切换（上一 attempt 已释放，复用同一 incarnation），
// 在其他会话环境中的旧 attempt 要求该环境已停止（故障恢复或替换，§8.1 (b)）。
func admitAttempt(ctx context.Context, tx pgx.Tx, taskID, status, desired, sessionEnv string) error {
	if status != "queued" || desired != "run" {
		return rejectf(persistence.CodeNotRunnable, "任务 %s 处于 %s、desired = %s", taskID, status, desired)
	}
	var running int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN environments e
			ON e.attempt_id = a.attempt_id OR (e.kind = 'session' AND e.env_id = a.env_id)
		WHERE a.task_id = $1 AND e.stopped_at IS NULL AND e.env_id <> $2`, taskID, sessionEnv).Scan(&running); err != nil {
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

// ApplyControl 以 applied_control_version 的 CAS 应用控制意图（实现 task.Store）。持有任务锁后
// 依次核对：已应用到不低于目标的版本时返回当前状态（重放）；目标版本尚未被接受为冲突；目标版本
// 已被更新的控制取代为 control_changed（调用方应用最新版本）；终态任务为 task_ended；调用方给出的
// 状态必须是 task.ControlTransition 对当前状态与 desired 的结果。写入以来源状态 CAS（规格 §8.1）。
// 锁顺序：tasks → task_control → task_event_seq。
func (s *Store) ApplyControl(ctx context.Context, c task.ApplyControl) (task.ControlState, error) {
	if c.TaskID == "" || c.ControlVersion < 1 || c.Status == "" {
		return task.ControlState{}, invalidf("ApplyControl 缺少 task_id、control_version 或 status")
	}
	var out task.ControlState
	err := s.run(ctx, "ApplyControl", fmt.Sprintf("%s@%d", c.TaskID, c.ControlVersion), func(ctx context.Context, tx pgx.Tx) error {
		sessionID, err := taskSessionID(ctx, tx, c.TaskID) // 会话 turn：从 sessions FOR UPDATE 开始（锁顺序）
		if err != nil {
			return err
		}
		if sessionID != "" {
			if _, _, err := lockSession(ctx, tx, sessionID); err != nil {
				return err
			}
		}
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
		case task.IsTerminal(st.Status):
			return rejectf(persistence.CodeTaskEnded, "任务 %s 已是 %s", c.TaskID, st.Status)
		}
		if next, ok := task.ControlTransition(st.Status, st.Desired); !ok || c.Status != next {
			return invalidf("任务 %s 处于 %s、desired = %s 时不能转换为 %s", c.TaskID, st.Status, st.Desired, c.Status)
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks SET applied_control_version = $2, status = $3, status_reason = $4,
			row_version = row_version + 1, resume_directive = CASE WHEN $3 IN ('paused', 'cancelled') THEN NULL ELSE resume_directive END
			WHERE task_id = $1 AND status = $5`, c.TaskID, c.ControlVersion, c.Status, c.StatusReason, st.Status)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 { // 持有任务行锁时不会发生；保留 CAS 作为最后一道检查
			return conflictf("任务 %s 已不在 %s", c.TaskID, st.Status)
		}
		if sessionID != "" {
			// 不经 attempt 的暂停（queued → paused）阻塞会话队列；取消（含 D5 取代、关闭）清除本 turn 的阻塞与占用（§12.3）。
			if err := releaseSessionTurn(ctx, tx, sessionID, c.TaskID, c.Status); err != nil {
				return err
			}
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
// 锁顺序：tasks → task_control → task_event_seq → subruns → attempts。
func (s *Store) FinalizeAttempt(ctx context.Context, v task.Verdict) (task.Attempt, error) {
	if v.AttemptID == "" || v.TaskID == "" || v.ControlVersion < 1 || v.FromStatus == "" || v.AttemptStatus == "" ||
		v.TaskStatus == "" || v.EventType == "" {
		return task.Attempt{}, invalidf("FinalizeAttempt 缺少必填字段")
	}
	if v.NotBefore != nil && v.TaskStatus != "queued" {
		return task.Attempt{}, invalidf("not_before 只用于回到 queued 的故障重试，任务状态为 %s", v.TaskStatus)
	}
	if ss := v.SessionState; ss != nil {
		if v.TaskStatus != "succeeded" || ss.CheckpointID == "" || (len(ss.State) == 0) == (ss.StateRef == "") {
			return task.Attempt{}, invalidf("session_state 只用于成功裁决，且须有 checkpoint_id 与恰好一个 state 或 state_ref")
		}
	}
	hash, err := verdictHash(v)
	if err != nil {
		return task.Attempt{}, err
	}
	var out task.Attempt
	err = s.run(ctx, "FinalizeAttempt", v.AttemptID, func(ctx context.Context, tx pgx.Tx) error {
		sessionID, err := taskSessionID(ctx, tx, v.TaskID) // session_id 不变：先取会话再按锁顺序加锁
		if err != nil {
			return err
		}
		var turn *turnVerdict
		if sessionID != "" {
			tv, err := lockTurnVerdict(ctx, tx, sessionID, v.AttemptID)
			if err != nil {
				return err
			}
			turn = &tv
		}
		switch {
		case turn == nil && v.SessionState != nil:
			return invalidf("任务 %s 不属于会话，不能提交 session_state", v.TaskID)
		case turn != nil && v.TaskStatus == "succeeded" && v.SessionState == nil:
			return invalidf("会话 turn %s 的成功裁决须提交 session_state", v.TaskID)
		}
		var current, base *string
		var status string
		err = tx.QueryRow(ctx, "SELECT current_attempt_id, status, base_session_checkpoint_id FROM tasks WHERE task_id = $1 FOR UPDATE",
			v.TaskID).Scan(&current, &status, &base)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", v.TaskID)
		}
		if err != nil {
			return err
		}
		committed := ""
		switch {
		case turn == nil:
		case v.SessionState != nil:
			committed = v.SessionState.CheckpointID
		case base != nil:
			committed = *base
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
		if err := lockTaskSubrunsTx(ctx, tx, v.TaskID); err != nil {
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
			a.CommittedSessionCheckpointID = committed
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
		case !task.VerdictAllowedReason(desired, v.TaskStatus, v.TaskStatusReason):
			return invalidf("desired = %s 时任务不能裁决为 %s（%s）", desired, v.TaskStatus, v.TaskStatusReason)
		case a.Status != v.FromStatus:
			return conflictf("attempt %s 处于 %s，不是 %s", v.AttemptID, a.Status, v.FromStatus)
		}
		if turn != nil {
			if err := turn.apply(ctx, tx, v, base); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE attempts SET status = $2, outcome_class = $3, exit_code = $4, exit_signal = $5,
			oom_kill_delta = $6, platform_killed = $7, verdict_hash = $8 WHERE attempt_id = $1`,
			v.AttemptID, v.AttemptStatus, v.OutcomeClass, v.ExitCode, v.ExitSignal, v.OOMKillDelta, v.PlatformKilled, hash); err != nil {
			return err
		}
		// 判决已按 controlVersion 裁决，一并推进 applied_control_version，actor 不再对已裁决的任务应用控制。
		// 终态与暂停裁决清空续跑指令（tasks.resume_directive 只用于下一次 attempt 的 task_start）。
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, status_reason = $3, result_json = $4,
			applied_control_version = GREATEST(applied_control_version, $5), not_before = $6, row_version = row_version + 1,
			resume_directive = CASE WHEN $2 = 'queued' THEN resume_directive END
			WHERE task_id = $1`, v.TaskID, v.TaskStatus, v.TaskStatusReason, nullJSON(v.Result), controlVersion, v.NotBefore); err != nil {
			return err
		}
		// 最终裁决收尾仍未终态的 sub-run（§8.4）：cancelled → cancelled（task_cancel），succeeded → failed
		// （not_completed_at_result）；其他裁决（回到 queued、paused、failed）不改动，留待恢复时重新绑定。
		if err := closeOpenSubrunsTx(ctx, tx, v.TaskID, v.TaskStatus); err != nil {
			return err
		}
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: v.TaskID, key: "attempt_finalized:" + v.AttemptID,
			attemptID: v.AttemptID, typ: v.EventType, payload: []byte(v.EventPayload)}); err != nil {
			return err
		}
		a.Status, a.OutcomeClass, a.VerdictHash = v.AttemptStatus, v.OutcomeClass, hash
		a.CommittedSessionCheckpointID = committed
		out = a
		return nil
	})
	return out, err
}

// ---- 会话 turn（规格 §8.1、§12.3；M4 Plan 12 Task 4） ----

// sessionTurn 是会话 turn 创建 attempt 时在 sessions FOR UPDATE 下读取的会话与 incarnation 事实。
type sessionTurn struct {
	sessionID, status, currentInc, currentTask, blockedBy string
	latest                                                *string // session_progress.latest_checkpoint_id
	incSession, incStatus, incEnvID                       string
}

// lockSessionTurn 按锁顺序 sessions → session_progress → incarnations 锁住会话 turn 准入需要的行。
func lockSessionTurn(ctx context.Context, tx pgx.Tx, sessionID, incarnationID string) (sessionTurn, error) {
	t := sessionTurn{sessionID: sessionID}
	err := tx.QueryRow(ctx, `SELECT status, COALESCE(current_incarnation_id, ''), COALESCE(current_task_id, ''),
			COALESCE(blocked_by_task_id, '') FROM sessions WHERE session_id = $1 FOR UPDATE`, sessionID).
		Scan(&t.status, &t.currentInc, &t.currentTask, &t.blockedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, notFoundf("会话 %s", sessionID)
	}
	if err != nil {
		return t, err
	}
	if err := tx.QueryRow(ctx, "SELECT latest_checkpoint_id FROM session_progress WHERE session_id = $1 FOR UPDATE",
		sessionID).Scan(&t.latest); err != nil {
		return t, err
	}
	inc, err := selectIncarnation(ctx, tx, incarnationID, true)
	if err != nil {
		return t, err
	}
	t.incSession, t.incStatus, t.incEnvID = inc.SessionID, inc.Status, inc.EnvID
	return t, nil
}

// admit 是会话 turn 的 attempt 准入（§8.1）：会话不被其他 turn 阻塞或占用、处于 idle（或 running 且未被占用），
// 授予的 incarnation 是会话的当前 incarnation 且为 idle（上一 turn 已释放）。
func (t sessionTurn) admit(taskID, incarnationID string) error {
	switch {
	case t.blockedBy != "" && t.blockedBy != taskID:
		return rejectf(persistence.CodeSessionBlocked, "会话 %s 被 turn %s 阻塞", t.sessionID, t.blockedBy)
	case t.currentTask != "" && t.currentTask != taskID:
		return rejectf(persistence.CodeSessionBlocked, "会话 %s 正在运行 turn %s", t.sessionID, t.currentTask)
	case t.status != "idle" && t.status != "running":
		return rejectf(persistence.CodeSessionBlocked, "会话 %s 处于 %s", t.sessionID, t.status)
	case t.incSession != t.sessionID || t.currentInc != incarnationID || t.incStatus != "idle":
		return rejectf(persistence.CodeIncarnationNotIdle, "incarnation %s（会话 %s，%s）不是会话 %s 的空闲当前 incarnation（当前 %q）",
			incarnationID, t.incSession, t.incStatus, t.sessionID, t.currentInc)
	}
	return nil
}

// start 是会话 turn 的交接写入（§8.1）：清除本 turn 的阻塞、current_task_id = 本 turn、会话 idle → running
// （row_version + 1，使 session actor 之后的 CAS 读到新事实）、incarnation idle → busy、
// tasks.base_session_checkpoint_id = 当前会话指针。会话生命周期事件（session_events）只由 session actor 的 Transition
// 追加；turn 的运行由 attempt_created 等 turn 事件反映。
func (t sessionTurn) start(ctx context.Context, tx pgx.Tx, taskID, incarnationID string) error {
	if _, err := tx.Exec(ctx, "UPDATE incarnations SET status = 'busy' WHERE incarnation_id = $1", incarnationID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET current_task_id = $2, blocked_by_task_id = NULL, status = 'running',
			idle_since = NULL, frozen_since = NULL, last_active_at = now(),
			row_version = row_version + CASE WHEN status = 'idle' THEN 1 ELSE 0 END
		WHERE session_id = $1`, t.sessionID, taskID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE tasks SET base_session_checkpoint_id = $2 WHERE task_id = $1", taskID, t.latest)
	return err
}

// turnVerdict 是会话 turn 裁决时在 sessions FOR UPDATE 下读取的事实。
type turnVerdict struct {
	sessionID        string
	latestID         *string // session_progress.latest_checkpoint_id
	latestSeq        int64
	incID, incStatus string // 运行该 attempt 的 incarnation（按环境；没有则为空）
}

// lockTurnVerdict 按锁顺序 sessions → session_progress → incarnations 锁住会话 turn 裁决需要的行。attempt 的 env_id
// 不变，按它找到运行该 attempt 的 incarnation。
func lockTurnVerdict(ctx context.Context, tx pgx.Tx, sessionID, attemptID string) (turnVerdict, error) {
	tv := turnVerdict{sessionID: sessionID}
	if _, _, err := lockSession(ctx, tx, sessionID); err != nil {
		return tv, err
	}
	if err := tx.QueryRow(ctx, "SELECT latest_checkpoint_id, latest_commit_seq FROM session_progress WHERE session_id = $1 FOR UPDATE",
		sessionID).Scan(&tv.latestID, &tv.latestSeq); err != nil {
		return tv, err
	}
	err := tx.QueryRow(ctx, `SELECT i.incarnation_id, i.status FROM incarnations i
		WHERE i.session_id = $1 AND i.env_id = (SELECT env_id FROM attempts WHERE attempt_id = $2) FOR UPDATE`, sessionID, attemptID).
		Scan(&tv.incID, &tv.incStatus)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return tv, err
	}
	return tv, nil
}

// apply 写入会话 turn 裁决的会话侧效果（§8.1、§12.3）：
//   - succeeded：base 须等于当前指针（否则 session_base_mismatch）；session_state 的引用须已授权到 attempt、task 或
//     会话 scope（否则 ref_not_authorized），同事务补授权到会话 scope；插入 checkpoints(scope = session) 并推进
//     session_progress（I10）；
//   - paused（含 awaiting_input）：blocked_by_task_id = 本 turn；
//   - failed、cancelled：清除等于本 turn 的 blocked_by_task_id；
//   - 除 queued（故障重试，turn 仍占用会话）外清除等于本 turn 的 current_task_id；
//   - 运行该 attempt 的 incarnation busy → releasing（释放后由 session actor 置回 idle）。
func (tv turnVerdict) apply(ctx context.Context, tx pgx.Tx, v task.Verdict, base *string) error {
	if ss := v.SessionState; ss != nil {
		if (base == nil) != (tv.latestID == nil) || (base != nil && *base != *tv.latestID) {
			return rejectf(persistence.CodeSessionBaseMismatch, "turn %s 的 base 为 %s，会话 %s 的当前指针为 %s",
				v.TaskID, strOr(base, "∅"), tv.sessionID, strOr(tv.latestID, "∅"))
		}
		if err := tv.commitSessionState(ctx, tx, v); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET
			current_task_id = CASE WHEN $3 <> 'queued' AND current_task_id = $2 THEN NULL ELSE current_task_id END,
			blocked_by_task_id = CASE WHEN $3 = 'paused' THEN $2
				WHEN $3 <> 'queued' AND blocked_by_task_id = $2 THEN NULL ELSE blocked_by_task_id END
		WHERE session_id = $1`, tv.sessionID, v.TaskID, v.TaskStatus); err != nil {
		return err
	}
	if tv.incStatus == "busy" {
		if _, err := tx.Exec(ctx, "UPDATE incarnations SET status = 'releasing' WHERE incarnation_id = $1", tv.incID); err != nil {
			return err
		}
	}
	return nil
}

// commitSessionState 插入成功裁决提议的 session checkpoint 并推进 session_progress（I10：只由成功裁决推进）。
func (tv turnVerdict) commitSessionState(ctx context.Context, tx pgx.Tx, v task.Verdict) error {
	ss := v.SessionState
	refs := append([]string(nil), ss.Refs...)
	if ss.StateRef != "" {
		refs = append(refs, ss.StateRef)
	}
	if len(refs) > 0 {
		rows, err := tx.Query(ctx, `SELECT r FROM unnest($1::text[]) AS r WHERE NOT EXISTS (
			SELECT 1 FROM scope_blobs sb WHERE sb.sha256 = r AND ((sb.scope_kind = 'attempt' AND sb.scope_id = $2)
				OR (sb.scope_kind = 'task' AND sb.scope_id = $3) OR (sb.scope_kind = 'session' AND sb.scope_id = $4)))`,
			refs, v.AttemptID, v.TaskID, tv.sessionID)
		if err != nil {
			return err
		}
		missing, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return rejectf(persistence.CodeRefNotAuthorized, "session_state 引用 %v 不存在或未授权到 turn %s 或会话 %s", missing, v.TaskID, tv.sessionID)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO scope_blobs (scope_kind, scope_id, sha256) SELECT 'session', $1, r FROM unnest($2::text[]) AS r
			ON CONFLICT DO NOTHING`, tv.sessionID, refs); err != nil {
			return err
		}
	}
	var taken bool // 否则主键冲突的 23505 会被当作可重试错误
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM checkpoints WHERE scope_kind = 'session' AND scope_id = $1 AND checkpoint_id = $2)",
		tv.sessionID, ss.CheckpointID).Scan(&taken); err != nil {
		return err
	}
	if taken {
		return conflictf("会话 %s 已有 checkpoint %s", tv.sessionID, ss.CheckpointID)
	}
	cp := runner.Checkpoint{Scope: runner.Scope{Kind: "session", ID: tv.sessionID}, CheckpointID: ss.CheckpointID, AttemptID: v.AttemptID,
		StepID: sessionStateStep, State: ss.State, StateRef: ss.StateRef, Refs: ss.Refs}
	b, _ := json.Marshal(nonNil(ss.Refs))
	seq := tv.latestSeq + 1
	if _, err := tx.Exec(ctx, `INSERT INTO checkpoints (scope_kind, scope_id, checkpoint_id, commit_seq, attempt_id, step_id,
			content_hash, state_inline, state_ref, refs_json)
		VALUES ('session', $1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9)`,
		tv.sessionID, ss.CheckpointID, seq, v.AttemptID, sessionStateStep, checkpointHash(cp), nullJSON(ss.State), ss.StateRef, b); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE session_progress SET latest_checkpoint_id = $2, latest_commit_seq = $3 WHERE session_id = $1",
		tv.sessionID, ss.CheckpointID, seq)
	return err
}

// sessionStateStep 是由 result.session_state 提交的 session checkpoint 的 step_id。
const sessionStateStep = "session_state"

func strOr(s *string, def string) string {
	if s == nil {
		return def
	}
	return *s
}

// releaseSessionTurn 是 ApplyControl 对会话 turn 的会话侧效果：不经 attempt 的暂停（queued → paused）阻塞会话
// 队列；取消（含 paused → cancelled 的 D5 取代与关闭）清除等于本 turn 的 blocked_by_task_id。两者都清除等于本
// turn 的 current_task_id。其余转换不改变会话。调用方已持有 sessions 行锁。
func releaseSessionTurn(ctx context.Context, tx pgx.Tx, sessionID, taskID, status string) error {
	if status != "paused" && status != "cancelled" {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE sessions SET
			current_task_id = CASE WHEN current_task_id = $2 THEN NULL ELSE current_task_id END,
			blocked_by_task_id = CASE WHEN $3 = 'paused' THEN $2 WHEN blocked_by_task_id = $2 THEN NULL ELSE blocked_by_task_id END
		WHERE session_id = $1`, sessionID, taskID, status)
	return err
}
