package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/session"
)

var _ session.Store = (*Store)(nil)

// session actor 的事务用例（规格 §12；M4 Plan 12）。sessions 的生命周期列只由这些用例（及启动恢复）写；
// 每个用例从 sessions FOR UPDATE 开始（锁顺序见 events.go）。

// sessionStatuses 是 sessions.status 的全部取值（与 0006 的检查约束一致）。
var sessionStatuses = []string{
	session.StatusCreating, session.StatusIdle, session.StatusRunning, session.StatusQuiescing, session.StatusFrozen,
	session.StatusEvicting, session.StatusEvicted, session.StatusRestoring, session.StatusClosing, session.StatusClosed,
}

// incarnationStatuses 是 incarnations.status 的全部取值。
var incarnationStatuses = []string{
	session.IncStarting, session.IncIdle, session.IncBusy, session.IncReleasing, session.IncQuiescing, session.IncFrozen,
	session.IncEnded,
}

// terminalTaskSQL 是任务终态的 SQL 列表（与 task.IsTerminal 一致）。
const terminalTaskSQL = "('succeeded', 'failed', 'cancelled')"

// loadSessionState 读取会话事实：会话行、控制行、session checkpoint、当前 incarnation 与非终态 turn。
func loadSessionState(ctx context.Context, q queryer, sessionID string) (session.State, error) {
	st := session.State{SessionID: sessionID}
	var cpID, stateRef, incID, incEnv, incStatus, incReason *string
	var cpSeq *int64
	var state, refs []byte
	var incStarted, incEnded *time.Time
	err := q.QueryRow(ctx, `SELECT s.status, COALESCE(s.owner_user_id, 0), COALESCE(s.uid_range_id, ''),
			COALESCE(s.current_incarnation_id, ''), COALESCE(s.current_task_id, ''), COALESCE(s.blocked_by_task_id, ''),
			s.idle_since, s.frozen_since, s.last_active_at, s.row_version,
			c.desired, c.control_version, c.wake_requested_version, c.applied_wake_version,
			cp.checkpoint_id, cp.commit_seq, cp.state_inline, cp.state_ref, cp.refs_json,
			i.incarnation_id, i.env_id, i.status, i.started_at, i.ended_at, i.end_reason
		FROM sessions s JOIN session_control c USING (session_id) JOIN session_progress p USING (session_id)
		LEFT JOIN checkpoints cp ON cp.scope_kind = 'session' AND cp.scope_id = s.session_id AND cp.checkpoint_id = p.latest_checkpoint_id
		LEFT JOIN incarnations i ON i.incarnation_id = s.current_incarnation_id
		WHERE s.session_id = $1`, sessionID).Scan(&st.Status, &st.OwnerUserID, &st.UIDRangeID,
		&st.CurrentIncarnationID, &st.CurrentTaskID, &st.BlockedByTaskID,
		&st.IdleSince, &st.FrozenSince, &st.LastActiveAt, &st.RowVersion,
		&st.Desired, &st.ControlVersion, &st.WakeRequestedVersion, &st.AppliedWakeVersion,
		&cpID, &cpSeq, &state, &stateRef, &refs,
		&incID, &incEnv, &incStatus, &incStarted, &incEnded, &incReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, notFoundf("会话 %s", sessionID)
	}
	if err != nil {
		return st, err
	}
	if cpID != nil {
		cp := &session.Checkpoint{CheckpointID: *cpID, CommitSeq: *cpSeq, State: state}
		if stateRef != nil {
			cp.StateRef = *stateRef
		}
		if err := json.Unmarshal(refs, &cp.Refs); err != nil {
			return st, err
		}
		st.Latest = cp
	}
	if incID != nil {
		st.Incarnation = &session.Incarnation{IncarnationID: *incID, SessionID: sessionID, EnvID: *incEnv, Status: *incStatus,
			StartedAt: *incStarted, EndedAt: incEnded, EndReason: *incReason}
	}
	rows, err := q.Query(ctx, `SELECT t.task_id, t.status, t.status_reason, c.desired, t.turn_index
		FROM tasks t JOIN task_control c USING (task_id)
		WHERE t.session_id = $1 AND t.status NOT IN `+terminalTaskSQL+` ORDER BY t.turn_index`, sessionID)
	if err != nil {
		return st, err
	}
	st.NonTerminalTurns, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (session.TurnFact, error) {
		var f session.TurnFact
		err := r.Scan(&f.TaskID, &f.Status, &f.StatusReason, &f.Desired, &f.TurnIndex)
		return f, err
	})
	return st, err
}

// LoadSession 一次读取 Decide 需要的会话事实（实现 session.Store）。
func (s *Store) LoadSession(ctx context.Context, sessionID string) (session.State, error) {
	var out session.State
	err := s.read(ctx, "LoadSession", func(ctx context.Context, q queryer) error {
		var err error
		out, err = loadSessionState(ctx, q, sessionID)
		return err
	})
	return out, err
}

// ListOpenSessions 返回 status ≠ closed 的会话，按创建时间排序（实现 session.Store）。
func (s *Store) ListOpenSessions(ctx context.Context) ([]string, error) {
	var out []string
	err := s.read(ctx, "ListOpenSessions", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, "SELECT session_id FROM sessions WHERE status <> 'closed' ORDER BY created_at, session_id")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return out, err
}

// lockSession 以 FOR UPDATE 锁住会话行，返回其 status 与 row_version。
func lockSession(ctx context.Context, tx pgx.Tx, sessionID string) (string, int64, error) {
	var status string
	var rv int64
	err := tx.QueryRow(ctx, "SELECT status, row_version FROM sessions WHERE session_id = $1 FOR UPDATE", sessionID).Scan(&status, &rv)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, notFoundf("会话 %s", sessionID)
	}
	return status, rv, err
}

// statePayload 是会话生命周期事件的 payload（{state}）。
func statePayload(state string) []byte {
	b, _ := json.Marshal(map[string]string{"state": state})
	return b
}

// Transition 以 row_version CAS 改变会话状态（实现 session.Store；见 session.Transition）。同一转换重跑（当前已是
// To 且 row_version = FromRowVersion+1）返回当前事实，事件以 event_key 不重复追加。
// 锁顺序：sessions → session_control → session_event_seq。
func (s *Store) Transition(ctx context.Context, t session.Transition) (session.State, error) {
	if t.SessionID == "" || len(t.From) == 0 || !slices.Contains(sessionStatuses, t.To) {
		return session.State{}, invalidf("Transition 缺少 session_id、来源状态，或目标状态 %q 未定义", t.To)
	}
	if t.Event != nil && !json.Valid(t.Event) {
		return session.State{}, invalidf("Transition 的事件不是 JSON")
	}
	var out session.State
	err := s.run(ctx, "Transition", fmt.Sprintf("%s@%d→%s", t.SessionID, t.FromRowVersion, t.To), func(ctx context.Context, tx pgx.Tx) error {
		status, rv, err := lockSession(ctx, tx, t.SessionID)
		if err != nil {
			return err
		}
		switch {
		case rv == t.FromRowVersion+1 && status == t.To: // 已提交的同一转换（提交结果未知后的重跑）
			out, err = loadSessionState(ctx, tx, t.SessionID)
			return err
		case rv != t.FromRowVersion:
			return conflictf("会话 %s 的 row_version 为 %d，不是 %d", t.SessionID, rv, t.FromRowVersion)
		case !slices.Contains(t.From, status):
			return rejectf(session.CodeInvalidTransition, "会话 %s 处于 %s，不在 %v 中", t.SessionID, status, t.From)
		}
		incarnation := ""
		if t.SetIncarnation != nil {
			incarnation = *t.SetIncarnation
		}
		if _, err := tx.Exec(ctx, `UPDATE sessions SET status = $2, row_version = row_version + 1,
				idle_since = CASE WHEN $2 = 'idle' THEN now() END,
				frozen_since = CASE WHEN $2 = 'frozen' THEN now() END,
				last_active_at = CASE WHEN $2 = 'running' THEN now() ELSE last_active_at END,
				current_incarnation_id = CASE WHEN $3 THEN NULLIF($4, '') ELSE current_incarnation_id END,
				last_error = COALESCE($5, last_error)
			WHERE session_id = $1`, t.SessionID, t.To, t.SetIncarnation != nil, incarnation, t.LastError); err != nil {
			return err
		}
		if t.AppliedWake != nil {
			if _, err := tx.Exec(ctx, "UPDATE session_control SET applied_wake_version = GREATEST(applied_wake_version, $2) WHERE session_id = $1",
				t.SessionID, *t.AppliedWake); err != nil {
				return err
			}
		}
		if t.Event != nil {
			if _, err := appendSessionEvent(ctx, tx, t.SessionID, fmt.Sprintf("%s:%d", t.To, rv+1), t.Event); err != nil {
				return err
			}
		}
		out, err = loadSessionState(ctx, tx, t.SessionID)
		return err
	})
	return out, err
}

// selectIncarnation 读取 incarnation；forUpdate 时加行锁。
func selectIncarnation(ctx context.Context, q queryer, incarnationID string, forUpdate bool) (session.Incarnation, error) {
	sql := "SELECT incarnation_id, session_id, env_id, status, started_at, ended_at, end_reason FROM incarnations WHERE incarnation_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var inc session.Incarnation
	err := q.QueryRow(ctx, sql, incarnationID).Scan(&inc.IncarnationID, &inc.SessionID, &inc.EnvID, &inc.Status, &inc.StartedAt,
		&inc.EndedAt, &inc.EndReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return inc, notFoundf("incarnation %s", incarnationID)
	}
	return inc, err
}

// CreateIncarnation 新建 incarnation 与其会话环境（实现 session.Store）。以 incarnation_id 幂等：已存在且会话与环境
// 相同返回原行。新建要求 row_version 未变、会话未关闭且没有存活 incarnation（I9），否则 ErrConflict。
// 锁顺序：sessions → incarnations → environments。
func (s *Store) CreateIncarnation(ctx context.Context, n session.NewIncarnation) (session.Incarnation, error) {
	if n.IncarnationID == "" || n.SessionID == "" || n.EnvID == "" {
		return session.Incarnation{}, invalidf("CreateIncarnation 缺少 incarnation_id、session_id 或 env_id")
	}
	var out session.Incarnation
	err := s.run(ctx, "CreateIncarnation", n.IncarnationID, func(ctx context.Context, tx pgx.Tx) error {
		status, rv, err := lockSession(ctx, tx, n.SessionID)
		if err != nil {
			return err
		}
		existing, err := selectIncarnation(ctx, tx, n.IncarnationID, false)
		switch {
		case err == nil:
			if existing.SessionID != n.SessionID || existing.EnvID != n.EnvID {
				return conflictf("incarnation %s 已存在且内容不同", n.IncarnationID)
			}
			out = existing
			return nil
		case !errors.Is(err, persistence.ErrNotFound):
			return err
		}
		if rv != n.FromRowVersion {
			return conflictf("会话 %s 的 row_version 为 %d，不是 %d", n.SessionID, rv, n.FromRowVersion)
		}
		if status == session.StatusClosed {
			return rejectf(session.CodeInvalidTransition, "会话 %s 已关闭", n.SessionID)
		}
		var live string // 否则 incarnations_one_live 的 23505 会被当作可重试错误一直重试到期限
		err = tx.QueryRow(ctx, "SELECT incarnation_id FROM incarnations WHERE session_id = $1 AND status <> 'ended'", n.SessionID).Scan(&live)
		if err == nil {
			return conflictf("会话 %s 已有存活的 incarnation %s", n.SessionID, live)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var envTaken bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM environments WHERE env_id = $1)", n.EnvID).Scan(&envTaken); err != nil {
			return err
		}
		if envTaken {
			return conflictf("环境 %s 已存在", n.EnvID)
		}
		if err := tx.QueryRow(ctx, `INSERT INTO incarnations (incarnation_id, session_id, env_id, status) VALUES ($1, $2, $3, 'starting')
			RETURNING incarnation_id, session_id, env_id, status, started_at, ended_at, end_reason`, n.IncarnationID, n.SessionID, n.EnvID).
			Scan(&out.IncarnationID, &out.SessionID, &out.EnvID, &out.Status, &out.StartedAt, &out.EndedAt, &out.EndReason); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO environments (env_id, kind, session_id, status) VALUES ($1, 'session', $2, 'creating')",
			n.EnvID, n.SessionID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE sessions SET current_incarnation_id = $2 WHERE session_id = $1", n.SessionID, n.IncarnationID)
		return err
	})
	return out, err
}

// SetUIDRange 写入会话保留的 UID 范围（实现 session.Store）：为空或已是同一范围时成功，已是另一个范围为 ErrConflict。
func (s *Store) SetUIDRange(ctx context.Context, sessionID, uidRangeID string) error {
	if sessionID == "" || uidRangeID == "" {
		return invalidf("SetUIDRange 缺少 session_id 或 uid_range_id")
	}
	return s.run(ctx, "SetUIDRange", sessionID, func(ctx context.Context, tx pgx.Tx) error {
		var current *string
		err := tx.QueryRow(ctx, "SELECT uid_range_id FROM sessions WHERE session_id = $1 FOR UPDATE", sessionID).Scan(&current)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("会话 %s", sessionID)
		}
		switch {
		case err != nil:
			return err
		case current != nil && *current == uidRangeID:
			return nil
		case current != nil:
			return conflictf("会话 %s 已保留 UID 范围 %s", sessionID, *current)
		}
		_, err = tx.Exec(ctx, "UPDATE sessions SET uid_range_id = $2 WHERE session_id = $1", sessionID, uidRangeID)
		return err
	})
}

// SetIncarnationStatus 以 from 集合 CAS incarnation 的状态（实现 session.Store）。已是 to 时返回当前行；结束用
// EndIncarnation。
func (s *Store) SetIncarnationStatus(ctx context.Context, incarnationID string, from []string, to string) (session.Incarnation, error) {
	if incarnationID == "" || len(from) == 0 || to == session.IncEnded || !slices.Contains(incarnationStatuses, to) {
		return session.Incarnation{}, invalidf("SetIncarnationStatus 缺少参数，或目标状态 %q 不可用（结束用 EndIncarnation）", to)
	}
	var out session.Incarnation
	err := s.run(ctx, "SetIncarnationStatus", incarnationID+"→"+to, func(ctx context.Context, tx pgx.Tx) error {
		inc, err := selectIncarnation(ctx, tx, incarnationID, true)
		if err != nil {
			return err
		}
		switch {
		case inc.Status == to:
			out = inc
			return nil
		case !slices.Contains(from, inc.Status):
			return rejectf(session.CodeInvalidTransition, "incarnation %s 处于 %s，不在 %v 中", incarnationID, inc.Status, from)
		}
		if _, err := tx.Exec(ctx, "UPDATE incarnations SET status = $2 WHERE incarnation_id = $1", incarnationID, to); err != nil {
			return err
		}
		inc.Status = to
		out = inc
		return nil
	})
	return out, err
}

// EndIncarnation 结束 incarnation（实现 session.Store）：已结束时返回原行（end_reason 以首次为准）；若为会话的当前
// incarnation 则清空 current_incarnation_id。锁顺序：sessions → incarnations。
func (s *Store) EndIncarnation(ctx context.Context, incarnationID, endReason string) (session.Incarnation, error) {
	if incarnationID == "" || endReason == "" {
		return session.Incarnation{}, invalidf("EndIncarnation 缺少 incarnation_id 或 end_reason")
	}
	var out session.Incarnation
	err := s.run(ctx, "EndIncarnation", incarnationID, func(ctx context.Context, tx pgx.Tx) error {
		inc, err := selectIncarnation(ctx, tx, incarnationID, false) // session_id 不变：先取会话再按锁顺序加锁
		if err != nil {
			return err
		}
		if _, _, err := lockSession(ctx, tx, inc.SessionID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE incarnations SET status = 'ended', ended_at = COALESCE(ended_at, now()),
				end_reason = CASE WHEN status = 'ended' THEN end_reason ELSE $2 END
			WHERE incarnation_id = $1
			RETURNING incarnation_id, session_id, env_id, status, started_at, ended_at, end_reason`, incarnationID, endReason).
			Scan(&out.IncarnationID, &out.SessionID, &out.EnvID, &out.Status, &out.StartedAt, &out.EndedAt, &out.EndReason); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE sessions SET current_incarnation_id = NULL WHERE session_id = $1 AND current_incarnation_id = $2",
			inc.SessionID, incarnationID)
		return err
	})
	return out, err
}

// LRUCandidates 返回驱逐候选：frozen 先于 idle，各自按 last_active_at 升序（实现 session.Store）。
func (s *Store) LRUCandidates(ctx context.Context, limit int) ([]session.LRUCandidate, error) {
	if limit < 1 {
		return nil, invalidf("LRUCandidates 的 limit 必须为正")
	}
	var out []session.LRUCandidate
	err := s.read(ctx, "LRUCandidates", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT session_id, status, last_active_at FROM sessions WHERE status IN ('frozen', 'idle')
			ORDER BY status = 'frozen' DESC, last_active_at, session_id LIMIT $1`, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (session.LRUCandidate, error) {
			var c session.LRUCandidate
			err := r.Scan(&c.SessionID, &c.Status, &c.LastActiveAt)
			return c, err
		})
		return err
	})
	return out, err
}

// FinishClose 完成关闭 closing → closed（实现 session.Store）：全部 turn 终态、全部 incarnation 已结束且其环境
// stopped_at 已记录；否则 ErrRejected（close_not_ready）。同一关闭重跑返回当前事实。
// 锁顺序：sessions → session_event_seq。
func (s *Store) FinishClose(ctx context.Context, sessionID string, fromRowVersion int64) (session.State, error) {
	if sessionID == "" {
		return session.State{}, invalidf("FinishClose 缺少 session_id")
	}
	var out session.State
	err := s.run(ctx, "FinishClose", sessionID, func(ctx context.Context, tx pgx.Tx) error {
		status, rv, err := lockSession(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		switch {
		case status == session.StatusClosed && rv == fromRowVersion+1:
			out, err = loadSessionState(ctx, tx, sessionID)
			return err
		case rv != fromRowVersion:
			return conflictf("会话 %s 的 row_version 为 %d，不是 %d", sessionID, rv, fromRowVersion)
		case status != session.StatusClosing:
			return rejectf(session.CodeInvalidTransition, "会话 %s 处于 %s，不是 closing", sessionID, status)
		}
		var turns, live, running int
		if err := tx.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM tasks WHERE session_id = $1 AND status NOT IN `+terminalTaskSQL+`),
				(SELECT count(*) FROM incarnations WHERE session_id = $1 AND status <> 'ended'),
				(SELECT count(*) FROM incarnations i JOIN environments e ON e.env_id = i.env_id
					WHERE i.session_id = $1 AND e.stopped_at IS NULL)`, sessionID).Scan(&turns, &live, &running); err != nil {
			return err
		}
		if turns > 0 || live > 0 || running > 0 {
			return rejectf(session.CodeCloseNotReady, "会话 %s：%d 个 turn 未终态，%d 个 incarnation 未结束，%d 个环境未确认停止",
				sessionID, turns, live, running)
		}
		if _, err := tx.Exec(ctx, `UPDATE sessions SET status = 'closed', row_version = row_version + 1, idle_since = NULL,
			frozen_since = NULL, current_incarnation_id = NULL WHERE session_id = $1`, sessionID); err != nil {
			return err
		}
		if _, err := appendSessionEvent(ctx, tx, sessionID, fmt.Sprintf("%s:%d", session.StatusClosed, rv+1),
			statePayload(session.StatusClosed)); err != nil {
			return err
		}
		out, err = loadSessionState(ctx, tx, sessionID)
		return err
	})
	return out, err
}
