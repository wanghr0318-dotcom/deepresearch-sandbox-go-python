package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/session"
)

// SessionAPI 实现 api.Sessions（会话 API 的事务用例；M4 Plan 12）。它与 *Store 共用连接池与事务辅助；不由 *Store
// 直接实现，因为 api.Sessions.CreateSession 与 api.Accounts.CreateSession（登录会话）同名而签名不同。
type SessionAPI struct{ s *Store }

var _ api.Sessions = (*SessionAPI)(nil)

// SessionAPI 返回会话 API 用例的实现。
func (s *Store) SessionAPI() *SessionAPI { return &SessionAPI{s: s} }

// api_requests.kind（I16 按 kind 区分资源是会话还是任务）。
const (
	requestCreateSession = "create_session"
	requestWakeSession   = "wake_session"
	requestCreateTurn    = "create_turn"
	requestTurnControl   = "turn_control"
)

// activeTaskSQL 是计入"每用户同时 1 个运行中"的任务状态（paused 不计）。
const activeTaskSQL = "('queued', 'running', 'pausing', 'cancelling')"

// sessionViewSQL 读取会话视图；与 scanSessionView 的列顺序一致。
const sessionViewSQL = `SELECT s.session_id, s.title, s.status, c.desired, s.last_active_at, s.created_at,
		COALESCE(s.owner_user_id, 0), COALESCE(u.username, '')
	FROM sessions s JOIN session_control c USING (session_id) LEFT JOIN users u ON u.id = s.owner_user_id`

// scanSessionView 读取一行会话视图。admin 时填 Owner（用户名）与 InternalState（sessions.status 原值）。
// 已请求关闭（desired = closed）而尚未 closed 的会话对外为 closing。
func scanSessionView(row pgx.Row, admin bool) (api.SessionView, error) {
	var v api.SessionView
	var status, desired, owner string
	if err := row.Scan(&v.SessionID, &v.Title, &status, &desired, &v.LastActiveAt, &v.CreatedAt, &v.OwnerUserID, &owner); err != nil {
		return v, err
	}
	v.State = api.UserSessionState(status)
	if desired == "closed" && status != session.StatusClosed {
		v.State = session.StatusClosing
	}
	v.LastActiveAt, v.CreatedAt = v.LastActiveAt.UTC(), v.CreatedAt.UTC()
	if admin {
		v.Owner, v.InternalState = owner, status
	}
	return v, nil
}

func getSessionView(ctx context.Context, q queryer, sessionID string) (api.SessionView, error) {
	v, err := scanSessionView(q.QueryRow(ctx, sessionViewSQL+" WHERE s.session_id = $1", sessionID), false)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, notFoundf("会话 %s", sessionID)
	}
	return v, err
}

// lockOpenSession 锁住会话行并读取控制行（锁顺序：sessions → session_control），返回 owner_user_id（无主为 0）与
// next_turn_index。会话关闭中或已关闭（含已请求关闭）时仍返回二者，错误为 api.ErrSessionClosed。
func lockOpenSession(ctx context.Context, tx pgx.Tx, sessionID string) (owner, nextTurn int64, err error) {
	var status, desired string
	err = tx.QueryRow(ctx, "SELECT status, COALESCE(owner_user_id, 0), next_turn_index FROM sessions WHERE session_id = $1 FOR UPDATE",
		sessionID).Scan(&status, &owner, &nextTurn)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, notFoundf("会话 %s", sessionID)
	}
	if err != nil {
		return 0, 0, err
	}
	if err := tx.QueryRow(ctx, "SELECT desired FROM session_control WHERE session_id = $1", sessionID).Scan(&desired); err != nil {
		return 0, 0, err
	}
	if status == session.StatusClosing || status == session.StatusClosed || desired == "closed" {
		return owner, nextTurn, fmt.Errorf("%w: 会话 %s 处于 %s（desired = %s）", api.ErrSessionClosed, sessionID, status, desired)
	}
	return owner, nextTurn, nil
}

// CreateSession 创建会话（status = creating）及其控制、进度与事件序号行（实现 api.Sessions）。以 request_id 幂等：
// 重放返回原会话的当前视图与 replayed = true。标题非空时 title_source = user（不再自动命名）。
func (a *SessionAPI) CreateSession(ctx context.Context, req api.CreateSessionRequest) (api.SessionView, bool, error) {
	if req.RequestID == "" || req.SessionID == "" || len(req.BodyHash) == 0 || req.OwnerUserID < 0 {
		return api.SessionView{}, false, invalidf("CreateSession 缺少 request_id、session_id 或 body_hash")
	}
	var view api.SessionView
	var replayed bool
	err := a.s.run(ctx, "CreateSession", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		var stored []byte
		var err error
		replayed, stored, err = claimRequest(ctx, tx, req.RequestID, requestCreateSession, req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			var res struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			view, err = getSessionView(ctx, tx, res.SessionID)
			if err == nil && view.OwnerUserID != req.OwnerUserID {
				return conflictf("request_conflict: request_id %s 已用于另一个请求", req.RequestID)
			}
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM sessions WHERE session_id = $1)", req.SessionID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return conflictf("会话 %s 已存在", req.SessionID)
		}
		source := "auto"
		if req.Title != "" {
			source = "user"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO sessions (session_id, owner_user_id, title, title_source, status)
			VALUES ($1, NULLIF($2::bigint, 0), $3, $4, 'creating')`, req.SessionID, req.OwnerUserID, req.Title, source); err != nil {
			return err
		}
		for _, q := range []string{
			"INSERT INTO session_control (session_id) VALUES ($1)",
			"INSERT INTO session_progress (session_id) VALUES ($1)",
			"INSERT INTO session_event_seq (session_id) VALUES ($1)",
		} {
			if _, err := tx.Exec(ctx, q, req.SessionID); err != nil {
				return err
			}
		}
		if view, err = getSessionView(ctx, tx, req.SessionID); err != nil {
			return err
		}
		return finishRequest(ctx, tx, req.RequestID, req.SessionID, map[string]string{"session_id": req.SessionID})
	})
	if err != nil {
		return api.SessionView{}, false, err
	}
	return view, replayed, nil
}

// ListSessions 按 last_active_at 倒序 keyset 分页（实现 api.Sessions）：ownerID > 0 只含该用户未关闭的会话；
// ownerID = 0 为全部（运维视图，带 Owner 与 InternalState）。
func (a *SessionAPI) ListSessions(ctx context.Context, ownerID int64, after string, limit int) ([]api.SessionView, string, error) {
	if limit < 1 || ownerID < 0 {
		return nil, "", invalidf("ListSessions 的 limit 必须为正、ownerID 不能为负")
	}
	var out []api.SessionView
	err := a.s.read(ctx, "ListSessions", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, sessionViewSQL+`
			WHERE ($3::bigint = 0 OR (s.owner_user_id = $3::bigint AND s.status <> 'closed'))
				AND ($1 = '' OR (s.last_active_at, s.session_id) < (SELECT last_active_at, session_id FROM sessions WHERE session_id = $1))
			ORDER BY s.last_active_at DESC, s.session_id DESC LIMIT $2`, after, limit, ownerID)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.SessionView, error) { return scanSessionView(r, ownerID == 0) })
		return err
	})
	if err != nil || len(out) < limit {
		return out, "", err
	}
	return out, out[len(out)-1].SessionID, nil
}

// GetSession 读取会话视图（实现 api.Sessions）；不含运维字段 Owner 与 InternalState。
func (a *SessionAPI) GetSession(ctx context.Context, sessionID string) (api.SessionView, error) {
	var v api.SessionView
	err := a.s.read(ctx, "GetSession", func(ctx context.Context, q queryer) error {
		var err error
		v, err = getSessionView(ctx, q, sessionID)
		return err
	})
	return v, err
}

// RenameSession 写入用户标题（实现 api.Sessions）；会话关闭中或已关闭为 api.ErrSessionClosed。
func (a *SessionAPI) RenameSession(ctx context.Context, sessionID, title string) (api.SessionView, error) {
	if sessionID == "" || title == "" {
		return api.SessionView{}, invalidf("RenameSession 缺少 session_id 或 title")
	}
	var v api.SessionView
	err := a.s.run(ctx, "RenameSession", sessionID, func(ctx context.Context, tx pgx.Tx) error {
		if _, _, err := lockOpenSession(ctx, tx, sessionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE sessions SET title = $2, title_source = 'user' WHERE session_id = $1", sessionID, title); err != nil {
			return err
		}
		var err error
		v, err = getSessionView(ctx, tx, sessionID)
		return err
	})
	return v, err
}

// nonTerminalTurns 返回会话中非终态 turn 的 (task_id, status, 当前 desired)，按 turn_index 升序。
func nonTerminalTurns(ctx context.Context, tx pgx.Tx, sessionID string) ([]session.TurnFact, error) {
	rows, err := tx.Query(ctx, `SELECT t.task_id, t.status, t.status_reason, c.desired, t.turn_index
		FROM tasks t JOIN task_control c USING (task_id)
		WHERE t.session_id = $1 AND t.status NOT IN `+terminalTaskSQL+` ORDER BY t.turn_index`, sessionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (session.TurnFact, error) {
		var f session.TurnFact
		err := r.Scan(&f.TaskID, &f.Status, &f.StatusReason, &f.Desired, &f.TurnIndex)
		return f, err
	})
}

// lockTurns 先按锁顺序锁住若干 turn 的 tasks、task_control 与 task_event_seq 行（各按 task_id 排序），再逐个写控制：
// 否则写第二个 turn 时会在已持有 session_event_seq 的情况下等待它的任务行，与事件追加形成环。
func lockTurns(ctx context.Context, tx pgx.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for _, table := range []string{"tasks", "task_control", "task_event_seq"} {
		rows, err := tx.Query(ctx, "SELECT task_id FROM "+table+" WHERE task_id = ANY($1) ORDER BY task_id FOR UPDATE", ids)
		if err != nil {
			return err
		}
		if _, err := pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
	}
	return nil
}

// cancelTurns 对每个 desired 尚不是 cancel 的非终态 turn 写 cancel 控制（reason 如 superseded、session_closed）。
func cancelTurns(ctx context.Context, tx pgx.Tx, turns []session.TurnFact, reason string) error {
	var ids []string
	for _, t := range turns {
		if t.Desired != "cancel" {
			ids = append(ids, t.TaskID)
		}
	}
	if err := lockTurns(ctx, tx, ids); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := writeControl(ctx, tx, id, "cancel", reason, func(status, _, _ string) error {
			if isTerminalStatus(status) { // 持有会话行锁时不会发生：turn 的终态裁决也从 sessions 开始
				return conflictf("turn %s 已是 %s", id, status)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func isTerminalStatus(status string) bool {
	return status == "succeeded" || status == "failed" || status == "cancelled"
}

// CloseSession 请求关闭会话（实现 api.Sessions）：desired = closed、control_version+1（已请求时不变），同事务对每个
// 非终态 turn 写 cancel 控制（reason session_closed；已为 cancel 的不重复）。已 closed 时只返回视图。幂等。
// 锁顺序：sessions → session_control → tasks → task_control → task_event_seq → session_event_seq。
func (a *SessionAPI) CloseSession(ctx context.Context, sessionID string) (api.SessionView, error) {
	if sessionID == "" {
		return api.SessionView{}, invalidf("CloseSession 缺少 session_id")
	}
	var v api.SessionView
	err := a.s.run(ctx, "CloseSession", sessionID, func(ctx context.Context, tx pgx.Tx) error {
		status, _, err := lockSession(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if status != session.StatusClosed {
			if _, err := tx.Exec(ctx, `UPDATE session_control SET desired = 'closed', control_version = control_version + 1
				WHERE session_id = $1 AND desired <> 'closed'`, sessionID); err != nil {
				return err
			}
			turns, err := nonTerminalTurns(ctx, tx, sessionID)
			if err != nil {
				return err
			}
			if err := cancelTurns(ctx, tx, turns, "session_closed"); err != nil {
				return err
			}
		}
		v, err = getSessionView(ctx, tx, sessionID)
		return err
	})
	return v, err
}

// WakeSession 请求唤醒（实现 api.Sessions）：control_version+1，wake_requested_version 取新的 control_version。以
// request_id 幂等；关闭中或已关闭为 api.ErrSessionClosed。
func (a *SessionAPI) WakeSession(ctx context.Context, requestID string, bodyHash []byte, sessionID string) error {
	if requestID == "" || sessionID == "" || len(bodyHash) == 0 {
		return invalidf("WakeSession 缺少 request_id、session_id 或 body_hash")
	}
	return a.s.run(ctx, "WakeSession", requestID, func(ctx context.Context, tx pgx.Tx) error {
		replayed, stored, err := claimRequest(ctx, tx, requestID, requestWakeSession, bodyHash)
		if err != nil {
			return err
		}
		if replayed {
			var res struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			if res.SessionID != sessionID {
				return conflictf("request_conflict: request_id %s 已用于会话 %s", requestID, res.SessionID)
			}
			return nil
		}
		if _, _, err := lockOpenSession(ctx, tx, sessionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE session_control SET control_version = control_version + 1,
			wake_requested_version = control_version + 1 WHERE session_id = $1`, sessionID); err != nil {
			return err
		}
		return finishRequest(ctx, tx, requestID, sessionID, map[string]string{"session_id": sessionID})
	})
}

// admitUserRun 锁定用户行（锁顺序在会话行之后、任务行之前）并检查该用户没有其他计入并发的任务（queued、running、
// pausing、cancelling；paused 不计）：exceptSession 非空时不计该会话的 turn（由 turn_in_progress 与 D5 处理），
// exceptTask 非空时不计该任务本身。用户不存在或已停用为 ErrNotFound；否则为 api.ErrUserTaskRunning。
func admitUserRun(ctx context.Context, tx pgx.Tx, userID int64, exceptSession, exceptTask string) error {
	var id int64
	err := tx.QueryRow(ctx, "SELECT id FROM users WHERE id = $1 AND NOT disabled FOR UPDATE", userID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("用户 %d", userID)
	}
	if err != nil {
		return err
	}
	var busy string
	err = tx.QueryRow(ctx, `SELECT task_id FROM tasks WHERE owner_user_id = $1 AND status IN `+activeTaskSQL+`
		AND ($2 = '' OR session_id IS DISTINCT FROM $2) AND task_id <> $3 LIMIT 1`, userID, exceptSession, exceptTask).Scan(&busy)
	switch {
	case err == nil:
		return fmt.Errorf("%w: 任务 %s 正在进行", api.ErrUserTaskRunning, busy)
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	}
	return err
}

// seedSource 是恢复种子的来源：源 turn 的最新 task checkpoint。
type seedSource struct {
	checkpointID, stepID string
	shas                 []string // refs 与 state_ref
}

// loadSeed 校验源 turn 可恢复（同一会话、cancelled 或 failed、有已提交 task checkpoint，且其 state_ref 与 refs 都已
// 授权到会话 scope），返回其最新 checkpoint。不满足为 api.ErrNotRestorable；源 turn 不存在为 ErrNotFound。
func loadSeed(ctx context.Context, tx pgx.Tx, sessionID, srcTaskID string) (seedSource, error) {
	var src seedSource
	var srcSession, cpID *string
	var status string
	err := tx.QueryRow(ctx, `SELECT t.session_id, t.status, p.latest_checkpoint_id FROM tasks t JOIN task_progress p USING (task_id)
		WHERE t.task_id = $1`, srcTaskID).Scan(&srcSession, &status, &cpID)
	if errors.Is(err, pgx.ErrNoRows) {
		return src, notFoundf("turn %s", srcTaskID)
	}
	if err != nil {
		return src, err
	}
	switch {
	case srcSession == nil || *srcSession != sessionID:
		return src, fmt.Errorf("%w: turn %s 不属于会话 %s", api.ErrNotRestorable, srcTaskID, sessionID)
	case status != "cancelled" && status != "failed":
		return src, fmt.Errorf("%w: turn %s 处于 %s", api.ErrNotRestorable, srcTaskID, status)
	case cpID == nil:
		return src, fmt.Errorf("%w: turn %s 没有已提交的 checkpoint", api.ErrNotRestorable, srcTaskID)
	}
	src.checkpointID = *cpID
	var stateRef *string
	var refs []byte
	if err := tx.QueryRow(ctx, `SELECT step_id, state_ref, refs_json FROM checkpoints
		WHERE scope_kind = 'task' AND scope_id = $1 AND checkpoint_id = $2`, srcTaskID, *cpID).Scan(&src.stepID, &stateRef, &refs); err != nil {
		return src, err
	}
	if err := json.Unmarshal(refs, &src.shas); err != nil {
		return src, err
	}
	if stateRef != nil {
		src.shas = append(src.shas, *stateRef)
	}
	rows, err := tx.Query(ctx, `SELECT r FROM unnest($1::text[]) AS r WHERE NOT EXISTS (
		SELECT 1 FROM scope_blobs sb WHERE sb.scope_kind = 'session' AND sb.scope_id = $2 AND sb.sha256 = r)`, nonNil(src.shas), sessionID)
	if err != nil {
		return src, err
	}
	missing, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return src, err
	}
	if len(missing) > 0 {
		return src, fmt.Errorf("%w: turn %s 的 checkpoint 引用 %v 未授权到会话 %s", api.ErrNotRestorable, srcTaskID, missing, sessionID)
	}
	return src, nil
}

// seedCheckpoint 把源 turn 的 checkpoint 复制为新 turn 的种子（checkpoint_id = "seed-" + 源 ID、commit_seq = 1、
// attempt_id 为空）、推进 task_progress、把引用授权到新 task scope，并追加 checkpoint_committed 事件。
func seedCheckpoint(ctx context.Context, tx pgx.Tx, taskID, srcTaskID string, src seedSource) error {
	seedID := "seed-" + src.checkpointID
	if _, err := tx.Exec(ctx, `INSERT INTO checkpoints (scope_kind, scope_id, checkpoint_id, commit_seq, attempt_id, step_id,
			content_hash, state_inline, state_ref, refs_json)
		SELECT 'task', $1, $2, 1, '', step_id, content_hash, state_inline, state_ref, refs_json FROM checkpoints
		WHERE scope_kind = 'task' AND scope_id = $3 AND checkpoint_id = $4`, taskID, seedID, srcTaskID, src.checkpointID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE task_progress SET latest_checkpoint_id = $2, latest_commit_seq = 1 WHERE task_id = $1",
		taskID, seedID); err != nil {
		return err
	}
	if len(src.shas) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO scope_blobs (scope_kind, scope_id, sha256) SELECT 'task', $1, r FROM unnest($2::text[]) AS r
			ON CONFLICT DO NOTHING`, taskID, src.shas); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref)
			SELECT 'task', $1, r, 'restore_seed', $3 FROM unnest($2::text[]) AS r`, taskID, src.shas, srcTaskID+"/"+src.checkpointID); err != nil {
			return err
		}
	}
	payload, _ := json.Marshal(map[string]any{"checkpoint_id": seedID, "commit_seq": 1, "step_id": src.stepID,
		"restored_from_task_id": srcTaskID})
	_, err := appendHostEvent(ctx, tx, hostEvent{taskID: taskID, key: "checkpoint_committed:" + seedID, typ: "checkpoint_committed",
		payload: payload})
	return err
}

// CreateTurn 向会话发送一条消息（或恢复一个 turn），创建新 turn（实现 api.Sessions）。事务从 sessions FOR UPDATE
// 开始：认领 request（重放返回原结果）→ 会话关闭中或已关闭为 ErrSessionClosed → 锁用户行，用户在其他会话或研究中
// 已有进行中的任务为 ErrUserTaskRunning → 会话中有 queued|running|cancelling 的 turn 为 ErrTurnInProgress；paused 或
// pausing 的 turn 写 cancel 控制（reason superseded，D5）→ 恢复种子的校验 → turn_index = next_turn_index → 创建任务
// （owner = 会话所有者，tool_call_limit = limits.max_tool_calls）→ 首个 turn 自动命名 → 种子 checkpoint。
func (a *SessionAPI) CreateTurn(ctx context.Context, req api.CreateTurnRequest) (api.CreateTurnResult, error) {
	if req.SessionID == "" || req.OwnerUserID < 0 {
		return api.CreateTurnResult{}, invalidf("CreateTurn 缺少 session_id")
	}
	taskReq := api.CreateTaskRequest{RequestID: req.RequestID, BodyHash: req.BodyHash, TaskID: req.TaskID, Spec: req.Spec,
		ConfigVersion: req.ConfigVersion, Limits: req.Limits, MaxFaultRetries: req.MaxFaultRetries}
	budget, err := createTaskBudget("CreateTurn", taskReq)
	if err != nil {
		return api.CreateTurnResult{}, err
	}
	var res api.CreateTurnResult
	err = a.s.run(ctx, "CreateTurn", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.CreateTurnResult{}
		// 先锁会话行（锁顺序起点），再认领 request：并发的同一 request_id 在会话行上串行化。
		owner, turnIndex, err := lockOpenSession(ctx, tx, req.SessionID)
		if err != nil && !errors.Is(err, api.ErrSessionClosed) {
			return err
		}
		replayed, stored, claimErr := claimRequest(ctx, tx, req.RequestID, requestCreateTurn, req.BodyHash)
		if claimErr != nil {
			return claimErr
		}
		if replayed {
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			var sessionID *string
			if err := tx.QueryRow(ctx, "SELECT session_id FROM tasks WHERE task_id = $1", res.TurnID).Scan(&sessionID); err != nil {
				return err
			}
			if sessionID == nil || *sessionID != req.SessionID {
				return conflictf("request_conflict: request_id %s 已用于另一个会话", req.RequestID)
			}
			res.Replayed = true
			return nil
		}
		if err != nil { // 会话关闭中或已关闭（重放不受影响）
			return err
		}
		if !req.Operator && owner != req.OwnerUserID { // 运维路径（POST /tasks 带 session_id）：turn 归会话所有者
			return notFoundf("会话 %s 不属于用户 %d", req.SessionID, req.OwnerUserID)
		}
		if owner > 0 {
			if err := admitUserRun(ctx, tx, owner, req.SessionID, ""); err != nil {
				return err
			}
		}
		turns, err := nonTerminalTurns(ctx, tx, req.SessionID)
		if err != nil {
			return err
		}
		var supersede []session.TurnFact
		for _, t := range turns {
			switch t.Status {
			case "paused", "pausing":
				supersede = append(supersede, t)
			default: // queued、running、cancelling：不排队（裁定 I）
				return fmt.Errorf("%w: turn %s 处于 %s", api.ErrTurnInProgress, t.TaskID, t.Status)
			}
		}
		var seed seedSource
		if req.RestoredFromTaskID != "" {
			if seed, err = loadSeed(ctx, tx, req.SessionID, req.RestoredFromTaskID); err != nil {
				return err
			}
		}
		if err := cancelTurns(ctx, tx, supersede, "superseded"); err != nil {
			return err
		}
		for _, t := range supersede {
			if t.Desired != "cancel" {
				res.SupersededTurnID = t.TaskID // 不排队：至多一个暂停中的 turn
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE sessions SET next_turn_index = next_turn_index + 1, last_active_at = now(),
				title = CASE WHEN $2 AND title_source = 'auto' THEN left($3, 40) ELSE title END
			WHERE session_id = $1`, req.SessionID, turnIndex == 0, req.Text); err != nil {
			return err
		}
		created := map[string]any{"turn_index": turnIndex, "text": req.Text, "deep_research": req.DeepResearch}
		if req.RestoredFromTaskID != "" {
			created["restored_from_turn_id"] = req.RestoredFromTaskID
		}
		payload, err := json.Marshal(created)
		if err != nil {
			return err
		}
		if err := createTaskTx(ctx, tx, taskReq, budget, owner, &turnRow{sessionID: req.SessionID, turnIndex: turnIndex,
			restoredFrom: req.RestoredFromTaskID, created: payload}); err != nil {
			return err
		}
		if req.RestoredFromTaskID != "" {
			if err := seedCheckpoint(ctx, tx, req.TaskID, req.RestoredFromTaskID, seed); err != nil {
				return err
			}
		}
		res.TurnID, res.TurnIndex = req.TaskID, turnIndex
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// ListTurns 按 turn_index 升序返回会话的 turn（实现 api.Sessions）。text、deep_research 取自 task_created 事件；
// route 取最新的 worker progress{kind = route}；工具额度取 budgets；summary、report（artifact_id = report 的输出）
// 取 result_json；restorable = cancelled|failed 且有已提交 task checkpoint。
func (a *SessionAPI) ListTurns(ctx context.Context, sessionID string, afterIndex int64, limit int) ([]api.TurnView, error) {
	if limit < 1 {
		return nil, invalidf("ListTurns 的 limit 必须为正")
	}
	var out []api.TurnView
	err := a.s.read(ctx, "ListTurns", func(ctx context.Context, q queryer) error {
		var exists bool
		if err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM sessions WHERE session_id = $1)", sessionID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return notFoundf("会话 %s", sessionID)
		}
		// 取消的 turn 以取消控制的原因（superseded、session_closed）作为对外 status_reason（OpenAPI Turn.status_reason）。
		rows, err := q.Query(ctx, `SELECT t.task_id, t.turn_index, t.status,
				CASE WHEN t.status = 'cancelled' AND tc.desired = 'cancel' AND tc.reason <> '' THEN tc.reason ELSE t.status_reason END,
				COALESCE(t.restored_from_task_id, ''),
				t.created_at, COALESCE(ev.payload, '{}'), COALESCE(b.tool_calls_used, 0), COALESCE(b.tool_call_limit, 0),
				t.result_json, p.latest_checkpoint_id IS NOT NULL,
				COALESCE((SELECT e.payload->'data'->>'route' FROM events e WHERE e.task_id = t.task_id AND e.source = 'worker'
					AND e.type = 'progress' AND e.payload->>'kind' = 'route' ORDER BY e.task_seq DESC LIMIT 1), '')
			FROM tasks t JOIN task_progress p USING (task_id) LEFT JOIN budgets b USING (task_id)
			JOIN task_control tc ON tc.task_id = t.task_id
			LEFT JOIN events ev ON ev.task_id = t.task_id AND ev.event_key = 'task_created'
			WHERE t.session_id = $1 AND t.turn_index > $2 ORDER BY t.turn_index LIMIT $3`, sessionID, afterIndex, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, scanTurnView)
		return err
	})
	return out, err
}

func scanTurnView(r pgx.CollectableRow) (api.TurnView, error) {
	var v api.TurnView
	var status, reason string
	var created, result []byte
	var hasCheckpoint bool
	if err := r.Scan(&v.TurnID, &v.TurnIndex, &status, &reason, &v.RestoredFromTurnID, &v.CreatedAt, &created,
		&v.ToolCallsUsed, &v.ToolCallLimit, &result, &hasCheckpoint, &v.Route); err != nil {
		return v, err
	}
	v.CreatedAt = v.CreatedAt.UTC()
	var c struct {
		Text         string `json:"text"`
		DeepResearch bool   `json:"deep_research"`
	}
	if err := json.Unmarshal(created, &c); err != nil {
		return v, err
	}
	v.Text, v.DeepResearch = c.Text, c.DeepResearch
	v.Status = api.UserTurnStatus(status, reason)
	if v.Status != "awaiting_input" {
		v.StatusReason = reason
	}
	v.Restorable = (status == "cancelled" || status == "failed") && hasCheckpoint
	if status == "failed" {
		v.UserMessage = api.FailedTurnMessage
	}
	if len(result) > 0 {
		var res struct {
			Summary string `json:"summary"`
			Outputs []struct {
				ArtifactID string `json:"artifact_id"`
				Version    int64  `json:"version"`
			} `json:"outputs"`
		}
		if json.Unmarshal(result, &res) == nil { // 不是 result 形状（例如错误提议）时不显示摘要与报告
			v.Summary = res.Summary
			for _, o := range res.Outputs {
				if o.ArtifactID == "report" && o.Version > 0 {
					v.Report = &api.ReportRef{ArtifactID: o.ArtifactID, Version: o.Version}
				}
			}
		}
	}
	return v, nil
}

// TurnSession 返回 turn 所属会话与会话所有者（实现 api.Sessions）；不是会话 turn 或不存在为 ErrNotFound。
func (a *SessionAPI) TurnSession(ctx context.Context, taskID string) (string, int64, error) {
	var sessionID string
	var owner int64
	err := a.s.read(ctx, "TurnSession", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT s.session_id, COALESCE(s.owner_user_id, 0) FROM tasks t JOIN sessions s ON s.session_id = t.session_id
			WHERE t.task_id = $1`, taskID).Scan(&sessionID, &owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("turn %s", taskID)
		}
		return err
	})
	return sessionID, owner, err
}

// turnActions 是 TurnControl 的动作：desired 与 resume_directive.kind（stop 不写 directive）。
var turnActions = map[string]struct{ desired, directive string }{
	"stop":     {"pause", ""},
	"continue": {"run", "continue"},
	"finish":   {"run", "finish_now"},
	"answer":   {"run", "answer"},
}

// admitTurnAction 是 turn 控制的状态要求；不满足（含已接受 cancel）为 api.ErrInvalidTurnState。
func admitTurnAction(taskID, action, status, statusReason, current string) error {
	awaiting := status == "paused" && statusReason == "awaiting_input"
	ok := current != "cancel"
	switch action {
	case "stop":
		ok = ok && (status == "queued" || status == "running")
	case "continue", "finish":
		ok = ok && status == "paused" && !awaiting
	case "answer":
		ok = ok && awaiting
	}
	if !ok {
		return fmt.Errorf("%w: turn %s 处于 %s（%s，desired = %s），不能 %s", api.ErrInvalidTurnState, taskID, status, statusReason, current, action)
	}
	return nil
}

// TurnControl 写入 turn 的控制意图（实现 api.Sessions）：复用 AcceptControl 的事务体（writeControl），以 request_id
// 幂等。continue、finish、answer 同事务写 tasks.resume_directive = {kind, answers?}，并检查用户没有其他进行中的任务。
// 锁顺序：users → tasks → task_control → task_event_seq → session_event_seq。
func (a *SessionAPI) TurnControl(ctx context.Context, req api.TurnControlRequest) (api.ControlResult, error) {
	act, ok := turnActions[req.Action]
	if !ok {
		return api.ControlResult{}, invalidf("turn 控制必须是 stop、continue、finish 或 answer，得到 %q", req.Action)
	}
	if req.RequestID == "" || req.TaskID == "" || len(req.BodyHash) == 0 {
		return api.ControlResult{}, invalidf("TurnControl 缺少 request_id、task_id 或 body_hash")
	}
	if (req.Action == "answer") != (len(req.Answers) > 0) || (len(req.Answers) > 0 && !json.Valid(req.Answers)) {
		return api.ControlResult{}, invalidf("answers 只用于 answer 且必须是 JSON")
	}
	var directive []byte
	if act.directive != "" {
		d := map[string]any{"kind": act.directive}
		if req.Action == "answer" {
			d["answers"] = req.Answers
		}
		var err error
		if directive, err = json.Marshal(d); err != nil {
			return api.ControlResult{}, invalidf("answers 无法编码: %v", err)
		}
	}
	var res api.ControlResult
	err := a.s.run(ctx, "TurnControl", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.ControlResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, requestTurnControl, req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			if res.TaskID != req.TaskID {
				return conflictf("request_conflict: request_id %s 已用于任务 %s", req.RequestID, res.TaskID)
			}
			res.Replayed = true
			return nil
		}
		var owner int64
		err = tx.QueryRow(ctx, `SELECT COALESCE(s.owner_user_id, 0) FROM tasks t JOIN sessions s ON s.session_id = t.session_id
			WHERE t.task_id = $1`, req.TaskID).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("turn %s", req.TaskID)
		}
		if err != nil {
			return err
		}
		if act.desired == "run" && owner > 0 {
			if err := admitUserRun(ctx, tx, owner, "", req.TaskID); err != nil {
				return err
			}
		}
		res.ControlVersion, err = writeControl(ctx, tx, req.TaskID, act.desired, "", func(status, statusReason, current string) error {
			return admitTurnAction(req.TaskID, req.Action, status, statusReason, current)
		})
		if err != nil {
			return err
		}
		if directive != nil {
			if _, err := tx.Exec(ctx, "UPDATE tasks SET resume_directive = $2 WHERE task_id = $1", req.TaskID, directive); err != nil {
				return err
			}
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// ListSessionEvents 按 session_seq 升序返回会话 task 的事件（events）与会话生命周期事件（session_events）
// （实现 api.Sessions）。session_seq 在提交顺序上连续，读到 n 即 n 之前的都已提交。
func (a *SessionAPI) ListSessionEvents(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]api.SessionEventRecord, error) {
	if limit < 1 {
		return nil, invalidf("ListSessionEvents 的 limit 必须为正")
	}
	var out []api.SessionEventRecord
	err := a.s.read(ctx, "ListSessionEvents", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT session_seq, task_id, task_seq, COALESCE(attempt_id, ''), source, type, payload, ts
				FROM events WHERE session_id = $1 AND session_seq > $2
			UNION ALL
			SELECT session_seq, '', 0, '', 'session', type, payload, ts FROM session_events WHERE session_id = $1 AND session_seq > $2
			ORDER BY 1 LIMIT $3`, sessionID, afterSeq, limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.SessionEventRecord, error) {
			var e api.SessionEventRecord
			var payload []byte
			err := r.Scan(&e.SessionSeq, &e.TaskID, &e.TaskSeq, &e.AttemptID, &e.Source, &e.Type, &payload, &e.TS)
			e.Payload = payload
			return e, err
		})
		return err
	})
	return out, err
}

// TurnBlobAuthorized 报告 sha 是否授权到该 turn 的 task scope 或其会话 scope（实现 api.Sessions）。
func (a *SessionAPI) TurnBlobAuthorized(ctx context.Context, taskID, sha string) (bool, error) {
	var ok bool
	err := a.s.read(ctx, "TurnBlobAuthorized", func(ctx context.Context, q queryer) error {
		return q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tasks t JOIN scope_blobs sb ON sb.sha256 = $2
			AND ((sb.scope_kind = 'task' AND sb.scope_id = t.task_id) OR (sb.scope_kind = 'session' AND sb.scope_id = t.session_id))
			WHERE t.task_id = $1)`, taskID, sha).Scan(&ok)
	})
	return ok, err
}
