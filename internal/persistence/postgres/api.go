package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

var _ api.Store = (*Store)(nil)

// claimRequest 以 request_id 认领一次 API 请求（规格 §7.3）。已存在时：kind 与 body_hash
// 相同返回其响应（replayed），不同为冲突。新认领的行在 finishRequest 中写入结果。
// INSERT … ON CONFLICT 会等待并发的同一 request_id 事务结束后再判定。
func claimRequest(ctx context.Context, tx pgx.Tx, requestID, kind string, bodyHash []byte) (replayed bool, response []byte, err error) {
	tag, err := tx.Exec(ctx, `INSERT INTO api_requests (request_id, kind, body_hash, resource_id, response)
		VALUES ($1, $2, $3, '', 'null') ON CONFLICT (request_id) DO NOTHING`, requestID, kind, bodyHash)
	if err != nil {
		return false, nil, err
	}
	if tag.RowsAffected() == 1 {
		return false, nil, nil
	}
	var existingKind string
	var existingHash []byte
	if err := tx.QueryRow(ctx, "SELECT kind, body_hash, response FROM api_requests WHERE request_id = $1",
		requestID).Scan(&existingKind, &existingHash, &response); err != nil {
		return false, nil, err
	}
	if existingKind != kind || !bytes.Equal(existingHash, bodyHash) {
		return false, nil, conflictf("request_conflict: request_id %s 已用于不同的请求", requestID)
	}
	return true, response, nil
}

func finishRequest(ctx context.Context, tx pgx.Tx, requestID, resourceID string, result any) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "UPDATE api_requests SET resource_id = $2, response = $3 WHERE request_id = $1", requestID, resourceID, b)
	return err
}

// CreateTask 创建任务、控制行、进度行、事件序号行与 task_created 事件（实现 api.Store）。
func (s *Store) CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.CreateTaskResult, error) {
	budget, err := createTaskBudget("CreateTask", req)
	if err != nil {
		return api.CreateTaskResult{}, err
	}
	var res api.CreateTaskResult
	err = s.run(ctx, "CreateTask", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.CreateTaskResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, "create_task", req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			res.Replayed = true
			return json.Unmarshal(stored, &res)
		}
		if err := createTaskTx(ctx, tx, req, budget, 0, nil); err != nil {
			return err
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// createTaskBudget 校验创建任务的输入并取出 task 层预算上限（CreateTask 与 CreateResearch 共用）。
func createTaskBudget(op string, req api.CreateTaskRequest) (int64, error) {
	if req.RequestID == "" || req.TaskID == "" || len(req.BodyHash) == 0 || len(req.Spec) == 0 {
		return 0, invalidf("%s 缺少 request_id、task_id、body_hash 或 spec", op)
	}
	return budgetLimit(req.Limits)
}

// turnRow 是会话 turn（会话 task）的附加列；独立任务为 nil。
type turnRow struct {
	sessionID    string
	turnIndex    int64
	restoredFrom string // restored_from_task_id；空为 NULL
	created      []byte // task_created 事件的 payload
}

// createTaskTx 是创建任务的事务体（request 已认领且不是重放）：任务行（owner 为 0 时 owner_user_id 为空）、
// 控制行、进度行、事件序号行、预算行（tool_call_limit = limits.max_tool_calls，缺省为 NULL 即不限）与
// task_created 事件。CreateTask、CreateResearch 与 CreateTurn（turn 非 nil）共用。
func createTaskTx(ctx context.Context, tx pgx.Tx, req api.CreateTaskRequest, budget, owner int64, turn *turnRow) error {
	toolLimit, err := toolCallLimit(req.Limits)
	if err != nil {
		return err
	}
	var exists bool // 否则 INSERT 的 23505 会被当作可重试错误一直重试到期限
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM tasks WHERE task_id = $1)", req.TaskID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return conflictf("任务 %s 已存在", req.TaskID)
	}
	var sessionID, restoredFrom string
	var turnIndex any
	var created []byte
	if turn != nil {
		sessionID, restoredFrom, turnIndex, created = turn.sessionID, turn.restoredFrom, turn.turnIndex, turn.created
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tasks (task_id, spec_json, config_version, limits_json, status, max_fault_retries, owner_user_id,
			session_id, turn_index, restored_from_task_id)
		VALUES ($1, $2, $3, $4, 'queued', $5, NULLIF($6::bigint, 0), NULLIF($7, ''), $8, NULLIF($9, ''))`,
		req.TaskID, []byte(req.Spec), req.ConfigVersion, nullJSON(req.Limits), req.MaxFaultRetries, owner,
		sessionID, turnIndex, restoredFrom); err != nil {
		return err
	}
	for _, q := range []string{
		"INSERT INTO task_control (task_id, control_version, desired) VALUES ($1, 1, 'run')",
		"INSERT INTO task_progress (task_id) VALUES ($1)",
		"INSERT INTO task_event_seq (task_id) VALUES ($1)",
	} {
		if _, err := tx.Exec(ctx, q, req.TaskID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, "INSERT INTO budgets (task_id, limit_micro, tool_call_limit) VALUES ($1, $2, $3)",
		req.TaskID, budget, toolLimit); err != nil {
		return err
	}
	if err := lockEventSeq(ctx, tx, req.TaskID); err != nil {
		return err
	}
	_, err = appendHostEvent(ctx, tx, hostEvent{taskID: req.TaskID, key: "task_created", typ: "task_created", payload: created})
	return err
}

// toolCallLimit 取 limits.max_tool_calls 作为每 turn 的工具调用额度（budgets.tool_call_limit）。缺省为 nil（不限，
// 独立任务）；负数或非整数为 ErrInvalid。
func toolCallLimit(limits json.RawMessage) (*int64, error) {
	if len(limits) == 0 || string(limits) == "null" {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(limits, &m); err != nil {
		return nil, invalidf("limits 不是 JSON 对象: %v", err)
	}
	raw, ok := m["max_tool_calls"]
	if !ok || string(raw) == "null" {
		return nil, nil
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < 0 {
		return nil, invalidf("limits.max_tool_calls 必须是非负整数，得到 %s", raw)
	}
	return &n, nil
}

// budgetLimit 取任务 limits.budget_micro 作为 task 层预算上限（微美元）。缺省时为 0（失败关闭：所有
// 付费调用都被 budget_exhausted 拒绝）；默认值与上限由 API 层补齐和校验。负数或非整数为 ErrInvalid。
func budgetLimit(limits json.RawMessage) (int64, error) {
	if len(limits) == 0 || string(limits) == "null" {
		return 0, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(limits, &m); err != nil {
		return 0, invalidf("limits 不是 JSON 对象: %v", err)
	}
	raw, ok := m["budget_micro"]
	if !ok || string(raw) == "null" {
		return 0, nil
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < 0 {
		return 0, invalidf("limits.budget_micro 必须是非负整数，得到 %s", raw)
	}
	return n, nil
}

// AcceptControl 写入控制意图并递增 control_version（实现 api.Store）。
func (s *Store) AcceptControl(ctx context.Context, req api.ControlRequest) (api.ControlResult, error) {
	switch req.Desired {
	case "run", "pause", "cancel":
	default:
		return api.ControlResult{}, invalidf("desired 必须是 run、pause 或 cancel，得到 %q", req.Desired)
	}
	if req.RequestID == "" || req.TaskID == "" || len(req.BodyHash) == 0 {
		return api.ControlResult{}, invalidf("AcceptControl 缺少 request_id、task_id 或 body_hash")
	}
	var res api.ControlResult
	err := s.run(ctx, "AcceptControl", req.RequestID, func(ctx context.Context, tx pgx.Tx) error {
		res = api.ControlResult{}
		replayed, stored, err := claimRequest(ctx, tx, req.RequestID, "control", req.BodyHash)
		if err != nil {
			return err
		}
		if replayed {
			if err := json.Unmarshal(stored, &res); err != nil {
				return err
			}
			if res.TaskID != req.TaskID { // body_hash 不含 task_id 时，同一 request_id 可能被另一个任务重用
				return conflictf("request_conflict: request_id %s 已用于任务 %s", req.RequestID, res.TaskID)
			}
			res.Replayed = true
			return nil
		}
		res.ControlVersion, err = writeControl(ctx, tx, req.TaskID, req.Desired, req.Reason,
			func(status, _, current string) error { return admitControl(req.TaskID, status, current, req.Desired) })
		if err != nil {
			return err
		}
		res.TaskID = req.TaskID
		return finishRequest(ctx, tx, req.RequestID, req.TaskID, res)
	})
	return res, err
}

// writeControl 是控制写入的事务体（AcceptControl、TurnControl、D5 取代与关闭会话共用）：锁 tasks、task_control
// （FOR UPDATE），以 admit(status, status_reason, 当前 desired) 检查准入，递增 control_version 并写 desired 与
// reason，追加 control_accepted 事件。返回新的 control_version。锁顺序：tasks → task_control → task_event_seq
// （→ session_event_seq）。
func writeControl(ctx context.Context, tx pgx.Tx, taskID, desired, reason string, admit func(status, statusReason, current string) error) (int64, error) {
	var status, statusReason, current string
	err := tx.QueryRow(ctx, "SELECT status, status_reason FROM tasks WHERE task_id = $1 FOR UPDATE", taskID).Scan(&status, &statusReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, notFoundf("任务 %s", taskID)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.QueryRow(ctx, "SELECT desired FROM task_control WHERE task_id = $1 FOR UPDATE", taskID).Scan(&current); err != nil {
		return 0, err
	}
	if err := admit(status, statusReason, current); err != nil {
		return 0, err
	}
	var version int64
	if err := tx.QueryRow(ctx, `UPDATE task_control SET control_version = control_version + 1, desired = $2, reason = $3
		WHERE task_id = $1 RETURNING control_version`, taskID, desired, reason).Scan(&version); err != nil {
		return 0, err
	}
	if err := lockEventSeq(ctx, tx, taskID); err != nil {
		return 0, err
	}
	payload, _ := json.Marshal(map[string]any{"desired": desired, "reason": reason, "control_version": version})
	if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: taskID, key: fmt.Sprintf("control_accepted:%d", version),
		typ: "control_accepted", payload: payload}); err != nil {
		return 0, err
	}
	return version, nil
}

// admitControl 是规格 §8.1 的控制写入规则：终态任务不再接受控制；已接受的 cancel 不可被
// pause 或 resume 覆盖；resume（desired = run）要求任务处于 paused。
func admitControl(taskID, status, current, next string) error {
	switch {
	case status == "succeeded" || status == "failed" || status == "cancelled":
		return rejectf(persistence.CodeTaskEnded, "任务 %s 已是 %s", taskID, status)
	case current == "cancel" && next != "cancel":
		return rejectf(persistence.CodeCancelPending, "任务 %s 已接受 cancel", taskID)
	case next == "run" && status != "paused":
		return rejectf(persistence.CodeNotPaused, "任务 %s 处于 %s，不能 resume", taskID, status)
	}
	return nil
}

// GetRequest 读取已提交的请求记录（实现 api.Store）。
func (s *Store) GetRequest(ctx context.Context, requestID string) (api.RequestRecord, error) {
	r := api.RequestRecord{RequestID: requestID}
	err := s.read(ctx, "GetRequest", func(ctx context.Context, q queryer) error {
		var resp []byte
		err := q.QueryRow(ctx, "SELECT kind, body_hash, resource_id, response FROM api_requests WHERE request_id = $1",
			requestID).Scan(&r.Kind, &r.BodyHash, &r.ResourceID, &resp)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("请求 %s", requestID)
		}
		r.Response = resp
		return err
	})
	return r, err
}

// GetTask 读取任务的当前视图（实现 api.Store）。
func (s *Store) GetTask(ctx context.Context, taskID string) (api.TaskView, error) {
	v := api.TaskView{TaskID: taskID}
	err := s.read(ctx, "GetTask", func(ctx context.Context, q queryer) error {
		var current *string
		err := q.QueryRow(ctx, `SELECT t.status, t.status_reason, t.current_attempt_id, c.desired, c.control_version,
				t.applied_control_version, t.attempts_total, t.created_at, COALESCE(t.spec_json->>'topic', '')
			FROM tasks t JOIN task_control c USING (task_id) WHERE t.task_id = $1`, taskID).
			Scan(&v.Status, &v.StatusReason, &current, &v.Desired, &v.ControlVersion, &v.AppliedControlVersion, &v.AttemptsTotal,
				&v.CreatedAt, &v.Topic)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		if current != nil {
			v.CurrentAttemptID = *current
		}
		v.CreatedAt = v.CreatedAt.UTC()
		return err
	})
	return v, err
}

// ListEvents 按 task_seq 升序返回事件（实现 api.Store）。
func (s *Store) ListEvents(ctx context.Context, taskID string, afterSeq int64, limit int) ([]api.Event, error) {
	var out []api.Event
	err := s.read(ctx, "ListEvents", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT task_seq, COALESCE(attempt_id, ''), source, type, COALESCE(worker_seq, 0), payload, ts
			FROM events WHERE task_id = $1 AND task_seq > $2 ORDER BY task_seq LIMIT $3`, taskID, afterSeq, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e api.Event
			var payload []byte
			if err := rows.Scan(&e.TaskSeq, &e.AttemptID, &e.Source, &e.Type, &e.WorkerSeq, &payload, &e.TS); err != nil {
				return err
			}
			e.Payload = payload
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}
