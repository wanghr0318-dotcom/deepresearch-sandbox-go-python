package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// 锁顺序（规格 §7.1，含 M4 Plan 12 的会话增补）。所有事务按下列顺序取其子集加锁：
//
//	sessions → session_control → session_progress → users → incarnations
//	→ tasks → task_control → task_progress → task_event_seq → session_event_seq
//	→ subruns → attempts → attempt_access → environments → artifact_heads
//	→ budgets → subrun_budgets → exec_quotas → calls → reservations
//
// session_event_seq 紧随 task_event_seq：会话 task 的事件先锁任务的序号行，再在同一事务内从会话的序号行分配
// session_seq；会话生命周期事件（session_events）只锁 sessions 与 session_event_seq。users 行锁（每用户同时 1 个
// 运行中的计数）位于会话行之后、任务行之前；incarnations 只由 session actor 的用例在会话行之后加锁。

// lockEventSeq 锁住任务的事件序号行（锁顺序中位于 task_progress 之后、session_event_seq 与 attempts 之前）。
// 同一任务的事件追加由此串行化。
func lockEventSeq(ctx context.Context, tx pgx.Tx, taskID string) error {
	var next int64
	err := tx.QueryRow(ctx, "SELECT next FROM task_event_seq WHERE task_id = $1 FOR UPDATE", taskID).Scan(&next)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFoundf("任务 %s 没有事件序号行", taskID)
	}
	return err
}

// nextEventSeq 分配下一个 task_seq；调用方必须已持有 lockEventSeq（规格 §7.2）。
func nextEventSeq(ctx context.Context, tx pgx.Tx, taskID string) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, "UPDATE task_event_seq SET next = next + 1 WHERE task_id = $1 RETURNING next", taskID).Scan(&seq)
	return seq, err
}

// taskSessionID 返回任务所属的会话（独立任务为空串）。session_id 创建后不变，无需加锁。
func taskSessionID(ctx context.Context, tx pgx.Tx, taskID string) (string, error) {
	var sessionID *string
	err := tx.QueryRow(ctx, "SELECT session_id FROM tasks WHERE task_id = $1", taskID).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", notFoundf("任务 %s", taskID)
	}
	if err != nil || sessionID == nil {
		return "", err
	}
	return *sessionID, nil
}

// nextSessionSeq 以 session_event_seq 的行锁分配下一个 session_seq（单调、连续：事务回滚时一并回滚，提交顺序
// 即序号顺序）。锁顺序在 task_event_seq 之后。
func nextSessionSeq(ctx context.Context, tx pgx.Tx, sessionID string) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, "UPDATE session_event_seq SET next = next + 1 WHERE session_id = $1 RETURNING next", sessionID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, notFoundf("会话 %s 没有事件序号行", sessionID)
	}
	return seq, err
}

// appendSessionSeq 为任务的一条新事件分配会话序号：会话 task 返回 (session_id, session_seq)，独立任务返回
// ("", 0)。调用方必须已持有 lockEventSeq。
func appendSessionSeq(ctx context.Context, tx pgx.Tx, taskID string) (string, int64, error) {
	sessionID, err := taskSessionID(ctx, tx, taskID)
	if err != nil || sessionID == "" {
		return "", 0, err
	}
	seq, err := nextSessionSeq(ctx, tx, sessionID)
	return sessionID, seq, err
}

// nullSeq 把 0 映射为 SQL NULL（独立任务的事件没有会话序号）。
func nullSeq(seq int64) any {
	if seq == 0 {
		return nil
	}
	return seq
}

// hostEvent 是一条 host 事件。
type hostEvent struct {
	taskID    string
	key       string // event_key，去重身份
	attemptID string
	typ       string
	payload   []byte
}

// appendHostEvent 以 event_key 幂等追加 host 事件：已存在且内容相同返回原 task_seq，
// 内容不同为冲突（规格 §7.2）。会话 task 的事件同事务分配 session_seq。调用方必须已持有 lockEventSeq。
func appendHostEvent(ctx context.Context, tx pgx.Tx, e hostEvent) (int64, error) {
	if e.payload == nil {
		e.payload = []byte("{}")
	}
	hash := contentHash([]byte(e.typ), []byte(e.attemptID), e.payload)
	var seq int64
	var existing []byte
	err := tx.QueryRow(ctx, "SELECT task_seq, content_hash FROM events WHERE task_id = $1 AND event_key = $2",
		e.taskID, e.key).Scan(&seq, &existing)
	switch {
	case err == nil && bytes.Equal(existing, hash):
		return seq, nil
	case err == nil:
		return 0, conflictf("事件 %s 已存在且内容不同", e.key)
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	seq, err = nextEventSeq(ctx, tx, e.taskID)
	if err != nil {
		return 0, err
	}
	sessionID, sessionSeq, err := appendSessionSeq(ctx, tx, e.taskID)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, source, type, payload, content_hash,
			session_id, session_seq)
		VALUES ($1, $2, $3, NULLIF($4, ''), 'host', $5, $6, $7, NULLIF($8, ''), $9)`,
		e.taskID, seq, e.key, e.attemptID, e.typ, e.payload, hash, sessionID, nullSeq(sessionSeq))
	return seq, err
}

// sessionEventType 是会话生命周期事件的类型（payload 为 {state, user_message?}；见 api.SessionEventRecord）。
const sessionEventType = "session_state"

// appendSessionEvent 以 event_key 幂等追加会话生命周期事件（session_events）：已存在且内容相同返回原序号，
// 内容不同为冲突。调用方必须已持有 sessions 行锁。
func appendSessionEvent(ctx context.Context, tx pgx.Tx, sessionID, key string, payload []byte) (int64, error) {
	var seq int64
	var same bool
	err := tx.QueryRow(ctx, "SELECT session_seq, payload = $3::jsonb FROM session_events WHERE session_id = $1 AND event_key = $2",
		sessionID, key, payload).Scan(&seq, &same)
	switch {
	case err == nil && same:
		return seq, nil
	case err == nil:
		return 0, conflictf("会话 %s 的事件 %s 已存在且内容不同", sessionID, key)
	case !errors.Is(err, pgx.ErrNoRows):
		return 0, err
	}
	seq, err = nextSessionSeq(ctx, tx, sessionID)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO session_events (session_id, session_seq, event_key, type, payload) VALUES ($1, $2, $3, $4, $5)",
		sessionID, seq, key, sessionEventType, payload)
	return seq, err
}
