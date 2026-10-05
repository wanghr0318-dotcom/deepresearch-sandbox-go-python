package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// lockEventSeq 锁住任务的事件序号行（规格 §7.1 锁顺序中位于 task_progress 之后、attempts 之前）。
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

// hostEvent 是一条 host 事件。
type hostEvent struct {
	taskID    string
	key       string // event_key，去重身份
	attemptID string
	typ       string
	payload   []byte
}

// appendHostEvent 以 event_key 幂等追加 host 事件：已存在且内容相同返回原 task_seq，
// 内容不同为冲突（规格 §7.2）。调用方必须已持有 lockEventSeq。
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
	_, err = tx.Exec(ctx, `INSERT INTO events (task_id, task_seq, event_key, attempt_id, source, type, payload, content_hash)
		VALUES ($1, $2, $3, NULLIF($4, ''), 'host', $5, $6, $7)`,
		e.taskID, seq, e.key, e.attemptID, e.typ, e.payload, hash)
	return seq, err
}
