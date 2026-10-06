package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

var _ task.TurnStore = (*Store)(nil)

// FailQueuedTurn 把没有活动 attempt 的 queued 会话 turn 裁决为 failed（实现 task.TurnStore；M4 Plan 12 Task 7：会话已
// 关闭或恢复两次失败时 session actor 拒绝授予）。同事务清除该 turn 对会话的占用（current_task_id）与阻塞
// （blocked_by_task_id）、清空续跑指令，并追加 host 事件 task_terminal。已是 failed 且原因相同时返回成功（重跑）；
// 其余非 queued 状态为 ErrRejected（not_runnable）。
// 锁顺序：sessions → tasks → task_event_seq → session_event_seq。
func (s *Store) FailQueuedTurn(ctx context.Context, taskID, reason string) error {
	if taskID == "" || reason == "" {
		return invalidf("FailQueuedTurn 缺少 task_id 或 reason")
	}
	return s.run(ctx, "FailQueuedTurn", taskID, func(ctx context.Context, tx pgx.Tx) error {
		sessionID, err := taskSessionID(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if sessionID == "" {
			return invalidf("任务 %s 不是会话 turn", taskID)
		}
		if _, _, err := lockSession(ctx, tx, sessionID); err != nil {
			return err
		}
		var status, statusReason string
		err = tx.QueryRow(ctx, "SELECT status, status_reason FROM tasks WHERE task_id = $1 FOR UPDATE", taskID).Scan(&status, &statusReason)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		switch {
		case err != nil:
			return err
		case status == "failed" && statusReason == reason:
			return nil
		case status != "queued":
			return rejectf(persistence.CodeNotRunnable, "turn %s 处于 %s，不是 queued", taskID, status)
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = 'failed', status_reason = $2, not_before = NULL,
				resume_directive = NULL, row_version = row_version + 1 WHERE task_id = $1`, taskID, reason); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE sessions SET
				current_task_id = CASE WHEN current_task_id = $2 THEN NULL ELSE current_task_id END,
				blocked_by_task_id = CASE WHEN blocked_by_task_id = $2 THEN NULL ELSE blocked_by_task_id END
			WHERE session_id = $1`, sessionID, taskID); err != nil {
			return err
		}
		if err := lockEventSeq(ctx, tx, taskID); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"task_status": "failed", "status_reason": reason})
		if _, err := appendHostEvent(ctx, tx, hostEvent{taskID: taskID, key: "task_terminal:" + reason, typ: "task_terminal",
			payload: payload}); err != nil {
			return err
		}
		// 故障重试之后留下的未终态 sub-run 随 turn 失败收尾（I13）；在追加事件（task_event_seq → session_event_seq）
		// 之后加锁，符合 … → session_event_seq → subruns 的锁顺序。
		return closeOpenSubrunsTx(ctx, tx, taskID, "failed")
	})
}
