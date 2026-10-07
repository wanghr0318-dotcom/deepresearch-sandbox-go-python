package postgres

// 本文件是装配（internal/app）为会话 turn 构造 task_start 时额外读取的事实（M4 Plan 12 Task 9）：会话 API 的接口
// 形式、answer 指令的提问 ID（契约 A/B），以及 D5 carryover 的来源与授权（契约 D）。

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
)

// Sessions 以 api.Sessions 接口返回会话 API 用例（装配以接口持有，不导入本包）。
func (s *Store) Sessions() api.Sessions { return s.SessionAPI() }

// EnvUIDRangeID 返回环境使用的 UID 范围（environments.uid_range_id；按 owner 分配的会话范围的 owner_id 不是 env_id，
// GetUIDRange 查不到它）。没有时为 ErrNotFound。装配据此写入 sessions.uid_range_id。
func (s *Store) EnvUIDRangeID(ctx context.Context, envID string) (string, error) {
	var out string
	err := s.read(ctx, "EnvUIDRangeID", func(ctx context.Context, q queryer) error {
		var id *string
		err := q.QueryRow(ctx, "SELECT uid_range_id FROM environments WHERE env_id = $1", envID).Scan(&id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return notFoundf("环境 %s", envID)
		case err != nil:
			return err
		case id == nil:
			return notFoundf("环境 %s 没有 UID 范围", envID)
		}
		out = *id
		return nil
	})
	return out, err
}

// PendingQuestionID 返回 turn 最近一次 awaiting_input 提议的 question_id（task_start.directive.answer 须携带它，
// 契约 A/B：resume_directive 只存 {kind, answers}）。没有时为空串。
func (s *Store) PendingQuestionID(ctx context.Context, taskID string) (string, error) {
	var out string
	err := s.read(ctx, "PendingQuestionID", func(ctx context.Context, q queryer) error {
		var id *string
		err := q.QueryRow(ctx, `SELECT payload->>'question_id' FROM events WHERE task_id = $1 AND type = 'awaiting_input'
			ORDER BY task_seq DESC LIMIT 1`, taskID).Scan(&id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			out = ""
			return nil
		case err != nil:
			return err
		}
		out = ""
		if id != nil {
			out = *id
		}
		return nil
	})
	return out, err
}

// TurnCarryover 返回 D5 carryover 的来源（契约 D）：同一会话中 turn_index 紧邻的上一个 turn 被本 turn 取代（cancel
// 控制的 reason = superseded）且有已提交的 task checkpoint 时，返回该 turn 与其最新 checkpoint 的 state_ref（已是
// 授权到会话 scope 的 blob）或 inline state；没有时 srcTaskID 为空。恢复 turn（restored_from_task_id 非空）从种子
// 继续，不带 carryover。
func (s *Store) TurnCarryover(ctx context.Context, taskID string) (srcTaskID, stateRef string, state json.RawMessage, err error) {
	err = s.read(ctx, "TurnCarryover", func(ctx context.Context, q queryer) error {
		srcTaskID, stateRef, state = "", "", nil
		var ref *string
		var inline []byte
		err := q.QueryRow(ctx, `SELECT p.task_id, c.state_ref, c.state_inline
			FROM tasks t
			JOIN tasks p ON p.session_id = t.session_id AND p.turn_index = t.turn_index - 1
			JOIN task_control pc ON pc.task_id = p.task_id AND pc.desired = 'cancel' AND pc.reason = 'superseded'
			JOIN task_progress pp ON pp.task_id = p.task_id
			JOIN checkpoints c ON c.scope_kind = 'task' AND c.scope_id = p.task_id AND c.checkpoint_id = pp.latest_checkpoint_id
			WHERE t.task_id = $1 AND t.session_id IS NOT NULL AND t.restored_from_task_id IS NULL`, taskID).
			Scan(&srcTaskID, &ref, &inline)
		if errors.Is(err, pgx.ErrNoRows) {
			srcTaskID = ""
			return nil
		}
		if err != nil {
			return err
		}
		if ref != nil {
			stateRef = *ref
		}
		if len(inline) > 0 {
			state = json.RawMessage(inline)
		}
		if stateRef == "" && len(state) == 0 {
			srcTaskID = ""
		}
		return nil
	})
	return srcTaskID, stateRef, state, err
}

// AuthorizeCarryover 登记 carryover blob（inline 的 checkpoint 状态由装配写入 BlobStore 后调用）并授权到 turn 的
// task scope 与其会话 scope（Worker 经 Gateway GET /blobs/<sha> 读取）。幂等；同一 sha256 的大小不同为冲突。
func (s *Store) AuthorizeCarryover(ctx context.Context, taskID, srcTaskID, sha string, size int64) error {
	return s.run(ctx, "AuthorizeCarryover", taskID+"/"+sha, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO blobs (sha256, size) VALUES ($1, $2) ON CONFLICT DO NOTHING", sha, size); err != nil {
			return err
		}
		var got int64
		if err := tx.QueryRow(ctx, "SELECT size FROM blobs WHERE sha256 = $1", sha).Scan(&got); err != nil {
			return err
		}
		if got != size {
			return conflictf("blob %s 已登记为 %d 字节，不是 %d", sha, got, size)
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM blob_provenance WHERE scope_kind = 'task' AND scope_id = $1
				AND sha256 = $2 AND source = 'carryover')`, taskID, sha).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(ctx, `INSERT INTO blob_provenance (scope_kind, scope_id, sha256, source, ref)
					VALUES ('task', $1, $2, 'carryover', $3)`, taskID, sha, srcTaskID); err != nil {
				return err
			}
		}
		return authorizeTaskBlob(ctx, tx, taskID, sha)
	})
}
