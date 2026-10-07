package postgres

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
)

// 按 owner 保留的 UID 范围（M4 Plan 12 Task 5；规格 §12.1"session 在整个生命周期保留其 UID 范围"、§4.5）。
// owner_id = "session:<session_id>"、owner_kind = 'session'。recovery 的未归还范围查询按 owner_id 关联环境，
// 因此 owner 范围不会被启动核对归还；只由 ReleaseOwnerUIDRange 归还。

var _ resource.OwnerUIDStore = (*Store)(nil)

// AssignOwnerUIDRange 实现 resource.OwnerUIDStore。锁顺序：environments → uid_ranges。
func (s *Store) AssignOwnerUIDRange(ctx context.Context, owner, envID, allocationID string) (resource.UIDRange, error) {
	if !strings.HasPrefix(owner, resource.OwnerPrefixSession) || envID == "" || allocationID == "" {
		return resource.UIDRange{}, invalidf("AssignOwnerUIDRange 参数不合法")
	}
	var out resource.UIDRange
	err := s.run(ctx, "AssignOwnerUIDRange", owner+"/"+envID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := selectEnvironment(ctx, tx, envID, true); err != nil {
			return err
		}
		cur, err := selectUIDRange(ctx, tx, "owner_id = $1 AND state = 'assigned'", owner)
		switch {
		case err == nil && cur.AllocationID != allocationID:
			return conflictf("owner %s 已有分配 %s", owner, cur.AllocationID)
		case err == nil:
		case errors.Is(err, pgx.ErrNoRows):
			free, err := selectUIDRange(ctx, tx, "state = 'free' ORDER BY base LIMIT 1 FOR UPDATE SKIP LOCKED", nil)
			if errors.Is(err, pgx.ErrNoRows) {
				return resource.ErrNoFreeUIDRange
			}
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE uid_ranges SET state = 'assigned', owner_kind = 'session', owner_id = $2, allocation_id = $3
				WHERE uid_range_id = $1`, free.UIDRangeID, owner, allocationID); err != nil {
				return err
			}
			free.State, free.OwnerID, free.AllocationID = "assigned", owner, allocationID
			cur = free
		default:
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE environments SET uid_range_id = $2 WHERE env_id = $1", envID, cur.UIDRangeID); err != nil {
			return err
		}
		out = cur
		return nil
	})
	return out, err
}

// OwnerUIDRange 实现 resource.OwnerUIDStore：owner 的 assigned 范围，以及使用过它（environments.uid_range_id）而
// 尚未"已停止且清理完成"的环境数（只读；归还时 ReleaseOwnerUIDRange 在事务内再次核对）。
func (s *Store) OwnerUIDRange(ctx context.Context, owner string) (resource.UIDRange, int, error) {
	var out resource.UIDRange
	var pending int
	err := s.read(ctx, "OwnerUIDRange", func(ctx context.Context, q queryer) error {
		err := q.QueryRow(ctx, `SELECT r.uid_range_id, r.base, r.size, r.state, r.owner_id, r.allocation_id,
				(SELECT count(*) FROM environments e WHERE e.uid_range_id = r.uid_range_id
					AND NOT (e.stopped_at IS NOT NULL AND e.cleanup_state = 'done'))
			FROM uid_ranges r WHERE r.owner_id = $1 AND r.state = 'assigned'`, owner).Scan(
			&out.UIDRangeID, &out.Base, &out.Size, &out.State, &out.OwnerID, &out.AllocationID, &pending)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("owner %s 没有 UID 范围", owner)
		}
		return err
	})
	return out, pending, err
}

// ReleaseOwnerUIDRange 实现 resource.OwnerUIDStore：使用过该范围的环境（environments.uid_range_id）须全部已停止且
// 清理完成，否则为冲突（规格 §4.5）。环境行不加锁读取，理由同 ReleaseUIDRange。
func (s *Store) ReleaseOwnerUIDRange(ctx context.Context, owner, allocationID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := s.run(ctx, "ReleaseOwnerUIDRange", owner+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectUIDRange(ctx, tx, "owner_id = $1 AND state = 'assigned'", owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("owner %s 没有 UID 范围", owner)
		}
		if err != nil {
			return err
		}
		if cur.AllocationID != allocationID {
			return conflictf("owner %s 的分配为 %q，不是 %q", owner, cur.AllocationID, allocationID)
		}
		var pending int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM environments
			WHERE uid_range_id = $1 AND NOT (stopped_at IS NOT NULL AND cleanup_state = 'done')`, cur.UIDRangeID).Scan(&pending); err != nil {
			return err
		}
		if pending > 0 {
			return conflictf("owner %s 的 UID 范围 %s 仍有 %d 个环境未停止并完成清理", owner, cur.UIDRangeID, pending)
		}
		if _, err := tx.Exec(ctx, "UPDATE uid_ranges SET state = 'free', owner_kind = '', owner_id = '' WHERE uid_range_id = $1",
			cur.UIDRangeID); err != nil {
			return err
		}
		cur.State, cur.OwnerID = "free", ""
		out = cur
		return nil
	})
	return out, err
}
