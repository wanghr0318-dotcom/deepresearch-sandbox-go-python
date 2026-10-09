package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
)

var _ resource.Store = (*Store)(nil)

// RecordIntent 记录资源意图（实现 resource.Store）：同一 intent_id 内容相同返回原结果，
// 内容不同或 (kind, name) 已被其他意图占用为冲突。
func (s *Store) RecordIntent(ctx context.Context, i resource.Intent) (resource.Intent, error) {
	if i.IntentID == "" || i.EnvID == "" || i.Kind == "" || i.Name == "" {
		return resource.Intent{}, invalidf("RecordIntent 缺少字段")
	}
	var out resource.Intent
	err := s.run(ctx, "RecordIntent", i.IntentID, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO resource_intents (intent_id, env_id, kind, name, state)
			VALUES ($1, $2, $3, $4, 'pending') ON CONFLICT DO NOTHING`, i.IntentID, i.EnvID, i.Kind, i.Name)
		if err != nil {
			return err
		}
		existing, err := selectIntent(ctx, tx, i.IntentID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return conflictf("资源 %s/%s 已被其他意图占用", i.Kind, i.Name)
		}
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 && (existing.EnvID != i.EnvID || existing.Kind != i.Kind || existing.Name != i.Name) {
			return conflictf("意图 %s 已存在且内容不同", i.IntentID)
		}
		out = existing
		return nil
	})
	return out, err
}

// intentTransitions 是意图状态允许的前进方向。
var intentTransitions = map[string][]string{
	resource.IntentPending:  {resource.IntentAcquired, resource.IntentFailed},
	resource.IntentAcquired: {resource.IntentReleased},
}

// ResolveIntent 推进意图状态（实现 resource.Store）：已在目标状态返回原结果，倒退或跳跃为冲突。
func (s *Store) ResolveIntent(ctx context.Context, intentID, state string) (resource.Intent, error) {
	var out resource.Intent
	err := s.run(ctx, "ResolveIntent", intentID+"->"+state, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectIntent(ctx, tx, intentID, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("意图 %s", intentID)
		}
		if err != nil {
			return err
		}
		if cur.State == state {
			out = cur
			return nil
		}
		if !contains(intentTransitions[cur.State], state) {
			return conflictf("意图 %s 不能从 %s 变为 %s", intentID, cur.State, state)
		}
		if _, err := tx.Exec(ctx, "UPDATE resource_intents SET state = $2 WHERE intent_id = $1", intentID, state); err != nil {
			return err
		}
		cur.State = state
		out = cur
		return nil
	})
	return out, err
}

// GetIntent 读取资源意图（实现 resource.Store）。
func (s *Store) GetIntent(ctx context.Context, intentID string) (resource.Intent, error) {
	var out resource.Intent
	err := s.read(ctx, "GetIntent", func(ctx context.Context, q queryer) error {
		i, err := selectIntent(ctx, q, intentID, false)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("意图 %s", intentID)
		}
		out = i
		return err
	})
	return out, err
}

func selectIntent(ctx context.Context, q queryer, intentID string, forUpdate bool) (resource.Intent, error) {
	sql := "SELECT intent_id, env_id, kind, name, state FROM resource_intents WHERE intent_id = $1"
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var i resource.Intent
	err := q.QueryRow(ctx, sql, intentID).Scan(&i.IntentID, &i.EnvID, &i.Kind, &i.Name, &i.State)
	return i, err
}

// SeedUIDRanges 幂等地登记 UID 范围（实现 resource.Store）。
func (s *Store) SeedUIDRanges(ctx context.Context, base, size int64, count int) error {
	if base < 0 || size < 1 || count < 1 {
		return invalidf("SeedUIDRanges 参数不合法")
	}
	return s.run(ctx, "SeedUIDRanges", "", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO uid_ranges (uid_range_id, base, size, state)
			SELECT 'uid-' || b, b, $2, 'free' FROM generate_series($1::bigint, $1 + ($3::bigint - 1) * $2, $2) AS b
			ON CONFLICT DO NOTHING`, base, size, count)
		return err
	})
}

// AssignUIDRange 为环境分配 UID 范围（实现 resource.Store）：以 (env_id, allocation_id) 为身份，
// 结果丢失后重试返回原分配。锁顺序：environments → uid_ranges。
func (s *Store) AssignUIDRange(ctx context.Context, envID, allocationID string) (resource.UIDRange, error) {
	if envID == "" || allocationID == "" {
		return resource.UIDRange{}, invalidf("AssignUIDRange 缺少 env_id 或 allocation_id")
	}
	var out resource.UIDRange
	err := s.run(ctx, "AssignUIDRange", envID+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := selectEnvironment(ctx, tx, envID, true); err != nil {
			return err
		}
		cur, err := selectUIDRange(ctx, tx, "owner_id = $1 AND state = 'assigned'", envID)
		switch {
		case err == nil && cur.AllocationID == allocationID:
			out = cur
			return nil
		case err == nil:
			return conflictf("环境 %s 已有分配 %s", envID, cur.AllocationID)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		// SKIP LOCKED：并发分配不等待同一行，也不会因对方刚分配而误报无空闲范围
		free, err := selectUIDRange(ctx, tx, "state = 'free' ORDER BY base LIMIT 1 FOR UPDATE SKIP LOCKED", nil)
		if errors.Is(err, pgx.ErrNoRows) {
			return resource.ErrNoFreeUIDRange
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE uid_ranges SET state = 'assigned', owner_kind = 'environment', owner_id = $2, allocation_id = $3
			WHERE uid_range_id = $1`, free.UIDRangeID, envID, allocationID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE environments SET uid_range_id = $2 WHERE env_id = $1", envID, free.UIDRangeID); err != nil {
			return err
		}
		free.State, free.OwnerID, free.AllocationID = "assigned", envID, allocationID
		out = free
		return nil
	})
	return out, err
}

// ReleaseUIDRange 释放 UID 范围（实现 resource.Store）：只有分配代次匹配才释放；
// 已释放且未复用返回原结果；已被后来的分配复用为冲突。规格 §4.5：拥有该范围的环境必须已
// 停止且清理完成（cleanup_state = done），否则为冲突——仍在运行的进程会与下一个使用者共用 UID。
// 文件属主与挂载等条件由调用方（coordinator）在调用前核对。环境行不加锁读取：stopped_at 与
// cleanup_state 只会前进，读到"已完成"之后不会倒退；不加锁也避免与 AssignUIDRange 的
// environments → uid_ranges 锁顺序成环。
func (s *Store) ReleaseUIDRange(ctx context.Context, uidRangeID, allocationID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := s.run(ctx, "ReleaseUIDRange", uidRangeID+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectUIDRange(ctx, tx, "uid_range_id = $1", uidRangeID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("UID 范围 %s", uidRangeID)
		}
		if err != nil {
			return err
		}
		switch {
		case cur.AllocationID != allocationID:
			return conflictf("UID 范围 %s 当前分配为 %q，不是 %q", uidRangeID, cur.AllocationID, allocationID)
		case cur.State == "free":
			out = cur
			return nil
		}
		var cleaned bool
		err = tx.QueryRow(ctx, "SELECT stopped_at IS NOT NULL AND cleanup_state = 'done' FROM environments WHERE env_id = $1",
			cur.OwnerID).Scan(&cleaned)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !cleaned {
			return conflictf("UID 范围 %s 的环境 %s 尚未停止并完成清理，不能归还", uidRangeID, cur.OwnerID)
		}
		if _, err := tx.Exec(ctx, "UPDATE uid_ranges SET state = 'free', owner_kind = '', owner_id = '' WHERE uid_range_id = $1",
			uidRangeID); err != nil {
			return err
		}
		cur.State, cur.OwnerID = "free", ""
		out = cur
		return nil
	})
	return out, err
}

// QuarantineUIDRange 隔离 UID 范围（实现 resource.Store；规格 §4.5"存疑则隔离"、E39）：只在分配代次匹配时把范围
// 置为 quarantined（保留 owner 以便诊断；quarantined 的范围不会被分配，也不在启动核对的未归还集合中），同一事务
// 记录 quarantined_resources(kind = uid_files)。已隔离时幂等（隔离记录按路径幂等，不覆盖原因）。
// 锁顺序：uid_ranges → quarantined_resources（不触碰 environments）。
func (s *Store) QuarantineUIDRange(ctx context.Context, uidRangeID, allocationID, reason string) error {
	if uidRangeID == "" || allocationID == "" || reason == "" {
		return invalidf("QuarantineUIDRange 缺少 uid_range_id、allocation_id 或 reason")
	}
	return s.run(ctx, "QuarantineUIDRange", uidRangeID+"/"+allocationID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectUIDRange(ctx, tx, "uid_range_id = $1", uidRangeID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("UID 范围 %s", uidRangeID)
		}
		if err != nil {
			return err
		}
		if cur.AllocationID != allocationID {
			return conflictf("UID 范围 %s 当前分配为 %q，不是 %q", uidRangeID, cur.AllocationID, allocationID)
		}
		if cur.State != "quarantined" {
			if _, err := tx.Exec(ctx, "UPDATE uid_ranges SET state = 'quarantined' WHERE uid_range_id = $1", uidRangeID); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO quarantined_resources (resource_path, kind, observed_owner, reason)
			VALUES ($1, $2, $3, $4) ON CONFLICT (resource_path) DO NOTHING`,
			resource.UIDRangeQuarantinePath(uidRangeID), "uid_files", cur.OwnerID, reason) // provider.LayerUIDFiles
		return err
	})
}

// GetUIDRange 读取分配给环境的 UID 范围（实现 resource.Store）。
func (s *Store) GetUIDRange(ctx context.Context, envID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := s.read(ctx, "GetUIDRange", func(ctx context.Context, q queryer) error {
		r, err := selectUIDRangeRead(ctx, q, envID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("环境 %s 没有 UID 范围", envID)
		}
		out = r
		return err
	})
	return out, err
}

// selectUIDRange 以 FOR UPDATE 读取一段范围；where 为条件子句（含可选参数）。
func selectUIDRange(ctx context.Context, tx pgx.Tx, where string, arg any) (resource.UIDRange, error) {
	sql := "SELECT uid_range_id, base, size, state, owner_id, allocation_id FROM uid_ranges WHERE " + where
	if !strings.Contains(where, "FOR UPDATE") {
		sql += " FOR UPDATE"
	}
	var r resource.UIDRange
	var row pgx.Row
	if arg == nil {
		row = tx.QueryRow(ctx, sql)
	} else {
		row = tx.QueryRow(ctx, sql, arg)
	}
	err := row.Scan(&r.UIDRangeID, &r.Base, &r.Size, &r.State, &r.OwnerID, &r.AllocationID)
	return r, err
}

func selectUIDRangeRead(ctx context.Context, q queryer, envID string) (resource.UIDRange, error) {
	var r resource.UIDRange
	err := q.QueryRow(ctx, `SELECT uid_range_id, base, size, state, owner_id, allocation_id FROM uid_ranges
		WHERE owner_id = $1 AND state = 'assigned'`, envID).Scan(&r.UIDRangeID, &r.Base, &r.Size, &r.State, &r.OwnerID, &r.AllocationID)
	return r, err
}

// MarkStopped 记录环境已停止（实现 resource.Store）：只在尚未记录时写入，重试不倒退。
func (s *Store) MarkStopped(ctx context.Context, envID string, at time.Time) (resource.Environment, error) {
	var out resource.Environment
	err := s.run(ctx, "MarkStopped", envID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE environments SET stopped_at = $2 WHERE env_id = $1 AND stopped_at IS NULL", envID, at); err != nil {
			return err
		}
		e, err := selectEnvironment(ctx, tx, envID, false)
		out = e
		return err
	})
	return out, err
}

// cleanupRank 定义清理状态的前进顺序。
var cleanupRank = map[string]int{resource.CleanupNone: 0, resource.CleanupPending: 1, resource.CleanupDone: 2}

// UpdateCleanup 以 cleanup_tries 的 CAS 记录一次清理尝试（实现 resource.Store）。
func (s *Store) UpdateCleanup(ctx context.Context, u resource.CleanupUpdate) (resource.Environment, error) {
	if _, ok := cleanupRank[u.State]; !ok || u.EnvID == "" || u.ExpectedTries < 0 {
		return resource.Environment{}, invalidf("UpdateCleanup 参数不合法")
	}
	var out resource.Environment
	err := s.run(ctx, "UpdateCleanup", u.EnvID, func(ctx context.Context, tx pgx.Tx) error {
		cur, err := selectEnvironment(ctx, tx, u.EnvID, true)
		if err != nil {
			return err
		}
		switch {
		case cur.CleanupTries == u.ExpectedTries+1 && cur.CleanupState == u.State && cur.CleanupError == u.Error:
			out = cur // 同一次尝试的重试
			return nil
		case cur.CleanupTries != u.ExpectedTries:
			return conflictf("环境 %s 的 cleanup_tries 为 %d，不是 %d", u.EnvID, cur.CleanupTries, u.ExpectedTries)
		case cleanupRank[u.State] < cleanupRank[cur.CleanupState]:
			return conflictf("环境 %s 的清理状态不能从 %s 退回 %s", u.EnvID, cur.CleanupState, u.State)
		}
		if _, err := tx.Exec(ctx, `UPDATE environments SET cleanup_state = $2, cleanup_tries = $3, cleanup_error = $4, next_retry_at = $5
			WHERE env_id = $1`, u.EnvID, u.State, u.ExpectedTries+1, u.Error, u.NextRetryAt); err != nil {
			return err
		}
		out, err = selectEnvironment(ctx, tx, u.EnvID, false)
		return err
	})
	return out, err
}

// GetEnvironment 读取环境记录（实现 resource.Store）。
func (s *Store) GetEnvironment(ctx context.Context, envID string) (resource.Environment, error) {
	var out resource.Environment
	err := s.read(ctx, "GetEnvironment", func(ctx context.Context, q queryer) error {
		e, err := selectEnvironment(ctx, q, envID, false)
		out = e
		return err
	})
	return out, err
}

func selectEnvironment(ctx context.Context, q queryer, envID string, forUpdate bool) (resource.Environment, error) {
	sql := `SELECT env_id, kind, COALESCE(attempt_id, ''), status, stopped_at, cleanup_state, cleanup_tries, cleanup_error, next_retry_at
		FROM environments WHERE env_id = $1`
	if forUpdate {
		sql += " FOR UPDATE"
	}
	var e resource.Environment
	err := q.QueryRow(ctx, sql, envID).Scan(&e.EnvID, &e.Kind, &e.AttemptID, &e.Status, &e.StoppedAt,
		&e.CleanupState, &e.CleanupTries, &e.CleanupError, &e.NextRetryAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, notFoundf("环境 %s", envID)
	}
	return e, err
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// EnvironmentStats 返回运维指标（agentbox_sandbox_envs、agentbox_sandbox_cleanup_backlog）：按 kind 统计尚未记录
// stopped_at 的环境数，以及已停止、清理尚未完成的环境数。只读；扫描 environments（规模随历史增长，由调用方缓存）。
func (s *Store) EnvironmentStats(ctx context.Context) (running map[string]int64, cleanupBacklog int64, err error) {
	running = map[string]int64{}
	err = s.read(ctx, "EnvironmentStats", func(ctx context.Context, q queryer) error {
		rows, err := q.Query(ctx, `SELECT kind,
			count(*) FILTER (WHERE stopped_at IS NULL),
			count(*) FILTER (WHERE stopped_at IS NOT NULL AND cleanup_state <> 'done')
			FROM environments GROUP BY kind`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kind string
			var live, backlog int64
			if err := rows.Scan(&kind, &live, &backlog); err != nil {
				return err
			}
			running[kind] = live
			cleanupBacklog += backlog
		}
		return rows.Err()
	})
	return running, cleanupBacklog, err
}
