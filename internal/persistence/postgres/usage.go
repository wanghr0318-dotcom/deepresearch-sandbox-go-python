package postgres

// 本文件是用量汇总、账单与对账（模型网关设计 2026-10-11 §4、§8；G5）的持久化：usage.Store。
//
// 汇总从已结算的预算 hold 重算 usage_daily：hold 的 charged 是钱（settled → cost_micro，charged_unknown →
// unknown_micro，与 quota_budgets 的 spent、unknown 一一对应），模型与 token 取自 hold 对应的 model_requests 行
// （source model_api）或沙箱调用（source runtime：calls.model，搜索、抓取等取端点名；运行时调用不记录 token）。
// 日期是 hold 的 created_at 所在的 UTC 日（与 hold 的 period 同一个 now()）。
//
// 并发：汇总与带汇总的对账在一条专用连接上先取会话级 advisory lock（各 server 的汇总串行），再开 REPEATABLE READ
// 事务，所以删除重算、移动 watermark 与对账的各次读取都在同一个快照上，不会与正在结算的 hold 交错出假差异。

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

var _ usage.Store = (*UsageStore)(nil)

const (
	// usageLockKey 是汇总的会话级 advisory lock 键（"agentusg" 的 ASCII），与所有权锁不同。
	usageLockKey int64 = 0x6167656e74757367
	// 汇总与对账扫描一个月的数据，比普通用例的期限长。
	usageOpTimeout        = 2 * time.Minute
	usageStatementTimeout = time.Minute
	// usageWatermarkLag：watermark 至多推进到这么久之前所在的一天（跨零点提交的事务）。
	usageWatermarkLag = "1 hour"
	// usageRecordGrace：期限过去这么久之内的 hold 仍可能变化（期限到时 sweep 每 30 s 一次转为 charged_unknown）。
	usageRecordGrace = "2 minutes"
	// usageRowGrace：已结算的模型 API hold 还没有 model_requests 行时，其一天在期限之后最多再保持这么久（G3 先结算 hold、
	// 再写行，写行有重试；hold 没有结算时间列，期限是结算时间的上界，所以"期限之后 10 分钟"覆盖"结算之后 10 分钟"）。
	usageRowGrace = "10 minutes"
)

// usageOpenSQL 是仍可能变化的最早时刻（复审 M1）：held 的 hold；期限未过（含 usageRecordGrace）的 hold——结算与之后写入的
// model_requests 行都在期限之内；已结算却还没有 model_requests 行、期限过去不到 usageRowGrace 的模型 API hold。
// 至多为一小时前。结果所在的 UTC 日即新的 watermark。
const usageOpenSQL = `SELECT (LEAST(
		(SELECT min(created_at) FROM quota_holds WHERE state = 'held'),
		(SELECT min(created_at) FROM quota_holds WHERE deadline_at > now() - interval '` + usageRecordGrace + `'),
		(SELECT min(q.created_at) FROM quota_holds q WHERE q.source = 'model_request' AND q.state IN ('settled', 'charged_unknown')
			AND q.deadline_at > now() - interval '` + usageRowGrace + `'
			AND NOT EXISTS (SELECT 1 FROM model_requests m WHERE m.hold_id = q.hold_id)),
		now() - interval '` + usageWatermarkLag + `') AT TIME ZONE 'UTC')::date`

// UsageStore 实现 usage.Store。
type UsageStore struct{ s *Store }

// Usage 以 usage.Store 接口返回用量用例（装配以接口持有，不导入本包）。
func (s *Store) Usage() usage.Store { return &UsageStore{s: s} }

// UsageStore 返回具体实现（CLI 与测试使用）。
func (s *Store) UsageStore() *UsageStore { return &UsageStore{s: s} }

// rawUsageSQL 从原始记录重算 [$1, $2) 内（$2 为 NULL 时不设上限）创建的已结算 hold 的按日用量，列与 usage_daily 相同。
// model_requests 按 created_at 分区：请求的 created_at 是请求开始时间，比 hold 早几毫秒，按 ±1 天裁剪分区。
const rawUsageSQL = `WITH h AS (
		SELECT q.hold_id, q.org_id, COALESCE(q.user_id, 0) AS user_id, COALESCE(q.key_id, '') AS key_id, q.source, q.ref, q.state,
			q.charged_micro, (q.created_at AT TIME ZONE 'UTC')::date AS day
		FROM quota_holds q
		WHERE q.state IN ('settled', 'charged_unknown') AND q.created_at >= $1::timestamptz
			AND ($2::timestamptz IS NULL OR q.created_at < $2::timestamptz))
	SELECT h.day, h.org_id, h.user_id, h.key_id,
		CASE WHEN h.source = 'model_request' THEN COALESCE(m.model, '')
			ELSE COALESCE(NULLIF(c.model, ''), regexp_replace(c.endpoint, '^/v1/', ''), '') END,
		CASE WHEN h.source = 'model_request' THEN 'model_api' ELSE 'runtime' END,
		count(*), COALESCE(sum(m.prompt_tokens), 0)::bigint, COALESCE(sum(m.completion_tokens), 0)::bigint,
		COALESCE(sum(h.charged_micro) FILTER (WHERE h.state = 'settled'), 0)::bigint,
		COALESCE(sum(h.charged_micro) FILTER (WHERE h.state = 'charged_unknown'), 0)::bigint
	FROM h
	LEFT JOIN model_requests m ON h.source = 'model_request' AND m.hold_id = h.hold_id AND m.id = h.ref
		AND m.created_at >= $1::timestamptz - interval '1 day'
		AND ($2::timestamptz IS NULL OR m.created_at < $2::timestamptz + interval '1 day')
	LEFT JOIN reservations r ON h.source = 'task_call' AND r.reservation_id = h.hold_id
	LEFT JOIN calls c ON c.task_id = r.task_id AND c.call_id = r.call_id
	GROUP BY 1, 2, 3, 4, 5, 6`

// snapshot 在专用连接上执行 fn：locked 时先取汇总的会话级 advisory lock（等其他 server 的汇总结束），然后开
// REPEATABLE READ 事务（快照在取得锁之后建立）。释放锁失败的连接直接关闭，不归还连接池。
func (u *UsageStore) snapshot(ctx context.Context, op string, locked bool, fn func(ctx context.Context, tx pgx.Tx) error) error {
	caller := ctx
	ctx, cancel, err := u.s.boundContext(ctx, usageOpTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	conn, err := u.s.pool.Acquire(ctx)
	if err != nil {
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	mayHold := false
	defer func() { releaseUsageConn(conn, mayHold) }()
	if locked {
		mayHold = true
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", usageLockKey); err != nil {
			return u.s.final(caller, ctx, op, "", nil, false, err)
		}
	}
	mode := pgx.ReadOnly
	if locked {
		mode = pgx.ReadWrite
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: mode})
	if err != nil {
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SELECT set_config('statement_timeout', $1, true), set_config('lock_timeout', $2, true)",
		durationSetting(usageStatementTimeout), durationSetting(u.s.opt.LockTimeout)); err != nil {
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	if err := fn(ctx, tx); err != nil {
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	if err := tx.Commit(ctx); err != nil {
		// 汇总可以重跑（幂等），提交结果未知时不必仲裁：按普通失败报告，下次汇总覆盖。
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	return nil
}

// releaseUsageConn 释放汇总锁并归还连接；可能仍持有锁的连接（释放失败）关闭而不归还。
func releaseUsageConn(conn *pgxpool.Conn, mayHold bool) {
	if !mayHold {
		conn.Release()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", usageLockKey); err != nil {
		_ = conn.Hijack().Close(ctx)
		return
	}
	conn.Release()
}

// Rollup 实现 usage.Store。
func (u *UsageStore) Rollup(ctx context.Context, from time.Time) (usage.RollupResult, error) {
	var res usage.RollupResult
	err := u.snapshot(ctx, "UsageRollup", true, func(ctx context.Context, tx pgx.Tx) error {
		r, err := rollupTx(ctx, tx, from)
		res = r
		return err
	})
	return res, err
}

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// rollupTx 删除并重算 usage_daily 中从起始日到今天的各天，然后移动 watermark。起始日是 watermark 与 from（非零时）
// 中较早的一个；从未汇总过时为最早的 hold 所在的一天（与 from 中较早的一个）。
func rollupTx(ctx context.Context, tx pgx.Tx, from time.Time) (usage.RollupResult, error) {
	var wm *time.Time
	if err := tx.QueryRow(ctx, `SELECT watermark FROM usage_rollup_state WHERE id FOR UPDATE`).Scan(&wm); err != nil {
		return usage.RollupResult{}, err
	}
	var low time.Time
	switch {
	case wm != nil && (from.IsZero() || wm.Before(utcDay(from))):
		low = utcDay(*wm)
	case !from.IsZero():
		low = utcDay(from)
	default:
		if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT min(created_at) FROM quota_holds), now())`).Scan(&low); err != nil {
			return usage.RollupResult{}, err
		}
		low = utcDay(low)
	}
	if wm == nil && !from.IsZero() { // 复审 N4：从未汇总过时，给出的起始日不能跳过更早的历史
		var first time.Time
		if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT min(created_at) FROM quota_holds), $1::timestamptz)`, low).Scan(&first); err != nil {
			return usage.RollupResult{}, err
		}
		if first = utcDay(first); first.Before(low) {
			low = first
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM usage_daily WHERE day >= $1::date`, low); err != nil {
		return usage.RollupResult{}, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO usage_daily (day, org_id, user_id, key_id, model, source, requests, prompt_tokens,
			completion_tokens, cost_micro, unknown_micro) `+rawUsageSQL, low, nil)
	if err != nil {
		return usage.RollupResult{}, err
	}
	var next time.Time
	if err := tx.QueryRow(ctx, usageOpenSQL).Scan(&next); err != nil {
		return usage.RollupResult{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE usage_rollup_state SET watermark = $1, last_run_at = now() WHERE id`, next); err != nil {
		return usage.RollupResult{}, err
	}
	return usage.RollupResult{From: low, Rows: tag.RowsAffected(), Watermark: utcDay(next)}, nil
}

// Status 实现 usage.Store。
func (u *UsageStore) Status(ctx context.Context) (usage.Status, error) {
	var st usage.Status
	err := u.s.read(ctx, "UsageStatus", func(ctx context.Context, q queryer) error {
		var wm, last *time.Time
		if err := q.QueryRow(ctx, `SELECT watermark, last_run_at FROM usage_rollup_state WHERE id`).Scan(&wm, &last); err != nil {
			return err
		}
		if wm != nil {
			st.Watermark = utcDay(*wm)
		}
		if last != nil {
			st.LastRunAt = last.UTC()
		}
		return nil
	})
	return st, err
}

// UserHasUsage 实现 usage.Store。
func (u *UsageStore) UserHasUsage(ctx context.Context, orgID, userID int64) (bool, error) {
	var ok bool
	err := u.s.read(ctx, "UserHasUsage", func(ctx context.Context, q queryer) error {
		return q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM usage_daily WHERE org_id = $1 AND user_id = $2)`, orgID, userID).Scan(&ok)
	})
	return ok, err
}

// usageDimSQL 是各维度的选择表达式（分组时）与不分组时的占位值；列顺序固定为 day、org、user、key、model、source。
var usageDimSQL = map[usage.Dim]struct{ on, off []string }{
	usage.DimDay:    {[]string{"to_char(u.day, 'YYYY-MM-DD')"}, []string{"''"}},
	usage.DimOrg:    {[]string{"u.org_id", "COALESCE(o.name, '')"}, []string{"0::bigint", "''"}},
	usage.DimUser:   {[]string{"u.user_id", "COALESCE(us.username, '')"}, []string{"0::bigint", "''"}},
	usage.DimKey:    {[]string{"u.key_id", "COALESCE(k.name, '')"}, []string{"''", "''"}},
	usage.DimModel:  {[]string{"u.model"}, []string{"''"}},
	usage.DimSource: {[]string{"u.source"}, []string{"''"}},
}

// Usage 实现 usage.Store。
func (u *UsageStore) Usage(ctx context.Context, q usage.Query) ([]usage.Group, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	grouped := map[usage.Dim]bool{}
	for _, d := range q.GroupBy {
		grouped[d] = true
	}
	var sel, group []string
	for _, d := range usage.AllDims {
		e := usageDimSQL[d]
		if grouped[d] {
			sel, group = append(sel, e.on...), append(group, e.on...)
		} else {
			sel = append(sel, e.off...)
		}
	}
	sql := `SELECT ` + strings.Join(sel, ", ") + `, COALESCE(sum(u.requests), 0)::bigint, COALESCE(sum(u.prompt_tokens), 0)::bigint,
			COALESCE(sum(u.completion_tokens), 0)::bigint, COALESCE(sum(u.cost_micro), 0)::bigint, COALESCE(sum(u.unknown_micro), 0)::bigint
		FROM usage_daily u
		LEFT JOIN orgs o ON o.id = u.org_id
		LEFT JOIN users us ON us.id = u.user_id
		LEFT JOIN api_keys k ON k.id = u.key_id
		WHERE u.day >= $1::date AND u.day <= $2::date AND ($3::bigint = 0 OR u.org_id = $3) AND ($4::bigint = 0 OR u.user_id = $4)
			AND ($5::text = '' OR u.key_id = $5)`
	if len(group) > 0 {
		sql += ` GROUP BY ` + strings.Join(group, ", ") + ` HAVING count(*) > 0 ORDER BY ` + strings.Join(group, ", ")
	} else {
		sql += ` HAVING count(*) > 0`
	}
	out := []usage.Group{}
	err := u.read(ctx, "Usage", func(ctx context.Context, db queryer) error {
		rows, err := db.Query(ctx, sql, q.From, q.To, q.OrgID, q.UserID, q.KeyID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var g usage.Group
			var src string
			if err := rows.Scan(&g.Day, &g.OrgID, &g.OrgName, &g.UserID, &g.Username, &g.KeyID, &g.KeyName, &g.Model, &src,
				&g.Requests, &g.PromptTokens, &g.CompletionTokens, &g.CostMicro, &g.UnknownMicro); err != nil {
				return err
			}
			g.Source = usage.Source(src)
			out = append(out, g)
		}
		return rows.Err()
	})
	return out, err
}

// read 是期限较长的只读查询（一个月的用量）。
func (u *UsageStore) read(ctx context.Context, op string, fn func(ctx context.Context, q queryer) error) error {
	caller := ctx
	ctx, cancel, err := u.s.boundContext(ctx, usageOpTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if err := fn(ctx, u.s.pool); err != nil {
		return u.s.final(caller, ctx, op, "", nil, false, err)
	}
	return nil
}

func scanUsageRows(rows pgx.Rows) ([]usage.Row, error) {
	defer rows.Close()
	var out []usage.Row
	for rows.Next() {
		var r usage.Row
		var day time.Time
		var src string
		if err := rows.Scan(&day, &r.OrgID, &r.UserID, &r.KeyID, &r.Model, &src, &r.Requests, &r.PromptTokens, &r.CompletionTokens,
			&r.CostMicro, &r.UnknownMicro); err != nil {
			return nil, err
		}
		r.Day, r.Source = day.Format("2006-01-02"), usage.Source(src)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Reconcile 实现 usage.Store：同一快照中（rollupFirst 时先汇总）重算该月、读 usage_daily 与 quota_budgets 并比较。
// 不先汇总时（复审 M3）只比较 watermark 之前的日子（它们已是终值；之后的日子要等下次汇总），预算总是比较。
func (u *UsageStore) Reconcile(ctx context.Context, month time.Time, rollupFirst bool) (usage.Report, error) {
	from := usage.Month(month)
	to := from.AddDate(0, 1, 0)
	var raw, daily []usage.Row
	var budgets []usage.BudgetRow
	orgs, users := map[int64]string{}, map[int64]string{}
	cutoff := "" // 空：比较全部日子
	err := u.snapshot(ctx, "UsageReconcile", rollupFirst, func(ctx context.Context, tx pgx.Tx) error {
		if rollupFirst {
			if _, err := rollupTx(ctx, tx, time.Time{}); err != nil {
				return fmt.Errorf("汇总: %w", err)
			}
		} else {
			var wm *time.Time
			if err := tx.QueryRow(ctx, `SELECT watermark FROM usage_rollup_state WHERE id`).Scan(&wm); err != nil {
				return err
			}
			cutoff = "0000-01-01" // 从未汇总过：没有终值的日子
			if wm != nil {
				cutoff = utcDay(*wm).Format("2006-01-02")
			}
		}
		rows, err := tx.Query(ctx, rawUsageSQL, from, to)
		if err != nil {
			return err
		}
		if raw, err = scanUsageRows(rows); err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT day, org_id, user_id, key_id, model, source, requests, prompt_tokens, completion_tokens,
				cost_micro, unknown_micro FROM usage_daily WHERE day >= $1::date AND day < $2::date`, from, to)
		if err != nil {
			return err
		}
		if daily, err = scanUsageRows(rows); err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT scope_kind, scope_id, spent_micro, unknown_micro FROM quota_budgets WHERE period = $1::date`, from)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var b usage.BudgetRow
			if err := rows.Scan(&b.Level, &b.ScopeID, &b.SpentMicro, &b.UnknownMicro); err != nil {
				return err
			}
			budgets = append(budgets, b)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return usageNames(ctx, tx, orgs, users)
	})
	if err != nil {
		return usage.Report{}, err
	}
	rep := usage.CompareBefore(from, raw, daily, budgets, cutoff)
	rep.OrgNames, rep.Usernames = orgs, users
	return rep, nil
}

// usageNames 读取 org 名称与用户名（只用于打印对账报告）。
func usageNames(ctx context.Context, tx pgx.Tx, orgs, users map[int64]string) error {
	for _, q := range []struct {
		sql string
		m   map[int64]string
	}{{`SELECT id, name FROM orgs`, orgs}, {`SELECT id, username FROM users`, users}} {
		rows, err := tx.Query(ctx, q.sql)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				rows.Close()
				return err
			}
			q.m[id] = name
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	return nil
}
