package postgres

// 本文件是月度预算与限额（模型网关设计 2026-10-11 §4、§6、§8；G2）的持久化：quota.Store（Reserve / Settle / Release、
// Sweep、限额与预算的读写），以及供 ReserveTry / SettleTry / ConvertLedger 在任务账本事务内复用的 hold 助手。
//
// 锁顺序：quota_budgets（org → user → key，单条 ORDER BY … FOR UPDATE）→ quota_holds。运行时调用在任务账本的锁
// （budgets → subrun_budgets → calls → reservations）之后取这两类锁；模型 API 与 sweep 只取这两类锁，因此不成环。
// 预算行懒创建：某层级某月第一次被用到时插入，limit_micro 沿用该层级最近一个更早月份的值（NULL = 不限）。
// 周期取数据库时间（UTC 月的第一天），同一事务内一致。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
)

// periodNow 是数据库时间所在的预算周期（UTC 月的第一天）；now() 是事务开始时间，同一事务内各语句一致。
const periodNow = `(date_trunc('month', now() AT TIME ZONE 'UTC'))::date`

// levelOrder 是层级的锁顺序表达式。
const levelOrder = `CASE scope_kind WHEN 'org' THEN 0 WHEN 'user' THEN 1 ELSE 2 END`

// QuotaStore 实现 quota.Store。
type QuotaStore struct {
	s     *Store
	opOrg atomic.Int64 // 保留 org operator 的 ID（读到后缓存；该 org 不会被删除或改名）
}

var _ quota.Store = (*QuotaStore)(nil)

// Quota 以 quota.Store 接口返回预算与限额用例（装配以接口持有，不导入本包）。
func (s *Store) Quota() quota.Store { return &QuotaStore{s: s} }

// QuotaStore 返回具体实现（CLI 与测试使用）。
func (s *Store) QuotaStore() *QuotaStore { return &QuotaStore{s: s} }

func targetArrays(ts []quota.Target) (kinds, ids []string) {
	for _, t := range ts {
		kinds, ids = append(kinds, string(t.Level)), append(ids, t.ID)
	}
	return kinds, ids
}

// lockQuotaBudgetsTx 按锁顺序锁住（缺失时先创建）targets 在 period（nil 为数据库当前周期）的预算行，按 targets 的顺序返回。
func lockQuotaBudgetsTx(ctx context.Context, tx pgx.Tx, targets []quota.Target, period *time.Time) ([]quota.Budget, error) {
	kinds, ids := targetArrays(targets)
	sel := `SELECT scope_kind, scope_id, period, limit_micro, reserved_micro, spent_micro, unknown_micro FROM quota_budgets
		WHERE period = COALESCE($1::date, ` + periodNow + `) AND (scope_kind, scope_id) IN (SELECT * FROM unnest($2::text[], $3::text[]))
		ORDER BY ` + levelOrder + ` FOR UPDATE`
	scan := func() (map[quota.Target]quota.Budget, error) {
		rows, err := tx.Query(ctx, sel, period, kinds, ids)
		if err != nil {
			return nil, err
		}
		got, err := pgx.CollectRows(rows, scanBudget)
		if err != nil {
			return nil, err
		}
		m := make(map[quota.Target]quota.Budget, len(got))
		for _, b := range got {
			m[quota.Target{Level: b.Level, ID: b.ID}] = b
		}
		return m, nil
	}
	m, err := scan()
	if err != nil {
		return nil, err
	}
	if len(m) < len(targets) {
		if _, err := tx.Exec(ctx, `INSERT INTO quota_budgets (scope_kind, scope_id, period, limit_micro)
			SELECT x.k, x.i, COALESCE($1::date, `+periodNow+`), (SELECT b.limit_micro FROM quota_budgets b
				WHERE b.scope_kind = x.k AND b.scope_id = x.i AND b.period < COALESCE($1::date, `+periodNow+`)
				ORDER BY b.period DESC LIMIT 1)
			FROM unnest($2::text[], $3::text[]) AS x(k, i) ORDER BY CASE x.k WHEN 'org' THEN 0 WHEN 'user' THEN 1 ELSE 2 END
			ON CONFLICT DO NOTHING`, period, kinds, ids); err != nil {
			return nil, err
		}
		if m, err = scan(); err != nil {
			return nil, err
		}
	}
	out := make([]quota.Budget, len(targets))
	for i, t := range targets {
		b, ok := m[t]
		if !ok {
			return nil, fmt.Errorf("postgres: 预算行 %s:%s 缺失", t.Level, t.ID)
		}
		out[i] = b
	}
	return out, nil
}

func scanBudget(r pgx.CollectableRow) (quota.Budget, error) {
	var b quota.Budget
	var kind string
	err := r.Scan(&kind, &b.ID, &b.Period, &b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro)
	b.Level = quota.Level(kind)
	b.Period = b.Period.UTC()
	return b, err
}

// holdRequest 是在事务内创建 hold 的输入。Deadline 为 nil 时由 deadlineSQL（一条 SQL 表达式）给出。
type holdRequest struct {
	ID       string
	Scope    quota.Scope // OrgID 已解析（≠ 0）
	Source   quota.Source
	Ref      string
	Amount   int64
	Deadline time.Time
}

// reserveHoldTx 在当前周期为 h 的每个层级预留 h.Amount 并插入 hold（held）；任一层级不足返回 *quota.InsufficientError，
// 不做任何改动（调用方回滚事务）。hold_id 已存在时（提交结果未知后的重跑）不重复预留，返回 nil。
func reserveHoldTx(ctx context.Context, tx pgx.Tx, h holdRequest) error {
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM quota_holds WHERE hold_id = $1)", h.ID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	targets := h.Scope.Targets()
	budgets, err := lockQuotaBudgetsTx(ctx, tx, targets, nil)
	if err != nil {
		return err
	}
	for _, b := range budgets {
		if b.LimitMicro != nil && b.Used()+h.Amount > *b.LimitMicro {
			return &quota.InsufficientError{Level: b.Level, ID: b.ID, Limit: *b.LimitMicro, Available: *b.LimitMicro - b.Used(),
				Amount: h.Amount}
		}
	}
	kinds, ids := targetArrays(targets)
	if h.Amount != 0 {
		if _, err := tx.Exec(ctx, `UPDATE quota_budgets SET reserved_micro = reserved_micro + $3
			WHERE period = `+periodNow+` AND (scope_kind, scope_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))`,
			kinds, ids, h.Amount); err != nil {
			return err
		}
	}
	var user *int64
	if h.Scope.UserID != 0 {
		user = &h.Scope.UserID
	}
	_, err = tx.Exec(ctx, `INSERT INTO quota_holds (hold_id, org_id, user_id, key_id, period, source, ref, amount_micro, state, deadline_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), `+periodNow+`, $5, $6, $7, 'held', $8)`,
		h.ID, h.Scope.OrgID, user, h.Scope.KeyID, string(h.Source), h.Ref, h.Amount, h.Deadline)
	return err
}

// settleHoldTx 把仍为 held 的 hold 转入 state（settled、released 或 charged_unknown），charged 记入对应桶：
// settled → spent += charged，charged_unknown → unknown += charged，released → 只扣 reserved。不是 held 时不改动
// （幂等），返回 false。hold 不存在为 ErrNotFound。锁顺序 quota_budgets → quota_holds。
func settleHoldTx(ctx context.Context, tx pgx.Tx, id, state string, charged int64) (bool, error) {
	var s quota.Scope
	var user *int64
	var key, cur string
	var period time.Time
	err := tx.QueryRow(ctx, `SELECT org_id, user_id, COALESCE(key_id, ''), period, state FROM quota_holds WHERE hold_id = $1`, id).
		Scan(&s.OrgID, &user, &key, &period, &cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, notFoundf("hold %s", id)
	}
	if err != nil {
		return false, err
	}
	if cur != quota.StateHeld {
		return false, nil
	}
	if user != nil {
		s.UserID = *user
	}
	s.KeyID = key
	targets := s.Targets()
	if _, err := lockQuotaBudgetsTx(ctx, tx, targets, &period); err != nil {
		return false, err
	}
	var amount int64
	if err := tx.QueryRow(ctx, "SELECT state, amount_micro FROM quota_holds WHERE hold_id = $1 FOR UPDATE", id).Scan(&cur, &amount); err != nil {
		return false, err
	}
	if cur != quota.StateHeld {
		return false, nil
	}
	var spent, unknown int64
	switch state {
	case quota.StateSettled:
		spent = charged
	case quota.StateChargedUnknown:
		unknown = charged
	case quota.StateReleased:
		charged = 0
	default:
		return false, invalidf("hold 的目标状态 %q 无效", state)
	}
	kinds, ids := targetArrays(targets)
	if _, err := tx.Exec(ctx, `UPDATE quota_budgets SET reserved_micro = reserved_micro - $4, spent_micro = spent_micro + $5,
			unknown_micro = unknown_micro + $6
		WHERE period = $3 AND (scope_kind, scope_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))`,
		kinds, ids, period, amount, spent, unknown); err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, "UPDATE quota_holds SET state = $2, charged_micro = $3 WHERE hold_id = $1", id, state, charged)
	return err == nil, err
}

// insufficientRejection 把预算不足转为 Gateway 的拒绝（insufficient_quota，消息说明层级）。
func insufficientRejection(err error) error {
	var ie *quota.InsufficientError
	if errors.As(err, &ie) {
		return rejectf(persistence.CodeInsufficientQuota, "%s 级月度预算不足（%s %s：可用 %d / 上限 %d，本次需要 %d）", ie.Level, ie.Level,
			ie.ID, ie.Available, ie.Limit, ie.Amount)
	}
	return err
}

func newHoldID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "qh-" + hex.EncodeToString(b), nil
}

// resolveScope 把运维的 OrgID 0 解析为保留 org operator。
func (q *QuotaStore) resolveScope(ctx context.Context, s quota.Scope) (quota.Scope, error) {
	if s.OrgID != 0 {
		return s, nil
	}
	id, err := q.OperatorOrg(ctx)
	s.OrgID = id
	return s, err
}

// OperatorOrg 返回保留 org operator 的 ID（实现 quota.Store）。
func (q *QuotaStore) OperatorOrg(ctx context.Context) (int64, error) {
	if id := q.opOrg.Load(); id != 0 {
		return id, nil
	}
	var id int64
	err := q.s.read(ctx, "OperatorOrg", func(ctx context.Context, db queryer) error {
		return db.QueryRow(ctx, "SELECT id FROM orgs WHERE name = 'operator'").Scan(&id)
	})
	if err != nil {
		return 0, err
	}
	q.opOrg.Store(id)
	return id, nil
}

// Reserve 实现 quota.Holder：一个事务内按 org → user → key 锁住本月预算行，全部够用才各加 reserved 并插入 hold。
func (q *QuotaStore) Reserve(ctx context.Context, s quota.Scope, source quota.Source, ref string, amount int64, deadline time.Time) (quota.HoldID, error) {
	if amount < 0 || ref == "" || (source != quota.SourceTaskCall && source != quota.SourceModelRequest) || deadline.IsZero() {
		return "", invalidf("Reserve 需要非负金额、ref、有效来源与期限")
	}
	s, err := q.resolveScope(ctx, s)
	if err != nil {
		return "", err
	}
	id, err := newHoldID()
	if err != nil {
		return "", err
	}
	err = q.s.run(ctx, "QuotaReserve", id, func(ctx context.Context, tx pgx.Tx) error {
		return reserveHoldTx(ctx, tx, holdRequest{ID: id, Scope: s, Source: source, Ref: ref, Amount: amount, Deadline: deadline})
	})
	if err != nil {
		return "", err
	}
	return quota.HoldID(id), nil
}

// Settle 实现 quota.Holder。unknown 时按 max(charged, 预留金额) 记入 unknown（保守）。
func (q *QuotaStore) Settle(ctx context.Context, id quota.HoldID, charged int64, unknown bool) error {
	if charged < 0 {
		return invalidf("Settle 的金额为负")
	}
	return q.s.run(ctx, "QuotaSettle", string(id), func(ctx context.Context, tx pgx.Tx) error {
		state := quota.StateSettled
		if unknown {
			state = quota.StateChargedUnknown
			var amount int64
			if err := tx.QueryRow(ctx, "SELECT amount_micro FROM quota_holds WHERE hold_id = $1", string(id)).Scan(&amount); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return notFoundf("hold %s", id)
				}
				return err
			}
			charged = max(charged, amount)
		}
		_, err := settleHoldTx(ctx, tx, string(id), state, charged)
		return err
	})
}

// Release 实现 quota.Holder。
func (q *QuotaStore) Release(ctx context.Context, id quota.HoldID) error {
	return q.s.run(ctx, "QuotaRelease", string(id), func(ctx context.Context, tx pgx.Tx) error {
		_, err := settleHoldTx(ctx, tx, string(id), quota.StateReleased, 0)
		return err
	})
}

// Sweep 实现 quota.Store：期限已过、仍为 held 的 hold 按全额转为 charged_unknown（与任务账本相同的保守规则），每个 hold
// 一个事务（锁顺序同 Settle）。运行时调用的 hold 若其 reservation 仍为 held，留给任务账本（随其结算或启动转换）。
func (q *QuotaStore) Sweep(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 500
	}
	var ids []string
	err := q.s.read(ctx, "QuotaSweepScan", func(ctx context.Context, db queryer) error {
		rows, err := db.Query(ctx, `SELECT h.hold_id FROM quota_holds h WHERE h.state = 'held' AND h.deadline_at < now()
			AND NOT (h.source = 'task_call' AND EXISTS (SELECT 1 FROM reservations r WHERE r.reservation_id = h.hold_id AND r.state = 'held'))
			ORDER BY h.deadline_at LIMIT $1`, limit)
		if err != nil {
			return err
		}
		ids, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		var done bool
		err := q.s.run(ctx, "QuotaSweep", id, func(ctx context.Context, tx pgx.Tx) error {
			done = false
			var amount int64
			var expired, taskHeld bool
			err := tx.QueryRow(ctx, `SELECT amount_micro, deadline_at < now(),
					source = 'task_call' AND EXISTS (SELECT 1 FROM reservations r WHERE r.reservation_id = hold_id AND r.state = 'held')
				FROM quota_holds WHERE hold_id = $1`, id).Scan(&amount, &expired, &taskHeld)
			if err != nil || !expired || taskHeld {
				return err
			}
			done, err = settleHoldTx(ctx, tx, id, quota.StateChargedUnknown, amount)
			return err
		})
		if err != nil {
			return n, err
		}
		if done {
			n++
		}
	}
	return n, nil
}

// GetLimits 实现 quota.Store。
func (q *QuotaStore) GetLimits(ctx context.Context, t quota.Target) (quota.Limits, error) {
	var l quota.Limits
	err := q.s.read(ctx, "GetLimits", func(ctx context.Context, db queryer) error {
		err := db.QueryRow(ctx, `SELECT rpm, tpm, max_sandboxes, max_streams FROM limits WHERE scope_kind = $1 AND scope_id = $2`,
			string(t.Level), t.ID).Scan(&l.RPM, &l.TPM, &l.MaxSandboxes, &l.MaxStreams)
		if errors.Is(err, pgx.ErrNoRows) {
			l = quota.Limits{}
			return nil
		}
		return err
	})
	return l, err
}

// PutLimits 实现 quota.Store：全部为空时删除该行。
func (q *QuotaStore) PutLimits(ctx context.Context, t quota.Target, l quota.Limits) error {
	for _, v := range []*int64{l.RPM, l.TPM, l.MaxSandboxes, l.MaxStreams} {
		if v != nil && (*v < 0 || *v > 1<<31-1) {
			return invalidf("限额须为 0–2147483647")
		}
	}
	return q.s.run(ctx, "PutLimits", string(t.Level)+":"+t.ID, func(ctx context.Context, tx pgx.Tx) error {
		if l.Empty() {
			_, err := tx.Exec(ctx, "DELETE FROM limits WHERE scope_kind = $1 AND scope_id = $2", string(t.Level), t.ID)
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO limits (scope_kind, scope_id, rpm, tpm, max_sandboxes, max_streams) VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (scope_kind, scope_id) DO UPDATE SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
				max_sandboxes = EXCLUDED.max_sandboxes, max_streams = EXCLUDED.max_streams`,
			string(t.Level), t.ID, l.RPM, l.TPM, l.MaxSandboxes, l.MaxStreams)
		return err
	})
}

// LimitsFor 实现 quota.Store：一条语句读取 s 各层级的限额。
func (q *QuotaStore) LimitsFor(ctx context.Context, s quota.Scope) (map[quota.Level]quota.Limits, error) {
	s, err := q.resolveScope(ctx, s)
	if err != nil {
		return nil, err
	}
	kinds, ids := targetArrays(s.Targets())
	out := map[quota.Level]quota.Limits{}
	err = q.s.read(ctx, "LimitsFor", func(ctx context.Context, db queryer) error {
		clear(out)
		rows, err := db.Query(ctx, `SELECT scope_kind, rpm, tpm, max_sandboxes, max_streams FROM limits
			WHERE (scope_kind, scope_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))`, kinds, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kind string
			var l quota.Limits
			if err := rows.Scan(&kind, &l.RPM, &l.TPM, &l.MaxSandboxes, &l.MaxStreams); err != nil {
				return err
			}
			out[quota.Level(kind)] = l
		}
		return rows.Err()
	})
	return out, err
}

// GetBudget 实现 quota.Store。
func (q *QuotaStore) GetBudget(ctx context.Context, t quota.Target, period time.Time) (quota.Budget, error) {
	period = quota.Period(period)
	b := quota.Budget{Level: t.Level, ID: t.ID, Period: period}
	err := q.s.read(ctx, "GetBudget", func(ctx context.Context, db queryer) error {
		rows, err := db.Query(ctx, `SELECT scope_kind, scope_id, period, limit_micro, reserved_micro, spent_micro, unknown_micro
			FROM quota_budgets WHERE scope_kind = $1 AND scope_id = $2 AND period = $3`, string(t.Level), t.ID, period)
		if err != nil {
			return err
		}
		got, err := pgx.CollectRows(rows, scanBudget)
		if err != nil {
			return err
		}
		if len(got) == 1 {
			b = got[0]
			return nil
		}
		b = quota.Budget{Level: t.Level, ID: t.ID, Period: period}
		err = db.QueryRow(ctx, `SELECT limit_micro FROM quota_budgets WHERE scope_kind = $1 AND scope_id = $2 AND period < $3
			ORDER BY period DESC LIMIT 1`, string(t.Level), t.ID, period).Scan(&b.LimitMicro)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	})
	return b, err
}

// PutBudget 实现 quota.Store：锁住（缺失时创建）period 的行，新上限低于当月已用为 *quota.BelowUsageError；
// 否则改为新上限。之后的月份（懒创建时）沿用它；已存在的更晚月份的行不变（它们有各自的已用额）。
func (q *QuotaStore) PutBudget(ctx context.Context, t quota.Target, period time.Time, limit *int64) (quota.Budget, error) {
	if limit != nil && *limit < 0 {
		return quota.Budget{}, invalidf("预算上限不能为负")
	}
	period = quota.Period(period)
	var out quota.Budget
	err := q.s.run(ctx, "PutBudget", string(t.Level)+":"+t.ID, func(ctx context.Context, tx pgx.Tx) error {
		bs, err := lockQuotaBudgetsTx(ctx, tx, []quota.Target{t}, &period)
		if err != nil {
			return err
		}
		if limit != nil && bs[0].Used() > *limit {
			return &quota.BelowUsageError{Used: bs[0].Used(), Limit: *limit}
		}
		if _, err := tx.Exec(ctx, `UPDATE quota_budgets SET limit_micro = $4 WHERE scope_kind = $1 AND scope_id = $2 AND period = $3`,
			string(t.Level), t.ID, period, limit); err != nil {
			return err
		}
		out = bs[0]
		out.LimitMicro = limit
		return nil
	})
	return out, err
}

// OrgSandboxes 返回 org 的 max_sandboxes（nil 为不限）与当前存活环境数：环境的 attempt 属于该 org 的任务，或环境属于
// 该 org 的会话；pending 是本 server 已准入、可能尚未建出环境的任务，其中尚无存活环境的也计入（各计一个）。
// 不限、或任务自己（或其会话）已有存活环境（不会新增沙箱）时 limit 为 nil，不计数。
func (s *Store) OrgSandboxes(ctx context.Context, taskID string, pending []string) (orgID int64, limit *int64, live int64, err error) {
	err = s.read(ctx, "OrgSandboxes", func(ctx context.Context, db queryer) error {
		limit = nil
		if err := db.QueryRow(ctx, `SELECT t.org_id, CASE WHEN EXISTS (SELECT 1 FROM environments e LEFT JOIN attempts a ON a.attempt_id = e.attempt_id
					WHERE e.stopped_at IS NULL AND e.kind <> 'exec' AND (a.task_id = t.task_id OR (t.session_id IS NOT NULL AND e.session_id = t.session_id)))
				THEN NULL ELSE l.max_sandboxes END
			FROM tasks t LEFT JOIN limits l ON l.scope_kind = 'org' AND l.scope_id = t.org_id::text WHERE t.task_id = $1`, taskID).
			Scan(&orgID, &limit); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFoundf("任务 %s", taskID)
			}
			return err
		}
		if limit == nil {
			return nil
		}
		return db.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM environments e
					LEFT JOIN attempts a ON a.attempt_id = e.attempt_id LEFT JOIN tasks t ON t.task_id = a.task_id
					LEFT JOIN sessions s ON s.session_id = e.session_id
					WHERE e.stopped_at IS NULL AND e.kind <> 'exec' AND COALESCE(s.org_id, t.org_id) = $1)
				+ (SELECT count(*) FROM tasks p WHERE p.task_id = ANY($2::text[]) AND p.org_id = $1 AND NOT EXISTS (
					SELECT 1 FROM environments e LEFT JOIN attempts a ON a.attempt_id = e.attempt_id
					WHERE e.stopped_at IS NULL AND e.kind <> 'exec' AND (a.task_id = p.task_id OR (p.session_id IS NOT NULL AND e.session_id = p.session_id))))`,
			orgID, pending).Scan(&live)
	})
	return orgID, limit, live, err
}

// convertTaskHoldsTx 是启动账本转换（ConvertLedger）的月度预算部分：reservation 仍为 held 的运行时调用 hold 按全额
// 转为 charged_unknown，各层级 reserved -= 金额、unknown += 金额（不变量 G1 保持）。须在 reservations 转换之前执行。
func convertTaskHoldsTx(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `WITH h AS (SELECT q.org_id, q.user_id, q.key_id, q.period, q.amount_micro FROM quota_holds q
			JOIN reservations r ON r.reservation_id = q.hold_id
			WHERE q.state = 'held' AND q.source = 'task_call' AND r.state = 'held'),
		lv AS (SELECT 'org' AS k, org_id::text AS i, period, amount_micro AS a FROM h
			UNION ALL SELECT 'user', user_id::text, period, amount_micro FROM h WHERE user_id IS NOT NULL
			UNION ALL SELECT 'key', key_id, period, amount_micro FROM h WHERE key_id IS NOT NULL),
		agg AS (SELECT k, i, period, sum(a) AS total FROM lv GROUP BY k, i, period)
		UPDATE quota_budgets b SET reserved_micro = b.reserved_micro - agg.total, unknown_micro = b.unknown_micro + agg.total
		FROM agg WHERE b.scope_kind = agg.k AND b.scope_id = agg.i AND b.period = agg.period`); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE quota_holds q SET state = 'charged_unknown', charged_micro = q.amount_micro
		FROM reservations r WHERE r.reservation_id = q.hold_id AND r.state = 'held' AND q.state = 'held' AND q.source = 'task_call'`)
	return err
}

// quotaScopeOfTask 读取任务的计费主体（org、owner、key）；在 ReserveTry 的事务中使用（tasks 行已共享锁住）。
func quotaScopeOfTask(ctx context.Context, tx pgx.Tx, taskID string) (quota.Scope, error) {
	var s quota.Scope
	var user *int64
	err := tx.QueryRow(ctx, "SELECT org_id, owner_user_id, COALESCE(key_id, '') FROM tasks WHERE task_id = $1", taskID).Scan(&s.OrgID, &user, &s.KeyID)
	if user != nil {
		s.UserID = *user
	}
	return s, err
}

// taskCallRef 是运行时调用 hold 的 ref：task/call#try。
func taskCallRef(taskID, callID string, tryNo int) string {
	return taskID + "/" + callID + "#" + strconv.Itoa(tryNo)
}
