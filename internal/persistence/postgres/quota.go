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
	"sync"
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
		return rejectf(persistence.CodeInsufficientQuota, "%s（%s %s：可用 %d / 上限 %d，本次需要 %d）", persistence.QuotaExhaustedMessage(string(ie.Level)),
			ie.Level, ie.ID, ie.Available, ie.Limit, ie.Amount)
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
	if err := q.s.ensureBudgetRows(ctx, s.Targets()); err != nil { // 复审 F6
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
	// 复审 F6：每月最后一天预先建好下个月有上限的层级的行。
	if _, err := q.precreateNextMonth(ctx, nil); err != nil {
		return 0, err
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

// periodArg 把 period 转为 SQL 参数：零值为 nil，即数据库时间所在的月份（与 hold 相同，见 periodNow）。
func periodArg(period time.Time) *time.Time {
	if period.IsZero() {
		return nil
	}
	p := quota.Period(period)
	return &p
}

// GetBudget 实现 quota.Store；period 为零值时取数据库时间所在的月份。
func (q *QuotaStore) GetBudget(ctx context.Context, t quota.Target, period time.Time) (quota.Budget, error) {
	p := periodArg(period)
	var b quota.Budget
	err := q.s.read(ctx, "GetBudget", func(ctx context.Context, db queryer) error {
		b = quota.Budget{Level: t.Level, ID: t.ID}
		var cur time.Time
		err := db.QueryRow(ctx, `WITH p AS (SELECT COALESCE($3::date, `+periodNow+`) AS period)
			SELECT p.period, CASE WHEN b.scope_kind IS NOT NULL THEN b.limit_micro ELSE (SELECT e.limit_micro FROM quota_budgets e
					WHERE e.scope_kind = $1 AND e.scope_id = $2 AND e.period < p.period ORDER BY e.period DESC LIMIT 1) END,
				COALESCE(b.reserved_micro, 0), COALESCE(b.spent_micro, 0), COALESCE(b.unknown_micro, 0)
			FROM p LEFT JOIN quota_budgets b ON b.scope_kind = $1 AND b.scope_id = $2 AND b.period = p.period`,
			string(t.Level), t.ID, p).Scan(&cur, &b.LimitMicro, &b.ReservedMicro, &b.SpentMicro, &b.UnknownMicro)
		if err != nil {
			return err
		}
		b.Period = cur.UTC()
		return nil
	})
	return b, err
}

// PutBudget 实现 quota.Store（period 为零值时取数据库时间所在的月份）：锁住（缺失时创建）该月的行，新上限低于当月已用为
// *quota.BelowUsageError；否则改为新上限。之后的月份（懒创建时）沿用它；已存在的更晚月份的行不变（它们有各自的已用额）。
func (q *QuotaStore) PutBudget(ctx context.Context, t quota.Target, period time.Time, limit *int64) (quota.Budget, error) {
	if limit != nil && *limit < 0 {
		return quota.Budget{}, invalidf("预算上限不能为负")
	}
	p := periodArg(period)
	var out quota.Budget
	err := q.s.run(ctx, "PutBudget", string(t.Level)+":"+t.ID, func(ctx context.Context, tx pgx.Tx) error {
		bs, err := lockQuotaBudgetsTx(ctx, tx, []quota.Target{t}, p)
		if err != nil {
			return err
		}
		if limit != nil && bs[0].Used() > *limit {
			return &quota.BelowUsageError{Used: bs[0].Used(), Limit: *limit}
		}
		if _, err := tx.Exec(ctx, `UPDATE quota_budgets SET limit_micro = $4 WHERE scope_kind = $1 AND scope_id = $2 AND period = $3`,
			string(t.Level), t.ID, bs[0].Period, limit); err != nil {
			return err
		}
		out = bs[0]
		out.LimitMicro = limit
		return nil
	})
	return out, err
}

// OrgSandboxLimit 返回任务所属的 org 与该 org 的 max_sandboxes（nil 为不限）。只读两行（主键查找）：没有上限的 org
// 在准入时不做任何计数（复审 F3）。任务不存在为 ErrNotFound。
func (s *Store) OrgSandboxLimit(ctx context.Context, taskID string) (orgID int64, limit *int64, err error) {
	err = s.read(ctx, "OrgSandboxLimit", func(ctx context.Context, db queryer) error {
		limit = nil
		err := db.QueryRow(ctx, `SELECT t.org_id, l.max_sandboxes FROM tasks t
			LEFT JOIN limits l ON l.scope_kind = 'org' AND l.scope_id = t.org_id::text WHERE t.task_id = $1`, taskID).Scan(&orgID, &limit)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFoundf("任务 %s", taskID)
		}
		return err
	})
	return orgID, limit, err
}

// OrgLiveSandboxes 返回 org 当前的存活沙箱数：attempt 属于该 org 的任务、或属于该 org 的会话的未停止环境（exec 环境
// 不计），加上 pending（本 server 已准入、可能尚未建出环境的任务）中尚无存活环境的（各计一个）。self 报告 taskID 自己
// （或其会话）已有存活环境——它不会新增沙箱。走 0013 的未停止环境部分索引（复审 F3）。
func (s *Store) OrgLiveSandboxes(ctx context.Context, orgID int64, taskID string, pending []string) (live int64, self bool, err error) {
	err = s.read(ctx, "OrgLiveSandboxes", func(ctx context.Context, db queryer) error {
		return db.QueryRow(ctx, `WITH le AS (SELECT e.attempt_id, e.session_id FROM environments e WHERE e.stopped_at IS NULL AND e.kind <> 'exec'),
			lt AS (SELECT a.task_id, le.session_id FROM le LEFT JOIN attempts a ON a.attempt_id = le.attempt_id)
			SELECT
				(SELECT count(*) FROM lt LEFT JOIN tasks t ON t.task_id = lt.task_id LEFT JOIN sessions s ON s.session_id = lt.session_id
					WHERE COALESCE(s.org_id, t.org_id) = $1)
				+ (SELECT count(*) FROM tasks p WHERE p.task_id = ANY($2::text[]) AND p.org_id = $1 AND NOT EXISTS (SELECT 1 FROM lt
					WHERE lt.task_id = p.task_id OR (p.session_id IS NOT NULL AND lt.session_id = p.session_id))),
				EXISTS (SELECT 1 FROM tasks t, lt WHERE t.task_id = $3 AND (lt.task_id = t.task_id OR (t.session_id IS NOT NULL AND lt.session_id = t.session_id)))`,
			orgID, pending, taskID).Scan(&live, &self)
	})
	return live, self, err
}

// convertTaskHoldsTx 是账本转换（convertLedgerTx；ids 为 nil 时全部）的月度预算部分：reservation 仍为 held 的运行时调用 hold 按全额
// 转为 charged_unknown，各层级 reserved -= 金额、unknown += 金额（不变量 G1 保持）。须在 reservations 转换之前执行。
func convertTaskHoldsTx(ctx context.Context, tx pgx.Tx, ids []string) error {
	if _, err := tx.Exec(ctx, `WITH h AS (SELECT q.org_id, q.user_id, q.key_id, q.period, q.amount_micro FROM quota_holds q
			JOIN reservations r ON r.reservation_id = q.hold_id
			WHERE q.state = 'held' AND q.source = 'task_call' AND r.state = 'held' AND ($1::text[] IS NULL OR r.reservation_id = ANY($1))),
		lv AS (SELECT 'org' AS k, org_id::text AS i, period, amount_micro AS a FROM h
			UNION ALL SELECT 'user', org_id::text || '/' || user_id::text, period, amount_micro FROM h WHERE user_id IS NOT NULL
			UNION ALL SELECT 'key', key_id, period, amount_micro FROM h WHERE key_id IS NOT NULL),
		agg AS (SELECT k, i, period, sum(a) AS total FROM lv GROUP BY k, i, period)
		UPDATE quota_budgets b SET reserved_micro = b.reserved_micro - agg.total, unknown_micro = b.unknown_micro + agg.total
		FROM agg WHERE b.scope_kind = agg.k AND b.scope_id = agg.i AND b.period = agg.period`, ids); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE quota_holds q SET state = 'charged_unknown', charged_micro = q.amount_micro
		FROM reservations r WHERE r.reservation_id = q.hold_id AND r.state = 'held' AND q.state = 'held' AND q.source = 'task_call'
			AND ($1::text[] IS NULL OR r.reservation_id = ANY($1))`, ids)
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

// ---- 月初的预算行（复审 F6）----

// quotaCacheMax 是进程内缓存的上限；超出时清空（只是少做一次独立的短事务）。
const quotaCacheMax = 50_000

// quotaCache 记住本进程已确认存在的预算行（按月份）与任务的计费主体（org、owner、key 在任务创建后不变）。
type quotaCache struct {
	mu     sync.Mutex
	rows   map[string]struct{} // "<period>|<kind>|<id>"
	scopes map[string]quota.Scope
}

func newQuotaCache() *quotaCache {
	return &quotaCache{rows: map[string]struct{}{}, scopes: map[string]quota.Scope{}}
}

func rowKey(period time.Time, t quota.Target) string {
	return period.Format("2006-01") + "|" + string(t.Level) + "|" + t.ID
}

// ensureBudgetRows 在独立的短事务中创建 targets 在数据库当前月份缺失的预算行（INSERT … ON CONFLICT DO NOTHING，
// 已提交），使随后加锁的预留事务只锁已存在的行：月初大量并发的第一次预留不再在加锁事务里互相等待插入。本进程已确认的
// 行（按应用时钟的月份）直接跳过；月份交界处应用时钟与数据库时钟不一致时只是多做一次，或回退到事务内的懒创建。
func (s *Store) ensureBudgetRows(ctx context.Context, targets []quota.Target) error {
	guess := quota.Period(time.Now())
	s.qc.mu.Lock()
	missing := false
	for _, t := range targets {
		if _, ok := s.qc.rows[rowKey(guess, t)]; !ok {
			missing = true
			break
		}
	}
	s.qc.mu.Unlock()
	if !missing {
		return nil
	}
	kinds, ids := targetArrays(targets)
	var period time.Time
	err := s.run(ctx, "QuotaEnsureRows", string(targets[0].Level)+":"+targets[0].ID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `WITH p AS (SELECT `+periodNow+` AS period),
			ins AS (INSERT INTO quota_budgets (scope_kind, scope_id, period, limit_micro)
				SELECT x.k, x.i, p.period, (SELECT b.limit_micro FROM quota_budgets b
					WHERE b.scope_kind = x.k AND b.scope_id = x.i AND b.period < p.period ORDER BY b.period DESC LIMIT 1)
				FROM p, unnest($1::text[], $2::text[]) AS x(k, i) ORDER BY CASE x.k WHEN 'org' THEN 0 WHEN 'user' THEN 1 ELSE 2 END
				ON CONFLICT DO NOTHING)
			SELECT period FROM p`, kinds, ids).Scan(&period)
	})
	if err != nil {
		return err
	}
	period = period.UTC()
	s.qc.mu.Lock()
	if len(s.qc.rows) > quotaCacheMax {
		clear(s.qc.rows)
	}
	for _, t := range targets {
		s.qc.rows[rowKey(period, t)] = struct{}{}
	}
	s.qc.mu.Unlock()
	return nil
}

// taskQuotaScope 返回任务的计费主体（缓存；任务不存在时 ok 为 false，由调用方的事务报告）。
func (s *Store) taskQuotaScope(ctx context.Context, taskID string) (quota.Scope, bool) {
	s.qc.mu.Lock()
	sc, ok := s.qc.scopes[taskID]
	s.qc.mu.Unlock()
	if ok {
		return sc, true
	}
	err := s.read(ctx, "TaskQuotaScope", func(ctx context.Context, q queryer) error {
		var user *int64
		err := q.QueryRow(ctx, "SELECT org_id, owner_user_id, COALESCE(key_id, '') FROM tasks WHERE task_id = $1", taskID).
			Scan(&sc.OrgID, &user, &sc.KeyID)
		if user != nil {
			sc.UserID = *user
		}
		return err
	})
	if err != nil {
		return quota.Scope{}, false
	}
	s.qc.mu.Lock()
	if len(s.qc.scopes) > quotaCacheMax {
		clear(s.qc.scopes)
	}
	s.qc.scopes[taskID] = sc
	s.qc.mu.Unlock()
	return sc, true
}

// precreateNextMonth 在每月最后一天（数据库时间，asOf 为 nil 时取 now()）为本月有上限的层级预先建好下个月的行（沿用
// 本月的上限），使零点之后的第一批预留不必创建行。
func (q *QuotaStore) precreateNextMonth(ctx context.Context, asOf *time.Time) (int, error) {
	var n int
	err := q.s.run(ctx, "QuotaPrecreate", "next-month", func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `WITH t AS (SELECT COALESCE($1::timestamptz, now()) AT TIME ZONE 'UTC' AS ts),
				p AS (SELECT date_trunc('month', ts)::date AS cur, (date_trunc('month', ts) + interval '1 month')::date AS next, ts FROM t)
			INSERT INTO quota_budgets (scope_kind, scope_id, period, limit_micro)
			SELECT b.scope_kind, b.scope_id, p.next, b.limit_micro FROM quota_budgets b, p
			WHERE b.period = p.cur AND b.limit_micro IS NOT NULL AND p.ts >= p.next - interval '1 day'
			ORDER BY b.scope_kind, b.scope_id
			ON CONFLICT DO NOTHING`, asOf)
		n = int(tag.RowsAffected())
		return err
	})
	return n, err
}
