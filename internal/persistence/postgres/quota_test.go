package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// undo0013 把库还原为只应用了 0012 的形状（undo0012 先执行它）。
const undo0013 = `DROP TABLE quota_holds, quota_budgets; DELETE FROM schema_migrations WHERE version >= 13;
`

// quotaViolations 返回 G1、G2 的违反（单条语句，一致快照）。
func quotaViolations(t *testing.T, s *Store) []string {
	t.Helper()
	vs, err := s.DBViolations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, v := range vs {
		if v.ID == "G1" || v.ID == "G2" {
			out = append(out, v.ID+" "+v.Detail)
		}
	}
	return out
}

// checkQuota 断言 G1、G2 与运行时调用的 hold 跟随其 reservation：同金额、同状态，charged 与 try 的结算一致
// （settled = 实际费用，charged_unknown = 金额，released = 0）。checkI3 在 quota 表存在时调用它。
func checkQuota(t *testing.T, s *Store) {
	t.Helper()
	if vs := quotaViolations(t, s); len(vs) > 0 {
		t.Fatalf("月度预算不变量违反：%v", vs)
	}
	var bad *string
	if err := s.pool.QueryRow(context.Background(), `SELECT string_agg(format('hold %s：%s/%s，金额 %s/%s，charged %s/%s', q.hold_id,
			q.state, r.state, q.amount_micro, r.amount, q.charged_micro, c.cost_micro), '; ')
		FROM quota_holds q JOIN reservations r ON r.reservation_id = q.hold_id JOIN call_tries c ON c.reservation_id = r.reservation_id
		WHERE q.source = 'task_call' AND (q.state <> r.state OR q.amount_micro <> r.amount OR q.charged_micro <> CASE r.state
			WHEN 'settled' THEN c.cost_micro WHEN 'charged_unknown' THEN r.amount ELSE 0 END)`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != nil {
		t.Fatalf("hold 与 reservation 不一致：%s", *bad)
	}
}

func quotaTablesExist(t *testing.T, s *Store) bool {
	t.Helper()
	var ok bool
	if err := s.pool.QueryRow(context.Background(), "SELECT to_regclass('quota_holds') IS NOT NULL").Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

type quotaWorld struct {
	s     *Store
	q     *QuotaStore
	org   int64
	user  int64
	key   string
	other int64 // 同 org 的另一个用户
}

// newQuotaWorld 建 org acme（alice 为 owner、bob 为 member）与 alice 的一个 key。
func newQuotaWorld(t *testing.T) quotaWorld {
	t.Helper()
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	o, err := ten.CreateOrg(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	alice, err := s.CreateUser(ctx, "alice", "alice", "h")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "bob", "bob", "h")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ten.AddMember(ctx, o.ID, "alice", tenant.Owner); err != nil {
		t.Fatal(err)
	}
	if _, err := ten.AddMember(ctx, o.ID, "bob", tenant.Member); err != nil {
		t.Fatal(err)
	}
	g := mustKey(t, s, alice.ID, tenant.AllScopes)
	return quotaWorld{s: s, q: s.QuotaStore(), org: o.ID, user: alice.ID, key: g.ID, other: bob.ID}
}

func (w quotaWorld) scope() quota.Scope { return quota.Scope{OrgID: w.org, UserID: w.user, KeyID: w.key} }

func (w quotaWorld) target(l quota.Level) quota.Target {
	switch l {
	case quota.LevelOrg:
		return quota.Target{Level: l, ID: strconv.FormatInt(w.org, 10)}
	case quota.LevelUser:
		return quota.Target{Level: l, ID: strconv.FormatInt(w.user, 10)}
	}
	return quota.Target{Level: l, ID: w.key}
}

func i64(v int64) *int64 { return &v }

func (w quotaWorld) setBudget(t *testing.T, l quota.Level, limit *int64) {
	t.Helper()
	if _, err := w.q.PutBudget(context.Background(), w.target(l), time.Now(), limit); err != nil {
		t.Fatal(err)
	}
}

func (w quotaWorld) budget(t *testing.T, l quota.Level) quota.Budget {
	t.Helper()
	b, err := w.q.GetBudget(context.Background(), w.target(l), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func expectUsage(t *testing.T, b quota.Budget, reserved, spent, unknown int64) {
	t.Helper()
	if b.ReservedMicro != reserved || b.SpentMicro != spent || b.UnknownMicro != unknown {
		t.Fatalf("%s:%s 应为 reserved %d spent %d unknown %d，得到 %+v", b.Level, b.ID, reserved, spent, unknown, b)
	}
}

func expectInsufficient(t *testing.T, err error, level quota.Level) {
	t.Helper()
	var ie *quota.InsufficientError
	if !errors.As(err, &ie) || ie.Level != level || !errors.Is(err, quota.ErrInsufficient) {
		t.Fatalf("应为 %s 级预算不足，得到 %v", level, err)
	}
}

// TestQuotaHolder：Reserve 在各层级预留同额、任一层级不足则全不改动并说明层级；Settle / Release / unknown 结算移动到
// 正确的桶且幂等；NULL 上限即不限；运维（OrgID 0）计入保留 org operator。
func TestQuotaHolder(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	dl := time.Now().Add(time.Hour)

	// 没有任何预算配置：照常预留，各层级懒创建（上限为 NULL）。
	h0, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "r0", 70, dl)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []quota.Level{quota.LevelOrg, quota.LevelUser, quota.LevelKey} {
		b := w.budget(t, l)
		if b.LimitMicro != nil {
			t.Fatalf("未配置的层级应不限：%+v", b)
		}
		expectUsage(t, b, 70, 0, 0)
	}
	checkQuota(t, w.s)

	w.setBudget(t, quota.LevelOrg, i64(1000))
	w.setBudget(t, quota.LevelKey, i64(300))
	_, err = w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "r1", 231, dl)
	expectInsufficient(t, err, quota.LevelKey) // 70 + 231 > 300
	expectUsage(t, w.budget(t, quota.LevelOrg), 70, 0, 0)
	h1, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "r1", 230, dl)
	if err != nil {
		t.Fatal(err)
	}
	// bob（同 org、无 key）不受 alice 的 key 上限约束，但受 org 上限约束：1000 − 300 = 700。
	bob := quota.Scope{OrgID: w.org, UserID: w.other}
	_, err = w.q.Reserve(ctx, bob, quota.SourceModelRequest, "r2", 701, dl)
	expectInsufficient(t, err, quota.LevelOrg)
	h2, err := w.q.Reserve(ctx, bob, quota.SourceModelRequest, "r2", 700, dl)
	if err != nil {
		t.Fatal(err)
	}
	checkQuota(t, w.s)

	if err := w.q.Settle(ctx, h1, 200, false); err != nil { // 实际费用低于预留
		t.Fatal(err)
	}
	if err := w.q.Settle(ctx, h1, 999, false); err != nil { // 幂等：不再改动
		t.Fatal(err)
	}
	if err := w.q.Release(ctx, h2); err != nil {
		t.Fatal(err)
	}
	if err := w.q.Settle(ctx, h0, 10, true); err != nil { // unknown 至少按预留金额
		t.Fatal(err)
	}
	expectUsage(t, w.budget(t, quota.LevelOrg), 0, 200, 70)
	expectUsage(t, w.budget(t, quota.LevelKey), 0, 200, 70)
	if err := w.q.Settle(ctx, "qh-missing", 1, false); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("不存在的 hold 应为 ErrNotFound，得到 %v", err)
	}
	checkQuota(t, w.s)

	// 运维：OrgID 0 → 保留 org operator，只有 org 层级。
	hop, err := w.q.Reserve(ctx, quota.Scope{}, quota.SourceModelRequest, "op", 5, dl)
	if err != nil {
		t.Fatal(err)
	}
	opOrg := orgIDByName(t, w.s, "operator")
	if n := count(t, w.s, "SELECT count(*) FROM quota_holds WHERE hold_id = $1 AND org_id = $2 AND user_id IS NULL AND key_id IS NULL",
		string(hop), opOrg); n != 1 {
		t.Fatal("运维的 hold 应只记在 operator org")
	}
	if _, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "", 1, dl); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("缺少 ref 应为 ErrInvalid，得到 %v", err)
	}
	checkQuota(t, w.s)
}

// TestQuotaBudgetsAndLimits：预算行按月懒创建并沿用最近一个更早月份的上限；上限低于当月已用被拒绝；PUT 改动当月及
// 之后已有的行；限额读写（全空删除行）与 LimitsFor。
func TestQuotaBudgetsAndLimits(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	org := w.target(quota.LevelOrg)
	now := quota.Period(time.Now())
	prev := now.AddDate(0, -1, 0)

	if b, err := w.q.GetBudget(ctx, org, now); err != nil || b.LimitMicro != nil || b.Used() != 0 {
		t.Fatalf("没有任何行时应不限且为零：%+v, %v", b, err)
	}
	if _, err := w.q.PutBudget(ctx, org, prev, i64(500)); err != nil {
		t.Fatal(err)
	}
	// 当月没有行：读取显示沿用的上限；第一次预留按它创建。
	if b, err := w.q.GetBudget(ctx, org, now); err != nil || b.LimitMicro == nil || *b.LimitMicro != 500 {
		t.Fatalf("当月应沿用上月上限 500：%+v, %v", b, err)
	}
	_, err := w.q.Reserve(ctx, quota.Scope{OrgID: w.org}, quota.SourceModelRequest, "x", 501, time.Now().Add(time.Hour))
	expectInsufficient(t, err, quota.LevelOrg)
	h, err := w.q.Reserve(ctx, quota.Scope{OrgID: w.org}, quota.SourceModelRequest, "x", 400, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var bu *quota.BelowUsageError
	if _, err := w.q.PutBudget(ctx, org, now, i64(399)); !errors.As(err, &bu) || bu.Used != 400 {
		t.Fatalf("低于当月已用应被拒绝，得到 %v", err)
	}
	if _, err := w.q.PutBudget(ctx, org, prev, i64(100)); err != nil { // 改上月不影响当月已有的行
		t.Fatal(err)
	}
	if b, _ := w.q.GetBudget(ctx, org, now); *b.LimitMicro != 500 {
		t.Fatalf("改上月不应改动当月：%+v", b)
	}
	if b, err := w.q.PutBudget(ctx, org, now, nil); err != nil || b.LimitMicro != nil {
		t.Fatalf("改为不限：%+v, %v", b, err)
	}
	if err := w.q.Release(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err := w.q.PutBudget(ctx, org, now, i64(-1)); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("负上限应为 ErrInvalid，得到 %v", err)
	}
	checkQuota(t, w.s)

	key := w.target(quota.LevelKey)
	if l, err := w.q.GetLimits(ctx, key); err != nil || !l.Empty() {
		t.Fatalf("%+v, %v", l, err)
	}
	if err := w.q.PutLimits(ctx, key, quota.Limits{RPM: i64(5), MaxStreams: i64(2)}); err != nil {
		t.Fatal(err)
	}
	if err := w.q.PutLimits(ctx, org, quota.Limits{RPM: i64(100), MaxSandboxes: i64(3)}); err != nil {
		t.Fatal(err)
	}
	m, err := w.q.LimitsFor(ctx, w.scope())
	if err != nil || len(m) != 2 || *m[quota.LevelKey].RPM != 5 || *m[quota.LevelOrg].MaxSandboxes != 3 || m[quota.LevelKey].TPM != nil {
		t.Fatalf("LimitsFor = %+v, %v", m, err)
	}
	if err := w.q.PutLimits(ctx, key, quota.Limits{}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, w.s, "SELECT count(*) FROM limits WHERE scope_kind = 'key'"); n != 0 {
		t.Fatal("全空的限额应删除行")
	}
	if err := w.q.PutLimits(ctx, key, quota.Limits{RPM: i64(-1)}); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("负限额应为 ErrInvalid，得到 %v", err)
	}
}

// TestQuotaSweep：期限已过的 held hold 按全额转为 charged_unknown（各层级 reserved → unknown）；未过期的与 reservation
// 仍为 held 的运行时调用 hold 不动。
func TestQuotaSweep(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	expired, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "old", 40, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "new", 7, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	gwFixture(t, w.s, "t1", 1000)
	beginCall(t, w.s, "t1", "c1")
	tr := mustReserve(t, w.s, "t1", "c1", 9)
	if _, err := w.s.pool.Exec(ctx, "UPDATE quota_holds SET deadline_at = now() - interval '1 second' WHERE hold_id = $1", tr.ReservationID); err != nil {
		t.Fatal(err)
	}
	n, err := w.q.Sweep(ctx, 0)
	if err != nil || n != 1 {
		t.Fatalf("Sweep = %d, %v", n, err)
	}
	var state string
	var charged int64
	if err := w.s.pool.QueryRow(ctx, "SELECT state, charged_micro FROM quota_holds WHERE hold_id = $1", string(expired)).Scan(&state, &charged); err != nil ||
		state != quota.StateChargedUnknown || charged != 40 {
		t.Fatalf("过期 hold 应为 charged_unknown 40：%s %d %v", state, charged, err)
	}
	expectUsage(t, w.budget(t, quota.LevelKey), 7, 0, 40)
	if n, err := w.q.Sweep(ctx, 0); err != nil || n != 0 {
		t.Fatalf("再次 Sweep = %d, %v", n, err)
	}
	checkQuota(t, w.s)
	// 运行时调用的 hold 随 reservation 结算。
	settle(t, w.s, call.Settlement{Try: tr, Outcome: "unknown"})
	if n := count(t, w.s, "SELECT count(*) FROM quota_holds WHERE hold_id = $1 AND state = 'charged_unknown'", tr.ReservationID); n != 1 {
		t.Fatal("运行时调用的 hold 应随 reservation 转为 charged_unknown")
	}
}

// TestQuotaConcurrentReservations：200 个并发预留打同一个 org 预算，随机 settle / release / unknown；检查者在运行中
// 不断读取 G1、G2（各一条语句，一致快照），全程与结束时都不违反；结束时 reserved 归零且已用不超过上限。
func TestQuotaConcurrentReservations(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	const limit = 40_000
	w.setBudget(t, quota.LevelOrg, i64(limit))
	w.setBudget(t, quota.LevelKey, i64(limit/5))

	stop := make(chan struct{})
	var checks atomic.Int64
	var checkErr atomic.Value
	var cw sync.WaitGroup
	cw.Add(1)
	go func() {
		defer cw.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			vs, err := w.s.DBViolations(ctx)
			if err != nil {
				checkErr.Store(err.Error())
				return
			}
			for _, v := range vs {
				if v.ID == "G1" || v.ID == "G2" {
					checkErr.Store(v.ID + " " + v.Detail)
					return
				}
			}
			checks.Add(1)
		}
	}()

	var ok, short atomic.Int64
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(i), 7))
			s := quota.Scope{OrgID: w.org, UserID: w.user}
			if i%2 == 1 {
				s = quota.Scope{OrgID: w.org, UserID: w.other}
			}
			if i%3 == 0 {
				s.KeyID = w.key
				s.UserID = w.user
			}
			amount := int64(100 + r.IntN(900))
			id, err := w.q.Reserve(ctx, s, quota.SourceModelRequest, fmt.Sprintf("req-%d", i), amount, time.Now().Add(time.Hour))
			if errors.Is(err, quota.ErrInsufficient) {
				short.Add(1)
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			ok.Add(1)
			switch r.IntN(3) {
			case 0:
				err = w.q.Settle(ctx, id, int64(r.IntN(int(amount)+1)), false)
			case 1:
				err = w.q.Release(ctx, id)
			default:
				err = w.q.Settle(ctx, id, 0, true)
			}
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(stop)
	cw.Wait()
	if v := checkErr.Load(); v != nil {
		t.Fatalf("运行中不变量违反：%v", v)
	}
	if checks.Load() == 0 {
		t.Fatal("检查者至少应完成一轮")
	}
	checkQuota(t, w.s)
	org := w.budget(t, quota.LevelOrg)
	if org.ReservedMicro != 0 || org.Used() > limit {
		t.Fatalf("结束时 reserved 应为 0 且已用不超过上限：%+v", org)
	}
	if ok.Load()+short.Load() != 200 || ok.Load() == 0 {
		t.Fatalf("成功 %d、不足 %d", ok.Load(), short.Load())
	}
	t.Logf("成功 %d、预算不足 %d、运行中检查 %d 轮，org 已用 %d / %d", ok.Load(), short.Load(), checks.Load(), org.Used(), int64(limit))
}

// quotaTask 以 alice 的 key 创建研究任务（owner alice，org acme，key 在其中）与其 attempt。
func quotaTask(t *testing.T, w quotaWorld, taskID string, budget int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := w.s.CreateResearch(ctx, w.user, api.CreateTaskRequest{RequestID: "req-" + taskID, BodyHash: []byte("h"), TaskID: taskID,
		Spec: json.RawMessage(`{"worker":"sim"}`), Limits: json.RawMessage(fmt.Sprintf(`{"budget_micro":%d}`, budget)), MaxFaultRetries: 3,
		KeyID: w.key}); err != nil {
		t.Fatalf("CreateResearch: %v", err)
	}
	if _, err := w.s.CreateAttempt(ctx, task.NewAttempt{TaskID: taskID, AttemptID: "att-" + taskID, AttemptNo: 1, EnvID: "env-" + taskID}); err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
}

// TestQuotaRuntimeCallHolds：沙箱的每次 Gateway try 在同一事务中放一个 hold（task 的 org / owner / key 各层级），随 try
// 的结算转换（ok → settled 实际费用、retryable/fatal → released、unknown → charged_unknown 全额），启动账本转换同样转换；
// 月度预算不足为 insufficient_quota 且任务账本不预留；任务预算的拒绝照旧优先。
func TestQuotaRuntimeCallHolds(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	quotaTask(t, w, "t1", 10_000)
	beginCall(t, w.s, "t1", "c1")
	tr := mustReserve(t, w.s, "t1", "c1", 300)
	var org, user int64
	var key, ref, source string
	if err := w.s.pool.QueryRow(ctx, "SELECT org_id, user_id, key_id, ref, source FROM quota_holds WHERE hold_id = $1", tr.ReservationID).
		Scan(&org, &user, &key, &ref, &source); err != nil {
		t.Fatalf("try 应有同 ID 的 hold：%v", err)
	}
	if org != w.org || user != w.user || key != w.key || ref != "t1/c1#1" || source != "task_call" {
		t.Fatalf("hold 主体 = %d %d %s %s %s", org, user, key, ref, source)
	}
	for _, l := range []quota.Level{quota.LevelOrg, quota.LevelUser, quota.LevelKey} {
		expectUsage(t, w.budget(t, l), 300, 0, 0)
	}
	settle(t, w.s, call.Settlement{Try: tr, Outcome: "ok", ActualMicro: 120, ResultSHA256: strings.Repeat("a", 64), ResultSize: 1})
	expectUsage(t, w.budget(t, quota.LevelKey), 0, 120, 0)

	beginCall(t, w.s, "t1", "c2")
	tr = mustReserve(t, w.s, "t1", "c2", 50)
	settle(t, w.s, call.Settlement{Try: tr, Outcome: "retryable"})
	tr = mustReserve(t, w.s, "t1", "c2", 60)
	settle(t, w.s, call.Settlement{Try: tr, Outcome: "unknown"})
	expectUsage(t, w.budget(t, quota.LevelOrg), 0, 120, 60)

	// 月度预算不足：insufficient_quota，任务账本与预算行都不变。
	w.setBudget(t, quota.LevelUser, i64(200)) // 已用 180
	beginCall(t, w.s, "t1", "c3")
	before, _ := w.s.LoadBudget(ctx, "t1")
	_, err := reserve(w.s, "t1", "c3", 21)
	expectRejected(t, err, persistence.CodeInsufficientQuota)
	if !strings.Contains(err.Error(), "user") {
		t.Fatalf("拒绝应说明层级：%v", err)
	}
	expectBudget(t, w.s, "t1", before)
	expectUsage(t, w.budget(t, quota.LevelUser), 0, 120, 60)
	// 任务预算的拒绝优先于月度预算。
	_, err = reserve(w.s, "t1", "c3", 20_000)
	expectRejected(t, err, persistence.CodeBudgetInsufficient)

	tr = mustReserve(t, w.s, "t1", "c3", 20)
	conv, err := w.s.ConvertLedger(ctx)
	if err != nil || conv.Reservations != 1 {
		t.Fatalf("ConvertLedger = %+v, %v", conv, err)
	}
	checkQuota(t, w.s)
	expectUsage(t, w.budget(t, quota.LevelUser), 0, 120, 80)
	if n := count(t, w.s, "SELECT count(*) FROM reservations r LEFT JOIN quota_holds q ON q.hold_id = r.reservation_id WHERE q.hold_id IS NULL"); n != 0 {
		t.Fatalf("%d 个 reservation 没有 hold", n)
	}

	// 运维任务（无 owner、无 key）只记在 operator org。
	gwFixture(t, w.s, "op1", 1000)
	beginCall(t, w.s, "op1", "c1")
	tr = mustReserve(t, w.s, "op1", "c1", 5)
	if n := count(t, w.s, "SELECT count(*) FROM quota_holds WHERE hold_id = $1 AND org_id = $2 AND user_id IS NULL AND key_id IS NULL",
		tr.ReservationID, orgIDByName(t, w.s, "operator")); n != 1 {
		t.Fatal("运维任务的 hold 应只在 operator org")
	}
	checkQuota(t, w.s)
}

// TestOrgSandboxes：org 的 max_sandboxes 不限时不计数；有上限时计入该 org 存活的任务环境与本 server 已准入、尚无环境的任务。
func TestOrgSandboxes(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	quotaTask(t, w, "t1", 100)
	org, limit, live, err := w.s.OrgSandboxes(ctx, "t1", nil)
	if err != nil || org != w.org || limit != nil || live != 0 {
		t.Fatalf("不限：%d %v %d %v", org, limit, live, err)
	}
	if err := w.q.PutLimits(ctx, w.target(quota.LevelOrg), quota.Limits{MaxSandboxes: i64(2)}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, w.s, "SELECT count(*) FROM environments WHERE env_id = 'env-t1' AND attempt_id = 'att-t1' AND stopped_at IS NULL"); n != 1 {
		t.Fatal("CreateAttempt 应已登记存活的环境 env-t1")
	}
	if _, err := w.s.CreateResearch(ctx, w.other, api.CreateTaskRequest{RequestID: "r2", BodyHash: []byte("h"), TaskID: "t2",
		Spec: json.RawMessage(`{}`), MaxFaultRetries: 1}); err != nil {
		t.Fatal(err)
	}
	_, limit, live, err = w.s.OrgSandboxes(ctx, "t2", []string{"t1", "t2", "zzz"})
	if err != nil || limit == nil || *limit != 2 || live != 2 { // env-t1 + t2（已准入、无环境）；t1 有环境不重复计
		t.Fatalf("有上限：%v %d %v", limit, live, err)
	}
	if _, err := w.s.pool.Exec(ctx, "UPDATE environments SET stopped_at = now() WHERE env_id = 'env-t1'"); err != nil {
		t.Fatal(err)
	}
	if _, _, live, _ = w.s.OrgSandboxes(ctx, "t2", nil); live != 0 {
		t.Fatalf("已停止的环境不计：%d", live)
	}
	if _, _, _, err := w.s.OrgSandboxes(ctx, "nope", nil); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

// TestQuotaOvershootReportedSeparately：hold 以高于预留的实际费用结算使层级超过上限——不是 G2 违反，由
// QuotaOvershoots 单独报告；直接篡改已用额造成的超出才是违反。
func TestQuotaOvershootReportedSeparately(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	w.setBudget(t, quota.LevelOrg, i64(100))
	h, err := w.q.Reserve(ctx, quota.Scope{OrgID: w.org}, quota.SourceModelRequest, "x", 90, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.q.Settle(ctx, h, 130, false); err != nil {
		t.Fatal(err)
	}
	checkQuota(t, w.s)
	notes, err := w.s.QuotaOvershoots(ctx)
	if err != nil || len(notes) != 1 || !strings.Contains(notes[0], "结算超出预留 40") {
		t.Fatalf("应单独报告一条超额：%v, %v", notes, err)
	}
	if _, err := w.s.pool.Exec(ctx, "UPDATE quota_budgets SET spent_micro = spent_micro + 20 WHERE scope_kind = 'org'"); err != nil {
		t.Fatal(err)
	}
	if vs := quotaViolations(t, w.s); len(vs) != 2 { // G1（spent ≠ Σ）与 G2（扣除超额后仍超出）
		t.Fatalf("篡改后应有 G1、G2 违反：%v", vs)
	}
}
