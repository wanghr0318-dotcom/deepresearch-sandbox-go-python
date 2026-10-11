package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

// undo0015 把库还原为只应用了 0014 的形状（undo0014 先执行它）。
const undo0015 = `DROP TABLE usage_daily, usage_rollup_state; DROP INDEX quota_holds_created, quota_holds_held_created;
	DELETE FROM schema_migrations WHERE version >= 15;
`

func today() time.Time { return utcDay(time.Now()) }

// usageWorld 在 quotaWorld 上放一组今天的已结算用量：
//   - 模型 API：mr_1 已知 77（token 13/5），mr_2 结果未知（按 hold 金额 100 计入 unknown），mr_3 已释放（不计）；
//   - 运行时：任务 t1 的调用 c1（模型 kimi，已知 120）、c2（search 端点，没有模型，已知 30）、c3（结果未知，60）。
func usageWorld(t *testing.T) quotaWorld {
	t.Helper()
	ctx := context.Background()
	w := newQuotaWorld(t)
	deadline := time.Now().Add(time.Hour)
	mr := func(id string, settle func(quota.HoldID)) {
		hold, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, id, 100, deadline)
		if err != nil {
			t.Fatal(err)
		}
		settle(hold)
		if err := w.s.ModelRequests().RecordModelRequest(ctx, modelRecord(w, id, hold, time.Now())); err != nil {
			t.Fatal(err)
		}
	}
	mr("mr_1", func(h quota.HoldID) { mustNil(t, w.q.Settle(ctx, h, 77, false)) })
	mr("mr_2", func(h quota.HoldID) { mustNil(t, w.q.Settle(ctx, h, 90, true)) })
	mr("mr_3", func(h quota.HoldID) { mustNil(t, w.q.Release(ctx, h)) })

	quotaTask(t, w, "t1", 10_000)
	for _, c := range []struct{ id, endpoint, model string }{{"c1", "/v1/chat/completions", "kimi"}, {"c2", "/v1/search", ""}, {"c3", "/v1/chat/completions", "kimi"}} {
		if _, err := w.s.BeginCall(ctx, call.BeginCallRequest{TaskID: "t1", CallID: c.id, AttemptID: "att-t1", Fingerprint: "fp-" + c.id,
			Endpoint: c.endpoint, Model: c.model, Deadline: 120 * time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	settle(t, w.s, call.Settlement{Try: mustReserve(t, w.s, "t1", "c1", 300), Outcome: "ok", ActualMicro: 120,
		ResultSHA256: strings.Repeat("a", 64), ResultSize: 1})
	settle(t, w.s, call.Settlement{Try: mustReserve(t, w.s, "t1", "c2", 40), Outcome: "ok", ActualMicro: 30,
		ResultSHA256: strings.Repeat("b", 64), ResultSize: 1})
	settle(t, w.s, call.Settlement{Try: mustReserve(t, w.s, "t1", "c3", 60), Outcome: "unknown"})
	return w
}

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func usageBy(t *testing.T, w quotaWorld, q usage.Query) map[string]usage.Totals {
	t.Helper()
	gs, err := w.s.UsageStore().Usage(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]usage.Totals{}
	for _, g := range gs {
		k := strings.Join([]string{g.Day, g.OrgName, g.Username, g.KeyName, g.Model, string(g.Source)}, "|")
		if _, dup := out[k]; dup {
			t.Fatalf("duplicate group %s", k)
		}
		out[k] = g.Totals
	}
	return out
}

func dailyRows(t *testing.T, s *Store) int {
	return count(t, s, "SELECT count(*) FROM usage_daily")
}

// The rollup aggregates settled model API holds (with their request's model and tokens) and settled/charged_unknown
// runtime holds (with the call's model, or the endpoint for search), skips released holds, and is idempotent; the
// month reconciles with usage_daily and quota_budgets.
func TestUsageRollup(t *testing.T) {
	ctx := context.Background()
	w := usageWorld(t)
	us := w.s.UsageStore()
	st, err := us.Status(ctx)
	if err != nil || !st.LastRunAt.IsZero() || !st.Watermark.IsZero() {
		t.Fatalf("before the first run: %+v %v", st, err)
	}
	res, err := us.Rollup(ctx, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.From.Equal(today()) || res.Rows != 3 {
		t.Fatalf("rollup = %+v", res)
	}
	day := today().Format("2006-01-02")
	q := usage.Query{From: today(), To: today(), GroupBy: []usage.Dim{usage.DimModel, usage.DimSource}}
	want := map[string]usage.Totals{
		"|||" + "|smart|model_api": {Requests: 2, PromptTokens: 26, CompletionTokens: 10, CostMicro: 77, UnknownMicro: 100},
		"|||" + "|kimi|runtime":    {Requests: 2, CostMicro: 120, UnknownMicro: 60},
		"|||" + "|search|runtime":  {Requests: 1, CostMicro: 30},
	}
	check := func() {
		t.Helper()
		got := usageBy(t, w, q)
		if len(got) != len(want) {
			t.Fatalf("groups = %+v", got)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("%s = %+v, want %+v (all %+v)", k, got[k], v, got)
			}
		}
	}
	check()
	n := dailyRows(t, w.s)
	for range 2 { // idempotent
		if _, err := us.Rollup(ctx, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if _, err := us.Rollup(ctx, today().AddDate(0, 0, -3)); err != nil { // re-run from an earlier day
			t.Fatal(err)
		}
		check()
		if dailyRows(t, w.s) != n {
			t.Fatalf("rows changed on re-run")
		}
	}
	// Grouped by day, user and key, with names; filters by user and key.
	byUser := usageBy(t, w, usage.Query{From: today(), To: today(), OrgID: w.org, UserID: w.user, KeyID: w.key,
		GroupBy: []usage.Dim{usage.DimDay, usage.DimUser, usage.DimKey}})
	if v := byUser[day+"||alice|k||"]; v.Requests != 5 || v.TotalMicro() != 387 || len(byUser) != 1 {
		t.Fatalf("by user = %+v", byUser)
	}
	if got := usageBy(t, w, usage.Query{From: today(), To: today(), UserID: w.other, GroupBy: []usage.Dim{usage.DimDay}}); len(got) != 0 {
		t.Fatalf("bob has no usage: %+v", got)
	}
	if got := usageBy(t, w, usage.Query{From: today(), To: today(), OrgID: w.org, GroupBy: []usage.Dim{usage.DimOrg}}); got["|acme||||"].Requests != 5 {
		t.Fatalf("by org = %+v", got)
	}
	if _, err := us.Usage(ctx, usage.Query{From: today(), To: today().AddDate(0, 0, -1)}); err == nil {
		t.Fatal("inverted range accepted")
	}
	rep, err := us.Reconcile(ctx, today(), true)
	if err != nil || !rep.OK() || rep.Raw.TotalMicro() != 387 || rep.Rollup != rep.Raw {
		t.Fatalf("reconcile = %+v %v", rep, err)
	}
	checkQuota(t, w.s)
}

// The watermark stays on the day of the oldest held hold, so a hold settled later is picked up by the next
// incremental run; days before the watermark are not recomputed unless asked for.
func TestUsageWatermark(t *testing.T) {
	ctx := context.Background()
	w := newQuotaWorld(t)
	us := w.s.UsageStore()
	hold, err := w.q.Reserve(ctx, w.scope(), quota.SourceModelRequest, "mr_held", 100, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// Move the hold two days back (its budget period is irrelevant here: no reconciliation).
	mustExec(t, w.s, `UPDATE quota_holds SET created_at = created_at - interval '2 days' WHERE hold_id = $1`, string(hold))
	held := today().AddDate(0, 0, -2)
	res, err := us.Rollup(ctx, time.Time{})
	if err != nil || !res.From.Equal(held) || !res.Watermark.Equal(held) || res.Rows != 0 {
		t.Fatalf("rollup with a held hold = %+v %v", res, err)
	}
	// A marker row before the watermark is left alone by incremental runs.
	mustExec(t, w.s, `INSERT INTO usage_daily (day, org_id, source, requests) VALUES ('2020-01-01', $1, 'runtime', 1)`, w.org)
	mustNil(t, w.q.Settle(ctx, hold, 40, false))
	if res, err = us.Rollup(ctx, time.Time{}); err != nil || !res.From.Equal(held) || res.Rows != 1 {
		t.Fatalf("rollup after settling = %+v %v", res, err)
	}
	wantWM := utcDay(time.Now().Add(-time.Hour))
	if !res.Watermark.Equal(wantWM) {
		t.Fatalf("watermark = %v, want %v", res.Watermark, wantWM)
	}
	got := usageBy(t, w, usage.Query{From: held, To: held, GroupBy: []usage.Dim{usage.DimSource}})
	if got["|||||model_api"].CostMicro != 40 || got["|||||model_api"].Requests != 1 {
		t.Fatalf("settled hold not rolled up: %+v", got)
	}
	st, err := us.Status(ctx)
	if err != nil || !st.Watermark.Equal(wantWM) || st.LastRunAt.IsZero() {
		t.Fatalf("status = %+v %v", st, err)
	}
	if _, err := us.Rollup(ctx, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if count(t, w.s, `SELECT count(*) FROM usage_daily WHERE day = '2020-01-01'`) != 1 {
		t.Fatal("an incremental run recomputed a day before the watermark")
	}
	// Re-running from that day recomputes it (no holds then: the marker goes).
	if _, err := us.Rollup(ctx, time.Date(2020, 1, 1, 15, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if count(t, w.s, `SELECT count(*) FROM usage_daily WHERE day = '2020-01-01'`) != 0 {
		t.Fatal("re-running from a day did not recompute it")
	}
	if got := usageBy(t, w, usage.Query{From: held, To: held, GroupBy: []usage.Dim{usage.DimSource}}); got["|||||model_api"].CostMicro != 40 {
		t.Fatalf("re-run lost data: %+v", got)
	}
}

func mustExec(t *testing.T, s *Store, sql string, args ...any) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// boundaryWorld writes settled holds (and their budget rows) on both sides of the 2020-01/2020-02 boundary. The
// February request started in January (its model_requests row is in the January partition) but its hold — and so
// its usage — is February's.
func boundaryWorld(t *testing.T) quotaWorld {
	t.Helper()
	w := newQuotaWorld(t)
	mustExec(t, w.s, `SELECT agentbox_model_request_partitions('2020-01-15'), agentbox_model_request_partitions('2020-02-15')`)
	for _, h := range []struct {
		id, created, period, state string
		charged                    int64
		reqAt                      string
	}{
		{"h_jan", "2020-01-31 23:59:59.5+00", "2020-01-01", "settled", 500, "2020-01-31 23:59:59.4+00"},
		{"h_feb", "2020-02-01 00:00:00.01+00", "2020-02-01", "settled", 700, "2020-01-31 23:59:59.99+00"},
		{"h_feb_unknown", "2020-02-29 23:00:00+00", "2020-02-01", "charged_unknown", 300, "2020-02-29 22:59:59+00"},
	} {
		mustExec(t, w.s, `INSERT INTO quota_holds (hold_id, org_id, user_id, key_id, period, source, ref, amount_micro, state, charged_micro,
				deadline_at, created_at) VALUES ($1, $2, $3, $4, $5, 'model_request', $1, 300, $6, $7, $8, $8)`,
			h.id, w.org, w.user, w.key, h.period, h.state, h.charged, h.created)
		mustExec(t, w.s, `INSERT INTO model_requests (id, org_id, user_id, key_id, client_protocol, model, stream, status,
				prompt_tokens, completion_tokens, cost_micro, latency_ms, hold_id, created_at)
			VALUES ($1, $2, $3, $4, 'openai', 'smart', false, 200, 10, 4, $5, 100, $1, $6)`, h.id, w.org, w.user, w.key, h.charged, h.reqAt)
	}
	for _, b := range []struct {
		period       string
		spent, unkno int64
	}{{"2020-01-01", 500, 0}, {"2020-02-01", 700, 300}} {
		for _, tg := range w.scope().Targets() {
			mustExec(t, w.s, `INSERT INTO quota_budgets (scope_kind, scope_id, period, spent_micro, unknown_micro) VALUES ($1, $2, $3, $4, $5)`,
				string(tg.Level), tg.ID, b.period, b.spent, b.unkno)
		}
	}
	return w
}

// Usage falls into the day and month of its hold across a month boundary and across model_requests partitions; each
// month reconciles; an injected difference in usage_daily or in a budget is reported (and a re-run for the day fixes
// usage_daily).
func TestUsageMonthBoundaryAndReconcile(t *testing.T) {
	ctx := context.Background()
	w := boundaryWorld(t)
	us := w.s.UsageStore()
	if res, err := us.Rollup(ctx, time.Time{}); err != nil || res.From.Format("2006-01-02") != "2020-01-31" || res.Rows != 3 {
		t.Fatalf("first rollup = %+v %v", res, err)
	}
	jan, _ := usage.ParseMonth("2020-01")
	feb, _ := usage.ParseMonth("2020-02")
	byDay := func(m time.Time) map[string]usage.Totals {
		from, to := usage.MonthRange(m)
		return usageBy(t, w, usage.Query{From: from, To: to, OrgID: w.org, GroupBy: []usage.Dim{usage.DimDay}})
	}
	if got := byDay(jan); len(got) != 1 || got["2020-01-31|||||"].CostMicro != 500 {
		t.Fatalf("january = %+v", got)
	}
	got := byDay(feb)
	if len(got) != 2 || got["2020-02-01|||||"] != (usage.Totals{Requests: 1, PromptTokens: 10, CompletionTokens: 4, CostMicro: 700}) ||
		got["2020-02-29|||||"].UnknownMicro != 300 {
		t.Fatalf("february = %+v", got)
	}
	for _, m := range []time.Time{jan, feb} {
		rep, err := us.Reconcile(ctx, m, true)
		if err != nil || !rep.OK() {
			t.Fatalf("reconcile %s = %+v %v", rep.Month, rep, err)
		}
	}
	// Inject a difference into usage_daily: reported (rollupFirst does not touch days before the watermark).
	mustExec(t, w.s, `UPDATE usage_daily SET cost_micro = cost_micro + 1 WHERE day = '2020-02-01'`)
	rep, err := us.Reconcile(ctx, feb, true)
	if err != nil || rep.OK() || len(rep.Usage) != 1 || rep.Usage[0].Day != "2020-02-01" || rep.Usage[0].Rollup.CostMicro != 701 ||
		rep.Usage[0].Raw.CostMicro != 700 || len(rep.Budgets) != 0 {
		t.Fatalf("injected usage difference: %+v %v", rep, err)
	}
	if l := rep.Lines(); len(l) != 1 || !strings.Contains(l[0], "org=acme user=alice") || !strings.Contains(l[0], "model=smart") {
		t.Fatalf("lines = %v", l)
	}
	if rep, _ := us.Reconcile(ctx, jan, false); !rep.OK() {
		t.Fatalf("january is unaffected: %+v", rep)
	}
	// Re-running the rollup for that day repairs it.
	if _, err := us.Rollup(ctx, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if rep, err := us.Reconcile(ctx, feb, false); err != nil || !rep.OK() {
		t.Fatalf("after re-run: %+v %v", rep, err)
	}
	// Inject a difference into a budget: reported per level.
	mustExec(t, w.s, `UPDATE quota_budgets SET unknown_micro = 299 WHERE scope_kind = 'key' AND period = '2020-02-01'`)
	rep, err = us.Reconcile(ctx, feb, false)
	if err != nil || len(rep.Usage) != 0 || len(rep.Budgets) != 1 || rep.Budgets[0].Level != "key" || rep.Budgets[0].UnknownMicro != 299 ||
		rep.Budgets[0].RawUnknown != 300 {
		t.Fatalf("injected budget difference: %+v %v", rep, err)
	}
}

// Concurrent rollups are serialised by the advisory lock: all succeed and the result is the same as one run.
func TestUsageConcurrentRollups(t *testing.T) {
	ctx := context.Background()
	w := usageWorld(t)
	us := w.s.UsageStore()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			from := time.Time{}
			if i%2 == 0 {
				from = today().AddDate(0, 0, -1)
			}
			_, err := us.Rollup(ctx, from)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if rep, err := us.Reconcile(ctx, today(), false); err != nil || !rep.OK() || rep.Raw.Requests != 5 {
		t.Fatalf("after concurrent runs: %+v %v", rep, err)
	}
	// No connection was left holding the lock: a plain session can take it at once.
	conn, err := w.s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, usageLockKey).Scan(&ok); err != nil || !ok {
		t.Fatalf("usage lock still held: %v %v", ok, err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, usageLockKey); err != nil {
		t.Fatal(err)
	}
}

func TestListMembers(t *testing.T) {
	w := newQuotaWorld(t)
	ms, err := w.s.TenancyStore().ListMembers(context.Background(), w.org)
	if err != nil || len(ms) != 2 || ms[0].Username != "alice" || ms[0].Role != tenant.Owner || ms[1].Username != "bob" ||
		ms[1].Role != tenant.Member || ms[1].OrgName != "acme" {
		t.Fatalf("members = %+v %v", ms, err)
	}
	if ms, err := w.s.TenancyStore().ListMembers(context.Background(), 999999); err != nil || len(ms) != 0 {
		t.Fatalf("unknown org = %+v %v", ms, err)
	}
}
