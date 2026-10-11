//go:build linux

package main

import (
	"context"
	"encoding/json"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

// agentbox usage / bill show / bill reconcile: usage grouped and filtered, a bill per org, reconciliation exits 0 on
// matching data and 1 on an injected difference; usage errors exit 2; the connection string never appears.
func TestUsageAndBillCommands(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("agentbox usage / bill 只在 Linux 上可用")
	}
	dsn, s := userTestDatabase(t)
	ctx := context.Background()
	if _, err := s.CreateUser(ctx, "alice", "alice", "h"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBOX_DATABASE_URL", dsn)
	run := func(fn func([]string, io.Writer, io.Writer) int, args ...string) (int, string, string) {
		t.Helper()
		var out, errb strings.Builder
		code := fn(args, &out, &errb)
		if all := out.String() + errb.String(); strings.Contains(all, dsn) {
			t.Fatalf("connection string in output: %s", all)
		}
		return code, out.String(), errb.String()
	}
	if code, _, errb := run(runOrg, "create", "acme", "--owner", "alice"); code != 0 {
		t.Fatalf("org create = %d %q", code, errb)
	}
	m, err := s.TenancyStore().MemberByName(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	q := s.QuotaStore()
	scope := quota.Scope{OrgID: m.OrgID, UserID: m.UserID}
	for i, c := range []struct {
		charged int64
		unknown bool
	}{{1_250_000, false}, {500_000, true}} {
		h, err := q.Reserve(ctx, scope, quota.SourceModelRequest, "mr_"+string(rune('a'+i)), 500_000, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Settle(ctx, h, c.charged, c.unknown); err != nil {
			t.Fatal(err)
		}
	}
	month := time.Now().UTC().Format("2006-01")

	if code, out, errb := run(runBill, "show", month, "--org", "acme"); code != 0 || !strings.Contains(out, "尚未汇总") ||
		!strings.Contains(out, "$0.000000") {
		t.Fatalf("bill before any rollup = %d %q %q", code, out, errb)
	}
	if code, out, errb := run(runBill, "show", month, "--rollup"); code != 0 || !strings.Contains(out, "org acme 账单 "+month) ||
		!strings.Contains(out, "$1.250000") || !strings.Contains(out, "$0.500000") || !strings.Contains(out, "$1.750000") ||
		!strings.Contains(out, "(未记录)") || !strings.Contains(out, "截至上次汇总") {
		t.Fatalf("bill show = %d %q %q", code, out, errb)
	}
	if code, out, errb := run(runUsage, "--group-by", "user,source", "--org", "acme"); code != 0 || !strings.Contains(out, "alice") ||
		!strings.Contains(out, "model_api") || !strings.Contains(out, "合计") || !strings.Contains(out, "USER") {
		t.Fatalf("usage = %d %q %q", code, out, errb)
	}
	if code, _, errb := run(runUsage, "--org", "nope"); code != 1 || !strings.Contains(errb, "不存在") {
		t.Fatalf("unknown org = %d %q", code, errb)
	}
	for _, bad := range [][]string{{"--group-by", "week"}, {"--from", "2026-10-02", "--to", "2026-10-01"}, {"extra"}} {
		if code, _, _ := run(runUsage, bad...); code != 2 {
			t.Fatalf("usage %v = %d", bad, code)
		}
	}
	if code, _, _ := run(runBill, "show", "2026-13"); code != 2 {
		t.Fatal("bad month accepted")
	}
	if code, _, _ := run(runBill, "pay", month); code != 2 {
		t.Fatal("unknown subcommand accepted")
	}

	if code, out, errb := run(runBill, "reconcile", month); code != 0 || !strings.Contains(out, "一致") {
		t.Fatalf("reconcile = %d %q %q", code, out, errb)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// Today is not final yet: without a rollup, a change to today's rows is not compared (review M3) — the report says so.
	if _, err := conn.Exec(ctx, `UPDATE usage_daily SET unknown_micro = unknown_micro + 7`); err != nil {
		t.Fatal(err)
	}
	if code, out, errb := run(runBill, "reconcile", month, "--no-rollup"); code != 0 || !strings.Contains(out, "未比较") {
		t.Fatalf("not-final days without a rollup = %d %q %q", code, out, errb)
	}
	// The default rollup first recomputes today's rows, which repairs it.
	if code, out, errb := run(runBill, "reconcile", month); code != 0 {
		t.Fatalf("reconcile after rollup = %d %q %q", code, out, errb)
	}

	// A past, final day (2020-01-15): rolled up with --rollup --from, then tampered with. Reconcile reports it with
	// and without a rollup (days before the watermark are never recomputed automatically); `usage --rollup --from`
	// repairs it (review M2).
	for _, q := range []string{
		`INSERT INTO quota_holds (hold_id, org_id, user_id, period, source, ref, amount_micro, state, charged_micro, deadline_at, created_at)
			VALUES ('h_2020', $1, $2, '2020-01-01', 'model_request', 'mr_2020', 900, 'settled', 800, '2020-01-15 13:00+00', '2020-01-15 12:00+00')`,
		`INSERT INTO quota_budgets (scope_kind, scope_id, period, spent_micro) VALUES ('org', $1::bigint::text, '2020-01-01', 800),
			('user', $1::bigint::text || '/' || $2::bigint::text, '2020-01-01', 800)`,
	} {
		if _, err := conn.Exec(ctx, q, m.OrgID, m.UserID); err != nil {
			t.Fatal(err)
		}
	}
	if code, out, errb := run(runUsage, "--rollup", "--from", "2020-01-01", "--to", "2020-01-31", "--org", "acme"); code != 0 ||
		!strings.Contains(out, "2020-01-15") || !strings.Contains(out, "$0.000800") {
		t.Fatalf("usage --rollup --from = %d %q %q", code, out, errb)
	}
	if code, out, errb := run(runBill, "reconcile", "2020-01", "--no-rollup"); code != 0 || strings.Contains(out, "未比较") {
		t.Fatalf("2020-01 = %d %q %q", code, out, errb)
	}
	if _, err := conn.Exec(ctx, `UPDATE usage_daily SET unknown_micro = unknown_micro + 7 WHERE day = '2020-01-15'`); err != nil {
		t.Fatal(err)
	}
	code, out, errb := run(runBill, "reconcile", "2020-01")
	if code != 1 || !strings.Contains(out, "差异 usage_daily 2020-01-15") || !strings.Contains(out, "org=acme user=alice") ||
		!strings.Contains(errb, "1 处差异") {
		t.Fatalf("injected difference = %d %q %q", code, out, errb)
	}
	code, out, _ = run(runBill, "reconcile", "2020-01", "--no-rollup", "--json")
	var rep usage.Report
	if err := json.Unmarshal([]byte(out), &rep); code != 1 || err != nil || len(rep.Usage) != 1 || rep.Usage[0].Rollup.UnknownMicro != 7 {
		t.Fatalf("json report = %d %v %s", code, err, out)
	}
	if code, _, errb := run(runUsage, "--rollup", "--from", "2020-01-15", "--to", "2020-01-15"); code != 0 {
		t.Fatalf("repair = %d %q", code, errb)
	}
	if code, out, errb := run(runBill, "reconcile", "2020-01"); code != 0 {
		t.Fatalf("after the repair = %d %q %q", code, out, errb)
	}
	if _, err := conn.Exec(ctx, `UPDATE quota_budgets SET spent_micro = spent_micro + 1 WHERE scope_kind = 'org'`); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := run(runBill, "reconcile", month); code != 1 || !strings.Contains(out, "差异 quota_budgets org:") {
		t.Fatalf("budget difference = %d %q", code, out)
	}
}
