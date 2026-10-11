//go:build linux

package main

// 本文件是 `agentbox usage` 与 `agentbox bill show|reconcile`（模型网关设计 2026-10-11 §8；G5）：与 budget 命令一样直接
// 连接 PostgreSQL（运维身份，看全部 org）。usage 与 bill show 读周期汇总的 usage_daily（--rollup 先汇总一次）；
// bill reconcile 在同一快照中先汇总、再从原始的 quota_holds 与 model_requests 重算该月，与 usage_daily 和
// quota_budgets 比较，有差异时逐条打印（org、用户、key、模型）并以退出码 1 结束。

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

const (
	usageCmdUsage = "用法: agentbox usage [--org NAME] [--user U] [--key ID] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--group-by day,model,key,user,org,source] [--rollup] [--database-url URL]"
	billUsage     = "用法: agentbox bill show <YYYY-MM> [--org NAME] [--rollup] | reconcile <YYYY-MM> [--no-rollup] [--json] [--database-url URL]"
)

// usageFilter 把 --org、--user、--key 解析为查询条件。
func usageFilter(ctx context.Context, s *postgres.Store, org, user, key string, q *usage.Query) error {
	ten := s.TenancyStore()
	if org != "" {
		o, err := ten.OrgByName(ctx, org)
		if err != nil {
			return fmt.Errorf("org %s: %s", org, tenancyError(err))
		}
		q.OrgID = o.ID
	}
	if user != "" {
		m, err := ten.MemberByName(ctx, usernameKeyOf(user))
		if err != nil {
			return fmt.Errorf("用户 %s: %s", user, tenancyError(err))
		}
		q.UserID = m.UserID
	}
	if key != "" {
		k, err := ten.GetKey(ctx, key)
		if err != nil {
			return fmt.Errorf("key %s: %s", key, tenancyError(err))
		}
		q.KeyID = k.ID
	}
	return nil
}

// rollupFirst 在 --rollup 时先汇总一次：from 为零时从 watermark 起；非零时（usage --rollup --from）另从该日起重算，
// 用于修复对账报告的已汇总日子（复审 M2）。
func rollupFirst(ctx context.Context, s *postgres.Store, on bool, from time.Time, stderr io.Writer, cmd string) bool {
	if !on {
		return true
	}
	if _, err := s.UsageStore().Rollup(ctx, from); err != nil {
		fmt.Fprintf(stderr, "agentbox %s: 汇总: %v\n", cmd, err)
		return false
	}
	return true
}

func printAsOf(ctx context.Context, s *postgres.Store, w io.Writer) {
	st, err := s.UsageStore().Status(ctx)
	switch {
	case err != nil:
	case st.LastRunAt.IsZero():
		fmt.Fprintln(w, "（尚未汇总过：server 每分钟汇总一次，或加 --rollup）")
	default:
		fmt.Fprintf(w, "截至上次汇总 %s\n", st.LastRunAt.UTC().Format(time.RFC3339))
	}
}

func runUsage(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("usage", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	org := fs.String("org", "", "只看这个 org（默认全部）")
	user := fs.String("user", "", "只看这个用户（用户名）")
	key := fs.String("key", "", "只看这个 key（key ID）")
	now := time.Now().UTC()
	from := fs.String("from", usage.Month(now).Format("2006-01-02"), "第一天 YYYY-MM-DD（UTC）")
	to := fs.String("to", now.Format("2006-01-02"), "最后一天 YYYY-MM-DD（UTC，含）")
	groupBy := fs.String("group-by", "day", "分组维度：day、model、key、user、org、source（逗号分隔）")
	rollup := fs.Bool("rollup", false, "查询前先汇总一次（同时给出 --from 时从该日起重算，用于修复对账差异）")
	pos, ok := cliArgs(fs, args)
	if !ok {
		return 2
	}
	if len(pos) != 0 {
		fmt.Fprintln(stderr, usageCmdUsage)
		return 2
	}
	var q usage.Query
	var err error
	if q.From, err = usage.ParseDay(*from); err == nil {
		q.To, err = usage.ParseDay(*to)
	}
	if err == nil {
		q.GroupBy, err = usage.ParseDims(*groupBy)
	}
	if err == nil {
		err = q.Validate()
	}
	if err != nil {
		fmt.Fprintln(stderr, "agentbox usage:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s, code := openTenancy(ctx, "usage", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	if err := usageFilter(ctx, s, *org, *user, *key, &q); err != nil {
		fmt.Fprintln(stderr, "agentbox usage:", err)
		return 1
	}
	var repairFrom time.Time // 只有显式给出 --from 时才从该日起重算
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "from" {
			repairFrom = q.From
		}
	})
	if !rollupFirst(ctx, s, *rollup, repairFrom, stderr, "usage") {
		return 1
	}
	rows, err := s.UsageStore().Usage(ctx, q)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox usage:", err)
		return 1
	}
	writeUsageTable(stdout, q.GroupBy, rows)
	printAsOf(ctx, s, stdout)
	return 0
}

// writeUsageTable 以表格打印分组后的用量（列为分组维度加各项计量）与合计。
func writeUsageTable(w io.Writer, dims []usage.Dim, rows []usage.Group) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	var head []string
	for _, d := range dims {
		head = append(head, strings.ToUpper(string(d)))
	}
	fmt.Fprintln(tw, strings.Join(append(head, "REQUESTS", "PROMPT", "COMPLETION", "COST", "UNKNOWN"), "\t"))
	var total usage.Totals
	for _, g := range rows {
		var cols []string
		for _, d := range dims {
			cols = append(cols, dimValue(d, g))
		}
		cols = append(cols, fmt.Sprint(g.Requests), fmt.Sprint(g.PromptTokens), fmt.Sprint(g.CompletionTokens), usage.USD(g.CostMicro),
			usage.USD(g.UnknownMicro))
		fmt.Fprintln(tw, strings.Join(cols, "\t"))
		total.Add(g.Totals)
	}
	cols := make([]string, len(dims))
	if len(cols) > 0 {
		cols[0] = "合计"
	}
	cols = append(cols, fmt.Sprint(total.Requests), fmt.Sprint(total.PromptTokens), fmt.Sprint(total.CompletionTokens),
		usage.USD(total.CostMicro), usage.USD(total.UnknownMicro))
	fmt.Fprintln(tw, strings.Join(cols, "\t"))
	_ = tw.Flush()
}

func dimValue(d usage.Dim, g usage.Group) string {
	switch d {
	case usage.DimDay:
		return g.Day
	case usage.DimOrg:
		return orDash(g.OrgName)
	case usage.DimUser:
		return orDash(g.Username)
	case usage.DimKey:
		if g.KeyID == "" {
			return "-"
		}
		return g.KeyID + "(" + g.KeyName + ")"
	case usage.DimModel:
		return usage.ModelLabel(g.Model)
	}
	return string(g.Source)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func runBill(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, billUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("bill "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	org := fs.String("org", "", "bill show：只看这个 org（默认每个有用量的 org 各一份）")
	rollup := fs.Bool("rollup", false, "bill show：先汇总一次")
	noRollup := fs.Bool("no-rollup", false, "bill reconcile：不先汇总，直接比较现有的 usage_daily")
	asJSON := fs.Bool("json", false, "bill reconcile：以 JSON 输出报告")
	pos, ok := cliArgs(fs, args[1:])
	if !ok {
		return 2
	}
	if (sub != "show" && sub != "reconcile") || len(pos) != 1 {
		fmt.Fprintln(stderr, billUsage)
		return 2
	}
	month, err := usage.ParseMonth(pos[0])
	if err != nil {
		fmt.Fprintln(stderr, "agentbox bill:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s, code := openTenancy(ctx, "bill", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	if sub == "reconcile" {
		return reconcileMonth(ctx, s, month, !*noRollup, *asJSON, stdout, stderr)
	}
	return showBills(ctx, s, month, *org, *rollup, stdout, stderr)
}

// showBills 打印该月的账单：给了 --org 时只打印它（没有用量也打印空账单），否则每个有用量的 org 各一份。
func showBills(ctx context.Context, s *postgres.Store, month time.Time, org string, rollup bool, stdout, stderr io.Writer) int {
	from, to := usage.MonthRange(month)
	q := usage.Query{From: from, To: to, GroupBy: []usage.Dim{usage.DimOrg, usage.DimModel, usage.DimSource}}
	if err := usageFilter(ctx, s, org, "", "", &q); err != nil {
		fmt.Fprintln(stderr, "agentbox bill:", err)
		return 1
	}
	if !rollupFirst(ctx, s, rollup, time.Time{}, stderr, "bill") {
		return 1
	}
	rows, err := s.UsageStore().Usage(ctx, q)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox bill:", err)
		return 1
	}
	byOrg := map[string][]usage.Group{}
	var names []string
	for _, g := range rows {
		if _, ok := byOrg[g.OrgName]; !ok {
			names = append(names, g.OrgName)
		}
		byOrg[g.OrgName] = append(byOrg[g.OrgName], g)
	}
	if org != "" && len(names) == 0 {
		names = []string{org}
	}
	if len(names) == 0 {
		fmt.Fprintf(stdout, "%s 没有用量\n", month.Format("2006-01"))
	}
	for i, n := range names {
		if i > 0 {
			fmt.Fprintln(stdout)
		}
		usage.WriteBill(stdout, "org "+n+" 账单", usage.NewBill(month, byOrg[n]))
	}
	printAsOf(ctx, s, stdout)
	return 0
}

// reconcileMonth 对账；有差异时退出码 1。
func reconcileMonth(ctx context.Context, s *postgres.Store, month time.Time, rollup, asJSON bool, stdout, stderr io.Writer) int {
	rep, err := s.UsageStore().Reconcile(ctx, month, rollup)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox bill reconcile:", err)
		return 2
	}
	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		usage.WriteReport(stdout, rep)
	}
	if !rep.OK() {
		fmt.Fprintf(stderr, "agentbox bill reconcile: %s 有 %d 处差异\n", rep.Month, len(rep.Usage)+len(rep.Budgets))
		return 1
	}
	return 0
}
