//go:build linux

package main

// 本文件是 `agentbox limits set|show` 与 `agentbox budget set|show`（模型网关设计 2026-10-11 §8；G2）：与 org、key 命令
// 一样直接连接 PostgreSQL（运维身份）。kind 为 org、user、key，id 分别为 org 名称、用户名、key ID。用户与 key 层级的值
// 不能超过其 org 的上限（与 HTTP API 相同）。limits set 只改给出的字段（none 清除该字段）；budget set 设置本月（UTC）
// 的月度上限，之后的月份沿用它。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
)

const (
	limitsUsage = "用法: agentbox limits set <org|user|key> <id> [--rpm N|none] [--tpm N|none] [--max-sandboxes N|none] [--max-streams N|none] | show <org|user|key> <id> [--database-url URL]"
	budgetUsage = "用法: agentbox budget set <org|user|key> <id> (--usd X | --limit-micro N | --limit-micro none) | show <org|user|key> <id> [--period YYYY-MM] [--database-url URL]"
)

// optValue 是可选的整数标志：未给出、给出整数、或 none（清除）。
type optValue struct {
	set, clear bool
	v          int64
}

func (o *optValue) String() string { return "" }

func (o *optValue) Set(s string) error {
	o.set = true
	if s == "none" {
		o.clear = true
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt32 {
		return errors.New("须为 0–2147483647 的整数或 none")
	}
	o.v = n
	return nil
}

// apply 把标志应用到字段。
func (o *optValue) apply(f **int64) {
	switch {
	case !o.set:
	case o.clear:
		*f = nil
	default:
		v := o.v
		*f = &v
	}
}

// resolveTarget 把 kind/id 解析为预算层级与其 org。
func resolveTarget(ctx context.Context, s *postgres.Store, kind, id string) (quota.Target, int64, error) {
	level, err := quota.ParseLevel(kind)
	if err != nil {
		return quota.Target{}, 0, err
	}
	ten := s.TenancyStore()
	switch level {
	case quota.LevelOrg:
		o, err := ten.OrgByName(ctx, id)
		if err != nil {
			return quota.Target{}, 0, fmt.Errorf("org %s: %s", id, tenancyError(err))
		}
		return quota.Target{Level: level, ID: strconv.FormatInt(o.ID, 10)}, o.ID, nil
	case quota.LevelUser:
		m, err := ten.MemberByName(ctx, usernameKeyOf(id))
		if err != nil {
			return quota.Target{}, 0, fmt.Errorf("用户 %s: %s", id, tenancyError(err))
		}
		return quota.Target{Level: level, ID: quota.UserTargetID(m.OrgID, m.UserID)}, m.OrgID, nil
	}
	k, err := ten.GetKey(ctx, id)
	if err != nil {
		return quota.Target{}, 0, fmt.Errorf("key %s: %s", id, tenancyError(err))
	}
	return quota.Target{Level: level, ID: k.ID}, k.OrgID, nil
}

func orgTarget(org int64) quota.Target {
	return quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(org, 10)}
}

func fmtLimit(v *int64) string {
	if v == nil {
		return "不限"
	}
	return strconv.FormatInt(*v, 10)
}

func runLimits(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, limitsUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("limits "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	var rpm, tpm, sandboxes, streams optValue
	fs.Var(&rpm, "rpm", "每分钟请求数（none 清除）")
	fs.Var(&tpm, "tpm", "每分钟 token 数（模型 API；none 清除）")
	fs.Var(&sandboxes, "max-sandboxes", "org 的并发沙箱数（none 清除）")
	fs.Var(&streams, "max-streams", "并发模型流（模型 API；none 清除）")
	pos, ok := cliArgs(fs, args[1:])
	if !ok {
		return 2
	}
	if (sub != "set" && sub != "show") || len(pos) != 2 {
		fmt.Fprintln(stderr, limitsUsage)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, code := openTenancy(ctx, "limits", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	t, org, err := resolveTarget(ctx, s, pos[0], pos[1])
	if err != nil {
		fmt.Fprintln(stderr, "agentbox limits:", err)
		return 1
	}
	q := s.QuotaStore()
	l, err := q.GetLimits(ctx, t)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox limits:", err)
		return 1
	}
	if sub == "set" {
		rpm.apply(&l.RPM)
		tpm.apply(&l.TPM)
		sandboxes.apply(&l.MaxSandboxes)
		streams.apply(&l.MaxStreams)
		if t.Level != quota.LevelOrg && l.MaxSandboxes != nil {
			fmt.Fprintln(stderr, "agentbox limits: max_sandboxes 只能在 org 层级设置")
			return 1
		}
		if t.Level != quota.LevelOrg {
			ol, err := q.GetLimits(ctx, orgTarget(org))
			if err != nil {
				fmt.Fprintln(stderr, "agentbox limits:", err)
				return 1
			}
			if f := l.Exceeding(ol); f != "" {
				fmt.Fprintf(stderr, "agentbox limits: %s 不能超过所属 org 的上限\n", f)
				return 1
			}
		}
		if err := q.PutLimits(ctx, t, l); err != nil {
			fmt.Fprintln(stderr, "agentbox limits:", err)
			return 1
		}
	}
	fmt.Fprintf(stdout, "%s %s: rpm=%s tpm=%s max_sandboxes=%s max_streams=%s\n", pos[0], pos[1], fmtLimit(l.RPM), fmtLimit(l.TPM),
		fmtLimit(l.MaxSandboxes), fmtLimit(l.MaxStreams))
	return 0
}

// parseUSD 把美元金额（最多 6 位小数）转为微美元。
func parseUSD(s string) (int64, error) {
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" || len(frac) > 6 || strings.ContainsAny(whole+frac, "+-") || strings.HasSuffix(s, ".") {
		return 0, errors.New("须为非负的美元金额，最多 6 位小数")
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || w > math.MaxInt64/1_000_000-1 {
		return 0, errors.New("须为非负的美元金额，最多 6 位小数")
	}
	var f int64
	if frac != "" {
		if f, err = strconv.ParseInt(frac+strings.Repeat("0", 6-len(frac)), 10, 64); err != nil {
			return 0, errors.New("须为非负的美元金额，最多 6 位小数")
		}
	}
	return w*1_000_000 + f, nil
}

func fmtMicro(v int64) string { return fmt.Sprintf("%d（$%.6f）", v, float64(v)/1e6) }

func runBudget(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, budgetUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("budget "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	usd := fs.String("usd", "", "budget set：月度上限（美元）")
	limitMicro := fs.String("limit-micro", "", "budget set：月度上限（微美元），none 为不限")
	period := fs.String("period", "", "budget show：月份 YYYY-MM（默认本月，UTC）")
	pos, ok := cliArgs(fs, args[1:])
	if !ok {
		return 2
	}
	if (sub != "set" && sub != "show") || len(pos) != 2 || (sub == "set" && (*usd == "") == (*limitMicro == "")) {
		fmt.Fprintln(stderr, budgetUsage)
		return 2
	}
	var limit *int64
	switch {
	case sub == "show":
	case *usd != "":
		v, err := parseUSD(*usd)
		if err != nil {
			fmt.Fprintln(stderr, "agentbox budget: --usd", err)
			return 2
		}
		limit = &v
	case *limitMicro != "none":
		v, err := strconv.ParseInt(*limitMicro, 10, 64)
		if err != nil || v < 0 {
			fmt.Fprintln(stderr, "agentbox budget: --limit-micro 须为非负整数或 none")
			return 2
		}
		limit = &v
	}
	var p time.Time // 零值：数据库时间所在的月份
	if *period != "" {
		t, err := time.Parse("2006-01", *period)
		if err != nil {
			fmt.Fprintln(stderr, "agentbox budget: --period 须为 YYYY-MM")
			return 2
		}
		p = t
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, code := openTenancy(ctx, "budget", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	t, org, err := resolveTarget(ctx, s, pos[0], pos[1])
	if err != nil {
		fmt.Fprintln(stderr, "agentbox budget:", err)
		return 1
	}
	q := s.QuotaStore()
	var b quota.Budget
	if sub == "set" {
		if t.Level != quota.LevelOrg && limit != nil {
			ob, err := q.GetBudget(ctx, orgTarget(org), p)
			if err != nil {
				fmt.Fprintln(stderr, "agentbox budget:", err)
				return 1
			}
			if ob.LimitMicro != nil && *limit > *ob.LimitMicro {
				fmt.Fprintf(stderr, "agentbox budget: 不能超过所属 org 的月度上限 %s\n", fmtMicro(*ob.LimitMicro))
				return 1
			}
		}
		b, err = q.PutBudget(ctx, t, p, limit)
	} else {
		b, err = q.GetBudget(ctx, t, p)
	}
	var below *quota.BelowUsageError
	if errors.As(err, &below) {
		fmt.Fprintf(stderr, "agentbox budget: 上限低于本月已用（含预留）%s\n", fmtMicro(below.Used))
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "agentbox budget:", err)
		return 1
	}
	lim := "不限"
	if b.LimitMicro != nil {
		lim = fmtMicro(*b.LimitMicro)
	}
	fmt.Fprintf(stdout, "%s %s %s: limit=%s spent=%s reserved=%s unknown=%s\n", pos[0], pos[1], b.Period.Format("2006-01"), lim,
		fmtMicro(b.SpentMicro), fmtMicro(b.ReservedMicro), fmtMicro(b.UnknownMicro))
	return 0
}
