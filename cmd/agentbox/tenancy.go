//go:build linux

package main

// 本文件是 `agentbox org create|list|add-member` 与 `agentbox key create|list|revoke`（模型网关设计 2026-10-11 §8；G1）：
// 与 `agentbox user` 一样直接连接 PostgreSQL（运维身份，不经 HTTP API）。key create 需要服务端的 pepper 文件
// （--data-dir 下的 apikey.pepper，server 首次启动时创建），完整 key 只打印这一次。连接串只取自 --database-url 或
// 环境变量 AGENTBOX_DATABASE_URL，不回显。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

const (
	orgUsage = "用法: agentbox org create <name> [--owner USER] | list | add-member <org> <user> [--role member|owner] [--database-url URL]（默认取环境变量 " + databaseURLEnv + "）"
	keyUsage = "用法: agentbox key create --user USER --name NAME --scopes runtime,models [--expires-in 720h] --data-dir DIR | list [--user USER] [--org ORG] | revoke <id> [--database-url URL]"
)

// cliArgs 逐段解析：标志可以出现在位置参数前后。
func cliArgs(fs *flag.FlagSet, args []string) ([]string, bool) {
	var pos []string
	for rest := args; ; {
		if err := fs.Parse(rest); err != nil {
			return nil, false
		}
		if fs.NArg() == 0 {
			return pos, true
		}
		pos, rest = append(pos, fs.Arg(0)), fs.Args()[1:]
	}
}

// openTenancy 连接数据库；失败时已输出错误并返回退出码。
func openTenancy(ctx context.Context, cmd, dsn string, stderr io.Writer) (*postgres.Store, int) {
	if dsn == "" {
		dsn = os.Getenv(databaseURLEnv)
	}
	if dsn == "" {
		fmt.Fprintf(stderr, "agentbox %s: 需要 --database-url（或环境变量 %s）\n", cmd, databaseURLEnv)
		return nil, 2
	}
	s, err := postgres.Open(ctx, postgres.Options{DSN: dsn})
	if err != nil {
		fmt.Fprintf(stderr, "agentbox %s: %v\n", cmd, err)
		return nil, 1
	}
	return s, 0
}

func usernameKeyOf(name string) string {
	if _, key, err := account.NormalizeUsername(name); err == nil {
		return key
	}
	return "\x00" // 不符合规则的名字不可能存在
}

// tenancyError 把用例错误转为一行可读消息。
func tenancyError(err error) string {
	switch {
	case errors.Is(err, tenant.ErrUserInOtherOrg):
		return "该用户已属于另一个 org（只能从其个人 org 迁入）"
	case errors.Is(err, tenant.ErrLastOwner):
		return "org 须至少保留一个 owner"
	case errors.Is(err, persistence.ErrConflict):
		return "名称已被使用"
	case errors.Is(err, persistence.ErrNotFound):
		return "不存在"
	}
	return err.Error()
}

func runOrg(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, orgUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("org "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	owner := fs.String("owner", "", "org create：把该用户（须仍在个人 org 中）移入为 owner")
	role := fs.String("role", "member", "org add-member：owner 或 member")
	pos, ok := cliArgs(fs, args[1:])
	if !ok {
		return 2
	}
	switch {
	case sub == "create" && len(pos) == 1:
	case sub == "list" && len(pos) == 0:
	case sub == "add-member" && len(pos) == 2:
	default:
		fmt.Fprintln(stderr, orgUsage)
		return 2
	}
	r, err := tenant.ParseRole(*role)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox org:", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, code := openTenancy(ctx, "org", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	ten := s.Tenancy()
	fail := func(err error) int {
		fmt.Fprintf(stderr, "agentbox org %s: %s\n", sub, tenancyError(err))
		return 1
	}
	switch sub {
	case "create":
		if err := tenant.ValidateOrgName(pos[0]); err != nil {
			fmt.Fprintln(stderr, "agentbox org create:", err)
			return 2
		}
		o, err := ten.CreateOrg(ctx, pos[0])
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "已创建 org %s（id %d）\n", o.Name, o.ID)
		if *owner != "" {
			m, err := ten.AddMember(ctx, o.ID, usernameKeyOf(*owner), tenant.Owner)
			if err != nil {
				return fail(err)
			}
			fmt.Fprintf(stdout, "%s 为 %s 的 owner\n", m.Username, o.Name)
		}
	case "list":
		orgs, err := ten.ListOrgs(ctx)
		if err != nil {
			return fail(err)
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "id\tname\tmembers\tdisabled\tcreated_at")
		for _, o := range orgs {
			fmt.Fprintf(tw, "%d\t%s\t%d\t%t\t%s\n", o.ID, o.Name, o.Members, o.Disabled, o.CreatedAt.UTC().Format(time.RFC3339))
		}
		if err := tw.Flush(); err != nil {
			return fail(err)
		}
	case "add-member":
		o, err := ten.OrgByName(ctx, pos[0])
		if err != nil {
			return fail(fmt.Errorf("org %q %s", pos[0], tenancyError(err)))
		}
		m, err := ten.AddMember(ctx, o.ID, usernameKeyOf(pos[1]), r)
		if errors.Is(err, persistence.ErrNotFound) {
			return fail(fmt.Errorf("用户 %q 不存在", pos[1]))
		}
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "%s 为 %s 的 %s\n", m.Username, m.OrgName, m.Role)
	}
	return 0
}

func runKey(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, keyUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("key "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	user := fs.String("user", "", "key create：key 的用户；key list：只列该用户的 key")
	org := fs.String("org", "", "key list：只列该 org 的 key")
	name := fs.String("name", "", "key create：key 的名称（1–64 个字符）")
	scopes := fs.String("scopes", "", "key create：逗号分隔的 runtime、models")
	expiresIn := fs.Duration("expires-in", 0, "key create：有效期（例如 720h）；0 为不过期")
	dataDir := fs.String("data-dir", "", "key create：server 的数据目录（读取 apikey.pepper）")
	pos, ok := cliArgs(fs, args[1:])
	if !ok {
		return 2
	}
	switch {
	case sub == "create" && len(pos) == 0 && *user != "" && *name != "" && *scopes != "" && *dataDir != "":
	case sub == "list" && len(pos) == 0:
	case sub == "revoke" && len(pos) == 1:
	default:
		fmt.Fprintln(stderr, keyUsage)
		return 2
	}
	var sc tenant.Scopes
	var pepper []byte
	if sub == "create" {
		var err error
		if sc, err = tenant.ParseScopes(strings.Split(*scopes, ",")); err != nil {
			fmt.Fprintln(stderr, "agentbox key create:", err)
			return 2
		}
		if !apikey.ValidName(*name) || *expiresIn < 0 {
			fmt.Fprintln(stderr, "agentbox key create: --name 须为 1–64 个字符，--expires-in 不能为负")
			return 2
		}
		if pepper, err = apikey.LoadPepper(*dataDir); err != nil {
			fmt.Fprintln(stderr, "agentbox key create:", err, "（先以同一 --data-dir 启动一次 server）")
			return 1
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, code := openTenancy(ctx, "key", *dsn, stderr)
	if s == nil {
		return code
	}
	defer s.Close()
	ten := s.Tenancy()
	fail := func(err error) int {
		fmt.Fprintf(stderr, "agentbox key %s: %s\n", sub, tenancyError(err))
		return 1
	}
	switch sub {
	case "create":
		m, err := ten.MemberByName(ctx, usernameKeyOf(*user))
		if err != nil {
			return fail(fmt.Errorf("用户 %q %s", *user, tenancyError(err)))
		}
		g, err := apikey.Generate(pepper)
		if err != nil {
			return fail(err)
		}
		nk := tenant.NewKey{ID: g.ID, UserID: m.UserID, Name: *name, SecretHMAC: g.SecretHMAC, Scopes: sc}
		if *expiresIn > 0 {
			exp := time.Now().Add(*expiresIn).UTC()
			nk.ExpiresAt = &exp
		}
		k, err := ten.CreateKey(ctx, nk)
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stderr, "已为 %s（org %s）创建 key %s（scopes %s）；完整 key 只显示这一次：\n", k.Username, m.OrgName, k.ID, k.Scopes)
		fmt.Fprintln(stdout, g.Full)
	case "list":
		var f tenant.KeyFilter
		if *user != "" {
			m, err := ten.MemberByName(ctx, usernameKeyOf(*user))
			if err != nil {
				return fail(fmt.Errorf("用户 %q %s", *user, tenancyError(err)))
			}
			f.UserID = m.UserID
		}
		if *org != "" {
			o, err := ten.OrgByName(ctx, *org)
			if err != nil {
				return fail(fmt.Errorf("org %q %s", *org, tenancyError(err)))
			}
			f.OrgID = o.ID
		}
		keys, err := ten.ListKeys(ctx, f)
		if err != nil {
			return fail(err)
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "id\tuser\torg_id\tname\tscopes\tstate\tlast_used_at")
		for _, k := range keys {
			state, last := "active", "-"
			switch {
			case k.RevokedAt != nil:
				state = "revoked"
			case k.ExpiresAt != nil && !time.Now().Before(*k.ExpiresAt):
				state = "expired"
			}
			if k.LastUsedAt != nil {
				last = k.LastUsedAt.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n", k.ID, k.Username, k.OrgID, strings.ReplaceAll(k.Name, "\t", " "), k.Scopes, state, last)
		}
		if err := tw.Flush(); err != nil {
			return fail(err)
		}
	case "revoke":
		if !apikey.ValidID(pos[0]) {
			return fail(fmt.Errorf("key %q 不存在", pos[0]))
		}
		k, err := ten.RevokeKey(ctx, pos[0])
		if err != nil {
			return fail(fmt.Errorf("key %q %s", pos[0], tenancyError(err)))
		}
		fmt.Fprintf(stdout, "已吊销 key %s（%s）\n", k.ID, k.Username)
	}
	return 0
}
