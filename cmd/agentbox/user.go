//go:build linux

package main

// 本文件是 `agentbox user list | disable <name> | enable <name>`：直接连接 PostgreSQL（不经 HTTP API）管理
// 用户账号。停用在同一事务中删除该用户的全部会话（立即吊销）。连接串只取自 --database-url 或环境变量
// AGENTBOX_DATABASE_URL，不回显（标志的默认值为空，用法输出中不会出现环境变量中的连接串）。

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/account"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence/postgres"
)

const userUsage = "用法: agentbox user list | disable <name> | enable <name> [--database-url URL]（默认取环境变量 " + databaseURLEnv + "）"

func runUser(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, userUsage)
		return 2
	}
	sub := args[0]
	fs := flag.NewFlagSet("user "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("database-url", "", "PostgreSQL 连接串（默认取环境变量 "+databaseURLEnv+"）")
	// 标志可以出现在用户名前后：逐段解析，非标志参数收集为位置参数。
	var pos []string
	for rest := args[1:]; ; {
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		if fs.NArg() == 0 {
			break
		}
		pos, rest = append(pos, fs.Arg(0)), fs.Args()[1:]
	}
	switch {
	case sub == "list" && len(pos) == 0:
	case (sub == "disable" || sub == "enable") && len(pos) == 1:
	default:
		fmt.Fprintln(stderr, userUsage)
		return 2
	}
	if *dsn == "" {
		*dsn = os.Getenv(databaseURLEnv)
	}
	if *dsn == "" {
		fmt.Fprintln(stderr, "agentbox user: 需要 --database-url（或环境变量 "+databaseURLEnv+"）")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, postgres.Options{DSN: *dsn})
	if err != nil {
		fmt.Fprintln(stderr, "agentbox user:", err)
		return 1
	}
	defer store.Close()
	if sub == "list" {
		return listUsers(ctx, store, stdout, stderr)
	}
	return setUserDisabled(ctx, store, pos[0], sub == "disable", stdout, stderr)
}

func listUsers(ctx context.Context, store *postgres.Store, stdout, stderr io.Writer) int {
	users, err := store.ListUsers(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox user list:", err)
		return 1
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "username\trole\tdisabled\tcreated_at")
	for _, u := range users {
		fmt.Fprintf(tw, "%s\t%s\t%t\t%s\n", u.Username, u.Role, u.Disabled, u.CreatedAt.UTC().Format(time.RFC3339))
	}
	if err := tw.Flush(); err != nil {
		fmt.Fprintln(stderr, "agentbox user list:", err)
		return 1
	}
	return 0
}

func setUserDisabled(ctx context.Context, store *postgres.Store, name string, disabled bool, stdout, stderr io.Writer) int {
	op := "enable"
	if disabled {
		op = "disable"
	}
	// 不符合用户名规则的名字不可能存在：与未知用户同样处理。
	_, key, err := account.NormalizeUsername(name)
	if err == nil {
		err = store.SetDisabled(ctx, key, disabled)
	} else {
		err = persistence.ErrNotFound
	}
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		fmt.Fprintf(stderr, "agentbox user %s: 用户 %q 不存在\n", op, name)
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "agentbox user %s: %v\n", op, err)
		return 1
	case disabled:
		fmt.Fprintf(stdout, "已停用用户 %s，其全部会话已吊销\n", name)
	default:
		fmt.Fprintf(stdout, "已启用用户 %s\n", name)
	}
	return 0
}
