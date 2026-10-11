//go:build linux

package main

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
)

// agentbox limits / budget：设置与查看各层级的限额和月度预算；用户层级不能超过 org；用法错误退出码 2；
// 连接串不出现在输出中。
func TestLimitsAndBudgetCommands(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("agentbox limits / budget 只在 Linux 上可用")
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
	if code, out, errb := run(runLimits, "set", "org", "acme", "--rpm", "10", "--max-sandboxes", "3"); code != 0 ||
		!strings.Contains(out, "rpm=10") || !strings.Contains(out, "max_sandboxes=3") || !strings.Contains(out, "tpm=不限") {
		t.Fatalf("limits set org = %d %q %q", code, out, errb)
	}
	if code, _, errb := run(runLimits, "set", "user", "alice", "--rpm", "11"); code != 1 || !strings.Contains(errb, "rpm 不能超过") {
		t.Fatalf("limits above the org = %d %q", code, errb)
	}
	if code, out, errb := run(runLimits, "set", "user", "alice", "--rpm", "5", "--tpm", "100"); code != 0 || !strings.Contains(out, "rpm=5 tpm=100") {
		t.Fatalf("limits set user = %d %q %q", code, out, errb)
	}
	if code, out, _ := run(runLimits, "set", "user", "alice", "--tpm", "none"); code != 0 || !strings.Contains(out, "rpm=5 tpm=不限") {
		t.Fatalf("only the given field changes, none clears: %q", out)
	}
	if code, out, _ := run(runLimits, "show", "org", "acme"); code != 0 || !strings.Contains(out, "rpm=10") {
		t.Fatalf("limits show = %q", out)
	}
	if code, _, errb := run(runLimits, "show", "key", "k_aaaaaaaaaaaaaaaa"); code != 1 || !strings.Contains(errb, "不存在") {
		t.Fatalf("unknown key = %d %q", code, errb)
	}
	if code, _, errb := run(runLimits, "set", "user", "alice", "--max-sandboxes", "1"); code != 1 || !strings.Contains(errb, "只能在 org 层级") {
		t.Fatalf("user-level max_sandboxes = %d %q", code, errb)
	}
	if code, _, _ := run(runLimits, "set", "team", "x"); code != 1 {
		t.Fatalf("unknown kind = %d", code)
	}
	if code, _, _ := run(runLimits, "set", "org", "acme", "--rpm", "-1"); code != 2 {
		t.Fatalf("bad value = %d", code)
	}

	if code, out, errb := run(runBudget, "set", "org", "acme", "--usd", "1.5"); code != 0 || !strings.Contains(out, "limit=1500000") {
		t.Fatalf("budget set org = %d %q %q", code, out, errb)
	}
	if code, _, errb := run(runBudget, "set", "user", "alice", "--limit-micro", "1500001"); code != 1 || !strings.Contains(errb, "不能超过") {
		t.Fatalf("budget above the org = %d %q", code, errb)
	}
	if code, out, errb := run(runBudget, "set", "user", "alice", "--limit-micro", "none"); code != 0 || !strings.Contains(out, "limit=不限") {
		t.Fatalf("budget unlimited = %d %q %q", code, out, errb)
	}
	if code, out, _ := run(runBudget, "show", "org", "acme"); code != 0 || !strings.Contains(out, "spent=0") {
		t.Fatalf("budget show = %q", out)
	}
	if code, out, _ := run(runBudget, "show", "org", "acme", "--period", "2020-01"); code != 0 || !strings.Contains(out, "2020-01") ||
		!strings.Contains(out, "limit=不限") {
		t.Fatalf("an earlier month has no carried limit: %q", out)
	}
	for _, args := range [][]string{{"set", "org", "acme"}, {"set", "org", "acme", "--usd", "1", "--limit-micro", "1"},
		{"set", "org", "acme", "--usd", "1.1234567"}, {"show", "org"}, {"show", "org", "acme", "--period", "2026"}} {
		if code, _, _ := run(runBudget, args...); code != 2 {
			t.Fatalf("usage error %v = %d", args, code)
		}
	}
}

func TestParseUSD(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "1": 1_000_000, "1.5": 1_500_000, "0.000001": 1, "12.345678": 12_345_678} {
		if got, err := parseUSD(in); err != nil || got != want {
			t.Errorf("parseUSD(%q) = %d, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "-1", "+1", "1.", ".5", "1.1234567", "abc", "1e3"} {
		if _, err := parseUSD(in); err == nil {
			t.Errorf("parseUSD(%q) should fail", in)
		}
	}
}
