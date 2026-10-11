package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// agentbox org / key：建 org 并移入 owner、加成员、按 server 的 pepper 建 key（完整 key 只打印一次且可被校验）、
// 列出与吊销；用法错误退出码 2，连接串不出现在输出中。
func TestOrgAndKeyCommands(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("agentbox org / key 只在 Linux 上可用")
	}
	dsn, s := userTestDatabase(t)
	ctx := context.Background()
	for _, name := range []string{"alice", "bob"} {
		if _, err := s.CreateUser(ctx, name, name, "h"); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	pepper, err := apikey.LoadOrCreatePepper(dir) // what the server creates on first start
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBOX_DATABASE_URL", dsn)
	run := func(fn func([]string, io.Writer, io.Writer) int, args ...string) (int, string, string) {
		t.Helper()
		var out, errb strings.Builder
		code := fn(args, &out, &errb)
		if all := out.String() + errb.String(); strings.Contains(all, dsn) || strings.Contains(all, "agentbox_cli_") {
			t.Fatalf("connection string in output: %s", all)
		}
		return code, out.String(), errb.String()
	}
	org, key := runOrg, runKey

	if code, out, errb := run(org, "create", "acme", "--owner", "alice"); code != 0 || !strings.Contains(out, "acme") {
		t.Fatalf("org create = %d %q %q", code, out, errb)
	}
	if code, _, errb := run(org, "create", "acme"); code != 1 || !strings.Contains(errb, "已被使用") {
		t.Fatalf("duplicate org = %d %q", code, errb)
	}
	if code, out, errb := run(org, "add-member", "acme", "bob"); code != 0 || !strings.Contains(out, "member") {
		t.Fatalf("org add-member = %d %q %q", code, out, errb)
	}
	if code, _, errb := run(org, "add-member", "acme", "nobody"); code != 1 || !strings.Contains(errb, "不存在") {
		t.Fatalf("unknown user = %d %q", code, errb)
	}
	code, out, errb := run(org, "list")
	if code != 0 || !strings.Contains(out, "acme") || !strings.Contains(out, "operator") || !strings.Contains(out, "user-1") {
		t.Fatalf("org list = %d %q %q", code, out, errb)
	}

	code, out, errb = run(key, "create", "--user", "bob", "--name", "ci", "--scopes", "runtime,models", "--expires-in", "720h", "--data-dir", dir)
	full := strings.TrimSpace(out)
	if code != 0 || !strings.HasPrefix(full, apikey.Prefix) || !strings.Contains(errb, "acme") {
		t.Fatalf("key create = %d %q %q", code, out, errb)
	}
	v := apikey.NewVerifier(s.APIKeys(), pepper, nil)
	id, err := v.Verify(ctx, full)
	if err != nil || id.Username != "bob" || id.Principal.Scopes != tenant.AllScopes || id.Principal.Role != tenant.Member {
		t.Fatalf("created key verifies: %+v %v", id, err)
	}
	if code, out, _ := run(key, "list", "--org", "acme"); code != 0 || !strings.Contains(out, id.Principal.KeyID) || !strings.Contains(out, "active") ||
		strings.Contains(out, full) {
		t.Fatalf("key list = %d %q", code, out)
	}
	if code, out, _ := run(key, "list", "--user", "alice"); code != 0 || strings.Contains(out, id.Principal.KeyID) {
		t.Fatalf("key list --user alice = %d %q", code, out)
	}
	if code, out, errb := run(key, "revoke", id.Principal.KeyID); code != 0 || !strings.Contains(out, id.Principal.KeyID) {
		t.Fatalf("key revoke = %d %q %q", code, out, errb)
	}
	if code, out, _ := run(key, "list"); code != 0 || !strings.Contains(out, "revoked") {
		t.Fatalf("revoked state = %d %q", code, out)
	}
	if code, _, _ := run(key, "revoke", "k_aaaaaaaaaaaaaaaa"); code != 1 {
		t.Fatalf("revoke unknown = %d", code)
	}
	// The pepper must exist (created by the server); a missing one is not silently created by the CLI.
	empty := t.TempDir()
	if code, _, errb := run(key, "create", "--user", "bob", "--name", "x", "--scopes", "models", "--data-dir", empty); code != 1 ||
		!strings.Contains(errb, "apikey.pepper") {
		t.Fatalf("missing pepper = %d %q", code, errb)
	}
	if _, err := os.Stat(filepath.Join(empty, apikey.PepperFile)); err == nil {
		t.Fatal("the CLI must not create a pepper")
	}
	for _, c := range []struct {
		fn   func([]string, io.Writer, io.Writer) int
		args []string
	}{
		{org, nil}, {org, []string{"create"}}, {org, []string{"create", "operator"}}, {org, []string{"add-member", "acme"}},
		{org, []string{"add-member", "acme", "bob", "--role", "admin"}}, {org, []string{"remove"}},
		{key, nil}, {key, []string{"create", "--user", "bob"}}, {key, []string{"create", "--user", "bob", "--name", "x", "--scopes", "root", "--data-dir", dir}},
		{key, []string{"revoke"}}, {key, []string{"list", "extra"}},
	} {
		if code, _, _ := run(c.fn, c.args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", c.args, code)
		}
	}
}
