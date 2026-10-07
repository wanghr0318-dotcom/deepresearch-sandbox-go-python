package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ownership"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
)

// 模型标志：--models 白名单须包含 --model-name；--model-price 可重复、逗号分隔，模型须已声明；
// 不合法时拒绝启动（runServer 在取得锁与连接数据库之前以退出码 2 返回）。
func TestModelFlags(t *testing.T) {
	const url = "https://api.moonshot.cn/v1"
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var prices listFlag
	fs.Var(&prices, "model-price", "")
	if err := fs.Parse([]string{"--model-price", "kimi-k3=2000000:8000000", "--model-price", " kimi-k2.6=1:2 , "}); err != nil {
		t.Fatal(err)
	}
	m, err := modelConfig(modelFlags{BaseURL: url, Name: "kimi-k2.6", Prices: prices, PriceIn: 5, PriceOut: 6,
		Models: "kimi-k2.6, kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3", MaxTokensCap: 32768})
	if err != nil {
		t.Fatal(err)
	}
	if m.MaxTokensCap != 32768 {
		t.Fatalf("MaxTokensCap = %d，期望 32768", m.MaxTokensCap)
	}
	if strings.Join(m.Models, ",") != "kimi-k2.6,kimi-k2.7-code,kimi-k2.7-code-highspeed,kimi-k3" ||
		m.Pricing != (upstream.Pricing{InputMicroPerMTok: 5, OutputMicroPerMTok: 6}) ||
		m.PricingByModel["kimi-k3"] != (upstream.Pricing{InputMicroPerMTok: 2_000_000, OutputMicroPerMTok: 8_000_000}) ||
		m.PricingByModel["kimi-k2.6"] != (upstream.Pricing{InputMicroPerMTok: 1, OutputMicroPerMTok: 2}) || len(m.PricingByModel) != 2 {
		t.Fatalf("ModelConfig = %+v", m)
	}
	// 无 --models：只声明 --model-name，其单价可单独给出。
	if m, err := modelConfig(modelFlags{BaseURL: url, Name: "a", Prices: []string{"a=1:1"}}); err != nil || m.Models != nil || len(m.PricingByModel) != 1 {
		t.Fatalf("无白名单：%+v %v", m, err)
	}
	for name, f := range map[string]modelFlags{
		"白名单不含默认模型":       {BaseURL: url, Name: "kimi-k2.6", Models: "kimi-k3"},
		"白名单重复":           {BaseURL: url, Name: "a", Models: "a,b,a"},
		"单价的模型未声明":        {BaseURL: url, Name: "a", Models: "a", Prices: []string{"b=1:1"}},
		"单价重复":            {BaseURL: url, Name: "a", Prices: []string{"a=1:1", "a=2:2"}},
		"单价格式":            {BaseURL: url, Name: "a", Prices: []string{"a=1"}},
		"单价为负":            {BaseURL: url, Name: "a", Prices: []string{"a=-1:1"}},
		"默认单价为负":          {BaseURL: url, Name: "a", PriceIn: -1},
		"max_tokens 上限为负": {BaseURL: url, Name: "a", MaxTokensCap: -1},
		"无上游地址的白名单":       {Name: "a", Models: "a"},
		"有上游地址无默认模型":      {BaseURL: url, Models: "a"},
	} {
		if _, err := modelConfig(f); err == nil {
			t.Errorf("%s：应拒绝", name)
		}
	}
	if runtime.GOOS != "linux" {
		return // 其他平台的 runServer 只报告不支持
	}
	var stderr strings.Builder
	if code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--model-base-url", url,
		"--model-name", "kimi-k2.6", "--models", "kimi-k3"}, &stderr); code != 2 || !strings.Contains(stderr.String(), "不含 --model-name") {
		t.Fatalf("白名单不含默认模型时 runServer 退出码 %d（%s），期望 2 并拒绝启动", code, stderr.String())
	}
}

// TestSearchProviderFlag：tavily / serper 没有 AGENTBOX_SEARCH_API_KEY 时拒绝启动（退出码 2，在取得锁与
// 连接数据库之前）；fake 须同时设置 --upstream-allow-private；未知供应商被拒。
func TestSearchProviderFlag(t *testing.T) {
	for _, tc := range []struct {
		provider, allowPrivate string
		hasKey                 bool
		err                    string
	}{
		{"ddg_lite", "", false, ""},
		{"tavily", "", true, ""},
		{"serper", "", true, ""},
		{"tavily", "", false, "AGENTBOX_SEARCH_API_KEY"},
		{"serper", "", false, "AGENTBOX_SEARCH_API_KEY"},
		{"fake", "", false, "--upstream-allow-private"},
		{"fake", "127.0.0.1", false, ""},
		{"google", "", true, "ddg_lite、tavily、serper 或 fake"},
	} {
		err := checkSearchProvider(tc.provider, tc.allowPrivate, tc.hasKey)
		if (err == nil) != (tc.err == "") || (err != nil && !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("%s key=%v：%v，期望 %q", tc.provider, tc.hasKey, err, tc.err)
		}
	}
	if runtime.GOOS != "linux" {
		return // 其他平台的 runServer 只报告不支持
	}
	t.Setenv("AGENTBOX_SEARCH_API_KEY", "")
	var stderr strings.Builder
	if code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--search-provider", "serper"}, &stderr); code != 2 ||
		!strings.Contains(stderr.String(), "AGENTBOX_SEARCH_API_KEY") {
		t.Fatalf("serper 无 Key 时 runServer 退出码 %d（%s），期望 2 并拒绝启动", code, stderr.String())
	}
}

// TestPlaintextListenWarning：非 loopback 监听且未启用内置 TLS 时 server 在 stderr 打印一行警告。
func TestPlaintextListenWarning(t *testing.T) {
	for _, tc := range []struct {
		listen string
		tls    bool
		warn   bool
	}{
		{"0.0.0.0:8080", false, true}, {":8080", false, true}, {"10.0.0.5:8080", false, true},
		{"0.0.0.0:8080", true, false}, {"127.0.0.1:8080", false, false}, {"localhost:8080", false, false}, {"[::1]:8080", false, false},
	} {
		got := plaintextListenWarning(tc.listen, tc.tls)
		if (got != "") != tc.warn {
			t.Errorf("%s tls=%v: 警告 %q，期望警告 = %v", tc.listen, tc.tls, got, tc.warn)
		}
		if tc.warn && (!strings.HasPrefix(got, "warning: ") || !strings.Contains(got, tc.listen) || !strings.Contains(got, "without TLS")) {
			t.Errorf("警告文本 %q 不含前缀、地址或 without TLS", got)
		}
	}
}

// TestCallDeadlineFlags：--call-deadline（搜索与抓取，默认 120 s）与 --model-call-deadline（模型调用，默认 300 s）
// 须 > 0，否则拒绝启动（退出码 2，在取得锁与连接数据库之前）；合法值原样进入 Gateway 限额。
func TestCallDeadlineFlags(t *testing.T) {
	lim, err := gatewayLimits(90*time.Second, 10*time.Minute)
	if err != nil || lim.CallDeadline != 90*time.Second || lim.ModelCallDeadline != 10*time.Minute {
		t.Fatalf("gatewayLimits = %+v, %v", lim, err)
	}
	for _, tc := range []struct {
		call, model time.Duration
		flag        string
	}{
		{0, time.Minute, "--call-deadline"}, {-time.Second, time.Minute, "--call-deadline"},
		{time.Minute, 0, "--model-call-deadline"}, {time.Minute, -time.Second, "--model-call-deadline"},
	} {
		if _, err := gatewayLimits(tc.call, tc.model); err == nil || !strings.Contains(err.Error(), tc.flag) {
			t.Errorf("call=%s model=%s：%v，期望提到 %s", tc.call, tc.model, err, tc.flag)
		}
	}
	if runtime.GOOS != "linux" {
		return // 其他平台的 runServer 只报告不支持
	}
	// -help 列出两个标志及其默认值（120 s 与 300 s）。
	var stderr strings.Builder
	if code := runServer([]string{"-help"}, &stderr); code != 2 ||
		!strings.Contains(stderr.String(), "-call-deadline duration") || !strings.Contains(stderr.String(), "(default 2m0s)") ||
		!strings.Contains(stderr.String(), "-model-call-deadline duration") || !strings.Contains(stderr.String(), "(default 5m0s)") {
		t.Fatalf("-help 退出码 %d：%s", code, stderr.String())
	}
	stderr.Reset()
	if code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--model-call-deadline", "0s"}, &stderr); code != 2 ||
		!strings.Contains(stderr.String(), "--model-call-deadline") {
		t.Fatalf("--model-call-deadline 0s 时 runServer 退出码 %d（%s），期望 2 并拒绝启动", code, stderr.String())
	}
}

// ---- 用户账号（Plan 11 Task 4） ----

// 配置了 --model-base-url 即启用账号：--user-orchestrator-model（默认 kimi-k3）与 --user-worker-model（默认
// kimi-k2.6）须为声明的模型，否则在取得锁与连接数据库之前以退出码 2 拒绝启动。
func TestUserModelFlags(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("runServer 只在 Linux 上可用")
	}
	var stderr strings.Builder
	if code := runServer([]string{"-help"}, &stderr); code != 2 ||
		!strings.Contains(stderr.String(), "-user-orchestrator-model string") || !strings.Contains(stderr.String(), `(default "kimi-k3")`) ||
		!strings.Contains(stderr.String(), "-user-worker-model string") || !strings.Contains(stderr.String(), `(default "kimi-k2.6")`) {
		t.Fatalf("-help 退出码 %d：%s", code, stderr.String())
	}
	base := []string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--model-base-url", "https://api.moonshot.cn/v1", "--model-name", "kimi-k2.6"}
	for _, tc := range []struct {
		extra []string
		flag  string
	}{
		{nil, "--user-orchestrator-model"}, // 默认 kimi-k3 未声明
		{[]string{"--models", "kimi-k2.6,kimi-k3", "--user-worker-model", "gpt-x"}, "--user-worker-model"},
	} {
		stderr.Reset()
		if code := runServer(append(append([]string{}, base...), tc.extra...), &stderr); code != 2 || !strings.Contains(stderr.String(), tc.flag) {
			t.Errorf("%v：退出码 %d（%s），期望 2 并指出 %s", tc.extra, code, stderr.String(), tc.flag)
		}
	}
}

// userTestDatabase 建立独立的已迁移测试库（读 AGENTBOX_TEST_DATABASE_URL；CI 中缺失即失败）。
func userTestDatabase(t *testing.T) (string, *postgres.Store) {
	t.Helper()
	admin := os.Getenv("AGENTBOX_TEST_DATABASE_URL")
	if admin == "" {
		if os.Getenv("CI") == "true" {
			t.Fatal("CI 中必须设置 AGENTBOX_TEST_DATABASE_URL")
		}
		t.Skip("未设置 AGENTBOX_TEST_DATABASE_URL，跳过")
	}
	ctx := context.Background()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	name := "agentbox_cli_" + hex.EncodeToString(b)
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("连接测试数据库: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() // 只用于建库，关闭错误无影响
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("创建测试数据库: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }() // 清理尽力而为
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	s, err := postgres.Open(ctx, postgres.Options{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.InitializeInstallation(ctx, "install-cli", make([]byte, ownership.TokenSize)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteInstallation(ctx, "install-cli"); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return u.String(), s
}

// agentbox user：list 输出 username role disabled created_at；disable 吊销该用户的全部会话（不影响他人），
// enable 恢复登录（已吊销的会话不恢复）；未知用户退出码 1；连接串（取自环境变量时）不出现在任何输出中。
func TestUserCommand(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("agentbox user 只在 Linux 上可用")
	}
	dsn, s := userTestDatabase(t)
	ctx := context.Background()
	sessions := map[string][]byte{}
	for _, name := range []string{"Alice", "bob"} {
		_, key, err := account.NormalizeUsername(name)
		if err != nil {
			t.Fatal(err)
		}
		u, err := s.CreateUser(ctx, name, key, "pbkdf2-sha256$600000$c2FsdA$aGFzaA")
		if err != nil {
			t.Fatal(err)
		}
		_, hash, err := account.NewSessionID()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CreateSession(ctx, hash, u.ID, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		sessions[name] = hash
	}
	t.Setenv("AGENTBOX_DATABASE_URL", dsn)
	run := func(args ...string) (int, string, string) {
		t.Helper()
		var out, errb strings.Builder
		code := runUser(args, &out, &errb)
		if all := out.String() + errb.String(); strings.Contains(all, dsn) || strings.Contains(all, "agentbox_cli_") {
			t.Fatalf("输出中出现了连接串：%s", all)
		}
		return code, out.String(), errb.String()
	}
	listed := func() map[string][]string {
		t.Helper()
		code, out, errb := run("list")
		lines := strings.Split(strings.TrimSpace(out), "\n")
		if code != 0 || strings.Join(strings.Fields(lines[0]), " ") != "username role disabled created_at" {
			t.Fatalf("user list = %d %q %q", code, out, errb)
		}
		rows := map[string][]string{}
		for _, l := range lines[1:] {
			f := strings.Fields(l)
			if len(f) != 4 {
				t.Fatalf("user list 行 %q", l)
			}
			if _, err := time.Parse(time.RFC3339, f[3]); err != nil {
				t.Fatalf("created_at %q: %v", f[3], err)
			}
			rows[f[0]] = f[1:3]
		}
		return rows
	}
	if rows := listed(); len(rows) != 2 || strings.Join(rows["Alice"], " ") != "user false" || strings.Join(rows["bob"], " ") != "user false" {
		t.Fatalf("user list = %v", rows)
	}

	if code, out, errb := run("disable", "alice"); code != 0 || !strings.Contains(out, "alice") {
		t.Fatalf("user disable alice = %d %q %q", code, out, errb)
	}
	if _, err := s.SessionUser(ctx, sessions["Alice"]); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("停用后 Alice 的会话应失效，得到 %v", err)
	}
	if u, err := s.SessionUser(ctx, sessions["bob"]); err != nil || u.Username != "bob" {
		t.Fatalf("bob 的会话不应受影响：%+v %v", u, err)
	}
	if rows := listed(); strings.Join(rows["Alice"], " ") != "user true" {
		t.Fatalf("停用后 user list = %v", rows)
	}
	if u, _, err := s.UserForLogin(ctx, "alice"); err != nil || !u.Disabled {
		t.Fatalf("停用后 UserForLogin = %+v %v", u, err)
	}

	// 标志可以写在用户名之后。
	if code, out, errb := run("enable", "Alice", "--database-url", dsn); code != 0 || !strings.Contains(out, "Alice") {
		t.Fatalf("user enable Alice = %d %q %q", code, out, errb)
	}
	if u, _, err := s.UserForLogin(ctx, "alice"); err != nil || u.Disabled {
		t.Fatalf("启用后 UserForLogin = %+v %v", u, err)
	}
	if _, err := s.SessionUser(ctx, sessions["Alice"]); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("启用不恢复已吊销的会话，得到 %v", err)
	}

	for _, name := range []string{"carol", "x"} { // 不存在 / 不符合用户名规则
		for _, op := range []string{"disable", "enable"} {
			if code, _, errb := run(op, name); code != 1 || !strings.Contains(errb, "不存在") {
				t.Errorf("user %s %s = %d %q，期望退出码 1", op, name, code, errb)
			}
		}
	}
	for _, args := range [][]string{{}, {"list", "extra"}, {"disable"}, {"remove", "bob"}, {"list", "--bogus"}} {
		if code, _, _ := run(args...); code != 2 {
			t.Errorf("user %v 退出码 %d，期望 2（用法错误）", args, code)
		}
	}
	t.Setenv("AGENTBOX_DATABASE_URL", "")
	if code, _, errb := run("list"); code != 2 || !strings.Contains(errb, "--database-url") {
		t.Fatalf("没有连接串时 user list = %d %q", code, errb)
	}
}

// ---- M4 Plan 12 Task 9：会话标志 ----

// 会话标志：--turn-tool-budget 为 1–1000（默认 30），--session-evict-after 须大于 --session-idle-freeze，
// --session-worker-argv 需要用户账号；违反时 runServer 在取得锁与连接数据库之前以退出码 2 返回。
func TestSessionFlags(t *testing.T) {
	var cfg app.Config
	cfg.Accounts = true
	if err := sessionFlags(&cfg, 30, 10*time.Minute, time.Hour, "python3,-m,chatagent"); err != nil ||
		cfg.TurnToolBudget != 30 || cfg.SessionIdleFreeze != 10*time.Minute || cfg.SessionEvictAfter != time.Hour ||
		strings.Join(cfg.SessionWorkerArgv, " ") != "python3 -m chatagent" {
		t.Fatalf("合法的会话标志 = %v，cfg = %+v", err, cfg)
	}
	for _, tc := range []struct {
		budget        int
		idle, evict   time.Duration
		argv          string
		accounts      bool
		wantInMessage string
	}{
		{0, time.Minute, time.Hour, "", true, "--turn-tool-budget"},
		{1001, time.Minute, time.Hour, "", true, "--turn-tool-budget"},
		{30, 0, time.Hour, "", true, "--session-idle-freeze"},
		{30, time.Hour, time.Hour, "", true, "--session-evict-after"},
		{30, time.Minute, time.Hour, "w", false, "--model-base-url"},
	} {
		c := app.Config{Accounts: tc.accounts}
		if err := sessionFlags(&c, tc.budget, tc.idle, tc.evict, tc.argv); err == nil || !strings.Contains(err.Error(), tc.wantInMessage) {
			t.Errorf("%+v：%v，期望指出 %s", tc, err, tc.wantInMessage)
		}
	}
	// sub-run 扩展的标志（M4 Plan 14）：T_subrun_cancel 须 > 0。
	if err := subrunFlags(&cfg, true, 10*time.Second); err != nil || !cfg.WorkerSubruns || cfg.Runner.SubrunCancelTimeout != 10*time.Second {
		t.Fatalf("合法的 sub-run 标志 = %v，cfg = %+v", err, cfg)
	}
	for _, d := range []time.Duration{0, -time.Second} {
		if err := subrunFlags(&app.Config{}, true, d); err == nil || !strings.Contains(err.Error(), "--subrun-cancel-timeout") {
			t.Errorf("--subrun-cancel-timeout %s：%v，期望拒绝", d, err)
		}
	}
	if runtime.GOOS != "linux" {
		t.Skip("runServer 只在 Linux 上可用")
	}
	var stderr strings.Builder
	if code := runServer([]string{"-help"}, &stderr); code != 2 || !strings.Contains(stderr.String(), "-worker-subruns") ||
		!strings.Contains(stderr.String(), "-subrun-cancel-timeout duration") || !strings.Contains(stderr.String(), "(default 10s)") ||
		!strings.Contains(stderr.String(), "-turn-tool-budget int") ||
		!strings.Contains(stderr.String(), "(default 30)") || !strings.Contains(stderr.String(), "(default 10m0s)") ||
		!strings.Contains(stderr.String(), "(default 1h0m0s)") || !strings.Contains(stderr.String(), "-session-worker-argv string") {
		t.Fatalf("-help 退出码 %d：%s", code, stderr.String())
	}
	base := []string{"--data-dir", t.TempDir(), "--database-url", "postgres://x"}
	for _, extra := range [][]string{
		{"--session-idle-freeze", "1h", "--session-evict-after", "1h"},
		{"--turn-tool-budget", "0"},
		{"--subrun-cancel-timeout", "0s"},
	} {
		stderr.Reset()
		if code := runServer(append(append([]string{}, base...), extra...), &stderr); code != 2 {
			t.Errorf("%v：退出码 %d（%s），期望 2", extra, code, stderr.String())
		}
	}
}

// exec 标志（M4 Plan 15，计划 D4）：默认值即计划的默认值；--exec-slots 0 关闭 exec（其余标志不检查）；其余标志须为正，
// 每任务上限不超过全局 slots，默认值不超过上限；违反时 runServer 在取得锁与连接数据库之前以退出码 2 返回。
func TestExecFlags(t *testing.T) {
	def := app.ExecConfig{Slots: app.DefaultExecSlots, PerTask: app.DefaultExecPerTask, CountLimit: app.DefaultExecCountLimit,
		CPUSeconds: app.DefaultExecCPUSeconds, WallLimit: app.DefaultExecWallLimit, WallDefault: app.DefaultExecWallDefault,
		WallMax: app.DefaultExecWallMax, MemoryDefault: app.DefaultExecMemoryDefault, MemoryMax: app.DefaultExecMemoryMax,
		QueueTimeout: app.DefaultExecQueueTimeout, PidsMax: app.DefaultExecPidsMax, CPUQuotaUs: app.DefaultExecCPUQuotaUs,
		TmpBytes: app.DefaultExecTmpBytes, OutBytes: app.DefaultExecOutBytes}
	if got, err := execFlags(def); err != nil || got != def || !got.Enabled() {
		t.Fatalf("默认 exec 标志 = %+v, %v", got, err)
	}
	if def.Slots != 4 || def.PerTask != 2 || def.CountLimit != 50 || def.CPUSeconds != 600 || def.WallLimit != 1800*time.Second ||
		def.WallDefault != time.Minute || def.WallMax != 5*time.Minute || def.MemoryDefault != 512<<20 || def.MemoryMax != 1<<30 ||
		def.QueueTimeout != time.Minute || def.PidsMax != 128 || def.CPUQuotaUs != 100000 || def.OutBytes != 64<<20 {
		t.Fatalf("exec 默认值与计划 D4 不符：%+v", def)
	}
	off := def
	off.Slots, off.PerTask = 0, 0
	if got, err := execFlags(off); err != nil || got.Enabled() {
		t.Fatalf("--exec-slots 0 = %+v, %v，期望关闭且不检查其余标志", got, err)
	}
	for _, tc := range []struct {
		mod  func(e *app.ExecConfig)
		want string
	}{
		{func(e *app.ExecConfig) { e.Slots = -1 }, "--exec-slots"},
		{func(e *app.ExecConfig) { e.PerTask = 5 }, "每任务上限"},
		{func(e *app.ExecConfig) { e.WallDefault = 10 * time.Minute }, "wall 默认值"},
		{func(e *app.ExecConfig) { e.MemoryDefault = 2 << 30 }, "内存默认值"},
		{func(e *app.ExecConfig) { e.QueueTimeout = 0 }, "--exec-queue-timeout"},
		{func(e *app.ExecConfig) { e.OutBytes = -1 }, "--exec-out-bytes"},
	} {
		e := def
		tc.mod(&e)
		if _, err := execFlags(e); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v：%v，期望指出 %s", e, err, tc.want)
		}
	}
	if runtime.GOOS != "linux" {
		t.Skip("runServer 只在 Linux 上可用")
	}
	var stderr strings.Builder
	if code := runServer([]string{"-help"}, &stderr); code != 2 || !strings.Contains(stderr.String(), "-exec-slots int") ||
		!strings.Contains(stderr.String(), "-exec-memory-max int") || !strings.Contains(stderr.String(), "(default 1073741824)") {
		t.Fatalf("-help 退出码 %d：%s", code, stderr.String())
	}
	stderr.Reset()
	if code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--exec-per-task", "9"}, &stderr); code != 2 {
		t.Errorf("--exec-per-task 9：退出码 %d（%s），期望 2", code, stderr.String())
	}
}
