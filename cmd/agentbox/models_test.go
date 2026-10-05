package main

import (
	"flag"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
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
