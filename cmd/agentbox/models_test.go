package main

import (
	"flag"
	"io"
	"runtime"
	"strings"
	"testing"

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
