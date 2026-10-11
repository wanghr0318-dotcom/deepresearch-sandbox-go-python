package main

// 本文件解析 `agentbox server` 的模型降级链配置（docs/design/2026-10-10-model-fallback-design.md）：
// --model-fallback-file 指向 JSON 文件，按顺序列出后备供应商；--model-breaker-failures、--model-breaker-open、
// --model-try-timeout、--model-hedge-delay 配置熔断、每 try 超时与对冲。与平台无关，便于在任何平台上测试。
//
// 文件格式（Key 不写在文件里，只给出宿主环境变量名）：
//
//	{"providers": [
//	  {"name": "backup", "base_url": "https://api.example.com/v1", "key_env": "AGENTBOX_MODEL_BACKUP_API_KEY",
//	   "models": {"kimi-k3": "vendor-model-name"},
//	   "price": {"in": 2000000, "out": 8000000},
//	   "model_prices": {"kimi-k3": {"in": 4000000, "out": 16000000}}},
//	  {"name": "claude", "protocol": "anthropic", "base_url": "https://api.anthropic.com",
//	   "key_env": "AGENTBOX_MODEL_CLAUDE_API_KEY", "anthropic_version": "2023-06-01", "weight": 3,
//	   "models": {"kimi-k3": "claude-model-id"},
//	   "price": {"in": 3000000, "out": 15000000, "cache_read": 300000, "cache_write": 3750000}}
//	],
//	 "routing": {"mode": "weighted", "primary_weight": 1}}
//
// 可选字段（模型网关设计 §7.6；旧文件仍然有效、行为不变）：protocol（openai 默认 | anthropic）、anthropic_version
// （只用于 anthropic，默认 2023-06-01）、weight（≥ 1，默认 1）、price 与 model_prices 的 cache_read、cache_write
// （缺省按输入单价计）、顶层 routing.mode（ordered | weighted | latency；给出任何权重时默认 weighted，否则 ordered）
// 与 routing.primary_weight（主供应商的权重，默认 1）。

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

type fallbackPrice struct {
	In         *int64 `json:"in"`
	Out        *int64 `json:"out"`
	CacheRead  *int64 `json:"cache_read"`
	CacheWrite *int64 `json:"cache_write"`
}

type fallbackProvider struct {
	Name             string                   `json:"name"`
	Protocol         string                   `json:"protocol"`
	BaseURL          string                   `json:"base_url"`
	KeyEnv           string                   `json:"key_env"`
	AnthropicVersion string                   `json:"anthropic_version"`
	Weight           *int                     `json:"weight"`
	Models           map[string]string        `json:"models"`
	Price            *fallbackPrice           `json:"price"`
	ModelPrices      map[string]fallbackPrice `json:"model_prices"`
}

type fallbackRouting struct {
	Mode          string `json:"mode"`
	PrimaryWeight *int   `json:"primary_weight"`
}

type fallbackFile struct {
	Providers []fallbackProvider `json:"providers"`
	Routing   *fallbackRouting   `json:"routing"`
}

// fileRouting 是降级链文件给出的路由模式与权重（路由名 → 权重）；文件没有给出时为零值。
type fileRouting struct {
	Mode    call.RoutingMode
	Weights map[string]int
}

var envNameRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func (p fallbackPrice) pricing(what string) (upstream.Pricing, error) {
	if p.In == nil || p.Out == nil || *p.In < 0 || *p.Out < 0 {
		return upstream.Pricing{}, fmt.Errorf("%s 须给出非负整数 in 与 out（每百万 token 的微美元）", what)
	}
	if (p.CacheRead != nil && *p.CacheRead < 0) || (p.CacheWrite != nil && *p.CacheWrite < 0) {
		return upstream.Pricing{}, fmt.Errorf("%s 的 cache_read 与 cache_write 须为非负整数（每百万 token 的微美元）", what)
	}
	pr := upstream.Pricing{InputMicroPerMTok: *p.In, OutputMicroPerMTok: *p.Out}
	if p.CacheRead != nil {
		pr.CacheReadMicroPerMTok = *p.CacheRead
	}
	if p.CacheWrite != nil {
		pr.CacheWriteMicroPerMTok = *p.CacheWrite
	}
	return pr, nil
}

// optWeight 校验可选的权重（≥ 1）；没有给出时为 0。
func optWeight(w *int, what string) (int, error) {
	if w == nil {
		return 0, nil
	}
	if *w < 1 {
		return 0, fmt.Errorf("%s 的 weight 须为至少 1 的整数，得到 %d", what, *w)
	}
	return *w, nil
}

// modelFallbacks 读取并校验 --model-fallback-file（path 为空时没有后备供应商）。每个供应商须给出 name、base_url 与
// price；key_env 可省略（无鉴权的本机服务），给出时须为环境变量名且已设置为非空值。Key 的值不进入错误文本。
// 名字、地址与模型映射的其余规则由 app.Config 校验（启动前以退出码 2 拒绝）。
func modelFallbacks(path string, getenv func(string) string) ([]app.ModelFallback, error) {
	fbs, _, err := modelFallbackFile(path, getenv)
	return fbs, err
}

// modelFallbackFile 同 modelFallbacks，另返回文件给出的路由模式与权重。旧文件（没有可选字段）得到相同的后备
// 供应商与零值的 fileRouting。
func modelFallbackFile(path string, getenv func(string) string) ([]app.ModelFallback, fileRouting, error) {
	var fr fileRouting
	if path == "" {
		return nil, fr, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fr, fmt.Errorf("--model-fallback-file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f fallbackFile
	if err := dec.Decode(&f); err != nil {
		return nil, fr, fmt.Errorf("--model-fallback-file %s: %w", path, err)
	}
	if len(f.Providers) == 0 {
		return nil, fr, fmt.Errorf("--model-fallback-file %s 没有列出 providers", path)
	}
	weights := map[string]int{}
	if f.Routing != nil {
		switch m := call.RoutingMode(f.Routing.Mode); m {
		case "", call.RoutingOrdered, call.RoutingWeighted, call.RoutingLatency:
			fr.Mode = m
		default:
			return nil, fr, fmt.Errorf("--model-fallback-file %s 的 routing.mode 须为 ordered、weighted 或 latency，得到 %q", path, f.Routing.Mode)
		}
		w, err := optWeight(f.Routing.PrimaryWeight, "--model-fallback-file routing.primary_weight")
		if err != nil {
			return nil, fr, err
		}
		if w > 0 {
			weights[app.PrimaryRoute] = w
		}
	}
	var out []app.ModelFallback
	for i, p := range f.Providers {
		fb, w, err := fallbackFromFile(i, p, getenv)
		if err != nil {
			return nil, fr, err
		}
		if w > 0 {
			weights[fb.Name] = w
		}
		out = append(out, fb)
	}
	if len(weights) > 0 {
		if fr.Mode == call.RoutingOrdered {
			return nil, fr, fmt.Errorf("--model-fallback-file %s：routing.mode 为 ordered 时不能给出 weight 或 primary_weight（权重只用于 weighted 与 latency）", path)
		}
		fr.Weights = weights
	}
	return out, fr, nil
}

// fallbackFromFile 校验文件中的一个后备供应商；w 为它的权重（没有给出时为 0）。
func fallbackFromFile(i int, p fallbackProvider, getenv func(string) string) (app.ModelFallback, int, error) {
	what := fmt.Sprintf("--model-fallback-file providers[%d]（%s）", i, p.Name)
	if p.Name == "" || p.BaseURL == "" || p.Price == nil {
		return app.ModelFallback{}, 0, fmt.Errorf("%s 须给出 name、base_url 与 price", what)
	}
	fb := app.ModelFallback{Name: p.Name, BaseURL: p.BaseURL, Models: p.Models}
	switch llmproto.Protocol(p.Protocol) {
	case "", llmproto.ProtocolOpenAI:
		if p.AnthropicVersion != "" {
			return fb, 0, fmt.Errorf("%s 的 anthropic_version 只用于 protocol anthropic", what)
		}
	case llmproto.ProtocolAnthropic:
		if p.AnthropicVersion != "" && !app.AnthropicVersionRE.MatchString(p.AnthropicVersion) {
			return fb, 0, fmt.Errorf("%s 的 anthropic_version %q 须为 YYYY-MM-DD 形式的日期", what, p.AnthropicVersion)
		}
		fb.Protocol, fb.AnthropicVersion = llmproto.ProtocolAnthropic, p.AnthropicVersion
	default:
		return fb, 0, fmt.Errorf("%s 的 protocol 须为 openai 或 anthropic，得到 %q", what, p.Protocol)
	}
	w, err := optWeight(p.Weight, what)
	if err != nil {
		return fb, 0, err
	}
	if fb.Pricing, err = p.Price.pricing(what + " 的 price"); err != nil {
		return fb, 0, err
	}
	for model, mp := range p.ModelPrices {
		pr, err := mp.pricing(fmt.Sprintf("%s 的 model_prices[%s]", what, model))
		if err != nil {
			return fb, 0, err
		}
		if fb.PricingByModel == nil {
			fb.PricingByModel = map[string]upstream.Pricing{}
		}
		fb.PricingByModel[model] = pr
	}
	if p.KeyEnv != "" {
		if !envNameRE.MatchString(p.KeyEnv) {
			return fb, 0, fmt.Errorf("%s 的 key_env %q 须为环境变量名（大写字母、数字与 _）", what, p.KeyEnv)
		}
		if fb.APIKey = getenv(p.KeyEnv); fb.APIKey == "" {
			return fb, 0, fmt.Errorf("%s 需要宿主环境变量 %s", what, p.KeyEnv)
		}
	}
	return fb, w, nil
}

// modelRouting 校验熔断、每 try 超时与对冲标志：阈值 ≥ 1，打开时长 > 0，超时与对冲延迟 ≥ 0（0 = 关闭）。
func modelRouting(failures int, open, tryTimeout, hedgeDelay time.Duration) (call.RoutingConfig, error) {
	switch {
	case failures < 1:
		return call.RoutingConfig{}, fmt.Errorf("--model-breaker-failures 须至少为 1，得到 %d", failures)
	case open <= 0:
		return call.RoutingConfig{}, errors.New("--model-breaker-open 须大于 0")
	case tryTimeout < 0 || hedgeDelay < 0:
		return call.RoutingConfig{}, errors.New("--model-try-timeout 与 --model-hedge-delay 不能为负（0 = 关闭）")
	}
	return call.RoutingConfig{BreakerFailures: failures, BreakerOpen: open, TryTimeout: tryTimeout, HedgeDelay: hedgeDelay}, nil
}
