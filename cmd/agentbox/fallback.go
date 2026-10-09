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
//	   "model_prices": {"kimi-k3": {"in": 4000000, "out": 16000000}}}
//	]}

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
)

type fallbackPrice struct {
	In  *int64 `json:"in"`
	Out *int64 `json:"out"`
}

type fallbackProvider struct {
	Name        string                   `json:"name"`
	BaseURL     string                   `json:"base_url"`
	KeyEnv      string                   `json:"key_env"`
	Models      map[string]string        `json:"models"`
	Price       *fallbackPrice           `json:"price"`
	ModelPrices map[string]fallbackPrice `json:"model_prices"`
}

type fallbackFile struct {
	Providers []fallbackProvider `json:"providers"`
}

var envNameRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func (p fallbackPrice) pricing(what string) (upstream.Pricing, error) {
	if p.In == nil || p.Out == nil || *p.In < 0 || *p.Out < 0 {
		return upstream.Pricing{}, fmt.Errorf("%s 须给出非负整数 in 与 out（每百万 token 的微美元）", what)
	}
	return upstream.Pricing{InputMicroPerMTok: *p.In, OutputMicroPerMTok: *p.Out}, nil
}

// modelFallbacks 读取并校验 --model-fallback-file（path 为空时没有后备供应商）。每个供应商须给出 name、base_url 与
// price；key_env 可省略（无鉴权的本机服务），给出时须为环境变量名且已设置为非空值。Key 的值不进入错误文本。
// 名字、地址与模型映射的其余规则由 app.Config 校验（启动前以退出码 2 拒绝）。
func modelFallbacks(path string, getenv func(string) string) ([]app.ModelFallback, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--model-fallback-file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f fallbackFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("--model-fallback-file %s: %w", path, err)
	}
	if len(f.Providers) == 0 {
		return nil, fmt.Errorf("--model-fallback-file %s 没有列出 providers", path)
	}
	var out []app.ModelFallback
	for i, p := range f.Providers {
		what := fmt.Sprintf("--model-fallback-file providers[%d]（%s）", i, p.Name)
		if p.Name == "" || p.BaseURL == "" || p.Price == nil {
			return nil, fmt.Errorf("%s 须给出 name、base_url 与 price", what)
		}
		fb := app.ModelFallback{Name: p.Name, BaseURL: p.BaseURL, Models: p.Models}
		if fb.Pricing, err = p.Price.pricing(what + " 的 price"); err != nil {
			return nil, err
		}
		for model, mp := range p.ModelPrices {
			pr, err := mp.pricing(fmt.Sprintf("%s 的 model_prices[%s]", what, model))
			if err != nil {
				return nil, err
			}
			if fb.PricingByModel == nil {
				fb.PricingByModel = map[string]upstream.Pricing{}
			}
			fb.PricingByModel[model] = pr
		}
		if p.KeyEnv != "" {
			if !envNameRE.MatchString(p.KeyEnv) {
				return nil, fmt.Errorf("%s 的 key_env %q 须为环境变量名（大写字母、数字与 _）", what, p.KeyEnv)
			}
			if fb.APIKey = getenv(p.KeyEnv); fb.APIKey == "" {
				return nil, fmt.Errorf("%s 需要宿主环境变量 %s", what, p.KeyEnv)
			}
		}
		out = append(out, fb)
	}
	return out, nil
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
