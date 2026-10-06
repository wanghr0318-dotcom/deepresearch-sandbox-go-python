package main

// 本文件解析 `agentbox server` 的模型标志：--model-name（默认模型）、--models（声明的白名单）、
// --model-price model=IN:OUT（按模型的单价），校验 --search-provider 与调用期限标志。与平台无关，便于在任何平台上测试。

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/app"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/call"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

// plaintextListenWarning 返回非 loopback 监听且未启用内置 TLS 时打印到 stderr 的警告行（§15.3：不拒绝，
// TLS 可以在外部终止）；不需要警告时返回空串。
func plaintextListenWarning(listen string, tls bool) string {
	host, _, err := net.SplitHostPort(listen)
	if tls || err != nil {
		return ""
	}
	if ip := net.ParseIP(host); strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback()) {
		return ""
	}
	return fmt.Sprintf("warning: listening on %s without TLS; the API token would cross the network in plain text", listen)
}

// splitList 拆分逗号分隔的列表，忽略空项。
func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// listFlag 是可重复的标志；每次的值再按逗号拆分。
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	*l = append(*l, splitList(v)...)
	return nil
}

// checkSearchProvider 校验 --search-provider（在取得锁、连接数据库之前）：fake 只用于测试，须同时设置
// --upstream-allow-private；tavily 与 serper 须有宿主环境变量 AGENTBOX_SEARCH_API_KEY（hasKey 只表示是否
// 设置，Key 的值不进入本函数与错误文本）。
func checkSearchProvider(provider, allowPrivate string, hasKey bool) error {
	switch provider {
	case upstream.SearchDDGLite:
	case upstream.SearchTavily, upstream.SearchSerper:
		if !hasKey {
			return fmt.Errorf("--search-provider %s 需要宿主环境变量 AGENTBOX_SEARCH_API_KEY", provider)
		}
	case upstream.SearchFake:
		if strings.TrimSpace(allowPrivate) == "" {
			return errors.New("--search-provider fake 只用于测试，须同时设置 --upstream-allow-private 指向本机 fake upstream")
		}
	default:
		return fmt.Errorf("--search-provider 须为 ddg_lite、tavily、serper 或 fake，得到 %q", provider)
	}
	return nil
}

// modelFlags 是模型相关标志的原始值。
type modelFlags struct {
	BaseURL, Name, Models string
	Prices                []string // model=IN:OUT（每百万 token 的微美元）
	PriceIn, PriceOut     int64    // 未在 Prices 中给出的模型的单价
	MaxTokensCap          int      // max_tokens 上限（0 = adapter 默认值；不能为负）
}

// modelConfig 校验模型标志并构造 app.ModelConfig（不含 Key）：
//   - 未给 --model-base-url 时不提供模型端点，--models 与 --model-price 不能使用；
//   - --models 非空时须包含 --model-name，且不能重复；空时只声明 --model-name；
//   - --model-price 的模型须已声明，每个模型至多一项，单价为非负整数。
func modelConfig(f modelFlags) (app.ModelConfig, error) {
	m := app.ModelConfig{BaseURL: f.BaseURL, Name: f.Name,
		Pricing: upstream.Pricing{InputMicroPerMTok: f.PriceIn, OutputMicroPerMTok: f.PriceOut}, MaxTokensCap: f.MaxTokensCap}
	if f.MaxTokensCap < 0 {
		return app.ModelConfig{}, errors.New("--model-max-tokens-cap 不能为负数")
	}
	if f.PriceIn < 0 || f.PriceOut < 0 {
		return app.ModelConfig{}, errors.New("--model-price-in/out-micro-per-mtok 不能为负数")
	}
	models := splitList(f.Models)
	if f.BaseURL == "" {
		if len(models) > 0 || len(f.Prices) > 0 {
			return app.ModelConfig{}, errors.New("--models 与 --model-price 需要 --model-base-url")
		}
		return m, nil
	}
	if f.Name == "" {
		return app.ModelConfig{}, errors.New("配置了 --model-base-url 时须给出 --model-name（默认模型）")
	}
	for i, name := range models {
		if slices.Contains(models[:i], name) {
			return app.ModelConfig{}, fmt.Errorf("--models 中模型 %q 重复", name)
		}
	}
	if len(models) > 0 && !slices.Contains(models, f.Name) {
		return app.ModelConfig{}, fmt.Errorf("--models %s 不含 --model-name %q（默认模型须在白名单中）", f.Models, f.Name)
	}
	m.Models = models
	for _, entry := range f.Prices {
		name, p, err := parseModelPrice(entry)
		if err != nil {
			return app.ModelConfig{}, err
		}
		if name != f.Name && !slices.Contains(models, name) {
			return app.ModelConfig{}, fmt.Errorf("--model-price %q：模型 %q 未声明（--model-name / --models）", entry, name)
		}
		if _, dup := m.PricingByModel[name]; dup {
			return app.ModelConfig{}, fmt.Errorf("--model-price：模型 %q 的单价给出了多次", name)
		}
		if m.PricingByModel == nil {
			m.PricingByModel = map[string]upstream.Pricing{}
		}
		m.PricingByModel[name] = p
	}
	return m, nil
}

// parseModelPrice 解析 model=IN:OUT（每百万 token 的微美元，非负整数）。
func parseModelPrice(entry string) (string, upstream.Pricing, error) {
	name, prices, ok := strings.Cut(entry, "=")
	in, out, ok2 := strings.Cut(prices, ":")
	name = strings.TrimSpace(name)
	if !ok || !ok2 || name == "" {
		return "", upstream.Pricing{}, fmt.Errorf("--model-price %q 须为 model=IN:OUT", entry)
	}
	pi, err1 := strconv.ParseInt(strings.TrimSpace(in), 10, 64)
	po, err2 := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err1 != nil || err2 != nil || pi < 0 || po < 0 {
		return "", upstream.Pricing{}, fmt.Errorf("--model-price %q：单价须为非负整数（每百万 token 的微美元）", entry)
	}
	return name, upstream.Pricing{InputMicroPerMTok: pi, OutputMicroPerMTok: po}, nil
}

// gatewayLimits 校验调用期限标志并构造 Gateway 限额：--call-deadline 用于搜索与抓取，--model-call-deadline
// 用于 /v1/chat/completions（推理模型的长输出可以合法地超过 120 s）。两者都须 > 0。
func gatewayLimits(callDeadline, modelCallDeadline time.Duration) (call.Limits, error) {
	if callDeadline <= 0 {
		return call.Limits{}, fmt.Errorf("--call-deadline 须大于 0，得到 %s", callDeadline)
	}
	if modelCallDeadline <= 0 {
		return call.Limits{}, fmt.Errorf("--model-call-deadline 须大于 0，得到 %s", modelCallDeadline)
	}
	return call.Limits{CallDeadline: callDeadline, ModelCallDeadline: modelCallDeadline}, nil
}

// sessionFlags 校验会话标志（M4 Plan 12）并写入 cfg：--turn-tool-budget 为 1–1000；--session-idle-freeze > 0；
// --session-evict-after 须大于 --session-idle-freeze；--session-worker-argv（逗号分隔）为空时不启用会话，非空时需要
// 用户账号（--model-base-url）。不合法时 runServer 在取得锁与连接数据库之前以退出码 2 返回。
func sessionFlags(cfg *app.Config, budget int, idleFreeze, evictAfter time.Duration, workerArgv string) error {
	switch {
	case budget < 1 || budget > app.MaxTurnToolBudget:
		return fmt.Errorf("--turn-tool-budget 须为 1–%d，得到 %d", app.MaxTurnToolBudget, budget)
	case idleFreeze <= 0:
		return fmt.Errorf("--session-idle-freeze 须大于 0，得到 %s", idleFreeze)
	case evictAfter <= idleFreeze:
		return fmt.Errorf("--session-evict-after（%s）须大于 --session-idle-freeze（%s）", evictAfter, idleFreeze)
	}
	argv := splitList(workerArgv)
	if len(argv) > 0 && !cfg.Accounts {
		return errors.New("--session-worker-argv 需要用户账号（--model-base-url）：会话只属于登录用户")
	}
	cfg.TurnToolBudget, cfg.SessionIdleFreeze, cfg.SessionEvictAfter, cfg.SessionWorkerArgv = budget, idleFreeze, evictAfter, argv
	return nil
}
