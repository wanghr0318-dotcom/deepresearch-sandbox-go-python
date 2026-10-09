package app

// serverInfo 给出 GET /server-info（运维专用）的非机密配置：评测（agentbox eval）的 run manifest 据此固定被评测的
// 服务端。只含模型名、供应商名、命令与摘要；不含 Key、token、价格与上游地址。上游地址与价格配置只以
// upstream_fingerprint（其 sha256 的前 16 个十六进制字符）出现：两次评测的上游或价格不同即可在对比中看出，
// 而不暴露地址本身。

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

func (s *server) serverInfo() map[string]any {
	c := s.cfg
	models := map[string]any{"default": c.Model.Name, "declared": declaredModels(c.Model)}
	if c.Accounts {
		models["user_orchestrator"] = c.UserOrchestratorModel
		models["user_worker"] = c.UserWorkerModel
	}
	if c.Model.BaseURL == "" {
		models = map[string]any{}
	}
	search := c.SearchProvider
	if search == "" {
		search = "ddg_lite"
	}
	info := map[string]any{
		"template":             c.Template,
		"worker_argv":          append([]string{}, c.WorkerArgv...),
		"models":               models,
		"search_provider":      search,
		"upstream_fingerprint": upstreamFingerprint(c),
		"accounts":             c.Accounts,
		"sessions":             c.sessionsEnabled(),
		"worker_subruns":       c.WorkerSubruns,
		"exec":                 map[string]any{"enabled": c.Exec.Enabled(), "slots": c.Exec.Slots, "image_digest": s.execDigest},
	}
	if len(c.Model.Fallbacks) > 0 {
		routes := []string{PrimaryRoute}
		for _, fb := range c.Model.Fallbacks {
			routes = append(routes, fb.Name)
		}
		info["model_routes"] = routes // 降级链的路由名（顺序即优先级）；地址与价格只进入 upstream_fingerprint
	}
	if c.sessionsEnabled() {
		info["session_worker_argv"] = append([]string{}, c.SessionWorkerArgv...)
	}
	return info
}

// upstreamFingerprint 是模型与搜索上游配置（地址、默认模型与白名单、单价、max_tokens 上限、降级链的路由、地址、
// 模型映射、单价与路由参数、搜索供应商与地址）的摘要；不含任何 Key。
func upstreamFingerprint(c Config) string {
	byModel := sortedPricing(c.Model.PricingByModel)
	type fallback struct {
		Name, BaseURL  string
		Models         map[string]string
		Pricing        any
		PricingByModel []string
	}
	fbs := make([]fallback, 0, len(c.Model.Fallbacks))
	for _, fb := range c.Model.Fallbacks { // 不含 APIKey
		fbs = append(fbs, fallback{fb.Name, fb.BaseURL, fb.Models, fb.Pricing, sortedPricing(fb.PricingByModel)})
	}
	b, _ := json.Marshal(struct {
		BaseURL, Name     string
		Models            []string
		Pricing           any
		PricingByModel    []string
		MaxTokensCap      int
		Search, SearchURL string
		Fallbacks         []fallback
		Routing           any
	}{c.Model.BaseURL, c.Model.Name, c.Model.Models, c.Model.Pricing, byModel, c.Model.MaxTokensCap,
		c.SearchProvider, c.SearchBaseURL, fbs, c.Model.Routing})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func declaredModels(m ModelConfig) []string {
	out := []string{}
	if len(m.Models) == 0 {
		if m.Name != "" {
			out = append(out, m.Name)
		}
		return out
	}
	return append(out, m.Models...)
}

func sortedPricing(m map[string]upstream.Pricing) []string {
	out := make([]string, 0, len(m))
	for name, p := range m {
		b, _ := json.Marshal(p)
		out = append(out, name+"="+string(b))
	}
	sort.Strings(out)
	return out
}
