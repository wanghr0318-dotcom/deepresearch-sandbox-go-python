package app

import (
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// TestUpstreamFingerprintUnchangedForOpenAIConfigs：只用 OpenAI 兼容供应商的配置（引入 protocol、weight、缓存单价
// 与路由模式之前的全部字段）的 upstream_fingerprint 与之前完全相同（golden 值取自引入这些字段之前的代码）。
func TestUpstreamFingerprintUnchangedForOpenAIConfigs(t *testing.T) {
	single := Config{Model: ModelConfig{BaseURL: "https://m.example/v1", Name: "m1", Models: []string{"m1", "m2"},
		Pricing: upstream.Pricing{InputMicroPerMTok: 2, OutputMicroPerMTok: 8}}, SearchProvider: "serper"}.withDefaults()
	chain := Config{Model: ModelConfig{BaseURL: "https://m.example/v1", Name: "m1", Models: []string{"m1", "m2"},
		Pricing:        upstream.Pricing{InputMicroPerMTok: 2, OutputMicroPerMTok: 8},
		PricingByModel: map[string]upstream.Pricing{"m2": {InputMicroPerMTok: 3, OutputMicroPerMTok: 9}},
		Fallbacks: []ModelFallback{{Name: "backup", BaseURL: "https://b.example/v1", Models: map[string]string{"m1": "v1"},
			Pricing: upstream.Pricing{InputMicroPerMTok: 5, OutputMicroPerMTok: 7}}},
		Routing: call.RoutingConfig{BreakerFailures: 3, BreakerOpen: 30 * time.Second, TryTimeout: time.Second}},
		SearchProvider: "serper"}.withDefaults()
	for _, c := range []struct {
		name string
		cfg  Config
		want string
	}{{"single", single, "a1ed27354c6f6b24"}, {"chain", chain, "00f1ba96196264f6"}} {
		if got := upstreamFingerprint(c.cfg); got != c.want {
			t.Errorf("%s: upstream_fingerprint %s, want %s", c.name, got, c.want)
		}
	}
}
