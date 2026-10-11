package app

import (
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// Provider protocols, cache prices and routing modes in the configuration (model gateway design §7.6).
func TestFallbackProtocolsAndRoutingModes(t *testing.T) {
	base := func() Config {
		c := testConfig().withDefaults()
		c.Model = ModelConfig{BaseURL: "https://m.example/v1", Name: "a", Fallbacks: []ModelFallback{
			{Name: "claude", Protocol: llmproto.ProtocolAnthropic, AnthropicVersion: "2023-06-01", BaseURL: "https://api.anthropic.com",
				Pricing: upstream.Pricing{InputMicroPerMTok: 3, OutputMicroPerMTok: 15, CacheReadMicroPerMTok: 1, CacheWriteMicroPerMTok: 4}}}}
		return c
	}
	if err := base().validate(); err != nil {
		t.Fatalf("anthropic fallback refused: %v", err)
	}
	for _, tc := range []struct {
		name string
		mod  func(*Config)
		err  string
	}{
		{"unknown protocol", func(c *Config) { c.Model.Fallbacks[0].Protocol = "gemini" }, "protocol"},
		{"anthropic_version on openai", func(c *Config) { c.Model.Fallbacks[0].Protocol = "" }, "anthropic_version"},
		{"negative cache price", func(c *Config) { c.Model.Fallbacks[0].Pricing.CacheWriteMicroPerMTok = -1 }, "不能为负数"},
		{"weighted", func(c *Config) {
			c.Model.Routing = call.RoutingConfig{Mode: call.RoutingWeighted, Weights: map[string]int{"primary": 1, "claude": 3}}
		}, ""},
		{"latency", func(c *Config) { c.Model.Routing = call.RoutingConfig{Mode: call.RoutingLatency} }, ""},
		{"unknown mode", func(c *Config) { c.Model.Routing = call.RoutingConfig{Mode: "fastest"} }, "路由模式"},
		{"weight of unknown route", func(c *Config) { c.Model.Routing = call.RoutingConfig{Weights: map[string]int{"nope": 1}} }, "权重"},
		{"weight 0", func(c *Config) { c.Model.Routing = call.RoutingConfig{Weights: map[string]int{"claude": 0}} }, "权重"},
		{"bad anthropic_version", func(c *Config) { c.Model.Fallbacks[0].AnthropicVersion = "2023-06-01\nX" }, "YYYY-MM-DD"},
		{"ordered with weights", func(c *Config) {
			c.Model.Routing = call.RoutingConfig{Mode: call.RoutingOrdered, Weights: map[string]int{"claude": 2}}
		}, "ordered"},
		{"mode without fallbacks", func(c *Config) {
			c.Model.Fallbacks = nil
			c.Model.Routing = call.RoutingConfig{Mode: call.RoutingWeighted}
		}, "需要后备供应商"},
	} {
		c := base()
		tc.mod(&c)
		err := c.validate()
		switch {
		case tc.err == "" && err != nil:
			t.Errorf("%s: want ok, got %v", tc.name, err)
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: want error containing %q, got %v", tc.name, tc.err, err)
		}
	}

	// The adapter gets the protocol; pricing versions name the cache prices only when they are set.
	cfg := base().withDefaults()
	if v := cfg.Model.Fallbacks[0].Pricing.Version; v != "config/in=3,out=15,cache_read=1,cache_write=4" {
		t.Fatalf("pricing version %q", v)
	}
	adapters, _, _ := cfg.gatewayAdapters(upstream.NewDialer(upstream.DialerConfig{}))
	var chat upstream.ChatAdapter
	for _, a := range adapters {
		if ca, ok := a.(upstream.ChatAdapter); ok {
			chat = ca
		}
	}
	if chat == nil || chat.RouteProtocol(0) != llmproto.ProtocolOpenAI || chat.RouteProtocol(1) != llmproto.ProtocolAnthropic {
		t.Fatal("chat adapter lacks the Anthropic route")
	}

	// The upstream fingerprint changes with the protocol and with the routing mode.
	openai := base()
	openai.Model.Fallbacks[0].Protocol, openai.Model.Fallbacks[0].AnthropicVersion = "", ""
	weighted := base()
	weighted.Model.Routing.Mode = call.RoutingWeighted
	fps := map[string]bool{upstreamFingerprint(base()): true, upstreamFingerprint(openai): true, upstreamFingerprint(weighted): true}
	if len(fps) != 3 {
		t.Fatal("upstream_fingerprint does not reflect protocol or routing mode")
	}
	// The default anthropic_version written out is the same configuration as leaving it out.
	omitted := base()
	omitted.Model.Fallbacks = []ModelFallback{omitted.Model.Fallbacks[0]}
	omitted.Model.Fallbacks[0].AnthropicVersion = ""
	if upstreamFingerprint(omitted) != upstreamFingerprint(base()) {
		t.Fatal("explicit default anthropic_version changes the fingerprint")
	}
	other := base()
	other.Model.Fallbacks = []ModelFallback{other.Model.Fallbacks[0]}
	other.Model.Fallbacks[0].AnthropicVersion = "2024-01-01"
	if upstreamFingerprint(other) == upstreamFingerprint(base()) {
		t.Fatal("a different anthropic_version does not change the fingerprint")
	}
	// "openai" written out is the same configuration as the default.
	explicit := openai
	explicit.Model.Fallbacks = []ModelFallback{openai.Model.Fallbacks[0]}
	explicit.Model.Fallbacks[0].Protocol = llmproto.ProtocolOpenAI
	if upstreamFingerprint(explicit) != upstreamFingerprint(openai) {
		t.Fatal("protocol openai changes the fingerprint")
	}
}
