package main

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// TestModelFallbackFileGatewayFields: the optional fields of the model gateway design §7.6 (protocol,
// anthropic_version, weight, cache prices, routing.mode, routing.primary_weight). An old file gives the same
// fallbacks and no routing mode or weights; invalid values are refused.
func TestModelFallbackFileGatewayFields(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	env := map[string]string{"AGENTBOX_MODEL_CLAUDE_API_KEY": "sk-ant-secret"}
	getenv := func(k string) string { return env[k] }

	old := write("old.json", `{"providers":[{"name":"backup","base_url":"https://b.example/v1","price":{"in":1,"out":2}}]}`)
	fbs, fr, err := modelFallbackFile(old, getenv)
	if err != nil || len(fbs) != 1 || fbs[0].Protocol != "" || fbs[0].AnthropicVersion != "" || fr.Mode != "" || fr.Weights != nil ||
		fbs[0].Pricing.CacheReadMicroPerMTok != 0 {
		t.Fatalf("old file: %+v %+v %v", fbs, fr, err)
	}

	full := write("full.json", `{"providers":[
		{"name":"claude","protocol":"anthropic","base_url":"https://api.anthropic.com","key_env":"AGENTBOX_MODEL_CLAUDE_API_KEY",
		 "anthropic_version":"2023-06-01","weight":1,"models":{"smart":"claude-model-id"},
		 "price":{"in":3000000,"out":15000000,"cache_read":300000,"cache_write":3750000},
		 "model_prices":{"smart":{"in":1,"out":2,"cache_read":3}}},
		{"name":"kimi","protocol":"openai","base_url":"https://api.moonshot.cn/v1","weight":3,"price":{"in":2000000,"out":8000000}}],
		"routing":{"mode":"latency","primary_weight":2}}`)
	fbs, fr, err = modelFallbackFile(full, getenv)
	if err != nil {
		t.Fatal(err)
	}
	c := fbs[0]
	if c.Protocol != llmproto.ProtocolAnthropic || c.AnthropicVersion != "2023-06-01" || c.APIKey != "sk-ant-secret" ||
		c.Pricing.CacheReadMicroPerMTok != 300_000 || c.Pricing.CacheWriteMicroPerMTok != 3_750_000 ||
		c.PricingByModel["smart"].CacheReadMicroPerMTok != 3 || fbs[1].Protocol != "" {
		t.Fatalf("parsed %+v", fbs)
	}
	if fr.Mode != call.RoutingLatency || fr.Weights["primary"] != 2 || fr.Weights["claude"] != 1 || fr.Weights["kimi"] != 3 {
		t.Fatalf("routing %+v", fr)
	}
	weightsOnly := write("w.json", `{"providers":[{"name":"b","base_url":"https://b","weight":2,"price":{"in":1,"out":1}}]}`)
	if _, fr, err := modelFallbackFile(weightsOnly, getenv); err != nil || fr.Mode != "" || fr.Weights["b"] != 2 {
		t.Fatalf("weights only: %+v %v (mode stays empty: the coordinator defaults to weighted)", fr, err)
	}

	for _, c := range []struct{ body, want string }{
		{`{"providers":[{"name":"b","protocol":"gemini","base_url":"https://b","price":{"in":1,"out":1}}]}`, "protocol"},
		{`{"providers":[{"name":"b","base_url":"https://b","anthropic_version":"2023-06-01","price":{"in":1,"out":1}}]}`, "anthropic_version"},
		{`{"providers":[{"name":"b","base_url":"https://b","weight":0,"price":{"in":1,"out":1}}]}`, "weight"},
		{`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1,"cache_read":-1}}]}`, "cache_read"},
		{`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1}}],"routing":{"mode":"random"}}`, "routing.mode"},
		{`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1}}],"routing":{"primary_weight":-1}}`, "primary_weight"},
		{`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1}}],"routing":{"hedge":true}}`, "unknown field"},
		{`{"providers":[{"name":"b","protocol":"anthropic","base_url":"https://b","anthropic_version":"2023-06-01\r\nX: y","price":{"in":1,"out":1}}]}`, "YYYY-MM-DD"},
		{`{"providers":[{"name":"b","protocol":"anthropic","base_url":"https://b","anthropic_version":"latest","price":{"in":1,"out":1}}]}`, "YYYY-MM-DD"},
		{`{"providers":[{"name":"b","base_url":"https://b","weight":2,"price":{"in":1,"out":1}}],"routing":{"mode":"ordered"}}`, "ordered"},
		{`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1}}],"routing":{"mode":"ordered","primary_weight":2}}`, "ordered"},
	} {
		_, _, err := modelFallbackFile(write("bad.json", c.body), getenv)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: want error containing %q, got %v", c.body, c.want, err)
		}
	}
}

// TestModelFallbackFileExitCode: an invalid new field stops `agentbox server` with exit code 2 before anything
// starts, like the other configuration errors.
func TestModelFallbackFileExitCode(t *testing.T) {
	dir := t.TempDir()
	for i, body := range []string{
		`{"providers":[{"name":"b","protocol":"gemini","base_url":"https://b.example/v1","price":{"in":1,"out":1}}]}`,
		`{"providers":[{"name":"b","base_url":"https://b.example/v1","price":{"in":1,"out":1}}],"routing":{"mode":"fastest"}}`,
		`{"providers":[{"name":"b","protocol":"anthropic","base_url":"https://b.example","anthropic_version":"v1","price":{"in":1,"out":1}}]}`,
		`{"providers":[{"name":"b","base_url":"https://b.example/v1","weight":2,"price":{"in":1,"out":1}}],"routing":{"mode":"ordered"}}`,
	} {
		p := filepath.Join(dir, "bad.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--model-base-url", "https://m.example/v1",
			"--model-name", "m", "--model-fallback-file", p}, &stderr)
		if code != 2 || !strings.Contains(stderr.String(), "model-fallback-file") {
			t.Errorf("case %d: exit %d (%s), want 2", i, code, stderr.String())
		}
	}
}

// routing.primary_models (model gateway G3 review M3): parsed into the routing; an unknown logical model, an empty
// vendor name or a declared model no route serves stops the server with exit code 2. Without it nothing changes.
func TestPrimaryModels(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "fb.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ok := write(`{"providers":[{"name":"claude","protocol":"anthropic","base_url":"https://c.example","models":{"smart":"claude-x"},"price":{"in":1,"out":1}}],
		"routing":{"primary_models":{"kimi-k3":"kimi-k3"}}}`)
	if _, fr, err := modelFallbackFile(ok, os.Getenv); err != nil || fr.PrimaryModels["kimi-k3"] != "kimi-k3" || len(fr.PrimaryModels) != 1 {
		t.Fatalf("%+v %v", fr, err)
	}
	if _, fr, err := modelFallbackFile(write(`{"providers":[{"name":"b","base_url":"https://b","price":{"in":1,"out":1}}]}`), os.Getenv); err != nil || fr.PrimaryModels != nil {
		t.Fatalf("absent: %+v %v", fr, err)
	}
	if runtime.GOOS != "linux" {
		return // runServer 只在 Linux 上可用
	}
	for i, body := range []string{
		`{"providers":[{"name":"c","base_url":"https://c.example/v1","models":{"smart":"x"},"price":{"in":1,"out":1}}],"routing":{"primary_models":{"nope":"x"}}}`,
		`{"providers":[{"name":"c","base_url":"https://c.example/v1","models":{"smart":"x"},"price":{"in":1,"out":1}}],"routing":{"primary_models":{"kimi-k3":" "}}}`,
		// kimi-k2.6 is declared but neither the primary nor the fallback serves it.
		`{"providers":[{"name":"c","base_url":"https://c.example/v1","models":{"smart":"x"},"price":{"in":1,"out":1}}],"routing":{"primary_models":{"kimi-k3":"kimi-k3"}}}`,
	} {
		var stderr bytes.Buffer
		code := runServer([]string{"--data-dir", t.TempDir(), "--database-url", "postgres://x", "--model-base-url", "https://m.example/v1",
			"--model-name", "kimi-k3", "--models", "kimi-k3,kimi-k2.6,smart", "--model-fallback-file", write(body)}, &stderr)
		if code != 2 || !strings.Contains(stderr.String(), "primary_models") {
			t.Errorf("case %d: exit %d (%s), want 2", i, code, stderr.String())
		}
	}
}
