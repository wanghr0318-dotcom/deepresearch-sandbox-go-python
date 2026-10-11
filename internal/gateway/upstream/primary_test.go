package upstream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// PrimaryModels (fallback file routing.primary_models): the primary serves only the listed logical models, under
// their vendor names, on the sandbox path and on streams; an empty map serves nothing; nil keeps today's behaviour.
func TestPrimaryModels(t *testing.T) {
	p := fakeupstream.New()
	t.Cleanup(p.Close)
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	mk := func(pm map[string]string) ChatAdapter {
		return NewChat(ChatConfig{BaseURL: p.ModelBaseURL(), Model: "kimi-k3", Models: []string{"kimi-k3", "smart"}, PrimaryModels: pm,
			HTTP: d.HTTPClient(DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0),
			Fallbacks: []ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: "https://c.example", Models: map[string]string{"smart": "claude-x"}}}}).(ChatAdapter)
	}
	if a := mk(nil); !a.Serves(0, "kimi-k3") || !a.Serves(0, "smart") {
		t.Fatal("nil: the primary serves every declared model")
	}
	if a := mk(map[string]string{}); a.Serves(0, "kimi-k3") || a.Serves(0, "smart") || !a.Serves(1, "smart") {
		t.Fatal("empty: the primary serves nothing")
	}
	a := mk(map[string]string{"kimi-k3": "kimi-k3-vendor"})
	if !a.Serves(0, "kimi-k3") || a.Serves(0, "smart") {
		t.Fatal("listed models only")
	}
	if _, e := a.DoOn(context.Background(), 0, []byte(`{"model":"kimi-k3","messages":[{"role":"user","content":"hi"}],"max_tokens":10}`)); e != nil {
		t.Fatal(e)
	}
	s, e := a.DoStream(context.Background(), 0, []byte(`{"model":"kimi-k3","messages":[{"role":"user","content":"hi"}]}`), llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	_ = s.Close()
	for _, r := range p.Requests(fakeupstream.Chat) {
		var b struct{ Model string }
		_ = json.Unmarshal([]byte(r.Body), &b)
		if b.Model != "kimi-k3-vendor" {
			t.Fatalf("primary got model %q", b.Model)
		}
	}
}
