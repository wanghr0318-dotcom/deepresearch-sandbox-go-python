package upstream

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// routeServer records the model and Authorization of each request and answers with the given body.
type routeServer struct {
	*httptest.Server
	mu     sync.Mutex
	models []string
	auths  []string
	reply  string
}

func newRouteServer(t *testing.T, reply string) *routeServer {
	t.Helper()
	rs := &routeServer{reply: reply}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var v struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(b, &v)
		rs.mu.Lock()
		rs.models = append(rs.models, v.Model)
		rs.auths = append(rs.auths, r.Header.Get("Authorization"))
		reply := rs.reply
		rs.mu.Unlock()
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *routeServer) seen() (models, auths []string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return append([]string(nil), rs.models...), append([]string(nil), rs.auths...)
}

const okReply = `{"id":"c1","choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`

func chainAdapter(primary, backup string, fallbackModels map[string]string) Adapter {
	d := NewDialer(DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	return NewChat(ChatConfig{BaseURL: primary + "/v1", Model: "m-1", Models: []string{"m-1", "m-2"}, APIKey: "key-primary",
		Pricing: Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}, MaxTokensDefault: 100,
		HTTP: d.HTTPClient(DefaultModelMaxBody, 10*time.Second),
		Fallbacks: []ChatRoute{{Name: "backup", BaseURL: backup + "/v1", APIKey: "key-backup", Models: fallbackModels,
			Pricing:        Pricing{InputMicroPerMTok: 5_000_000, OutputMicroPerMTok: 7_000_000},
			PricingByModel: map[string]Pricing{"m-2": {InputMicroPerMTok: 9_000_000, OutputMicroPerMTok: 9_000_000}},
			HTTP:           d.HTTPClient(DefaultModelMaxBody, 10*time.Second)}}})
}

// TestChatRoutes: Routes/Serves/PricingOn/EstimateOn; DoOn sends the provider's model name with the
// provider's key to the provider's base URL; the resolved (logical) request is untouched.
func TestChatRoutes(t *testing.T) {
	p, b := newRouteServer(t, okReply), newRouteServer(t, okReply)
	a := chainAdapter(p.URL, b.URL, map[string]string{"m-1": "vendor-m1"})
	r, ok := a.(Router)
	if !ok {
		t.Fatal("chat adapter does not implement Router")
	}
	if got := r.Routes(); len(got) != 2 || got[0] != "primary" || got[1] != "backup" {
		t.Fatalf("Routes %v", got)
	}
	for _, c := range []struct {
		route int
		model string
		want  bool
	}{{0, "m-1", true}, {0, "m-2", true}, {1, "m-1", true}, {1, "m-2", false}, {0, "nope", false}, {2, "m-1", false}} {
		if got := r.Serves(c.route, c.model); got != c.want {
			t.Errorf("Serves(%d, %s) = %v", c.route, c.model, got)
		}
	}
	if p := r.PricingOn(1, "m-1"); p.InputMicroPerMTok != 5_000_000 {
		t.Errorf("backup default price %+v", p)
	}
	if p := r.PricingOn(1, "m-2"); p.InputMicroPerMTok != 9_000_000 {
		t.Errorf("backup per-model price %+v", p)
	}
	resolved := mustResolve(t, a, `{"messages":[{"role":"user","content":"abcd"}]}`)
	in := (int64(len(`[{"role":"user","content":"abcd"}]`)) + 3) / 4
	if e0, err := a.Estimate(resolved); err != nil || e0 != in*1+100*2 {
		t.Errorf("primary estimate %d %v", e0, err)
	}
	if e1, err := r.EstimateOn(1, resolved); err != nil || e1 != in*5+100*7 {
		t.Errorf("backup estimate %d %v", e1, err)
	}
	before := string(resolved)
	if _, e := r.DoOn(context.Background(), 1, resolved); e != nil {
		t.Fatal(e)
	}
	if _, e := r.DoOn(context.Background(), 0, resolved); e != nil {
		t.Fatal(e)
	}
	if string(resolved) != before {
		t.Fatal("DoOn modified the resolved request")
	}
	bm, ba := b.seen()
	pm, pa := p.seen()
	if len(bm) != 1 || bm[0] != "vendor-m1" || ba[0] != "Bearer key-backup" {
		t.Errorf("backup saw models %v auth %v", bm, ba)
	}
	if len(pm) != 1 || pm[0] != "m-1" || pa[0] != "Bearer key-primary" {
		t.Errorf("primary saw models %v auth %v", pm, pa)
	}
}

// TestChatEmptyChoicesOnlyInChainMode: a 2xx without choices is ok for a single provider (unchanged
// behaviour) and unknown / upstream_bad_response in chain mode (so it fails over).
func TestChatEmptyChoicesOnlyInChainMode(t *testing.T) {
	empty := newRouteServer(t, `{"id":"c2","choices":[]}`)
	single := newChat(empty.URL, Pricing{})
	if _, e := single.Do(context.Background(), mustResolve(t, single, `{"messages":[{"role":"user","content":"x"}]}`)); e != nil {
		t.Fatalf("single provider: %v", e)
	}
	chain := chainAdapter(empty.URL, empty.URL, nil)
	_, e := chain.Do(context.Background(), mustResolve(t, chain, `{"messages":[{"role":"user","content":"x"}]}`))
	wantErr(t, e, OutcomeUnknown, CodeUpstreamBadResponse)
	// A missing choices field is the same.
	empty.mu.Lock()
	empty.reply = `{"id":"c3"}`
	empty.mu.Unlock()
	_, e = chain.(Router).DoOn(context.Background(), 1, mustResolve(t, chain, `{"messages":[{"role":"user","content":"x"}]}`))
	wantErr(t, e, OutcomeUnknown, CodeUpstreamBadResponse)
}

// TestChatSingleProviderRoutes: without fallbacks there is exactly one route and Do is unchanged.
func TestChatSingleProviderRoutes(t *testing.T) {
	a := newChat("http://127.0.0.1:1", Pricing{})
	if got := a.(Router).Routes(); len(got) != 1 || got[0] != "primary" {
		t.Fatalf("Routes %v", got)
	}
}
