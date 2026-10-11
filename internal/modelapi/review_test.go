package modelapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// REVIEW 1: clients that disconnect before the first token are reported to the shared breaker as provider failures.
func TestReviewClientCancelOpensBreaker(t *testing.T) {
	e := newEnv(t, modeOpenAI)
	e.p.SetLatency(fakeupstream.Chat, 2*time.Second)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+PathChatCompletions,
			strings.NewReader(chatBody(llmproto.ProtocolOpenAI, "m-1", true, "")))
		_, _ = http.DefaultClient.Do(req)
		cancel()
		r := e.record()
		t.Logf("cancelled request %d: status %d code %s tries %+v", i, r.Status, r.ErrorCode, r.Tries)
	}
	e.p.SetLatency(fakeupstream.Chat, 0)
	resp := e.post(context.Background(), llmproto.ProtocolOpenAI, chatBody(llmproto.ProtocolOpenAI, "m-1", false, ""))
	_ = readAll(t, resp)
	r := e.record()
	t.Logf("healthy request after 3 client cancels: provider %s tries %+v", r.Provider, r.Tries)
	if r.Provider != "primary" || strings.Contains(r.Tries[0].Skipped, "circuit_open") {
		t.Errorf("BUG: client cancellations opened the primary's breaker: provider %s tries %+v", r.Provider, r.Tries)
	}
}

// REVIEW 2: a client that stops reading a stream blocks the handler in Write past --model-api-max-duration; the
// stream slot and the hold stay taken.
func TestReviewStalledReaderOutlivesMaxDuration(t *testing.T) {
	prov := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		chunk := `{"id":"c","object":"chat.completion.chunk","created":1,"model":"m-1","choices":[{"index":0,"delta":{"content":"` +
			strings.Repeat("x", 2000) + `"},"finish_reason":null}]}`
		for r.Context().Err() == nil {
			if _, err := fmt.Fprintf(w, "data: %s\n\n", chunk); err != nil {
				return
			}
			fl.Flush()
		}
	}))
	defer prov.Close()
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	ad := upstream.NewChat(upstream.ChatConfig{BaseURL: prov.URL + "/v1", Model: "m-1", APIKey: "k", Pricing: testPrice,
		HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0)})
	c, err := call.New(call.Config{Store: nopCallStore{}, Blobs: nopBlobs{}, Events: nopEvents{}, Adapters: []upstream.Adapter{ad}})
	if err != nil {
		t.Fatal(err)
	}
	rt := c.ModelRouting()
	holder := newFakeHolder()
	one := int64(1)
	lims := &fakeLimits{ls: map[quota.Level]quota.Limits{quota.LevelKey: {MaxStreams: &one}}}
	store := &fakeStore{ch: make(chan Record, 8)}
	h := New(Config{Routing: func() *call.ModelRouting { return rt }, Models: []string{"m-1"}, Holder: holder, Limits: lims,
		Limiter: ratelimit.NewLocal(time.Now), Store: store, MaxDuration: 1 * time.Second})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(tenant.WithPrincipal(r.Context(), tenant.Principal{OrgID: 1, UserID: 2, KeyID: "k1", Scopes: tenant.Models})))
	}))
	defer srv.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	body := `{"model":"m-1","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	// Never read. Wait well past the 1 s maximum duration.
	select {
	case r := <-store.ch:
		t.Logf("recorded in time: %+v", r.ErrorCode)
	case <-time.After(6 * time.Second):
		t.Errorf("BUG: stream to a non-reading client still running 6 s after a 1 s max duration (no record, hold held)")
	}
	// The key's single stream slot is still taken.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+PathChatCompletions, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b := readAll(t, resp)
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Errorf("BUG: stream slot still held by the stalled request: %d %s", resp.StatusCode, b)
	}
}

// REVIEW 3 (known limit): Kimi primary + Claude fallback with logical model "smart". The primary receives the
// logical name verbatim, a real provider answers 404 (fatal, provider fault: no failover, breaker failure), so
// "smart" never reaches Claude while the primary is closed, and three "smart" requests open the primary's breaker
// for its real model too.
func TestReviewLogicalModelOnPrimary(t *testing.T) {
	var seen []string
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Model string `json:"model"`
		}
		_ = jsonDecode(r, &b)
		seen = append(seen, b.Model)
		if b.Model != "kimi-k3" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":{"message":"Not found the model","type":"resource_not_found_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"kimi-k3\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"kimi-k3\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer kimi.Close()
	q := fakeupstream.New()
	defer q.Close()
	q.SetAnthropicKey("ka")
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	claude := upstream.ChatRoute{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: q.URL(), APIKey: "ka",
		Models: map[string]string{"smart": "claude-x"}, Pricing: upstream.Pricing{InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000},
		HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0)}
	ad := upstream.NewChat(upstream.ChatConfig{BaseURL: kimi.URL + "/v1", Model: "kimi-k3", Models: []string{"kimi-k3", "smart"}, APIKey: "kk",
		Pricing: testPrice, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0),
		Fallbacks: []upstream.ChatRoute{claude}})
	c, err := call.New(call.Config{Store: nopCallStore{}, Blobs: nopBlobs{}, Events: nopEvents{}, Adapters: []upstream.Adapter{ad}})
	if err != nil {
		t.Fatal(err)
	}
	rt := c.ModelRouting()
	store := &fakeStore{ch: make(chan Record, 16)}
	h := New(Config{Routing: func() *call.ModelRouting { return rt }, Models: []string{"kimi-k3", "smart"}, Holder: newFakeHolder(),
		Limits: &fakeLimits{}, Store: store})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(tenant.WithPrincipal(r.Context(), tenant.Principal{OrgID: 1, UserID: 2, KeyID: "k1", Scopes: tenant.Models})))
	}))
	defer srv.Close()
	post := func(model string) (int, string) {
		resp, err := http.Post(srv.URL+PathChatCompletions, "application/json",
			strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		b := readAll(t, resp)
		r := <-store.ch
		return resp.StatusCode, fmt.Sprintf("%s provider=%s tries=%+v", b, r.Provider, r.Tries)
	}
	for i := 0; i < 3; i++ {
		st, s := post("smart")
		t.Logf("smart #%d: %d %s", i, st, s)
		// Fixed: the primary's 404 for a model it does not have is not its fault and not the end: claude serves it.
		if st != 200 || !strings.Contains(s, "provider=claude") {
			t.Errorf("smart #%d not served by the fallback: %d %s", i, st, s)
		}
	}
	st, s := post("kimi-k3")
	t.Logf("kimi-k3 after smart traffic: %d %s", st, s)
	t.Logf("models seen by the primary: %v", seen)
	if st != 200 || !strings.Contains(s, "provider=primary") || strings.Contains(s, "circuit_open") {
		t.Errorf("BUG: 'smart' traffic broke the primary for kimi-k3: %d %s", st, s)
	}
}

// With routing.primary_models the primary never receives a model it does not have: "smart" goes straight to the
// fallback (the primary is skipped as model_not_served) and kimi-k3 stays on the primary.
func TestReviewPrimaryModels(t *testing.T) {
	var seen []string
	kimi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Model string `json:"model"`
		}
		_ = jsonDecode(r, &b)
		seen = append(seen, b.Model)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"kimi-k3\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"kimi-k3\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer kimi.Close()
	q := fakeupstream.New()
	defer q.Close()
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	ad := upstream.NewChat(upstream.ChatConfig{BaseURL: kimi.URL + "/v1", Model: "kimi-k3", Models: []string{"kimi-k3", "smart"}, APIKey: "kk",
		Pricing: testPrice, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0),
		PrimaryModels: map[string]string{"kimi-k3": "kimi-k3"},
		Fallbacks: []upstream.ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: q.URL(),
			Models: map[string]string{"smart": "claude-x"}, Pricing: testPrice,
			HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0)}}})
	c, err := call.New(call.Config{Store: nopCallStore{}, Blobs: nopBlobs{}, Events: nopEvents{}, Adapters: []upstream.Adapter{ad}})
	if err != nil {
		t.Fatal(err)
	}
	rt := c.ModelRouting()
	store := &fakeStore{ch: make(chan Record, 16)}
	h := New(Config{Routing: func() *call.ModelRouting { return rt }, Models: []string{"kimi-k3", "smart"}, Holder: newFakeHolder(),
		Limits: &fakeLimits{}, Store: store})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(tenant.WithPrincipal(r.Context(), tenant.Principal{OrgID: 1, UserID: 2, KeyID: "k1", Scopes: tenant.Models})))
	}))
	defer srv.Close()
	for _, c := range []struct{ model, provider string }{{"smart", "claude"}, {"kimi-k3", "primary"}, {"smart", "claude"}} {
		resp, err := http.Post(srv.URL+PathChatCompletions, "application/json",
			strings.NewReader(`{"model":"`+c.model+`","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		b := readAll(t, resp)
		r := <-store.ch
		if resp.StatusCode != 200 || r.Provider != c.provider || len(r.Tries) != 1 {
			t.Fatalf("%s: %d %s %+v", c.model, resp.StatusCode, b, r.Tries)
		}
	}
	if strings.Join(seen, ",") != "kimi-k3" {
		t.Fatalf("the primary saw %v", seen)
	}
}

func jsonDecode(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(v) }
