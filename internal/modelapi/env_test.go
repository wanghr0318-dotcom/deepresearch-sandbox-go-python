package modelapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// ---- fakes ----

type hold struct {
	amount, charged int64
	state           string
	ref             string
}

// fakeHolder is an in-memory quota.Holder.
type fakeHolder struct {
	mu      sync.Mutex
	holds   map[quota.HoldID]*hold
	n       int
	reserve error // returned by Reserve when set
	settle  error // returned by Settle and Release when set
}

func newFakeHolder() *fakeHolder { return &fakeHolder{holds: map[quota.HoldID]*hold{}} }

func (f *fakeHolder) Reserve(_ context.Context, _ quota.Scope, src quota.Source, ref string, amount int64, _ time.Time) (quota.HoldID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reserve != nil {
		return "", f.reserve
	}
	if src != quota.SourceModelRequest {
		return "", errors.New("wrong source")
	}
	f.n++
	id := quota.HoldID(fmt.Sprintf("h%d", f.n))
	f.holds[id] = &hold{amount: amount, state: quota.StateHeld, ref: ref}
	return id, nil
}

func (f *fakeHolder) Settle(_ context.Context, id quota.HoldID, charged int64, unknown bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.settle != nil {
		return f.settle
	}
	h := f.holds[id]
	if h.state != quota.StateHeld {
		return nil
	}
	h.state, h.charged = quota.StateSettled, charged
	if unknown {
		h.state, h.charged = quota.StateChargedUnknown, max(charged, h.amount)
	}
	return nil
}

func (f *fakeHolder) Release(_ context.Context, id quota.HoldID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.settle != nil {
		return f.settle
	}
	if h := f.holds[id]; h.state == quota.StateHeld {
		h.state = quota.StateReleased
	}
	return nil
}

func (f *fakeHolder) get(id quota.HoldID) hold {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.holds[id]
}

type fakeLimits struct {
	mu  sync.Mutex
	ls  map[quota.Level]quota.Limits
	err error
}

func (f *fakeLimits) LimitsFor(context.Context, quota.Scope) (map[quota.Level]quota.Limits, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ls, f.err
}

type fakeStore struct {
	mu   sync.Mutex
	recs []Record
	ch   chan Record
}

func (f *fakeStore) RecordModelRequest(_ context.Context, r Record) error {
	f.mu.Lock()
	f.recs = append(f.recs, r)
	f.mu.Unlock()
	f.ch <- r
	return nil
}

// call.New requires these; the model routes never use them.
type nopCallStore struct{ call.Store }
type nopBlobs struct{ call.BlobStore }
type nopEvents struct{ call.HostEvents }

// ---- environment ----

type mode int

const (
	modeOpenAI    mode = iota // primary (OpenAI fake p), then claude (Anthropic fake q)
	modeAnthropic             // primary unreachable, then claude (q): the Anthropic provider answers
	modeThree                 // primary unreachable, then claude (q), then backup (OpenAI fake p)
)

type env struct {
	t       *testing.T
	p, q    *fakeupstream.Server
	h       *Handler
	srv     *httptest.Server
	holder  *fakeHolder
	limits  *fakeLimits
	store   *fakeStore
	rt      *call.ModelRouting
	princ   tenant.Principal
	princMu sync.Mutex
}

var testPrice = upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}

func newEnv(t *testing.T, m mode, opts ...func(*Config)) *env {
	t.Helper()
	p, q := fakeupstream.New(), fakeupstream.New()
	t.Cleanup(p.Close)
	t.Cleanup(q.Close)
	q.SetAnthropicKey("key-anthropic")
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	claude := upstream.ChatRoute{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: q.URL(), APIKey: "key-anthropic",
		Models:  map[string]string{"m-1": "claude-x"},
		Pricing: upstream.Pricing{InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000},
		HTTP:    d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0)}
	primary := p.ModelBaseURL()
	fallbacks := []upstream.ChatRoute{claude}
	if m != modeOpenAI {
		primary = "http://127.0.0.1:1/v1" // refused: a retryable failure before sending
	}
	if m == modeThree {
		fallbacks = append(fallbacks, upstream.ChatRoute{Name: "backup", BaseURL: p.ModelBaseURL(), APIKey: "key-openai",
			Pricing: testPrice, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0)})
	}
	ad := upstream.NewChat(upstream.ChatConfig{BaseURL: primary, Model: "m-1", Models: []string{"m-1", "m-2"}, APIKey: "key-openai",
		Pricing: testPrice, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 10*time.Second), StreamHTTP: d.HTTPClient(0, 0),
		Fallbacks: fallbacks})
	c, err := call.New(call.Config{Store: nopCallStore{}, Blobs: nopBlobs{}, Events: nopEvents{}, Adapters: []upstream.Adapter{ad}})
	if err != nil {
		t.Fatal(err)
	}
	rt := c.ModelRouting()
	e := &env{t: t, p: p, q: q, holder: newFakeHolder(), limits: &fakeLimits{}, store: &fakeStore{ch: make(chan Record, 64)}, rt: rt,
		princ: tenant.Principal{OrgID: 1, UserID: 2, KeyID: "k_test", Scopes: tenant.Models}}
	cfg := Config{Routing: func() *call.ModelRouting { return rt }, Models: []string{"m-1", "m-2"}, Holder: e.holder,
		Limits: e.limits, Limiter: ratelimit.NewLocal(time.Now), Store: e.store}
	for _, o := range opts {
		o(&cfg)
	}
	e.h = New(cfg)
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.princMu.Lock()
		pr := e.princ
		e.princMu.Unlock()
		e.h.ServeHTTP(w, r.WithContext(tenant.WithPrincipal(r.Context(), pr)))
	}))
	t.Cleanup(e.srv.Close)
	return e
}

// post sends a model API request in proto's format.
func (e *env) post(ctx context.Context, proto llmproto.Protocol, body string) *http.Response {
	e.t.Helper()
	path := PathChatCompletions
	if proto == llmproto.ProtocolAnthropic {
		path = PathMessages
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+path, strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if proto == llmproto.ProtocolAnthropic {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp
}

// record waits for the next recorded request.
func (e *env) record() Record {
	e.t.Helper()
	select {
	case r := <-e.store.ch:
		return r
	case <-time.After(10 * time.Second):
		e.t.Fatal("no request recorded")
	}
	return Record{}
}

// noRecord asserts nothing was recorded.
func (e *env) noRecord() {
	e.t.Helper()
	select {
	case r := <-e.store.ch:
		e.t.Fatalf("unexpected record %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// streamEvents reads a stream response in proto's format as neutral events.
func streamEvents(t *testing.T, resp *http.Response, proto llmproto.Protocol) (llmproto.Response, *llmproto.Collector, error) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	rd := llmproto.NewEventReader(resp.Body, proto)
	col := &llmproto.Collector{}
	for {
		ev, err := rd.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return col.Response(), col, nil
			}
			return col.Response(), col, err
		}
		col.Add(ev)
	}
}

// errorOf decodes an error body in proto's format: (OpenAI code or Anthropic type, message).
func errorOf(t *testing.T, proto llmproto.Protocol, b []byte) (string, string) {
	t.Helper()
	var v struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("error body %s: %v", b, err)
	}
	if proto == llmproto.ProtocolAnthropic {
		if v.Type != "error" {
			t.Fatalf("not an Anthropic error: %s", b)
		}
		return v.Error.Type, v.Error.Message
	}
	return v.Error.Code, v.Error.Message
}

func chatBody(proto llmproto.Protocol, model string, stream bool, extra string) string {
	s := ""
	if stream {
		s = `"stream":true,`
	}
	if proto == llmproto.ProtocolAnthropic {
		return `{"model":"` + model + `",` + s + extra + `"max_tokens":100,"messages":[{"role":"user","content":"hello"}]}`
	}
	return `{"model":"` + model + `",` + s + extra + `"messages":[{"role":"user","content":"hello"}]}`
}

var _ = bytes.NewReader
