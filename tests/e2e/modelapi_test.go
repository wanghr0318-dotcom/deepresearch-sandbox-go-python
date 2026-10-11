//go:build linux

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/modelapi"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// Quota and ModelRequests pass the optional store capabilities through the gate (the model API needs both).
func (s gatedStore) Quota() quota.Store {
	return s.Store.(interface{ Quota() quota.Store }).Quota()
}

func (s gatedStore) ModelRequests() modelapi.Store {
	return s.Store.(interface{ ModelRequests() modelapi.Store }).ModelRequests()
}

// Usage passes the usage store through the gate (G5: the usage endpoints and the periodic rollup).
func (s gatedStore) Usage() usage.Store {
	return s.Store.(interface{ Usage() usage.Store }).Usage()
}

// The model API wired by app.Run (--model-api, model gateway G3): a user registers, mints a models-scoped key, and
// calls /v1/chat/completions and /v1/messages (stream and non-stream) and /v1/models with it; every request is
// recorded in model_requests with a settled hold, and verify-invariants (G1–G3) holds at the end.
func TestModelAPIEndToEnd(t *testing.T) {
	fu := fakeupstream.New()
	defer fu.Close()
	h := newHarness(t)
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.Accounts, cfg.UserOrchestratorModel, cfg.UserWorkerModel = true, fakeupstream.Model, fakeupstream.Model
	cfg.ModelAPI = true
	cfg.Model.Fallbacks = []app.ModelFallback{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: fu.URL(),
		APIKey: "sk-ant-e2e", Pricing: upstream.Pricing{InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000}}}
	h.start(cfg)

	resp, err := http.Post(h.base+"/auth/register", "application/json", strings.NewReader(`{"username":"mia","password":"Passw0rdX"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	var cookie string
	for _, c := range resp.Cookies() {
		if c.Name == "agentbox_session" {
			cookie = c.Name + "=" + c.Value
		}
	}
	if resp.StatusCode != http.StatusCreated || cookie == "" {
		t.Fatalf("register: %d", resp.StatusCode)
	}
	do := func(method, path, body string, hdr map[string]string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, h.base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	st, b := do("POST", "/v1/keys", `{"name":"sdk","scopes":["models"]}`, map[string]string{"Cookie": cookie})
	var k struct {
		Key string `json:"key"`
	}
	if st != 201 || json.Unmarshal(b, &k) != nil || k.Key == "" {
		t.Fatalf("create key: %d %s", st, b)
	}
	auth := map[string]string{"Authorization": "Bearer " + k.Key}
	xkey := map[string]string{"x-api-key": k.Key, "anthropic-version": "2023-06-01"}
	model := fakeupstream.Model
	for _, c := range []struct {
		path, body string
		hdr        map[string]string
		want       string
	}{
		{"/v1/chat/completions", `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`, auth, `"chat.completion"`},
		{"/v1/chat/completions", `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`, auth, "data: [DONE]"},
		{"/v1/messages", `{"model":"` + model + `","max_tokens":50,"messages":[{"role":"user","content":"hi"}]}`, xkey, `"type":"message"`},
		{"/v1/messages", `{"model":"` + model + `","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, xkey, "event: message_stop"},
	} {
		if st, b := do("POST", c.path, c.body, c.hdr); st != 200 || !strings.Contains(string(b), c.want) {
			t.Fatalf("%s %s: %d %s", c.path, c.body, st, b)
		}
	}
	if st, b := do("GET", "/v1/models", "", auth); st != 200 || !strings.Contains(string(b), `"id":"`+model+`"`) {
		t.Fatalf("models: %d %s", st, b)
	}
	if st, b := do("POST", "/v1/chat/completions", `{"model":"`+model+`","messages":[]}`, map[string]string{"Authorization": "Bearer abx_live_k_AAAAAAAAAAAAAAAA_x"}); st != 401 ||
		!strings.Contains(string(b), `"invalid_api_key"`) {
		t.Fatalf("bad key: %d %s", st, b)
	}

	conn, err := pgx.Connect(context.Background(), h.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var n, settled int
	if err := conn.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE q.state = 'settled' AND q.charged_micro = r.cost_micro)
		FROM model_requests r JOIN quota_holds q ON q.hold_id = r.hold_id WHERE r.status = 200 AND r.key_id IS NOT NULL`).Scan(&n, &settled); err != nil {
		t.Fatal(err)
	}
	if n != 4 || settled != 4 {
		t.Fatalf("model_requests: %d rows, %d settled at their cost", n, settled)
	}

	// G5: after a rollup (the server's runs every minute; this one is direct), the four requests are the user's usage
	// and bill, by model; an API key cannot read usage.
	ps, err := postgres.Open(context.Background(), postgres.Options{DSN: h.dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	if _, err := ps.UsageStore().Rollup(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	var u struct {
		Rows []struct {
			Model  string `json:"model"`
			Source string `json:"source"`
			usage.Totals
		} `json:"rows"`
	}
	st, b = do("GET", "/v1/usage?group_by=model,source", "", map[string]string{"Cookie": cookie})
	if st != 200 || json.Unmarshal(b, &u) != nil || len(u.Rows) != 1 || u.Rows[0].Model != model || u.Rows[0].Source != "model_api" ||
		u.Rows[0].Requests != 4 || u.Rows[0].CostMicro <= 0 || u.Rows[0].PromptTokens <= 0 {
		t.Fatalf("usage: %d %s", st, b)
	}
	st, b = do("GET", "/v1/bills/"+time.Now().UTC().Format("2006-01"), "", map[string]string{"Cookie": cookie})
	if st != 200 || !strings.Contains(string(b), `"model":"`+model+`"`) || !strings.Contains(string(b), `"requests":4`) {
		t.Fatalf("bill: %d %s", st, b)
	}
	if st, b := do("GET", "/v1/usage", "", auth); st != 403 || !strings.Contains(string(b), "permission_denied") {
		t.Fatalf("usage with a key: %d %s", st, b)
	}
	rep, err := ps.UsageStore().Reconcile(context.Background(), time.Now(), true)
	if err != nil || !rep.OK() || rep.Raw.Requests != 4 {
		t.Fatalf("reconcile: %+v %v", rep, err)
	}
	h.finish()
}
