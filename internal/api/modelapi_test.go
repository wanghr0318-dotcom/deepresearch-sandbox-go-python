package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// modelEcho stands in for internal/modelapi: it answers with the caller's principal; with ?stream it writes one
// line, waits for release, then writes the rest.
type modelEcho struct {
	mu      sync.Mutex
	calls   int
	release chan struct{}
}

func (m *modelEcho) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	p, _ := tenant.FromContext(r.Context())
	if r.URL.Query().Has("stream") {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-m.release
		_, _ = fmt.Fprint(w, "data: second\n\ndata: [DONE]\n\n")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"key": p.KeyID, "user": p.UserID, "operator": p.Operator})
}

func newModelServer(t *testing.T, mh http.Handler) *tenantServer {
	t.Helper()
	ts := &tenantServer{}
	ts.testServer, ts.acc, _ = newSessionServer(t, func(c *Config) {
		acc := c.Accounts.(*fakeAccounts)
		ts.ten = newFakeTenancy(acc)
		v := apikey.NewVerifier(ts.ten, []byte("pepper-for-tests-0123456789abcdef"), nil)
		ts.ten.evict = v.Evict
		ts.ses = &recordingSessions{fakeSessions: c.Sessions.(*fakeSessions)}
		c.Accounts, c.Tenancy, c.APIKeys, c.Sessions = tenantAccounts{fakeAccounts: acc, ten: ts.ten}, ts.ten, v, ts.ses
		c.ModelAPI = mh
	})
	return ts
}

// modelErr decodes an error in either format: (OpenAI code, or "anthropic:"+type).
func modelErr(t *testing.T, body []byte) string {
	t.Helper()
	var v struct {
		Type  string `json:"type"`
		Error struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	if v.Type == "error" {
		return "anthropic:" + v.Error.Type
	}
	return v.Error.Code
}

// Without ModelAPI the paths do not exist, exactly as before.
func TestModelAPIOff(t *testing.T) {
	ts := newTenantServer(t)
	st, body, _ := ts.do("POST", "/v1/chat/completions", `{}`, adminAuth)
	expect(t, st, body, 404, "not_found")
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:1", ModelAPI: &modelEcho{}}); err == nil {
		t.Fatal("ModelAPI without Tenancy must be refused")
	}
}

// Authentication and authorization of the model API, with errors in the caller's format (§5, §7.5): keys by
// Authorization: Bearer or x-api-key with the models scope, the operator token, the session cookie.
func TestModelAPIAuth(t *testing.T) {
	mh := &modelEcho{}
	ts := newModelServer(t, mh)
	_, cookie := ts.acc.seedUser(t, "alice")
	models, modelsID := ts.mintKey(t, cookie, "models")
	runtime, _ := ts.mintKey(t, cookie, "runtime")
	bad, _ := apikey.Generate([]byte("other pepper"))
	av := map[string]string{"anthropic-version": "2023-06-01"}
	with := func(a, b map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range a {
			out[k] = v
		}
		for k, v := range b {
			out[k] = v
		}
		return out
	}
	for _, c := range []struct {
		method, path string
		hdr          map[string]string
		status       int
		code         string
	}{
		{"POST", "/v1/chat/completions", nil, 401, "invalid_api_key"},
		{"POST", "/v1/messages", av, 401, "anthropic:authentication_error"},
		{"GET", "/v1/models", nil, 401, "invalid_api_key"},
		{"GET", "/v1/models", av, 401, "anthropic:authentication_error"},
		{"POST", "/v1/chat/completions", bearer(bad.Full), 401, "invalid_api_key"},
		{"POST", "/v1/messages", with(av, xAPIKey(bad.Full)), 401, "anthropic:authentication_error"},
		{"POST", "/v1/chat/completions", bearer("not-the-operator-token"), 401, "invalid_api_key"},
		{"POST", "/v1/chat/completions", bearer(runtime), 403, "permission_denied"},
		{"POST", "/v1/messages", with(av, xAPIKey(runtime)), 403, "anthropic:permission_error"},
		{"POST", "/v1/chat/completions", bearer(models), 200, ""},
		{"POST", "/v1/messages", with(av, xAPIKey(models)), 200, ""},
		{"POST", "/v1/messages", with(av, bearer(models)), 200, ""},
		{"GET", "/v1/models", xAPIKey(models), 200, ""},
		{"POST", "/v1/chat/completions", session(cookie), 200, ""},
		{"POST", "/v1/chat/completions", adminAuth, 200, ""},
	} {
		st, body, hdr := ts.do(c.method, c.path, `{}`, c.hdr)
		if st != c.status {
			t.Fatalf("%s %s %v: %d %s", c.method, c.path, c.hdr, st, body)
		}
		if c.status != 200 {
			if got := modelErr(t, body); got != c.code {
				t.Fatalf("%s %s %v: %s, want %s", c.method, c.path, c.hdr, got, c.code)
			}
			if c.status == 401 && hdr.Get("WWW-Authenticate") == "" {
				t.Fatalf("no WWW-Authenticate")
			}
			continue
		}
		var p struct {
			Key      string `json:"key"`
			User     int64  `json:"user"`
			Operator bool   `json:"operator"`
		}
		decodeJSONBody(t, body, &p)
		switch {
		case c.hdr["Cookie"] != "":
			if p.Key != "" || p.User == 0 {
				t.Fatalf("cookie principal %+v", p)
			}
		case c.hdr["Authorization"] == adminAuth["Authorization"]:
			if !p.Operator {
				t.Fatalf("operator principal %+v", p)
			}
		default:
			if p.Key != modelsID || p.User == 0 {
				t.Fatalf("key principal %+v", p)
			}
		}
	}
	// Restricted modes answer 503 in the caller's format.
	ts.setMode(ModeDiagnostic)
	st, body, _ := ts.do("POST", "/v1/messages", `{}`, with(av, xAPIKey(models)))
	if st != 503 || modelErr(t, body) != "anthropic:api_error" {
		t.Fatalf("diagnostic: %d %s", st, body)
	}
}

// A key revoked during a stream: the stream completes (bounded by --model-api-max-duration in modelapi), and the
// next request with the key is 401 (§7.8).
func TestModelAPIKeyRevokedMidStream(t *testing.T) {
	mh := &modelEcho{release: make(chan struct{})}
	ts := newModelServer(t, mh)
	_, cookie := ts.acc.seedUser(t, "alice")
	full, id := ts.mintKey(t, cookie, "models")
	req, _ := http.NewRequest("POST", ts.srv.URL+"/v1/chat/completions?stream", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+full)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	rd := bufio.NewReader(resp.Body)
	if line, _ := rd.ReadString('\n'); line != "data: first\n" {
		t.Fatalf("first line %q", line)
	}
	st, body, _ := ts.do("DELETE", "/v1/keys/"+id, "", session(cookie))
	expect(t, st, body, 200, "")
	close(mh.release)
	rest, err := io.ReadAll(rd)
	if err != nil || !strings.Contains(string(rest), "data: [DONE]") {
		t.Fatalf("stream did not complete: %q %v", rest, err)
	}
	st, body, _ = ts.do("POST", "/v1/chat/completions", `{}`, bearer(full))
	if st != 401 || modelErr(t, body) != "invalid_api_key" {
		t.Fatalf("after revoke: %d %s", st, body)
	}
}
