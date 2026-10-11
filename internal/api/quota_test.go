package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
)

// fakeQuota is an in-memory quota.Store for the management endpoints and the runtime rate limit.
type fakeQuota struct {
	mu      sync.Mutex
	limits  map[quota.Target]quota.Limits
	budgets map[quota.Target]quota.Budget // current period only
	calls   int                           // LimitsFor calls
}

func newFakeQuota() *fakeQuota {
	return &fakeQuota{limits: map[quota.Target]quota.Limits{}, budgets: map[quota.Target]quota.Budget{}}
}

func (f *fakeQuota) Reserve(context.Context, quota.Scope, quota.Source, string, int64, time.Time) (quota.HoldID, error) {
	return "", nil
}
func (f *fakeQuota) Settle(context.Context, quota.HoldID, int64, bool) error { return nil }
func (f *fakeQuota) Release(context.Context, quota.HoldID) error             { return nil }
func (f *fakeQuota) Sweep(context.Context, int) (int, error)                 { return 0, nil }
func (f *fakeQuota) OperatorOrg(context.Context) (int64, error)              { return 1, nil }

func (f *fakeQuota) GetLimits(_ context.Context, t quota.Target) (quota.Limits, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.limits[t], nil
}

func (f *fakeQuota) PutLimits(_ context.Context, t quota.Target, l quota.Limits) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits[t] = l
	return nil
}

func (f *fakeQuota) LimitsFor(_ context.Context, s quota.Scope) (map[quota.Level]quota.Limits, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	out := map[quota.Level]quota.Limits{}
	for _, t := range s.Targets() {
		if l, ok := f.limits[t]; ok {
			out[t.Level] = l
		}
	}
	return out, nil
}

func (f *fakeQuota) GetBudget(_ context.Context, t quota.Target, p time.Time) (quota.Budget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.budgets[t]
	b.Level, b.ID, b.Period = t.Level, t.ID, quota.Period(p)
	return b, nil
}

func (f *fakeQuota) PutBudget(_ context.Context, t quota.Target, p time.Time, limit *int64) (quota.Budget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.budgets[t]
	if limit != nil && b.Used() > *limit {
		return quota.Budget{}, &quota.BelowUsageError{Used: b.Used(), Limit: *limit}
	}
	b.Level, b.ID, b.Period, b.LimitMicro = t.Level, t.ID, quota.Period(p), limit
	f.budgets[t] = b
	return b, nil
}

type quotaServer struct {
	*tenantServer
	q *fakeQuota
}

func newQuotaServer(t *testing.T, lim ratelimit.Limiter) *quotaServer {
	t.Helper()
	qs := &quotaServer{tenantServer: &tenantServer{}, q: newFakeQuota()}
	ts := qs.tenantServer
	ts.testServer, ts.acc, _ = newSessionServer(t, func(c *Config) {
		acc := c.Accounts.(*fakeAccounts)
		ts.ten = newFakeTenancy(acc)
		v := apikey.NewVerifier(ts.ten, []byte("pepper-for-tests-0123456789abcdef"), nil)
		ts.ten.evict = v.Evict
		ts.ses = &recordingSessions{fakeSessions: c.Sessions.(*fakeSessions)}
		c.Accounts, c.Tenancy, c.APIKeys, c.Sessions = tenantAccounts{fakeAccounts: acc, ten: ts.ten}, ts.ten, v, ts.ses
		c.Quota, c.RateLimiter = qs.q, lim
	})
	return qs
}

// acme sets up org acme (alice owner, bob member) and carol in her personal org; it returns the cookies and acme's id.
func (qs *quotaServer) acme(t *testing.T) (alice, bob, carol string, org int64) {
	t.Helper()
	_, alice = qs.acc.seedUser(t, "alice")
	_, bob = qs.acc.seedUser(t, "bob")
	_, carol = qs.acc.seedUser(t, "carol")
	st, body, _ := qs.do("POST", "/v1/orgs", `{"name":"acme","owner":"alice"}`, adminAuth)
	expect(t, st, body, 201, "")
	var o orgJSON
	decodeJSONBody(t, body, &o)
	st, body, _ = qs.do("POST", "/v1/orgs/members", `{"username":"bob","role":"member"}`, session(alice))
	expect(t, st, body, 200, "")
	return alice, bob, carol, o.ID
}

// TestLimitsAndBudgetsAuthorization: owners read their org and manage its users and keys; they cannot raise their
// own org's limits; members, other orgs and API keys cannot; the operator can do everything.
func TestLimitsAndBudgetsAuthorization(t *testing.T) {
	qs := newQuotaServer(t, nil)
	alice, bob, carol, _ := qs.acme(t)
	_, keyID := qs.mintKey(t, alice, "runtime")
	full, _ := qs.mintKey(t, alice, "runtime", "models")
	paths := []string{"/v1/limits/org/acme", "/v1/limits/user/bob", "/v1/limits/key/" + keyID,
		"/v1/budgets/org/acme", "/v1/budgets/user/bob", "/v1/budgets/key/" + keyID}
	put := func(p string) string {
		if p[4] == 'l' {
			return `{"rpm":5}`
		}
		return `{"limit_micro":1000}`
	}
	for _, p := range paths {
		orgLevel := p == "/v1/limits/org/acme" || p == "/v1/budgets/org/acme"
		cases := []struct {
			who        string
			hdr        map[string]string
			get, putSt int
			putCode    string
		}{
			{"operator", adminAuth, 200, 200, ""},
			{"owner", session(alice), 200, map[bool]int{true: 403, false: 200}[orgLevel], map[bool]string{true: "forbidden", false: ""}[orgLevel]},
			{"member", session(bob), 403, 403, "forbidden"},
			{"other org owner", session(carol), 404, 404, ""},
			{"api key", bearer(full), 403, 403, "permission_denied"},
		}
		for _, c := range cases {
			st, body, _ := qs.do("GET", p, "", c.hdr)
			if st != c.get {
				t.Errorf("%s GET %s = %d %s", c.who, p, st, body)
			}
			st, body, _ = qs.do("PUT", p, put(p), c.hdr)
			if st != c.putSt || (c.putCode != "" && errCode(t, body) != c.putCode) {
				t.Errorf("%s PUT %s = %d %s", c.who, p, st, body)
			}
		}
	}
	st, body, _ := qs.do("GET", "/v1/limits/team/x", "", adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = qs.do("GET", "/v1/limits/user/nobody", "", adminAuth)
	expect(t, st, body, 404, "user_not_found")
	st, body, _ = qs.do("GET", "/v1/limits/key/not-a-key", "", adminAuth)
	expect(t, st, body, 404, "key_not_found")
	st, body, _ = qs.do("GET", "/v1/limits/org/nope", "", adminAuth)
	expect(t, st, body, 404, "org_not_found")
}

// TestLimitsAndBudgetsValues: values round-trip; user and key values cannot exceed the org's; a budget below usage
// is 409; without Quota the endpoints are 503.
func TestLimitsAndBudgetsValues(t *testing.T) {
	qs := newQuotaServer(t, nil)
	alice, _, _, org := qs.acme(t)
	_, keyID := qs.mintKey(t, alice, "runtime")
	st, body, _ := qs.do("PUT", "/v1/limits/org/acme", `{"rpm":10,"max_sandboxes":2}`, adminAuth)
	expect(t, st, body, 200, "")
	st, body, _ = qs.do("GET", "/v1/limits/org/acme", "", session(alice))
	expect(t, st, body, 200, "")
	var l limitsJSON
	decodeJSONBody(t, body, &l)
	if l.Kind != "org" || l.ID != "acme" || *l.RPM != 10 || *l.MaxSandboxes != 2 || l.TPM != nil {
		t.Fatalf("limits %s", body)
	}
	st, body, _ = qs.do("PUT", "/v1/limits/user/bob", `{"rpm":11}`, session(alice))
	expect(t, st, body, 400, "exceeds_org_limit")
	st, body, _ = qs.do("PUT", "/v1/limits/key/"+keyID, `{"rpm":10,"tpm":99999}`, session(alice)) // tpm unset at the org
	expect(t, st, body, 200, "")
	st, body, _ = qs.do("PUT", "/v1/limits/user/bob", `{"rpm":-1}`, adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = qs.do("PUT", "/v1/limits/user/bob", `{"rpm":1,"extra":1}`, adminAuth)
	expect(t, st, body, 400, "invalid_request")

	st, body, _ = qs.do("PUT", "/v1/budgets/org/acme", `{"limit_micro":5000}`, adminAuth)
	expect(t, st, body, 200, "")
	var b budgetJSON
	decodeJSONBody(t, body, &b)
	if b.Period != time.Now().UTC().Format("2006-01") || *b.LimitMicro != 5000 || *b.AvailableMicro != 5000 {
		t.Fatalf("budget %s", body)
	}
	st, body, _ = qs.do("PUT", "/v1/budgets/user/bob", `{"limit_micro":5001}`, session(alice))
	expect(t, st, body, 400, "exceeds_org_limit")
	st, body, _ = qs.do("PUT", "/v1/budgets/user/bob", `{"limit_micro":null}`, session(alice)) // unlimited at the user level
	expect(t, st, body, 200, "")
	qs.q.mu.Lock()
	ot := quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(org, 10)}
	ob := qs.q.budgets[ot]
	ob.SpentMicro = 4000
	qs.q.budgets[ot] = ob
	qs.q.mu.Unlock()
	st, body, _ = qs.do("PUT", "/v1/budgets/org/acme", `{"limit_micro":3999}`, adminAuth)
	expect(t, st, body, 409, "budget_below_usage")
	st, body, _ = qs.do("GET", "/v1/budgets/org/acme?period=2026-13x", "", adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = qs.do("GET", "/v1/budgets/org/acme", "", adminAuth)
	expect(t, st, body, 200, "")
	b = budgetJSON{}
	decodeJSONBody(t, body, &b)
	if b.SpentMicro != 4000 || *b.AvailableMicro != 1000 {
		t.Fatalf("budget %s", body)
	}

	// Quota off: the endpoints exist (tenancy on) but are unavailable.
	ts := newTenantServer(t)
	st, body, _ = ts.do("GET", "/v1/limits/org/operator", "", adminAuth)
	expect(t, st, body, 503, "quota_unavailable")
}

// TestRuntimeRateLimit: requests per minute at the key, user and org level on the runtime creation endpoints; the
// 429 keeps the {code, message} body and carries Retry-After and the x-ratelimit-* headers; each key has its own
// bucket while the user's bucket is shared by its keys and cookie; nothing is limited without limits.
func TestRuntimeRateLimit(t *testing.T) {
	qs := newQuotaServer(t, ratelimit.NewLocal(nil))
	alice, _, _, _ := qs.acme(t)
	k1, _ := qs.mintKey(t, alice, "runtime")
	k2, k2ID := qs.mintKey(t, alice, "runtime")
	n := 0
	create := func(hdr map[string]string) (int, []byte, http.Header) {
		n++
		st, body, h := qs.do("POST", "/sessions", fmt.Sprintf(`{"request_id":"s%d"}`, n), hdr)
		return st, body, h
	}
	// No limits configured: no headers.
	st, body, h := create(bearer(k1))
	expect(t, st, body, 201, "")
	if h.Get("x-ratelimit-limit-requests") != "" {
		t.Fatal("no limits, no rate-limit headers")
	}
	st, body, _ = qs.do("PUT", "/v1/limits/key/"+k2ID, `{"rpm":2}`, session(alice))
	expect(t, st, body, 200, "")
	st, body, _ = qs.do("PUT", "/v1/limits/user/alice", `{"rpm":4}`, session(alice))
	expect(t, st, body, 200, "")
	st, body, h = create(bearer(k2))
	expect(t, st, body, 201, "")
	if h.Get("x-ratelimit-limit-requests") != "2" || h.Get("x-ratelimit-remaining-requests") != "1" || h.Get("x-ratelimit-reset-requests") == "" {
		t.Fatalf("headers %v", h)
	}
	st, body, _ = create(xAPIKey(k2))
	expect(t, st, body, 201, "")
	st, body, h = create(bearer(k2))
	expect(t, st, body, 429, "rate_limit_exceeded")
	if ra, err := strconv.Atoi(h.Get("Retry-After")); err != nil || ra < 1 || ra > 30 {
		t.Fatalf("Retry-After %q", h.Get("Retry-After"))
	}
	if h.Get("x-ratelimit-limit-requests") != "2" || h.Get("x-ratelimit-remaining-requests") != "0" {
		t.Fatalf("429 headers %v", h)
	}
	// The user bucket (4) has 2 left: the cookie and the other key share it.
	st, body, _ = create(session(alice))
	expect(t, st, body, 201, "")
	st, body, _ = create(bearer(k1))
	expect(t, st, body, 201, "")
	st, body, h = qs.do("POST", "/research", `{"request_id":"r1","topic":"x"}`, session(alice))
	expect(t, st, body, 429, "rate_limit_exceeded")
	if h.Get("x-ratelimit-limit-requests") != "4" {
		t.Fatalf("user level reported: %v", h)
	}
	// Reads are never limited.
	st, body, _ = qs.do("GET", "/sessions", "", bearer(k2))
	expect(t, st, body, 200, "")
}

// TestRuntimeRateLimitRedisDown: with Redis unreachable the limiter degrades to the in-process buckets: requests keep
// working and the limit is still enforced.
func TestRuntimeRateLimitRedisDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	r := cache.NewRedis(cache.RedisConfig{Addr: addr})
	t.Cleanup(func() { _ = r.Close() })
	lim := ratelimit.NewFallback(r, ratelimit.FallbackOptions{})
	qs := newQuotaServer(t, lim)
	alice, _, _, _ := qs.acme(t)
	st, body, _ := qs.do("PUT", "/v1/limits/org/acme", `{"rpm":2}`, adminAuth)
	expect(t, st, body, 200, "")
	for i, want := range []int{201, 201, 429} {
		st, body, _ := qs.do("POST", "/sessions", fmt.Sprintf(`{"request_id":"d%d"}`, i), session(alice))
		if st != want {
			t.Fatalf("request %d = %d %s", i, st, body)
		}
	}
	if !lim.Degraded() {
		t.Fatal("limiter should report degraded")
	}
	var me map[string]any
	st, body, _ = qs.do("GET", "/v1/me", "", session(alice))
	expect(t, st, body, 200, "")
	_ = json.Unmarshal(body, &me)
}
