package api

import (
	"context"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/usage"
)

// ListMembers makes the fake tenancy a memberLister (the members page).
func (f *fakeTenancy) ListMembers(_ context.Context, orgID int64) ([]tenant.Membership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []tenant.Membership{}
	for _, m := range f.members {
		if m.OrgID == orgID {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out, nil
}

// fakeUsage records the queries it answers and returns rows.
type fakeUsage struct {
	mu      sync.Mutex
	queries []usage.Query
	rows    []usage.Group
	lastRun time.Time
}

func (f *fakeUsage) Rollup(context.Context, time.Time) (usage.RollupResult, error) {
	return usage.RollupResult{}, nil
}

func (f *fakeUsage) Usage(_ context.Context, q usage.Query) ([]usage.Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q)
	return f.rows, nil
}

func (f *fakeUsage) Reconcile(context.Context, time.Time, bool) (usage.Report, error) {
	return usage.Report{}, nil
}

func (f *fakeUsage) Status(context.Context) (usage.Status, error) {
	return usage.Status{LastRunAt: f.lastRun}, nil
}

func (f *fakeUsage) last(t *testing.T) usage.Query {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queries) == 0 {
		t.Fatal("no usage query")
	}
	return f.queries[len(f.queries)-1]
}

func newUsageServer(t *testing.T, u usage.Store) *tenantServer {
	t.Helper()
	ts := &tenantServer{}
	ts.testServer, ts.acc, _ = newSessionServer(t, func(c *Config) {
		acc := c.Accounts.(*fakeAccounts)
		ts.ten = newFakeTenancy(acc)
		v := apikey.NewVerifier(ts.ten, []byte("pepper-for-tests-0123456789abcdef"), nil)
		ts.ten.evict = v.Evict
		ts.ses = &recordingSessions{fakeSessions: c.Sessions.(*fakeSessions)}
		c.Accounts, c.Tenancy, c.APIKeys, c.Sessions = tenantAccounts{fakeAccounts: acc, ten: ts.ten}, ts.ten, v, ts.ses
		c.Usage = u
	})
	return ts
}

// acmeOrg sets up org acme (alice owner, bob member) and carol alone in her personal org.
func acmeOrg(t *testing.T, ts *tenantServer) (alice, bob, carol string, org int64) {
	t.Helper()
	qs := &quotaServer{tenantServer: ts}
	return qs.acme(t)
}

// TestUsageAuthorization: members see only their own usage, owners their org's (optionally one user or key),
// the operator everything (?org= picks one); API keys are refused; objects of other orgs are the same 404 as
// missing ones.
func TestUsageAuthorization(t *testing.T) {
	fu := &fakeUsage{lastRun: time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)}
	ts := newUsageServer(t, fu)
	alice, bob, carol, org := acmeOrg(t, ts)
	_, aliceKey := ts.mintKey(t, alice, "runtime")
	_, bobKey := ts.mintKey(t, bob, "runtime")
	full, _ := ts.mintKey(t, alice, "runtime", "models")
	bobID := ts.ten.members[ts.acc.users["bob"].u.ID].UserID
	st, body, _ := ts.do("GET", "/v1/me", "", session(carol)) // creates carol's personal org
	expect(t, st, body, 200, "")
	carolOrg := tenant.PersonalOrgName(ts.acc.users["carol"].u.ID)

	type want struct {
		status       int
		code         string
		org, user    int64
		key          string
		checkedQuery bool
	}
	cases := []struct {
		who  string
		hdr  map[string]string
		path string
		want want
	}{
		{"operator all", adminAuth, "/v1/usage", want{200, "", 0, 0, "", true}},
		{"operator one org", adminAuth, "/v1/usage?org=acme", want{200, "", org, 0, "", true}},
		{"operator one user", adminAuth, "/v1/usage?user=bob", want{200, "", 0, bobID, "", true}},
		{"operator unknown org", adminAuth, "/v1/usage?org=nope", want{404, "org_not_found", 0, 0, "", false}},
		{"owner", session(alice), "/v1/usage", want{200, "", org, 0, "", true}},
		{"owner by user", session(alice), "/v1/usage?user=bob", want{200, "", org, bobID, "", true}},
		{"owner by key", session(alice), "/v1/usage?key=" + bobKey, want{200, "", org, 0, bobKey, true}},
		{"owner own org by name", session(alice), "/v1/usage?org=acme", want{200, "", org, 0, "", true}},
		{"owner other org", session(alice), "/v1/usage?org=" + url.QueryEscape(carolOrg), want{404, "org_not_found", 0, 0, "", false}},
		{"owner missing org", session(alice), "/v1/usage?org=nope", want{404, "org_not_found", 0, 0, "", false}},
		{"owner other org's user", session(alice), "/v1/usage?user=carol", want{404, "user_not_found", 0, 0, "", false}},
		{"owner missing user", session(alice), "/v1/usage?user=nobody", want{404, "user_not_found", 0, 0, "", false}},
		{"owner missing key", session(alice), "/v1/usage?key=k_aaaaaaaaaaaaaaaa", want{404, "key_not_found", 0, 0, "", false}},
		{"owner malformed key", session(alice), "/v1/usage?key=nope", want{404, "key_not_found", 0, 0, "", false}},
		{"member", session(bob), "/v1/usage", want{200, "", org, bobID, "", true}},
		{"member self", session(bob), "/v1/usage?user=bob", want{200, "", org, bobID, "", true}},
		{"member own key", session(bob), "/v1/usage?key=" + bobKey, want{200, "", org, bobID, bobKey, true}},
		{"member other user", session(bob), "/v1/usage?user=alice", want{404, "user_not_found", 0, 0, "", false}},
		{"member other's key", session(bob), "/v1/usage?key=" + aliceKey, want{404, "key_not_found", 0, 0, "", false}},
		{"other org's key", session(carol), "/v1/usage?key=" + aliceKey, want{404, "key_not_found", 0, 0, "", false}},
		{"api key", bearer(full), "/v1/usage", want{403, "permission_denied", 0, 0, "", false}},
		{"api key bill", bearer(full), "/v1/bills/2026-10", want{403, "permission_denied", 0, 0, "", false}},
		{"api key members", bearer(full), "/v1/orgs/members", want{403, "permission_denied", 0, 0, "", false}},
		{"anonymous", nil, "/v1/usage", want{401, "", 0, 0, "", false}},
		{"owner bill", session(alice), "/v1/bills/2026-10", want{200, "", org, 0, "", true}},
		{"member bill", session(bob), "/v1/bills/2026-10", want{200, "", org, bobID, "", true}},
		{"operator bill needs org", adminAuth, "/v1/bills/2026-10", want{400, "invalid_request", 0, 0, "", false}},
		{"operator bill", adminAuth, "/v1/bills/2026-10?org=acme", want{200, "", org, 0, "", true}},
		{"bad month", session(alice), "/v1/bills/2026-13", want{400, "invalid_request", 0, 0, "", false}},
	}
	for _, c := range cases {
		n := len(fu.queries)
		st, body, _ := ts.do("GET", c.path, "", c.hdr)
		if st != c.want.status || (c.want.code != "" && errCode(t, body) != c.want.code) {
			t.Errorf("%s: GET %s = %d %s", c.who, c.path, st, body)
			continue
		}
		if !c.want.checkedQuery {
			if len(fu.queries) != n {
				t.Errorf("%s: a refused request queried usage", c.who)
			}
			continue
		}
		q := fu.last(t)
		if q.OrgID != c.want.org || q.UserID != c.want.user || q.KeyID != c.want.key {
			t.Errorf("%s: query %+v, want org %d user %d key %q", c.who, q, c.want.org, c.want.user, c.want.key)
		}
	}

	// The 404 for another org's object is the same body as for a missing one.
	_, a, _ := ts.do("GET", "/v1/usage?org="+url.QueryEscape(carolOrg), "", session(alice))
	_, b, _ := ts.do("GET", "/v1/usage?org=nope", "", session(alice))
	if string(a) != string(b) {
		t.Fatalf("404 bodies differ: %s vs %s", a, b)
	}

	// Members: owners and the operator (with ?org=); members are refused.
	st, body, _ = ts.do("GET", "/v1/orgs/members", "", session(alice))
	expect(t, st, body, 200, "")
	var ml memberListJSON
	decodeJSONBody(t, body, &ml)
	if ml.Org.Name != "acme" || len(ml.Members) != 2 || ml.Members[0].Username != "alice" || ml.Members[1].Role != tenant.Member {
		t.Fatalf("members = %s", body)
	}
	st, body, _ = ts.do("GET", "/v1/orgs/members", "", session(bob))
	expect(t, st, body, 403, "forbidden")
	st, body, _ = ts.do("GET", "/v1/orgs/members", "", adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = ts.do("GET", "/v1/orgs/members?org=acme", "", adminAuth)
	expect(t, st, body, 200, "")
	st, body, _ = ts.do("GET", "/v1/orgs/members?org=acme", "", session(carol))
	expect(t, st, body, 404, "org_not_found")
}

// TestUsageResponses: defaults (this month by day), group_by and range validation, totals, as_of, and the bill's
// per-model lines with known and unknown cost.
func TestUsageResponses(t *testing.T) {
	fu := &fakeUsage{rows: []usage.Group{
		{Model: "smart", Source: usage.SourceModelAPI, Totals: usage.Totals{Requests: 2, PromptTokens: 30, CompletionTokens: 9, CostMicro: 200, UnknownMicro: 50}},
		{Model: "search", Source: usage.SourceRuntime, Totals: usage.Totals{Requests: 1, CostMicro: 10}},
	}}
	ts := newUsageServer(t, fu)
	alice, bob, _, _ := acmeOrg(t, ts)
	st, body, _ := ts.do("GET", "/v1/usage", "", session(alice))
	expect(t, st, body, 200, "")
	var u usageJSON
	decodeJSONBody(t, body, &u)
	if !strings.HasSuffix(u.From, "-01") || u.To < u.From || u.To[:7] != u.From[:7] || len(u.GroupBy) != 1 || u.GroupBy[0] != usage.DimDay ||
		u.Total.CostMicro != 210 || u.Total.UnknownMicro != 50 || u.Total.Requests != 3 || u.AsOf != nil || len(u.Rows) != 2 {
		t.Fatalf("usage = %s", body)
	}
	st, body, _ = ts.do("GET", "/v1/usage?from=2026-09-01&to=2026-09-30&group_by=model,key", "", session(alice))
	expect(t, st, body, 200, "")
	if q := fu.last(t); q.From.Format("2006-01-02") != "2026-09-01" || q.To.Format("2006-01-02") != "2026-09-30" ||
		len(q.GroupBy) != 2 || q.GroupBy[0] != usage.DimKey || q.GroupBy[1] != usage.DimModel {
		t.Fatalf("query = %+v", q)
	}
	for _, p := range []string{"/v1/usage?from=2026-9-1", "/v1/usage?to=x", "/v1/usage?group_by=week", "/v1/usage?group_by=day,day",
		"/v1/usage?from=2026-10-02&to=2026-10-01", "/v1/usage?from=2024-01-01&to=2026-01-01"} {
		st, body, _ := ts.do("GET", p, "", session(alice))
		expect(t, st, body, 400, "invalid_request")
	}
	fu.lastRun = time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)
	st, body, _ = ts.do("GET", "/v1/bills/2026-10", "", session(bob))
	expect(t, st, body, 200, "")
	var b billJSON
	decodeJSONBody(t, body, &b)
	if b.Month != "2026-10" || b.Org.Name != "acme" || b.Username != "bob" || b.AsOf == nil || len(b.Models) != 2 ||
		b.Models[0].Model != "smart" || b.Models[0].TotalMicro != 250 || b.Total.CostMicro != 210 || b.Total.UnknownMicro != 50 ||
		len(b.Sources) != 2 {
		t.Fatalf("bill = %s", body)
	}
	if q := fu.last(t); q.From.Format("2006-01-02") != "2026-10-01" || q.To.Format("2006-01-02") != "2026-10-31" {
		t.Fatalf("bill range = %+v", q)
	}

	off := newUsageServer(t, nil)
	a2, _, _, _ := acmeOrg(t, off)
	for _, p := range []string{"/v1/usage", "/v1/bills/2026-10"} {
		st, body, _ := off.do("GET", p, "", session(a2))
		expect(t, st, body, 503, "usage_unavailable")
	}
}
