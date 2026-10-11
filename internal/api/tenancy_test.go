package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// fakeTenancy is an in-memory tenant.Store and apikey.Store with the same rules as the PostgreSQL implementation.
// Users come from fakeAccounts; every user without a membership gets a personal org on first use.
type fakeTenancy struct {
	mu      sync.Mutex
	acc     *fakeAccounts
	nextOrg int64
	orgs    map[int64]*tenant.Org
	members map[int64]tenant.Membership // user id → membership
	keys    map[string]*fakeKey
	evict   func(id string) // NOTIFY apikey_changed
}

type fakeKey struct {
	k    tenant.Key
	hmac []byte
}

func newFakeTenancy(acc *fakeAccounts) *fakeTenancy {
	f := &fakeTenancy{acc: acc, orgs: map[int64]*tenant.Org{}, members: map[int64]tenant.Membership{}, keys: map[string]*fakeKey{}}
	f.orgLocked(tenant.OperatorOrg)
	return f
}

func (f *fakeTenancy) orgLocked(name string) *tenant.Org {
	for _, o := range f.orgs {
		if o.Name == name {
			return o
		}
	}
	f.nextOrg++
	o := &tenant.Org{ID: f.nextOrg, Name: name, CreatedAt: time.Unix(1_800_000_000, 0).UTC()}
	f.orgs[o.ID] = o
	return o
}

// userLocked returns the user and their membership (creating the personal org on first use).
func (f *fakeTenancy) userLocked(key string) (User, tenant.Membership, bool) {
	f.acc.mu.Lock()
	fu, ok := f.acc.users[key]
	f.acc.mu.Unlock()
	if !ok {
		return User{}, tenant.Membership{}, false
	}
	m, ok := f.members[fu.u.ID]
	if !ok {
		o := f.orgLocked(tenant.PersonalOrgName(fu.u.ID))
		m = tenant.Membership{OrgID: o.ID, OrgName: o.Name, UserID: fu.u.ID, Username: fu.u.Username, Role: tenant.Owner}
		f.members[fu.u.ID] = m
	}
	return fu.u, m, true
}

func (f *fakeTenancy) withCount(o tenant.Org) tenant.Org {
	for _, m := range f.members {
		if m.OrgID == o.ID {
			o.Members++
		}
	}
	return o
}

func (f *fakeTenancy) CreateOrg(_ context.Context, name string) (tenant.Org, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.orgs {
		if o.Name == name {
			return tenant.Org{}, persistence.ErrConflict
		}
	}
	return *f.orgLocked(name), nil
}

func (f *fakeTenancy) ListOrgs(context.Context) ([]tenant.Org, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []tenant.Org
	for _, o := range f.orgs {
		out = append(out, f.withCount(*o))
	}
	return out, nil
}

func (f *fakeTenancy) OrgByName(_ context.Context, name string) (tenant.Org, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.orgs {
		if o.Name == name {
			return f.withCount(*o), nil
		}
	}
	return tenant.Org{}, persistence.ErrNotFound
}

func (f *fakeTenancy) MemberByName(_ context.Context, key string) (tenant.Membership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, m, ok := f.userLocked(key)
	if !ok {
		return tenant.Membership{}, persistence.ErrNotFound
	}
	return m, nil
}

func (f *fakeTenancy) owners(orgID, except int64) int {
	n := 0
	for uid, m := range f.members {
		if m.OrgID == orgID && m.Role == tenant.Owner && uid != except {
			n++
		}
	}
	return n
}

func (f *fakeTenancy) revokeLocked(userID, orgID int64) {
	for id, k := range f.keys {
		if k.k.UserID == userID && k.k.OrgID == orgID && k.k.RevokedAt == nil {
			now := time.Now()
			k.k.RevokedAt = &now
			if f.evict != nil {
				f.evict(id)
			}
		}
	}
}

func (f *fakeTenancy) AddMember(_ context.Context, orgID int64, key string, role tenant.Role) (tenant.Membership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.orgs[orgID]
	if !ok {
		return tenant.Membership{}, persistence.ErrNotFound
	}
	if o.Name == tenant.OperatorOrg {
		return tenant.Membership{}, persistence.ErrInvalid
	}
	u, cur, ok := f.userLocked(key)
	if !ok {
		return tenant.Membership{}, persistence.ErrNotFound
	}
	switch {
	case cur.OrgID == orgID:
		if cur.Role == tenant.Owner && role != tenant.Owner && f.owners(orgID, u.ID) == 0 {
			return tenant.Membership{}, tenant.ErrLastOwner
		}
	case !tenant.IsPersonalOrg(cur.OrgName, u.ID) || f.withCount(*f.orgs[cur.OrgID]).Members > 1:
		return tenant.Membership{}, tenant.ErrUserInOtherOrg
	default:
		f.revokeLocked(u.ID, cur.OrgID)
	}
	m := tenant.Membership{OrgID: orgID, OrgName: o.Name, UserID: u.ID, Username: u.Username, Role: role}
	f.members[u.ID] = m
	return m, nil
}

func (f *fakeTenancy) RemoveMember(_ context.Context, orgID int64, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, cur, ok := f.userLocked(key)
	if !ok || cur.OrgID != orgID {
		return persistence.ErrNotFound
	}
	if tenant.IsPersonalOrg(cur.OrgName, u.ID) {
		return tenant.ErrPersonalOrg
	}
	if cur.Role == tenant.Owner && f.owners(orgID, u.ID) == 0 {
		return tenant.ErrLastOwner
	}
	f.revokeLocked(u.ID, orgID)
	o := f.orgLocked(tenant.PersonalOrgName(u.ID))
	f.members[u.ID] = tenant.Membership{OrgID: o.ID, OrgName: o.Name, UserID: u.ID, Username: u.Username, Role: tenant.Owner}
	return nil
}

func (f *fakeTenancy) CreateKey(_ context.Context, k tenant.NewKey) (tenant.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.members[k.UserID]
	if !ok {
		return tenant.Key{}, persistence.ErrNotFound
	}
	key := tenant.Key{ID: k.ID, OrgID: m.OrgID, UserID: k.UserID, Username: m.Username, Name: k.Name, Scopes: k.Scopes,
		CreatedAt: time.Now().UTC(), ExpiresAt: k.ExpiresAt}
	f.keys[k.ID] = &fakeKey{k: key, hmac: k.SecretHMAC}
	return key, nil
}

func (f *fakeTenancy) ListKeys(_ context.Context, flt tenant.KeyFilter) ([]tenant.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []tenant.Key
	for _, k := range f.keys {
		if (flt.OrgID == 0 || k.k.OrgID == flt.OrgID) && (flt.UserID == 0 || k.k.UserID == flt.UserID) {
			out = append(out, k.k)
		}
	}
	return out, nil
}

func (f *fakeTenancy) GetKey(_ context.Context, id string) (tenant.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return tenant.Key{}, persistence.ErrNotFound
	}
	return k.k, nil
}

func (f *fakeTenancy) RevokeKey(_ context.Context, id string) (tenant.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return tenant.Key{}, persistence.ErrNotFound
	}
	if k.k.RevokedAt == nil {
		now := time.Now().UTC()
		k.k.RevokedAt = &now
	}
	if f.evict != nil {
		f.evict(id)
	}
	return k.k, nil
}

// LookupKey and TouchKey implement apikey.Store.
func (f *fakeTenancy) LookupKey(_ context.Context, id string) (apikey.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	if !ok {
		return apikey.Record{}, persistence.ErrNotFound
	}
	m, member := f.members[k.k.UserID]
	return apikey.Record{ID: id, OrgID: k.k.OrgID, UserID: k.k.UserID, Username: k.k.Username, Role: m.Role, Scopes: k.k.Scopes,
		SecretHMAC: k.hmac, ExpiresAt: k.k.ExpiresAt, Revoked: k.k.RevokedAt != nil, Disabled: !member || m.OrgID != k.k.OrgID}, nil
}

func (f *fakeTenancy) TouchKey(context.Context, string, time.Time) error { return nil }

// tenantAccounts fills User.OrgID / OrgRole like the PostgreSQL SessionUser.
type tenantAccounts struct {
	*fakeAccounts
	ten *fakeTenancy
}

func (a tenantAccounts) SessionUser(ctx context.Context, idHash []byte) (User, error) {
	u, err := a.fakeAccounts.SessionUser(ctx, idHash)
	if err != nil {
		return u, err
	}
	m, err := a.ten.MemberByName(ctx, strings.ToLower(u.Username))
	if err != nil {
		return u, err
	}
	u.OrgID, u.OrgRole = m.OrgID, string(m.Role)
	return u, nil
}

// recordingSessions records the last CreateSession request.
type recordingSessions struct {
	*fakeSessions
	mu   sync.Mutex
	last CreateSessionRequest
}

func (r *recordingSessions) CreateSession(ctx context.Context, req CreateSessionRequest) (SessionView, bool, error) {
	r.mu.Lock()
	r.last = req
	r.mu.Unlock()
	return r.fakeSessions.CreateSession(ctx, req)
}

type tenantServer struct {
	*testServer
	acc *fakeAccounts
	ten *fakeTenancy
	ses *recordingSessions
}

func newTenantServer(t *testing.T) *tenantServer {
	t.Helper()
	ts := &tenantServer{}
	ts.testServer, ts.acc, _ = newSessionServer(t, func(c *Config) {
		acc := c.Accounts.(*fakeAccounts)
		ts.ten = newFakeTenancy(acc)
		v := apikey.NewVerifier(ts.ten, []byte("pepper-for-tests-0123456789abcdef"), nil)
		ts.ten.evict = v.Evict
		ts.ses = &recordingSessions{fakeSessions: c.Sessions.(*fakeSessions)}
		c.Accounts, c.Tenancy, c.APIKeys, c.Sessions = tenantAccounts{fakeAccounts: acc, ten: ts.ten}, ts.ten, v, ts.ses
	})
	return ts
}

func bearer(k string) map[string]string  { return map[string]string{"Authorization": "Bearer " + k} }
func xAPIKey(k string) map[string]string { return map[string]string{"x-api-key": k} }

// mintKey creates a key with a session cookie and returns the full key and its ID.
func (ts *tenantServer) mintKey(t *testing.T, cookie string, scopes ...string) (string, string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"name": "k", "scopes": scopes})
	st, body, _ := ts.do("POST", "/v1/keys", string(b), session(cookie))
	expect(t, st, body, 201, "")
	var out struct {
		Key string `json:"key"`
		ID  string `json:"id"`
	}
	decodeJSONBody(t, body, &out)
	if !strings.HasPrefix(out.Key, apikey.Prefix+out.ID+"_") {
		t.Fatalf("created key %s", body)
	}
	return out.Key, out.ID
}

func TestTenancyOffKeepsTodaysAuth(t *testing.T) {
	ts, _ := newAccountServer(t, nil)
	st, body, _ := ts.do("GET", "/v1/me", "", adminAuth)
	expect(t, st, body, 404, "not_found")
	g, _ := apikey.Generate([]byte("p"))
	st, body, _ = ts.do("GET", "/tasks", "", bearer(g.Full))
	expect(t, st, body, 401, "unauthorized") // abx_ Bearer is checked as the operator token, as today
	if _, err := New(Config{Store: newFakeStore(), Blobs: &fakeBlobs{}, ListenAddr: "127.0.0.1:1", Tenancy: newFakeTenancy(nil)}); err == nil {
		t.Fatal("Tenancy without Accounts/APIKeys must be refused")
	}
}

func TestMe(t *testing.T) {
	ts := newTenantServer(t)
	_, cookie := ts.acc.seedUser(t, "Alice")
	st, body, _ := ts.do("GET", "/v1/me", "", adminAuth)
	expect(t, st, body, 200, "")
	var me meJSON
	decodeJSONBody(t, body, &me)
	if !me.Operator || me.Username != "" || len(me.Scopes) != 2 {
		t.Fatalf("operator me %s", body)
	}
	st, body, _ = ts.do("GET", "/v1/me", "", session(cookie))
	expect(t, st, body, 200, "")
	me = meJSON{}
	decodeJSONBody(t, body, &me)
	if me.Operator || me.Username != "Alice" || me.Org == nil || me.Org.Name != "user-1" || me.Role != "owner" || me.KeyID != "" {
		t.Fatalf("user me %s", body)
	}
	full, id := ts.mintKey(t, cookie, "models")
	for _, hdr := range []map[string]string{bearer(full), xAPIKey(full)} {
		st, body, _ = ts.do("GET", "/v1/me", "", hdr)
		expect(t, st, body, 200, "")
		me = meJSON{}
		decodeJSONBody(t, body, &me)
		if me.KeyID != id || me.Username != "Alice" || len(me.Scopes) != 1 || me.Scopes[0] != "models" || me.Org.Name != "user-1" {
			t.Fatalf("key me %s", body)
		}
	}
	st, body, _ = ts.do("GET", "/v1/me", "", nil)
	expect(t, st, body, 401, "unauthorized")
}

// TestKeyActsAsUserOnRuntime: a runtime key uses the user-facing runtime endpoints as its user; the task and
// session record the key.
func TestKeyActsAsUserOnRuntime(t *testing.T) {
	ts := newTenantServer(t)
	_, cookie := ts.acc.seedUser(t, "alice")
	full, id := ts.mintKey(t, cookie, "runtime")
	st, body, _ := ts.do("POST", "/research", `{"request_id":"r1","topic":"x"}`, bearer(full))
	expect(t, st, body, 202, "")
	if ts.acc.lastReq.KeyID != id {
		t.Fatalf("research KeyID = %q", ts.acc.lastReq.KeyID)
	}
	var res CreateTaskResult
	decodeJSONBody(t, body, &res)
	st, body, _ = ts.do("GET", "/tasks/"+res.TaskID, "", xAPIKey(full))
	expect(t, st, body, 200, "")
	st, body, _ = ts.do("GET", "/tasks", "", xAPIKey(full))
	expect(t, st, body, 200, "")
	if !strings.Contains(string(body), res.TaskID) {
		t.Fatalf("own task listed: %s", body)
	}
	st, body, _ = ts.do("POST", "/sessions", `{"request_id":"s1"}`, bearer(full))
	expect(t, st, body, 201, "")
	if ts.ses.last.KeyID != id || ts.ses.last.OwnerUserID == 0 {
		t.Fatalf("session request %+v", ts.ses.last)
	}
	st, body, _ = ts.do("POST", "/sessions/s1/messages", `{"request_id":"m1","text":"hi","deep_research":false}`, bearer(full))
	expect(t, st, body, 202, "")
	if ts.ses.lastTurn.KeyID != id {
		t.Fatalf("turn KeyID = %q", ts.ses.lastTurn.KeyID)
	}
	// A cookie caller records no key.
	st, body, _ = ts.do("POST", "/sessions", `{"request_id":"s2"}`, session(cookie))
	expect(t, st, body, 201, "")
	if ts.ses.last.KeyID != "" {
		t.Fatalf("cookie session KeyID = %q", ts.ses.last.KeyID)
	}
	// Not with a key: logout (needs the cookie) and operator operations.
	st, body, _ = ts.do("POST", "/auth/logout", "", bearer(full))
	expect(t, st, body, 403, "permission_denied")
	st, body, _ = ts.do("POST", "/tasks", `{"request_id":"t1","spec":{}}`, bearer(full))
	expect(t, st, body, 403, "permission_denied")
	st, body, _ = ts.do("GET", "/server-info", "", bearer(full))
	expect(t, st, body, 403, "permission_denied")
}

func TestModelsKeyHasNoRuntime(t *testing.T) {
	ts := newTenantServer(t)
	_, cookie := ts.acc.seedUser(t, "alice")
	full, _ := ts.mintKey(t, cookie, "models")
	for _, op := range [][2]string{{"POST", "/research"}, {"GET", "/tasks"}, {"POST", "/sessions"}, {"GET", "/sessions"}, {"GET", "/auth/me"}} {
		st, body, _ := ts.do(op[0], op[1], `{"request_id":"r1","topic":"x"}`, bearer(full))
		expect(t, st, body, 403, "permission_denied")
	}
}

// TestKeyCannotManage: no key — not even an owner's key with every scope — can mint keys or manage members/orgs.
func TestKeyCannotManage(t *testing.T) {
	ts := newTenantServer(t)
	_, cookie := ts.acc.seedUser(t, "alice")
	ts.acc.seedUser(t, "bob")
	full, id := ts.mintKey(t, cookie, "runtime", "models")
	for _, op := range []struct{ method, path, body string }{
		{"POST", "/v1/keys", `{"name":"x","scopes":["models"]}`},
		{"GET", "/v1/keys", ""},
		{"DELETE", "/v1/keys/" + id, ""},
		{"POST", "/v1/orgs/members", `{"username":"bob","role":"member"}`},
		{"DELETE", "/v1/orgs/members/bob", ""},
		{"POST", "/v1/orgs", `{"name":"acme"}`},
	} {
		for _, hdr := range []map[string]string{bearer(full), xAPIKey(full)} {
			st, body, _ := ts.do(op.method, op.path, op.body, hdr)
			expect(t, st, body, 403, "permission_denied")
		}
	}
	if keys, _ := ts.ten.ListKeys(context.Background(), tenant.KeyFilter{}); len(keys) != 1 {
		t.Fatalf("a key minted keys: %d", len(keys))
	}
}

func TestInvalidKeys(t *testing.T) {
	ts := newTenantServer(t)
	_, cookie := ts.acc.seedUser(t, "alice")
	full, id := ts.mintKey(t, cookie, "runtime")
	st, body, _ := ts.do("GET", "/v1/me", "", bearer(full))
	expect(t, st, body, 200, "") // cached in the verifier now
	for name, hdr := range map[string]map[string]string{
		"malformed bearer":  bearer("abx_live_nope"),
		"malformed x-key":   xAPIKey("abx_live_nope"),
		"non-abx x-api-key": xAPIKey(adminToken),
		"key plus cookie":   {"Authorization": "Bearer abx_live_nope", "Cookie": SessionCookie + "=" + cookie},
	} {
		st, body, hdrs := ts.do("GET", "/v1/me", "", hdr)
		expect(t, st, body, 401, "invalid_api_key")
		if hdrs.Get("WWW-Authenticate") == "" {
			t.Errorf("%s: no WWW-Authenticate", name)
		}
	}
	// A wrong operator token is still the operator path.
	st, body, _ = ts.do("GET", "/v1/me", "", bearer("not-the-token"))
	expect(t, st, body, 401, "unauthorized")
	// Revocation takes effect at once (the fake NOTIFY evicts the verifier's cache).
	st, body, _ = ts.do("DELETE", "/v1/keys/"+id, "", session(cookie))
	expect(t, st, body, 200, "")
	st, body, _ = ts.do("GET", "/v1/me", "", bearer(full))
	expect(t, st, body, 401, "invalid_api_key")
	// Expired.
	exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	st, body, _ = ts.do("POST", "/v1/keys", fmt.Sprintf(`{"name":"e","scopes":["runtime"],"expires_at":%q}`, exp), session(cookie))
	expect(t, st, body, 201, "")
	var k struct {
		Key string `json:"key"`
		ID  string `json:"id"`
	}
	decodeJSONBody(t, body, &k)
	past := time.Now().Add(-time.Second)
	ts.ten.mu.Lock()
	ts.ten.keys[k.ID].k.ExpiresAt = &past
	ts.ten.mu.Unlock()
	ts.ten.evict(k.ID)
	st, body, _ = ts.do("GET", "/v1/me", "", bearer(k.Key))
	expect(t, st, body, 401, "invalid_api_key")
	// Bad requests.
	for _, b := range []string{`{"name":"","scopes":["runtime"]}`, `{"name":"x","scopes":["root"]}`, `{"name":"x","scopes":[]}`,
		`{"name":"x","scopes":["runtime"],"expires_at":"2000-01-01T00:00:00Z"}`, `{"name":"x","scopes":["runtime"],"extra":1}`} {
		st, body, _ := ts.do("POST", "/v1/keys", b, session(cookie))
		if st != 400 {
			t.Errorf("%s: %d %s", b, st, body)
		}
	}
}

// TestManagementMatrix: member / owner / operator / other org against keys and members.
func TestManagementMatrix(t *testing.T) {
	ts := newTenantServer(t)
	_, alice := ts.acc.seedUser(t, "alice")
	_, bob := ts.acc.seedUser(t, "bob")
	_, carol := ts.acc.seedUser(t, "carol")
	ts.acc.seedUser(t, "dave")

	// The operator creates org acme with alice as owner; alice adds bob.
	st, body, _ := ts.do("POST", "/v1/orgs", `{"name":"acme","owner":"alice"}`, adminAuth)
	expect(t, st, body, 201, "")
	st, body, _ = ts.do("POST", "/v1/orgs", `{"name":"acme"}`, adminAuth)
	expect(t, st, body, 409, "org_exists")
	st, body, _ = ts.do("POST", "/v1/orgs", `{"name":"user-9"}`, adminAuth)
	expect(t, st, body, 400, "invalid_org_name")
	st, body, _ = ts.do("POST", "/v1/orgs", `{"name":"beta"}`, session(alice))
	expect(t, st, body, 403, "forbidden")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"bob","role":"member"}`, session(alice))
	expect(t, st, body, 200, "")
	// bob (member) cannot manage members; carol (other org) cannot add to acme.
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"dave","role":"member"}`, session(bob))
	expect(t, st, body, 403, "forbidden")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"dave","role":"member","org":"acme"}`, session(carol))
	expect(t, st, body, 403, "forbidden")
	// The operator must name the org; bob is already in acme so carol's org cannot take him.
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"dave","role":"member"}`, adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"bob","role":"member","org":"user-3"}`, adminAuth)
	expect(t, st, body, 409, "user_in_other_org")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"nobody","role":"member"}`, session(alice))
	expect(t, st, body, 404, "user_not_found")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"dave","role":"member","org":"operator"}`, adminAuth)
	expect(t, st, body, 400, "invalid_request")

	_, bobKey := ts.mintKey(t, bob, "runtime")
	_, aliceKey := ts.mintKey(t, alice, "models")
	_, carolKey := ts.mintKey(t, carol, "models")
	list := func(cookie map[string]string) []string {
		st, body, _ := ts.do("GET", "/v1/keys", "", cookie)
		expect(t, st, body, 200, "")
		var out keyListJSON
		decodeJSONBody(t, body, &out)
		var ids []string
		for _, k := range out.Keys {
			ids = append(ids, k.ID)
		}
		return ids
	}
	if ids := list(session(bob)); len(ids) != 1 || ids[0] != bobKey {
		t.Fatalf("member sees own keys only: %v", ids)
	}
	if ids := list(session(alice)); len(ids) != 2 {
		t.Fatalf("owner sees the org's keys: %v", ids)
	}
	if ids := list(adminAuth); len(ids) != 3 {
		t.Fatalf("operator sees all keys: %v", ids)
	}
	// Revocation: member not someone else's; other org's owner not; owner yes; operator yes.
	st, body, _ = ts.do("DELETE", "/v1/keys/"+aliceKey, "", session(bob))
	expect(t, st, body, 404, "key_not_found")
	st, body, _ = ts.do("DELETE", "/v1/keys/"+bobKey, "", session(carol))
	expect(t, st, body, 404, "key_not_found")
	st, body, _ = ts.do("DELETE", "/v1/keys/"+bobKey, "", session(alice))
	expect(t, st, body, 200, "")
	st, body, _ = ts.do("DELETE", "/v1/keys/"+carolKey, "", adminAuth)
	expect(t, st, body, 200, "")
	st, body, _ = ts.do("DELETE", "/v1/keys/k_aaaaaaaaaaaaaaaa", "", adminAuth)
	expect(t, st, body, 404, "key_not_found")
	// Keys for others: a user cannot, the operator can (and must name the user).
	st, body, _ = ts.do("POST", "/v1/keys", `{"name":"x","scopes":["models"],"username":"bob"}`, session(alice))
	expect(t, st, body, 403, "forbidden")
	st, body, _ = ts.do("POST", "/v1/keys", `{"name":"x","scopes":["models"]}`, adminAuth)
	expect(t, st, body, 400, "invalid_request")
	st, body, _ = ts.do("POST", "/v1/keys", `{"name":"x","scopes":["models"],"username":"dave"}`, adminAuth)
	expect(t, st, body, 201, "")

	// Members: the last owner stays; removing bob returns him to his personal org.
	st, body, _ = ts.do("DELETE", "/v1/orgs/members/alice", "", session(alice))
	expect(t, st, body, 409, "last_owner")
	st, body, _ = ts.do("POST", "/v1/orgs/members", `{"username":"alice","role":"member"}`, session(alice))
	expect(t, st, body, 409, "last_owner")
	st, body, _ = ts.do("DELETE", "/v1/orgs/members/bob", "", session(bob))
	expect(t, st, body, 403, "forbidden")
	st, body, _ = ts.do("DELETE", "/v1/orgs/members/bob", "", session(alice))
	expect(t, st, body, 204, "")
	st, body, _ = ts.do("DELETE", "/v1/orgs/members/bob", "", session(alice))
	expect(t, st, body, 404, "member_not_found")
	st, body, _ = ts.do("DELETE", "/v1/orgs/members/carol?org=user-3", "", adminAuth)
	expect(t, st, body, 409, "personal_org")
	st, body, _ = ts.do("GET", "/v1/me", "", session(bob))
	expect(t, st, body, 200, "")
	var me meJSON
	decodeJSONBody(t, body, &me)
	if me.Org == nil || me.Org.Name != "user-2" || me.Role != "owner" {
		t.Fatalf("bob back in his personal org: %s", body)
	}
}
