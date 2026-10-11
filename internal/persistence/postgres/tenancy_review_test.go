package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// Regression tests for the G1 review (D:\agentbox\notes\gateway-program\g1-review.md).

// tenancyServer starts the real handler on PostgreSQL and returns a request helper and per-user cookies.
func tenancyServer(t *testing.T, s *Store, users ...string) (func(method, path, body, hk, hv string) (int, []byte), map[string]string) {
	t.Helper()
	ctx := context.Background()
	v := watchedVerifier(t, s)
	srv := httptest.NewUnstartedServer(nil)
	h, err := api.New(api.Config{Store: s, Blobs: noBlobs{}, ListenAddr: srv.Listener.Addr().String(), Token: "operator-token-0123456789",
		Accounts: s, ResearchSpec: func(topic string) (json.RawMessage, error) { return json.Marshal(map[string]string{"topic": topic}) },
		Tenancy: s.Tenancy(), APIKeys: v})
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = h
	srv.Start()
	t.Cleanup(srv.Close)
	cookie := map[string]string{}
	for _, name := range users {
		u, err := s.CreateUser(ctx, name, name, "h")
		if err != nil {
			t.Fatal(err)
		}
		id, idHash, _ := account.NewSessionID()
		if err := s.CreateSession(ctx, idHash, u.ID, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		cookie[name] = api.SessionCookie + "=" + id
	}
	do := func(method, path, body, hk, hv string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set(hk, hv)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	return do, cookie
}

// TestReviewHijackViaPersonalOrg (blocker): a self-registered user can no longer pull another user into their org;
// the victim's keys stay valid, and a user who did join an org can leave it by themselves.
func TestReviewHijackViaPersonalOrg(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	do, cookie := tenancyServer(t, s, "mallory", "victim")
	code, b := do("POST", "/v1/keys", `{"name":"ci","scopes":["runtime"]}`, "Cookie", cookie["victim"])
	if code != 201 {
		t.Fatalf("victim key: %d %s", code, b)
	}
	var created struct{ Key string }
	if err := json.Unmarshal(b, &created); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"username":"victim","role":"member"}`, `{"username":"victim","role":"member","org":"user-1"}`} {
		if code, b := do("POST", "/v1/orgs/members", body, "Cookie", cookie["mallory"]); code != 403 {
			t.Fatalf("mallory adds victim directly: %d %s", code, b)
		}
	}
	// Inviting into a personal org is refused as well.
	if code, b := do("POST", "/v1/orgs/invites", `{"username":"victim","role":"member"}`, "Cookie", cookie["mallory"]); code != 409 ||
		!strings.Contains(string(b), "personal_org_closed") {
		t.Fatalf("invite into a personal org: %d %s", code, b)
	}
	m, err := s.TenancyStore().MemberByName(ctx, "victim")
	if err != nil || m.OrgName != tenant.PersonalOrgName(m.UserID) {
		t.Fatalf("victim moved: %+v %v", m, err)
	}
	if code, _ := do("GET", "/v1/me", "", "x-api-key", created.Key); code != 200 {
		t.Fatalf("victim's key must stay valid: %d", code)
	}
	// Even the operator cannot put someone into another user's personal org.
	if code, b := do("POST", "/v1/orgs/members", `{"username":"victim","role":"member","org":"user-1"}`,
		"Authorization", "Bearer operator-token-0123456789"); code != 409 {
		t.Fatalf("operator into a personal org: %d %s", code, b)
	}
	// With consent: the operator creates acme (mallory owner), mallory invites, victim accepts — then may leave.
	if code, b := do("POST", "/v1/orgs", `{"name":"acme","owner":"mallory"}`, "Authorization", "Bearer operator-token-0123456789"); code != 201 {
		t.Fatalf("create acme: %d %s", code, b)
	}
	code, b = do("POST", "/v1/orgs/invites", `{"username":"victim","role":"member"}`, "Cookie", cookie["mallory"])
	if code != 201 {
		t.Fatalf("invite: %d %s", code, b)
	}
	var inv tenant.Invite
	if err := json.Unmarshal(b, &inv); err != nil {
		t.Fatal(err)
	}
	if code, b := do("POST", "/v1/orgs/invites/"+inv.ID+"/accept", "", "Cookie", cookie["mallory"]); code != 404 {
		t.Fatalf("only the invited user accepts: %d %s", code, b)
	}
	if code, b := do("POST", "/v1/orgs/invites/"+inv.ID+"/accept", "", "Cookie", cookie["victim"]); code != 200 {
		t.Fatalf("accept: %d %s", code, b)
	}
	if code, b := do("DELETE", "/v1/orgs/members/victim", "", "Cookie", cookie["victim"]); code != 204 {
		t.Fatalf("victim leaves: %d %s", code, b)
	}
	if m, _ := s.TenancyStore().MemberByName(ctx, "victim"); m.OrgName != tenant.PersonalOrgName(m.UserID) || m.Role != tenant.Owner {
		t.Fatalf("victim back home: %+v", m)
	}
	if code, b := do("DELETE", "/v1/orgs/members/mallory", "", "Cookie", cookie["mallory"]); code != 409 {
		t.Fatalf("the last owner cannot leave: %d %s", code, b)
	}
	expectNoDBViolations(t, s)
}

// TestReviewOwnerlessPersonalOrg: a move out of the user's current org waits for that org's lock, so a concurrent
// change of the old org cannot slip past the size check (the move re-checks after the lock is released).
func TestReviewOwnerlessPersonalOrg(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{LockTimeout: 5 * time.Second, OpDeadline: 20 * time.Second})
	ten := s.TenancyStore()
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	carol, _ := s.CreateUser(ctx, "carol", "carol", "h")
	o, _ := ten.CreateOrg(ctx, "acme")
	personal := orgIDByName(t, s, tenant.PersonalOrgName(bob.ID))
	// tx2 holds bob's personal org row (like a member change of that org) and puts carol into it.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SELECT name FROM orgs WHERE id = $1 FOR UPDATE", personal); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT id FROM users WHERE id = $1 FOR UPDATE", carol.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ten.AddMember(ctx, o.ID, "bob", tenant.Owner); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("the move must wait for the old org's lock, returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if _, err := tx.Exec(ctx, "DELETE FROM org_members WHERE user_id = $1", carol.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO org_members (org_id, user_id, role) VALUES ($1, $2, 'member')", personal, carol.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, tenant.ErrUserInOtherOrg) {
		t.Fatalf("bob's personal org is no longer his alone: %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM org_members WHERE org_id = "+strconv.FormatInt(personal, 10)+" AND role = 'owner'"); n != 1 {
		t.Fatalf("owners of user-bob: %d", n)
	}
}

// TestCreateOrgWithOwnerAtomic: the org and its owner are created together or not at all.
func TestCreateOrgWithOwnerAtomic(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	if _, err := s.CreateUser(ctx, "alice", "alice", "h"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(ctx, "bob", "bob", "h"); err != nil {
		t.Fatal(err)
	}
	o, m, err := ten.CreateOrgWithOwner(ctx, "acme", "alice")
	if err != nil || o.Name != "acme" || o.Members != 1 || m.Role != tenant.Owner || m.OrgID != o.ID {
		t.Fatalf("CreateOrgWithOwner = %+v %+v %v", o, m, err)
	}
	for _, c := range []struct {
		name, owner string
		want        error
	}{{"beta", "alice", tenant.ErrUserInOtherOrg}, {"gamma", "nobody", persistence.ErrNotFound}, {"acme", "bob", persistence.ErrConflict}} {
		if _, _, err := ten.CreateOrgWithOwner(ctx, c.name, c.owner); !errors.Is(err, c.want) {
			t.Errorf("%s/%s: %v, want %v", c.name, c.owner, err, c.want)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM orgs WHERE name IN ('beta', 'gamma')"); n != 0 {
		t.Fatalf("failed creations left %d orgs", n)
	}
	if m, _ := ten.MemberByName(ctx, "bob"); m.OrgName != tenant.PersonalOrgName(m.UserID) {
		t.Fatalf("bob unchanged: %+v", m)
	}
}

// TestInvitesStore: create/renew, visibility filters, expiry, accept (move + key revocation), decline.
func TestInvitesStore(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	if _, err := s.CreateUser(ctx, "carol", "carol", "h"); err != nil {
		t.Fatal(err)
	}
	acme, _, err := ten.CreateOrgWithOwner(ctx, "acme", "alice")
	if err != nil {
		t.Fatal(err)
	}
	exp := time.Now().Add(tenant.InviteTTL)
	inv, err := ten.CreateInvite(ctx, acme.ID, "bob", tenant.Member, alice.ID, exp)
	if err != nil || inv.OrgName != "acme" || inv.Username != "bob" || inv.InvitedBy != "alice" || !apikeyLikeInviteID(inv.ID) {
		t.Fatalf("CreateInvite = %+v %v", inv, err)
	}
	again, err := ten.CreateInvite(ctx, acme.ID, "bob", tenant.Owner, alice.ID, exp)
	if err != nil || again.ID != inv.ID || again.Role != tenant.Owner {
		t.Fatalf("renew keeps the id: %+v %v", again, err)
	}
	if _, err := ten.CreateInvite(ctx, acme.ID, "alice", tenant.Member, alice.ID, exp); !errors.Is(err, tenant.ErrAlreadyMember) {
		t.Fatalf("already member: %v", err)
	}
	if _, err := ten.CreateInvite(ctx, orgIDByName(t, s, "user-3"), "bob", tenant.Member, alice.ID, exp); !errors.Is(err, tenant.ErrPersonalOrgClosed) {
		t.Fatalf("personal org: %v", err)
	}
	if _, err := ten.CreateInvite(ctx, acme.ID, "nobody", tenant.Member, alice.ID, exp); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("unknown user: %v", err)
	}
	expired, err := ten.CreateInvite(ctx, acme.ID, "carol", tenant.Member, alice.ID, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ten.GetInvite(ctx, expired.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("expired invite: %v", err)
	}
	carol, _ := ten.MemberByName(ctx, "carol")
	if _, err := ten.AcceptInvite(ctx, expired.ID, carol.UserID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("accept expired: %v", err)
	}
	if l, _ := ten.ListInvites(ctx, tenant.InviteFilter{UserID: bob.ID}); len(l) != 1 || l[0].ID != inv.ID {
		t.Fatalf("bob's invites: %+v", l)
	}
	if l, _ := ten.ListInvites(ctx, tenant.InviteFilter{UserID: carol.UserID}); len(l) != 0 {
		t.Fatalf("carol sees nothing pending: %+v", l)
	}
	if l, _ := ten.ListInvites(ctx, tenant.InviteFilter{UserID: alice.ID, OrgID: acme.ID}); len(l) != 1 {
		t.Fatalf("acme's pending invites: %+v", l)
	}
	if _, err := ten.AcceptInvite(ctx, inv.ID, alice.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("another user's invite: %v", err)
	}
	notes := listen(t, s)
	g := mustKey(t, s, bob.ID, tenant.Runtime)
	m, err := ten.AcceptInvite(ctx, inv.ID, bob.ID)
	if err != nil || m.OrgID != acme.ID || m.Role != tenant.Owner {
		t.Fatalf("accept: %+v %v", m, err)
	}
	notes.expect(t, g.ID)
	if _, err := ten.GetInvite(ctx, inv.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("an accepted invite is gone")
	}
	d, err := ten.CreateInvite(ctx, acme.ID, "carol", tenant.Member, alice.ID, exp)
	if err != nil {
		t.Fatal(err)
	}
	if err := ten.DeleteInvite(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if err := ten.DeleteInvite(ctx, d.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
	expectNoDBViolations(t, s)
}

func apikeyLikeInviteID(id string) bool {
	return strings.HasPrefix(id, "inv_") && len(id) == 20 && strings.Trim(id[4:], "abcdefghijklmnopqrstuvwxyz234567") == ""
}

// TestEnsurePepper: first start creates and records the pepper; a restart reuses it; a missing file with keys in
// the database, or another server's pepper, refuses to start (apikey.ErrPepper).
func TestEnsurePepper(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	dir := t.TempDir()
	p1, err := apikey.EnsurePepper(ctx, dir, s.APIKeys())
	if err != nil {
		t.Fatal(err)
	}
	if fp, keys, err := s.TenancyStore().PepperState(ctx); err != nil || keys || string(fp) != string(apikey.Fingerprint(p1)) {
		t.Fatalf("recorded fingerprint: %v %v", keys, err)
	}
	p2, err := apikey.EnsurePepper(ctx, dir, s.APIKeys())
	if err != nil || string(p1) != string(p2) {
		t.Fatalf("restart: %v", err)
	}
	other := t.TempDir()
	if _, err := apikey.LoadOrCreatePepper(other); err != nil {
		t.Fatal(err)
	}
	if _, err := apikey.EnsurePepper(ctx, other, s.APIKeys()); !errors.Is(err, apikey.ErrPepper) {
		t.Fatalf("another server's pepper: %v", err)
	}
	if _, err := apikey.EnsurePepper(ctx, t.TempDir(), s.APIKeys()); !errors.Is(err, apikey.ErrPepper) {
		t.Fatalf("missing pepper with a recorded fingerprint: %v", err)
	}
	// Keys but no fingerprint (a database from before the check): a missing file is refused, not re-created.
	u, _ := s.CreateUser(ctx, "alice", "alice", "h")
	mustKey(t, s, u.ID, tenant.Models)
	if _, err := s.pool.Exec(ctx, "DELETE FROM apikey_pepper"); err != nil {
		t.Fatal(err)
	}
	empty := t.TempDir()
	if _, err := apikey.EnsurePepper(ctx, empty, s.APIKeys()); !errors.Is(err, apikey.ErrPepper) {
		t.Fatalf("missing pepper with keys: %v", err)
	}
	if _, err := apikey.LoadPepper(empty); err == nil {
		t.Fatal("no pepper may be created when keys exist")
	}
}
