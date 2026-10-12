package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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

// TestRRStress (re-review R2-1): random concurrent moves, accepts, leaves, role changes, invites and key creation
// finish without deadlock or contention errors; no org with members lacks an owner, no personal org holds another
// user, and the invariants hold. Trimmed from the reviewer's 8 workers x 60 ops to run in a few seconds.
func TestRRStress(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	const nu = 8
	ids := map[string]int64{}
	for i := 0; i < nu; i++ {
		n := fmt.Sprintf("su%d", i)
		u, err := s.CreateUser(ctx, n, n, "h")
		if err != nil {
			t.Fatal(err)
		}
		ids[n] = u.ID
	}
	var orgs []int64
	for i := 0; i < 3; i++ {
		o, _, err := ten.CreateOrgWithOwner(ctx, fmt.Sprintf("so%d", i), fmt.Sprintf("su%d", i))
		if err != nil {
			t.Fatal(err)
		}
		orgs = append(orgs, o.ID)
	}
	var mu sync.Mutex
	var bad []error
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for k := 0; k < 25; k++ {
				u := fmt.Sprintf("su%d", r.Intn(nu))
				o := orgs[r.Intn(len(orgs))]
				role := tenant.Member
				if r.Intn(2) == 0 {
					role = tenant.Owner
				}
				inviter := ids[fmt.Sprintf("su%d", r.Intn(nu))]
				var err error
				switch r.Intn(5) {
				case 0:
					_, err = ten.AddMember(ctx, o, u, role)
				case 1:
					err = ten.RemoveMember(ctx, o, u)
				case 2:
					var inv tenant.Invite
					if inv, err = ten.CreateInvite(ctx, o, u, role, inviter, time.Now().Add(time.Hour)); err == nil {
						_, err = ten.AcceptInvite(ctx, inv.ID, ids[u])
					}
				case 3:
					g, _ := apikey.Generate(testPepper)
					_, err = ten.CreateKey(ctx, tenant.NewKey{ID: g.ID, UserID: ids[u], Name: "k", SecretHMAC: g.SecretHMAC, Scopes: tenant.Runtime})
				case 4:
					var inv tenant.Invite
					if inv, err = ten.CreateInvite(ctx, o, u, role, inviter, time.Now().Add(time.Hour)); err == nil {
						err = ten.DeleteInvite(ctx, inv.ID)
					}
				}
				if err != nil && !isDomain(err) && !errors.Is(err, tenant.ErrUserInOtherOrg) && !errors.Is(err, tenant.ErrLastOwner) &&
					!errors.Is(err, tenant.ErrAlreadyMember) && !errors.Is(err, tenant.ErrPersonalOrg) && !errors.Is(err, tenant.ErrPersonalOrgClosed) {
					mu.Lock()
					bad = append(bad, err)
					mu.Unlock()
				}
			}
		}(int64(w))
	}
	wg.Wait()
	for _, e := range bad {
		t.Errorf("non-domain error: %v", e)
	}
	if n := count(t, s, `SELECT count(*) FROM orgs o WHERE o.name <> 'operator' AND EXISTS (SELECT 1 FROM org_members m WHERE m.org_id = o.id)
		AND NOT EXISTS (SELECT 1 FROM org_members m WHERE m.org_id = o.id AND m.role = 'owner')`); n != 0 {
		t.Errorf("%d orgs with members but no owner", n)
	}
	if n := count(t, s, `SELECT count(*) FROM org_members m JOIN orgs o ON o.id = m.org_id WHERE o.name ~ '^user-[0-9]+$' AND o.name <> 'user-' || m.user_id`); n != 0 {
		t.Errorf("%d foreign members in personal orgs", n)
	}
	if n := count(t, s, `SELECT count(*) FROM org_invites i WHERE NOT EXISTS (SELECT 1 FROM org_members m
		WHERE m.org_id = i.org_id AND m.user_id = i.invited_by AND m.role = 'owner')`); n != 0 {
		t.Logf("%d pending invites from non-owners (created by the test with random inviters; refused at accept)", n)
	}
	expectNoDBViolations(t, s)
}

// TestAcceptStaleInviter (R2-2): an invite whose inviter is no longer an owner of the org is refused at accept
// (404) and deleted; demoting or removing an owner deletes the invites they sent.
func TestAcceptStaleInviter(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	carol, _ := s.CreateUser(ctx, "carol", "carol", "h")
	if _, err := s.CreateUser(ctx, "dave", "dave", "h"); err != nil {
		t.Fatal(err)
	}
	acme, _, err := ten.CreateOrgWithOwner(ctx, "acme", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ten.AddMember(ctx, acme.ID, "bob", tenant.Owner); err != nil {
		t.Fatal(err)
	}
	exp := time.Now().Add(time.Hour)
	// The inviter lost owner rights without going through the store (e.g. a raw role change): refused at accept.
	stale, err := ten.CreateInvite(ctx, acme.ID, "carol", tenant.Owner, bob.ID, exp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE org_members SET role = 'member' WHERE user_id = $1", bob.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := ten.AcceptInvite(ctx, stale.ID, carol.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("accept of a stale invite: %v", err)
	}
	if _, err := ten.GetInvite(ctx, stale.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("the stale invite must be deleted")
	}
	if m, _ := ten.MemberByName(ctx, "carol"); m.OrgName != tenant.PersonalOrgName(carol.ID) {
		t.Fatalf("carol must not have moved: %+v", m)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE org_members SET role = 'owner' WHERE user_id = $1", bob.ID); err != nil {
		t.Fatal(err)
	}
	// Demotion through the store deletes bob's sent invites; alice's stay.
	fromBob, _ := ten.CreateInvite(ctx, acme.ID, "carol", tenant.Member, bob.ID, exp)
	fromAlice, _ := ten.CreateInvite(ctx, acme.ID, "dave", tenant.Member, alice.ID, exp)
	if _, err := ten.AddMember(ctx, acme.ID, "bob", tenant.Member); err != nil {
		t.Fatal(err)
	}
	if _, err := ten.GetInvite(ctx, fromBob.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("a demoted owner's invites must be deleted")
	}
	if _, err := ten.GetInvite(ctx, fromAlice.ID); err != nil {
		t.Fatalf("other owners' invites stay: %v", err)
	}
	// Removal deletes them too.
	if _, err := ten.AddMember(ctx, acme.ID, "bob", tenant.Owner); err != nil {
		t.Fatal(err)
	}
	fromBob, _ = ten.CreateInvite(ctx, acme.ID, "carol", tenant.Member, bob.ID, exp)
	if err := ten.RemoveMember(ctx, acme.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := ten.GetInvite(ctx, fromBob.ID); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("a removed owner's invites must be deleted")
	}
	expectNoDBViolations(t, s)
}

// TestInviteLockOrder: invite rows are locked only after the user and org rows (users → orgs → invites), the order
// member changes use when they drop a demoted or removed owner's sent invites. Before the fix AcceptInvite locked
// the invite before the org, forming a cycle with a concurrent member change that surfaced as a lock timeout (the
// TestRRStress flake). The drop below runs with a lock timeout under PostgreSQL's 1 s deadlock_timeout, so a cycle
// fails the test instead of being resolved by the deadlock detector and a retry.
func TestInviteLockOrder(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{LockTimeout: 5 * time.Second, OpDeadline: 20 * time.Second})
	ten := s.TenancyStore()
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	acme, _, err := ten.CreateOrgWithOwner(ctx, "acme", "alice")
	if err != nil {
		t.Fatal(err)
	}
	// memberChange starts what AddMember/RemoveMember of alice does: users FOR UPDATE, then the org row.
	memberChange := func() pgx.Tx {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "SELECT id FROM users WHERE id = $1 FOR UPDATE", alice.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "SELECT name FROM orgs WHERE id = $1 FOR NO KEY UPDATE", acme.ID); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	dropSent := func(tx pgx.Tx) {
		t.Helper()
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '500ms'"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM org_invites WHERE invited_by = $1 AND org_id = $2", alice.ID, acme.ID); err != nil {
			rollback(tx) // release the locks so the waiting accept can finish
			t.Fatalf("dropping the sent invites waits on a lock the invite operation holds: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("accept", func(t *testing.T) {
		inv, err := ten.CreateInvite(ctx, acme.ID, "bob", tenant.Member, alice.ID, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		tx := memberChange()
		done := make(chan error, 1)
		go func() { _, err := ten.AcceptInvite(ctx, inv.ID, bob.ID); done <- err }()
		time.Sleep(300 * time.Millisecond) // AcceptInvite now waits for the org row
		var acceptErr error
		received := false
		defer func() { // never leave the accept running past the test
			if !received {
				<-done
			}
		}()
		dropSent(tx)
		acceptErr, received = <-done, true
		if err := acceptErr; !errors.Is(err, persistence.ErrNotFound) {
			t.Fatalf("the invite was dropped while the accept waited: %v", err)
		}
		if m, _ := ten.MemberByName(ctx, "bob"); m.OrgName != tenant.PersonalOrgName(bob.ID) {
			t.Fatalf("bob must not have moved: %+v", m)
		}
	})
}
