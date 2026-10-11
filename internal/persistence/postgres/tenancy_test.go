package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/apikey"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// undo0012 把库还原为只应用了 0011 的形状；回滚到更早迁移的测试先执行它。
const undo0012 = `DROP TRIGGER tasks_fill_org ON tasks; DROP TRIGGER sessions_fill_org ON sessions; DROP FUNCTION agentbox_fill_org();
	ALTER TABLE tasks DROP COLUMN org_id, DROP COLUMN key_id;
	ALTER TABLE sessions DROP COLUMN org_id, DROP COLUMN key_id;
	DROP TABLE limits, api_keys, org_members, orgs;
	DELETE FROM schema_migrations WHERE version >= 12;
`

var testPepper = []byte("0123456789abcdef0123456789abcdef")

func orgIDByName(t *testing.T, s *Store, name string) int64 {
	t.Helper()
	var id int64
	if err := s.pool.QueryRow(context.Background(), "SELECT id FROM orgs WHERE name = $1", name).Scan(&id); err != nil {
		t.Fatalf("org %s: %v", name, err)
	}
	return id
}

func rowOrg(t *testing.T, s *Store, table, idCol, id string) (int64, string) {
	t.Helper()
	var org int64
	var key *string
	if err := s.pool.QueryRow(context.Background(), "SELECT org_id, key_id FROM "+table+" WHERE "+idCol+" = $1", id).Scan(&org, &key); err != nil {
		t.Fatalf("%s %s: %v", table, id, err)
	}
	if key == nil {
		return org, ""
	}
	return org, *key
}

// TestMigration0012Backfill：0011 的库升级到 0012：每个用户一个个人 org user-<id>（owner），保留 org operator；
// 已有任务与会话按 owner 归入其个人 org，无主的归 operator；之后的不变量全部成立。
func TestMigration0012Backfill(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	if _, err := s.pool.Exec(ctx, undo0012); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO users (username, username_key, password_hash) VALUES ('Alice', 'alice', 'h'), ('bob', 'bob', 'h');
		INSERT INTO tasks (task_id, spec_json, status, max_fault_retries, owner_user_id)
			SELECT 'ta', '{}', 'queued', 0, id FROM users WHERE username_key = 'alice';
		INSERT INTO tasks (task_id, spec_json, status, max_fault_retries) VALUES ('top', '{}', 'queued', 0);
		INSERT INTO sessions (session_id, owner_user_id, status) SELECT 'sb', id, 'idle' FROM users WHERE username_key = 'bob';
		INSERT INTO sessions (session_id, status) VALUES ('sop', 'idle')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ten := s.TenancyStore()
	alice, err := ten.MemberByName(ctx, "alice")
	if err != nil || alice.OrgName != tenant.PersonalOrgName(alice.UserID) || alice.Role != tenant.Owner || alice.Username != "Alice" {
		t.Fatalf("alice = %+v, %v", alice, err)
	}
	bob, err := ten.MemberByName(ctx, "bob")
	if err != nil || bob.OrgName != tenant.PersonalOrgName(bob.UserID) || bob.OrgID == alice.OrgID {
		t.Fatalf("bob = %+v, %v", bob, err)
	}
	op := orgIDByName(t, s, tenant.OperatorOrg)
	for _, c := range []struct {
		table, col, id string
		want           int64
	}{{"tasks", "task_id", "ta", alice.OrgID}, {"tasks", "task_id", "top", op}, {"sessions", "session_id", "sb", bob.OrgID}, {"sessions", "session_id", "sop", op}} {
		if got, key := rowOrg(t, s, c.table, c.col, c.id); got != c.want || key != "" {
			t.Errorf("%s %s: org %d key %q, want %d", c.table, c.id, got, key, c.want)
		}
	}
	if n := count(t, s, "SELECT count(*) FROM orgs"); n != 3 {
		t.Fatalf("orgs = %d, want operator + 2 personal", n)
	}
	// New rows after the migration get an org from the trigger.
	if _, err := s.pool.Exec(ctx, `INSERT INTO tasks (task_id, spec_json, status, max_fault_retries) VALUES ('later', '{}', 'queued', 0)`); err != nil {
		t.Fatal(err)
	}
	if got, _ := rowOrg(t, s, "tasks", "task_id", "later"); got != op {
		t.Fatalf("later task org %d", got)
	}
	// A user without an org cannot own a task (programming error, rejected by the trigger).
	if _, err := s.pool.Exec(ctx, `INSERT INTO users (username, username_key, password_hash) VALUES ('orphan', 'orphan', 'h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO tasks (task_id, spec_json, status, max_fault_retries, owner_user_id)
		SELECT 'orph', '{}', 'queued', 0, id FROM users WHERE username_key = 'orphan'`); err == nil {
		t.Fatal("a task owned by a user without an org must be rejected")
	}
}

// TestTenancyOrgsAndMembers：注册建个人 org；建 org、迁入（旧 org 的 key 吊销并 NOTIFY）、他 org 冲突、最后一个 owner、
// 个人 org 不能离开、移出回到个人 org。
func TestTenancyOrgsAndMembers(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	if _, err := s.CreateUser(ctx, "carol", "carol", "h"); err != nil {
		t.Fatal(err)
	}
	m, err := ten.MemberByName(ctx, "alice")
	if err != nil || m.OrgName != tenant.PersonalOrgName(alice.ID) || m.Role != tenant.Owner {
		t.Fatalf("personal org at sign-up: %+v, %v", m, err)
	}
	if _, err := ten.MemberByName(ctx, "nobody"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("unknown user: %v", err)
	}
	if _, err := ten.CreateOrg(ctx, "operator"); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("reserved name: %v", err)
	}
	acme, err := ten.CreateOrg(ctx, "acme")
	if err != nil || acme.Name != "acme" || acme.Members != 0 {
		t.Fatalf("CreateOrg = %+v, %v", acme, err)
	}
	if _, err := ten.CreateOrg(ctx, "acme"); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("duplicate org: %v", err)
	}

	notes := listen(t, s)
	g := mustKey(t, s, bob.ID, tenant.Runtime) // bob's key in his personal org
	if _, err := ten.AddMember(ctx, acme.ID, "alice", tenant.Owner); err != nil {
		t.Fatal(err)
	}
	if _, err := ten.AddMember(ctx, acme.ID, "bob", tenant.Member); err != nil {
		t.Fatal(err)
	}
	notes.expect(t, g.ID) // moving bob revoked his personal-org key
	if k, _ := ten.GetKey(ctx, g.ID); k.RevokedAt == nil {
		t.Fatal("the moved user's old key must be revoked")
	}
	if _, err := ten.AddMember(ctx, orgIDByName(t, s, tenant.PersonalOrgName(alice.ID)), "bob", tenant.Member); !errors.Is(err, tenant.ErrUserInOtherOrg) {
		t.Fatalf("user in another org: %v", err)
	}
	if _, err := ten.AddMember(ctx, orgIDByName(t, s, tenant.OperatorOrg), "carol", tenant.Member); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("operator org takes no members: %v", err)
	}
	if _, err := ten.AddMember(ctx, acme.ID, "alice", tenant.Member); !errors.Is(err, tenant.ErrLastOwner) {
		t.Fatalf("demote last owner: %v", err)
	}
	if err := ten.RemoveMember(ctx, acme.ID, "alice"); !errors.Is(err, tenant.ErrLastOwner) {
		t.Fatalf("remove last owner: %v", err)
	}
	if err := ten.RemoveMember(ctx, orgIDByName(t, s, "user-3"), "carol"); !errors.Is(err, tenant.ErrPersonalOrg) {
		t.Fatalf("leave personal org: %v", err)
	}
	if err := ten.RemoveMember(ctx, acme.ID, "carol"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("not a member: %v", err)
	}
	if m, err := ten.AddMember(ctx, acme.ID, "bob", tenant.Owner); err != nil || m.Role != tenant.Owner {
		t.Fatalf("promote: %+v, %v", m, err)
	}
	g2 := mustKey(t, s, bob.ID, tenant.Models) // in acme
	if err := ten.RemoveMember(ctx, acme.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	notes.expect(t, g2.ID)
	m, err = ten.MemberByName(ctx, "bob")
	if err != nil || m.OrgName != tenant.PersonalOrgName(bob.ID) || m.Role != tenant.Owner {
		t.Fatalf("bob back home: %+v, %v", m, err)
	}
	orgs, err := ten.ListOrgs(ctx)
	if err != nil || len(orgs) != 5 { // operator, 3 personal, acme
		t.Fatalf("ListOrgs = %+v, %v", orgs, err)
	}
	if o, err := ten.OrgByName(ctx, "acme"); err != nil || o.Members != 1 {
		t.Fatalf("OrgByName = %+v, %v", o, err)
	}
	expectNoDBViolations(t, s)
}

func mustKey(t *testing.T, s *Store, userID int64, scopes tenant.Scopes) apikey.Generated {
	t.Helper()
	g, err := apikey.Generate(testPepper)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TenancyStore().CreateKey(context.Background(), tenant.NewKey{ID: g.ID, UserID: userID, Name: "k", SecretHMAC: g.SecretHMAC,
		Scopes: scopes}); err != nil {
		t.Fatal(err)
	}
	return g
}

type notifications struct{ ch chan string }

// listen starts LISTEN apikey_changed and waits until it is in effect.
func listen(t *testing.T, s *Store) *notifications {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n := &notifications{ch: make(chan string, 64)}
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.TenancyStore().ListenKeyChanges(ctx, func() { close(ready) }, func(p string) { n.ch <- p })
	}()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("LISTEN not ready")
	}
	return n
}

func (n *notifications) expect(t *testing.T, payload string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case p := <-n.ch:
			if p == payload {
				return
			}
		case <-deadline:
			t.Fatalf("no NOTIFY apikey_changed %q", payload)
		}
	}
}

// readyListener signals when the Verifier's subscription is in effect.
type readyListener struct {
	apikey.Listener
	once  sync.Once
	ready chan struct{}
}

func (l *readyListener) ListenKeyChanges(ctx context.Context, ready func(), on func(string)) error {
	return l.Listener.ListenKeyChanges(ctx, func() { ready(); l.once.Do(func() { close(l.ready) }) }, on)
}

// watchedVerifier returns a Verifier subscribed to apikey_changed.
func watchedVerifier(t *testing.T, s *Store) *apikey.Verifier {
	t.Helper()
	v := apikey.NewVerifier(s.APIKeys(), testPepper, nil)
	l := &readyListener{Listener: s.APIKeys(), ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); v.Watch(ctx, l, nil) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-l.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("verifier not subscribed")
	}
	return v
}

// eventuallyInvalid waits (well under the 30 s cache TTL) until the key no longer verifies.
func eventuallyInvalid(t *testing.T, v *apikey.Verifier, full string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := v.Verify(context.Background(), full)
		if errors.Is(err, apikey.ErrInvalidKey) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("key still valid after NOTIFY: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAPIKeysStore：建 key、读取、过滤、吊销 + NOTIFY 使缓存立即失效、last_used_at 每分钟至多一次、停用用户 NOTIFY "*"。
func TestAPIKeysStore(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	ten := s.TenancyStore()
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	bob, _ := s.CreateUser(ctx, "bob", "bob", "h")
	v := watchedVerifier(t, s)

	ga := mustKey(t, s, alice.ID, tenant.Runtime)
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	gb, _ := apikey.Generate(testPepper)
	kb, err := ten.CreateKey(ctx, tenant.NewKey{ID: gb.ID, UserID: bob.ID, Name: "ci", SecretHMAC: gb.SecretHMAC, Scopes: tenant.AllScopes, ExpiresAt: &exp})
	if err != nil || kb.Username != "bob" || kb.Scopes != tenant.AllScopes || kb.ExpiresAt == nil || !kb.ExpiresAt.Equal(exp) {
		t.Fatalf("CreateKey = %+v, %v", kb, err)
	}
	// Rerun with the same HMAC is the same key; another HMAC under the same ID is a conflict.
	if _, err := ten.CreateKey(ctx, tenant.NewKey{ID: gb.ID, UserID: bob.ID, Name: "ci", SecretHMAC: gb.SecretHMAC, Scopes: tenant.AllScopes}); err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if _, err := ten.CreateKey(ctx, tenant.NewKey{ID: gb.ID, UserID: bob.ID, Name: "ci", SecretHMAC: ga.SecretHMAC, Scopes: tenant.AllScopes}); !errors.Is(err, persistence.ErrConflict) {
		t.Fatalf("ID collision: %v", err)
	}
	if n := count(t, s, "SELECT count(*) FROM api_keys WHERE secret_hmac = $1", gb.SecretHMAC); n != 1 {
		t.Fatal("only the HMAC is stored")
	}
	if n := count(t, s, "SELECT count(*) FROM api_keys WHERE position($1::bytea IN secret_hmac) > 0", []byte(gb.Full)); n != 0 {
		t.Fatal("the secret must not be stored")
	}
	all, _ := ten.ListKeys(ctx, tenant.KeyFilter{})
	mine, _ := ten.ListKeys(ctx, tenant.KeyFilter{UserID: alice.ID})
	if len(all) != 2 || len(mine) != 1 || mine[0].ID != ga.ID {
		t.Fatalf("ListKeys all=%d mine=%+v", len(all), mine)
	}

	id, err := v.Verify(ctx, ga.Full)
	if err != nil || id.Principal.UserID != alice.ID || id.Principal.Role != tenant.Owner || id.Principal.Scopes != tenant.Runtime || id.Username != "alice" {
		t.Fatalf("Verify = %+v, %v", id, err)
	}
	k, _ := ten.GetKey(ctx, ga.ID)
	if k.LastUsedAt == nil {
		t.Fatal("last_used_at written on first use")
	}
	first := *k.LastUsedAt
	if err := ten.TouchKey(ctx, ga.ID, first.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if k, _ = ten.GetKey(ctx, ga.ID); !k.LastUsedAt.Equal(first) {
		t.Fatalf("touch within a minute must not write: %v", k.LastUsedAt)
	}
	if err := ten.TouchKey(ctx, ga.ID, first.Add(61*time.Second)); err != nil {
		t.Fatal(err)
	}
	if k, _ = ten.GetKey(ctx, ga.ID); k.LastUsedAt.Equal(first) {
		t.Fatal("touch after a minute writes")
	}

	// Revoke: NOTIFY evicts the cached key at once (well before the 30 s TTL).
	if _, err := ten.RevokeKey(ctx, ga.ID); err != nil {
		t.Fatal(err)
	}
	eventuallyInvalid(t, v, ga.Full)
	if r1, _ := ten.RevokeKey(ctx, ga.ID); r1.RevokedAt == nil {
		t.Fatal("revoke is idempotent")
	}
	if _, err := ten.RevokeKey(ctx, "k_aaaaaaaaaaaaaaaa"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("revoke unknown: %v", err)
	}

	// Disabling the user evicts every key ("*") and LookupKey reports Disabled.
	if _, err := v.Verify(ctx, gb.Full); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDisabled(ctx, "bob", true); err != nil {
		t.Fatal(err)
	}
	eventuallyInvalid(t, v, gb.Full)
	if r, _ := ten.LookupKey(ctx, gb.ID); !r.Disabled {
		t.Fatal("disabled user")
	}
	if err := s.SetDisabled(ctx, "bob", false); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) { // the "*" NOTIFY evicts the disabled record
		if _, err := v.Verify(ctx, gb.Full); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("re-enabled: %v", err)
		}
	}
	if _, err := ten.LookupKey(ctx, "k_bbbbbbbbbbbbbbbb"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := ten.CreateKey(ctx, tenant.NewKey{ID: "bad", UserID: bob.ID, Name: "x", SecretHMAC: gb.SecretHMAC, Scopes: tenant.Models}); !errors.Is(err, persistence.ErrInvalid) {
		t.Fatalf("bad id: %v", err)
	}
	expectNoDBViolations(t, s)
}

// TestTenancyOwnershipOfRows：任务与会话的 org/key：运维 → operator；用户研究、会话与 turn 带 key 时记 key 并归 key
// 的 org；运维在用户会话中建的 turn 归会话所有者的 org。不变量 G4、G5 成立，破坏后被报告。
func TestTenancyOwnershipOfRows(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	alice, _ := s.CreateUser(ctx, "alice", "alice", "h")
	g := mustKey(t, s, alice.ID, tenant.Runtime)
	m, _ := s.TenancyStore().MemberByName(ctx, "alice")
	op := orgIDByName(t, s, tenant.OperatorOrg)

	fixture(t, s, "opt")
	if org, key := rowOrg(t, s, "tasks", "task_id", "opt"); org != op || key != "" {
		t.Fatalf("operator task org %d key %q", org, key)
	}
	if _, err := s.CreateResearch(ctx, alice.ID, api.CreateTaskRequest{RequestID: "r1", BodyHash: []byte("h"), TaskID: "rk",
		Spec: json.RawMessage(`{"topic":"x"}`), KeyID: g.ID}); err != nil {
		t.Fatal(err)
	}
	if org, key := rowOrg(t, s, "tasks", "task_id", "rk"); org != m.OrgID || key != g.ID {
		t.Fatalf("research via key: org %d key %q", org, key)
	}
	if _, _, err := s.SessionAPI().CreateSession(ctx, api.CreateSessionRequest{RequestID: "rs1", BodyHash: []byte("h"), SessionID: "sk",
		OwnerUserID: alice.ID, KeyID: g.ID}); err != nil {
		t.Fatal(err)
	}
	if org, key := rowOrg(t, s, "sessions", "session_id", "sk"); org != m.OrgID || key != g.ID {
		t.Fatalf("session via key: org %d key %q", org, key)
	}
	newSession(t, s, "sop", 0)
	if org, _ := rowOrg(t, s, "sessions", "session_id", "sop"); org != op {
		t.Fatalf("operator session org %d", org)
	}
	if _, err := s.pool.Exec(ctx, "UPDATE tasks SET status = 'succeeded' WHERE task_id = 'rk'"); err != nil {
		t.Fatal(err)
	}
	req := turnReq("sk", "turn1", alice.ID)
	req.KeyID = g.ID
	if _, err := s.SessionAPI().CreateTurn(ctx, req); err != nil {
		t.Fatal(err)
	}
	if org, key := rowOrg(t, s, "tasks", "task_id", "turn1"); org != m.OrgID || key != g.ID {
		t.Fatalf("turn via key: org %d key %q", org, key)
	}
	expectNoDBViolations(t, s)

	// G4: a live key whose org differs from its user's org; G5: a task/session of a key in another org.
	if _, err := s.pool.Exec(ctx, "UPDATE api_keys SET org_id = $1 WHERE id = $2", op, g.ID); err != nil {
		t.Fatal(err)
	}
	vs, err := s.DBViolations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for _, v := range vs {
		ids[v.ID]++
	}
	if ids["G4"] != 1 || ids["G5"] != 3 { // rk, turn1, sk
		t.Fatalf("violations %+v", vs)
	}
}

// TestTenancyHTTP drives the real handler on PostgreSQL: the member / owner / operator matrix, a key cannot mint
// keys, and a key revoked over HTTP is refused at once.
func TestTenancyHTTP(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, Options{})
	const opToken = "operator-token-0123456789"
	v := watchedVerifier(t, s)
	srv := httptest.NewUnstartedServer(nil)
	h, err := api.New(api.Config{Store: s, Blobs: noBlobs{}, ListenAddr: srv.Listener.Addr().String(), Token: opToken,
		Accounts: s, ResearchSpec: func(topic string) (json.RawMessage, error) { return json.Marshal(map[string]string{"topic": topic}) },
		Tenancy: s.Tenancy(), APIKeys: v})
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = h
	srv.Start()
	t.Cleanup(srv.Close)
	cookie := map[string]string{}
	for _, name := range []string{"alice", "bob", "carol"} {
		u, err := s.CreateUser(ctx, name, name, "h")
		if err != nil {
			t.Fatal(err)
		}
		id, idHash, _ := account.NewSessionID()
		if err := s.CreateSession(ctx, idHash, u.ID, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		cookie[name] = "Cookie:" + api.SessionCookie + "=" + id
	}
	operator := "Authorization:Bearer " + opToken
	do := func(method, path, body, hdr string, want int) []byte {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if k, v, ok := strings.Cut(hdr, ":"); ok {
			req.Header.Set(k, v)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("%s %s as %q = %d %s, want %d", method, path, hdr, resp.StatusCode, b, want)
		}
		return b
	}
	do("POST", "/v1/orgs", `{"name":"acme","owner":"alice"}`, operator, 201)
	do("POST", "/v1/orgs/members", `{"username":"bob","role":"member"}`, cookie["alice"], 200)
	do("POST", "/v1/orgs/members", `{"username":"carol","role":"member"}`, cookie["bob"], 403) // member
	do("POST", "/v1/orgs/members", `{"username":"carol","role":"member","org":"acme"}`, cookie["carol"], 403)

	var created struct {
		Key string `json:"key"`
		ID  string `json:"id"`
	}
	if err := json.Unmarshal(do("POST", "/v1/keys", `{"name":"bob ci","scopes":["runtime","models"]}`, cookie["bob"], 201), &created); err != nil {
		t.Fatal(err)
	}
	key := "Authorization:Bearer " + created.Key
	xkey := "x-api-key:" + created.Key
	// The key cannot mint keys or manage anything — with either header.
	for _, hdr := range []string{key, xkey} {
		do("POST", "/v1/keys", `{"name":"evil","scopes":["models"]}`, hdr, 403)
		do("GET", "/v1/keys", "", hdr, 403)
		do("DELETE", "/v1/keys/"+created.ID, "", hdr, 403)
		do("POST", "/v1/orgs/members", `{"username":"carol","role":"owner"}`, hdr, 403)
		do("POST", "/v1/orgs", `{"name":"evil"}`, hdr, 403)
	}
	if n := count(t, s, "SELECT count(*) FROM api_keys"); n != 1 {
		t.Fatalf("api_keys = %d", n)
	}
	// It runs research as bob; the task is attributed to the key and acme.
	var res api.CreateTaskResult
	if err := json.Unmarshal(do("POST", "/research", `{"request_id":"r1","topic":"t"}`, xkey, 202), &res); err != nil {
		t.Fatal(err)
	}
	if org, k := rowOrg(t, s, "tasks", "task_id", res.TaskID); org != orgIDByName(t, s, "acme") || k != created.ID {
		t.Fatalf("task org %d key %q", org, k)
	}
	do("GET", "/tasks/"+res.TaskID, "", key, 200)
	do("GET", "/tasks/"+res.TaskID+"/inspect", "", key, 403) // operator-only
	// Keys: carol cannot see or revoke bob's key; alice (owner) and the operator can list it.
	do("DELETE", "/v1/keys/"+created.ID, "", cookie["carol"], 404)
	if b := do("GET", "/v1/keys", "", cookie["alice"], 200); !strings.Contains(string(b), created.ID) {
		t.Fatalf("owner list: %s", b)
	}
	if b := do("GET", "/v1/keys", "", cookie["carol"], 200); strings.Contains(string(b), created.ID) {
		t.Fatalf("other org list: %s", b)
	}
	do("DELETE", "/v1/keys/"+created.ID, "", cookie["alice"], 200)
	deadline := time.Now().Add(5 * time.Second)
	for {
		req, _ := http.NewRequest("GET", srv.URL+"/v1/me", nil)
		req.Header.Set("x-api-key", created.Key)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("revoked key still accepted: %d", resp.StatusCode)
		}
		time.Sleep(20 * time.Millisecond)
	}
	do("GET", "/v1/me", "", operator, 200)
	expectNoDBViolations(t, s)
}

type noBlobs struct{}

func (noBlobs) Open(string) (io.ReadCloser, error) { return nil, errors.New("no blobs") }
