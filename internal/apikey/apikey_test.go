package apikey

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

var pepper = []byte("0123456789abcdef0123456789abcdef")

func TestGenerateAndParse(t *testing.T) {
	g, err := Generate(pepper)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(g.Full, Prefix+g.ID+"_") || len(g.Full) != len(Prefix)+18+1+52 || !ValidID(g.ID) {
		t.Fatalf("bad key %q / %q", g.Full, g.ID)
	}
	id, secret, err := Parse(g.Full)
	if err != nil || id != g.ID || !CheckSecret(pepper, secret, g.SecretHMAC) {
		t.Fatalf("Parse = %q, %v", id, err)
	}
	if CheckSecret([]byte("other pepper"), secret, g.SecretHMAC) {
		t.Fatal("a different pepper must not verify")
	}
	g2, _ := Generate(pepper)
	if g2.ID == g.ID || g2.Full == g.Full {
		t.Fatal("keys must be random")
	}
	if strings.Contains(string(g.SecretHMAC), g.Full[len(Prefix)+19:]) {
		t.Fatal("the HMAC must not contain the secret")
	}
}

func TestParseRejects(t *testing.T) {
	g, _ := Generate(pepper)
	sec := g.Full[len(Prefix)+19:]
	for name, s := range map[string]string{
		"empty":         "",
		"no prefix":     strings.TrimPrefix(g.Full, "abx_"),
		"test prefix":   "abx_test_" + strings.TrimPrefix(g.Full, Prefix),
		"short":         g.Full[:len(g.Full)-1],
		"long":          g.Full + "a",
		"upper":         strings.ToUpper(g.Full),
		"bad separator": Prefix + g.ID + "-" + sec,
		"bad id":        Prefix + "x_" + g.ID[2:] + "_" + sec,
		"bad alphabet":  Prefix + g.ID + "_" + "0" + sec[1:],
		"non-canonical": Prefix + g.ID + "_" + sec[:51] + "b", // trailing pad bits set
	} {
		if _, _, err := Parse(s); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("%s: Parse(%q) err = %v", name, s, err)
		}
	}
	if !LooksLikeKey("abx_anything") || LooksLikeKey("operator-token") {
		t.Error("LooksLikeKey")
	}
}

func TestValidName(t *testing.T) {
	if !ValidName("ci key") || !ValidName(strings.Repeat("密", 64)) || ValidName("") || ValidName(strings.Repeat("a", 65)) || ValidName("a\nb") {
		t.Error("ValidName")
	}
}

func TestPepperFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadPepper(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing pepper: %v", err)
	}
	p1, err := LoadOrCreatePepper(dir)
	if err != nil || len(p1) != 32 {
		t.Fatalf("create: %v", err)
	}
	p2, err := LoadOrCreatePepper(dir)
	if err != nil || string(p1) != string(p2) {
		t.Fatalf("second start must reuse the pepper: %v", err)
	}
	fi, err := os.Stat(filepath.Join(dir, PepperFile))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if fi.Mode().Perm() != 0o600 {
			t.Fatalf("mode %v", fi.Mode().Perm())
		}
		if err := os.Chmod(filepath.Join(dir, PepperFile), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPepper(dir); err == nil {
			t.Fatal("a group/world readable pepper must be refused")
		}
		_ = os.Chmod(filepath.Join(dir, PepperFile), 0o600)
	}
	if err := os.WriteFile(filepath.Join(dir, PepperFile), []byte("short\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreatePepper(dir); err == nil {
		t.Fatal("a malformed pepper must be refused, not replaced")
	}
}

// fakeStore counts lookups and touches.
type fakeStore struct {
	mu      sync.Mutex
	recs    map[string]Record
	lookups int
	touches []time.Time
	fail    error
}

func (f *fakeStore) LookupKey(_ context.Context, id string) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if f.fail != nil {
		return Record{}, f.fail
	}
	r, ok := f.recs[id]
	if !ok {
		return Record{}, persistence.ErrNotFound
	}
	return r, nil
}

func (f *fakeStore) TouchKey(_ context.Context, _ string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touches = append(f.touches, at)
	return nil
}

func (f *fakeStore) set(r Record) {
	f.mu.Lock()
	f.recs[r.ID] = r
	f.mu.Unlock()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newKey(t *testing.T, st *fakeStore, scopes tenant.Scopes) (Generated, Record) {
	t.Helper()
	g, err := Generate(pepper)
	if err != nil {
		t.Fatal(err)
	}
	r := Record{ID: g.ID, OrgID: 5, UserID: 9, Username: "alice", Role: tenant.Member, Scopes: scopes, SecretHMAC: g.SecretHMAC}
	st.set(r)
	return g, r
}

func TestVerify(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{recs: map[string]Record{}}
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	v := NewVerifier(st, pepper, c.now)
	g, rec := newKey(t, st, tenant.Runtime)

	id, err := v.Verify(ctx, g.Full)
	if err != nil {
		t.Fatal(err)
	}
	want := tenant.Principal{OrgID: 5, UserID: 9, KeyID: g.ID, Scopes: tenant.Runtime, Role: tenant.Member}
	if id.Principal != want || id.Username != "alice" {
		t.Fatalf("identity %+v", id)
	}
	// Wrong secret with a valid ID: same error as an unknown key.
	other, _ := Generate(pepper)
	forged := Prefix + g.ID + "_" + other.Full[len(Prefix)+19:]
	if _, err := v.Verify(ctx, forged); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("forged secret: %v", err)
	}
	if _, err := v.Verify(ctx, other.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := v.Verify(ctx, "abx_live_garbage"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("malformed: %v", err)
	}

	// Cached for 30 s: a revocation without NOTIFY is seen only after the TTL.
	rec.Revoked = true
	st.set(rec)
	if _, err := v.Verify(ctx, g.Full); err != nil {
		t.Fatalf("within the TTL the cached record is used: %v", err)
	}
	c.add(CacheTTL)
	if _, err := v.Verify(ctx, g.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("after the TTL a revoked key is invalid: %v", err)
	}

	// Evict (NOTIFY) takes effect at once.
	g2, rec2 := newKey(t, st, tenant.AllScopes)
	if _, err := v.Verify(ctx, g2.Full); err != nil {
		t.Fatal(err)
	}
	rec2.Revoked = true
	st.set(rec2)
	v.Evict(g2.ID)
	if _, err := v.Verify(ctx, g2.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("evicted revoked key: %v", err)
	}

	// Expiry is checked on every use, also from the cache.
	g3, rec3 := newKey(t, st, tenant.Models)
	exp := c.now().Add(10 * time.Second)
	rec3.ExpiresAt = &exp
	st.set(rec3)
	if _, err := v.Verify(ctx, g3.Full); err != nil {
		t.Fatal(err)
	}
	c.add(10 * time.Second)
	if _, err := v.Verify(ctx, g3.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("expired: %v", err)
	}

	// Disabled user or org.
	g4, rec4 := newKey(t, st, tenant.Models)
	rec4.Disabled = true
	st.set(rec4)
	if _, err := v.Verify(ctx, g4.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("disabled: %v", err)
	}

	// Storage failures are not "invalid key".
	g5, _ := newKey(t, st, tenant.Models)
	st.mu.Lock()
	st.fail = persistence.ErrUnavailable
	st.mu.Unlock()
	if _, err := v.Verify(ctx, g5.Full); !errors.Is(err, persistence.ErrUnavailable) {
		t.Fatalf("store down: %v", err)
	}
}

func TestVerifyCachesAndTouchesOncePerMinute(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{recs: map[string]Record{}}
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	v := NewVerifier(st, pepper, c.now)
	g, _ := newKey(t, st, tenant.Runtime)
	for range 10 {
		if _, err := v.Verify(ctx, g.Full); err != nil {
			t.Fatal(err)
		}
		c.add(5 * time.Second) // 50 s in total
	}
	if st.lookups != 2 || len(st.touches) != 1 {
		t.Fatalf("lookups=%d touches=%d; want 2 (30 s TTL) and 1", st.lookups, len(st.touches))
	}
	c.add(15 * time.Second)
	if _, err := v.Verify(ctx, g.Full); err != nil {
		t.Fatal(err)
	}
	if len(st.touches) != 2 {
		t.Fatalf("a minute later last_used_at is written again: %d", len(st.touches))
	}
}

// TestEvictDuringLookup: a lookup that started before an eviction does not repopulate the cache.
func TestEvictDuringLookup(t *testing.T) {
	st := &blockingStore{fakeStore: fakeStore{recs: map[string]Record{}}, release: make(chan struct{}), entered: make(chan struct{})}
	v := NewVerifier(st, pepper, nil)
	g, rec := newKey(t, &st.fakeStore, tenant.Runtime)
	done := make(chan error)
	go func() { _, err := v.Verify(context.Background(), g.Full); done <- err }()
	<-st.entered
	rec.Revoked = true
	st.set(rec)
	v.Evict(g.ID)
	close(st.release)
	if err := <-done; err != nil {
		t.Fatalf("the in-flight lookup returns its (old) record: %v", err)
	}
	if _, err := v.Verify(context.Background(), g.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("the stale record must not have been cached: %v", err)
	}
}

type blockingStore struct {
	fakeStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStore) LookupKey(ctx context.Context, id string) (Record, error) {
	r, err := b.fakeStore.LookupKey(ctx, id)
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.entered)
		<-b.release
	}
	return r, err
}

type fakeListener struct {
	calls chan struct{}
	on    func(string)
	mu    sync.Mutex
	stop  chan error
}

func (l *fakeListener) ListenKeyChanges(ctx context.Context, ready func(), on func(string)) error {
	l.mu.Lock()
	l.on = on
	l.mu.Unlock()
	ready()
	l.calls <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-l.stop:
		return err
	}
}

func TestWatch(t *testing.T) {
	st := &fakeStore{recs: map[string]Record{}}
	v := NewVerifier(st, pepper, nil)
	g, rec := newKey(t, st, tenant.Runtime)
	l := &fakeListener{calls: make(chan struct{}, 4), stop: make(chan error)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { v.Watch(ctx, l, nil); close(done) }()
	<-l.calls
	if _, err := v.Verify(ctx, g.Full); err != nil {
		t.Fatal(err)
	}
	rec.Revoked = true
	st.set(rec)
	l.mu.Lock()
	on := l.on
	l.mu.Unlock()
	on(g.ID) // NOTIFY apikey_changed, '<id>'
	if _, err := v.Verify(ctx, g.Full); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("after NOTIFY: %v", err)
	}
	cancel()
	<-done
}
