package apikey

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

const (
	// CacheTTL bounds how long a verified key is served from memory without a database read.
	CacheTTL = 30 * time.Second
	// TouchInterval is the minimum gap between last_used_at writes for one key.
	TouchInterval = time.Minute
	// Channel is the PostgreSQL NOTIFY channel; the payload is a key ID, or "*" for every key.
	Channel = "apikey_changed"
	// EvictAllPayload evicts every cached key (for example after a user is disabled).
	EvictAllPayload = "*"
	touchTimeout    = 2 * time.Second
)

// Record is what verification needs about a stored key.
type Record struct {
	ID         string
	OrgID      int64
	UserID     int64
	Username   string
	Role       tenant.Role
	Scopes     tenant.Scopes
	SecretHMAC []byte
	ExpiresAt  *time.Time
	Revoked    bool
	// Disabled is true when the key's user or org is disabled.
	Disabled bool
}

// Store reads keys for verification. LookupKey returns persistence.ErrNotFound for an unknown ID.
type Store interface {
	LookupKey(ctx context.Context, id string) (Record, error)
	// TouchKey sets last_used_at = at unless it is already within TouchInterval of at.
	TouchKey(ctx context.Context, id string, at time.Time) error
}

// Listener delivers apikey_changed notifications. ListenKeyChanges blocks until ctx ends or the connection fails;
// it calls ready once LISTEN is in effect and onChange for every notification payload.
type Listener interface {
	ListenKeyChanges(ctx context.Context, ready func(), onChange func(payload string)) error
}

// Source is a Store that can also deliver change notifications (the PostgreSQL implementation).
type Source interface {
	Store
	Listener
}

// Identity is a verified key: its principal plus the username (for the user-facing runtime endpoints).
type Identity struct {
	Principal tenant.Principal
	Username  string
}

// Verifier verifies keys against Store through a short-lived cache.
type Verifier struct {
	store  Store
	pepper []byte
	now    func() time.Time

	mu      sync.Mutex
	gen     uint64 // bumped by every eviction; a lookup started before an eviction is not cached
	cache   map[string]cached
	touched map[string]time.Time
}

type cached struct {
	rec Record
	at  time.Time
}

// NewVerifier returns a Verifier; now is time.Now when nil.
func NewVerifier(store Store, pepper []byte, now func() time.Time) *Verifier {
	if now == nil {
		now = time.Now
	}
	return &Verifier{store: store, pepper: append([]byte(nil), pepper...), now: now,
		cache: map[string]cached{}, touched: map[string]time.Time{}}
}

// Generate mints a key under the verifier's pepper.
func (v *Verifier) Generate() (Generated, error) { return Generate(v.pepper) }

// Verify checks a full key. Any invalid key is ErrInvalidKey; other errors are storage failures.
func (v *Verifier) Verify(ctx context.Context, raw string) (Identity, error) {
	id, secret, err := Parse(raw)
	if err != nil {
		return Identity{}, ErrInvalidKey
	}
	rec, err := v.record(ctx, id)
	if errors.Is(err, persistence.ErrNotFound) {
		return Identity{}, ErrInvalidKey
	}
	if err != nil {
		return Identity{}, err
	}
	now := v.now()
	if !CheckSecret(v.pepper, secret, rec.SecretHMAC) || rec.Revoked || rec.Disabled ||
		(rec.ExpiresAt != nil && !now.Before(*rec.ExpiresAt)) {
		return Identity{}, ErrInvalidKey
	}
	v.touch(ctx, id, now)
	return Identity{Username: rec.Username, Principal: tenant.Principal{OrgID: rec.OrgID, UserID: rec.UserID, KeyID: rec.ID,
		Scopes: rec.Scopes, Role: rec.Role}}, nil
}

func (v *Verifier) record(ctx context.Context, id string) (Record, error) {
	now := v.now()
	v.mu.Lock()
	c, ok := v.cache[id]
	gen := v.gen
	v.mu.Unlock()
	if ok && now.Sub(c.at) < CacheTTL && !now.Before(c.at) {
		return c.rec, nil
	}
	rec, err := v.store.LookupKey(ctx, id)
	if err != nil {
		return Record{}, err
	}
	v.mu.Lock()
	if v.gen == gen {
		v.cache[id] = cached{rec: rec, at: now}
	}
	v.mu.Unlock()
	return rec, nil
}

// touch updates last_used_at at most once per TouchInterval per key from this server (the store also guards
// across servers). Failures are ignored: last_used_at is informational.
func (v *Verifier) touch(ctx context.Context, id string, now time.Time) {
	v.mu.Lock()
	last, ok := v.touched[id]
	due := !ok || now.Sub(last) >= TouchInterval || now.Before(last)
	if due {
		v.touched[id] = now
	}
	v.mu.Unlock()
	if !due {
		return
	}
	tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), touchTimeout)
	defer cancel()
	_ = v.store.TouchKey(tctx, id, now)
}

// Evict drops one key (payload EvictAllPayload drops all).
func (v *Verifier) Evict(payload string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.gen++
	if payload == EvictAllPayload {
		clear(v.cache)
		return
	}
	delete(v.cache, payload)
}

// EvictAll drops every cached key.
func (v *Verifier) EvictAll() { v.Evict(EvictAllPayload) }

// Watch keeps a LISTEN apikey_changed subscription until ctx ends, evicting keys as notifications arrive. The
// whole cache is dropped whenever the subscription is (re)established or lost, so notifications missed while
// disconnected cannot leave a revoked key cached; at worst a key stays valid for CacheTTL after revocation.
func (v *Verifier) Watch(ctx context.Context, l Listener, log *slog.Logger) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := l.ListenKeyChanges(ctx, func() { v.EvictAll(); backoff = time.Second }, v.Evict)
		v.EvictAll()
		if ctx.Err() != nil {
			return
		}
		if log != nil {
			log.Warn("apikey: LISTEN apikey_changed lost; retrying", "error", errString(err), "backoff", backoff.String())
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func errString(err error) string {
	if err == nil {
		return "listener returned"
	}
	return err.Error()
}
