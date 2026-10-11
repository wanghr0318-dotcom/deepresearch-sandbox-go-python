package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
)

// Redis-backed cases use a real Redis (deploy/docker-compose.yml locally, a service container in CI): skipped
// locally without AGENTBOX_TEST_REDIS_ADDR, failing in CI without it.

const redisAddrEnv = "AGENTBOX_TEST_REDIS_ADDR"

func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		if os.Getenv("CI") == "true" {
			t.Fatalf("%s must be set in CI", redisAddrEnv)
		}
		t.Skipf("%s not set", redisAddrEnv)
	}
	return addr
}

func uniq(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "test-" + hex.EncodeToString(b)
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)} }

func take(t *testing.T, l Limiter, bs []Bucket, n int64) Decision {
	t.Helper()
	d, err := l.Take(context.Background(), bs, n)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// semantics runs the shared bucket semantics against a limiter: all-or-nothing over levels, the most constrained
// bucket reported, refund capped at the limit.
func semantics(t *testing.T, l Limiter, prefix string) {
	key := Bucket{Scope: prefix + "key:k1", Kind: "rpm", Limit: 2, Window: time.Hour}
	org := Bucket{Scope: prefix + "org:1", Kind: "rpm", Limit: 10, Window: time.Hour}
	both := []Bucket{key, org}
	d := take(t, l, both, 1)
	if !d.Allowed || d.Limit != 2 || d.Remaining != 1 {
		t.Fatalf("first take: %+v", d)
	}
	if d := take(t, l, both, 1); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("second take: %+v", d)
	}
	d = take(t, l, both, 1)
	if d.Allowed || d.Limit != 2 || d.Remaining != 0 || d.RetryAfter <= 0 || d.RetryAfter > 30*time.Minute+time.Second {
		t.Fatalf("third take must be refused by the key bucket: %+v", d)
	}
	// Nothing was taken from the org bucket by the refused request: 8 left exactly.
	if d := take(t, l, []Bucket{org}, 8); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("org bucket should have had 8 tokens: %+v", d)
	}
	if d := take(t, l, []Bucket{org}, 1); d.Allowed {
		t.Fatalf("org bucket should be empty: %+v", d)
	}
	l.Refund(context.Background(), []Bucket{org}, 3)
	l.Refund(context.Background(), []Bucket{key}, 100) // capped at the limit
	if d := take(t, l, []Bucket{org}, 3); !d.Allowed {
		t.Fatalf("refunded tokens should be takeable: %+v", d)
	}
	if d := take(t, l, []Bucket{key}, 2); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("refund must cap at the limit: %+v", d)
	}
	// Limit 0 and n above the limit never pass; an empty list always passes.
	if d := take(t, l, []Bucket{{Scope: prefix + "x", Kind: "rpm", Limit: 0, Window: time.Minute}}, 1); d.Allowed {
		t.Fatalf("limit 0: %+v", d)
	}
	if d := take(t, l, []Bucket{{Scope: prefix + "y", Kind: "tpm", Limit: 5, Window: time.Minute}}, 6); d.Allowed {
		t.Fatalf("n above limit: %+v", d)
	}
	if d := take(t, l, nil, 1); !d.Allowed {
		t.Fatalf("no buckets: %+v", d)
	}
	if _, err := l.Take(context.Background(), []Bucket{{Scope: "", Kind: "rpm", Limit: 1, Window: time.Minute}}, 1); !errors.Is(err, ErrInvalidBucket) {
		t.Fatalf("invalid bucket: %v", err)
	}
}

func TestLocalSemantics(t *testing.T) { semantics(t, NewLocal(nil), "") }

func TestLocalRefill(t *testing.T) {
	c := newClock()
	l := NewLocal(c.now)
	b := []Bucket{{Scope: "key:k", Kind: "rpm", Limit: 60, Window: time.Minute}}
	if d := take(t, l, b, 60); !d.Allowed || d.Reset != time.Minute {
		t.Fatalf("take all: %+v", d)
	}
	d := take(t, l, b, 1)
	if d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("empty bucket: %+v", d)
	}
	c.add(time.Second)
	if d := take(t, l, b, 1); !d.Allowed {
		t.Fatalf("one token refilled after 1s: %+v", d)
	}
	c.add(time.Hour)
	if d := take(t, l, b, 60); !d.Allowed {
		t.Fatalf("refill caps at the limit: %+v", d)
	}
}

// TestRedisScript runs the Lua script against a real Redis: the same semantics as the in-process limiter, the
// keys' expiry, and exact accounting under concurrency (50 parallel takes from a bucket of 20).
func TestRedisScript(t *testing.T) {
	r := cache.NewRedis(cache.RedisConfig{Addr: redisAddr(t), ReadTimeout: time.Second})
	t.Cleanup(func() { _ = r.Close() })
	f := NewFallback(r, FallbackOptions{Timeout: time.Second})
	p := uniq(t) + ":"
	semantics(t, f, p)
	if f.Degraded() {
		t.Fatal("must not be degraded with a healthy Redis")
	}
	if fb, fails := f.Stats(); fb != 0 || fails != 0 {
		t.Fatalf("no fallback expected: %d %d", fb, fails)
	}
	ttl, err := r.Eval(context.Background(), "return redis.call('PTTL', KEYS[1])", []string{"rl:" + p + "key:k1:rpm"})
	if err != nil {
		t.Fatal(err)
	}
	if ms := string(ttl); ms == "-1" || ms == "-2" || len(ms) < 7 { // 2 × 1 h ≈ 7 200 000 ms
		t.Fatalf("bucket key must expire after twice its window, PTTL %s", ms)
	}

	b := []Bucket{{Scope: p + "org:c", Kind: "rpm", Limit: 20, Window: time.Hour}}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := f.Take(context.Background(), b, 1)
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 20 {
		t.Fatalf("exactly 20 of 50 parallel takes must pass, got %d", allowed.Load())
	}
}

// noscript fakes a Redis without the script cached: EVALSHA answers NOSCRIPT, EVAL runs on the wrapped client.
type noscript struct {
	Evaler
	shas, evals atomic.Int64
}

func (n *noscript) EvalSHA(context.Context, string, []string, ...string) ([]byte, error) {
	n.shas.Add(1)
	return nil, &cache.RedisError{Msg: "NOSCRIPT No matching script. Please use EVAL."}
}

func (n *noscript) Eval(ctx context.Context, s string, keys []string, args ...string) ([]byte, error) {
	n.evals.Add(1)
	return n.Evaler.Eval(ctx, s, keys, args...)
}

func TestRedisNoScriptFallsBackToEval(t *testing.T) {
	r := cache.NewRedis(cache.RedisConfig{Addr: redisAddr(t), ReadTimeout: time.Second})
	t.Cleanup(func() { _ = r.Close() })
	ns := &noscript{Evaler: r}
	f := NewFallback(ns, FallbackOptions{Timeout: time.Second})
	b := []Bucket{{Scope: uniq(t), Kind: "rpm", Limit: 1, Window: time.Minute}}
	if d := take(t, f, b, 1); !d.Allowed || d.Degraded {
		t.Fatalf("%+v", d)
	}
	if d := take(t, f, b, 1); d.Allowed || d.Degraded {
		t.Fatalf("%+v", d)
	}
	if ns.shas.Load() != 2 || ns.evals.Load() != 2 {
		t.Fatalf("EVALSHA then EVAL expected: %d %d", ns.shas.Load(), ns.evals.Load())
	}
}

// hanging accepts TCP connections and never answers: every Redis call times out.
func hanging(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().String()
}

// closedPort returns an address nobody listens on (connection refused).
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestFallbackWhenRedisDown: with Redis refusing connections or hanging, decisions come from the in-process limiter
// within the 50 ms budget, limits stay enforced, the limiter reports degraded and calls OnChange once; after the
// cool-off it tries Redis again, and recovers when Redis answers.
func TestFallbackWhenRedisDown(t *testing.T) {
	for name, addr := range map[string]string{"refused": closedPort(t), "hanging": hanging(t)} {
		t.Run(name, func(t *testing.T) {
			c := newClock()
			var changes []bool
			var mu sync.Mutex
			r := cache.NewRedis(cache.RedisConfig{Addr: addr})
			t.Cleanup(func() { _ = r.Close() })
			f := NewFallback(r, FallbackOptions{Now: c.now, OnChange: func(d bool, _ error) {
				mu.Lock()
				changes = append(changes, d)
				mu.Unlock()
			}})
			b := []Bucket{{Scope: "key:k", Kind: "rpm", Limit: 2, Window: time.Minute}}
			start := time.Now()
			d := take(t, f, b, 1)
			if el := time.Since(start); el > time.Second {
				t.Fatalf("fallback took %v", el)
			}
			if !d.Allowed || !d.Degraded || !f.Degraded() {
				t.Fatalf("degraded decision expected: %+v", d)
			}
			if d := take(t, f, b, 1); !d.Allowed || !d.Degraded {
				t.Fatalf("%+v", d)
			}
			if d := take(t, f, b, 1); d.Allowed {
				t.Fatalf("limits stay enforced in process: %+v", d)
			}
			if fb, fails := f.Stats(); fb != 3 || fails != 1 {
				t.Fatalf("one Redis failure (cool-off) and three fallback decisions expected: %d %d", fb, fails)
			}
			c.add(DefaultCooloff)
			_ = take(t, f, b, 1)
			if _, fails := f.Stats(); fails != 2 {
				t.Fatalf("after the cool-off Redis is tried again: %d failures", fails)
			}
			mu.Lock()
			if len(changes) != 1 || !changes[0] {
				t.Fatalf("OnChange(true) once expected: %v", changes)
			}
			mu.Unlock()
		})
	}
}

func TestFallbackRecovers(t *testing.T) {
	addr := redisAddr(t)
	c := newClock()
	var changes []bool
	good := cache.NewRedis(cache.RedisConfig{Addr: addr, ReadTimeout: time.Second})
	t.Cleanup(func() { _ = good.Close() })
	sw := &switchable{}
	sw.set(cache.NewRedis(cache.RedisConfig{Addr: closedPort(t)}))
	f := NewFallback(sw, FallbackOptions{Now: c.now, Timeout: time.Second, OnChange: func(d bool, _ error) { changes = append(changes, d) }})
	b := []Bucket{{Scope: uniq(t), Kind: "rpm", Limit: 5, Window: time.Minute}}
	if d := take(t, f, b, 1); !d.Degraded {
		t.Fatalf("%+v", d)
	}
	sw.set(good)
	c.add(DefaultCooloff)
	if d := take(t, f, b, 1); d.Degraded || !d.Allowed || f.Degraded() {
		t.Fatalf("recovered decision expected: %+v", d)
	}
	if len(changes) != 2 || !changes[0] || changes[1] {
		t.Fatalf("OnChange(true), OnChange(false) expected: %v", changes)
	}
}

type switchable struct {
	mu sync.Mutex
	ev Evaler
}

func (s *switchable) set(ev Evaler) { s.mu.Lock(); s.ev = ev; s.mu.Unlock() }
func (s *switchable) get() Evaler   { s.mu.Lock(); defer s.mu.Unlock(); return s.ev }
func (s *switchable) EvalSHA(ctx context.Context, sha string, k []string, a ...string) ([]byte, error) {
	return s.get().EvalSHA(ctx, sha, k, a...)
}
func (s *switchable) Eval(ctx context.Context, sc string, k []string, a ...string) ([]byte, error) {
	return s.get().Eval(ctx, sc, k, a...)
}

func TestNoRedisIsLocalAndNotDegraded(t *testing.T) {
	f := NewFallback(nil, FallbackOptions{})
	semantics(t, f, "")
	if f.Degraded() {
		t.Fatal("no Redis configured is not degraded")
	}
	if fb, _ := f.Stats(); fb != 0 {
		t.Fatalf("no Redis configured counts no fallbacks: %d", fb)
	}
}

func TestConcurrency(t *testing.T) {
	c := NewConcurrency()
	key := Slot{Scope: "key:k:streams", Limit: 1}
	org := Slot{Scope: "org:1:streams", Limit: 2}
	rel1, _, ok := c.Acquire([]Slot{key, org})
	if !ok {
		t.Fatal("first acquire")
	}
	if _, full, ok := c.Acquire([]Slot{key, org}); ok || full != key {
		t.Fatalf("key slot full: %v %v", ok, full)
	}
	if c.InUse(org.Scope) != 1 {
		t.Fatal("refused acquire must not hold the org slot")
	}
	rel2, _, ok := c.Acquire([]Slot{{Scope: "key:k2:streams", Limit: 5}, org})
	if !ok {
		t.Fatal("second key, same org")
	}
	if _, full, ok := c.Acquire([]Slot{{Scope: "key:k3:streams", Limit: 5}, org}); ok || full != org {
		t.Fatal("org full")
	}
	rel1()
	rel1() // idempotent
	if c.InUse(org.Scope) != 1 || c.InUse(key.Scope) != 0 {
		t.Fatalf("release: org %d key %d", c.InUse(org.Scope), c.InUse(key.Scope))
	}
	rel2()
	if c.InUse(org.Scope) != 0 {
		t.Fatal("all released")
	}
}

// TestConcurrencyDuplicateScope (review F14): a scope listed twice takes one unit and is checked against the smaller
// limit.
func TestConcurrencyDuplicateScope(t *testing.T) {
	c := NewConcurrency()
	s := Slot{Scope: "org:1:streams", Limit: 2}
	rel, _, ok := c.Acquire([]Slot{s, s})
	if !ok || c.InUse(s.Scope) != 1 {
		t.Fatalf("one unit expected: ok=%v in use %d", ok, c.InUse(s.Scope))
	}
	if _, _, ok := c.Acquire([]Slot{s, {Scope: s.Scope, Limit: 1}}); ok {
		t.Fatal("the smaller limit (1) applies")
	}
	rel()
	if c.InUse(s.Scope) != 0 {
		t.Fatal("released")
	}
}

// busyEvaler answers like a Redis client whose connection pool is full.
type busyEvaler struct{}

func (busyEvaler) EvalSHA(context.Context, string, []string, ...string) ([]byte, error) {
	return nil, fmt.Errorf("cache: redis EVALSHA: 等待连接: %w", cache.ErrPoolBusy)
}
func (busyEvaler) Eval(context.Context, string, []string, ...string) ([]byte, error) {
	return nil, fmt.Errorf("cache: redis EVAL: 等待连接: %w", cache.ErrPoolBusy)
}

// TestPoolBusyIsNotARedisFailure (review F9): waiting for a free connection decides in process for that request but
// neither counts a Redis failure nor marks the limiter degraded.
func TestPoolBusyIsNotARedisFailure(t *testing.T) {
	f := NewFallback(busyEvaler{}, FallbackOptions{})
	b := []Bucket{{Scope: "key:k", Kind: "rpm", Limit: 1, Window: time.Minute}}
	if d := take(t, f, b, 1); !d.Allowed {
		t.Fatalf("%+v", d)
	}
	if d := take(t, f, b, 1); d.Allowed {
		t.Fatalf("still enforced in process: %+v", d)
	}
	if fb, fails := f.Stats(); f.Degraded() || fails != 0 || fb != 2 {
		t.Fatalf("degraded=%v failures=%d fallbacks=%d", f.Degraded(), fails, fb)
	}
	if !errors.Is(cache.ErrPoolBusy, cache.ErrPoolBusy) {
		t.Fatal("sentinel")
	}
}
