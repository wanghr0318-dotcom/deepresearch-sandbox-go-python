package ratelimit

import (
	"context"
	"sync"
	"time"
)

// localMaxEntries bounds the in-process bucket map; when it is reached, buckets idle for more than twice their window
// (which are full again, so dropping them changes nothing) are removed.
const localMaxEntries = 100_000

// Local is an in-process Limiter: the same token buckets as the Redis script, per server. It is the fallback when
// Redis is unavailable and the limiter used when no Redis is configured.
type Local struct {
	now func() time.Time

	mu sync.Mutex
	m  map[string]*localEntry
}

type localEntry struct {
	state
	window time.Duration
}

// NewLocal returns an in-process limiter; now is the clock (nil for time.Now).
func NewLocal(now func() time.Time) *Local {
	if now == nil {
		now = time.Now
	}
	return &Local{now: now, m: map[string]*localEntry{}}
}

// Take implements Limiter.
func (l *Local) Take(_ context.Context, buckets []Bucket, n int64) (Decision, error) {
	if err := validate(buckets, n); err != nil {
		return Decision{}, err
	}
	if len(buckets) == 0 {
		return Decision{Allowed: true}, nil
	}
	if d, ok := zeroLimit(buckets, n); ok {
		return d, nil
	}
	now := l.now().UnixMilli()
	l.mu.Lock()
	defer l.mu.Unlock()
	tokens := make([]float64, len(buckets))
	for i, b := range buckets {
		tokens[i] = refill(l.get(b), b, now)
	}
	d := decide(buckets, tokens, n)
	if d.Allowed {
		for i, b := range buckets {
			l.set(b, tokens[i]-float64(n), now)
		}
	}
	return d, nil
}

// Refund implements Limiter.
func (l *Local) Refund(_ context.Context, buckets []Bucket, n int64) {
	if validate(buckets, n) != nil || n == 0 {
		return
	}
	now := l.now().UnixMilli()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, b := range buckets {
		t := refill(l.get(b), b, now) + float64(n)
		if t > float64(b.Limit) {
			t = float64(b.Limit)
		}
		l.set(b, t, now)
	}
}

func (l *Local) get(b Bucket) *state {
	if e, ok := l.m[b.Key()]; ok {
		return &e.state
	}
	return nil
}

func (l *Local) set(b Bucket, tokens float64, now int64) {
	k := b.Key()
	if e, ok := l.m[k]; ok {
		e.tokens, e.ms, e.window = tokens, now, b.Window
		return
	}
	if len(l.m) >= localMaxEntries {
		for key, e := range l.m {
			if now-e.ms > 2*e.window.Milliseconds() {
				delete(l.m, key)
			}
		}
	}
	l.m[k] = &localEntry{state: state{tokens: tokens, ms: now}, window: b.Window}
}

// Len returns the number of tracked buckets (tests).
func (l *Local) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.m)
}
