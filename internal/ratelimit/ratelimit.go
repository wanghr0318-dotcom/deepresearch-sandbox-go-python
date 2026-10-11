// Package ratelimit implements the gateway's rate limits (model gateway design 2026-10-11 §6.1): token buckets per
// level (key, user, org) taken all-or-nothing, in Redis through one Lua script with an in-process fallback, and
// in-process concurrency counters for model streams.
//
// Rate limits protect capacity, not money: money is protected by quotas (internal/quota). When Redis is unavailable
// the limiter degrades to per-server buckets instead of failing requests.
//
// The package depends on nothing in this module; the Redis client is passed in as the Evaler interface
// (*cache.Redis satisfies it).
package ratelimit

import (
	"context"
	"errors"
	"math"
	"time"
)

// Bucket is one token bucket: Limit tokens per Window, refilled continuously. Scope names the level and its ID (for
// example "key:k_abc…", "user:42", "org:7"); Kind names the measure ("rpm", "tpm").
type Bucket struct {
	Scope, Kind string
	Limit       int64
	Window      time.Duration
}

// Decision is the result of Take. Limit, Remaining and Reset describe the most constrained bucket (the one with the
// fewest tokens left after the take, or the one that refused it); RetryAfter is set when not allowed. Degraded
// reports that the decision was made by the in-process fallback.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Limit      int64
	Remaining  int64
	Reset      time.Duration
	Degraded   bool
}

// Limiter takes and refunds tokens.
type Limiter interface {
	// Take takes n tokens from every bucket, or from none if any bucket has fewer than n (all-or-nothing). An empty
	// bucket list is always allowed. The error is non-nil only for invalid buckets or a cancelled ctx.
	Take(ctx context.Context, buckets []Bucket, n int64) (Decision, error)
	// Refund returns n tokens to every bucket (capped at its limit), e.g. the unused part of a token estimate.
	Refund(ctx context.Context, buckets []Bucket, n int64)
}

// ErrInvalidBucket reports a bucket without a scope or kind, or with a non-positive window or negative limit.
var ErrInvalidBucket = errors.New("ratelimit: invalid bucket")

// Key is the Redis key of a bucket: rl:{scope}:{kind}.
func (b Bucket) Key() string { return "rl:" + b.Scope + ":" + b.Kind }

func validate(buckets []Bucket, n int64) error {
	if n < 0 {
		return ErrInvalidBucket
	}
	for _, b := range buckets {
		if b.Scope == "" || b.Kind == "" || b.Window <= 0 || b.Limit < 0 {
			return ErrInvalidBucket
		}
	}
	return nil
}

// zeroLimit returns the decision for buckets that can never grant n tokens (limit 0, or n above the limit), or false.
func zeroLimit(buckets []Bucket, n int64) (Decision, bool) {
	for _, b := range buckets {
		if b.Limit == 0 || n > b.Limit {
			return Decision{Allowed: false, RetryAfter: b.Window, Limit: b.Limit, Remaining: 0, Reset: b.Window}, true
		}
	}
	return Decision{}, false
}

// state is one bucket's stored state: tokens at time ms (milliseconds).
type state struct {
	tokens float64
	ms     int64
}

// refill returns the tokens of s at now for bucket b (a missing state is a full bucket).
func refill(s *state, b Bucket, now int64) float64 {
	if s == nil {
		return float64(b.Limit)
	}
	rate := float64(b.Limit) / float64(b.Window.Milliseconds()) // tokens per ms
	elapsed := now - s.ms
	if elapsed < 0 {
		elapsed = 0
	}
	return math.Min(float64(b.Limit), s.tokens+float64(elapsed)*rate)
}

// decide computes the all-or-nothing decision over tokens (the refilled level of each bucket).
func decide(buckets []Bucket, tokens []float64, n int64) Decision {
	d := Decision{Allowed: true}
	best := math.Inf(1)
	for i, b := range buckets {
		rate := float64(b.Limit) / float64(b.Window.Milliseconds())
		if tokens[i] < float64(n) {
			wait := time.Duration(math.Ceil((float64(n)-tokens[i])/rate)) * time.Millisecond
			if !d.Allowed && wait <= d.RetryAfter {
				continue
			}
			d.Allowed, d.RetryAfter = false, wait
			d.Limit, d.Remaining = b.Limit, int64(math.Floor(tokens[i]))
			d.Reset = time.Duration(math.Ceil((float64(b.Limit)-tokens[i])/rate)) * time.Millisecond
			continue
		}
		if !d.Allowed {
			continue
		}
		left := tokens[i] - float64(n)
		if left < best {
			best = left
			d.Limit, d.Remaining = b.Limit, int64(math.Floor(left))
			d.Reset = time.Duration(math.Ceil((float64(b.Limit)-left)/rate)) * time.Millisecond
		}
	}
	return d
}
