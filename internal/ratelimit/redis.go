package ratelimit

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Evaler runs Lua scripts on Redis (*cache.Redis satisfies it). Both return the script's bulk-string reply.
type Evaler interface {
	EvalSHA(ctx context.Context, sha string, keys []string, args ...string) ([]byte, error)
	Eval(ctx context.Context, script string, keys []string, args ...string) ([]byte, error)
}

// script checks and takes from (or refunds to) every bucket atomically. KEYS are the buckets' keys; ARGV is
// n, mode ("t" take, "r" refund), then limit and window (ms) per bucket. The time is Redis's own clock (TIME), so
// servers with skewed clocks share one view. A take writes nothing unless every bucket has n tokens. Keys expire
// after twice their window (an idle bucket is full again by then). The reply is "<allowed 0|1> <tokens before>…".
const script = `
if redis.replicate_commands then redis.replicate_commands() end
local n = tonumber(ARGV[1])
local refund = ARGV[2] == 'r'
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local toks = {}
local ok = true
for i = 1, #KEYS do
  local limit = tonumber(ARGV[1 + 2 * i])
  local window = tonumber(ARGV[2 + 2 * i])
  local v = redis.call('HMGET', KEYS[i], 't', 'ms')
  local tok = limit
  if v[1] and v[2] then
    local elapsed = now - tonumber(v[2])
    if elapsed < 0 then elapsed = 0 end
    tok = math.min(limit, tonumber(v[1]) + elapsed * limit / window)
  end
  toks[i] = tok
  if tok < n then ok = false end
end
if refund or ok then
  for i = 1, #KEYS do
    local limit = tonumber(ARGV[1 + 2 * i])
    local window = tonumber(ARGV[2 + 2 * i])
    local nt
    if refund then nt = math.min(limit, toks[i] + n) else nt = toks[i] - n end
    redis.call('HSET', KEYS[i], 't', string.format('%.17g', nt), 'ms', string.format('%d', now))
    redis.call('PEXPIRE', KEYS[i], 2 * window)
  end
end
local out = {ok and '1' or '0'}
for i = 1, #KEYS do out[#out + 1] = string.format('%.17g', toks[i]) end
return table.concat(out, ' ')
`

var scriptSHA = func() string {
	s := sha1.Sum([]byte(script))
	return hex.EncodeToString(s[:])
}()

// redisScript runs the script, loading it with EVAL when Redis does not have it cached (NOSCRIPT).
func redisScript(ctx context.Context, ev Evaler, buckets []Bucket, n int64, mode string) (bool, []float64, error) {
	keys := make([]string, len(buckets))
	args := make([]string, 0, 2+2*len(buckets))
	args = append(args, strconv.FormatInt(n, 10), mode)
	for i, b := range buckets {
		keys[i] = b.Key()
		args = append(args, strconv.FormatInt(b.Limit, 10), strconv.FormatInt(b.Window.Milliseconds(), 10))
	}
	out, err := ev.EvalSHA(ctx, scriptSHA, keys, args...)
	if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
		out, err = ev.Eval(ctx, script, keys, args...)
	}
	if err != nil {
		return false, nil, err
	}
	f := strings.Fields(string(out))
	if len(f) != 1+len(buckets) || (f[0] != "0" && f[0] != "1") {
		return false, nil, fmt.Errorf("ratelimit: unexpected script reply %q", out)
	}
	tokens := make([]float64, len(buckets))
	for i := range buckets {
		if tokens[i], err = strconv.ParseFloat(f[1+i], 64); err != nil {
			return false, nil, fmt.Errorf("ratelimit: unexpected script reply %q", out)
		}
	}
	return f[0] == "1", tokens, nil
}

// DefaultRedisTimeout is how long a Redis decision may take before the fallback decides instead (§6.1: 50 ms).
const DefaultRedisTimeout = 50 * time.Millisecond

// DefaultCooloff is how long the limiter stays on the in-process fallback after a Redis failure before it tries
// Redis again (so a dead Redis costs one timeout per cool-off, not one per request).
const DefaultCooloff = 5 * time.Second

// Fallback is the production Limiter: Redis while it answers within the timeout, otherwise the in-process Local
// limiter (limits then hold per server instead of globally). It reports itself degraded while on the fallback.
type Fallback struct {
	ev      Evaler
	local   *Local
	timeout time.Duration
	cooloff time.Duration
	now     func() time.Time

	mu        sync.Mutex
	downUntil time.Time
	degraded  atomic.Bool
	fallbacks atomic.Int64 // decisions made by the fallback
	failures  atomic.Int64 // Redis errors and timeouts
	onChange  func(degraded bool, err error)
}

// FallbackOptions configures NewFallback; zero fields take defaults.
type FallbackOptions struct {
	Timeout time.Duration    // DefaultRedisTimeout
	Cooloff time.Duration    // DefaultCooloff
	Now     func() time.Time // time.Now
	// OnChange is called (outside locks) when the limiter enters or leaves degraded mode; err is the Redis error that
	// caused degradation (nil when recovering).
	OnChange func(degraded bool, err error)
}

// NewFallback returns a limiter over Redis (ev) with an in-process fallback. ev nil means no Redis: every decision
// is local and the limiter is not reported degraded.
func NewFallback(ev Evaler, o FallbackOptions) *Fallback {
	if o.Timeout <= 0 {
		o.Timeout = DefaultRedisTimeout
	}
	if o.Cooloff <= 0 {
		o.Cooloff = DefaultCooloff
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Fallback{ev: ev, local: NewLocal(o.Now), timeout: o.Timeout, cooloff: o.Cooloff, now: o.Now, onChange: o.OnChange}
}

// Degraded reports whether the last decision was made by the fallback because Redis failed.
func (f *Fallback) Degraded() bool { return f.degraded.Load() }

// Stats returns the number of fallback decisions and of Redis failures since start.
func (f *Fallback) Stats() (fallbacks, failures int64) { return f.fallbacks.Load(), f.failures.Load() }

// useRedis reports whether to try Redis now (configured and not cooling off after a failure).
func (f *Fallback) useRedis() bool {
	if f.ev == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.now().Before(f.downUntil)
}

func (f *Fallback) failed(err error) {
	f.failures.Add(1)
	f.mu.Lock()
	f.downUntil = f.now().Add(f.cooloff)
	f.mu.Unlock()
	if !f.degraded.Swap(true) && f.onChange != nil {
		f.onChange(true, err)
	}
}

func (f *Fallback) recovered() {
	if f.degraded.Swap(false) && f.onChange != nil {
		f.onChange(false, nil)
	}
}

// Take implements Limiter.
func (f *Fallback) Take(ctx context.Context, buckets []Bucket, n int64) (Decision, error) {
	if err := validate(buckets, n); err != nil {
		return Decision{}, err
	}
	if len(buckets) == 0 {
		return Decision{Allowed: true}, nil
	}
	if d, ok := zeroLimit(buckets, n); ok {
		return d, nil
	}
	if f.useRedis() {
		rctx, cancel := context.WithTimeout(ctx, f.timeout)
		ok, tokens, err := redisScript(rctx, f.ev, buckets, n, "t")
		cancel()
		if err == nil {
			f.recovered()
			d := decide(buckets, tokens, n)
			d.Allowed = ok // the script's verdict is authoritative (same comparison on the same numbers)
			return d, nil
		}
		if ctx.Err() != nil {
			return Decision{}, ctx.Err() // the caller gave up: not a Redis failure
		}
		f.failed(err)
	}
	if f.ev != nil {
		f.fallbacks.Add(1)
	}
	d, err := f.local.Take(ctx, buckets, n)
	d.Degraded = f.ev != nil
	return d, err
}

// Refund implements Limiter. A refund that cannot reach Redis is dropped (it only ever makes limits stricter).
func (f *Fallback) Refund(ctx context.Context, buckets []Bucket, n int64) {
	if validate(buckets, n) != nil || len(buckets) == 0 || n == 0 {
		return
	}
	if f.useRedis() {
		rctx, cancel := context.WithTimeout(ctx, f.timeout)
		_, _, err := redisScript(rctx, f.ev, buckets, n, "r")
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() == nil {
			f.failed(err)
		}
	}
	f.local.Refund(ctx, buckets, n)
}
