package modelapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// taken is what a request took from the rate limits (§7.7 step 3); finish gives back what was not used.
type taken struct {
	h       *Handler
	tpm     []ratelimit.Bucket
	tokens  int64
	release func() // stream slot
}

// refundTokens returns n of the estimated tokens to the tokens-per-minute buckets.
func (t *taken) refundTokens(ctx context.Context, n int64) {
	if n > 0 && len(t.tpm) > 0 {
		t.h.cfg.Limiter.Refund(ctx, t.tpm, min(n, t.tokens))
	}
}

func (t *taken) releaseSlot() {
	if t.release != nil {
		t.release()
	}
}

// buckets returns the request (rpm) and token (tpm) buckets of the levels of s that have such a limit.
func buckets(s quota.Scope, ls map[quota.Level]quota.Limits) (rpm, tpm []ratelimit.Bucket) {
	for _, t := range s.Targets() {
		l, ok := ls[t.Level]
		if !ok {
			continue
		}
		scope := string(t.Level) + ":" + t.ID
		if l.RPM != nil {
			rpm = append(rpm, ratelimit.Bucket{Scope: scope, Kind: "rpm", Limit: *l.RPM, Window: time.Minute})
		}
		if l.TPM != nil {
			tpm = append(tpm, ratelimit.Bucket{Scope: scope, Kind: "tpm", Limit: *l.TPM, Window: time.Minute})
		}
	}
	return rpm, tpm
}

// streamSlots returns the concurrent stream slots of the levels of s that have max_streams.
func streamSlots(s quota.Scope, ls map[quota.Level]quota.Limits) []ratelimit.Slot {
	var out []ratelimit.Slot
	for _, t := range s.Targets() {
		if l, ok := ls[t.Level]; ok && l.MaxStreams != nil {
			out = append(out, ratelimit.Slot{Scope: string(t.Level) + ":" + t.ID + ":streams", Limit: *l.MaxStreams})
		}
	}
	return out
}

// limit takes one request and tokens estimated tokens at every level with a limit, and a stream slot when stream
// (§6.1, §7.7 step 3). The operator is not limited (as on the runtime API). Limits are read from PostgreSQL: when it
// is unavailable the request is refused with 503 (fail closed). A limiter fault lets the request through (rate
// limits protect capacity; money is protected by the hold). On refusal it writes 429 rate_limit_exceeded with
// Retry-After and the x-ratelimit headers and returns false.
func (h *Handler) limit(ctx context.Context, w http.ResponseWriter, p llmproto.Protocol, pr tenant.Principal, s quota.Scope,
	tokens int64, stream bool) (*taken, bool) {
	t := &taken{h: h, tokens: tokens}
	if pr.Operator || s.OrgID == 0 {
		return t, true
	}
	ls, err := h.cfg.Limits.LimitsFor(ctx, s)
	if err != nil {
		h.cfg.Logger.Warn("model api: reading limits failed", "error", err.Error())
		reject(w, p, "unavailable", llmproto.ErrorInfo{Class: llmproto.ClassUnavailable, Message: "limits are unavailable; try again later"})
		return nil, false
	}
	rpm, tpm := buckets(s, ls)
	if h.cfg.Limiter == nil {
		rpm, tpm = nil, nil
	}
	refundRPM := func() {}
	if len(rpm) > 0 {
		d, ok := h.take(ctx, rpm, 1)
		if ok {
			rateHeaders(w, "requests", d)
		}
		if ok && !d.Allowed {
			h.tooMany(w, p, d, "too many requests: the requests-per-minute limit is reached")
			return nil, false
		}
		if ok {
			refundRPM = func() { h.cfg.Limiter.Refund(context.WithoutCancel(ctx), rpm, 1) }
		}
	}
	if len(tpm) > 0 {
		d, ok := h.take(ctx, tpm, tokens)
		if ok {
			rateHeaders(w, "tokens", d)
		}
		if ok && !d.Allowed {
			refundRPM()
			h.tooMany(w, p, d, "too many tokens: the tokens-per-minute limit is reached (estimate "+strconv.FormatInt(tokens, 10)+")")
			return nil, false
		}
		if ok {
			t.tpm = tpm
		}
	}
	if stream {
		if slots := streamSlots(s, ls); len(slots) > 0 {
			release, full, ok := h.cfg.Streams.Acquire(slots)
			if !ok {
				refundRPM()
				t.refundTokens(context.WithoutCancel(ctx), tokens)
				w.Header().Set("Retry-After", "1")
				reject(w, p, "rate_limit", llmproto.ErrorInfo{Class: llmproto.ClassRateLimit,
					Message: "too many concurrent streams (max_streams " + strconv.FormatInt(full.Limit, 10) + ")"})
				return nil, false
			}
			t.release = release
		}
	}
	return t, true
}

// take takes n from buckets; ok is false when the limiter failed (the request is let through).
func (h *Handler) take(ctx context.Context, b []ratelimit.Bucket, n int64) (ratelimit.Decision, bool) {
	d, err := h.cfg.Limiter.Take(ctx, b, n)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.cfg.Logger.Warn("model api: rate limiter failed; request let through", "error", err.Error())
		}
		return d, false
	}
	return d, true
}

func (h *Handler) tooMany(w http.ResponseWriter, p llmproto.Protocol, d ratelimit.Decision, msg string) {
	w.Header().Set("Retry-After", strconv.FormatInt(int64(ceilSeconds(d.RetryAfter)/time.Second), 10))
	reject(w, p, "rate_limit", llmproto.ErrorInfo{Class: llmproto.ClassRateLimit, Message: msg})
}

// rateHeaders writes x-ratelimit-limit-<what>, x-ratelimit-remaining-<what> and x-ratelimit-reset-<what>.
func rateHeaders(w http.ResponseWriter, what string, d ratelimit.Decision) {
	h := w.Header()
	h.Set("x-ratelimit-limit-"+what, strconv.FormatInt(d.Limit, 10))
	h.Set("x-ratelimit-remaining-"+what, strconv.FormatInt(max(d.Remaining, 0), 10))
	h.Set("x-ratelimit-reset-"+what, ceilSeconds(d.Reset).String())
}

// ceilSeconds rounds d up to whole seconds (at least 1 s).
func ceilSeconds(d time.Duration) time.Duration {
	s := (d + time.Second - 1) / time.Second
	if s < 1 {
		s = 1
	}
	return s * time.Second
}
