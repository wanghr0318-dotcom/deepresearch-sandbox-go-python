package modelapi

import (
	"context"
	"path"
	"regexp"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// settleAttempts bounds the settlement retries; a hold left held is swept to charged_unknown at its deadline.
const settleAttempts = 3

// finish is §7.7 step 8: settle the hold with the actual cost (unknown outcomes conservatively, charged_unknown),
// refund unused tokens, release the stream slot, record the request and its tries, and update the metrics. It runs
// on a context of its own: a client that left does not stop the accounting.
func (h *Handler) finish(x *exchange, pr tenant.Principal, lim *taken, hold quota.HoldID, est int64) {
	if x.finished { // idempotent: the panic guard may run it after a normal call failed half-way
		return
	}
	x.finished = true
	ctx, cancel := context.WithTimeout(context.WithoutCancel(x.r.Context()), finishTimeout)
	defer cancel()
	defer lim.releaseSlot()

	charged, unknown := x.unknownCost, x.anyUnknown
	var okCost int64
	if x.picked {
		price := x.rt.Adapter().PricingOn(x.pick.Route, x.req.Model)
		c, ok := price.ChatCost(upstream.UsageFrom(x.usage))
		switch {
		case x.usageKnown && ok:
			okCost = c
		default: // usage never arrived (or is unusable): charge at least the worst case, as unknown
			okCost, unknown = max(c, x.estimateOn(x.pick.Route)), true
		}
		x.tries[x.okTry].CostMicro = okCost
		charged += okCost
	}
	answered := x.picked || x.anyUnknown // a provider may have charged for this request
	if !h.settle(ctx, x.id, hold, answered, charged, unknown) {
		return // the hold stays held; the sweep charges it as unknown, and no row is written (invariant G3)
	}
	switch {
	case !answered:
		lim.refundTokens(ctx, lim.tokens)
	case x.usageKnown && !unknown:
		lim.refundTokens(ctx, lim.tokens-x.usage.Total())
	}
	if unknown {
		charged = max(charged, est) // what the hold charged (Holder.Settle with unknown)
	} else if charged > est {
		// The actual cost exceeded the reservation (the estimate is conservative, not a bound): the actual is settled
		// and the overshoot is reported separately by verify-invariants (G2); count it.
		obs.M().ModelHoldOvershoot(string(x.proto))
	}

	provider := "none"
	rec := Record{ID: x.id, OrgID: pr.OrgID, UserID: pr.UserID, KeyID: pr.KeyID, ClientProtocol: x.proto, Model: x.req.Model,
		Stream: x.req.Stream, Status: x.status, ErrorCode: x.errCode, Usage: x.usage, UsageKnown: x.usageKnown && !unknown,
		CostMicro: charged, TTFB: x.ttfb, Latency: h.cfg.Now().Sub(x.start), StreamComplete: x.complete,
		Dropped: x.dropped, HoldID: hold, CreatedAt: x.start, Tries: x.tries}
	if len(x.tries) > 0 {
		provider = x.pick.Name
		rec.Provider, rec.ProviderProtocol, rec.Converted = x.pick.Name, x.pick.Protocol, x.converted
	}
	h.record(ctx, rec)
	m := obs.M()
	m.ModelRequest(string(x.proto), x.req.Model, provider, x.status, rec.Latency)
	if x.complete != nil {
		m.ModelStream(string(x.proto), *x.complete)
	}
	for _, f := range x.dropped {
		m.ModelDropped(fieldName(f))
	}
}

// recordAttempts bounds the retries of writing the model_requests row.
const recordAttempts = 3

// record writes the request's row, retrying a few times; a row that cannot be written is logged and counted (the hold
// is already settled, so invariant G3 still holds; the usage reconcile reports the missing row).
func (h *Handler) record(ctx context.Context, rec Record) {
	var err error
	for i := range recordAttempts {
		if err = h.cfg.Store.RecordModelRequest(ctx, rec); err == nil {
			return
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(i+1) * 200 * time.Millisecond):
		}
	}
	obs.M().ModelRecordFailure()
	h.cfg.Logger.Error("model api: recording the request failed", "request_id", rec.ID, "hold_id", string(rec.HoldID),
		"error", err.Error())
}

// settle settles (or, when no provider answered, releases) the hold, retrying a few times.
func (h *Handler) settle(ctx context.Context, id string, hold quota.HoldID, answered bool, charged int64, unknown bool) bool {
	var err error
	for i := range settleAttempts {
		if answered {
			err = h.cfg.Holder.Settle(ctx, hold, charged, unknown)
		} else {
			err = h.cfg.Holder.Release(ctx, hold)
		}
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(i+1) * 200 * time.Millisecond):
		}
	}
	h.cfg.Logger.Error("model api: settling the quota hold failed; the sweep will charge it as unknown", "request_id", id,
		"hold_id", string(hold), "error", err.Error())
	return false
}

var indexRe = regexp.MustCompile(`\[\d+\]`)

// fieldName is the bounded metric label of a dropped field: its last path segment without indices
// ("messages[2].content[0].cache_control" → "cache_control").
func fieldName(f string) string {
	f = indexRe.ReplaceAllString(f, "")
	if i := len(f) - len(path.Ext(f)); i < len(f) {
		return f[i+1:]
	}
	return f
}
