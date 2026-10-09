package call

import (
	"context"
	"errors"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// Call results recorded on gateway.call spans (call.result) and agentbox_gateway_calls_total besides the stable
// error codes of failed calls.
const (
	ResultCompleted = "completed"
	ResultReplayed  = "replayed"
	ResultCacheHit  = "cache_hit"
	ResultCoalesced = "coalesced"
	// ResultInternal is a store/internal failure (err != nil) other than the caller leaving.
	ResultInternal = "internal_error"
	// ResultDetached is the caller (worker request) leaving before the result; the call continues and settles.
	ResultDetached = "detached"
)

// callAttrs are the allowlisted attributes of a gateway.call span: IDs and the endpoint kind only — never the
// request body (prompts, queries, URLs).
func callAttrs(kind, taskID, attemptID, callID, subrunID string) []obs.Attr {
	a := []obs.Attr{obs.Str("gateway.kind", kind), obs.Str("task.id", taskID), obs.Str("attempt.id", attemptID),
		obs.Str("call.id", callID)}
	if subrunID != "" {
		a = append(a, obs.Str("subrun.id", subrunID))
	}
	return a
}

// callResult maps the outcome of Invoke/Exec to a bounded result label.
func callResult(res Result, err error) string {
	switch {
	case err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		return ResultDetached
	case err != nil:
		return ResultInternal
	case res.Code != "":
		return res.Code
	case res.Replayed:
		return ResultReplayed
	case res.source != "":
		return res.source
	}
	return ResultCompleted
}

// endCall finishes the gateway.call span and records the call metrics. Tool calls (search, fetch, exec) are
// counted per task unless replayed (a replay re-delivers a call the worker already made).
func endCall(span obs.Span, kind, taskID string, res Result, err error) {
	r := callResult(res, err)
	span.SetAttrs(obs.Str("call.result", r))
	if res.Status != 0 {
		span.SetAttrs(obs.Int("http.status_code", int64(res.Status)))
	}
	switch r {
	case ResultCompleted, ResultReplayed, ResultCacheHit, ResultCoalesced, ResultDetached:
	default:
		span.Fail(r)
	}
	span.End()
	m := obs.M()
	m.GatewayCall(kind, r)
	if kind != "chat" && r != ResultReplayed {
		m.ToolCall(taskID, kind)
	}
}

// Try outcomes recorded on gateway.try spans and agentbox_upstream_tries_total besides the upstream outcomes
// (ok, retryable, fatal, unknown): a hedge leg cancelled because the other leg decided the call, and a leg ended by
// the Coordinator's own cancellation. Neither is a provider failure.
const (
	TryHedgeLost = "hedge_lost"
	TryAborted   = "aborted"
)

// legProvider is the provider label of a leg: the route name, or the adapter's provider for single-provider kinds.
func legProvider(j *job, l leg) string {
	if l.provider != "" {
		return l.provider
	}
	return j.ad.Provider()
}

// tryAttrs are the allowlisted attributes of a gateway.try span: try number and, for routed kinds, the route,
// the routes skipped before it ("name:reason" from closed enumerations), whether it is a hedge leg and the
// route's breaker state when the leg started.
func tryAttrs(rs *routeSet, l leg, try Try) []obs.Attr {
	a := []obs.Attr{obs.Int("try.no", int64(try.TryNo))}
	if rs != nil && l.route >= 0 {
		a = append(a, obs.Str("route", l.provider), obs.Str("route_skipped", l.skipped), obs.Bool("hedge", l.hedge),
			obs.Str("breaker", rs.breakers[l.route].State().String()))
	}
	return a
}

// callRoute records the route of the leg that produced the call's result on the gateway.call span.
func callRoute(j *job, l leg) {
	if l.provider != "" {
		j.span.SetAttrs(obs.Str("gateway.route", l.provider), obs.Bool("hedge", l.hedge))
	}
}

// endLeg ends a leg's gateway.try span and records the upstream try metrics. A hedge leg that lost, or a leg ended
// by the Coordinator's cancellation, is recorded as hedge_lost / aborted and not as a failure.
func endLeg(j *job, rs *routeSet, r legResult, lost bool) {
	outcome, status, code := string(upstream.OutcomeOK), 200, ""
	if r.uerr != nil {
		outcome, status, code = string(r.uerr.Outcome), r.uerr.Status, r.uerr.Code
	}
	switch {
	case lost:
		outcome = TryHedgeLost
	case r.aborted && r.uerr != nil:
		outcome = TryAborted
	}
	if r.span != nil {
		r.span.SetAttrs(obs.Str("try.outcome", outcome), obs.Int("http.status_code", int64(status)))
		if rs != nil && r.leg.route >= 0 {
			r.span.SetAttrs(obs.Str("breaker_after", rs.breakers[r.leg.route].State().String()))
		}
		if code != "" && outcome != TryHedgeLost && outcome != TryAborted {
			r.span.Fail(code)
		}
		r.span.End()
	}
	obs.M().UpstreamTry(string(j.in.Kind), legProvider(j, r.leg), j.model, outcome, status, r.latency)
}
