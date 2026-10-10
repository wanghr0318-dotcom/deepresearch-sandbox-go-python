package doctor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

// Stable Gateway codes the rules look for (persistence.Code*, upstream.Code*, call.Code*; copied, not imported:
// the doctor reads the public journal, whose error strings are these codes).
const (
	codeCallDeadline     = "call_deadline_exceeded"
	codeModelDegraded    = "model_degraded"
	codeToolBudget       = "tool_budget_exhausted"
	codeBudgetExhausted  = "budget_exhausted"
	codeBudgetShort      = "budget_insufficient_for_request"
	codeSubrunBudget     = "subrun_budget_exhausted"
	codeExecQueueTimeout = "exec_queue_timeout"
)

// blockedFetchCodes are fetch errors where the target or the egress policy refused the request.
var blockedFetchCodes = map[string]bool{"egress_blocked": true, "upstream_rejected": true,
	"too_many_redirects": true, "invalid_url": true}

type rule struct {
	id  string
	run func(a *analysis) *Finding
}

// rules in precedence order: a failed or slow task is attributed to the first rule that explains it.
var rules = []rule{
	{"model_try_hang", ruleModelHang},
	{"breaker_degraded", ruleBreaker},
	{"model_upstream_errors", ruleModelErrors},
	{"fetch_deadline", ruleFetchDeadline},
	{"fetch_blocked", ruleFetchBlocked},
	{"tool_budget_exhausted", ruleToolBudget},
	{"cost_budget_exhausted", ruleCostBudget},
	{"exec_timeout", ruleExecTimeout},
	{"retry_backoff", ruleBackoff},
	{"hedge_waste", ruleHedge},
	{"sandbox_start", ruleSandboxStart},
	{"admission_queue", ruleQueue},
	{"stop_latency", ruleStop},
}

const maxExamples = 5

func routeName(t eval.Try) string {
	if t.Provider == "" {
		return "default"
	}
	return t.Provider
}

// okLatencies collects the latencies of ok tries of a call kind, per route and overall.
func (a *analysis) okLatencies(kinds ...string) (map[string][]int64, []int64) {
	per := map[string][]int64{}
	var all []int64
	for _, r := range a.recs {
		for _, c := range r.tr.Calls {
			if !contains(kinds, kindOf(c.Endpoint)) {
				continue
			}
			for _, t := range c.Tries {
				if t.Outcome == "ok" && !t.HedgeLost {
					per[routeName(t)] = append(per[routeName(t)], t.LatencyMs)
					all = append(all, t.LatencyMs)
				}
			}
		}
	}
	return per, all
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// ---- model_try_hang ----

// hungTry reports whether a model try that did not succeed ran ≥ max(10 s, 10 × its route's median ok latency) or
// never finished.
func (a *analysis) hungTry(t eval.Try) bool {
	if t.Outcome == "ok" || t.HedgeLost {
		return false
	}
	if t.State == "in_flight" {
		return true
	}
	if a.hangThr == nil {
		a.hangThr = map[string]int64{}
		okPer, _ := a.okLatencies("chat")
		for route, xs := range okPer {
			a.hangThr[route] = max(int64(10_000), 10*pct(xs, 50))
		}
	}
	thr, ok := a.hangThr[routeName(t)]
	if !ok {
		thr = 10_000
	}
	return t.LatencyMs >= thr
}

func ruleModelHang(a *analysis) *Finding {
	okPer, okAll := a.okLatencies("chat")
	ev := Evidence{Counts: map[string]int64{}, LatencyMs: map[string]int64{}, Shares: map[string]float64{}}
	var hungLatency, affectedLatency int64
	calls := 0
	for _, r := range a.recs {
		hit := false
		for _, c := range r.tr.Calls {
			if kindOf(c.Endpoint) != "chat" {
				continue
			}
			callHit := false
			for _, t := range c.Tries {
				route := routeName(t)
				if !a.hungTry(t) {
					continue
				}
				ev.Counts["hung_tries."+route]++
				ev.Counts["hung_tries"]++
				hungLatency += t.LatencyMs
				ev.LatencyMs["hung_max."+route] = max(ev.LatencyMs["hung_max."+route], t.LatencyMs)
				callHit = true
				if len(ev.Examples) < maxExamples && (r.failed || r.slow) {
					ev.Examples = append(ev.Examples, example(r, c.CallID,
						fmt.Sprintf("try %d on %s: %s after %d ms (%s)", t.TryNo, route, outcomeOf(t), t.LatencyMs, nz(t.Error, "no error code"))))
				}
			}
			if callHit {
				calls++
				hit = true
			}
		}
		if hit {
			ev.Counts["tasks_with_hung_tries"]++
			if a.mark(r, "model_try_hang") {
				affectedLatency += r.tr.LatencyMs
			}
		}
	}
	if ev.Counts["hung_tries"] == 0 {
		return nil
	}
	ev.Counts["hung_calls"] = int64(calls)
	for route, xs := range okPer {
		ev.LatencyMs["ok_p50."+route] = pct(xs, 50)
		ev.LatencyMs["ok_p99."+route] = pct(xs, 99)
		ev.Counts["ok_tries."+route] = int64(len(xs))
	}
	if affectedLatency > 0 {
		ev.Shares["hung_try_time_of_affected_task_latency"] = share(hungLatency, affectedLatency)
	}
	cur, curText := current(a.cfg, "model-try-timeout")
	ev.Detail = append(ev.Detail, fmt.Sprintf("--model-try-timeout is %s", curText),
		"a model try that never answers holds the whole call until the model call deadline (or the client gives up) unless a per-try timeout moves it to the next provider")
	f := &Finding{Severity: severity(a, "model_try_hang"), Title: "Model tries hang without a per-try timeout",
		Summary: fmt.Sprintf("%d model tries in %d calls ran ≥ 10× the provider's median latency (or never finished) without succeeding; ok tries took p50 %d ms / p99 %d ms.",
			ev.Counts["hung_tries"], calls, pct(okAll, 50), pct(okAll, 99)), Evidence: ev}
	if a.chain {
		to := clampMs(4*pct(okAll, 99), 2000, 120_000)
		if cur == 0 || cur > to {
			f.Proposals = append(f.Proposals, Proposal{Kind: KindApply, Flag: "--model-try-timeout", From: curText,
				To: formatMs(to), Reason: fmt.Sprintf("4 × p99 of ok model tries (%d ms), bounded to [2s, 2m]: a hung try fails over to the next provider after %s", pct(okAll, 99), formatMs(to))})
		}
	} else {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory, Text: "configure a model fallback chain (--model-fallback-file) and a --model-try-timeout: with a single provider a hung try can only wait for the call deadline"})
	}
	return f
}

func outcomeOf(t eval.Try) string {
	if t.State == "in_flight" {
		return "still in flight"
	}
	return nz(t.Outcome, t.State)
}

func nz(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// severity is critical when the rule explains a failed task, warning when it explains a slow one, else info.
func severity(a *analysis, ruleID string) string {
	c := a.explain[ruleID]
	switch {
	case c[0] > 0:
		return SevCritical
	case c[1] > 0:
		return SevWarning
	}
	return SevInfo
}

// ---- breaker_degraded ----

func ruleBreaker(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}}
	for _, r := range a.recs {
		hit := false
		for _, c := range r.tr.Calls {
			if c.FailReason == codeModelDegraded {
				ev.Counts["calls_model_degraded"]++
				hit = true
				if len(ev.Examples) < maxExamples {
					ev.Examples = append(ev.Examples, example(r, c.CallID, "every provider's breaker was open: 503 model_degraded"))
				}
			}
			for _, t := range c.Tries {
				for _, s := range strings.Split(t.Skipped, ",") {
					if name, reason, ok := strings.Cut(s, ":"); ok && reason == "circuit_open" {
						ev.Counts["skipped_circuit_open."+name]++
					}
				}
			}
		}
		if hit {
			a.mark(r, "breaker_degraded")
		}
	}
	if len(ev.Counts) == 0 {
		return nil
	}
	f := &Finding{Severity: severity(a, "breaker_degraded"), Title: "Circuit breakers open / degraded mode",
		Summary: fmt.Sprintf("%d model calls failed fast with model_degraded; breaker skips per route are in the counts.",
			ev.Counts["calls_model_degraded"]), Evidence: ev}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "a provider stayed unhealthy long enough to open its breaker: check the provider (status page, key, quota) or add another fallback provider; the breaker itself worked as designed"})
	return f
}

// ---- model_upstream_errors ----

func ruleModelErrors(a *analysis) *Finding {
	tries := map[string]int64{}
	errs := map[string]int64{}
	codes := map[string]int64{}
	ev := Evidence{Counts: map[string]int64{}, Shares: map[string]float64{}}
	var routesOrder []string
	for _, r := range a.recs {
		failedCall := false
		for _, c := range r.tr.Calls {
			if kindOf(c.Endpoint) != "chat" {
				continue
			}
			for _, t := range c.Tries {
				if t.HedgeLost || a.hungTry(t) {
					continue
				}
				route := routeName(t)
				if _, ok := tries[route]; !ok {
					routesOrder = append(routesOrder, route)
				}
				tries[route]++
				if t.Outcome == "retryable" || t.Outcome == "fatal" || t.Outcome == "unknown" {
					errs[route]++
					codes[route+"."+nz(t.Error, t.Outcome)]++
					if len(ev.Examples) < maxExamples && (r.failed || r.slow) {
						ev.Examples = append(ev.Examples, example(r, c.CallID,
							fmt.Sprintf("try %d on %s: %s %s after %d ms", t.TryNo, route, t.Outcome, t.Error, t.LatencyMs)))
					}
				}
			}
			if providerFailure(c) {
				failedCall = true
			}
		}
		if failedCall {
			a.mark(r, "model_upstream_errors")
		}
	}
	worst, worstShare := "", 0.0
	fire := false
	for _, route := range routesOrder {
		s := share(errs[route], tries[route])
		ev.Counts["tries."+route] = tries[route]
		ev.Counts["errors."+route] = errs[route]
		ev.Shares["error_rate."+route] = s
		if errs[route] >= 2 && s >= 0.05 {
			fire = true
		}
		if s > worstShare {
			worst, worstShare = route, s
		}
	}
	if !fire {
		return nil
	}
	for k, v := range codes {
		ev.Counts["code."+k] = v
	}
	f := &Finding{Severity: severity(a, "model_upstream_errors"), Title: "Model provider errors",
		Summary: fmt.Sprintf("route %s failed %.0f%% of its model tries (codes in the counts).", worst, worstShare*100), Evidence: ev}
	if !a.chain {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
			Text: "add a fallback provider (--model-fallback-file): with a single provider every retryable error costs a backoff (2 s, 4 s, …) instead of an immediate failover"})
		return f
	}
	if worstShare > 0.5 {
		cur, curText := current(a.cfg, "model-breaker-failures")
		if cur > 2 {
			f.Proposals = append(f.Proposals, Proposal{Kind: KindApply, Flag: "--model-breaker-failures", From: curText, To: "2",
				Reason: fmt.Sprintf("route %s fails %.0f%% of tries: open its breaker after 2 consecutive failures so calls skip it sooner", worst, worstShare*100)})
		}
		if worst == routesOrder[0] {
			f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
				Text: fmt.Sprintf("the first route (%s) is the least reliable: consider making a healthier provider the primary (chain order is not changed automatically)", worst)})
		}
	}
	return f
}

// providerFailure reports whether a failed call failed because of its upstream tries (not a ledger, tool-budget
// or breaker refusal, which other rules explain).
func providerFailure(c eval.Call) bool {
	if c.State != "failed" || len(c.Tries) == 0 {
		return false
	}
	switch c.FailReason {
	case codeModelDegraded, codeBudgetExhausted, codeBudgetShort, codeSubrunBudget, codeToolBudget:
		return false
	}
	return true
}

// ---- fetch_deadline ----

// deadlineCutoff returns the latency (ms) at which search/fetch tries are cut by the call deadline: from the config
// when given (90 % of --call-deadline, or of its default), otherwise inferred when ≥ 3 unsuccessful tries cluster
// within 15 % of the longest one (a deadline cuts every slow try at the same point). 0 = unknown.
func (a *analysis) deadlineCutoff(kinds ...string) (cutoff int64, inferred bool) {
	if a.cfg != nil {
		cur, _ := current(a.cfg, "call-deadline")
		return cur * 85 / 100, false
	}
	var xs []int64
	for _, r := range a.recs {
		for _, c := range r.tr.Calls {
			if !contains(kinds, kindOf(c.Endpoint)) {
				continue
			}
			for _, t := range c.Tries {
				if t.Outcome == "unknown" || t.Outcome == "retryable" {
					xs = append(xs, t.LatencyMs)
				}
			}
		}
	}
	if len(xs) < 3 || pct(xs, 100) < 500 || pct(xs, 10) < pct(xs, 100)*85/100 {
		return 0, false
	}
	return pct(xs, 10), true
}

func ruleFetchDeadline(a *analysis) *Finding {
	_, okAll := a.okLatencies("search", "fetch")
	cutoff, inferred := a.deadlineCutoff("search", "fetch")
	ev := Evidence{Counts: map[string]int64{}, LatencyMs: map[string]int64{}}
	var cut []int64
	for _, r := range a.recs {
		hit := false // one example per task: the examples should span tasks
		for _, c := range r.tr.Calls {
			k := kindOf(c.Endpoint)
			if k != "search" && k != "fetch" || c.State == "completed" {
				continue
			}
			deadline := c.FailReason == codeCallDeadline
			for _, t := range c.Tries {
				if t.Outcome == "ok" {
					continue
				}
				if t.Error == codeCallDeadline || (cutoff > 0 && t.LatencyMs >= cutoff) {
					deadline = true
					cut = append(cut, t.LatencyMs)
				}
			}
			if deadline {
				ev.Counts["calls_deadline."+k]++
				ev.Counts["calls_deadline"]++
				if len(ev.Examples) < maxExamples && !hit {
					note := fmt.Sprintf("%s call %s: %s", k, c.State, nz(c.FailReason, "no fail reason"))
					if n := len(c.Tries); n > 0 {
						last := c.Tries[n-1]
						note = fmt.Sprintf("%s call %s: try %d %s %s after %d ms", k, c.State, last.TryNo,
							nz(last.Outcome, last.State), nz(last.Error, c.FailReason), last.LatencyMs)
					}
					ev.Examples = append(ev.Examples, example(r, c.CallID, note))
				}
				hit = true
			}
		}
		if hit {
			ev.Counts["tasks"]++
			a.mark(r, "fetch_deadline")
		}
	}
	if ev.Counts["calls_deadline"] == 0 {
		return nil
	}
	for _, k := range []string{"search", "fetch"} {
		_, xs := a.okLatencies(k)
		ev.Counts["ok_tries."+k] = int64(len(xs))
		if len(xs) > 0 {
			ev.LatencyMs["ok_p99."+k] = pct(xs, 99)
		}
	}
	ev.LatencyMs["cut_try_p50"] = pct(cut, 50)
	ev.LatencyMs["cut_try_max"] = pct(cut, 100)
	cur, curText := current(a.cfg, "call-deadline")
	if a.cfg == nil {
		// Without the config the deadline is at least the longest cut try.
		cur = max(int64(1000), clampMs(pct(cut, 100), 0, 1<<40))
		curText = "unknown (≈ " + formatMs(cur) + " inferred)"
	}
	how := "the tries were cut at ≈ 85–100 % of --call-deadline"
	if inferred {
		how = "the failed tries all stopped at the same latency, the signature of a deadline"
	}
	ev.Detail = append(ev.Detail, "--call-deadline is "+curText, how,
		"a search/fetch call that hits its deadline settles unknown (upstream_unconfirmed) and the worker gets 504; if every fetch of a research task is cut, the task has no evidence")
	to := clampMs(max(4*pct(okAll, 99), 10*cur), 10_000, 120_000)
	f := &Finding{Severity: severity(a, "fetch_deadline"), Title: "Search/fetch calls hit the call deadline",
		Summary: fmt.Sprintf("%d search/fetch calls were cut by the call deadline (cut tries p50 %d ms, max %d ms — the cut-off, not the time the target needs); ok fetch tries: %d.",
			ev.Counts["calls_deadline"], pct(cut, 50), pct(cut, 100), ev.Counts["ok_tries.fetch"]), Evidence: ev}
	if to > cur {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindApply, Flag: "--call-deadline", From: curText, To: formatMs(to),
			Reason: fmt.Sprintf("max(4 × p99 of ok search/fetch tries, 10 × the current deadline), bounded to [10s, 2m]: %s", formatMs(to))})
	}
	return f
}

// ---- fetch_blocked ----

func ruleFetchBlocked(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}}
	for _, r := range a.recs {
		blocked, fetches := 0, 0
		for _, c := range r.tr.Calls {
			if kindOf(c.Endpoint) != "fetch" {
				continue
			}
			fetches++
			code := c.FailReason
			for _, t := range c.Tries {
				if blockedFetchCodes[t.Error] {
					code = t.Error
				}
			}
			if blockedFetchCodes[code] {
				blocked++
				ev.Counts["code."+code]++
				ev.Counts["blocked_fetches"]++
				if len(ev.Examples) < maxExamples {
					ev.Examples = append(ev.Examples, example(r, c.CallID, "fetch refused: "+code))
				}
			}
		}
		if blocked > 0 && blocked == fetches {
			a.mark(r, "fetch_blocked")
		}
	}
	if ev.Counts["blocked_fetches"] == 0 {
		return nil
	}
	f := &Finding{Severity: severity(a, "fetch_blocked"), Title: "Fetches refused by the target or the egress policy",
		Summary: fmt.Sprintf("%d fetches were refused (codes in the counts).", ev.Counts["blocked_fetches"]), Evidence: ev}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "skill text: ask the research agent to prefer primary sources and to skip sites that refuse automated clients; check the egress allowlist for legitimate targets"})
	return f
}

// ---- tool_budget_exhausted ----

func ruleToolBudget(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}}
	for _, r := range a.recs {
		hit := false
		for _, c := range r.tr.Calls {
			if c.FailReason == codeToolBudget {
				ev.Counts["calls"]++
				hit = true
				if len(ev.Examples) < maxExamples {
					ev.Examples = append(ev.Examples, example(r, c.CallID, "429 tool_budget_exhausted"))
				}
			}
		}
		if hit {
			ev.Counts["tasks"]++
			a.mark(r, "tool_budget_exhausted")
		}
	}
	if ev.Counts["calls"] == 0 {
		return nil
	}
	cur, curText := current(a.cfg, "turn-tool-budget")
	to := min(int64(1000), (cur*3+1)/2)
	f := &Finding{Severity: severity(a, "tool_budget_exhausted"), Title: "Tool budget exhausted",
		Summary: fmt.Sprintf("%d search/fetch calls were refused with tool_budget_exhausted in %d tasks.", ev.Counts["calls"], ev.Counts["tasks"]), Evidence: ev}
	if to > cur {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindApply, Flag: "--turn-tool-budget", From: curText, To: fmt.Sprint(to),
			Reason: "session turns ran out of web_search/web_fetch calls: +50 % (bounded 1000)"})
	}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "for research tasks lower research.max_fetch / max_tasks in the suite or task spec instead, so the agent plans within its budget"})
	return f
}

// ---- cost_budget_exhausted ----

func ruleCostBudget(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}}
	for _, r := range a.recs {
		hit := false
		for _, c := range r.tr.Calls {
			switch c.FailReason {
			case codeBudgetExhausted, codeBudgetShort, codeSubrunBudget:
				ev.Counts["code."+c.FailReason]++
				ev.Counts["calls"]++
				hit = true
				if len(ev.Examples) < maxExamples {
					note := c.FailReason
					if b := r.tr.Budget; b != nil {
						note += fmt.Sprintf(" (spent %d of %d micro-USD)", b.SpentMicro, b.LimitMicro)
					}
					ev.Examples = append(ev.Examples, example(r, c.CallID, note))
				}
			}
		}
		if hit || strings.Contains(r.tr.StatusReason, "budget") {
			ev.Counts["tasks"]++
			a.mark(r, "cost_budget_exhausted")
		}
	}
	if ev.Counts["tasks"] == 0 {
		return nil
	}
	f := &Finding{Severity: severity(a, "cost_budget_exhausted"), Title: "Cost budget exhausted",
		Summary: fmt.Sprintf("%d calls were refused by the task budget in %d tasks.", ev.Counts["calls"], ev.Counts["tasks"]), Evidence: ev}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "raise limits.budget_micro for these tasks (suite defaults or task limits) or lower max_tokens; the ledger refused calls whose worst-case estimate did not fit"})
	return f
}

// ---- exec_timeout ----

func ruleExecTimeout(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}}
	queue := false
	for _, r := range a.recs {
		hit := false
		switch r.tr.Outcome.Category {
		case "exec_timeout", "check_timeout":
			ev.Counts["category."+r.tr.Outcome.Category]++
			hit = true
		}
		for _, c := range r.tr.Calls {
			if kindOf(c.Endpoint) != "exec" {
				continue
			}
			for _, t := range c.Tries {
				if t.Error == codeExecQueueTimeout {
					ev.Counts["exec_queue_timeout"]++
					queue, hit = true, true
				}
			}
		}
		if hit {
			ev.Counts["tasks"]++
			if len(ev.Examples) < maxExamples {
				ev.Examples = append(ev.Examples, example(r, "", nz(r.tr.Outcome.Category, "exec queue timeout")))
			}
			a.mark(r, "exec_timeout")
		}
	}
	if ev.Counts["tasks"] == 0 {
		return nil
	}
	f := &Finding{Severity: severity(a, "exec_timeout"), Title: "Exec runs timed out",
		Summary: fmt.Sprintf("%d tasks had exec/checker timeouts or exec queue timeouts.", ev.Counts["tasks"]), Evidence: ev}
	if queue {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
			Text: "exec requests waited longer than --exec-queue-timeout for a slot: raise --exec-slots (memory: slots × --exec-memory-max) or lower eval concurrency"})
	}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "raise wall_ms for the affected tasks in the suite if correct solutions need longer; a checker timeout with a reference solution is a suite problem"})
	return f
}

// ---- retry_backoff ----

func ruleBackoff(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}, LatencyMs: map[string]int64{}}
	var total, lat int64
	for _, r := range a.recs {
		if !(r.failed || r.slow) || r.bd.BackoffEstMs < 2000 || share(r.bd.BackoffEstMs, r.tr.LatencyMs) < 0.3 {
			continue
		}
		ev.Counts["tasks"]++
		total += r.bd.BackoffEstMs
		lat += r.tr.LatencyMs
		if len(ev.Examples) < maxExamples {
			ev.Examples = append(ev.Examples, example(r, "", fmt.Sprintf("≈%d ms of backoff in %d ms", r.bd.BackoffEstMs, r.tr.LatencyMs)))
		}
		a.mark(r, "retry_backoff")
	}
	if ev.Counts["tasks"] == 0 {
		return nil
	}
	ev.LatencyMs["backoff_est_total"] = total
	f := &Finding{Severity: severity(a, "retry_backoff"), Title: "Retries and backoff dominate wall time",
		Summary:  fmt.Sprintf("in %d failed/slow tasks the Gateway's retry backoff is ≈%.0f%% of their latency.", ev.Counts["tasks"], share(total, lat)*100),
		Evidence: ev}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
		Text: "a fallback provider turns retries into immediate failovers (no backoff between providers); fix the failing upstream first"})
	return f
}

// ---- hedge_waste ----

func ruleHedge(a *analysis) *Finding {
	ev := Evidence{Counts: map[string]int64{}, LatencyMs: map[string]int64{}}
	for _, r := range a.recs {
		for _, c := range r.tr.Calls {
			hedged := false
			for _, t := range c.Tries {
				if t.Hedge {
					hedged = true
					ev.Counts["hedge_legs"]++
					if t.Outcome == "ok" && !t.HedgeLost {
						ev.Counts["hedge_wins"]++
					}
				}
				if t.HedgeLost {
					ev.Counts["legs_lost"]++
				}
			}
			if hedged && c.PossibleDuplicate {
				ev.Counts["possible_duplicates"]++
			}
		}
		if r.tr.Metrics.UnknownMicro > 0 && ev.Counts["hedge_legs"] > 0 {
			ev.Counts["unknown_micro"] += r.tr.Metrics.UnknownMicro
		}
	}
	legs := ev.Counts["hedge_legs"]
	if legs < 3 || share(ev.Counts["hedge_wins"], legs) >= 0.2 {
		return nil
	}
	f := &Finding{Severity: SevWarning, Title: "Hedged requests rarely win",
		Summary:  fmt.Sprintf("%d hedge legs, %d won (%.0f%%); lost legs that were already sent are charged as unknown.", legs, ev.Counts["hedge_wins"], share(ev.Counts["hedge_wins"], legs)*100),
		Evidence: ev}
	cur, curText := current(a.cfg, "model-hedge-delay")
	if cur > 0 {
		to := min(int64(60_000), 2*cur)
		f.Proposals = append(f.Proposals, Proposal{Kind: KindApply, Flag: "--model-hedge-delay", From: curText, To: formatMs(to),
			Reason: "hedges fire too early: double the delay (it should sit near the primary's p95 latency)"})
	} else {
		f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory,
			Text: "hedging costs up to two estimates per call and rarely wins here: raise --model-hedge-delay toward the primary's p95 or turn it off"})
	}
	return f
}

// ---- sandbox_start / admission_queue / stop_latency ----

func ruleSandboxStart(a *analysis) *Finding {
	var starts []int64
	for _, r := range a.recs {
		starts = append(starts, r.starts...)
	}
	return a.latencyRule("sandbox_start", starts, 10_000, func(r *record) int64 { return r.bd.StartMs },
		"Sandbox start latency", "attempt created → worker ready",
		"pre-warm environments (k8s provider warm pool) or shrink the template; check host load during the run")
}

func ruleQueue(a *analysis) *Finding {
	var qs []int64
	for _, r := range a.recs {
		qs = append(qs, r.bd.QueueMs)
	}
	return a.latencyRule("admission_queue", qs, 5_000, func(r *record) int64 { return r.bd.QueueMs },
		"Admission queueing", "task created → first attempt",
		"tasks waited for a run slot: raise --run-slots if memory allows (--memory-bytes), or lower client concurrency")
}

func ruleStop(a *analysis) *Finding {
	var ss []int64
	for _, r := range a.recs {
		if r.bd.StopMs > 0 {
			ss = append(ss, r.bd.StopMs)
		}
	}
	return a.latencyRule("stop_latency", ss, 10_000, func(r *record) int64 { return r.bd.StopMs },
		"Stop latency", "pause/cancel accepted → stopped",
		"stops wait for in-flight calls to settle and the worker's final checkpoint; look at the slowest tasks' traces")
}

// latencyRule fires when p95 of xs ≥ p95Limit, or when the component is ≥ 30 % of some slow tasks' latency.
func (a *analysis) latencyRule(id string, xs []int64, p95Limit int64, part func(*record) int64, title, what, advice string) *Finding {
	if len(xs) == 0 {
		return nil
	}
	ev := Evidence{Counts: map[string]int64{"samples": int64(len(xs))},
		LatencyMs: map[string]int64{"p50": pct(xs, 50), "p95": pct(xs, 95), "max": pct(xs, 100)}}
	type cand struct {
		r *record
		v int64
	}
	var cs []cand
	for _, r := range a.recs {
		if (r.failed || r.slow) && part(r) >= 1000 && share(part(r), r.tr.LatencyMs) >= 0.3 {
			cs = append(cs, cand{r, part(r)})
		}
	}
	if pct(xs, 95) < p95Limit && len(cs) == 0 {
		return nil
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].v > cs[j].v })
	for _, c := range cs {
		if a.mark(c.r, id) && len(ev.Examples) < maxExamples {
			ev.Examples = append(ev.Examples, example(c.r, "", fmt.Sprintf("%s %d ms of %d ms", what, c.v, c.r.tr.LatencyMs)))
		}
	}
	if a.explain[id] == [2]int{} && pct(xs, 95) < p95Limit {
		return nil
	}
	f := &Finding{Severity: severity(a, id), Title: title,
		Summary:  fmt.Sprintf("%s: p50 %d ms, p95 %d ms, max %d ms over %d samples.", what, pct(xs, 50), pct(xs, 95), pct(xs, 100), len(xs)),
		Evidence: ev}
	f.Proposals = append(f.Proposals, Proposal{Kind: KindAdvisory, Text: advice})
	return f
}
