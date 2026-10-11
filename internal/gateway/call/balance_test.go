package call

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ---- routing modes and provider protocols (model gateway design §7.6) ----

// Smooth weighted round-robin over 10 000 picks matches the weights exactly (every cycle of sum(weights) picks
// gives each route its weight) and is smooth: the heaviest route never runs more than its share in a row.
func TestSmoothWeightedRoundRobin(t *testing.T) {
	b := newBalancer(RoutingWeighted, []string{"a", "b", "c"}, map[string]int{"a": 5, "b": 3, "c": 2})
	counts := make([]int, 3)
	run, maxRun, last := 0, 0, -1
	for n := 0; n < 10_000; n++ {
		i := b.next([]int{0, 1, 2}, signalResponse)
		counts[i]++
		if i == last {
			run++
		} else {
			run, last = 1, i
		}
		maxRun = max(maxRun, run)
	}
	if counts[0] != 5000 || counts[1] != 3000 || counts[2] != 2000 {
		t.Fatalf("counts %v, want [5000 3000 2000]", counts)
	}
	if maxRun > 2 {
		t.Fatalf("not smooth: route ran %d times in a row", maxRun)
	}
	// The nginx sequence for 5:1:1 is a a b a c a a.
	b = newBalancer(RoutingWeighted, []string{"a", "b", "c"}, map[string]int{"a": 5, "b": 1, "c": 1})
	var seq []string
	for n := 0; n < 7; n++ {
		seq = append(seq, []string{"a", "b", "c"}[b.next([]int{0, 1, 2}, signalResponse)])
	}
	if got := strings.Join(seq, " "); got != "a a b a c a a" {
		t.Fatalf("sequence %s", got)
	}
}

// Latency mode: effective weight = weight / EWMA(ttft), with a floor; traffic moves to the fast route and back
// when the fast route slows down, and the slow route keeps probe traffic.
func TestLatencyModeShiftsTraffic(t *testing.T) {
	b := newBalancer(RoutingLatency, []string{"a", "b"}, nil)
	share := func(n int) []int {
		c := make([]int, 2)
		for k := 0; k < n; k++ {
			c[b.next([]int{0, 1}, signalResponse)]++
		}
		return c
	}
	if c := share(1000); c[0] != 500 || c[1] != 500 {
		t.Fatalf("no samples: %v, want equal split", c)
	}
	b.observe(signalResponse, 0, 100*time.Millisecond)
	b.observe(signalResponse, 1, 10*time.Millisecond)
	if c := share(1100); c[1] < 950 || c[0] < 50 {
		t.Fatalf("b 10× faster: %v, want b ≈ 10/11", c)
	}
	for k := 0; k < 50; k++ { // b becomes very slow: its EWMA converges to 5 s
		b.observe(signalResponse, 1, 5*time.Second)
	}
	c := share(1000)
	if c[0] < 900 || c[1] == 0 {
		t.Fatalf("b slow: %v, want most traffic on a and probes on b", c)
	}
	if float64(c[1]) < LatencyFloor*float64(c[0])*0.9 {
		t.Fatalf("floor: b got %d of %v", c[1], c)
	}
	// A route without samples is treated like the fastest known one (it gets probed).
	b2 := newBalancer(RoutingLatency, []string{"a", "b"}, nil)
	b2.observe(signalResponse, 0, 50*time.Millisecond)
	if eff := b2.effective([]int{0, 1}, signalResponse); eff[0] != eff[1] {
		t.Fatalf("unsampled route effective weight %v", eff)
	}
}

// weighted mode through the Coordinator: traffic follows the weights; a route whose breaker is open is not a
// candidate (no try, no reservation), and the fallback order after the pick is by weight.
func TestWeightedRoutingCoordinator(t *testing.T) {
	fr := newRoutes("primary", "b", "c")
	h := newRouteHarness(t, routeLimits(), RoutingConfig{Weights: map[string]int{"primary": 1, "b": 3, "c": 1}}, fr)
	h.store.setLimit("t1", 1_000_000_000)
	for n := 0; n < 50; n++ {
		if r := h.invoke(t, inv(fmt.Sprintf("w%d", n), chatBody)); r.Status != 200 {
			t.Fatalf("call %d: %+v", n, r)
		}
	}
	if got := []int{fr.calls(0), fr.calls(1), fr.calls(2)}; got[0] != 10 || got[1] != 30 || got[2] != 10 {
		t.Fatalf("calls per route %v, want [10 30 10]", got)
	}
	// b fails: the call fails over immediately (routeLimits forbids backoff) to the next route by weight; after
	// its breaker opens, b gets no tries at all.
	fr.setDown(1, true)
	for n := 0; n < 20; n++ {
		if r := h.invoke(t, inv(fmt.Sprintf("x%d", n), chatBody)); r.Status != 200 {
			t.Fatalf("failover call %d: %+v", n, r)
		}
	}
	if b := fr.calls(1); b != 30+DefaultBreakerFailures {
		t.Fatalf("b got %d tries, want %d (only until its breaker opened)", b, 30+DefaultBreakerFailures)
	}
	if !strings.Contains(states(h.c), "b:open") {
		t.Fatalf("states %s", states(h.c))
	}
}

// The order of a weighted call: the round-robin pick first, the rest by weight (ties in configuration order).
func TestWeightedOrder(t *testing.T) {
	fr := newRoutes("primary", "b", "c", "d")
	h := newRouteHarness(t, routeLimits(), RoutingConfig{Mode: RoutingWeighted, Weights: map[string]int{"c": 4, "d": 2}}, fr)
	rs := h.c.routes[upstream.KindChat]
	all := func(int) bool { return true }
	if got := fmt.Sprint(rs.order(all, signalResponse)); got != "[2 3 0 1]" {
		t.Fatalf("first order %s", got)
	}
	// ordered mode is the configuration order, always.
	ho := newRouteHarness(t, routeLimits(), RoutingConfig{}, newRoutes("primary", "b"))
	if got := fmt.Sprint(ho.c.routes[upstream.KindChat].order(all, signalResponse)); got != "[0 1]" || ho.c.routes[upstream.KindChat].bal != nil {
		t.Fatalf("ordered %s", got)
	}
	if _, err := New(Config{Store: h.store, Blobs: h.blobs, Events: h.events, Routing: RoutingConfig{Mode: "random"}}); err == nil {
		t.Fatal("unknown mode accepted")
	}
	if _, err := New(Config{Store: h.store, Blobs: h.blobs, Events: h.events, Routing: RoutingConfig{Weights: map[string]int{"b": 0}}}); err == nil {
		t.Fatal("weight 0 accepted")
	}
}

// mixedChain is the production chat adapter: an OpenAI primary on p and an Anthropic fallback "claude" on q.
func mixedChain(p, q *fakeupstream.Server) upstream.Adapter {
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	return upstream.NewChat(upstream.ChatConfig{BaseURL: p.ModelBaseURL(), Model: fakeupstream.Model, APIKey: "key-openai",
		Pricing: upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}, MaxTokensDefault: 64,
		HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0), StreamHTTP: d.HTTPClient(0, 0),
		Fallbacks: []upstream.ChatRoute{{Name: "claude", Protocol: llmproto.ProtocolAnthropic, BaseURL: q.URL(), APIKey: "key-anthropic",
			Models:  map[string]string{fakeupstream.Model: "claude-x"},
			Pricing: upstream.Pricing{InputMicroPerMTok: 3_000_000, OutputMicroPerMTok: 15_000_000, CacheReadMicroPerMTok: 300_000},
			HTTP:    d.HTTPClient(upstream.DefaultModelMaxBody, 0), StreamHTTP: d.HTTPClient(0, 0)}}})
}

func mixedServers(t *testing.T) (*fakeupstream.Server, *fakeupstream.Server) {
	t.Helper()
	p, q := fakeupstream.New(), fakeupstream.New()
	t.Cleanup(p.Close)
	t.Cleanup(q.Close)
	q.SetAnthropicKey("key-anthropic")
	return p, q
}

// A mixed-protocol chain: the OpenAI primary fails, the Anthropic fallback serves the sandbox call; the worker
// gets an OpenAI-shaped body; the journal records both tries, settles at the Anthropic price (cache reads at
// their price), and a retry of the same call replays the stored result without another upstream request. The
// fingerprint is that of the logical call (identical to the OpenAI-only chain).
func TestMixedProtocolFailoverJournals(t *testing.T) {
	p, q := mixedServers(t)
	p.SetAlways(fakeupstream.Chat, fakeupstream.Action{Status: 503})
	q.SetCacheUsage(4, 0)
	h := chaosHarness(t, mixedChain(p, q), routeLimits(), RoutingConfig{})
	body := `{"messages":[{"role":"user","content":"hi"}]}`
	r := h.invoke(t, inv("m1", body))
	if r.Status != 200 {
		t.Fatalf("result %+v", r)
	}
	var out struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(r.Body, &out); err != nil || out.Object != "chat.completion" || len(out.Choices) != 1 ||
		!strings.HasPrefix(out.Choices[0].Message.Content, "fake 回复") || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("body not OpenAI-shaped: %s", r.Body)
	}
	rec, tries := h.call(t, "t1", "m1")
	if got := providers(tries); got != "primary/retryable claude/ok[primary:tried]" {
		t.Fatalf("tries %s", got)
	}
	// Anthropic price: (prompt − 4) × 3 + 4 × 0.3 + completion × 15 micro-USD per 1e6 tokens, each part rounded up.
	plain := out.Usage.PromptTokens - 4
	want := (plain*3_000_000+999_999)/1_000_000 + (4*300_000+999_999)/1_000_000 + (out.Usage.CompletionTokens*15_000_000+999_999)/1_000_000
	if rec.CostCharged != want || rec.State != StateCompleted {
		t.Fatalf("record %+v, want cost %d", rec, want)
	}
	if b := h.budget(t, "t1"); b.SpentMicro != want || b.ReservedMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	again := h.invoke(t, inv("m1", body))
	if !again.Replayed || string(again.Body) != string(r.Body) || q.Count(fakeupstream.Messages) != 1 {
		t.Fatalf("replay %+v, messages=%d", again, q.Count(fakeupstream.Messages))
	}
	// The fingerprint does not depend on the routes' protocols.
	plainChain := chaosHarness(t, chaosAdapter(p.ModelBaseURL(), q.ModelBaseURL()), routeLimits(), RoutingConfig{})
	p.SetAlways(fakeupstream.Chat, fakeupstream.Action{})
	plainChain.invoke(t, inv("m1", body))
	if prec, _ := plainChain.call(t, "t1", "m1"); prec.Fingerprint != rec.Fingerprint {
		t.Fatalf("fingerprint %s vs OpenAI-only chain %s", rec.Fingerprint, prec.Fingerprint)
	}
}

// A request that uses a field only OpenAI has (json_schema response_format) is never sent to the Anthropic
// route: it is skipped as unsupported_protocol.
func TestProtocolSkipForFormatOnlyRequest(t *testing.T) {
	p, q := mixedServers(t)
	p.SetAlways(fakeupstream.Chat, fakeupstream.Action{Status: 503})
	h := chaosHarness(t, mixedChain(p, q), routeLimits(), RoutingConfig{})
	body := `{"messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_schema","json_schema":{"name":"x","schema":{"type":"object"}}}}`
	r := h.invoke(t, inv("s1", body))
	if r.Status == 200 || q.Count(fakeupstream.Messages) != 0 {
		t.Fatalf("result %+v, messages=%d", r, q.Count(fakeupstream.Messages))
	}
	_, tries := h.call(t, "t1", "s1")
	if len(tries) == 0 || !strings.Contains(providers(tries), "primary/retryable") {
		t.Fatalf("tries %s", providers(tries))
	}
	if strings.Contains(providers(tries), "claude/") {
		t.Fatalf("anthropic route tried: %s", providers(tries))
	}
}

// ---- the routing API of the model API (G3) ----

func modelRouting(t *testing.T, ad upstream.Adapter, rc RoutingConfig) *ModelRouting {
	t.Helper()
	h := chaosHarness(t, ad, routeLimits(), rc)
	m := h.c.ModelRouting()
	if m == nil {
		t.Fatal("no ModelRouting")
	}
	return m
}

// Plan/Next/Done: failover before the first byte across protocols, the protocol filter, degraded and no-route.
func TestModelRoutingPlan(t *testing.T) {
	p, q := mixedServers(t)
	m := modelRouting(t, mixedChain(p, q), RoutingConfig{BreakerFailures: 1})
	if rs := m.Routes(); len(rs) != 2 || rs[1].Protocol != llmproto.ProtocolAnthropic || rs[1].Name != "claude" {
		t.Fatalf("routes %+v", rs)
	}
	p.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Status: 503})
	body := []byte(`{"model":"` + fakeupstream.Model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)
	plan := m.Plan(fakeupstream.Model, "")
	if fmt.Sprint(plan.Eligible()) != "[0 1]" || plan.NoRoute() {
		t.Fatalf("eligible %v", plan.Eligible())
	}
	var served string
	for {
		pick, ok := plan.Next()
		if !ok {
			t.Fatal("ran out of routes")
		}
		s, e := m.DoStream(context.Background(), pick.Route, body, llmproto.ProtocolOpenAI)
		plan.Done(pick, e, false)
		if e != nil {
			if !Failover(context.Background(), e) {
				t.Fatalf("fatal %v", e)
			}
			continue
		}
		if _, err := drain(s); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		served = pick.Name + "[" + pick.Skipped + "]"
		break
	}
	if served != "claude[primary:tried]" {
		t.Fatalf("served by %s", served)
	}
	// BreakerFailures 1: the primary is open now. A plan that is restricted to OpenAI routes is degraded.
	only := m.Plan(fakeupstream.Model, llmproto.ProtocolOpenAI)
	if pick, ok := only.Next(); ok || !only.Degraded() || pick.Skipped != "primary:circuit_open,claude:unsupported_protocol" {
		t.Fatalf("restricted plan: %+v ok=%v degraded=%v", pick, ok, only.Degraded())
	}
	if none := m.Plan("other-model", ""); !none.NoRoute() {
		t.Fatal("unknown model has routes")
	}
}

func drain(s upstream.Stream) (llmproto.Response, error) {
	var c llmproto.Collector
	for {
		ev, err := s.Next()
		if errors.Is(err, io.EOF) {
			return c.Response(), nil
		}
		if err != nil {
			return c.Response(), err
		}
		c.Add(ev)
	}
}

// The per-try timeout covers the time to first token: a provider that hangs before answering, or that sends
// message_start and then stalls, is cut (unknown, so the model API can fail over); a stream whose events keep
// coming after the first content is not cut even when the whole stream takes longer than the timeout.
func TestModelRoutingTryTimeoutUntilFirstByte(t *testing.T) {
	p, q := mixedServers(t)
	const tt = 600 * time.Millisecond
	m := modelRouting(t, mixedChain(p, q), RoutingConfig{TryTimeout: tt})
	body := []byte(`{"model":"` + fakeupstream.Model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)
	q.Inject(fakeupstream.Messages, 1, fakeupstream.Action{Hang: true})
	start := time.Now()
	_, e := m.DoStream(context.Background(), 1, body, llmproto.ProtocolOpenAI)
	if e == nil || e.Outcome != upstream.OutcomeUnknown || !Failover(context.Background(), e) || time.Since(start) > 5*time.Second {
		t.Fatalf("hang: %v after %s", e, time.Since(start))
	}
	// message_start arrives at once, then every event waits 3 s: no content within the timeout.
	q.SetChunkDelay(fakeupstream.Messages, 3*time.Second)
	start = time.Now()
	_, e = m.DoStream(context.Background(), 1, body, llmproto.ProtocolOpenAI)
	if e == nil || e.Outcome != upstream.OutcomeUnknown || time.Since(start) > 2*time.Second {
		t.Fatalf("stall after message_start: %v after %s", e, time.Since(start))
	}
	// First content after 3 × 120 ms = 360 ms < 600 ms; the whole stream (7 events) takes ≥ 720 ms > 600 ms.
	q.SetChunkDelay(fakeupstream.Messages, 120*time.Millisecond)
	start = time.Now()
	s, e := m.DoStream(context.Background(), 1, body, llmproto.ProtocolOpenAI)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	resp, err := drain(s)
	if err != nil || len(resp.Blocks) != 1 || time.Since(start) < tt {
		t.Fatalf("slow stream cut: %v %+v after %s", err, resp, time.Since(start))
	}
	if info, ok := s.(upstream.StreamInfo); !ok || !info.Converted() {
		t.Fatal("StreamInfo not passed through")
	}
	// A cancelled client never fails over.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Failover(ctx, e) || Failover(ctx, &upstream.Error{Outcome: upstream.OutcomeRetryable}) {
		t.Fatal("Failover after the client went away")
	}
}

// ModelRouting.DoStream records time to first token itself: after both routes were measured (the primary
// slow, the Anthropic route fast), most latency-mode plans start on the Anthropic route.
func TestModelRoutingLatencyMode(t *testing.T) {
	p, q := mixedServers(t)
	p.SetLatency(fakeupstream.Chat, 200*time.Millisecond)
	m := modelRouting(t, mixedChain(p, q), RoutingConfig{Mode: RoutingLatency})
	body := []byte(`{"model":"` + fakeupstream.Model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":20}`)
	for _, route := range []int{0, 1} {
		s, e := m.DoStream(context.Background(), route, body, llmproto.ProtocolOpenAI)
		if e != nil {
			t.Fatal(e)
		}
		_ = s.Close()
	}
	rs := m.c.routes[upstream.KindChat]
	if rs.bal.ewma[signalTTFT][0] < 0.2 || rs.bal.ewma[signalTTFT][1] >= 0.1 || rs.bal.ewma[signalResponse][0] != 0 {
		t.Fatalf("ttft samples %v, response samples %v", rs.bal.ewma[signalTTFT], rs.bal.ewma[signalResponse])
	}
	first := map[string]int{}
	for n := 0; n < 200; n++ {
		pick, ok := m.Plan(fakeupstream.Model, "").Next()
		if !ok {
			t.Fatal("no route")
		}
		first[pick.Name]++
		rs.breakers[pick.Route].Abort()
	}
	if first["claude"] < 170 || first["primary"] == 0 {
		t.Fatalf("first picks %v", first)
	}
}

// The two latency signals are kept apart; a plan uses its own signal and falls back to the other one only
// when no candidate has a sample of its own.
func TestLatencySignalsSeparateWithFallback(t *testing.T) {
	b := newBalancer(RoutingLatency, []string{"a", "b"}, nil)
	b.observe(signalResponse, 0, 10*time.Millisecond) // sandbox: a fast
	b.observe(signalResponse, 1, 100*time.Millisecond)
	if eff := b.effective([]int{0, 1}, signalTTFT); eff[0] <= eff[1] {
		t.Fatalf("no TTFT samples: falls back to the response signal, got %v", eff)
	}
	b.observe(signalTTFT, 0, 100*time.Millisecond) // model API: a slow
	b.observe(signalTTFT, 1, 10*time.Millisecond)
	if eff := b.effective([]int{0, 1}, signalTTFT); eff[0] >= eff[1] {
		t.Fatalf("TTFT plan must use TTFT samples, got %v", eff)
	}
	if eff := b.effective([]int{0, 1}, signalResponse); eff[0] <= eff[1] {
		t.Fatalf("sandbox plan must use response samples, got %v", eff)
	}
}

// Timeouts and unknown outcomes record a penalty (max of the try timeout and 2× the EWMA), so a route that
// starts hanging loses its traffic in latency mode before its breaker opens; aborted tries record nothing.
func TestLatencyPenalty(t *testing.T) {
	fr := newRoutes("primary", "b")
	h := newRouteHarness(t, routeLimits(), RoutingConfig{Mode: RoutingLatency, TryTimeout: time.Second}, fr)
	rs := h.c.routes[upstream.KindChat]
	rs.bal.observe(signalResponse, 0, 50*time.Millisecond)
	rs.bal.observe(signalResponse, 1, 10*time.Millisecond)
	unknown := &upstream.Error{Outcome: upstream.OutcomeUnknown, Code: upstream.CodeUpstreamUnconfirmed}
	rs.observe(leg{route: 1}, unknown, true, 2*time.Second, time.Second) // aborted: nothing
	if got := rs.bal.ewma[signalResponse][1]; got != 0.01 {
		t.Fatalf("aborted try changed the EWMA: %v", got)
	}
	rs.observe(leg{route: 1}, unknown, false, 300*time.Millisecond, time.Second)
	if got := rs.bal.ewma[signalResponse][1]; got < 0.2 { // 0.2 × 1 s + 0.8 × 0.01 s
		t.Fatalf("penalty not recorded: %v", got)
	}
	retryTimeout := &upstream.Error{Outcome: upstream.OutcomeRetryable, Code: upstream.CodeUpstreamUnreachable}
	before := rs.bal.ewma[signalResponse][1]
	rs.observe(leg{route: 1}, retryTimeout, false, time.Second, time.Second) // hit the try timeout
	if rs.bal.ewma[signalResponse][1] <= before {
		t.Fatal("timeout penalty not recorded")
	}
	for range 10 {
		rs.observe(leg{route: 1}, unknown, false, time.Second, time.Second)
	}
	if eff := rs.bal.effective([]int{0, 1}, signalResponse); eff[1] > eff[0]*LatencyFloor*1.01 {
		t.Fatalf("hanging route keeps traffic: %v", eff)
	}
}

// Breaker recovery in weighted mode (review round 1, blocker): a route that was down until its breaker opened
// gets its weighted share back once it is healthy and the open window has elapsed — the round-robin offers it
// as a half-open probe, the probe succeeds and closes the breaker.
func TestWeightedRouteRecoversAfterBreakerOpens(t *testing.T) {
	fr := newRoutes("primary", "b", "c")
	h := newRouteHarness(t, routeLimits(), RoutingConfig{Weights: map[string]int{"primary": 1, "b": 3, "c": 1}}, fr)
	h.store.setLimit("t1", 1_000_000_000)
	fr.setDown(1, true)
	for n := 0; n < 20; n++ {
		if r := h.invoke(t, inv(fmt.Sprintf("d%d", n), chatBody)); r.Status != 200 {
			t.Fatalf("call %d: %+v", n, r)
		}
	}
	if !strings.Contains(states(h.c), "b:open") {
		t.Fatalf("states %s", states(h.c))
	}
	fr.setDown(1, false)
	h.clk.advance(time.Hour)
	before := fr.calls(1)
	for n := 0; n < 100; n++ {
		if r := h.invoke(t, inv(fmt.Sprintf("r%d", n), chatBody)); r.Status != 200 {
			t.Fatalf("call %d: %+v", n, r)
		}
	}
	if got := fr.calls(1) - before; got < 55 || got > 65 {
		t.Fatalf("b got %d of 100 calls after recovery, want its share of 60", got)
	}
	if !strings.Contains(states(h.c), "b:closed") {
		t.Fatalf("states %s", states(h.c))
	}
}

// The same recovery through the model API's Plan/Next: once the window has elapsed the open route is offered
// first in its turn (half-open probe) and, after Done with success, is closed again.
func TestModelRoutingPlanRecoversOpenRoute(t *testing.T) {
	fr := newRoutes("primary", "b")
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 1, Weights: map[string]int{"primary": 1, "b": 3}}, fr)
	rs := h.c.routes[upstream.KindChat]
	m := &ModelRouting{c: h.c, rs: rs, ad: fakeChat{fr}}
	fail := &upstream.Error{Outcome: upstream.OutcomeRetryable, Code: upstream.CodeUpstreamUnavailable}
	for {
		plan := m.Plan("m", "")
		pick, ok := plan.Next()
		if !ok {
			t.Fatal("no route")
		}
		if pick.Name == "b" {
			plan.Done(pick, fail, false)
			break
		}
		plan.Done(pick, nil, false)
	}
	if rs.breakers[1].Ready() {
		t.Fatal("b not open")
	}
	h.clk.advance(time.Hour)
	picks := map[string]int{}
	for n := 0; n < 40; n++ {
		plan := m.Plan("m", "")
		pick, ok := plan.Next()
		if !ok {
			t.Fatal("no route")
		}
		picks[pick.Name]++
		plan.Done(pick, nil, false)
	}
	if picks["b"] < 25 || states(h.c) != "primary:closed,b:closed" {
		t.Fatalf("picks %v states %s", picks, states(h.c))
	}
}

// fakeChat adapts fakeRoutes to upstream.ChatAdapter (all routes OpenAI, no streaming).
type fakeChat struct{ *fakeRoutes }

func (fakeChat) RouteProtocol(int) llmproto.Protocol { return llmproto.ProtocolOpenAI }
func (fakeChat) Convertible(int, []byte) bool        { return true }
func (fakeChat) DoStream(context.Context, int, []byte, llmproto.Protocol) (upstream.Stream, *upstream.Error) {
	return nil, &upstream.Error{Outcome: upstream.OutcomeFatal}
}

// countingProto is a fakeRoutes whose route 1 is an Anthropic route; it counts Convertible calls.
type countingProto struct {
	*fakeRoutes
	mu    sync.Mutex
	calls int
}

func (c *countingProto) RouteProtocol(i int) llmproto.Protocol {
	if i == 1 {
		return llmproto.ProtocolAnthropic
	}
	return llmproto.ProtocolOpenAI
}

func (c *countingProto) Convertible(int, []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return true
}

// The conversion check of a non-OpenAI route runs once per call, not in order, pick and ready again.
func TestConvertibleCachedPerCall(t *testing.T) {
	fr := newRoutes("primary", "claude")
	fr.setDown(0, true)
	cp := &countingProto{fakeRoutes: fr}
	h := &harness{store: newFakeStore(t), ad: fr.fakeAdapter, blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{}, logs: &syncBuffer{}}
	c, err := New(Config{Store: h.store, Adapters: []upstream.Adapter{cp}, Blobs: h.blobs, Events: h.events, Limits: routeLimits(),
		Pricing: map[upstream.Kind]upstream.Pricing{upstream.KindChat: fr.prices[0]}, Routing: RoutingConfig{Mode: RoutingWeighted}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	h.c = c
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 200 {
		t.Fatalf("result %+v", r)
	}
	if cp.calls != 1 {
		t.Fatalf("Convertible called %d times in one call, want 1", cp.calls)
	}
}
