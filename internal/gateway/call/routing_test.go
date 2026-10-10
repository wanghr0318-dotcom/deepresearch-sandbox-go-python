package call

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
)

// ---- model provider chain (docs/design/2026-10-10-model-fallback-design.md) ----

// fakeRoutes is a chat adapter with scripted routes: route i is legs[i] (a fakeAdapter); route 0 also
// backs Adapter.Do. serves[i] (nil = all models) restricts the logical models of route i.
type fakeRoutes struct {
	*fakeAdapter
	names  []string
	legs   []*fakeAdapter
	serves []map[string]bool
	prices []upstream.Pricing
}

func newRoutes(names ...string) *fakeRoutes {
	f := &fakeRoutes{names: names}
	for i := range names {
		a := newAdapter("openai_compat")
		a.est = int64(100 * (i + 1)) // route i reserves 100·(i+1)
		f.legs = append(f.legs, a)
		f.serves = append(f.serves, nil)
		f.prices = append(f.prices, upstream.Pricing{InputMicroPerMTok: int64(1_000_000 * (i + 1)), OutputMicroPerMTok: int64(2_000_000 * (i + 1))})
	}
	f.fakeAdapter = f.legs[0]
	return f
}

func (f *fakeRoutes) Routes() []string { return f.names }
func (f *fakeRoutes) Serves(i int, model string) bool {
	return i >= 0 && i < len(f.names) && (f.serves[i] == nil || f.serves[i][model])
}
func (f *fakeRoutes) EstimateOn(i int, _ []byte) (int64, error) { return f.legs[i].est, nil }
func (f *fakeRoutes) DoOn(ctx context.Context, i int, b []byte) (upstream.Response, *upstream.Error) {
	return f.legs[i].Do(ctx, b)
}
func (f *fakeRoutes) PricingOn(i int, _ string) upstream.Pricing { return f.prices[i] }

func (f *fakeRoutes) setDown(i int, down bool) {
	f.legs[i].mu.Lock()
	f.legs[i].down = down
	f.legs[i].mu.Unlock()
}

func (f *fakeRoutes) calls(i int) int {
	n, _, _ := f.legs[i].stats()
	return n
}

// routeLimits makes any backoff impossible: a backoff of 1 h would exceed the 10 s call deadline, so the
// Coordinator gives up with call_deadline_exceeded instead of waiting. A successful failover under these limits
// therefore proves it happened without backoff — deterministically, without wall-clock assertions.
func routeLimits() Limits {
	return Limits{CallDeadline: 10 * time.Second, ModelCallDeadline: 10 * time.Second, BackoffBase: time.Hour, BackoffMax: time.Hour}
}

// shortBackoff is for tests that need a real round of backoff.
func shortBackoff() Limits {
	lim := routeLimits()
	lim.BackoffBase, lim.BackoffMax = time.Millisecond, time.Millisecond
	return lim
}

// testClock is the breakers' injected clock (RoutingConfig.breakerNow); deadlines still use real time.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type routeHarness struct {
	*harness
	clk *testClock
}

func newRouteHarness(t *testing.T, lim Limits, rc RoutingConfig, fr *fakeRoutes) routeHarness {
	t.Helper()
	clk := &testClock{t: time.Unix(1_000_000, 0)}
	rc.breakerNow = clk.now
	h := &harness{store: newFakeStore(t), ad: fr.fakeAdapter, blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{},
		logs: &syncBuffer{}}
	c, err := New(Config{
		Store: h.store, Adapters: []upstream.Adapter{fr}, Blobs: h.blobs, Events: h.events, Limits: lim,
		Pricing: map[upstream.Kind]upstream.Pricing{upstream.KindChat: fr.prices[0]},
		Logger:  slog.New(slog.NewJSONHandler(h.logs, nil)), Routing: rc,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	h.c = c
	return routeHarness{harness: h, clk: clk}
}

func providers(tries []TryRecord) string {
	var parts []string
	for _, tr := range tries {
		p := tr.Provider + "/" + tr.Outcome
		if tr.Skipped != "" {
			p += "[" + tr.Skipped + "]"
		}
		if tr.Hedge {
			p += "+hedge"
		}
		if tr.HedgeLost {
			p += "+lost"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, " ")
}

func states(c *Coordinator) string {
	var parts []string
	for _, s := range c.RouteStates() {
		parts = append(parts, s.Route+":"+s.State)
	}
	return strings.Join(parts, ",")
}

// A down → B serves without backoff (routeLimits makes any backoff fail the call); the try rows say which
// provider ran and why the next one was chosen; B's settlement uses B's price; the logical call (fingerprint,
// model) is the same as without fallback.
func TestRouteFailoverAToB(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	fr.legs[1].script = []step{{body: `{"answer":"from-b"}`, usage: upstream.Usage{InputTokens: 10, OutputTokens: 20}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"from-b"}` {
		t.Fatalf("result %+v (a backoff would have been call_deadline_exceeded)", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if got := providers(tries); got != "primary/retryable backup/ok[primary:tried]" {
		t.Fatalf("tries %s", got)
	}
	if tries[0].Error != upstream.CodeUpstreamUnavailable {
		t.Fatalf("primary try error %q", tries[0].Error)
	}
	// B's price: 10 × 2 + 20 × 4 = 100 micro-USD; reservations were 100 (A) and 200 (B), all released/settled.
	if b := h.budget(t, "t1"); b.SpentMicro != 100 || b.ReservedMicro != 0 || b.UnknownMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	// The fingerprint is that of the logical call: identical to the same request on a single provider.
	single := newHarness(t, testLimits(), newAdapter("openai_compat"))
	single.invoke(t, inv("c1", chatBody))
	if srec, _ := single.call(t, "t1", "c1"); srec.Fingerprint != rec.Fingerprint || srec.Model != rec.Model {
		t.Fatalf("fingerprint %s vs single %s", rec.Fingerprint, srec.Fingerprint)
	}
	if !strings.Contains(h.logs.String(), `"route":"backup"`) || !strings.Contains(h.logs.String(), `"route_skipped":"primary:tried"`) {
		t.Fatalf("log lacks route attributes: %s", h.logs.String())
	}
}

// A flapping: consecutive failures open A's breaker; while open A is skipped without a try
// ("primary:circuit_open"); after the open duration (fake clock) one half-open probe goes to A; a failing probe
// re-opens it, a succeeding probe closes it.
func TestRouteBreakerOpensAndHalfOpens(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 2, BreakerOpen: time.Minute}, fr)
	for _, id := range []string{"c1", "c2"} {
		if r := h.invoke(t, inv(id, chatBody)); r.Status != 200 {
			t.Fatalf("%s %+v", id, r)
		}
	}
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("after 2 failures: %s", got)
	}
	aCalls := fr.calls(0)
	if r := h.invoke(t, inv("c3", chatBody)); r.Status != 200 || fr.calls(0) != aCalls {
		t.Fatalf("open A must be skipped without a try: %+v, A calls %d→%d", r, aCalls, fr.calls(0))
	}
	if _, tries := h.call(t, "t1", "c3"); providers(tries) != "backup/ok[primary:circuit_open]" {
		t.Fatalf("c3 tries %s", providers(tries))
	}
	// Probe fails → open again.
	h.clk.advance(time.Minute)
	h.invoke(t, inv("c4", chatBody))
	if _, tries := h.call(t, "t1", "c4"); providers(tries) != "primary/retryable backup/ok[primary:tried]" {
		t.Fatalf("c4 (failed probe) tries %s", providers(tries))
	}
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("after failed probe: %s", got)
	}
	// Still open just before the duration elapses again.
	h.clk.advance(time.Minute - time.Second)
	h.invoke(t, inv("c5", chatBody))
	if _, tries := h.call(t, "t1", "c5"); providers(tries) != "backup/ok[primary:circuit_open]" {
		t.Fatalf("c5 tries %s", providers(tries))
	}
	// A recovers; the next probe succeeds and closes the breaker.
	fr.setDown(0, false)
	h.clk.advance(time.Second)
	h.invoke(t, inv("c6", chatBody))
	if _, tries := h.call(t, "t1", "c6"); providers(tries) != "primary/ok" {
		t.Fatalf("c6 (probe) tries %s", providers(tries))
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("after successful probe: %s", got)
	}
	logs := h.logs.String()
	for _, want := range []string{`"from":"closed","to":"open"`, `"from":"open","to":"half_open"`, `"from":"half_open","to":"open"`, `"from":"half_open","to":"closed"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("breaker transition %s not logged", want)
		}
	}
}

// All providers down: the first call spends its tries (A, B, backoff, …) and opens both breakers; it ends
// with 503 model_degraded as soon as no provider is allowed. The next call fails fast with model_degraded:
// no try, no reservation, no upstream request. X-Agentbox-Retry is allowed (the breakers recover).
func TestRouteAllDownDegraded(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	fr.setDown(1, true)
	h := newRouteHarness(t, shortBackoff(), RoutingConfig{BreakerFailures: 1, BreakerOpen: time.Minute}, fr)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 503 || r.Code != CodeModelDegraded {
		t.Fatalf("c1 %+v", r)
	}
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "primary/retryable backup/retryable[primary:tried]" {
		t.Fatalf("c1 tries %s", providers(tries))
	}
	before := fr.calls(0) + fr.calls(1)
	r = h.invoke(t, inv("c2", chatBody))
	if r.Status != 503 || r.Code != CodeModelDegraded || fr.calls(0)+fr.calls(1) != before {
		t.Fatalf("c2 %+v, provider calls %d→%d", r, before, fr.calls(0)+fr.calls(1))
	}
	rec, tries := h.call(t, "t1", "c2")
	if rec.State != StateFailed || rec.FailReason != CodeModelDegraded || len(tries) != 0 {
		t.Fatalf("c2 journal %+v tries %d", rec, len(tries))
	}
	if b := h.budget(t, "t1"); b.ReservedMicro != 0 || b.SpentMicro != 0 || b.UnknownMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	if !retryableReason(CodeModelDegraded) || statusFor(CodeModelDegraded) != 503 {
		t.Fatal("model_degraded must be a retryable 503")
	}
	if r := h.invoke(t, inv("c2", chatBody)); r.Code != CodeModelDegraded {
		t.Fatalf("replayed failure %+v", r)
	}
	retry := inv("c2", chatBody)
	retry.Retry = true
	fr.setDown(1, false)
	if r := h.invoke(t, retry); r.Code != CodeModelDegraded {
		t.Fatalf("breakers still open → degraded again: %+v", r)
	}
	// After the open duration the retry probes the recovered backup and succeeds.
	h.clk.advance(time.Minute)
	if r := h.invoke(t, retry); r.Status != 200 {
		t.Fatalf("retry after recovery %+v", r)
	}
	if !strings.Contains(h.logs.String(), `"msg":"gateway: degraded"`) {
		t.Fatal("degraded not logged")
	}
}

// A completed call is replayed from the journal regardless of which provider served it or of the current
// breaker state: no provider is contacted.
func TestRouteReplayDoesNotReexecute(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	h := newRouteHarness(t, shortBackoff(), RoutingConfig{BreakerFailures: 1, BreakerOpen: time.Minute}, fr)
	first := h.invoke(t, inv("c1", chatBody))
	fr.setDown(1, true) // now every provider is down / open
	h.invoke(t, inv("c2", chatBody))
	calls := fr.calls(0) + fr.calls(1)
	again := h.invoke(t, inv("c1", chatBody))
	if !again.Replayed || again.BlobSHA256 != first.BlobSHA256 || fr.calls(0)+fr.calls(1) != calls {
		t.Fatalf("replay %+v (calls %d→%d)", again, calls, fr.calls(0)+fr.calls(1))
	}
}

// Crash mid-fallback: A failed, the try on B was sent and the server died before settling it. The restart's
// ledger conversion charges B's reservation as unknown and leaves the call unknown. The Worker re-sends the
// same call id to the new process: a new try is made (fresh breakers: A again, then B), the call completes,
// and later replays return the journaled result without re-execution. The dead process's late settlement
// changes nothing.
func TestRouteCrashMidFallbackReplay(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	gate := make(chan struct{})
	fr.legs[1].script = []step{{gate: gate, hold: true, body: `{"answer":"late"}`}, {body: `{"answer":"after-restart"}`}}
	lim := routeLimits()
	lim.MaxTries = 5
	h := newRouteHarness(t, lim, RoutingConfig{}, fr)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = h.c.Invoke(ctx, inv("c1", chatBody)) }()
	waitEntered(t, fr.legs[1])
	cancel()
	// "Crash": the startup ledger conversion (postgres.ConvertLedger) on the shared store.
	h.store.convertLedger()
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateUnknown || providers(tries) != "primary/retryable backup/unknown[primary:tried]" {
		t.Fatalf("after conversion %+v %s", rec, providers(tries))
	}
	// New process on the same store.
	old := h.c
	h2 := &harness{store: h.store, blobs: h.blobs, events: h.events, logs: &syncBuffer{}}
	c2, err := New(Config{Store: h.store, Adapters: []upstream.Adapter{fr}, Blobs: h.blobs, Events: h.events, Limits: lim,
		Pricing: map[upstream.Kind]upstream.Pricing{upstream.KindChat: fr.prices[0]}, Logger: slog.New(slog.NewJSONHandler(h2.logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c2.Close)
	h2.c = c2
	r := h2.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"after-restart"}` {
		t.Fatalf("after restart %+v", r)
	}
	rec, tries = h2.call(t, "t1", "c1")
	if rec.State != StateCompleted || providers(tries) != "primary/retryable backup/unknown[primary:tried] primary/retryable backup/ok[primary:tried]" {
		t.Fatalf("journal %+v %s", rec, providers(tries))
	}
	calls := fr.calls(0) + fr.calls(1)
	again := h2.invoke(t, inv("c1", chatBody))
	if !again.Replayed || again.BlobSHA256 != r.BlobSHA256 || fr.calls(0)+fr.calls(1) != calls {
		t.Fatalf("replay %+v", again)
	}
	close(gate) // the dead process's try returns late: its settlement is idempotent and changes nothing
	old.Close()
	if rec2, _ := h2.call(t, "t1", "c1"); rec2.State != StateCompleted || rec2.ResultRef != r.BlobSHA256 {
		t.Fatalf("late settlement changed the journal: %+v", rec2)
	}
	if b := h2.budget(t, "t1"); b.ReservedMicro != 0 || b.UnknownMicro != 200 {
		t.Fatalf("budget %+v (B's estimate 200 charged as unknown once)", b)
	}
}

// Per-try timeout: a hanging provider is abandoned after TryTimeout (sent → unknown, its estimate charged)
// and the next provider serves the call without backoff.
func TestRouteTryTimeout(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{{hang: true, sent: true}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{TryTimeout: 50 * time.Millisecond}, fr)
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 200 {
		t.Fatalf("%+v", r)
	}
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "primary/unknown backup/ok[primary:tried]" {
		t.Fatalf("tries %s", providers(tries))
	}
	if b := h.budget(t, "t1"); b.UnknownMicro != 100 {
		t.Fatalf("budget %+v", b)
	}
}

// A provider that does not serve the requested model is skipped as model_not_served.
func TestRouteModelNotServed(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.serves[0] = map[string]bool{"other": true}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	h.invoke(t, inv("c1", chatBody))
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "backup/ok[primary:model_not_served]" {
		t.Fatalf("tries %s", providers(tries))
	}
}

// Fatal outcomes do not fail over. A request rejection (400) counts as a provider answer; a provider-side fault
// (401/403/404, egress blocked) counts as a breaker failure.
func TestRouteFatalDoesNotFailOver(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{
		{err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 400, Code: upstream.CodeUpstreamRejected}},
		{err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 401, Code: upstream.CodeUpstreamRejected}},
	}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 1}, fr)
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 400 || fr.calls(1) != 0 {
		t.Fatalf("%+v backup calls %d", r, fr.calls(1))
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("400 must not open the breaker: %s", got)
	}
	if r := h.invoke(t, inv("c2", chatBody)); r.Status != 401 || fr.calls(1) != 0 {
		t.Fatalf("%+v backup calls %d", r, fr.calls(1))
	}
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("401 must count as a provider failure: %s", got)
	}
}

// Single-provider configuration is unchanged: no routes, no breaker, no provider recorded; a dead provider is
// retried with backoff until tries are exhausted (never model_degraded).
func TestSingleProviderUnchanged(t *testing.T) {
	ad := newAdapter("openai_compat")
	ad.down = true
	h := newHarness(t, testLimits(), ad)
	if len(h.c.RouteStates()) != 0 {
		t.Fatal("single provider must not have routes")
	}
	r := h.invoke(t, inv("c1", chatBody))
	if r.Code != persistence.CodeTriesExhausted {
		t.Fatalf("%+v", r)
	}
	_, tries := h.call(t, "t1", "c1")
	if len(tries) != 3 || providers(tries) != "/retryable /retryable /retryable" {
		t.Fatalf("tries %s", providers(tries))
	}
	if strings.Contains(h.logs.String(), `"route"`) {
		t.Fatal("single-provider logs must not carry route attributes")
	}
}

// ---- hedging ----

// afterLeg closes gate once, right after the first result of a leg of the given kind (hedge or first leg) has
// been delivered to the hedge's result channel. Results are read in delivery order, so a leg gated this way is
// collected after that leg — without sleeps.
func afterLeg(h routeHarness, hedge bool, gate chan struct{}) {
	var once sync.Once
	h.c.legDone = func(_ int, isHedge bool) {
		if isHedge == hedge {
			once.Do(func() { close(gate) })
		}
	}
}

// Hedging is off by default: a slow primary is simply waited for.
func TestHedgeOffByDefault(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, body: `{"answer":"slow-a"}`}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	go func() { time.Sleep(50 * time.Millisecond); close(gate) }()
	if r := h.invoke(t, inv("c1", chatBody)); string(r.Body) != `{"answer":"slow-a"}` || fr.calls(1) != 0 {
		t.Fatalf("%+v backup calls %d", r, fr.calls(1))
	}
}

// Primary slow (already sent, never answers) → after HedgeDelay the backup is asked too and wins. The primary
// leg is cancelled and, because it had been sent, charged its full estimate as unknown with its own error code
// and hedge_lost; it is settled charge-only before the winner, so the call ends completed with the backup's
// result. The primary ran longer than HedgeDelay without a result: that is a (slow) failure for its breaker,
// so a persistently hanging primary is eventually skipped.
func TestHedgeBackupWinsAndSlowPrimaryTripsBreaker(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{{hang: true, sent: true}, {hang: true, sent: true}}
	fr.legs[1].script = []step{{body: `{"answer":"hedge-b"}`, usage: upstream.Usage{InputTokens: 1, OutputTokens: 1}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 20 * time.Millisecond, BreakerFailures: 2}, fr)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"hedge-b"}` {
		t.Fatalf("%+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.ResultRef != r.BlobSHA256 || !rec.PossibleExternalDuplicate {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/unknown+lost backup/ok[primary:tried]+hedge" || tries[0].Error != upstream.CodeUpstreamUnconfirmed {
		t.Fatalf("tries %s (%q)", got, tries[0].Error)
	}
	// Loser charged conservatively: A's estimate 100 to unknown; B's actual 1×2 + 1×4 = 6 spent.
	if b := h.budget(t, "t1"); b.UnknownMicro != 100 || b.SpentMicro != 6 || b.ReservedMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("one slow failure: %s", got)
	}
	h.invoke(t, inv("c2", chatBody))
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("two slow failures must open the primary: %s", got)
	}
	h.invoke(t, inv("c3", chatBody))
	if _, tries := h.call(t, "t1", "c3"); providers(tries) != "backup/ok[primary:circuit_open]" {
		t.Fatalf("c3 tries %s", providers(tries))
	}
}

// Primary answers after the hedge started but before the backup: the primary wins; the backup leg (not yet
// sent) is cancelled and released at no cost.
func TestHedgePrimaryWins(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, body: `{"answer":"a"}`}}
	fr.legs[1].script = []step{{hang: true, sent: false}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 20 * time.Millisecond}, fr)
	go func() {
		<-fr.legs[1].entered // the hedge leg has started
		close(gate)
	}()
	r := h.invoke(t, inv("c1", chatBody))
	if string(r.Body) != `{"answer":"a"}` {
		t.Fatalf("%+v", r)
	}
	_, tries := h.call(t, "t1", "c1")
	if got := providers(tries); got != "primary/ok backup/retryable[primary:tried]+hedge+lost" || tries[1].Error != upstream.CodeUpstreamUnreachable {
		t.Fatalf("tries %s (%q)", got, tries[1].Error)
	}
	if b := h.budget(t, "t1"); b.UnknownMicro != 0 || b.ReservedMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("a cancelled hedge leg is not a failure: %s", got)
	}
}

// Both legs return ok (the loser finished before the cancellation took effect): the loser's result is stored
// and its cost charged, but it never becomes the call's result — the call completes once, with the winner's
// blob and upstream request id.
func TestHedgeBothOK(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, hold: true, body: `{"answer":"late-a"}`, usage: upstream.Usage{InputTokens: 1, OutputTokens: 1}}}
	fr.legs[1].script = []step{{body: `{"answer":"fast-b"}`, usage: upstream.Usage{InputTokens: 1, OutputTokens: 1}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 10 * time.Millisecond, BreakerFailures: 1}, fr)
	afterLeg(h, true, gate) // A answers ok only after B's ok was delivered (A ignores the cancellation)
	r := h.invoke(t, inv("c1", chatBody))
	if string(r.Body) != `{"answer":"fast-b"}` {
		t.Fatalf("%+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.ResultRef != r.BlobSHA256 || rec.FailReason != "" {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/ok+lost backup/ok[primary:tried]+hedge" {
		t.Fatalf("tries %s", got)
	}
	// Both costs are real: A 1×1+1×2 = 3, B 1×2+1×4 = 6.
	if b := h.budget(t, "t1"); b.SpentMicro != 9 || b.ReservedMicro != 0 || rec.CostCharged != 9 {
		t.Fatalf("budget %+v cost_charged %d", b, rec.CostCharged)
	}
	// The losing primary answered ok: it is healthy, so it is not a slow failure (BreakerFailures 1 would open it).
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("breakers %s", got)
	}
}

// A fatal answer from the hedge (backup) leg — e.g. a wrong backup key — is not decisive: the slow but healthy
// primary is still waited for and wins. The backup's fatal is settled charge-only (the call never becomes
// failed) and counts against the backup's breaker.
func TestHedgeFatalFromHedgeLegNotDecisive(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, body: `{"answer":"a"}`}}
	fr.legs[1].script = []step{{err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 401, Code: upstream.CodeUpstreamRejected}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 10 * time.Millisecond, BreakerFailures: 1}, fr)
	afterLeg(h, true, gate) // A answers after B's fatal was delivered
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"a"}` {
		t.Fatalf("%+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.FailReason != "" {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/ok backup/fatal[primary:tried]+hedge" {
		t.Fatalf("tries %s", got)
	}
	if got := states(h.c); got != "primary:closed,backup:open" {
		t.Fatalf("breakers %s", got)
	}
}

// The first leg's fatal arrives first, but the hedge leg answers ok while being cancelled: the ok is the final
// result. The fatal leg is settled charge-only, so the call is never failed, not even between the settlements.
func TestHedgeFatalFirstThenOK(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gateA, gateB := make(chan struct{}), make(chan struct{})
	fr.legs[0].script = []step{{gate: gateA, err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 400, Code: upstream.CodeUpstreamRejected}}}
	fr.legs[1].script = []step{{gate: gateB, hold: true, body: `{"answer":"b"}`}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 10 * time.Millisecond}, fr)
	afterLeg(h, false, gateB) // B answers ok (ignoring its cancellation) only after A's fatal was delivered
	go func() {
		<-fr.legs[1].entered
		close(gateA)
	}()
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"b"}` {
		t.Fatalf("%+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.FailReason != "" || rec.ResultRef != r.BlobSHA256 {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/fatal backup/ok[primary:tried]+hedge" {
		t.Fatalf("tries %s", got)
	}
}

// Both legs fail: both tries are settled, both routes count as tried, and the call backs off before a new round.
func TestHedgeBothFail(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, err: unavailable()}, {body: `{"answer":"round2"}`}}
	fr.legs[1].script = []step{{err: unavailable()}}
	h := newRouteHarness(t, shortBackoff(), RoutingConfig{HedgeDelay: 10 * time.Millisecond}, fr)
	afterLeg(h, true, gate) // A fails after B's failure was delivered
	r := h.invoke(t, inv("c1", chatBody))
	if string(r.Body) != `{"answer":"round2"}` {
		t.Fatalf("%+v", r)
	}
	_, tries := h.call(t, "t1", "c1")
	if got := providers(tries); got != "primary/retryable backup/retryable[primary:tried]+hedge primary/ok" {
		t.Fatalf("tries %s", got)
	}
}

// The first leg fails retryably and the hedge (backup) leg then fails with a provider fault (wrong backup key):
// the backup's misconfiguration must not end the call as fatal. The first leg's retryable result is final, so the
// call backs off and is served by the primary in the next round.
func TestHedgeFirstLegRetryableBeatsBackupProviderFault(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gateA, gateB := make(chan struct{}), make(chan struct{})
	fr.legs[0].script = []step{{gate: gateA, err: unavailable()}, {body: `{"answer":"round2"}`}}
	fr.legs[1].script = []step{{gate: gateB, hold: true, err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 401, Code: upstream.CodeUpstreamRejected}}}
	h := newRouteHarness(t, shortBackoff(), RoutingConfig{HedgeDelay: 10 * time.Millisecond}, fr)
	afterLeg(h, false, gateB) // B's 401 arrives after A's retryable failure was delivered
	go func() {
		<-fr.legs[1].entered
		close(gateA)
	}()
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"round2"}` {
		t.Fatalf("%+v", r)
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/retryable backup/fatal[primary:tried]+hedge primary/ok" {
		t.Fatalf("tries %s", got)
	}
}

// convertLedger is the fake equivalent of postgres.ConvertLedger (startup after a crash): every held
// reservation → charged_unknown (try settled unknown, error server_restart), every in_flight call → unknown
// (possible_external_duplicate when it held money).
func (s *fakeStore) convertLedger() {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.checkI3()
	for k, ts := range s.tries {
		held := false
		for _, ft := range ts {
			if ft.rstate != "held" {
				continue
			}
			b := s.budget(k.taskID)
			b.ReservedMicro -= ft.amount
			b.UnknownMicro += ft.amount
			ft.rstate = "charged_unknown"
			ft.rec.State, ft.rec.Outcome, ft.rec.Error = "settled", "unknown", "server_restart"
			held = true
		}
		if rec := s.calls[k]; rec != nil && rec.State == StateInFlight {
			rec.State = StateUnknown
			rec.PossibleExternalDuplicate = rec.PossibleExternalDuplicate || held
		}
	}
}
