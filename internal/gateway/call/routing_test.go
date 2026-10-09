package call

import (
	"context"
	"log/slog"
	"strings"
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

// routeLimits makes backoff long (1 s) so a test can tell immediate failover from a backoff.
func routeLimits() Limits {
	return Limits{CallDeadline: 10 * time.Second, ModelCallDeadline: 10 * time.Second, BackoffBase: time.Second, BackoffMax: time.Second}
}

func newRouteHarness(t *testing.T, lim Limits, rc RoutingConfig, fr *fakeRoutes) *harness {
	t.Helper()
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
	return h
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

// A down → B serves, immediately (no backoff between providers); the try rows say which provider ran and
// why the next one was chosen; B's settlement uses B's price; the logical call (fingerprint, model) is the
// same as without fallback.
func TestRouteFailoverAToB(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	fr.legs[1].script = []step{{body: `{"answer":"from-b"}`, usage: upstream.Usage{InputTokens: 10, OutputTokens: 20}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	start := time.Now()
	r := h.invoke(t, inv("c1", chatBody))
	elapsed := time.Since(start)
	if r.Status != 200 || string(r.Body) != `{"answer":"from-b"}` {
		t.Fatalf("result %+v", r)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("failover waited %s (backoff is 1 s): should be immediate", elapsed)
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
// ("primary:circuit_open"); after the open duration one half-open probe goes to A; a failing probe re-opens
// it, a succeeding probe closes it.
func TestRouteBreakerOpensAndHalfOpens(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.setDown(0, true)
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 2, BreakerOpen: 150 * time.Millisecond}, fr)
	for _, id := range []string{"c1", "c2"} {
		if r := h.invoke(t, inv(id, chatBody)); r.Status != 200 {
			t.Fatalf("%s %+v", id, r)
		}
	}
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("after 2 failures: %s", got)
	}
	aCalls := fr.calls(0)
	start := time.Now()
	r := h.invoke(t, inv("c3", chatBody))
	if r.Status != 200 || fr.calls(0) != aCalls || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("open A must be skipped without a try: %+v, A calls %d→%d", r, aCalls, fr.calls(0))
	}
	if _, tries := h.call(t, "t1", "c3"); providers(tries) != "backup/ok[primary:circuit_open]" {
		t.Fatalf("c3 tries %s", providers(tries))
	}
	// Probe fails → open again.
	time.Sleep(160 * time.Millisecond)
	h.invoke(t, inv("c4", chatBody))
	if _, tries := h.call(t, "t1", "c4"); providers(tries) != "primary/retryable backup/ok[primary:tried]" {
		t.Fatalf("c4 (failed probe) tries %s", providers(tries))
	}
	if got := states(h.c); got != "primary:open,backup:closed" {
		t.Fatalf("after failed probe: %s", got)
	}
	// A recovers; the next probe succeeds and closes the breaker.
	fr.setDown(0, false)
	time.Sleep(160 * time.Millisecond)
	h.invoke(t, inv("c5", chatBody))
	if _, tries := h.call(t, "t1", "c5"); providers(tries) != "primary/ok" {
		t.Fatalf("c5 (probe) tries %s", providers(tries))
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
	lim := routeLimits()
	lim.BackoffBase, lim.BackoffMax = 10*time.Millisecond, 10*time.Millisecond
	h := newRouteHarness(t, lim, RoutingConfig{BreakerFailures: 1, BreakerOpen: time.Minute}, fr)
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 503 || r.Code != CodeModelDegraded {
		t.Fatalf("c1 %+v", r)
	}
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "primary/retryable backup/retryable[primary:tried]" {
		t.Fatalf("c1 tries %s", providers(tries))
	}
	before := fr.calls(0) + fr.calls(1)
	start := time.Now()
	r = h.invoke(t, inv("c2", chatBody))
	elapsed := time.Since(start)
	if r.Status != 503 || r.Code != CodeModelDegraded {
		t.Fatalf("c2 %+v", r)
	}
	if elapsed > 50*time.Millisecond || fr.calls(0)+fr.calls(1) != before {
		t.Fatalf("degraded must be fast and contact no provider: %s, calls %d→%d", elapsed, before, fr.calls(0)+fr.calls(1))
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
	// Replay without the retry header returns the persisted failure; with it, a new attempt is made (still
	// degraded here).
	if r := h.invoke(t, inv("c2", chatBody)); r.Code != CodeModelDegraded {
		t.Fatalf("replayed failure %+v", r)
	}
	retry := inv("c2", chatBody)
	retry.Retry = true
	fr.setDown(1, false)
	if r := h.invoke(t, retry); r.Code != CodeModelDegraded {
		t.Fatalf("breakers still open → degraded again: %+v", r)
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
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 1, BreakerOpen: time.Minute}, fr)
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
// and the next provider serves the call.
func TestRouteTryTimeout(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{{hang: true, sent: true}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{TryTimeout: 50 * time.Millisecond}, fr)
	start := time.Now()
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("%+v after %s", r, time.Since(start))
	}
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "primary/unknown backup/ok[primary:tried]" {
		t.Fatalf("tries %s", providers(tries))
	}
	if b := h.budget(t, "t1"); b.UnknownMicro != 100 {
		t.Fatalf("budget %+v", b)
	}
}

// A provider that does not serve the requested model is skipped as model_not_served; if none serves it
// besides open ones, the call is degraded.
func TestRouteModelNotServed(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.serves[0] = map[string]bool{"other": true}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	h.invoke(t, inv("c1", chatBody))
	if _, tries := h.call(t, "t1", "c1"); providers(tries) != "backup/ok[primary:model_not_served]" {
		t.Fatalf("tries %s", providers(tries))
	}
}

// Fatal outcomes do not fail over (the request is invalid for every provider) and count as a provider answer.
func TestRouteFatalDoesNotFailOver(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{{err: &upstream.Error{Outcome: upstream.OutcomeFatal, Status: 400, Code: upstream.CodeUpstreamRejected}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{BreakerFailures: 1}, fr)
	if r := h.invoke(t, inv("c1", chatBody)); r.Status != 400 || fr.calls(1) != 0 {
		t.Fatalf("%+v backup calls %d", r, fr.calls(1))
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("fatal must not open the breaker: %s", got)
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

// Hedging is off by default: a slow primary is simply waited for.
func TestHedgeOffByDefault(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, body: `{"answer":"slow-a"}`}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{}, fr)
	go func() { time.Sleep(100 * time.Millisecond); close(gate) }()
	if r := h.invoke(t, inv("c1", chatBody)); string(r.Body) != `{"answer":"slow-a"}` || fr.calls(1) != 0 {
		t.Fatalf("%+v backup calls %d", r, fr.calls(1))
	}
}

// Primary slow (already sent, never answers) → after HedgeDelay the backup is asked too and wins; the primary
// leg is cancelled and, because it had been sent, settled as unknown (hedge_lost) with its full estimate —
// before the winner, so the call ends completed with the backup's result.
func TestHedgeBackupWins(t *testing.T) {
	fr := newRoutes("primary", "backup")
	fr.legs[0].script = []step{{hang: true, sent: true}}
	fr.legs[1].script = []step{{body: `{"answer":"hedge-b"}`, usage: upstream.Usage{InputTokens: 1, OutputTokens: 1}}}
	h := newRouteHarness(t, routeLimits(), RoutingConfig{HedgeDelay: 30 * time.Millisecond}, fr)
	start := time.Now()
	r := h.invoke(t, inv("c1", chatBody))
	if r.Status != 200 || string(r.Body) != `{"answer":"hedge-b"}` || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("%+v after %s", r, time.Since(start))
	}
	rec, tries := h.call(t, "t1", "c1")
	if rec.State != StateCompleted || rec.ResultRef != r.BlobSHA256 || !rec.PossibleExternalDuplicate {
		t.Fatalf("journal %+v", rec)
	}
	if got := providers(tries); got != "primary/unknown backup/ok[primary:tried]+hedge" || tries[0].Error != CodeHedgeLost {
		t.Fatalf("tries %s (%q)", got, tries[0].Error)
	}
	// Loser charged conservatively: A's estimate 100 to unknown; B's actual 1×2 + 1×4 = 6 spent.
	if b := h.budget(t, "t1"); b.UnknownMicro != 100 || b.SpentMicro != 6 || b.ReservedMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
	if got := states(h.c); got != "primary:closed,backup:closed" {
		t.Fatalf("a hedge loser must not count as a provider failure: %s", got)
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
	if got := providers(tries); got != "primary/ok backup/retryable[primary:tried]+hedge" || tries[1].Error != CodeHedgeLost {
		t.Fatalf("tries %s", got)
	}
	if b := h.budget(t, "t1"); b.UnknownMicro != 0 || b.ReservedMicro != 0 {
		t.Fatalf("budget %+v", b)
	}
}

// Both legs fail: both tries are settled, both routes count as tried, and the call backs off before a new round.
func TestHedgeBothFail(t *testing.T) {
	fr := newRoutes("primary", "backup")
	gate := make(chan struct{})
	fr.legs[0].script = []step{{gate: gate, err: unavailable()}, {body: `{"answer":"round2"}`}}
	fr.legs[1].script = []step{{err: unavailable()}}
	lim := routeLimits()
	lim.BackoffBase, lim.BackoffMax = 50*time.Millisecond, 50*time.Millisecond
	h := newRouteHarness(t, lim, RoutingConfig{HedgeDelay: 10 * time.Millisecond}, fr)
	go func() {
		<-fr.legs[1].entered
		time.Sleep(10 * time.Millisecond)
		close(gate)
	}()
	r := h.invoke(t, inv("c1", chatBody))
	if string(r.Body) != `{"answer":"round2"}` {
		t.Fatalf("%+v", r)
	}
	_, tries := h.call(t, "t1", "c1")
	if got := providers(tries); got != "primary/retryable backup/retryable[primary:tried]+hedge primary/ok" {
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
