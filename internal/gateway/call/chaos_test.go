package call

// Chaos tests for the model provider chain (docs/design/2026-10-10-model-fallback-design.md §6): the real
// OpenAI-compatible chat adapter over HTTP against in-process fake upstreams (tests/e2e/fakeupstream) with
// fault injection, the Coordinator with production backoff/limits, and the in-memory journal. They measure
// the latency of a model call with and without failover; the numbers in
// docs/evidence/2026-10-10-model-fallback.md come from
//
//	AGENTBOX_CHAOS_RUNS=5 go test ./internal/gateway/call -run TestChaos -v
//
// They measure real backoff and timeouts (seconds) and assert wall-clock bounds, so they only run when
// AGENTBOX_CHAOS_RUNS is set; the same behaviours are covered deterministically by routing_test.go.
// Zero cost: no real provider is contacted.

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

const chaosLatency = 50 * time.Millisecond // normal reply latency of every fake provider

func chaosRuns() int {
	if n, err := strconv.Atoi(os.Getenv("AGENTBOX_CHAOS_RUNS")); err == nil && n > 0 {
		return n
	}
	return 1
}

func chaosServer(t *testing.T) *fakeupstream.Server {
	t.Helper()
	if os.Getenv("AGENTBOX_CHAOS_RUNS") == "" {
		t.Skip("chaos measurement: set AGENTBOX_CHAOS_RUNS=N to run")
	}
	s := fakeupstream.New()
	t.Cleanup(s.Close)
	s.SetLatency(fakeupstream.Chat, chaosLatency)
	return s
}

// deadURL is a base URL whose port refuses connections (a provider that is down hard).
func deadURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return "http://" + addr + "/v1"
}

// chaosAdapter is the production chat adapter: primary at primaryURL, fallbacks (in order) at the others.
func chaosAdapter(primaryURL string, fallbackURLs ...string) upstream.Adapter {
	d := upstream.NewDialer(upstream.DialerConfig{AllowPrivate: []string{"127.0.0.1"}})
	price := upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}
	var fbs []upstream.ChatRoute
	for i, u := range fallbackURLs {
		fbs = append(fbs, upstream.ChatRoute{Name: fmt.Sprintf("backup%d", i+1), BaseURL: u, Pricing: price,
			HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0)})
	}
	return upstream.NewChat(upstream.ChatConfig{BaseURL: primaryURL, Model: fakeupstream.Model, Pricing: price,
		MaxTokensDefault: 64, HTTP: d.HTTPClient(upstream.DefaultModelMaxBody, 0), Fallbacks: fbs})
}

// chaosHarness uses production limits (backoff 2 s doubling, 3 tries, 300 s model deadline) unless lim overrides.
func chaosHarness(t *testing.T, ad upstream.Adapter, lim Limits, rc RoutingConfig) *harness {
	t.Helper()
	h := &harness{store: newFakeStore(t), blobs: &fakeBlobs{m: map[string][]byte{}}, events: &fakeEvents{}, logs: &syncBuffer{}}
	h.store.setLimit("t1", 1_000_000_000)
	c, err := New(Config{Store: h.store, Adapters: []upstream.Adapter{ad}, Blobs: h.blobs, Events: h.events, Limits: lim,
		Pricing: map[upstream.Kind]upstream.Pricing{upstream.KindChat: {InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 2_000_000}},
		Logger:  slog.New(slog.NewJSONHandler(h.logs, nil)), Routing: rc})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	h.c = c
	return h
}

var chaosSeq int

// timed invokes one fresh logical model call and returns the result and its wall-clock latency.
func timed(t *testing.T, h *harness) (Result, time.Duration) {
	t.Helper()
	chaosSeq++
	start := time.Now()
	r := h.invoke(t, inv(fmt.Sprintf("chaos-%d", chaosSeq), `{"messages":[{"role":"user","content":"chaos"}]}`))
	return r, time.Since(start)
}

type sample struct {
	name   string
	result string
	d      []time.Duration
}

func (s sample) row() string {
	d := append([]time.Duration(nil), s.d...)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return fmt.Sprintf("| %s | %s | %d | %s | %s |", s.name, s.result, len(d), d[len(d)/2].Round(10*time.Microsecond),
		d[len(d)-1].Round(10*time.Microsecond))
}

func measure(t *testing.T, name string, runs int, h *harness, wantStatus int, wantCode string) sample {
	t.Helper()
	s := sample{name: name}
	for i := 0; i < runs; i++ {
		r, d := timed(t, h)
		if r.Status != wantStatus || r.Code != wantCode {
			t.Fatalf("%s: got %d %s, want %d %s", name, r.Status, r.Code, wantStatus, wantCode)
		}
		s.d = append(s.d, d)
		s.result = fmt.Sprint(r.Status)
		if r.Code != "" {
			s.result += " " + r.Code
		}
	}
	return s
}

func logTable(t *testing.T, rows []sample) {
	t.Helper()
	var b strings.Builder
	b.WriteString("\n| scenario | result | runs | median | max |\n|---|---|---|---|---|\n")
	for _, r := range rows {
		b.WriteString(r.row() + "\n")
	}
	t.Log(b.String())
}

// TestChaosPrimaryDown: the primary answers 503 to everything. Without a fallback the call spends its 3 tries
// with backoff and fails; with a fallback it is served by the backup right away; once the primary's breaker is
// open, the primary is not even tried.
func TestChaosPrimaryDown(t *testing.T) {
	runs := chaosRuns()
	a, b := chaosServer(t), chaosServer(t)
	a.SetAlways(fakeupstream.Chat, fakeupstream.Action{Status: 503})
	var rows []sample

	healthy := chaosHarness(t, chaosAdapter(b.ModelBaseURL()), Limits{}, RoutingConfig{})
	rows = append(rows, measure(t, "healthy single provider (baseline)", runs, healthy, 200, ""))

	single := chaosHarness(t, chaosAdapter(a.ModelBaseURL()), Limits{}, RoutingConfig{})
	rows = append(rows, measure(t, "primary 503, no fallback (before)", runs, single, 429, persistence.CodeTriesExhausted))

	chain := chaosHarness(t, chaosAdapter(a.ModelBaseURL(), b.ModelBaseURL()), Limits{}, RoutingConfig{BreakerFailures: 3})
	// The first 3 calls each fail over (and open the primary's breaker on the 3rd).
	rows = append(rows, measure(t, "primary 503, fallback, breaker closed", 3, chain, 200, ""))
	if got := states(chain.c); got != "primary:open,backup1:closed" {
		t.Fatalf("breaker after 3 failures: %s", got)
	}
	before := a.Count(fakeupstream.Chat)
	rows = append(rows, measure(t, "primary 503, fallback, breaker open", max(runs, 3), chain, 200, ""))
	if a.Count(fakeupstream.Chat) != before {
		t.Fatal("an open primary was contacted")
	}
	logTable(t, rows)
	if rows[2].d[0] > time.Second || rows[3].d[0] > time.Second {
		t.Fatalf("failover took too long: %v %v", rows[2].d, rows[3].d)
	}
}

// TestChaosPrimaryUnreachable: the primary's port refuses connections; the backup serves.
func TestChaosPrimaryUnreachable(t *testing.T) {
	b := chaosServer(t)
	dead := deadURL(t)
	chain := chaosHarness(t, chaosAdapter(dead, b.ModelBaseURL()), Limits{}, RoutingConfig{})
	rows := []sample{measure(t, "primary connection refused, fallback", max(chaosRuns(), 3), chain, 200, "")}
	logTable(t, rows)
	_, tries := chain.call(t, "t1", fmt.Sprintf("chaos-%d", chaosSeq))
	if !strings.Contains(providers(tries), "backup1/ok") {
		t.Fatalf("tries %s", providers(tries))
	}
}

// TestChaosPrimaryHangs: the primary accepts the request and never answers. Without a per-try timeout the call
// waits for the model call deadline (300 s in production; 3 s here); with --model-try-timeout 1s it fails over.
func TestChaosPrimaryHangs(t *testing.T) {
	a, b := chaosServer(t), chaosServer(t)
	a.SetAlways(fakeupstream.Chat, fakeupstream.Action{Hang: true})
	var rows []sample
	single := chaosHarness(t, chaosAdapter(a.ModelBaseURL()), Limits{ModelCallDeadline: 3 * time.Second}, RoutingConfig{})
	r, d := timed(t, single)
	if r.Status != 504 || r.Code != persistence.CodeCallDeadlineExceeded {
		t.Fatalf("hanging single provider: %+v", r)
	}
	rows = append(rows, sample{name: "primary hangs, no fallback, deadline 3 s (before)", result: fmt.Sprintf("%d %s", r.Status, r.Code), d: []time.Duration{d}})

	chain := chaosHarness(t, chaosAdapter(a.ModelBaseURL(), b.ModelBaseURL()), Limits{}, RoutingConfig{TryTimeout: time.Second})
	rows = append(rows, measure(t, "primary hangs, fallback, try timeout 1 s", max(chaosRuns(), 1), chain, 200, ""))
	logTable(t, rows)
	if rows[1].d[0] > 2*time.Second {
		t.Fatalf("failover after try timeout took %v", rows[1].d)
	}
}

// TestChaosAllDown: every provider answers 503. The first calls spend their tries and open the breakers; after
// that the Gateway answers 503 model_degraded immediately instead of retrying for seconds.
func TestChaosAllDown(t *testing.T) {
	a, b := chaosServer(t), chaosServer(t)
	a.SetAlways(fakeupstream.Chat, fakeupstream.Action{Status: 503})
	b.SetAlways(fakeupstream.Chat, fakeupstream.Action{Status: 503})
	chain := chaosHarness(t, chaosAdapter(a.ModelBaseURL(), b.ModelBaseURL()), Limits{}, RoutingConfig{BreakerFailures: 2})
	var rows []sample
	r, d := timed(t, chain)
	rows = append(rows, sample{name: "all providers 503, breakers closed (first call)", result: fmt.Sprintf("%d %s", r.Status, r.Code), d: []time.Duration{d}})
	for chain.c.RouteStates()[0].State != "open" || chain.c.RouteStates()[1].State != "open" {
		if r, _ := timed(t, chain); r.Status == 200 {
			t.Fatal("unexpected success")
		}
	}
	reqs := a.Count(fakeupstream.Chat) + b.Count(fakeupstream.Chat)
	rows = append(rows, measure(t, "all providers 503, breakers open (degraded mode)", max(chaosRuns(), 5), chain, 503, CodeModelDegraded))
	if a.Count(fakeupstream.Chat)+b.Count(fakeupstream.Chat) != reqs {
		t.Fatal("degraded mode contacted a provider")
	}
	logTable(t, rows)
	if last := rows[len(rows)-1]; last.d[0] > 100*time.Millisecond {
		t.Fatalf("degraded answer took %v", last.d)
	}
}

// TestChaosHedge: the primary is slow (1 s per reply), the backup fast. With --model-hedge-delay 200ms the
// backup's answer arrives first; the slow primary leg is cancelled and charged its estimate as unknown.
func TestChaosHedge(t *testing.T) {
	a, b := chaosServer(t), chaosServer(t)
	a.SetLatency(fakeupstream.Chat, time.Second)
	var rows []sample
	plain := chaosHarness(t, chaosAdapter(a.ModelBaseURL(), b.ModelBaseURL()), Limits{}, RoutingConfig{})
	rows = append(rows, measure(t, "slow primary (1 s), no hedging", max(chaosRuns(), 1), plain, 200, ""))
	hedged := chaosHarness(t, chaosAdapter(a.ModelBaseURL(), b.ModelBaseURL()), Limits{}, RoutingConfig{HedgeDelay: 200 * time.Millisecond})
	rows = append(rows, measure(t, "slow primary (1 s), hedge delay 200 ms", max(chaosRuns(), 1), hedged, 200, ""))
	logTable(t, rows)
	_, tries := hedged.call(t, "t1", fmt.Sprintf("chaos-%d", chaosSeq))
	if got := providers(tries); got != "primary/unknown backup1/ok[primary:tried]+hedge" {
		t.Fatalf("hedged tries %s", got)
	}
	bud := hedged.budget(t, "t1")
	t.Logf("hedged calls: spent %d µUSD, charged as unknown (cancelled primary legs) %d µUSD", bud.SpentMicro, bud.UnknownMicro)
	if bud.UnknownMicro == 0 || rows[1].d[0] > 800*time.Millisecond {
		t.Fatalf("hedge: %v, budget %+v", rows[1].d, bud)
	}
}
