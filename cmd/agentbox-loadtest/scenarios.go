package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// config holds the flags every scenario shares.
type config struct {
	addr, tokenFile, dsn       string
	fakeOpenAI, fakeAnthropic  string // direct baseline: fakeupstream model_base_url (…/v1) and root url
	modelOA, modelAN, fakeName string // logical models served by the OpenAI / Anthropic provider; model sent directly
	modelSlow                  string // budget: logical model whose provider sends events slowly (abandoned streams)
	maxTokens, promptBytes     int
	seed                       uint64
	prefix                     string
	stdout                     io.Writer
}

// path is a client format → provider format combination.
type path struct{ client, provider Protocol }

func (p path) String() string { return short(p.client) + ">" + short(p.provider) }

func short(p Protocol) string {
	if p == Anthropic {
		return "an"
	}
	return "oa"
}

func parsePaths(s string) ([]path, error) {
	var out []path
	for _, item := range strings.Split(s, ",") {
		c, p, ok := strings.Cut(strings.TrimSpace(item), ">")
		pc, okc := protoOf(c)
		pp, okp := protoOf(p)
		if !ok || !okc || !okp {
			return nil, fmt.Errorf("path %q: want client>provider with oa or an, e.g. oa>an", item)
		}
		out = append(out, path{pc, pp})
	}
	return out, nil
}

func protoOf(s string) (Protocol, bool) {
	switch s {
	case "oa":
		return OpenAI, true
	case "an":
		return Anthropic, true
	}
	return "", false
}

// model is the logical model routed to the provider format p.
func (c config) model(p Protocol) string {
	if p == Anthropic {
		return c.modelAN
	}
	return c.modelOA
}

// prompt is a deterministic user message of about promptBytes bytes.
func (c config) prompt(w, i int) string {
	s := fmt.Sprintf("load test request %d/%d: ", w, i)
	if n := c.promptBytes - len(s); n > 0 {
		s += strings.Repeat("abcdefghij ", n/11+1)[:n]
	}
	return s
}

func (c config) spec(p path, stream bool, key string, w, i int) reqSpec {
	return reqSpec{Proto: p.client, Model: c.model(p.provider), Stream: stream, MaxTokens: c.maxTokens, Prompt: c.prompt(w, i), Key: key}
}

func (c config) printf(format string, a ...any) { _, _ = fmt.Fprintf(c.stdout, format, a...) }

func runner(hc *http.Client, t target) func(ctx context.Context, s reqSpec) result {
	return func(ctx context.Context, s reqSpec) result { return send(ctx, hc, t, s) }
}

func streamModes(s string) ([]bool, error) {
	switch s {
	case "off":
		return []bool{false}, nil
	case "on":
		return []bool{true}, nil
	case "both":
		return []bool{false, true}, nil
	}
	return nil, fmt.Errorf("--stream %q: want off, on or both", s)
}

// ---- latency: gateway overhead and TTFT overhead ----

type latencyRow struct {
	Path     string  `json:"path"`
	Stream   bool    `json:"stream"`
	Gateway  summary `json:"gateway"`
	Direct   summary `json:"direct"`
	OverP50  float64 `json:"overhead_p50_ms"`
	OverP95  float64 `json:"overhead_p95_ms"`
	OverP99  float64 `json:"overhead_p99_ms"`
	TTFTOver float64 `json:"ttft_overhead_p50_ms,omitempty"`
	TTFTP99  float64 `json:"ttft_overhead_p99_ms,omitempty"`
	XactsReq float64 `json:"db_transactions_per_request"`
}

// dbXacts reads the committed + rolled-back transaction count of the database (pg_stat_database).
func dbXacts(ctx context.Context, conn *pgx.Conn) (int64, error) {
	var n int64
	err := conn.QueryRow(ctx, `SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database()`).Scan(&n)
	return n, err
}

// dbRTT measures n round trips of SELECT 1 on conn.
func dbRTT(ctx context.Context, conn *pgx.Conn, n int) (dist, error) {
	ds := make([]time.Duration, 0, n)
	for range n {
		t := time.Now()
		if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
			return dist{}, err
		}
		ds = append(ds, time.Since(t))
	}
	return distOf(ds), nil
}

// latency measures each path through the gateway and the same request sent straight to the fake provider in the
// provider's format; the difference of the percentiles is the gateway's overhead.
func latency(ctx context.Context, c config, e env, conc int, dur, warmup time.Duration, paths []path, modes []bool, lim limits) (any, error) {
	f, err := e.newFixture(ctx, c.prefix, 1, 1)
	if err != nil {
		return nil, err
	}
	if err := e.setAllLimits(ctx, f, lim.RPM, lim.TPM); err != nil {
		return nil, err
	}
	key := f.Orgs[0].Users[0].Key
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rtt, err := dbRTT(ctx, conn, 500)
	if err != nil {
		return nil, err
	}
	hc := newHTTPClient(conc)
	gw, direct := runner(hc, gatewayTarget(c.addr)), runner(hc, directTarget(c.fakeOpenAI, c.fakeAnthropic))
	baselines := map[string]summary{}
	var rows []latencyRow
	for _, p := range paths {
		for _, stream := range modes {
			g := func(w, i int) reqSpec { return c.spec(p, stream, key, w, i) }
			closedLoop(ctx, conc, warmup, gw, g)
			time.Sleep(1500 * time.Millisecond) // pg_stat_database is flushed about once a second
			x0, err := dbXacts(ctx, conn)
			if err != nil {
				return nil, err
			}
			rs, el := closedLoop(ctx, conc, dur, gw, g)
			time.Sleep(1500 * time.Millisecond)
			x1, err := dbXacts(ctx, conn)
			if err != nil {
				return nil, err
			}
			row := latencyRow{Path: p.String(), Stream: stream, Gateway: summarize(rs, el)}
			if len(rs) > 0 {
				row.XactsReq = round3(float64(x1-x0-1) / float64(len(rs)))
			}
			bk := fmt.Sprintf("%s/%v", p.provider, stream)
			base, ok := baselines[bk]
			if !ok {
				dg := func(w, i int) reqSpec {
					return reqSpec{Proto: p.provider, Model: c.fakeName, Stream: stream, MaxTokens: c.maxTokens, Prompt: c.prompt(w, i)}
				}
				closedLoop(ctx, conc, warmup, direct, dg)
				drs, del := closedLoop(ctx, conc, dur, direct, dg)
				base = summarize(drs, del)
				baselines[bk] = base
			}
			row.Direct = base
			row.OverP50 = round3(row.Gateway.Total.P50 - base.Total.P50)
			row.OverP95 = round3(row.Gateway.Total.P95 - base.Total.P95)
			row.OverP99 = round3(row.Gateway.Total.P99 - base.Total.P99)
			if stream {
				row.TTFTOver = round3(row.Gateway.TTFT.P50 - base.TTFT.P50)
				row.TTFTP99 = round3(row.Gateway.TTFT.P99 - base.TTFT.P99)
			}
			rows = append(rows, row)
		}
	}
	var t [][]string
	for _, r := range rows {
		ttft := "—"
		if r.Stream {
			ttft = fmt.Sprintf("%s / %s (direct %s / %s)", f2(r.Gateway.TTFT.P50), f2(r.Gateway.TTFT.P99), f2(r.Direct.TTFT.P50), f2(r.Direct.TTFT.P99))
		}
		t = append(t, []string{r.Path, strconv.FormatBool(r.Stream), strconv.Itoa(r.Gateway.Requests), f1(r.Gateway.RPS),
			f2(r.Gateway.Total.P50), f2(r.Gateway.Total.P95), f2(r.Gateway.Total.P99),
			f2(r.Direct.Total.P50), f2(r.Direct.Total.P99), f2(r.OverP50), f2(r.OverP95), f2(r.OverP99), ttft, f1(r.XactsReq), r.Gateway.outcomesString()})
	}
	c.printf("latency: concurrency %d, %s per row (warm-up %s), max_tokens %d, prompt %d B; %s; database round trip (SELECT 1) p50 %.3f ms, p99 %.3f ms\n",
		conc, dur, warmup, c.maxTokens, c.promptBytes, lim, rtt.P50, rtt.P99)
	c.printf("%s", table([]string{"path", "stream", "n", "rps", "gw p50", "gw p95", "gw p99", "direct p50", "direct p99",
		"overhead p50", "overhead p95", "overhead p99", "TTFT p50 / p99 ms", "db tx/req", "outcomes"}, t))
	return map[string]any{"concurrency": conc, "seconds_per_row": dur.Seconds(), "limits": lim, "db_rtt": rtt, "rows": rows}, nil
}

// limits are the rpm and tpm set at the org, user and key level of every fixture key (0 = not set).
type limits struct {
	RPM int64 `json:"rpm"`
	TPM int64 `json:"tpm"`
}

func (l limits) String() string {
	if l.RPM <= 0 && l.TPM <= 0 {
		return "no rpm/tpm limits (the rate limiter is not on the path; limits are still read from PostgreSQL)"
	}
	return fmt.Sprintf("rpm %d, tpm %d at org, user and key (every request takes from the Redis Lua buckets; none is refused)", l.RPM, l.TPM)
}

// ---- ladder: maximum requests per second ----

type ladderRow struct {
	Concurrency int     `json:"concurrency"`
	Summary     summary `json:"summary"`
}

// ladder raises the concurrency step by step until the error rate passes 1 % or the throughput stops growing
// (less than +5 % over the best level for two levels in a row: the knee).
func ladder(ctx context.Context, c config, e env, levels []int, step time.Duration, p path, stream bool, orgs int, lim limits) (any, error) {
	f, err := e.newFixture(ctx, c.prefix, orgs, 1)
	if err != nil {
		return nil, err
	}
	if err := e.setAllLimits(ctx, f, lim.RPM, lim.TPM); err != nil {
		return nil, err
	}
	keys := f.keys()
	top := 0
	for _, l := range levels {
		top = max(top, l)
	}
	hc := newHTTPClient(top)
	gw := runner(hc, gatewayTarget(c.addr))
	var rows []ladderRow
	best, flat := 0.0, 0
	stop := ""
	for _, conc := range levels {
		g := func(w, i int) reqSpec { return c.spec(p, stream, keys[w%len(keys)].Key, w, i) }
		closedLoop(ctx, conc, time.Second, gw, g)
		rs, el := closedLoop(ctx, conc, step, gw, g)
		s := summarize(rs, el)
		rows = append(rows, ladderRow{conc, s})
		c.printf("  concurrency %4d: %8.1f rps  p50 %7.2f ms  p99 %7.2f ms  %s\n", conc, s.RPS, s.Total.P50, s.Total.P99, s.outcomesString())
		if s.errorRate() > 0.01 {
			stop = fmt.Sprintf("error rate %.2f%% at concurrency %d", 100*s.errorRate(), conc)
			break
		}
		if s.RPS > best*1.05 {
			best, flat = s.RPS, 0
		} else if flat++; flat >= 2 {
			stop = fmt.Sprintf("throughput flat (< +5%%) for two levels at concurrency %d", conc)
			break
		}
		if s.RPS > best {
			best = s.RPS
		}
	}
	var t [][]string
	maxRPS, at := 0.0, 0
	for _, r := range rows {
		if r.Summary.RPS > maxRPS && r.Summary.errorRate() <= 0.01 {
			maxRPS, at = r.Summary.RPS, r.Concurrency
		}
		t = append(t, []string{strconv.Itoa(r.Concurrency), f1(r.Summary.RPS), f2(r.Summary.Total.P50), f2(r.Summary.Total.P95),
			f2(r.Summary.Total.P99), f2(100 * r.Summary.errorRate()), r.Summary.outcomesString()})
	}
	if stop == "" {
		stop = "last level reached"
	}
	c.printf("ladder: path %s, stream %v, %d orgs × 1 key, %s per level; %s; stop: %s\n", p, stream, orgs, step, lim, stop)
	c.printf("%s", table([]string{"concurrency", "rps", "p50 ms", "p95 ms", "p99 ms", "error %", "outcomes"}, t))
	c.printf("max: %.1f rps at concurrency %d\n", maxRPS, at)
	return map[string]any{"path": p.String(), "stream": stream, "orgs": orgs, "seconds_per_level": step.Seconds(), "limits": lim,
		"levels": rows, "max_rps": maxRPS, "max_rps_concurrency": at, "stop": stop}, nil
}

// ---- ratelimit: allowed vs configured ----

type rlRow struct {
	RPM       int64             `json:"rpm"`
	Offered   float64           `json:"offered_per_s"`
	Drained   int               `json:"drained"`
	Allowed   int               `json:"allowed"`
	Limited   int               `json:"limited"`
	Other     int               `json:"other"`
	Skipped   int               `json:"skipped"`
	Expected  float64           `json:"expected"`
	Accuracy  float64           `json:"accuracy"`
	Seconds   float64           `json:"seconds"`
	Sample429 map[string]string `json:"sample_429_headers"`
}

// ratelimit gives each level its own key with that rpm, empties the bucket, then offers factor × the limit for dur
// and counts the requests let through. With an empty bucket at the start the expected count is rpm × dur / 60.
func ratelimit(ctx context.Context, c config, e env, levels []int64, dur time.Duration, factor float64) (any, error) {
	f, err := e.newFixture(ctx, c.prefix, len(levels), 1)
	if err != nil {
		return nil, err
	}
	for i, l := range levels {
		if err := e.setKeyRPM(ctx, f.Orgs[i].Users[0].KeyID, l); err != nil {
			return nil, err
		}
	}
	hc := newHTTPClient(256)
	gw := runner(hc, gatewayTarget(c.addr))
	p := path{OpenAI, OpenAI}
	rows := make([]rlRow, len(levels))
	var wg sync.WaitGroup
	for i, l := range levels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := f.Orgs[i].Users[0].Key
			row := rlRow{RPM: l, Offered: round3(factor * float64(l) / 60)}
			row.Drained, row.Sample429 = drain(ctx, gw, func(n int) reqSpec { return c.spec(p, false, key, i, n) }, 3*int(l)+1000)
			rs, skipped, el := openLoop(ctx, row.Offered, dur, 256, gw, func(_, n int) reqSpec { return c.spec(p, false, key, i, n) })
			for _, r := range rs {
				switch {
				case r.Status == http.StatusOK:
					row.Allowed++
				case r.Status == http.StatusTooManyRequests && r.Code == "rate_limit_exceeded":
					row.Limited++
				default:
					row.Other++
				}
			}
			row.Skipped, row.Seconds = skipped, round3(el.Seconds())
			row.Expected = round3(float64(l) * dur.Seconds() / 60)
			row.Accuracy = round3(float64(row.Allowed) / row.Expected)
			rows[i] = row
		}()
	}
	wg.Wait()
	var t [][]string
	for _, r := range rows {
		t = append(t, []string{strconv.FormatInt(r.RPM, 10), f1(r.Offered), strconv.Itoa(r.Drained), strconv.Itoa(r.Allowed),
			f1(r.Expected), fmt.Sprintf("%.3f", r.Accuracy), strconv.Itoa(r.Limited), strconv.Itoa(r.Other)})
	}
	c.printf("ratelimit: key-level rpm, bucket drained first, then %.1f× the limit offered for %s (open loop)\n", factor, dur)
	c.printf("%s", table([]string{"rpm", "offered/s", "drain", "allowed", "expected", "allowed/expected", "429 rate_limit", "other"}, t))
	if len(rows) > 0 && rows[0].Sample429 != nil {
		c.printf("first 429 headers (rpm %d): %v\n", rows[0].RPM, rows[0].Sample429)
	}
	return map[string]any{"factor": factor, "seconds": dur.Seconds(), "rows": rows}, nil
}

// drain empties a bucket: 16 workers send requests until one gets 429 (or limit requests were sent). It returns how
// many were let through and the headers of the first 429.
func drain(ctx context.Context, run func(context.Context, reqSpec) result, g func(n int) reqSpec, limit int) (int, map[string]string) {
	var mu sync.Mutex
	sent, ok := 0, 0
	var hdr map[string]string
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if hdr != nil || sent >= limit || ctx.Err() != nil {
					mu.Unlock()
					return
				}
				n := sent
				sent++
				mu.Unlock()
				r := run(ctx, g(n))
				mu.Lock()
				if r.Status == http.StatusTooManyRequests && hdr == nil {
					hdr = r.Header
				} else if r.Status == http.StatusOK {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return ok, hdr
}

// ---- budget: overspend under concurrency ----

// budgetSample is the worst budget row seen by one sampler query (one snapshot).
type budgetRow struct {
	Kind, ID                     string
	Limit, Spent, Reserved, Unkn int64
}

func (b budgetRow) used() int64 { return b.Spent + b.Reserved + b.Unkn }

// sampleBudgets reads every limited budget row of the current month in one statement (one snapshot).
func sampleBudgets(ctx context.Context, conn *pgx.Conn, ids []string) ([]budgetRow, error) {
	rows, err := conn.Query(ctx, `SELECT scope_kind, scope_id, limit_micro, spent_micro, reserved_micro, unknown_micro
		FROM quota_budgets WHERE limit_micro IS NOT NULL AND scope_id = ANY($1)
		AND period = date_trunc('month', now() AT TIME ZONE 'UTC')::date`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []budgetRow
	for rows.Next() {
		var b budgetRow
		if err := rows.Scan(&b.Kind, &b.ID, &b.Limit, &b.Spent, &b.Reserved, &b.Unkn); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// budget runs conc workers against one org with a monthly budget (and a smaller budget on one key) until the budget
// is exhausted. Each request is, by seeded draw, a complete non-stream request, a complete stream, or a stream the
// client abandons after the first token (settled as charged_unknown at the estimate). A sampler checks every 100 ms,
// in one snapshot, that spent + reserved + unknown ≤ limit for every limited row, and the end state is checked again
// after every request has finished.
func budget(ctx context.Context, c config, e env, conc, users int, dur time.Duration, limitMicro, keyLimitMicro int64,
	pStream, pCancel float64) (any, error) {
	f, err := e.newFixture(ctx, c.prefix, 1, users)
	if err != nil {
		return nil, err
	}
	org := f.Orgs[0]
	if err := e.setOrgBudget(ctx, org.ID, limitMicro); err != nil {
		return nil, err
	}
	if err := e.setKeyBudget(ctx, org.Users[0].KeyID, keyLimitMicro); err != nil {
		return nil, err
	}
	ids := []string{strconv.FormatInt(org.ID, 10), org.Users[0].KeyID}
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	sctx, stopSampler := context.WithCancel(ctx)
	var samples, violations int
	var maxRatio float64
	var worst budgetRow
	var sampleErr error
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
			}
			bs, err := sampleBudgets(sctx, conn, ids)
			if err != nil {
				if sctx.Err() == nil {
					sampleErr = err
				}
				return
			}
			samples++
			for _, b := range bs {
				if b.used() > b.Limit {
					violations++
				}
				if r := float64(b.used()) / float64(b.Limit); r > maxRatio {
					maxRatio, worst = r, b
				}
			}
		}
	}()

	hc := newHTTPClient(conc)
	gw := runner(hc, gatewayTarget(c.addr))
	keys := org.Users
	var mu sync.Mutex
	rngs := make([]*rand.Rand, conc)
	for w := range conc {
		rngs[w] = rand.New(rand.NewPCG(c.seed, uint64(w)))
	}
	g := func(w, i int) reqSpec {
		mu.Lock()
		x := rngs[w].Float64()
		mu.Unlock()
		p := path{OpenAI, OpenAI}
		if i%2 == 1 {
			p = path{Anthropic, Anthropic}
		}
		s := c.spec(p, x < pStream+pCancel, keys[w%len(keys)].Key, w, i)
		if x < pCancel {
			// Abandoned streams go to the slow provider (delay between events), so the client is gone before the
			// usage arrives and the hold is settled as charged_unknown at its estimate.
			s.CancelAfter, s.Proto, s.Model = true, OpenAI, c.modelSlow
		}
		return s
	}
	// Stop early once the budget has been exhausted for 3 s: every request then is a cheap 429.
	rctx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	var exhaustedAt time.Time
	var first sync.Once
	run := func(ctx context.Context, s reqSpec) result {
		r := gw(ctx, s)
		if r.Code == "insufficient_quota" && strings.Contains(r.Msg, "org level") {
			first.Do(func() {
				mu.Lock()
				exhaustedAt = time.Now()
				mu.Unlock()
				time.AfterFunc(3*time.Second, stopRun)
			})
		}
		return r
	}
	start := time.Now()
	rs, el := closedLoop(rctx, conc, dur, run, g)
	time.Sleep(500 * time.Millisecond) // let the sampler see the settled end state
	stopSampler()
	<-sampled
	if sampleErr != nil {
		return nil, fmt.Errorf("budget sampler: %w", sampleErr)
	}
	end, err := sampleBudgets(ctx, conn, ids)
	if err != nil {
		return nil, err
	}
	s := summarize(rs, el)
	type endRow struct {
		Kind       string  `json:"kind"`
		LimitMicro int64   `json:"limit_micro"`
		Spent      int64   `json:"spent_micro"`
		Reserved   int64   `json:"reserved_micro"`
		Unknown    int64   `json:"unknown_micro"`
		Overspend  int64   `json:"overspend_micro"`
		UsedRatio  float64 `json:"used_ratio"`
	}
	var ends []endRow
	var t [][]string
	over := int64(0)
	for _, b := range end {
		o := max(0, b.used()-b.Limit)
		over += o
		ends = append(ends, endRow{b.Kind, b.Limit, b.Spent, b.Reserved, b.Unkn, o, round3(float64(b.used()) / float64(b.Limit))})
		t = append(t, []string{b.Kind, strconv.FormatInt(b.Limit, 10), strconv.FormatInt(b.Spent, 10), strconv.FormatInt(b.Reserved, 10),
			strconv.FormatInt(b.Unkn, 10), strconv.FormatInt(o, 10)})
	}
	exhaustedAfter := 0.0
	if !exhaustedAt.IsZero() {
		exhaustedAfter = round3(exhaustedAt.Sub(start).Seconds())
	}
	c.printf("budget: 1 org, %d users/keys, concurrency %d, org limit %d µUSD, key %s limit %d µUSD; mix stream %.2f, abandoned %.2f\n",
		users, conc, limitMicro, org.Users[0].KeyID, keyLimitMicro, pStream, pCancel)
	c.printf("requests %d in %.1f s (%s); first insufficient_quota after %.1f s\n", s.Requests, s.Seconds, s.outcomesString(), exhaustedAfter)
	c.printf("sampler: %d snapshots every 100 ms, %d rows over their limit, max used/limit %.4f (%s %s)\n",
		samples, violations, maxRatio, worst.Kind, worst.ID)
	c.printf("%s", table([]string{"level", "limit µUSD", "spent", "reserved", "unknown", "overspend"}, t))
	return map[string]any{"concurrency": conc, "users": users, "org_limit_micro": limitMicro, "key_limit_micro": keyLimitMicro,
		"p_stream": pStream, "p_abandon": pCancel, "summary": s, "exhausted_after_s": exhaustedAfter, "samples": samples,
		"sample_violations": violations, "max_used_ratio": round3(maxRatio), "end": ends, "overspend_micro": over}, nil
}

// ---- steady: constant load with hook commands (Redis stopped mid-run) ----

type hook struct {
	At  time.Duration
	Cmd string
}

func parseHook(s string) (hook, error) {
	at, cmd, ok := strings.Cut(s, "=")
	d, err := time.ParseDuration(at)
	if !ok || err != nil || cmd == "" {
		return hook{}, fmt.Errorf("--hook %q: want DURATION=COMMAND", s)
	}
	return hook{d, cmd}, nil
}

var metricLine = regexp.MustCompile(`^(agentbox_ratelimit_degraded|agentbox_ratelimit_fallback_decisions|agentbox_ratelimit_redis_failures)\s+(\S+)$`)

// scrape returns the rate-limiter gauges from a Prometheus text endpoint.
func scrape(ctx context.Context, hc *http.Client, url string) map[string]float64 {
	out := map[string]float64{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return out
	}
	resp, err := hc.Do(req)
	if err != nil {
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if m := metricLine.FindStringSubmatch(line); m != nil {
			v, _ := strconv.ParseFloat(m[2], 64)
			out[strings.TrimPrefix(m[1], "agentbox_ratelimit_")] = v
		}
	}
	return out
}

// steady runs conc workers for dur with a high rpm limit on every key (so each request goes through the Redis
// limiter), runs the hook commands at their times, and scrapes the limiter gauges every second.
func steady(ctx context.Context, c config, e env, conc int, dur time.Duration, rpm int64, hooks []hook, metricsURL string) (any, error) {
	f, err := e.newFixture(ctx, c.prefix, min(conc, 8), 1) // one key per org: no shared budget row (see lockwait)
	if err != nil {
		return nil, err
	}
	keys := f.keys()
	for _, k := range keys {
		if err := e.setKeyRPM(ctx, k.KeyID, rpm); err != nil {
			return nil, err
		}
	}
	hc := newHTTPClient(conc)
	gw := runner(hc, gatewayTarget(c.addr))
	type scrapeRow struct {
		T      float64            `json:"t"`
		Gauges map[string]float64 `json:"gauges"`
	}
	var scrapes []scrapeRow
	type hookRun struct {
		At     float64 `json:"at_s"`
		Cmd    string  `json:"cmd"`
		Output string  `json:"output"`
		Err    string  `json:"error,omitempty"`
	}
	var hookRuns []hookRun
	var mu sync.Mutex
	start := time.Now()
	bg, stopBG := context.WithCancel(ctx)
	var bgWG sync.WaitGroup
	for _, h := range hooks {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			select {
			case <-bg.Done():
				return
			case <-time.After(time.Until(start.Add(h.At))):
			}
			out, err := exec.CommandContext(bg, "sh", "-c", h.Cmd).CombinedOutput()
			hr := hookRun{At: round3(time.Since(start).Seconds()), Cmd: h.Cmd, Output: strings.TrimSpace(string(out))}
			if err != nil {
				hr.Err = err.Error()
			}
			mu.Lock()
			hookRuns = append(hookRuns, hr)
			mu.Unlock()
		}()
	}
	if metricsURL != "" {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-bg.Done():
					return
				case <-t.C:
				}
				g := scrape(bg, hc, metricsURL)
				mu.Lock()
				scrapes = append(scrapes, scrapeRow{round3(time.Since(start).Seconds()), g})
				mu.Unlock()
			}
		}()
	}
	g := func(w, i int) reqSpec { return c.spec(path{OpenAI, OpenAI}, w%2 == 1, keys[w%len(keys)].Key, w, i) }
	rs, el := closedLoop(ctx, conc, dur, gw, g)
	stopBG()
	bgWG.Wait()
	s := summarize(rs, el)
	secs := perSecond(rs, start)
	c.printf("steady: concurrency %d for %s, key rpm %d (every request takes from the limiter), half the workers stream\n", conc, dur, rpm)
	c.printf("requests %d, ok %d, %s\n", s.Requests, s.OK, s.outcomesString())
	for _, h := range hookRuns {
		c.printf("hook at %.1f s: %s → %q %s\n", h.At, h.Cmd, h.Output, h.Err)
	}
	var t [][]string
	for i, sec := range secs {
		gs := ""
		for _, sc := range scrapes {
			if int(sc.T) == i+1 {
				gs = fmt.Sprintf("degraded=%v fallback=%v redis_failures=%v", sc.Gauges["degraded"], sc.Gauges["fallback_decisions"], sc.Gauges["redis_failures"])
			}
		}
		t = append(t, []string{strconv.Itoa(sec.Second), strconv.Itoa(sec.Requests), strconv.Itoa(sec.Errors), f2(sec.P50), f2(sec.P99), f2(sec.Max), gs})
	}
	c.printf("%s", table([]string{"second", "requests", "errors", "p50 ms", "p99 ms", "max ms", "limiter gauges (end of second)"}, t))
	return map[string]any{"concurrency": conc, "rpm": rpm, "summary": s, "per_second": secs, "hooks": hookRuns, "scrapes": scrapes}, nil
}

// ---- lockwait: one hot org row vs the same load spread over many orgs ----

// lockwait runs the same closed-loop load twice per org count: once with every key in one org (one budget row
// locked by every reservation and settlement) and once with the keys spread over many orgs. A sampler counts
// backends of the server's database waiting on a lock every 20 ms.
func lockwait(ctx context.Context, c config, e env, conc, keys int, orgCounts []int, dur time.Duration, limitMicro int64) (any, error) {
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	type row struct {
		Orgs         int     `json:"orgs"`
		KeysPerOrg   int     `json:"keys_per_org"`
		Summary      summary `json:"summary"`
		Samples      int     `json:"samples"`
		MeanWaiters  float64 `json:"mean_lock_waiters"`
		MaxWaiters   int     `json:"max_lock_waiters"`
		ShareWaiting float64 `json:"share_of_samples_with_waiters"`
		MeanActive   float64 `json:"mean_active_backends"`
	}
	var rows []row
	hc := newHTTPClient(conc)
	gw := runner(hc, gatewayTarget(c.addr))
	for i, orgs := range orgCounts {
		per := max(1, keys/orgs)
		f, err := e.newFixture(ctx, fmt.Sprintf("%s-%d", c.prefix, i), orgs, per)
		if err != nil {
			return nil, err
		}
		for _, o := range f.Orgs {
			if err := e.setOrgBudget(ctx, o.ID, limitMicro); err != nil {
				return nil, err
			}
		}
		ks := f.keys()
		g := func(w, i int) reqSpec { return c.spec(path{OpenAI, OpenAI}, false, ks[w%len(ks)].Key, w, i) }
		closedLoop(ctx, conc, 2*time.Second, gw, g)
		sctx, stop := context.WithCancel(ctx)
		var samples, waitSum, maxWait, withWait, activeSum int
		done := make(chan struct{})
		go func() {
			defer close(done)
			t := time.NewTicker(20 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-sctx.Done():
					return
				case <-t.C:
				}
				var waiting, active int
				if err := conn.QueryRow(sctx, `SELECT count(*) FILTER (WHERE wait_event_type = 'Lock'), count(*) FILTER (WHERE state = 'active')
					FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`).Scan(&waiting, &active); err != nil {
					return
				}
				samples++
				waitSum += waiting
				activeSum += active
				maxWait = max(maxWait, waiting)
				if waiting > 0 {
					withWait++
				}
			}
		}()
		rs, el := closedLoop(ctx, conc, dur, gw, g)
		stop()
		<-done
		r := row{Orgs: orgs, KeysPerOrg: per, Summary: summarize(rs, el), Samples: samples, MaxWaiters: maxWait}
		if samples > 0 {
			r.MeanWaiters = round3(float64(waitSum) / float64(samples))
			r.ShareWaiting = round3(float64(withWait) / float64(samples))
			r.MeanActive = round3(float64(activeSum) / float64(samples))
		}
		rows = append(rows, r)
	}
	var t [][]string
	for _, r := range rows {
		t = append(t, []string{strconv.Itoa(r.Orgs), strconv.Itoa(r.KeysPerOrg), f1(r.Summary.RPS), f2(r.Summary.Total.P50), f2(r.Summary.Total.P99),
			f2(r.MeanWaiters), strconv.Itoa(r.MaxWaiters), f2(100 * r.ShareWaiting), f2(r.MeanActive), r.Summary.outcomesString()})
	}
	c.printf("lockwait: concurrency %d, %d keys, %s per row, non-stream oa>oa; lock waiters sampled every 20 ms from pg_stat_activity\n", conc, keys, dur)
	c.printf("%s", table([]string{"orgs", "keys/org", "rps", "p50 ms", "p99 ms", "mean lock waiters", "max", "% samples waiting", "mean active", "outcomes"}, t))
	return map[string]any{"concurrency": conc, "keys": keys, "seconds": dur.Seconds(), "rows": rows}, nil
}

// usd converts dollars to micro-dollars.
func usd(v float64) int64 { return int64(math.Round(v * 1e6)) }

// readToken reads the operator token file.
func readToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
