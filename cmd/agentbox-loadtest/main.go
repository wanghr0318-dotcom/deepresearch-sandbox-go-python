// Command agentbox-loadtest measures the model API (model gateway design 2026-10-11 §10) against a running
// `agentbox server --model-api` whose providers are tests/e2e/fakeupstream processes. Everything is local and costs
// nothing. scripts/loadtest-gateway.sh starts the server, the fakes and a disposable Redis and runs the scenarios.
//
// Scenarios (one per invocation):
//
//	latency    gateway overhead: each path (client format > provider format) through the gateway and the same
//	           request sent straight to the fake provider; P50/P95/P99 of both, and of time to first token for streams
//	ladder     maximum requests per second: closed-loop concurrency raised level by level until errors or the knee
//	ratelimit  rate-limiter accuracy: keys at several rpm levels, bucket drained, then 2× the limit offered for 60 s
//	budget     overspend under concurrency: one org budget, concurrent complete, streamed and abandoned requests,
//	           spent + reserved + unknown ≤ limit sampled every 100 ms and at the end
//	steady     constant load with hook commands at given times (Redis stopped mid-run) and limiter gauges scraped
//	lockwait   the same load with every key in one org vs spread over many orgs; lock waiters sampled
//
// Each scenario creates its own users (through the store, so the /auth/register limiter does not apply), orgs and
// models-scoped keys (through the operator API), with names starting with a fresh prefix, so runs never collide and
// can be repeated on the same database. Randomness (the budget mix) is seeded with --seed. The result is printed as
// Markdown tables and, with --out, appended to a JSON-lines file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
)

const usage = `usage: agentbox-loadtest <latency|ladder|ratelimit|budget|steady|lockwait> [flags]
run "agentbox-loadtest <scenario> -h" for the flags`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	name := args[0]
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := config{stdout: stdout}
	fs.StringVar(&c.addr, "addr", "http://127.0.0.1:8080", "agentbox server address")
	fs.StringVar(&c.tokenFile, "token-file", "", "operator token file (<data>/api.token; required)")
	fs.StringVar(&c.dsn, "database-url", os.Getenv("AGENTBOX_DATABASE_URL"), "the server's database (default $AGENTBOX_DATABASE_URL)")
	fs.StringVar(&c.fakeOpenAI, "fake-openai", "", "OpenAI fake provider model_base_url (…/v1), for the direct baseline")
	fs.StringVar(&c.fakeAnthropic, "fake-anthropic", "", "Anthropic fake provider url (root), for the direct baseline")
	fs.StringVar(&c.modelOA, "model-openai", "m-oa", "logical model served by the OpenAI-format provider")
	fs.StringVar(&c.modelAN, "model-anthropic", "m-an", "logical model served by the Anthropic-format provider")
	fs.StringVar(&c.fakeName, "fake-model", "fake-model", "model name sent in direct requests")
	fs.StringVar(&c.modelSlow, "model-slow", "m-slow", "budget: logical model served by a provider with a delay between stream events")
	fs.IntVar(&c.maxTokens, "max-tokens", 32, "max_tokens of every request")
	fs.IntVar(&c.promptBytes, "prompt-bytes", 200, "size of the user message")
	fs.Uint64Var(&c.seed, "seed", 1, "random seed")
	prefix := fs.String("prefix", "", "name prefix of created users and orgs (default lt<unix-ms in base 36>)")
	out := fs.String("out", "", "append the JSON result to this file")
	label := fs.String("label", "", "free-form label stored with the JSON result (e.g. repeat number)")
	conc := fs.Int("concurrency", 1, "concurrent clients (closed loop)")
	dur := fs.Duration("duration", 15*time.Second, "measured duration (per row / level)")
	warmup := fs.Duration("warmup", 2*time.Second, "latency: warm-up per row, not measured")
	paths := fs.String("paths", "oa>oa,an>oa,oa>an,an>an", "latency: client>provider paths (oa = OpenAI, an = Anthropic)")
	streams := fs.String("stream", "both", "latency: off, on or both; ladder: off or on")
	levels := fs.String("levels", "", "ladder: concurrency levels (default 1,2,4,8,16,32,64,128,256); ratelimit: rpm levels (default 60,600,3000)")
	pth := fs.String("path", "oa>oa", "ladder: the path")
	orgs := fs.Int("orgs", 16, "ladder: orgs (one key each) the clients are spread over")
	factor := fs.Float64("factor", 2, "ratelimit: offered rate as a multiple of the limit")
	users := fs.Int("users", 4, "budget: users (one key each) in the org")
	budgetUSD := fs.Float64("budget-usd", 2, "budget: the org's monthly budget in USD")
	keyBudgetUSD := fs.Float64("key-budget-usd", 0.25, "budget: the first key's monthly budget in USD")
	pStream := fs.Float64("p-stream", 0.25, "budget: share of complete streams")
	pAbandon := fs.Float64("p-abandon", 0.15, "budget: share of streams abandoned after the first token (charged_unknown)")
	rpm := fs.Int64("rpm", 0, "latency, ladder: rpm at the org, user and key level (0 = none); steady: rpm on every key (default 1000000)")
	tpm := fs.Int64("tpm", 0, "latency, ladder: tpm at the org, user and key level (0 = none)")
	var hooks []hook
	fs.Func("hook", "steady: DURATION=COMMAND run with sh -c at that time (repeatable)", func(s string) error {
		h, err := parseHook(s)
		hooks = append(hooks, h)
		return err
	})
	metrics := fs.String("metrics-url", "", "steady: Prometheus endpoint to scrape every second (http://…/metrics)")
	keys := fs.Int("keys", 16, "lockwait: keys in total")
	orgCounts := fs.String("org-counts", "1,16", "lockwait: org counts to compare")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if c.tokenFile == "" || c.dsn == "" {
		fmt.Fprintln(stderr, "agentbox-loadtest: --token-file and --database-url (or AGENTBOX_DATABASE_URL) are required")
		return 2
	}
	c.prefix = *prefix
	if c.prefix == "" {
		c.prefix = "lt" + strconv.FormatInt(time.Now().UnixMilli(), 36)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	token, err := readToken(c.tokenFile)
	if err != nil {
		fmt.Fprintln(stderr, "agentbox-loadtest:", err)
		return 2
	}
	store, err := postgres.Open(ctx, postgres.Options{DSN: c.dsn})
	if err != nil {
		fmt.Fprintln(stderr, "agentbox-loadtest:", err)
		return 1
	}
	defer store.Close()
	e := env{store: store, op: operator{addr: c.addr, token: token, hc: newHTTPClient(4)}}

	var res any
	params := map[string]any{"seed": c.seed, "max_tokens": c.maxTokens, "prompt_bytes": c.promptBytes, "prefix": c.prefix}
	switch name {
	case "latency":
		ps, perr := parsePaths(*paths)
		modes, merr := streamModes(*streams)
		if perr != nil || merr != nil || c.fakeOpenAI == "" || c.fakeAnthropic == "" {
			fmt.Fprintln(stderr, "agentbox-loadtest latency: --paths, --stream, --fake-openai and --fake-anthropic must be valid", perr, merr)
			return 2
		}
		res, err = latency(ctx, c, e, *conc, *dur, *warmup, ps, modes, limits{*rpm, *tpm})
	case "ladder":
		ls, lerr := ints(*levels, "1,2,4,8,16,32,64,128,256")
		p, perr := parsePaths(*pth)
		if lerr != nil || perr != nil || len(p) != 1 || (*streams != "off" && *streams != "on" && *streams != "both") {
			fmt.Fprintln(stderr, "agentbox-loadtest ladder: invalid --levels, --path or --stream")
			return 2
		}
		res, err = ladder(ctx, c, e, ls, *dur, p[0], *streams == "on", *orgs, limits{*rpm, *tpm})
	case "ratelimit":
		ls, lerr := ints(*levels, "60,600,3000")
		if lerr != nil {
			fmt.Fprintln(stderr, "agentbox-loadtest ratelimit:", lerr)
			return 2
		}
		rl := make([]int64, len(ls))
		for i, l := range ls {
			rl[i] = int64(l)
		}
		res, err = ratelimit(ctx, c, e, rl, *dur, *factor)
	case "budget":
		res, err = budget(ctx, c, e, *conc, *users, *dur, usd(*budgetUSD), usd(*keyBudgetUSD), *pStream, *pAbandon)
	case "steady":
		steadyRPM := *rpm
		if steadyRPM <= 0 {
			steadyRPM = 1_000_000
		}
		res, err = steady(ctx, c, e, *conc, *dur, steadyRPM, hooks, *metrics)
	case "lockwait":
		oc, oerr := ints(*orgCounts, "1,16")
		if oerr != nil {
			fmt.Fprintln(stderr, "agentbox-loadtest lockwait:", oerr)
			return 2
		}
		res, err = lockwait(ctx, c, e, *conc, *keys, oc, *dur, usd(1000))
	default:
		fmt.Fprintln(stderr, usage)
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "agentbox-loadtest %s: %v\n", name, err)
		return 1
	}
	if *out != "" {
		rec := map[string]any{"scenario": name, "label": *label, "at": time.Now().UTC().Format(time.RFC3339),
			"go": runtime.Version(), "cpus": runtime.NumCPU(), "params": params, "args": redactArgs(args[1:]), "result": res}
		if err := appendJSON(*out, rec); err != nil {
			fmt.Fprintln(stderr, "agentbox-loadtest:", err)
			return 1
		}
	}
	return 0
}

// redactArgs replaces the password of any URL argument (the database URL) with "xxxxx".
func redactArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a
		if u, err := url.Parse(a); err == nil && u.User != nil {
			if _, ok := u.User.Password(); ok {
				u.User = url.UserPassword(u.User.Username(), "xxxxx")
				out[i] = u.String()
			}
		}
		if k, v, ok := strings.Cut(a, "="); ok && strings.HasPrefix(k, "-") {
			if r := redactArgs([]string{v}); r[0] != v {
				out[i] = k + "=" + r[0]
			}
		}
	}
	return out
}

// ints parses a comma-separated list of positive integers (def when s is empty).
func ints(s, def string) ([]int, error) {
	if s == "" {
		s = def
	}
	var out []int
	for _, item := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%q: want positive integers separated by commas", s)
		}
		out = append(out, n)
	}
	return out, nil
}

func appendJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
