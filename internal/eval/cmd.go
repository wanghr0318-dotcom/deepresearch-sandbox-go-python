package eval

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Environment variables read by the eval CLI (values are never printed).
const (
	envToken    = "AGENTBOX_TOKEN"
	envAddr     = "AGENTBOX_ADDR"
	envJudgeKey = "AGENTBOX_JUDGE_API_KEY"
	envModelKey = "AGENTBOX_MODEL_API_KEY"
)

const usage = `usage:
  agentbox eval run --suite FILE [--addr URL] [--data-dir DIR] [--out DIR] [--run-id ID]
                    [--agent model|reference] [--model NAME] [--tasks a,b] [--concurrency N]
                    [--repeat N] [--seed N] [--timeout D] [--min-success R]
                    [--judge-model M --judge-base-url URL [--judge-budget-usd X] [--judge-price IN:OUT]]
  agentbox eval report RUN_DIR
  agentbox eval compare RUN_A RUN_B [--json]
token: $AGENTBOX_TOKEN or <data-dir>/api.token; judge key: $AGENTBOX_JUDGE_API_KEY or $AGENTBOX_MODEL_API_KEY`

// Exit codes.
const (
	ExitOK          = 0
	ExitError       = 1
	ExitUsage       = 2
	ExitBelowTarget = 3 // run completed but success rate < --min-success
	ExitInterrupted = 130
)

// Main runs `agentbox eval ...` and returns the process exit code. getenv is injectable for tests.
func Main(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if getenv == nil {
		getenv = os.Getenv
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return ExitUsage
	}
	switch args[0] {
	case "run":
		return cmdRun(args[1:], stdout, stderr, getenv)
	case "report":
		return cmdReport(args[1:], stdout, stderr)
	case "compare":
		return cmdCompare(args[1:], stdout, stderr)
	}
	fmt.Fprintln(stderr, "eval: unknown command "+strconv.Quote(args[0]))
	fmt.Fprintln(stderr, usage)
	return ExitUsage
}

type runFlags struct {
	suite, addr, dataDir, out, runID, agent, model, tasks string
	concurrency, repeat                                   int
	seed                                                  int64
	timeout                                               time.Duration
	minSuccess                                            float64
	judgeModel, judgeURL, judgePrice                      string
	judgeBudget                                           float64
}

func parseRunFlags(args []string, stderr io.Writer) (*runFlags, error) {
	fs := flag.NewFlagSet("eval run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	f := &runFlags{}
	fs.StringVar(&f.suite, "suite", "", "suite file (.yaml/.yml/.json)")
	fs.StringVar(&f.addr, "addr", "", "server address (default $AGENTBOX_ADDR or http://127.0.0.1:8080)")
	fs.StringVar(&f.dataDir, "data-dir", "", "server data directory, to read api.token (or set $AGENTBOX_TOKEN)")
	fs.StringVar(&f.out, "out", "eval-runs", "output directory; the run is written to <out>/<run-id>")
	fs.StringVar(&f.runID, "run-id", "", "run id (default <suite>-<UTC time>)")
	fs.StringVar(&f.agent, "agent", AgentModel, "coding agent under test: model | reference")
	fs.StringVar(&f.model, "model", "", "model for the agent (must be declared on the server; default: server default)")
	fs.StringVar(&f.tasks, "tasks", "", "comma-separated task ids (default: all)")
	fs.IntVar(&f.concurrency, "concurrency", 1, "tasks in flight")
	fs.IntVar(&f.repeat, "repeat", 1, "repetitions of each task")
	fs.Int64Var(&f.seed, "seed", 1, "seed for the work order")
	fs.DurationVar(&f.timeout, "timeout", 10*time.Minute, "per-task timeout when the suite sets none")
	fs.Float64Var(&f.minSuccess, "min-success", 0, "exit 3 if the success rate is below this ratio")
	fs.StringVar(&f.judgeModel, "judge-model", "", "enable the LLM judge with this model (off by default)")
	fs.StringVar(&f.judgeURL, "judge-base-url", "", "OpenAI-compatible base URL of the judge (…/v1)")
	fs.Float64Var(&f.judgeBudget, "judge-budget-usd", 0.20, "hard budget for all judge calls of the run")
	fs.StringVar(&f.judgePrice, "judge-price", "", "judge price IN:OUT in micro-USD per million tokens")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	switch {
	case fs.NArg() != 0:
		return nil, errors.New("unexpected arguments")
	case f.suite == "":
		return nil, errors.New("--suite is required")
	case f.concurrency < 1 || f.concurrency > 64:
		return nil, errors.New("--concurrency must be 1–64")
	case f.repeat < 1 || f.repeat > 100:
		return nil, errors.New("--repeat must be 1–100")
	case f.agent != AgentModel && f.agent != AgentReference:
		return nil, errors.New("--agent must be model or reference")
	case f.minSuccess < 0 || f.minSuccess > 1:
		return nil, errors.New("--min-success must be within [0, 1]")
	case f.judgeModel != "" && f.judgeURL == "":
		return nil, errors.New("--judge-model needs --judge-base-url")
	case f.judgeBudget < 0 || f.judgeBudget > 5:
		return nil, errors.New("--judge-budget-usd must be within [0, 5]")
	}
	return f, nil
}

func parsePrice(s string) (in, out int64, err error) {
	if s == "" {
		return 0, 0, nil
	}
	a, b, ok := strings.Cut(s, ":")
	if ok {
		in, err = strconv.ParseInt(a, 10, 64)
	}
	if ok && err == nil {
		out, err = strconv.ParseInt(b, 10, 64)
	}
	if !ok || err != nil || in < 0 || out < 0 {
		return 0, 0, fmt.Errorf("--judge-price %q must be IN:OUT (non-negative integers)", s)
	}
	return in, out, nil
}

func resolveClient(addr, dataDir string, getenv func(string) string) (*Client, error) {
	if addr == "" {
		addr = getenv(envAddr)
	}
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	token := strings.TrimSpace(getenv(envToken))
	if token == "" && dataDir != "" {
		b, err := os.ReadFile(filepath.Join(dataDir, "api.token"))
		if err != nil {
			return nil, fmt.Errorf("read api.token: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}
	return &Client{Base: strings.TrimRight(addr, "/"), Token: token}, nil
}

func cmdRun(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	f, err := parseRunFlags(args, stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stderr, "eval run:", err)
		}
		return ExitUsage
	}
	suite, err := LoadSuite(f.suite)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	client, err := resolveClient(f.addr, f.dataDir, getenv)
	if err != nil {
		fmt.Fprintln(stderr, "eval run:", err)
		return ExitError
	}
	o := Options{Suite: suite, SuitePath: f.suite, Client: client, OutDir: f.out, RunID: f.runID,
		Concurrency: f.concurrency, Repetitions: f.repeat, Seed: f.seed, Agent: f.agent, Model: f.model,
		Tasks: splitCSV(f.tasks), DefaultTimeout: f.timeout, Log: stdout}
	if f.judgeModel != "" {
		in, out, err := parsePrice(f.judgePrice)
		if err != nil {
			fmt.Fprintln(stderr, "eval run:", err)
			return ExitUsage
		}
		key := getenv(envJudgeKey)
		if key == "" {
			key = getenv(envModelKey)
		}
		o.Judge = &Judge{BaseURL: f.judgeURL, Model: f.judgeModel, APIKey: key,
			PriceInMicroPerMTok: in, PriceOutMicroPerMTok: out, BudgetMicro: int64(f.judgeBudget * 1e6)}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dir, sum, err := Run(ctx, o)
	if dir != "" {
		fmt.Fprintln(stdout, "run directory:", dir)
	}
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(stderr, "eval run: interrupted; partial results written")
		return ExitInterrupted
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	fmt.Fprintf(stdout, "success %d/%d (%s), latency p50 %s p95 %s, cost %s\n", sum.Passed, sum.Runs, pct(sum.SuccessRate),
		secs(sum.LatencyMs.P50), secs(sum.LatencyMs.P95), usd(sum.CostMicro))
	if sum.SuccessRate < f.minSuccess {
		fmt.Fprintf(stderr, "eval run: success rate %s below --min-success %s\n", pct(sum.SuccessRate), pct(f.minSuccess))
		return ExitBelowTarget
	}
	return ExitOK
}

func splitCSV(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func cmdReport(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, usage)
		return ExitUsage
	}
	lr, err := LoadRun(args[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	s := lr.Summary()
	// keep the judge usage recorded at run time (it is not derivable from the trajectories)
	if b, err := os.ReadFile(filepath.Join(args[0], "summary.json")); err == nil {
		var old Summary
		if json.Unmarshal(b, &old) == nil {
			s.Judge = old.Judge
		}
	}
	if err := WriteReports(args[0], s); err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	fmt.Fprint(stdout, RenderMarkdown(s))
	return ExitOK
}

func cmdCompare(args []string, stdout, stderr io.Writer) int {
	asJSON := false
	var dirs []string
	for _, a := range args {
		switch {
		case a == "--json" || a == "-json":
			asJSON = true
		case strings.HasPrefix(a, "-"):
			fmt.Fprintln(stderr, "eval compare: unknown flag "+a)
			return ExitUsage
		default:
			dirs = append(dirs, a)
		}
	}
	if len(dirs) != 2 {
		fmt.Fprintln(stderr, usage)
		return ExitUsage
	}
	a, err := LoadRun(dirs[0])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	b, err := LoadRun(dirs[1])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return ExitError
	}
	c := Compare(a, b)
	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(c); err != nil {
			fmt.Fprintln(stderr, err)
			return ExitError
		}
		return ExitOK
	}
	fmt.Fprint(stdout, RenderCompare(c))
	return ExitOK
}
