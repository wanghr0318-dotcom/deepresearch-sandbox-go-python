package doctor

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

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

// Environment variables (values are never printed or written).
const (
	envToken      = "AGENTBOX_TOKEN"
	envAdvisorKey = "AGENTBOX_DOCTOR_API_KEY"
	envModelKey   = "AGENTBOX_MODEL_API_KEY"
)

// Exit codes.
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
)

const usage = `usage:
  agentbox doctor-traces (--run RUN_DIR | --server URL [--since 1h] [--until RFC3339] [--max-tasks N] [--data-dir DIR])
                         [--config FLAGS_FILE | --apply-to FLAGS_FILE] [--out DIR] [--json]
                         [--slow-factor 3] [--slow-ms 0]
                         [--tempo URL] [--prometheus URL] [--loki URL] [--loki-selector '{job="agentbox"}']
                         [--advisor-model M --advisor-base-url URL --advisor-price IN:OUT [--advisor-budget-usd 0.05] [--advisor-max-tokens 1024]]
Reads failed and slow runs, classifies root causes with deterministic rules, and proposes allowlisted
configuration changes (findings.json, findings.md, proposal.patch). --apply-to writes <out>/experiment.flags
(a new file for an experiment run; the input is never modified).
token (--server): $AGENTBOX_TOKEN or <data-dir>/api.token; advisor key: $AGENTBOX_DOCTOR_API_KEY or $AGENTBOX_MODEL_API_KEY`

type cliFlags struct {
	run, server, since, until, dataDir, config, applyTo, out string
	maxTasks                                                 int
	jsonOut                                                  bool
	slowFactor                                               float64
	slowMs                                                   int64
	tempo, prom, loki, lokiSel                               string
	advModel, advURL, advPrice                               string
	advBudget                                                float64
	advMaxTokens                                             int
}

func parseFlags(args []string, stderr io.Writer) (*cliFlags, error) {
	fs := flag.NewFlagSet("doctor-traces", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintln(stderr, usage) }
	f := &cliFlags{}
	fs.StringVar(&f.run, "run", "", "eval run directory (trajectories.jsonl + manifest.json)")
	fs.StringVar(&f.server, "server", "", "server address to read a time window from (operator API)")
	fs.StringVar(&f.since, "since", "1h", "server window start: a duration before now (1h) or an RFC 3339 time")
	fs.StringVar(&f.until, "until", "", "server window end (RFC 3339; default now)")
	fs.IntVar(&f.maxTasks, "max-tasks", 500, "server window: at most this many tasks (newest first)")
	fs.StringVar(&f.dataDir, "data-dir", "", "server data directory, to read api.token (or set $AGENTBOX_TOKEN)")
	fs.StringVar(&f.config, "config", "", "the server flags file the run was made with (read only)")
	fs.StringVar(&f.applyTo, "apply-to", "", "like --config, and also write <out>/experiment.flags with the proposals applied")
	fs.StringVar(&f.out, "out", "", "output directory (default <run>/doctor, or ./doctor-<time> for --server)")
	fs.BoolVar(&f.jsonOut, "json", false, "print findings.json instead of the Markdown report")
	fs.Float64Var(&f.slowFactor, "slow-factor", 3, "slow = latency ≥ factor × median latency of passing tasks of the same kind")
	fs.Int64Var(&f.slowMs, "slow-ms", 0, "also slow = latency ≥ this many ms (0 = off)")
	fs.StringVar(&f.tempo, "tempo", "", "Tempo base URL (e.g. http://127.0.0.1:3200): trace ids and span shares")
	fs.StringVar(&f.prom, "prometheus", "", "Prometheus base URL (e.g. http://127.0.0.1:9090): metric cross-checks")
	fs.StringVar(&f.loki, "loki", "", "Loki base URL (e.g. http://127.0.0.1:3100): WARN/ERROR log counts")
	fs.StringVar(&f.lokiSel, "loki-selector", `{job="agentbox"}`, "Loki stream selector of the server log")
	fs.StringVar(&f.advModel, "advisor-model", "", "enable the optional model step with this model (off by default)")
	fs.StringVar(&f.advURL, "advisor-base-url", "", "OpenAI-compatible base URL of the advisor (…/v1)")
	fs.StringVar(&f.advPrice, "advisor-price", "", "advisor price IN:OUT in micro-USD per million tokens (required with --advisor-model)")
	fs.Float64Var(&f.advBudget, "advisor-budget-usd", 0.05, "hard budget for the advisor call")
	fs.IntVar(&f.advMaxTokens, "advisor-max-tokens", 1024, "reply cap of the advisor call (also its worst-case output reservation)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	switch {
	case fs.NArg() != 0:
		return nil, errors.New("unexpected arguments")
	case (f.run == "") == (f.server == ""):
		return nil, errors.New("give exactly one of --run and --server")
	case f.config != "" && f.applyTo != "":
		return nil, errors.New("--config and --apply-to are exclusive (--apply-to also reads the file)")
	case f.slowFactor < 1:
		return nil, errors.New("--slow-factor must be ≥ 1")
	case f.advModel != "" && (f.advURL == "" || f.advPrice == ""):
		return nil, errors.New("--advisor-model needs --advisor-base-url and --advisor-price")
	case f.advBudget <= 0 || f.advBudget > 1:
		return nil, errors.New("--advisor-budget-usd must be within (0, 1]")
	case f.advMaxTokens < 64 || f.advMaxTokens > 32768:
		return nil, errors.New("--advisor-max-tokens must be within [64, 32768]")
	}
	return f, nil
}

// Main runs `agentbox doctor-traces ...` and returns the exit code. getenv is injectable for tests.
func Main(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if getenv == nil {
		getenv = os.Getenv
	}
	f, err := parseFlags(args, stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stderr, "doctor-traces:", err)
			fmt.Fprintln(stderr, usage)
		}
		return ExitUsage
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, f, stdout, stderr, getenv); err != nil {
		fmt.Fprintln(stderr, "doctor-traces:", err)
		return ExitError
	}
	return ExitOK
}

func run(ctx context.Context, f *cliFlags, stdout, stderr io.Writer, getenv func(string) string) error {
	var cfg *Flags
	cfgPath := f.config
	if f.applyTo != "" {
		cfgPath = f.applyTo
	}
	if cfgPath != "" {
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			return err
		}
		if cfg, err = ParseFlags(data); err != nil {
			return fmt.Errorf("%s: %w", cfgPath, err)
		}
	}
	var (
		src   Source
		trs   []*eval.Trajectory
		notes []string
		err   error
	)
	if f.run != "" {
		if src, trs, err = LoadRunDir(f.run); err != nil {
			return err
		}
	} else {
		c, cerr := serverClient(f, getenv)
		if cerr != nil {
			return cerr
		}
		w, werr := parseWindow(f.since, f.until, time.Now())
		if werr != nil {
			return werr
		}
		w.MaxTasks = f.maxTasks
		if src, trs, notes, err = LoadServer(ctx, c, w); err != nil {
			return err
		}
	}
	if cfgPath != "" {
		src.Config = filepath.Base(cfgPath)
	}
	rep := Analyze(src, trs, Options{SlowFactor: f.slowFactor, SlowMs: f.slowMs, Config: cfg})
	rep.Notes = append(notes, rep.Notes...)
	Enrich(ctx, rep, trs, ObsConfig{Tempo: f.tempo, Prometheus: f.prom, Loki: f.loki, LokiSelector: f.lokiSel})
	if f.advModel != "" {
		in, out, perr := parsePrice(f.advPrice)
		if perr != nil {
			return perr
		}
		key := getenv(envAdvisorKey)
		if key == "" {
			key = getenv(envModelKey)
		}
		ad := &Advisor{BaseURL: f.advURL, Model: f.advModel, APIKey: key, PriceInMicroPerMTok: in, PriceOutMicroPerMTok: out,
			BudgetMicro: int64(f.advBudget * 1e6), MaxTokens: f.advMaxTokens}
		ApplyAdvisor(rep, ad.Run(ctx, rep, cfg))
	}
	outDir := f.out
	if outDir == "" {
		if f.run != "" {
			outDir = filepath.Join(f.run, "doctor")
		} else {
			outDir = "doctor-" + time.Now().UTC().Format("20060102T150405Z")
		}
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if cfgPath != "" {
		// The input config is never modified: refuse an output directory whose files would overwrite it.
		for _, name := range []string{"experiment.flags", "proposal.patch", "findings.json", "findings.md"} {
			if samePath(filepath.Join(outDir, name), cfgPath) {
				return fmt.Errorf("--out %s would overwrite the input config %s (%s); choose another directory", outDir, cfgPath, name)
			}
		}
	}
	js, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	md := RenderMarkdown(rep)
	files := map[string][]byte{"findings.json": append(js, '\n'), "findings.md": []byte(md)}
	if cfg != nil {
		files["proposal.patch"] = []byte(Patch(filepath.Base(cfgPath), cfg, rep.Proposals))
	}
	if f.applyTo != "" {
		files["experiment.flags"] = Experiment(cfg, rep.Proposals).Bytes()
	}
	for _, name := range sortedKeys(files) {
		if err := os.WriteFile(filepath.Join(outDir, name), files[name], 0o644); err != nil {
			return err
		}
	}
	if f.jsonOut {
		_, _ = stdout.Write(files["findings.json"])
	} else {
		fmt.Fprint(stdout, md)
	}
	fmt.Fprintf(stderr, "doctor-traces: wrote %s to %s\n", strings.Join(sortedKeys(files), ", "), outDir)
	return nil
}

// samePath reports whether two paths name the same file (cleaned absolute paths, symlinks resolved, or the same
// file by os.SameFile when both exist).
func samePath(a, b string) bool {
	norm := func(p string) string {
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		if r, err := filepath.EvalSymlinks(p); err == nil {
			p = r
		}
		return filepath.Clean(p)
	}
	if norm(a) == norm(b) {
		return true
	}
	fa, errA := os.Stat(a)
	fb, errB := os.Stat(b)
	return errA == nil && errB == nil && os.SameFile(fa, fb)
}

func serverClient(f *cliFlags, getenv func(string) string) (*eval.Client, error) {
	token := getenv(envToken)
	if token == "" && f.dataDir != "" {
		b, err := os.ReadFile(filepath.Join(f.dataDir, "api.token"))
		if err != nil {
			return nil, fmt.Errorf("read api.token: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}
	if token == "" {
		return nil, errors.New("no operator token: set $AGENTBOX_TOKEN or --data-dir")
	}
	return &eval.Client{Base: strings.TrimRight(f.server, "/"), Token: token}, nil
}

// parseWindow parses --since (a duration before now or an RFC 3339 time) and --until.
func parseWindow(since, until string, now time.Time) (Window, error) {
	var w Window
	if d, err := time.ParseDuration(since); err == nil && d > 0 {
		w.Since = now.Add(-d)
	} else if t, err := time.Parse(time.RFC3339, since); err == nil {
		w.Since = t
	} else {
		return w, fmt.Errorf("--since %q: want a duration (1h) or an RFC 3339 time", since)
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return w, fmt.Errorf("--until %q: want an RFC 3339 time", until)
		}
		w.Until = t
	}
	return w, nil
}

func parsePrice(s string) (in, out int64, err error) {
	a, b, ok := strings.Cut(s, ":")
	if ok {
		in, err = strconv.ParseInt(a, 10, 64)
	}
	if ok && err == nil {
		out, err = strconv.ParseInt(b, 10, 64)
	}
	if !ok || err != nil || in <= 0 || out <= 0 {
		return 0, 0, fmt.Errorf("--advisor-price %q must be IN:OUT (positive integers)", s)
	}
	return in, out, nil
}
