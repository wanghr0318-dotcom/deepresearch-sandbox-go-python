package eval

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Agents under test for coding tasks.
const (
	AgentModel     = "model"     // the model (via the Gateway) writes the solution
	AgentReference = "reference" // the suite's reference solution (harness validation)
)

// Schemas of the files a run writes.
const (
	SchemaTrajectory = "agentbox.eval.trajectory/v1"
	SchemaManifest   = "agentbox.eval.manifest/v1"
	SchemaSummary    = "agentbox.eval.summary/v1"
)

// Options configure one evaluation run.
type Options struct {
	Suite       *Suite
	SuitePath   string
	Client      *Client
	OutDir      string // the run directory is OutDir/RunID
	RunID       string // default: <suite>-<UTC timestamp>
	Concurrency int    // default 1
	Repetitions int    // default 1
	Seed        int64  // shuffles the work order; default 1
	Agent       string // model | reference (coding tasks); default model
	Model       string // optional model for the agent (must be declared on the server)
	Tasks       []string
	// DefaultTimeout applies when neither the task nor the suite sets one (default 10 min).
	DefaultTimeout time.Duration
	Judge          *Judge
	Log            io.Writer
	Now            func() time.Time
	// Nonce makes this run's request ids unique (default: 64 random bits); recorded in the manifest.
	Nonce string
}

// Manifest pins what a run evaluated.
type Manifest struct {
	Schema        string            `json:"schema"`
	RunID         string            `json:"run_id"`
	Nonce         string            `json:"nonce"` // part of every request id of the run
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at,omitempty"`
	Suite         SuiteRef          `json:"suite"`
	Seed          int64             `json:"seed"`
	Concurrency   int               `json:"concurrency"`
	Repetitions   int               `json:"repetitions"`
	Agent         string            `json:"agent"`
	Model         string            `json:"model,omitempty"`
	Tasks         []string          `json:"tasks,omitempty"`
	Server        ServerRef         `json:"server"`
	Workers       []string          `json:"workers,omitempty"`        // name@version from the workers' ready events
	ModelsUsed    []string          `json:"models_used,omitempty"`    // resolved models in the call journal
	ProvidersUsed []string          `json:"providers_used,omitempty"` // provider routes that ran model tries
	ExecDigests   []string          `json:"exec_digests,omitempty"`   // exec image digests reported by checker runs
	EvalBuild     map[string]string `json:"eval_build,omitempty"`     // build of the agentbox binary running the eval
	Judge         *JudgeRef         `json:"judge,omitempty"`
}

// SuiteRef identifies the suite.
type SuiteRef struct {
	Name    string `json:"name"`
	Version int    `json:"version,omitempty"`
	Path    string `json:"path,omitempty"`
	SHA256  string `json:"sha256"`
	Tasks   int    `json:"tasks"`
}

// ServerRef identifies the server under evaluation.
type ServerRef struct {
	Addr string          `json:"addr"`           // without userinfo
	Info json.RawMessage `json:"info,omitempty"` // GET /server-info; absent on older servers
	Note string          `json:"note,omitempty"`
}

// JudgeRef records the judge configuration (endpoint host only; never the key).
type JudgeRef struct {
	Model                string `json:"model"`
	Host                 string `json:"host"`
	BudgetMicro          int64  `json:"budget_micro"`
	PriceInMicroPerMTok  int64  `json:"price_in_micro_per_mtok"`
	PriceOutMicroPerMTok int64  `json:"price_out_micro_per_mtok"`
	MaxCalls             int    `json:"max_calls"`
	MaxTokens            int    `json:"max_tokens"`
	Required             bool   `json:"required,omitempty"`
}

// Trajectory is one line of trajectories.jsonl.
type Trajectory struct {
	Schema       string            `json:"schema"`
	RunID        string            `json:"run_id"`
	Suite        string            `json:"suite"`
	TaskID       string            `json:"task_id"`
	Kind         string            `json:"kind"`
	Rep          int               `json:"rep"`
	Agent        string            `json:"agent"`
	ServerTaskID string            `json:"server_task_id,omitempty"`
	SubmittedAt  time.Time         `json:"submitted_at"`
	TerminalAt   time.Time         `json:"terminal_at,omitempty"`
	LatencyMs    int64             `json:"latency_ms"`
	Status       string            `json:"status,omitempty"`
	StatusReason string            `json:"status_reason,omitempty"`
	Outcome      Outcome           `json:"outcome"`
	Metrics      Metrics           `json:"metrics"`
	Grades       []Grade           `json:"grades"`
	Citations    *CitationStats    `json:"citations,omitempty"`
	Error        string            `json:"error,omitempty"`
	Events       []Event           `json:"events"`
	Calls        []Call            `json:"calls"`
	Attempts     []json.RawMessage `json:"attempts,omitempty"`
	Checkpoints  []json.RawMessage `json:"checkpoints,omitempty"`
	Subruns      []json.RawMessage `json:"subruns,omitempty"`
	Budget       *Budget           `json:"budget,omitempty"`
	Result       *Result           `json:"result,omitempty"`
	Artifact     json.RawMessage   `json:"artifact,omitempty"` // coding: the eval artifact
	Report       string            `json:"report,omitempty"`   // research: report text (capped)
}

// Metrics are per-run numbers derived from the inspect data.
type Metrics struct {
	CostMicro    int64            `json:"cost_micro"`
	UnknownMicro int64            `json:"unknown_micro"`
	Calls        map[string]int   `json:"calls"` // per endpoint kind
	ModelCalls   int              `json:"model_calls"`
	ToolCalls    int              `json:"tool_calls"` // search + fetch
	ExecCalls    int              `json:"exec_calls"`
	Tries        int64            `json:"tries"`
	Attempts     int64            `json:"attempts"`
	Models       []string         `json:"models,omitempty"`
	Providers    []string         `json:"providers,omitempty"`       // fallback-chain routes that ran tries
	HedgeTries   int              `json:"hedge_tries,omitempty"`     // hedged tries (fallback chain)
	CallLatency  map[string]int64 `json:"call_latency_ms,omitempty"` // summed try latency per kind
}

const maxReportBytes = 64 << 10

type workItem struct {
	task *Task
	rep  int
}

// Run executes the suite and writes manifest.json, trajectories.jsonl, summary.json and report.md under
// OutDir/RunID. It returns the run directory and the summary. A cancelled ctx cancels the server tasks this
// run created and still writes the partial outputs.
func Run(ctx context.Context, o Options) (string, *Summary, error) {
	r, err := setupRun(ctx, o)
	if err != nil {
		return "", nil, err
	}
	defer r.file.Close()
	r.runPool(ctx)
	return r.finalize(ctx)
}

// withDefaults validates the options and fills in the defaults.
func (o Options) withDefaults() (Options, error) {
	if o.Suite == nil || o.Client == nil {
		return o, errors.New("eval: Suite and Client are required")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	o.Concurrency = max(o.Concurrency, 1)
	o.Repetitions = max(o.Repetitions, 1)
	if o.Seed == 0 {
		o.Seed = 1
	}
	if o.Agent == "" {
		o.Agent = AgentModel
	}
	if o.Agent != AgentModel && o.Agent != AgentReference {
		return o, fmt.Errorf("eval: unknown agent %q", o.Agent)
	}
	if o.DefaultTimeout <= 0 {
		o.DefaultTimeout = 10 * time.Minute
	}
	if o.Log == nil {
		o.Log = io.Discard
	}
	if o.Nonce == "" {
		o.Nonce = newNonce()
	}
	return o, nil
}

// setupRun validates the options, checks the server, creates the run directory and writes the initial
// manifest; it returns the run with its shuffled work items.
func setupRun(ctx context.Context, o Options) (*run, error) {
	o, err := o.withDefaults()
	if err != nil {
		return nil, err
	}
	started := o.Now().UTC()
	if o.RunID == "" {
		o.RunID = o.Suite.Name + "-" + started.Format("20060102T150405Z")
	}
	if !idPattern.MatchString(o.RunID) {
		return nil, fmt.Errorf("eval: run id %q must match %s", o.RunID, idPattern)
	}
	tasks, err := selectTasks(o.Suite, o.Tasks)
	if err != nil {
		return nil, err
	}
	hash, err := o.Suite.Hash()
	if err != nil {
		return nil, err
	}
	if err := o.Client.Status(ctx); err != nil {
		return nil, fmt.Errorf("eval: server unreachable: %w", err)
	}
	dir := filepath.Join(o.OutDir, o.RunID)
	if _, err := os.Stat(dir); err == nil {
		return nil, fmt.Errorf("eval: run directory %s already exists", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("eval: %w", err)
	}
	man := newManifest(ctx, o, started, SuiteRef{Name: o.Suite.Name, Version: o.Suite.Version,
		Path: filepath.ToSlash(o.SuitePath), SHA256: hash, Tasks: len(tasks)})
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), man); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, "trajectories.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("eval: %w", err)
	}
	return &run{o: o, dir: dir, man: man, file: f, out: f, items: workItems(tasks, o.Repetitions, o.Seed)}, nil
}

// newManifest pins what the run evaluates (completed with provenance in finalize).
func newManifest(ctx context.Context, o Options, started time.Time, suite SuiteRef) *Manifest {
	man := &Manifest{Schema: SchemaManifest, RunID: o.RunID, Nonce: o.Nonce, StartedAt: started, Suite: suite,
		Seed: o.Seed, Concurrency: o.Concurrency, Repetitions: o.Repetitions, Agent: o.Agent, Model: o.Model,
		Tasks: o.Tasks, Server: ServerRef{Addr: redactURL(o.Client.Base)}, EvalBuild: buildInfo()}
	if info, err := o.Client.ServerInfo(ctx); err == nil {
		man.Server.Info = info
	} else {
		man.Server.Note = "server-info unavailable: " + err.Error()
	}
	if j := o.Judge; j != nil {
		man.Judge = &JudgeRef{Model: j.Model, Host: urlHost(j.BaseURL), BudgetMicro: j.BudgetMicro,
			PriceInMicroPerMTok: j.PriceInMicroPerMTok, PriceOutMicroPerMTok: j.PriceOutMicroPerMTok,
			MaxCalls: j.maxCalls(), MaxTokens: j.maxTokens(), Required: j.Required}
	}
	return man
}

// workItems expands tasks × repetitions and shuffles them with the seed (reproducible order).
func workItems(tasks []*Task, reps int, seed int64) []workItem {
	items := make([]workItem, 0, len(tasks)*reps)
	for rep := 1; rep <= reps; rep++ {
		for _, t := range tasks {
			items = append(items, workItem{task: t, rep: rep})
		}
	}
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	return items
}

// runPool runs the work items with Concurrency workers; after ctx ends no new item is submitted.
func (r *run) runPool(ctx context.Context) {
	queue := make(chan workItem)
	var wg sync.WaitGroup
	for range r.o.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range queue {
				if ctx.Err() != nil {
					continue // interrupted: drain without submitting
				}
				r.record(r.runItem(ctx, it))
			}
		}()
	}
feed:
	for _, it := range r.items {
		select {
		case queue <- it:
		case <-ctx.Done():
			break feed
		}
	}
	close(queue)
	wg.Wait()
}

// finalize completes the manifest with provenance and writes summary.json and report.md.
func (r *run) finalize(ctx context.Context) (string, *Summary, error) {
	if r.err != nil {
		return r.dir, nil, r.err
	}
	r.man.FinishedAt = r.o.Now().UTC()
	r.man.Workers, r.man.ModelsUsed, r.man.ExecDigests = r.provenance()
	r.man.ProvidersUsed = r.providersUsed()
	if err := writeJSONFile(filepath.Join(r.dir, "manifest.json"), r.man); err != nil {
		return r.dir, nil, err
	}
	sum := Summarize(r.man, r.trajs)
	if r.o.Judge != nil {
		js := r.o.Judge.Stats()
		sum.Judge = &js
	}
	if err := WriteReports(r.dir, sum); err != nil {
		return r.dir, nil, err
	}
	if ctx.Err() != nil {
		return r.dir, sum, ctx.Err()
	}
	return r.dir, sum, nil
}

func selectTasks(s *Suite, ids []string) ([]*Task, error) {
	if len(ids) == 0 {
		out := make([]*Task, len(s.Tasks))
		for i := range s.Tasks {
			out[i] = &s.Tasks[i]
		}
		return out, nil
	}
	var out []*Task
	for _, id := range ids {
		t, ok := s.TaskByID(id)
		if !ok {
			return nil, fmt.Errorf("eval: suite has no task %q", id)
		}
		out = append(out, t)
	}
	return out, nil
}

type run struct {
	o     Options
	dir   string
	man   *Manifest
	file  *os.File
	items []workItem
	mu    sync.Mutex
	out   io.Writer
	trajs []*Trajectory
	err   error
}

func (r *run) record(t *Trajectory) {
	b, err := json.Marshal(t)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		b = append(b, '\n')
		_, err = r.out.Write(b)
	}
	if err != nil && r.err == nil {
		r.err = fmt.Errorf("eval: write trajectory: %w", err)
	}
	r.trajs = append(r.trajs, t)
	fmt.Fprintf(r.o.Log, "%-5s %-24s rep %d  %6.1fs  %s %s\n", t.Outcome.Verdict, t.TaskID, t.Rep,
		float64(t.LatencyMs)/1000, t.Status, t.Outcome.Category)
}

// provenance collects worker versions, models and exec digests observed in the trajectories.
func (r *run) provenance() (workers, models, digests []string) {
	ws, ms, ds := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, t := range r.trajs {
		for _, ev := range t.Events {
			if ev.Type != "ready" {
				continue
			}
			var p struct {
				Worker struct {
					Name    string `json:"name"`
					Version string `json:"version"`
				} `json:"worker"`
			}
			if json.Unmarshal(ev.Payload, &p) == nil && p.Worker.Name != "" {
				ws[p.Worker.Name+"@"+p.Worker.Version] = true
			}
		}
		for _, m := range t.Metrics.Models {
			ms[m] = true
		}
		var a CodingArtifact
		if len(t.Artifact) > 0 && json.Unmarshal(t.Artifact, &a) == nil && a.Exec != nil && a.Exec.ImageDigest != "" {
			ds[a.Exec.ImageDigest] = true
		}
	}
	return sortedKeys(ws), sortedKeys(ms), sortedKeys(ds)
}

// providersUsed is the set of fallback-chain routes that ran tries in this run (empty with a single provider).
func (r *run) providersUsed() []string {
	ps := map[string]bool{}
	for _, t := range r.trajs {
		for _, p := range t.Metrics.Providers {
			ps[p] = true
		}
	}
	if len(ps) == 0 {
		return nil
	}
	return sortedKeys(ps)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// newNonce is a random per-run value that makes request ids unique across runs (also with the same run id).
func newNonce() string {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("eval: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// requestID is the idempotency key of one work item: stable within a run (client retries reuse it), unique
// across runs thanks to the nonce, and at most 64 bytes whatever the run and task ids (server limit 128).
func requestID(nonce, runID, taskID string, rep int) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + taskID + "\x00" + strconv.Itoa(rep)))
	return "eval-" + nonce + "-" + hex.EncodeToString(sum[:16])
}

// redactURL drops userinfo (credentials) from a URL for recording; unparsable input is kept as host only.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "unparsable-url"
	}
	u.User = nil
	return u.String()
}

// urlHost is the host[:port] of a URL (the judge endpoint is recorded without path or credentials).
func urlHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

// artifact downloads a pinned output of the result (only artifacts pinned in the result are fetched).
func (r *run) artifact(ctx context.Context, tr *Trajectory, id string) ([]byte, bool) {
	if tr.Result == nil {
		return nil, false
	}
	for _, o := range tr.Result.Outputs {
		if o.ArtifactID == id && o.Version > 0 {
			b, err := r.o.Client.Artifact(ctx, tr.ServerTaskID, id, o.Version)
			return b, err == nil
		}
	}
	return nil, false
}

func reportArtifactID(t *Task) string {
	var cfg struct {
		ID string `json:"report_artifact_id"`
	}
	if len(t.Research) > 0 && json.Unmarshal(t.Research, &cfg) == nil && cfg.ID != "" {
		return cfg.ID
	}
	return "report"
}

func metricsOf(in *Inspection) Metrics {
	m := Metrics{Calls: map[string]int{}, CallLatency: map[string]int64{}, Attempts: in.Task.AttemptsTotal}
	if in.Budget != nil {
		m.CostMicro, m.UnknownMicro = in.Budget.SpentMicro, in.Budget.UnknownMicro
	}
	models, providers := map[string]bool{}, map[string]bool{}
	var charged int64
	for _, c := range in.Calls {
		k := endpointKind(c.Endpoint)
		m.Calls[k]++
		m.Tries += c.TriesUsed
		charged += c.CostChargedMicro
		for _, t := range c.Tries {
			m.CallLatency[k] += t.LatencyMs
			if t.Provider != "" {
				providers[t.Provider] = true
			}
			if t.Hedge {
				m.HedgeTries++
			}
		}
		switch k {
		case "chat":
			m.ModelCalls++
		case "search", "fetch":
			m.ToolCalls++
		case "exec":
			m.ExecCalls++
		}
		if c.Model != "" {
			models[c.Model] = true
		}
	}
	if in.Budget == nil {
		m.CostMicro = charged
	}
	m.Models = sortedKeys(models)
	if len(providers) > 0 {
		m.Providers = sortedKeys(providers)
	}
	return m
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("eval: encode %s: %w", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("eval: %w", err)
	}
	return nil
}

func buildInfo() map[string]string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	out := map[string]string{"go": bi.GoVersion, "module_version": bi.Main.Version}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision", "vcs.time", "vcs.modified":
			out[s.Key] = s.Value
		}
	}
	return out
}
