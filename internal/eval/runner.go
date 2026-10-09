package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
}

// Manifest pins what a run evaluated.
type Manifest struct {
	Schema      string            `json:"schema"`
	RunID       string            `json:"run_id"`
	StartedAt   time.Time         `json:"started_at"`
	FinishedAt  time.Time         `json:"finished_at,omitempty"`
	Suite       SuiteRef          `json:"suite"`
	Seed        int64             `json:"seed"`
	Concurrency int               `json:"concurrency"`
	Repetitions int               `json:"repetitions"`
	Agent       string            `json:"agent"`
	Model       string            `json:"model,omitempty"`
	Tasks       []string          `json:"tasks,omitempty"`
	Server      ServerRef         `json:"server"`
	Workers     []string          `json:"workers,omitempty"`      // name@version from the workers' ready events
	ModelsUsed  []string          `json:"models_used,omitempty"`  // resolved models in the call journal
	ExecDigests []string          `json:"exec_digests,omitempty"` // exec image digests reported by checker runs
	EvalBuild   map[string]string `json:"eval_build,omitempty"`   // build of the agentbox binary running the eval
	Judge       *JudgeRef         `json:"judge,omitempty"`
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
	Addr string          `json:"addr"`
	Info json.RawMessage `json:"info,omitempty"` // GET /server-info; absent on older servers
	Note string          `json:"note,omitempty"`
}

// JudgeRef records the judge configuration.
type JudgeRef struct {
	Model       string `json:"model"`
	BudgetMicro int64  `json:"budget_micro"`
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
	if o.Suite == nil || o.Client == nil {
		return "", nil, errors.New("eval: Suite and Client are required")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.Repetitions <= 0 {
		o.Repetitions = 1
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	if o.Agent == "" {
		o.Agent = AgentModel
	}
	if o.Agent != AgentModel && o.Agent != AgentReference {
		return "", nil, fmt.Errorf("eval: unknown agent %q", o.Agent)
	}
	if o.DefaultTimeout <= 0 {
		o.DefaultTimeout = 10 * time.Minute
	}
	if o.Log == nil {
		o.Log = io.Discard
	}
	started := o.Now().UTC()
	if o.RunID == "" {
		o.RunID = o.Suite.Name + "-" + started.Format("20060102T150405Z")
	}
	if !idPattern.MatchString(o.RunID) {
		return "", nil, fmt.Errorf("eval: run id %q must match %s", o.RunID, idPattern)
	}
	tasks, err := selectTasks(o.Suite, o.Tasks)
	if err != nil {
		return "", nil, err
	}
	hash, err := o.Suite.Hash()
	if err != nil {
		return "", nil, err
	}
	if err := o.Client.Status(ctx); err != nil {
		return "", nil, fmt.Errorf("eval: server unreachable: %w", err)
	}
	dir := filepath.Join(o.OutDir, o.RunID)
	if _, err := os.Stat(dir); err == nil {
		return "", nil, fmt.Errorf("eval: run directory %s already exists", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, fmt.Errorf("eval: %w", err)
	}

	man := &Manifest{Schema: SchemaManifest, RunID: o.RunID, StartedAt: started,
		Suite: SuiteRef{Name: o.Suite.Name, Version: o.Suite.Version, Path: filepath.ToSlash(o.SuitePath), SHA256: hash, Tasks: len(tasks)},
		Seed:  o.Seed, Concurrency: o.Concurrency, Repetitions: o.Repetitions, Agent: o.Agent, Model: o.Model,
		Tasks: o.Tasks, Server: ServerRef{Addr: o.Client.Base}, EvalBuild: buildInfo()}
	if info, err := o.Client.ServerInfo(ctx); err == nil {
		man.Server.Info = info
	} else {
		man.Server.Note = "server-info unavailable: " + err.Error()
	}
	if o.Judge != nil {
		man.Judge = &JudgeRef{Model: o.Judge.Model, BudgetMicro: o.Judge.BudgetMicro}
	}
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), man); err != nil {
		return "", nil, err
	}

	items := make([]workItem, 0, len(tasks)*o.Repetitions)
	for rep := 1; rep <= o.Repetitions; rep++ {
		for _, t := range tasks {
			items = append(items, workItem{task: t, rep: rep})
		}
	}
	rng := rand.New(rand.NewSource(o.Seed))
	rng.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })

	f, err := os.Create(filepath.Join(dir, "trajectories.jsonl"))
	if err != nil {
		return "", nil, fmt.Errorf("eval: %w", err)
	}
	defer f.Close()
	r := &run{o: o, out: f}
	queue := make(chan workItem)
	var wg sync.WaitGroup
	for range o.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range queue {
				r.record(r.runItem(ctx, it))
			}
		}()
	}
feed:
	for _, it := range items {
		select {
		case queue <- it:
		case <-ctx.Done():
			break feed
		}
	}
	close(queue)
	wg.Wait()
	if r.err != nil {
		return dir, nil, r.err
	}

	man.FinishedAt = o.Now().UTC()
	man.Workers, man.ModelsUsed, man.ExecDigests = r.provenance()
	if err := writeJSONFile(filepath.Join(dir, "manifest.json"), man); err != nil {
		return dir, nil, err
	}
	sum := Summarize(man, r.trajs)
	if o.Judge != nil {
		js := o.Judge.Stats()
		sum.Judge = &js
	}
	if err := WriteReports(dir, sum); err != nil {
		return dir, nil, err
	}
	if ctx.Err() != nil {
		return dir, sum, ctx.Err()
	}
	return dir, sum, nil
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

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// spec builds the POST /tasks spec for a task.
func (r *run) spec(t *Task) (json.RawMessage, error) {
	ev := map[string]any{"kind": t.Kind, "task_id": t.ID, "suite": r.o.Suite.Name}
	switch t.Kind {
	case KindCoding:
		ev["agent"] = r.o.Agent
		ev["prompt"] = t.Prompt
		ev["check"] = t.Check
		if len(t.Files) > 0 {
			ev["files"] = t.Files
		}
		if r.o.Agent == AgentReference {
			if t.Reference == "" {
				return nil, fmt.Errorf("task %s has no reference solution", t.ID)
			}
			ev["reference"] = t.Reference // never sent to the model agent
		}
		if w := r.o.Suite.EffectiveWallMs(t); w > 0 {
			ev["wall_ms"] = w
		}
		if r.o.Model != "" {
			ev["model"] = r.o.Model
		}
		return json.Marshal(map[string]any{"eval": ev})
	default:
		cfg := map[string]any{}
		if len(t.Research) > 0 {
			if err := json.Unmarshal(t.Research, &cfg); err != nil {
				return nil, err
			}
		}
		cfg["topic"] = t.Topic
		if r.o.Model != "" {
			for _, k := range []string{"orchestrator_model", "worker_model"} {
				if _, ok := cfg[k]; !ok {
					cfg[k] = r.o.Model
				}
			}
		}
		cfg["eval"] = ev
		return json.Marshal(cfg)
	}
}

// terminalGrace bounds how long the runner waits for task_terminal after cancelling a timed-out task.
const terminalGrace = 60 * time.Second

func (r *run) runItem(ctx context.Context, it workItem) *Trajectory {
	t := it.task
	tr := &Trajectory{Schema: SchemaTrajectory, RunID: r.o.RunID, Suite: r.o.Suite.Name, TaskID: t.ID, Kind: t.Kind,
		Rep: it.rep, Agent: r.o.Agent, Events: []Event{}, Calls: []Call{}, Grades: []Grade{}}
	if t.Kind == KindResearch {
		tr.Agent = "deepresearch"
	}
	fail := func(cat string, err error) *Trajectory {
		tr.Outcome = Outcome{Verdict: VerdictError, Category: cat}
		if err != nil {
			tr.Error = err.Error()
		}
		return tr
	}
	spec, err := r.spec(t)
	if err != nil {
		return fail("invalid_task", err)
	}
	tr.SubmittedAt = r.o.Now().UTC()
	rid := "eval-" + r.o.RunID + "-" + t.ID + "-" + strconv.Itoa(it.rep)
	id, err := r.o.Client.CreateTask(ctx, rid, spec, r.o.Suite.EffectiveLimits(t))
	if err != nil {
		return fail("infra_error", err)
	}
	tr.ServerTaskID = id

	timeout := r.o.Suite.EffectiveTimeout(t, r.o.DefaultTimeout)
	wctx, cancel := context.WithTimeout(ctx, timeout)
	var evMu sync.Mutex
	collect := func(ev Event) {
		evMu.Lock()
		tr.Events = append(tr.Events, ev)
		evMu.Unlock()
	}
	werr := r.o.Client.Events(wctx, id, collect)
	cancel()
	timedOut := false
	if werr != nil {
		// Timeout or interrupted run: cancel the server task, then wait (bounded) for its terminal event.
		timedOut = errors.Is(werr, context.DeadlineExceeded) && ctx.Err() == nil
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), terminalGrace)
		reason := "eval timeout"
		if !timedOut {
			reason = "eval interrupted"
		}
		_ = r.o.Client.Cancel(cctx, id, rid+"-cancel", reason)
		evMu.Lock()
		tr.Events = tr.Events[:0]
		evMu.Unlock()
		_ = r.o.Client.Events(cctx, id, collect)
		ccancel()
	}
	tr.TerminalAt = r.o.Now().UTC()
	tr.LatencyMs = tr.TerminalAt.Sub(tr.SubmittedAt).Milliseconds()

	ictx, icancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer icancel()
	in, _, err := r.o.Client.Inspect(ictx, id)
	if err != nil {
		return fail("infra_error", fmt.Errorf("inspect: %w", err))
	}
	tr.Status, tr.StatusReason = in.Task.Status, in.Task.StatusReason
	tr.Calls = in.Calls
	if tr.Calls == nil {
		tr.Calls = []Call{}
	}
	tr.Attempts, tr.Checkpoints, tr.Subruns, tr.Budget = in.Attempts, in.Checkpoints, in.Subruns, in.Budget
	tr.Metrics = metricsOf(in)
	if res, err := r.o.Client.Result(ictx, id); err == nil {
		tr.Result = res
	}

	var answer string
	switch t.Kind {
	case KindCoding:
		var art *CodingArtifact
		if b, ok := r.artifact(ictx, tr, "eval"); ok {
			tr.Artifact = json.RawMessage(b)
			if a, err := parseCodingArtifact(b); err == nil {
				art = a
			} else {
				tr.Error = err.Error()
			}
		}
		tr.Grades = GradeCoding(t, tr.Status, tr.StatusReason, art)
		if b, ok := r.artifact(ictx, tr, "solution"); ok {
			answer = string(b)
		}
	default:
		if b, ok := r.artifact(ictx, tr, reportArtifactID(t)); ok {
			answer = string(b)
			tr.Report = answer
			if len(tr.Report) > maxReportBytes {
				tr.Report = tr.Report[:maxReportBytes]
			}
		}
		gs, st := GradeResearch(t, tr.Status, tr.StatusReason, answer, tr.Calls)
		tr.Grades, tr.Citations = gs, &st
	}
	if r.o.Judge != nil && t.Judge != nil && answer != "" {
		tr.Grades = append(tr.Grades, r.o.Judge.Grade(ictx, t, answer))
	}
	tr.Outcome = Decide(tr.Grades)
	if timedOut {
		tr.Outcome = Outcome{Verdict: VerdictFail, Category: "timeout"}
	}
	return tr
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
	models := map[string]bool{}
	var charged int64
	for _, c := range in.Calls {
		k := endpointKind(c.Endpoint)
		m.Calls[k]++
		m.Tries += c.TriesUsed
		charged += c.CostChargedMicro
		for _, t := range c.Tries {
			m.CallLatency[k] += t.LatencyMs
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
