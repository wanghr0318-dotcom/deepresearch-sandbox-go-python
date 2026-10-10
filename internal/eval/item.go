package eval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// One work item: submit and wait (submitAndWait), collect the server-side record (collect), grade (grade).

// terminalGrace bounds how long the runner waits for task_terminal after cancelling a timed-out task.
const terminalGrace = 60 * time.Second

// spec builds the POST /tasks spec for a task.
func (r *run) spec(t *Task) (json.RawMessage, error) {
	ev := map[string]any{"kind": t.Kind, "task_id": t.ID, "suite": r.o.Suite.Name}
	if t.Kind == KindCoding {
		return r.codingSpec(t, ev)
	}
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

func (r *run) codingSpec(t *Task, ev map[string]any) (json.RawMessage, error) {
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
}

func (r *run) runItem(ctx context.Context, it workItem) *Trajectory {
	t := it.task
	tr := &Trajectory{Schema: SchemaTrajectory, RunID: r.o.RunID, Suite: r.o.Suite.Name, TaskID: t.ID, Kind: t.Kind,
		Rep: it.rep, Agent: r.o.Agent, Events: []Event{}, Calls: []Call{}, Grades: []Grade{}}
	if t.Kind == KindResearch {
		tr.Agent = "deepresearch"
	}
	timedOut, cat, err := r.submitAndWait(ctx, it, tr)
	if err == nil {
		cat, err = "infra_error", r.collect(ctx, tr)
	}
	if err != nil {
		tr.Outcome, tr.Error = Outcome{Verdict: VerdictError, Category: cat}, err.Error()
		return tr
	}
	r.grade(ctx, t, tr)
	if timedOut {
		tr.Outcome = Outcome{Verdict: VerdictFail, Category: "timeout"}
	}
	return tr
}

// submitAndWait creates the server task and reads its event stream until task_terminal. On timeout or
// interruption it cancels the task and waits (bounded) for the terminal event. On error it returns the
// failure category.
func (r *run) submitAndWait(ctx context.Context, it workItem, tr *Trajectory) (timedOut bool, category string, err error) {
	t := it.task
	spec, err := r.spec(t)
	if err != nil {
		return false, "invalid_task", err
	}
	tr.SubmittedAt = r.o.Now().UTC()
	rid := requestID(r.o.Nonce, r.o.RunID, t.ID, it.rep)
	id, err := r.o.Client.CreateTask(ctx, rid, spec, r.o.Suite.EffectiveLimits(t))
	if err != nil {
		return false, "infra_error", err
	}
	tr.ServerTaskID = id

	var mu sync.Mutex
	collect := func(ev Event) {
		mu.Lock()
		tr.Events = append(tr.Events, ev)
		mu.Unlock()
	}
	wctx, cancel := context.WithTimeout(ctx, r.o.Suite.EffectiveTimeout(t, r.o.DefaultTimeout))
	werr := r.o.Client.Events(wctx, id, collect)
	cancel()
	if werr != nil {
		timedOut = errors.Is(werr, context.DeadlineExceeded) && ctx.Err() == nil
		reason := "eval interrupted"
		if timedOut {
			reason = "eval timeout"
		}
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), terminalGrace)
		_ = r.o.Client.Cancel(cctx, id, rid+"-cancel", reason)
		mu.Lock()
		tr.Events = tr.Events[:0] // the stream is re-read from the start
		mu.Unlock()
		_ = r.o.Client.Events(cctx, id, collect)
		ccancel()
	}
	tr.TerminalAt = r.o.Now().UTC()
	tr.LatencyMs = tr.TerminalAt.Sub(tr.SubmittedAt).Milliseconds()
	return timedOut, "", nil
}

// collect reads the server-side record of the task: inspect (calls, attempts, ledger), the pinned result and
// the artifacts the graders need.
func (r *run) collect(ctx context.Context, tr *Trajectory) error {
	ictx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	in, _, err := r.o.Client.Inspect(ictx, tr.ServerTaskID)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	tr.Status, tr.StatusReason = in.Task.Status, in.Task.StatusReason
	if in.Calls != nil {
		tr.Calls = in.Calls
	}
	tr.Attempts, tr.Checkpoints, tr.Subruns, tr.Budget = in.Attempts, in.Checkpoints, in.Subruns, in.Budget
	tr.Metrics = metricsOf(in)
	if res, err := r.o.Client.Result(ictx, tr.ServerTaskID); err == nil {
		tr.Result = res
	}
	return nil
}

// grade fetches the artifacts, applies the deterministic graders and, when configured, the judge.
func (r *run) grade(ctx context.Context, t *Task, tr *Trajectory) {
	gctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	var answer string
	if t.Kind == KindCoding {
		var art *CodingArtifact
		if b, ok := r.artifact(gctx, tr, "eval"); ok {
			tr.Artifact = json.RawMessage(b)
			if a, err := parseCodingArtifact(b); err == nil {
				art = a
			} else {
				tr.Error = err.Error()
			}
		}
		tr.Grades = GradeCoding(t, tr.Status, tr.StatusReason, art)
		if b, ok := r.artifact(gctx, tr, "solution"); ok {
			answer = string(b)
		}
	} else {
		if b, ok := r.artifact(gctx, tr, reportArtifactID(t)); ok {
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
		tr.Grades = append(tr.Grades, r.o.Judge.Grade(gctx, t, answer))
	}
	tr.Outcome = Decide(tr.Grades)
}
