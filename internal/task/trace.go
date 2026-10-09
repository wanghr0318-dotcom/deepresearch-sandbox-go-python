package task

import (
	"context"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// actorTrace is the actor's observability state (only touched by the actor goroutine). It is inert when
// observability is off: every method is then a few nil checks and no-op calls.
//
//   - run span ("task" or "turn"): one per run of the task — from the first effect of the run (slot request or
//     attempt creation) to the verdict that stops it running (paused, awaiting_input or terminal; a fault retry
//     stays in the same run). It is parented to the API request that started the run when this process saw it
//     (create, resume, continue or answer — obs submit registry), so "one user action = one trace"; a paused
//     task does not keep a span open while it waits;
//   - attempt span: CreateAttempt committed → verdict committed; the actor's async effects for the attempt
//     (environment creation, worker run, stop) run with the attempt span in their context, so the runner's
//     worker.run span, the Gateway binding and every Gateway call of the attempt are its descendants;
//   - metrics: attempt ready latency, submit→ready (per run), stop latency, attempt outcomes, verdicts.
//
// Durations use the wall clock (time.Now), not the actor's injectable Clock.
type actorTrace struct {
	on        bool
	taskID    string
	sessionID string
	kind      string // "task" or "turn"
	base      context.Context
	runs      int64

	ctx      context.Context // current run span (valid while span != nil)
	span     obs.Span
	submitAt time.Time // zero: run not started by a request seen by this process
	started  bool      // first worker ready of the current run observed

	attempts map[string]*attemptTrace
	// ended keeps the context of attempts whose span has ended: effects that complete after the verdict (the
	// session handoff of a turn) stay in the attempt's trace instead of opening a new run.
	ended map[string]context.Context

	stopDesired string // pause/cancel first observed (stop latency start)
	stopAt      time.Time
}

type attemptTrace struct {
	ctx       context.Context
	span      obs.Span
	createdAt time.Time
}

// begin records the task after it has been loaded; the run span starts lazily (ensureRun).
func (t *actorTrace) begin(base context.Context, taskID string, s State) {
	if !obs.Enabled() {
		return
	}
	t.on, t.base, t.taskID, t.sessionID = true, base, taskID, s.SessionID
	t.kind = "task"
	if s.SessionID != "" {
		t.kind = "turn"
	}
	t.attempts, t.ended = map[string]*attemptTrace{}, map[string]context.Context{}
	if s.Desired == "pause" || s.Desired == "cancel" {
		t.stopDesired, t.stopAt = s.Desired, time.Now()
	}
}

// ensureRun starts the run span if none is open.
func (t *actorTrace) ensureRun() {
	if !t.on || t.span != nil {
		return
	}
	parent := t.base
	t.submitAt, t.started = time.Time{}, false
	if tp, at, ok := obs.TakeSubmit(t.taskID); ok {
		t.submitAt = at
		parent = obs.WithRemoteParent(t.base, tp)
	}
	t.runs++
	attrs := []obs.Attr{obs.Str("task.id", t.taskID), obs.Str("task.kind", t.kind), obs.Int("task.run", t.runs)}
	if t.sessionID != "" {
		attrs = append(attrs, obs.Str("session.id", t.sessionID))
	}
	t.ctx, t.span = obs.Start(parent, t.kind, attrs...)
}

// endRun ends the open attempt spans and the run span with status.
func (t *actorTrace) endRun(status, failCode string) {
	for id, at := range t.attempts {
		at.span.Fail("run_ended")
		at.span.End()
		t.ended[id] = at.ctx
		delete(t.attempts, id)
	}
	if t.span == nil {
		return
	}
	t.span.SetAttrs(obs.Str("task.status", status))
	if failCode != "" {
		t.span.Fail(failCode)
	}
	t.span.End()
	t.span, t.ctx = nil, nil
}

// end is called when the actor exits.
func (t *actorTrace) end(status string) {
	if t.on {
		t.endRun(status, "")
	}
}

// taskCtx returns base carrying the run span (starting it if needed).
func (t *actorTrace) taskCtx(base context.Context) context.Context {
	if !t.on {
		return base
	}
	t.ensureRun()
	return obs.Carry(base, t.ctx)
}

// attemptCtx returns base carrying the attempt span (falls back to the run span).
func (t *actorTrace) attemptCtx(base context.Context, attemptID string) context.Context {
	if at := t.attempts[attemptID]; at != nil {
		return obs.Carry(base, at.ctx)
	}
	if ctx := t.ended[attemptID]; ctx != nil {
		return obs.Carry(base, ctx)
	}
	return t.taskCtx(base)
}

func (t *actorTrace) attemptCreated(na NewAttempt) {
	if !t.on {
		return
	}
	t.ensureRun()
	ctx, span := obs.Start(t.ctx, "attempt", obs.Str("attempt.id", na.AttemptID), obs.Int("attempt.no", na.AttemptNo),
		obs.Str("env.id", na.EnvID), obs.Str("retry", string(na.Retry)))
	t.attempts[na.AttemptID] = &attemptTrace{ctx: ctx, span: span, createdAt: time.Now()}
}

func (t *actorTrace) ready(attemptID string) {
	at := t.attempts[attemptID]
	if at == nil {
		return
	}
	now := time.Now()
	at.span.Event("worker_ready")
	m := obs.M()
	m.AttemptReady(t.kind, now.Sub(at.createdAt))
	if !t.started {
		t.started = true
		if !t.submitAt.IsZero() {
			m.TaskStarted(t.kind, now.Sub(t.submitAt))
		}
	}
}

func (t *actorTrace) attemptFinished(attemptID string, o Outcome) {
	if !t.on {
		return
	}
	if at := t.attempts[attemptID]; at != nil {
		at.span.SetAttrs(obs.Str("outcome.class", o.Class))
	}
	obs.M().AttemptFinished(t.kind, o.Class)
}

// controlChanged notes the first pause/cancel observed (start of the stop latency).
func (t *actorTrace) controlChanged(desired string) {
	if !t.on {
		return
	}
	switch {
	case (desired == "pause" || desired == "cancel") && t.stopDesired == "":
		t.stopDesired, t.stopAt = desired, time.Now()
		if t.span != nil {
			t.span.Event("stop_requested", obs.Str("desired", desired))
		}
	case desired == "run":
		t.stopDesired, t.stopAt = "", time.Time{}
	}
}

// verdict ends the attempt span with the committed verdict and records verdict metrics. A verdict that puts the
// task back to queued (fault retry) continues the run; any other verdict ends it.
func (t *actorTrace) verdict(v Verdict) {
	if !t.on {
		return
	}
	if at := t.attempts[v.AttemptID]; at != nil {
		at.span.SetAttrs(obs.Str("attempt.status", v.AttemptStatus), obs.Str("outcome.class", v.OutcomeClass),
			obs.Str("task.status", v.TaskStatus))
		if !benignClass(v.OutcomeClass) {
			at.span.Fail(v.OutcomeClass)
		}
		at.span.End()
		t.ended[v.AttemptID] = at.ctx
		delete(t.attempts, v.AttemptID)
	}
	if v.TaskStatus == "queued" || v.TaskStatus == "" {
		return
	}
	m := obs.M()
	m.TaskFinished(t.taskID, t.kind, v.TaskStatus)
	if t.stopDesired != "" && (v.TaskStatus == "paused" || v.TaskStatus == "cancelled") {
		m.StopCompleted(t.kind, t.stopDesired, time.Since(t.stopAt))
		t.stopDesired, t.stopAt = "", time.Time{}
	}
	fail := ""
	if v.TaskStatus == "failed" {
		fail = v.OutcomeClass
	}
	t.endRun(v.TaskStatus, fail)
}

// benignClass: outcomes that are not errors on a span.
func benignClass(c string) bool {
	return c == ClassSucceeded || c == ClassPaused || c == ClassCancelled || c == "awaiting_input"
}
