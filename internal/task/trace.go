package task

import (
	"context"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// actorTrace is the actor's observability state (only touched by the actor goroutine). It is inert when
// observability is off: every method is then a few nil checks and no-op calls.
//
//   - task span ("task" or "turn"): from load to actor exit, parented to the API request that submitted the
//     task when that request was seen by this process (obs submit registry);
//   - attempt span: CreateAttempt committed → verdict committed; the actor's async effects for the attempt
//     (environment creation, worker run, stop) run with the attempt span in their context, so the runner's
//     worker.run span, the Gateway binding and every Gateway call of the attempt are its descendants;
//   - metrics: attempt ready latency, submit→ready, stop latency, attempt outcomes, verdicts.
//
// Durations use the wall clock (time.Now), not the actor's injectable Clock.
type actorTrace struct {
	kind     string // "task" or "turn"
	ctx      context.Context
	span     obs.Span
	submitAt time.Time // zero: submitted before this process started (or observability was off)
	started  bool      // first worker ready observed

	attempts map[string]*attemptTrace

	stopDesired string // pause/cancel first observed while running (stop latency start)
	stopAt      time.Time
}

type attemptTrace struct {
	ctx       context.Context
	span      obs.Span
	createdAt time.Time
}

// begin starts the task span after the task has been loaded.
func (t *actorTrace) begin(base context.Context, taskID string, s State) {
	t.kind = "task"
	if s.SessionID != "" {
		t.kind = "turn"
	}
	t.attempts = map[string]*attemptTrace{}
	t.ctx = base
	if !obs.Enabled() {
		return // nothing to record; spans and metrics below would be no-ops
	}
	if tp, at, ok := obs.TakeSubmit(taskID); ok {
		t.submitAt = at
		t.ctx = obs.WithRemoteParent(base, tp)
	}
	attrs := []obs.Attr{obs.Str("task.id", taskID), obs.Str("task.kind", t.kind)}
	if s.SessionID != "" {
		attrs = append(attrs, obs.Str("session.id", s.SessionID))
	}
	t.ctx, t.span = obs.Start(t.ctx, t.kind, attrs...)
	if s.Desired == "pause" || s.Desired == "cancel" {
		t.stopDesired, t.stopAt = s.Desired, time.Now()
	}
}

// end ends open attempt spans and the task span with the last known status.
func (t *actorTrace) end(status string) {
	if t.span == nil {
		return
	}
	for id, at := range t.attempts {
		at.span.Fail("actor_exited")
		at.span.End()
		delete(t.attempts, id)
	}
	t.span.SetAttrs(obs.Str("task.status", status))
	t.span.End()
	t.span = nil
}

// taskCtx returns base carrying the task span (effects not tied to an attempt, e.g. admission).
func (t *actorTrace) taskCtx(base context.Context) context.Context {
	return obs.Carry(base, t.ctx)
}

// attemptCtx returns base carrying the attempt span (falls back to the task span).
func (t *actorTrace) attemptCtx(base context.Context, attemptID string) context.Context {
	if at := t.attempts[attemptID]; at != nil {
		return obs.Carry(base, at.ctx)
	}
	return t.taskCtx(base)
}

func (t *actorTrace) attemptCreated(na NewAttempt) {
	if t.attempts == nil {
		return
	}
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
	if at := t.attempts[attemptID]; at != nil {
		at.span.SetAttrs(obs.Str("outcome.class", o.Class))
	}
	if t.attempts != nil {
		obs.M().AttemptFinished(t.kind, o.Class)
	}
}

// controlChanged notes the first pause/cancel observed (start of the stop latency).
func (t *actorTrace) controlChanged(desired string) {
	if (desired == "pause" || desired == "cancel") && t.stopDesired == "" {
		t.stopDesired, t.stopAt = desired, time.Now()
		if t.span != nil {
			t.span.Event("stop_requested", obs.Str("desired", desired))
		}
	} else if desired == "run" {
		t.stopDesired, t.stopAt = "", time.Time{}
	}
}

// verdict ends the attempt span with the committed verdict and records verdict metrics. A verdict that puts the
// task back to queued (retry) does not finish the task.
func (t *actorTrace) verdict(taskID string, v Verdict) {
	if t.attempts == nil {
		return
	}
	if at := t.attempts[v.AttemptID]; at != nil {
		at.span.SetAttrs(obs.Str("attempt.status", v.AttemptStatus), obs.Str("outcome.class", v.OutcomeClass),
			obs.Str("task.status", v.TaskStatus))
		if v.AttemptStatus != "succeeded" && v.OutcomeClass != ClassPaused && v.OutcomeClass != ClassCancelled &&
			v.OutcomeClass != ClassSucceeded {
			at.span.Fail(v.OutcomeClass)
		}
		at.span.End()
		delete(t.attempts, v.AttemptID)
	}
	if v.TaskStatus == "queued" || v.TaskStatus == "" {
		return
	}
	m := obs.M()
	m.TaskFinished(taskID, t.kind, v.TaskStatus)
	if t.stopDesired != "" && (v.TaskStatus == "paused" || v.TaskStatus == "cancelled") {
		m.StopCompleted(t.kind, t.stopDesired, time.Since(t.stopAt))
		t.stopDesired, t.stopAt = "", time.Time{}
	}
	if t.span != nil {
		t.span.SetAttrs(obs.Str("task.status", v.TaskStatus))
	}
}

// startSpan starts a child span of the attempt (or task) span for an async effect.
func (t *actorTrace) startSpan(base context.Context, attemptID, name string, attrs ...obs.Attr) (context.Context, obs.Span) {
	return obs.Start(t.attemptCtx(base, attemptID), name, attrs...)
}
