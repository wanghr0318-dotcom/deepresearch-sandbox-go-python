package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// One trace per run: the API request that started the run (create, resume) → run span ("task") → attempt
// spans → (env.create, worker run, env.stop). A pause ends the run; the resume starts a new run under the resume
// request. Metrics: ready latency, submit→ready per run, stop latency, attempt outcomes and verdicts.
func TestActorTraceAndMetrics(t *testing.T) {
	tr, rec := obstest.Install(t)
	apiCtx, apiSpan := obs.Start(context.Background(), "POST /tasks")
	obs.NoteSubmit(apiCtx, "t1")
	apiSpan.End()

	h := newActorHarness(t, queuedTask("t1"))
	a := h.spawn("t1")
	r := h.nextRun()
	r.ready()
	h.st.setControl("t1", "pause")
	a.Notify()
	if c := r.control(); c.Kind != "pause" {
		t.Fatalf("control %+v", c)
	}
	r.finish(Outcome{Class: ClassPaused, ProposalKind: "paused"})
	h.waitFor("paused", func() bool { return h.st.task("t1").Status == "paused" && h.log.count("release:1") == 1 })
	h.waitFor("run span ended", func() bool { s := tr.Named("task"); return len(s) == 1 && s[0].Ended })

	resumeCtx, resumeSpan := obs.Start(context.Background(), "POST /tasks/{id}/resume")
	obs.NoteSubmit(resumeCtx, "t1")
	resumeSpan.End()
	h.st.setControl("t1", "run")
	a.Notify()
	r2 := h.nextRun()
	r2.ready()
	r2.finish(success())
	h.waitDone()

	api, resume := tr.Named("POST /tasks")[0], tr.Named("POST /tasks/{id}/resume")[0]
	runs := tr.Named("task")
	if len(runs) != 2 {
		t.Fatalf("run spans: %d", len(runs))
	}
	if runs[0].ParentID != api.SpanID || runs[0].TraceID != api.TraceID || runs[0].Attrs["task.status"] != "paused" ||
		runs[0].Attrs["task.run"] != int64(1) {
		t.Errorf("first run %+v (api %s)", runs[0], api.SpanID)
	}
	if runs[1].ParentID != resume.SpanID || runs[1].TraceID != resume.TraceID || runs[1].Attrs["task.status"] != "succeeded" ||
		runs[1].Attrs["task.run"] != int64(2) || !runs[1].Ended {
		t.Errorf("second run %+v (resume %s)", runs[1], resume.SpanID)
	}
	attempts := tr.Named("attempt")
	if len(attempts) != 2 || attempts[0].ParentID != runs[0].SpanID || attempts[1].ParentID != runs[1].SpanID {
		t.Fatalf("attempt spans %+v", attempts)
	}
	for i, run := range []*fkRun{r, r2} {
		tc, ok := obs.ParseTraceparent(run.traceparent)
		if !ok || tc.ParentID != attempts[i].SpanID {
			t.Errorf("run %d traceparent %q, want parent %s", i, run.traceparent, attempts[i].SpanID)
		}
	}
	for _, name := range []string{"admission.acquire", "env.create", "env.stop"} {
		spans := tr.Named(name)
		if len(spans) != 2 || spans[0].TraceID != api.TraceID || spans[1].TraceID != resume.TraceID || !spans[0].Ended || !spans[1].Ended {
			t.Errorf("%s spans %+v", name, spans)
		}
	}
	for _, e := range []string{
		"AttemptReady kind=task", "TaskStarted kind=task", "StopCompleted kind=task desired=pause",
		"AttemptFinished kind=task class=paused", "AttemptFinished kind=task class=succeeded",
		"RunEnded kind=task status=paused", "RunEnded kind=task status=succeeded",
		"TaskFinished task=t1 kind=task status=succeeded",
	} {
		if !rec.Has(e) {
			t.Errorf("missing %q in %v", e, rec.Events())
		}
	}
	if rec.Has("TaskFinished task=t1 kind=task status=paused") {
		t.Error("a pause is not a finished task")
	}
	n := 0
	for _, e := range rec.Events() {
		if e == "TaskStarted kind=task" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("TaskStarted recorded %d times, want once per run", n)
	}
}

// Off: the runner gets no span and nothing is recorded.
func TestActorTraceOff(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	r.ready()
	r.finish(success())
	h.waitDone()
	if r.traceparent != "" {
		t.Errorf("traceparent %q with observability off", r.traceparent)
	}
}

// A session turn hands its incarnation back after the verdict: the handoff span stays in the turn's trace (child of
// the ended attempt) and does not open a second run.
func TestActorTraceTurnHandoffStaysInTrace(t *testing.T) {
	tr, _ := obstest.Install(t)
	h := newActorHarness(t, turnTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	r.ready()
	r.finish(turnSuccess())
	h.waitDone()
	runs, handoffs, attempts := tr.Named("turn"), tr.Named("session.handoff"), tr.Named("attempt")
	if len(runs) != 1 || len(handoffs) != 1 || len(attempts) != 1 {
		t.Fatalf("runs %d, handoffs %d, attempts %d", len(runs), len(handoffs), len(attempts))
	}
	if handoffs[0].ParentID != attempts[0].SpanID || handoffs[0].TraceID != runs[0].TraceID {
		t.Errorf("handoff %+v not under attempt %s", handoffs[0], attempts[0].SpanID)
	}
}

// A queued task cancelled before any attempt (ApplyControl queued → cancelled, no verdict, no run opened) is
// counted as finished, leaves no span open, and the stop latency starts at the accepted request (obs.NoteStop).
func TestActorTraceApplyControlEndsRun(t *testing.T) {
	tr, rec := obstest.Install(t)
	ts := queuedTask("t1")
	ts.Desired, ts.ControlVersion = "cancel", 2 // cancel accepted, not yet applied
	h := newActorHarness(t, ts)
	obs.NoteStop("t1")
	h.spawn("t1")
	h.waitDone()
	if st := h.st.task("t1").Status; st != "cancelled" {
		t.Fatalf("status %s", st)
	}
	for _, e := range []string{"TaskFinished task=t1 kind=task status=cancelled",
		"StopCompleted kind=task desired=cancel"} {
		if !rec.Has(e) {
			t.Errorf("missing %q in %v", e, rec.Events())
		}
	}
	for _, s := range tr.Spans() {
		if !s.Ended {
			t.Errorf("span %s left open", s.Name)
		}
	}
	if _, ok := obs.TakeStop("t1"); ok {
		t.Error("stop stamp not consumed")
	}
}

// A turn failed while queued (session unavailable; FailTurn, no attempt) ends its run and counts as failed.
func TestActorTraceFailTurnEndsRun(t *testing.T) {
	tr, rec := obstest.Install(t)
	h := newActorHarness(t, turnTask("t1"))
	h.sess.grantErr = []error{fmt.Errorf("session s1: %w", ErrSessionUnavailable)}
	h.spawn("t1")
	h.waitDone()
	if !rec.Has("TaskFinished task=t1 kind=turn status=failed") || !rec.Has("RunEnded kind=turn status=failed") {
		t.Errorf("events %v", rec.Events())
	}
	runs := tr.Named("turn")
	if len(runs) != 1 || !runs[0].Ended || runs[0].Failed != ReasonSessionUnavailable {
		t.Errorf("run spans %+v", runs)
	}
}

// Submissions noted before a run ended are dropped with it: a later run does not adopt a stale parent.
func TestActorTraceDropsStaleSubmission(t *testing.T) {
	tr, _ := obstest.Install(t)
	h := newActorHarness(t, queuedTask("t1"))
	a := h.spawn("t1")
	r := h.nextRun()
	r.ready()
	stale, sp := obs.Start(context.Background(), "stale")
	obs.NoteSubmit(stale, "t1") // e.g. a resume replay racing with the running task
	sp.End()
	h.st.setControl("t1", "pause")
	a.Notify()
	_ = r.control()
	r.finish(Outcome{Class: ClassPaused, ProposalKind: "paused"})
	h.waitFor("paused", func() bool { return h.st.task("t1").Status == "paused" && h.log.count("release:1") == 1 })
	h.st.setControl("t1", "run")
	a.Notify()
	r2 := h.nextRun()
	r2.ready()
	r2.finish(success())
	h.waitDone()
	runs := tr.Named("task")
	if len(runs) != 2 || runs[1].ParentID != "" {
		t.Errorf("second run adopted a stale submission: %+v", runs)
	}
}

// A paused task cancelled via ApplyControl (no run open) counts as finished but not as a second run end.
func TestActorTracePausedCancelNoSecondRunEnd(t *testing.T) {
	_, rec := obstest.Install(t)
	ts := queuedTask("t1")
	ts.Status, ts.Desired, ts.ControlVersion = "paused", "cancel", 2
	h := newActorHarness(t, ts)
	h.spawn("t1")
	h.waitDone()
	if st := h.st.task("t1").Status; st != "cancelled" {
		t.Fatalf("status %s", st)
	}
	if !rec.Has("TaskFinished task=t1 kind=task status=cancelled") {
		t.Errorf("events %v", rec.Events())
	}
	for _, e := range rec.Events() {
		if e == "RunEnded kind=task status=cancelled" {
			t.Errorf("run end counted without an open run: %v", rec.Events())
		}
	}
}

// The stale-submission cutoff is when the ending store write was issued: a resume accepted while the write was in
// flight survives and parents the next run.
func TestStatusCommittedKeepsSubmissionAcceptedDuringWrite(t *testing.T) {
	obstest.Install(t)
	tr := &actorTrace{}
	tr.begin(context.Background(), "t-cut", State{TaskID: "t-cut"})
	tr.ensureRun()
	issued := time.Now()
	time.Sleep(2 * time.Millisecond)
	ctx, sp := obs.Start(context.Background(), "POST /tasks/{id}/resume")
	obs.NoteSubmit(ctx, "t-cut") // accepted after the paused write was issued
	sp.End()
	tr.statusCommitted("paused", "", issued)
	if _, _, ok := obs.TakeSubmit("t-cut"); !ok {
		t.Fatal("a submission accepted after the write was issued was dropped")
	}
	obs.NoteSubmit(ctx, "t-cut")
	tr.ensureRun()
	tr.statusCommitted("paused", "", time.Now()) // noted before this write: stale
	if _, _, ok := obs.TakeSubmit("t-cut"); ok {
		t.Fatal("a stale submission survived the run end")
	}
}
