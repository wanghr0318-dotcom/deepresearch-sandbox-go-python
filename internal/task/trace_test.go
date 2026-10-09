package task

import (
	"context"
	"testing"

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
		"TaskFinished task=t1 kind=task status=paused", "TaskFinished task=t1 kind=task status=succeeded",
	} {
		if !rec.Has(e) {
			t.Errorf("missing %q in %v", e, rec.Events())
		}
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
