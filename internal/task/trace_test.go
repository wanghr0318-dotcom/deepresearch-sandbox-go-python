package task

import (
	"context"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// One trace per task: the API span that submitted it → task span → attempt spans → (env.create, worker run,
// env.stop); metrics for ready latency, submit→ready, stop latency, attempt outcomes and verdicts.
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
	h.st.setControl("t1", "run")
	a.Notify()
	r2 := h.nextRun()
	r2.ready()
	r2.finish(success())
	h.waitDone()

	api := tr.Named("POST /tasks")[0]
	tasks := tr.Named("task")
	if len(tasks) != 1 || tasks[0].TraceID != api.TraceID || tasks[0].ParentID != api.SpanID || !tasks[0].Ended {
		t.Fatalf("task span %+v not a child of the API span %+v", tasks, api)
	}
	if tasks[0].Attrs["task.status"] != "succeeded" || tasks[0].Attrs["task.id"] != "t1" {
		t.Errorf("task attrs %v", tasks[0].Attrs)
	}
	attempts := tr.Named("attempt")
	if len(attempts) != 2 {
		t.Fatalf("attempt spans: %d", len(attempts))
	}
	for i, at := range attempts {
		if at.ParentID != tasks[0].SpanID || !at.Ended {
			t.Errorf("attempt %d: %+v", i, at)
		}
	}
	if attempts[0].Attrs["task.status"] != "paused" || attempts[1].Attrs["task.status"] != "succeeded" {
		t.Errorf("attempt verdicts %v / %v", attempts[0].Attrs, attempts[1].Attrs)
	}
	// The runner received the attempt span as parent (worker.run is a child of it).
	for i, run := range []*fkRun{r, r2} {
		tc, ok := obs.ParseTraceparent(run.traceparent)
		if !ok || tc.ParentID != attempts[i].SpanID {
			t.Errorf("run %d traceparent %q, want parent %s", i, run.traceparent, attempts[i].SpanID)
		}
	}
	for _, name := range []string{"admission.acquire", "env.create", "env.stop"} {
		spans := tr.Named(name)
		if len(spans) != 2 {
			t.Errorf("%s spans: %d", name, len(spans))
			continue
		}
		for _, s := range spans {
			if s.TraceID != api.TraceID || !s.Ended {
				t.Errorf("%s outside the task trace or not ended: %+v", name, s)
			}
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
	if n != 1 {
		t.Errorf("TaskStarted recorded %d times (submit→ready is the first ready only)", n)
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
