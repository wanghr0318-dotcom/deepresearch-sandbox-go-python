package runner_test

import (
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// With tracing on, init carries the traceparent of the worker.run span (so the worker's Gateway calls join the
// trace), and the run is broken into start / handshake / checkpoint.commit / finalize spans.
func TestTraceparentInInitAndRunSpans(t *testing.T) {
	e := newEnv(t)
	tr, _ := obstest.Install(t)
	c := e.newCase(t, "traced")
	out, p := c.run(t, 0, w(ready(1)), w(checkpoint(2, "cp-1")), h(), w(result(3)))
	noViolation(t, out)
	runs := tr.Named("worker.run")
	if len(runs) != 1 {
		t.Fatalf("worker.run spans: %d", len(runs))
	}
	run := runs[0]
	tp, _ := p.init["traceparent"].(string)
	tc, ok := obs.ParseTraceparent(tp)
	if !ok || tc.TraceID != run.TraceID || tc.ParentID != run.SpanID {
		t.Fatalf("init.traceparent %q does not identify worker.run %s/%s", tp, run.TraceID, run.SpanID)
	}
	if run.Attrs["outcome.class"] != "succeeded" || run.Attrs["attempt.id"] != c.att || !run.Ended {
		t.Errorf("worker.run attrs %v", run.Attrs)
	}
	for _, name := range []string{"worker.start", "worker.handshake", "checkpoint.commit", "worker.finalize"} {
		s := tr.Named(name)
		if len(s) != 1 || s[0].ParentID != run.SpanID || !s[0].Ended || s[0].Failed != "" {
			t.Errorf("%s: %+v", name, s)
		}
	}
	if cp := tr.Named("checkpoint.commit"); len(cp) == 1 && cp[0].Attrs["checkpoint.status"] != "committed" {
		t.Errorf("checkpoint attrs %v", cp[0].Attrs)
	}
}

// Off (default): init is exactly as before — no traceparent key.
func TestNoTraceparentWhenOff(t *testing.T) {
	e := newEnv(t)
	c := e.newCase(t, "untraced")
	out, p := c.run(t, 0, w(ready(1)), w(result(2)))
	noViolation(t, out)
	if _, has := p.init["traceparent"]; has {
		t.Fatalf("init has traceparent with tracing off: %v", p.init)
	}
}
