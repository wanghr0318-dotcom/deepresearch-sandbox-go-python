package app

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// End to end through the HTTP API with observability on: one trace from POST /tasks through the task actor, the
// attempt and the runner; verdict and request metrics; scrape-time gauges from the admission gate.
func TestObservabilityEndToEnd(t *testing.T) {
	tr, rec := obstest.Install(t)
	h := newHarness(t)
	h.start(testConfig(), task.SystemClock())
	base := h.waitAddr()
	id := submit(t, base, "r-obs", `{"steps":[]}`)
	eventually(t, "task terminal", nil, func() bool {
		st, b := httpDo(t, "GET", base+"/tasks/"+id, "")
		var v struct{ Status string }
		if st != http.StatusOK || json.Unmarshal(b, &v) != nil {
			t.Fatalf("GET = %d %s", st, b)
		}
		return task.IsTerminal(v.Status)
	})
	eventually(t, "task span ended", nil, func() bool {
		s := tr.Named("task")
		return len(s) == 1 && s[0].Ended
	})
	post := tr.Named("POST /tasks")
	if len(post) != 1 {
		t.Fatalf("POST /tasks spans: %d", len(post))
	}
	trace := post[0].TraceID
	for _, name := range []string{"task", "attempt", "admission.acquire", "env.create", "worker.run", "worker.start", "worker.handshake"} {
		s := tr.Named(name)
		if len(s) == 0 || s[0].TraceID != trace {
			t.Errorf("%s not in the task trace: %+v", name, s)
		}
	}
	if !rec.Has("TaskFinished task="+id+" kind=task status=succeeded") || !rec.Has("APIRequest route=/tasks method=POST status=201") {
		t.Errorf("metric events %v", rec.Events())
	}
	gauges := map[string]float64{}
	for _, g := range rec.Gauges() {
		if len(g.Labels) == 0 {
			gauges[g.Name] = g.Value
		}
	}
	if gauges["run_slots_capacity"] != 4 {
		t.Errorf("gauges %v", gauges)
	}
	h.cancel()
	if err := h.wait(); err != nil {
		t.Fatal(err)
	}
}
