package api

import (
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// Each API request is one span named by the route pattern (never the concrete path with IDs) and one metric
// event labelled by the same pattern; unmatched paths are "other".
func TestRequestSpanAndMetricUseRoutePattern(t *testing.T) {
	tr, rec := obstest.Install(t)
	ts := newTestServer(t, nil)
	ts.do("GET", "/tasks/task_0123456789abcdef0123456789abcdef", "", nil) // 404 from the store, route matched
	ts.do("GET", "/status", "", nil)
	ts.do("GET", "/nope/xyz", "", nil)

	var names []string
	for _, s := range tr.Spans() {
		names = append(names, s.Name)
		if !s.Ended {
			t.Errorf("span %s not ended", s.Name)
		}
		for k, v := range s.Attrs {
			if str, ok := v.(string); ok && strings.Contains(str, "task_0123") {
				t.Errorf("span %s attribute %s leaks the concrete path: %q", s.Name, k, str)
			}
		}
	}
	want := []string{"GET /tasks/{id}", "GET /status", "GET other"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("spans = %v, want %v", names, want)
	}
	if got := tr.Spans()[0].Attrs["http.status_code"]; got != int64(404) {
		t.Errorf("status attr = %v", got)
	}
	for _, e := range []string{
		"APIRequest route=/tasks/{id} method=GET status=404",
		"APIRequest route=/status method=GET status=200",
		"APIRequest route=other method=GET status=404",
	} {
		if !rec.Has(e) {
			t.Errorf("missing metric event %q in %v", e, rec.Events())
		}
	}
	// The access log of a traced request carries no trace_id unless the logger is wrapped (cmd wraps it);
	// the plain test logger must still log exactly as before.
	if strings.Contains(ts.logs.String(), "trace_id") {
		t.Error("unwrapped logger must not gain trace_id")
	}
}

func TestOffRecordsNothing(t *testing.T) {
	ts := newTestServer(t, nil)
	if code, _, _ := ts.do("GET", "/status", "", nil); code != 200 {
		t.Fatalf("status = %d", code)
	}
}
