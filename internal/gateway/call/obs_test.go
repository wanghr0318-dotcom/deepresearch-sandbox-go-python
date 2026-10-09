package call

import (
	"context"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// allowedAttrs is the span attribute allowlist of the Gateway (design §6): IDs, numbers and closed enumerations.
var allowedAttrs = map[string]bool{
	"gateway.kind": true, "task.id": true, "attempt.id": true, "call.id": true, "subrun.id": true,
	"gateway.provider": true, "gateway.model": true, "budget.estimate_micro": true, "cost.actual_micro": true,
	"call.result": true, "http.status_code": true, "try.no": true, "try.outcome": true,
}

// A call retried once: one gateway.call span (child of the caller's span) with two gateway.try children, the
// result and cost recorded, and only allowlisted attributes — the prompt never appears.
func TestCallSpanWithTries(t *testing.T) {
	tr, rec := obstest.Install(t)
	ad := newAdapter("p", step{err: rateLimited()}, step{body: `{"ok":1}`, usage: upstream.Usage{InputTokens: 10, OutputTokens: 20}})
	h := newHarness(t, testLimits(), ad)
	parent, ps := obs.Start(context.Background(), "worker.run")
	r, err := h.c.Invoke(parent, inv("c1", chatBody))
	ps.End()
	if err != nil || r.Status != 200 {
		t.Fatalf("Invoke = %+v, %v", r, err)
	}
	calls, tries := tr.Named("gateway.call"), tr.Named("gateway.try")
	if len(calls) != 1 || len(tries) != 2 {
		t.Fatalf("spans: %d calls, %d tries", len(calls), len(tries))
	}
	c := calls[0]
	if c.ParentID != tr.Named("worker.run")[0].SpanID {
		t.Error("gateway.call is not a child of the caller span")
	}
	for i, ts := range tries {
		if ts.ParentID != c.SpanID || !ts.Ended || ts.Attrs["try.no"] != int64(i+1) {
			t.Errorf("try %d: %+v", i+1, ts)
		}
	}
	if tries[0].Failed == "" || tries[0].Attrs["try.outcome"] != string(upstream.OutcomeRetryable) {
		t.Errorf("first try not failed retryable: %+v", tries[0])
	}
	if c.Attrs["call.result"] != ResultCompleted || c.Attrs["cost.actual_micro"] != int64(50) || c.Attrs["gateway.model"] != "m1" {
		t.Errorf("call attrs %v", c.Attrs)
	}
	for _, s := range append(calls, tries...) {
		for k, v := range s.Attrs {
			if !allowedAttrs[k] {
				t.Errorf("span %s has non-allowlisted attribute %s", s.Name, k)
			}
			if str, ok := v.(string); ok && strings.Contains(str, "secret prompt") {
				t.Errorf("span %s leaks the prompt in %s", s.Name, k)
			}
		}
	}
	for _, e := range []string{
		"UpstreamTry kind=chat provider=p model=m1 outcome=retryable status=429",
		"UpstreamTry kind=chat provider=p model=m1 outcome=ok status=200",
		"GatewayCall kind=chat result=completed",
		"Cost kind=chat provider=p model=m1 micro=50",
	} {
		if !rec.Has(e) {
			t.Errorf("missing %q in %v", e, rec.Events())
		}
	}
	for _, e := range rec.Events() {
		if strings.HasPrefix(e, "ToolCall") {
			t.Errorf("chat counted as a tool call: %s", e)
		}
	}
	// A replay is recorded as replayed and does not create tries.
	if r := h.invoke(t, inv("c1", chatBody)); !r.Replayed {
		t.Fatal("expected replay")
	}
	if !rec.Has("GatewayCall kind=chat result=replayed") || len(tr.Named("gateway.try")) != 2 {
		t.Errorf("replay: %v", rec.Events())
	}
}

// A cache hit and a rejection are visible as call results; search counts as a tool call (not on replay).
func TestCallResultCacheHitAndRejection(t *testing.T) {
	tr, rec := obstest.Install(t)
	fc := &fakeCache{entries: map[string]fakeEntry{}}
	h := newCacheHarness(t, fc)
	h.seed(t, fc, `{"results":["cached"]}`)
	if r := h.invoke(t, searchInv("c1")); r.Status != 200 {
		t.Fatalf("hit %+v", r)
	}
	if got := tr.Named("gateway.call")[0].Attrs["call.result"]; got != ResultCacheHit {
		t.Errorf("call.result = %v", got)
	}
	h.invoke(t, searchInv("c1")) // replay: not another tool call
	r := h.invoke(t, Invoke{TaskID: "t1", AttemptID: "a1", EnvID: "e1", CallID: "c2", Kind: upstream.KindSearch, Body: []byte(`{"q":1,"q":2}`)})
	if r.Code != CodeDuplicateJSONKey {
		t.Fatalf("rejection %+v", r)
	}
	last := tr.Named("gateway.call")[2]
	if last.Attrs["call.result"] != CodeDuplicateJSONKey || last.Failed != CodeDuplicateJSONKey {
		t.Errorf("rejected call span %+v", last)
	}
	n := 0
	for _, e := range rec.Events() {
		if e == "ToolCall task=t1 kind=search" {
			n++
		}
	}
	if n != 2 { // the hit and the rejected request; not the replay
		t.Errorf("tool calls = %d, events %v", n, rec.Events())
	}
}
