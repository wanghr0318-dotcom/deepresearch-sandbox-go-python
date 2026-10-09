package edge

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs/obstest"
)

// The worker's traceparent header is untrusted: it only selects the parent. The call is parented to the host's own
// record of the worker.run span (host flags) when the header names exactly that span in the attempt's trace;
// otherwise the header is ignored and the call is parented to the attempt span that Bind was called with.
func TestTraceparentHeaderValidatedAgainstAttemptTrace(t *testing.T) {
	obstest.Install(t)
	var mu sync.Mutex
	var got []string
	calls := &fakeCalls{invoke: func(ctx context.Context, _ call.Invoke) (call.Result, error) {
		mu.Lock()
		got = append(got, obs.Traceparent(ctx))
		mu.Unlock()
		return call.Result{Body: []byte(`{}`), BlobSHA256: testSHA, Status: 200}, nil
	}}
	e := newEdge(t, Config{}, calls)
	actx, sp := obs.Start(context.Background(), "attempt")
	defer sp.End()
	path, err := e.Bind(actx, "a1", "e1")
	if err != nil {
		t.Fatal(err)
	}
	attemptTP := obs.Traceparent(actx)
	wctx, wsp := obs.Start(actx, "worker.run")
	defer wsp.End()
	workerTP := obs.Traceparent(wctx) // sampled (flags 01)
	defer obs.ExpectWorker("a1", workerTP)()
	w, _ := obs.ParseTraceparent(workerTP)
	unsampled := "00-" + w.TraceID + "-" + w.ParentID + "-00"
	madeUp := "00-" + w.TraceID + "-1111111111111111-01"
	otherTrace := "00-4bf92f3577b34da6a3ce929d0e0e4736-" + w.ParentID + "-01"
	cases := []struct{ header, want string }{
		{"", attemptTP},
		{workerTP, workerTP},
		{unsampled, workerTP}, // the worker cannot unsample: the host's flags are used
		{madeUp, attemptTP},   // a parent the host never handed out is ignored
		{otherTrace, attemptTP},
		{"garbage", attemptTP},
		{strings.ToUpper(workerTP), attemptTP},
	}
	for i, c := range cases {
		hdr := callHdr("c" + string(rune('0'+i)))
		if c.header != "" {
			hdr[HeaderTraceparent] = c.header
		}
		if r := do(t, path, "POST", "/v1/chat/completions", strings.NewReader(`{}`), hdr); r.status != 200 {
			t.Fatalf("request %d: %d %s", i, r.status, r.body)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range cases {
		if got[i] != c.want {
			t.Errorf("header %q: parent %q, want %q", c.header, got[i], c.want)
		}
	}
}

// With tracing off the call context carries nothing and the header is ignored.
func TestTraceparentIgnoredWhenOff(t *testing.T) {
	var parent string
	calls := &fakeCalls{invoke: func(ctx context.Context, _ call.Invoke) (call.Result, error) {
		parent = obs.Traceparent(ctx)
		return call.Result{Body: []byte(`{}`), BlobSHA256: testSHA, Status: 200}, nil
	}}
	e := newEdge(t, Config{}, calls)
	path := bind(t, e, "a1", "e1")
	hdr := callHdr("c1")
	hdr[HeaderTraceparent] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if r := do(t, path, "POST", "/v1/chat/completions", strings.NewReader(`{}`), hdr); r.status != 200 {
		t.Fatalf("%d", r.status)
	}
	if parent != "" {
		t.Errorf("parent = %q with tracing off", parent)
	}
}
