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

// The worker's traceparent header is untrusted: it is used as the call's parent only when it belongs to the
// bound attempt's trace; otherwise the call is parented to the attempt span that Bind was called with.
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
	tc, _ := obs.ParseTraceparent(attemptTP)
	sameTrace := "00-" + tc.TraceID + "-1111111111111111-01"
	otherTrace := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for i, h := range []string{"", sameTrace, otherTrace, "garbage", strings.ToUpper(sameTrace)} {
		hdr := callHdr("c" + string(rune('0'+i)))
		if h != "" {
			hdr[HeaderTraceparent] = h
		}
		if r := do(t, path, "POST", "/v1/chat/completions", strings.NewReader(`{}`), hdr); r.status != 200 {
			t.Fatalf("request %d: %d %s", i, r.status, r.body)
		}
	}
	want := []string{attemptTP, sameTrace, attemptTP, attemptTP, attemptTP}
	mu.Lock()
	defer mu.Unlock()
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d: parent %q, want %q", i, got[i], want[i])
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
