// Package obstest provides in-memory obs.Tracer and obs.Recorder implementations for tests of the
// instrumented packages (stdlib only, so packages restricted to the standard library can use it).
package obstest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// Span is a recorded span.
type Span struct {
	Name              string
	TraceID, SpanID   string
	ParentID          string // "" for a root span
	Attrs             map[string]any
	Events            []string
	Failed            string
	Ended             bool
	Start, EndTime    time.Time
	RemoteParentTrace string // set when the parent came from WithRemoteParent
}

// Tracer records spans in memory.
type Tracer struct {
	mu    sync.Mutex
	spans []*Span
}

type ctxKey struct{}

type spanCtx struct {
	traceID, spanID string
	remote          bool
}

type live struct {
	t *Tracer
	s *Span
}

func (l live) SetAttrs(attrs ...obs.Attr) {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	for _, a := range attrs {
		l.s.Attrs[a.Key] = a.Value
	}
}

func (l live) Event(name string, attrs ...obs.Attr) {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	l.s.Events = append(l.s.Events, name)
	for _, a := range attrs {
		l.s.Attrs[name+"."+a.Key] = a.Value
	}
}

func (l live) Fail(code string) {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	l.s.Failed = code
}

func (l live) Rename(name string) {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	l.s.Name = name
}

func (l live) End() {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	l.s.Ended, l.s.EndTime = true, time.Now()
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start implements obs.Tracer.
func (t *Tracer) Start(ctx context.Context, name string, attrs []obs.Attr) (context.Context, obs.Span) {
	s := &Span{Name: name, SpanID: randHex(8), Attrs: map[string]any{}, Start: time.Now()}
	if p, ok := ctx.Value(ctxKey{}).(spanCtx); ok {
		s.TraceID, s.ParentID = p.traceID, p.spanID
		if p.remote {
			s.RemoteParentTrace = p.traceID
		}
	} else {
		s.TraceID = randHex(16)
	}
	for _, a := range attrs {
		s.Attrs[a.Key] = a.Value
	}
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return context.WithValue(ctx, ctxKey{}, spanCtx{traceID: s.TraceID, spanID: s.SpanID}), live{t, s}
}

// IDs implements obs.Tracer.
func (t *Tracer) IDs(ctx context.Context) (string, string) {
	if p, ok := ctx.Value(ctxKey{}).(spanCtx); ok && !p.remote {
		return p.traceID, p.spanID
	}
	return "", ""
}

// Traceparent implements obs.Tracer.
func (t *Tracer) Traceparent(ctx context.Context) string {
	if p, ok := ctx.Value(ctxKey{}).(spanCtx); ok {
		return obs.FormatTraceparent(obs.TraceContext{TraceID: p.traceID, ParentID: p.spanID, Flags: 1})
	}
	return ""
}

// WithRemoteParent implements obs.Tracer.
func (t *Tracer) WithRemoteParent(ctx context.Context, tp string) context.Context {
	tc, ok := obs.ParseTraceparent(tp)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, spanCtx{traceID: tc.TraceID, spanID: tc.ParentID, remote: true})
}

// Carry implements obs.Tracer.
func (t *Tracer) Carry(dst, src context.Context) context.Context {
	if p, ok := src.Value(ctxKey{}).(spanCtx); ok {
		return context.WithValue(dst, ctxKey{}, p)
	}
	return dst
}

// Spans returns a snapshot of the recorded spans in start order.
func (t *Tracer) Spans() []Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Span, len(t.spans))
	for i, s := range t.spans {
		out[i] = *s
		out[i].Attrs = map[string]any{}
		for k, v := range s.Attrs {
			out[i].Attrs[k] = v
		}
		out[i].Events = append([]string(nil), s.Events...)
	}
	return out
}

// Named returns the recorded spans with the given name.
func (t *Tracer) Named(name string) []Span {
	var out []Span
	for _, s := range t.Spans() {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// Recorder records metric events as strings ("Method label=value ...") for assertions.
type Recorder struct {
	mu     sync.Mutex
	events []string
	gauges []func() []obs.Gauge
}

func (r *Recorder) add(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf(format, args...))
}

// Events returns the recorded events.
func (r *Recorder) Events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// Has reports whether an event equal to e was recorded.
func (r *Recorder) Has(e string) bool {
	for _, x := range r.Events() {
		if x == e {
			return true
		}
	}
	return false
}

// Gauges samples the registered gauge callbacks, sorted by name.
func (r *Recorder) Gauges() []obs.Gauge {
	r.mu.Lock()
	fns := append([]func() []obs.Gauge(nil), r.gauges...)
	r.mu.Unlock()
	var out []obs.Gauge
	for _, f := range fns {
		out = append(out, f()...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *Recorder) APIRequest(route, method string, status int, _ time.Duration) {
	r.add("APIRequest route=%s method=%s status=%d", route, method, status)
}
func (r *Recorder) TaskFinished(taskID, kind, status string) {
	r.add("TaskFinished task=%s kind=%s status=%s", taskID, kind, status)
}
func (r *Recorder) RunEnded(kind, status string) {
	r.add("RunEnded kind=%s status=%s", kind, status)
}
func (r *Recorder) AttemptFinished(kind, class string) {
	r.add("AttemptFinished kind=%s class=%s", kind, class)
}
func (r *Recorder) AttemptReady(kind string, _ time.Duration) { r.add("AttemptReady kind=%s", kind) }
func (r *Recorder) TaskStarted(kind string, _ time.Duration)  { r.add("TaskStarted kind=%s", kind) }
func (r *Recorder) StopCompleted(kind, desired string, _ time.Duration) {
	r.add("StopCompleted kind=%s desired=%s", kind, desired)
}
func (r *Recorder) GatewayCall(kind, result string) {
	r.add("GatewayCall kind=%s result=%s", kind, result)
}
func (r *Recorder) UpstreamTry(kind, provider, model, outcome string, status int, _ time.Duration) {
	r.add("UpstreamTry kind=%s provider=%s model=%s outcome=%s status=%d", kind, provider, model, outcome, status)
}
func (r *Recorder) BreakerTransition(kind, route, to string) {
	r.add("BreakerTransition kind=%s route=%s to=%s", kind, route, to)
}
func (r *Recorder) Cost(kind, provider, model string, micro int64) {
	r.add("Cost kind=%s provider=%s model=%s micro=%d", kind, provider, model, micro)
}
func (r *Recorder) ToolCall(taskID, kind string) { r.add("ToolCall task=%s kind=%s", taskID, kind) }
func (r *Recorder) RegisterGauges(fn func() []obs.Gauge) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges = append(r.gauges, fn)
}

// Install installs a fresh Tracer and Recorder for the duration of the test. Tests using it must not run in
// parallel with other tests that install an implementation (obs is process-global).
func Install(t testing.TB) (*Tracer, *Recorder) {
	t.Helper()
	tr, rec := &Tracer{}, &Recorder{}
	restore := obs.Set(tr, rec)
	t.Cleanup(restore)
	return tr, rec
}
