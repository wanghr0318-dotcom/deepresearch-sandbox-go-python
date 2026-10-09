// Package obs is the stdlib-only observability facade used by the domain packages (api, task, session,
// runner, gateway/edge, gateway/call). It defines spans, W3C traceparent handling and metric events as small
// interfaces with a no-op default; internal/telemetry installs the OpenTelemetry / Prometheus implementation
// with Set at startup when tracing or metrics are configured.
//
// Keeping the facade free of third-party imports preserves the architecture rules (gateway/call depends only
// on the standard library and this module; task and session do not import net/http) and makes the "off"
// path a nil-interface check.
//
// Attribute allowlist (docs/design/2026-10-10-observability-design.md §6): only opaque IDs, numbers and
// closed enumerations may be passed as attributes or metric labels — never prompts, model output, queries,
// URLs, tokens or free-text error messages.
package obs

import (
	"context"
	"sync/atomic"
	"time"
)

// Attr is a span attribute. Value is a string, int64, bool or float64.
type Attr struct {
	Key   string
	Value any
}

// Str returns a string attribute.
func Str(k, v string) Attr { return Attr{k, v} }

// Int returns an integer attribute.
func Int(k string, v int64) Attr { return Attr{k, v} }

// Bool returns a boolean attribute.
func Bool(k string, v bool) Attr { return Attr{k, v} }

// Span is an in-progress span. All methods are safe on the no-op span.
type Span interface {
	SetAttrs(attrs ...Attr)
	// Event records a point-in-time event on the span.
	Event(name string, attrs ...Attr)
	// Fail marks the span as failed with a stable error code (never a free-text message).
	Fail(code string)
	// Rename replaces the span name (e.g. once the HTTP route is known).
	Rename(name string)
	End()
}

// Tracer is the tracing implementation.
type Tracer interface {
	Start(ctx context.Context, name string, attrs []Attr) (context.Context, Span)
	// IDs returns the hex trace and span id of the span active in ctx ("" when none).
	IDs(ctx context.Context) (traceID, spanID string)
	// Traceparent returns the W3C traceparent of the span active in ctx ("" when none).
	Traceparent(ctx context.Context) string
	// WithRemoteParent returns ctx with a remote parent parsed from traceparent (ctx unchanged when invalid).
	WithRemoteParent(ctx context.Context, traceparent string) context.Context
	// Carry returns dst carrying the span active in src (used where work continues in another context).
	Carry(dst, src context.Context) context.Context
}

// Recorder receives metric events. Every label argument must be a bounded enumeration (no task or user IDs);
// taskID arguments are only used as in-memory keys and never become labels.
type Recorder interface {
	// APIRequest is one HTTP API request; route is the registered pattern or "other".
	APIRequest(route, method string, status int, d time.Duration)
	// TaskFinished is a terminal status of a task (kind "task") or session turn (kind "turn"): succeeded, failed or
	// cancelled. The tool calls counted for taskID are observed and forgotten here.
	TaskFinished(taskID, kind, status string)
	// RunEnded is the end of one run of a task/turn: paused (incl. awaiting_input) or a terminal status.
	RunEnded(kind, status string)
	// AttemptFinished is the classified outcome of one attempt.
	AttemptFinished(kind, outcomeClass string)
	// AttemptReady is the time from attempt creation to the worker being ready.
	AttemptReady(kind string, d time.Duration)
	// TaskStarted is the time from API submission to the first ready worker of a task/turn.
	TaskStarted(kind string, d time.Duration)
	// StopCompleted is the time from the actor observing a pause/cancel to the stop verdict being committed.
	StopCompleted(kind, desired string, d time.Duration)
	// GatewayCall is the result of one Gateway call (completed, replayed, cache_hit, coalesced or an error code).
	GatewayCall(kind, result string)
	// UpstreamTry is one upstream try.
	UpstreamTry(kind, provider, model, outcome string, status int, d time.Duration)
	// BreakerTransition is a circuit breaker of a model route changing state (to: closed, open, half_open).
	BreakerTransition(kind, route, to string)
	// Cost is the settled actual cost of an upstream call in micro-USD.
	Cost(kind, provider, model string, micro int64)
	// ToolCall counts a search/fetch/exec call against a task (observed per task at TaskFinished).
	ToolCall(taskID, kind string)
	// RegisterGauges registers a callback sampled at scrape time.
	RegisterGauges(fn func() []Gauge)
}

// Gauge is one sampled gauge value. Labels must be bounded enumerations.
type Gauge struct {
	Name, Help string
	Labels     map[string]string
	Value      float64
}

type impl struct {
	t Tracer
	r Recorder
}

var cur atomic.Pointer[impl]

// Set installs the implementation; nil arguments mean "off". It returns a function restoring the previous
// implementation (tests).
func Set(t Tracer, r Recorder) (restore func()) {
	prev := cur.Swap(&impl{t: t, r: r})
	return func() { cur.Store(prev) }
}

func tracer() Tracer {
	if p := cur.Load(); p != nil {
		return p.t
	}
	return nil
}

// Tracing reports whether a tracer is installed.
func Tracing() bool { return tracer() != nil }

// Metrics reports whether a metric recorder is installed.
func Metrics() bool { return M() != nop{} }

// Enabled reports whether tracing or metrics are on.
func Enabled() bool { return Tracing() || Metrics() }

// M returns the metric recorder (a no-op recorder when metrics are off).
func M() Recorder {
	if p := cur.Load(); p != nil && p.r != nil {
		return p.r
	}
	return nop{}
}

// Start starts a span named name as a child of the span in ctx.
func Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if t := tracer(); t != nil {
		return t.Start(ctx, name, attrs)
	}
	return ctx, noSpan{}
}

// IDs returns the trace and span id active in ctx ("" when tracing is off or no span is active).
func IDs(ctx context.Context) (traceID, spanID string) {
	if t := tracer(); t != nil {
		return t.IDs(ctx)
	}
	return "", ""
}

// Traceparent returns the W3C traceparent of the span active in ctx ("" when none).
func Traceparent(ctx context.Context) string {
	if t := tracer(); t != nil {
		return t.Traceparent(ctx)
	}
	return ""
}

// WithRemoteParent returns ctx whose parent is the (valid) traceparent; otherwise ctx unchanged.
func WithRemoteParent(ctx context.Context, traceparent string) context.Context {
	if t := tracer(); t != nil && traceparent != "" {
		return t.WithRemoteParent(ctx, traceparent)
	}
	return ctx
}

// Carry returns dst carrying the span active in src.
func Carry(dst, src context.Context) context.Context {
	if t := tracer(); t != nil && src != nil {
		return t.Carry(dst, src)
	}
	return dst
}

type noSpan struct{}

func (noSpan) SetAttrs(...Attr)      {}
func (noSpan) Event(string, ...Attr) {}
func (noSpan) Fail(string)           {}
func (noSpan) Rename(string)         {}
func (noSpan) End()                  {}

type nop struct{}

func (nop) APIRequest(string, string, int, time.Duration)                  {}
func (nop) TaskFinished(string, string, string)                            {}
func (nop) BreakerTransition(string, string, string)                       {}
func (nop) RunEnded(string, string)                                        {}
func (nop) AttemptFinished(string, string)                                 {}
func (nop) AttemptReady(string, time.Duration)                             {}
func (nop) TaskStarted(string, time.Duration)                              {}
func (nop) StopCompleted(string, string, time.Duration)                    {}
func (nop) GatewayCall(string, string)                                     {}
func (nop) UpstreamTry(string, string, string, string, int, time.Duration) {}
func (nop) Cost(string, string, string, int64)                             {}
func (nop) ToolCall(string, string)                                        {}
func (nop) RegisterGauges(func() []Gauge)                                  {}
