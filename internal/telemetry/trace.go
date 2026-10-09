package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// InstrumentationName is the OpenTelemetry instrumentation scope.
const InstrumentationName = "github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python"

type otelTracer struct {
	t    trace.Tracer
	prop propagation.TraceContext
}

func newTracer(tp *sdktrace.TracerProvider) *otelTracer {
	return &otelTracer{t: tp.Tracer(InstrumentationName)}
}

func kv(attrs []obs.Attr) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			out = append(out, attribute.String(a.Key, v))
		case int64:
			out = append(out, attribute.Int64(a.Key, v))
		case int:
			out = append(out, attribute.Int(a.Key, v))
		case bool:
			out = append(out, attribute.Bool(a.Key, v))
		case float64:
			out = append(out, attribute.Float64(a.Key, v))
		}
	}
	return out
}

func (o *otelTracer) Start(ctx context.Context, name string, attrs []obs.Attr) (context.Context, obs.Span) {
	ctx, sp := o.t.Start(ctx, name, trace.WithAttributes(kv(attrs)...))
	return ctx, span{sp}
}

func (o *otelTracer) IDs(ctx context.Context) (string, string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}

func (o *otelTracer) Traceparent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	o.prop.Inject(ctx, c)
	return c.Get("traceparent")
}

func (o *otelTracer) WithRemoteParent(ctx context.Context, tp string) context.Context {
	if _, ok := obs.ParseTraceparent(tp); !ok {
		return ctx
	}
	return o.prop.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}

func (o *otelTracer) Carry(dst, src context.Context) context.Context {
	sc := trace.SpanContextFromContext(src)
	if !sc.IsValid() {
		return dst
	}
	return trace.ContextWithSpan(dst, trace.SpanFromContext(src))
}

type span struct{ s trace.Span }

func (s span) SetAttrs(attrs ...obs.Attr) { s.s.SetAttributes(kv(attrs)...) }

func (s span) Event(name string, attrs ...obs.Attr) {
	s.s.AddEvent(name, trace.WithAttributes(kv(attrs)...))
}

func (s span) Fail(code string) {
	s.s.SetAttributes(attribute.String("error.code", code))
	s.s.SetStatus(codes.Error, code)
}

func (s span) Rename(name string) { s.s.SetName(name) }

func (s span) End() { s.s.End() }
