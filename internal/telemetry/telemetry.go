// Package telemetry implements internal/obs with the OpenTelemetry Go SDK (traces exported over OTLP/HTTP)
// and prometheus/client_golang (metrics served on a separate operator-only listener). It is wired only by
// cmd/agentbox; the domain packages see the stdlib-only obs facade.
//
// Both halves are off unless configured: an empty Config installs nothing and Setup returns a no-op.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// ServiceName is the OpenTelemetry service.name of the server.
const ServiceName = "agentbox"

// Config configures Setup. Zero value = everything off.
type Config struct {
	// OTLPEndpoint is the OTLP/HTTP base URL of a collector, e.g. http://127.0.0.1:4318 (traces are sent to
	// <endpoint>/v1/traces). Empty disables tracing. http:// is sent without TLS.
	OTLPEndpoint string
	// SampleRatio is the parent-based ratio sampler's ratio for new root spans (0 < r ≤ 1; 0 means 1).
	SampleRatio float64
	// MetricsListen is the listen address of the /metrics endpoint (operator-only). Empty disables metrics.
	MetricsListen string
	// ServiceVersion is reported as service.version.
	ServiceVersion string
	Logger         *slog.Logger
	// SpanExporter overrides the OTLP exporter (tests); used only when OTLPEndpoint is non-empty or this is set.
	SpanExporter sdktrace.SpanExporter
	// Listen overrides net.Listen for the metrics listener (tests).
	Listen func(addr string) (net.Listener, error)
}

// Telemetry is the running telemetry. Shutdown flushes spans and stops the metrics listener.
type Telemetry struct {
	tp       *sdktrace.TracerProvider
	srv      *http.Server
	ln       net.Listener
	reg      *prometheus.Registry
	restore  func()
	shutdown bool
}

// Validate checks the configuration without side effects (flag parsing).
func (c Config) Validate() error {
	if c.OTLPEndpoint != "" {
		u, err := url.Parse(c.OTLPEndpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("telemetry: --otlp-endpoint must be an http(s) URL like http://127.0.0.1:4318, got %q", c.OTLPEndpoint)
		}
	}
	if c.SampleRatio < 0 || c.SampleRatio > 1 {
		return fmt.Errorf("telemetry: --trace-sample-ratio must be in (0, 1], got %v", c.SampleRatio)
	}
	if c.MetricsListen != "" {
		if _, _, err := net.SplitHostPort(c.MetricsListen); err != nil {
			return fmt.Errorf("telemetry: --metrics-listen %q: %w", c.MetricsListen, err)
		}
	}
	return nil
}

// MetricsWarning returns a warning when the metrics listener is not loopback ("" otherwise).
func MetricsWarning(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || addr == "" {
		return ""
	}
	if ip := net.ParseIP(host); strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback()) {
		return ""
	}
	return "warning: --metrics-listen " + addr + " is not loopback: /metrics has no authentication and must " +
		"only be reachable by the operator's Prometheus (never by users)"
}

// Setup installs the configured implementation into obs. With an empty Config it installs nothing.
func Setup(ctx context.Context, c Config) (*Telemetry, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	t := &Telemetry{}
	var tracer obs.Tracer
	if c.OTLPEndpoint != "" || c.SpanExporter != nil {
		exp := c.SpanExporter
		if exp == nil {
			u, _ := url.Parse(c.OTLPEndpoint)
			opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
			if u.Scheme == "http" {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
			if p := strings.TrimSuffix(u.Path, "/"); p != "" {
				opts = append(opts, otlptracehttp.WithURLPath(p+"/v1/traces"))
			}
			var err error
			// New does not connect: an unreachable collector only drops spans later (never blocks requests).
			if exp, err = otlptracehttp.New(ctx, opts...); err != nil {
				return nil, fmt.Errorf("telemetry: OTLP exporter: %w", err)
			}
		}
		ratio := c.SampleRatio
		if ratio == 0 {
			ratio = 1
		}
		res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
			attribute.String("service.name", ServiceName), attribute.String("service.version", c.ServiceVersion)))
		if err != nil {
			return nil, fmt.Errorf("telemetry: resource: %w", err)
		}
		t.tp = sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))))
		tracer = newTracer(t.tp)
	}
	var rec obs.Recorder
	if c.MetricsListen != "" {
		t.reg = prometheus.NewRegistry()
		t.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		m := newMetrics(t.reg)
		rec = m
		listen := c.Listen
		if listen == nil {
			listen = func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }
		}
		ln, err := listen(c.MetricsListen)
		if err != nil {
			if t.tp != nil {
				_ = t.tp.Shutdown(ctx)
			}
			return nil, fmt.Errorf("telemetry: metrics listener %s: %w", c.MetricsListen, err)
		}
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(t.reg, promhttp.HandlerOpts{}))
		t.ln, t.srv = ln, &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := t.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && c.Logger != nil {
				c.Logger.Error("telemetry: metrics listener stopped", "error", err.Error())
			}
		}()
	}
	if tracer != nil || rec != nil {
		t.restore = obs.Set(tracer, rec)
	}
	return t, nil
}

// MetricsAddr returns the actual metrics listen address ("" when metrics are off).
func (t *Telemetry) MetricsAddr() string {
	if t == nil || t.ln == nil {
		return ""
	}
	return t.ln.Addr().String()
}

// Registry returns the Prometheus registry (nil when metrics are off).
func (t *Telemetry) Registry() *prometheus.Registry {
	if t == nil {
		return nil
	}
	return t.reg
}

// ForceFlush exports all ended spans (tests and the demo).
func (t *Telemetry) ForceFlush(ctx context.Context) error {
	if t == nil || t.tp == nil {
		return nil
	}
	return t.tp.ForceFlush(ctx)
}

// Shutdown flushes spans, stops the metrics listener and uninstalls the implementation.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.shutdown {
		return nil
	}
	t.shutdown = true
	var errs []error
	if t.srv != nil {
		errs = append(errs, t.srv.Shutdown(ctx))
	}
	if t.tp != nil {
		errs = append(errs, t.tp.Shutdown(ctx))
	}
	if t.restore != nil {
		t.restore()
	}
	return errors.Join(errs...)
}
