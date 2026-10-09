package telemetry_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/telemetry"
)

func TestEmptyConfigInstallsNothing(t *testing.T) {
	tel, err := telemetry.Setup(context.Background(), telemetry.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
	if obs.Enabled() {
		t.Fatal("empty config must leave observability off")
	}
	if tel.MetricsAddr() != "" {
		t.Errorf("metrics listener started: %s", tel.MetricsAddr())
	}
}

func TestValidate(t *testing.T) {
	for _, c := range []telemetry.Config{
		{OTLPEndpoint: "127.0.0.1:4318"},
		{OTLPEndpoint: "grpc://x:1"},
		{SampleRatio: 1.5},
		{SampleRatio: -1},
		{MetricsListen: "9464"},
	} {
		if err := c.Validate(); err == nil {
			t.Errorf("Validate(%+v) accepted", c)
		}
	}
	if err := (telemetry.Config{OTLPEndpoint: "http://127.0.0.1:4318", MetricsListen: "127.0.0.1:0", SampleRatio: 0.5}).Validate(); err != nil {
		t.Error(err)
	}
	if telemetry.MetricsWarning("127.0.0.1:9464") != "" || telemetry.MetricsWarning("localhost:9464") != "" {
		t.Error("loopback must not warn")
	}
	if telemetry.MetricsWarning("0.0.0.0:9464") == "" {
		t.Error("non-loopback must warn")
	}
}

func TestTracingPropagation(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tel, err := telemetry.Setup(context.Background(), telemetry.Config{SpanExporter: exp, ServiceVersion: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
	if !obs.Tracing() || obs.Metrics() {
		t.Fatal("expected tracing on, metrics off")
	}

	api, sp := obs.Start(context.Background(), "HTTP POST /tasks", obs.Str("http.route", "/tasks"))
	tp := obs.Traceparent(api)
	tc, ok := obs.ParseTraceparent(tp)
	if !ok || !tc.Sampled() {
		t.Fatalf("traceparent %q", tp)
	}
	sp.End()

	// Actor side: new context, remote parent from the registry value.
	actx := obs.WithRemoteParent(context.Background(), tp)
	actx, task := obs.Start(actx, "task", obs.Str("task.id", "task_1"))
	// Work continuing in another context (Gateway coordinator) keeps the span via Carry.
	cctx := obs.Carry(context.Background(), actx)
	_, call := obs.Start(cctx, "gateway.call", obs.Int("try.no", 1))
	call.Fail("upstream_unavailable")
	call.End()
	task.End()
	// Invalid traceparent leaves the context untouched.
	if obs.WithRemoteParent(context.Background(), "garbage") != context.Background() {
		t.Error("invalid traceparent changed the context")
	}
	if err := tel.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exp.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("got %d spans", len(spans))
	}
	byName := map[string]tracetest.SpanStub{}
	for _, s := range spans {
		byName[s.Name] = s
	}
	root, taskSp, callSp := byName["HTTP POST /tasks"], byName["task"], byName["gateway.call"]
	if taskSp.SpanContext.TraceID() != root.SpanContext.TraceID() || taskSp.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Error("task span is not a child of the API span")
	}
	if callSp.Parent.SpanID() != taskSp.SpanContext.SpanID() {
		t.Error("gateway.call is not a child of the task span")
	}
	if callSp.Status.Description != "upstream_unavailable" {
		t.Errorf("status = %+v", callSp.Status)
	}
	var svc string
	for _, a := range root.Resource.Attributes() {
		if a.Key == "service.name" {
			svc = a.Value.AsString()
		}
	}
	if svc != telemetry.ServiceName {
		t.Errorf("service.name = %q", svc)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	tel, err := telemetry.Setup(context.Background(), telemetry.Config{MetricsListen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()
	if obs.Tracing() || !obs.Metrics() {
		t.Fatal("expected metrics on, tracing off")
	}
	m := obs.M()
	m.APIRequest("/tasks", "POST", 201, 3*time.Millisecond)
	m.ToolCall("task_1", "search")
	m.ToolCall("task_1", "fetch")
	m.TaskFinished("task_1", "task", "succeeded")
	m.AttemptFinished("task", "succeeded")
	m.AttemptReady("task", 800*time.Millisecond)
	m.TaskStarted("task", time.Second)
	m.StopCompleted("turn", "pause", 2*time.Second)
	m.GatewayCall("chat", "completed")
	m.UpstreamTry("chat", "openai_compat", "fake-model", "ok", 200, 40*time.Millisecond)
	m.Cost("chat", "openai_compat", "fake-model", 1234)
	m.RegisterGauges(func() []obs.Gauge {
		return []obs.Gauge{
			{Name: "sandbox_envs", Help: "Environments not yet stopped.", Labels: map[string]string{"kind": "task"}, Value: 2},
			{Name: "run_slots_in_use", Help: "Run slots in use.", Value: 1},
		}
	})
	resp, err := http.Get("http://" + tel.MetricsAddr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	body := string(b)
	for _, want := range []string{
		`agentbox_http_requests_total{code="201",method="POST",route="/tasks"} 1`,
		`agentbox_tasks_finished_total{kind="task",status="succeeded"} 1`,
		`agentbox_tool_calls_per_task_sum{kind="task"} 2`,
		`agentbox_attempts_finished_total{kind="task",outcome_class="succeeded"} 1`,
		`agentbox_attempt_ready_seconds_count{kind="task"} 1`,
		`agentbox_task_start_seconds_count{kind="task"} 1`,
		`agentbox_stop_seconds_count{desired="pause",kind="turn"} 1`,
		`agentbox_gateway_calls_total{kind="chat",result="completed"} 1`,
		`agentbox_upstream_tries_total{kind="chat",model="fake-model",outcome="ok",provider="openai_compat",status="200"} 1`,
		`agentbox_cost_micro_usd_total{kind="chat",model="fake-model",provider="openai_compat"} 1234`,
		`agentbox_sandbox_envs{kind="task"} 2`,
		`agentbox_run_slots_in_use 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
	for _, forbidden := range []string{"task_1"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("/metrics leaks %q", forbidden)
		}
	}
}
