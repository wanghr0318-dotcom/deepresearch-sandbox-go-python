package telemetry

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// Metric names (prefix agentbox_). Every label is a bounded enumeration; task, user and session IDs are never
// labels (docs/design/2026-10-10-observability-design.md §5).
const ns = "agentbox"

// toolCountMax bounds the in-memory per-task tool call counters (entries are removed at TaskFinished; tasks
// that never finish in this process are dropped oldest-first beyond the bound).
const toolCountMax = 8192

var (
	latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
	startBuckets   = []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300}
	toolBuckets    = []float64{0, 1, 2, 4, 8, 16, 32, 64, 128}
)

type metrics struct {
	httpReqs     *prometheus.CounterVec
	httpDur      *prometheus.HistogramVec
	tasks        *prometheus.CounterVec
	runs         *prometheus.CounterVec
	attempts     *prometheus.CounterVec
	attemptReady *prometheus.HistogramVec
	taskStart    *prometheus.HistogramVec
	stop         *prometheus.HistogramVec
	calls        *prometheus.CounterVec
	tries        *prometheus.CounterVec
	tryDur       *prometheus.HistogramVec
	cost         *prometheus.CounterVec
	breakers     *prometheus.CounterVec
	tools        *prometheus.HistogramVec

	mu        sync.Mutex
	toolCount map[string]int
	toolOrder []string

	gauges *gaugeCollector
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		httpReqs: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "http_requests_total",
			Help: "API requests by route pattern, method and status code."}, []string{"route", "method", "code"}),
		httpDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "http_request_duration_seconds",
			Help: "API request duration (SSE streams count until they end).", Buckets: latencyBuckets}, []string{"route", "method"}),
		tasks: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "tasks_finished_total",
			Help: "Tasks (kind=task) and session turns (kind=turn) reaching a terminal status: succeeded, failed, cancelled."}, []string{"kind", "status"}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "task_runs_ended_total",
			Help: "Runs of tasks/turns ending: paused (incl. awaiting_input) or a terminal status."}, []string{"kind", "status"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "attempts_finished_total",
			Help: "Classified attempt outcomes."}, []string{"kind", "outcome_class"}),
		attemptReady: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "attempt_ready_seconds",
			Help: "Attempt created to worker ready (environment creation, worker start, handshake).", Buckets: startBuckets}, []string{"kind"}),
		taskStart: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "task_start_seconds",
			Help: "API submit to first worker ready (submit->ready, includes queueing).", Buckets: startBuckets}, []string{"kind"}),
		stop: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "stop_seconds",
			Help: "Stop observed by the actor to stop verdict committed (stop->paused / stop->cancelled).", Buckets: startBuckets}, []string{"kind", "desired"}),
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "gateway_calls_total",
			Help: "Gateway calls by kind and result (completed, replayed, cache_hit, coalesced or a stable error code)."}, []string{"kind", "result"}),
		tries: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "upstream_tries_total",
			Help: "Upstream tries by kind, provider, model, HTTP status and outcome."}, []string{"kind", "provider", "model", "status", "outcome"}),
		tryDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "upstream_try_duration_seconds",
			Help: "Upstream try latency.", Buckets: latencyBuckets}, []string{"kind", "provider", "model"}),
		cost: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "cost_micro_usd_total",
			Help: "Settled actual cost of successful upstream calls in micro-USD."}, []string{"kind", "provider", "model"}),
		breakers: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "breaker_transitions_total",
			Help: "Circuit breaker state changes of model routes (to: closed, open, half_open)."}, []string{"kind", "route", "to"}),
		tools: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "tool_calls_per_task",
			Help: "search/fetch/exec calls per task or turn over all its runs, observed at its terminal status.", Buckets: toolBuckets}, []string{"kind"}),
		toolCount: map[string]int{},
		gauges:    &gaugeCollector{},
	}
	reg.MustRegister(m.httpReqs, m.httpDur, m.tasks, m.runs, m.attempts, m.attemptReady, m.taskStart, m.stop, m.calls,
		m.tries, m.tryDur, m.cost, m.breakers, m.tools, m.gauges)
	return m
}

var _ obs.Recorder = (*metrics)(nil)

func (m *metrics) APIRequest(route, method string, status int, d time.Duration) {
	m.httpReqs.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.httpDur.WithLabelValues(route, method).Observe(d.Seconds())
}

func (m *metrics) TaskFinished(taskID, kind, status string) {
	m.tasks.WithLabelValues(kind, status).Inc()
	m.mu.Lock()
	n := m.toolCount[taskID]
	delete(m.toolCount, taskID)
	m.mu.Unlock()
	m.tools.WithLabelValues(kind).Observe(float64(n))
}

func (m *metrics) RunEnded(kind, status string) { m.runs.WithLabelValues(kind, status).Inc() }

func (m *metrics) AttemptFinished(kind, class string) { m.attempts.WithLabelValues(kind, class).Inc() }

func (m *metrics) AttemptReady(kind string, d time.Duration) {
	m.attemptReady.WithLabelValues(kind).Observe(d.Seconds())
}

func (m *metrics) TaskStarted(kind string, d time.Duration) {
	m.taskStart.WithLabelValues(kind).Observe(d.Seconds())
}

func (m *metrics) StopCompleted(kind, desired string, d time.Duration) {
	m.stop.WithLabelValues(kind, desired).Observe(d.Seconds())
}

func (m *metrics) GatewayCall(kind, result string) { m.calls.WithLabelValues(kind, result).Inc() }

func (m *metrics) UpstreamTry(kind, provider, model, outcome string, status int, d time.Duration) {
	m.tries.WithLabelValues(kind, provider, model, strconv.Itoa(status), outcome).Inc()
	m.tryDur.WithLabelValues(kind, provider, model).Observe(d.Seconds())
}

func (m *metrics) BreakerTransition(kind, route, to string) {
	m.breakers.WithLabelValues(kind, route, to).Inc()
}

func (m *metrics) Cost(kind, provider, model string, micro int64) {
	if micro > 0 {
		m.cost.WithLabelValues(kind, provider, model).Add(float64(micro))
	}
}

func (m *metrics) ToolCall(taskID, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.toolCount[taskID]; !ok {
		if len(m.toolOrder) >= toolCountMax {
			m.compactLocked()
		}
		m.toolOrder = append(m.toolOrder, taskID)
	}
	m.toolCount[taskID]++
}

// compactLocked drops order entries of finished tasks, then the oldest live entries beyond the bound.
func (m *metrics) compactLocked() {
	live := m.toolOrder[:0]
	for _, id := range m.toolOrder {
		if _, ok := m.toolCount[id]; ok {
			live = append(live, id)
		}
	}
	for len(live) >= toolCountMax {
		delete(m.toolCount, live[0])
		live = live[1:]
	}
	m.toolOrder = append([]string(nil), live...)
}

func (m *metrics) RegisterGauges(fn func() []obs.Gauge) { m.gauges.add(fn) }

// gaugeCollector is an unchecked collector sampling the registered callbacks at scrape time.
type gaugeCollector struct {
	mu  sync.Mutex
	fns []func() []obs.Gauge
}

func (g *gaugeCollector) add(fn func() []obs.Gauge) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fns = append(g.fns, fn)
}

func (g *gaugeCollector) Describe(chan<- *prometheus.Desc) {} // unchecked: label sets are sampled

func (g *gaugeCollector) Collect(ch chan<- prometheus.Metric) {
	g.mu.Lock()
	fns := append([]func() []obs.Gauge(nil), g.fns...)
	g.mu.Unlock()
	seen := map[string]bool{} // a later registration of the same series wins nothing: duplicates would fail the scrape
	for _, fn := range fns {
		for _, s := range fn() {
			keys := make([]string, 0, len(s.Labels))
			for k := range s.Labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			vals := make([]string, len(keys))
			for i, k := range keys {
				vals[i] = s.Labels[k]
			}
			id := s.Name + "|" + strings.Join(keys, ",") + "|" + strings.Join(vals, ",")
			if seen[id] {
				continue
			}
			seen[id] = true
			d := prometheus.NewDesc(prometheus.BuildFQName(ns, "", s.Name), s.Help, keys, nil)
			if mt, err := prometheus.NewConstMetric(d, prometheus.GaugeValue, s.Value, vals...); err == nil {
				ch <- mt
			}
		}
	}
}
