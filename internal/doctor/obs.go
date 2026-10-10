package doctor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

// ObsConfig points at the observability stack's query APIs (all optional).
type ObsConfig struct {
	Tempo        string // e.g. http://127.0.0.1:3200
	Prometheus   string // e.g. http://127.0.0.1:9090
	Loki         string // e.g. http://127.0.0.1:3100
	LokiSelector string // default {job="agentbox"}
	HTTP         *http.Client
}

func (o ObsConfig) enabled() bool { return o.Tempo != "" || o.Prometheus != "" || o.Loki != "" }

func (o ObsConfig) client() *http.Client {
	if o.HTTP != nil {
		return o.HTTP
	}
	return &http.Client{Timeout: 15 * time.Second}
}

// Enrich adds trace ids, span shares, metric tables and log counts to the report. Every failure becomes a note.
func Enrich(ctx context.Context, rep *Report, trs []*eval.Trajectory, o ObsConfig) {
	if !o.enabled() {
		return
	}
	from, to := window(trs)
	or := &ObsReport{Tempo: o.Tempo, Prometheus: o.Prometheus, Loki: o.Loki}
	rep.Observability = or
	if o.Tempo != "" {
		enrichTempo(ctx, rep, or, o, from, to)
	}
	if o.Prometheus != "" {
		enrichPrometheus(ctx, or, o, from, to)
	}
	if o.Loki != "" {
		enrichLoki(ctx, or, o, from, to)
	}
}

// window is the time range covered by the trajectories, padded for scrape and export delays.
func window(trs []*eval.Trajectory) (time.Time, time.Time) {
	var from, to time.Time
	for _, t := range trs {
		if !t.SubmittedAt.IsZero() && (from.IsZero() || t.SubmittedAt.Before(from)) {
			from = t.SubmittedAt
		}
		end := t.TerminalAt
		if end.IsZero() {
			end = t.SubmittedAt.Add(time.Duration(t.LatencyMs) * time.Millisecond)
		}
		if end.After(to) {
			to = end
		}
	}
	if from.IsZero() {
		to = time.Now()
		from = to.Add(-time.Hour)
	}
	return from.Add(-30 * time.Second), to.Add(30 * time.Second)
}

func getJSON(ctx context.Context, c *http.Client, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
	}
	return json.Unmarshal(body, out)
}

// ---- Tempo ----

var hexID = regexp.MustCompile(`^[0-9a-fA-F]{1,32}$`)

// TempoTraceID finds the trace of a task's run by the run span's task.id attribute (TraceQL search).
func TempoTraceID(ctx context.Context, c *http.Client, base, taskID string, from, to time.Time) (string, error) {
	q := url.Values{}
	q.Set("q", fmt.Sprintf(`{ span.task.id = %q }`, taskID))
	q.Set("start", strconv.FormatInt(from.Unix(), 10))
	q.Set("end", strconv.FormatInt(to.Unix()+1, 10))
	q.Set("limit", "5")
	var res struct {
		Traces []struct {
			TraceID           string `json:"traceID"`
			StartTimeUnixNano string `json:"startTimeUnixNano"`
		} `json:"traces"`
	}
	if err := getJSON(ctx, c, strings.TrimRight(base, "/")+"/api/search?"+q.Encode(), &res); err != nil {
		return "", err
	}
	// A task with several runs has several traces; the earliest one is its first run.
	sort.Slice(res.Traces, func(i, j int) bool { return res.Traces[i].StartTimeUnixNano < res.Traces[j].StartTimeUnixNano })
	for _, t := range res.Traces {
		if hexID.MatchString(t.TraceID) {
			return strings.ToLower(fmt.Sprintf("%032s", t.TraceID)), nil
		}
	}
	return "", nil
}

type otlpSpan struct {
	Name  string `json:"name"`
	Start string `json:"startTimeUnixNano"`
	End   string `json:"endTimeUnixNano"`
}

type otlpScope struct {
	Spans []otlpSpan `json:"spans"`
}

type otlpResource struct {
	ScopeSpans []otlpScope `json:"scopeSpans"`
	ILS        []otlpScope `json:"instrumentationLibrarySpans"`
}

// TempoSpanShares fetches a trace and aggregates its spans by name (count, total duration, share of the trace).
func TempoSpanShares(ctx context.Context, c *http.Client, base, traceID string) ([]SpanShare, error) {
	var res struct {
		Batches       []otlpResource `json:"batches"`
		ResourceSpans []otlpResource `json:"resourceSpans"`
		Trace         *struct {
			ResourceSpans []otlpResource `json:"resourceSpans"`
		} `json:"trace"`
	}
	if err := getJSON(ctx, c, strings.TrimRight(base, "/")+"/api/traces/"+url.PathEscape(traceID), &res); err != nil {
		return nil, err
	}
	rs := append(res.Batches, res.ResourceSpans...)
	if res.Trace != nil {
		rs = append(rs, res.Trace.ResourceSpans...)
	}
	type agg struct {
		n     int
		total int64
	}
	by := map[string]*agg{}
	var first, last int64
	seen := false
	for _, r := range rs {
		for _, sc := range append(r.ScopeSpans, r.ILS...) {
			for _, sp := range sc.Spans {
				s, err1 := strconv.ParseInt(sp.Start, 10, 64)
				e, err2 := strconv.ParseInt(sp.End, 10, 64)
				if err1 != nil || err2 != nil || e < s {
					continue
				}
				if !seen || s < first {
					first, seen = s, true
				}
				last = max(last, e)
				a := by[sp.Name]
				if a == nil {
					a = &agg{}
					by[sp.Name] = a
				}
				a.n++
				a.total += e - s
			}
		}
	}
	dur := last - first
	var out []SpanShare
	for name, a := range by {
		out = append(out, SpanShare{Name: name, Count: a.n, TotalMs: a.total / 1e6, Share: share(a.total, dur)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalMs != out[j].TotalMs {
			return out[i].TotalMs > out[j].TotalMs
		}
		return out[i].Name < out[j].Name
	})
	return out[:min(len(out), 8)], nil
}

func enrichTempo(ctx context.Context, rep *Report, or *ObsReport, o ObsConfig, from, to time.Time) {
	c := o.client()
	ids := map[string]string{}
	var firstErr error
	lookup := func(task string) string {
		if task == "" {
			return ""
		}
		if id, ok := ids[task]; ok {
			return id
		}
		id, err := TempoTraceID(ctx, c, o.Tempo, task, from, to)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		ids[task] = id
		return id
	}
	for i := range rep.Findings {
		ex := rep.Findings[i].Evidence.Examples
		for j := range ex[:min(len(ex), 3)] {
			ex[j].TraceID = lookup(ex[j].ServerTaskID)
		}
	}
	for i := range rep.Slowest {
		s := &rep.Slowest[i]
		s.TraceID = lookup(s.ServerTaskID)
		if s.TraceID == "" {
			continue
		}
		spans, err := TempoSpanShares(ctx, c, o.Tempo, s.TraceID)
		if err != nil {
			or.Notes = append(or.Notes, fmt.Sprintf("tempo trace %s: %v", s.TraceID, err))
			continue
		}
		s.Spans = spans
	}
	for _, id := range ids {
		if id != "" {
			or.TraceIDs++
		}
	}
	if firstErr != nil {
		or.Notes = append(or.Notes, "tempo search: "+firstErr.Error())
	}
}

// ---- Prometheus ----

// promQueries are the cross-checks run over the analysed window (%s = range).
var promQueries = []struct{ title, query string }{
	{"Upstream tries by provider and outcome", `sum by (kind, provider, outcome) (increase(agentbox_upstream_tries_total[%s]))`},
	{"Gateway calls by result", `sum by (kind, result) (increase(agentbox_gateway_calls_total[%s]))`},
	{"Breaker transitions", `sum by (route, to) (increase(agentbox_breaker_transitions_total[%s]))`},
	{"Tasks finished by status", `sum by (kind, status) (increase(agentbox_tasks_finished_total[%s]))`},
}

// PromInstant runs an instant query and returns the vector.
func PromInstant(ctx context.Context, c *http.Client, base, query string, at time.Time) ([]MetricRow, error) {
	q := url.Values{}
	q.Set("query", query)
	q.Set("time", strconv.FormatInt(at.Unix(), 10))
	var res struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
		Error string `json:"error"`
	}
	if err := getJSON(ctx, c, strings.TrimRight(base, "/")+"/api/v1/query?"+q.Encode(), &res); err != nil {
		return nil, err
	}
	if res.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s", res.Error)
	}
	var rows []MetricRow
	for _, r := range res.Data.Result {
		s, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || v == 0 {
			continue
		}
		rows = append(rows, MetricRow{Labels: r.Metric, Value: v})
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i].Labels) < fmt.Sprint(rows[j].Labels) })
	return rows, nil
}

func enrichPrometheus(ctx context.Context, or *ObsReport, o ObsConfig, from, to time.Time) {
	rng := fmt.Sprintf("%ds", max(int64(60), int64(to.Sub(from).Seconds())))
	for _, pq := range promQueries {
		query := fmt.Sprintf(pq.query, rng)
		rows, err := PromInstant(ctx, o.client(), o.Prometheus, query, to)
		if err != nil {
			or.Notes = append(or.Notes, "prometheus: "+err.Error())
			return
		}
		or.Metrics = append(or.Metrics, MetricTable{Title: pq.title, Query: query, Rows: rows})
	}
}

// ---- Loki ----

// LokiWarnings counts WARN/ERROR log lines in the window by message, with up to 3 trace ids each.
func LokiWarnings(ctx context.Context, c *http.Client, base, selector string, from, to time.Time) ([]LogCount, error) {
	q := url.Values{}
	q.Set("query", selector+` |~ "\"level\":\"(WARN|ERROR)\""`)
	q.Set("start", strconv.FormatInt(from.UnixNano(), 10))
	q.Set("end", strconv.FormatInt(to.UnixNano(), 10))
	q.Set("limit", "5000")
	q.Set("direction", "forward")
	var res struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(ctx, c, strings.TrimRight(base, "/")+"/loki/api/v1/query_range?"+q.Encode(), &res); err != nil {
		return nil, err
	}
	by := map[string]*LogCount{}
	for _, st := range res.Data.Result {
		for _, v := range st.Values {
			var line struct {
				Msg     string `json:"msg"`
				TraceID string `json:"trace_id"`
			}
			if json.Unmarshal([]byte(v[1]), &line) != nil || line.Msg == "" {
				continue
			}
			lc := by[line.Msg]
			if lc == nil {
				lc = &LogCount{Message: line.Msg}
				by[line.Msg] = lc
			}
			lc.Count++
			if line.TraceID != "" && len(lc.TraceIDs) < 3 && !contains(lc.TraceIDs, line.TraceID) {
				lc.TraceIDs = append(lc.TraceIDs, line.TraceID)
			}
		}
	}
	var out []LogCount
	for _, lc := range by {
		out = append(out, *lc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Message < out[j].Message
	})
	return out[:min(len(out), 15)], nil
}

func enrichLoki(ctx context.Context, or *ObsReport, o ObsConfig, from, to time.Time) {
	sel := o.LokiSelector
	if sel == "" {
		sel = `{job="agentbox"}`
	}
	logs, err := LokiWarnings(ctx, o.client(), o.Loki, sel, from, to)
	if err != nil {
		or.Notes = append(or.Notes, "loki: "+err.Error())
		return
	}
	or.Logs = logs
}
