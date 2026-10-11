package main

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// result is one request as the client saw it.
type result struct {
	Start    time.Time
	Status   int           // HTTP status; 0 on a transport error
	Code     string        // error code from the body (OpenAI error.code, else error.type); "transport" on a transport error
	Msg      string        // error message from the body
	TTFB     time.Duration // request sent → response headers
	TTFT     time.Duration // request sent → first content event (streams only)
	Total    time.Duration // request sent → body fully read (or cancelled)
	Stream   bool
	Complete bool // stream ended with [DONE] / message_stop
	ErrEvent bool // stream carried an error event
	Canceled bool // the client cancelled the request on purpose (after the first content)
	Header   map[string]string
}

// ok reports a 200 whose stream (if any) completed or was cancelled on purpose.
func (r result) ok() bool {
	return r.Status == 200 && (!r.Stream || r.Complete || r.Canceled) && !r.ErrEvent
}

// outcome is a short label used in status tables: "200", "429 insufficient_quota", "transport", ….
func (r result) outcome() string {
	switch {
	case r.Status == 0:
		return "transport"
	case r.Status == 200 && r.ErrEvent:
		return "200 stream_error_event"
	case r.Status == 200 && r.Stream && !r.Complete && !r.Canceled:
		return "200 stream_incomplete"
	case r.Status == 200 && r.Canceled:
		return "200 client_cancelled"
	case r.Status == 200:
		return "200"
	case r.Code != "":
		return fmt.Sprintf("%d %s", r.Status, r.Code)
	default:
		return fmt.Sprint(r.Status)
	}
}

// dist is a latency distribution in milliseconds.
type dist struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean_ms"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
}

// distOf computes a nearest-rank distribution over ds.
func distOf(ds []time.Duration) dist {
	if len(ds) == 0 {
		return dist{}
	}
	ms := make([]float64, len(ds))
	sum := 0.0
	for i, d := range ds {
		ms[i] = float64(d) / float64(time.Millisecond)
		sum += ms[i]
	}
	sort.Float64s(ms)
	return dist{N: len(ms), Mean: round3(sum / float64(len(ms))), P50: round3(percentile(ms, 50)),
		P95: round3(percentile(ms, 95)), P99: round3(percentile(ms, 99)), Max: round3(ms[len(ms)-1])}
}

// percentile is the nearest-rank percentile of sorted values (p in (0, 100]).
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1]
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

// summary aggregates a run.
type summary struct {
	Requests int            `json:"requests"`
	OK       int            `json:"ok"`
	Seconds  float64        `json:"seconds"`
	RPS      float64        `json:"rps"`
	Outcomes map[string]int `json:"outcomes"`
	Total    dist           `json:"latency"`
	TTFT     dist           `json:"ttft,omitempty"`
}

// summarize builds a summary of rs over the wall time elapsed. Latency is over ok requests only.
func summarize(rs []result, elapsed time.Duration) summary {
	s := summary{Requests: len(rs), Seconds: round3(elapsed.Seconds()), Outcomes: map[string]int{}}
	var total, ttft []time.Duration
	for _, r := range rs {
		s.Outcomes[r.outcome()]++
		if !r.ok() {
			continue
		}
		s.OK++
		if !r.Canceled {
			total = append(total, r.Total)
		}
		if r.Stream && r.TTFT > 0 {
			ttft = append(ttft, r.TTFT)
		}
	}
	if elapsed > 0 {
		s.RPS = round3(float64(len(rs)) / elapsed.Seconds())
	}
	s.Total, s.TTFT = distOf(total), distOf(ttft)
	return s
}

// errorRate is the share of requests that were not ok.
func (s summary) errorRate() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Requests-s.OK) / float64(s.Requests)
}

// outcomesString renders the outcome counts in a stable order.
func (s summary) outcomesString() string {
	keys := make([]string, 0, len(s.Outcomes))
	for k := range s.Outcomes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s×%d", k, s.Outcomes[k])
	}
	return strings.Join(parts, " ")
}

// table renders rows as a Markdown table.
func table(header []string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(header, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat("---|", len(header)) + "\n")
	for _, r := range rows {
		b.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	return b.String()
}

func f1(v float64) string { return fmt.Sprintf("%.1f", v) }
func f2(v float64) string { return fmt.Sprintf("%.2f", v) }
