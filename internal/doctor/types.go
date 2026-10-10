package doctor

import "time"

// SchemaFindings is the schema of findings.json.
const SchemaFindings = "agentbox.doctor.findings/v1"

// Severities of a finding.
const (
	SevCritical = "critical" // explains failed tasks
	SevWarning  = "warning"  // explains slow tasks or wastes money
	SevInfo     = "info"     // worth knowing, nothing failed because of it
)

// Proposal kinds.
const (
	KindApply    = "apply"    // an allowlisted server flag; --apply-to writes it into the experiment config
	KindAdvisory = "advisory" // shown and commented in the patch, never written
)

// Report is the result of one analysis (findings.json).
type Report struct {
	Schema        string         `json:"schema"`
	GeneratedAt   time.Time      `json:"generated_at"`
	Source        Source         `json:"source"`
	Totals        Totals         `json:"totals"`
	Causes        []CauseCount   `json:"causes"`
	Findings      []Finding      `json:"findings"`
	Proposals     []Proposal     `json:"proposals"` // merged: one per flag, plus advisories
	Rejected      []Proposal     `json:"rejected,omitempty"`
	Slowest       []TaskSummary  `json:"slowest"`
	Unexplained   []TaskSummary  `json:"unexplained"`
	Observability *ObsReport     `json:"observability,omitempty"`
	Advisor       *AdvisorReport `json:"advisor,omitempty"`
	Notes         []string       `json:"notes,omitempty"`
}

// Source describes what was analysed.
type Source struct {
	Kind   string `json:"kind"` // eval_run | server
	Dir    string `json:"dir,omitempty"`
	RunID  string `json:"run_id,omitempty"`
	Suite  string `json:"suite,omitempty"`
	Agent  string `json:"agent,omitempty"`
	Server string `json:"server,omitempty"` // address without userinfo
	Since  string `json:"since,omitempty"`
	Until  string `json:"until,omitempty"`
	Config string `json:"config,omitempty"` // the flags file the run was made with, when given
}

// Totals counts the analysed tasks.
type Totals struct {
	Tasks     int `json:"tasks"`
	Failed    int `json:"failed"`
	Slow      int `json:"slow"` // slow tasks that did not fail
	Explained int `json:"explained"`
	// SlowThresholdMs is the slow threshold per task kind.
	SlowThresholdMs map[string]int64 `json:"slow_threshold_ms"`
}

// CauseCount is the number of failed and slow tasks attributed to one cause.
type CauseCount struct {
	Cause  string `json:"cause"`
	Failed int    `json:"failed"`
	Slow   int    `json:"slow"`
}

// Finding is the output of one rule.
type Finding struct {
	Rule      string     `json:"rule"`
	Severity  string     `json:"severity"`
	Title     string     `json:"title"`
	Summary   string     `json:"summary"`
	Failed    int        `json:"failed_tasks"` // failed tasks attributed to this rule
	Slow      int        `json:"slow_tasks"`   // slow tasks attributed to this rule
	Evidence  Evidence   `json:"evidence"`
	Proposals []Proposal `json:"proposals,omitempty"`
}

// Evidence backs a finding with numbers and examples.
type Evidence struct {
	Counts    map[string]int64   `json:"counts,omitempty"`
	LatencyMs map[string]int64   `json:"latency_ms,omitempty"`
	Shares    map[string]float64 `json:"shares,omitempty"`
	Examples  []Example          `json:"examples,omitempty"`
	Detail    []string           `json:"detail,omitempty"`
}

// Example points at one affected task (and call, and trace when known).
type Example struct {
	TaskID       string `json:"task_id,omitempty"` // suite task id (eval runs)
	Rep          int    `json:"rep,omitempty"`
	ServerTaskID string `json:"server_task_id"`
	CallID       string `json:"call_id,omitempty"`
	TraceID      string `json:"trace_id,omitempty"`
	Note         string `json:"note,omitempty"`
}

// Proposal is one configuration change.
type Proposal struct {
	Kind   string `json:"kind"`           // apply | advisory
	Flag   string `json:"flag,omitempty"` // apply: the server flag (with leading --)
	From   string `json:"from,omitempty"` // current value (or "default <v>")
	To     string `json:"to,omitempty"`
	Text   string `json:"text,omitempty"` // advisory text
	Rule   string `json:"rule"`
	Source string `json:"source"` // rule | model
	Reason string `json:"reason"`
}

// Breakdown is the per-task timing breakdown (milliseconds). Call times are summed try latencies; parallel
// calls overlap, so the components may add up to more than the latency.
type Breakdown struct {
	QueueMs      int64 `json:"queue_ms"`
	StartMs      int64 `json:"start_ms"`
	ModelMs      int64 `json:"model_ms"`
	SearchMs     int64 `json:"search_ms"`
	FetchMs      int64 `json:"fetch_ms"`
	ExecMs       int64 `json:"exec_ms"`
	OtherCallsMs int64 `json:"other_calls_ms"`
	BackoffEstMs int64 `json:"backoff_est_ms"`
	StopMs       int64 `json:"stop_ms"`
}

// TaskSummary is one task in the report.
type TaskSummary struct {
	TaskID       string      `json:"task_id,omitempty"`
	Rep          int         `json:"rep,omitempty"`
	ServerTaskID string      `json:"server_task_id"`
	Kind         string      `json:"kind"`
	Status       string      `json:"status"`
	Verdict      string      `json:"verdict"`
	Category     string      `json:"category,omitempty"`
	Cause        string      `json:"cause,omitempty"`
	LatencyMs    int64       `json:"latency_ms"`
	Breakdown    Breakdown   `json:"breakdown"`
	TraceID      string      `json:"trace_id,omitempty"`
	Spans        []SpanShare `json:"spans,omitempty"`
}

// SpanShare aggregates the spans of one trace by name.
type SpanShare struct {
	Name    string  `json:"name"`
	Count   int     `json:"count"`
	TotalMs int64   `json:"total_ms"`
	Share   float64 `json:"share"` // total / trace duration
}

// ObsReport holds the optional observability enrichment.
type ObsReport struct {
	Tempo      string        `json:"tempo,omitempty"`
	Prometheus string        `json:"prometheus,omitempty"`
	Loki       string        `json:"loki,omitempty"`
	TraceIDs   int           `json:"trace_ids_found"`
	Metrics    []MetricTable `json:"metrics,omitempty"`
	Logs       []LogCount    `json:"logs,omitempty"`
	Notes      []string      `json:"notes,omitempty"`
}

// MetricTable is the result of one PromQL query.
type MetricTable struct {
	Title string      `json:"title"`
	Query string      `json:"query"`
	Rows  []MetricRow `json:"rows"`
}

// MetricRow is one series of an instant vector.
type MetricRow struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

// LogCount is the number of log lines with one message in the window, with sample trace ids.
type LogCount struct {
	Message  string   `json:"message"`
	Count    int      `json:"count"`
	TraceIDs []string `json:"trace_ids,omitempty"`
}

// AdvisorReport records the optional model step.
type AdvisorReport struct {
	Model       string     `json:"model"`
	Host        string     `json:"host"`
	Called      bool       `json:"called"`
	SpentMicro  int64      `json:"spent_micro"`
	BudgetMicro int64      `json:"budget_micro"`
	Summary     string     `json:"summary,omitempty"`
	Accepted    []Proposal `json:"accepted,omitempty"`
	Rejected    []Proposal `json:"rejected,omitempty"`
	Error       string     `json:"error,omitempty"`
}
