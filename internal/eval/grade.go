package eval

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Grade is the verdict of one grader on one task run.
type Grade struct {
	Grader  string  `json:"grader"`
	Pass    bool    `json:"pass"`
	Skipped bool    `json:"skipped,omitempty"`
	Score   float64 `json:"score"`
	Detail  string  `json:"detail,omitempty"`
	// Category is the failure category this grader reports when it fails (see Outcome).
	Category string `json:"category,omitempty"`
}

// Outcome is the overall verdict of a task run.
type Outcome struct {
	Verdict  string `json:"verdict"` // pass | fail | error
	Category string `json:"category,omitempty"`
}

// Verdicts.
const (
	VerdictPass  = "pass"
	VerdictFail  = "fail"
	VerdictError = "error"
)

// CodingArtifact is the `eval` output artifact written by the eval worker for coding tasks
// (schema agentbox.eval.coding/v1).
type CodingArtifact struct {
	Schema         string    `json:"schema"`
	TaskID         string    `json:"task_id"`
	Agent          string    `json:"agent"`
	Extract        string    `json:"extract,omitempty"` // code_block | raw | reference
	SolutionSHA256 string    `json:"solution_sha256,omitempty"`
	SolutionBytes  int       `json:"solution_bytes,omitempty"`
	Exec           *ExecInfo `json:"exec,omitempty"`
	Error          string    `json:"error,omitempty"`
}

// ExecInfo is the checker run in the exec sandbox.
type ExecInfo struct {
	CallID          string `json:"call_id,omitempty"`
	Status          string `json:"status"` // completed | timed_out | cancelled | unknown
	ExitCode        *int   `json:"exit_code,omitempty"`
	Signal          int    `json:"signal,omitempty"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated,omitempty"`
	WallMs          int64  `json:"wall_ms,omitempty"`
	QueueMs         int64  `json:"queue_ms,omitempty"`
	ImageDigest     string `json:"image_digest,omitempty"`
}

// checkTimeoutExit is the eval harness's exit code when the checker exceeds its timeout inside the exec.
const checkTimeoutExit = 124

// gradeStatus is the task_status grader common to all kinds.
func gradeStatus(status, reason string) Grade {
	g := Grade{Grader: "task_status", Pass: status == "succeeded", Detail: status}
	if reason != "" {
		g.Detail += " (" + reason + ")"
	}
	if g.Pass {
		g.Score = 1
	} else {
		cat := "task_" + status
		if reason != "" {
			cat += ":" + reason
		}
		g.Category = cat
	}
	return g
}

// GradeCoding grades a coding task run from its eval artifact (nil when it could not be read).
func GradeCoding(t *Task, status, reason string, art *CodingArtifact) []Grade {
	gs := []Grade{gradeStatus(status, reason)}
	if art == nil || art.Exec == nil {
		detail := "no eval artifact"
		if art != nil && art.Error != "" {
			detail = art.Error
		}
		gs = append(gs, Grade{Grader: "exit_code", Detail: detail, Category: "no_check_result"})
		return gs
	}
	ex := art.Exec
	want := 0
	if t.Expect.ExitCode != nil {
		want = *t.Expect.ExitCode
	}
	ec := Grade{Grader: "exit_code"}
	switch {
	case ex.Status == "timed_out":
		ec.Detail, ec.Category = "checker timed out", "exec_timeout"
	case ex.Status != "completed":
		ec.Detail, ec.Category = "exec "+ex.Status, "exec_"+ex.Status
	case ex.ExitCode == nil:
		ec.Detail, ec.Category = fmt.Sprintf("killed by signal %d", ex.Signal), "exec_signal"
	case *ex.ExitCode == checkTimeoutExit && want != checkTimeoutExit:
		ec.Detail, ec.Category = "checker timed out inside the exec (harness exit 124)", "check_timeout"
	case *ex.ExitCode != want:
		ec.Detail, ec.Category = fmt.Sprintf("exit code %d, want %d", *ex.ExitCode, want), "wrong_exit_code"
	default:
		ec.Pass, ec.Score, ec.Detail = true, 1, fmt.Sprintf("exit code %d", want)
	}
	gs = append(gs, ec)
	if g, ok := gradeStdout(t.Expect, ex.Stdout); ok {
		gs = append(gs, g)
	}
	return gs
}

// gradeStdout applies the stdout expectations; ok is false when none is configured.
func gradeStdout(e Expect, stdout string) (Grade, bool) {
	if e.StdoutEquals == nil && len(e.StdoutContains) == 0 && e.StdoutRegex == "" {
		return Grade{}, false
	}
	g := Grade{Grader: "stdout", Category: "wrong_output"}
	var problems []string
	if e.StdoutEquals != nil && strings.TrimSpace(stdout) != strings.TrimSpace(*e.StdoutEquals) {
		problems = append(problems, fmt.Sprintf("stdout %q != %q", truncate(strings.TrimSpace(stdout), 120), truncate(strings.TrimSpace(*e.StdoutEquals), 120)))
	}
	for _, s := range e.StdoutContains {
		if !strings.Contains(stdout, s) {
			problems = append(problems, fmt.Sprintf("stdout lacks %q", s))
		}
	}
	if e.StdoutRegex != "" {
		if re, err := regexp.Compile(e.StdoutRegex); err != nil || !re.MatchString(stdout) {
			problems = append(problems, fmt.Sprintf("stdout does not match /%s/", e.StdoutRegex))
		}
	}
	if len(problems) == 0 {
		g.Pass, g.Score, g.Category, g.Detail = true, 1, "", "stdout matches"
	} else {
		g.Detail = strings.Join(problems, "; ")
	}
	return g, true
}

// Citation analysis of a research report.

var (
	citeRef      = regexp.MustCompile(`\[(\d{1,4})\]`)
	evidenceLine = regexp.MustCompile(`^\s*[-*]\s*\[(\d{1,4})\].*sha256:([0-9a-f]{64})`)
	evidenceHead = regexp.MustCompile(`(?m)^#{1,6}\s*(证据|Evidence)\s*$`)
)

// CitationStats is the citation analysis of one report.
type CitationStats struct {
	Cited     []int `json:"cited"`     // distinct [n] cited in the body
	Evidence  int   `json:"evidence"`  // evidence entries listed
	Locatable int   `json:"locatable"` // cited n whose evidence sha256 is a completed fetch result of this task
	Dangling  []int `json:"dangling,omitempty"`
}

// Ratio is locatable / cited (1 when nothing is cited).
func (c CitationStats) Ratio() float64 {
	if len(c.Cited) == 0 {
		return 1
	}
	return float64(c.Locatable) / float64(len(c.Cited))
}

// AnalyzeCitations parses the report body citations and the evidence list, and resolves each citation to
// a fetch result blob of the task (fetchRefs = result_ref of completed fetch calls).
func AnalyzeCitations(report string, fetchRefs map[string]bool) CitationStats {
	body, evidence := report, ""
	if loc := evidenceHead.FindStringIndex(report); loc != nil {
		body, evidence = report[:loc[0]], report[loc[1]:]
	}
	ev := map[int]string{}
	for _, line := range strings.Split(evidence, "\n") {
		if m := evidenceLine.FindStringSubmatch(line); m != nil {
			n, _ := strconv.Atoi(m[1])
			ev[n] = m[2]
		}
	}
	seen := map[int]bool{}
	var st CitationStats
	for _, m := range citeRef.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[1])
		if seen[n] {
			continue
		}
		seen[n] = true
		st.Cited = append(st.Cited, n)
		if sha, ok := ev[n]; ok && fetchRefs[sha] {
			st.Locatable++
		} else {
			st.Dangling = append(st.Dangling, n)
		}
	}
	sort.Ints(st.Cited)
	sort.Ints(st.Dangling)
	st.Evidence = len(ev)
	return st
}

// FetchRefs returns the result blobs of the completed fetch calls.
func FetchRefs(calls []Call) map[string]bool {
	out := map[string]bool{}
	for _, c := range calls {
		if endpointKind(c.Endpoint) == "fetch" && c.State == "completed" && c.ResultRef != "" {
			out[c.ResultRef] = true
		}
	}
	return out
}

// endpointKind maps a journal endpoint name to chat | search | fetch | exec | other.
func endpointKind(endpoint string) string {
	e := strings.ToLower(endpoint)
	for _, k := range []string{"chat", "search", "fetch", "exec"} {
		if strings.Contains(e, k) {
			return k
		}
	}
	return "other"
}

// GradeResearch grades a research task run from its report text and call journal.
func GradeResearch(t *Task, status, reason, report string, calls []Call) ([]Grade, CitationStats) {
	gs := []Grade{gradeStatus(status, reason)}
	st := AnalyzeCitations(report, FetchRefs(calls))
	minCites := 1
	if t.Expect.MinCitations != nil {
		minCites = *t.Expect.MinCitations
	}
	minRatio := 1.0
	if t.Expect.MinLocatableRatio != nil {
		minRatio = *t.Expect.MinLocatableRatio
	}
	cg := Grade{Grader: "citations", Score: st.Ratio(),
		Detail: fmt.Sprintf("%d cited, %d locatable, %d evidence", len(st.Cited), st.Locatable, st.Evidence)}
	switch {
	case report == "":
		cg.Score, cg.Category, cg.Detail = 0, "no_report", "no report"
	case len(st.Cited) < minCites:
		cg.Category = "no_citations"
		cg.Detail += fmt.Sprintf(" (want ≥ %d citations)", minCites)
	case st.Ratio() < minRatio:
		cg.Category = "unlocatable_citations"
		cg.Detail += fmt.Sprintf(" (dangling %v)", st.Dangling)
	default:
		cg.Pass = true
	}
	gs = append(gs, cg)
	if len(t.Expect.MustMention) > 0 {
		mg := Grade{Grader: "must_mention", Category: "missing_terms"}
		var missing []string
		low := strings.ToLower(report)
		for _, term := range t.Expect.MustMention {
			if !strings.Contains(low, strings.ToLower(term)) {
				missing = append(missing, term)
			}
		}
		mg.Score = float64(len(t.Expect.MustMention)-len(missing)) / float64(len(t.Expect.MustMention))
		if len(missing) == 0 {
			mg.Pass, mg.Category, mg.Detail = true, "", "all terms present"
		} else {
			mg.Detail = "missing " + strings.Join(missing, ", ")
		}
		gs = append(gs, mg)
	}
	return gs, st
}

// Decide combines grades into an outcome: pass if every non-skipped grade passes, otherwise the category of
// the first failing grade (graders are appended in priority order).
func Decide(gs []Grade) Outcome {
	for _, g := range gs {
		if g.Skipped || g.Pass {
			continue
		}
		cat := g.Category
		if cat == "" {
			cat = g.Grader + "_failed"
		}
		return Outcome{Verdict: VerdictFail, Category: cat}
	}
	return Outcome{Verdict: VerdictPass}
}

// parseCodingArtifact decodes the eval artifact.
func parseCodingArtifact(b []byte) (*CodingArtifact, error) {
	var a CodingArtifact
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, fmt.Errorf("eval artifact: %w", err)
	}
	return &a, nil
}
