// Package eval is the operator-side agent evaluation harness behind `agentbox eval`.
//
// It runs a suite of tasks (research questions and coding/terminal tasks) against a running server
// through the public operator REST API, captures each task's trajectory (events, Gateway call journal,
// attempts, ledger, outcome) as JSONL, grades the outcomes, and writes a pinned run manifest plus
// summary reports that can be compared run against run.
//
// The package is a pure API client: it does not import any server internals (enforced by archtest),
// so it evaluates exactly what an operator would see. See docs/design/2026-10-10-eval-platform-design.md.
package eval

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/jcs"
)

// Task kinds.
const (
	KindCoding   = "coding"
	KindResearch = "research"
)

// Suite is a parsed evaluation suite.
type Suite struct {
	Name        string   `json:"name"`
	Version     int      `json:"version,omitempty"`
	Description string   `json:"description,omitempty"`
	Defaults    Defaults `json:"defaults,omitempty"`
	Tasks       []Task   `json:"tasks"`
}

// Defaults apply to every task that does not set the field itself.
type Defaults struct {
	// Timeout is the per-task wall-clock limit (Go duration, e.g. "5m"); the runner cancels the task on expiry.
	Timeout string `json:"timeout,omitempty"`
	// Limits is the task's `limits` object for POST /tasks (e.g. {"budget_micro": 200000}).
	Limits json.RawMessage `json:"limits,omitempty"`
	// WallMs is the exec wall limit for coding checkers.
	WallMs int `json:"wall_ms,omitempty"`
}

// Task is one suite task.
type Task struct {
	ID   string   `json:"id"`
	Kind string   `json:"kind"`
	Tags []string `json:"tags,omitempty"`

	// Coding tasks.
	Prompt    string            `json:"prompt,omitempty"`
	Reference string            `json:"reference,omitempty"` // reference solution (--agent reference)
	Files     map[string]string `json:"files,omitempty"`     // fixture files placed next to solution.py
	Check     string            `json:"check,omitempty"`     // Python checker; exit code is the verdict
	FakeReply string            `json:"fake_reply,omitempty"`
	WallMs    int               `json:"wall_ms,omitempty"`

	// Research tasks.
	Topic    string          `json:"topic,omitempty"`
	Research json.RawMessage `json:"research,omitempty"` // deepresearch config overrides

	Expect  Expect          `json:"expect,omitempty"`
	Judge   *JudgeSpec      `json:"judge,omitempty"`
	Timeout string          `json:"timeout,omitempty"`
	Limits  json.RawMessage `json:"limits,omitempty"`
}

// Expect holds the deterministic grading expectations.
type Expect struct {
	ExitCode       *int     `json:"exit_code,omitempty"`
	StdoutEquals   *string  `json:"stdout_equals,omitempty"`
	StdoutContains []string `json:"stdout_contains,omitempty"`
	StdoutRegex    string   `json:"stdout_regex,omitempty"`

	MinCitations      *int     `json:"min_citations,omitempty"`
	MinLocatableRatio *float64 `json:"min_locatable_ratio,omitempty"`
	MustMention       []string `json:"must_mention,omitempty"`
}

// JudgeSpec configures the optional LLM judge for a task.
type JudgeSpec struct {
	Rubric    string   `json:"rubric"`
	Threshold *float64 `json:"threshold,omitempty"` // default 0.6
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// fixture names are plain relative file names (no directories), so the exec harness can write them safely.
var filePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// LoadSuite reads a suite from a .yaml/.yml or .json file.
func LoadSuite(path string) (*Suite, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("eval: read suite: %w", err)
	}
	ext := strings.ToLower(filepath.Ext(path))
	s, err := ParseSuite(b, ext == ".yaml" || ext == ".yml")
	if err != nil {
		return nil, fmt.Errorf("eval: suite %s: %w", path, err)
	}
	return s, nil
}

// ParseSuite parses and validates a suite. YAML is converted to JSON first, so both formats share one strict
// schema (unknown fields are rejected).
func ParseSuite(data []byte, isYAML bool) (*Suite, error) {
	if isYAML {
		var v any
		if err := yaml.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("yaml: %w", err)
		}
		j, err := yamlToJSON(v)
		if err != nil {
			return nil, err
		}
		if data, err = json.Marshal(j); err != nil {
			return nil, fmt.Errorf("yaml to json: %w", err)
		}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var s Suite
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode: trailing data after the suite object")
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// yamlToJSON converts YAML-decoded values into JSON-compatible ones (map keys must be strings).
func yamlToJSON(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			c, err := yamlToJSON(val)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			ks, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("yaml: non-string key %v", k)
			}
			c, err := yamlToJSON(val)
			if err != nil {
				return nil, err
			}
			out[ks] = c
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			c, err := yamlToJSON(val)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	case time.Time:
		return t.Format(time.RFC3339Nano), nil
	}
	return v, nil
}

func (s *Suite) validate() error {
	if !idPattern.MatchString(s.Name) {
		return fmt.Errorf("name %q must match %s", s.Name, idPattern)
	}
	if len(s.Tasks) == 0 {
		return errors.New("suite has no tasks")
	}
	if err := checkDuration("defaults.timeout", s.Defaults.Timeout); err != nil {
		return err
	}
	if err := checkObject("defaults.limits", s.Defaults.Limits); err != nil {
		return err
	}
	seen := map[string]bool{}
	for i := range s.Tasks {
		t := &s.Tasks[i]
		if !idPattern.MatchString(t.ID) {
			return fmt.Errorf("tasks[%d]: id %q must match %s", i, t.ID, idPattern)
		}
		if seen[t.ID] {
			return fmt.Errorf("tasks[%d]: duplicate id %q", i, t.ID)
		}
		seen[t.ID] = true
		if err := t.validate(); err != nil {
			return fmt.Errorf("task %s: %w", t.ID, err)
		}
	}
	return nil
}

func (t *Task) validate() error {
	switch t.Kind {
	case KindCoding:
		if strings.TrimSpace(t.Prompt) == "" || strings.TrimSpace(t.Check) == "" {
			return errors.New("coding tasks need prompt and check")
		}
		if t.Topic != "" || len(t.Research) > 0 {
			return errors.New("coding tasks cannot set topic or research")
		}
		for name := range t.Files {
			if !filePattern.MatchString(name) || name == "solution.py" || name == "check.py" || strings.HasPrefix(name, ".") {
				return fmt.Errorf("invalid fixture file name %q", name)
			}
		}
		if t.WallMs < 0 {
			return errors.New("wall_ms must be positive")
		}
	case KindResearch:
		if strings.TrimSpace(t.Topic) == "" {
			return errors.New("research tasks need topic")
		}
		if t.Prompt != "" || t.Check != "" || t.Reference != "" || len(t.Files) > 0 {
			return errors.New("research tasks cannot set prompt, check, reference or files")
		}
		if err := checkObject("research", t.Research); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown kind %q (want %s or %s)", t.Kind, KindCoding, KindResearch)
	}
	if t.Expect.StdoutRegex != "" {
		if _, err := regexp.Compile(t.Expect.StdoutRegex); err != nil {
			return fmt.Errorf("expect.stdout_regex: %w", err)
		}
	}
	if r := t.Expect.MinLocatableRatio; r != nil && (*r < 0 || *r > 1) {
		return errors.New("expect.min_locatable_ratio must be within [0, 1]")
	}
	if t.Judge != nil {
		if strings.TrimSpace(t.Judge.Rubric) == "" {
			return errors.New("judge.rubric is required")
		}
		if th := t.Judge.Threshold; th != nil && (*th < 0 || *th > 1) {
			return errors.New("judge.threshold must be within [0, 1]")
		}
	}
	if err := checkDuration("timeout", t.Timeout); err != nil {
		return err
	}
	return checkObject("limits", t.Limits)
}

func checkDuration(field, v string) error {
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fmt.Errorf("%s %q must be a positive Go duration", field, v)
	}
	return nil
}

func checkObject(field string, raw json.RawMessage) error {
	if len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("%s must be a JSON object", field)
	}
	return nil
}

// Hash is the sha256 of the suite's JCS canonical JSON form: equal for the same suite in YAML or JSON.
func (s *Suite) Hash() (string, error) {
	canon, err := jcs.Canonical(s)
	if err != nil {
		return "", fmt.Errorf("eval: canonical suite: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// TaskByID returns the task with the given id.
func (s *Suite) TaskByID(id string) (*Task, bool) {
	for i := range s.Tasks {
		if s.Tasks[i].ID == id {
			return &s.Tasks[i], true
		}
	}
	return nil, false
}

// EffectiveTimeout is the task timeout, else the suite default, else fallback.
func (s *Suite) EffectiveTimeout(t *Task, fallback time.Duration) time.Duration {
	for _, v := range []string{t.Timeout, s.Defaults.Timeout} {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

// EffectiveLimits is the task's limits object, else the suite default (nil when neither is set).
func (s *Suite) EffectiveLimits(t *Task) json.RawMessage {
	if len(t.Limits) > 0 && string(bytes.TrimSpace(t.Limits)) != "null" {
		return t.Limits
	}
	if len(s.Defaults.Limits) > 0 && string(bytes.TrimSpace(s.Defaults.Limits)) != "null" {
		return s.Defaults.Limits
	}
	return nil
}

// EffectiveWallMs is the checker wall limit for a coding task (0 = server default).
func (s *Suite) EffectiveWallMs(t *Task) int {
	if t.WallMs > 0 {
		return t.WallMs
	}
	return s.Defaults.WallMs
}
