package eval

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal operator client for the agentbox REST API (the subset the evaluator needs).
// The token is only placed in the Authorization header and never appears in errors.
type Client struct {
	Base  string // e.g. http://127.0.0.1:8080
	Token string
	HTTP  *http.Client
	// Retries is the number of extra tries for transient failures (connection errors, 502/503/504). Default 4.
	Retries int
	// Sleep is the backoff sleep (tests inject a no-op). Default time.Sleep honoring ctx.
	Sleep func(ctx context.Context, d time.Duration)
}

// APIError is a non-2xx API response.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("api %d %s: %s", e.Status, e.Code, e.Message)
}

// Event is one task event (operator view).
type Event struct {
	TaskSeq   int64           `json:"task_seq"`
	AttemptID string          `json:"attempt_id,omitempty"`
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	WorkerSeq int64           `json:"worker_seq,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	TS        time.Time       `json:"ts"`
}

// Inspection is GET /tasks/{id}/inspect (the fields the evaluator uses are typed; the rest is kept raw).
type Inspection struct {
	Task        TaskView          `json:"task"`
	Attempts    []json.RawMessage `json:"attempts"`
	Checkpoints []json.RawMessage `json:"checkpoints"`
	Calls       []Call            `json:"calls"`
	Budget      *Budget           `json:"budget,omitempty"`
	Subruns     []json.RawMessage `json:"subruns"`
}

// TaskView is the task object.
type TaskView struct {
	TaskID        string    `json:"task_id"`
	Status        string    `json:"status"`
	StatusReason  string    `json:"status_reason,omitempty"`
	AttemptsTotal int64     `json:"attempts_total"`
	CreatedAt     time.Time `json:"created_at"`
}

// Call is one Gateway journal entry (metadata only — the API never returns bodies or prompts).
type Call struct {
	CallID            string    `json:"call_id"`
	Endpoint          string    `json:"endpoint"`
	Model             string    `json:"model,omitempty"`
	SubrunID          string    `json:"subrun_id,omitempty"`
	State             string    `json:"state"`
	Source            string    `json:"source,omitempty"`
	TriesUsed         int64     `json:"tries_used"`
	CostChargedMicro  int64     `json:"cost_charged_micro"`
	ResultRef         string    `json:"result_ref,omitempty"`
	FailReason        string    `json:"fail_reason,omitempty"`
	SupersedesCallID  string    `json:"supersedes_call_id,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	Tries             []Try     `json:"tries"`
	PossibleDuplicate bool      `json:"possible_external_duplicate,omitempty"`
}

// Try is one try of a call.
type Try struct {
	TryNo     int64  `json:"try_no"`
	AttemptID string `json:"attempt_id,omitempty"`
	State     string `json:"state"`
	Outcome   string `json:"outcome,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	CostMicro int64  `json:"cost_micro,omitempty"`
	Error     string `json:"error,omitempty"`
	QueueMs   int64  `json:"queue_ms,omitempty"`
	WallMs    int64  `json:"wall_ms,omitempty"`
}

// Budget is the task-layer ledger.
type Budget struct {
	LimitMicro    int64 `json:"limit_micro"`
	ReservedMicro int64 `json:"reserved_micro"`
	SpentMicro    int64 `json:"spent_micro"`
	UnknownMicro  int64 `json:"unknown_micro"`
	ToolCallsUsed int64 `json:"tool_calls_used"`
}

// Result is the pinned task result.
type Result struct {
	Summary string         `json:"summary"`
	Outputs []PinnedOutput `json:"outputs"`
}

// PinnedOutput is one pinned output artifact.
type PinnedOutput struct {
	ArtifactID string `json:"artifact_id"`
	Version    int64  `json:"version,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
}

func (c *Client) retries() int {
	if c.Retries <= 0 {
		return 4
	}
	return c.Retries
}

func (c *Client) sleep(ctx context.Context, d time.Duration) {
	if c.Sleep != nil {
		c.Sleep(ctx, d)
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) newRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.Base, "/")+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return req, nil
}

// redact removes the token from an error text.
func (c *Client) redact(err error) error {
	if err == nil || c.Token == "" || !strings.Contains(err.Error(), c.Token) {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), c.Token, "[REDACTED]"))
}

func transient(status int) bool {
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// do sends a request with retries on transient failures and returns the body of a 2xx response.
func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	var lastErr error
	for try := 0; try <= c.retries(); try++ {
		if try > 0 {
			c.sleep(ctx, backoff(try))
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		req, err := c.newRequest(ctx, method, path, body)
		if err != nil {
			return nil, err
		}
		resp, err := c.httpClient().Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = c.redact(err)
			continue
		}
		data, rerr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		if resp.StatusCode/100 == 2 {
			return data, nil
		}
		aerr := parseAPIError(resp.StatusCode, data)
		if !transient(resp.StatusCode) {
			return nil, aerr
		}
		lastErr = aerr
	}
	return nil, fmt.Errorf("eval: %s %s: %w", method, path, lastErr)
}

func backoff(try int) time.Duration {
	d := 200 * time.Millisecond << min(try-1, 5)
	return min(d, 5*time.Second)
}

func parseAPIError(status int, data []byte) *APIError {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &e) // a non-JSON body leaves code empty
	return &APIError{Status: status, Code: e.Error.Code, Message: e.Error.Message}
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	data, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("eval: decode %s: %w", path, err)
	}
	return nil
}

// ServerInfo returns GET /server-info as raw JSON.
func (c *Client) ServerInfo(ctx context.Context) (json.RawMessage, error) {
	data, err := c.do(ctx, http.MethodGet, "/server-info", nil)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// Status returns GET /status (reachability check).
func (c *Client) Status(ctx context.Context) error {
	var v map[string]any
	return c.getJSON(ctx, "/status", &v)
}

// CreateTask posts a task; requestID makes the creation idempotent across client retries.
func (c *Client) CreateTask(ctx context.Context, requestID string, spec, limits json.RawMessage) (string, error) {
	body := map[string]json.RawMessage{"spec": spec}
	rid, _ := json.Marshal(requestID)
	body["request_id"] = rid
	if len(limits) > 0 {
		body["limits"] = limits
	}
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	data, err := c.do(ctx, http.MethodPost, "/tasks", b)
	if err != nil {
		return "", err
	}
	var res struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(data, &res); err != nil || res.TaskID == "" {
		return "", fmt.Errorf("eval: create task: bad response %q", truncate(string(data), 200))
	}
	return res.TaskID, nil
}

// Cancel requests cancellation of a task.
func (c *Client) Cancel(ctx context.Context, taskID, requestID, reason string) error {
	b, _ := json.Marshal(map[string]string{"request_id": requestID, "reason": reason})
	_, err := c.do(ctx, http.MethodPost, "/tasks/"+url.PathEscape(taskID)+"/cancel", b)
	return err
}

// Inspect returns GET /tasks/{id}/inspect.
func (c *Client) Inspect(ctx context.Context, taskID string) (*Inspection, json.RawMessage, error) {
	data, err := c.do(ctx, http.MethodGet, "/tasks/"+url.PathEscape(taskID)+"/inspect", nil)
	if err != nil {
		return nil, nil, err
	}
	var in Inspection
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, nil, fmt.Errorf("eval: decode inspect: %w", err)
	}
	return &in, json.RawMessage(data), nil
}

// Result returns the pinned result of a terminal task.
func (c *Client) Result(ctx context.Context, taskID string) (*Result, error) {
	var r Result
	if err := c.getJSON(ctx, "/tasks/"+url.PathEscape(taskID)+"/result", &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Artifact downloads one output artifact version (version 0 = latest).
func (c *Client) Artifact(ctx context.Context, taskID, artifactID string, version int64) ([]byte, error) {
	p := "/tasks/" + url.PathEscape(taskID) + "/artifacts/" + url.PathEscape(artifactID)
	if version > 0 {
		p += "/versions/" + strconv.FormatInt(version, 10)
	}
	return c.do(ctx, http.MethodGet, p, nil)
}

// Events streams the task's events from the beginning until task_terminal, reconnecting with Last-Event-ID.
// Every new event is passed to fn in order. It returns when task_terminal was seen, ctx ends, or the stream
// fails without progress more than the retry budget.
func (c *Client) Events(ctx context.Context, taskID string, fn func(Event)) error {
	var last int64
	idle := 0
	for {
		terminal, progressed, err := c.eventsOnce(ctx, taskID, &last, fn)
		if terminal {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var aerr *APIError
		if errors.As(err, &aerr) && !transient(aerr.Status) {
			return err
		}
		if progressed {
			idle = 0
		} else {
			idle++
		}
		if idle > c.retries()+6 {
			if err == nil {
				err = errors.New("stream ended without task_terminal")
			}
			return fmt.Errorf("eval: events %s: %w", taskID, err)
		}
		if idle > 0 {
			c.sleep(ctx, backoff(idle))
		}
	}
}

func (c *Client) eventsOnce(ctx context.Context, taskID string, last *int64, fn func(Event)) (terminal, progressed bool, err error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/tasks/"+url.PathEscape(taskID)+"/events", nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if *last > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(*last, 10))
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return false, false, c.redact(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return false, false, parseAPIError(resp.StatusCode, data)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	var id, typ string
	var data strings.Builder
	for sc.Scan() {
		line := strings.TrimSuffix(sc.Text(), "\r")
		switch {
		case line == "":
			if data.Len() > 0 {
				seq, perr := strconv.ParseInt(id, 10, 64)
				if perr == nil && seq > *last {
					var ev Event
					if json.Unmarshal([]byte(data.String()), &ev) == nil {
						if ev.Type == "" {
							ev.Type = typ
						}
						if ev.TaskSeq == 0 {
							ev.TaskSeq = seq
						}
						fn(ev)
						*last = seq
						progressed = true
						if typ == "task_terminal" || ev.Type == "task_terminal" {
							return true, true, nil
						}
					}
				}
			}
			id, typ = "", ""
			data.Reset()
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "id:"):
			id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "event:"):
			typ = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return false, progressed, sc.Err()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
