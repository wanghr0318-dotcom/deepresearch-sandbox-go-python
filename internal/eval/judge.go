package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

// Judge is the optional LLM-judge grader: an OpenAI-compatible chat endpoint called from the CLI (outside the
// sandbox) with a hard budget and a hard call limit. Off unless configured (--judge-model).
type Judge struct {
	BaseURL string // e.g. https://api.moonshot.cn/v1 (POST <BaseURL>/chat/completions)
	Model   string
	APIKey  string // from the environment only; never logged or recorded
	HTTP    *http.Client
	// MaxTokens bounds the judge reply (default 512); it is also the worst-case output reservation.
	MaxTokens int
	// Prices in micro-USD per million tokens (configuration, used for reservation and accounting). The CLI
	// requires both to be positive, so the budget always binds.
	PriceInMicroPerMTok, PriceOutMicroPerMTok int64
	// BudgetMicro is the total judge budget for the run.
	BudgetMicro int64
	// MaxCalls is a hard limit on judge calls for the run, independent of prices (default 50).
	MaxCalls int
	// Required turns a judge that could not grade (budget, call limit, error) into a failed grade
	// (category judge_unavailable) instead of a skipped one.
	Required bool
	// MaxAnswerBytes caps the answer text sent to the judge (default 24 KiB).
	MaxAnswerBytes int

	mu       sync.Mutex
	spent    int64 // settled cost
	pending  int64 // reservations in flight
	inflight int   // calls reserved but not settled
	calls    int
	skipped  int
	errors   int
}

// JudgeStats summarises judge usage for the run.
type JudgeStats struct {
	Model      string `json:"model"`
	Calls      int    `json:"calls"`
	Errors     int    `json:"errors"`  // calls that returned no usable score
	Skipped    int    `json:"skipped"` // judgements not attempted (budget or call limit)
	SpentMicro int64  `json:"spent_micro"`
	Budget     int64  `json:"budget_micro"`
	MaxCalls   int    `json:"max_calls"`
	Required   bool   `json:"required,omitempty"`
}

// Stats returns the usage so far.
func (j *Judge) Stats() JudgeStats {
	j.mu.Lock()
	defer j.mu.Unlock()
	return JudgeStats{Model: j.Model, Calls: j.calls, Errors: j.errors, Skipped: j.skipped, SpentMicro: j.spent,
		Budget: j.BudgetMicro, MaxCalls: j.maxCalls(), Required: j.Required}
}

func (j *Judge) maxTokens() int {
	if j.MaxTokens <= 0 {
		return 512
	}
	return j.MaxTokens
}

func (j *Judge) maxCalls() int {
	if j.MaxCalls <= 0 {
		return 50
	}
	return j.MaxCalls
}

// tokens is a conservative token estimate (≈ 1 token per 3 bytes, CJK-safe upper bound).
func tokens(s string) int64 { return int64(len(s))/3 + 16 }

func (j *Judge) cost(in, out int64) int64 {
	return (in*j.PriceInMicroPerMTok + out*j.PriceOutMicroPerMTok + 999_999) / 1_000_000
}

// reserve books the worst-case cost of one call; it refuses (with the reason) when the budget or the call
// limit would be exceeded.
func (j *Judge) reserve(worst int64) (bool, string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.calls+j.inflight >= j.maxCalls():
		j.skipped++
		return false, fmt.Sprintf("judge call limit (%d) reached", j.maxCalls())
	case j.spent+j.pending+worst > j.BudgetMicro:
		j.skipped++
		return false, "judge budget exhausted"
	}
	j.pending += worst
	j.inflight++
	return true, ""
}

func (j *Judge) settle(worst, actual int64, failed bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.pending -= worst
	j.inflight--
	j.spent += actual
	j.calls++
	if failed {
		j.errors++
	}
}

const judgeSystem = "You are a strict evaluator. Score the ANSWER against the RUBRIC for the TASK. " +
	"Reply with only a JSON object {\"score\": <number between 0 and 1>, \"reason\": \"<one sentence>\"}."

var jsonObject = regexp.MustCompile(`(?s)\{.*\}`)

// unavailable is the grade when the judge could not score: skipped, or failed when the judge is required.
func (j *Judge) unavailable(detail string) Grade {
	if j.Required {
		return Grade{Grader: "llm_judge", Category: "judge_unavailable", Detail: detail}
	}
	return Grade{Grader: "llm_judge", Skipped: true, Detail: detail}
}

// Grade asks the judge to score answer against the task's rubric.
func (j *Judge) Grade(ctx context.Context, t *Task, answer string) Grade {
	limit := j.MaxAnswerBytes
	if limit <= 0 {
		limit = 24 << 10
	}
	if len(answer) > limit {
		answer = answer[:limit] + "\n…(truncated)"
	}
	taskText := t.Prompt
	if t.Kind == KindResearch {
		taskText = t.Topic
	}
	user := fmt.Sprintf("TASK:\n%s\n\nRUBRIC:\n%s\n\nANSWER:\n%s", taskText, t.Judge.Rubric, answer)
	worst := j.cost(tokens(judgeSystem)+tokens(user), int64(j.maxTokens()))
	if ok, why := j.reserve(worst); !ok {
		return j.unavailable(why)
	}
	score, reason, in, out, err := j.call(ctx, user)
	actual := worst // usage unknown: settle the reservation
	if in+out > 0 {
		actual = j.cost(in, out)
	}
	j.settle(worst, actual, err != nil)
	if err != nil {
		return j.unavailable("judge error: " + err.Error())
	}
	th := 0.6
	if t.Judge.Threshold != nil {
		th = *t.Judge.Threshold
	}
	g := Grade{Grader: "llm_judge", Score: score, Detail: reason, Category: "judge_below_threshold"}
	if score >= th {
		g.Pass, g.Category = true, ""
	}
	return g
}

func (j *Judge) call(ctx context.Context, user string) (score float64, reason string, in, out int64, err error) {
	body, _ := json.Marshal(map[string]any{
		"model":       j.Model,
		"max_tokens":  j.maxTokens(),
		"temperature": 0,
		"messages": []map[string]string{
			{"role": "system", "content": judgeSystem},
			{"role": "user", "content": user},
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(j.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return 0, "", 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if j.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+j.APIKey)
	}
	hc := j.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, "", 0, 0, errors.New("request failed") // the transport error may echo the URL; keep it terse
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, "", 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", 0, 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	var r struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &r); err != nil || len(r.Choices) == 0 {
		return 0, "", 0, 0, errors.New("malformed reply")
	}
	in, out = r.Usage.PromptTokens, r.Usage.CompletionTokens
	var v struct {
		Score  *float64 `json:"score"`
		Reason string   `json:"reason"`
	}
	m := jsonObject.FindString(r.Choices[0].Message.Content)
	if m == "" || json.Unmarshal([]byte(m), &v) != nil || v.Score == nil || *v.Score < 0 || *v.Score > 1 {
		return 0, "", in, out, errors.New("judge reply is not {score, reason}")
	}
	return *v.Score, truncate(v.Reason, 300), in, out, nil
}
