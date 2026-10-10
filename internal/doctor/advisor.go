package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Advisor is the optional model step: one OpenAI-compatible chat call that reads the findings (metadata only) and
// returns a short diagnosis plus proposals from the same allowlist. Off unless configured.
type Advisor struct {
	BaseURL string // e.g. https://api.moonshot.cn/v1
	Model   string
	APIKey  string // from the environment only; never written anywhere
	// Prices in micro-USD per million tokens; both must be positive so the budget always binds.
	PriceInMicroPerMTok, PriceOutMicroPerMTok int64
	BudgetMicro                               int64
	MaxTokens                                 int // reply cap (default 1024), also the worst-case output reservation
	HTTP                                      *http.Client
}

const advisorSystem = `You are an SRE assistant for a sandboxed agent runtime (Go control plane, Python workers, a Gateway that
proxies model/search/fetch/exec calls with retries, a model fallback chain, circuit breakers and hedging).
You receive the findings of a deterministic trace analysis: counts, latencies, error codes and ids only.
A try cut by a timeout or deadline reports the latency at which it was cut, not the time the upstream needed;
ok-try percentiles are per call kind as labelled (search and fetch are separate kinds).
Write a short diagnosis (at most 8 sentences) for the operator and propose configuration changes.
You may ONLY propose these server flags, with values inside the bounds:
%s
Reply with one JSON object and nothing else:
{"summary": "<diagnosis>", "proposals": [{"flag": "--name", "value": "<value>", "reason": "<one sentence>"}]}
Propose nothing rather than guess. Never propose other flags.`

func allowlistText() string {
	var b strings.Builder
	for _, s := range allowlist {
		zero := ""
		if s.allowZero {
			zero = " (or 0 = off)"
		}
		fmt.Fprintf(&b, "- --%s: %s; range %s–%s%s; server default %s\n", s.name, s.meaning, s.format(s.min), s.format(s.max), zero, s.def)
	}
	return b.String()
}

// advisorInput is what the model sees: the findings without free text from tasks.
type advisorInput struct {
	Totals    Totals       `json:"totals"`
	Causes    []CauseCount `json:"causes"`
	Findings  []Finding    `json:"findings"`
	Proposals []Proposal   `json:"rule_proposals"`
	Config    []string     `json:"current_flags,omitempty"` // allowlisted flags only
}

// Run calls the model (if the budget allows) and returns the advisor report. Accepted proposals are validated
// against the allowlist; the caller merges them after the rule proposals.
func (ad *Advisor) Run(ctx context.Context, rep *Report, cfg *Flags) *AdvisorReport {
	ar := &AdvisorReport{Model: ad.Model, Host: hostOf(ad.BaseURL), BudgetMicro: ad.BudgetMicro}
	in := advisorInput{Totals: rep.Totals, Causes: rep.Causes, Findings: rep.Findings, Proposals: rep.Proposals}
	if cfg != nil {
		for _, s := range allowlist {
			if v, ok := cfg.Get(s.name); ok {
				in.Config = append(in.Config, "--"+s.name+"="+v)
			}
		}
	}
	for i := range in.Findings { // examples carry ids only, but keep the prompt small
		in.Findings[i].Evidence.Examples = in.Findings[i].Evidence.Examples[:min(2, len(in.Findings[i].Evidence.Examples))]
	}
	data, err := json.Marshal(in)
	if err != nil {
		ar.Error = err.Error()
		return ar
	}
	maxTokens := ad.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	system := fmt.Sprintf(advisorSystem, allowlistText())
	estIn := int64(len(system)+len(data))/2 + 64 // conservative: ≥ 1 token per 2 bytes
	worst := cost(estIn, int64(maxTokens), ad.PriceInMicroPerMTok, ad.PriceOutMicroPerMTok)
	if worst > ad.BudgetMicro {
		ar.Error = fmt.Sprintf("not called: worst-case cost %d micro-USD exceeds the budget %d", worst, ad.BudgetMicro)
		return ar
	}
	body, _ := json.Marshal(map[string]any{
		"model": ad.Model, "max_tokens": maxTokens,
		"messages": []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": string(data)}},
	})
	ar.Called = true
	reply, usage, err := ad.post(ctx, body)
	if usage.in > 0 || usage.out > 0 {
		ar.SpentMicro = cost(usage.in, usage.out, ad.PriceInMicroPerMTok, ad.PriceOutMicroPerMTok)
	} else if err == nil {
		ar.SpentMicro = worst // no usage reported: account the reservation
	}
	if err != nil {
		ar.Error = err.Error()
		return ar
	}
	var out struct {
		Summary   string `json:"summary"`
		Proposals []struct {
			Flag   string `json:"flag"`
			Value  any    `json:"value"`
			Reason string `json:"reason"`
		} `json:"proposals"`
	}
	if err := json.Unmarshal([]byte(extractJSON(reply)), &out); err != nil {
		ar.Error = "reply is not the requested JSON: " + err.Error()
		return ar
	}
	// Everything from the reply is untrusted text: flatten it before it can reach a file or a report.
	ar.Summary = truncateText(SafeText(out.Summary), 2000)
	for _, p := range out.Proposals {
		flag := "--" + strings.TrimLeft(truncateText(SafeText(p.Flag), 80), "-")
		value := truncateText(SafeText(fmt.Sprint(p.Value)), 80)
		pr := Proposal{Kind: KindApply, Flag: flag, To: value, Rule: "advisor", Source: "model", Reason: truncateText(SafeText(p.Reason), 300)}
		norm, verr := Validate(flag, value)
		if verr != nil {
			pr.Reason = SafeText(verr.Error())
			ar.Rejected = append(ar.Rejected, pr)
			continue
		}
		pr.To = norm
		_, pr.From = current(cfg, flag)
		ar.Accepted = append(ar.Accepted, pr)
	}
	return ar
}

type tokenUsage struct{ in, out int64 }

func (ad *Advisor) post(ctx context.Context, body []byte) (string, tokenUsage, error) {
	var u tokenUsage
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(ad.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", u, errors.New("invalid advisor base URL")
	}
	req.Header.Set("Content-Type", "application/json")
	if ad.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ad.APIKey)
	}
	c := ad.HTTP
	if c == nil {
		c = &http.Client{}
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", u, errors.New("advisor request failed (transport error)")
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var r struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	_ = json.Unmarshal(data, &r)
	u = tokenUsage{r.Usage.PromptTokens, r.Usage.CompletionTokens}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return "", u, fmt.Errorf("advisor HTTP %d %s %s", resp.StatusCode, e.Error.Type, truncateText(oneLine(e.Error.Message), 200))
	}
	if len(r.Choices) == 0 {
		return "", u, errors.New("advisor reply has no choices")
	}
	if c := r.Choices[0]; strings.TrimSpace(c.Message.Content) == "" {
		// Reasoning models spend output tokens before the answer: an empty answer cut at the limit needs a larger cap.
		return "", u, fmt.Errorf("advisor reply is empty (finish_reason %q): raise --advisor-max-tokens", c.FinishReason)
	}
	return r.Choices[0].Message.Content, u, nil
}

// cost in micro-USD (rounded up).
func cost(in, out, priceIn, priceOut int64) int64 {
	return (in*priceIn + out*priceOut + 999_999) / 1_000_000
}

// extractJSON returns the outermost {...} of a reply (models sometimes wrap JSON in a code fence).
func extractJSON(s string) string {
	i, j := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if i < 0 || j < i {
		return s
	}
	return s[i : j+1]
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

func hostOf(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return u.Host
}

// ApplyAdvisor merges accepted model proposals after the rule proposals: a flag a rule already proposes keeps the
// rule's value (rules win); the others are added. Rejected ones are recorded in the report.
func ApplyAdvisor(rep *Report, ar *AdvisorReport) {
	rep.Advisor = ar
	rep.Rejected = append(rep.Rejected, ar.Rejected...)
	ruled := map[string]bool{}
	for _, p := range rep.Proposals {
		if p.Kind == KindApply {
			ruled[p.Flag] = true
		}
	}
	for _, p := range ar.Accepted {
		if ruled[p.Flag] {
			continue
		}
		rep.Proposals = append(rep.Proposals, p)
	}
	rep.Proposals = MergeProposals(rep.Proposals)
}
