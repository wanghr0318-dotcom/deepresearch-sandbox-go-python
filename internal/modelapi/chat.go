package modelapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// Error codes recorded for requests that failed after the response started.
const (
	codeStreamError        = "stream_error"
	codeClientDisconnected = "client_disconnected"
	codeMaxDuration        = "max_duration"
)

// statusClientClosed is recorded when the client left before any response was written (nginx's 499).
const statusClientClosed = 499

// exchange is one model API request after its hold was reserved (§7.7 steps 6–8).
type exchange struct {
	h        *Handler
	w        http.ResponseWriter
	r        *http.Request
	proto    llmproto.Protocol
	req      llmproto.Request
	body     []byte
	start    time.Time
	id       string
	rt       *call.ModelRouting
	plan     *call.RoutePlan
	inputEst int64
	maxTok   int64

	tries       []Try
	unknownCost int64 // worst-case cost of tries whose outcome is unknown (sent, no answer)
	anyUnknown  bool
	lastErr     *upstream.Error

	picked    bool // a try delivered a response (its index is okTry)
	okTry     int
	pick      call.RoutePick
	dropped   []string
	converted bool
	ttfb      *time.Duration

	usage      llmproto.Usage // the final usage (usageKnown) or the usage seen so far
	usageKnown bool
	complete   *bool // streams only

	status  int
	errCode string
}

// serveChat is POST /v1/chat/completions and POST /v1/messages (§7.7).
func (h *Handler) serveChat(w http.ResponseWriter, r *http.Request, p llmproto.Protocol) {
	start := h.cfg.Now()
	ctx := r.Context()
	pr, ok := tenant.FromContext(ctx)
	if !ok { // internal/api authenticates first; this is a wiring error
		reject(w, p, "auth", llmproto.ErrorInfo{Class: llmproto.ClassInvalidAPIKey, Message: "authentication required"})
		return
	}
	// 2. Decode in the client's format.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBody))
	if err != nil {
		reject(w, p, "invalid", llmproto.ErrorInfo{Class: llmproto.ClassInvalidRequest, Message: "request body unreadable or larger than the limit"})
		return
	}
	req, err := llmproto.DecodeRequest(p, body)
	if err == nil {
		err = checkSupported(req)
	}
	if err != nil {
		info := llmproto.ErrorFrom(err)
		reason := "invalid"
		if info.Class == llmproto.ClassUnsupportedField {
			reason = "unsupported"
		}
		reject(w, p, reason, info)
		return
	}
	body, maxTok, err := withMaxTokens(body, req, h.cfg.DefaultMaxTokens)
	if err != nil {
		reject(w, p, "invalid", llmproto.ErrorFrom(err))
		return
	}
	inputEst := (int64(len(body)) + 2) / 3 // conservative: about 3 bytes per token
	scope := quota.ScopeOf(pr)
	// 3. Rate limits: requests, estimated tokens and a stream slot.
	lim, ok := h.limit(ctx, w, p, pr, scope, inputEst+maxTok, req.Stream)
	if !ok {
		return
	}
	undo := func() {
		lim.refundTokens(context.WithoutCancel(ctx), lim.tokens)
		lim.releaseSlot()
	}
	// 4. Candidate routes (format-only features pin the request to routes of its own format).
	rt := h.cfg.Routing()
	if rt == nil {
		undo()
		reject(w, p, "unavailable", llmproto.ErrorInfo{Class: llmproto.ClassUnavailable, Message: "the model routes are not ready"})
		return
	}
	only, _ := req.OnlyProtocol()
	plan := rt.Plan(req.Model, only)
	if plan.NoRoute() {
		undo()
		if only != "" && !rt.Plan(req.Model, "").NoRoute() {
			f := req.FormatOnly()[0]
			reject(w, p, "unsupported", llmproto.ErrorInfo{Class: llmproto.ClassUnsupportedField, Param: f,
				Message: fmt.Sprintf("field %s is only supported by %s providers, and none serves model %q", f, only, req.Model)})
			return
		}
		reject(w, p, "invalid", llmproto.ErrorInfo{Class: llmproto.ClassModelNotFound, Param: "model",
			Message: fmt.Sprintf("model %q does not exist or is not served", req.Model)})
		return
	}
	// 5. Worst-case estimate and the quota hold.
	est := worstCase(rt, plan.Eligible(), req.Model, inputEst, maxTok)
	id := h.cfg.NewID()
	hold, err := h.cfg.Holder.Reserve(ctx, scope, quota.SourceModelRequest, id, est, start.Add(h.cfg.MaxDuration+holdGrace))
	if err != nil {
		undo()
		var ie *quota.InsufficientError
		if errors.As(err, &ie) {
			reject(w, p, "quota", llmproto.ErrorInfo{Class: llmproto.ClassInsufficientQuota,
				Message: fmt.Sprintf("insufficient quota: the monthly budget at the %s level is exhausted", ie.Level)})
			return
		}
		h.cfg.Logger.Warn("model api: quota hold failed", "error", err.Error())
		reject(w, p, "unavailable", llmproto.ErrorInfo{Class: llmproto.ClassUnavailable, Message: "budgets are unavailable; try again later"})
		return
	}
	w.Header().Set("x-request-id", id)
	x := &exchange{h: h, w: w, r: r, proto: p, req: req, body: body, start: start, id: id, rt: rt, plan: plan,
		inputEst: inputEst, maxTok: maxTok}
	rctx, cancel := context.WithTimeout(ctx, h.cfg.MaxDuration)
	x.run(rctx)
	cancel()
	h.finish(x, pr, lim, hold, est)
}

// checkSupported refuses what the gateway cannot serve whatever the provider: more than one choice, and log
// probabilities on non-stream requests (non-stream replies are assembled from the provider's stream).
func checkSupported(req llmproto.Request) error {
	if raw, ok := req.Extras["n"]; ok && strings.TrimSpace(string(raw)) != "1" && strings.TrimSpace(string(raw)) != "null" {
		return &llmproto.UnsupportedFieldError{Field: "n", Reason: "only one choice (n = 1) is supported"}
	}
	if !req.Stream {
		for _, f := range []string{"logprobs", "top_logprobs"} {
			if raw, ok := req.Extras[f]; ok {
				v := strings.TrimSpace(string(raw))
				if v != "null" && v != "false" {
					return &llmproto.UnsupportedFieldError{Field: f, Reason: "only supported with stream: true"}
				}
			}
		}
	}
	return nil
}

// withMaxTokens adds max_tokens = def to a request that has none, so the worst-case cost is bounded, and returns
// the body and the effective max_tokens.
func withMaxTokens(body []byte, req llmproto.Request, def int64) ([]byte, int64, error) {
	if req.MaxTokens > 0 {
		return body, req.MaxTokens, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, 0, &llmproto.InvalidRequestError{Message: "body is not a JSON object"}
	}
	obj["max_tokens"] = json.RawMessage(fmt.Sprint(def))
	out, err := json.Marshal(obj)
	return out, def, err
}

// mulCeil is ⌈a·b / 10⁶⌉ (tokens × micro-USD per million tokens), saturating far below overflow.
func mulCeil(a, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > math.MaxInt64/4/b {
		return math.MaxInt64 / 4
	}
	p := a * b
	return (p + 999_999) / 1_000_000
}

// estimateWith is the worst-case cost of one try at the given prices (+2 µ$ rounding allowance for cache prices).
func estimateWith(inPrice, outPrice, input, maxTok int64) int64 {
	return mulCeil(input, inPrice) + mulCeil(maxTok, outPrice) + 2
}

// worstCase is the §7.7 step 5 estimate: the input estimate × the highest input price among the candidates +
// max_tokens × the highest output price.
func worstCase(rt *call.ModelRouting, routes []int, model string, input, maxTok int64) int64 {
	var in, out int64
	for _, i := range routes {
		pr := rt.Adapter().PricingOn(i, model)
		in, out = max(in, pr.EstimateInputPrice()), max(out, pr.OutputMicroPerMTok)
	}
	return estimateWith(in, out, input, maxTok)
}

// estimateOn is the worst-case cost of one try on route.
func (x *exchange) estimateOn(route int) int64 {
	pr := x.rt.Adapter().PricingOn(route, x.req.Model)
	return estimateWith(pr.EstimateInputPrice(), pr.OutputMicroPerMTok, x.inputEst, x.maxTok)
}

// run tries the routes in the plan's order (§7.7 step 6): a retryable or unknown failure before the first byte to
// the client fails over to the next route; a fatal one ends the request. Non-stream requests write nothing until the
// reply is complete, so a reply cut short is also failed over.
func (x *exchange) run(ctx context.Context) {
	for {
		pick, ok := x.plan.Next()
		if !ok {
			break
		}
		t0 := x.h.cfg.Now()
		s, uerr := x.rt.DoStream(ctx, pick.Route, x.body, x.proto)
		x.plan.Done(pick, uerr, false)
		try := Try{No: len(x.tries) + 1, Provider: pick.Name, Skipped: pick.Skipped}
		if uerr != nil {
			try.Outcome, try.Status, try.Latency = string(uerr.Outcome), uerr.Status, x.h.cfg.Now().Sub(t0)
			x.failedTry(try, pick, uerr)
			if call.Failover(ctx, uerr) {
				continue
			}
			break
		}
		ttfb := x.h.cfg.Now().Sub(x.start)
		x.ttfb = &ttfb
		obs.M().ModelTTFT(string(x.proto), pick.Name, ttfb)
		x.pick, x.picked, x.okTry = pick, true, len(x.tries)
		if info, ok := s.(upstream.StreamInfo); ok {
			x.dropped, x.converted = info.Dropped(), info.Converted()
		}
		try.Outcome, try.Status = string(upstream.OutcomeOK), http.StatusOK
		x.tries = append(x.tries, try)
		var done bool
		if x.req.Stream {
			x.stream(ctx, s, pick)
			done = true
		} else {
			done = x.collect(ctx, s, pick)
		}
		_ = s.Close()
		x.tries[x.okTry].Latency = x.h.cfg.Now().Sub(t0)
		if done {
			return
		}
	}
	x.fail(ctx)
}

// failedTry records a try that failed before the first content.
func (x *exchange) failedTry(try Try, pick call.RoutePick, uerr *upstream.Error) {
	if uerr.Outcome == upstream.OutcomeUnknown {
		x.unknownCost += x.estimateOn(pick.Route)
		x.anyUnknown = true
	}
	x.tries = append(x.tries, try)
	x.lastErr, x.pick = uerr, pick
}

// fail answers a request no route served.
func (x *exchange) fail(ctx context.Context) {
	switch {
	case x.r.Context().Err() != nil:
		x.status, x.errCode = statusClientClosed, codeClientDisconnected
		return
	case ctx.Err() != nil:
		x.writeError(llmproto.ErrorInfo{Class: llmproto.ClassUpstream, Message: "the request exceeded the maximum duration before the first token"})
		x.errCode = codeMaxDuration
		return
	case x.plan.Degraded() || x.lastErr == nil:
		x.writeError(llmproto.ErrorInfo{Class: llmproto.ClassModelDegraded,
			Message: fmt.Sprintf("all providers of model %q are unavailable; try again later", x.req.Model)})
		return
	}
	x.writeError(upstreamErrorInfo(x.lastErr))
}

// upstreamErrorInfo maps the last provider failure to the client's error (§7.5); provider bodies are never echoed.
func upstreamErrorInfo(e *upstream.Error) llmproto.ErrorInfo {
	switch {
	case e.Code == upstream.CodeUnsupportedField:
		return llmproto.ErrorInfo{Class: llmproto.ClassUnsupportedField, Message: "the request has a field the provider format cannot express"}
	case e.Outcome == upstream.OutcomeFatal && (e.Status == http.StatusBadRequest || e.Status == http.StatusRequestEntityTooLarge ||
		e.Status == http.StatusUnprocessableEntity):
		return llmproto.ErrorInfo{Class: llmproto.ClassInvalidRequest, Message: fmt.Sprintf("the provider rejected the request (HTTP %d)", e.Status)}
	case e.Status == http.StatusTooManyRequests:
		return llmproto.ErrorInfo{Class: llmproto.ClassRateLimit, Message: "the providers are rate limiting; try again later"}
	}
	return llmproto.ErrorInfo{Class: llmproto.ClassUpstream, Message: fmt.Sprintf("the provider failed (HTTP %d)", e.Status)}
}

func (x *exchange) writeError(e llmproto.ErrorInfo) {
	x.status, x.errCode = e.Class.Status(), string(e.Class)
	writeErrorInfo(x.w, x.proto, e)
}

// tracker follows a stream's completeness and final usage.
type tracker struct {
	stop, errEvent bool
	final          *llmproto.Usage
	errInfo        *llmproto.ErrorInfo
}

func (t *tracker) add(ev llmproto.Event) {
	switch ev.Type {
	case llmproto.EventMessageDelta:
		if ev.Usage != nil {
			u := *ev.Usage
			t.final = &u
		}
	case llmproto.EventMessageStop:
		t.stop = true
	case llmproto.EventError:
		t.errEvent, t.errInfo = true, ev.Err
	}
}

func (t *tracker) complete() bool { return t.stop && !t.errEvent }

// settleUsage records the usage of the answering try: the final usage when the reply is complete and reported it
// (an OpenAI stream cut after finish_reason but before its usage chunk is complete without usage: unknown), else the
// usage seen so far.
func (x *exchange) settleUsage(t *tracker, s upstream.Stream) {
	if t.complete() && t.final != nil {
		x.usage, x.usageKnown = *t.final, true
		return
	}
	x.usage, _ = s.Usage()
}

// collect reads a whole reply for a non-stream request and writes it (true), or reports a reply cut short (false:
// the try counts as unknown and the next route may be tried).
func (x *exchange) collect(ctx context.Context, s upstream.Stream, pick call.RoutePick) bool {
	var col llmproto.Collector
	var t tracker
	for {
		ev, err := s.Next()
		if err != nil {
			break
		}
		col.Add(ev)
		t.add(ev)
	}
	x.settleUsage(&t, s)
	if !t.complete() {
		// Sent and partly answered: the provider may charge it. Count it unknown and fail over when possible.
		x.picked = false
		x.tries[x.okTry].Outcome = string(upstream.OutcomeUnknown)
		x.unknownCost += x.estimateOn(pick.Route)
		x.anyUnknown = true
		x.usage, x.usageKnown = llmproto.Usage{}, false
		e := &upstream.Error{Outcome: upstream.OutcomeUnknown, Status: http.StatusBadGateway, Code: upstream.CodeUpstreamUnconfirmed}
		x.lastErr = e
		return !call.Failover(ctx, e)
	}
	resp := col.Response()
	if resp.Created == 0 {
		resp.Created = x.start.Unix()
	}
	if resp.Model == "" {
		resp.Model = x.req.Model
	}
	b, err := llmproto.EncodeResponse(x.proto, resp)
	if err != nil {
		x.writeError(llmproto.ErrorInfo{Class: llmproto.ClassUpstream, Message: "the provider's reply cannot be converted"})
		return true
	}
	x.w.Header().Set("Content-Type", "application/json")
	x.w.Header().Set("Cache-Control", "no-store")
	x.w.WriteHeader(http.StatusOK)
	_, _ = x.w.Write(b)
	x.status = http.StatusOK
	return true
}

// stream writes the reply as it arrives (§7.3): a same-format stream is forwarded event by event as the provider
// sent it (the usage-only chunk the gateway asked for is left out when the client did not); a converted one is
// encoded in the client's format. A failure after the first byte cannot fail over: the client gets an error event in
// its format and stream_complete is false (§7.6, §7.8).
func (x *exchange) stream(ctx context.Context, s upstream.Stream, pick call.RoutePick) {
	raw, _ := s.(upstream.RawStream)
	pass := false
	var pending []llmproto.SSEEvent // raw events read before the first content (DoStream's prefetch)
	if raw != nil && pick.Protocol == x.proto {
		pending, pass = raw.TakeRaw()
	}
	var enc llmproto.StreamEncoder
	if x.proto == llmproto.ProtocolAnthropic {
		enc = llmproto.NewAnthropicStreamEncoder(llmproto.AnthropicStreamOptions{Source: pick.Protocol, Model: x.req.Model})
	} else {
		enc = llmproto.NewOpenAIStreamEncoder(llmproto.OpenAIStreamOptions{IncludeUsage: x.req.IncludeUsage, Created: x.start.Unix(), Model: x.req.Model})
	}
	hd := x.w.Header()
	hd.Set("Content-Type", llmproto.SSEContentType)
	hd.Set("Cache-Control", "no-cache")
	hd.Set("X-Accel-Buffering", "no")
	x.w.WriteHeader(http.StatusOK)
	x.status = http.StatusOK
	flusher, _ := x.w.(http.Flusher)
	var t tracker
	gone := false
	write := func(b []byte) {
		if len(b) == 0 || gone {
			return
		}
		if _, err := x.w.Write(b); err != nil {
			gone = true
		}
	}
	var streamErr error
	for !gone {
		ev, err := s.Next()
		if err == nil {
			t.add(ev)
		}
		if pass {
			evs, _ := raw.TakeRaw()
			evs, pending = append(pending, evs...), nil
			for _, re := range evs {
				if !x.skipRaw(re) {
					write(llmproto.FormatSSE(re.Event, []byte(re.Data)))
				}
			}
		} else if err == nil {
			b, eerr := enc.Encode(ev)
			if eerr != nil {
				streamErr = eerr
				break
			}
			write(b)
		}
		if flusher != nil && !gone {
			flusher.Flush()
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				streamErr = err
			}
			break
		}
	}
	x.settleUsage(&t, s)
	complete := t.complete() && streamErr == nil && !gone
	x.complete = &complete
	if complete {
		return
	}
	switch {
	case x.r.Context().Err() != nil || gone:
		x.errCode = codeClientDisconnected
	case ctx.Err() != nil:
		x.errCode = codeMaxDuration
	default:
		x.errCode = codeStreamError
	}
	if gone || x.r.Context().Err() != nil || t.errEvent {
		return // the client is gone, or it already got the provider's error event
	}
	msg := "the provider's stream failed"
	if x.errCode == codeMaxDuration {
		msg = "the stream exceeded the maximum duration"
	}
	write(errorEvent(x.proto, llmproto.ErrorInfo{Class: llmproto.ClassUpstream, Message: msg}))
	if flusher != nil && !gone {
		flusher.Flush()
	}
}

// errorEvent is an error event in the client's stream format.
func errorEvent(p llmproto.Protocol, e llmproto.ErrorInfo) []byte {
	if p == llmproto.ProtocolAnthropic {
		return llmproto.FormatSSE("error", llmproto.EncodeAnthropicError(e))
	}
	return llmproto.FormatSSE("", llmproto.EncodeOpenAIError(e))
}

// skipRaw reports a raw OpenAI event to leave out: the usage-only chunk (choices: []) that the gateway asked for with
// stream_options.include_usage when the client did not.
func (x *exchange) skipRaw(ev llmproto.SSEEvent) bool {
	if x.proto != llmproto.ProtocolOpenAI || x.req.IncludeUsage || !strings.Contains(ev.Data, `"usage"`) {
		return false
	}
	var c struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   json.RawMessage   `json:"usage"`
	}
	return json.Unmarshal([]byte(ev.Data), &c) == nil && c.Choices != nil && len(c.Choices) == 0 && len(c.Usage) > 0 &&
		string(c.Usage) != "null"
}
