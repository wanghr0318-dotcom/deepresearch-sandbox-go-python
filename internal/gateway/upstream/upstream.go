// Package upstream 实现 Gateway 的上游协议与出站防护（规格 §9.3、§9.6–§9.9）。
//
// 本包只负责：把 Worker 的请求体解析为规范化的上游请求（补默认、拒绝不支持的字段），按 §9.6 给出保守估算，
// 经 §9.8 的验证 dialer 发出请求，并把结果归类为 ok / retryable / fatal / unknown。
// 记账、journal 与重试决策属于 gateway/call；本包不导入 persistence，也不写日志。
// 供应商 Key 只放在请求头中，不进入任何错误文本。
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Kind 是调用类别。
type Kind string

const (
	KindChat   Kind = "chat"
	KindSearch Kind = "search"
	KindFetch  Kind = "fetch"
)

// Request 是一次上游调用的输入；Body 是已校验的规范化 JSON。
type Request struct {
	Kind     Kind
	Body     []byte
	Deadline time.Time
}

// Usage 是上游返回或 adapter 计量的用量。
type Usage struct {
	InputTokens, OutputTokens int64
	Requests                  int
	ResponseBytes             int64
}

// Response 是上游结果。Do 返回 retryable 错误时，Response 只携带 RetryAfter（供应商给出 Retry-After 时非零）。
type Response struct {
	Body              []byte
	Usage             Usage
	UpstreamRequestID string
	RetryAfter        time.Duration
	// HTTP 是目标站点响应的 HTTP 元数据，只由抓取 adapter 在结果为 ok 时填写；Gateway 据此判定能否进入
	// 共享缓存（§11.3 准入与新鲜度）。其余 adapter 为 nil。
	HTTP *HTTPMeta
}

// HTTPMeta 是一次抓取的 HTTP 响应元数据。RequestTime 与 ResponseTime 是 Gateway 时钟上发出请求与读完
// 响应的时刻（RFC 9111 §4.2.3 的 request_time、response_time）；Truncated 表示正文超过上限被截断。
type HTTPMeta struct {
	Status                    int
	Header                    http.Header
	RequestTime, ResponseTime time.Time
	Truncated                 bool
}

// Outcome 是一次 try 的结果类别（§9.7）。
type Outcome string

const (
	OutcomeOK        Outcome = "ok"
	OutcomeRetryable Outcome = "retryable" // 429、5xx、发送前失败：可以考虑重试
	OutcomeFatal     Outcome = "fatal"     // 请求本身不可接受或被上游明确拒绝
	OutcomeUnknown   Outcome = "unknown"   // 已发出，但无法确认结果（读响应失败、超大响应等）
)

// Error 是 adapter 的失败结果。Status 是建议返回给 Worker 的 HTTP 状态（上游状态或 Gateway 自身状态），
// Code 是稳定的错误码；Err 是底层原因，文本不含 Key。
type Error struct {
	Outcome Outcome
	Status  int
	Code    string
	Err     error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("upstream: %s %d %s", e.Outcome, e.Status, e.Code)
	}
	return fmt.Sprintf("upstream: %s %d %s: %v", e.Outcome, e.Status, e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// 稳定错误码。
const (
	CodeUnsupportedField    = "unsupported_field"
	CodeUnsupportedModel    = "unsupported_model"
	CodeUnsupportedProvider = "unsupported_provider"
	CodeInvalidRequest      = "invalid_request"
	CodeInvalidURL          = "invalid_url"
	CodeEgressBlocked       = "egress_blocked"
	CodeTooManyRedirects    = "too_many_redirects"
	CodeUpstreamUnreachable = "upstream_unreachable" // 发送前失败
	CodeUpstreamUnconfirmed = "upstream_unconfirmed" // 已发出、读响应失败
	CodeUpstreamRateLimited = "upstream_rate_limited"
	CodeUpstreamUnavailable = "upstream_unavailable"
	CodeUpstreamRejected    = "upstream_rejected"
	CodeUpstreamBadResponse = "upstream_bad_response"
	CodeResponseTooLarge    = "response_too_large"
)

// Adapter 是一种上游（模型、搜索或抓取）的协议适配。
type Adapter interface {
	Kind() Kind
	Provider() string // 进入指纹 resolved.provider
	Version() string  // adapter_version，进入指纹与缓存键
	// Resolve 补默认（如 max_tokens）、拒绝不支持的字段；返回规范化请求体与 applied_defaults。
	// 失败时 err 为 *Error（fatal）。
	Resolve(body []byte) (resolved []byte, defaults map[string]any, err error)
	// Estimate 按 §9.6 给出保守估算（微美元）。
	Estimate(resolved []byte) (micro int64, err error)
	Do(ctx context.Context, resolved []byte) (Response, *Error)
}

// Pricing 是价格表；Version 标识价格表版本。
type Pricing struct {
	Version                                     string
	InputMicroPerMTok, OutputMicroPerMTok       int64
	SearchMicroPerRequest, FetchMicroPerRequest int64
}

// 默认上限（§19）。
const (
	DefaultModelMaxBody   = 8 << 20 // 模型与搜索响应 8 MiB
	DefaultFetchMaxBody   = 5 << 20 // 抓取 5 MiB
	DefaultFetchTimeout   = 20 * time.Second
	DefaultMaxRedirects   = 5
	defaultMaxTokens      = 1024
	defaultMaxTokensCap   = 4096
	fetchErrorBodyMax     = 4 << 10 // 目标站点返回 HTTP 错误时保留的正文上限
	defaultSearchResults  = 5
	maxSearchResults      = 20
	maxRetryAfter         = 24 * time.Hour
	userAgent             = "agentbox-gateway/1"
	contentTypeJSON       = "application/json"
	microPerMillionTokens = 1_000_000
)

func newErr(o Outcome, status int, code string, err error) *Error {
	return &Error{Outcome: o, Status: status, Code: code, Err: err}
}

func fatalf(status int, code, format string, args ...any) *Error {
	return newErr(OutcomeFatal, status, code, fmt.Errorf(format, args...))
}

// decodeObject 把请求体解码为顶层对象（数字保留原文）。
func decodeObject(body []byte) (map[string]json.RawMessage, *Error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var obj map[string]json.RawMessage
	if err := dec.Decode(&obj); err != nil || obj == nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "请求体须为 JSON 对象")
	}
	if dec.More() {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "请求体含多余内容")
	}
	return obj, nil
}

// checkFields 拒绝白名单之外的字段。
func checkFields(obj map[string]json.RawMessage, allowed ...string) *Error {
	for k := range obj {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
				break
			}
		}
		if !ok {
			return fatalf(http.StatusBadRequest, CodeUnsupportedField, "不支持的字段 %q", k)
		}
	}
	return nil
}

// positiveInt 解析正整数字段。
func positiveInt(raw json.RawMessage, name string) (int64, *Error) {
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n <= 0 {
		return 0, fatalf(http.StatusBadRequest, CodeInvalidRequest, "%s 须为正整数", name)
	}
	return n, nil
}

// mulCeil 返回 ceil(a × b / div)，溢出时报错。
func mulCeil(a, b, div int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, errors.New("估算参数为负")
	}
	if a != 0 && b > math.MaxInt64/a {
		return 0, errors.New("估算溢出")
	}
	p := a * b
	return p/div + boolInt(p%div != 0), nil
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// parseRetryAfter 解析 Retry-After（秒数或 HTTP 日期）；无法解析或为负时返回 0。
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n <= 0 {
			return 0
		}
		if n > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			return 0
		}
		return min(d, maxRetryAfter)
	}
	return 0
}

// httpResult 是一次完成的 HTTP 交换。
type httpResult struct {
	resp       *http.Response
	body       []byte
	truncated  bool
	retryAfter time.Duration // 429 / 5xx 的 Retry-After
}

// roundTrip 发出请求并读完（受上限约束的）响应体，把传输层失败归类：
// 出站防护拒绝 → fatal；请求头尚未写出 → retryable；已写出后失败 → unknown。
// 状态码的归类由 classifyStatus 完成。
func roundTrip(ctx context.Context, client *http.Client, req *http.Request) (httpResult, *Error) {
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))
	resp, err := client.Do(req)
	if err != nil {
		return httpResult{}, transportError(err, sent.Load())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if errors.Is(err, ErrTruncated) {
		return httpResult{resp: resp, body: body, truncated: true}, nil
	}
	if err != nil {
		return httpResult{}, newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamUnconfirmed, err)
	}
	return httpResult{resp: resp, body: body}, nil
}

func transportError(err error, sent bool) *Error {
	switch {
	case errors.Is(err, ErrBlocked):
		return newErr(OutcomeFatal, http.StatusForbidden, CodeEgressBlocked, err)
	case errors.Is(err, ErrTooManyRedirects):
		return newErr(OutcomeFatal, http.StatusBadGateway, CodeTooManyRedirects, err)
	case errors.Is(err, ErrInvalidURL):
		return newErr(OutcomeFatal, http.StatusBadRequest, CodeInvalidURL, err)
	case sent:
		return newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamUnconfirmed, err)
	default:
		return newErr(OutcomeRetryable, http.StatusBadGateway, CodeUpstreamUnreachable, err)
	}
}

// classifyStatus：2xx → nil；429 / 5xx → retryable；其他 → fatal。
// 错误文本只含状态码，不含响应体（响应体可能回显请求头）。
func classifyStatus(resp *http.Response) *Error {
	sc := resp.StatusCode
	switch {
	case sc >= 200 && sc < 300:
		return nil
	case sc == http.StatusTooManyRequests || sc >= 500:
		code := CodeUpstreamUnavailable
		if sc == http.StatusTooManyRequests {
			code = CodeUpstreamRateLimited
		}
		return newErr(OutcomeRetryable, sc, code, fmt.Errorf("上游返回 %d", sc))
	default:
		return newErr(OutcomeFatal, sc, CodeUpstreamRejected, fmt.Errorf("上游返回 %d", sc))
	}
}

// exchange 是 roundTrip + classifyStatus；strict 时超出上限的响应按 unknown 处理（无法确认用量）。
func exchange(ctx context.Context, client *http.Client, req *http.Request, strict bool) (httpResult, *Error) {
	r, e := roundTrip(ctx, client, req)
	if e != nil {
		return r, e
	}
	if e := classifyStatus(r.resp); e != nil {
		if e.Outcome == OutcomeRetryable {
			r.retryAfter = parseRetryAfter(r.resp.Header.Get("Retry-After"), time.Now())
		}
		return r, e
	}
	if strict && r.truncated {
		return r, newErr(OutcomeUnknown, http.StatusBadGateway, CodeResponseTooLarge, errors.New("上游响应超过上限"))
	}
	return r, nil
}

func defaultClient(c *http.Client, maxBody int64) *http.Client {
	if c != nil {
		return c
	}
	return NewDialer(DialerConfig{}).HTTPClient(maxBody, 0)
}
