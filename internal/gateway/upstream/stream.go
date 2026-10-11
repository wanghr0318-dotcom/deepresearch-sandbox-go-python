package upstream

// 流式对话（模型网关设计 §7.3、§7.6、§11）：DoStream 在一个路由上发出流式请求，返回中立格式的事件流。它供模型
// API（modelapi）使用；沙箱路径仍是非流式的 Do / DoOn。
//
// 第一个事件到达之前的失败与 Do 同样归类（发送前失败、429、5xx 为 retryable；请求被拒为 fatal；已发出后读失败为
// unknown），调用方据此决定能否换供应商；第一个事件之后的失败由 Stream.Next 返回（不能再换供应商）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// Stream 是一个路由上的流式回复（§11）。
type Stream interface {
	// Next 返回下一个事件；流正常结束（message_stop 或错误事件之后）时为 io.EOF，流被切断时为 io.ErrUnexpectedEOF。
	Next() (llmproto.Event, error)
	// Usage 是流中到目前为止报告的用量（各项互不包含，llmproto 语义）。
	Usage() (llmproto.Usage, bool)
	// Close 结束流（取消上游请求、关闭响应体）；可重复调用。
	Close() error
}

// ProtocolRouter 是路由有线路格式的 Router（模型网关设计 §7.6）。
type ProtocolRouter interface {
	Router
	// RouteProtocol 是路由的线路格式。
	RouteProtocol(route int) llmproto.Protocol
	// Convertible 报告规范化的（OpenAI 格式）请求能否在该路由上执行：只有一种格式才有的字段不能转到另一种格式的路由。
	Convertible(route int, resolved []byte) bool
}

// ChatAdapter 是模型 adapter 的完整接口：Adapter、带线路格式的路由与流式对话（§11）。NewChat 的结果实现它。
type ChatAdapter interface {
	Adapter
	ProtocolRouter
	// DoStream 在路由上发出流式请求。body 是 proto 格式的请求（model 为逻辑模型名）：与路由格式相同时原样转发
	// （只换模型名、打开流式与用量报告），不同时经 llmproto 转换（被丢弃的字段由 StreamInfo 报告）。返回时第一个
	// 事件已经到达。
	DoStream(ctx context.Context, route int, body []byte, proto llmproto.Protocol) (Stream, *Error)
}

// StreamInfo 是 DoStream 返回的流的附加信息（类型断言取得）。
type StreamInfo interface {
	// Dropped 是转换时丢弃的只有源格式才有的字段（原样转发时为空）。
	Dropped() []string
	// Converted 报告请求是否经过格式转换。
	Converted() bool
}

var _ ChatAdapter = (*chatAdapter)(nil)

// errorBodyMax 是流式请求得到 HTTP 错误时读取（并丢弃）的响应体上限。
const errorBodyMax = 64 << 10

// DoStream 实现 ChatAdapter。
func (a *chatAdapter) DoStream(ctx context.Context, route int, body []byte, proto llmproto.Protocol) (Stream, *Error) {
	if route < 0 || route >= len(a.routes) {
		return nil, fatalf(http.StatusBadGateway, CodeUnsupportedProvider, "路由 %d 不存在", route)
	}
	rt := a.routes[route]
	out, dropped, e := a.streamBody(route, rt, body, proto)
	if e != nil {
		return nil, e
	}
	req, e := rt.newRequest(out, true)
	if e != nil {
		return nil, e
	}
	sctx, cancel := context.WithCancel(ctx)
	resp, e := streamTrip(sctx, rt.http, req)
	if e != nil {
		cancel()
		return nil, e
	}
	st := &eventStream{body: resp.Body, cancel: cancel, rd: llmproto.NewEventReader(resp.Body, rt.protocol),
		dropped: dropped, converted: proto != rt.protocol}
	first, err := st.rd.Next()
	if err != nil {
		_ = st.Close()
		return nil, firstEventError(ctx, err)
	}
	if first.Type == llmproto.EventError {
		_ = st.Close()
		return nil, streamErrorEvent(first.Err)
	}
	st.first = &first
	return st, nil
}

// streamBody 把 proto 格式的请求转为发往路由的流式请求体。
func (a *chatAdapter) streamBody(route int, rt chatRoute, body []byte, proto llmproto.Protocol) ([]byte, []string, *Error) {
	if proto != llmproto.ProtocolOpenAI && proto != llmproto.ProtocolAnthropic {
		return nil, nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "未知的请求格式 %q", proto)
	}
	if proto == rt.protocol {
		out, e := a.passThrough(route, rt, body)
		return out, nil, e
	}
	req, err := llmproto.DecodeRequest(proto, body)
	if err != nil {
		return nil, nil, conversionError(err)
	}
	if !a.Serves(route, req.Model) {
		return nil, nil, fatalf(http.StatusBadRequest, CodeUnsupportedModel, "路由不提供模型 %q", req.Model)
	}
	req.Model = rt.providerModel(req.Model)
	req.Stream, req.IncludeUsage = true, true
	if req.MaxTokens == 0 { // Anthropic 要求 max_tokens
		req.MaxTokens = int64(a.cfg.MaxTokensDefault)
	}
	enc, err := llmproto.EncodeRequest(req, rt.protocol)
	if err != nil {
		return nil, nil, conversionError(err)
	}
	return enc.Body, enc.Dropped, nil
}

// passThrough 是同格式的请求：只换模型名、设置 stream（OpenAI 另要求 stream_options.include_usage；Anthropic
// 缺 max_tokens 时补默认值），其余字段原样保留。
func (a *chatAdapter) passThrough(route int, rt chatRoute, body []byte) ([]byte, *Error) {
	obj, e := decodeObject(body)
	if e != nil {
		return nil, e
	}
	var model string
	if json.Unmarshal(obj["model"], &model) != nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "model 须为字符串")
	}
	if !a.Serves(route, model) {
		return nil, fatalf(http.StatusBadRequest, CodeUnsupportedModel, "路由不提供模型 %q", model)
	}
	obj["model"], _ = json.Marshal(rt.providerModel(model))
	obj["stream"] = json.RawMessage("true")
	if rt.protocol == llmproto.ProtocolOpenAI {
		if e := includeUsage(obj); e != nil {
			return nil, e
		}
	} else if _, ok := obj["max_tokens"]; !ok {
		obj["max_tokens"], _ = json.Marshal(a.cfg.MaxTokensDefault)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fatalf(http.StatusBadRequest, CodeInvalidRequest, "无法编码请求")
	}
	return out, nil
}

// includeUsage 设置 OpenAI 请求的 stream_options.include_usage（计量需要用量），保留 stream_options 的其他字段。
func includeUsage(obj map[string]json.RawMessage) *Error {
	opts := map[string]json.RawMessage{}
	if raw, ok := obj["stream_options"]; ok && !isJSONNull(raw) {
		if json.Unmarshal(raw, &opts) != nil || opts == nil {
			return fatalf(http.StatusBadRequest, CodeInvalidRequest, "stream_options 须为对象")
		}
	}
	opts["include_usage"] = json.RawMessage("true")
	obj["stream_options"], _ = json.Marshal(opts)
	return nil
}

// streamTrip 发出流式请求、只读响应头：传输层失败按 transportError 归类；非 2xx 读完（有上限）错误响应体后按
// classifyStatus 归类。成功时响应体由调用方关闭。
func streamTrip(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, *Error) {
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))
	resp, err := client.Do(req)
	if err != nil {
		return nil, transportError(err, sent.Load())
	}
	if e := classifyStatus(resp); e != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errorBodyMax))
		_ = resp.Body.Close()
		return nil, e
	}
	return resp, nil
}

// firstEventError 归类第一个事件之前的读失败：请求已发出、结果无法确认，为 unknown（与 Do 读响应失败相同）。
// 超过响应上限为 response_too_large；流格式错误为 upstream_bad_response。
func firstEventError(ctx context.Context, err error) *Error {
	switch {
	case errors.Is(err, ErrTruncated):
		return newErr(OutcomeUnknown, http.StatusBadGateway, CodeResponseTooLarge, errors.New("上游响应超过上限"))
	case errors.Is(err, llmproto.ErrBadStream), errors.Is(err, llmproto.ErrSSETooLarge), isDecodeError(err):
		return newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamBadResponse, fmt.Errorf("上游流格式错误: %w", err))
	case ctx.Err() != nil:
		return newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamUnconfirmed, ctx.Err())
	}
	return newErr(OutcomeUnknown, http.StatusBadGateway, CodeUpstreamUnconfirmed, fmt.Errorf("上游流在第一个事件之前结束: %w", err))
}

// isDecodeError 报告 err 是否为 llmproto 的解码错误（上游发出了不合法的事件）。
func isDecodeError(err error) bool {
	var ir *llmproto.InvalidRequestError
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	return errors.As(err, &ir) || errors.As(err, &se) || errors.As(err, &te)
}

// streamErrorEvent 归类作为第一个事件到达的错误事件（还没有任何内容）：限流、过载与服务端错误为 retryable，
// 其余为 fatal。
func streamErrorEvent(info *llmproto.ErrorInfo) *Error {
	if info == nil {
		return newErr(OutcomeRetryable, http.StatusBadGateway, CodeUpstreamUnavailable, errors.New("上游流以错误事件开始"))
	}
	err := fmt.Errorf("上游流以错误事件开始: %s", info.Class)
	switch info.Class {
	case llmproto.ClassRateLimit:
		return newErr(OutcomeRetryable, http.StatusTooManyRequests, CodeUpstreamRateLimited, err)
	case llmproto.ClassModelDegraded, llmproto.ClassUpstream, llmproto.ClassInternal:
		return newErr(OutcomeRetryable, http.StatusServiceUnavailable, CodeUpstreamUnavailable, err)
	}
	return newErr(OutcomeFatal, info.Class.Status(), CodeUpstreamRejected, err)
}

// eventStream 是 DoStream 的流：第一个事件已预读。
type eventStream struct {
	body      io.ReadCloser
	cancel    context.CancelFunc
	rd        *llmproto.EventReader
	first     *llmproto.Event
	dropped   []string
	converted bool
	closeOnce sync.Once
}

func (s *eventStream) Next() (llmproto.Event, error) {
	if s.first != nil {
		ev := *s.first
		s.first = nil
		return ev, nil
	}
	return s.rd.Next()
}

func (s *eventStream) Usage() (llmproto.Usage, bool) { return s.rd.Usage() }

func (s *eventStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.body.Close()
	})
	return nil
}

func (s *eventStream) Dropped() []string { return s.dropped }
func (s *eventStream) Converted() bool   { return s.converted }
