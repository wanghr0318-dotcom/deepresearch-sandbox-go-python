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
	// Close 结束流（取消上游请求、关闭响应体、清空预读的事件）；可重复调用。之后 Next 立即返回 ErrStreamClosed。
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
	// 内容事件（首 token）已经到达：之前的 message_start、ping 与空的块被缓存，由 Next 重放；之前的失败（含错误
	// 事件）以 *Error 返回，调用方可换供应商。流使用不限总长的 HTTP 客户端（单个事件仍受 llmproto 的上限约束），
	// 总时长由调用方的 ctx 约束。
	DoStream(ctx context.Context, route int, body []byte, proto llmproto.Protocol) (Stream, *Error)
}

// StreamInfo 是 DoStream 返回的流的附加信息（类型断言取得）。
type StreamInfo interface {
	// Dropped 是转换时丢弃的只有源格式才有的字段（原样转发时为空）。
	Dropped() []string
	// Converted 报告请求是否经过格式转换。
	Converted() bool
}

// RawStream 是 DoStream 返回的流的原文视图（类型断言取得）：请求格式与路由格式相同（原样转发）时，TakeRaw 返回自
// 上次调用以来读到的供应商 SSE 事件原文（含 ping、[DONE] 等不产生中立事件的事件），ok 为 true；经过格式转换的流
// ok 为 false。模型 API 据此把同格式的流逐个事件原样转发给客户端（§7.3），同时照常经 Next 读取用量与完整性。
type RawStream interface {
	TakeRaw() (events []llmproto.SSEEvent, ok bool)
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
	resp, e := streamTrip(sctx, rt.streamHTTP, req)
	if e != nil {
		cancel()
		return nil, e
	}
	st := &eventStream{body: resp.Body, cancel: cancel, rd: llmproto.NewEventReader(resp.Body, rt.protocol),
		dropped: dropped, converted: proto != rt.protocol}
	if !st.converted {
		st.rd.OnSSE(func(ev llmproto.SSEEvent) {
			st.mu.Lock()
			st.raw = append(st.raw, ev)
			st.mu.Unlock()
		})
	}
	if e := st.prefetch(ctx); e != nil {
		_ = st.Close()
		return nil, e
	}
	return st, nil
}

// 首 token 之前缓存的前导事件的上限（先到者为准）。超出时上游在没有任何内容的情况下发出了异常多的事件：按
// retryable / upstream_bad_response 处理（还没有向客户端写出任何字节，可以换供应商）。
const (
	maxPrefetchEvents = 1024
	maxPrefetchBytes  = 1 << 20
)

// errPrefetchTooLarge 是前导事件超过上限的原因。
var errPrefetchTooLarge = errors.New("上游流在首 token 之前发出的事件超过上限")

// prefetch 读到第一个内容事件（首 token）为止：message_start、只有角色或内容为空的块开始等前导事件先缓存，由
// Next 依次交付；空的文本片段（不含任何内容的 block_delta）直接丢弃，不缓存、不计数。期间的读失败与 Do 同样
// 归类；内容之前到达的错误事件按 StreamEventError 归类（可换供应商）；缓存超过上限（maxPrefetchEvents 个事件或
// maxPrefetchBytes 字节）为 retryable / upstream_bad_response。
func (s *eventStream) prefetch(ctx context.Context) *Error {
	size := 0
	for {
		ev, err := s.rd.Next()
		if err != nil {
			return firstEventError(ctx, err)
		}
		if ev.Type == llmproto.EventError {
			return StreamEventError(ev.Err)
		}
		if ev.Type == llmproto.EventBlockDelta && !isContent(ev) {
			continue // 空的片段：没有内容，交付它也没有意义
		}
		size += eventSize(ev)
		if len(s.pending) >= maxPrefetchEvents || size > maxPrefetchBytes {
			return newErr(OutcomeRetryable, http.StatusBadGateway, CodeUpstreamBadResponse, errPrefetchTooLarge)
		}
		s.pending = append(s.pending, ev)
		if isContent(ev) {
			return nil
		}
	}
}

// eventSize 估算一个缓存事件占用的字节数（字符串字段之和加固定开销）。
func eventSize(ev llmproto.Event) int {
	return 64 + len(ev.ID) + len(ev.Model) + len(ev.ToolID) + len(ev.ToolName) + len(ev.Text) + len(ev.Arguments) +
		len(ev.Signature) + len(ev.StopSequence)
}

// isContent 报告事件是否为首 token：非空的文本、参数或签名片段，工具调用的开始（带名字），或者结束类事件
// （模型没有输出任何内容就停止，也是一个确定的回复）。
func isContent(ev llmproto.Event) bool {
	switch ev.Type {
	case llmproto.EventBlockDelta:
		return ev.Text != "" || ev.Arguments != "" || ev.Signature != ""
	case llmproto.EventBlockStart:
		return ev.Block == llmproto.BlockToolCall
	case llmproto.EventMessageDelta, llmproto.EventMessageStop:
		return true
	}
	return false
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

// providerErrorStatus 是供应商在流中给出的错误类型对应的 HTTP 状态（Anthropic 的 error.type 与 OpenAI 常见的
// error.type / code）；未知类型为 0。
var providerErrorStatus = map[string]int{
	"invalid_request_error": http.StatusBadRequest, "authentication_error": http.StatusUnauthorized,
	"invalid_api_key": http.StatusUnauthorized, "permission_error": http.StatusForbidden,
	"not_found_error": http.StatusNotFound, "model_not_found": http.StatusNotFound,
	"request_too_large": http.StatusRequestEntityTooLarge, "rate_limit_error": http.StatusTooManyRequests,
	"rate_limit_exceeded": http.StatusTooManyRequests, "insufficient_quota": http.StatusTooManyRequests,
	"api_error": http.StatusInternalServerError, "server_error": http.StatusInternalServerError,
	"overloaded_error": 529, "service_unavailable": http.StatusServiceUnavailable,
}

// StreamEventError 把流中第一个内容之前到达的错误事件归为 *Error，与同一错误以 HTTP 状态返回时相同
// （classifyStatus）：429 与 5xx（含 Anthropic 的 overloaded_error）为 retryable，可换供应商；其余为 fatal
// （401、403、404 计入熔断，同 HTTP）。供应商类型未知时按 llmproto 的错误类别。内容之后的错误事件不能换供应商，
// 由 Stream.Next 交付。
func StreamEventError(info *llmproto.ErrorInfo) *Error {
	status := http.StatusBadGateway
	if info != nil {
		if st, ok := providerErrorStatus[info.ProviderType]; ok {
			status = st
		} else {
			status = info.Class.Status()
		}
	}
	class := "unknown"
	if info != nil {
		class = string(info.Class)
		if info.ProviderType != "" {
			class = info.ProviderType
		}
	}
	err := fmt.Errorf("上游流在内容之前发出错误事件: %s", class)
	switch {
	case status == http.StatusTooManyRequests:
		return newErr(OutcomeRetryable, status, CodeUpstreamRateLimited, err)
	case status >= 500:
		return newErr(OutcomeRetryable, status, CodeUpstreamUnavailable, err)
	}
	return newErr(OutcomeFatal, status, CodeUpstreamRejected, err)
}

// eventStream 是 DoStream 的流：首 token 及其之前的事件已预读（pending）。
type eventStream struct {
	body      io.ReadCloser
	cancel    context.CancelFunc
	rd        *llmproto.EventReader
	dropped   []string
	converted bool
	closeOnce sync.Once

	mu      sync.Mutex          // 保护 pending、raw 与 closed（Close 可能与 Next 在不同 goroutine 中调用）
	pending []llmproto.Event    // 预读的事件，由 Next 重放
	raw     []llmproto.SSEEvent // 原样转发时读到而尚未取走的 SSE 事件原文（TakeRaw）
	closed  bool
}

// ErrStreamClosed 是 Close 之后调用 Next 得到的错误。
var ErrStreamClosed = errors.New("upstream: 流已关闭")

// Next 先重放预读的事件，再读流。流结束为 io.EOF；其他读失败（连接切断、超过单个事件的上限、格式错误）都包装为
// io.ErrUnexpectedEOF（errors.Is 可判断，原因保留在文本中）。
// Close 之后 Next 立即返回 ErrStreamClosed（预读的事件已清空，不再交付）。
func (s *eventStream) Next() (llmproto.Event, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return llmproto.Event{}, ErrStreamClosed
	}
	if len(s.pending) > 0 {
		ev := s.pending[0]
		s.pending = s.pending[1:]
		s.mu.Unlock()
		return ev, nil
	}
	s.mu.Unlock()
	ev, err := s.rd.Next()
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		err = fmt.Errorf("%w: %w", io.ErrUnexpectedEOF, err)
	}
	return ev, err
}

func (s *eventStream) Usage() (llmproto.Usage, bool) { return s.rd.Usage() }

func (s *eventStream) Close() error {
	s.mu.Lock()
	s.closed, s.pending, s.raw = true, nil, nil
	s.mu.Unlock()
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.body.Close()
	})
	return nil
}

func (s *eventStream) Dropped() []string { return s.dropped }

// TakeRaw 实现 RawStream。
func (s *eventStream) TakeRaw() ([]llmproto.SSEEvent, bool) {
	if s.converted {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.raw
	s.raw = nil
	return out, true
}

func (s *eventStream) Converted() bool { return s.converted }
