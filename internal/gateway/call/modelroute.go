package call

// 模型 API 的路由（模型网关设计 §7.6、§7.7 第 6 步、§11 G3）：modelapi 不经 journal，但与沙箱调用共用同一组
// 路由、熔断器与轮询 / 延迟状态。一次请求先 Plan（算出路由顺序，可按线路格式过滤），再依次 Next / Done：
// 第一个字节之前可重试或 unknown 的失败换下一个路由；之后不能再换（两个流无法拼接）。流式请求不对冲。

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
)

// ModelRouting 是模型 API 使用的 chat 路由视图。
type ModelRouting struct {
	c  *Coordinator
	ad upstream.ChatAdapter
	rs *routeSet // 只有一条路由时为 nil（没有熔断器，与沙箱路径相同）
}

// RouteInfo 是一个路由的名字与线路格式。
type RouteInfo struct {
	Name     string
	Protocol llmproto.Protocol
}

// ModelRouting 返回 chat 路由视图；没有模型 adapter（或它不支持线路格式与流式）时为 nil。
func (c *Coordinator) ModelRouting() *ModelRouting {
	ad, ok := c.adapters[upstream.KindChat].(upstream.ChatAdapter)
	if !ok {
		return nil
	}
	return &ModelRouting{c: c, ad: ad, rs: c.routes[upstream.KindChat]}
}

// Adapter 是底层的 chat adapter（PricingOn、Serves 等）。
func (m *ModelRouting) Adapter() upstream.ChatAdapter { return m.ad }

// Routes 是全部路由（下标即路由号）。
func (m *ModelRouting) Routes() []RouteInfo {
	names := m.ad.Routes()
	out := make([]RouteInfo, len(names))
	for i, n := range names {
		out[i] = RouteInfo{Name: n, Protocol: m.ad.RouteProtocol(i)}
	}
	return out
}

// RoutePick 是 Plan.Next 选出的路由。Skipped 是在它之前跳过的路由（"name:reason,..."，同 call_tries.skipped）。
type RoutePick struct {
	Route    int
	Name     string
	Protocol llmproto.Protocol
	Skipped  string
}

// RoutePlan 是一次模型 API 请求的路由过程（不可并发使用）。
type RoutePlan struct {
	m        *ModelRouting
	model    string
	only     llmproto.Protocol
	order    []int
	tried    map[int]bool
	eligible []int
	refused  bool // 有路由被熔断器拒绝
}

// Plan 为逻辑模型 model 的一次请求算出路由顺序（weighted / latency 模式推进一次轮询）。only 非空时只用该线路
// 格式的路由（请求用了只有一种格式才有的字段时，llmproto.Request.OnlyProtocol）。
func (m *ModelRouting) Plan(model string, only llmproto.Protocol) *RoutePlan {
	p := &RoutePlan{m: m, model: model, only: only, tried: map[int]bool{}}
	for i := range m.ad.Routes() {
		if p.usable(i) {
			p.eligible = append(p.eligible, i)
		}
	}
	if m.rs != nil {
		p.order = m.rs.order(p.usable)
	} else {
		p.order = []int{0}
	}
	return p
}

func (p *RoutePlan) usable(i int) bool {
	return p.m.ad.Serves(i, p.model) && (p.only == "" || p.m.ad.RouteProtocol(i) == p.only)
}

// Eligible 是提供该模型（且格式符合）的全部路由，不看熔断器：用于按最高单价估算预留（§7.7 第 5 步）。
func (p *RoutePlan) Eligible() []int { return append([]int(nil), p.eligible...) }

// NoRoute 报告没有任何路由提供该模型（且格式符合）：only 非空时为 400 unsupported_field，否则为未知模型。
func (p *RoutePlan) NoRoute() bool { return len(p.eligible) == 0 }

// Degraded 报告 Next 已没有路由、且本请求还没有尝试过任何路由、至少一个路由被熔断器拒绝：503 model_degraded。
func (p *RoutePlan) Degraded() bool { return len(p.tried) == 0 && p.refused }

// Next 选下一个路由：按顺序第一个可用、本请求未尝试过且熔断器放行的路由。ok 时熔断器已放行，须恰好调用一次
// Done（未发出请求时以 aborted 调用）。
func (p *RoutePlan) Next() (RoutePick, bool) {
	var skipped []string
	names := p.m.ad.Routes()
	for _, i := range p.order {
		name := names[i]
		reason := ""
		switch {
		case !p.m.ad.Serves(i, p.model):
			reason = SkipModelNotServed
		case p.only != "" && p.m.ad.RouteProtocol(i) != p.only:
			reason = SkipProtocol
		case p.tried[i]:
			reason = SkipTried
		case p.m.rs != nil && !p.m.rs.breakers[i].Allow():
			reason, p.refused = SkipCircuitOpen, true
		}
		if reason == "" {
			return RoutePick{Route: i, Name: name, Protocol: p.m.ad.RouteProtocol(i), Skipped: strings.Join(skipped, ",")}, true
		}
		skipped = append(skipped, name+":"+reason)
	}
	return RoutePick{Skipped: strings.Join(skipped, ",")}, false
}

// Done 报告一次已放行的尝试的结果（熔断器规则同沙箱路径：ok 与请求本身被拒为成功，可重试、unknown 与供应商
// 配置类的 fatal 为失败，aborted 只释放半开试探名额）。之后 Next 不再选这个路由。
func (p *RoutePlan) Done(pick RoutePick, uerr *upstream.Error, aborted bool) {
	p.tried[pick.Route] = true
	p.m.rs.report(leg{route: pick.Route}, uerr, aborted)
}

// Failover 报告这次失败之后能否换下一个路由（第一个字节之前）：retryable 与 unknown 可以，fatal 不可以。
func Failover(uerr *upstream.Error) bool {
	return uerr != nil && (uerr.Outcome == upstream.OutcomeRetryable || uerr.Outcome == upstream.OutcomeUnknown)
}

// ObserveTTFT 记录路由的一次首 token 时间（latency 模式据此分配流量）。
func (m *ModelRouting) ObserveTTFT(route int, d time.Duration) {
	if m.rs != nil {
		m.rs.bal.observe(route, d)
	}
}

// errTryTimeout 是每 try 超时（到第一个事件为止）的取消原因。
var errTryTimeout = errors.New("call: 每 try 超时（第一个事件之前）")

// DoStream 在路由上发出流式请求（upstream.ChatAdapter.DoStream），每 try 超时（RoutingConfig.TryTimeout）只作用于
// 第一个事件到达之前：超时的请求按 unknown 归类（已发出）或 retryable（未发出），与沙箱路径的每 try 超时相同。
// 返回的流实现 upstream.StreamInfo。
func (m *ModelRouting) DoStream(ctx context.Context, route int, body []byte, proto llmproto.Protocol) (upstream.Stream, *upstream.Error) {
	sctx, cancel := context.WithCancelCause(ctx)
	var timer *time.Timer
	if tt := m.c.routing.TryTimeout; tt > 0 {
		timer = time.AfterFunc(tt, func() { cancel(errTryTimeout) })
	}
	s, e := m.ad.DoStream(sctx, route, body, proto)
	if timer != nil && !timer.Stop() && e == nil {
		// 超时恰好在第一个事件到达时触发：流已被取消，按 unknown（已发出、结果无法确认）处理。
		_ = s.Close()
		s, e = nil, &upstream.Error{Outcome: upstream.OutcomeUnknown, Status: 502, // 同沙箱路径已发出后超时（upstream_unconfirmed）
			Code: upstream.CodeUpstreamUnconfirmed, Err: errTryTimeout}
	}
	if e != nil {
		cancel(nil)
		return nil, e
	}
	return &routedStream{Stream: s, cancel: cancel}, nil
}

// routedStream 在 Close 时同时结束每 try 超时的上下文。
type routedStream struct {
	upstream.Stream
	cancel context.CancelCauseFunc
}

func (s *routedStream) Close() error {
	err := s.Stream.Close()
	s.cancel(nil)
	return err
}

func (s *routedStream) Dropped() []string {
	if i, ok := s.Stream.(upstream.StreamInfo); ok {
		return i.Dropped()
	}
	return nil
}

func (s *routedStream) Converted() bool {
	i, ok := s.Stream.(upstream.StreamInfo)
	return ok && i.Converted()
}
