package call

// 模型降级链（docs/design/2026-10-10-model-fallback-design.md）：实现 upstream.Router 且路由多于一条的 adapter
// 按路由执行 try。每个路由一个熔断器；try 循环每轮按顺序选第一个提供该模型、本轮未失败且熔断器放行的路由，
// 可重试或 unknown 的失败之后立即换下一个就绪的路由（不退避），本轮全部失败才退避；所有路由都不放行时不预留、
// 立即以 503 model_degraded 失败（降级模式）。可选的对冲请求见 hedge.go。
//
// 单供应商（没有 Router 或只有一条路由）不建 routeSet：没有熔断器、不记录 provider，行为与引入降级链之前完全相同。

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/breaker"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/llmproto"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
)

// CodeModelDegraded 是降级模式的错误码：提供该模型的全部供应商的熔断器都处于打开状态（503，可重试）。
const CodeModelDegraded = "model_degraded"

// 路由被跳过的原因（call_tries.skipped 中的 "name:reason"）。
const (
	SkipCircuitOpen    = "circuit_open"
	SkipTried          = "tried"
	SkipModelNotServed = "model_not_served"
	// SkipProtocol：请求用了只有 OpenAI 格式才有的字段，不能转到该（Anthropic 格式的）路由。
	SkipProtocol = "unsupported_protocol"
)

// RoutingConfig 配置模型降级链。零值字段取默认值；只对多路由的 adapter 生效。
type RoutingConfig struct {
	BreakerFailures int           // 连续失败多少次打开熔断器，默认 3
	BreakerOpen     time.Duration // 打开时长（之后放行一次半开试探），默认 30 s
	// TryTimeout 是链模式下每个 try 的超时（0 = 不设，只受调用期限约束）。已发出后超时按 unknown 结算（同取消）。
	// 不设时挂起的供应商会占满整个调用期限，无法转到下一供应商。
	TryTimeout time.Duration
	// HedgeDelay > 0 时启用对冲：try 发出后这么久仍未结束、且另有就绪路由时，向它并发发出同一请求（hedge.go）。
	HedgeDelay time.Duration
	// Mode 是路由模式（balance.go）：空时没有权重为 ordered（与引入路由模式之前相同），有权重为 weighted。
	// 零值不进入 JSON（配置摘要与之前相同）。
	Mode RoutingMode `json:",omitempty"`
	// Weights 是路由名 → 权重（≥ 1；缺省 1），用于 weighted 与 latency 模式。
	Weights map[string]int `json:",omitempty"`

	// breakerNow 是熔断器的时钟（测试注入；nil 时用 Coordinator 的时钟）。
	breakerNow func() time.Time
}

// 降级链默认值。
const (
	DefaultBreakerFailures = 3
	DefaultBreakerOpen     = 30 * time.Second
)

func (r RoutingConfig) withDefaults() RoutingConfig {
	if r.BreakerFailures <= 0 {
		r.BreakerFailures = DefaultBreakerFailures
	}
	if r.BreakerOpen <= 0 {
		r.BreakerOpen = DefaultBreakerOpen
	}
	return r
}

// routeSet 是一个类别的路由与各自的熔断器。
type routeSet struct {
	router   upstream.Router
	proto    upstream.ProtocolRouter // 路由有线路格式时非 nil
	names    []string
	breakers []*breaker.Breaker
	bal      *balancer // weighted 与 latency 模式；ordered 为 nil
}

// RouteState 是一个路由的熔断器状态（RouteStates；供指标与诊断使用）。
type RouteState struct {
	Kind  upstream.Kind
	Route string
	State string // closed | open | half_open
}

// newRouteSet 为多路由的 adapter 建立路由与熔断器；单路由或不是 Router 时返回 nil。
func (c *Coordinator) newRouteSet(a upstream.Adapter) *routeSet {
	r, ok := a.(upstream.Router)
	if !ok || len(r.Routes()) < 2 {
		return nil
	}
	rs := &routeSet{router: r, names: r.Routes()}
	rs.proto, _ = a.(upstream.ProtocolRouter)
	if m := c.routing.effectiveMode(); m != RoutingOrdered {
		rs.bal = newBalancer(m, rs.names, c.routing.Weights)
	}
	for _, name := range rs.names {
		name, kind := name, a.Kind()
		rs.breakers = append(rs.breakers, breaker.New(breaker.Config{
			FailureThreshold: c.routing.BreakerFailures, OpenDuration: c.routing.BreakerOpen, Now: c.breakerClock(),
			OnTransition: func(from, to breaker.State) {
				obs.M().BreakerTransition(string(kind), name, to.String())
				c.log.Warn("gateway: breaker", "endpoint", endpointOf(kind), "route", name, "from", from.String(), "to", to.String())
			},
		}))
	}
	return rs
}

func (c *Coordinator) breakerClock() func() time.Time {
	if c.routing.breakerNow != nil {
		return c.routing.breakerNow
	}
	return c.now
}

// RouteStates 返回全部多路由类别的路由与熔断器状态（按类别、路由顺序）。
func (c *Coordinator) RouteStates() []RouteState {
	var out []RouteState
	for _, k := range []upstream.Kind{upstream.KindChat, upstream.KindSearch, upstream.KindFetch} {
		rs := c.routes[k]
		if rs == nil {
			continue
		}
		for i, name := range rs.names {
			out = append(out, RouteState{Kind: k, Route: name, State: rs.breakers[i].State().String()})
		}
	}
	return out
}

// leg 是一次 try 的执行参数：route = -1 表示不按路由执行（单供应商，adapter.Do）。
type leg struct {
	route    int
	provider string // 路由名；单供应商为空
	skipped  string // 本 try 之前跳过的路由（"name:reason,..."）
	hedge    bool
	est      int64
}

// pick 是一轮中的路由选择结果。ok 时 route 已取得熔断器放行（之后须恰好报告一次）。
type pick struct {
	leg      leg
	ok       bool
	anyTried bool // 有路由因本轮已失败而被跳过（没有可放行的路由时：本轮结束，而不是降级）
}

// convertible 报告规范化请求能否在路由 i 上执行：OpenAI 格式的路由总能（不解析请求体）；其他格式的路由须能转换。
func (rs *routeSet) convertible(i int, resolved []byte) bool {
	if rs.proto == nil || rs.proto.RouteProtocol(i) == llmproto.ProtocolOpenAI {
		return true
	}
	return rs.proto.Convertible(i, resolved)
}

// eligible 报告路由 i 能否执行本调用（提供该模型、格式可用），不看熔断器。
func (rs *routeSet) eligible(j *job, i int) bool {
	return rs.router.Serves(i, j.model) && rs.convertible(i, j.resolved)
}

// planOrder 在调用开始时算出本调用的路由顺序（ordered 为配置顺序）。
func (rs *routeSet) planOrder(j *job) {
	j.order = rs.order(func(i int) bool { return rs.eligible(j, i) })
}

// pick 按本调用的路由顺序（j.order；未算出时为配置顺序）选第一个提供 model、格式可用、不在 tried 中且熔断器放行的
// 路由，并给出估算。估算失败的路由按未提供处理。
func (rs *routeSet) pick(j *job, tried map[int]bool) pick {
	var skipped []string
	var p pick
	order := j.order
	if order == nil {
		for i := range rs.names {
			order = append(order, i)
		}
	}
	for _, i := range order {
		name := rs.names[i]
		reason := ""
		switch {
		case !rs.router.Serves(i, j.model):
			reason = SkipModelNotServed
		case !rs.convertible(i, j.resolved):
			reason = SkipProtocol
		case tried[i]:
			reason, p.anyTried = SkipTried, true
		case !rs.breakers[i].Allow():
			reason = SkipCircuitOpen
		}
		if reason == "" {
			est, err := rs.router.EstimateOn(i, j.resolved)
			if err != nil {
				rs.breakers[i].Abort()
				reason = SkipModelNotServed
			} else {
				p.leg, p.ok = leg{route: i, provider: name, skipped: strings.Join(skipped, ","), est: est}, true
				return p
			}
		}
		skipped = append(skipped, name+":"+reason)
	}
	p.leg.skipped = strings.Join(skipped, ",")
	return p
}

// ready 报告除 tried 之外是否还有能执行本调用且熔断器就绪的路由（不改变熔断器状态）：决定失败之后是立即换路由
// 还是退避。
func (rs *routeSet) ready(j *job, tried map[int]bool) bool {
	for i := range rs.names {
		if !tried[i] && rs.eligible(j, i) && rs.breakers[i].Ready() {
			return true
		}
	}
	return false
}

// states 是各路由的熔断器状态（日志）。
func (rs *routeSet) states() string {
	parts := make([]string, len(rs.names))
	for i, name := range rs.names {
		parts[i] = name + ":" + rs.breakers[i].State().String()
	}
	return strings.Join(parts, ",")
}

// report 把一次已放行 try 的结果报告给熔断器：ok 与请求本身被拒的 fatal（供应商作出了回答）→ 成功；retryable、
// unknown 与供应商配置类的 fatal（providerFault）→ 失败；被 Coordinator 自己取消（取消类原因、期限、关闭、对冲
// 落败）→ 中止（不作判定，只释放半开试探名额）。
func (rs *routeSet) report(l leg, uerr *upstream.Error, aborted bool) {
	if rs == nil || l.route < 0 {
		return
	}
	b := rs.breakers[l.route]
	switch {
	case aborted:
		b.Abort()
	case uerr == nil || (uerr.Outcome == upstream.OutcomeFatal && !providerFault(uerr)):
		b.Success()
	default:
		b.Failure()
	}
}

// providerFault 报告 fatal 结果是否源于供应商本身或其配置，而不是请求：鉴权失败（401、403）、地址或模型不存在
// （404）、出站防护拒绝、地址不合法、重定向过多。它们不换供应商（fatal 仍结束调用），但计入熔断。
func providerFault(e *upstream.Error) bool {
	switch e.Code {
	case upstream.CodeEgressBlocked, upstream.CodeInvalidURL, upstream.CodeTooManyRedirects:
		return true
	}
	return e.Status == 401 || e.Status == 403 || e.Status == 404
}

// observe 把一次 ok 的 try 的延迟记入该路由的 EWMA（latency 模式；其余模式与单供应商为空操作）。
func (rs *routeSet) observe(l leg, uerr *upstream.Error, d time.Duration) {
	if rs != nil && l.route >= 0 && uerr == nil {
		rs.bal.observe(l.route, d)
	}
}

// release 结束一个已放行但没有发出的路由（预留被拒、槽位等待被取消）：熔断器中止。
func (rs *routeSet) release(l leg) {
	if rs != nil && l.route >= 0 {
		rs.breakers[l.route].Abort()
	}
}

// semKey 是 try 的每 provider 在途槽位键：单供应商为 adapter 的 Provider()，路由为 Provider()/路由名。
func (j *job) semKey(l leg) string {
	if l.route < 0 {
		return j.ad.Provider()
	}
	return j.ad.Provider() + "/" + l.provider
}

// errHedgeLost 是对冲中落败一方的取消原因。
var errHedgeLost = errors.New("call: 对冲请求的另一方已得出结果")

// do 发出一次 try（Tx2 已提交）：按路由执行时套用每 try 超时。aborted 表示结果源于 Coordinator 自己的取消
// （父上下文已结束或对冲落败），而不是供应商的问题。
func (c *Coordinator) do(ctx context.Context, j *job, rs *routeSet, l leg) (resp upstream.Response, uerr *upstream.Error, aborted bool) {
	if l.route < 0 {
		resp, uerr = j.ad.Do(ctx, j.resolved)
		return resp, uerr, ctx.Err() != nil
	}
	tctx := ctx
	if c.routing.TryTimeout > 0 {
		var cancel context.CancelFunc
		tctx, cancel = context.WithTimeout(ctx, c.routing.TryTimeout)
		defer cancel()
	}
	resp, uerr = rs.router.DoOn(tctx, l.route, j.resolved)
	return resp, uerr, ctx.Err() != nil
}

// costOn 计算 ok 结算的实际费用：后备路由按其价格表（PricingOn），主路由与单供应商按 Coordinator 的价格表
// （与引入降级链之前相同）。
func (c *Coordinator) costOn(j *job, rs *routeSet, l leg, u upstream.Usage) int64 {
	if rs == nil || l.route <= 0 {
		return c.cost(j.in.Kind, j.model, u, l.est)
	}
	v, ok := rs.router.PricingOn(l.route, j.model).ChatCost(u)
	if !ok {
		return l.est
	}
	return v
}
