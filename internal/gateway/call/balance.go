package call

// 路由模式（模型网关设计 §7.6）：决定一次调用依次尝试各路由的顺序。熔断、每 try 超时、对冲与失败后换路由的规则
// 不变（routing.go），只有顺序随模式而变：
//
//   - ordered（没有权重时的默认）：配置顺序，与引入路由模式之前完全相同。
//   - weighted（给出任何权重时的默认）：在提供该模型且熔断器关闭的路由中按平滑加权轮询（nginx 算法）选第一个，
//     其余路由按权重从高到低（同权重按配置顺序）作为后备。
//   - latency：同 weighted，但有效权重 = 权重 / EWMA(首 token 时间)，并设下限（不低于最快路由有效权重的
//     LatencyFloor 倍），慢的路由仍有少量探测流量，恢复后能重新得到流量。
//
// 顺序在每次调用开始时算一次（调用内的换路由与对冲沿用它），因此一次调用只推进一次轮询状态。

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/breaker"
)

// RoutingMode 是路由模式。
type RoutingMode string

// 路由模式。
const (
	RoutingOrdered  RoutingMode = "ordered"
	RoutingWeighted RoutingMode = "weighted"
	RoutingLatency  RoutingMode = "latency"
)

// 延迟模式的参数。
const (
	// LatencyFloor 是有效权重的下限：不低于候选路由中最大有效权重的这个比例。
	LatencyFloor = 0.05
	// latencyAlpha 是 EWMA 的平滑系数（新样本的权重）。
	latencyAlpha = 0.2
	// minLatency 是参与计算的最小延迟（避免除以零）。
	minLatency = time.Millisecond
)

// EffectiveMode 是生效的模式：未设置时有权重为 weighted，否则 ordered。
func (r RoutingConfig) EffectiveMode() RoutingMode {
	if r.Mode != "" {
		return r.Mode
	}
	if len(r.Weights) > 0 {
		return RoutingWeighted
	}
	return RoutingOrdered
}

// validate 检查模式与权重。
func (r RoutingConfig) validate() error {
	switch r.Mode {
	case "", RoutingOrdered, RoutingWeighted, RoutingLatency:
	default:
		return fmt.Errorf("call: 路由模式须为 ordered、weighted 或 latency，得到 %q", r.Mode)
	}
	for name, w := range r.Weights {
		if w < 1 {
			return fmt.Errorf("call: 路由 %s 的权重须至少为 1，得到 %d", name, w)
		}
	}
	return nil
}

// balancer 是一个路由集合的轮询与延迟状态。
type balancer struct {
	mode    RoutingMode
	weights []float64 // 配置的权重（缺省 1）

	mu      sync.Mutex
	current []float64 // 平滑加权轮询的当前值
	ewma    []float64 // 首 token 时间的 EWMA（秒）；0 = 还没有样本
}

func newBalancer(mode RoutingMode, names []string, weights map[string]int) *balancer {
	b := &balancer{mode: mode, weights: make([]float64, len(names)), current: make([]float64, len(names)),
		ewma: make([]float64, len(names))}
	for i, n := range names {
		b.weights[i] = 1
		if w, ok := weights[n]; ok && w > 0 {
			b.weights[i] = float64(w)
		}
	}
	return b
}

// observe 记录路由 i 的一次首 token 时间（非流式调用为整个回复的时间）。
func (b *balancer) observe(i int, d time.Duration) {
	if b == nil || i < 0 || i >= len(b.ewma) || d <= 0 {
		return
	}
	s := max(d, minLatency).Seconds()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ewma[i] == 0 {
		b.ewma[i] = s
		return
	}
	b.ewma[i] = latencyAlpha*s + (1-latencyAlpha)*b.ewma[i]
}

// effective 返回候选路由的有效权重（weighted：配置的权重；latency：权重 / EWMA，有下限）。须持有 mu。
func (b *balancer) effective(cands []int) []float64 {
	eff := make([]float64, len(cands))
	if b.mode != RoutingLatency {
		for k, i := range cands {
			eff[k] = b.weights[i]
		}
		return eff
	}
	known := 0.0 // 候选路由中最小的已知延迟；没有样本的路由按它计算（新路由先得到与最快路由相当的流量）
	for _, i := range cands {
		if l := b.ewma[i]; l > 0 && (known == 0 || l < known) {
			known = l
		}
	}
	top := 0.0
	for k, i := range cands {
		l := b.ewma[i]
		if l == 0 {
			l = known
		}
		if l == 0 {
			l = 1 // 都没有样本：等同 weighted
		}
		eff[k] = b.weights[i] / max(l, minLatency.Seconds())
		top = max(top, eff[k])
	}
	for k := range eff {
		eff[k] = max(eff[k], LatencyFloor*top)
	}
	return eff
}

// next 在候选路由中按平滑加权轮询选一个（候选为空时返回 -1）：每个候选的当前值加上有效权重，选当前值最大者
// （同值取配置顺序靠前者），再从它的当前值中减去有效权重之和。
func (b *balancer) next(cands []int) int {
	if len(cands) == 0 {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	eff := b.effective(cands)
	total, best := 0.0, -1
	for k, i := range cands {
		b.current[i] += eff[k]
		total += eff[k]
		if best < 0 || b.current[i] > b.current[cands[best]] {
			best = k
		}
	}
	b.current[cands[best]] -= total
	return cands[best]
}

// order 返回一次调用依次考虑的路由顺序。ordered 为配置顺序；weighted 与 latency 先是在 eligible 路由（提供该
// 模型、格式可用）中熔断器关闭者里轮询选出的一个，其余按权重从高到低。
func (rs *routeSet) order(eligible func(i int) bool) []int {
	idx := make([]int, len(rs.names))
	for i := range idx {
		idx[i] = i
	}
	if rs.bal == nil || rs.bal.mode == RoutingOrdered {
		return idx
	}
	var cands []int
	for i := range rs.names {
		if eligible(i) && rs.breakers[i].State() == breaker.StateClosed {
			cands = append(cands, i)
		}
	}
	first := rs.bal.next(cands)
	rest := slices.DeleteFunc(idx, func(i int) bool { return i == first })
	sort.SliceStable(rest, func(a, b int) bool { return rs.bal.weights[rest[a]] > rs.bal.weights[rest[b]] })
	if first < 0 {
		return rest
	}
	return append([]int{first}, rest...)
}
