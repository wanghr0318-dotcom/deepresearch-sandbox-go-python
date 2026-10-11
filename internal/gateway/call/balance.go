package call

// 路由模式（模型网关设计 §7.6）：决定一次调用依次尝试各路由的顺序。熔断、每 try 超时、对冲与失败后换路由的规则
// 不变（routing.go），只有顺序随模式而变：
//
//   - ordered（没有权重时的默认）：配置顺序，与引入路由模式之前完全相同。
//   - weighted（给出任何权重时的默认）：在提供该模型且熔断器就绪（关闭，或打开时长已过、可以半开试探）的路由中
//     按平滑加权轮询（nginx 算法）选第一个，其余路由按权重从高到低（同权重按配置顺序）作为后备。熔断后恢复的
//     路由由此得到半开试探，成功后重新得到它的份额。
//   - latency：同 weighted，但有效权重 = 权重 / EWMA(延迟)，并设下限（不低于最快路由有效权重的 LatencyFloor
//     倍），慢的路由仍有少量探测流量。每个路由有两个 EWMA：沙箱调用的整个回复时间与模型 API 流式调用的首 token
//     时间；一次调用用与它的路径相符的那个，候选路由都没有该样本时用另一个。超时与 unknown 的结果记一个惩罚样本
//     （每 try 超时与当前 EWMA 两倍中的较大者），挂起的路由因此迅速失去流量。
//
// 顺序在每次调用开始时算一次（调用内的换路由与对冲沿用它），因此一次调用只推进一次轮询状态。之后在任何 try
// 之前被拒绝的调用（预算、槽位）也已推进轮询：这只让分配在短时间内略有偏差，不影响正确性，因此不回退。

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
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
	if r.Mode == RoutingOrdered && len(r.Weights) > 0 {
		return fmt.Errorf("call: 路由模式 ordered 不使用权重")
	}
	for name, w := range r.Weights {
		if w < 1 {
			return fmt.Errorf("call: 路由 %s 的权重须至少为 1，得到 %d", name, w)
		}
	}
	return nil
}

// latencySignal 是延迟样本的来源。
type latencySignal int

const (
	signalResponse latencySignal = iota // 沙箱调用（非流式）：整个回复的时间
	signalTTFT                          // 模型 API 流式调用：首 token 时间
	numSignals
)

// balancer 是一个路由集合的轮询与延迟状态。
type balancer struct {
	mode    RoutingMode
	weights []float64 // 配置的权重（缺省 1）

	mu      sync.Mutex
	current []float64             // 平滑加权轮询的当前值
	ewma    [numSignals][]float64 // 各信号的延迟 EWMA（秒）；0 = 还没有样本
}

func newBalancer(mode RoutingMode, names []string, weights map[string]int) *balancer {
	b := &balancer{mode: mode, weights: make([]float64, len(names)), current: make([]float64, len(names))}
	for sig := range b.ewma {
		b.ewma[sig] = make([]float64, len(names))
	}
	for i, n := range names {
		b.weights[i] = 1
		if w, ok := weights[n]; ok && w > 0 {
			b.weights[i] = float64(w)
		}
	}
	return b
}

// observe 记录路由 i 的一次延迟样本。
func (b *balancer) observe(sig latencySignal, i int, d time.Duration) {
	if b == nil || i < 0 || i >= len(b.current) || d <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.add(sig, i, max(d, minLatency).Seconds())
}

// penalize 记录路由 i 的一次超时或 unknown 结果：样本取 elapsed、每 try 超时与当前 EWMA 两倍中的最大者。
func (b *balancer) penalize(sig latencySignal, i int, elapsed, tryTimeout time.Duration) {
	if b == nil || i < 0 || i >= len(b.current) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := max(elapsed.Seconds(), tryTimeout.Seconds(), 2*b.ewma[sig][i])
	if s <= 0 {
		return // 没有任何依据（没有超时、没有样本、没有耗时）
	}
	b.add(sig, i, max(s, minLatency.Seconds()))
}

// add 把样本 s（秒）计入 EWMA。须持有 mu。
func (b *balancer) add(sig latencySignal, i int, s float64) {
	if b.ewma[sig][i] == 0 {
		b.ewma[sig][i] = s
		return
	}
	b.ewma[sig][i] = latencyAlpha*s + (1-latencyAlpha)*b.ewma[sig][i]
}

// signalFor 是候选路由使用的信号：有候选路由带 sig 的样本时用 sig，否则用另一个信号。须持有 mu。
func (b *balancer) signalFor(sig latencySignal, cands []int) latencySignal {
	for _, i := range cands {
		if b.ewma[sig][i] > 0 {
			return sig
		}
	}
	return (sig + 1) % numSignals
}

// effective 返回候选路由的有效权重（weighted：配置的权重；latency：权重 / EWMA，有下限）。须持有 mu。
func (b *balancer) effective(cands []int, sig latencySignal) []float64 {
	eff := make([]float64, len(cands))
	if b.mode != RoutingLatency {
		for k, i := range cands {
			eff[k] = b.weights[i]
		}
		return eff
	}
	ewma := b.ewma[b.signalFor(sig, cands)]
	known := 0.0 // 候选路由中最小的已知延迟；没有样本的路由按它计算（新路由先得到与最快路由相当的流量）
	for _, i := range cands {
		if l := ewma[i]; l > 0 && (known == 0 || l < known) {
			known = l
		}
	}
	top := 0.0
	for k, i := range cands {
		l := ewma[i]
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
func (b *balancer) next(cands []int, sig latencySignal) int {
	if len(cands) == 0 {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	eff := b.effective(cands, sig)
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
// 模型、格式可用）中熔断器就绪者（Ready：关闭，或打开时长已过、没有试探在进行）里轮询选出的一个，其余按权重从高
// 到低。sig 是本调用的延迟信号（latency 模式）。
func (rs *routeSet) order(eligible func(i int) bool, sig latencySignal) []int {
	idx := make([]int, len(rs.names))
	for i := range idx {
		idx[i] = i
	}
	if rs.bal == nil || rs.bal.mode == RoutingOrdered {
		return idx
	}
	var cands []int
	for i := range rs.names {
		if eligible(i) && rs.breakers[i].Ready() {
			cands = append(cands, i)
		}
	}
	first := rs.bal.next(cands, sig)
	rest := slices.DeleteFunc(idx, func(i int) bool { return i == first })
	sort.SliceStable(rest, func(a, b int) bool { return rs.bal.weights[rest[a]] > rs.bal.weights[rest[b]] })
	if first < 0 {
		return rest
	}
	return append([]int{first}, rest...)
}
