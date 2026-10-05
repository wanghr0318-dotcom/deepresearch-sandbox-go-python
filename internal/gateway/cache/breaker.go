package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// 熔断默认值（规格 §11.5、§19）。
const (
	DefaultFailureThreshold = 5
	DefaultOpenDuration     = 30 * time.Second
)

// BreakerState 是熔断器状态。
type BreakerState int

const (
	StateClosed   BreakerState = iota // 正常放行
	StateOpen                         // 拒绝所有操作，直到打开时长届满
	StateHalfOpen                     // 届满后只放行一次试探
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Breaker 是连续失败熔断器：连续失败 5 次 → 打开 30 s → 半开，放行一次试探；试探成功则关闭，失败则重新打开。
// 零值可用（使用 time.Now）；测试用 NewBreaker 注入时钟。每次 Allow 返回 true 之后必须恰好报告一次
// Success、Failure 或 Abort，否则半开试探不会结束。
type Breaker struct {
	now func() time.Time

	mu       sync.Mutex
	state    BreakerState
	failures int
	openedAt time.Time
	probing  bool
}

// NewBreaker 创建使用给定时钟的熔断器；now 为 nil 时用 time.Now。
func NewBreaker(now func() time.Time) *Breaker { return &Breaker{now: now} }

func (b *Breaker) clock() time.Time {
	if b.now == nil {
		return time.Now()
	}
	return b.now()
}

// Allow 报告本次操作能否发出。打开期满后转为半开并只放行一个试探。
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.clock().Sub(b.openedAt) < DefaultOpenDuration {
			return false
		}
		b.state = StateHalfOpen
	}
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

// Success 报告一次成功：半开时关闭熔断器；关闭时清零连续失败计数。
// 打开状态下迟到的成功（熔断前发出的操作）不改变状态。
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.state, b.probing, b.failures = StateClosed, false, 0
	case StateClosed:
		b.failures = 0
	}
}

// Failure 报告一次失败：半开时重新打开；关闭时累计，连续达到 5 次则打开。打开状态下迟到的失败被忽略。
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.state, b.probing, b.openedAt = StateOpen, false, b.clock()
	case StateClosed:
		b.failures++
		if b.failures >= DefaultFailureThreshold {
			b.state, b.failures, b.openedAt = StateOpen, 0, b.clock()
		}
	}
}

// Abort 结束一次既非成功也非失败的操作（例如调用方取消了 ctx）：不改变状态与计数，只释放半开试探名额。
func (b *Breaker) Abort() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen {
		b.probing = false
	}
}

// State 返回当前状态（打开期满但尚未有操作到来时仍报告 open）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Metrics 是缓存指标（§11.5）。Guarded 只记录 Error 与 BreakerOpen；Hit、Miss、Bypass、Coalesced、
// IntegrityFailure 由知道校验结果的上层记录（Redis 返回了条目不等于命中：条目还要通过完整性校验）。
type Metrics struct {
	Hit, Miss, Bypass, Coalesced, Error, BreakerOpen, IntegrityFailure atomic.Int64
}

// Guarded 用熔断器与指标包装 kv，使 Redis 永远不成为错误来源：
// 熔断打开或出错 → 视为未命中（Get 返回 nil, false, nil）；Set、Del 失败只计数并返回 nil。
// 熔断打开时操作不发出，计 BreakerOpen；出错计 Error。调用方 ctx 已结束导致的错误不计入熔断。
// 超过 MaxValueSize 的值在发出前被拒绝（计 Error，不计入熔断）。b、m 为 nil 时使用新的零值。
func Guarded(kv KV, b *Breaker, m *Metrics) KV {
	if b == nil {
		b = &Breaker{}
	}
	if m == nil {
		m = &Metrics{}
	}
	return &guarded{kv: kv, b: b, m: m}
}

type guarded struct {
	kv KV
	b  *Breaker
	m  *Metrics
}

func (g *guarded) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if !g.b.Allow() {
		g.m.BreakerOpen.Add(1)
		return nil, false, nil
	}
	v, ok, err := g.kv.Get(ctx, key)
	if g.report(ctx, err) {
		return nil, false, nil
	}
	return v, ok, nil
}

func (g *guarded) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if len(val) > MaxValueSize {
		g.m.Error.Add(1)
		return nil
	}
	if !g.b.Allow() {
		g.m.BreakerOpen.Add(1)
		return nil
	}
	g.report(ctx, g.kv.Set(ctx, key, val, ttl))
	return nil
}

func (g *guarded) Del(ctx context.Context, key string) error {
	if !g.b.Allow() {
		g.m.BreakerOpen.Add(1)
		return nil
	}
	g.report(ctx, g.kv.Del(ctx, key))
	return nil
}

// report 把一次已放行操作的结果报告给熔断器与指标，返回是否出错。
func (g *guarded) report(ctx context.Context, err error) bool {
	switch {
	case err == nil:
		g.b.Success()
		return false
	case ctx.Err() != nil:
		// 调用方的 ctx 已结束（取消或其期限早于操作期限）：错误不能归因于 Redis。
		g.m.Error.Add(1)
		g.b.Abort()
	default:
		g.m.Error.Add(1)
		g.b.Failure()
	}
	return true
}
