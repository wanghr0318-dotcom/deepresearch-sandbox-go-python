package cache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/breaker"
)

// 熔断器的实现在 internal/gateway/breaker（与模型供应商链共用）；这里保留缓存一直使用的名字与默认值
// （连续失败 5 次 → 打开 30 s → 半开，放行一次试探）。
const (
	DefaultFailureThreshold = breaker.DefaultFailureThreshold
	DefaultOpenDuration     = breaker.DefaultOpenDuration
)

// BreakerState 是熔断器状态。
type BreakerState = breaker.State

const (
	StateClosed   = breaker.StateClosed
	StateOpen     = breaker.StateOpen
	StateHalfOpen = breaker.StateHalfOpen
)

// Breaker 是连续失败熔断器（breaker.Breaker）。零值可用（默认值、time.Now）；每次 Allow 返回 true 之后必须恰好
// 报告一次 Success、Failure 或 Abort。
type Breaker = breaker.Breaker

// NewBreaker 创建使用给定时钟的熔断器（默认阈值与打开时长）；now 为 nil 时用 time.Now。
func NewBreaker(now func() time.Time) *Breaker { return breaker.New(breaker.Config{Now: now}) }

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
