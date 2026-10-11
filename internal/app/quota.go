package app

// 本文件装配模型网关 G2 的限额与预算（只在启用账号、即多租户开启时）：运行时 API 的限流器（Redis Lua 脚本，Redis
// 不可用时退化为进程内桶，指标 agentbox_ratelimit_degraded）、hold 的周期 sweep，以及 org 的并发沙箱上限
// （准入时按 PostgreSQL 中该 org 的存活环境计数，超出时排队等待）。

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/cache"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/obs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/ratelimit"
)

// quotaStorer 是 Store 的可选能力（internal/persistence/postgres 实现；测试用的 Store 不必实现）。
type quotaStorer interface {
	Quota() quota.Store
}

// orgSandboxer 是 Store 的可选能力：org 的 max_sandboxes 与存活环境数（含本 server 已准入、尚无环境的任务）。
type orgSandboxer interface {
	OrgSandboxes(ctx context.Context, taskID string, pending []string) (orgID int64, limit *int64, live int64, err error)
}

// 默认值。
const (
	quotaSweepInterval  = 30 * time.Second
	quotaSweepBatch     = 500
	sandboxGatePoll     = 2 * time.Second
	sandboxGateQueryMax = 5 * time.Second
	// limiterRedisPool 是限流器的 Redis 连接数：大于缓存的默认 8，减少并发请求等待连接（复审 F9）。
	limiterRedisPool = 32
)

// newRateLimiter 构造运行时 API 的限流器：配置了 Redis 时用 Redis（独立的连接池），否则只用进程内桶。
func (s *server) newRateLimiter() *ratelimit.Fallback {
	var ev ratelimit.Evaler
	if s.cfg.RedisAddr != "" {
		r := cache.NewRedis(cache.RedisConfig{Addr: s.cfg.RedisAddr, PoolSize: limiterRedisPool})
		s.limiterRedis = r
		ev = r
	}
	lim := ratelimit.NewFallback(ev, ratelimit.FallbackOptions{OnChange: func(degraded bool, err error) {
		if degraded {
			s.log.Warn("限流器退化为进程内桶：Redis 不可用或超过 50 ms（限额改为每个 server 各自生效；预算不受影响）",
				"error", err.Error())
			return
		}
		s.log.Info("限流器已恢复使用 Redis")
	}})
	if obs.Metrics() {
		obs.M().RegisterGauges(func() []obs.Gauge {
			v := 0.0
			if lim.Degraded() {
				v = 1
			}
			fb, fails := lim.Stats()
			return []obs.Gauge{
				{Name: "ratelimit_degraded", Help: "1 while the rate limiter uses in-process buckets because Redis failed.", Value: v},
				{Name: "ratelimit_fallback_decisions", Help: "Rate-limit decisions made in process because Redis failed (since start).", Value: float64(fb)},
				{Name: "ratelimit_redis_failures", Help: "Redis errors and timeouts seen by the rate limiter (since start).", Value: float64(fails)},
			}
		})
	}
	return lim
}

// sweepQuota 周期地把期限已过的 hold 转为 charged_unknown，直到 ctx 取消。
func (s *server) sweepQuota(ctx context.Context, q quota.Store) {
	t := time.NewTicker(quotaSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := q.Sweep(ctx, quotaSweepBatch)
		switch {
		case err != nil && ctx.Err() == nil:
			s.log.Warn("预算 hold sweep 失败", "error", err.Error())
		case n > 0:
			s.log.Warn("期限已过的预算 hold 按全额转为 charged_unknown", "holds", n)
		}
	}
}

// sandboxGate 在准入时执行 org 的并发沙箱上限（limits.max_sandboxes，org 层级）：存活环境数（PostgreSQL，含其他 server）
// 加本 server 已准入、尚无环境的任务达到上限时，任务等待（每 sandboxGatePoll 复查）。同一 server 上的检查串行，
// 跨 server 的并发准入可能各超出一个。查询失败时放行（这是容量限制；钱由预算保护）。
type sandboxGate struct {
	store orgSandboxer
	log   *slog.Logger
	poll  time.Duration

	admitMu sync.Mutex // 串行化"计数 + 登记"
	mu      sync.Mutex
	pending map[string]bool // 本 server 持有授予的任务
}

func newSandboxGate(store orgSandboxer, log *slog.Logger) *sandboxGate {
	return &sandboxGate{store: store, log: log, poll: sandboxGatePoll, pending: map[string]bool{}}
}

func (g *sandboxGate) pendingTasks() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.pending))
	for id := range g.pending {
		out = append(out, id)
	}
	return out
}

// admit 等到 taskID 所属 org 有空位，并把它登记为本 server 已准入；ctx 取消时返回其错误。
func (g *sandboxGate) admit(ctx context.Context, taskID string) error {
	for {
		ok, err := g.try(ctx, taskID)
		if err != nil || ok {
			return err
		}
		t := time.NewTimer(g.poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (g *sandboxGate) try(ctx context.Context, taskID string) (bool, error) {
	g.admitMu.Lock()
	defer g.admitMu.Unlock()
	qctx, cancel := context.WithTimeout(ctx, sandboxGateQueryMax)
	org, limit, live, err := g.store.OrgSandboxes(qctx, taskID, g.pendingTasks())
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		g.log.Warn("org 并发沙箱检查失败，放行", "task_id", taskID, "error", err.Error())
	} else if limit != nil && live >= *limit {
		return false, nil
	} else if limit != nil {
		g.log.Debug("org 并发沙箱检查通过", "org_id", org, "live", live, "limit", *limit)
	}
	g.mu.Lock()
	g.pending[taskID] = true
	g.mu.Unlock()
	return true, nil
}

// release 在任务归还授予时注销。
func (g *sandboxGate) release(taskID string) {
	g.mu.Lock()
	delete(g.pending, taskID)
	g.mu.Unlock()
}

// ---- 结算失败后仍为 held 的预留（复审 F7）----

// staleLedgerStore 是 Store 的可选能力：列出调用期限已过仍为 held 的 money reservation，并按启动账本转换的同一路径
// 逐个转为 charged_unknown（reservation、任务账本与月度预算的 hold 同一事务）。
type staleLedgerStore interface {
	StaleHeldReservations(ctx context.Context, grace time.Duration) ([]string, error)
	ConvertStaleReservations(ctx context.Context, ids []string) (int, error)
}

// 结算失败的预留的转换：每分钟一次；调用期限过去 staleGrace 之后才考虑（try 不会比调用期限活得更久）。
const (
	staleLedgerInterval = time.Minute
	staleGrace          = 2 * time.Minute
)

// inflightTries 是本进程中已预留、尚未尝试结算的 try（按 reservation ID）。
type inflightTries struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newInflightTries() *inflightTries { return &inflightTries{m: map[string]struct{}{}} }

func (t *inflightTries) add(id string) {
	t.mu.Lock()
	t.m[id] = struct{}{}
	t.mu.Unlock()
}

func (t *inflightTries) remove(id string) {
	t.mu.Lock()
	delete(t.m, id)
	t.mu.Unlock()
}

func (t *inflightTries) has(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.m[id]
	return ok
}

// trackingCallStore 在 Gateway 的 call.Store 外登记在途 try：ReserveTry 成功后登记，SettleTry 尝试之后（无论成败）注销。
// 注销之后仍为 held 的 reservation 即结算失败，由 convertStaleLedger 转换。
type trackingCallStore struct {
	call.Store
	t *inflightTries
}

func (s trackingCallStore) ReserveTry(ctx context.Context, r call.ReserveTryRequest) (call.Try, error) {
	tr, err := s.Store.ReserveTry(ctx, r)
	if err == nil {
		s.t.add(tr.ReservationID)
	}
	return tr, err
}

func (s trackingCallStore) SettleTry(ctx context.Context, st call.Settlement) (call.CallRecord, error) {
	defer s.t.remove(st.Try.ReservationID)
	return s.Store.SettleTry(ctx, st)
}

// convertStaleLedgerOnce 把调用期限已过 staleGrace、仍为 held 且不在本进程中执行的 money reservation（及其 hold）转为
// charged_unknown。在途的 try 从不触碰。
func convertStaleLedgerOnce(ctx context.Context, st staleLedgerStore, inflight *inflightTries) (int, error) {
	ids, err := st.StaleHeldReservations(ctx, staleGrace)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	stale := ids[:0]
	for _, id := range ids {
		if !inflight.has(id) {
			stale = append(stale, id)
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}
	return st.ConvertStaleReservations(ctx, stale)
}

// convertStaleLedger 每 staleLedgerInterval 运行一次 convertStaleLedgerOnce，直到 ctx 取消。
func (s *server) convertStaleLedger(ctx context.Context, st staleLedgerStore) {
	t := time.NewTicker(staleLedgerInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := convertStaleLedgerOnce(ctx, st, s.inflight)
		switch {
		case err != nil && ctx.Err() == nil:
			s.log.Warn("转换结算失败的预留失败", "error", err.Error())
		case n > 0:
			s.log.Warn("结算失败、仍为 held 的预留按全额转为 charged_unknown（与重启时的账本转换相同）", "reservations", n)
		}
	}
}
