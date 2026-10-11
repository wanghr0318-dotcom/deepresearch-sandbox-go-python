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

// orgSandboxer 是 Store 的可选能力：org 的 max_sandboxes 与存活沙箱数（含本 server 已准入、尚无环境的任务）。
type orgSandboxer interface {
	OrgSandboxLimit(ctx context.Context, taskID string) (orgID int64, limit *int64, err error)
	OrgLiveSandboxes(ctx context.Context, orgID int64, taskID string, pending []string) (live int64, self bool, err error)
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

// sandboxGate 在准入时执行 org 的并发沙箱上限（limits.max_sandboxes，只在 org 层级）：
//   - 没有上限的 org 只读一次上限（主键查找），不计数、不排队（复审 F3）；
//   - 有上限时按 org 严格 FIFO 排队（复审 F2）：只有队首计数——PostgreSQL 中该 org 的存活沙箱（含其他 server）加本 server
//     已准入、尚无环境的任务——有空位即准入；本 server 归还授予时唤醒队首，轮询（sandboxGatePoll）只是兜底（其他
//     server 的沙箱停止、上限被调高）；
//   - 计数 + 登记按 org 串行（每个 org 一把锁，不是全局锁），跨 server 的并发准入可能各超出一个；
//   - 查询失败时放行（这是容量限制；钱由预算保护）。
//
// 等待期间任务保持 queued（与等待 run slot 相同；没有单独的等待事件）。
type sandboxGate struct {
	store orgSandboxer
	log   *slog.Logger
	poll  time.Duration

	mu      sync.Mutex
	orgs    map[int64]*orgQueue
	pending map[string]int64 // 本 server 持有授予的任务 → 其 org
}

// orgQueue 是一个 org 的等待者（FIFO）与"计数 + 登记"的锁。
type orgQueue struct {
	admit   sync.Mutex
	waiters []*gateWaiter
}

type gateWaiter struct {
	task string
	wake chan struct{} // 容量 1：队首变化或沙箱归还时通知
}

func newSandboxGate(store orgSandboxer, log *slog.Logger) *sandboxGate {
	return &sandboxGate{store: store, log: log, poll: sandboxGatePoll, orgs: map[int64]*orgQueue{}, pending: map[string]int64{}}
}

func notify(w *gateWaiter) {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Waiting 返回正在等待 org 并发沙箱上限的任务数（指标）。
func (g *sandboxGate) Waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, q := range g.orgs {
		n += len(q.waiters)
	}
	return n
}

func (g *sandboxGate) register(task string, org int64) {
	g.mu.Lock()
	g.pending[task] = org
	g.mu.Unlock()
}

// admit 等到 taskID 所属 org 有空位，并把它登记为本 server 已准入；ctx 取消时返回其错误。
func (g *sandboxGate) admit(ctx context.Context, taskID string) error {
	org, limit, err := g.limit(ctx, taskID)
	if err != nil || limit == nil {
		if err == nil || ctx.Err() == nil {
			g.register(taskID, org)
			return nil
		}
		return err
	}
	w := &gateWaiter{task: taskID, wake: make(chan struct{}, 1)}
	g.mu.Lock()
	q := g.orgs[org]
	if q == nil {
		q = &orgQueue{}
		g.orgs[org] = q
	}
	q.waiters = append(q.waiters, w)
	g.mu.Unlock()
	defer g.leave(org, q, w)
	waited := false
	for {
		if g.head(q) == w {
			ok, err := g.try(ctx, org, q, taskID)
			if err != nil || ok {
				return err
			}
		}
		if !waited {
			waited = true
			g.log.Info("任务等待 org 的并发沙箱上限", "task_id", taskID, "org_id", org)
		}
		t := time.NewTimer(g.poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-w.wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// limit 读取任务的 org 与其上限；查询失败时放行（limit nil）。
func (g *sandboxGate) limit(ctx context.Context, taskID string) (int64, *int64, error) {
	qctx, cancel := context.WithTimeout(ctx, sandboxGateQueryMax)
	defer cancel()
	org, limit, err := g.store.OrgSandboxLimit(qctx, taskID)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		g.log.Warn("读取 org 并发沙箱上限失败，放行", "task_id", taskID, "error", err.Error())
		return org, nil, nil
	}
	return org, limit, nil
}

func (g *sandboxGate) head(q *orgQueue) *gateWaiter {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(q.waiters) == 0 {
		return nil
	}
	return q.waiters[0]
}

// leave 把 w 移出队列；队首变化时通知新的队首。
func (g *sandboxGate) leave(org int64, q *orgQueue, w *gateWaiter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	wasHead := len(q.waiters) > 0 && q.waiters[0] == w
	for i, x := range q.waiters {
		if x == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
	if len(q.waiters) == 0 {
		delete(g.orgs, org)
		return
	}
	if wasHead {
		notify(q.waiters[0])
	}
}

// try 是队首的一次检查：在 org 的锁下重读上限并计数，有空位（或任务自己已有存活环境）则登记。
func (g *sandboxGate) try(ctx context.Context, org int64, q *orgQueue, taskID string) (bool, error) {
	q.admit.Lock()
	defer q.admit.Unlock()
	_, limit, err := g.limit(ctx, taskID)
	if err != nil {
		return false, err
	}
	if limit == nil {
		g.register(taskID, org)
		return true, nil
	}
	g.mu.Lock()
	pend := make([]string, 0, len(g.pending))
	for id, o := range g.pending {
		if o == org {
			pend = append(pend, id)
		}
	}
	g.mu.Unlock()
	qctx, cancel := context.WithTimeout(ctx, sandboxGateQueryMax)
	live, self, err := g.store.OrgLiveSandboxes(qctx, org, taskID, pend)
	cancel()
	switch {
	case err != nil && ctx.Err() != nil:
		return false, ctx.Err()
	case err != nil:
		g.log.Warn("org 并发沙箱计数失败，放行", "task_id", taskID, "error", err.Error())
	case !self && live >= *limit:
		return false, nil
	}
	g.register(taskID, org)
	return true, nil
}

// release 在任务归还授予时注销，并唤醒其 org 的队首。
func (g *sandboxGate) release(taskID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	org, ok := g.pending[taskID]
	if !ok {
		return
	}
	delete(g.pending, taskID)
	if q := g.orgs[org]; q != nil && len(q.waiters) > 0 {
		notify(q.waiters[0])
	}
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
