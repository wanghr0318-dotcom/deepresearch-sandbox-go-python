package call

// 对冲请求（docs/design/2026-10-10-model-fallback-design.md §4.5；默认关闭，RoutingConfig.HedgeDelay > 0 启用）。
//
// 一次 try 发出 HedgeDelay 之后仍未结束、且另有就绪路由时，为该路由另预留一个并发的 try（ReserveTry{Hedge}），
// 向它发出同一逻辑请求。先得出 ok 或 fatal 的一方为决定方，另一方以 errHedgeLost 取消。结算顺序保证调用的最终
// 状态总是决定方的：先等落败方结束并结算（未发出 → retryable 释放；已发出 → unknown，按估算全额计入 unknown，
// 即保守计费），最后由调用方结算决定方。两方都不是决定性结果时，先结束的一方先结算，后结束的一方交给调用方。
//
// 只用于模型调用（模型调用没有外部副作用，只有费用；从不缓存或合并）。对冲需要空闲的 try 次数、第二份估算的预算
// 与空闲的在途槽位；缺任何一项就不对冲，只等第一条腿。

import (
	"context"
	"errors"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// CodeHedgeLost 是对冲中落败一方的 call_tries.error（只是审计元数据）。
const CodeHedgeLost = "hedge_lost"

// runHedged 发出第一条腿，必要时在 HedgeDelay 之后发出对冲腿；返回由调用方结算的最终结果与用过的路由。
func (c *Coordinator) runHedged(ctx context.Context, j *job, rs *routeSet, first leg, firstTry Try, firstRelease func(),
	tried map[int]bool) (legResult, []int) {
	results := make(chan legResult, 2)
	cancels := map[int]context.CancelCauseFunc{} // try_no → 取消
	start := func(l leg, try Try, release func()) {
		lctx, lcancel := context.WithCancelCause(ctx)
		cancels[try.TryNo] = lcancel
		go func() {
			begin := c.now()
			resp, uerr, aborted := c.do(lctx, j, rs, l)
			faultinject.Point(faultinject.CallInFlight)
			r := legResult{leg: l, try: try, resp: resp, uerr: uerr, latency: c.now().Sub(begin), aborted: aborted}
			release()
			lcancel(nil)
			results <- r
		}()
	}
	start(first, firstTry, firstRelease)
	routes := []int{first.route}
	running := 1
	timer := time.NewTimer(c.routing.HedgeDelay)
	defer timer.Stop()
	var done []legResult
	hedgeTried := false
wait:
	for {
		select {
		case r := <-results:
			running--
			done = append(done, r)
			if decisive(r) || running == 0 {
				break wait
			}
		case <-timer.C:
			if hedgeTried || running != 1 {
				continue
			}
			hedgeTried = true
			if l, try, release, ok := c.startHedge(ctx, j, rs, first, tried); ok {
				routes = append(routes, l.route)
				start(l, try, release)
				running++
			}
		}
	}
	final := done[len(done)-1]
	for no, cancel := range cancels { // 取消落败方（已结束的腿取消是空操作）
		if no != final.try.TryNo {
			cancel(errHedgeLost)
		}
	}
	for ; running > 0; running-- {
		done = append(done, <-results)
	}
	// 决定方是 fatal 而落败方在取消生效前已 ok：以 ok 为最终结果（先结算 fatal，最后结算 ok，调用以 completed 结束）。
	if final.outcome() == upstream.OutcomeFatal {
		for _, r := range done {
			if r.outcome() == upstream.OutcomeOK {
				final = r
				break
			}
		}
	}
	for _, r := range done {
		lost := r.try.TryNo != final.try.TryNo && decisive(final)
		rs.report(r.leg, r.outcome(), r.aborted)
		c.logTry(j, rs, r)
		if r.try.TryNo == final.try.TryNo {
			continue
		}
		c.settleLeg(j, rs, r, lost)
	}
	return final, routes
}

// decisive 报告一条腿的结果是否决定调用（ok 或 fatal）：此时另一条腿被取消。
func decisive(r legResult) bool {
	o := r.outcome()
	return o == upstream.OutcomeOK || o == upstream.OutcomeFatal
}

// startHedge 为对冲腿选路由（排除第一条腿与本轮已失败的路由）、不等待地取得在途槽位并预留（Hedge）。任何一步
// 不成立都不对冲（已取得的熔断器放行与槽位随即释放）。
func (c *Coordinator) startHedge(ctx context.Context, j *job, rs *routeSet, first leg, tried map[int]bool) (leg, Try, func(), bool) {
	if ctx.Err() != nil {
		return leg{}, Try{}, nil, false
	}
	t := map[int]bool{first.route: true}
	for k, v := range tried {
		t[k] = v
	}
	p := rs.pick(j, t)
	if !p.ok {
		return leg{}, Try{}, nil, false
	}
	l := p.leg
	l.hedge = true
	release, err := c.acquire(ctx, j.in.TaskID, j.in.SubrunID, j.semKey(l), true)
	if err != nil {
		rs.release(l)
		return leg{}, Try{}, nil, false
	}
	try, err := c.reserve(j, l)
	if err != nil {
		release()
		rs.release(l)
		c.log.Info("gateway: hedge not started", "task_id", j.in.TaskID, "call_id", j.in.CallID, "route", l.provider, "err", err)
		return leg{}, Try{}, nil, false
	}
	return l, try, release, true
}

// settleLeg 结算不是最终结果的一条腿（在最终结果之前）。落败方（另一方得出了决定性结果）的错误码记为 hedge_lost；
// 若它在取消生效前已 ok，照常保存结果并按实际费用结算（费用已发生）。结算失败只记日志：预留保持 held，
// 由启动账本转换按 unknown 处理。
func (c *Coordinator) settleLeg(j *job, rs *routeSet, r legResult, lost bool) {
	var err error
	switch o := r.outcome(); o {
	case upstream.OutcomeOK:
		_, err = c.complete(j, r.try, r.resp, r.latency, c.costOn(j, rs, r.leg, r.resp.Usage))
	default:
		code := r.uerr.Code
		if lost {
			code = CodeHedgeLost
		}
		_, err = c.settle(Settlement{Try: r.try, Outcome: string(o), LatencyMs: r.latency.Milliseconds(), Error: code})
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		c.log.Warn("gateway: 对冲腿结算失败", "task_id", j.in.TaskID, "call_id", j.in.CallID, "try_no", r.try.TryNo, "err", err)
	}
}
