package call

// 对冲请求（docs/design/2026-10-10-model-fallback-design.md §4.5；默认关闭，RoutingConfig.HedgeDelay > 0 启用）。
//
// 一次 try 发出 HedgeDelay 之后仍未结束、且另有就绪路由时，为该路由另预留一个并发的 try（ReserveTry{Hedge}），
// 向它发出同一逻辑请求。决定性结果是任一腿的 ok，或第一条腿的 fatal；对冲腿的 fatal 不是决定性的（后备供应商的
// Key、模型名或地址配错不应取消健康但较慢的主供应商），此时继续等第一条腿。得出决定性结果后另一条腿以
// errHedgeLost 取消。
//
// 只有最终结果改变调用的状态与结局：其余的腿一律以 Sibling（只记账）结算——预留、账本与 try 行照常（未发出 →
// 释放；已发出 → unknown，按估算全额计入 unknown，即保守计费；已 ok → 按实际费用），但 calls.state、result_ref、
// fail_reason 不变。因此两条腿都 ok、或 fatal 先到 ok 后到时，调用都只以最终结果结束；两次结算之间崩溃时，
// 最终一条腿的预留仍持有，启动账本转换把调用置为 unknown（可重试），而不会留下 failed 或别的结果。
//
// 只用于模型调用（模型调用没有外部副作用，只有费用；从不缓存或合并）。对冲需要空闲的 try 次数、第二份估算的预算
// 与空闲的在途槽位；缺任何一项就不对冲，只等第一条腿。

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
)

// hedgeRun 是一次可能对冲的 try 的进行状态（只在调用方 goroutine 中读写，results 除外）。
type hedgeRun struct {
	results chan legResult
	cancels map[int]context.CancelCauseFunc // try_no → 取消
	routes  []int
	running int
	hedged  bool // 已发出对冲腿
}

// runHedged 发出第一条腿，必要时在 HedgeDelay 之后发出对冲腿；返回由调用方结算的最终结果与用过的路由。
// 其余的腿在返回之前已以 Sibling 结算。
func (c *Coordinator) runHedged(ctx context.Context, j *job, rs *routeSet, first leg, firstTry Try, firstRelease func(),
	tried map[int]bool) (legResult, []int) {
	h := &hedgeRun{results: make(chan legResult, 2), cancels: map[int]context.CancelCauseFunc{}}
	c.startLeg(ctx, j, rs, h, first, firstTry, firstRelease)
	done := c.collectLegs(ctx, j, rs, h, first, tried)
	winner := chooseFinal(done)
	for no, cancel := range h.cancels { // 取消落败方（已结束的腿取消是空操作）
		if no != winner.try.TryNo {
			cancel(errHedgeLost)
		}
	}
	cancelled := map[int]bool{} // 得出决定性结果时仍在进行、随即被取消的腿（落败方）
	for ; h.running > 0; h.running-- {
		r := <-h.results
		cancelled[r.try.TryNo] = decisive(winner)
		done = append(done, r)
	}
	final := chooseFinal(done) // 取消期间到达的 ok 优先于先到的 fatal
	c.finishLegs(j, rs, h, done, final, cancelled)
	return final, h.routes
}

// startLeg 在后台发出一条腿；结束时释放其在途槽位并把结果送入 h.results。
func (c *Coordinator) startLeg(ctx context.Context, j *job, rs *routeSet, h *hedgeRun, l leg, try Try, release func()) {
	lctx, lcancel := context.WithCancelCause(ctx)
	h.cancels[try.TryNo] = lcancel
	h.routes = append(h.routes, l.route)
	h.running++
	go func() {
		begin := c.now()
		resp, uerr, aborted := c.do(lctx, j, rs, l)
		faultinject.Point(faultinject.CallInFlight)
		r := legResult{leg: l, try: try, resp: resp, uerr: uerr, latency: c.now().Sub(begin), aborted: aborted}
		release()
		lcancel(nil)
		h.results <- r
	}()
}

// collectLegs 等到得出决定性结果或所有腿都结束；HedgeDelay 到时若第一条腿仍在进行则尝试发出对冲腿（至多一次）。
func (c *Coordinator) collectLegs(ctx context.Context, j *job, rs *routeSet, h *hedgeRun, first leg, tried map[int]bool) []legResult {
	timer := time.NewTimer(c.routing.HedgeDelay)
	defer timer.Stop()
	var done []legResult
	for {
		select {
		case r := <-h.results:
			h.running--
			done = append(done, r)
			if decisive(r) || h.running == 0 {
				return done
			}
		case <-timer.C:
			if h.hedged || h.running != 1 {
				continue
			}
			h.hedged = true
			if l, try, release, ok := c.startHedge(ctx, j, rs, first, tried); ok {
				c.startLeg(ctx, j, rs, h, l, try, release)
			}
		}
	}
}

// decisive 报告一条腿的结果是否决定调用：ok，或第一条腿（非对冲腿）的 fatal。
func decisive(r legResult) bool {
	switch r.outcome() {
	case upstream.OutcomeOK:
		return true
	case upstream.OutcomeFatal:
		return !r.leg.hedge
	}
	return false
}

// chooseFinal 选最终结果：有 ok 取第一个 ok；否则有第一条腿的 fatal 取它；否则取最后结束的一条腿。
func chooseFinal(done []legResult) legResult {
	for _, r := range done {
		if r.outcome() == upstream.OutcomeOK {
			return r
		}
	}
	for _, r := range done {
		if decisive(r) {
			return r
		}
	}
	return done[len(done)-1]
}

// finishLegs 向熔断器报告每条腿、写日志，并以 Sibling 结算最终结果之外的腿。触发对冲的第一条腿落败时（它已运行
// 超过 HedgeDelay 仍无结果），按失败报告（慢失败），使持续挂起或很慢的主供应商也能被熔断；对冲腿落败只是中止。
func (c *Coordinator) finishLegs(j *job, rs *routeSet, h *hedgeRun, done []legResult, final legResult, cancelled map[int]bool) {
	for _, r := range done {
		isFinal := r.try.TryNo == final.try.TryNo
		lost := !isFinal && cancelled[r.try.TryNo]
		if lost && h.hedged && !r.leg.hedge {
			rs.breakers[r.leg.route].Failure()
		} else {
			rs.report(r.leg, r.uerr, r.aborted)
		}
		c.logTry(j, rs, r)
		if !isFinal {
			c.settleSibling(j, rs, r, lost)
		}
	}
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

// settleSibling 以 Sibling（只记账）结算不是最终结果的一条腿：错误码保留它自己的，落败另记 HedgeLost。
// 已 ok 的腿照常保存结果 blob 并按实际费用结算（费用已发生），但不成为调用的结果；blob 保存失败则按 unknown。
// 结算失败只记日志：预留保持 held，由启动账本转换按 unknown 处理。
func (c *Coordinator) settleSibling(j *job, rs *routeSet, r legResult, lost bool) {
	st := Settlement{Try: r.try, Outcome: string(r.outcome()), LatencyMs: r.latency.Milliseconds(), Sibling: true, HedgeLost: lost}
	if r.uerr == nil {
		ctx, cancel := c.opCtx()
		ref, err := c.blobs.Put(ctx, bytes.NewReader(r.resp.Body))
		cancel()
		if err != nil {
			st.Outcome, st.Error = string(upstream.OutcomeUnknown), CodeBlobWriteFailed
		} else {
			st.ActualMicro, st.ResultSHA256, st.ResultSize = c.costOn(j, rs, r.leg, r.resp.Usage), ref.SHA256, ref.Size
		}
	} else {
		st.Error = r.uerr.Code
	}
	if _, err := c.settle(st); err != nil && !errors.Is(err, context.Canceled) {
		c.log.Warn("gateway: 对冲腿结算失败", "task_id", j.in.TaskID, "call_id", j.in.CallID, "try_no", r.try.TryNo, "err", err)
	}
}
