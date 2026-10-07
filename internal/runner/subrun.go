package runner

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/subrun"
)

// 本文件是 runner 对 sub-run 扩展（规格 §13、§5.4 sub-run 扩展；M4 Plan 14 Task 6）的处理：subrun_start 的持久化与
// 答复、subrun_end / subrun_cancel 的提议、deadline 定时器与 T_subrun_cancel。task 模式与 session 模式共用（processor）。
// 事件流规则（协商、未知 ID）由 protocol 包的 WorkerStream / SessionStream 检查，违规码原样成为 Outcome.Violation。

// SubrunHost 是 runner 需要的 sub-run 持久化与 Gateway 取消（消费者窄接口；postgres Store 与
// call.Coordinator.CancelSubrun 组合实现，装配在 internal/app）。
type SubrunHost interface {
	Start(ctx context.Context, taskID, attemptID, subrunID string, d subrun.Definition) (subrun.Record, error)
	ProposeEnd(ctx context.Context, taskID, attemptID, subrunID, status, summary string) (subrun.Record, error)
	RequestCancel(ctx context.Context, taskID, subrunID, reason string) (subrun.Record, error)
	List(ctx context.Context, taskID string) ([]subrun.Record, error)
	// CancelGateway 取消该 sub-run 的在途 Gateway 调用（call.Coordinator.CancelSubrun）。
	CancelGateway(taskID, attemptID, subrunID string)
}

// sub-run 扩展的违规码（与 protocol 包的事件流错误码相同）与终止原因。
const (
	ViolationExtensionMismatch      = protocol.CodeExtensionMismatch
	ViolationExtensionNotNegotiated = protocol.CodeExtensionNotNegotiated
	ViolationSubrunUnknown          = protocol.CodeSubrunUnknown
	// KillSubrunCancelTimeout：T_subrun_cancel 到期仍未收到该 sub-run 的终态 subrun_end，或 subrun_cancel_requested
	// 无法送达（§5.5 第 8 条的升级终止）。分类为 subrun_cancel_timeout（故障重试，从 checkpoint 恢复）。
	KillSubrunCancelTimeout = "subrun_cancel_timeout"
)

var errNoSubrunHost = errors.New("runner: 请求了 subruns 扩展却没有提供 SubrunHost")

// requestsSubruns 判断 extensions 是否请求了 sub-run 扩展（与 protocol 的协商判定一致）。
func requestsSubruns(exts []string) bool { return slices.Contains(exts, protocol.ExtensionSubruns) }

// subrunRetryDelay 是 deadline 到期时 RequestCancel 暂时失败后的重试间隔（Store 故障阈值仍由 noteStore 执行）。
const subrunRetryDelay = time.Second

type subrunTimerKind int

const (
	timerDeadline subrunTimerKind = iota + 1 // deadline_at 到期：宿主发起取消
	timerCancel                              // T_subrun_cancel：等 Worker 的 subrun_end
)

// subrunFire 是一次定时器到期；gen 与 subrunState.gen 不同的为已撤销的旧定时器。
type subrunFire struct {
	id   string
	kind subrunTimerKind
	gen  uint64
}

// subrunState 是一个 sub-run 在本 attempt 中的宿主侧状态（处理器 goroutine 独占）。
type subrunState struct {
	timer      *time.Timer
	kind       subrunTimerKind // 当前定时器（0：无）
	gen        uint64
	cancelWait bool // 已发起取消，T_subrun_cancel 在等 subrun_end{cancelled|failed}
	ended      bool // Worker 已报告终态，或宿主记录为终态：不再计时
}

// subrunTracker 是一个 attempt 的 sub-run 状态与定时器。除 stop 外只由处理器 goroutine 访问；定时器经 fired 回到
// 处理器（与 Worker 事件串行处理）。stop 之后（终态提议、违规或 attempt 结束）不再处理任何到期。
type subrunTracker struct {
	host          SubrunHost
	cancelTimeout time.Duration
	subs          map[string]*subrunState
	gen           uint64
	fired         chan subrunFire
	stopCh        chan struct{}
	stopOnce      sync.Once
}

func newSubrunTracker(host SubrunHost, cancelTimeout time.Duration) *subrunTracker {
	return &subrunTracker{host: host, cancelTimeout: cancelTimeout, subs: map[string]*subrunState{},
		fired: make(chan subrunFire, 2*protocol.MaxSubrunsPerTask), stopCh: make(chan struct{})}
}

// stop 停止处理定时器（任意 goroutine 可调用）：之后到期的定时器不再送达，已送达的被忽略。
func (t *subrunTracker) stop() { t.stopOnce.Do(func() { close(t.stopCh) }) }

func (t *subrunTracker) stopped() bool { return isClosed(t.stopCh) }

// stopTimers 停止全部定时器并 stop（处理器 goroutine）。
func (t *subrunTracker) stopTimers() {
	t.stop()
	for _, st := range t.subs {
		t.disarm(st)
	}
}

func (t *subrunTracker) state(id string) *subrunState {
	st := t.subs[id]
	if st == nil {
		st = &subrunState{}
		t.subs[id] = st
	}
	return st
}

func (t *subrunTracker) arm(id string, st *subrunState, kind subrunTimerKind, d time.Duration) {
	t.disarm(st)
	if t.stopped() {
		return
	}
	t.gen++
	st.gen, st.kind = t.gen, kind
	f := subrunFire{id: id, kind: kind, gen: st.gen}
	st.timer = time.AfterFunc(max(d, 0), func() {
		select {
		case t.fired <- f:
		case <-t.stopCh:
		}
	})
}

func (t *subrunTracker) disarm(st *subrunState) {
	if st.timer != nil {
		st.timer.Stop()
	}
	st.timer, st.kind = nil, 0
	t.gen++
	st.gen = t.gen
}

// end 记录 sub-run 已结束：停止全部计时。
func (t *subrunTracker) end(st *subrunState) {
	t.disarm(st)
	st.ended, st.cancelWait = true, false
}

// ---- 事件处理（处理器 goroutine）----

// handleSubrun 处理一条 subrun_start / subrun_end / subrun_cancel（调用方已 flush）。返回非空违规码时调用方按违规处理。
func (at *processor) handleSubrun(ctx context.Context, m protocol.Message) (violation string) {
	if at.subs == nil { // 事件流已保证协商；未协商时不会到达这里
		return ViolationExtensionNotNegotiated
	}
	switch m := m.(type) {
	case *protocol.SubrunStart:
		at.send(at.subrunStart(ctx, m))
	case *protocol.SubrunEnd:
		return at.subrunEnd(ctx, m)
	case *protocol.SubrunCancel:
		return at.subrunCancel(ctx, m)
	}
	return ""
}

// subrunStart 持久化 subrun_start 并答复：成功 → started，登记 deadline 定时器（剩余时间按宿主时钟由记录的
// deadline_at 换算，不超过 deadline_ms）；前置条件不满足 → rejected 带原因码；参数不合法 → invalid_field；Store
// 暂时故障 → retryable_error（SDK 以同一定义重试，计入 Store 故障阈值）。
func (at *processor) subrunStart(ctx context.Context, m *protocol.SubrunStart) *protocol.SubrunStarted {
	res := &protocol.SubrunStarted{HostHeader: hostHeader(protocol.TypeSubrunStarted), SubrunID: m.SubrunID}
	d := subrun.Definition{ParentStepID: m.ParentStepID, BudgetCapMicro: m.BudgetCapMicro, DeadlineMS: m.DeadlineMS}
	rec, err := at.subs.host.Start(ctx, at.taskID, at.attemptID, m.SubrunID, d)
	at.noteStore(err)
	var rej *persistence.RejectedError
	switch {
	case err == nil:
		res.Status = protocol.SubrunStatusStarted
	case errors.As(err, &rej):
		res.Status, res.Code = protocol.SubrunStatusRejected, rej.Code
		return res
	case errors.Is(err, persistence.ErrInvalid):
		res.Status, res.Code = protocol.SubrunStatusRejected, protocol.SubrunRejectInvalidField
		return res
	default:
		res.Status, res.Code = protocol.SubrunStatusRejected, protocol.SubrunRejectRetryableError
		return res
	}
	st := at.subs.state(m.SubrunID)
	if (rec.Status == subrun.Started || rec.Status == subrun.EndProposed) && st.kind == 0 && !st.cancelWait && !st.ended {
		limit := time.Duration(m.DeadlineMS) * time.Millisecond
		at.subs.arm(m.SubrunID, st, timerDeadline, min(time.Until(rec.DeadlineAt), limit))
	}
	return res
}

// subrunEnd 提议结束。Worker 报告 failed / cancelled 即停止该 sub-run 的计时（满足 T_subrun_cancel）；succeeded 只是
// 提议，取消等待中不停止计时。宿主已请求取消后的 succeeded（P14-T2 的竞争）得到 invalid_transition，不是协议违规。
// 宿主没有该 sub-run 的任何记录 → subrun_unknown。
func (at *processor) subrunEnd(ctx context.Context, m *protocol.SubrunEnd) string {
	st := at.subs.state(m.SubrunID)
	if m.Status != protocol.SubrunEndSucceeded {
		at.subs.end(st)
	}
	rec, err := at.subs.host.ProposeEnd(ctx, at.taskID, at.attemptID, m.SubrunID, m.Status, m.Summary)
	if !errors.Is(err, persistence.ErrNotFound) {
		at.noteStore(err)
	}
	switch {
	case err == nil:
		if rec.Status.Terminal() {
			at.subs.end(st)
		}
	case errors.Is(err, persistence.ErrNotFound):
		return ViolationSubrunUnknown
	}
	// 其余（invalid_transition、subrun_closed、暂时故障）：提议未生效；状态由之后的 checkpoint 与裁决收尾
	return ""
}

// subrunCancel：编排层放弃（reason = orchestrator）→ CancelGateway → T_subrun_cancel；不回发 subrun_cancel_requested。
func (at *processor) subrunCancel(ctx context.Context, m *protocol.SubrunCancel) string {
	rec, err := at.subs.host.RequestCancel(ctx, at.taskID, m.SubrunID, subrun.ReasonOrchestrator)
	if !errors.Is(err, persistence.ErrNotFound) {
		at.noteStore(err)
	}
	st := at.subs.state(m.SubrunID)
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return ViolationSubrunUnknown
	case err == nil && rec.Status.Terminal():
		at.subs.end(st)
		return ""
	case errors.Is(err, persistence.ErrRejected) || st.ended:
		return ""
	}
	// 暂时故障时取消未记录，仍尽力取消在途调用并等 Worker 结束它
	at.subs.host.CancelGateway(at.taskID, at.attemptID, m.SubrunID)
	if !st.cancelWait {
		st.cancelWait = true
		at.subs.arm(m.SubrunID, st, timerCancel, at.subs.cancelTimeout)
	}
	return ""
}

// subrunTimer 处理一次定时器到期（处理器 goroutine；ctx 是处理器的 ctx）。
func (at *processor) subrunTimer(ctx context.Context, f subrunFire) {
	t := at.subs
	if t == nil || t.stopped() {
		return
	}
	st := t.subs[f.id]
	if st == nil || st.gen != f.gen || st.kind != f.kind {
		return // 已撤销
	}
	st.timer, st.kind = nil, 0
	switch f.kind {
	case timerCancel:
		if st.cancelWait {
			at.killFn(KillSubrunCancelTimeout)
		}
	case timerDeadline:
		at.subrunDeadline(ctx, f.id, st)
	}
}

// subrunDeadline：deadline 到期 → RequestCancel(deadline) → CancelGateway → subrun_cancel_requested{deadline}（无法送达
// 则升级终止，§5.5 第 8 条）→ T_subrun_cancel。
func (at *processor) subrunDeadline(ctx context.Context, id string, st *subrunState) {
	t := at.subs
	if st.ended || st.cancelWait {
		return
	}
	rec, err := t.host.RequestCancel(ctx, at.taskID, id, subrun.ReasonDeadline)
	if !errors.Is(err, persistence.ErrNotFound) {
		at.noteStore(err)
	}
	switch {
	case err == nil && rec.Status.Terminal():
		t.end(st)
		return
	case err != nil && (errors.Is(err, persistence.ErrNotFound) || errors.Is(err, persistence.ErrRejected) || errors.Is(err, persistence.ErrInvalid)):
		return
	case err != nil: // 暂时故障：稍后重试（持续失败由 Store 故障阈值终止）
		t.arm(id, st, timerDeadline, subrunRetryDelay)
		return
	}
	t.host.CancelGateway(at.taskID, at.attemptID, id)
	msg := &protocol.SubrunCancelRequested{HostHeader: hostHeader(protocol.TypeSubrunCancelRequested), SubrunID: id,
		Reason: protocol.SubrunCancelDeadline}
	if !at.ctlFn(msg) {
		at.killFn(KillSubrunCancelTimeout)
		return
	}
	st.cancelWait = true
	t.arm(id, st, timerCancel, t.cancelTimeout)
}

// stopSubruns 在终态提议、违规或 attempt 结束时停止全部 sub-run 计时（处理器 goroutine）：提议之后 Worker 不再发送
// subrun_end（P14-T8），T_subrun_cancel 不再适用。
func (at *processor) stopSubruns() {
	if at.subs != nil {
		at.subs.stopTimers()
	}
}

// subrunFired 返回定时器到期的通道；未协商时为 nil（不参与 select）。
func (at *processor) subrunFired() <-chan subrunFire {
	if at.subs == nil {
		return nil
	}
	return at.subs.fired
}
