package session

// 本文件是 session actor（规格 §12；M4 Plan 12 Task 7）：每个会话一个 select 循环，只作决策、提交状态、安排操作。
// actor 以 Decide 计算下一步，同一时刻至多一个操作在独立 goroutine 中执行，结果（含操作后重新读取的会话事实）回到
// 收件箱。物理操作经窄接口交给 coordinator/provider（Env）、runner（Workers、IncarnationHandle）、Gateway 入口与
// admission，持久化只经 Store 用例。session 不导入 runner、gateway、provider/local 与数据库驱动：装配代码（Task 9）
// 把各包的实现适配到这里声明的接口。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
)

// ReloadInterval 是 actor 周期重新读取会话事实的间隔：没有通知的变化（另一 turn 的控制应用清除了阻塞、turn 终态
// 之后的关闭条件）由它发现。
const ReloadInterval = 10 * time.Second

// ---- 窄接口 ----

// Config 是会话生命周期参数。零值字段取默认值（Default*）。
type Config struct {
	IdleFreeze        time.Duration // --session-idle-freeze，默认 10 min
	EvictAfter        time.Duration // --session-evict-after，默认 1 h（自 idle_since 起算；须 > IdleFreeze）
	FreezeConfirm     time.Duration // 默认 5 s
	QuiesceGrace      time.Duration // 默认 10 s
	CloseGrace        time.Duration // 默认 10 s
	IncarnationMemory int64         // incarnation 环境内存（admission MemoryOnly）
}

func (c Config) withDefaults() Config {
	if c.IdleFreeze <= 0 {
		c.IdleFreeze = DefaultIdleFreeze
	}
	if c.EvictAfter <= 0 {
		c.EvictAfter = DefaultEvictAfter
	}
	if c.FreezeConfirm <= 0 {
		c.FreezeConfirm = DefaultFreezeConfirm
	}
	if c.QuiesceGrace <= 0 {
		c.QuiesceGrace = DefaultQuiesceGrace
	}
	if c.CloseGrace <= 0 {
		c.CloseGrace = DefaultCloseGrace
	}
	return c
}

// Validate 校验配置（装配时调用）：EvictAfter 须大于 IdleFreeze。
func (c Config) Validate() error {
	c = c.withDefaults()
	if c.EvictAfter <= c.IdleFreeze {
		return fmt.Errorf("session: --session-evict-after（%s）须大于 --session-idle-freeze（%s）", c.EvictAfter, c.IdleFreeze)
	}
	return nil
}

// Env 是 coordinator/provider 的会话子集（app 适配）。
type Env interface {
	// CreateIncarnationEnv：workspace = <data>/sessions/<session_id>/workspace（不存在则建），UIDOwner = "session:<id>"，
	// RestoreDir 非空时挂载，GatewaySocket = socket。
	CreateIncarnationEnv(ctx context.Context, s IncarnationEnv) error
	FreezeEnv(ctx context.Context, envID string) error
	ThawEnv(ctx context.Context, envID string) error
	// StopEnv 停止环境并记录 stopped_at；对创建从未开始的环境（CreateIncarnation 已登记、CreateIncarnationEnv 未调用
	// 或失败）同样须记录并报告 Recorded，否则该 incarnation 无法结束。
	StopEnv(ctx context.Context, envID string) (StopReport, error)
	// StageRestore 校验 sha 已完整保存并授权到会话 scope，复制到 <env>/restore/<sha> 并返回宿主目录；UnstageRestore
	// 删除暂存文件（幂等；没有暂存时成功）。
	StageRestore(ctx context.Context, sessionID, envID, sha string) (dir string, err error)
	UnstageRestore(ctx context.Context, envID string) error
	DeleteWorkspace(ctx context.Context, sessionID string) error
	ReleaseUID(ctx context.Context, sessionID string) error
}

// IncarnationEnv 是创建 incarnation 环境的输入。
type IncarnationEnv struct {
	SessionID, EnvID, GatewaySocket, RestoreDir string
	MemoryBytes                                 int64
}

// StopReport 是停止报告的事实。**Recorded 为真表示执行已确认结束**：环境停止且 stopped_at 已记录；Handoff 中
// Release 成功（incarnation 回到 idle）同样报告 Recorded（task actor 据此归还 run slot、允许替代执行）。
type StopReport struct{ Stopped, Recorded, Blocked bool }

// Workers 是 runner 的会话子集。
type Workers interface {
	// Start 启动 incarnation 的 Worker 并完成握手（init(session) → ready）；返回之后 incarnation 可接受 task_start。
	Start(ctx context.Context, s WorkerStart) (IncarnationHandle, error)
}

// WorkerStart 是启动 incarnation 的输入。
type WorkerStart struct {
	SessionID, IncarnationID, EnvID string
	Resume                          *ResumeState // nil = 新会话
}

// ResumeState 是冷恢复的 session 状态（init.session_resume）：State 与 StagedPath 恰有一个。
type ResumeState struct {
	CheckpointID string
	State        json.RawMessage // inline 时
	StagedPath   string          // state_ref 时：/run/agentbox/restore/<sha>
	Refs         []string
}

// IncarnationHandle 是存活 incarnation 的句柄（runner.Incarnation 的适配）。任务书原名 Incarnation，与 Task 3 的
// incarnation 行类型（store.go 的 Incarnation）同名，故改名。
type IncarnationHandle interface {
	Release(ctx context.Context, attemptID, verdict, committedSessionCheckpointID string) (released bool, reason string)
	Quiesce(ctx context.Context, grace time.Duration) (string, error)
	Close(ctx context.Context, grace time.Duration) error
	Exited() <-chan struct{}
}

// Gateway 是 incarnation 的 Gateway 入口（gateway/edge 的适配）。
type Gateway interface {
	BindIncarnation(ctx context.Context, incarnationID, envID string) (string, error)
	RevokeIncarnation(ctx context.Context, incarnationID string) error
}

// Admission 是 incarnation 内存的准入（admission.Request{MemoryOnly} 的适配）。AcquireMemory 阻塞直到授予或 ctx 结束；
// Release 幂等且不阻塞。
type Admission interface {
	AcquireMemory(ctx context.Context, sessionID string, bytes int64) (Grant, error)
	Release(g Grant)
}

// Grant 是一份内存授予。
type Grant struct{ ID uint64 }

// Clock 是 actor 的时间来源（与 task.Clock 相同的语义）。
type Clock interface {
	Now() time.Time
	At(t time.Time) (c <-chan time.Time, stop func())
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) At(t time.Time) (<-chan time.Time, func()) {
	tm := time.NewTimer(time.Until(t))
	return tm.C, func() { tm.Stop() }
}

// Deps 是 actor 的依赖。Clock、IDs 为空时取系统时钟与随机 ID。
type Deps struct {
	Store     Store
	Env       Env
	Workers   Workers
	Gateway   Gateway
	Admission Admission
	Clock     Clock // 同 task.Clock 语义
	IDs       func() string
	Config    Config
	Notify    func(taskID string) // 授予或拒绝后通知 task Scheduler（可选）
	OnFatal   func(sessionID string, err error)
}

func (d Deps) withDefaults() Deps {
	if d.Clock == nil {
		d.Clock = systemClock{}
	}
	if d.IDs == nil {
		d.IDs = randomID
	}
	d.Config = d.Config.withDefaults()
	return d
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// GrantInfo 是授予给 turn 的 incarnation。
type GrantInfo struct{ IncarnationID, EnvID string }

// Handoff 是 turn 交还 incarnation：Destroy=false → Release（失败则销毁并记 release_timeout）；Destroy=true（故障、
// 取消生效）→ 撤销入口、停止环境、结束 incarnation。Verdict 是 task_outcome.verdict（succeeded|failed|cancelled|
// paused）。
type Handoff struct {
	TaskID, AttemptID, EnvID, Verdict, CommittedSessionCheckpointID string
	Destroy                                                         bool
}

// ErrUnavailable 表示会话已关闭（或正在关闭），或恢复两次失败。
var ErrUnavailable = errors.New("session: 会话暂时无法恢复或已关闭")

// ErrStopped 表示 Scheduler 已停止或会话的 actor 因致命错误退出：不是会话本身不可用（调用方稍后重试，不应让 turn 失败）。
var ErrStopped = errors.New("session: session actor 已停止")

// ---- Actor ----

type grantReply struct {
	info GrantInfo
	err  error
}

type grantReq struct {
	taskID string
	reply  chan grantReply
}

type withdrawReq struct {
	taskID string
	reply  chan grantReply
}

type handoffReq struct {
	h     Handoff
	reply chan StopReport
}

// opResult 是一个操作在 goroutine 中执行的结果：done 交给 Decide，其余是 actor 本地资源的变化。
type opResult struct {
	done        OpDone
	handle      IncarnationHandle // 新 incarnation 的句柄（OpStart 成功）
	handleEnv   string
	clearHandle bool
	mem         *Grant
	memSet      bool
}

type loadResult struct {
	st  State
	err error
}

type panicked struct{ err error }

// Actor 驱动一个会话。全部状态只由 actor goroutine 访问；外部只经 Scheduler 交互。
type Actor struct {
	id     string
	d      Deps
	sch    *Scheduler
	ctx    context.Context
	cancel context.CancelFunc
	reqs   chan any // grantReq、withdrawReq、handoffReq（无缓冲：送达即被处理）
	inbox  chan any // 操作结果
	notify chan struct{}
	press  chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
	fatal  error

	s        ActorState
	grants   map[string][]chan grantReply
	handoffs map[Handoff][]chan StopReport
	handle   IncarnationHandle
	handleID string
	exitC    <-chan struct{}
	mem      *Grant
	exit     bool

	wakes     []time.Time
	reloadAt  time.Time
	timerAt   time.Time
	timerC    <-chan time.Time
	timerStop func()
}

func spawn(ctx context.Context, sessionID string, d Deps, sch *Scheduler) *Actor {
	ctx, cancel := context.WithCancel(ctx)
	a := &Actor{id: sessionID, d: d, sch: sch, ctx: ctx, cancel: cancel, reqs: make(chan any), inbox: make(chan any, 4),
		notify: make(chan struct{}, 1), press: make(chan struct{}, 1), done: make(chan struct{}),
		grants: map[string][]chan grantReply{}, handoffs: map[Handoff][]chan StopReport{}}
	a.s = ActorState{Config: d.Config, Session: State{SessionID: sessionID}}
	go a.main()
	return a
}

// Done 在 actor 退出且其 goroutine 全部返回后关闭。
func (a *Actor) Done() <-chan struct{} { return a.done }

// Err 返回致命错误；Done 关闭之前调用结果未定义。
func (a *Actor) Err() error {
	select {
	case <-a.done:
		return a.fatal
	default:
		return nil
	}
}

func (a *Actor) poke() {
	select {
	case a.notify <- struct{}{}:
	default:
	}
}

func (a *Actor) pressure() {
	select {
	case a.press <- struct{}{}:
	default:
	}
}

func (a *Actor) main() {
	defer close(a.done)
	defer func() {
		a.cancel()
		if a.timerStop != nil {
			a.timerStop()
		}
		a.wg.Wait()
		a.sch.setIncarnation(a, "", nil)
	}()
	defer func() {
		if r := recover(); r != nil {
			a.die(fmt.Errorf("session: 会话 %s 的 actor panic: %v", a.id, r))
		}
	}()
	a.reloadAt = a.d.Clock.Now().Add(ReloadInterval)
	a.apply(Tick{})
	for a.fatal == nil && !a.exit {
		a.arm()
		select {
		case <-a.ctx.Done():
			return
		case m := <-a.reqs:
			a.request(m)
		case <-a.notify:
			a.apply(Notified{})
		case <-a.press:
			a.apply(PressureEvict{})
		case m := <-a.inbox:
			a.result(m)
		case <-a.exitC:
			a.exitC = nil
			a.apply(IncarnationExited{IncarnationID: a.handleID})
		case <-a.timerC:
			a.timerC = nil
			a.fire()
		}
	}
}

func (a *Actor) die(err error) {
	if a.fatal != nil {
		return
	}
	a.fatal = err
	if a.d.OnFatal != nil {
		a.d.OnFatal(a.id, err)
	}
	for _, chs := range a.grants { // 申请者不再等待一个不会答复的 actor（不是 ErrUnavailable：turn 稍后重试）
		for _, ch := range chs {
			ch <- grantReply{err: fmt.Errorf("%w: %w", ErrStopped, err)}
		}
	}
	a.grants = map[string][]chan grantReply{}
}

func (a *Actor) request(m any) {
	switch r := m.(type) {
	case grantReq:
		a.grants[r.taskID] = append(a.grants[r.taskID], r.reply)
		a.apply(GrantRequested{TaskID: r.taskID})
	case withdrawReq:
		a.grants[r.taskID] = slices.DeleteFunc(a.grants[r.taskID], func(c chan grantReply) bool { return c == r.reply })
		if len(a.grants[r.taskID]) == 0 {
			delete(a.grants, r.taskID)
			a.apply(GrantWithdrawn{TaskID: r.taskID})
		}
	case handoffReq:
		a.handoffs[r.h] = append(a.handoffs[r.h], r.reply)
		if len(a.handoffs[r.h]) == 1 {
			a.apply(HandoffRequested{Handoff: r.h})
		}
	}
}

// ---- 计时 ----

func (a *Actor) arm() {
	next := a.reloadAt
	for _, w := range a.wakes {
		if w.Before(next) {
			next = w
		}
	}
	if a.timerC != nil && next.Equal(a.timerAt) {
		return
	}
	if a.timerStop != nil {
		a.timerStop()
	}
	a.timerAt = next
	a.timerC, a.timerStop = a.d.Clock.At(next)
}

func (a *Actor) fire() {
	now := a.d.Clock.Now()
	if !now.Before(a.reloadAt) {
		a.reloadAt = now.Add(ReloadInterval)
		a.apply(Notified{})
	}
	due := false
	a.wakes = slices.DeleteFunc(a.wakes, func(w time.Time) bool {
		if now.Before(w) {
			return false
		}
		due = true
		return true
	})
	if due {
		a.apply(Tick{})
	}
}

// ---- 决策 ----

func (a *Actor) apply(e Event) {
	if a.fatal != nil || a.exit {
		return
	}
	a.s.Now = a.d.Clock.Now()
	dec := Decide(a.s, e)
	if dec.Err != nil {
		a.die(fmt.Errorf("session: 会话 %s 的决策（%T）失败: %w", a.id, e, dec.Err))
		return
	}
	a.s = dec.Next
	for _, eff := range dec.Effects {
		a.exec(eff)
	}
}

func (a *Actor) exec(eff Effect) {
	switch f := eff.(type) {
	case ReplyGrant:
		for _, ch := range a.grants[f.TaskID] {
			ch <- grantReply{info: f.Info, err: f.Err}
		}
		delete(a.grants, f.TaskID)
		if a.d.Notify != nil {
			a.d.Notify(f.TaskID)
		}
	case ReplyHandoff:
		for _, ch := range a.handoffs[f.Handoff] {
			ch <- f.Report
		}
		delete(a.handoffs, f.Handoff)
	case WakeAt:
		if !slices.ContainsFunc(a.wakes, f.Time.Equal) {
			a.wakes = append(a.wakes, f.Time)
		}
	case Exit:
		a.exit = true
	case Load:
		a.async(func(ctx context.Context) any {
			st, err := a.d.Store.LoadSession(ctx, a.id)
			return loadResult{st: st, err: err}
		})
	default:
		handle, mem, handleID := a.handle, a.mem, a.handleID
		a.async(func(ctx context.Context) any { return a.runOp(ctx, eff, handle, handleID, mem) })
	}
}

func (a *Actor) async(f func(ctx context.Context) any) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		var v any
		func() {
			defer func() {
				if r := recover(); r != nil {
					v = panicked{err: fmt.Errorf("session: 会话 %s 的操作 panic: %v", a.id, r)}
				}
			}()
			v = f(a.ctx)
		}()
		select {
		case a.inbox <- v:
		case <-a.ctx.Done():
		}
	}()
}

func (a *Actor) result(m any) {
	switch r := m.(type) {
	case loadResult:
		switch {
		case r.err == nil:
			a.apply(Loaded{State: r.st})
		case errors.Is(r.err, persistence.ErrOwnershipLost):
			a.die(r.err)
		case a.ctx.Err() == nil:
			a.apply(LoadFailed{Err: r.err, NotFound: errors.Is(r.err, persistence.ErrNotFound)})
		}
	case opResult:
		if r.memSet {
			a.mem = r.mem
		}
		if r.clearHandle {
			a.handle, a.handleID, a.exitC = nil, "", nil
			a.sch.setIncarnation(a, "", nil)
		}
		if r.handle != nil {
			a.handle, a.handleID, a.exitC = r.handle, r.done.IncarnationID, r.handle.Exited()
			a.sch.setIncarnation(a, r.handleEnv, r.handle)
		}
		if err := r.done.Err; err != nil && (errors.Is(err, persistence.ErrOwnershipLost) || errors.Is(err, persistence.ErrInvalid)) {
			a.die(fmt.Errorf("session: 会话 %s 的操作 %s: %w", a.id, r.done.Op, err))
			return
		}
		if a.ctx.Err() != nil {
			return
		}
		a.apply(r.done)
	case panicked:
		a.die(r.err)
	}
}

// ---- 操作 ----

func (a *Actor) runOp(ctx context.Context, eff Effect, handle IncarnationHandle, handleID string, mem *Grant) opResult {
	var r opResult
	switch f := eff.(type) {
	case DoTransition:
		r.done = OpDone{Op: OpTransition, To: f.T.To}
		_, err := retry(ctx, a.d, func(ctx context.Context) (State, error) { return a.d.Store.Transition(ctx, f.T) })
		a.storeErr(&r.done, err)
	case StartIncarnation:
		r = a.opStart(ctx, f, mem)
	case QuiesceIncarnation:
		r.done = OpDone{Op: OpQuiesce}
		if a.setInc(ctx, &r.done, f.IncarnationID, []string{IncIdle}, IncQuiescing) {
			switch id, err := quiesce(ctx, handle, handleID, f.IncarnationID, a.d.Config.QuiesceGrace); {
			case err != nil:
				r.done.Failed = err
			case id != f.Expect:
				r.done.Failed = fmt.Errorf("quiesced 报告 checkpoint %q，最新已提交为 %q", id, f.Expect)
			default:
				r.done.CheckpointID = id
			}
		}
	case FreezeIncarnation:
		r.done = OpDone{Op: OpFreeze}
		fctx, cancel := context.WithTimeout(ctx, a.d.Config.FreezeConfirm)
		err := a.d.Env.FreezeEnv(fctx, f.EnvID)
		cancel()
		if err != nil {
			r.done.Failed = fmt.Errorf("冻结环境 %s: %w", f.EnvID, err)
		} else {
			a.setInc(ctx, &r.done, f.IncarnationID, []string{IncQuiescing}, IncFrozen)
		}
	case ThawIncarnation:
		r.done = OpDone{Op: OpThaw}
		if err := a.d.Env.ThawEnv(ctx, f.EnvID); err != nil {
			r.done.Failed = fmt.Errorf("解冻环境 %s: %w", f.EnvID, err)
		} else {
			a.setInc(ctx, &r.done, f.IncarnationID, []string{IncFrozen}, IncIdle)
		}
	case ReleaseIncarnation:
		r.done = OpDone{Op: OpRelease}
		h := f.Handoff
		if handle == nil {
			r.done.Failed = errors.New("没有 incarnation 句柄")
			break
		}
		if ok, reason := handle.Release(ctx, h.AttemptID, h.Verdict, h.CommittedSessionCheckpointID); !ok {
			r.done.Failed = fmt.Errorf("释放 attempt %s 失败: %s", h.AttemptID, reason)
			break
		}
		a.setInc(ctx, &r.done, handleID, []string{IncBusy, IncReleasing}, IncIdle)
	case CloseIncarnation:
		r.done = OpDone{Op: OpClose}
		if handle != nil && handleID == f.IncarnationID {
			_ = handle.Close(ctx, a.d.Config.CloseGrace) // 失败不影响随后的销毁（StopEnv 终止进程）
		}
	case DestroyIncarnation:
		r = a.opDestroy(ctx, f, handleID, mem)
	case FinishCloseOp:
		r.done = OpDone{Op: OpFinishClose}
		// E32：workspace 只在全部环境停止之后删除（Decide 只在没有存活 incarnation 时发出本操作；销毁在 StopEnv
		// Recorded 之后才结束 incarnation）。
		if err := a.d.Env.DeleteWorkspace(ctx, a.id); err != nil {
			r.done.Err = fmt.Errorf("删除会话 %s 的 workspace: %w", a.id, err)
			break
		}
		if err := a.d.Env.ReleaseUID(ctx, a.id); err != nil {
			r.done.Err = fmt.Errorf("释放会话 %s 的 UID 范围: %w", a.id, err)
			break
		}
		_, err := retry(ctx, a.d, func(ctx context.Context) (State, error) { return a.d.Store.FinishClose(ctx, a.id, f.FromRowVersion) })
		a.storeErr(&r.done, err)
	default:
		r.done.Err = fmt.Errorf("%w: 未知副作用 %T", persistence.ErrInvalid, eff)
		return r
	}
	a.reload(ctx, &r.done)
	return r
}

func quiesce(ctx context.Context, handle IncarnationHandle, handleID, incID string, grace time.Duration) (string, error) {
	if handle == nil || handleID != incID {
		return "", errors.New("没有 incarnation 句柄")
	}
	qctx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	return handle.Quiesce(qctx, grace)
}

// opStart 新建 incarnation（见 StartIncarnation）。
func (a *Actor) opStart(ctx context.Context, f StartIncarnation, mem *Grant) opResult {
	r := opResult{done: OpDone{Op: OpStart}}
	if mem == nil {
		g, err := a.d.Admission.AcquireMemory(ctx, a.id, a.d.Config.IncarnationMemory)
		if err != nil {
			r.done.Failed = fmt.Errorf("申请 incarnation 内存: %w", err)
			return r
		}
		mem = &g
		r.mem, r.memSet = mem, true
	}
	releaseMem := func() {
		a.d.Admission.Release(*mem)
		r.mem, r.memSet = nil, true
	}
	incID, envID := a.d.IDs(), a.d.IDs()
	_, err := retry(ctx, a.d, func(ctx context.Context) (Incarnation, error) {
		inc, err := a.d.Store.CreateIncarnation(ctx, NewIncarnation{IncarnationID: incID, SessionID: a.id, EnvID: envID,
			FromRowVersion: f.FromRowVersion})
		return inc, err
	})
	if err != nil {
		releaseMem()
		a.storeErr(&r.done, err)
		return r
	}
	staged := false
	cleanup := func(cause error) opResult {
		_ = a.d.Gateway.RevokeIncarnation(ctx, incID)
		if staged {
			_ = a.d.Env.UnstageRestore(ctx, envID)
		}
		// 环境确认停止之后才结束 incarnation；否则它保持存活，由下一次决策按 EndOrphaned 重试销毁（内存随之归还）。
		if rep, err := a.d.Env.StopEnv(ctx, envID); err == nil && rep.Recorded {
			if _, err := retry(ctx, a.d, func(ctx context.Context) (Incarnation, error) {
				inc, err := a.d.Store.EndIncarnation(ctx, incID, EndStartFailed)
				return inc, err
			}); err == nil {
				releaseMem()
			}
		}
		r.done.Failed = cause
		return r
	}
	sock, err := a.d.Gateway.BindIncarnation(ctx, incID, envID)
	if err != nil {
		return cleanup(fmt.Errorf("绑定 incarnation %s 的 Gateway 入口: %w", incID, err))
	}
	var resume *ResumeState
	restoreDir := ""
	if cp := f.Resume; cp != nil {
		resume = &ResumeState{CheckpointID: cp.CheckpointID, State: cp.State, Refs: cp.Refs}
		if cp.StateRef != "" { // §12.2 冷恢复的 state 读取：宿主暂存到只读路径，不交给 Worker state_ref
			dir, err := a.d.Env.StageRestore(ctx, a.id, envID, cp.StateRef)
			if err != nil {
				return cleanup(fmt.Errorf("暂存 session state %s: %w", cp.StateRef, err))
			}
			staged, restoreDir = true, dir
			resume.State, resume.StagedPath = nil, protocol.StagedStateDir+cp.StateRef
		}
	}
	if err := a.d.Env.CreateIncarnationEnv(ctx, IncarnationEnv{SessionID: a.id, EnvID: envID, GatewaySocket: sock,
		RestoreDir: restoreDir, MemoryBytes: a.d.Config.IncarnationMemory}); err != nil {
		return cleanup(fmt.Errorf("创建 incarnation 环境 %s: %w", envID, err))
	}
	inc, err := a.d.Workers.Start(ctx, WorkerStart{SessionID: a.id, IncarnationID: incID, EnvID: envID, Resume: resume})
	if err != nil {
		return cleanup(fmt.Errorf("启动 incarnation %s: %w", incID, err))
	}
	if staged { // 暂存文件在 incarnation ready 之后卸载（§12.2）
		_ = a.d.Env.UnstageRestore(ctx, envID)
		staged = false
	}
	if _, err := retry(ctx, a.d, func(ctx context.Context) (Incarnation, error) {
		row, err := a.d.Store.SetIncarnationStatus(ctx, incID, []string{IncStarting}, IncIdle)
		return row, err
	}); err != nil {
		return cleanup(fmt.Errorf("记录 incarnation %s idle: %w", incID, err))
	}
	r.handle, r.handleEnv, r.done.IncarnationID = inc, envID, incID
	return r
}

// opDestroy 销毁 incarnation：撤销入口 → StopEnv（须 Recorded）→ EndIncarnation → 删除暂存 → 归还内存。
func (a *Actor) opDestroy(ctx context.Context, f DestroyIncarnation, handleID string, mem *Grant) opResult {
	r := opResult{done: OpDone{Op: OpDestroy, IncarnationID: f.IncarnationID}}
	if err := a.d.Gateway.RevokeIncarnation(ctx, f.IncarnationID); err != nil {
		r.done.Err = fmt.Errorf("撤销 incarnation %s 的 Gateway 入口: %w", f.IncarnationID, err)
		return r
	}
	rep, err := a.d.Env.StopEnv(ctx, f.EnvID)
	if err == nil && !rep.Recorded {
		err = fmt.Errorf("环境 %s 的停止未确认（stopped=%v blocked=%v）", f.EnvID, rep.Stopped, rep.Blocked)
	}
	if err != nil {
		r.done.Err = err
		return r
	}
	if _, err := retry(ctx, a.d, func(ctx context.Context) (Incarnation, error) {
		inc, err := a.d.Store.EndIncarnation(ctx, f.IncarnationID, f.Reason)
		return inc, err
	}); err != nil {
		a.storeErr(&r.done, err)
		return r
	}
	_ = a.d.Env.UnstageRestore(ctx, f.EnvID) // 未卸载的暂存（启动中断）在环境停止之后删除
	if f.IncarnationID == handleID {
		r.clearHandle = true
	}
	// 会话至多一个存活 incarnation：持有的内存属于它（句柄所属，或启动失败后残留的无句柄 incarnation）。
	if mem != nil && (f.IncarnationID == handleID || handleID == "") {
		a.d.Admission.Release(*mem)
		r.mem, r.memSet = nil, true
	}
	return r
}

// setInc 以 CAS 改变 incarnation 状态；失败写入 done（暂时失败为 Err，冲突为 Conflict）。
func (a *Actor) setInc(ctx context.Context, done *OpDone, incID string, from []string, to string) bool {
	_, err := retry(ctx, a.d, func(ctx context.Context) (Incarnation, error) {
		inc, err := a.d.Store.SetIncarnationStatus(ctx, incID, from, to)
		return inc, err
	})
	a.storeErr(done, err)
	return err == nil
}

// storeErr 把 Store 的确定结果分类：冲突与前置条件不满足为 Conflict（按新事实重新决策），其余为 Err。
func (a *Actor) storeErr(done *OpDone, err error) {
	switch {
	case err == nil:
	case errors.Is(err, persistence.ErrConflict), errors.Is(err, persistence.ErrRejected):
		done.Conflict = true
	default:
		done.Err = err
	}
}

// reload 在操作之后重新读取会话事实（读取失败时 State 为 nil，Decide 稍后重读）。
func (a *Actor) reload(ctx context.Context, done *OpDone) {
	st, err := retry(ctx, a.d, func(ctx context.Context) (State, error) { return a.d.Store.LoadSession(ctx, a.id) })
	if err == nil {
		done.State = &st
	}
}

// definitive 报告 Store 错误是否是确定的结果（不再以同一身份重试）。
func definitive(err error) bool {
	return errors.Is(err, persistence.ErrRejected) || errors.Is(err, persistence.ErrConflict) ||
		errors.Is(err, persistence.ErrNotFound) || errors.Is(err, persistence.ErrInvalid) ||
		errors.Is(err, persistence.ErrOwnershipLost)
}

// retry 执行 Store 用例（各用例幂等），暂时失败时按退避重试，直到成功、确定失败或 ctx 结束。
func retry[T any](ctx context.Context, d Deps, f func(context.Context) (T, error)) (T, error) {
	for n := 0; ; n++ {
		v, err := f(ctx)
		if err == nil || ctx.Err() != nil || definitive(err) {
			return v, err
		}
		c, stop := d.Clock.At(d.Clock.Now().Add(Backoff(n)))
		select {
		case <-c:
			stop()
		case <-ctx.Done():
			stop()
			return v, ctx.Err()
		}
	}
}
