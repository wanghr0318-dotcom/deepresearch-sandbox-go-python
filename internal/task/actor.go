package task

// 本文件是 task actor（规格 §3.2、§6；代码组织 §5）：每个任务一个 select 循环，只作决策、提交状态、
// 安排操作。actor 以 Decide 计算新状态与副作用，按顺序执行副作用；异步操作在独立 goroutine 中执行，
// 结果以带 attempt_id 的事件回到收件箱。actor 不关闭 socket、不写 cgroup、不重试数据库连接：物理操作
// 经窄接口交给 coordinator、AttemptRunner 与 Gateway，持久化只经 Store 用例。
//
// task 不导入 runner、resource、admission 或 provider：本文件声明 actor 需要的窄接口，装配代码把
// 各包的实现适配到这些接口（分类由适配器经 runner.Classify 完成，actor 只接收已分类的 Outcome）。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/faultinject"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// RunTimePersistInterval 是运行时间的持久化周期（规格 §14.4、§19），也是周期比较控制版本的周期（§8.1）。
const RunTimePersistInterval = 10 * time.Second

// revokeTimeout 是停止环境之前撤销 attempt 访问（Store）的单次期限。Store 不可用时不让它拖住物理停止
// （§14.5"先完成物理终止"）：失败的撤销进入 Store 队列，按退避补提交。
const revokeTimeout = 5 * time.Second

// ---- 窄接口 ----

// SlotRequest 是 run slot 的申请；适配器据 Limits 计算内存（admission.Request）。SessionID 非空（会话 turn）时
// 适配器申请 MemoryBytes = 0：incarnation 的内存由 session actor 以 MemoryOnly 授予持有。
type SlotRequest struct {
	TaskID    string
	Limits    json.RawMessage
	SessionID string
}

// SessionGate 是 session actor 的授予与交还（session.Scheduler 的适配，Task 9 装配）。
//
//   - Grant 阻塞直到会话把 incarnation 授予该 turn；会话已关闭或恢复两次失败时返回 ErrSessionUnavailable（适配器把
//     session.ErrUnavailable 包装为它），ctx 结束时撤回。
//   - Handoff 在裁决提交之后交还 incarnation（Destroy=false → Release），或要求销毁（Destroy=true）。返回的
//     StopReport.Recorded 为真表示该 attempt 的执行已确认结束（释放成功，或环境 stopped_at 已记录）。
type SessionGate interface {
	Grant(ctx context.Context, sessionID, taskID string) (SessionGrant, error)
	Handoff(ctx context.Context, sessionID string, h SessionHandoff) (StopReport, error)
}

// SessionGrant 是授予给会话 turn 的 incarnation 与其环境。
type SessionGrant struct{ IncarnationID, EnvID string }

// SessionHandoff 是会话 turn 交还 incarnation（session.Handoff 的对应）。
type SessionHandoff struct {
	TaskID, AttemptID, EnvID, Verdict, CommittedSessionCheckpointID string
	Destroy                                                         bool
}

// ErrSessionUnavailable 表示会话已关闭或暂时无法恢复：queued 的 turn 失败（status_reason = session_unavailable）。
var ErrSessionUnavailable = errors.New("task: 会话不可用")

// TurnStore 是会话 turn 额外需要的 Store 用例（*postgres.Store 实现；独立任务不需要，故不并入 Store）。
type TurnStore interface {
	// FailQueuedTurn 把没有活动 attempt 的 queued 会话 turn 裁决为 failed（status_reason = reason），同事务清除它对会话的
	// 占用与阻塞并追加 host 事件 task_terminal。已是同一结果时成功（重跑）；不是 queued 为 ErrRejected。
	FailQueuedTurn(ctx context.Context, taskID, reason string) error
}

// SlotGrant 是一份已授予的容量（与 admission.Grant 对应，按 ID 归还）。
type SlotGrant struct {
	ID          uint64
	TaskID      string
	MemoryBytes int64
}

// Admission 是 actor 使用的准入闸门（admission.Admission 的适配）。Acquire 阻塞直到授予或 ctx 结束；
// Release 幂等且不阻塞。
type Admission interface {
	Acquire(ctx context.Context, r SlotRequest) (SlotGrant, error)
	Release(g SlotGrant)
}

// EnvSpec 是创建任务环境的输入；适配器据 Spec 与 Limits 构造 resource.EnvRequest。GatewaySocket 是
// Access.Bind 返回的宿主侧 socket（空为不挂载），适配器把它放入 Mounts.GatewaySocket。BindErr 非 nil
// 表示 Gateway 入口绑定失败：适配器不创建环境，按创建失败分类返回（与 RunSpec.StartErr 的约定相同）。
type EnvSpec struct {
	TaskID, AttemptID, EnvID string
	Spec, Limits             json.RawMessage
	GatewaySocket            string
	BindErr                  error
}

// StopReport 是停止环境报告的事实（resource.StopResult 的对应）。**只有 Recorded 为真时 actor 才认为
// 环境已停止并归还容量**；Stopped 而未 Recorded 表示停止已确认、stopped_at 尚未持久化，Blocked 表示
// 无法确认停止（stop_blocked）。
type StopReport struct {
	Stopped, Recorded, Blocked bool
	At                         time.Time
}

// EnvController 是 actor 使用的 resource coordinator 子集。CreateEnv 失败时返回 error 与适配器经
// runner.Classify 给出的分类结果（不能为 nil）；StopEnv 只在停止本身不确定时返回 error。
type EnvController interface {
	CreateEnv(ctx context.Context, s EnvSpec) (failure *Outcome, err error)
	StopEnv(ctx context.Context, envID string) (StopReport, error)
}

// RunSpec 是一次执行的输入。Task 是启动前重新读取的任务事实（含 init.resume 所需的最新 checkpoint 与
// init.budget_limits 所需的 limits）。StartErr 非 nil 表示启动前的准备（读取任务）失败：适配器不启动
// Worker，按启动失败分类返回。Gateway 入口在创建环境之前绑定（EnvSpec.GatewaySocket）。OnReady 在合法的
// ready 处理完毕后同步调用恰好一次（runner.Attempt.OnReady），不阻塞。
type RunSpec struct {
	Task             TaskState
	AttemptID, EnvID string
	AttemptNo        int64
	StartErr         error
	OnReady          func()
}

// Control 是交给 AttemptRunner 的 cancel/pause（runner.Control 的对应）。
type Control struct {
	Kind    string
	GraceMs int64
}

// AttemptRunner 执行一次 attempt 并返回已分类的结果（适配器把 runner.Outcome 转换为 task.Outcome，
// Class 与 Retry 取自 runner.Classify）。Run 返回之前 OnReady 至多调用一次。ctx 结束（服务停止）时
// Run 须尽快返回。
type AttemptRunner interface {
	Run(ctx context.Context, a RunSpec, controls <-chan Control) Outcome
}

// Clock 是 actor 的时间来源。At 返回在 t（或之后）触发的通道与释放它的函数；t 不晚于当前时间时立即触发。
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

// SystemClock 返回基于 time 包的 Clock（Now 带单调读数，间隔按单调时钟计算）。
func SystemClock() Clock { return systemClock{} }

// Deps 是 actor 的依赖。Clock、IDs、Jitter、Access 为空时分别取 SystemClock、随机 ID、均匀随机数与
// M1 的 FakeAccess。OnFatal 可选：actor 遇到不可继续的错误（决策不合法、失去数据库所有权、panic）时
// 在退出前调用（规格：actor panic → 致命停止，由装配代码执行进程级处理）。
type Deps struct {
	Store     Store
	Admission Admission
	Env       EnvController
	Runner    AttemptRunner
	Access    Access
	Clock     Clock
	IDs       func() string
	Jitter    func() float64
	OnFatal   func(taskID string, err error)
	// Session 是会话 turn 的授予与交还；只有会话 turn 使用（为 nil 时会话 turn 的 actor 致命停止）。
	Session SessionGate

	// RunTimeLimit 可选：任务的累计运行时限（§14.4），0 表示不限。装配代码取任务 limits 的
	// max_run_time_ms，没有时取服务配置的默认值。限额按任务计（已持久化的 run_time_ms 加当前区间），
	// 新 attempt 不重置：执行中到达时以 cause context.DeadlineExceeded 取消 Run 的 ctx（runner 据此
	// 分类为 task_deadline_exceeded）；已达到限额时启动的执行以已取消的 ctx 开始；限额已用尽时可重试的
	// 结果改为 task_deadline_exceeded、不重试（capRetry）。
	RunTimeLimit func(TaskState) time.Duration
	// OnStopRecorded 可选：恢复交来的 stop_blocked attempt 的环境确认停止（Recorded）后调用恰好一次
	// 成功，在提交判决之前（经 Store 队列排在 Finalize 之前），把未记账区间计入运行时间（装配为
	// recovery.Store.AccountUnrecordedRunTime，§14.1 第 8 步）。暂时失败按退避重试，事实保留。
	OnStopRecorded func(ctx context.Context, attemptID string, stoppedAt time.Time) error
}

func (d Deps) withDefaults() Deps {
	if d.Clock == nil {
		d.Clock = SystemClock()
	}
	if d.IDs == nil {
		d.IDs = randomID
	}
	if d.Jitter == nil {
		d.Jitter = mrand.Float64
	}
	if d.Access == nil {
		d.Access = &FakeAccess{}
	}
	return d
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SpawnOption 配置 Spawn。
type SpawnOption func(*spawnOptions)

type spawnOptions struct{ stopBlocked *SlotGrant }

// WithStopBlocked 是启动恢复交来的 stop_blocked 任务（规格 §14.1"停止失败"）：任务的当前 attempt 的
// 环境未确认停止，g 是恢复为它重建的 admission 授予。actor 以 stop_blocked、结果 lost_on_restart
// （故障重试）建立该 attempt 并持有 g，退避重试停止；确认停止（Recorded）后才归还 g 并按 desired 裁决。
// 只用于非终态任务。
func WithStopBlocked(g SlotGrant) SpawnOption {
	return func(o *spawnOptions) { o.stopBlocked = &g }
}

// ---- 重试类别 ----

// outcomeClasses 是 runner.Classify 与恢复给出的 outcome_class 全集（task 不导入 runner，此处列出名称）。
var outcomeClasses = map[string]bool{
	ClassSucceeded: true, ClassOOMObserved: true, "crashed_signal": true, "control_lost": true,
	"ready_timeout": true, "lost_on_restart": true, "subrun_cancel_timeout": true, "release_timeout": true,
	"create_failed_transient": true, "create_failed_env": true, "create_outcome_unknown": true,
	"store_unavailable": true, classWorkerOOMLikely: true, "worker_error": true, "protocol_mismatch": true,
	"protocol_violation": true, "output_limit_exceeded": true, "exit_after_result": true,
	ClassDeadlineExceeded: true, ClassCancelled: true, ClassPaused: true, "exited_without_proposal": true,
	"output_incomplete": true,
}

const classWorkerOOMLikely = "worker_oom_likely"

// RetryAfter 推出 queued 任务下一次 CreateAttempt 的重试类别（Plan 5 执行中修订）。待定的重试类别不单独
// 持久化：Decide 只在 Outcome.Retry 非空时把任务裁决为 queued，并以 outcome_class 为 status_reason。
// 因此 queued 且 status_reason 是 outcome class 时，worker_oom_likely 为 oom、其余类别为 fault（worker_error
// 是否可重试取决于 Worker 的 retryable 与错误码，已由裁决体现在"是否 queued"上）；status_reason 不是
// outcome class（新建、resume 等）时为 none。重启后同样成立。
func RetryAfter(status, statusReason string) RetryKind {
	switch {
	case status != "queued" || !outcomeClasses[statusReason]:
		return RetryNone
	case statusReason == classWorkerOOMLikely:
		return RetryOOM
	}
	return RetryFault
}

// ---- Actor ----

// Actor 驱动一个任务。全部状态只由 actor goroutine 访问；外部只经 Notify、Done、Err 交互。
type Actor struct {
	taskID string
	d      Deps
	opt    spawnOptions
	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan any
	notify chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
	fatal  error // 在 done 关闭之前写入

	s             State
	limits, spec  json.RawMessage
	grant         SlotGrant
	inflight      int // 已发起、结果未处理的异步操作
	attemptsTotal int64
	attemptNo     map[string]int64
	controls      map[string]chan Control
	revoking      map[string]string    // attempt → 停止前须撤销访问的原因
	stopAt        map[string]time.Time // env → 已确认的停止时间（运行时间的终点）
	wakes         []time.Time
	housekeepAt   time.Time
	timerAt       time.Time
	timerC        <-chan time.Time
	timerStop     func()

	controlReading, controlAgain bool

	storeQ        []*storeOp // 待提交的事实，按产生顺序串行提交
	storeBusy     bool
	storeFailures int64
	storeRetryAt  *time.Time

	rt runTime
	// limit 是累计运行时限（0 不限）；runCancel 是执行中 attempt 的 Run ctx 的取消函数。
	limit     time.Duration
	runCancel map[string]context.CancelCauseFunc
	// accountAttempt 是恢复交来、尚待 OnStopRecorded 计入运行时间的 attempt。
	accountAttempt string
	// grantCancel 撤回在途的会话授予申请（WithdrawSessionGrant）。
	grantCancel context.CancelFunc
}

// runTime 是累计运行时间（§14.4）：base 是已结束区间的累计（毫秒），openAt 非 nil 时当前 attempt 的
// 区间（starting 至 stopped_at）尚未结束。
type runTime struct {
	base      int64
	attemptID string
	openAt    *time.Time
}

func (r runTime) total(now time.Time) int64 {
	if r.openAt == nil {
		return r.base
	}
	return r.base + max(now.Sub(*r.openAt), 0).Milliseconds()
}

// Spawn 启动任务 taskID 的 actor。ctx 结束时 actor 取消在途操作、等待它们返回后关闭 Done。
func Spawn(ctx context.Context, taskID string, d Deps, opts ...SpawnOption) *Actor {
	ctx, cancel := context.WithCancel(ctx)
	a := &Actor{taskID: taskID, d: d.withDefaults(), ctx: ctx, cancel: cancel,
		inbox: make(chan any, 16), notify: make(chan struct{}, 1), done: make(chan struct{}),
		attemptNo: map[string]int64{}, controls: map[string]chan Control{}, revoking: map[string]string{},
		stopAt: map[string]time.Time{}, runCancel: map[string]context.CancelCauseFunc{}}
	for _, o := range opts {
		o(&a.opt)
	}
	go a.main()
	return a
}

// Notify 通知 actor 控制意图可能已变化（非阻塞；多次通知合并）。
func (a *Actor) Notify() {
	select {
	case a.notify <- struct{}{}:
	default:
	}
}

// Done 在 actor 退出且其全部 goroutine 返回后关闭：任务已终态且资源已归还，或 ctx 结束，或致命错误。
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

func (a *Actor) main() {
	defer close(a.done)
	defer func() {
		a.cancel()
		if a.timerStop != nil {
			a.timerStop()
		}
		a.wg.Wait()
	}()
	defer func() {
		if r := recover(); r != nil {
			a.die(fmt.Errorf("task: 任务 %s 的 actor panic: %v", a.taskID, r))
		}
	}()
	if !a.load() {
		return
	}
	a.housekeepAt = a.d.Clock.Now().Add(RunTimePersistInterval)
	a.apply(Tick{})
	for a.fatal == nil && !a.finished() {
		a.arm()
		select {
		case <-a.ctx.Done():
			return
		case <-a.notify:
			a.readControl()
		case m := <-a.inbox:
			a.handle(m)
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
		a.d.OnFatal(a.taskID, err)
	}
}

// load 以 LoadTask 建立 State；恢复交来的 stop_blocked 任务另读取当前 attempt。
func (a *Actor) load() bool {
	ts, err := readRetry(a.ctx, a.d, func(ctx context.Context) (TaskState, error) { return a.d.Store.LoadTask(ctx, a.taskID) })
	if err != nil {
		if a.ctx.Err() == nil {
			a.die(fmt.Errorf("task: 读取任务 %s: %w", a.taskID, err))
		}
		return false
	}
	a.limits, a.spec, a.attemptsTotal = ts.Limits, ts.Spec, ts.AttemptsTotal
	a.rt.base = ts.RunTimeMs
	if a.d.RunTimeLimit != nil {
		a.limit = max(a.d.RunTimeLimit(ts), 0)
	}
	a.s = State{TaskID: a.taskID, TaskStatus: ts.Status, Desired: ts.Desired, ControlVersion: ts.ControlVersion,
		AppliedControlVersion: ts.AppliedControlVersion, FaultRetriesUsed: ts.FaultRetriesUsed,
		MaxFaultRetries: ts.MaxFaultRetries, OOMRetriesUsed: ts.OOMRetriesUsed,
		NextRetry: RetryAfter(ts.Status, ts.StatusReason), NotBefore: ts.NotBefore, SessionID: ts.SessionID}
	if g := a.opt.stopBlocked; g != nil {
		att, err := readRetry(a.ctx, a.d, func(ctx context.Context) (Attempt, error) {
			return a.d.Store.GetAttempt(ctx, ts.CurrentAttemptID)
		})
		if err != nil {
			if a.ctx.Err() == nil {
				a.die(fmt.Errorf("task: 读取任务 %s 的 stop_blocked attempt %q: %w", a.taskID, ts.CurrentAttemptID, err))
			}
			return false
		}
		a.s.Attempt = &AttemptState{AttemptID: att.AttemptID, EnvID: att.EnvID, Status: "stop_blocked",
			StoredStatus: att.Status, Outcome: &Outcome{Class: "lost_on_restart", Retry: RetryFault}}
		a.s.SlotHeld, a.grant = true, *g
		if a.d.OnStopRecorded != nil {
			a.accountAttempt = att.AttemptID
		}
	}
	return true
}

// finished：任务已终态，资源已归还（环境确认停止、槽位归还），待提交的事实与在途操作均已完成。
func (a *Actor) finished() bool {
	at := a.s.Attempt
	return IsTerminal(a.s.TaskStatus) && a.inflight == 0 && len(a.storeQ) == 0 && !a.s.SlotHeld &&
		!a.s.SlotRequested && (at == nil || at.EnvStopped)
}

// capRetry：累计运行时间已达到限额时，可重试的结果改为 task_deadline_exceeded、不重试（§14.3、§14.4：
// 新 attempt 不重置限额），使重试判决不会让已用尽限额的任务重新排队。不可重试的结果保持原样。
func (a *Actor) capRetry(o Outcome) Outcome {
	if o.Retry != RetryNone && a.overLimit(a.d.Clock.Now()) {
		o.Class, o.Retry = ClassDeadlineExceeded, RetryNone
	}
	return o
}

// limitAt 返回执行中 attempt 到达累计运行时限的时间；没有限额、区间未开始或 Run 已被取消时 ok 为假。
func (a *Actor) limitAt() (time.Time, bool) {
	if a.limit <= 0 || a.rt.openAt == nil || a.runCancel[a.rt.attemptID] == nil {
		return time.Time{}, false
	}
	return a.rt.openAt.Add(a.limit - time.Duration(a.rt.base)*time.Millisecond), true
}

// overLimit：累计运行时间已达到限额。
func (a *Actor) overLimit(now time.Time) bool {
	return a.limit > 0 && a.rt.total(now) >= a.limit.Milliseconds()
}

// ---- 计时 ----

// arm 让唯一的计时器指向最早的到期时间：Decide 的 WakeAt、Store 退避重试与周期任务。
func (a *Actor) arm() {
	next := a.housekeepAt
	for _, w := range a.wakes {
		if w.Before(next) {
			next = w
		}
	}
	if a.storeRetryAt != nil && a.storeRetryAt.Before(next) {
		next = *a.storeRetryAt
	}
	if at, ok := a.limitAt(); ok && at.Before(next) {
		next = at
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
	if at, ok := a.limitAt(); ok && !now.Before(at) {
		a.stopRunForLimit(a.rt.attemptID)
	}
	if a.storeRetryAt != nil && !now.Before(*a.storeRetryAt) {
		a.storeRetryAt = nil
		a.pumpStore()
	}
	if !now.Before(a.housekeepAt) {
		a.housekeepAt = now.Add(RunTimePersistInterval)
		a.readControl() // actor 也周期比较 control_version（§8.1）
		if a.rt.openAt != nil {
			a.persistRunTime(now)
		}
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
	if a.fatal != nil {
		return
	}
	now := a.d.Clock.Now()
	a.s.Now, a.s.Jitter = now, a.d.Jitter()
	dec, err := Decide(a.s, e)
	if err != nil {
		a.die(fmt.Errorf("task: 任务 %s 的决策（%T）失败: %w", a.taskID, e, err))
		return
	}
	a.s = dec.Next
	a.trackRunTime(e, now)
	for _, eff := range dec.Effects {
		a.exec(eff)
	}
}

// trackRunTime：运行时间是持有执行环境的时间（starting 至 stopped_at，§14.4），以 Clock 的单调读数计算。
func (a *Actor) trackRunTime(e Event, now time.Time) {
	switch e := e.(type) {
	case AttemptCreated:
		if cur := a.s.Attempt; cur != nil && cur.AttemptID == e.AttemptID {
			a.rt.attemptID, a.rt.openAt = e.AttemptID, &now
		}
	case EnvStopped:
		if a.rt.openAt == nil || a.rt.attemptID != e.AttemptID {
			return
		}
		end := now
		if at, ok := a.stopAt[e.EnvID]; ok && !at.IsZero() && at.Before(now) {
			end = at
		}
		a.rt.base, a.rt.openAt = a.rt.total(end), nil
		a.persistRunTime(now) // 区间结束：补提交最终值
	}
}

func (a *Actor) exec(eff Effect) {
	switch f := eff.(type) {
	case RequestSlot:
		// 已达累计运行时限的 queued 任务（重启记账或调低配置之后）同样走正常路径：startWorker 以已取消
		// 的 ctx 启动执行，runner 分类为 task_deadline_exceeded，经现有判决路径结束（代价是创建一次环境）。
		req := SlotRequest{TaskID: a.taskID, Limits: a.limits, SessionID: a.s.SessionID}
		a.async(func(ctx context.Context) any {
			g, err := a.d.Admission.Acquire(ctx, req)
			return grantResult{g: g, err: err}
		})
	case ReleaseSlot:
		a.d.Admission.Release(a.grant)
		a.grant = SlotGrant{}
	case RequestSessionGrant:
		a.requestSessionGrant()
	case WithdrawSessionGrant:
		if a.grantCancel != nil {
			a.grantCancel()
		}
	case FailTurn:
		a.enqueue(&storeOp{kind: opFailTurn, reason: f.Reason})
	case CreateAttempt:
		na := NewAttempt{TaskID: a.taskID, AttemptID: a.d.IDs(), AttemptNo: a.attemptsTotal + 1, Retry: f.Retry}
		if g := f.Session; g != nil { // 会话 turn：attempt 在授予的 incarnation 环境中运行
			na.EnvID, na.SessionID, na.IncarnationID = g.EnvID, a.s.SessionID, g.IncarnationID
		} else {
			na.EnvID = a.d.IDs()
		}
		a.enqueue(&storeOp{kind: opCreateAttempt, attempt: na})
	case CreateEnvironment:
		spec := EnvSpec{TaskID: a.taskID, AttemptID: f.AttemptID, EnvID: f.EnvID, Spec: a.spec, Limits: a.limits}
		a.async(func(ctx context.Context) any {
			// Gateway 入口先于环境建立：socket 须在 init 挂载它时已存在（§9.1）。绑定失败交给适配器分类。
			if spec.GatewaySocket, spec.BindErr = a.d.Access.Bind(ctx, f.AttemptID, f.EnvID); spec.BindErr != nil {
				spec.BindErr = fmt.Errorf("task: 绑定 attempt %s 的 Gateway 入口: %w", f.AttemptID, spec.BindErr)
			}
			o, err := a.d.Env.CreateEnv(ctx, spec)
			if err == nil {
				o = nil
			}
			return EnvCreated{AttemptID: f.AttemptID, EnvID: f.EnvID, Err: err, Outcome: o}
		})
	case StartWorker:
		a.startWorker(f)
	case SendControl:
		ch := a.controls[f.AttemptID]
		if ch == nil {
			return // 执行已结束：控制只影响裁决
		}
		select {
		case ch <- Control{Kind: f.Kind, GraceMs: f.GraceMs}:
		default: // Decide 每个 attempt 至多发出 cancel 与 pause 各一次
			a.die(fmt.Errorf("task: attempt %s 的控制通道已满", f.AttemptID))
		}
	case RevokeAccess:
		a.revoking[f.AttemptID] = f.Reason
	case RevokeGateway:
		// 取消生效时只关闭 Gateway 入口（§9.1；不动 attempt_access，见 RevokeGateway）。排入 Store 队列，使它在
		// 同一决策的 ApplyControl（desired = cancel 的持久化执行点）提交之后才执行：先持久化、再关闭。
		a.enqueue(&storeOp{kind: opRevokeGateway, attemptID: f.AttemptID, reason: f.Reason})
	case StopEnvironment:
		a.stopEnv(f)
	case Finalize:
		a.enqueue(&storeOp{kind: opFinalize, verdict: f.Verdict})
	case ApplyControl:
		a.enqueue(&storeOp{kind: opApplyControl, control: f})
	case WakeAt:
		if !slices.ContainsFunc(a.wakes, f.Time.Equal) {
			a.wakes = append(a.wakes, f.Time)
		}
	default:
		a.die(fmt.Errorf("task: 未知副作用 %T", eff))
	}
}

// startWorker 在独立 goroutine 中重新读取任务事实（init.resume、init.budget_limits）并运行 attempt。
// OnReady 以 WorkerStarted 回到收件箱；Run 返回后以 AttemptFinished 回到收件箱。
func (a *Actor) startWorker(f StartWorker) {
	ch := make(chan Control, 4)
	a.controls[f.AttemptID] = ch
	spec := RunSpec{AttemptID: f.AttemptID, EnvID: f.EnvID, AttemptNo: a.attemptNo[f.AttemptID]}
	taskID := a.taskID
	// Run 的 ctx 可单独以 context.DeadlineExceeded 取消（累计运行时限，§14.4）；结果仍经 actor 的 ctx 投递。
	runCtx, cancelRun := context.WithCancelCause(a.ctx)
	a.runCancel[f.AttemptID] = cancelRun
	if a.rt.attemptID == f.AttemptID && a.overLimit(a.d.Clock.Now()) {
		a.stopRunForLimit(f.AttemptID)
	}
	a.async(func(ctx context.Context) any {
		defer cancelRun(nil)
		spec.Task, spec.StartErr = readRetry(ctx, a.d, func(ctx context.Context) (TaskState, error) {
			return a.d.Store.LoadTask(ctx, taskID)
		})
		spec.OnReady = func() { a.post(ctx, WorkerStarted{AttemptID: f.AttemptID, EnvID: f.EnvID}) }
		return AttemptFinished{AttemptID: f.AttemptID, EnvID: f.EnvID, Outcome: a.d.Runner.Run(runCtx, spec, ch)}
	})
}

// stopRunForLimit 以 cause context.DeadlineExceeded 取消 attempt 的 Run（runner 分类为
// task_deadline_exceeded，Decide 不重试）。每个 attempt 至多一次。
func (a *Actor) stopRunForLimit(attemptID string) {
	if cancel := a.runCancel[attemptID]; cancel != nil {
		cancel(context.DeadlineExceeded)
		a.runCancel[attemptID] = nil
	}
}

// stopEnv：先撤销访问，再经 coordinator 停止环境。撤销顺序是 **DB 先提交、再关连接**（§9.1"持久化
// 检查才是执行点"）：Store.RevokeAttemptAccess 提交（或被确定拒绝）之后才 Access.Revoke；Store 暂时失败时
// 不关闭入口（它在 Store 中仍是 active），由 Store 队列补提交后再关闭（见 opRevoke），物理停止不等待它。
// 原因由 Decide 给出（desired = cancel 时为 RevokeReasonCancel，Gateway 据此取消该 attempt 的在途上游 try）。
//
// 会话 turn（f.Session 非 nil）以 SessionGate.Handoff 代替 StopEnv：撤销顺序相同，Recorded 即该 attempt 的执行已确认
// 结束（释放成功或 incarnation 环境已停止）。
func (a *Actor) stopEnv(f StopEnvironment) {
	reason, revoke := a.revoking[f.AttemptID]
	delete(a.revoking, f.AttemptID)
	if f.Session != nil && a.d.Session == nil {
		a.die(fmt.Errorf("task: 会话 turn %s 没有 SessionGate", a.taskID))
		return
	}
	sessionID := a.s.SessionID
	a.async(func(ctx context.Context) any {
		r := stopDone{attemptID: f.AttemptID, envID: f.EnvID, reason: reason}
		if revoke {
			rctx, cancel := context.WithTimeout(ctx, revokeTimeout)
			r.revokeErr = a.d.Store.RevokeAttemptAccess(rctx, f.AttemptID, reason)
			cancel()
			if r.revokeErr == nil || definitive(r.revokeErr) {
				r.accessErr = a.d.Access.Revoke(ctx, f.AttemptID, reason)
			}
		}
		if f.Session != nil {
			r.report, r.err = a.d.Session.Handoff(ctx, sessionID, *f.Session)
			if r.report.Stopped && r.report.At.IsZero() {
				r.report.At = a.d.Clock.Now()
			}
			return r
		}
		r.report, r.err = a.d.Env.StopEnv(ctx, f.EnvID)
		return r
	})
}

// requestSessionGrant 向 session actor 申请 incarnation（可由 WithdrawSessionGrant 撤回）。
func (a *Actor) requestSessionGrant() {
	if a.d.Session == nil {
		a.die(fmt.Errorf("task: 会话 turn %s 没有 SessionGate", a.taskID))
		return
	}
	gctx, cancel := context.WithCancel(a.ctx)
	a.grantCancel = cancel
	sessionID, taskID := a.s.SessionID, a.taskID
	a.async(func(context.Context) any {
		defer cancel()
		g, err := a.d.Session.Grant(gctx, sessionID, taskID)
		if err != nil {
			return SessionUnavailable{Err: err}
		}
		return SessionGranted{IncarnationID: g.IncarnationID, EnvID: g.EnvID}
	})
}

// ---- 异步操作 ----

type asyncDone struct{ v any }

type grantResult struct {
	g   SlotGrant
	err error
}

type controlResult struct {
	cs  ControlState
	err error
}

type stopDone struct {
	attemptID, envID, reason string
	accessErr, revokeErr     error
	report                   StopReport
	err                      error
}

type panicked struct{ err error }

// async 在独立 goroutine 中执行 f，结果经收件箱回到 actor。发送在 ctx.Done 上 select，actor 退出后不阻塞。
func (a *Actor) async(f func(ctx context.Context) any) {
	a.inflight++
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		var v any
		func() {
			defer func() {
				if r := recover(); r != nil {
					v = panicked{err: fmt.Errorf("task: 任务 %s 的异步操作 panic: %v", a.taskID, r)}
				}
			}()
			v = f(a.ctx)
		}()
		a.post(a.ctx, asyncDone{v: v})
	}()
}

func (a *Actor) post(ctx context.Context, m any) {
	select {
	case a.inbox <- m:
	case <-ctx.Done():
	}
}

func (a *Actor) handle(m any) {
	switch m := m.(type) {
	case asyncDone:
		a.inflight--
		a.handleResult(m.v)
	case Event: // OnReady 发来的 WorkerStarted
		a.apply(m)
	}
}

func (a *Actor) handleResult(v any) {
	switch r := v.(type) {
	case grantResult:
		switch {
		case r.err == nil:
			a.grant = r.g
			a.apply(SlotGranted{})
		case a.ctx.Err() == nil:
			// 例如 ErrExceedsCapacity（请求超过总容量）：创建任务时 API 已拒绝永远无法满足的 limits，
			// 到这里是配置变化或程序错误；Decide 没有"永远无法授予"的事件，按致命错误处理。
			a.die(fmt.Errorf("task: 任务 %s 申请 run slot: %w", a.taskID, r.err))
		}
	case EnvCreated:
		if r.Err != nil && r.Outcome != nil {
			o := a.capRetry(*r.Outcome)
			r.Outcome = &o
		}
		a.apply(r)
	case AttemptFinished:
		delete(a.controls, r.AttemptID)
		delete(a.runCancel, r.AttemptID)
		r.Outcome = a.capRetry(r.Outcome)
		a.apply(r)
	case controlResult:
		a.controlReading = false
		switch {
		case r.err == nil:
			a.apply(ControlChanged{Desired: r.cs.Desired, ControlVersion: r.cs.ControlVersion})
		case errors.Is(r.err, persistence.ErrOwnershipLost):
			a.die(r.err)
		} // 其余读取失败：下一次通知或周期比较再读
		if a.controlAgain {
			a.controlAgain = false
			a.readControl()
		}
	case SessionGranted:
		a.grantCancel = nil
		a.apply(r)
	case SessionUnavailable:
		a.grantCancel = nil
		if a.ctx.Err() == nil {
			a.apply(r)
		}
	case stopDone:
		a.handleStop(r)
	case storeDone:
		a.handleStore(r)
	case panicked:
		a.die(r.err)
	}
}

func (a *Actor) readControl() {
	if a.controlReading {
		a.controlAgain = true
		return
	}
	a.controlReading = true
	taskID := a.taskID
	a.async(func(ctx context.Context) any {
		cs, err := a.d.Store.GetControlState(ctx, taskID)
		return controlResult{cs: cs, err: err}
	})
}

// handleStop 把 coordinator 报告的事实交给 Decide：只有 Recorded 才是 EnvStopped（归还容量、允许替代
// 执行）；已停止未记录、无法确认与停止出错都按 StopUnconfirmed 退避重试。
func (a *Actor) handleStop(r stopDone) {
	if r.revokeErr != nil {
		switch {
		case errors.Is(r.revokeErr, persistence.ErrOwnershipLost):
			a.die(r.revokeErr)
			return
		case !definitive(r.revokeErr): // 保留"访问应撤销"的事实，由 Store 队列补提交
			a.enqueue(&storeOp{kind: opRevoke, attemptID: r.attemptID, reason: r.reason})
		}
	}
	if r.err == nil && r.report.Stopped {
		if _, ok := a.stopAt[r.envID]; !ok || r.report.Recorded {
			a.stopAt[r.envID] = r.report.At
		}
	}
	if r.err == nil && r.report.Recorded {
		if r.attemptID != "" && r.attemptID == a.accountAttempt {
			// 恢复交来的 attempt：未记账区间在判决之前计入（Store 队列保证排在 Finalize 之前）。
			a.accountAttempt = ""
			a.enqueue(&storeOp{kind: opAccountRunTime, attemptID: r.attemptID, stoppedAt: r.report.At})
		}
		a.apply(EnvStopped{AttemptID: r.attemptID, EnvID: r.envID})
		delete(a.stopAt, r.envID)
		return
	}
	a.apply(StopUnconfirmed{AttemptID: r.attemptID, EnvID: r.envID})
}

// ---- Store 队列 ----

type storeOpKind int

const (
	opCreateAttempt storeOpKind = iota
	opFinalize
	opApplyControl
	opPersistRunTime
	opRevoke
	opAccountRunTime // 恢复交来的 attempt：Deps.OnStopRecorded
	opRevokeGateway  // 只关闭 Gateway 入口（Access.Revoke），不写 Store；排在之前的 Store 写入之后
	opFailTurn       // 会话 turn 没有 attempt 即失败（TurnStore.FailQueuedTurn）
)

// storeOp 是一个待提交的事实。队列按产生顺序串行提交：暂时失败（Store 不可用、提交结果未知、锁争用）
// 时按退避以同一身份重试（各用例幂等），事实保留在队首，直到提交或被确定拒绝（§14.5 在线补偿）。
type storeOp struct {
	kind      storeOpKind
	attempt   NewAttempt
	verdict   Verdict
	control   ApplyControl
	attemptID string
	reason    string
	totalMs   int64
	stoppedAt time.Time
	started   bool
}

type storeDone struct {
	attempt   Attempt
	err       error
	runTimeMs *int64 // opAccountRunTime 之后重新读取的 run_time_ms（读取失败为 nil）
}

func (a *Actor) enqueue(op *storeOp) {
	a.storeQ = append(a.storeQ, op)
	a.pumpStore()
}

func (a *Actor) pumpStore() {
	if a.storeBusy || a.storeRetryAt != nil || len(a.storeQ) == 0 {
		return
	}
	op := a.storeQ[0]
	op.started, a.storeBusy = true, true
	cp, taskID := *op, a.taskID
	a.async(func(ctx context.Context) any {
		var r storeDone
		switch cp.kind {
		case opCreateAttempt:
			faultinject.Point(faultinject.AttemptCreateBefore)
			r.attempt, r.err = a.d.Store.CreateAttempt(ctx, cp.attempt)
			faultinject.Point(faultinject.AttemptCreateAfter)
		case opFinalize:
			faultinject.Point(faultinject.VerdictBefore)
			r.attempt, r.err = a.d.Store.FinalizeAttempt(ctx, cp.verdict)
			faultinject.Point(faultinject.VerdictAfter)
		case opFailTurn:
			ts, ok := a.d.Store.(TurnStore)
			if !ok {
				r.err = fmt.Errorf("%w: Store 没有实现 TurnStore", persistence.ErrInvalid)
				break
			}
			r.err = ts.FailQueuedTurn(ctx, taskID, cp.reason)
		case opApplyControl:
			_, r.err = a.d.Store.ApplyControl(ctx, cp.control)
		case opPersistRunTime:
			_, r.err = a.d.Store.PersistRunTime(ctx, taskID, cp.attemptID, cp.totalMs)
		case opRevoke:
			// 补提交的撤销：提交（或被确定拒绝）之后才关闭 Gateway 入口（与 stopEnv 相同的顺序）。
			r.err = a.d.Store.RevokeAttemptAccess(ctx, cp.attemptID, cp.reason)
			if r.err == nil || definitive(r.err) {
				// 关闭入口只是清理：失败（例如删除 socket 文件失败）不影响已提交的撤销，也不重试。
				_ = a.d.Access.Revoke(ctx, cp.attemptID, cp.reason)
			}
		case opRevokeGateway:
			// 关闭入口只是清理：失败（例如删除 socket 文件失败）不重试，停止时的撤销会再次关闭。
			_ = a.d.Access.Revoke(ctx, cp.attemptID, cp.reason)
		case opAccountRunTime:
			if r.err = a.d.OnStopRecorded(ctx, cp.attemptID, cp.stoppedAt); r.err == nil {
				if ts, err := a.d.Store.LoadTask(ctx, taskID); err == nil { // 限额以计入后的累计为准
					r.runTimeMs = &ts.RunTimeMs
				}
			}
		}
		return r
	})
}

func (a *Actor) handleStore(r storeDone) {
	a.storeBusy = false
	op := a.storeQ[0]
	switch {
	case r.err == nil:
		a.storeQ, a.storeFailures = a.storeQ[1:], 0
		a.committed(op, r)
	case errors.Is(r.err, persistence.ErrOwnershipLost), errors.Is(r.err, persistence.ErrInvalid):
		a.die(fmt.Errorf("task: 任务 %s 的持久化操作 %d: %w", a.taskID, op.kind, r.err))
		return
	case definitive(r.err):
		a.storeQ, a.storeFailures = a.storeQ[1:], 0
		a.rejected(op, r.err)
	case a.ctx.Err() != nil:
		return
	default:
		at := a.d.Clock.Now().Add(RetryBackoff(a.storeFailures, a.d.Jitter()))
		a.storeFailures++
		a.storeRetryAt = &at
	}
	a.pumpStore()
}

func (a *Actor) committed(op *storeOp, r storeDone) {
	switch op.kind {
	case opCreateAttempt:
		na := op.attempt
		a.attemptsTotal = max(a.attemptsTotal, na.AttemptNo)
		a.attemptNo[na.AttemptID] = na.AttemptNo
		a.apply(AttemptCreated{AttemptID: na.AttemptID, EnvID: na.EnvID, Status: r.attempt.Status})
	case opFinalize:
		a.apply(VerdictCommitted{Verdict: op.verdict, CommittedSessionCheckpointID: r.attempt.CommittedSessionCheckpointID})
	case opAccountRunTime:
		if r.runTimeMs != nil && a.rt.openAt == nil {
			a.rt.base = max(a.rt.base, *r.runTimeMs)
		}
	}
}

// rejected：写入被确定拒绝（前置条件不满足、冲突）。创建与判决交给 Decide（StoreFailed），并重新读取
// 控制，以便按最新控制重算；控制写入被拒（control_changed）同样重新读取控制。
func (a *Actor) rejected(op *storeOp, err error) {
	switch op.kind {
	case opCreateAttempt:
		a.apply(StoreFailed{Op: OpCreateAttempt, AttemptID: op.attempt.AttemptID, Err: err})
		a.readControl()
	case opFinalize:
		a.apply(StoreFailed{Op: OpFinalize, AttemptID: op.verdict.AttemptID, ControlVersion: op.verdict.ControlVersion, Err: err})
		a.readControl()
	case opApplyControl:
		a.readControl()
	case opFailTurn: // 只有 actor 改变 queued 的 turn：被拒说明事实不一致
		a.die(fmt.Errorf("task: 会话 turn %s 的失败裁决被拒绝: %w", a.taskID, err))
	} // 运行时间（stale_attempt）与访问撤销被拒：事实已不适用
}

// persistRunTime 把当前累计运行时间加入 Store 队列；队列中尚未开始的同类写入直接更新为较大值。
func (a *Actor) persistRunTime(now time.Time) {
	if a.rt.attemptID == "" {
		return
	}
	total := a.rt.total(now)
	for _, op := range a.storeQ {
		if op.kind == opPersistRunTime && !op.started && op.attemptID == a.rt.attemptID {
			op.totalMs = max(op.totalMs, total)
			return
		}
	}
	a.enqueue(&storeOp{kind: opPersistRunTime, attemptID: a.rt.attemptID, totalMs: total})
}

// definitive 报告 Store 错误是否是确定的结果（不再以同一身份重试）。
func definitive(err error) bool {
	return errors.Is(err, persistence.ErrRejected) || errors.Is(err, persistence.ErrConflict) ||
		errors.Is(err, persistence.ErrNotFound) || errors.Is(err, persistence.ErrInvalid) ||
		errors.Is(err, persistence.ErrOwnershipLost)
}

// readRetry 执行只读用例，暂时失败时按退避重试，直到成功、确定失败或 ctx 结束。
func readRetry[T any](ctx context.Context, d Deps, f func(context.Context) (T, error)) (T, error) {
	for n := int64(0); ; n++ {
		v, err := f(ctx)
		if err == nil || ctx.Err() != nil || definitive(err) {
			return v, err
		}
		c, stop := d.Clock.At(d.Clock.Now().Add(RetryBackoff(n, d.Jitter())))
		select {
		case <-c:
			stop()
		case <-ctx.Done():
			stop()
			return v, ctx.Err()
		}
	}
}
