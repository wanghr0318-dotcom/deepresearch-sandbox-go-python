package task

// 本文件是 task 与 attempt 状态机的纯决策函数（规格 §8.1、§8.2；代码组织 §5）。Decide 只读取
// State 与 Event，返回新状态与封闭集合内的副作用；时间与抖动由调用方经 State.Now、State.Jitter
// 注入，不读取时钟、不使用全局随机数、不做 I/O。task actor 依次执行副作用，异步结果以带
// attempt_id 的事件回到 Decide。

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// 规格 §19 的默认参数。
const (
	MaxOOMRetries    = 1               // worker_oom_likely 至多重试一次（§14.3）
	RetryBackoffBase = 2 * time.Second // 重试退避基数
	RetryBackoffCap  = 60 * time.Second
	// DefaultControlGraceMs 是 State.ControlGraceMs 为 0 时 cancel/pause 的 grace_ms。规格未单列该值，
	// 取与 exit_grace 相同的 10 s。
	DefaultControlGraceMs = 10_000
)

// Decide 读取的 outcome_class（分类本身只由 runner.Classify 给出；Decide 只按类别裁决）。
const (
	ClassSucceeded        = "succeeded"
	ClassOOMObserved      = "oom_observed_in_attempt" // 正常结束但观察到 OOM：按 Worker 结果裁决（§14.3）
	ClassDeadlineExceeded = "task_deadline_exceeded"  // 累计运行时限超限：任务级事实，新 attempt 不重置，不重试
	ClassCancelled        = "cancelled"               // 宿主意图：执行尚未启动时由 Decide 直接给出
	ClassPaused           = "paused"
)

// State 是 Decide 读取的任务事实。Attempt 是任务的当前 attempt（可能已 ended）；nil 表示从未创建。
type State struct {
	TaskID                                string
	TaskStatus                            string // queued | running | pausing | cancelling | paused | succeeded | failed | cancelled
	Desired                               string // run | pause | cancel
	ControlVersion, AppliedControlVersion int64
	Attempt                               *AttemptState
	FaultRetriesUsed, MaxFaultRetries     int64
	OOMRetriesUsed                        int64
	// NextRetry 是下一次创建 attempt 的重试类别：故障重试裁决提交后取该 attempt 的 Outcome.Retry，
	// 创建 attempt 后清空（§8.1"重试计数"）。
	NextRetry RetryKind
	NotBefore *time.Time // 下一次 attempt 的最早时间
	Now       time.Time
	// Jitter ∈ [0, 1) 是本次决策使用的退避抖动，由调用方提供（越界时截断）。
	Jitter         float64
	ControlGraceMs int64 // cancel/pause 的 grace_ms；0 取 DefaultControlGraceMs
	SlotRequested  bool  // 已发出 RequestSlot，尚未收到 SlotGranted
	SlotHeld       bool  // 持有 run slot：从授予到执行环境确认停止（§14.5）
}

// AttemptState 是当前 attempt 的事实。Status 取值 starting | handshaking | active | finishing |
// ended | stop_blocked：starting 为 attempt 已提交、环境创建中；handshaking 为环境已创建、Worker
// 启动与握手中；active 为 Worker 已就绪；finishing 为结果已知、判决待提交。规格 §8.2 的 created
// 是 attempt 提交之前的阶段，由 State.SlotHeld 且无活动 attempt 表示，不出现在这里。
type AttemptState struct {
	AttemptID, EnvID, Status string
	StoredStatus             string // attempts.status 的持久化值；判决以它作为 CAS 来源（Verdict.FromStatus）
	EnvStopped               bool   // 环境已记录 stopped_at
	StopRequested            bool   // 已发出 RevokeAccess 与 StopEnvironment
	StopFailures             int64  // 连续无法确认停止的次数（退避用）
	StopRetryAt              *time.Time
	Outcome                  *Outcome // finishing 及之后为 attempt 结果；stop_blocked 时为恢复给出的结果
	Verdict                  *Verdict // finishing：待提交的判决（nil 表示需重算）；ended：已提交的判决
}

// Outcome 是 Decide 使用的 attempt 结果（纯数据）；actor 把 runner.Outcome 转换为它，Decide 不导入 runner。
type Outcome struct {
	Class            string
	Retry            RetryKind // runner.Classify 给出的重试资格；"" 表示不重试
	ProposalKind     string    // result | error | paused | ""
	Result           json.RawMessage
	ExitCode         *int64
	ExitSignal       *int64
	OOMKillDelta     int64
	PlatformKilled   bool
	OutputIncomplete bool
}

// Event 是 Decide 的输入（封闭集合）。
type Event interface{ isEvent() }

// Tick 表示时间推进（WakeAt 到期或周期检查）：重新评估启动条件、到期的停止重试与需重算的判决。
type Tick struct{}

// ControlChanged 是从 Store 读取的最新控制意图。
type ControlChanged struct {
	Desired        string
	ControlVersion int64
}

// SlotGranted 表示 admission 授予了 run slot。
type SlotGranted struct{}

// AttemptCreated 表示 CreateAttempt 事务已提交；Status 为 Store 返回的 attempts.status。
type AttemptCreated struct{ AttemptID, EnvID, Status string }

// EnvCreated 是环境创建的结果。Err 非 nil 时 Outcome 为 runner.Classify 给出的失败结果。
type EnvCreated struct {
	AttemptID, EnvID string
	Err              error
	Outcome          *Outcome
}

// WorkerStarted 表示 Worker 已启动并完成握手；Err 非 nil 时 Outcome 为分类后的失败结果，
// 且该 attempt 不会再有 AttemptFinished。
type WorkerStarted struct {
	AttemptID, EnvID string
	Err              error
	Outcome          *Outcome
}

// AttemptFinished 是 AttemptRunner 完成屏障之后的结果。
type AttemptFinished struct {
	AttemptID, EnvID string
	Outcome          Outcome
}

// VerdictCommitted 表示 FinalizeAttempt 已提交该判决。
type VerdictCommitted struct{ Verdict Verdict }

// EnvStopped 表示环境已确认停止并记录 stopped_at。
type EnvStopped struct{ AttemptID, EnvID string }

// StopUnconfirmed 表示停止无法确认（ErrStopUnconfirmed）；环境继续占用槽位并阻止替代执行。
type StopUnconfirmed struct{ AttemptID, EnvID string }

// StoreOp 是 StoreFailed 报告的事务用例。
type StoreOp string

const (
	OpCreateAttempt StoreOp = "create_attempt"
	OpFinalize      StoreOp = "finalize"
)

// StoreFailed 表示一次 Store 写入确定没有提交（被拒绝，或 actor 的退避重试已放弃）。提交结果
// 未知（CommitUnknownError）不属于此事件：actor 应重新 LoadTask。ControlVersion 用于 OpFinalize，
// 标识被拒绝的判决。
type StoreFailed struct {
	Op             StoreOp
	AttemptID      string
	ControlVersion int64
	Err            error
}

func (Tick) isEvent()             {}
func (ControlChanged) isEvent()   {}
func (SlotGranted) isEvent()      {}
func (AttemptCreated) isEvent()   {}
func (EnvCreated) isEvent()       {}
func (WorkerStarted) isEvent()    {}
func (AttemptFinished) isEvent()  {}
func (VerdictCommitted) isEvent() {}
func (EnvStopped) isEvent()       {}
func (StopUnconfirmed) isEvent()  {}
func (StoreFailed) isEvent()      {}

// Decision 是 Decide 的输出：新状态与按顺序执行的副作用。
type Decision struct {
	Next    State
	Effects []Effect
}

// Effect 是 actor 要执行的副作用（封闭集合）。
type Effect interface{ isEffect() }

// RequestSlot 向 admission 申请 run slot；结果为 SlotGranted。
type RequestSlot struct{}

// ReleaseSlot 归还持有的 run slot。
type ReleaseSlot struct{}

// CreateAttempt 以 actor 生成的 attempt_id、env_id 调用 Store.CreateAttempt；结果为 AttemptCreated 或
// StoreFailed{OpCreateAttempt}。
type CreateAttempt struct{ Retry RetryKind }

// CreateEnvironment 经 coordinator 创建任务环境；结果为 EnvCreated。
type CreateEnvironment struct{ AttemptID, EnvID string }

// StartWorker 启动 AttemptRunner；结果为 WorkerStarted 与 AttemptFinished。
type StartWorker struct{ AttemptID, EnvID string }

// SendControl 把 cancel/pause 交给 AttemptRunner（协议消息与 grace 到期终止由 runner 执行）。
type SendControl struct {
	AttemptID, Kind string
	GraceMs         int64
}

// StopEnvironment 经 coordinator 停止环境；结果为 EnvStopped 或 StopUnconfirmed。
type StopEnvironment struct{ AttemptID, EnvID string }

// RevokeAccess 撤销 attempt 的访问（Store 与 Gateway）；在停止之前执行。
type RevokeAccess struct{ AttemptID, Reason string }

// Finalize 以 Store.FinalizeAttempt 提交判决；结果为 VerdictCommitted 或 StoreFailed{OpFinalize}。
type Finalize struct{ Verdict Verdict }

// WakeAt 要求 actor 在该时间发送 Tick。
type WakeAt struct{ Time time.Time }

// ApplyControl（定义于 store.go，也是 Store.ApplyControl 的输入）作为副作用：以 CAS 写入控制转换。
func (ApplyControl) isEffect() {}

func (RequestSlot) isEffect()       {}
func (ReleaseSlot) isEffect()       {}
func (CreateAttempt) isEffect()     {}
func (CreateEnvironment) isEffect() {}
func (StartWorker) isEffect()       {}
func (SendControl) isEffect()       {}
func (StopEnvironment) isEffect()   {}
func (RevokeAccess) isEffect()      {}
func (Finalize) isEffect()          {}
func (WakeAt) isEffect()            {}

// ErrInvalid 表示状态与事件的组合不合法（actor 的程序错误或事实不一致）。
var ErrInvalid = errors.New("task: 不合法的状态或事件")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Decide 计算事件 e 作用于状态 s 的新状态与副作用。不合法的组合返回 ErrInvalid，Next 为原状态。
func Decide(s State, e Event) (Decision, error) {
	if err := validateState(s); err != nil {
		return Decision{Next: s}, err
	}
	d := &decider{s: s}
	if s.Attempt != nil {
		a := *s.Attempt // 不修改调用方持有的 attempt
		d.s.Attempt = &a
	}
	if err := d.dispatch(e); err != nil {
		return Decision{Next: s}, err
	}
	return Decision{Next: d.s, Effects: d.effects}, nil
}

type decider struct {
	s       State
	effects []Effect
}

func (d *decider) emit(e ...Effect) { d.effects = append(d.effects, e...) }

func (d *decider) dispatch(e Event) error {
	switch e := e.(type) {
	case Tick:
		return d.tick()
	case ControlChanged:
		return d.controlChanged(e)
	case SlotGranted:
		return d.slotGranted()
	case AttemptCreated:
		return d.attemptCreated(e)
	case EnvCreated:
		return d.envCreated(e)
	case WorkerStarted:
		return d.workerStarted(e)
	case AttemptFinished:
		return d.attemptFinished(e)
	case VerdictCommitted:
		return d.verdictCommitted(e)
	case EnvStopped:
		return d.envStopped(e)
	case StopUnconfirmed:
		return d.stopUnconfirmed(e)
	case StoreFailed:
		return d.storeFailed(e)
	}
	return invalid("未知事件 %T", e)
}

// ---- 事件处理 ----

func (d *decider) tick() error {
	if a := d.s.Attempt; a != nil && !a.EnvStopped {
		switch {
		case a.Status == "stop_blocked" && !a.StopRequested: // 恢复交来的 stop_blocked：重新发起停止
			d.stop()
		case a.StopRetryAt != nil && !d.s.Now.Before(*a.StopRetryAt):
			a.StopRetryAt = nil
			d.emit(StopEnvironment{AttemptID: a.AttemptID, EnvID: a.EnvID})
		}
	}
	// 未应用的控制，以及被拒绝后需按最新控制重算的判决（finishing 且 Verdict 为 nil）。
	if err := d.reconcileControl(); err != nil {
		return err
	}
	d.maybeStart()
	return nil
}

func (d *decider) controlChanged(e ControlChanged) error {
	switch e.Desired {
	case "run", "pause", "cancel":
	default:
		return invalid("desired %q 未定义", e.Desired)
	}
	if e.ControlVersion <= d.s.ControlVersion { // 重复或更旧的读取
		return nil
	}
	d.s.Desired, d.s.ControlVersion = e.Desired, e.ControlVersion
	return d.reconcileControl()
}

func (d *decider) slotGranted() error {
	if !d.s.SlotRequested || d.s.SlotHeld {
		return invalid("未请求槽位或已持有槽位时收到授予")
	}
	d.s.SlotRequested, d.s.SlotHeld = false, true
	if d.canStart() {
		d.emit(CreateAttempt{Retry: d.s.NextRetry})
		return nil
	}
	d.releaseSlot() // 等待期间控制或时间条件已变化：归还授予（§3.2）
	return nil
}

func (d *decider) attemptCreated(e AttemptCreated) error {
	if !d.creating() {
		return invalid("没有进行中的 CreateAttempt 时收到 attempt %s", e.AttemptID)
	}
	if e.AttemptID == "" || e.EnvID == "" || e.Status == "" {
		return invalid("AttemptCreated 缺少 attempt_id、env_id 或 status")
	}
	switch d.s.NextRetry { // 与 Store 在同一事务中的递增一致（§8.1"重试计数"）
	case RetryFault:
		d.s.FaultRetriesUsed++
	case RetryOOM:
		d.s.OOMRetriesUsed++
	}
	d.s.NextRetry, d.s.NotBefore = RetryNone, nil
	d.s.TaskStatus = "running" // Store 已 CAS queued → running
	d.s.Attempt = &AttemptState{AttemptID: e.AttemptID, EnvID: e.EnvID, Status: "starting", StoredStatus: e.Status}
	// 创建期间被接受的控制在此应用；cancel/pause 使 attempt 不再创建环境而直接结束。
	if err := d.reconcileControl(); err != nil {
		return err
	}
	if d.s.Attempt.Status == "starting" {
		d.emit(CreateEnvironment{AttemptID: e.AttemptID, EnvID: e.EnvID})
	}
	return nil
}

func (d *decider) envCreated(e EnvCreated) error {
	a := d.current(e.AttemptID)
	if a == nil { // 过期：迟到的创建结果交给清理
		d.emit(StopEnvironment{AttemptID: e.AttemptID, EnvID: e.EnvID})
		return nil
	}
	switch {
	case a.Status == "starting" && e.Err == nil:
		a.Status = "handshaking"
		d.emit(StartWorker{AttemptID: a.AttemptID, EnvID: a.EnvID})
		return nil
	case a.Status == "starting":
		return d.failBeforeActive(e.Err, e.Outcome)
	case a.StopRequested: // 创建期间已决定停止；停止在 coordinator 中排在创建之后
		return nil
	}
	return invalid("attempt %s 处于 %s 时收到 EnvCreated", a.AttemptID, a.Status)
}

func (d *decider) workerStarted(e WorkerStarted) error {
	a := d.current(e.AttemptID)
	if a == nil {
		d.emit(StopEnvironment{AttemptID: e.AttemptID, EnvID: e.EnvID})
		return nil
	}
	switch {
	case a.Status == "handshaking" && e.Err != nil:
		return d.failBeforeActive(e.Err, e.Outcome)
	case a.StopRequested: // 已直接停止：等待 AttemptFinished
		return nil
	case a.Status == "handshaking":
		a.Status = "active"
		return nil
	}
	return invalid("attempt %s 处于 %s 时收到 WorkerStarted", a.AttemptID, a.Status)
}

func (d *decider) attemptFinished(e AttemptFinished) error {
	a := d.current(e.AttemptID)
	if a == nil { // 过期：只做资源转交
		d.emit(StopEnvironment{AttemptID: e.AttemptID, EnvID: e.EnvID})
		return nil
	}
	if a.Status != "handshaking" && a.Status != "active" {
		return invalid("attempt %s 处于 %s 时收到 AttemptFinished", a.AttemptID, a.Status)
	}
	if err := validateOutcome(e.Outcome); err != nil {
		return err
	}
	d.finish(e.Outcome)
	return nil
}

func (d *decider) verdictCommitted(e VerdictCommitted) error {
	v := e.Verdict
	a := d.current(v.AttemptID)
	if a == nil || a.Status == "ended" { // 过期或重复
		return nil
	}
	if a.Status != "finishing" {
		return invalid("attempt %s 处于 %s 时收到判决提交", a.AttemptID, a.Status)
	}
	if !knownStatus(v.TaskStatus) {
		return invalid("判决的任务状态 %q 未定义", v.TaskStatus)
	}
	// 已提交的判决是事实：即使它基于较旧的控制版本（该判决先于新控制提交，§8.1），也以它为准。
	a.Status, a.Verdict = "ended", &v
	d.s.TaskStatus, d.s.NotBefore = v.TaskStatus, v.NotBefore
	d.s.AppliedControlVersion = max(d.s.AppliedControlVersion, v.ControlVersion)
	d.s.NextRetry = RetryNone
	if v.TaskStatus == "queued" {
		d.s.NextRetry = a.Outcome.Retry
	}
	if err := d.reconcileControl(); err != nil {
		return err
	}
	d.maybeStart()
	return nil
}

func (d *decider) envStopped(e EnvStopped) error {
	a := d.current(e.AttemptID)
	if a == nil || a.EnvStopped {
		return nil
	}
	a.EnvStopped, a.StopRequested, a.StopRetryAt = true, true, nil
	if d.s.SlotHeld { // 只有确认停止后才归还容量（§14.5）
		d.releaseSlot()
	}
	if a.Status == "stop_blocked" { // 确认停止后按恢复给出的结果裁决（§8.2：stop_blocked → ended）
		if a.Outcome == nil {
			return invalid("stop_blocked 的 attempt %s 没有结果", a.AttemptID)
		}
		d.finish(*a.Outcome)
	}
	d.maybeStart()
	return nil
}

func (d *decider) stopUnconfirmed(e StopUnconfirmed) error {
	a := d.current(e.AttemptID)
	if a == nil || a.EnvStopped {
		return nil
	}
	at := d.s.Now.Add(RetryBackoff(a.StopFailures, d.s.Jitter))
	a.StopFailures++
	a.StopRequested, a.StopRetryAt = true, &at
	d.emit(WakeAt{Time: at})
	return nil
}

func (d *decider) storeFailed(e StoreFailed) error {
	switch e.Op {
	case OpCreateAttempt:
		if !d.creating() {
			return nil
		}
		d.releaseSlot()
		if d.s.ControlVersion > d.s.AppliedControlVersion { // 多半因控制变化被拒（not_runnable）
			return d.reconcileControl()
		}
		at := d.s.Now.Add(RetryBackoff(0, d.s.Jitter))
		d.s.NotBefore = &at
		d.emit(WakeAt{Time: at})
		return nil
	case OpFinalize:
		a := d.current(e.AttemptID)
		if a == nil || a.Status != "finishing" || a.Verdict == nil || a.Verdict.ControlVersion != e.ControlVersion {
			return nil // 已被更新的判决取代
		}
		a.Verdict = nil // 下一次 Tick 按最新控制重算
		d.emit(WakeAt{Time: d.s.Now.Add(RetryBackoff(0, d.s.Jitter))})
		return nil
	}
	return invalid("StoreFailed 的操作 %q 未定义", e.Op)
}

// ---- 规则 ----

// reconcileControl 应用尚未应用的控制（§8.1"控制写入"、"cancel 优先于 pause"）。
func (d *decider) reconcileControl() error {
	if IsTerminal(d.s.TaskStatus) || d.creating() { // 终态不再接受控制；创建中等待结果后再应用
		return nil
	}
	a := d.s.Attempt
	if a != nil && a.Status == "finishing" { // 判决按最新控制计算；FinalizeAttempt 同时推进已应用版本
		if a.Verdict == nil || a.Verdict.ControlVersion != d.s.ControlVersion {
			d.finalize()
		}
		return nil
	}
	if d.s.ControlVersion <= d.s.AppliedControlVersion {
		return nil
	}
	prev := d.s.TaskStatus
	next, ok := ControlTransition(prev, d.s.Desired)
	if !ok {
		return invalid("任务处于 %s 时不能应用 desired = %s", prev, d.s.Desired)
	}
	d.s.TaskStatus, d.s.AppliedControlVersion = next, d.s.ControlVersion
	d.emit(ApplyControl{TaskID: d.s.TaskID, ControlVersion: d.s.ControlVersion, Status: next, StatusReason: d.s.Desired + "_requested"})
	if next != prev && (next == "pausing" || next == "cancelling") && live(a) {
		d.interrupt()
	}
	d.maybeStart()
	return nil
}

// interrupt 对活动 attempt 执行 cancel/pause：Worker 就绪前直接停止执行（§8.1），就绪后交给 runner。
func (d *decider) interrupt() {
	a := d.s.Attempt
	switch a.Status {
	case "starting": // 环境尚在创建、Worker 未启动：按宿主意图直接结束
		class := ClassPaused
		if d.s.Desired == "cancel" {
			class = ClassCancelled
		}
		d.finish(Outcome{Class: class})
	case "handshaking":
		d.stop()
	case "active":
		if !a.StopRequested {
			grace := d.s.ControlGraceMs
			if grace == 0 {
				grace = DefaultControlGraceMs
			}
			d.emit(SendControl{AttemptID: a.AttemptID, Kind: d.s.Desired, GraceMs: grace})
		}
	} // stop_blocked：没有可控制的执行，确认停止后按 desired 裁决
}

// finish 记录 attempt 结果、撤销访问并停止环境，提交判决（§8.2：active 之前的失败也总是发起停止）。
func (d *decider) finish(o Outcome) {
	a := d.s.Attempt
	a.Status, a.Outcome, a.Verdict = "finishing", &o, nil
	d.stop()
	d.finalize()
}

func (d *decider) failBeforeActive(err error, o *Outcome) error {
	if o == nil {
		return invalid("失败事件缺少分类结果: %v", err)
	}
	if err := validateOutcome(*o); err != nil {
		return err
	}
	d.finish(*o)
	return nil
}

func (d *decider) stop() {
	a := d.s.Attempt
	if a.StopRequested || a.EnvStopped {
		return
	}
	a.StopRequested = true
	d.emit(RevokeAccess{AttemptID: a.AttemptID, Reason: "attempt_stopping"},
		StopEnvironment{AttemptID: a.AttemptID, EnvID: a.EnvID})
}

func (d *decider) finalize() {
	v := d.verdict(*d.s.Attempt.Outcome)
	d.s.Attempt.Verdict = &v
	d.emit(Finalize{Verdict: v})
}

// verdict 是 §8.1"最终裁决"：读取 desired；run 时按结果类别与重试资格（§5.8、§14.3）。
func (d *decider) verdict(o Outcome) Verdict {
	a := d.s.Attempt
	v := Verdict{AttemptID: a.AttemptID, TaskID: d.s.TaskID, ControlVersion: d.s.ControlVersion,
		FromStatus: a.StoredStatus, AttemptStatus: "ended", OutcomeClass: o.Class,
		ExitCode: o.ExitCode, ExitSignal: o.ExitSignal, OOMKillDelta: o.OOMKillDelta, PlatformKilled: o.PlatformKilled}
	valid := validResult(o)
	switch {
	case d.s.Desired == "cancel":
		v.TaskStatus, v.TaskStatusReason = "cancelled", "cancelled"
		if valid {
			v.TaskStatusReason, v.Result = "completed_during_cancel", o.Result
		}
	case d.s.Desired == "pause":
		v.TaskStatus, v.TaskStatusReason = "paused", "paused"
		if valid {
			v.TaskStatusReason, v.Result = "completed_during_pause", o.Result
		}
	case valid:
		v.TaskStatus, v.TaskStatusReason, v.Result = "succeeded", o.Class, o.Result
	case d.retryAllowed(o):
		at := d.s.Now.Add(RetryBackoff(d.s.FaultRetriesUsed+d.s.OOMRetriesUsed, d.s.Jitter))
		v.TaskStatus, v.TaskStatusReason, v.NotBefore = "queued", o.Class, &at
	default:
		v.TaskStatus, v.TaskStatusReason = "failed", o.Class
	}
	v.EventType = "attempt_ended"
	if IsTerminal(v.TaskStatus) {
		v.EventType = "task_terminal"
	}
	v.EventPayload, _ = json.Marshal(verdictEvent{AttemptID: v.AttemptID, OutcomeClass: v.OutcomeClass, TaskStatus: v.TaskStatus,
		StatusReason: v.TaskStatusReason, PlatformKilled: o.PlatformKilled, OutputIncomplete: o.OutputIncomplete})
	return v
}

type verdictEvent struct {
	AttemptID        string `json:"attempt_id"`
	OutcomeClass     string `json:"outcome_class"`
	TaskStatus       string `json:"task_status"`
	StatusReason     string `json:"status_reason"`
	PlatformKilled   bool   `json:"platform_killed"`
	OutputIncomplete bool   `json:"output_incomplete"`
}

// validResult：Worker 的 result 已被接受为有效结果（分类为成功）。
func validResult(o Outcome) bool {
	return o.ProposalKind == "result" && len(o.Result) > 0 && (o.Class == ClassSucceeded || o.Class == ClassOOMObserved)
}

// retryAllowed 是重试计数规则（§8.1、§14.3）：资格来自 runner.Classify 的 Retry；所有重试受
// max_fault_retries 约束；OOM 重试另受 oom_retries_used 上限约束；累计运行时限超限不重试。
func (d *decider) retryAllowed(o Outcome) bool {
	switch {
	case o.Retry == RetryNone, o.Class == ClassDeadlineExceeded:
		return false
	case d.s.FaultRetriesUsed >= d.s.MaxFaultRetries:
		return false
	case o.Retry == RetryOOM:
		return d.s.OOMRetriesUsed < MaxOOMRetries
	}
	return true
}

// maybeStart 在满足创建前置条件时申请槽位；not_before 未到时安排唤醒。
func (d *decider) maybeStart() {
	if d.s.SlotRequested || d.s.SlotHeld || !d.startable() {
		return
	}
	if d.s.NotBefore != nil && d.s.Now.Before(*d.s.NotBefore) {
		d.emit(WakeAt{Time: *d.s.NotBefore})
		return
	}
	d.s.SlotRequested = true
	d.emit(RequestSlot{})
}

// startable 是创建 attempt 的前置条件（§8.1）中除时间外的部分：queued、desired = run、
// 无未停止的旧执行（stop_blocked 与未确认停止的环境阻止替代执行，§2）。
func (d *decider) startable() bool {
	a := d.s.Attempt
	return d.s.TaskStatus == "queued" && d.s.Desired == "run" && d.s.ControlVersion == d.s.AppliedControlVersion &&
		(a == nil || (a.Status == "ended" && a.EnvStopped))
}

func (d *decider) canStart() bool {
	return d.startable() && (d.s.NotBefore == nil || !d.s.Now.Before(*d.s.NotBefore))
}

// creating：已持有槽位且没有阻止执行的 attempt，即 CreateAttempt 已发出、结果未到。
func (d *decider) creating() bool {
	a := d.s.Attempt
	return d.s.TaskStatus == "queued" && d.s.SlotHeld && (a == nil || (a.Status == "ended" && a.EnvStopped))
}

func (d *decider) releaseSlot() {
	d.s.SlotHeld = false
	d.emit(ReleaseSlot{})
}

// current 返回 attemptID 对应的当前 attempt；非当前（过期）返回 nil。
func (d *decider) current(attemptID string) *AttemptState {
	if a := d.s.Attempt; a != nil && a.AttemptID == attemptID {
		return a
	}
	return nil
}

func live(a *AttemptState) bool { return a != nil && a.Status != "ended" }

// RetryBackoff 是第 n 次（从 0 起）重试前的退避：min(基数·2^n, 上限)，抖动取其后一半
// （jitter ∈ [0, 1)，越界截断），即结果落在 [d/2, d)。
func RetryBackoff(n int64, jitter float64) time.Duration {
	d := RetryBackoffCap
	if n >= 0 && n < 6 {
		d = min(RetryBackoffBase<<n, RetryBackoffCap)
	}
	jitter = min(max(jitter, 0), 0.999999)
	half := d / 2
	return half + time.Duration(float64(half)*jitter)
}

// ---- 校验 ----

func knownStatus(s string) bool {
	switch s {
	case "queued", "running", "pausing", "cancelling", "paused", "succeeded", "failed", "cancelled":
		return true
	}
	return false
}

func validateState(s State) error {
	if !knownStatus(s.TaskStatus) {
		return invalid("任务状态 %q 未定义", s.TaskStatus)
	}
	switch s.Desired {
	case "run", "pause", "cancel":
	default:
		return invalid("desired %q 未定义", s.Desired)
	}
	a := s.Attempt
	if a != nil {
		switch a.Status {
		case "starting", "handshaking", "active", "ended", "stop_blocked":
		case "finishing":
			if a.Outcome == nil {
				return invalid("finishing 的 attempt %s 没有结果", a.AttemptID)
			}
		default:
			return invalid("attempt 状态 %q 未定义", a.Status)
		}
	}
	executing := s.TaskStatus == "running" || s.TaskStatus == "pausing" || s.TaskStatus == "cancelling"
	if executing != live(a) {
		return invalid("任务状态 %s 与当前 attempt 不一致", s.TaskStatus)
	}
	return nil
}

func validateOutcome(o Outcome) error {
	switch o.Retry {
	case RetryNone, RetryFault, RetryOOM:
	default:
		return invalid("重试类别 %q 未定义", o.Retry)
	}
	if o.Class == "" {
		return invalid("结果缺少 outcome_class")
	}
	return nil
}
