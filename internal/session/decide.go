package session

// 本文件是会话生命周期状态机的纯决策函数（规格 §12.2、§12.4、§12.6；M4 Plan 12 Task 7）。Decide 只读取
// ActorState 与 Event，返回新状态与封闭集合内的副作用；时间由调用方经 ActorState.Now 注入，不做 I/O。
//
// 决策是"对照事实收敛"：session actor 每完成一个操作都重新读取会话事实（Store.LoadSession），Decide 依据
// 最新事实与 actor 的本地事实（持有的 incarnation 句柄、等待授予的 turn、待处理的交还）选出**下一个**操作。
// 同一时刻至多一个操作在途（ActorState.Busy）：生命周期的步骤天然串行，操作结果以 OpDone 回到 Decide。
//
// 转换（§12.2）：
//
//	creating ──首个 Grant / wake──► StartIncarnation ──► idle
//	idle ──授予──► running（由 task 的 attempt 创建事务写入）──交还──► idle
//	idle ──IdleFreeze──► quiescing ──quiesced = Latest──► FreezeEnv 成功 ──► frozen（E31）
//	                         └─ 不等、超时、冻结失败 ──► evicting
//	frozen ──Grant / wake──► ThawEnv ──► idle；thaw 失败 ──► evicting ──► evicted ──► 冷恢复
//	frozen ──EvictAfter / 内存压力──► evicting（直接 StopEnv）──► evicted
//	evicted ──Grant / wake──► restoring ──► StartIncarnation(Resume = Latest) ──► idle；两次失败 ──► evicted
//	任意 ──desired = closed──► closing ──turn 全部终态──► Close ──► StopEnv（Recorded）──► 删除 workspace ──► closed

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// 默认参数（规格 §19、M4 设计 D4）。
const (
	DefaultIdleFreeze    = 10 * time.Minute
	DefaultEvictAfter    = time.Hour
	DefaultFreezeConfirm = 5 * time.Second
	DefaultQuiesceGrace  = 10 * time.Second
	DefaultCloseGrace    = 10 * time.Second
	// MaxStartTries 是一轮唤醒中启动（新建或冷恢复）incarnation 的次数上限：两次失败后会话回到 evicted，
	// 等待中的授予得到 ErrUnavailable（M4 设计"唤醒失败"）。
	MaxStartTries = 2
	// UnavailableMessage 是恢复失败时 session_state{evicted} 事件的 user_message。
	UnavailableMessage = "会话暂时无法恢复"
)

// incarnation 的 end_reason（incarnations.end_reason）。
const (
	EndReleaseTimeout = "release_timeout" // 释放核验失败（§12.4：incarnation 故障，不是任务故障）
	EndTaskDestroyed  = "task_destroyed"  // task actor 交还时要求销毁（故障、取消生效）
	EndWorkerExited   = "worker_exited"   // incarnation 进程意外退出（E28）
	EndQuiesceFailed  = "quiesce_failed"  // quiesce 超时，或报告的 checkpoint 不等于最新已提交 checkpoint
	EndFreezeFailed   = "freeze_failed"   // FreezeEnv 未确认（E31）
	EndThawFailed     = "thaw_failed"
	EndEvicted        = "evicted"         // 驱逐时限
	EndMemoryPressure = "memory_pressure" // 内存压力下的 LRU 驱逐
	EndClosed         = "closed"
	EndOrphaned       = "orphaned" // 存活的 incarnation 不属于本进程（启动时遗留或启动失败后的残留）
	EndStartFailed    = "start_failed"
)

// ActorState 是 Decide 读取的全部事实：Session 是最近一次读取的持久化事实，其余是 actor 的本地事实。
type ActorState struct {
	Now    time.Time
	Config Config

	Session  State
	Loaded   bool // Session 至少读取过一次
	NeedLoad bool // 需要重新读取（通知、请求到达、读取失败后）
	Busy     bool // 有在途操作（含读取）
	Failures int  // 连续失败的操作（退避）
	RetryAt  *time.Time

	// HandleInc 是本进程持有句柄的 incarnation（Workers.Start 返回）；空表示没有。
	HandleInc string
	Exited    bool // HandleInc 的进程已退出
	Quiesced  bool // HandleInc 已 quiesce，且报告的 checkpoint 等于最新已提交 checkpoint
	// QuiescedID 是最近一次成功 quiesce 报告的 session_checkpoint_id（随 frozen 事件持久化，I9）。
	QuiescedID string
	CloseSent  bool // 已对 HandleInc 发送 session_close
	ThawFail   bool // HandleInc 的 thaw 失败
	RelFail    bool // Handoffs[0] 的 Release 失败（释放核验未通过）

	Waiters   []string  // 等待授予的 turn（到达顺序）
	Granted   string    // 已授予、attempt 尚未创建的 turn
	GrantedAt time.Time // 授予时间：未使用的授予在 IdleFreeze 之后失效
	Handoffs  []Handoff // 待处理的交还（到达顺序）

	StartTries  int    // 本轮唤醒中启动 incarnation 失败的次数
	LastError   string // 最近一次启动失败的原因
	EvictWanted bool   // 内存压力要求驱逐
	EvictReason string // 进入 evicting 的原因（EndIncarnation 的 end_reason）
	// IdleSince 是最近一次进入 idle 的时间：Store 在离开 idle 时清空 idle_since，frozen 的驱逐时限仍自它起算。
	IdleSince *time.Time
	Closed    bool // 会话已 closed，actor 退出
}

// Event 是 Decide 的输入（封闭集合）。
type Event interface{ isEvent() }

// Loaded 是 LoadSession 的结果。
type Loaded struct{ State State }

// LoadFailed 表示读取失败（暂时性；退避后重读）。NotFound 表示会话不存在（视为已关闭）。
type LoadFailed struct {
	Err      error
	NotFound bool
}

// Tick 表示时间推进（WakeAt 到期）。
type Tick struct{}

// Notified 表示会话事实可能已变化（新 turn、控制、wake、close 之后）：重新读取。
type Notified struct{}

// GrantRequested 是 task actor 的授予申请。
type GrantRequested struct{ TaskID string }

// GrantWithdrawn 表示申请者已放弃（ctx 结束）。
type GrantWithdrawn struct{ TaskID string }

// HandoffRequested 是 task actor 在 turn 裁决提交之后（或执行须销毁时）交还 incarnation。
type HandoffRequested struct{ Handoff Handoff }

// PressureEvict 是内存压力下的驱逐请求（Scheduler.OnMemoryPressure 按 LRU 选中本会话）。
type PressureEvict struct{}

// IncarnationExited 表示 incarnation 的进程已退出（Incarnation.Exited 关闭）。
type IncarnationExited struct{ IncarnationID string }

// OpKind 是操作的种类。
type OpKind string

const (
	OpTransition  OpKind = "transition"
	OpStart       OpKind = "start"
	OpQuiesce     OpKind = "quiesce"
	OpFreeze      OpKind = "freeze"
	OpThaw        OpKind = "thaw"
	OpRelease     OpKind = "release"
	OpClose       OpKind = "close"
	OpDestroy     OpKind = "destroy"
	OpFinishClose OpKind = "finish_close"
)

// OpDone 是一个操作的结果。Err 是暂时性失败（退避后重新决策）；Conflict 表示事实已变化（Store 的 CAS 冲突或
// 前置条件不满足：按重新读取的事实立即重新决策）；Failed 是操作语义上的失败（quiesce 不一致、冻结未确认、
// thaw 失败、启动失败、释放核验失败），由 Decide 按状态机处理。State 是操作之后重新读取的会话事实（读取失败为
// nil）。
type OpDone struct {
	Op            OpKind
	To            string // OpTransition 的目标状态
	Err           error
	Conflict      bool
	Failed        error
	State         *State
	IncarnationID string // OpStart 成功时的新 incarnation；OpDestroy 结束的 incarnation
	CheckpointID  string // OpQuiesce 成功时 quiesced 报告的 session_checkpoint_id（空串 = 会话尚无 checkpoint）
}

func (Loaded) isEvent()            {}
func (LoadFailed) isEvent()        {}
func (Tick) isEvent()              {}
func (Notified) isEvent()          {}
func (GrantRequested) isEvent()    {}
func (GrantWithdrawn) isEvent()    {}
func (HandoffRequested) isEvent()  {}
func (PressureEvict) isEvent()     {}
func (IncarnationExited) isEvent() {}
func (OpDone) isEvent()            {}

// Decision 是 Decide 的输出。Err 非 nil 表示状态与事件的组合不合法（actor 的程序错误或事实不一致），
// 此时 Next 为原状态、没有副作用。
type Decision struct {
	Next    ActorState
	Effects []Effect
	Err     error
}

// Effect 是 actor 要执行的副作用（封闭集合）。操作类副作用（Load、DoTransition、StartIncarnation 等）每个决策
// 至多一个，结果以 Loaded/LoadFailed/OpDone 回到 Decide；ReplyGrant、ReplyHandoff、WakeAt、Exit 同步执行。
type Effect interface{ isEffect() }

// Load 读取会话事实。
type Load struct{}

// DoTransition 以 row_version CAS 改变会话状态（Store.Transition）。
type DoTransition struct{ T Transition }

// StartIncarnation 新建 incarnation：申请内存 → CreateIncarnation → BindIncarnation →（state_ref 时 StageRestore）→
// CreateIncarnationEnv → Workers.Start → UnstageRestore → incarnation idle。Resume 为 nil 表示新会话。失败时已清理
// 本次创建的资源（环境未确认停止时 incarnation 保持存活，由下一次决策按 EndOrphaned 销毁）。
type StartIncarnation struct {
	Resume         *Checkpoint
	FromRowVersion int64
}

// QuiesceIncarnation：incarnation idle → quiescing，发送 quiesce；报告的 checkpoint 须等于 Expect（Latest 的 ID，
// 没有为空串）。
type QuiesceIncarnation struct{ IncarnationID, Expect string }

// FreezeIncarnation：FreezeEnv（期限 FreezeConfirm）成功之后 incarnation quiescing → frozen（E31）。
type FreezeIncarnation struct{ IncarnationID, EnvID string }

// ThawIncarnation：ThawEnv 成功之后 incarnation frozen → idle。
type ThawIncarnation struct{ IncarnationID, EnvID string }

// ReleaseIncarnation：Incarnation.Release（task_outcome → task_released → 核验）；成功后 incarnation → idle。
type ReleaseIncarnation struct{ Handoff Handoff }

// CloseIncarnation：发送 session_close 并等待进程退出（期限 CloseGrace；失败不影响随后的销毁）。
type CloseIncarnation struct{ IncarnationID string }

// DestroyIncarnation：撤销 Gateway 入口 → StopEnv（须 Recorded）→ EndIncarnation(Reason) → 删除暂存 → 归还内存。
type DestroyIncarnation struct{ IncarnationID, EnvID, Reason string }

// FinishCloseOp：DeleteWorkspace → ReleaseUID → Store.FinishClose（E32：只在全部环境停止之后）。
type FinishCloseOp struct{ FromRowVersion int64 }

// ReplyGrant 答复 taskID 的全部等待中的授予申请。
type ReplyGrant struct {
	TaskID string
	Info   GrantInfo
	Err    error
}

// ReplyHandoff 答复一个交还。
type ReplyHandoff struct {
	Handoff Handoff
	Report  StopReport
}

// WakeAt 要求 actor 在该时间发送 Tick。
type WakeAt struct{ Time time.Time }

// Exit 表示会话已关闭，actor 退出。
type Exit struct{}

func (Load) isEffect()               {}
func (DoTransition) isEffect()       {}
func (StartIncarnation) isEffect()   {}
func (QuiesceIncarnation) isEffect() {}
func (FreezeIncarnation) isEffect()  {}
func (ThawIncarnation) isEffect()    {}
func (ReleaseIncarnation) isEffect() {}
func (CloseIncarnation) isEffect()   {}
func (DestroyIncarnation) isEffect() {}
func (FinishCloseOp) isEffect()      {}
func (ReplyGrant) isEffect()         {}
func (ReplyHandoff) isEffect()       {}
func (WakeAt) isEffect()             {}
func (Exit) isEffect()               {}

// ErrInvalid 表示状态与事件的组合不合法。
var ErrInvalid = errors.New("session: 不合法的状态或事件")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// sessionStatuses 是会话状态的全集。
var sessionStatuses = []string{StatusCreating, StatusIdle, StatusRunning, StatusQuiescing, StatusFrozen, StatusEvicting,
	StatusEvicted, StatusRestoring, StatusClosing, StatusClosed}

// Decide 计算事件 e 作用于状态 s 的新状态与副作用（纯函数）。
func Decide(s ActorState, e Event) Decision {
	d := &decider{s: s}
	d.s.Waiters = slices.Clone(s.Waiters)
	d.s.Handoffs = slices.Clone(s.Handoffs)
	if err := d.dispatch(e); err != nil {
		return Decision{Next: s, Err: err}
	}
	if !d.s.Busy && !d.s.Closed {
		d.step()
	}
	return Decision{Next: d.s, Effects: d.effects}
}

type decider struct {
	s       ActorState
	effects []Effect
}

func (d *decider) emit(e ...Effect) { d.effects = append(d.effects, e...) }

func (d *decider) dispatch(e Event) error {
	switch e := e.(type) {
	case Loaded:
		if !d.s.Busy {
			return invalid("没有在途读取时收到读取结果")
		}
		d.s.Busy, d.s.NeedLoad, d.s.Failures, d.s.RetryAt = false, false, 0, nil
		return d.loaded(e.State)
	case LoadFailed:
		if !d.s.Busy {
			return invalid("没有在途读取时收到读取失败")
		}
		d.s.Busy = false
		if e.NotFound { // 会话不存在：视为已关闭
			d.s.Loaded, d.s.NeedLoad = true, false
			d.s.Session = State{SessionID: d.s.Session.SessionID, Status: StatusClosed}
			return nil
		}
		d.backoff()
		return nil
	case Tick:
		return nil
	case Notified:
		d.s.NeedLoad = true
		return nil
	case GrantRequested:
		if e.TaskID == "" {
			return invalid("授予申请缺少 task_id")
		}
		if d.s.Granted == e.TaskID { // 再次申请：上一次授予未被使用（例如 attempt 创建被拒绝）
			d.s.Granted = ""
		}
		if !slices.Contains(d.s.Waiters, e.TaskID) {
			d.s.Waiters = append(d.s.Waiters, e.TaskID)
		}
		d.s.NeedLoad = true
		return nil
	case GrantWithdrawn:
		d.s.Waiters = slices.DeleteFunc(d.s.Waiters, func(w string) bool { return w == e.TaskID })
		if d.s.Granted == e.TaskID {
			d.s.Granted = ""
		}
		return nil
	case HandoffRequested:
		if e.Handoff.AttemptID == "" || e.Handoff.EnvID == "" {
			return invalid("交还缺少 attempt_id 或 env_id")
		}
		if d.s.Granted == e.Handoff.TaskID {
			d.s.Granted = ""
		}
		d.s.Handoffs = append(d.s.Handoffs, e.Handoff)
		d.s.NeedLoad = true
		return nil
	case PressureEvict:
		d.s.EvictWanted, d.s.NeedLoad = true, true
		return nil
	case IncarnationExited: // 先重新读取：会话可能已由 attempt 创建事务转为 running（此时等交还）
		if e.IncarnationID != "" && e.IncarnationID == d.s.HandleInc {
			d.s.Exited, d.s.NeedLoad = true, true
		}
		return nil
	case OpDone:
		return d.opDone(e)
	}
	return invalid("未知事件 %T", e)
}

func (d *decider) loaded(st State) error {
	if !slices.Contains(sessionStatuses, st.Status) {
		return invalid("会话状态 %q 未定义", st.Status)
	}
	if inc := st.Incarnation; inc != nil && inc.IncarnationID != st.CurrentIncarnationID {
		return invalid("当前 incarnation %q 与会话的 current_incarnation_id %q 不一致", inc.IncarnationID, st.CurrentIncarnationID)
	}
	d.s.Session, d.s.Loaded = st, true
	if st.Status == StatusIdle && st.IdleSince != nil {
		at := *st.IdleSince
		d.s.IdleSince = &at
	}
	// 已授予的 turn 创建了 attempt（running 且由它占用），或已不再是非终态：授予已使用或失效。
	if g := d.s.Granted; g != "" && ((st.Status == StatusRunning && st.CurrentTaskID == g) || !turnOpen(st, g)) {
		d.s.Granted = ""
	}
	return nil
}

func (d *decider) opDone(e OpDone) error {
	if !d.s.Busy {
		return invalid("没有在途操作时收到 %s 的结果", e.Op)
	}
	d.s.Busy = false
	if e.State != nil {
		if err := d.loaded(*e.State); err != nil {
			return err
		}
		d.s.NeedLoad = false
	} else {
		d.s.NeedLoad = true
	}
	if e.Err != nil {
		d.backoff()
	} else {
		d.s.Failures, d.s.RetryAt = 0, nil
	}
	if e.Err != nil || e.Conflict {
		return nil // 操作未完成：按（重新读取的）事实重新决策
	}
	switch e.Op {
	case OpTransition:
		switch e.To {
		case StatusIdle:
			d.s.StartTries, d.s.LastError = 0, ""
		case StatusEvicted:
			d.s.EvictWanted, d.s.EvictReason = false, ""
			if d.s.StartTries >= MaxStartTries { // 启动两次失败的 evicted 已提交：答复触发唤醒的等待者
				d.failWaiters(true)
				d.s.StartTries = 0
			}
		}
	case OpStart:
		if e.Failed != nil {
			d.s.StartTries++
			d.s.LastError = e.Failed.Error()
			return nil
		}
		if e.IncarnationID == "" {
			return invalid("启动成功但没有 incarnation_id")
		}
		d.s.HandleInc = e.IncarnationID
		d.s.Exited, d.s.Quiesced, d.s.CloseSent, d.s.ThawFail = false, false, false, false
	case OpQuiesce:
		d.s.Quiesced = e.Failed == nil
		d.s.QuiescedID = e.CheckpointID
	case OpFreeze:
		if e.Failed != nil {
			d.s.Quiesced = false
			d.s.EvictReason = EndFreezeFailed
		}
	case OpThaw:
		d.s.ThawFail = e.Failed != nil
		if e.Failed == nil {
			// 唤醒后 Worker 会继续运行 turn：上一次 quiesce 的结果作废，下次空闲须重新 quiesce（否则 stepQuiescing
			// 跳过 quiesce 并判为 quiesce_failed 而驱逐）。
			d.s.Quiesced = false
		}
	case OpRelease:
		if len(d.s.Handoffs) == 0 {
			return invalid("没有待处理的交还时收到释放结果")
		}
		if e.Failed != nil {
			d.s.RelFail = true
			return nil
		}
		h := d.s.Handoffs[0]
		d.s.Handoffs = d.s.Handoffs[1:]
		d.emit(ReplyHandoff{Handoff: h, Report: StopReport{Stopped: true, Recorded: true}})
	case OpClose:
		d.s.CloseSent = true
	case OpDestroy:
		if e.IncarnationID == d.s.HandleInc {
			d.s.HandleInc = ""
			d.s.Exited, d.s.Quiesced, d.s.CloseSent, d.s.ThawFail = false, false, false, false
		}
		d.s.RelFail = false
	case OpFinishClose:
	default:
		return invalid("操作 %q 未定义", e.Op)
	}
	return nil
}

func (d *decider) backoff() {
	at := d.s.Now.Add(Backoff(d.s.Failures))
	d.s.Failures++
	d.s.RetryAt = &at
	d.s.NeedLoad = true
}

// Backoff 是第 n 次（从 0 起）连续失败后的重试间隔：min(1 s·2^n, 30 s)。
func Backoff(n int) time.Duration {
	if n < 0 || n > 5 {
		return 30 * time.Second
	}
	return min(time.Second<<n, 30*time.Second)
}

// ---- 收敛 ----

func (d *decider) op(e Effect) {
	d.s.Busy = true
	d.emit(e)
}

func (d *decider) transition(to string, from []string, mut func(*Transition)) {
	t := Transition{SessionID: d.s.Session.SessionID, FromRowVersion: d.s.Session.RowVersion, From: from, To: to}
	switch to { // 过渡态（running、quiescing、evicting、closing）不产生用户事件
	case StatusIdle, StatusFrozen, StatusEvicted, StatusRestoring:
		t.Event = stateEvent(to, "")
	}
	if mut != nil {
		mut(&t)
	}
	d.op(DoTransition{T: t})
}

func stateEvent(state, userMessage string) json.RawMessage {
	m := map[string]string{"state": state}
	if userMessage != "" {
		m["user_message"] = userMessage
	}
	b, _ := json.Marshal(m)
	return b
}

// frozenEvent 是 session_state{frozen} 的 payload，另带 quiesced 报告的 checkpoint（内部字段，I9 据此核对）。
func frozenEvent(quiescedID string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"state": StatusFrozen, "quiesced_checkpoint_id": quiescedID})
	return b
}

// step 选出下一个操作（没有在途操作时）。规则按优先级排列：关闭 → 交还 → 遗留 incarnation → 各状态的收敛。
func (d *decider) step() {
	s := &d.s
	st := s.Session
	closing := s.Loaded && (st.Desired == "closed" || st.Status == StatusClosing)
	if s.Loaded && st.Status == StatusClosed {
		d.failWaiters(false)
		for _, h := range s.Handoffs { // closed 要求全部环境已停止
			d.emit(ReplyHandoff{Handoff: h, Report: StopReport{Stopped: true, Recorded: true}})
		}
		s.Handoffs, s.Closed = nil, true
		d.emit(Exit{})
		return
	}
	if closing {
		d.failWaiters(false)
		s.Granted = ""
	}
	if s.RetryAt != nil && s.Now.Before(*s.RetryAt) {
		d.emit(WakeAt{Time: *s.RetryAt})
		return
	}
	if !s.Loaded || s.NeedLoad {
		d.op(Load{})
		return
	}
	inc := liveIncarnation(st)

	// 交还（§12.4）：Release 成功即归还；要求销毁、进程已退出或释放失败时销毁 incarnation。交还的环境已不是存活
	// incarnation 的环境：它已被销毁（销毁总在 StopEnv 确认之后才结束 incarnation），直接答复。
	for len(s.Handoffs) > 0 {
		h := s.Handoffs[0]
		if inc == nil || inc.EnvID != h.EnvID {
			s.Handoffs, s.RelFail = s.Handoffs[1:], false
			d.emit(ReplyHandoff{Handoff: h, Report: StopReport{Stopped: true, Recorded: true}})
			continue
		}
		switch {
		case s.RelFail:
			d.destroy(inc, EndReleaseTimeout)
		case h.Destroy:
			d.destroy(inc, EndTaskDestroyed)
		case s.Exited || s.HandleInc != inc.IncarnationID:
			d.destroy(inc, EndWorkerExited)
		default:
			d.op(ReleaseIncarnation{Handoff: h})
		}
		return
	}

	// 存活但不属于本进程的 incarnation（启动时遗留、启动失败后的残留）：销毁。
	if inc != nil && s.HandleInc != inc.IncarnationID {
		d.destroy(inc, EndOrphaned)
		return
	}
	// 进程意外退出（E28）：running 时等 task actor 的交还（Destroy = true），其余状态直接销毁（关闭中的销毁见
	// stepClosing）。
	if inc != nil && s.Exited && st.Status != StatusRunning && !closing {
		d.destroy(inc, EndWorkerExited)
		return
	}

	if closing {
		d.stepClosing(inc)
		return
	}
	if inc == nil {
		switch st.Status {
		case StatusIdle, StatusRunning, StatusQuiescing, StatusFrozen, StatusEvicting:
			d.transition(StatusEvicted, []string{st.Status}, nil)
			return
		}
	}
	switch st.Status {
	case StatusCreating, StatusRestoring:
		d.stepStarting(inc)
	case StatusEvicted:
		if d.wantAwake() {
			d.transition(StatusRestoring, []string{StatusEvicted}, nil)
		}
	case StatusRunning:
		d.stepRunning(inc)
	case StatusIdle:
		d.stepIdle(inc)
	case StatusQuiescing:
		d.stepQuiescing(inc)
	case StatusFrozen:
		d.stepFrozen(inc)
	case StatusEvicting:
		reason := s.EvictReason
		if reason == "" {
			reason = EndEvicted
		}
		d.destroy(inc, reason)
	}
}

func (d *decider) destroy(inc *Incarnation, reason string) {
	d.op(DestroyIncarnation{IncarnationID: inc.IncarnationID, EnvID: inc.EnvID, Reason: reason})
}

// stepClosing（§12.6）：拒绝新授予（已在 step 中答复）→ 等全部 turn 终态 → session_close → 停止环境并结束
// incarnation → 删除 workspace、释放 UID 范围、closed。
func (d *decider) stepClosing(inc *Incarnation) {
	s := &d.s
	st := s.Session
	if st.Status != StatusClosing {
		from := slices.DeleteFunc(slices.Clone(sessionStatuses), func(x string) bool { return x == StatusClosed || x == StatusClosing })
		d.transition(StatusClosing, from, nil)
		return
	}
	if len(st.NonTerminalTurns) > 0 {
		return // 取消控制已由 API 写入；等 task actor 裁决（Notify 或周期读取后再决策）
	}
	if inc != nil {
		if s.HandleInc == inc.IncarnationID && !s.Exited && !s.CloseSent && inc.Status != IncFrozen {
			d.op(CloseIncarnation{IncarnationID: inc.IncarnationID})
			return
		}
		d.destroy(inc, EndClosed)
		return
	}
	d.op(FinishCloseOp{FromRowVersion: st.RowVersion})
}

// stepStarting：creating 与 restoring 新建 incarnation（restoring 以 Latest 恢复）；成功后 → idle，两次失败 → evicted。
func (d *decider) stepStarting(inc *Incarnation) {
	s := &d.s
	st := s.Session
	if inc != nil { // 句柄属于本进程（遗留的已在 step 中销毁）
		if inc.Status == IncIdle {
			wake := st.WakeRequestedVersion
			d.transition(StatusIdle, []string{st.Status}, func(t *Transition) { t.AppliedWake = &wake })
		}
		return
	}
	if st.Status == StatusCreating && !d.wantAwake() {
		return // 新会话在首个 turn 的授予（或 wake）到来时才创建 incarnation
	}
	if s.StartTries >= MaxStartTries {
		// 等待者在 evicted（含 last_error 与 user_message）提交之后才得到 ErrUnavailable（见 opDone）：先答复会让
		// 申请者看到仍是 restoring 的会话；转换冲突或暂时失败时 StartTries 保留，重新决策会再次尝试这次转换。
		wake, msg := st.WakeRequestedVersion, s.LastError
		d.transition(StatusEvicted, []string{st.Status}, func(t *Transition) {
			t.AppliedWake, t.LastError, t.Event = &wake, &msg, stateEvent(StatusEvicted, UnavailableMessage)
		})
		return
	}
	var resume *Checkpoint
	if st.Status == StatusRestoring {
		resume = st.Latest
	}
	d.op(StartIncarnation{Resume: resume, FromRowVersion: st.RowVersion})
}

func (d *decider) stepRunning(inc *Incarnation) {
	st := d.s.Session
	if inc.Status == IncIdle && st.CurrentTaskID == "" { // turn 的结束由 turn 事件反映，不追加会话事件
		d.transition(StatusIdle, []string{StatusRunning}, func(t *Transition) { t.Event = nil })
		return
	}
	if inc.Status == IncIdle {
		d.grant(inc) // 占用会话的 turn（故障重试）再次申请
	}
}

func (d *decider) stepIdle(inc *Incarnation) {
	s := &d.s
	st := s.Session
	if inc.Status != IncIdle {
		return
	}
	if st.WakeRequestedVersion > st.AppliedWakeVersion {
		wake := st.WakeRequestedVersion
		d.transition(StatusIdle, []string{StatusIdle}, func(t *Transition) { t.AppliedWake, t.Event = &wake, nil })
		return
	}
	d.grant(inc)
	if s.Granted != "" {
		if until := s.GrantedAt.Add(s.Config.IdleFreeze); s.Now.Before(until) {
			d.emit(WakeAt{Time: until})
			return
		}
		s.Granted = "" // 授予未被使用（attempt 未创建）：失效
	}
	if d.grantable() != "" {
		return
	}
	if s.EvictWanted {
		s.EvictReason = EndMemoryPressure
		d.transition(StatusQuiescing, []string{StatusIdle}, nil)
		return
	}
	since := s.Now
	if s.IdleSince != nil {
		since = *s.IdleSince
	}
	if at := since.Add(s.Config.IdleFreeze); s.Now.Before(at) {
		d.emit(WakeAt{Time: at})
		return
	}
	d.transition(StatusQuiescing, []string{StatusIdle}, nil)
}

// stepQuiescing（§12.2 冻结前条件、E31）：quiesce 报告的 checkpoint 等于最新已提交 checkpoint 且 FreezeEnv 确认之后
// 才记录 frozen；任何一步失败 → evicting。内存压力的驱逐在 quiesce 之后直接 evicting。
func (d *decider) stepQuiescing(inc *Incarnation) {
	s := &d.s
	switch {
	case inc.Status == IncIdle && !s.Quiesced:
		expect := ""
		if s.Session.Latest != nil {
			expect = s.Session.Latest.CheckpointID
		}
		d.op(QuiesceIncarnation{IncarnationID: inc.IncarnationID, Expect: expect})
	case inc.Status == IncFrozen && !s.EvictWanted:
		// 事件 payload 带 quiesced 报告的 checkpoint（I9：frozen 会话最近一次 quiesced 的 ID 等于会话指针）；
		// 用户视图只保留 state 与 user_message。
		qid := s.QuiescedID
		d.transition(StatusFrozen, []string{StatusQuiescing}, func(t *Transition) {
			t.Event = frozenEvent(qid)
		})
	case inc.Status == IncQuiescing && s.Quiesced && !s.EvictWanted:
		d.op(FreezeIncarnation{IncarnationID: inc.IncarnationID, EnvID: inc.EnvID})
	default:
		if s.EvictReason == "" {
			s.EvictReason = EndQuiesceFailed
		}
		d.transition(StatusEvicting, []string{StatusQuiescing}, nil)
	}
}

// stepFrozen：唤醒 → thaw（失败 → evicting，随后冷恢复）；驱逐时限或内存压力 → evicting（不 thaw）。
func (d *decider) stepFrozen(inc *Incarnation) {
	s := &d.s
	st := s.Session
	switch {
	case inc.Status == IncIdle: // 已 thaw
		wake := st.WakeRequestedVersion
		d.transition(StatusIdle, []string{StatusFrozen}, func(t *Transition) { t.AppliedWake = &wake })
		return
	case s.ThawFail:
		s.EvictReason = EndThawFailed
		d.transition(StatusEvicting, []string{StatusFrozen}, nil)
		return
	case d.wantAwake():
		d.op(ThawIncarnation{IncarnationID: inc.IncarnationID, EnvID: inc.EnvID})
		return
	case s.EvictWanted:
		s.EvictReason = EndMemoryPressure
		d.transition(StatusEvicting, []string{StatusFrozen}, nil)
		return
	}
	at := d.evictAt()
	if s.Now.Before(at) {
		d.emit(WakeAt{Time: at})
		return
	}
	s.EvictReason = EndEvicted
	d.transition(StatusEvicting, []string{StatusFrozen}, nil)
}

// evictAt 是驱逐时限：自 idle_since 起 EvictAfter（idle_since 未知时以 frozen_since 推算）。
func (d *decider) evictAt() time.Time {
	s := &d.s
	switch {
	case s.IdleSince != nil:
		return s.IdleSince.Add(s.Config.EvictAfter)
	case s.Session.FrozenSince != nil:
		return s.Session.FrozenSince.Add(s.Config.EvictAfter - s.Config.IdleFreeze)
	}
	return s.Now
}

// grant 在 incarnation idle 时把它授予下一个可运行的等待者（一次至多一个未使用的授予）。
func (d *decider) grant(inc *Incarnation) {
	s := &d.s
	if s.Granted != "" && s.Now.Before(s.GrantedAt.Add(s.Config.IdleFreeze)) {
		return
	}
	w := d.grantable()
	if w == "" {
		return
	}
	s.Waiters = slices.DeleteFunc(s.Waiters, func(x string) bool { return x == w })
	s.Granted, s.GrantedAt = w, s.Now
	d.emit(ReplyGrant{TaskID: w, Info: GrantInfo{IncarnationID: inc.IncarnationID, EnvID: inc.EnvID}})
}

// grantable 返回第一个可运行的等待者。
func (d *decider) grantable() string {
	for _, w := range d.s.Waiters {
		if d.runnable(w) {
			return w
		}
	}
	return ""
}

// wantAwake：有可运行的等待者，或有未应用的 wake 请求。
func (d *decider) wantAwake() bool {
	st := d.s.Session
	return d.grantable() != "" || st.WakeRequestedVersion > st.AppliedWakeVersion
}

// failWaiters 以 ErrUnavailable 答复等待者：onlyGrantable 时只答复触发唤醒的（可运行的）等待者。
func (d *decider) failWaiters(onlyGrantable bool) {
	s := &d.s
	var keep []string
	for _, w := range s.Waiters {
		if onlyGrantable && !d.runnable(w) {
			keep = append(keep, w)
			continue
		}
		d.emit(ReplyGrant{TaskID: w, Err: ErrUnavailable})
	}
	s.Waiters = keep
}

// runnable：turn w 未被其他 turn 阻塞或占用（§12.3），且仍是非终态 turn。
func (d *decider) runnable(w string) bool {
	st := d.s.Session
	return (st.BlockedByTaskID == "" || st.BlockedByTaskID == w) && (st.CurrentTaskID == "" || st.CurrentTaskID == w) && turnOpen(st, w)
}

func turnOpen(st State, taskID string) bool {
	return slices.ContainsFunc(st.NonTerminalTurns, func(f TurnFact) bool { return f.TaskID == taskID })
}

func liveIncarnation(st State) *Incarnation {
	if inc := st.Incarnation; inc != nil && inc.Status != IncEnded {
		return inc
	}
	return nil
}
