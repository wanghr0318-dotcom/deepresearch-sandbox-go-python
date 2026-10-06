package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
)

// ==== M4 Plan 12 Task 7：session actor（Decide、actor、Scheduler） ====
//
// 全部依赖为内存替身，时间由 fkClock 推进。等待以"条件成立"为准（有真实时间上限），断言只针对不依赖调度先后的事实：
// 调用日志的相对顺序与 Store 中的结果。

var t0 = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

var testConfig = Config{IdleFreeze: 10 * time.Minute, EvictAfter: time.Hour, FreezeConfirm: 5 * time.Second,
	QuiesceGrace: 10 * time.Second, CloseGrace: 10 * time.Second, IncarnationMemory: 100}

// ---- Decide ----

func at(d time.Duration) *time.Time {
	v := t0.Add(d)
	return &v
}

// idleState 是一个 idle 会话（incarnation i1 idle、本进程持有句柄）与非终态 turn t1 的事实。
func idleState() ActorState {
	return ActorState{Now: t0, Config: testConfig, Loaded: true, HandleInc: "i1", IdleSince: at(0),
		Session: State{SessionID: "s1", Status: StatusIdle, CurrentIncarnationID: "i1", RowVersion: 3, Desired: "active",
			IdleSince: at(0), Incarnation: &Incarnation{IncarnationID: "i1", SessionID: "s1", EnvID: "e1", Status: IncIdle},
			NonTerminalTurns: []TurnFact{{TaskID: "t1", Status: "queued"}}}}
}

func with(s ActorState, f func(*ActorState)) ActorState {
	if s.Session.Incarnation != nil {
		inc := *s.Session.Incarnation
		s.Session.Incarnation = &inc
	}
	f(&s)
	return s
}

func status(st string) func(*ActorState) { return func(s *ActorState) { s.Session.Status = st } }

func incStatus(st string) func(*ActorState) {
	return func(s *ActorState) { s.Session.Incarnation.Status = st }
}

func noInc(s *ActorState) {
	s.Session.Incarnation, s.Session.CurrentIncarnationID, s.HandleInc = nil, "", ""
}

func effectKinds(effects []Effect) string {
	names := make([]string, len(effects))
	for i, e := range effects {
		names[i] = strings.TrimPrefix(reflect.TypeOf(e).String(), "session.")
	}
	return strings.Join(names, ",")
}

// describe 把副作用写成便于比较的短文本：转换写目标状态，销毁写原因。
func describe(effects []Effect) string {
	var out []string
	for _, e := range effects {
		switch e := e.(type) {
		case DoTransition:
			out = append(out, "→"+e.T.To)
		case DestroyIncarnation:
			out = append(out, "Destroy("+e.Reason+")")
		case ReplyGrant:
			if e.Err != nil {
				out = append(out, "Reply("+e.TaskID+",unavailable)")
			} else {
				out = append(out, "Reply("+e.TaskID+","+e.Info.IncarnationID+")")
			}
		case ReplyHandoff:
			out = append(out, fmt.Sprintf("Handoff(%s,recorded=%v)", e.Handoff.AttemptID, e.Report.Recorded))
		default:
			out = append(out, strings.TrimPrefix(reflect.TypeOf(e).String(), "session."))
		}
	}
	return strings.Join(out, ",")
}

// TestDecideTransitions 是 §12.2 的状态机：每个合法转换一例（由 Tick 触发收敛）。
func TestDecideTransitions(t *testing.T) {
	waiter := func(s *ActorState) { s.Waiters = []string{"t1"} }
	latest := func(s *ActorState) { s.Session.Latest = &Checkpoint{CheckpointID: "sc-1", StateRef: "ab"} }
	cases := []struct {
		name string
		s    ActorState
		want string
	}{
		{"creating 无申请：不建 incarnation", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status = StatusCreating }), ""},
		{"creating 首个授予申请 → 新建", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status = StatusCreating; waiter(s) }), "StartIncarnation"},
		{"creating 的 incarnation 就绪 → idle", with(idleState(), status(StatusCreating)), "→idle"},
		{"idle 授予可运行的 turn", with(idleState(), waiter), "Reply(t1,i1),WakeAt"},
		{"idle 被其他 turn 阻塞：不授予", with(idleState(), func(s *ActorState) { waiter(s); s.Session.BlockedByTaskID = "t0" }), "WakeAt"},
		{"idle 未到冻结时限", idleState(), "WakeAt"},
		{"idle 到达冻结时限 → quiescing", with(idleState(), func(s *ActorState) { s.Now = t0.Add(10 * time.Minute) }), "→quiescing"},
		{"idle 的 wake → 应用 wake", with(idleState(), func(s *ActorState) { s.Session.WakeRequestedVersion = 1 }), "→idle"},
		{"内存压力：idle 先 quiesce", with(idleState(), func(s *ActorState) { s.EvictWanted = true }), "→quiescing"},
		{"quiescing → quiesce", with(idleState(), func(s *ActorState) { latest(s); s.Session.Status = StatusQuiescing }), "QuiesceIncarnation"},
		{"quiesced 一致 → 冻结", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status, s.Quiesced = StatusQuiescing, IncQuiescing, true
		}), "FreezeIncarnation"},
		{"quiesce 不一致或超时 → evicting", with(idleState(), func(s *ActorState) { s.Session.Status, s.Session.Incarnation.Status = StatusQuiescing, IncQuiescing }), "→evicting"},
		{"冻结确认后 → frozen", with(idleState(), func(s *ActorState) { s.Session.Status, s.Session.Incarnation.Status = StatusQuiescing, IncFrozen }), "→frozen"},
		{"内存压力：quiesce 之后驱逐", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status, s.Quiesced, s.EvictWanted = StatusQuiescing, IncQuiescing, true, true
		}), "→evicting"},
		{"frozen 唤醒 → thaw", with(idleState(), func(s *ActorState) {
			waiter(s)
			s.Session.Status, s.Session.Incarnation.Status = StatusFrozen, IncFrozen
		}), "ThawIncarnation"},
		{"thaw 完成 → idle", with(idleState(), func(s *ActorState) { s.Session.Status = StatusFrozen }), "→idle"},
		{"thaw 失败 → evicting", with(idleState(), func(s *ActorState) {
			waiter(s)
			s.Session.Status, s.Session.Incarnation.Status, s.ThawFail = StatusFrozen, IncFrozen, true
		}), "→evicting"},
		{"frozen 未到驱逐时限", with(idleState(), func(s *ActorState) { s.Session.Status, s.Session.Incarnation.Status = StatusFrozen, IncFrozen }), "WakeAt"},
		{"frozen 到达驱逐时限 → evicting（不 thaw）", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status, s.Now = StatusFrozen, IncFrozen, t0.Add(time.Hour)
		}), "→evicting"},
		{"frozen 内存压力 → evicting", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status, s.EvictWanted = StatusFrozen, IncFrozen, true
		}), "→evicting"},
		{"evicting → 销毁", with(idleState(), func(s *ActorState) { s.Session.Status, s.EvictReason = StatusEvicting, EndFreezeFailed }), "Destroy(freeze_failed)"},
		{"evicting 已销毁 → evicted", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status = StatusEvicting }), "→evicted"},
		{"evicted 无申请：保持", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status = StatusEvicted }), ""},
		{"evicted 授予申请 → restoring", with(idleState(), func(s *ActorState) { noInc(s); waiter(s); s.Session.Status = StatusEvicted }), "→restoring"},
		{"evicted 的 wake → restoring", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status, s.Session.WakeRequestedVersion = StatusEvicted, 2 }), "→restoring"},
		{"restoring → 冷恢复", with(idleState(), func(s *ActorState) { noInc(s); latest(s); waiter(s); s.Session.Status = StatusRestoring }), "StartIncarnation"},
		{"恢复两次失败 → evicted，申请得 ErrUnavailable", with(idleState(), func(s *ActorState) {
			noInc(s)
			waiter(s)
			s.Session.Status, s.StartTries, s.LastError = StatusRestoring, 2, "启动失败"
		}), "→evicted"},
		{"running 释放后 → idle", with(idleState(), status(StatusRunning)), "→idle"},
		{"running 中 incarnation 忙", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.CurrentTaskID, s.Session.Incarnation.Status = StatusRunning, "t1", IncBusy
		}), ""},
		{"running 中进程退出：等交还", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.CurrentTaskID, s.Session.Incarnation.Status, s.Exited = StatusRunning, "t1", IncBusy, true
		}), ""},
		{"running 的 incarnation 已销毁 → evicted", with(idleState(), func(s *ActorState) { noInc(s); s.Session.Status = StatusRunning }), "→evicted"},
		{"idle 中进程退出 → 销毁（E28）", with(idleState(), func(s *ActorState) { s.Exited = true }), "Destroy(worker_exited)"},
		{"不属于本进程的 incarnation → 销毁", with(idleState(), func(s *ActorState) { s.HandleInc = "" }), "Destroy(orphaned)"},
		{"desired = closed → closing，申请得 ErrUnavailable", with(idleState(), func(s *ActorState) { waiter(s); s.Session.Desired = "closed" }), "Reply(t1,unavailable),→closing"},
		{"closing 等 turn 终态", with(idleState(), func(s *ActorState) { s.Session.Status, s.Session.Desired = StatusClosing, "closed" }), ""},
		{"closing turn 全部终态 → session_close", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Desired, s.Session.NonTerminalTurns = StatusClosing, "closed", nil
		}), "CloseIncarnation"},
		{"closing 已 close → 销毁", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Desired, s.Session.NonTerminalTurns, s.CloseSent = StatusClosing, "closed", nil, true
		}), "Destroy(closed)"},
		{"closing 环境已停止 → 完成关闭", with(idleState(), func(s *ActorState) {
			noInc(s)
			s.Session.Status, s.Session.Desired, s.Session.NonTerminalTurns = StatusClosing, "closed", nil
		}), "FinishCloseOp"},
		{"closed → 退出", with(idleState(), func(s *ActorState) { noInc(s); waiter(s); s.Session.Status = StatusClosed }), "Reply(t1,unavailable),Exit"},
		{"交还 → Release", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status = StatusRunning, IncReleasing
			s.Handoffs = []Handoff{{TaskID: "t1", AttemptID: "a1", EnvID: "e1", Verdict: "succeeded"}}
		}), "ReleaseIncarnation"},
		{"交还要求销毁", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status = StatusRunning, IncBusy
			s.Handoffs = []Handoff{{TaskID: "t1", AttemptID: "a1", EnvID: "e1", Destroy: true}}
		}), "Destroy(task_destroyed)"},
		{"释放失败 → 销毁（release_timeout）", with(idleState(), func(s *ActorState) {
			s.Session.Status, s.Session.Incarnation.Status, s.RelFail = StatusRunning, IncReleasing, true
			s.Handoffs = []Handoff{{TaskID: "t1", AttemptID: "a1", EnvID: "e1", Verdict: "succeeded"}}
		}), "Destroy(release_timeout)"},
		{"交还的环境已销毁 → 直接答复", with(idleState(), func(s *ActorState) {
			noInc(s)
			s.Session.Status = StatusEvicted
			s.Handoffs = []Handoff{{TaskID: "t1", AttemptID: "a1", EnvID: "e0", Destroy: true}}
		}), "Handoff(a1,recorded=true)"},
		{"退避期间不操作", with(idleState(), func(s *ActorState) { s.RetryAt = at(time.Second) }), "WakeAt"},
		{"需要重读", with(idleState(), func(s *ActorState) { s.NeedLoad = true }), "Load"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := Decide(tc.s, Tick{})
			if d.Err != nil {
				t.Fatalf("Decide: %v", d.Err)
			}
			if got := describe(d.Effects); got != tc.want {
				t.Fatalf("副作用 = %q，期望 %q", got, tc.want)
			}
			ops := 0
			for _, e := range d.Effects {
				switch e.(type) {
				case ReplyGrant, ReplyHandoff, WakeAt, Exit:
				default:
					ops++
				}
			}
			if ops > 1 || (ops == 1) != d.Next.Busy {
				t.Fatalf("每个决策至多一个在途操作：%s（Busy = %v）", effectKinds(d.Effects), d.Next.Busy)
			}
		})
	}
}

// TestDecideTransitionDetails：转换携带的事件（用户可见的 idle/frozen/evicted/restoring）、恢复失败的 last_error 与
// user_message、quiesce 期望的 checkpoint、冷恢复的 Resume。
func TestDecideTransitionDetails(t *testing.T) {
	op := func(t *testing.T, s ActorState) Effect {
		t.Helper()
		d := Decide(s, Tick{})
		if d.Err != nil || len(d.Effects) == 0 {
			t.Fatalf("Decide = %s, %v", effectKinds(d.Effects), d.Err)
		}
		return d.Effects[len(d.Effects)-1]
	}
	fail := op(t, with(idleState(), func(s *ActorState) {
		noInc(s)
		s.Waiters, s.Session.Status, s.StartTries, s.LastError, s.Session.WakeRequestedVersion = []string{"t1"}, StatusRestoring, 2, "boom", 4
	})).(DoTransition)
	if fail.T.LastError == nil || *fail.T.LastError != "boom" || fail.T.AppliedWake == nil || *fail.T.AppliedWake != 4 ||
		string(fail.T.Event) != `{"state":"evicted","user_message":"会话暂时无法恢复"}` || fail.T.FromRowVersion != 3 {
		t.Errorf("恢复失败的转换 = %+v（事件 %s）", fail.T, fail.T.Event)
	}
	if q := op(t, with(idleState(), func(s *ActorState) {
		s.Session.Status, s.Session.Latest = StatusQuiescing, &Checkpoint{CheckpointID: "sc-7"}
	})).(QuiesceIncarnation); q.Expect != "sc-7" || q.IncarnationID != "i1" {
		t.Errorf("quiesce = %+v", q)
	}
	if tr := op(t, with(idleState(), func(s *ActorState) { s.Now = t0.Add(time.Hour) })).(DoTransition); tr.T.Event != nil {
		t.Errorf("quiescing 是过渡态，不产生用户事件：%s", tr.T.Event)
	}
	if tr := op(t, with(idleState(), func(s *ActorState) { s.Session.Status, s.Session.Incarnation.Status = StatusQuiescing, IncFrozen })).(DoTransition); string(tr.T.Event) != `{"state":"frozen"}` {
		t.Errorf("frozen 事件 = %s", tr.T.Event)
	}
	cp := &Checkpoint{CheckpointID: "sc-1", StateRef: "ab"}
	if st := op(t, with(idleState(), func(s *ActorState) {
		noInc(s)
		s.Session.Status, s.Session.Latest, s.Waiters = StatusRestoring, cp, []string{"t1"}
	})).(StartIncarnation); st.Resume != cp {
		t.Errorf("冷恢复应以 Latest 恢复：%+v", st)
	}
	if st := op(t, with(idleState(), func(s *ActorState) {
		noInc(s)
		s.Session.Status, s.Session.Latest, s.Waiters = StatusCreating, cp, []string{"t1"}
	})).(StartIncarnation); st.Resume != nil {
		t.Errorf("新会话不恢复：%+v", st)
	}
}

// TestDecideRestoreFailureRepliesAfterEvictedCommitted：启动两次失败时，等待者只在 evicted（last_error、user_message）
// 提交之后得到 ErrUnavailable；转换冲突时不答复、保留 StartTries，重新决策再次转换。
func TestDecideRestoreFailureRepliesAfterEvictedCommitted(t *testing.T) {
	failing := with(idleState(), func(s *ActorState) {
		noInc(s)
		s.Waiters, s.Session.Status, s.StartTries, s.LastError = []string{"t1"}, StatusRestoring, 2, "boom"
	})
	d := Decide(failing, Tick{})
	if got := describe(d.Effects); got != "→evicted" || !slices.Equal(d.Next.Waiters, []string{"t1"}) {
		t.Fatalf("转换提交之前不答复：副作用 %q，等待者 %v", got, d.Next.Waiters)
	}
	conflict := Decide(d.Next, OpDone{Op: OpTransition, To: StatusEvicted, Conflict: true, State: &failing.Session})
	if got := describe(conflict.Effects); got != "→evicted" || conflict.Next.StartTries != 2 {
		t.Fatalf("冲突后应重试转换且不答复：副作用 %q，StartTries %d", got, conflict.Next.StartTries)
	}
	evicted := failing.Session
	evicted.Status, evicted.RowVersion = StatusEvicted, 4
	done := Decide(conflict.Next, OpDone{Op: OpTransition, To: StatusEvicted, State: &evicted})
	if got := describe(done.Effects); got != "Reply(t1,unavailable)" || len(done.Next.Waiters) != 0 || done.Next.StartTries != 0 {
		t.Fatalf("提交后答复：副作用 %q，等待者 %v，StartTries %d", got, done.Next.Waiters, done.Next.StartTries)
	}
}

// TestDecideRejectsInvalid：非法组合返回错误且不改变状态。
func TestDecideRejectsInvalid(t *testing.T) {
	for name, tc := range map[string]struct {
		s ActorState
		e Event
	}{
		"没有在途操作时的结果":   {idleState(), OpDone{Op: OpFreeze}},
		"没有在途读取时的读取结果": {idleState(), Loaded{State: idleState().Session}},
		"未定义的会话状态":     {with(idleState(), func(s *ActorState) { s.Busy = true }), Loaded{State: State{SessionID: "s1", Status: "sleeping"}}},
		"incarnation 不一致": {with(idleState(), func(s *ActorState) { s.Busy = true }), Loaded{State: with(idleState(), func(s *ActorState) {
			s.Session.CurrentIncarnationID = "i9"
		}).Session}},
		"交还缺少 attempt": {idleState(), HandoffRequested{Handoff: Handoff{EnvID: "e1"}}},
		"授予申请缺少 task":  {idleState(), GrantRequested{}},
		"未定义的操作":       {with(idleState(), func(s *ActorState) { s.Busy = true }), OpDone{Op: "teleport"}},
		"启动成功但没有 ID":   {with(idleState(), func(s *ActorState) { s.Busy = true }), OpDone{Op: OpStart}},
		"没有交还时的释放结果":   {with(idleState(), func(s *ActorState) { s.Busy = true }), OpDone{Op: OpRelease}},
	} {
		t.Run(name, func(t *testing.T) {
			d := Decide(tc.s, tc.e)
			if !errors.Is(d.Err, ErrInvalid) || len(d.Effects) != 0 || !reflect.DeepEqual(d.Next, tc.s) {
				t.Fatalf("Decide = %v，副作用 %s", d.Err, effectKinds(d.Effects))
			}
		})
	}
}

// ---- 替身 ----

type fkClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fkTimer
}

type fkTimer struct {
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (c *fkClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fkClock) At(t time.Time) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &fkTimer{at: t, ch: make(chan time.Time, 1)}
	if !t.After(c.now) {
		tm.ch <- c.now
	} else {
		c.timers = append(c.timers, tm)
	}
	return tm.ch, func() {
		c.mu.Lock()
		tm.stopped = true
		c.mu.Unlock()
	}
}

func (c *fkClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.timers[:0]
	for _, tm := range c.timers {
		switch {
		case tm.stopped:
		case !tm.at.After(c.now):
			tm.ch <- c.now
		default:
			kept = append(kept, tm)
		}
	}
	c.timers = kept
}

type fkLog struct {
	mu    sync.Mutex
	items []string
}

func (l *fkLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = append(l.items, fmt.Sprintf(format, args...))
}

func (l *fkLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.items...)
}

// index 返回第 n 个（从 1 起）以 prefix 开头的记录的位置；没有为 -1。
func (l *fkLog) index(prefix string, n int) int {
	for i, s := range l.all() {
		if strings.HasPrefix(s, prefix) {
			if n--; n == 0 {
				return i
			}
		}
	}
	return -1
}

func (l *fkLog) count(prefix string) int {
	n := 0
	for _, s := range l.all() {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// fkStore 是 Store 的内存替身，按真实用例的前置条件做校验（Task 3 的 Transition、CreateIncarnation、FinishClose 语义）。
type fkStore struct {
	mu        sync.Mutex
	clk       *fkClock
	log       *fkLog
	ss        map[string]*fkSession
	incs      map[string]*Incarnation
	stopped   map[string]bool // env → stopped_at 已记录
	transFail map[string]error
}

type fkSession struct {
	st        State
	lastError string
	events    []string
}

func newFkStore(clk *fkClock, log *fkLog) *fkStore {
	return &fkStore{clk: clk, log: log, ss: map[string]*fkSession{}, incs: map[string]*Incarnation{}, stopped: map[string]bool{},
		transFail: map[string]error{}}
}

// addSession 建立 creating 的会话及其非终态 turn。
func (m *fkStore) addSession(id string, turns ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &fkSession{st: State{SessionID: id, Status: StatusCreating, Desired: "active", LastActiveAt: m.clk.now}}
	for i, t := range turns {
		s.st.NonTerminalTurns = append(s.st.NonTerminalTurns, TurnFact{TaskID: t, Status: "queued", TurnIndex: int64(i)})
	}
	m.ss[id] = s
}

func (m *fkStore) with(id string, f func(s *fkSession)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f(m.ss[id])
}

func (m *fkStore) LoadSession(_ context.Context, id string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadLocked(id)
}

func (m *fkStore) loadLocked(id string) (State, error) {
	s, ok := m.ss[id]
	if !ok {
		return State{}, persistence.ErrNotFound
	}
	st := s.st
	st.NonTerminalTurns = slices.Clone(st.NonTerminalTurns)
	if inc := m.incs[st.CurrentIncarnationID]; inc != nil {
		c := *inc
		st.Incarnation = &c
	}
	return st, nil
}

func (m *fkStore) ListOpenSessions(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id, s := range m.ss {
		if s.st.Status != StatusClosed {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

func (m *fkStore) Transition(_ context.Context, t Transition) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.transFail[t.To]; err != nil {
		delete(m.transFail, t.To)
		return State{}, err
	}
	s := m.ss[t.SessionID]
	switch {
	case s.st.RowVersion == t.FromRowVersion+1 && s.st.Status == t.To:
		return m.loadLocked(t.SessionID)
	case s.st.RowVersion != t.FromRowVersion:
		return State{}, persistence.ErrConflict
	case !slices.Contains(t.From, s.st.Status):
		return State{}, &persistence.RejectedError{Code: CodeInvalidTransition}
	}
	m.log.add("transition:%s:%s→%s", t.SessionID, s.st.Status, t.To)
	now := m.clk.now
	s.st.Status, s.st.RowVersion = t.To, s.st.RowVersion+1
	s.st.IdleSince, s.st.FrozenSince = nil, nil
	switch t.To {
	case StatusIdle:
		s.st.IdleSince = &now
	case StatusFrozen:
		s.st.FrozenSince = &now
	case StatusRunning:
		s.st.LastActiveAt = now
	}
	if t.SetIncarnation != nil {
		s.st.CurrentIncarnationID = *t.SetIncarnation
	}
	if t.AppliedWake != nil {
		s.st.AppliedWakeVersion = max(s.st.AppliedWakeVersion, *t.AppliedWake)
	}
	if t.LastError != nil {
		s.lastError = *t.LastError
	}
	if t.Event != nil {
		s.events = append(s.events, string(t.Event))
	}
	return m.loadLocked(t.SessionID)
}

func (m *fkStore) CreateIncarnation(_ context.Context, n NewIncarnation) (Incarnation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inc, ok := m.incs[n.IncarnationID]; ok {
		return *inc, nil
	}
	s := m.ss[n.SessionID]
	if s.st.RowVersion != n.FromRowVersion {
		return Incarnation{}, persistence.ErrConflict
	}
	for _, inc := range m.incs {
		if inc.SessionID == n.SessionID && inc.Status != IncEnded {
			return Incarnation{}, persistence.ErrConflict
		}
	}
	inc := &Incarnation{IncarnationID: n.IncarnationID, SessionID: n.SessionID, EnvID: n.EnvID, Status: IncStarting, StartedAt: m.clk.now}
	m.incs[n.IncarnationID] = inc
	s.st.CurrentIncarnationID = n.IncarnationID
	m.log.add("create_incarnation:%s:%s", n.IncarnationID, n.EnvID)
	return *inc, nil
}

func (m *fkStore) SetUIDRange(context.Context, string, string) error { return nil }

func (m *fkStore) SetIncarnationStatus(_ context.Context, id string, from []string, to string) (Incarnation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inc := m.incs[id]
	switch {
	case inc.Status == to:
		return *inc, nil
	case !slices.Contains(from, inc.Status):
		return Incarnation{}, &persistence.RejectedError{Code: CodeInvalidTransition}
	}
	m.log.add("incarnation:%s:%s→%s", id, inc.Status, to)
	inc.Status = to
	return *inc, nil
}

func (m *fkStore) EndIncarnation(_ context.Context, id, reason string) (Incarnation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inc := m.incs[id]
	if inc.Status != IncEnded {
		now := m.clk.now
		inc.Status, inc.EndedAt, inc.EndReason = IncEnded, &now, reason
		m.log.add("end_incarnation:%s:%s", id, reason)
	}
	if s := m.ss[inc.SessionID]; s.st.CurrentIncarnationID == id {
		s.st.CurrentIncarnationID = ""
	}
	return *inc, nil
}

func (m *fkStore) LRUCandidates(_ context.Context, limit int) ([]LRUCandidate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []LRUCandidate
	for id, s := range m.ss {
		if s.st.Status == StatusFrozen || s.st.Status == StatusIdle {
			out = append(out, LRUCandidate{SessionID: id, Status: s.st.Status, LastActiveAt: s.st.LastActiveAt})
		}
	}
	slices.SortFunc(out, func(a, b LRUCandidate) int {
		if (a.Status == StatusFrozen) != (b.Status == StatusFrozen) {
			if a.Status == StatusFrozen {
				return -1
			}
			return 1
		}
		return a.LastActiveAt.Compare(b.LastActiveAt)
	})
	return out[:min(limit, len(out))], nil
}

func (m *fkStore) FinishClose(_ context.Context, id string, from int64) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.ss[id]
	switch {
	case s.st.Status == StatusClosed && s.st.RowVersion == from+1:
		return m.loadLocked(id)
	case s.st.RowVersion != from:
		return State{}, persistence.ErrConflict
	case s.st.Status != StatusClosing:
		return State{}, &persistence.RejectedError{Code: CodeInvalidTransition}
	case len(s.st.NonTerminalTurns) > 0:
		return State{}, &persistence.RejectedError{Code: CodeCloseNotReady}
	}
	for _, inc := range m.incs {
		if inc.SessionID == id && (inc.Status != IncEnded || !m.stopped[inc.EnvID]) {
			return State{}, &persistence.RejectedError{Code: CodeCloseNotReady}
		}
	}
	s.st.Status, s.st.RowVersion = StatusClosed, s.st.RowVersion+1
	s.events = append(s.events, `{"state":"closed"}`)
	m.log.add("finish_close:%s", id)
	return m.loadLocked(id)
}

// startTurn 模拟 task 的 attempt 创建事务（§8.1）：incarnation idle → busy、会话 → running、current_task_id。
func (m *fkStore) startTurn(t *testing.T, sid, taskID string, g GrantInfo) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	s, inc := m.ss[sid], m.incs[g.IncarnationID]
	if s.st.CurrentIncarnationID != g.IncarnationID || inc.Status != IncIdle || (s.st.Status != StatusIdle && s.st.Status != StatusRunning) {
		t.Fatalf("授予的 incarnation 不可用：会话 %+v，incarnation %+v", s.st, inc)
	}
	inc.Status = IncBusy
	if s.st.Status == StatusIdle {
		s.st.RowVersion++
	}
	s.st.Status, s.st.CurrentTaskID, s.st.BlockedByTaskID, s.st.IdleSince = StatusRunning, taskID, "", nil
	s.st.LastActiveAt = m.clk.now
}

// verdict 模拟裁决事务：incarnation busy → releasing；非 queued 清除占用，paused 阻塞会话，终态移出非终态 turn。
func (m *fkStore) verdict(sid, taskID, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.ss[sid]
	if inc := m.incs[s.st.CurrentIncarnationID]; inc != nil && inc.Status == IncBusy {
		inc.Status = IncReleasing
	}
	m.setTurnLocked(s, taskID, status)
}

func (m *fkStore) setTurn(sid, taskID, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setTurnLocked(m.ss[sid], taskID, status)
}

func (m *fkStore) setTurnLocked(s *fkSession, taskID, status string) {
	if status != "queued" && s.st.CurrentTaskID == taskID {
		s.st.CurrentTaskID = ""
	}
	if status == "paused" {
		s.st.BlockedByTaskID = taskID
	} else if s.st.BlockedByTaskID == taskID {
		s.st.BlockedByTaskID = ""
	}
	s.st.NonTerminalTurns = slices.DeleteFunc(s.st.NonTerminalTurns, func(f TurnFact) bool {
		return f.TaskID == taskID && (status == "succeeded" || status == "failed" || status == "cancelled")
	})
	for i := range s.st.NonTerminalTurns {
		if s.st.NonTerminalTurns[i].TaskID == taskID {
			s.st.NonTerminalTurns[i].Status = status
		}
	}
}

func (m *fkStore) session(id string) (State, string, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, _ := m.loadLocked(id)
	return st, m.ss[id].lastError, slices.Clone(m.ss[id].events)
}

func (m *fkStore) incarnation(id string) Incarnation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *m.incs[id]
}

// fkEnv 记录环境操作；StopEnv 在 Store 中记录 stopped_at。
type fkEnv struct {
	mu         sync.Mutex
	log        *fkLog
	st         *fkStore
	freezeErr  error
	freezeGate chan struct{} // 非 nil 时 FreezeEnv 等它关闭
	thawErr    error
	createErr  []error
	stops      []StopReport
}

func (f *fkEnv) CreateIncarnationEnv(_ context.Context, s IncarnationEnv) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.add("create_env:%s:%s sock=%s restore=%s mem=%d", s.SessionID, s.EnvID, s.GatewaySocket, s.RestoreDir, s.MemoryBytes)
	if len(f.createErr) > 0 {
		err := f.createErr[0]
		f.createErr = f.createErr[1:]
		return err
	}
	return nil
}

func (f *fkEnv) FreezeEnv(ctx context.Context, envID string) error {
	f.mu.Lock()
	gate, err := f.freezeGate, f.freezeErr
	f.mu.Unlock()
	f.log.add("freeze:%s", envID)
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.log.add("frozen:%s", envID)
	return err
}

func (f *fkEnv) ThawEnv(_ context.Context, envID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.add("thaw:%s", envID)
	return f.thawErr
}

func (f *fkEnv) StopEnv(_ context.Context, envID string) (StopReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.stops) > 0 {
		r := f.stops[0]
		f.stops = f.stops[1:]
		f.log.add("stop:%s recorded=%v", envID, r.Recorded)
		return r, nil
	}
	f.st.mu.Lock()
	f.st.stopped[envID] = true
	f.st.mu.Unlock()
	f.log.add("stop:%s recorded=true", envID)
	return StopReport{Stopped: true, Recorded: true}, nil
}

func (f *fkEnv) StageRestore(_ context.Context, sessionID, envID, sha string) (string, error) {
	f.log.add("stage:%s:%s:%s", sessionID, envID, sha)
	return "/data/envs/" + envID + "/restore", nil
}

func (f *fkEnv) UnstageRestore(_ context.Context, envID string) error {
	f.log.add("unstage:%s", envID)
	return nil
}

func (f *fkEnv) DeleteWorkspace(_ context.Context, sessionID string) error {
	f.log.add("delete_workspace:%s", sessionID)
	return nil
}

func (f *fkEnv) ReleaseUID(_ context.Context, sessionID string) error {
	f.log.add("release_uid:%s", sessionID)
	return nil
}

// fkWorkers 启动 fkInc；startErr 依次给出启动失败。
type fkWorkers struct {
	mu       sync.Mutex
	log      *fkLog
	startErr []error
	starts   []WorkerStart
	incs     map[string]*fkInc
}

func (f *fkWorkers) Start(_ context.Context, s WorkerStart) (IncarnationHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, s)
	f.log.add("start:%s:%s", s.IncarnationID, s.EnvID)
	if len(f.startErr) > 0 {
		err := f.startErr[0]
		f.startErr = f.startErr[1:]
		return nil, err
	}
	inc := &fkInc{id: s.IncarnationID, log: f.log, exited: make(chan struct{}), released: true}
	f.incs[s.IncarnationID] = inc
	return inc, nil
}

func (f *fkWorkers) inc(id string) *fkInc {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.incs[id]
}

func (f *fkWorkers) all() []WorkerStart {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.starts)
}

type fkInc struct {
	mu       sync.Mutex
	id       string
	log      *fkLog
	exited   chan struct{}
	once     sync.Once
	released bool
	quiesced string
}

func (i *fkInc) Release(_ context.Context, attemptID, verdict, committed string) (bool, string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.log.add("release:%s:%s:%s:%s", i.id, attemptID, verdict, committed)
	if !i.released {
		return false, EndReleaseTimeout
	}
	return true, ""
}

func (i *fkInc) Quiesce(context.Context, time.Duration) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.log.add("quiesce:%s", i.id)
	return i.quiesced, nil
}

func (i *fkInc) Close(context.Context, time.Duration) error {
	i.log.add("close:%s", i.id)
	i.exit()
	return nil
}

func (i *fkInc) Exited() <-chan struct{} { return i.exited }

func (i *fkInc) exit() { i.once.Do(func() { close(i.exited) }) }

func (i *fkInc) set(f func(i *fkInc)) {
	i.mu.Lock()
	defer i.mu.Unlock()
	f(i)
}

type fkGateway struct{ log *fkLog }

func (g fkGateway) BindIncarnation(_ context.Context, incID, envID string) (string, error) {
	g.log.add("bind:%s:%s", incID, envID)
	return "/data/gateway/" + incID + ".sock", nil
}

func (g fkGateway) RevokeIncarnation(_ context.Context, incID string) error {
	g.log.add("revoke:%s", incID)
	return nil
}

type fkAdmission struct {
	mu   sync.Mutex
	log  *fkLog
	n    uint64
	held map[uint64]bool
}

func (a *fkAdmission) AcquireMemory(_ context.Context, sessionID string, bytes int64) (Grant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	a.held[a.n] = true
	a.log.add("mem_acquire:%s:%d:%d", sessionID, bytes, a.n)
	return Grant{ID: a.n}, nil
}

func (a *fkAdmission) Release(g Grant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.held, g.ID)
	a.log.add("mem_release:%d", g.ID)
}

func (a *fkAdmission) heldCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.held)
}

// ---- 测试装置 ----

type harness struct {
	t     *testing.T
	clk   *fkClock
	log   *fkLog
	st    *fkStore
	env   *fkEnv
	wk    *fkWorkers
	adm   *fkAdmission
	sch   *Scheduler
	ctx   context.Context
	fatal chan error
}

func newHarness(t *testing.T) *harness {
	clk, log := &fkClock{now: t0}, &fkLog{}
	st := newFkStore(clk, log)
	h := &harness{t: t, clk: clk, log: log, st: st, env: &fkEnv{log: log, st: st}, wk: &fkWorkers{log: log, incs: map[string]*fkInc{}},
		adm: &fkAdmission{log: log, held: map[uint64]bool{}}, fatal: make(chan error, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	var mu sync.Mutex
	n := 0
	h.sch = NewScheduler(ctx, Deps{Store: st, Env: h.env, Workers: h.wk, Gateway: fkGateway{log}, Admission: h.adm, Clock: clk,
		Config: testConfig, OnFatal: func(_ string, err error) { h.fatal <- err },
		IDs: func() string {
			mu.Lock()
			defer mu.Unlock()
			n++
			return fmt.Sprintf("id-%d", n)
		}})
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { h.sch.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("ctx 结束后 actor 未退出")
		}
	})
	return h
}

type grantResult struct {
	info GrantInfo
	err  error
}

func (h *harness) grantAsync(sid, taskID string) chan grantResult {
	ch := make(chan grantResult, 1)
	go func() {
		info, err := h.sch.Grant(h.ctx, sid, taskID)
		ch <- grantResult{info, err}
	}()
	return ch
}

// await 等待授予结果（期间推进时钟，使退避与定时器得以触发）。
func (h *harness) await(ch chan grantResult) grantResult {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case r := <-ch:
			return r
		case err := <-h.fatal:
			h.t.Fatalf("actor 致命错误: %v", err)
		case <-time.After(2 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("授予超时；日志: %v", h.log.all())
		}
	}
}

func (h *harness) grant(sid, taskID string) GrantInfo {
	h.t.Helper()
	r := h.await(h.grantAsync(sid, taskID))
	if r.err != nil {
		h.t.Fatalf("Grant(%s, %s): %v；日志: %v", sid, taskID, r.err, h.log.all())
	}
	return r.info
}

func (h *harness) handoff(sid string, ho Handoff) StopReport {
	h.t.Helper()
	ch := make(chan StopReport, 1)
	go func() {
		r, err := h.sch.Handoff(h.ctx, sid, ho)
		if err != nil {
			h.t.Errorf("Handoff: %v", err)
		}
		ch <- r
	}()
	select {
	case r := <-ch:
		return r
	case err := <-h.fatal:
		h.t.Fatalf("actor 致命错误: %v", err)
	case <-time.After(5 * time.Second):
		h.t.Fatalf("交还超时；日志: %v", h.log.all())
	}
	return StopReport{}
}

func (h *harness) waitFor(desc string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		select {
		case err := <-h.fatal:
			h.t.Fatalf("等待 %s 时 actor 致命错误: %v", desc, err)
		default:
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("等待 %s 超时；日志: %v", desc, h.log.all())
		}
		time.Sleep(time.Millisecond)
	}
}

// advanceUntil 以 step 推进时钟直到条件成立（每步之间让出 actor 处理计时器）。
func (h *harness) advanceUntil(desc string, step time.Duration, cond func() bool) {
	h.t.Helper()
	for i := 0; !cond(); i++ {
		select {
		case err := <-h.fatal:
			h.t.Fatalf("等待 %s 时 actor 致命错误: %v", desc, err)
		default:
		}
		if i > 3000 {
			h.t.Fatalf("推进时钟后仍未 %s；日志: %v", desc, h.log.all())
		}
		h.clk.Advance(step)
		time.Sleep(time.Millisecond)
	}
}

// settle 分步推进时钟 d，让 actor 处理期间的定时器与周期读取（用于断言"某事没有发生"）。
func (h *harness) settle(d time.Duration) {
	for i := 0; i < 20; i++ {
		h.clk.Advance(d / 20)
		time.Sleep(2 * time.Millisecond)
	}
}

func (h *harness) is(sid, want string) func() bool {
	return func() bool {
		st, _, _ := h.st.session(sid)
		return st.Status == want
	}
}

func (h *harness) logged(prefix string, n int) func() bool {
	return func() bool { return h.log.count(prefix) >= n }
}

// before 断言日志中 a 的第 1 次出现早于 b 的第 1 次出现（两者都必须出现）。
func (h *harness) before(a, b string) {
	h.t.Helper()
	ia, ib := h.log.index(a, 1), h.log.index(b, 1)
	if ia < 0 || ib < 0 || ia >= ib {
		h.t.Errorf("期望 %q 早于 %q；日志: %v", a, b, h.log.all())
	}
}

// idleSession 建立会话 sid（turn t1）并经首个授予进入 idle，再完成 t1（succeeded）并交还：返回 incarnation。
func (h *harness) idleSession(sid string) GrantInfo {
	h.t.Helper()
	h.st.addSession(sid, "t1")
	g := h.grant(sid, "t1")
	h.st.startTurn(h.t, sid, "t1", g)
	h.st.verdict(sid, "t1", "succeeded")
	if r := h.handoff(sid, Handoff{TaskID: "t1", AttemptID: "a-" + sid, EnvID: g.EnvID, Verdict: "succeeded"}); !r.Recorded {
		h.t.Fatalf("释放应报告 Recorded：%+v", r)
	}
	h.waitFor("会话回到 idle", h.is(sid, StatusIdle))
	return g
}

// ---- actor ----

// 新会话：首个授予申请 → 内存 → incarnation → Gateway 入口 → 环境 → Worker（无 Resume）→ incarnation idle → 会话
// idle → 授予；turn 结束后 Release 带裁决与会话指针，incarnation 与会话回到 idle。
func TestActorCreateGrantAndRelease(t *testing.T) {
	h := newHarness(t)
	h.st.addSession("s1", "t1", "t2")
	g := h.grant("s1", "t1")
	if g.IncarnationID != "id-1" || g.EnvID != "id-2" {
		t.Fatalf("授予 = %+v", g)
	}
	for _, p := range [][2]string{{"mem_acquire:s1:100", "create_incarnation:id-1:id-2"}, {"create_incarnation:id-1", "bind:id-1:id-2"},
		{"bind:id-1", "create_env:s1:id-2 sock=/data/gateway/id-1.sock restore= mem=100"}, {"create_env:", "start:id-1:id-2"},
		{"start:id-1", "incarnation:id-1:starting→idle"}, {"incarnation:id-1:starting→idle", "transition:s1:creating→idle"}} {
		h.before(p[0], p[1])
	}
	if ws := h.wk.all(); len(ws) != 1 || ws[0].Resume != nil {
		t.Errorf("新会话不恢复：%+v", ws)
	}
	if inc, ok := h.sch.Incarnation("id-2"); !ok || inc != h.wk.inc("id-1") {
		t.Errorf("Scheduler.Incarnation(env) 应返回存活的句柄")
	}
	// 授予只给可运行的下一个 turn：t1 占用会话时 t2 等待。
	h.st.startTurn(t, "s1", "t1", g)
	ch2 := h.grantAsync("s1", "t2")
	h.st.verdict("s1", "t1", "succeeded")
	r := h.handoff("s1", Handoff{TaskID: "t1", AttemptID: "a1", EnvID: "id-2", Verdict: "succeeded", CommittedSessionCheckpointID: "sc-1"})
	if !r.Recorded {
		t.Fatalf("Release 成功应报告 Recorded：%+v", r)
	}
	if h.log.count("release:id-1:a1:succeeded:sc-1") != 1 {
		t.Errorf("Release 应带裁决与会话指针；日志: %v", h.log.all())
	}
	if g2 := h.await(ch2); g2.err != nil || g2.info != g {
		t.Fatalf("释放后同一 incarnation 授予下一个 turn：%+v", g2)
	}
	if st, _, ev := h.st.session("s1"); st.Status != StatusIdle || st.Incarnation.Status != IncIdle || !slices.Equal(ev, []string{`{"state":"idle"}`}) {
		t.Errorf("会话 = %s / %s，事件 %v", st.Status, st.Incarnation.Status, ev)
	}
}

// E31：FreezeEnv 返回成功之前不记录 frozen；冻结确认后才 Transition(frozen) 并追加事件。
func TestActorFreezeOnlyAfterConfirmed(t *testing.T) {
	h := newHarness(t)
	gate := make(chan struct{})
	h.env.freezeGate = gate
	g := h.idleSession("s1")
	h.advanceUntil("开始冻结", 30*time.Second, h.logged("freeze:"+g.EnvID, 1))
	h.before("quiesce:"+g.IncarnationID, "freeze:")
	if st, _, _ := h.st.session("s1"); st.Status != StatusQuiescing || h.log.count("incarnation:"+g.IncarnationID+":quiescing→frozen") != 0 {
		t.Fatalf("冻结确认之前会话 = %s；日志: %v", st.Status, h.log.all())
	}
	h.st.with("s1", func(*fkSession) { h.st.transFail[StatusFrozen] = persistence.ErrUnavailable }) // 暂时失败：按退避重跑
	close(gate)
	h.advanceUntil("frozen", time.Second, h.is("s1", StatusFrozen))
	h.before("frozen:"+g.EnvID, "incarnation:"+g.IncarnationID+":quiescing→frozen")
	h.before("incarnation:"+g.IncarnationID+":quiescing→frozen", "transition:s1:quiescing→frozen")
	if _, _, ev := h.st.session("s1"); ev[len(ev)-1] != `{"state":"frozen"}` {
		t.Errorf("事件 = %v", ev)
	}
	if h.adm.heldCount() != 1 {
		t.Errorf("frozen 的内存保留（计入容量）")
	}
}

// E31：冻结未确认 → 驱逐（StopEnv、EndIncarnation(freeze_failed)、evicted），不记录 frozen，内存归还。
func TestActorFreezeFailureEvicts(t *testing.T) {
	h := newHarness(t)
	h.env.freezeErr = errors.New("cgroup.events 未确认 frozen 1")
	g := h.idleSession("s1")
	h.advanceUntil("驱逐", 30*time.Second, h.is("s1", StatusEvicted))
	h.before("freeze:"+g.EnvID, "stop:"+g.EnvID)
	h.before("stop:"+g.EnvID, "end_incarnation:"+g.IncarnationID+":freeze_failed")
	if h.log.count("transition:s1:quiescing→frozen") != 0 {
		t.Errorf("冻结失败不应记录 frozen；日志: %v", h.log.all())
	}
	if h.adm.heldCount() != 0 {
		t.Errorf("驱逐应归还 incarnation 内存")
	}
	if _, _, ev := h.st.session("s1"); ev[len(ev)-1] != `{"state":"evicted"}` {
		t.Errorf("事件 = %v", ev)
	}
}

// quiesce 报告的 checkpoint 与最新已提交的不同 → 不冻结、驱逐（quiesce_failed）。
func TestActorQuiesceMismatchEvicts(t *testing.T) {
	h := newHarness(t)
	g := h.idleSession("s1")
	h.st.with("s1", func(s *fkSession) { s.st.Latest = &Checkpoint{CheckpointID: "sc-2", State: json.RawMessage(`{}`)} })
	h.wk.inc(g.IncarnationID).set(func(i *fkInc) { i.quiesced = "sc-1" })
	h.sch.Notify("s1")
	h.advanceUntil("驱逐", 30*time.Second, h.is("s1", StatusEvicted))
	if h.log.count("freeze:") != 0 || h.st.incarnation(g.IncarnationID).EndReason != EndQuiesceFailed {
		t.Errorf("不一致时不应冻结；incarnation %+v；日志: %v", h.st.incarnation(g.IncarnationID), h.log.all())
	}
}

// E28（actor 层）：idle 时进程退出 → 结束 incarnation、evicted；下一个授予 → restoring → 新 incarnation（同一会话的
// workspace 与 UID 范围由 CreateIncarnationEnv 按 session_id 决定）、新 socket；state_ref 的 checkpoint 经 StageRestore，
// Worker 收到 StagedPath 而不是 state_ref；ready 之后卸载暂存。
func TestActorExitedThenColdRestoreStagesState(t *testing.T) {
	h := newHarness(t)
	g := h.idleSession("s1")
	sha := strings.Repeat("ab", 32)
	h.st.with("s1", func(s *fkSession) {
		s.st.Latest = &Checkpoint{CheckpointID: "sc-3", StateRef: sha, Refs: []string{"r1"}}
		s.st.NonTerminalTurns = append(s.st.NonTerminalTurns, TurnFact{TaskID: "t2", Status: "queued", TurnIndex: 1})
	})
	h.wk.inc(g.IncarnationID).exit()
	h.waitFor("evicted", h.is("s1", StatusEvicted))
	if inc := h.st.incarnation(g.IncarnationID); inc.EndReason != EndWorkerExited {
		t.Errorf("end_reason = %q", inc.EndReason)
	}
	if _, ok := h.sch.Incarnation(g.EnvID); ok {
		t.Errorf("销毁后不应再返回旧句柄")
	}
	g2 := h.grant("s1", "t2")
	if g2.IncarnationID == g.IncarnationID || g2.EnvID == g.EnvID {
		t.Fatalf("恢复应新建 incarnation 与环境：%+v", g2)
	}
	for _, p := range [][2]string{{"transition:s1:evicted→restoring", "create_incarnation:" + g2.IncarnationID},
		{"bind:" + g2.IncarnationID, "stage:s1:" + g2.EnvID + ":" + sha},
		{"stage:s1:", "create_env:s1:" + g2.EnvID + " sock=/data/gateway/" + g2.IncarnationID + ".sock restore=/data/envs/" + g2.EnvID + "/restore"},
		{"start:" + g2.IncarnationID, "unstage:" + g2.EnvID}, {"unstage:" + g2.EnvID, "transition:s1:restoring→idle"}} {
		h.before(p[0], p[1])
	}
	ws := h.wk.all()
	if r := ws[len(ws)-1].Resume; r == nil || r.CheckpointID != "sc-3" || r.StagedPath != "/run/agentbox/restore/"+sha || r.State != nil ||
		!slices.Equal(r.Refs, []string{"r1"}) {
		t.Errorf("Resume = %+v", r)
	}
	if _, _, ev := h.st.session("s1"); !slices.Contains(ev, `{"state":"restoring"}`) {
		t.Errorf("事件 = %v", ev)
	}
}

// 唤醒失败：thaw 失败 → evicting → evicted → 冷恢复一次成功（inline state 直接传）。
func TestActorThawFailureColdRestores(t *testing.T) {
	h := newHarness(t)
	g := h.idleSession("s1")
	h.st.with("s1", func(s *fkSession) {
		s.st.Latest = &Checkpoint{CheckpointID: "sc-1", State: json.RawMessage(`{"n":1}`)}
		s.st.NonTerminalTurns = []TurnFact{{TaskID: "t2", Status: "queued", TurnIndex: 1}}
	})
	h.wk.inc(g.IncarnationID).set(func(i *fkInc) { i.quiesced = "sc-1" })
	h.advanceUntil("frozen", 30*time.Second, h.is("s1", StatusFrozen))
	h.env.thawErr = errors.New("thaw 超时")
	g2 := h.grant("s1", "t2")
	h.before("thaw:"+g.EnvID, "end_incarnation:"+g.IncarnationID+":thaw_failed")
	h.before("end_incarnation:"+g.IncarnationID, "transition:s1:evicted→restoring")
	ws := h.wk.all()
	if r := ws[len(ws)-1].Resume; r == nil || string(r.State) != `{"n":1}` || r.StagedPath != "" || g2.IncarnationID == g.IncarnationID {
		t.Errorf("Resume = %+v，授予 %+v", r, g2)
	}
	if h.log.count("stage:") != 0 {
		t.Errorf("inline state 不暂存")
	}
}

// 冷恢复连续失败两次 → 授予得 ErrUnavailable、last_error 写入、session_state{evicted, user_message}；下一次授予重新尝试。
func TestActorRestoreFailsTwiceIsUnavailable(t *testing.T) {
	h := newHarness(t)
	h.st.addSession("s1", "t1")
	h.st.with("s1", func(s *fkSession) { s.st.Status = StatusEvicted })
	h.wk.startErr = []error{errors.New("ready 超时"), errors.New("ready 超时")}
	r := h.await(h.grantAsync("s1", "t1"))
	if !errors.Is(r.err, ErrUnavailable) {
		t.Fatalf("授予 = %+v", r)
	}
	st, lastErr, ev := h.st.session("s1")
	if st.Status != StatusEvicted || !strings.Contains(lastErr, "ready 超时") ||
		ev[len(ev)-1] != `{"state":"evicted","user_message":"会话暂时无法恢复"}` {
		t.Fatalf("会话 = %s，last_error %q，事件 %v", st.Status, lastErr, ev)
	}
	if h.log.count("start:") != 2 || h.log.count("end_incarnation:") != 2 || h.adm.heldCount() != 0 {
		t.Errorf("两次启动失败都应清理（环境停止后结束 incarnation、归还内存）；日志: %v", h.log.all())
	}
	if g := h.grant("s1", "t1"); g.IncarnationID == "" {
		t.Errorf("之后的授予应重新尝试恢复")
	}
}

// E29、E30（actor 层）：Release 失败 → 销毁 incarnation（end_reason = release_timeout），交还仍报告 Recorded；会话指针
// 与 turn 的事实不由 session actor 改变；下一个 turn 在新 incarnation 中运行。
func TestActorReleaseFailureDestroysIncarnation(t *testing.T) {
	h := newHarness(t)
	h.st.addSession("s1", "t1", "t2")
	h.st.with("s1", func(s *fkSession) { s.st.Latest = &Checkpoint{CheckpointID: "sc-1", State: json.RawMessage(`{}`)} })
	g := h.grant("s1", "t1")
	h.st.startTurn(t, "s1", "t1", g)
	h.wk.inc(g.IncarnationID).set(func(i *fkInc) { i.released = false })
	h.st.verdict("s1", "t1", "succeeded")
	before, _, _ := h.st.session("s1")
	r := h.handoff("s1", Handoff{TaskID: "t1", AttemptID: "a1", EnvID: g.EnvID, Verdict: "succeeded", CommittedSessionCheckpointID: "sc-1"})
	if !r.Recorded {
		t.Fatalf("销毁后应报告 Recorded：%+v", r)
	}
	h.before("release:"+g.IncarnationID, "stop:"+g.EnvID)
	h.before("stop:"+g.EnvID, "end_incarnation:"+g.IncarnationID+":release_timeout")
	after, _, _ := h.st.session("s1")
	if after.Latest.CheckpointID != before.Latest.CheckpointID || !reflect.DeepEqual(after.NonTerminalTurns, before.NonTerminalTurns) {
		t.Errorf("释放失败不改变会话指针与 turn：%+v → %+v", before, after)
	}
	g2 := h.grant("s1", "t2")
	if g2.IncarnationID == g.IncarnationID {
		t.Fatalf("下一个 turn 应在新 incarnation 中：%+v", g2)
	}
}

// running 中进程退出：等待 task actor 的交还（Destroy=true），之后下一次授予走恢复。
func TestActorRunningExitWaitsForHandoff(t *testing.T) {
	h := newHarness(t)
	h.st.addSession("s1", "t1")
	g := h.grant("s1", "t1")
	h.st.startTurn(t, "s1", "t1", g)
	h.wk.inc(g.IncarnationID).exit()
	h.settle(30 * time.Second)
	if h.log.count("stop:") != 0 {
		t.Fatalf("running 时不应自行销毁；日志: %v", h.log.all())
	}
	h.st.verdict("s1", "t1", "queued") // 故障重试：turn 仍占用会话
	if r := h.handoff("s1", Handoff{TaskID: "t1", AttemptID: "a1", EnvID: g.EnvID, Destroy: true}); !r.Recorded {
		t.Fatalf("交还 = %+v", r)
	}
	h.waitFor("evicted", h.is("s1", StatusEvicted))
	if inc := h.st.incarnation(g.IncarnationID); inc.EndReason != EndTaskDestroyed {
		t.Errorf("end_reason = %q", inc.EndReason)
	}
	if g2 := h.grant("s1", "t1"); g2.IncarnationID == g.IncarnationID {
		t.Fatalf("重试应在恢复的新 incarnation 中：%+v", g2)
	}
}

// E32：关闭时有 queued 与 paused turn → 等它们终态后才 Close/StopEnv；DeleteWorkspace 严格在 StopEnv Recorded 之后；
// 关闭期间的授予申请得 ErrUnavailable。
func TestActorCloseWaitsForTurnsThenDeletesWorkspace(t *testing.T) {
	h := newHarness(t)
	g := h.idleSession("s1")
	h.st.with("s1", func(s *fkSession) {
		s.st.NonTerminalTurns = []TurnFact{{TaskID: "t2", Status: "paused", TurnIndex: 1}, {TaskID: "t3", Status: "queued", TurnIndex: 2}}
		s.st.BlockedByTaskID, s.st.Desired = "t2", "closed"
	})
	h.env.stops = []StopReport{{Stopped: true}} // 第一次停止未记录：不得删除 workspace
	h.sch.Notify("s1")
	h.waitFor("closing", h.is("s1", StatusClosing))
	if r := h.await(h.grantAsync("s1", "t3")); !errors.Is(r.err, ErrUnavailable) {
		t.Fatalf("关闭中的授予 = %+v", r)
	}
	h.settle(30 * time.Second)
	if h.log.count("close:") != 0 || h.log.count("stop:") != 0 {
		t.Fatalf("turn 终态之前不应关闭 incarnation；日志: %v", h.log.all())
	}
	h.st.setTurn("s1", "t2", "cancelled")
	h.st.setTurn("s1", "t3", "cancelled")
	h.sch.Notify("s1")
	h.advanceUntil("closed", time.Second, h.is("s1", StatusClosed))
	h.before("close:"+g.IncarnationID, "stop:"+g.EnvID)
	if i, j := h.log.index("stop:"+g.EnvID+" recorded=true", 1), h.log.index("delete_workspace:s1", 1); i < 0 || j < i {
		t.Errorf("workspace 只在环境停止（Recorded）之后删除；日志: %v", h.log.all())
	}
	h.before("delete_workspace:s1", "release_uid:s1")
	h.before("release_uid:s1", "finish_close:s1")
	if h.adm.heldCount() != 0 {
		t.Errorf("关闭应归还内存")
	}
}

// LRU：两个 frozen、一个 idle，压力缺口为一个 incarnation 的内存 → 只驱逐最久未活动的 frozen。
func TestSchedulerMemoryPressureEvictsLRUFrozen(t *testing.T) {
	h := newHarness(t)
	gs := []GrantInfo{h.idleSession("s1"), h.idleSession("s2")}
	h.advanceUntil("s1、s2 frozen", 30*time.Second, func() bool { return h.is("s1", StatusFrozen)() && h.is("s2", StatusFrozen)() })
	gs = append(gs, h.idleSession("s3")) // 刚活动：idle
	h.st.with("s1", func(s *fkSession) { s.st.LastActiveAt = t0.Add(time.Minute) })
	h.st.with("s2", func(s *fkSession) { s.st.LastActiveAt = t0 }) // 最久未活动
	h.sch.OnMemoryPressure(testConfig.IncarnationMemory)
	h.waitFor("s2 被驱逐", h.is("s2", StatusEvicted))
	h.settle(time.Minute)
	if !h.is("s1", StatusFrozen)() || !h.is("s3", StatusIdle)() || h.log.count("stop:"+gs[0].EnvID) != 0 || h.log.count("stop:"+gs[2].EnvID) != 0 {
		t.Errorf("只驱逐最久未活动的 frozen；日志: %v", h.log.all())
	}
	if h.log.count("thaw:") != 0 {
		t.Errorf("驱逐 frozen 不 thaw")
	}
	if inc := h.st.incarnation(gs[1].IncarnationID); inc.EndReason != EndMemoryPressure {
		t.Errorf("end_reason = %q", inc.EndReason)
	}
}

// 驱逐时限：idle 之后 EvictAfter → 驱逐；frozen 状态下到期直接 StopEnv，不调用 ThawEnv。
func TestActorEvictAfterFromFrozenWithoutThaw(t *testing.T) {
	h := newHarness(t)
	g := h.idleSession("s1")
	h.advanceUntil("frozen", 30*time.Second, h.is("s1", StatusFrozen))
	frozenAt := h.clk.Now()
	h.advanceUntil("evicted", time.Minute, h.is("s1", StatusEvicted))
	if h.log.count("thaw:") != 0 || h.st.incarnation(g.IncarnationID).EndReason != EndEvicted {
		t.Errorf("frozen 到期应直接停止；日志: %v", h.log.all())
	}
	if el := h.clk.Now().Sub(t0); el < time.Hour || el > time.Hour+2*time.Minute || frozenAt.Sub(t0) < testConfig.IdleFreeze {
		t.Errorf("驱逐时间 %s（冻结于 %s），期望自 idle_since 起约 1 h", el, frozenAt.Sub(t0))
	}
}

// Scheduler.Start 为未关闭的会话建立 actor；遗留的存活 incarnation（不属于本进程）被销毁并转 evicted。
func TestSchedulerStartDestroysOrphans(t *testing.T) {
	h := newHarness(t)
	h.st.addSession("s1", "t1")
	h.st.with("s1", func(s *fkSession) { s.st.Status, s.st.CurrentIncarnationID = StatusIdle, "old" })
	h.st.incs["old"] = &Incarnation{IncarnationID: "old", SessionID: "s1", EnvID: "old-env", Status: IncIdle}
	h.st.addSession("s2")
	h.st.with("s2", func(s *fkSession) { s.st.Status = StatusClosed })
	if err := h.sch.Start(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("evicted", h.is("s1", StatusEvicted))
	if inc := h.st.incarnation("old"); inc.EndReason != EndOrphaned {
		t.Errorf("end_reason = %q", inc.EndReason)
	}
	h.before("stop:old-env", "end_incarnation:old")
}
