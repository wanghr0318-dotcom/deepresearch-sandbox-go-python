package task

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

var t0 = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func i64(v int64) *int64 { return &v }

func base() State {
	return State{TaskID: "t1", TaskStatus: "queued", Desired: "run", ControlVersion: 1, AppliedControlVersion: 1,
		MaxFaultRetries: 3, Now: t0}
}

func att(id, status string) *AttemptState {
	return &AttemptState{AttemptID: id, EnvID: "env-" + id, Status: status, StoredStatus: "starting"}
}

func endedAtt(id string, envStopped bool) *AttemptState {
	a := att(id, "ended")
	a.StopRequested, a.EnvStopped = true, envStopped
	return a
}

func success() Outcome {
	return Outcome{Class: ClassSucceeded, ProposalKind: "result", Result: json.RawMessage(`{"ok":true}`), ExitCode: i64(0)}
}

func crash() Outcome { return Outcome{Class: "crashed_signal", Retry: RetryFault, ExitSignal: i64(9)} }

func oom() Outcome {
	return Outcome{Class: "worker_oom_likely", Retry: RetryOOM, ExitSignal: i64(9), OOMKillDelta: 1}
}

// kinds 把副作用序列写成以逗号分隔的类型名，便于在表中比较。
func kinds(effects []Effect) string {
	names := make([]string, len(effects))
	for i, e := range effects {
		names[i] = strings.TrimPrefix(reflect.TypeOf(e).String(), "task.")
	}
	return strings.Join(names, ",")
}

func attStatus(s State) string {
	if s.Attempt == nil {
		return ""
	}
	return s.Attempt.Status
}

func mustDecide(t *testing.T, s State, e Event) Decision {
	t.Helper()
	d, err := Decide(s, e)
	if err != nil {
		t.Fatalf("Decide(%s, %T) 出错: %v", s.TaskStatus, e, err)
	}
	return d
}

// run 依次投递事件，返回最终状态与全部副作用。
func run(t *testing.T, s State, events ...Event) (State, []Effect) {
	t.Helper()
	var all []Effect
	for _, e := range events {
		d := mustDecide(t, s, e)
		s, all = d.Next, append(all, d.Effects...)
	}
	return s, all
}

func finalizeOf(t *testing.T, effects []Effect) Verdict {
	t.Helper()
	var out *Verdict
	for _, e := range effects {
		if f, ok := e.(Finalize); ok {
			v := f.Verdict
			out = &v
		}
	}
	if out == nil {
		t.Fatalf("副作用中没有 Finalize: %s", kinds(effects))
	}
	return *out
}

func has[T Effect](effects []Effect) bool {
	for _, e := range effects {
		if _, ok := e.(T); ok {
			return true
		}
	}
	return false
}

// ---- 状态 × 事件 ----

type fixture struct {
	name string
	s    State
}

func fixtures() []fixture {
	with := func(f func(*State)) State { s := base(); f(&s); return s }
	nb := t0.Add(10 * time.Second)
	return []fixture{
		{"queued/idle", base()},
		{"queued/requested", with(func(s *State) { s.SlotRequested = true })},
		{"queued/creating", with(func(s *State) { s.SlotHeld = true })},
		{"queued/backoff", with(func(s *State) {
			s.Attempt, s.NotBefore, s.NextRetry, s.FaultRetriesUsed = endedAtt("a1", true), &nb, RetryFault, 0
		})},
		{"queued/old-env", with(func(s *State) { s.Attempt, s.SlotHeld = endedAtt("a1", false), true })},
		{"running/starting", with(func(s *State) { s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a2", "starting"), true })},
		{"running/handshaking", with(func(s *State) { s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a2", "handshaking"), true })},
		{"running/active", with(func(s *State) { s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a2", "active"), true })},
		{"pausing/active", with(func(s *State) {
			s.TaskStatus, s.Desired, s.Attempt, s.SlotHeld = "pausing", "pause", att("a2", "active"), true
		})},
		{"cancelling/active", with(func(s *State) {
			s.TaskStatus, s.Desired, s.Attempt, s.SlotHeld = "cancelling", "cancel", att("a2", "active"), true
		})},
		{"running/finishing", with(func(s *State) {
			a := att("a2", "finishing")
			o := success()
			a.Outcome, a.StopRequested = &o, true
			a.Verdict = &Verdict{AttemptID: "a2", TaskID: "t1", ControlVersion: 1, TaskStatus: "succeeded"}
			s.TaskStatus, s.Attempt, s.SlotHeld = "running", a, true
		})},
		{"running/stop_blocked", with(func(s *State) {
			a := att("a2", "stop_blocked")
			o := crash()
			a.Outcome, a.StoredStatus = &o, "stop_blocked"
			s.TaskStatus, s.Attempt, s.SlotHeld = "running", a, true
		})},
		{"paused", with(func(s *State) { s.TaskStatus, s.Desired, s.Attempt = "paused", "pause", endedAtt("a1", true) })},
		{"succeeded", with(func(s *State) { s.TaskStatus, s.Attempt = "succeeded", endedAtt("a1", true) })},
		{"cancelled/env-running", with(func(s *State) {
			s.TaskStatus, s.Desired, s.Attempt, s.SlotHeld = "cancelled", "cancel", endedAtt("a1", false), true
		})},
	}
}

var eventNames = []string{"Tick", "ControlChanged", "SlotGranted", "AttemptCreated", "EnvCreated", "WorkerStarted",
	"AttemptFinished", "VerdictCommitted", "EnvStopped", "StopUnconfirmed", "StoreFailed"}

// eventFor 构造针对当前 attempt 的事件；没有 attempt 时使用不存在的 ID（即过期事件）。
func eventFor(name string, s State) Event {
	id, env := "a-none", "env-none"
	if s.Attempt != nil {
		id, env = s.Attempt.AttemptID, s.Attempt.EnvID
	}
	switch name {
	case "Tick":
		return Tick{}
	case "ControlChanged":
		return ControlChanged{Desired: "cancel", ControlVersion: s.ControlVersion + 1}
	case "SlotGranted":
		return SlotGranted{}
	case "AttemptCreated":
		return AttemptCreated{AttemptID: "a9", EnvID: "env-a9", Status: "starting"}
	case "EnvCreated":
		return EnvCreated{AttemptID: id, EnvID: env}
	case "WorkerStarted":
		return WorkerStarted{AttemptID: id, EnvID: env}
	case "AttemptFinished":
		return AttemptFinished{AttemptID: id, EnvID: env, Outcome: success()}
	case "VerdictCommitted":
		if s.Attempt != nil && s.Attempt.Verdict != nil {
			return VerdictCommitted{Verdict: *s.Attempt.Verdict}
		}
		return VerdictCommitted{Verdict: Verdict{AttemptID: id, TaskID: "t1", ControlVersion: s.ControlVersion, TaskStatus: "succeeded"}}
	case "EnvStopped":
		return EnvStopped{AttemptID: id, EnvID: env}
	case "StopUnconfirmed":
		return StopUnconfirmed{AttemptID: id, EnvID: env}
	case "StoreFailed":
		return StoreFailed{Op: OpFinalize, AttemptID: id, ControlVersion: s.ControlVersion, Err: errors.New("rejected")}
	}
	panic(name)
}

type cell struct {
	err             bool
	status, att, fx string
}

func c(status, att, fx string) cell { return cell{status: status, att: att, fx: fx} }

var bad = cell{err: true}

// stale 是没有当前 attempt 的状态中，attempt 事件的结果：过期，只转交资源。
func staleRow(status string, extra map[string]cell) map[string]cell {
	row := map[string]cell{
		"SlotGranted": bad, "AttemptCreated": bad,
		"EnvCreated": c(status, "", "StopEnvironment"), "WorkerStarted": c(status, "", "StopEnvironment"),
		"AttemptFinished": c(status, "", "StopEnvironment"), "VerdictCommitted": c(status, "", ""),
		"EnvStopped": c(status, "", ""), "StopUnconfirmed": c(status, "", ""), "StoreFailed": c(status, "", ""),
	}
	for k, v := range extra {
		row[k] = v
	}
	return row
}

// endedRow 是当前 attempt 已 ended 时的公共结果。
func endedRow(status string, extra map[string]cell) map[string]cell {
	row := map[string]cell{
		"SlotGranted": bad, "AttemptCreated": bad,
		"EnvCreated": c(status, "ended", ""), "WorkerStarted": c(status, "ended", ""), "AttemptFinished": bad,
		"VerdictCommitted": c(status, "ended", ""), "EnvStopped": c(status, "ended", ""),
		"StopUnconfirmed": c(status, "ended", ""), "StoreFailed": c(status, "ended", ""),
	}
	for k, v := range extra {
		row[k] = v
	}
	return row
}

// liveRow 是执行中 attempt 的公共结果。
func liveRow(status, att string, extra map[string]cell) map[string]cell {
	row := map[string]cell{
		"Tick": c(status, att, ""), "SlotGranted": bad, "AttemptCreated": bad,
		"EnvCreated": bad, "WorkerStarted": bad, "AttemptFinished": c(status, "finishing", "RevokeAccess,StopEnvironment,Finalize"),
		"VerdictCommitted": bad, "EnvStopped": c(status, att, "ReleaseSlot"), "StopUnconfirmed": c(status, att, "WakeAt"),
		"StoreFailed": c(status, att, ""),
	}
	for k, v := range extra {
		row[k] = v
	}
	return row
}

var matrix = map[string]map[string]cell{
	"queued/idle": staleRow("queued", map[string]cell{
		"Tick": c("queued", "", "RequestSlot"), "ControlChanged": c("cancelled", "", "ApplyControl")}),
	"queued/requested": staleRow("queued", map[string]cell{
		"Tick": c("queued", "", ""), "ControlChanged": c("cancelled", "", "ApplyControl"),
		"SlotGranted": c("queued", "", "CreateAttempt")}),
	"queued/creating": staleRow("queued", map[string]cell{
		"Tick": c("queued", "", ""), "ControlChanged": c("queued", "", ""), // 创建中：控制等结果后应用
		"AttemptCreated": c("running", "starting", "CreateEnvironment")}),
	"queued/backoff": endedRow("queued", map[string]cell{
		"Tick": c("queued", "ended", "WakeAt"), "ControlChanged": c("cancelled", "ended", "ApplyControl")}),
	"queued/old-env": endedRow("queued", map[string]cell{
		"Tick": c("queued", "ended", ""), "ControlChanged": c("cancelled", "ended", "ApplyControl"),
		"EnvStopped": c("queued", "ended", "ReleaseSlot,RequestSlot"), "StopUnconfirmed": c("queued", "ended", "WakeAt")}),
	"running/starting": liveRow("running", "starting", map[string]cell{
		"ControlChanged": c("cancelling", "finishing", "ApplyControl,RevokeAccess,StopEnvironment,Finalize"),
		"EnvCreated":     c("running", "handshaking", "StartWorker"), "AttemptFinished": bad}),
	"running/handshaking": liveRow("running", "handshaking", map[string]cell{
		"ControlChanged": c("cancelling", "handshaking", "ApplyControl,RevokeAccess,StopEnvironment"),
		"WorkerStarted":  c("running", "active", "")}),
	"running/active": liveRow("running", "active", map[string]cell{
		"ControlChanged": c("cancelling", "active", "ApplyControl,SendControl")}),
	"pausing/active": liveRow("pausing", "active", map[string]cell{
		"ControlChanged": c("cancelling", "active", "ApplyControl,SendControl")}), // cancel 优先于 pause
	"cancelling/active": liveRow("cancelling", "active", map[string]cell{
		"ControlChanged": c("cancelling", "active", "ApplyControl")}),
	"running/finishing": liveRow("running", "finishing", map[string]cell{
		"ControlChanged": c("running", "finishing", "Finalize"), // 按最新控制重算判决
		"EnvCreated":     c("running", "finishing", ""), "WorkerStarted": c("running", "finishing", ""),
		"AttemptFinished": bad, "VerdictCommitted": c("succeeded", "ended", ""),
		"StoreFailed": c("running", "finishing", "WakeAt")}),
	"running/stop_blocked": liveRow("running", "stop_blocked", map[string]cell{
		"Tick":            c("running", "stop_blocked", "RevokeAccess,StopEnvironment"),
		"ControlChanged":  c("cancelling", "stop_blocked", "ApplyControl"),
		"AttemptFinished": bad, "EnvStopped": c("running", "finishing", "ReleaseSlot,Finalize")}),
	"paused": endedRow("paused", map[string]cell{
		"Tick": c("paused", "ended", ""), "ControlChanged": c("cancelled", "ended", "ApplyControl")}),
	"succeeded": endedRow("succeeded", map[string]cell{
		"Tick": c("succeeded", "ended", ""), "ControlChanged": c("succeeded", "ended", "")}), // 迟到的 cancel 不改变终态
	"cancelled/env-running": endedRow("cancelled", map[string]cell{
		"Tick": c("cancelled", "ended", ""), "ControlChanged": c("cancelled", "ended", ""),
		"EnvStopped": c("cancelled", "ended", "ReleaseSlot"), "StopUnconfirmed": c("cancelled", "ended", "WakeAt")}),
}

func TestDecideMatrix(t *testing.T) {
	for _, f := range fixtures() {
		row, ok := matrix[f.name]
		if !ok {
			t.Errorf("矩阵缺少状态 %s", f.name)
			continue
		}
		for _, name := range eventNames {
			want, ok := row[name]
			if !ok {
				t.Errorf("矩阵缺少 %s × %s", f.name, name)
				continue
			}
			t.Run(f.name+"×"+name, func(t *testing.T) {
				before := deepCopy(f.s)
				e := eventFor(name, f.s)
				d, err := Decide(f.s, e)
				if !reflect.DeepEqual(f.s, before) {
					t.Fatalf("Decide 修改了输入状态")
				}
				if want.err {
					if !errors.Is(err, ErrInvalid) {
						t.Fatalf("期望 ErrInvalid，得到 err=%v effects=%s", err, kinds(d.Effects))
					}
					if !reflect.DeepEqual(d.Next, f.s) || d.Effects != nil {
						t.Fatalf("出错时应返回原状态且无副作用")
					}
					return
				}
				if err != nil {
					t.Fatalf("意外错误: %v", err)
				}
				if got := kinds(d.Effects); d.Next.TaskStatus != want.status || attStatus(d.Next) != want.att || got != want.fx {
					t.Fatalf("得到 (%s, %s, [%s])，期望 (%s, %s, [%s])",
						d.Next.TaskStatus, attStatus(d.Next), got, want.status, want.att, want.fx)
				}
				d2, _ := Decide(f.s, e)
				if !reflect.DeepEqual(d, d2) {
					t.Fatalf("相同输入的决策不同")
				}
				checkEffects(t, f.s, d)
			})
		}
	}
}

// checkEffects 检查每个决策都遵守共享规则：ApplyControl 的目标状态等于 ControlTransition 的结果；
// 判决满足 VerdictAllowed，not_before 只用于回到 queued，终态事件为 task_terminal。
func checkEffects(t *testing.T, prev State, d Decision) {
	t.Helper()
	status := prev.TaskStatus
	for _, e := range d.Effects {
		switch e := e.(type) {
		case ApplyControl:
			if next, ok := ControlTransition(status, d.Next.Desired); !ok || next != e.Status || e.ControlVersion != d.Next.ControlVersion {
				t.Fatalf("ApplyControl %+v 不是 ControlTransition(%s, %s)", e, status, d.Next.Desired)
			}
			status = e.Status
		case Finalize:
			v := e.Verdict
			if !VerdictAllowed(d.Next.Desired, v.TaskStatus) || v.ControlVersion != d.Next.ControlVersion {
				t.Fatalf("判决 %s 不符合 desired = %s（控制版本 %d/%d）", v.TaskStatus, d.Next.Desired, v.ControlVersion, d.Next.ControlVersion)
			}
			if (v.NotBefore != nil) != (v.TaskStatus == "queued") {
				t.Fatalf("not_before 只用于 queued：%+v", v)
			}
			if IsTerminal(v.TaskStatus) != (v.EventType == "task_terminal") {
				t.Fatalf("事件类型 %s 与任务状态 %s 不符", v.EventType, v.TaskStatus)
			}
		}
	}
}

func deepCopy(s State) State {
	if s.Attempt != nil {
		a := *s.Attempt
		s.Attempt = &a
	}
	return s
}

// ---- §8.1 边与前置条件 ----

func TestHappyPathEffectOrder(t *testing.T) {
	s, fx := run(t, base(), Tick{}, SlotGranted{}, AttemptCreated{AttemptID: "a1", EnvID: "e1", Status: "starting"},
		EnvCreated{AttemptID: "a1", EnvID: "e1"}, WorkerStarted{AttemptID: "a1", EnvID: "e1"},
		AttemptFinished{AttemptID: "a1", EnvID: "e1", Outcome: success()})
	v := finalizeOf(t, fx)
	s, fx2 := run(t, s, VerdictCommitted{Verdict: v}, EnvStopped{AttemptID: "a1", EnvID: "e1"})
	fx = append(fx, fx2...)
	want := "RequestSlot,CreateAttempt,CreateEnvironment,StartWorker,RevokeAccess,StopEnvironment,Finalize,ReleaseSlot"
	if got := kinds(fx); got != want {
		t.Fatalf("副作用顺序 %s，期望 %s", got, want)
	}
	if s.TaskStatus != "succeeded" || v.TaskStatus != "succeeded" || string(v.Result) != `{"ok":true}` ||
		v.EventType != "task_terminal" || v.FromStatus != "starting" || v.AttemptStatus != "ended" || s.SlotHeld {
		t.Fatalf("终态 %+v，判决 %+v", s, v)
	}
}

func TestStartPreconditions(t *testing.T) {
	future, past := t0.Add(time.Second), t0.Add(-time.Second)
	cases := []struct {
		name string
		mut  func(*State)
		fx   string
	}{
		{"queued、run、无旧执行", func(*State) {}, "RequestSlot"},
		{"desired = pause 待应用", func(s *State) { s.Desired, s.ControlVersion = "pause", 2 }, "ApplyControl"},
		{"not_before 未到", func(s *State) { s.NotBefore = &future }, "WakeAt"},
		{"not_before 已到", func(s *State) { s.NotBefore = &past }, "RequestSlot"},
		{"旧环境未确认停止", func(s *State) { s.Attempt, s.SlotHeld = endedAtt("a1", false), true }, ""},
		{"旧环境已停止", func(s *State) { s.Attempt = endedAtt("a1", true) }, "RequestSlot"},
		{"paused", func(s *State) { s.TaskStatus, s.Desired = "paused", "pause" }, ""},
		{"已申请槽位", func(s *State) { s.SlotRequested = true }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base()
			tc.mut(&s)
			if got := kinds(mustDecide(t, s, Tick{}).Effects); got != tc.fx {
				t.Fatalf("副作用 %s，期望 %s", got, tc.fx)
			}
		})
	}
}

func TestSlotGrantedAfterConditionChangedIsReturned(t *testing.T) {
	s := base()
	s.SlotRequested = true
	s, _ = run(t, s, ControlChanged{Desired: "pause", ControlVersion: 2}) // queued → paused
	d := mustDecide(t, s, SlotGranted{})
	if kinds(d.Effects) != "ReleaseSlot" || d.Next.SlotHeld || d.Next.SlotRequested {
		t.Fatalf("授予应归还：%s %+v", kinds(d.Effects), d.Next)
	}
}

func TestStopBlockedPreventsReplacement(t *testing.T) {
	// 故障重试已裁决为 queued，但旧环境无法确认停止：反复 Tick 与重试停止都不创建新 attempt。
	s := base()
	s.Attempt, s.SlotHeld = endedAtt("a1", false), true
	s, fx := run(t, s, StopUnconfirmed{AttemptID: "a1", EnvID: "env-a1"}, Tick{})
	s.Now = s.Attempt.StopRetryAt.Add(time.Millisecond)
	s, fx2 := run(t, s, Tick{}, StopUnconfirmed{AttemptID: "a1", EnvID: "env-a1"}, Tick{})
	fx = append(fx, fx2...)
	if has[CreateAttempt](fx) || has[RequestSlot](fx) || has[ReleaseSlot](fx) {
		t.Fatalf("未确认停止时不得替代执行或归还槽位: %s", kinds(fx))
	}
	if kinds(fx) != "WakeAt,StopEnvironment,WakeAt" || s.Attempt.StopFailures != 2 {
		t.Fatalf("停止应退避重试: %s, %+v", kinds(fx), s.Attempt)
	}
	// stop_blocked 的 attempt（恢复交来）同样阻止。
	sb := base()
	sb.TaskStatus, sb.Attempt, sb.SlotHeld = "running", att("a1", "stop_blocked"), true
	o := crash()
	sb.Attempt.Outcome = &o
	_, fx = run(t, sb, Tick{}, Tick{})
	if has[CreateAttempt](fx) || has[RequestSlot](fx) || kinds(fx) != "RevokeAccess,StopEnvironment" {
		t.Fatalf("stop_blocked: %s", kinds(fx))
	}
	// 确认停止后才裁决、归还槽位，并在重试时创建替代执行。
	d := mustDecide(t, sb, EnvStopped{AttemptID: "a1", EnvID: "env-a1"})
	v := finalizeOf(t, d.Effects)
	if v.TaskStatus != "queued" || v.FromStatus != "starting" {
		t.Fatalf("stop_blocked 确认后的判决 %+v", v)
	}
}

// ---- 控制 ----

func TestControlTransitionTable(t *testing.T) {
	statuses := []string{"queued", "running", "pausing", "cancelling", "paused", "succeeded", "failed", "cancelled"}
	want := map[string]map[string]string{ // 规格 §8.1；空串表示不合法
		"cancel": {"queued": "cancelled", "paused": "cancelled", "running": "cancelling", "pausing": "cancelling", "cancelling": "cancelling"},
		"pause":  {"queued": "paused", "running": "pausing", "pausing": "pausing", "paused": "paused"},
		"run":    {"paused": "queued", "queued": "queued", "running": "running"},
	}
	for desired, row := range want {
		for _, st := range statuses {
			next, ok := ControlTransition(st, desired)
			if next != row[st] || ok != (row[st] != "") {
				t.Errorf("ControlTransition(%s, %s) = %q, %v；期望 %q", st, desired, next, ok, row[st])
			}
			allowed := map[string]bool{"cancel": st == "cancelled", "pause": st == "paused",
				"run": st == "succeeded" || st == "failed" || st == "queued"}[desired]
			if VerdictAllowed(desired, st) != allowed {
				t.Errorf("VerdictAllowed(%s, %s) != %v", desired, st, allowed)
			}
		}
	}
}

func TestControlBeforeWorkerReadyStopsDirectly(t *testing.T) {
	for _, desired := range []string{"cancel", "pause"} {
		t.Run(desired, func(t *testing.T) {
			s := base()
			s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "starting"), true
			d := mustDecide(t, s, ControlChanged{Desired: desired, ControlVersion: 2})
			v := finalizeOf(t, d.Effects)
			if has[SendControl](d.Effects) || !has[StopEnvironment](d.Effects) || !VerdictAllowed(desired, v.TaskStatus) {
				t.Fatalf("starting: %s %+v", kinds(d.Effects), v)
			}
			// 之后到达的创建结果不再启动 Worker。
			if fx := mustDecide(t, d.Next, EnvCreated{AttemptID: "a1", EnvID: "env-a1"}).Effects; len(fx) != 0 {
				t.Fatalf("停止后的 EnvCreated 产生了 %s", kinds(fx))
			}

			s.Attempt = att("a1", "handshaking")
			d = mustDecide(t, s, ControlChanged{Desired: desired, ControlVersion: 2})
			if kinds(d.Effects) != "ApplyControl,RevokeAccess,StopEnvironment" {
				t.Fatalf("handshaking: %s", kinds(d.Effects))
			}
			// 就绪消息迟到不使 attempt 变为 active；runner 结束后按 desired 裁决，不重复停止。
			n, fx := run(t, d.Next, WorkerStarted{AttemptID: "a1", EnvID: "env-a1"},
				AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: crash()})
			if v := finalizeOf(t, fx); kinds(fx) != "Finalize" || !VerdictAllowed(desired, v.TaskStatus) || n.Attempt.Status != "finishing" {
				t.Fatalf("handshaking 结束: %s %+v", kinds(fx), v)
			}
		})
	}
}

func TestActiveControlUsesGrace(t *testing.T) {
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
	d := mustDecide(t, s, ControlChanged{Desired: "pause", ControlVersion: 2})
	if sc := d.Effects[1].(SendControl); sc.Kind != "pause" || sc.GraceMs != DefaultControlGraceMs || sc.AttemptID != "a1" {
		t.Fatalf("SendControl %+v", sc)
	}
	s.ControlGraceMs = 2500
	d = mustDecide(t, s, ControlChanged{Desired: "cancel", ControlVersion: 2})
	if sc := d.Effects[1].(SendControl); sc.Kind != "cancel" || sc.GraceMs != 2500 {
		t.Fatalf("SendControl %+v", sc)
	}
	// 重复或更旧的控制读取被忽略。
	if d := mustDecide(t, d.Next, ControlChanged{Desired: "pause", ControlVersion: 2}); len(d.Effects) != 0 || d.Next.Desired != "cancel" {
		t.Fatalf("旧控制被应用: %s", kinds(d.Effects))
	}
}

func TestResumeFromPaused(t *testing.T) {
	s := base()
	s.TaskStatus, s.Desired, s.Attempt = "paused", "pause", endedAtt("a1", true)
	d := mustDecide(t, s, ControlChanged{Desired: "run", ControlVersion: 2})
	if kinds(d.Effects) != "ApplyControl,RequestSlot" || d.Next.TaskStatus != "queued" {
		t.Fatalf("resume: %s %+v", kinds(d.Effects), d.Next)
	}
	_, fx := run(t, d.Next, SlotGranted{})
	if ca := fx[0].(CreateAttempt); ca.Retry != RetryNone {
		t.Fatalf("暂停恢复不是重试: %+v", ca)
	}
}

func TestControlDuringCreateAttempt(t *testing.T) {
	creating := base()
	creating.SlotHeld = true
	cancelled, _ := run(t, creating, ControlChanged{Desired: "cancel", ControlVersion: 2})
	t.Run("attempt 已提交：随即按 cancel 结束，不创建环境", func(t *testing.T) {
		d := mustDecide(t, cancelled, AttemptCreated{AttemptID: "a1", EnvID: "e1", Status: "starting"})
		if kinds(d.Effects) != "ApplyControl,RevokeAccess,StopEnvironment,Finalize" || d.Next.TaskStatus != "cancelling" {
			t.Fatalf("%s %+v", kinds(d.Effects), d.Next)
		}
		if v := finalizeOf(t, d.Effects); v.TaskStatus != "cancelled" || v.EventType != "task_terminal" {
			t.Fatalf("判决 %+v", v)
		}
	})
	t.Run("创建被拒：归还槽位后应用 cancel", func(t *testing.T) {
		d := mustDecide(t, cancelled, StoreFailed{Op: OpCreateAttempt, Err: errors.New("not_runnable")})
		if kinds(d.Effects) != "ReleaseSlot,ApplyControl" || d.Next.TaskStatus != "cancelled" || d.Next.SlotHeld {
			t.Fatalf("%s %+v", kinds(d.Effects), d.Next)
		}
	})
	t.Run("创建被拒且控制未变：退避后重新申请", func(t *testing.T) {
		s, fx := run(t, creating, StoreFailed{Op: OpCreateAttempt, Err: errors.New("previous_not_stopped")}, Tick{})
		if kinds(fx) != "ReleaseSlot,WakeAt,WakeAt" {
			t.Fatalf("%s", kinds(fx))
		}
		s.Now = s.NotBefore.Add(0)
		if kinds(mustDecide(t, s, Tick{}).Effects) != "RequestSlot" {
			t.Fatalf("到期后应重新申请")
		}
	})
}

// ---- 裁决 ----

func TestVerdictByDesired(t *testing.T) {
	withResult := success()
	errored := Outcome{Class: "worker_error", ProposalKind: "error"}
	cases := []struct {
		desired string
		o       Outcome
		status  string
		reason  string
		result  bool
	}{
		{"run", withResult, "succeeded", ClassSucceeded, true},
		{"run", Outcome{Class: ClassOOMObserved, ProposalKind: "result", Result: json.RawMessage(`1`)}, "succeeded", ClassOOMObserved, true},
		{"run", errored, "failed", "worker_error", false},
		{"cancel", withResult, "cancelled", "completed_during_cancel", true},
		{"cancel", crash(), "cancelled", "cancelled", false},
		{"pause", withResult, "paused", "completed_during_pause", true},
		{"pause", Outcome{Class: ClassPaused, ProposalKind: "paused"}, "paused", "paused", false},
		{"pause", crash(), "paused", "paused", false}, // 宿主暂停已生效：不判 crashed、不重试
	}
	for _, tc := range cases {
		t.Run(tc.desired+"/"+tc.o.Class, func(t *testing.T) {
			s := base()
			s.TaskStatus = map[string]string{"run": "running", "cancel": "cancelling", "pause": "pausing"}[tc.desired]
			s.Desired, s.Attempt, s.SlotHeld = tc.desired, att("a1", "active"), true
			v := finalizeOf(t, mustDecide(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: tc.o}).Effects)
			if v.TaskStatus != tc.status || v.TaskStatusReason != tc.reason || (len(v.Result) > 0) != tc.result || v.NotBefore != nil {
				t.Fatalf("判决 %+v", v)
			}
		})
	}
}

func TestCancelAcceptedBeforeResultNeverSucceeds(t *testing.T) {
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
	// 顺序 1：cancel 先被接受，result 随后到达。
	s1, fx := run(t, s, ControlChanged{Desired: "cancel", ControlVersion: 2},
		AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()})
	v := finalizeOf(t, fx)
	if v.TaskStatus != "cancelled" || v.TaskStatusReason != "completed_during_cancel" || v.ControlVersion != 2 {
		t.Fatalf("判决 %+v", v)
	}
	if n, _ := run(t, s1, VerdictCommitted{Verdict: v}); n.TaskStatus != "cancelled" {
		t.Fatalf("终态 %s", n.TaskStatus)
	}
	// 顺序 2：成功判决已发出但尚未提交时 cancel 被接受：按新控制重算，旧判决的拒绝被忽略。
	s2, fx := run(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()})
	old := finalizeOf(t, fx)
	s2, fx = run(t, s2, ControlChanged{Desired: "cancel", ControlVersion: 2},
		StoreFailed{Op: OpFinalize, AttemptID: "a1", ControlVersion: old.ControlVersion, Err: errors.New("control_changed")})
	v = finalizeOf(t, fx)
	if old.TaskStatus != "succeeded" || v.TaskStatus != "cancelled" || kinds(fx) != "Finalize" || s2.Attempt.Verdict.ControlVersion != 2 {
		t.Fatalf("重算 %s %+v", kinds(fx), v)
	}
	if n, _ := run(t, s2, VerdictCommitted{Verdict: v}); n.TaskStatus != "cancelled" || n.AppliedControlVersion != 2 {
		t.Fatalf("终态 %+v", n)
	}
	// StoreFailed 先于 ControlChanged 到达：清除判决，控制到达或 Tick 时按最新控制重算。
	s3, _ := run(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()},
		StoreFailed{Op: OpFinalize, AttemptID: "a1", ControlVersion: 1})
	if s3.Attempt.Verdict != nil {
		t.Fatalf("被拒绝的判决未清除")
	}
	n, fx := run(t, s3, ControlChanged{Desired: "cancel", ControlVersion: 2}, Tick{})
	if kinds(fx) != "Finalize" || n.Attempt.Verdict.TaskStatus != "cancelled" {
		t.Fatalf("重算 %s %+v", kinds(fx), n.Attempt.Verdict)
	}
	s3.Desired, s3.ControlVersion = "cancel", 2 // Tick 路径：控制已由重新加载得到
	if n, fx = run(t, s3, Tick{}); kinds(fx) != "Finalize" || n.Attempt.Verdict.TaskStatus != "cancelled" {
		t.Fatalf("Tick 重算 %s", kinds(fx))
	}
}

func TestLateCancelAfterVerdictCommitted(t *testing.T) {
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
	s, fx := run(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()})
	v := finalizeOf(t, fx)
	s, _ = run(t, s, VerdictCommitted{Verdict: v})
	d := mustDecide(t, s, ControlChanged{Desired: "cancel", ControlVersion: 2})
	if d.Next.TaskStatus != "succeeded" || len(d.Effects) != 0 {
		t.Fatalf("终态被改变: %s %s", d.Next.TaskStatus, kinds(d.Effects))
	}
	// 判决先于 cancel 提交的竞争：较新的控制对已提交的 succeeded 判决无效。
	s2 := base()
	s2.TaskStatus, s2.Attempt, s2.SlotHeld = "running", att("a1", "active"), true
	s2, _ = run(t, s2, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()},
		ControlChanged{Desired: "cancel", ControlVersion: 2})
	if n, fx := run(t, s2, VerdictCommitted{Verdict: v}); n.TaskStatus != "succeeded" || len(fx) != 0 {
		t.Fatalf("已提交的旧判决应为终态: %s %s", n.TaskStatus, kinds(fx))
	}
}

func TestRetryLimits(t *testing.T) {
	cases := []struct {
		name             string
		o                Outcome
		faultUsed, oomUs int64
		status           string
		backoff          time.Duration
	}{
		{"故障首次重试", crash(), 0, 0, "queued", time.Second},
		{"故障第三次重试", crash(), 2, 0, "queued", 4 * time.Second},
		{"故障重试到上限", crash(), 3, 0, "failed", 0},
		{"OOM 首次重试", oom(), 0, 0, "queued", time.Second},
		{"OOM 第二次失败", oom(), 0, 1, "failed", 0},
		{"OOM 受 max_fault_retries 约束", oom(), 3, 0, "failed", 0},
		{"累计运行时限超限不重试", Outcome{Class: ClassDeadlineExceeded, Retry: RetryFault}, 0, 0, "failed", 0},
		{"不可重试", Outcome{Class: "protocol_violation"}, 0, 0, "failed", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base()
			s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
			s.FaultRetriesUsed, s.OOMRetriesUsed = tc.faultUsed, tc.oomUs
			d := mustDecide(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: tc.o})
			v := finalizeOf(t, d.Effects)
			if v.TaskStatus != tc.status || v.TaskStatusReason != tc.o.Class {
				t.Fatalf("判决 %+v", v)
			}
			if tc.status == "queued" && (v.NotBefore == nil || !v.NotBefore.Equal(t0.Add(tc.backoff)) || v.EventType != "attempt_ended") {
				t.Fatalf("not_before %v，期望 now + %v", v.NotBefore, tc.backoff)
			}
			if tc.status == "failed" && v.EventType != "task_terminal" {
				t.Fatalf("事件类型 %s", v.EventType)
			}
		})
	}
}

func TestRetryCountingAcrossAttempts(t *testing.T) {
	// OOM → queued(not_before) → 环境停止（才归还槽位并安排唤醒）→ 到期 → CreateAttempt{oom}，
	// 计数在 attempt 创建时递增。
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
	s, fx := run(t, s, AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: oom()})
	s, fx = run(t, s, VerdictCommitted{Verdict: finalizeOf(t, fx)}, EnvStopped{AttemptID: "a1", EnvID: "env-a1"})
	if kinds(fx) != "ReleaseSlot,WakeAt" || s.NextRetry != RetryOOM || s.TaskStatus != "queued" {
		t.Fatalf("%s %+v", kinds(fx), s)
	}
	s.Now = *s.NotBefore
	s, fx = run(t, s, Tick{}, SlotGranted{}, AttemptCreated{AttemptID: "a2", EnvID: "e2", Status: "starting"})
	if ca := fx[1].(CreateAttempt); ca.Retry != RetryOOM || s.OOMRetriesUsed != 1 || s.FaultRetriesUsed != 0 || s.NextRetry != RetryNone || s.NotBefore != nil {
		t.Fatalf("%+v %+v", fx, s)
	}
	// 第二次 OOM：失败。
	s, fx = run(t, s, EnvCreated{AttemptID: "a2", EnvID: "e2"}, WorkerStarted{AttemptID: "a2", EnvID: "e2"},
		AttemptFinished{AttemptID: "a2", EnvID: "e2", Outcome: oom()})
	if v := finalizeOf(t, fx); v.TaskStatus != "failed" {
		t.Fatalf("第二次 OOM 应失败: %+v", v)
	}
}

func TestFailureBeforeActive(t *testing.T) {
	transient := &Outcome{Class: "create_failed_transient", Retry: RetryFault}
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "starting"), true
	d := mustDecide(t, s, EnvCreated{AttemptID: "a1", EnvID: "env-a1", Err: errors.New("boom"), Outcome: transient})
	if kinds(d.Effects) != "RevokeAccess,StopEnvironment,Finalize" || finalizeOf(t, d.Effects).TaskStatus != "queued" {
		t.Fatalf("创建失败: %s", kinds(d.Effects))
	}
	if _, err := Decide(s, EnvCreated{AttemptID: "a1", EnvID: "env-a1", Err: errors.New("boom")}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("缺少分类结果应出错: %v", err)
	}
	s.Attempt = att("a1", "handshaking")
	d = mustDecide(t, s, WorkerStarted{AttemptID: "a1", EnvID: "env-a1", Err: errors.New("exec"),
		Outcome: &Outcome{Class: "ready_timeout", Retry: RetryFault}})
	if kinds(d.Effects) != "RevokeAccess,StopEnvironment,Finalize" || d.Next.Attempt.Status != "finishing" {
		t.Fatalf("启动失败: %s", kinds(d.Effects))
	}
}

// ---- 过期结果 ----

func TestStaleResultsOnlyHandOverResources(t *testing.T) {
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a2", "active"), true
	cases := []struct {
		e  Event
		fx string
	}{
		{AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: success()}, "StopEnvironment"},
		{EnvCreated{AttemptID: "a1", EnvID: "env-a1"}, "StopEnvironment"},
		{WorkerStarted{AttemptID: "a1", EnvID: "env-a1"}, "StopEnvironment"},
		{VerdictCommitted{Verdict: Verdict{AttemptID: "a1", TaskStatus: "succeeded"}}, ""},
		{EnvStopped{AttemptID: "a1", EnvID: "env-a1"}, ""},
		{StopUnconfirmed{AttemptID: "a1", EnvID: "env-a1"}, ""},
		{StoreFailed{Op: OpFinalize, AttemptID: "a1", ControlVersion: 1}, ""},
	}
	for _, tc := range cases {
		d := mustDecide(t, s, tc.e)
		if !reflect.DeepEqual(d.Next, s) || kinds(d.Effects) != tc.fx {
			t.Errorf("%T: 状态变化或副作用 %s", tc.e, kinds(d.Effects))
		}
		if se, ok := firstStop(d.Effects); ok && se.AttemptID != "a1" {
			t.Errorf("%T: 停止了错误的环境 %+v", tc.e, se)
		}
	}
}

func firstStop(effects []Effect) (StopEnvironment, bool) {
	for _, e := range effects {
		if se, ok := e.(StopEnvironment); ok {
			return se, true
		}
	}
	return StopEnvironment{}, false
}

// ---- 退避与校验 ----

func TestRetryBackoff(t *testing.T) {
	cases := []struct {
		n      int64
		jitter float64
		want   time.Duration
	}{
		{0, 0, time.Second}, {0, 0.5, 1500 * time.Millisecond}, {1, 0, 2 * time.Second},
		{4, 0, 16 * time.Second}, {5, 0, 30 * time.Second}, {40, 0, 30 * time.Second},
		{-1, 0, 30 * time.Second}, {0, -3, time.Second},
	}
	for _, tc := range cases {
		if got := RetryBackoff(tc.n, tc.jitter); got != tc.want {
			t.Errorf("RetryBackoff(%d, %v) = %v，期望 %v", tc.n, tc.jitter, got, tc.want)
		}
	}
	for _, j := range []float64{0, 0.3, 0.999999, 1, 7} {
		if d := RetryBackoff(10, j); d < 30*time.Second || d >= RetryBackoffCap {
			t.Errorf("RetryBackoff(10, %v) = %v 超出 [30s, 60s)", j, d)
		}
	}
}

func TestInvalidStateRejected(t *testing.T) {
	cases := map[string]func(*State){
		"未知任务状态":             func(s *State) { s.TaskStatus = "lost" },
		"未知 desired":         func(s *State) { s.Desired = "stop" },
		"未知 attempt 状态":      func(s *State) { s.TaskStatus, s.Attempt = "running", att("a1", "created") },
		"running 没有 attempt": func(s *State) { s.TaskStatus = "running" },
		"queued 有活动 attempt": func(s *State) { s.Attempt = att("a1", "active") },
		"finishing 没有结果":     func(s *State) { s.TaskStatus, s.Attempt = "running", att("a1", "finishing") },
	}
	for name, mut := range cases {
		s := base()
		mut(&s)
		if _, err := Decide(s, Tick{}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: 期望 ErrInvalid，得到 %v", name, err)
		}
	}
	s := base()
	s.TaskStatus, s.Attempt, s.SlotHeld = "running", att("a1", "active"), true
	bad := AttemptFinished{AttemptID: "a1", EnvID: "env-a1", Outcome: Outcome{Class: "x", Retry: "always"}}
	if _, err := Decide(s, bad); !errors.Is(err, ErrInvalid) {
		t.Errorf("未定义的重试类别应出错: %v", err)
	}
	if _, err := Decide(s, StoreFailed{Op: "persist_run_time"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("未定义的 StoreOp 应出错: %v", err)
	}
}
