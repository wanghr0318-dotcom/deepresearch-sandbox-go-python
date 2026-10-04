package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
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

// ==== task actor 与 Scheduler（Plan 5 Task 7）====
//
// 全部依赖为内存替身，时间由 fkClock 推进。等待以"条件成立"为准（有真实时间上限），断言只针对
// 不依赖调度先后的事实：调用日志的相对顺序、Store 中的结果、Clock 读数。

// ---- 替身 ----

// fkClock 是可手动推进的 Clock。At 以绝对时间登记：推进发生在登记之前时，登记即触发。
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

// fkLog 按发生顺序记录各替身的调用。
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

// memStore 是 Store 的内存替身，按真实用例的前置条件做最小校验。fail 按方法名给出依次返回的错误。
type memStore struct {
	mu            sync.Mutex
	log           *fkLog
	clk           *fkClock
	tasks         map[string]*TaskState
	attempts      map[string]*Attempt
	created       []NewAttempt // 每次 CreateAttempt 调用的输入
	finalizeCalls []Verdict
	callTimes     map[string][]time.Time
	runTimes      []int64
	fail          map[string][]error
}

func newMemStore(log *fkLog, clk *fkClock, tasks ...TaskState) *memStore {
	m := &memStore{log: log, clk: clk, tasks: map[string]*TaskState{}, attempts: map[string]*Attempt{},
		callTimes: map[string][]time.Time{}, fail: map[string][]error{}}
	for _, ts := range tasks {
		ts := ts
		m.tasks[ts.TaskID] = &ts
	}
	return m
}

func (m *memStore) enter(method string) error {
	m.callTimes[method] = append(m.callTimes[method], m.clk.Now())
	if q := m.fail[method]; len(q) > 0 {
		m.fail[method] = q[1:]
		return q[0]
	}
	return nil
}

func (m *memStore) CreateAttempt(_ context.Context, a NewAttempt) (Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created = append(m.created, a)
	m.log.add("create_attempt:%s:%s", a.AttemptID, a.Retry)
	if err := m.enter("CreateAttempt"); err != nil {
		return Attempt{}, err
	}
	if at, ok := m.attempts[a.AttemptID]; ok { // 同一身份重跑
		return *at, nil
	}
	ts := m.tasks[a.TaskID]
	if ts.Status != "queued" || ts.Desired != "run" {
		return Attempt{}, &persistence.RejectedError{Code: persistence.CodeNotRunnable}
	}
	at := &Attempt{AttemptID: a.AttemptID, TaskID: a.TaskID, AttemptNo: a.AttemptNo, EnvID: a.EnvID, Status: "starting"}
	m.attempts[a.AttemptID] = at
	ts.Status, ts.StatusReason, ts.CurrentAttemptID, ts.NotBefore = "running", "", a.AttemptID, nil
	ts.AttemptsTotal++
	switch a.Retry {
	case RetryFault:
		ts.FaultRetriesUsed++
	case RetryOOM:
		ts.OOMRetriesUsed++
	}
	return *at, nil
}

func (m *memStore) GetAttempt(_ context.Context, id string) (Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at, ok := m.attempts[id]; ok {
		return *at, nil
	}
	return Attempt{}, persistence.ErrNotFound
}

func (m *memStore) ApplyControl(_ context.Context, c ApplyControl) (ControlState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log.add("apply_control:%s", c.Status)
	if err := m.enter("ApplyControl"); err != nil {
		return ControlState{}, err
	}
	ts := m.tasks[c.TaskID]
	switch {
	case c.ControlVersion < ts.ControlVersion:
		return ControlState{}, &persistence.RejectedError{Code: persistence.CodeControlChanged}
	case c.ControlVersion > ts.AppliedControlVersion:
		ts.AppliedControlVersion, ts.Status, ts.StatusReason = c.ControlVersion, c.Status, c.StatusReason
	}
	return m.control(ts), nil
}

func (m *memStore) control(ts *TaskState) ControlState {
	return ControlState{TaskID: ts.TaskID, Desired: ts.Desired, ControlVersion: ts.ControlVersion,
		AppliedControlVersion: ts.AppliedControlVersion, Status: ts.Status}
}

func (m *memStore) GetControlState(_ context.Context, taskID string) (ControlState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.control(m.tasks[taskID]), nil
}

func (m *memStore) FinalizeAttempt(_ context.Context, v Verdict) (Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalizeCalls = append(m.finalizeCalls, v)
	m.log.add("finalize:%s:%s", v.AttemptID, v.TaskStatus)
	if err := m.enter("FinalizeAttempt"); err != nil {
		return Attempt{}, err
	}
	ts, at := m.tasks[v.TaskID], m.attempts[v.AttemptID]
	if at.Status == "ended" {
		return *at, nil
	}
	if v.ControlVersion != ts.ControlVersion {
		return Attempt{}, &persistence.RejectedError{Code: persistence.CodeControlChanged}
	}
	at.Status, at.OutcomeClass = "ended", v.OutcomeClass
	ts.Status, ts.StatusReason, ts.NotBefore = v.TaskStatus, v.TaskStatusReason, v.NotBefore
	ts.AppliedControlVersion = max(ts.AppliedControlVersion, v.ControlVersion)
	return *at, nil
}

func (m *memStore) LoadTask(_ context.Context, taskID string) (TaskState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.enter("LoadTask"); err != nil {
		return TaskState{}, err
	}
	ts, ok := m.tasks[taskID]
	if !ok {
		return TaskState{}, persistence.ErrNotFound
	}
	return *ts, nil
}

func (m *memStore) ListActiveTasks(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id, ts := range m.tasks {
		if !IsTerminal(ts.Status) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (m *memStore) PersistRunTime(_ context.Context, taskID, attemptID string, totalMs int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log.add("runtime:%d", totalMs)
	if err := m.enter("PersistRunTime"); err != nil {
		return 0, err
	}
	ts := m.tasks[taskID]
	m.runTimes = append(m.runTimes, totalMs)
	ts.RunTimeMs = max(ts.RunTimeMs, totalMs)
	return ts.RunTimeMs, nil
}

func (m *memStore) RevokeAttemptAccess(_ context.Context, attemptID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log.add("revoke_db:%s", attemptID)
	return m.enter("RevokeAttemptAccess")
}

func (m *memStore) task(id string) TaskState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *m.tasks[id]
}

func (m *memStore) setControl(id, desired string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tasks[id]
	ts.Desired, ts.ControlVersion = desired, ts.ControlVersion+1
}

func (m *memStore) setFail(method string, errs ...error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail[method] = errs
}

func (m *memStore) snapshot() (created []NewAttempt, finalize []Verdict, runTimes []int64, times map[string][]time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	times = map[string][]time.Time{}
	for k, v := range m.callTimes {
		times[k] = append([]time.Time(nil), v...)
	}
	return append([]NewAttempt(nil), m.created...), append([]Verdict(nil), m.finalizeCalls...),
		append([]int64(nil), m.runTimes...), times
}

// fkAdmission 立即授予，记录申请与归还。
type fkAdmission struct {
	mu     sync.Mutex
	log    *fkLog
	clk    *fkClock
	nextID uint64
	held   map[uint64]bool
}

func (f *fkAdmission) Acquire(ctx context.Context, r SlotRequest) (SlotGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.held[f.nextID] = true
	f.log.add("acquire:%d@%s", f.nextID, f.clk.Now().Sub(t0))
	return SlotGrant{ID: f.nextID, TaskID: r.TaskID}, ctx.Err()
}

func (f *fkAdmission) Release(g SlotGrant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, g.ID)
	f.log.add("release:%d", g.ID)
}

// fkEnv 记录创建与停止；stops 依次给出停止报告，用完后为已停止且已记录。
type fkEnv struct {
	mu    sync.Mutex
	log   *fkLog
	clk   *fkClock
	acc   *FakeAccess
	stops []StopReport
}

func (f *fkEnv) CreateEnv(_ context.Context, s EnvSpec) (*Outcome, error) {
	f.log.add("create_env:%s", s.EnvID)
	return nil, nil
}

func (f *fkEnv) StopEnv(_ context.Context, envID string) (StopReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.add("stop:%s revoked=%v", envID, len(f.acc.Revoked()) > 0)
	if len(f.stops) > 0 {
		r := f.stops[0]
		f.stops = f.stops[1:]
		return r, nil
	}
	return StopReport{Stopped: true, Recorded: true, At: f.clk.Now()}, nil
}

// fkRunner 把每次 Run 交给测试（fkRun），由测试决定何时 ready、何时结束。
type fkRunner struct {
	log  *fkLog
	runs chan *fkRun
	// onDeadline 非 nil 时，ctx 以 DeadlineExceeded 取消后返回它（模拟同时发生的可重试故障）。
	onDeadline *Outcome
}

type fkRun struct {
	spec     RunSpec
	controls <-chan Control
	result   chan Outcome
	cause    chan error // Run 的 ctx 被取消时的 cause
}

func (r *fkRunner) Run(ctx context.Context, spec RunSpec, controls <-chan Control) Outcome {
	r.log.add("run:%s sock=%s", spec.AttemptID, spec.GatewaySocket)
	h := &fkRun{spec: spec, controls: controls, result: make(chan Outcome, 1), cause: make(chan error, 1)}
	select {
	case r.runs <- h:
	case <-ctx.Done():
	}
	select {
	case o := <-h.result:
		return o
	case <-ctx.Done():
		cause := context.Cause(ctx)
		h.cause <- cause
		if errors.Is(cause, context.DeadlineExceeded) { // 与 runner.Classify 相同：累计运行时限
			if r.onDeadline != nil {
				return *r.onDeadline
			}
			return Outcome{Class: ClassDeadlineExceeded}
		}
		return Outcome{Class: "lost_on_restart", Retry: RetryFault}
	}
}

// cancelled 报告 Run 的 ctx 是否已被取消（不等待）。
func (h *fkRun) cancelled() bool { return len(h.cause) > 0 }

func (h *fkRun) ready()             { h.spec.OnReady() }
func (h *fkRun) finish(o Outcome)   { h.result <- o }
func (h *fkRun) noControl() bool    { return len(h.controls) == 0 }
func (h *fkRun) control() Control   { return <-h.controls }
func (h *fkRun) attemptID() string  { return h.spec.AttemptID }
func (h *fkRun) attemptNo() int64   { return h.spec.AttemptNo }
func (h *fkRun) taskStatus() string { return h.spec.Task.Status }

// ---- 测试装置 ----

type actorHarness struct {
	t      *testing.T
	clk    *fkClock
	log    *fkLog
	st     *memStore
	adm    *fkAdmission
	env    *fkEnv
	rn     *fkRunner
	acc    *FakeAccess
	ctx    context.Context
	cancel context.CancelFunc
	actor  *Actor
	fatal  chan error

	limit       time.Duration // Deps.RunTimeLimit 的返回值
	accountMu   sync.Mutex
	accounts    []string // OnStopRecorded 的调用：attempt@stoppedAt
	accountFail []error
}

func (h *actorHarness) accountCalls() []string {
	h.accountMu.Lock()
	defer h.accountMu.Unlock()
	return append([]string(nil), h.accounts...)
}

func queuedTask(id string) TaskState {
	return TaskState{TaskID: id, Status: "queued", Desired: "run", ControlVersion: 1, AppliedControlVersion: 1,
		MaxFaultRetries: 3, Limits: json.RawMessage(`{}`), Spec: json.RawMessage(`{}`)}
}

func newActorHarness(t *testing.T, tasks ...TaskState) *actorHarness {
	clk, log := &fkClock{now: t0}, &fkLog{}
	acc := &FakeAccess{}
	h := &actorHarness{t: t, clk: clk, log: log, st: newMemStore(log, clk, tasks...),
		adm: &fkAdmission{log: log, clk: clk, held: map[uint64]bool{}},
		env: &fkEnv{log: log, clk: clk, acc: acc}, rn: &fkRunner{log: log, runs: make(chan *fkRun, 8)},
		acc: acc, fatal: make(chan error, 8)}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() {
		h.cancel()
		if h.actor != nil {
			select {
			case <-h.actor.Done():
			case <-time.After(5 * time.Second):
				t.Errorf("ctx 结束后 actor 未退出")
			}
		}
	})
	return h
}

func (h *actorHarness) deps() Deps {
	var mu sync.Mutex
	n := 0
	return Deps{Store: h.st, Admission: h.adm, Env: h.env, Runner: h.rn, Access: h.acc, Clock: h.clk,
		IDs: func() string {
			mu.Lock()
			defer mu.Unlock()
			n++
			return fmt.Sprintf("id-%d", n)
		},
		Jitter:       func() float64 { return 0 },
		OnFatal:      func(_ string, err error) { h.fatal <- err },
		RunTimeLimit: func(TaskState) time.Duration { return h.limit },
		OnStopRecorded: func(_ context.Context, attemptID string, at time.Time) error {
			h.accountMu.Lock()
			defer h.accountMu.Unlock()
			h.accounts = append(h.accounts, fmt.Sprintf("%s@%s", attemptID, at.Sub(t0)))
			h.log.add("account:%s", attemptID)
			if len(h.accountFail) > 0 {
				err := h.accountFail[0]
				h.accountFail = h.accountFail[1:]
				return err
			}
			return nil
		},
	}
}

func (h *actorHarness) spawn(taskID string, opts ...SpawnOption) *Actor {
	h.actor = Spawn(h.ctx, taskID, h.deps(), opts...)
	return h.actor
}

func (h *actorHarness) nextRun() *fkRun {
	h.t.Helper()
	select {
	case r := <-h.rn.runs:
		return r
	case err := <-h.fatal:
		h.t.Fatalf("actor 致命错误: %v", err)
	case <-time.After(5 * time.Second):
		h.t.Fatalf("没有新的 Run；日志: %v", h.log.all())
	}
	return nil
}

// waitFor 等待条件成立（真实时间上限 5 s）；期间 actor 的致命错误使测试失败。
func (h *actorHarness) waitFor(desc string, cond func() bool) {
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
func (h *actorHarness) advanceUntil(desc string, step time.Duration, cond func() bool) {
	h.t.Helper()
	for i := 0; !cond(); i++ {
		if i > 2000 {
			h.t.Fatalf("推进时钟后仍未 %s；日志: %v", desc, h.log.all())
		}
		h.clk.Advance(step)
		time.Sleep(2 * time.Millisecond)
	}
}

func (h *actorHarness) waitDone() {
	h.t.Helper()
	select {
	case <-h.actor.Done():
	case <-time.After(5 * time.Second):
		h.t.Fatalf("actor 未退出；日志: %v", h.log.all())
	}
	if err := h.actor.Err(); err != nil {
		h.t.Fatalf("actor 致命错误: %v", err)
	}
}

func (h *actorHarness) logged(prefix string, n int) func() bool {
	return func() bool { return h.log.count(prefix) >= n }
}

// before 断言日志中 a 的第 1 次出现早于 b 的第 1 次出现（两者都必须出现）。
func (h *actorHarness) before(a, b string) {
	h.t.Helper()
	ia, ib := h.log.index(a, 1), h.log.index(b, 1)
	if ia < 0 || ib < 0 || ia >= ib {
		h.t.Errorf("期望 %q 早于 %q；日志: %v", a, b, h.log.all())
	}
}

// ---- 正常路径 ----

func TestActorHappyPathEffectOrder(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	if r.attemptID() != "id-1" || r.attemptNo() != 1 || r.spec.EnvID != "id-2" || r.spec.GatewaySocket != FakeGatewaySocket {
		t.Fatalf("RunSpec 不符: %+v", r.spec)
	}
	if r.taskStatus() != "running" {
		t.Errorf("Run 前应重新读取任务（running），得到 %s", r.taskStatus())
	}
	r.ready()
	r.finish(success())
	h.waitDone() // 终态且资源已归还：actor 自行退出

	// 申请槽位 → 创建 attempt → 创建环境 → 启动 → 撤销访问 → 停止 → 归还槽位；裁决在 Worker 结束之后。
	for _, pair := range [][2]string{
		{"acquire:1", "create_attempt:id-1"}, {"create_attempt:id-1", "create_env:id-2"},
		{"create_env:id-2", "run:id-1"}, {"run:id-1", "revoke_db:id-1"}, {"revoke_db:id-1", "stop:id-2 revoked=true"},
		{"stop:id-2", "release:1"}, {"run:id-1", "finalize:id-1:succeeded"},
	} {
		h.before(pair[0], pair[1])
	}
	if ts := h.st.task("t1"); ts.Status != "succeeded" || ts.AttemptsTotal != 1 {
		t.Errorf("任务应 succeeded 且 attempts_total = 1: %+v", ts)
	}
	if got := h.acc.Bound(); !reflect.DeepEqual(got, []string{"id-1"}) {
		t.Errorf("Gateway 绑定 = %v", got)
	}
	if got := h.acc.Revoked(); !reflect.DeepEqual(got, []string{"id-1"}) {
		t.Errorf("Gateway 撤销 = %v", got)
	}
	if created, _, _, _ := h.st.snapshot(); len(created) != 1 || created[0].Retry != RetryNone {
		t.Errorf("首个 attempt 的重试类别应为 none: %+v", created)
	}
	if got := h.accountCalls(); len(got) != 0 {
		t.Errorf("非恢复的 attempt 不应调用 OnStopRecorded: %v", got)
	}
}

// ---- 重试 ----

func TestActorFaultRetryWaitsBackoff(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	r.ready()
	r.finish(crash())
	h.waitFor("裁决为 queued 且归还槽位", func() bool { return h.st.task("t1").Status == "queued" && h.log.count("release:1") == 1 })
	nb := h.st.task("t1").NotBefore
	if nb == nil || !nb.Equal(t0.Add(RetryBackoff(0, 0))) {
		t.Fatalf("not_before = %v，期望 t0 + %s", nb, RetryBackoff(0, 0))
	}
	h.advanceUntil("开始第二个 attempt", 250*time.Millisecond, h.logged("acquire:", 2))
	r2 := h.nextRun()
	created, _, _, times := h.st.snapshot()
	if len(created) != 2 || created[1].Retry != RetryFault || created[1].AttemptNo != 2 || r2.attemptNo() != 2 {
		t.Fatalf("第二个 attempt 应为故障重试、attempt_no = 2: %+v", created)
	}
	if at := times["CreateAttempt"][1]; at.Before(*nb) {
		t.Errorf("第二个 attempt 在 not_before 之前创建: %v < %v", at, *nb)
	}
	if ts := h.st.task("t1"); ts.FaultRetriesUsed != 1 {
		t.Errorf("fault_retries_used = %d", ts.FaultRetriesUsed)
	}
	r2.ready()
	r2.finish(success())
	h.waitDone()
}

func TestActorOOMRetryOnceThenFails(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	r.finish(oom()) // ready 之前被 OOM 终止同样是 attempt 结果
	h.advanceUntil("开始 OOM 重试", 250*time.Millisecond, h.logged("acquire:", 2))
	r2 := h.nextRun()
	r2.ready()
	r2.finish(oom())
	h.waitDone()
	created, _, _, _ := h.st.snapshot()
	if len(created) != 2 || created[1].Retry != RetryOOM {
		t.Fatalf("第二个 attempt 应为 OOM 重试: %+v", created)
	}
	if ts := h.st.task("t1"); ts.Status != "failed" || ts.StatusReason != "worker_oom_likely" || ts.OOMRetriesUsed != 1 || ts.FaultRetriesUsed != 0 {
		t.Errorf("第二次 OOM 后应 failed: %+v", ts)
	}
}

func TestRetryAfterDerivation(t *testing.T) {
	cases := []struct {
		status, reason string
		want           RetryKind
	}{
		{"queued", "worker_oom_likely", RetryOOM},
		{"queued", "worker_error", RetryFault}, // 可重试与否已由裁决体现在 queued 上
		{"queued", "crashed_signal", RetryFault},
		{"queued", "lost_on_restart", RetryFault},
		{"queued", "", RetryNone},              // 新建
		{"queued", "run_requested", RetryNone}, // 暂停恢复
		{"paused", "worker_error", RetryNone},
	}
	for _, c := range cases {
		if got := RetryAfter(c.status, c.reason); got != c.want {
			t.Errorf("RetryAfter(%s, %s) = %q，期望 %q", c.status, c.reason, got, c.want)
		}
	}
}

// 重启后由新 actor 从 LoadTask 的 status_reason 推出重试类别。
func TestActorRederivesRetryAfterRestart(t *testing.T) {
	for _, c := range []struct {
		reason string
		want   RetryKind
	}{{"worker_error", RetryFault}, {"worker_oom_likely", RetryOOM}, {"lost_on_restart", RetryFault}, {"run_requested", RetryNone}} {
		t.Run(c.reason, func(t *testing.T) {
			ts := queuedTask("t1")
			ts.StatusReason, ts.AttemptsTotal, ts.FaultRetriesUsed = c.reason, 2, 1
			h := newActorHarness(t, ts)
			h.spawn("t1")
			r := h.nextRun()
			created, _, _, _ := h.st.snapshot()
			if len(created) != 1 || created[0].Retry != c.want || created[0].AttemptNo != 3 || r.attemptNo() != 3 {
				t.Fatalf("重启后的 attempt = %+v，期望重试类别 %q、attempt_no = 3", created, c.want)
			}
		})
	}
}

// ---- 控制 ----

// ready 之前的 cancel 直接停止执行（§8.1），不经协议。
func TestActorCancelBeforeReadyStopsDirectly(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	a := h.spawn("t1")
	r := h.nextRun() // handshaking
	h.st.setControl("t1", "cancel")
	a.Notify()
	h.waitFor("停止环境", h.logged("stop:id-2", 1))
	if !r.noControl() {
		t.Errorf("ready 之前不应发送协议控制")
	}
	h.before("revoke_db:id-1", "stop:id-2 revoked=true")
	r.finish(crash()) // 环境被停止后 Worker 被杀
	h.waitDone()
	if ts := h.st.task("t1"); ts.Status != "cancelled" {
		t.Errorf("任务应 cancelled: %+v", ts)
	}
}

// ready 之后的 cancel 经协议交给 runner（grace），由 runner 结束执行后再停止环境。
func TestActorCancelAfterReadyUsesProtocol(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	a := h.spawn("t1")
	r := h.nextRun()
	r.ready() // WorkerStarted 先于控制读取的结果进入收件箱
	h.st.setControl("t1", "cancel")
	a.Notify()
	if c := r.control(); c != (Control{Kind: "cancel", GraceMs: DefaultControlGraceMs}) {
		t.Fatalf("控制 = %+v", c)
	}
	if h.log.count("stop:") != 0 {
		t.Errorf("ready 之后的 cancel 不应直接停止环境")
	}
	r.finish(Outcome{Class: ClassCancelled})
	h.waitDone()
	h.before("run:id-1", "stop:id-2")
	if ts := h.st.task("t1"); ts.Status != "cancelled" {
		t.Errorf("任务应 cancelled: %+v", ts)
	}
}

func TestActorPauseThenResume(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	a := h.spawn("t1")
	r := h.nextRun()
	r.ready()
	h.st.setControl("t1", "pause")
	a.Notify()
	if c := r.control(); c.Kind != "pause" {
		t.Fatalf("控制 = %+v", c)
	}
	r.finish(Outcome{Class: ClassPaused, ProposalKind: "paused"})
	h.waitFor("paused 且归还槽位", func() bool { return h.st.task("t1").Status == "paused" && h.log.count("release:1") == 1 })
	select {
	case <-a.Done():
		t.Fatalf("paused 任务的 actor 不应退出")
	default:
	}
	h.st.setControl("t1", "run")
	a.Notify()
	r2 := h.nextRun()
	created, _, _, _ := h.st.snapshot()
	if len(created) != 2 || created[1].Retry != RetryNone || r2.attemptNo() != 2 {
		t.Fatalf("恢复后的 attempt 应不计重试: %+v", created)
	}
	if ts := h.st.task("t1"); ts.FaultRetriesUsed != 0 || ts.AttemptsTotal != 2 {
		t.Errorf("暂停恢复只增加 attempts_total: %+v", ts)
	}
	r2.ready()
	r2.finish(success())
	h.waitDone()
}

// ---- stop_blocked ----

// 恢复交来的 stop_blocked：退避重试停止；只有 Recorded 才归还容量，之前不创建替代执行。
func TestActorStopBlockedHoldsCapacityUntilRecorded(t *testing.T) {
	ts := queuedTask("t1")
	ts.Status, ts.CurrentAttemptID, ts.AttemptsTotal = "running", "a0", 1
	h := newActorHarness(t, ts)
	h.st.attempts["a0"] = &Attempt{AttemptID: "a0", TaskID: "t1", AttemptNo: 1, EnvID: "e0", Status: "active"}
	h.env.stops = []StopReport{{Blocked: true}, {Stopped: true, At: t0}}
	h.spawn("t1", WithStopBlocked(SlotGrant{ID: 77, TaskID: "t1"}))

	h.waitFor("首次停止", h.logged("stop:e0", 1))
	h.advanceUntil("第二次停止（已停止未记录）", 100*time.Millisecond, h.logged("stop:e0", 2))
	h.advanceUntil("第三次停止（已记录）", 100*time.Millisecond, h.logged("stop:e0", 3))
	h.waitFor("归还恢复的授予", h.logged("release:77", 1))
	h.advanceUntil("故障重试", 250*time.Millisecond, h.logged("acquire:", 1))
	r := h.nextRun()

	if i, j := h.log.index("stop:e0", 3), h.log.index("release:77", 1); i < 0 || j < i {
		t.Errorf("容量在确认记录之前被归还；日志: %v", h.log.all())
	}
	if i, j := h.log.index("release:77", 1), h.log.index("create_attempt:", 1); j < i {
		t.Errorf("替代执行早于确认停止；日志: %v", h.log.all())
	}
	if got := h.log.count("revoke_db:a0"); got != 1 {
		t.Errorf("访问撤销应在首次停止前执行一次，得到 %d", got)
	}
	created, fin, _, _ := h.st.snapshot()
	if len(fin) != 1 || fin[0].OutcomeClass != "lost_on_restart" || fin[0].TaskStatus != "queued" || fin[0].FromStatus != "active" {
		t.Fatalf("判决 = %+v", fin)
	}
	if created[0].Retry != RetryFault || r.attemptNo() != 2 {
		t.Errorf("替代 attempt 应为故障重试: %+v", created)
	}
	r.finish(success())
	h.waitDone()
}

// ---- Store 故障 ----

// 写入暂时失败时按退避以同一身份重试，待提交的事实（attempt 身份、判决）保持不变，最终由同一 actor 提交。
func TestActorStoreFailureRetriesWithBackoff(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	unknown := &persistence.CommitUnknownError{Op: "x", Err: persistence.ErrUnavailable}
	h.st.setFail("CreateAttempt", unknown)
	h.st.setFail("FinalizeAttempt", persistence.ErrUnavailable, unknown)
	h.spawn("t1")
	h.waitFor("首次创建失败", h.logged("create_attempt:", 1))
	h.advanceUntil("重试创建", 100*time.Millisecond, h.logged("create_attempt:", 2))
	r := h.nextRun()
	r.ready()
	r.finish(success())
	h.waitFor("首次判决失败", h.logged("finalize:", 1))
	h.advanceUntil("判决提交", 100*time.Millisecond, h.logged("finalize:", 3))
	h.waitDone()

	created, fin, _, times := h.st.snapshot()
	if len(created) != 2 || created[0] != created[1] {
		t.Errorf("CreateAttempt 重试应保持同一身份: %+v", created)
	}
	if len(fin) != 3 || !reflect.DeepEqual(fin[0], fin[1]) || !reflect.DeepEqual(fin[1], fin[2]) {
		t.Errorf("判决重试应保持同一内容: %+v", fin)
	}
	ft := times["FinalizeAttempt"]
	if gap := ft[1].Sub(ft[0]); gap < RetryBackoff(0, 0) {
		t.Errorf("第一次重试间隔 %s 小于退避 %s", gap, RetryBackoff(0, 0))
	}
	if gap := ft[2].Sub(ft[1]); gap < RetryBackoff(1, 0) {
		t.Errorf("第二次重试间隔 %s 小于退避 %s", gap, RetryBackoff(1, 0))
	}
	if ts := h.st.task("t1"); ts.Status != "succeeded" || ts.AttemptsTotal != 1 {
		t.Errorf("任务应 succeeded 且只创建一个 attempt: %+v", ts)
	}
}

// 判决被确定拒绝（控制已变化）时交给 Decide，按最新控制重算后提交。
func TestActorFinalizeRejectedRecomputesWithLatestControl(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	r.ready()
	h.st.setControl("t1", "cancel") // 已接受但尚未通知 actor
	r.finish(success())
	h.advanceUntil("按最新控制重算", 250*time.Millisecond, h.logged("finalize:id-1:cancelled", 1))
	h.waitDone()
	if ts := h.st.task("t1"); ts.Status != "cancelled" || ts.StatusReason != "completed_during_cancel" {
		t.Errorf("任务应 cancelled（completed_during_cancel）: %+v", ts)
	}
}

// ---- 运行时间 ----

func TestActorPersistsRunTimeEvery10s(t *testing.T) {
	ts := queuedTask("t1")
	ts.RunTimeMs = 5000 // 之前 attempt 的累计
	h := newActorHarness(t, ts)
	h.spawn("t1")
	r := h.nextRun() // 区间从 attempt 创建（t0）开始
	h.clk.Advance(9 * time.Second)
	h.clk.Advance(time.Second)
	h.waitFor("第一次持久化", h.logged("runtime:", 1))
	h.clk.Advance(10 * time.Second)
	h.waitFor("第二次持久化", h.logged("runtime:", 2))
	r.ready()
	r.finish(success())
	h.waitDone()
	_, _, rts, _ := h.st.snapshot()
	if want := []int64{15000, 25000, 25000}; !reflect.DeepEqual(rts, want) {
		t.Errorf("运行时间 = %v，期望 %v（每 10 s 一次，停止时补提交，单调）", rts, want)
	}
}

// ---- 退出 ----

// ctx 结束：在途操作被取消，全部 goroutine 返回后 Done 关闭；迟到的 OnReady 不阻塞。
func TestActorShutdownLeaksNothing(t *testing.T) {
	before := runtime.NumGoroutine()
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	h.cancel()
	select {
	case <-h.actor.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("ctx 结束后 actor 未退出")
	}
	if err := h.actor.Err(); err != nil {
		t.Errorf("服务停止不是致命错误: %v", err)
	}
	ready := make(chan struct{})
	go func() { r.ready(); close(ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("actor 退出后 OnReady 阻塞")
	}
	h.waitFor("goroutine 回落", func() bool { return runtime.NumGoroutine() <= before })
	if ts := h.st.task("t1"); ts.Status != "running" {
		t.Errorf("服务停止不提交裁决（由启动恢复处理）: %+v", ts)
	}
}

// 决策不合法（事实不一致）是致命错误：actor 报告并退出。
func TestActorInconsistentFactsAreFatal(t *testing.T) {
	ts := queuedTask("t1")
	ts.Status = "running" // 没有 stop_blocked 交接却处于执行中
	h := newActorHarness(t, ts)
	h.spawn("t1")
	select {
	case <-h.actor.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("actor 未退出")
	}
	if err := h.actor.Err(); !errors.Is(err, ErrInvalid) {
		t.Errorf("Err = %v，期望 ErrInvalid", err)
	}
}

// ---- Scheduler ----

func TestSchedulerStartsActiveTasksAndSkipsExcluded(t *testing.T) {
	done := TaskState{TaskID: "t3", Status: "succeeded", Desired: "run", ControlVersion: 1, AppliedControlVersion: 1}
	h := newActorHarness(t, queuedTask("t1"), queuedTask("t2"), done)
	s := NewScheduler(h.ctx, h.deps())
	if err := s.Start(StartOptions{Excluded: []string{"t2"}}); err != nil {
		t.Fatal(err)
	}
	r := h.nextRun()
	if r.spec.Task.TaskID != "t1" || s.Actor("t2") != nil || s.Actor("t3") != nil {
		t.Fatalf("只应为 t1 建立 actor: run=%s", r.spec.Task.TaskID)
	}
	s.Submit("t2") // 被隔离：忽略
	h.st.mu.Lock()
	t4 := queuedTask("t4")
	h.st.tasks["t4"] = &t4
	h.st.mu.Unlock()
	s.Submit("t4")
	r4 := h.nextRun()
	if r4.spec.Task.TaskID != "t4" || s.Actor("t2") != nil {
		t.Fatalf("Submit 应为新任务建立 actor: %s", r4.spec.Task.TaskID)
	}
	r.ready()
	r.finish(success())
	h.waitFor("t1 的 actor 退出并移除", func() bool { return s.Actor("t1") == nil })
	h.cancel()
	waited := make(chan struct{})
	go func() { s.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatalf("ctx 结束后 Scheduler.Wait 未返回")
	}
}

// ==== 修订 1：累计运行时限与恢复 attempt 的运行时间记账 ====

// waitCause 等待 Run 的 ctx 被取消并返回 cause。
func (h *actorHarness) waitCause(r *fkRun) error {
	h.t.Helper()
	select {
	case err := <-r.cause:
		return err
	case <-time.After(5 * time.Second):
		h.t.Fatalf("Run 的 ctx 未被取消；日志: %v", h.log.all())
	}
	return nil
}

// notCancelledYet 让 actor 处理已到期的计时器后断言 Run 仍在运行。
func (h *actorHarness) notCancelledYet(r *fkRun, at string) {
	h.t.Helper()
	time.Sleep(20 * time.Millisecond)
	if r.cancelled() {
		h.t.Fatalf("%s 时 Run 不应被取消", at)
	}
}

// 执行中到达累计运行时限：以 context.DeadlineExceeded 取消 Run，任务 task_deadline_exceeded、不重试。
func TestActorRunTimeLimitCancelsRun(t *testing.T) {
	ts := queuedTask("t1")
	ts.RunTimeMs = 5000 // 限额按任务计：已持久化的部分计入
	h := newActorHarness(t, ts)
	h.limit = 30 * time.Second
	h.spawn("t1")
	r := h.nextRun() // 区间从 t0 开始，到达限额在 t0 + 25 s
	h.clk.Advance(24 * time.Second)
	h.notCancelledYet(r, "累计 29 s")
	h.clk.Advance(time.Second)
	if cause := h.waitCause(r); !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("cause = %v，期望 context.DeadlineExceeded", cause)
	}
	h.waitDone()
	if ts := h.st.task("t1"); ts.Status != "failed" || ts.StatusReason != ClassDeadlineExceeded || ts.AttemptsTotal != 1 {
		t.Errorf("任务应 failed（task_deadline_exceeded）且不重试: %+v", ts)
	}
}

// 限额跨 attempt 累计：新 attempt 不重置。
func TestActorRunTimeLimitCountsAcrossAttempts(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.limit = 20 * time.Second
	h.spawn("t1")
	r := h.nextRun()
	h.clk.Advance(12 * time.Second) // 第一个 attempt 用去 12 s
	r.finish(crash())
	h.waitFor("裁决为 queued 且归还槽位", func() bool { return h.st.task("t1").Status == "queued" && h.log.count("release:1") == 1 })
	h.clk.Advance(RetryBackoff(0, 0)) // 到 not_before
	r2 := h.nextRun()                 // 区间从 t0 + 13 s 开始，剩余 8 s
	if created, _, _, _ := h.st.snapshot(); len(created) != 2 || created[1].Retry != RetryFault {
		t.Fatalf("限额未用尽时重试不变: %+v", created)
	}
	h.clk.Advance(7 * time.Second)
	h.notCancelledYet(r2, "累计 19 s")
	h.clk.Advance(time.Second)
	if cause := h.waitCause(r2); !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("cause = %v", cause)
	}
	h.waitDone()
	if ts := h.st.task("t1"); ts.Status != "failed" || ts.StatusReason != ClassDeadlineExceeded || ts.RunTimeMs != 20000 {
		t.Errorf("任务应在累计 20 s 时 failed: %+v", ts)
	}
}

func TestActorRunTimeLimitZeroNeverCancels(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.spawn("t1")
	r := h.nextRun()
	for i := 0; i < 6; i++ {
		h.clk.Advance(10 * time.Minute)
	}
	h.notCancelledYet(r, "运行 1 h（不限）")
	r.ready()
	r.finish(success())
	h.waitDone()
}

// queued 任务已达到限额（重启记账或调低配置之后）：走正常路径，执行以已取消的 ctx 开始，
// 经现有判决路径结束为 failed / task_deadline_exceeded。
func TestActorRunTimeLimitExhaustedEndsThroughNormalPath(t *testing.T) {
	ts := queuedTask("t1")
	ts.RunTimeMs = 30000
	h := newActorHarness(t, ts)
	h.limit = 30 * time.Second
	h.spawn("t1")
	h.waitDone() // 不推进时钟：取消在启动时即已生效
	if ts := h.st.task("t1"); ts.Status != "failed" || ts.StatusReason != ClassDeadlineExceeded || ts.AttemptsTotal != 1 {
		t.Errorf("任务应经一次 attempt 结束为 failed（task_deadline_exceeded）: %+v", ts)
	}
	h.before("create_env:", "stop:")
}

// 限额已用尽时可重试的结果（例如与限额取消同时发生的崩溃）改为 task_deadline_exceeded，不再排队。
func TestActorRetryableOutcomeOverLimitIsNotRetried(t *testing.T) {
	h := newActorHarness(t, queuedTask("t1"))
	h.limit = 20 * time.Second
	crashed := crash()
	h.rn.onDeadline = &crashed
	h.spawn("t1")
	r := h.nextRun()
	h.clk.Advance(20 * time.Second)
	if err := h.waitCause(r); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超限应以 DeadlineExceeded 取消 run ctx，得到 %v", err)
	}
	h.waitDone()
	ts := h.st.task("t1")
	if ts.Status != "failed" || ts.StatusReason != ClassDeadlineExceeded || ts.AttemptsTotal != 1 || ts.FaultRetriesUsed != 0 {
		t.Errorf("已用尽限额的任务不应重试: %+v", ts)
	}
	if _, fin, _, _ := h.st.snapshot(); len(fin) != 1 || fin[0].OutcomeClass != ClassDeadlineExceeded || fin[0].ExitSignal == nil {
		t.Errorf("判决应为 task_deadline_exceeded 并保留退出事实: %+v", fin)
	}
}

// 恢复交来的 stop_blocked：确认记录后、判决之前调用 OnStopRecorded 恰好一次成功；失败按退避重试。
func TestActorStopBlockedAccountsUnrecordedRunTime(t *testing.T) {
	ts := queuedTask("t1")
	ts.Status, ts.CurrentAttemptID, ts.AttemptsTotal = "running", "a0", 1
	h := newActorHarness(t, ts)
	h.st.attempts["a0"] = &Attempt{AttemptID: "a0", TaskID: "t1", AttemptNo: 1, EnvID: "e0", Status: "active"}
	h.accountFail = []error{persistence.ErrUnavailable}
	h.spawn("t1", WithStopBlocked(SlotGrant{ID: 77, TaskID: "t1"}))
	h.waitFor("首次记账失败", func() bool { return len(h.accountCalls()) == 1 })
	if h.log.count("finalize:") != 0 {
		t.Fatalf("记账成功之前不应提交判决；日志: %v", h.log.all())
	}
	h.advanceUntil("记账重试并提交判决", 100*time.Millisecond, h.logged("finalize:a0:queued", 1))
	h.before("account:a0", "finalize:a0")
	if i, j := h.log.index("account:a0", 2), h.log.index("finalize:a0", 1); i < 0 || j < i {
		t.Errorf("判决应在记账成功之后；日志: %v", h.log.all())
	}
	h.advanceUntil("故障重试", 250*time.Millisecond, h.logged("acquire:", 1))
	r := h.nextRun()
	r.finish(success())
	h.waitDone()
	if got := h.accountCalls(); !reflect.DeepEqual(got, []string{"a0@0s", "a0@0s"}) {
		t.Errorf("OnStopRecorded 调用 = %v，期望同一事实一次失败、一次成功", got)
	}
}
