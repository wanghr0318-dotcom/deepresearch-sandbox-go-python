package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/persistence"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/reconcile"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/task"
)

// world 是 recovery.Store、task.Store、resource.Store 与 Coordinator 的内存替身（方法名互不冲突，一个类型
// 实现全部接口）。故障注入：第 failAt 次调用（读写都计数）失败；commitThenFail 时先生效再返回错误
// （提交结果未知）。dup 统计对已完成对象的重复写入——幂等执行器应使它保持为 0。
type world struct {
	tasks      map[string]*task.TaskState
	attempts   map[string]*task.Attempt
	envs       map[string]*resource.Environment
	intents    map[string]*resource.Intent
	ranges     map[string]*resource.UIDRange
	quarantine map[string]resource.Quarantine
	accounted  map[string]time.Time
	reclaimed  map[string]bool
	stopMode   map[string]string // "" 正常 | blocked | unrecorded
	verdicts   map[string]task.Verdict

	stopAt         time.Time
	calls, failAt  int
	commitThenFail bool
	dup            int
	finalizes      int
}

var errFault = errors.New("注入的故障")

func newWorld() *world {
	return &world{
		tasks: map[string]*task.TaskState{}, attempts: map[string]*task.Attempt{}, envs: map[string]*resource.Environment{},
		intents: map[string]*resource.Intent{}, ranges: map[string]*resource.UIDRange{},
		quarantine: map[string]resource.Quarantine{}, accounted: map[string]time.Time{}, reclaimed: map[string]bool{},
		stopMode: map[string]string{}, verdicts: map[string]task.Verdict{},
		stopAt: time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC),
	}
}

// call 计数并按注入规则执行 apply；apply 返回业务错误时原样返回。
func (w *world) call(apply func() error) error {
	w.calls++
	if w.calls == w.failAt {
		if w.commitThenFail && apply != nil {
			_ = apply()
		}
		return errFault
	}
	if apply == nil {
		return nil
	}
	return apply()
}

// ---- recovery.Store ----

func (w *world) LoadRecoveryFacts(context.Context) (Facts, error) {
	return Facts{}, errors.New("未使用")
}
func (w *world) RevokeAllActive(context.Context, string) (int, error) {
	return 0, errors.New("未使用")
}
func (w *world) AccountUnrecordedRunTime(_ context.Context, taskID, attemptID string, until time.Time) error {
	return w.call(func() error {
		w.accounted[taskID+"/"+attemptID] = until
		return nil
	})
}

// ---- task.Store ----

func (w *world) CreateAttempt(context.Context, task.NewAttempt) (task.Attempt, error) {
	return task.Attempt{}, errors.New("恢复不得创建 attempt")
}
func (w *world) GetAttempt(_ context.Context, id string) (task.Attempt, error) {
	var a task.Attempt
	err := w.call(func() error {
		p, ok := w.attempts[id]
		if !ok {
			return persistence.ErrNotFound
		}
		a = *p
		return nil
	})
	return a, err
}
func (w *world) ApplyControl(_ context.Context, c task.ApplyControl) (task.ControlState, error) {
	err := w.call(func() error {
		t := w.tasks[c.TaskID]
		if c.ControlVersion <= t.AppliedControlVersion {
			w.dup++
			return persistence.ErrRejected
		}
		t.Status, t.StatusReason, t.AppliedControlVersion = c.Status, c.StatusReason, c.ControlVersion
		return nil
	})
	return task.ControlState{}, err
}
func (w *world) GetControlState(context.Context, string) (task.ControlState, error) {
	return task.ControlState{}, errors.New("未使用")
}
func (w *world) FinalizeAttempt(_ context.Context, v task.Verdict) (task.Attempt, error) {
	var out task.Attempt
	err := w.call(func() error {
		a, t := w.attempts[v.AttemptID], w.tasks[v.TaskID]
		if a.Status == attemptEnded {
			w.dup++
			return persistence.ErrConflict
		}
		if t.CurrentAttemptID != v.AttemptID || a.Status != v.FromStatus {
			return persistence.ErrRejected
		}
		a.Status, a.OutcomeClass = attemptEnded, v.OutcomeClass
		t.Status, t.StatusReason, t.NotBefore = v.TaskStatus, v.TaskStatusReason, v.NotBefore
		t.AppliedControlVersion = max(t.AppliedControlVersion, v.ControlVersion)
		w.verdicts[v.AttemptID] = v
		w.finalizes++
		out = *a
		return nil
	})
	return out, err
}
func (w *world) LoadTask(_ context.Context, id string) (task.TaskState, error) {
	var ts task.TaskState
	err := w.call(func() error {
		t, ok := w.tasks[id]
		if !ok {
			return persistence.ErrNotFound
		}
		ts = *t
		return nil
	})
	return ts, err
}
func (w *world) ListActiveTasks(context.Context) ([]string, error) {
	return nil, errors.New("未使用")
}
func (w *world) PersistRunTime(context.Context, string, string, int64) (int64, error) {
	return 0, errors.New("未使用")
}
func (w *world) RevokeAttemptAccess(context.Context, string, string) error {
	return errors.New("未使用")
}

// ---- resource.Store ----

func (w *world) RecordIntent(context.Context, resource.Intent) (resource.Intent, error) {
	return resource.Intent{}, errors.New("未使用")
}
func (w *world) ResolveIntent(_ context.Context, id, state string) (resource.Intent, error) {
	var out resource.Intent
	err := w.call(func() error {
		i := w.intents[id]
		if i.State == state {
			w.dup++
			return nil
		}
		i.State = state
		out = *i
		return nil
	})
	return out, err
}
func (w *world) GetIntent(_ context.Context, id string) (resource.Intent, error) {
	var out resource.Intent
	err := w.call(func() error {
		i, ok := w.intents[id]
		if !ok {
			return persistence.ErrNotFound
		}
		out = *i
		return nil
	})
	return out, err
}
func (w *world) SeedUIDRanges(context.Context, int64, int64, int) error {
	return errors.New("未使用")
}
func (w *world) AssignUIDRange(context.Context, string, string) (resource.UIDRange, error) {
	return resource.UIDRange{}, errors.New("未使用")
}
func (w *world) ReleaseUIDRange(_ context.Context, id, alloc string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := w.call(func() error {
		r := w.ranges[id]
		if r.State != "assigned" || r.AllocationID != alloc {
			w.dup++
			return persistence.ErrConflict
		}
		r.State, r.OwnerID = "free", ""
		out = *r
		return nil
	})
	return out, err
}
func (w *world) GetUIDRange(_ context.Context, envID string) (resource.UIDRange, error) {
	var out resource.UIDRange
	err := w.call(func() error {
		for _, r := range w.ranges {
			if r.OwnerID == envID && r.State == "assigned" {
				out = *r
				return nil
			}
		}
		return persistence.ErrNotFound
	})
	return out, err
}
func (w *world) MarkStopped(context.Context, string, time.Time) (resource.Environment, error) {
	return resource.Environment{}, errors.New("恢复经 coordinator 停止")
}
func (w *world) UpdateCleanup(context.Context, resource.CleanupUpdate) (resource.Environment, error) {
	return resource.Environment{}, errors.New("未使用")
}
func (w *world) GetEnvironment(_ context.Context, id string) (resource.Environment, error) {
	var out resource.Environment
	err := w.call(func() error {
		e, ok := w.envs[id]
		if !ok {
			return persistence.ErrNotFound
		}
		out = *e
		return nil
	})
	return out, err
}
func (w *world) ListCleanupCandidates(context.Context, time.Time, int) ([]resource.Environment, error) {
	return nil, errors.New("未使用")
}
func (w *world) RecordQuarantine(_ context.Context, q resource.Quarantine) error {
	return w.call(func() error {
		w.quarantine[q.Path] = q
		return nil
	})
}

// ---- Coordinator ----

func (w *world) StopEnv(_ context.Context, envID string) (resource.StopResult, error) {
	var res resource.StopResult
	err := w.call(func() error {
		switch w.stopMode[envID] {
		case "blocked":
			res = resource.StopResult{Blocked: true}
		case "unrecorded":
			res = resource.StopResult{Stopped: true, At: w.stopAt}
		default:
			e := w.envs[envID]
			if e.StoppedAt != nil {
				w.dup++
			} else {
				at := w.stopAt
				e.StoppedAt = &at
			}
			res = resource.StopResult{Stopped: true, Recorded: true, At: *e.StoppedAt}
		}
		return nil
	})
	return res, err
}
func (w *world) ReclaimOrphan(_ context.Context, envID string) error {
	return w.call(func() error {
		w.reclaimed[envID] = true
		return nil
	})
}

func (w *world) deps() Deps {
	return Deps{Store: w, Tasks: w, Resources: w, Coordinator: w,
		Clock:   func() time.Time { return time.Date(2026, 10, 5, 9, 1, 0, 0, time.UTC) },
		Jitter:  func() float64 { return 0.5 },
		Options: Options{DefaultMemoryBytes: 512 << 20}}
}

// snapshot 是用于比较最终状态的深拷贝。
type snapshot struct {
	Tasks      map[string]task.TaskState
	Attempts   map[string]task.Attempt
	Envs       map[string]resource.Environment
	Intents    map[string]resource.Intent
	Ranges     map[string]resource.UIDRange
	Quarantine map[string]resource.Quarantine
	Accounted  map[string]time.Time
	Reclaimed  map[string]bool
	Verdicts   map[string]task.Verdict
}

func (w *world) snapshot() snapshot {
	s := snapshot{Tasks: map[string]task.TaskState{}, Attempts: map[string]task.Attempt{}, Envs: map[string]resource.Environment{},
		Intents: map[string]resource.Intent{}, Ranges: map[string]resource.UIDRange{}, Quarantine: map[string]resource.Quarantine{},
		Accounted: map[string]time.Time{}, Reclaimed: map[string]bool{}, Verdicts: map[string]task.Verdict{}}
	for k, v := range w.tasks {
		s.Tasks[k] = *v
	}
	for k, v := range w.attempts {
		s.Attempts[k] = *v
	}
	for k, v := range w.envs {
		s.Envs[k] = *v
	}
	for k, v := range w.intents {
		s.Intents[k] = *v
	}
	for k, v := range w.ranges {
		s.Ranges[k] = *v
	}
	for k, v := range w.quarantine {
		s.Quarantine[k] = v
	}
	for k, v := range w.accounted {
		s.Accounted[k] = v
	}
	for k, v := range w.reclaimed {
		s.Reclaimed[k] = v
	}
	for k, v := range w.verdicts {
		s.Verdicts[k] = v
	}
	return s
}

const lostMemory = 256 << 20

// scenario 建立一个覆盖全部步骤种类的库，并以 ReconcileFacts + reconcile.Plan 生成计划（同时验证转换）。
func scenario(t *testing.T) (*world, reconcile.RecoveryPlan) {
	t.Helper()
	w := newWorld()
	stopped := w.stopAt.Add(-time.Hour)
	addTask := func(id, status, desired string, cv, applied int64, attemptID, attemptStatus, envID string) {
		w.tasks[id] = &task.TaskState{TaskID: id, Status: status, Desired: desired, ControlVersion: cv,
			AppliedControlVersion: applied, CurrentAttemptID: attemptID, MaxFaultRetries: 3}
		if attemptID != "" {
			w.attempts[attemptID] = &task.Attempt{AttemptID: attemptID, TaskID: id, EnvID: envID, Status: attemptStatus}
			w.envs[envID] = &resource.Environment{EnvID: envID, AttemptID: attemptID, CleanupState: resource.CleanupNone}
		}
	}
	addTask("t-lost", "running", "run", 1, 1, "a1", "active", "e1")
	w.tasks["t-lost"].Limits = json.RawMessage(fmt.Sprintf(`{"memory_max": %d}`, lostMemory))
	addTask("t-cancel", "running", "cancel", 2, 1, "a2", "active", "e2")
	addTask("t-pausing", "pausing", "pause", 2, 2, "a6", "handshaking", "e6")
	addTask("t-pause", "queued", "pause", 2, 1, "", "", "")
	addTask("t-cq", "queued", "cancel", 2, 1, "", "", "")
	addTask("t-queued", "queued", "run", 1, 1, "", "", "")
	addTask("t-paused", "paused", "pause", 2, 2, "", "", "")
	addTask("t-bad", "queued", "run", 1, 1, "a-bad", "active", "e-bad")
	// 终态任务的 attempt：环境未停止。
	w.attempts["a-old"] = &task.Attempt{AttemptID: "a-old", TaskID: "t-done", EnvID: "e3", Status: attemptEnded}
	w.envs["e3"] = &resource.Environment{EnvID: "e3", AttemptID: "a-old", CleanupState: resource.CleanupNone}
	// 已清理完成的环境：未结束的 intent 与未归还的 UID 范围。
	w.envs["e4"] = &resource.Environment{EnvID: "e4", StoppedAt: &stopped, CleanupState: resource.CleanupDone}
	w.intents["i4"] = &resource.Intent{IntentID: "i4", EnvID: "e4", Kind: "environment", Name: "e4", State: resource.IntentAcquired}
	w.envs["e5"] = &resource.Environment{EnvID: "e5", StoppedAt: &stopped, CleanupState: resource.CleanupDone}
	w.ranges["r5"] = &resource.UIDRange{UIDRangeID: "r5", Base: 100000, Size: 4096, State: "assigned", OwnerID: "e5", AllocationID: "al5"}

	f := Facts{
		PendingIntents:   []resource.Intent{*w.intents["i4"]},
		UnreleasedRanges: []resource.UIDRange{*w.ranges["r5"]},
	}
	for _, id := range []string{"t-lost", "t-cancel", "t-pausing", "t-pause", "t-cq", "t-queued", "t-paused", "t-bad"} {
		ts := w.tasks[id]
		tf := TaskFact{TaskID: id, Status: ts.Status, Desired: ts.Desired, ControlVersion: ts.ControlVersion}
		if a := w.attempts[ts.CurrentAttemptID]; a != nil {
			tf.CurrentAttempt = &AttemptFact{AttemptID: a.AttemptID, Status: a.Status, EnvID: a.EnvID}
		}
		f.Tasks = append(f.Tasks, tf)
	}
	for _, id := range []string{"e1", "e2", "e6", "e-bad", "e3"} {
		e := w.envs[id]
		f.Environments = append(f.Environments, EnvFact{EnvID: id, AttemptID: e.AttemptID, CleanupState: e.CleanupState})
	}
	scan := provider.ScanReport{Items: []provider.ScanItem{
		{Layer: "env_dir", Path: "/data/envs/e9", EnvID: "e9", Owner: provider.OwnedComplete},
		{Layer: "cgroup", Path: "/sys/fs/cgroup/agentbox-x/env-e8", EnvID: "e8", Owner: provider.Unknown},
	}}
	return w, reconcile.Plan(ReconcileFacts(f), scan, "inst-1")
}

func statuses(r Report) map[string]StepStatus {
	m := map[string]StepStatus{}
	for _, s := range r.Steps {
		m[s.StepID] = s.Status
	}
	return m
}

// 每类步骤一个断言：计划由 reconcile 生成，覆盖全部种类；执行后各对象到达预期状态。
func TestExecuteEachStepKind(t *testing.T) {
	w, plan := scenario(t)
	kinds := map[reconcile.StepKind]bool{}
	for _, s := range plan.Steps {
		kinds[s.Kind] = true
	}
	for _, k := range []reconcile.StepKind{reconcile.StopEnv, reconcile.MarkAttemptLost, reconcile.CancelTask, reconcile.PauseTask,
		reconcile.KeepQueued, reconcile.KeepPaused, reconcile.KeepTerminal, reconcile.ReclaimOrphan, reconcile.Quarantine,
		reconcile.Alert, reconcile.ResolveIntent, reconcile.ReleaseUIDRange} {
		if !kinds[k] {
			t.Fatalf("场景的计划缺少步骤种类 %s: %+v", k, plan.Steps)
		}
	}

	r, err := Execute(context.Background(), plan, w.deps())
	if err != nil || !r.Ready {
		t.Fatalf("Execute = %+v, %v", r, err)
	}
	st := statuses(r)

	// StopEnv：所有未停止的环境（含被隔离任务与终态 attempt 的环境）都已记录 stopped_at。
	for _, id := range []string{"e1", "e2", "e6", "e-bad", "e3"} {
		if w.envs[id].StoppedAt == nil || st["stop_env:"+id] != StepDone {
			t.Errorf("环境 %s 未停止（%s）", id, st["stop_env:"+id])
		}
	}
	// MarkAttemptLost：Decide 给出故障重试排队，not_before = now + 退避；先补记运行时限到 stopped_at。
	v := w.verdicts["a1"]
	wantNB := w.deps().Clock().Add(task.RetryBackoff(0, 0.5))
	if v.OutcomeClass != classLostOnRestart || v.TaskStatus != "queued" || v.FromStatus != "active" ||
		v.NotBefore == nil || !v.NotBefore.Equal(wantNB) || w.tasks["t-lost"].Status != "queued" {
		t.Errorf("t-lost 的判决 = %+v", v)
	}
	if w.tasks["t-lost"].FaultRetriesUsed != 0 {
		t.Error("恢复不得递增 fault_retries_used（只在创建替代 attempt 的事务中递增）")
	}
	if got := w.accounted["t-lost/a1"]; !got.Equal(w.stopAt) {
		t.Errorf("t-lost 运行时限补记到 %v，want stopped_at %v", got, w.stopAt)
	}
	// CancelTask/PauseTask：有未结束 attempt → 判决 cancelled/paused；无 attempt → 控制转换。
	for id, want := range map[string]string{"t-cancel": "cancelled", "t-pausing": "paused", "t-cq": "cancelled", "t-pause": "paused"} {
		if got := w.tasks[id].Status; got != want {
			t.Errorf("%s = %s, want %s", id, got, want)
		}
	}
	if w.verdicts["a2"].OutcomeClass != classLostOnRestart || w.verdicts["a6"].FromStatus != "handshaking" {
		t.Errorf("取消/暂停的判决 = %+v / %+v", w.verdicts["a2"], w.verdicts["a6"])
	}
	// KeepQueued/KeepPaused/KeepTerminal：只复核，不写入。
	for _, id := range []string{"keep_queued:t-queued", "keep_paused:t-paused", "keep_terminal:a-old"} {
		if st[id] != StepSkipped {
			t.Errorf("%s = %s, want skipped", id, st[id])
		}
	}
	if w.tasks["t-queued"].Status != "queued" || w.tasks["t-paused"].Status != "paused" {
		t.Error("保持类步骤改变了任务状态")
	}
	// ReclaimOrphan、ResolveIntent、ReleaseUIDRange。
	if !w.reclaimed["e9"] {
		t.Error("孤立环境 e9 未回收")
	}
	if w.intents["i4"].State != resource.IntentReleased {
		t.Errorf("intent i4 = %s", w.intents["i4"].State)
	}
	if w.ranges["r5"].State != "free" {
		t.Errorf("UID 范围 r5 = %s", w.ranges["r5"].State)
	}
	// Quarantine + Alert：记录隔离、成对报警、计入占用；被隔离的任务列入 Excluded，不处置。
	if _, ok := w.quarantine["record/task/t-bad"]; !ok {
		t.Errorf("隔离记录 = %v", w.quarantine)
	}
	if _, ok := w.quarantine["/sys/fs/cgroup/agentbox-x/env-e8"]; !ok {
		t.Errorf("隔离记录 = %v", w.quarantine)
	}
	if !reflect.DeepEqual(r.Excluded, []string{"t-bad"}) {
		t.Errorf("Excluded = %v", r.Excluded)
	}
	if len(r.Alerts) != len(r.Quarantined) || len(r.Alerts) != 2 {
		t.Errorf("Alerts = %+v, Quarantined = %v", r.Alerts, r.Quarantined)
	}
	if w.attempts["a-bad"].Status == attemptEnded {
		t.Error("被隔离任务的 attempt 不得被处置")
	}
	// 占用：被隔离的环境 e-bad、e8 各一个 run slot；已停止的环境不占用。
	var occ []string
	for _, g := range r.Occupied {
		occ = append(occ, fmt.Sprintf("%s:%d", g.TaskID, g.MemoryBytes))
	}
	sort.Strings(occ)
	want := []string{fmt.Sprintf(":%d", 512<<20), fmt.Sprintf("t-bad:%d", 512<<20)}
	if !reflect.DeepEqual(occ, want) || len(r.StopBlocked) != 0 {
		t.Errorf("Occupied = %v, StopBlocked = %v; want %v", occ, r.StopBlocked, want)
	}
	if w.dup != 0 {
		t.Errorf("重复写入 %d 次", w.dup)
	}

	// 以同一计划重跑：没有任何步骤再次写入（除按路径幂等的隔离记录）。
	before := w.snapshot()
	r2, err := Execute(context.Background(), plan, w.deps())
	if err != nil || !r2.Ready {
		t.Fatalf("重跑 = %v", err)
	}
	for _, s := range r2.Steps {
		// ReclaimOrphan 的复核在 coordinator 内（逐层扫描，资源已不存在时为空操作），执行器无记录可读。
		if s.Status == StepDone && s.Kind != reconcile.ReclaimOrphan {
			t.Errorf("重跑时步骤 %s 再次执行", s.StepID)
		}
	}
	if !reflect.DeepEqual(before, w.snapshot()) || w.dup != 0 || w.finalizes != 3 {
		t.Errorf("重跑改变了状态（dup = %d, finalizes = %d）", w.dup, w.finalizes)
	}
}

// 在任意一次 Store/coordinator 调用处失败（包括已生效但返回错误的"提交结果未知"），重跑后最终状态与
// 一次成功执行相同，且没有重复提交。
func TestExecuteFailureMidwayThenRerun(t *testing.T) {
	ref, plan := scenario(t)
	if _, err := Execute(context.Background(), plan, ref.deps()); err != nil {
		t.Fatal(err)
	}
	want, total := ref.snapshot(), ref.calls
	for _, commit := range []bool{false, true} {
		for k := 1; k <= total; k++ {
			t.Run(fmt.Sprintf("commit=%v/call=%d", commit, k), func(t *testing.T) {
				w, plan := scenario(t)
				w.failAt, w.commitThenFail = k, commit
				r, err := Execute(context.Background(), plan, w.deps())
				if err != nil && r.Ready {
					t.Fatal("失败时报告 Ready")
				}
				w.failAt = 0
				for i := 0; i < 2; i++ { // 第一次失败可能被记为 stop_blocked：再跑至收敛
					if r, err = Execute(context.Background(), plan, w.deps()); err != nil || !r.Ready {
						t.Fatalf("重跑 = %+v, %v", r, err)
					}
				}
				if got := w.snapshot(); !reflect.DeepEqual(got, want) {
					t.Errorf("最终状态不同:\n got %+v\nwant %+v", got, want)
				}
				if w.dup != 0 || w.finalizes != 3 {
					t.Errorf("dup = %d, finalizes = %d", w.dup, w.finalizes)
				}
			})
		}
	}
}

// 停止未确认（Blocked），或停止已确认但 stopped_at 未持久化：环境计入占用（内存取任务 limits），
// 不提交 lost、不补记运行时限，任务不进入排队；确认停止后的重跑才提交。
func TestStopBlockedOccupiesAndDoesNotQueue(t *testing.T) {
	for _, mode := range []string{"blocked", "unrecorded"} {
		t.Run(mode, func(t *testing.T) {
			w, plan := scenario(t)
			w.stopMode["e1"] = mode
			r, err := Execute(context.Background(), plan, w.deps())
			if err != nil || !r.Ready {
				t.Fatalf("Execute = %+v, %v", r, err)
			}
			if !reflect.DeepEqual(r.StopBlocked, []string{"e1"}) {
				t.Errorf("StopBlocked = %v", r.StopBlocked)
			}
			st := statuses(r)
			if st["stop_env:e1"] != StepStopBlocked || st["mark_attempt_lost:t-lost:a1"] != StepStopBlocked {
				t.Errorf("步骤状态 = %v", st)
			}
			if _, ok := w.verdicts["a1"]; ok || w.attempts["a1"].Status != "active" || w.tasks["t-lost"].Status != "running" {
				t.Error("停止未确认时提交了 lost")
			}
			if _, ok := w.accounted["t-lost/a1"]; ok {
				t.Error("停止未确认时补记了运行时限")
			}
			found := false
			for _, g := range r.Occupied {
				if g.TaskID == "t-lost" && g.MemoryBytes == lostMemory {
					found = true
				}
			}
			if !found || len(r.Occupied) != 3 {
				t.Errorf("Occupied = %+v", r.Occupied)
			}

			w.stopMode["e1"] = ""
			if r, err = Execute(context.Background(), plan, w.deps()); err != nil || len(r.StopBlocked) != 0 {
				t.Fatalf("确认停止后重跑 = %+v, %v", r, err)
			}
			if w.tasks["t-lost"].Status != "queued" || w.verdicts["a1"].OutcomeClass != classLostOnRestart {
				t.Errorf("确认停止后 t-lost = %s", w.tasks["t-lost"].Status)
			}
		})
	}
}

// 复核发现计划之后不再成立的事实：不猜测，隔离并报警；任务隔离列入 Excluded。
func TestRecheckMismatchQuarantines(t *testing.T) {
	w := newWorld()
	w.tasks["t1"] = &task.TaskState{TaskID: "t1", Status: "paused", Desired: "pause"}
	w.tasks["t2"] = &task.TaskState{TaskID: "t2", Status: "running", Desired: "run", CurrentAttemptID: "a2-new"}
	w.attempts["a-live"] = &task.Attempt{AttemptID: "a-live", TaskID: "t9", EnvID: "e9", Status: "active"}
	plan := reconcile.RecoveryPlan{Steps: []reconcile.Step{
		{ID: "keep_queued:t1", Kind: reconcile.KeepQueued, TaskID: "t1", Expect: "queued"},
		{ID: "mark_attempt_lost:t2:a2", Kind: reconcile.MarkAttemptLost, TaskID: "t2", AttemptID: "a2", EnvID: "e2", Expect: "lost"},
		{ID: "keep_terminal:a-live", Kind: reconcile.KeepTerminal, AttemptID: "a-live", EnvID: "e9", Expect: "verdict_committed"},
	}}
	r, err := Execute(context.Background(), plan, w.deps())
	if err != nil || !r.Ready {
		t.Fatalf("Execute = %+v, %v", r, err)
	}
	if !reflect.DeepEqual(r.Excluded, []string{"t1", "t2"}) {
		t.Errorf("Excluded = %v", r.Excluded)
	}
	wantQ := []string{"record/task/t1", "record/task/t2", "record/attempt/a-live"}
	if !reflect.DeepEqual(r.Quarantined, wantQ) || len(r.Alerts) != 3 {
		t.Errorf("Quarantined = %v, Alerts = %+v", r.Quarantined, r.Alerts)
	}
	if w.tasks["t1"].Status != "paused" || w.finalizes != 0 {
		t.Error("复核不一致时写入了任务状态")
	}
	// e2（t2）与 e9（隔离的 attempt 记录）各计一个占用。
	if len(r.Occupied) != 2 {
		t.Errorf("Occupied = %+v", r.Occupied)
	}
}

// 转换逐字段对应：两侧类型任一新增字段而转换未更新时，此用例失败。
func TestReconcileFactsMirrorsFields(t *testing.T) {
	pairs := []struct{ a, b any }{
		{Facts{}, reconcile.Facts{}}, {TaskFact{}, reconcile.TaskFact{}},
		{AttemptFact{}, reconcile.AttemptFact{}}, {EnvFact{}, reconcile.EnvFact{}},
	}
	for _, p := range pairs {
		ta, tb := reflect.TypeOf(p.a), reflect.TypeOf(p.b)
		if ta.NumField() != tb.NumField() {
			t.Fatalf("%s 与 %s 字段数不同", ta, tb)
		}
		for i := 0; i < ta.NumField(); i++ {
			if ta.Field(i).Name != tb.Field(i).Name {
				t.Errorf("%s.%s 与 %s.%s 不对应", ta, ta.Field(i).Name, tb, tb.Field(i).Name)
			}
		}
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := Facts{
		Tasks: []TaskFact{{TaskID: "t", Status: "running", Desired: "run", ControlVersion: 3, NotBefore: &at, RunTimePersistedAt: &at,
			CurrentAttempt: &AttemptFact{AttemptID: "a", Status: "active", EnvID: "e", HasVerdict: true, ProposalKind: "result"}}},
		Environments:     []EnvFact{{EnvID: "e", AttemptID: "a", StoppedAt: &at, CleanupState: "pending"}},
		PendingIntents:   []resource.Intent{{IntentID: "i"}},
		UnreleasedRanges: []resource.UIDRange{{UIDRangeID: "r"}},
	}
	g := ReconcileFacts(f)
	a, b := mustJSON(t, f), mustJSON(t, g)
	if a != b {
		t.Errorf("转换丢失字段:\n%s\n%s", a, b)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
