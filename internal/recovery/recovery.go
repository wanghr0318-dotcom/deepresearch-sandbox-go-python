package recovery

// 本文件是启动恢复的执行器（规格 §14.1 第 4–9 步、§14.2、§14.4、§8.3；代码组织 §6.1）：按顺序幂等执行
// reconcile 生成的 RecoveryPlan。每步执行前重新读取对象的当前状态，已在预期状态则跳过，因而可在任意一步
// 中断后以同一计划（或由新事实重新生成的计划）重跑。执行器不创建 actor、不应用新的控制；丢失 attempt 的
// 裁决由 task.Decide 计算。结果是就绪报告：实际占用的容量（供 admission.Rebuild）、stop_blocked 的环境、
// 隔离项与不得启动 actor 的任务。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/admission"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/reconcile"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// Coordinator 是执行器使用的 coordinator 子集（消费者声明窄接口）。
type Coordinator interface {
	// StopEnv 经 coordinator 停止环境（§14.1 第 6 步；不接管存活环境）。
	StopEnv(ctx context.Context, envID string) (resource.StopResult, error)
	// ReclaimOrphan 回收属于本安装、无对应记录的环境资源（§14.1 扫描表）：在该环境的串行执行者内
	// 停止并逐层销毁；幂等，资源已不存在时返回 nil。
	ReclaimOrphan(ctx context.Context, envID string) error
	// ReleaseCheckedUIDRange 归还清理已完成的环境的 UID 范围，归还前与 cleanup loop 相同地回收 workspace 属主并以
	// provider.ScanUIDFiles 核查残留（M4 Plan 15 D13）：仍有归该范围的文件时隔离该范围并报警（released 为假、无错误）。
	// 分配代次已变化为 persistence.ErrConflict。
	ReleaseCheckedUIDRange(ctx context.Context, ur resource.UIDRange) (released bool, err error)
}

var _ Coordinator = (*resource.Coordinator)(nil)

// Options 配置执行器。
type Options struct {
	// DefaultMemoryBytes 是占用项的内存：任务 limits 没有 memory_max（或无法关联到任务）时使用。
	DefaultMemoryBytes int64
}

// Deps 是执行器的依赖。Clock 与 Jitter 为空时取 time.Now 与均匀随机数。
type Deps struct {
	Store       Store
	Tasks       task.Store
	Resources   resource.Store
	Coordinator Coordinator
	Clock       func() time.Time
	Jitter      func() float64
	Options     Options
}

// StepStatus 是一步的执行结果。
type StepStatus string

const (
	StepDone        StepStatus = "done"         // 本次执行完成
	StepSkipped     StepStatus = "skipped"      // 复核时已在预期状态（或已由其所有者处理），无需写入
	StepStopBlocked StepStatus = "stop_blocked" // 停止未确认：占用容量、阻止替代执行，由 actor 退避重试
	StepQuarantined StepStatus = "quarantined"  // 已写入隔离（计划的隔离步骤，或复核发现事实无法自洽）
	StepAlerted     StepStatus = "alerted"
)

// StepResult 是一步的结果；Detail 说明跳过、阻塞或隔离的原因。
type StepResult struct {
	StepID string
	Kind   reconcile.StepKind
	TaskID string
	EnvID  string
	Status StepStatus
	Detail string
}

// Alert 是一条报警（M1 尚无报警出口，记录在报告中，由装配代码写日志并经 inspect 暴露）。
type Alert struct {
	StepID, TaskID, EnvID string
	Layer, Path, Reason   string
}

// Report 是恢复的就绪报告。
type Report struct {
	// Ready：全部步骤已完成，或已记录为 stop_blocked 或隔离（§14.1"执行就绪条件"中的全局核对）。
	Ready bool
	// Occupied 是 admission.Rebuild 的实际占用：stop_blocked 的环境与被隔离的环境资源各占一个 run slot。
	// 其余环境均已由恢复停止（不接管存活环境），不占用。
	Occupied []admission.Grant
	// StopBlocked 是未能确认停止的环境 ID（按计划顺序）。
	StopBlocked []string
	// Quarantined 是已写入 quarantined_resources 的路径（按计划顺序）。
	Quarantined []string
	// Excluded 是以 record/task/<id> 隔离的任务：装配代码不得为它们启动 actor。
	Excluded []string
	Alerts   []Alert
	Steps    []StepResult
	// Ledger 是本次执行的账本转换结果（§14.1 第 4 步；重跑时已转换的记录不再计入）。
	Ledger LedgerConversion
	// EvictedSessions 是重启驱逐（§12.6、§14.1 第 7 步）的结果：被驱逐的会话与被结束的 incarnation（重跑时为空）。
	EvictedSessions []EvictedSession
}

const (
	classLostOnRestart = "lost_on_restart" // §14.3：故障重试
	recordLayer        = "record"
	recordTaskPrefix   = "record/task/"
	attemptEnded       = "ended"
)

// ReconcileFacts 把 Store 读取的事实转换为 reconcile 的输入（reconcile 不导入 recovery，规则 4）。
func ReconcileFacts(f Facts) reconcile.Facts {
	out := reconcile.Facts{
		PendingIntents:   append([]resource.Intent(nil), f.PendingIntents...),
		UnreleasedRanges: append([]resource.UIDRange(nil), f.UnreleasedRanges...),
	}
	for _, t := range f.Tasks {
		rt := reconcile.TaskFact{TaskID: t.TaskID, Status: t.Status, Desired: t.Desired, ControlVersion: t.ControlVersion,
			NotBefore: t.NotBefore, RunTimePersistedAt: t.RunTimePersistedAt}
		if a := t.CurrentAttempt; a != nil {
			rt.CurrentAttempt = &reconcile.AttemptFact{AttemptID: a.AttemptID, Status: a.Status, EnvID: a.EnvID,
				HasVerdict: a.HasVerdict, ProposalKind: a.ProposalKind}
		}
		out.Tasks = append(out.Tasks, rt)
	}
	for _, e := range f.Environments {
		out.Environments = append(out.Environments, reconcile.EnvFact{EnvID: e.EnvID, AttemptID: e.AttemptID,
			StoppedAt: e.StoppedAt, CleanupState: e.CleanupState})
	}
	return out
}

// Execute 按顺序执行计划。Store 或 coordinator 的错误使执行在该步停止并返回错误（Ready 为假，报告含
// 已完成的部分）；以同一计划重跑会跳过已完成的步骤，不重复提交。
func Execute(ctx context.Context, plan reconcile.RecoveryPlan, d Deps) (Report, error) {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.Jitter == nil {
		d.Jitter = rand.Float64
	}
	x := &executor{d: d, seen: map[string]bool{}, envTask: map[string]string{}, excluded: map[string]bool{}}
	for _, s := range plan.Steps {
		if s.TaskID != "" && s.EnvID != "" {
			x.envTask[s.EnvID] = s.TaskID
		}
	}
	ledger, err := convertLedger(ctx, d)
	if err != nil {
		return x.r, fmt.Errorf("recovery: 账本转换: %w", err)
	}
	x.r.Ledger = ledger
	for _, s := range plan.Steps {
		if err := ctx.Err(); err != nil {
			return x.r, err
		}
		if err := x.step(ctx, s); err != nil {
			return x.r, fmt.Errorf("recovery: 步骤 %s: %w", s.ID, err)
		}
	}
	// 第 7 步（规格 §12.6）：会话的环境（kind = session，含 frozen 的：provider.Stop 直接 kill 冻结的进程并等
	// populated 0）已由计划的 StopEnv 停止；运行中 turn 的 attempt 已按 §14.2 提交 lost_on_restart（故障重试，下一次
	// 授予走冷恢复）。此后单事务驱逐全部未关闭会话、结束遗留 incarnation，最新 session checkpoint 保留。
	evicted, err := d.Store.EvictSessionsOnRestart(ctx)
	if err != nil {
		return x.r, fmt.Errorf("recovery: 重启驱逐会话: %w", err)
	}
	x.r.EvictedSessions = evicted
	occupied, err := x.occupied(ctx)
	if err != nil {
		return x.r, fmt.Errorf("recovery: 计算占用: %w", err)
	}
	x.r.Occupied, x.r.Ready = occupied, true
	return x.r, nil
}

// convertLedger 是 §14.1 第 4 步（账本转换：held → charged_unknown、in_flight → unknown），在任何恢复步骤
// 之前执行：上一进程的在途 try 不会再结算，其预留按无法确认的结果转入 unknown，调用之后可在累计上限与
// deadline_at 内新建 try。resolving 调用的复位由装配在撤销访问之后完成（Store.ResetResolving）。幂等。
func convertLedger(ctx context.Context, d Deps) (LedgerConversion, error) {
	return d.Store.ConvertLedger(ctx)
}

type executor struct {
	d        Deps
	r        Report
	seen     map[string]bool   // 已记录的 StopBlocked/Quarantined/Excluded 项（带前缀去重）
	envTask  map[string]string // 环境 → 任务（由计划的任务步骤得出，用于占用的内存）
	excluded map[string]bool
	occEnvs  []string // 计入占用的环境（按出现顺序）
	occPaths []string // 无环境的隔离项（各计一个占用）
}

func (x *executor) once(key string) bool {
	if x.seen[key] {
		return false
	}
	x.seen[key] = true
	return true
}

func (x *executor) result(s reconcile.Step, st StepStatus, detail string) {
	x.r.Steps = append(x.r.Steps, StepResult{StepID: s.ID, Kind: s.Kind, TaskID: s.TaskID, EnvID: s.EnvID, Status: st, Detail: detail})
}

func (x *executor) occupy(envID string) {
	if envID != "" && x.once("occ:"+envID) {
		x.occEnvs = append(x.occEnvs, envID)
	}
}

func (x *executor) block(s reconcile.Step, envID, detail string) {
	if x.once("blocked:" + envID) {
		x.r.StopBlocked = append(x.r.StopBlocked, envID)
	}
	x.occupy(envID)
	x.result(s, StepStopBlocked, detail)
}

func (x *executor) step(ctx context.Context, s reconcile.Step) error {
	switch s.Kind {
	case reconcile.Quarantine:
		return x.quarantine(ctx, s, s.Layer, s.Path, s.Reason)
	case reconcile.Alert:
		x.r.Alerts = append(x.r.Alerts, Alert{StepID: s.ID, TaskID: s.TaskID, EnvID: s.EnvID, Layer: s.Layer, Path: s.Path, Reason: s.Reason})
		x.result(s, StepAlerted, s.Reason)
		return nil
	case reconcile.StopEnv:
		return x.stopEnv(ctx, s)
	case reconcile.MarkAttemptLost, reconcile.CancelTask, reconcile.PauseTask:
		return x.settle(ctx, s)
	case reconcile.KeepQueued, reconcile.KeepPaused:
		return x.keep(ctx, s)
	case reconcile.KeepTerminal:
		return x.keepTerminal(ctx, s)
	case reconcile.ReclaimOrphan:
		if err := x.d.Coordinator.ReclaimOrphan(ctx, s.EnvID); err != nil {
			return err
		}
		x.result(s, StepDone, "")
		return nil
	case reconcile.ResolveIntent:
		return x.resolveIntent(ctx, s)
	case reconcile.ReleaseUIDRange:
		return x.releaseUIDRange(ctx, s)
	}
	return fmt.Errorf("未知的步骤种类 %q", s.Kind)
}

// quarantine 写入隔离（按路径幂等）并计入占用；以 record/task/<id> 隔离的任务排除在 actor 启动之外。
func (x *executor) quarantine(ctx context.Context, s reconcile.Step, layer, path, reason string) error {
	if layer == "" || path == "" {
		return fmt.Errorf("隔离步骤缺少 Layer 或 Path")
	}
	if err := x.d.Resources.RecordQuarantine(ctx, resource.Quarantine{Layer: layer, Path: path, Reason: reason}); err != nil {
		return err
	}
	if x.once("q:" + path) {
		x.r.Quarantined = append(x.r.Quarantined, path)
		if s.EnvID == "" && layer != recordLayer {
			x.occPaths = append(x.occPaths, path)
		}
	}
	x.occupy(s.EnvID)
	if layer == recordLayer && strings.HasPrefix(path, recordTaskPrefix) {
		id := strings.TrimPrefix(path, recordTaskPrefix)
		if x.once("x:" + id) {
			x.excluded[id] = true
			x.r.Excluded = append(x.r.Excluded, id)
		}
		if s.EnvID != "" {
			x.envTask[s.EnvID] = id
		}
	}
	x.result(s, StepQuarantined, reason)
	return nil
}

// isolateTask 处理复核时发现的无法自洽的任务事实：与计划的隔离相同（记录、报警、排除），不猜测业务状态。
func (x *executor) isolateTask(ctx context.Context, s reconcile.Step, reason string) error {
	x.r.Alerts = append(x.r.Alerts, Alert{StepID: s.ID, TaskID: s.TaskID, EnvID: s.EnvID, Layer: recordLayer,
		Path: recordTaskPrefix + s.TaskID, Reason: reason})
	return x.quarantine(ctx, s, recordLayer, recordTaskPrefix+s.TaskID, reason)
}

// stopEnv：已记录 stopped_at 则跳过；否则经 coordinator 停止。只有 Recorded 才算停止完成；未确认停止、
// 停止已确认但尚未持久化、以及停止本身不确定（provider 错误）都是 stop_blocked。
func (x *executor) stopEnv(ctx context.Context, s reconcile.Step) error {
	env, err := x.d.Resources.GetEnvironment(ctx, s.EnvID)
	switch {
	case err == nil && env.StoppedAt != nil:
		x.result(s, StepSkipped, "已记录 stopped_at")
		return nil
	case err != nil && !errors.Is(err, persistence.ErrNotFound):
		return err
	}
	res, err := x.d.Coordinator.StopEnv(ctx, s.EnvID)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		x.block(s, s.EnvID, "停止无法确认: "+err.Error())
	case res.Blocked:
		x.block(s, s.EnvID, "停止无法确认")
	case !res.Recorded:
		x.block(s, s.EnvID, "停止已确认但 stopped_at 尚未持久化")
	default:
		x.result(s, StepDone, "")
	}
	return nil
}

// settle 执行 MarkAttemptLost、CancelTask、PauseTask。有未结束 attempt 时：确认其环境已记录 stopped_at，
// 补记运行时限未记账区间（§14.4），再以 AttemptFinished{lost_on_restart} 交给 task.Decide，提交其
// 给出的判决（desired 决定 cancelled、paused 或按故障恢复策略排队）。没有未结束 attempt 时：以 Tick
// 交给 task.Decide，只执行它给出的 ApplyControl（已被接受的 cancel/pause 的状态转换）。
func (x *executor) settle(ctx context.Context, s reconcile.Step) error {
	if x.excluded[s.TaskID] {
		x.result(s, StepSkipped, "任务已隔离")
		return nil
	}
	ts, err := x.d.Tasks.LoadTask(ctx, s.TaskID)
	if err != nil {
		return err
	}
	if s.EnvID == "" { // 计划认定没有未结束 attempt
		return x.applyAccepted(ctx, s, ts)
	}
	if ts.CurrentAttemptID != s.AttemptID {
		return x.isolateTask(ctx, s, "当前 attempt 已不是 "+s.AttemptID)
	}
	att, err := x.d.Tasks.GetAttempt(ctx, s.AttemptID)
	if err != nil {
		return err
	}
	if att.Status == attemptEnded {
		x.result(s, StepSkipped, "判决已提交")
		return nil
	}
	env, err := x.d.Resources.GetEnvironment(ctx, s.EnvID)
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return x.isolateTask(ctx, s, "attempt 的环境记录 "+s.EnvID+" 不存在")
	case err != nil:
		return err
	case env.StoppedAt == nil: // 停止未确认：不提交 lost，保持 stop_blocked
		x.block(s, s.EnvID, "环境未确认停止，不提交判决")
		return nil
	}
	if err := x.d.Store.AccountUnrecordedRunTime(ctx, s.TaskID, s.AttemptID, *env.StoppedAt); err != nil {
		return err
	}
	st := x.state(ts, &task.AttemptState{AttemptID: att.AttemptID, EnvID: att.EnvID, Status: "active",
		StoredStatus: att.Status, EnvStopped: true, StopRequested: true})
	dec, err := task.Decide(st, task.AttemptFinished{AttemptID: att.AttemptID, EnvID: att.EnvID,
		Outcome: task.Outcome{Class: classLostOnRestart, Retry: task.RetryFault}})
	if errors.Is(err, task.ErrInvalid) {
		return x.isolateTask(ctx, s, err.Error())
	}
	if err != nil {
		return err
	}
	for _, e := range dec.Effects {
		if f, ok := e.(task.Finalize); ok {
			if _, err := x.d.Tasks.FinalizeAttempt(ctx, f.Verdict); err != nil {
				return err
			}
			x.result(s, StepDone, f.Verdict.TaskStatus)
			return nil
		}
	}
	return fmt.Errorf("task.Decide 未给出判决")
}

// applyAccepted 处理没有未结束 attempt 的 cancel/pause：已在预期状态则跳过，否则执行 Decide 给出的控制转换。
func (x *executor) applyAccepted(ctx context.Context, s reconcile.Step, ts task.TaskState) error {
	if ts.Status == s.Expect {
		x.result(s, StepSkipped, "已处于 "+s.Expect)
		return nil
	}
	var att *task.AttemptState
	if ts.CurrentAttemptID != "" {
		a, err := x.d.Tasks.GetAttempt(ctx, ts.CurrentAttemptID)
		if err != nil {
			return err
		}
		att = &task.AttemptState{AttemptID: a.AttemptID, EnvID: a.EnvID, Status: a.Status, StoredStatus: a.Status, EnvStopped: true}
	}
	dec, err := task.Decide(x.state(ts, att), task.Tick{})
	if errors.Is(err, task.ErrInvalid) {
		return x.isolateTask(ctx, s, err.Error())
	}
	if err != nil {
		return err
	}
	for _, e := range dec.Effects {
		if c, ok := e.(task.ApplyControl); ok && c.Status == s.Expect {
			if _, err := x.d.Tasks.ApplyControl(ctx, c); err != nil {
				return err
			}
			x.result(s, StepDone, c.Status)
			return nil
		}
	}
	return x.isolateTask(ctx, s, fmt.Sprintf("任务处于 %s（desired = %s），无法到达 %s", ts.Status, ts.Desired, s.Expect))
}

// keep 复核 KeepQueued/KeepPaused：只读，不写入（计数、not_before 与恢复点保持不变）。
func (x *executor) keep(ctx context.Context, s reconcile.Step) error {
	if x.excluded[s.TaskID] {
		x.result(s, StepSkipped, "任务已隔离")
		return nil
	}
	ts, err := x.d.Tasks.LoadTask(ctx, s.TaskID)
	if err != nil {
		return err
	}
	if ts.Status != s.Expect {
		return x.isolateTask(ctx, s, fmt.Sprintf("预期 %s，实际 %s", s.Expect, ts.Status))
	}
	x.result(s, StepSkipped, s.Reason)
	return nil
}

// keepTerminal 复核环境所属 attempt 的裁决已提交（残留由 cleanup loop 处理）；否则隔离该 attempt 记录。
func (x *executor) keepTerminal(ctx context.Context, s reconcile.Step) error {
	att, err := x.d.Tasks.GetAttempt(ctx, s.AttemptID)
	reason := ""
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		reason = "环境 " + s.EnvID + " 所属的 attempt 不存在"
	case err != nil:
		return err
	case att.Status != attemptEnded:
		reason = "attempt 不是非终态任务的当前 attempt，但裁决未提交"
	default:
		x.result(s, StepSkipped, "裁决已提交")
		return nil
	}
	path := recordLayer + "/attempt/" + s.AttemptID
	x.r.Alerts = append(x.r.Alerts, Alert{StepID: s.ID, EnvID: s.EnvID, Layer: recordLayer, Path: path, Reason: reason})
	return x.quarantine(ctx, s, recordLayer, path, reason)
}

// resolveIntent：已在目标状态则跳过；环境仍有未完成的清理时交给 cleanup loop（它在 Destroy 后结束 intent）。
func (x *executor) resolveIntent(ctx context.Context, s reconcile.Step) error {
	in, err := x.d.Resources.GetIntent(ctx, s.IntentID)
	if err != nil {
		return err
	}
	if in.State == s.Expect {
		x.result(s, StepSkipped, "已处于 "+s.Expect)
		return nil
	}
	env, err := x.d.Resources.GetEnvironment(ctx, s.EnvID)
	switch {
	case err == nil && env.CleanupState != resource.CleanupDone:
		x.result(s, StepSkipped, "环境清理未完成，由 cleanup loop 结束 intent")
		return nil
	case err != nil && !errors.Is(err, persistence.ErrNotFound):
		return err
	}
	if _, err := x.d.Resources.ResolveIntent(ctx, s.IntentID, s.Expect); err != nil {
		return err
	}
	x.result(s, StepDone, s.Expect)
	return nil
}

// releaseUIDRange：范围已不属于该环境或该分配代次（已归还、已隔离或已被复用）则跳过；否则经 coordinator 回收、
// 核查并归还（M4 Plan 15 D13）：仍有归该范围的文件时该范围已被隔离并报警（不归还、不计入占用）。
func (x *executor) releaseUIDRange(ctx context.Context, s reconcile.Step) error {
	ur, err := x.d.Resources.GetUIDRange(ctx, s.EnvID)
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		x.result(s, StepSkipped, "环境没有 UID 范围")
		return nil
	case err != nil:
		return err
	case ur.UIDRangeID != s.UIDRangeID || ur.AllocationID != s.AllocationID || ur.State != "assigned":
		x.result(s, StepSkipped, "UID 范围已归还")
		return nil
	}
	released, err := x.d.Coordinator.ReleaseCheckedUIDRange(ctx, ur)
	switch {
	case errors.Is(err, persistence.ErrConflict):
		x.result(s, StepSkipped, "UID 范围已被后来的分配复用")
		return nil
	case err != nil:
		return err
	case !released:
		path := resource.UIDRangeQuarantinePath(ur.UIDRangeID)
		if x.once("q:" + path) {
			x.r.Quarantined = append(x.r.Quarantined, path)
		}
		x.result(s, StepQuarantined, "UID 范围仍拥有数据目录中的文件，已隔离并报警")
		return nil
	}
	x.result(s, StepDone, "")
	return nil
}

// state 把 LoadTask 的事实转换为 Decide 的输入。
func (x *executor) state(ts task.TaskState, a *task.AttemptState) task.State {
	return task.State{TaskID: ts.TaskID, TaskStatus: ts.Status, Desired: ts.Desired,
		ControlVersion: ts.ControlVersion, AppliedControlVersion: ts.AppliedControlVersion, Attempt: a,
		FaultRetriesUsed: ts.FaultRetriesUsed, MaxFaultRetries: ts.MaxFaultRetries, OOMRetriesUsed: ts.OOMRetriesUsed,
		NotBefore: ts.NotBefore, Now: x.d.Clock(), Jitter: x.d.Jitter()}
}

// occupied 为 stop_blocked 与被隔离的环境（各一个 run slot）以及无环境的隔离项生成占用；内存取所属任务
// limits 的 memory_max，没有则取 DefaultMemoryBytes。ID 从 1 起按出现顺序分配。
func (x *executor) occupied(ctx context.Context) ([]admission.Grant, error) {
	var out []admission.Grant
	add := func(taskID string, mem int64) {
		out = append(out, admission.Grant{ID: uint64(len(out) + 1), TaskID: taskID, MemoryBytes: mem})
	}
	for _, envID := range x.occEnvs {
		taskID := x.envTask[envID]
		mem, err := x.memory(ctx, taskID)
		if err != nil {
			return nil, err
		}
		add(taskID, mem)
	}
	for range x.occPaths {
		add("", x.d.Options.DefaultMemoryBytes)
	}
	return out, nil
}

func (x *executor) memory(ctx context.Context, taskID string) (int64, error) {
	if taskID == "" {
		return x.d.Options.DefaultMemoryBytes, nil
	}
	ts, err := x.d.Tasks.LoadTask(ctx, taskID)
	if errors.Is(err, persistence.ErrNotFound) {
		return x.d.Options.DefaultMemoryBytes, nil
	}
	if err != nil {
		return 0, err
	}
	var l struct {
		MemoryMax int64 `json:"memory_max"`
	}
	if len(ts.Limits) > 0 && json.Unmarshal(ts.Limits, &l) == nil && l.MemoryMax > 0 {
		return l.MemoryMax, nil
	}
	return x.d.Options.DefaultMemoryBytes, nil
}
