// Package reconcile 把启动时的数据库事实与 provider 的独立原始扫描转换为 RecoveryPlan（规格 §14.1 第 5 步、
// §14.2；代码组织设计 §6.1）。本包只生成计划：不调用 actor、不执行、不读写 Store；执行由 recovery 完成，
// 它在每步前重新检查条件。依赖规则 4：不依赖 task、session、recovery——因此本包声明自己的事实类型
// （与 recovery.Facts 逐字段对应），由 recovery 转换后传入。
//
// 计划是确定的：输入在内部按对象 ID 排序，步骤 ID 只由步骤种类与对象 ID 派生，同一事实总生成同一计划。
// 计划不含进程句柄。遇到无法自洽的事实组合时只生成隔离与报警步骤，不猜测业务状态。
package reconcile

import (
	"sort"
	"strings"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/resource"
)

// Facts 是恢复所需的数据库事实；与 recovery.Facts 逐字段对应（规则 4 禁止导入 recovery）。
type Facts struct {
	Tasks            []TaskFact          // 非终态任务
	Environments     []EnvFact           // 未停止或清理未完成的环境
	PendingIntents   []resource.Intent   // pending 或 acquired 的资源意图
	UnreleasedRanges []resource.UIDRange // 所属环境已清理完成但仍为 assigned 的 UID 范围
}

// TaskFact 是一个非终态任务的事实（对应 recovery.TaskFact）。
type TaskFact struct {
	TaskID, Status, Desired string
	ControlVersion          int64
	CurrentAttempt          *AttemptFact
	NotBefore               *time.Time
	RunTimePersistedAt      *time.Time
}

// AttemptFact 是任务当前 attempt 的事实（对应 recovery.AttemptFact）。
type AttemptFact struct {
	AttemptID, Status, EnvID string
	HasVerdict               bool
	ProposalKind             string
}

// EnvFact 是一个环境的事实（对应 recovery.EnvFact）。
type EnvFact struct {
	EnvID, AttemptID string
	StoppedAt        *time.Time
	CleanupState     string
}

// StepKind 是恢复步骤的种类（封闭集合）。取值同时是步骤 ID 的前缀。
type StepKind string

const (
	// StopEnv：经 coordinator 停止环境并记录 stopped_at（§14.1 第 6 步；不接管存活环境）。Expect = "stopped"。
	StopEnv StepKind = "stop_env"
	// MarkAttemptLost：desired = run 且有本次重启丢失的 attempt——确认 EnvID 已停止后以 lost_on_restart 交给
	// task.Decide，按故障恢复策略排队（§14.2 第 4 行）。Expect = "lost"。
	MarkAttemptLost StepKind = "mark_attempt_lost"
	// CancelTask：cancel 已接受——有未结束 attempt 时先确认 EnvID 已停止，再 → cancelled，不重试（§14.2 第 1 行）。
	CancelTask StepKind = "cancel_task"
	// PauseTask：pause 已接受——有未结束 attempt 时先确认停止，→ paused，保留恢复点（§14.2 第 2 行）。
	PauseTask StepKind = "pause_task"
	// KeepQueued：desired = run 且无未结束 attempt——保持排队，保留计数与 not_before（§14.2 第 3 行）；
	// 裁决已安排故障重试时不再计数（第 5 行）。Expect = "queued"。
	KeepQueued StepKind = "keep_queued"
	// KeepPaused：已暂停——保持 paused 与恢复点。desired = run（resume 已接受未应用）同样保持：恢复不应用
	// 控制，由 actor 启动后应用。Expect = "paused"。
	KeepPaused StepKind = "keep_paused"
	// KeepTerminal：环境所属 attempt 不是任何非终态任务的当前 attempt——其裁决已提交，保留终态，只处理残留
	// 资源（§14.2 第 6 行）。Expect = "verdict_committed"，执行器据此复核。
	KeepTerminal StepKind = "keep_terminal"
	// ReclaimOrphan：属于本安装、无对应记录的环境资源，作为孤立资源回收（§14.1 扫描表）。Expect = "destroyed"。
	ReclaimOrphan StepKind = "reclaim_orphan"
	// Quarantine：归属不明或冲突的资源（Layer/Path），或无法自洽的事实（TaskID/EnvID 等）：写入隔离、
	// 不自动销毁、计入占用（§14.1 扫描表）。Expect = "quarantined"。
	Quarantine StepKind = "quarantine"
	// Alert：与每个 Quarantine 配对的报警。Expect = "alerted"。
	Alert StepKind = "alert"
	// ResolveIntent：环境已清理完成且扫描确认无残留时结束未结束的 intent（§8.3）；Expect 是目标状态
	// （pending → failed，acquired → released）。
	ResolveIntent StepKind = "resolve_intent"
	// ReleaseUIDRange：清理已完成但 UID 范围仍为 assigned（coordinator 归还失败后的重试只在内存中，
	// 重启由恢复补齐）。Expect = "free"。
	ReleaseUIDRange StepKind = "release_uid_range"
)

// Step 是计划中的一步。ID 稳定：由 Kind 与对象 ID 派生，同一事实重复生成相同 ID，执行器据此幂等。
// 只填写与种类相关的字段；不含进程句柄。
type Step struct {
	ID                       string
	Kind                     StepKind
	TaskID, AttemptID, EnvID string
	Expect                   string // 执行后应达到（并复核）的状态
	Reason                   string // 隔离、报警与保持类步骤的原因
	Layer, Path              string // Quarantine/Alert：扫描项，或 "record" 与 "record/<类别>/<ID>"（非空）
	IntentID                 string // ResolveIntent，或被隔离的 intent
	UIDRangeID, AllocationID string // ReleaseUIDRange，或被隔离的 UID 范围
}

// RecoveryPlan 是有序的恢复步骤。顺序：隔离与报警 → 停止环境 → 任务与 attempt 的处置 → 回收孤立资源 →
// 结束 intent → 归还 UID 范围；依赖关系（例如先确认停止再提交 lost）由执行器按 EnvID 复核。
type RecoveryPlan struct {
	Steps []Step
}

// 任务、attempt、控制与清理状态的取值（与 task、persistence 的存储取值一致；规则 4 禁止导入 task）。
const (
	taskQueued     = "queued"
	taskRunning    = "running"
	taskPausing    = "pausing"
	taskCancelling = "cancelling"
	taskPaused     = "paused"

	desiredRun    = "run"
	desiredPause  = "pause"
	desiredCancel = "cancel"

	attemptEnded = "ended"
	uidAssigned  = "assigned"

	recordLayer = "record" // 无物理资源的隔离项的 Layer 与 Path 前缀
)

// openAttemptStatus 是未结束 attempt 的全部状态（§8.2；stop_blocked 为非终态）。
var openAttemptStatus = map[string]bool{
	"created": true, "starting": true, "handshaking": true, "active": true, "finishing": true, "stop_blocked": true,
}

var taskStatus = map[string]bool{taskQueued: true, taskRunning: true, taskPausing: true, taskCancelling: true, taskPaused: true}

var cleanupState = map[string]bool{resource.CleanupNone: true, resource.CleanupPending: true, resource.CleanupDone: true}

// Plan 是纯函数：由数据库事实与独立原始扫描生成恢复计划。scan 的归属分类（ScanItem.Owner）已由 provider
// 相对 installID 判定；installID 只标识计划针对的安装，本函数不据此重新分类。
func Plan(f Facts, scan provider.ScanReport, installID string) RecoveryPlan {
	_ = installID
	f, items := normalize(f, scan)
	p := &planner{seen: map[string]bool{}}

	envs := make(map[string]EnvFact, len(f.Environments))
	for _, e := range f.Environments {
		envs[e.EnvID] = e
	}
	current := map[string]bool{} // 非终态任务的当前 attempt
	refs := map[string]bool{}    // 有记录引用的环境（含已清理完成的）
	for _, t := range f.Tasks {
		if a := t.CurrentAttempt; a != nil {
			current[a.AttemptID] = true
			refs[a.EnvID] = true
		}
	}
	for _, i := range f.PendingIntents {
		refs[i.EnvID] = true
	}
	for _, r := range f.UnreleasedRanges {
		refs[r.OwnerID] = true
	}

	quarantined := p.scan(items, envs, refs)
	for _, t := range f.Tasks {
		p.task(t, envs)
	}
	for _, e := range f.Environments {
		p.env(e, current)
	}
	for _, i := range f.PendingIntents {
		p.intent(i, envs, quarantined)
	}
	for _, r := range f.UnreleasedRanges {
		p.uidRange(r, envs, quarantined)
	}
	return p.plan()
}

// normalize 复制并按对象 ID 排序输入，使计划与输入顺序无关。
func normalize(f Facts, scan provider.ScanReport) (Facts, []provider.ScanItem) {
	out := Facts{
		Tasks:            append([]TaskFact(nil), f.Tasks...),
		Environments:     append([]EnvFact(nil), f.Environments...),
		PendingIntents:   append([]resource.Intent(nil), f.PendingIntents...),
		UnreleasedRanges: append([]resource.UIDRange(nil), f.UnreleasedRanges...),
	}
	sort.SliceStable(out.Tasks, func(i, j int) bool { return out.Tasks[i].TaskID < out.Tasks[j].TaskID })
	sort.SliceStable(out.Environments, func(i, j int) bool { return out.Environments[i].EnvID < out.Environments[j].EnvID })
	sort.SliceStable(out.PendingIntents, func(i, j int) bool { return out.PendingIntents[i].IntentID < out.PendingIntents[j].IntentID })
	sort.SliceStable(out.UnreleasedRanges, func(i, j int) bool {
		return out.UnreleasedRanges[i].UIDRangeID < out.UnreleasedRanges[j].UIDRangeID
	})
	items := append([]provider.ScanItem(nil), scan.Items...)
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.EnvID != b.EnvID {
			return a.EnvID < b.EnvID
		}
		if a.Layer != b.Layer {
			return a.Layer < b.Layer
		}
		return a.Path < b.Path
	})
	return out, items
}

// 计划的阶段；步骤按阶段输出，阶段内按输入排序后的顺序。
const (
	phaseQuarantine = iota
	phaseStop
	phaseTask
	phaseReclaim
	phaseIntent
	phaseRelease
	phaseCount
)

type planner struct {
	phases [phaseCount][]Step
	seen   map[string]bool
}

func id(kind StepKind, parts ...string) string {
	return string(kind) + ":" + strings.Join(parts, ":")
}

// add 追加一步；同一 ID 只出现一次（例如任务与环境两处都要求停止同一环境）。
func (p *planner) add(phase int, s Step) {
	if p.seen[s.ID] {
		return
	}
	p.seen[s.ID] = true
	p.phases[phase] = append(p.phases[phase], s)
}

func (p *planner) plan() RecoveryPlan {
	var steps []Step
	for _, ph := range p.phases {
		steps = append(steps, ph...)
	}
	return RecoveryPlan{Steps: steps}
}

// isolate 生成一对隔离与报警步骤；obj 是被隔离对象的类别与 ID（用于步骤 ID）。没有物理资源的隔离
// （任务、环境记录、intent、UID 范围的不一致）以 Layer = "record"、Path = "record/<类别>/<ID>" 标识，
// 使执行器能以路径为键写入 quarantined_resources 并报警，并把被隔离的任务排除在 actor 启动之外。
func (p *planner) isolate(s Step, obj ...string) {
	if s.Layer == "" {
		s.Layer, s.Path = recordLayer, recordLayer+"/"+strings.Join(obj, "/")
	}
	q, a := s, s
	q.Kind, q.ID, q.Expect = Quarantine, id(Quarantine, obj...), "quarantined"
	a.Kind, a.ID, a.Expect = Alert, id(Alert, obj...), "alerted"
	p.add(phaseQuarantine, q)
	p.add(phaseQuarantine, a)
}

func (p *planner) stop(envID string) {
	p.add(phaseStop, Step{ID: id(StopEnv, envID), Kind: StopEnv, EnvID: envID, Expect: "stopped"})
}

// scan 按 §14.1 扫描表处理独立原始扫描，返回被隔离的环境 ID。按环境分组判断：
//   - 属于其他安装 → 不操作；
//   - 归属不明，或同一环境同时有本安装与其他安装的资源（归属冲突）→ 隔离该环境非外部的全部资源；
//   - 属于本安装、环境仍有未完成记录 → 无步骤（由停止与清理处理）；
//   - 属于本安装、记录显示环境已清理完成（被 attempt、intent 或 UID 范围引用但不在未完成环境中）→
//     记录与实际冲突，隔离；
//   - 属于本安装、无任何记录 → 作为孤立资源回收。
func (p *planner) scan(items []provider.ScanItem, envs map[string]EnvFact, refs map[string]bool) map[string]bool {
	quarantined := map[string]bool{}
	isolateItems := func(group []provider.ScanItem, reason string) {
		for _, it := range group {
			if it.Owner == provider.Foreign {
				continue
			}
			p.isolate(Step{EnvID: it.EnvID, Layer: it.Layer, Path: it.Path, Reason: reason}, "scan", it.Layer, it.Path)
		}
	}
	for start := 0; start < len(items); {
		end := start
		for end < len(items) && items[end].EnvID == items[start].EnvID {
			end++
		}
		group, envID := items[start:end], items[start].EnvID
		start = end

		var owned, foreign, unknown int
		for _, it := range group {
			switch it.Owner {
			case provider.OwnedComplete, provider.OwnedPartial:
				owned++
			case provider.Foreign:
				foreign++
			default: // Unknown 或无效分类：不猜测
				unknown++
			}
		}
		_, live := envs[envID]
		switch {
		case envID == "":
			isolateItems(group, "无法识别 env_id 的资源")
			continue
		case unknown > 0:
			isolateItems(group, "环境含归属无法判定的资源")
		case owned > 0 && foreign > 0:
			isolateItems(group, "同一环境的资源归属冲突")
		case owned == 0 || live:
			continue
		case refs[envID]:
			isolateItems(group, "记录显示环境已清理完成，但仍有本安装的资源")
		default:
			p.add(phaseReclaim, Step{ID: id(ReclaimOrphan, envID), Kind: ReclaimOrphan, EnvID: envID, Expect: "destroyed",
				Reason: "属于本安装、无对应记录"})
			continue
		}
		quarantined[envID] = true
	}
	return quarantined
}

// task 按 §14.2 处理一个非终态任务。cancel 优先于 pause；有未结束 attempt 时先停止其环境
// （环境已无未完成记录即已停止且清理完成，不再停止）。
func (p *planner) task(t TaskFact, envs map[string]EnvFact) {
	a := t.CurrentAttempt
	if reason := inconsistency(t, envs); reason != "" {
		s := Step{TaskID: t.TaskID, Reason: reason}
		if a != nil {
			s.AttemptID, s.EnvID = a.AttemptID, a.EnvID
		}
		p.isolate(s, "task", t.TaskID)
		return
	}
	open := a != nil && a.Status != attemptEnded
	s := Step{TaskID: t.TaskID}
	parts := []string{t.TaskID}
	if a != nil {
		s.AttemptID = a.AttemptID
	}
	if open {
		s.EnvID = a.EnvID
		parts = append(parts, a.AttemptID)
		if e, ok := envs[a.EnvID]; ok && e.StoppedAt == nil {
			p.stop(a.EnvID)
		}
	}
	switch {
	case t.Desired == desiredCancel:
		s.Kind, s.Expect = CancelTask, "cancelled"
	case t.Desired == desiredPause && (open || t.Status == taskQueued):
		s.Kind, s.Expect = PauseTask, taskPaused
	case t.Desired == desiredPause:
		s.Kind, s.Expect = KeepPaused, taskPaused
	case open: // desired = run
		s.Kind, s.Expect, s.Reason = MarkAttemptLost, "lost", "lost_on_restart"
	case t.Status == taskPaused: // desired = run：resume 已接受未应用；恢复不应用控制，由 actor 启动后应用
		s.Kind, s.Expect, s.Reason = KeepPaused, taskPaused, "resume 已接受，由 actor 启动后应用"
	default: // desired = run，无未结束 attempt
		s.Kind, s.Expect = KeepQueued, taskQueued
		switch {
		case a != nil: // 裁决已提交且安排了重试
			s.Reason = "已安排重试，不再计数"
		default:
			s.Reason = "无未结束 attempt"
		}
	}
	s.ID = id(s.Kind, parts...)
	p.add(phaseTask, s)
}

// inconsistency 返回任务事实无法自洽的原因；空表示一致。
func inconsistency(t TaskFact, envs map[string]EnvFact) string {
	executing := t.Status == taskRunning || t.Status == taskPausing || t.Status == taskCancelling
	switch {
	case t.TaskID == "":
		return "任务缺少 task_id"
	case !taskStatus[t.Status]:
		return "未知的任务状态 " + t.Status
	case t.Desired != desiredRun && t.Desired != desiredPause && t.Desired != desiredCancel:
		return "未知的 desired " + t.Desired
	case t.Status == taskCancelling && t.Desired != desiredCancel:
		return "cancelling 但 desired 不是 cancel"
	case t.Status == taskPausing && t.Desired == desiredRun:
		return "pausing 但 desired 是 run"
	}
	a := t.CurrentAttempt
	if a == nil {
		if executing {
			return "任务处于 " + t.Status + " 但没有当前 attempt"
		}
		return ""
	}
	open := openAttemptStatus[a.Status]
	switch {
	case a.AttemptID == "" || a.EnvID == "":
		return "当前 attempt 缺少 attempt_id 或 env_id"
	case !open && a.Status != attemptEnded:
		return "未知的 attempt 状态 " + a.Status
	case a.HasVerdict && open:
		return "裁决已提交但当前 attempt 未结束"
	case !a.HasVerdict && !open:
		return "attempt 已结束但没有裁决"
	case open && !executing:
		return "任务处于 " + t.Status + " 但当前 attempt 未结束"
	case a.HasVerdict && executing:
		return "裁决已提交但任务仍处于 " + t.Status
	}
	if e, ok := envs[a.EnvID]; ok && e.AttemptID != "" && e.AttemptID != a.AttemptID {
		return "环境 " + a.EnvID + " 的记录属于另一个 attempt " + e.AttemptID
	}
	return ""
}

// env 处理一个未完成的环境：未停止的一律停止（不接管存活环境）；所属 attempt 不是任何非终态任务的当前
// attempt 时，其裁决已提交，保留终态，只处理残留（§14.2 第 6 行；清理由 cleanup loop 完成）。
func (p *planner) env(e EnvFact, current map[string]bool) {
	if !cleanupState[e.CleanupState] || (e.StoppedAt == nil && e.CleanupState != resource.CleanupNone) {
		p.isolate(Step{EnvID: e.EnvID, AttemptID: e.AttemptID,
			Reason: "清理状态 " + e.CleanupState + " 与停止记录不一致"}, "env", e.EnvID)
	}
	if e.StoppedAt == nil {
		p.stop(e.EnvID) // 隔离也停止：停止不是销毁，且重启后没有所有者接管存活环境
	}
	if e.AttemptID != "" && !current[e.AttemptID] {
		p.add(phaseTask, Step{ID: id(KeepTerminal, e.AttemptID), Kind: KeepTerminal, AttemptID: e.AttemptID, EnvID: e.EnvID,
			Expect: "verdict_committed", Reason: "attempt 不是非终态任务的当前 attempt，只处理残留资源"})
	}
}

// intent 按 §8.3 处理一个未结束的资源意图：环境仍有未完成记录时由停止与清理结束它（清理在 Destroy 逐层
// 核对后结束 intent）；环境已清理完成且扫描未发现任何非外部资源时结束它（pending → failed，
// acquired → released）；发现残留时该环境已被隔离，intent 保持未结束、计入占用。
func (p *planner) intent(i resource.Intent, envs map[string]EnvFact, quarantined map[string]bool) {
	var target string
	switch i.State {
	case resource.IntentPending:
		target = resource.IntentFailed
	case resource.IntentAcquired:
		target = resource.IntentReleased
	}
	if target == "" || i.IntentID == "" || i.EnvID == "" {
		p.isolate(Step{EnvID: i.EnvID, IntentID: i.IntentID, Reason: "intent 状态 " + i.State + " 或身份无效"},
			"intent", i.IntentID)
		return
	}
	if _, live := envs[i.EnvID]; live || quarantined[i.EnvID] {
		return
	}
	p.add(phaseIntent, Step{ID: id(ResolveIntent, i.IntentID), Kind: ResolveIntent, EnvID: i.EnvID, IntentID: i.IntentID,
		Expect: target, Reason: "环境已清理完成，独立扫描未发现残留"})
}

// uidRange 为清理已完成但仍为 assigned 的 UID 范围生成归还步骤；所属环境被隔离（仍有残留）时不归还。
func (p *planner) uidRange(r resource.UIDRange, envs map[string]EnvFact, quarantined map[string]bool) {
	_, live := envs[r.OwnerID]
	if r.State != uidAssigned || r.UIDRangeID == "" || r.OwnerID == "" || live {
		p.isolate(Step{EnvID: r.OwnerID, UIDRangeID: r.UIDRangeID, AllocationID: r.AllocationID,
			Reason: "UID 范围状态 " + r.State + " 与环境记录不一致"}, "uid_range", r.UIDRangeID)
		return
	}
	if quarantined[r.OwnerID] {
		return
	}
	p.add(phaseRelease, Step{ID: id(ReleaseUIDRange, r.UIDRangeID, r.AllocationID), Kind: ReleaseUIDRange,
		EnvID: r.OwnerID, UIDRangeID: r.UIDRangeID, AllocationID: r.AllocationID, Expect: "free"})
}
