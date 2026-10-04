package reconcile

import (
	"reflect"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/resource"
)

const install = "inst-1"

var stoppedAt = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func task(id, status, desired string, a *AttemptFact) TaskFact {
	return TaskFact{TaskID: id, Status: status, Desired: desired, ControlVersion: 1, CurrentAttempt: a}
}

func openAttempt(id, env, status string) *AttemptFact {
	return &AttemptFact{AttemptID: id, EnvID: env, Status: status}
}

func endedAttempt(id, env string) *AttemptFact {
	return &AttemptFact{AttemptID: id, EnvID: env, Status: "ended", HasVerdict: true}
}

func liveEnv(id, attempt string) EnvFact {
	return EnvFact{EnvID: id, AttemptID: attempt, CleanupState: resource.CleanupNone}
}

func stoppedEnv(id, attempt string) EnvFact {
	return EnvFact{EnvID: id, AttemptID: attempt, StoppedAt: &stoppedAt, CleanupState: resource.CleanupPending}
}

func item(env, layer string, owner provider.Owner) provider.ScanItem {
	return provider.ScanItem{Layer: layer, Path: "/var/lib/agentbox/" + layer + "/" + env, EnvID: env, Owner: owner}
}

func ids(p RecoveryPlan) []string {
	out := []string{}
	for _, s := range p.Steps {
		out = append(out, s.ID)
	}
	return out
}

func step(t *testing.T, p RecoveryPlan, id string) Step {
	t.Helper()
	for _, s := range p.Steps {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("计划中没有步骤 %s：%v", id, ids(p))
	return Step{}
}

// TestPlan 表驱动覆盖 §14.2 每行、§14.1 扫描表每行、pending 与 acquired intent、未归还的 UID 范围，
// 以及无法自洽的事实组合（只隔离与报警，不猜测）。want 是按顺序的完整步骤 ID 列表。
func TestPlan(t *testing.T) {
	scanPath := func(env, layer string) string { return "/var/lib/agentbox/" + layer + "/" + env }
	cases := []struct {
		name  string
		facts Facts
		scan  []provider.ScanItem
		want  []string
		check func(t *testing.T, p RecoveryPlan)
	}{
		// ---- §14.2 ----
		{
			name: "14.2 cancel 已接受：停止旧环境 → cancelled，不重试",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "cancelling", "cancel", openAttempt("att-1", "env-1", "active"))},
				Environments: []EnvFact{liveEnv("env-1", "att-1")},
			},
			want: []string{"stop_env:env-1", "cancel_task:t1:att-1"},
			check: func(t *testing.T, p RecoveryPlan) {
				if s := step(t, p, "cancel_task:t1:att-1"); s.EnvID != "env-1" || s.Expect != "cancelled" {
					t.Fatalf("cancel 步骤应以 env-1 停止为前提：%+v", s)
				}
			},
		},
		{
			name:  "14.2 cancel 已接受、无执行：直接 cancelled",
			facts: Facts{Tasks: []TaskFact{task("t1", "queued", "cancel", nil)}},
			want:  []string{"cancel_task:t1"},
		},
		{
			name: "14.2 pause 已接受：停止并清理 → paused",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "pausing", "pause", openAttempt("att-1", "env-1", "finishing"))},
				Environments: []EnvFact{liveEnv("env-1", "att-1")},
			},
			want: []string{"stop_env:env-1", "pause_task:t1:att-1"},
		},
		{
			name:  "14.2 pause 已接受、排队中：→ paused",
			facts: Facts{Tasks: []TaskFact{task("t1", "queued", "pause", nil)}},
			want:  []string{"pause_task:t1"},
		},
		{
			name:  "14.2 已暂停：保持 paused",
			facts: Facts{Tasks: []TaskFact{task("t1", "paused", "pause", endedAttempt("att-1", "env-1"))}},
			want:  []string{"keep_paused:t1"},
		},
		{
			name:  "14.2 desired=run 无未完成 attempt：保持排队",
			facts: Facts{Tasks: []TaskFact{task("t1", "queued", "run", nil)}},
			want:  []string{"keep_queued:t1"},
		},
		{
			name: "14.2 desired=run 有丢失 attempt：确认停止后按故障恢复排队",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "running", "run", openAttempt("att-1", "env-1", "active"))},
				Environments: []EnvFact{liveEnv("env-1", "att-1")},
			},
			want: []string{"stop_env:env-1", "mark_attempt_lost:t1:att-1"},
			check: func(t *testing.T, p RecoveryPlan) {
				if s := step(t, p, "mark_attempt_lost:t1:att-1"); s.EnvID != "env-1" || s.AttemptID != "att-1" || s.Expect != "lost" {
					t.Fatalf("lost 步骤：%+v", s)
				}
			},
		},
		{
			name: "14.2 丢失 attempt 的环境已记录停止：不再停止",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "running", "run", openAttempt("att-1", "env-1", "stop_blocked"))},
				Environments: []EnvFact{stoppedEnv("env-1", "att-1")},
			},
			want: []string{"mark_attempt_lost:t1:att-1"},
		},
		{
			name: "14.2 已完成故障分类并安排重试：保持排队、不再计数",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "queued", "run", endedAttempt("att-1", "env-1"))},
				Environments: []EnvFact{stoppedEnv("env-1", "att-1")},
			},
			want: []string{"keep_queued:t1"},
			check: func(t *testing.T, p RecoveryPlan) {
				if s := step(t, p, "keep_queued:t1"); s.Reason != "已安排重试，不再计数" {
					t.Fatalf("应标明不再计数：%+v", s)
				}
			},
		},
		{
			name:  "14.2 裁决已提交（任务终态）：保留终态，只处理残留资源",
			facts: Facts{Environments: []EnvFact{liveEnv("env-9", "att-9")}},
			want:  []string{"stop_env:env-9", "keep_terminal:att-9"},
		},

		// ---- §14.1 扫描表 ----
		{
			name: "扫描：属于本安装、无对应记录 → 回收孤立资源",
			scan: []provider.ScanItem{item("env-x", "cgroup", provider.OwnedPartial), item("env-x", "env_dir", provider.OwnedPartial)},
			want: []string{"reclaim_orphan:env-x"},
		},
		{
			name:  "扫描：属于本安装、有未完成记录 → 交给停止与清理",
			facts: Facts{Environments: []EnvFact{liveEnv("env-s", "")}},
			scan:  []provider.ScanItem{item("env-s", "cgroup", provider.OwnedComplete)},
			want:  []string{"stop_env:env-s"},
		},
		{
			name: "扫描：归属不明 → 隔离、报警、不销毁",
			scan: []provider.ScanItem{item("env-u", "env_dir", provider.Unknown)},
			want: []string{"quarantine:scan:env_dir:" + scanPath("env-u", "env_dir"), "alert:scan:env_dir:" + scanPath("env-u", "env_dir")},
		},
		{
			name: "扫描：属于其他安装 → 不操作",
			scan: []provider.ScanItem{item("env-f", "cgroup", provider.Foreign)},
			want: []string{},
		},
		{
			name: "扫描：同一环境归属冲突 → 只隔离非外部资源",
			scan: []provider.ScanItem{item("env-c", "cgroup", provider.OwnedPartial), item("env-c", "env_dir", provider.Foreign)},
			want: []string{"quarantine:scan:cgroup:" + scanPath("env-c", "cgroup"), "alert:scan:cgroup:" + scanPath("env-c", "cgroup")},
		},
		{
			name: "扫描：属于本安装但记录冲突（已清理完成仍有资源）→ 隔离，不归还 UID 范围",
			facts: Facts{UnreleasedRanges: []resource.UIDRange{
				{UIDRangeID: "u1", Base: 100000, Size: 4096, State: "assigned", OwnerID: "env-d", AllocationID: "env-uid:env-d"}}},
			scan: []provider.ScanItem{item("env-d", "uid_files", provider.OwnedPartial)},
			want: []string{"quarantine:scan:uid_files:" + scanPath("env-d", "uid_files"), "alert:scan:uid_files:" + scanPath("env-d", "uid_files")},
		},
		{
			name: "扫描：无法识别 env_id 的本安装资源 → 隔离",
			scan: []provider.ScanItem{{Layer: "mount", Path: "/mnt/x", Owner: provider.OwnedPartial}},
			want: []string{"quarantine:scan:mount:/mnt/x", "alert:scan:mount:/mnt/x"},
		},

		// ---- §8.3 intent 与 UID 范围 ----
		{
			name:  "intent pending、环境已清理且无残留 → failed",
			facts: Facts{PendingIntents: []resource.Intent{{IntentID: "env-create:env-p", EnvID: "env-p", Kind: "environment", Name: "env-p", State: "pending"}}},
			want:  []string{"resolve_intent:env-create:env-p"},
			check: func(t *testing.T, p RecoveryPlan) {
				if s := step(t, p, "resolve_intent:env-create:env-p"); s.Expect != resource.IntentFailed {
					t.Fatalf("pending 应结束为 failed：%+v", s)
				}
			},
		},
		{
			name:  "intent acquired、环境已清理且无残留 → released",
			facts: Facts{PendingIntents: []resource.Intent{{IntentID: "env-create:env-a", EnvID: "env-a", Kind: "environment", Name: "env-a", State: "acquired"}}},
			want:  []string{"resolve_intent:env-create:env-a"},
			check: func(t *testing.T, p RecoveryPlan) {
				if s := step(t, p, "resolve_intent:env-create:env-a"); s.Expect != resource.IntentReleased {
					t.Fatalf("acquired 应结束为 released：%+v", s)
				}
			},
		},
		{
			name: "intent pending、环境仍有未完成记录 → 只停止，由清理结束 intent",
			facts: Facts{
				Environments:   []EnvFact{liveEnv("env-p", "")},
				PendingIntents: []resource.Intent{{IntentID: "env-create:env-p", EnvID: "env-p", State: "pending"}},
			},
			want: []string{"stop_env:env-p"},
		},
		{
			name:  "intent pending、扫描发现残留 → 隔离，intent 保持未结束",
			facts: Facts{PendingIntents: []resource.Intent{{IntentID: "env-create:env-p", EnvID: "env-p", State: "pending"}}},
			scan:  []provider.ScanItem{item("env-p", "cgroup", provider.OwnedPartial)},
			want:  []string{"quarantine:scan:cgroup:" + scanPath("env-p", "cgroup"), "alert:scan:cgroup:" + scanPath("env-p", "cgroup")},
		},
		{
			name: "清理完成但 UID 范围仍为 assigned → 归还",
			facts: Facts{UnreleasedRanges: []resource.UIDRange{
				{UIDRangeID: "u1", Base: 100000, Size: 4096, State: "assigned", OwnerID: "env-d", AllocationID: "env-uid:env-d"}}},
			want: []string{"release_uid_range:u1:env-uid:env-d"},
		},

		// ---- 无法自洽的组合：只隔离与报警 ----
		{
			name: "无效：裁决已提交但当前 attempt 未结束",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "running", "run", &AttemptFact{AttemptID: "att-1", EnvID: "env-1", Status: "active", HasVerdict: true})},
				Environments: []EnvFact{liveEnv("env-1", "att-1")},
			},
			want: []string{"quarantine:task:t1", "alert:task:t1", "stop_env:env-1"},
		},
		{
			name:  "无效：执行中但没有当前 attempt",
			facts: Facts{Tasks: []TaskFact{task("t1", "running", "run", nil)}},
			want:  []string{"quarantine:task:t1", "alert:task:t1"},
		},
		{
			name:  "无效：排队中但当前 attempt 未结束",
			facts: Facts{Tasks: []TaskFact{task("t1", "queued", "run", openAttempt("att-1", "env-1", "active"))}},
			want:  []string{"quarantine:task:t1", "alert:task:t1"},
		},
		{
			name:  "无效：attempt 已结束但没有裁决",
			facts: Facts{Tasks: []TaskFact{task("t1", "queued", "run", &AttemptFact{AttemptID: "att-1", EnvID: "env-1", Status: "ended"})}},
			want:  []string{"quarantine:task:t1", "alert:task:t1"},
		},
		{
			name:  "无效：cancelling 但 desired 是 run",
			facts: Facts{Tasks: []TaskFact{task("t1", "cancelling", "run", openAttempt("att-1", "env-1", "active"))}},
			want:  []string{"quarantine:task:t1", "alert:task:t1"},
		},
		{
			name: "无效：环境记录属于另一个 attempt",
			facts: Facts{
				Tasks:        []TaskFact{task("t1", "running", "run", openAttempt("att-1", "env-1", "active"))},
				Environments: []EnvFact{liveEnv("env-1", "att-0")},
			},
			want: []string{"quarantine:task:t1", "alert:task:t1", "stop_env:env-1", "keep_terminal:att-0"},
		},
		{
			name:  "无效：未停止的环境清理已完成",
			facts: Facts{Environments: []EnvFact{{EnvID: "env-z", CleanupState: resource.CleanupDone}}},
			want:  []string{"quarantine:env:env-z", "alert:env:env-z", "stop_env:env-z"},
		},
		{
			name:  "无效：未知 intent 状态",
			facts: Facts{PendingIntents: []resource.Intent{{IntentID: "i9", EnvID: "env-9", State: "released"}}},
			want:  []string{"quarantine:intent:i9", "alert:intent:i9"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Plan(c.facts, provider.ScanReport{Items: c.scan}, install)
			if got := ids(p); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("步骤\n got %v\nwant %v", got, c.want)
			}
			if c.check != nil {
				c.check(t, p)
			}
		})
	}
}

// TestPlanStable：同一事实两次生成相同计划；输入顺序不同也得到同一计划；步骤 ID 唯一。
func TestPlanStable(t *testing.T) {
	facts := Facts{
		Tasks: []TaskFact{
			task("t2", "cancelling", "cancel", openAttempt("att-2", "env-2", "active")),
			task("t1", "running", "run", openAttempt("att-1", "env-1", "handshaking")),
			task("t3", "queued", "run", endedAttempt("att-3", "env-3")),
		},
		Environments: []EnvFact{liveEnv("env-2", "att-2"), liveEnv("env-1", "att-1"), stoppedEnv("env-3", "att-3"), liveEnv("env-9", "att-9")},
		PendingIntents: []resource.Intent{
			{IntentID: "env-create:env-1", EnvID: "env-1", State: "acquired"},
			{IntentID: "env-create:env-8", EnvID: "env-8", State: "pending"},
		},
		UnreleasedRanges: []resource.UIDRange{{UIDRangeID: "u7", State: "assigned", OwnerID: "env-7", AllocationID: "env-uid:env-7"}},
	}
	scan := provider.ScanReport{Items: []provider.ScanItem{
		item("env-x", "cgroup", provider.OwnedPartial), item("env-u", "mount", provider.Unknown), item("env-1", "cgroup", provider.OwnedComplete),
	}}
	first := Plan(facts, scan, install)
	if again := Plan(facts, scan, install); !reflect.DeepEqual(first, again) {
		t.Fatalf("同一事实两次生成的计划不同：\n%v\n%v", ids(first), ids(again))
	}

	rev := Facts{}
	for i := len(facts.Tasks) - 1; i >= 0; i-- {
		rev.Tasks = append(rev.Tasks, facts.Tasks[i])
	}
	for i := len(facts.Environments) - 1; i >= 0; i-- {
		rev.Environments = append(rev.Environments, facts.Environments[i])
	}
	for i := len(facts.PendingIntents) - 1; i >= 0; i-- {
		rev.PendingIntents = append(rev.PendingIntents, facts.PendingIntents[i])
	}
	rev.UnreleasedRanges = facts.UnreleasedRanges
	revScan := provider.ScanReport{Items: []provider.ScanItem{scan.Items[2], scan.Items[1], scan.Items[0]}}
	if shuffled := Plan(rev, revScan, install); !reflect.DeepEqual(first, shuffled) {
		t.Fatalf("输入顺序改变了计划：\n%v\n%v", ids(first), ids(shuffled))
	}

	seen := map[string]bool{}
	for _, s := range first.Steps {
		if seen[s.ID] {
			t.Fatalf("步骤 ID 重复：%s", s.ID)
		}
		seen[s.ID] = true
	}
	want := []string{
		"quarantine:scan:mount:/var/lib/agentbox/mount/env-u", "alert:scan:mount:/var/lib/agentbox/mount/env-u",
		"stop_env:env-1", "stop_env:env-2", "stop_env:env-9",
		"mark_attempt_lost:t1:att-1", "cancel_task:t2:att-2", "keep_queued:t3", "keep_terminal:att-9",
		"reclaim_orphan:env-x",
		"resolve_intent:env-create:env-8",
		"release_uid_range:u7:env-uid:env-7",
	}
	if got := ids(first); !reflect.DeepEqual(got, want) {
		t.Fatalf("组合计划\n got %v\nwant %v", got, want)
	}
}
