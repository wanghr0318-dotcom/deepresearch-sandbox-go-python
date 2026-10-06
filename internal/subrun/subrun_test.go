package subrun

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestValidID(t *testing.T) {
	for _, id := range []string{"st1", "a", "0", "a_b-c", strings.Repeat("a", 32)} {
		if !ValidID(id) {
			t.Errorf("ValidID(%q) = false，应通过", id)
		}
	}
	for _, id := range []string{"root", "St1", "", strings.Repeat("a", 33), "a/b", "_a", "-a", "a b", "st1\n"} {
		if ValidID(id) {
			t.Errorf("ValidID(%q) = true，应拒绝", id)
		}
	}
}

func i64(v int64) *int64 { return &v }

func TestDefinitionValidate(t *testing.T) {
	ok := []Definition{
		{ParentStepID: "plan", DeadlineMS: 1},
		{ParentStepID: "plan", DeadlineMS: MaxDeadlineMS, BudgetCapMicro: i64(0)},
		{ParentStepID: strings.Repeat("p", 256), DeadlineMS: 600_000, BudgetCapMicro: i64(1<<53 - 1)},
	}
	for _, d := range ok {
		if err := d.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v，应通过", d, err)
		}
	}
	bad := map[string]Definition{
		"deadline 0":      {ParentStepID: "plan", DeadlineMS: 0},
		"deadline 超上限":    {ParentStepID: "plan", DeadlineMS: MaxDeadlineMS + 1},
		"负上限":             {ParentStepID: "plan", DeadlineMS: 1000, BudgetCapMicro: i64(-1)},
		"上限超 I-JSON 安全整数": {ParentStepID: "plan", DeadlineMS: 1000, BudgetCapMicro: i64(1 << 53)},
		"空 parent":        {ParentStepID: "", DeadlineMS: 1000},
		"parent 超 256 字节": {ParentStepID: strings.Repeat("p", 257), DeadlineMS: 1000},
	}
	for name, d := range bad {
		if err := d.Validate(); !errors.Is(err, ErrInvalidDefinition) {
			t.Errorf("%s：Validate = %v，应为 ErrInvalidDefinition", name, err)
		}
	}
}

func TestDefinitionHash(t *testing.T) {
	a := Definition{ParentStepID: "plan", DeadlineMS: 600_000, BudgetCapMicro: i64(5)}
	b := Definition{DeadlineMS: 600_000, BudgetCapMicro: i64(5), ParentStepID: "plan"}
	if !bytes.Equal(a.Hash(), b.Hash()) {
		t.Fatal("同一定义的哈希不同")
	}
	if len(a.Hash()) != sha256.Size {
		t.Fatalf("哈希长度 %d，应为 %d", len(a.Hash()), sha256.Size)
	}
	// 固定规范形式：键按字典序，nil 上限编码为 null（字段顺序与 Go 结构无关）。
	want := sha256.Sum256([]byte(`{"budget_cap_micro":5,"deadline_ms":600000,"parent_step_id":"plan"}`))
	if !bytes.Equal(a.Hash(), want[:]) {
		t.Error("哈希不是 sha256(JCS({parent_step_id, budget_cap_micro, deadline_ms}))")
	}
	nilCap := Definition{ParentStepID: "plan", DeadlineMS: 600_000}
	wantNil := sha256.Sum256([]byte(`{"budget_cap_micro":null,"deadline_ms":600000,"parent_step_id":"plan"}`))
	if !bytes.Equal(nilCap.Hash(), wantNil[:]) {
		t.Error("nil 上限未编码为 null")
	}
	zeroCap := Definition{ParentStepID: "plan", DeadlineMS: 600_000, BudgetCapMicro: i64(0)}
	if bytes.Equal(nilCap.Hash(), zeroCap.Hash()) {
		t.Error("budget_cap_micro nil 与 0 的哈希相同")
	}
	for _, other := range []Definition{
		{ParentStepID: "plan2", DeadlineMS: 600_000, BudgetCapMicro: i64(5)},
		{ParentStepID: "plan", DeadlineMS: 600_001, BudgetCapMicro: i64(5)},
		{ParentStepID: "plan", DeadlineMS: 600_000, BudgetCapMicro: i64(6)},
	} {
		if bytes.Equal(a.Hash(), other.Hash()) {
			t.Errorf("不同定义 %+v 的哈希相同", other)
		}
	}
}

// TestNextTable 穷举 7 状态 × 12 事件。"X" 表示 ErrInvalidTransition；"C" 表示取消族终态（按 cancelReason：
// deadline → timed_out，其他 → cancelled），对每个此类单元分别以 deadline 与 orchestrator 验证。
func TestNextTable(t *testing.T) {
	const (
		X = Status("X")
		C = Status("C")
	)
	events := []Event{EvEndSucceeded, EvEndFailed, EvEndCancelled, EvCancelRequest, EvCheckpointCompleted,
		EvCheckpointFailed, EvCheckpointCancelled, EvCheckpointStarted, EvExpired, EvTaskCancelled, EvTaskSucceeded, EvTaskFailed}
	table := map[Status][12]Status{
		//               EndSucc      EndFail  EndCancel  CancelReq        CkCompleted CkFailed CkCancelled CkStarted    Expired  TaskCancel TaskSucc TaskFail
		Started:         {EndProposed, Failed, C, CancelRequested, Completed, Failed, C, Started, TimedOut, Cancelled, Failed, Failed},
		EndProposed:     {X, Failed, C, CancelRequested, Completed, Failed, C, EndProposed, TimedOut, Cancelled, Failed, Failed},
		CancelRequested: {X, Failed, C, CancelRequested, X, Failed, C, CancelRequested, C, Cancelled, Failed, Failed},
		Completed:       {X, X, X, X, Completed, X, X, X, X, X, X, X},
		Failed:          {X, Failed, X, X, X, Failed, X, X, X, X, X, X},
		Cancelled:       {X, X, Cancelled, X, X, X, Cancelled, X, X, X, X, X},
		TimedOut:        {X, X, TimedOut, X, X, X, TimedOut, X, X, X, X, X},
	}
	if len(table) != 7 {
		t.Fatalf("表应覆盖 7 个状态，实际 %d", len(table))
	}
	for from, row := range table {
		for i, ev := range events {
			for _, reason := range []string{ReasonDeadline, ReasonOrchestrator} {
				want := row[i]
				if want == C {
					want = Cancelled
					if reason == ReasonDeadline {
						want = TimedOut
					}
				}
				got, err := Next(from, ev, reason)
				if want == X {
					if !errors.Is(err, ErrInvalidTransition) {
						t.Errorf("Next(%s, %d, %s) = (%s, %v)，应为 ErrInvalidTransition", from, ev, reason, got, err)
					}
					continue
				}
				if err != nil || got != want {
					t.Errorf("Next(%s, %d, %s) = (%s, %v)，应为 %s", from, ev, reason, got, err, want)
				}
			}
		}
	}
}

// TestNextNamedCases 固定 brief 点名的关键转换（与表重叠，便于定位回归）。
func TestNextNamedCases(t *testing.T) {
	cases := []struct {
		name   string
		from   Status
		ev     Event
		reason string
		want   Status
		bad    bool
	}{
		{"end_proposed 入 checkpoint 才完成", EndProposed, EvCheckpointCompleted, "", Completed, false},
		{"started 直接由 checkpoint 完成", Started, EvCheckpointCompleted, "", Completed, false},
		{"E41：已请求取消不能完成", CancelRequested, EvCheckpointCompleted, "", "", true},
		{"已取消不能完成", Cancelled, EvCheckpointCompleted, "", "", true},
		{"deadline 取消结束为 timed_out", CancelRequested, EvEndCancelled, ReasonDeadline, TimedOut, false},
		{"编排层取消结束为 cancelled", CancelRequested, EvEndCancelled, ReasonOrchestrator, Cancelled, false},
		{"终态不因过期改变", Completed, EvExpired, "", "", true},
		{"成功裁决时未完成为 failed", Started, EvTaskSucceeded, "", Failed, false},
		{"E42：end_proposed 未入 checkpoint 不视为完成", EndProposed, EvTaskSucceeded, "", Failed, false},
		{"失败裁决时未终态为 failed（已请求的取消同样收尾）", CancelRequested, EvTaskFailed, ReasonDeadline, Failed, false},
		{"失败裁决不改写终态", Completed, EvTaskFailed, "", "", true},
		{"同一终态重复确认", Completed, EvCheckpointCompleted, "", Completed, false},
		{"timed_out 被 checkpoint 列为 cancelled 是确认", TimedOut, EvCheckpointCancelled, "", TimedOut, false},
		{"未知状态", Status("bogus"), EvCheckpointStarted, "", "", true},
		{"未知事件", Started, Event(0), "", "", true},
	}
	for _, c := range cases {
		got, err := Next(c.from, c.ev, c.reason)
		if c.bad {
			if !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("%s：Next = (%s, %v)，应为 ErrInvalidTransition", c.name, got, err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s：Next = (%s, %v)，应为 %s", c.name, got, err, c.want)
		}
	}
}

func TestTerminal(t *testing.T) {
	for s, want := range map[Status]bool{Started: false, EndProposed: false, CancelRequested: false,
		Completed: true, Cancelled: true, Failed: true, TimedOut: true} {
		if s.Terminal() != want {
			t.Errorf("%s.Terminal() = %v", s, !want)
		}
	}
}

func TestResumeDecision(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	future, past := now.Add(time.Minute), now.Add(-time.Second)
	cases := []struct {
		name   string
		r      Record
		status Status
		rerun  bool
	}{
		{"completed 恢复结果不重跑", Record{Status: Completed, DeadlineAt: future}, Completed, false},
		{"completed 即使过期也不变", Record{Status: Completed, DeadlineAt: past}, Completed, false},
		{"cancelled 不重跑", Record{Status: Cancelled, DeadlineAt: future}, Cancelled, false},
		{"failed 不重跑", Record{Status: Failed, DeadlineAt: future}, Failed, false},
		{"timed_out 不重跑", Record{Status: TimedOut, DeadlineAt: future}, TimedOut, false},
		{"started 未过期继续", Record{Status: Started, DeadlineAt: future}, Started, true},
		{"E45：started 已过期", Record{Status: Started, DeadlineAt: past}, TimedOut, false},
		{"恰在 deadline 视为过期", Record{Status: Started, DeadlineAt: now}, TimedOut, false},
		{"E42：end_proposed 未过期重新执行", Record{Status: EndProposed, DeadlineAt: future}, Started, true},
		{"end_proposed 已过期", Record{Status: EndProposed, DeadlineAt: past}, TimedOut, false},
		{"E40：取消后崩溃不恢复执行", Record{Status: CancelRequested, CancelReason: ReasonOrchestrator, DeadlineAt: future}, Cancelled, false},
		{"已请求取消且过期仍按取消原因", Record{Status: CancelRequested, CancelReason: ReasonOrchestrator, DeadlineAt: past}, Cancelled, false},
		{"deadline 取消后崩溃为 timed_out", Record{Status: CancelRequested, CancelReason: ReasonDeadline, DeadlineAt: past}, TimedOut, false},
	}
	for _, c := range cases {
		s, rerun := ResumeDecision(c.r, now)
		if s != c.status || rerun != c.rerun {
			t.Errorf("%s：ResumeDecision = (%s, %v)，应为 (%s, %v)", c.name, s, rerun, c.status, c.rerun)
		}
	}
}

func TestBudgetAvailable(t *testing.T) {
	if got := (Budget{ReservedMicro: 10, SpentMicro: 20, UnknownMicro: 30}).Available(); got != math.MaxInt64 {
		t.Errorf("无上限 Available = %d，应为 MaxInt64", got)
	}
	if got := (Budget{CapMicro: i64(100), ReservedMicro: 10, SpentMicro: 20, UnknownMicro: 30}).Available(); got != 40 {
		t.Errorf("Available = %d，应为 40", got)
	}
	// 超支（结算超过预留）时可用为负，调用方据此判定 subrun_budget_exhausted。
	if got := (Budget{CapMicro: i64(10), SpentMicro: 15}).Available(); got != -5 {
		t.Errorf("超支 Available = %d，应为 -5", got)
	}
}
