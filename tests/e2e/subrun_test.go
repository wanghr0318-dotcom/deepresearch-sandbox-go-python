//go:build linux

// M4：sub-run 故障实验（E24、E40–E45）。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ==== M4 Plan 14 Task 11：sub-run 故障实验（规格 §13、§16.4 E24、E40–E45；本段到此结束前不含其他任务的用例） ====
//
// Worker 是 sim_worker 的 sub-run 操作（subrun_start、带 subrun 的 Gateway 步骤、subrun_end、subrun_cancel、
// subrun_ignore_cancel、checkpoint 的 forge_completed；见 worker/sim_worker/app.py）。每个实验一对用例：进程内装置
// （fake provider 的进程型 Program，非 root）与真实隔离（provider/local + 生产启动器，root；TestRealE…）。二者共用
// testE…(t, h)，结束时都以 h.finish() 运行静止时的不变量检查（verify-invariants 的同一检查，含 I3 两层与 I13）。

// subrunCfg 是开启 sub-run 扩展的 Gateway 配置（app.Config.WorkerSubruns 的零值为关闭；cmd/agentbox 默认开启）。
func subrunCfg(fu *fakeupstream.Server) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.WorkerSubruns = true
	return cfg
}

func srStartStep(id string, deadlineMs int) step {
	return step{"op": "subrun_start", "subrun_id": id, "deadline_ms": deadlineMs}
}

func srFetchStep(sub, u string) step {
	return step{"op": "fetch", "step_id": "f", "subrun": sub, "url": u}
}

func srEndStep(id string) step {
	return step{"op": "subrun_end", "subrun_id": id, "summary": id + " 的摘要"}
}

// crashStep 只在第 1 个 attempt 以 SIGKILL 杀死 Worker 自身。
func crashStep() step { return step{"op": "exit", "code": 137, "signal": 9, "attempt": 1} }

// subrunRow 返回 sub-run 的 status|bound attempt_no|failure_reason|cancel_reason。
func (h *harness) subrunRow(taskID, subrunID string) string {
	h.t.Helper()
	var out string
	h.queryRow(`SELECT concat_ws('|', s.status, a.attempt_no, s.failure_reason, s.cancel_reason) FROM subruns s
		JOIN attempts a ON a.attempt_id = s.bound_attempt_id WHERE s.task_id = $1 AND s.subrun_id = $2`,
		[]any{taskID, subrunID}, &out)
	return out
}

// resumeSubruns 是第 n 个 attempt 的 init.resume.subruns（subrun_id → status）。
func (h *harness) resumeSubruns(taskID string, n int64) map[string]string {
	h.t.Helper()
	in, ok := h.rec(taskID).init(n)
	if !ok || in.Resume == nil {
		h.t.Fatalf("attempt %d 没有 init.resume：%+v", n, in)
	}
	out := map[string]string{}
	for _, s := range in.Resume.Subruns {
		out[s.SubrunID] = s.Status
	}
	return out
}

// subrunCalls 返回 call_id 以 prefix 开头的调用：call_id → state/source/首个 attempt_no。
func (h *harness) subrunCalls(taskID, prefix string) map[string]string {
	h.t.Helper()
	var agg string
	h.queryRow(`SELECT COALESCE(string_agg(c.call_id || '=' || concat_ws('/', c.state, c.source, a.attempt_no), ' ' ORDER BY c.call_id), '')
		FROM calls c JOIN attempts a ON a.attempt_id = c.first_attempt_id WHERE c.task_id = $1 AND starts_with(c.call_id, $2)`,
		[]any{taskID, prefix}, &agg)
	out := map[string]string{}
	for _, kv := range strings.Fields(agg) {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}
	return out
}

func (h *harness) controlTask(taskID, action, requestID string) {
	h.t.Helper()
	st, b, err := httpDo("POST", h.base+"/tasks/"+taskID+"/"+action, `{"request_id":"`+requestID+`","reason":"e2e"}`)
	if err != nil || st != http.StatusOK {
		h.t.Fatalf("%s %s = %d %s %v", action, taskID, st, b, err)
	}
}

// TestE24SubrunCoalesce：E24（sub-run 部分）——同一 sub-run 内 10 个相同抓取 → fake upstream 计数 1（1 个 upstream、
// 9 个 coalesced）；另一个 sub-run 的同一抓取不合并（合并键含 subrun_id，§11.4）→ 计数 2。
func TestE24SubrunCoalesce(t *testing.T) { testE24Subrun(t, newHarness(t)) }

func TestRealE24SubrunCoalesce(t *testing.T) { testE24Subrun(t, newRealHarness(t)) }

func testE24Subrun(t *testing.T, h *harness) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e24s-"+randSuffix()+"/a", nil) // 没有新鲜度头：不写缓存，只看合并
	fu.Inject(fakeupstream.Fetch, 1, fakeupstream.Action{Hang: true})
	cfg := cacheCfg(fu, addr)
	cfg.WorkerSubruns = true
	h.start(cfg)
	id := h.submit("e24s", spec(nil, "e24s", nil, srStartStep("st1", 600000), srStartStep("st2", 600000), sleepStep(600000)))
	sock := h.waitAttemptSocket(id)
	eventually(t, "两个 sub-run 已启动", func() bool {
		var n int
		h.queryRow("SELECT count(*) FROM subruns WHERE task_id = $1 AND status = 'started'", []any{id}, &n)
		return n == 2
	})
	before := h.cacheMetrics()

	const n = 10
	body := `{"url":"` + u + `"}`
	res := make([]gwResp, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res[i], errs[i] = gwDoSubrun(sock, "st1", http.MethodPost, "/v1/fetch", fmt.Sprintf("st1/e24/fetch/%d", i+1), body)
		}()
	}
	eventually(t, "st1 的 leader 挂起在上游、9 个请求作为 follower 加入", func() bool {
		return fu.Hanging() == 1 && metricsDelta(h.cacheMetrics(), before)["coalesced"] == n-1
	})
	// st2 的同一抓取不加入 st1 的共享请求：自行访问上游（第 2 次抓取不挂起）。
	r2, err := gwDoSubrun(sock, "st2", http.MethodPost, "/v1/fetch", "st2/e24/fetch/1", body)
	if err != nil || r2.Status != http.StatusOK || r2.Replayed || r2.Blob == "" {
		t.Fatalf("st2 的抓取：%+v %v", r2, err)
	}
	fu.Release()
	wg.Wait()
	for i := range n {
		if errs[i] != nil || res[i].Status != http.StatusOK || res[i].Blob == "" || res[i].Blob != res[0].Blob {
			t.Fatalf("st1 请求 %d：%+v %v（请求 1：%+v）", i+1, res[i], errs[i], res[0])
		}
	}
	st1 := h.subrunCalls(id, "st1/e24/")
	var up, co int
	for _, v := range st1 {
		up += strings.Count(v, "/upstream/")
		co += strings.Count(v, "/coalesced/")
	}
	if len(st1) != n || up != 1 || co != n-1 || fu.Count(fakeupstream.Fetch) != 2 {
		t.Fatalf("st1 的调用 %v；上游抓取 %d 次", st1, fu.Count(fakeupstream.Fetch))
	}
	if st2 := h.subrunCalls(id, "st2/"); st2["st2/e24/fetch/1"] != "completed/upstream/1" {
		t.Fatalf("st2 的调用 %v", st2)
	}
	if st, code := h.cancelTask(id, "e24s-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	if v := h.waitTerminal(id); v.Status != "cancelled" {
		t.Fatalf("任务结束为 %s", v.Status)
	}
	for _, sub := range []string{"st1", "st2"} {
		if g := h.subrunRow(id, sub); g != "cancelled|1|task_cancel|task_cancel" {
			t.Fatalf("取消裁决后 %s = %s", sub, g)
		}
	}
	t.Logf("E24（sub-run）：st1 内 10 个相同抓取 + st2 同一抓取 → 上游 %d 次", fu.Count(fakeupstream.Fetch))
	h.finish()
}

// TestE40CancelThenCrash：E40——编排层取消 st2 后 SIGKILL Worker → 新 attempt 的 init.resume.subruns 中 st2 为
// cancelled（不恢复执行），st2 没有新调用；st1 重新绑定并完成。
func TestE40CancelThenCrash(t *testing.T) { testE40(t, newHarness(t)) }

func TestRealE40CancelThenCrash(t *testing.T) { testE40(t, newRealHarness(t)) }

func testE40(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	p := "/e40-" + randSuffix()
	a, b, c, d := cachePage(fu, p+"/a", nil), cachePage(fu, p+"/b", nil), cachePage(fu, p+"/c", nil), cachePage(fu, p+"/d", nil)
	h.start(subrunCfg(fu))
	id := h.submit("e40", spec(nil, "e40", nil,
		srStartStep("st1", 600000), srStartStep("st2", 600000),
		srFetchStep("st1", a), srFetchStep("st2", b),
		checkpointStep("c1"),
		step{"op": "subrun_cancel", "subrun_id": "st2"},
		crashStep(),
		srFetchStep("st2", c), // 恢复后不得执行：st2 已 cancelled
		srFetchStep("st1", d),
		srEndStep("st1"),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	if a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2); a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v / %+v", a1, a2)
	}
	if got := h.resumeSubruns(id, 2); got["st2"] != "cancelled" || got["st1"] != "started" {
		t.Fatalf("attempt 2 的 resume.subruns = %v", got)
	}
	if g := h.subrunRow(id, "st2"); g != "cancelled|1||orchestrator" {
		t.Fatalf("st2 = %s", g)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|2||" {
		t.Fatalf("st1 = %s", g)
	}
	st2 := h.subrunCalls(id, "st2/")
	if len(st2) != 1 || st2["st2/f/fetch/1"] != "completed/upstream/1" || pathCount(t, fu, c) != 0 {
		t.Fatalf("st2 的调用 %v；/c 上游抓取 %d 次", st2, pathCount(t, fu, c))
	}
	if pathCount(t, fu, a) != 1 || pathCount(t, fu, d) != 1 {
		t.Fatalf("st1 的抓取：/a %d 次、/d %d 次", pathCount(t, fu, a), pathCount(t, fu, d))
	}
	h.finish()
}

// TestE41LateCheckpointCompleted：E41——已取消的 sub-run 被迟到的 checkpoint 列为 completed → checkpoint_result
// rejected/invalid_transition，该 checkpoint 不写入、指针不变（之后的 checkpoint 的 commit_seq 连续）；st2 保持 cancelled。
func TestE41LateCheckpointCompleted(t *testing.T) { testE41(t, newHarness(t)) }

func TestRealE41LateCheckpointCompleted(t *testing.T) { testE41(t, newRealHarness(t)) }

func testE41(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h.start(subrunCfg(fu))
	id := h.submit("e41", spec(nil, "e41", nil,
		srStartStep("st1", 600000), srStartStep("st2", 600000),
		checkpointStep("c1"),
		step{"op": "subrun_cancel", "subrun_id": "st2"},
		step{"op": "checkpoint", "step_id": "forged", "forge_completed": []string{"st2"}, "expect_rejected": "invalid_transition"},
		srEndStep("st1"),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	var rejected int
	for _, l := range h.rec(id).hostLines() {
		if l.Type == protocol.TypeCheckpointResult && l.Status == protocol.CheckpointRejected && l.Code == "invalid_transition" {
			rejected++
		}
	}
	in := h.inspect(id)
	if rejected != 1 || len(in.Checkpoints) != 2 {
		t.Fatalf("invalid_transition 拒绝 %d 次；已提交 checkpoint %+v", rejected, in.Checkpoints)
	}
	if c1, c2 := checkpointByStep(in, "c1"), checkpointByStep(in, "c2"); c1.CommitSeq != 1 || c2.CommitSeq != 2 {
		t.Fatalf("commit_seq：c1 %d、c2 %d（被拒的 checkpoint 不应占用序号）", c1.CommitSeq, c2.CommitSeq)
	}
	if g := h.subrunRow(id, "st2"); g != "cancelled|1||orchestrator" {
		t.Fatalf("st2 = %s", g)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|1||" {
		t.Fatalf("st1 = %s", g)
	}
	h.finish()
}

// TestE42EndThenCrash：E42——subrun_end{succeeded} 之后、checkpoint 之前崩溃 → end_proposed 不视为完成：恢复时 st1
// 重新绑定（resume.subruns 中为 started）并重新执行。checkpoint 之后完成的调用以同一 call id 重放（上游不重复），新的
// 调用以续号 call id 出现（st1/f/fetch/3，首个 attempt 为 2）；checkpoint 之前的调用不重做。
func TestE42EndThenCrash(t *testing.T) { testE42(t, newHarness(t)) }

func TestRealE42EndThenCrash(t *testing.T) { testE42(t, newRealHarness(t)) }

func testE42(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	p := "/e42-" + randSuffix()
	a, b, c := cachePage(fu, p+"/a", nil), cachePage(fu, p+"/b", nil), cachePage(fu, p+"/c", nil)
	fetchC := srFetchStep("st1", c)
	fetchC["attempt"] = 2
	h.start(subrunCfg(fu))
	id := h.submit("e42", spec(nil, "e42", nil,
		srStartStep("st1", 600000),
		srFetchStep("st1", a),
		checkpointStep("c1"),
		srFetchStep("st1", b),
		fetchC,
		srEndStep("st1"),
		crashStep(),
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	if a1 := attemptByNo(in, 1); a1.OutcomeClass != runner.ClassCrashedSignal {
		t.Fatalf("attempt 1：%+v", a1)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "started" {
		t.Fatalf("attempt 2 的 resume.subruns = %v（end_proposed 未入 checkpoint 不视为完成）", got)
	}
	var subrunEnds int
	for _, typ := range h.rec(id).workerTypes(1) {
		if typ == "subrun_end" {
			subrunEnds++
		}
	}
	if subrunEnds != 1 {
		t.Fatalf("attempt 1 应在崩溃前发出 subrun_end：%q", h.rec(id).workerTypes(1))
	}
	if g := h.subrunRow(id, "st1"); g != "completed|2||" {
		t.Fatalf("st1 = %s", g)
	}
	want := map[string]string{"st1/f/fetch/1": "completed/upstream/1", "st1/f/fetch/2": "completed/upstream/1",
		"st1/f/fetch/3": "completed/upstream/2"}
	if got := h.subrunCalls(id, "st1/"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("st1 的调用 %v，期望 %v", got, want)
	}
	if pathCount(t, fu, a) != 1 || pathCount(t, fu, b) != 1 || pathCount(t, fu, c) != 1 {
		t.Fatalf("上游抓取：/a %d、/b %d、/c %d（重放不应再访问上游）", pathCount(t, fu, a), pathCount(t, fu, b), pathCount(t, fu, c))
	}
	h.finish()
}

// TestE43SubrunBudget：E43——sub-run 上限 0：付费调用（chat）被拒 402 subrun_budget_exhausted，不访问上游、两层账本不动；
// 同 URL 的缓存命中（root 先抓取并写入共享缓存）仍返回；sub-run 以抽取式总结完成（预算耗尽不自动取消 sub-run）。
func TestE43SubrunBudget(t *testing.T) { testE43(t, newHarness(t)) }

func TestRealE43SubrunBudget(t *testing.T) { testE43(t, newRealHarness(t)) }

func testE43(t *testing.T, h *harness) {
	addr := testRedisAddr(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e43-"+randSuffix()+"/p", ccHeader("max-age=600"))
	cfg := cacheCfg(fu, addr)
	cfg.WorkerSubruns = true
	h.start(cfg)
	// 先由另一个任务的 root 抓取写入共享缓存（写入在响应之后完成：等到 Redis 中出现该键）。
	h.runFetches("e43-warm", u)
	r, key := testRedis(t, addr), fetchKey(t, u)
	eventually(t, "抓取结果写入共享缓存", func() bool { _, ok := cacheEntry(t, r, key); return ok })
	chat := chatStep("s")
	chat["subrun"], chat["expect_error"] = "st1", "subrun_budget_exhausted"
	id := h.submit("e43", spec(nil, "e43", nil,
		step{"op": "subrun_start", "subrun_id": "st1", "deadline_ms": 600000, "budget_cap_micro": 0},
		chat,
		srFetchStep("st1", u),
		srEndStep("st1"),
		checkpointStep("c1"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	if g := h.subrunRow(id, "st1"); g != "completed|1||" {
		t.Fatalf("st1 = %s", g)
	}
	calls := h.subrunCalls(id, "st1/")
	if calls["st1/f/fetch/1"] != "completed/cache/1" || fu.Count(fakeupstream.Chat) != 0 || pathCount(t, fu, u) != 1 {
		t.Fatalf("st1 的调用 %v；上游 chat %d 次、抓取 %d 次", calls, fu.Count(fakeupstream.Chat), pathCount(t, fu, u))
	}
	var tries int
	var spent, reserved, unknown int64
	h.queryRow(`SELECT (SELECT count(*) FROM call_tries WHERE task_id = $1 AND call_id LIKE 'st1/%'),
			sb.spent_micro, sb.reserved_micro, sb.unknown_micro FROM subrun_budgets sb WHERE sb.task_id = $1 AND sb.subrun_id = 'st1'`,
		[]any{id}, &tries, &spent, &reserved, &unknown)
	if tries != 0 || spent != 0 || reserved != 0 || unknown != 0 {
		t.Fatalf("sub-run 层：try %d 个，spent/reserved/unknown = %d/%d/%d", tries, spent, reserved, unknown)
	}
	h.finish()
}

// TestE44IgnoreCancel：E44——SDK 忽略 subrun_cancel_requested（deadline 触发）→ T_subrun_cancel 到期后 attempt 被终止
// （outcome_class = subrun_cancel_timeout，故障重试）→ 新 attempt 从 checkpoint 恢复，st1 为 timed_out 且不再执行。
func TestE44IgnoreCancel(t *testing.T) { testE44(t, newHarness(t)) }

func TestRealE44IgnoreCancel(t *testing.T) { testE44(t, newRealHarness(t)) }

func testE44(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	cfg := subrunCfg(fu)
	cfg.Runner.SubrunCancelTimeout = time.Second
	h.start(cfg)
	id := h.submit("e44", spec(nil, "e44", nil,
		step{"op": "subrun_ignore_cancel"},
		srStartStep("st1", 1500),
		checkpointStep("c1"),
		step{"op": "sleep", "ms": 600000, "subrun": "st1"},
		checkpointStep("c2"),
	))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	in := h.inspect(id)
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	if a1.OutcomeClass != runner.ClassSubrunCancelTimeout || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v / %+v", a1, a2)
	}
	init2, _ := h.rec(id).init(2)
	if c1 := checkpointByStep(in, "c1"); init2.Resume == nil || init2.Resume.CheckpointID != c1.CheckpointID {
		t.Fatalf("attempt 2 的 resume %+v，期望 c1（%s）", init2.Resume, c1.CheckpointID)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "timed_out" {
		t.Fatalf("attempt 2 的 resume.subruns = %v", got)
	}
	if g := h.subrunRow(id, "st1"); !strings.HasPrefix(g, "timed_out|1|") || !strings.HasSuffix(g, "|deadline") {
		t.Fatalf("st1 = %s", g)
	}
	if got := h.rec(id).workerTypes(2); slices.Contains(got, "subrun_start") {
		t.Fatalf("attempt 2 不应重发已终态 sub-run 的 subrun_start：%q", got)
	}
	h.finish()
}

// TestE45PausePastDeadline：E45（规格 §13.5 执行中修订，M4 验收 2026-10-06）——暂停期间 sub-run 的 deadline 不计时：
// 暂停久于剩余时间（原 deadline_at 在暂停期间过去）后继续，新 attempt 的 init.resume.subruns 告知 st1 为 started，
// deadline 按暂停时记录的剩余时间重新起算，st1 完成其余步骤；任务成功。暂停前已过期仍为 timed_out 见 internal/app 的
// TestSubrunAttemptLifecycle 与 postgres 的 TestSubrunDeadlineSuspendedWhilePaused。
func TestE45PausePastDeadline(t *testing.T) { testE45(t, newHarness(t)) }

func TestRealE45PausePastDeadline(t *testing.T) { testE45(t, newRealHarness(t)) }

func testE45(t *testing.T, h *harness) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u := cachePage(fu, "/e45-"+randSuffix()+"/a", nil)
	steps := []step{srStartStep("st1", 6000)}
	for i := range 20 { // 每 100 ms 一个 checkpoint：暂停请求在下一个提交边界生效
		steps = append(steps, step{"op": "sleep", "ms": 100, "subrun": "st1"}, checkpointStep(fmt.Sprintf("k%d", i)))
	}
	steps = append(steps, srFetchStep("st1", u), srEndStep("st1"), checkpointStep("done"))
	h.start(subrunCfg(fu))
	id := h.submit("e45", spec(nil, "e45", nil, steps...))
	eventually(t, "st1 已启动", func() bool {
		var n int
		h.queryRow("SELECT count(*) FROM subruns WHERE task_id = $1", []any{id}, &n)
		return n == 1
	})
	h.controlTask(id, "pause", "e45-pause")
	eventually(t, "任务暂停", func() bool {
		st, _, _ := h.httpTask(id)
		return st == "paused"
	})
	if g := h.subrunRow(id, "st1"); g != "started|1||" {
		t.Fatalf("暂停时 st1 = %s（应在 deadline 之前暂停）", g)
	}
	var remaining int64
	h.queryRow("SELECT COALESCE(remaining_ms, -1) FROM subruns WHERE task_id = $1 AND subrun_id = 'st1'", []any{id}, &remaining)
	if remaining <= 0 || remaining > 6000 {
		t.Fatalf("暂停裁决应记录 st1 的剩余时间，得到 %d ms", remaining)
	}
	eventuallyWithin(t, "st1 的原 deadline 在暂停期间过去", 15*time.Second, 100*time.Millisecond, func() bool {
		var past bool
		h.queryRow("SELECT now() > deadline_at FROM subruns WHERE task_id = $1 AND subrun_id = 'st1'", []any{id}, &past)
		return past
	})
	h.controlTask(id, "resume", "e45-resume")
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	if got := h.resumeSubruns(id, 2); got["st1"] != "started" {
		t.Fatalf("attempt 2 的 resume.subruns = %v（暂停期间不计时，st1 应继续）", got)
	}
	if g := h.subrunRow(id, "st1"); !strings.HasPrefix(g, "completed|2") {
		t.Fatalf("st1 = %s", g)
	}
	if pathCount(t, fu, u) != 1 {
		t.Fatalf("继续后 st1 应完成抓取：/a 抓取 %d 次", pathCount(t, fu, u))
	}
	h.finish()
}

// ==== M4 Plan 14 Task 11 段结束 ====
