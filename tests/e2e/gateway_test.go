//go:build linux

// M2：Gateway 故障实验、真实沙箱冒烟与 DeepResearch 业务验收（fake upstream）。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ---- M2：Gateway 故障实验（fake upstream；规格 §16.4 E17、E19、E20、E48、E11b）----
//
// 上游是进程内的 tests/e2e/fakeupstream（不访问外网）；Worker 是真实的 sim_worker（chat 操作经 Gateway 的
// Unix socket 调用模型）。E17、E19、E20 用进程内装置（provider/fake 的进程型 Program），E48、E11b 需要
// faultinject，用 agentbox-e2e 子进程（procprov）。测试也直接连接 attempt 的 socket（与 Worker 同属主）发出
// 同 ID 的请求，观察 Worker 看不到的响应（409、504、重放头）。每条用例以静止时的不变量检查结束（含 I3、I14）。

const (
	gatewaySocketEnv = "AGENTBOX_GATEWAY_SOCKET" // Worker SDK 的 socket 路径覆盖
	modelKeyEnv      = "AGENTBOX_MODEL_API_KEY"  // agentbox-e2e 读取的模型 Key
	e2eCallID        = "root/s1/chat/1"          // sim_worker 的 chat 操作（step_id s1）的第一个调用 ID
	probeCallID      = "e2e/probe/chat/1"        // 测试直接发出的另一调用
)

// gatewayPoints 是 Gateway 的钩子点及覆盖它们的实验（E5 不遍历它们：任务不经 Gateway 时不会到达）。
var gatewayPoints = map[string]string{faultinject.CallInFlight: "E48", faultinject.ReservationCommit: "E11b"}

var (
	gwMessages = []map[string]string{
		{"role": "system", "content": fakeupstream.StagePlan + " e2e 规划"},
		{"role": "user", "content": "研究主题：e2e"},
	}
	probeMessages = []map[string]string{{"role": "user", "content": "e2e 探针调用"}}
)

func chatStep(stepID string) step {
	return step{"op": "chat", "step_id": stepID, "messages": gwMessages}
}

// chatBody 是 sim_worker 的 chat 操作发出的请求体（{"messages": ...}，未设置 max_tokens）：指纹按 JCS 规范化，
// 与 Worker 的编码细节无关。
func chatBody(msgs []map[string]string) string {
	b, err := json.Marshal(map[string]any{"messages": msgs})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func gwSecret() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b) // crypto/rand 不会失败（Go 1.20 起失败时直接终止进程）
	return "sk-e2e-" + hex.EncodeToString(b)
}

// gatewayCfg 是进程内装置的 Gateway 配置：模型上游指向 fake upstream（单价每 token 1 微美元），搜索供应商 fake
// 且经 SearchBaseURL 访问 fake upstream 的 /search，fake upstream 的 host:port 显式放行（与 cmd/agentbox 的
// --search-provider fake 校验相同）。
func gatewayCfg(fu *fakeupstream.Server, key string, lim call.Limits) app.Config {
	cfg := baseConfig()
	cfg.Model = app.ModelConfig{BaseURL: fu.ModelBaseURL(), Name: fakeupstream.Model, APIKey: key,
		Pricing: upstream.Pricing{InputMicroPerMTok: 1_000_000, OutputMicroPerMTok: 1_000_000}}
	cfg.SearchProvider, cfg.SearchBaseURL = upstream.SearchFake, fu.URL()
	cfg.UpstreamAllowPrivate = []string{fu.HostPort()}
	cfg.Gateway = lim
	return cfg
}

func (h *harness) gwSocket(attemptID string) string {
	return filepath.Join(h.dir, "gateway", attemptID+".sock")
}

func (s *sysHarness) gwSocket(attemptID string) string {
	return filepath.Join(s.dir, "gateway", attemptID+".sock")
}

// gwResp 是测试直接发给 Gateway 的一次请求的结果。
type gwResp struct {
	Status   int
	Code     string
	Replayed bool
	Blob     string
}

// gwDo 在 attempt 的 socket 上以一条新连接发出一个请求（不复用连接）。
func gwDo(sock, method, path, callID, body string) (gwResp, error) {
	return gwDoSubrun(sock, "", method, path, callID, body)
}

// gwDoSubrun 同 gwDo；subrunID 非空时带 X-Agentbox-Subrun（归属该 sub-run，call id 须以 <subrunID>/ 开头）。
func gwDoSubrun(sock, subrunID, method, path, callID, body string) (gwResp, error) {
	tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequest(method, "http://gateway"+path, strings.NewReader(body))
	if err != nil {
		return gwResp{}, err
	}
	if callID != "" {
		req.Header.Set("X-Agentbox-Call-Id", callID)
	}
	if subrunID != "" {
		req.Header.Set("X-Agentbox-Subrun", subrunID)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return gwResp{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return gwResp{}, err
	}
	r := gwResp{Status: resp.StatusCode, Replayed: resp.Header.Get("X-Agentbox-Replayed") == "true",
		Blob: resp.Header.Get("X-Agentbox-Blob")}
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &e); err != nil {
			return r, fmt.Errorf("错误响应体 %q: %w", b, err)
		}
		r.Code = e.Error.Code
	}
	return r, nil
}

// gwCall 是 calls 表中的一行（测试关心的列）。
type gwCall struct {
	State          string
	Tries          int
	PED            bool // possible_external_duplicate
	FirstAttempt   string
	ResultRef      string
	FailReason     string
	Deadline       time.Time
	ResolvingSince *time.Time
}

func gwCallRow(t *testing.T, dsn, taskID, callID string) gwCall {
	t.Helper()
	var c gwCall
	pgQueryRow(t, dsn, `SELECT state, tries_used, possible_external_duplicate, first_attempt_id, COALESCE(result_ref, ''),
			fail_reason, deadline_at, resolving_since FROM calls WHERE task_id = $1 AND call_id = $2`, []any{taskID, callID},
		&c.State, &c.Tries, &c.PED, &c.FirstAttempt, &c.ResultRef, &c.FailReason, &c.Deadline, &c.ResolvingSince)
	return c
}

// gwTry 是一次 try 与它的 reservation。
type gwTry struct {
	TryNo       int
	AttemptID   string
	Outcome     string
	Error       string
	Reservation string // reservation 的状态（held、settled、released、charged_unknown）
	Amount      int64
	Cost        int64
}

func gwTries(t *testing.T, dsn, taskID, callID string) []gwTry {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT t.try_no, t.attempt_id, t.outcome, t.error, r.state, r.amount, t.cost_micro
		FROM call_tries t JOIN reservations r USING (reservation_id) WHERE t.task_id = $1 AND t.call_id = $2 ORDER BY t.try_no`,
		taskID, callID)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (gwTry, error) {
		var x gwTry
		err := r.Scan(&x.TryNo, &x.AttemptID, &x.Outcome, &x.Error, &x.Reservation, &x.Amount, &x.Cost)
		return x, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// gwBudget 读取任务账本（reserved、spent、unknown）。
func gwBudget(t *testing.T, dsn, taskID string) (reserved, spent, unknown int64) {
	t.Helper()
	pgQueryRow(t, dsn, "SELECT reserved_micro, spent_micro, unknown_micro FROM budgets WHERE task_id = $1", []any{taskID},
		&reserved, &spent, &unknown)
	return reserved, spent, unknown
}

// assertKeyOnlyUpstream：Key 只出现在发往上游的 Authorization 头中，不出现在服务日志与事件中（§9.9）。
func assertKeyOnlyUpstream(t *testing.T, key, logs string, evs []api.Event, reqs []fakeupstream.Request) {
	t.Helper()
	b, err := json.Marshal(evs)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs, key) || strings.Contains(string(b), key) {
		t.Fatal("模型 Key 出现在服务日志或事件中")
	}
	for _, r := range reqs {
		if r.Authorization != "Bearer "+key {
			t.Fatalf("上游请求 #%d 的 Authorization 头不是配置的 Key", r.Seq)
		}
	}
}

func outcomes(reqs []fakeupstream.Request) []string {
	var out []string
	for _, r := range reqs {
		out = append(out, fmt.Sprintf("#%d %s → %s", r.N, r.Action, r.Outcome))
	}
	return out
}

// TestE17UpstreamFaults：E17——同一逻辑调用的上游依次返回 429（Retry-After: 1）、响应中途断开、挂起至调用期限
// → 共 3 次 try（累计上限）：429 按 Retry-After 退避后重试、预留释放；中途断开与超时的挂起都已发出、按 unknown
// 各计一次（估算转入 unknown，不重复计入）；不再新建第 4 次 try；I3 成立。
//
// 挂起排在最后：chat adapter 没有独立于调用期限的超时，挂起的 try 只能由 deadline_at 结束，期限之后不再新建 try。
func TestE17UpstreamFaults(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Status: http.StatusTooManyRequests, RetryAfter: "1"})
	fu.Inject(fakeupstream.Chat, 2, fakeupstream.Action{Disconnect: true})
	fu.Inject(fakeupstream.Chat, 3, fakeupstream.Action{Hang: true})
	key := gwSecret()
	const deadline = 8 * time.Second
	h.start(gatewayCfg(fu, key, call.Limits{ModelCallDeadline: deadline, BackoffBase: 50 * time.Millisecond}))
	id := h.submit("e17", spec(nil, "e17", nil, chatStep("s1")))
	v := h.waitTerminal(id)

	eventually(t, "挂起的第 3 个请求在调用期限时被 Gateway 放弃", func() bool {
		reqs := fu.Requests(fakeupstream.Chat)
		return len(reqs) == 3 && reqs[2].Outcome == fakeupstream.OutcomeAborted
	})
	reqs := fu.Requests(fakeupstream.Chat)
	if reqs[0].Outcome != fakeupstream.OutcomeStatus || reqs[1].Outcome != fakeupstream.OutcomeDisconnected || fu.Distinct(fakeupstream.Chat) != 1 {
		t.Fatalf("上游请求 %q，不同请求体 %d 个", outcomes(reqs), fu.Distinct(fakeupstream.Chat))
	}
	if gap := reqs[1].At.Sub(reqs[0].At); gap < time.Second {
		t.Fatalf("429 之后 %s 就重试，未遵从 Retry-After: 1", gap)
	}
	tries := gwTries(t, h.dsn, id, e2eCallID)
	if len(tries) != 3 ||
		tries[0].Outcome != "retryable" || tries[0].Error != upstream.CodeUpstreamRateLimited || tries[0].Reservation != "released" ||
		tries[1].Outcome != "unknown" || tries[1].Reservation != "charged_unknown" ||
		tries[2].Outcome != "unknown" || tries[2].Reservation != "charged_unknown" {
		t.Fatalf("tries %+v；期望 retryable/released、unknown/charged_unknown × 2", tries)
	}
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	if c.State != "unknown" || c.Tries != 3 || !c.PED {
		t.Fatalf("调用 %+v；期望 unknown、tries_used 3、possible_external_duplicate", c)
	}
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if reserved != 0 || spent != 0 || unknown != tries[1].Amount+tries[2].Amount {
		t.Fatalf("账本 reserved=%d spent=%d unknown=%d；期望 0、0、%d（每个 unknown try 的估算各计一次）",
			reserved, spent, unknown, tries[1].Amount+tries[2].Amount)
	}
	// Worker 得到 504 call_deadline_exceeded（不可重试的 Worker 失败）：任务结束，没有新的 attempt 与 try。
	if v.Status != "failed" || h.loadTask(id).AttemptsTotal != 1 || fu.Count(fakeupstream.Chat) != 3 {
		t.Fatalf("任务 %+v，attempts_total=%d，上游请求 %d 个", v, h.loadTask(id).AttemptsTotal, fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), reqs)
	t.Logf("E17：上游 %q；tries %+v；账本 reserved=%d spent=%d unknown=%d；任务 %s/%s",
		outcomes(reqs), tries, reserved, spent, unknown, v.Status, v.StatusReason)
	h.finish()
}

// TestE19AttemptReplacedDuringCall：E19——上游调用在途时杀死 Worker（新 attempt 取代旧 attempt）→ 旧 try 不被
// 取消，继续至完成并写入 journal；新 attempt 以同一调用 ID 得到 409 call_in_progress；完成后新 attempt 的 Worker
// 命中重放（tries_used 仍为 1、上游计数不增、它的 checkpoint refs 含同一结果 blob），任务成功。
func TestE19AttemptReplacedDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("e19", spec(nil, "e19", nil, chatStep("s1"), checkpointStep("c1")))
	eventually(t, "调用到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	r := h.rec(id)
	in1, _ := r.init(1)
	r.mu.Lock()
	kill := r.kills[1]
	r.mu.Unlock()
	kill() // 上游 try 在途时 Worker 崩溃：离开原因不是取消，try 继续（§9.1）

	var in2 protocol.Init
	eventually(t, "attempt 2 的 Worker 启动、Gateway socket 就绪", func() bool {
		var ok bool
		if in2, ok = r.init(2); !ok {
			return false
		}
		_, err := os.Stat(h.gwSocket(in2.AttemptID))
		return err == nil
	})
	// 旧 try 仍挂起在上游：attempt 2 的 Worker 在它完成之前无法结束（其同 ID 请求得到 call_in_progress 并重试），
	// 因此此时从测试侧发出的同 ID 请求所在的入口一定存活。
	got, err := gwDo(h.gwSocket(in2.AttemptID), http.MethodPost, "/v1/chat/completions", e2eCallID, chatBody(gwMessages))
	if err != nil || got.Status != http.StatusConflict || got.Code != "call_in_progress" {
		t.Fatalf("旧 try 在途时新 attempt 的同 ID 请求得到 %+v %v，期望 409 call_in_progress", got, err)
	}
	if _, err := os.Stat(h.gwSocket(in1.AttemptID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("旧 attempt 的 socket 应已撤销：%v", err)
	}

	// 放行旧 try：它结算并写入 journal；attempt 2 的 Worker（SDK 对 call_in_progress 以同一 ID 有界重试）随后
	// 命中重放，提交 checkpoint c1（refs 含结果 blob）并结束。此后不再从测试侧发请求：attempt 2 结束时入口被撤销。
	fu.Release()
	v := h.waitTerminal(id)
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	tries := gwTries(t, h.dsn, id, e2eCallID)
	if v.Status != "succeeded" || len(tries) != 1 || tries[0].AttemptID != in1.AttemptID || tries[0].Outcome != "ok" ||
		tries[0].Reservation != "settled" || c.State != "completed" || c.FirstAttempt != in1.AttemptID || c.Tries != 1 {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；期望旧 attempt 的唯一 try 以 ok 结算、调用 completed", v.Status, c, tries)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个请求，重放不应访问上游", fu.Count(fakeupstream.Chat))
	}
	in := h.inspect(id)
	a2 := attemptByNo(in, 2)
	if a1 := attemptByNo(in, 1); a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("attempts %+v", in.Attempts)
	}
	// inspect 的调用审计：completed、source upstream、1 个 try（旧 attempt 的），结果为同一 blob。
	var cv api.CallView
	for _, x := range in.Calls {
		if x.CallID == e2eCallID {
			cv = x
		}
	}
	if cv.State != "completed" || cv.Source != "upstream" || cv.TriesUsed != 1 || len(cv.Tries) != 1 ||
		cv.Tries[0].AttemptID != in1.AttemptID || cv.ResultRef != c.ResultRef {
		t.Fatalf("inspect 的调用 %+v", cv)
	}
	// 重放：attempt 2 的 Worker 得到同一结果 blob（它在 checkpoint c1 的 refs 中），而没有新 try。
	var refs []string
	var refsJSON string
	pgQueryRow(t, h.dsn, "SELECT refs_json::text FROM checkpoints WHERE attempt_id = $1 AND step_id = 'c1'", []any{a2.AttemptID}, &refsJSON)
	if err := json.Unmarshal([]byte(refsJSON), &refs); err != nil || !slices.Contains(refs, c.ResultRef) {
		t.Fatalf("attempt 2 的 checkpoint refs %s 应含结果 blob %s（%v）", refsJSON, c.ResultRef, err)
	}
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if reserved != 0 || unknown != 0 || spent != tries[0].Cost {
		t.Fatalf("账本 reserved=%d spent=%d unknown=%d，try 费用 %d", reserved, spent, unknown, tries[0].Cost)
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E19：tries %+v（attempt 1 = %s）；新 attempt 409 后重放 blob %s；上游请求 %d 个；账本 spent=%d",
		tries, in1.AttemptID, c.ResultRef, fu.Count(fakeupstream.Chat), spent)
	h.finish()
}

// TestE20CancelDuringCall：E20——上游调用在途（请求已到达上游）时用户取消 → 取消立即撤销 Gateway 入口：
// 在途 try 被取消，已发出因此按 unknown 结算（估算转入 unknown）；旧 Worker 的现有连接被关闭，socket 删除，
// 之后任何端点（同 ID 重放、/v1/budget）的新连接都被拒绝。
func TestE20CancelDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("e20", spec(nil, "e20", nil, chatStep("s1")))
	eventually(t, "调用到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	in1, _ := h.rec(id).init(1)
	sock := h.gwSocket(in1.AttemptID)

	// 一条已建立的空闲连接：取消前 /v1/budget 可用，并看到在途 try 的预留。
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	req, err := http.NewRequest(http.MethodGet, "http://gateway/v1/budget", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close() // 正文已读完，关闭错误无关紧要
	var bud struct {
		ReservedMicro int64 `json:"reserved_micro"`
	}
	if err != nil || resp.StatusCode != http.StatusOK || json.Unmarshal(body, &bud) != nil || bud.ReservedMicro <= 0 {
		t.Fatalf("取消前 /v1/budget = %d %s %v", resp.StatusCode, body, err)
	}

	if st, code := h.cancelTask(id, "e20-cancel"); st != http.StatusOK {
		t.Fatalf("取消 = %d %s", st, code)
	}
	// 现有连接被关闭：读到 EOF 或连接错误（期限只是失败判定）。
	if err := conn.SetReadDeadline(time.Now().Add(waitLimit)); err != nil {
		t.Fatal(err)
	}
	_, rerr := br.ReadByte()
	var ne net.Error
	if rerr == nil || (errors.As(rerr, &ne) && ne.Timeout()) {
		t.Fatalf("取消后旧连接仍然打开：%v", rerr)
	}
	eventually(t, "旧 attempt 的 socket 被删除", func() bool {
		_, err := os.Stat(sock)
		return errors.Is(err, fs.ErrNotExist)
	})
	for _, probe := range []struct{ method, path, callID, body string }{
		{http.MethodPost, "/v1/chat/completions", e2eCallID, chatBody(gwMessages)},
		{http.MethodGet, "/v1/budget", "", ""},
	} {
		if got, err := gwDo(sock, probe.method, probe.path, probe.callID, probe.body); err == nil {
			t.Fatalf("取消后 %s %s 的新连接应被拒绝，得到 %+v", probe.method, probe.path, got)
		}
	}

	eventually(t, "在途的上游请求被取消", func() bool {
		reqs := fu.Requests(fakeupstream.Chat)
		return len(reqs) == 1 && reqs[0].Outcome == fakeupstream.OutcomeAborted
	})
	v := h.waitTerminal(id)
	eventually(t, "在途 try 结算", func() bool {
		tr := gwTries(t, h.dsn, id, e2eCallID)
		return len(tr) == 1 && tr[0].Outcome != ""
	})
	tries := gwTries(t, h.dsn, id, e2eCallID)
	c := gwCallRow(t, h.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, h.dsn, id)
	if v.Status != "cancelled" || tries[0].Outcome != "unknown" || tries[0].Reservation != "charged_unknown" ||
		c.State != "unknown" || !c.PED || reserved != 0 || spent != 0 || unknown != tries[0].Amount {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；账本 reserved=%d spent=%d unknown=%d；期望已发出的 try 按 unknown 结算",
			v.Status, c, tries, reserved, spent, unknown)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个请求", fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E20：取消前预留 %d；try %+v；账本 unknown=%d；旧连接 %v；上游 %q",
		bud.ReservedMicro, tries[0], unknown, rerr, outcomes(fu.Requests(fakeupstream.Chat)))
	h.finish()
}

// TestSearchViaFakeUpstream：搜索供应商 fake 配置了 SearchBaseURL 时，Worker 的搜索经 Gateway（验证 dialer，主机
// 显式放行）到达 fake upstream 的 /search：上游恰收到一次请求（不带 Key），Worker 得到上游给出的结果（URL 指向
// fake upstream 的页面），调用以 ok 结算、结果授权到任务 scope。
func TestSearchViaFakeUpstream(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newHarness(t)
	h.start(gatewayCfg(fu, gwSecret(), call.Limits{}))
	id := h.submit("search", spec(nil, "search", nil,
		step{"op": "search", "step_id": "q1", "query": "固态电池", "max_results": 3}))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	const callID = "root/q1/search/1"
	c := gwCallRow(t, h.dsn, id, callID)
	tries := gwTries(t, h.dsn, id, callID)
	reqs := fu.Requests(fakeupstream.Search)
	if c.State != "completed" || len(tries) != 1 || tries[0].Outcome != "ok" || len(reqs) != 1 || reqs[0].Authorization != "" {
		t.Fatalf("调用 %+v；tries %+v；上游搜索请求 %+v", c, tries, reqs)
	}
	var res struct {
		Results []struct {
			URL     string `json:"url"`
			Snippet string `json:"snippet"`
		} `json:"results"`
	}
	if err := json.Unmarshal(h.readBlob(c.ResultRef), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 3 || !strings.HasPrefix(res.Results[0].URL, fu.URL()+"/pages/") || res.Results[0].Snippet == "" {
		t.Fatalf("搜索结果 %+v，期望来自 fake upstream 的 3 条", res.Results)
	}
	t.Logf("搜索经 fake upstream：上游请求 %d 个；结果 %+v", fu.Count(fakeupstream.Search), res.Results)
	h.finish()
}

// currentAttempt 返回任务的当前 attempt（没有时为空）。
func (s *sysHarness) currentAttempt(taskID string) string {
	s.t.Helper()
	var a *string
	pgQueryRow(s.t, s.dsn, "SELECT current_attempt_id FROM tasks WHERE task_id = $1", []any{taskID}, &a)
	if a == nil {
		return ""
	}
	return *a
}

// waitServerConnsGone 等待被杀死的 server 的数据库连接全部结束：它已发出的 COMMIT 仍可能完成，之后的事实才稳定。
func (s *sysHarness) waitServerConnsGone() {
	s.t.Helper()
	eventuallyWithin(s.t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
}

// killAtCallInFlight 以 faultinject 点 call.in_flight 启动 server、提交一个 chat 任务，等待 server 在调用 A
// （Worker 的 root/s1/chat/1）的上游 try 返回之后、结算之前被 SIGKILL，并等待其数据库连接全部结束。
// before 非空时调用方须让 A 在上游挂起：本函数等 A 到达上游后执行 before，再放行 A；为空时 A 直接返回。
func (s *sysHarness) killAtCallInFlight(fu *fakeupstream.Server, flags []string, before func(id string)) (string, *serverProc) {
	s.t.Helper()
	first := s.start(faultinject.CallInFlight+":1", flags...)
	id := s.submit("e48", spec(nil, "e48", nil, chatStep("s1")))
	if before != nil {
		eventually(s.t, "调用 A 到达上游并挂起", func() bool { return fu.Hanging() == 1 })
		before(id)
		fu.Release() // A 的上游请求返回 → 结算之前到达 call.in_flight → SIGKILL
	}
	ps := s.waitExit(first)
	if !killedBySIGKILL(ps) || !strings.Contains(first.logs.tail(1<<20), "faultinject: SIGKILL at "+faultinject.CallInFlight+":1") {
		s.t.Fatalf("server 应在 %s 被 SIGKILL，退出状态 %v", faultinject.CallInFlight, ps)
	}
	s.waitServerConnsGone()
	return id, first
}

// assertRestartConverted：重启后的账本转换把被杀死时在途的 try 按 unknown 结算（server_restart）、预留转入 unknown，
// 调用 A 不再是 in_flight；deadline_at 与重启前相同。
func assertRestartConverted(t *testing.T, s *sysHarness, id string, a0 gwCall) gwTry {
	t.Helper()
	tries := gwTries(t, s.dsn, id, e2eCallID)
	if len(tries) == 0 || tries[0].Outcome != "unknown" || tries[0].Error != "server_restart" || tries[0].Reservation != "charged_unknown" {
		t.Fatalf("重启后 try 1 应按 unknown（server_restart）结算：%+v", tries)
	}
	if a := gwCallRow(t, s.dsn, id, e2eCallID); a.State == "in_flight" || !a.PED || !a.Deadline.Equal(a0.Deadline) {
		t.Fatalf("重启后调用 A %+v；期望不再 in_flight、possible_external_duplicate、deadline_at 不变（%s）", a, a0.Deadline)
	}
	if !strings.Contains(s.srv.logs.tail(1<<30), `"msg":"账本转换","reservations":1,"calls":1`) {
		t.Fatal("重启后的 server 日志缺少账本转换的计数（1 笔预留、1 个调用）")
	}
	return tries[0]
}

// TestE48ServerKilledDuringCall：E48——调用进行中 SIGKILL server（faultinject 点 call.in_flight：Tx2 已提交、
// 上游请求已发出并返回、尚未结算；fake upstream 确认收到请求），等两个调用都超过 deadline_at 后重启：
//   - 启动账本转换（§14.1 第 4 步）把在途调用 A 的 try 按 unknown 结算、held 预留转入 unknown；deadline_at 不变；
//   - 遗留的 resolving 调用 B（同一 attempt 的另一调用，因每任务在途上限 1 等待槽位）被复位为可重新解析，期限不变；
//   - 新 attempt 的 Worker 以同 ID 请求 A 得到 504 call_deadline_exceeded（不再访问上游），任务失败。
//
// 复位调用在期限之后被接管时得到 call_deadline_exceeded 由 TestResetResolving（存储层）覆盖：新 attempt 的 Worker
// 收到 504 后立即失败，测试无法确定地在它结束之前另行连接该 attempt 的 socket。
func TestE48ServerKilledDuringCall(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	fu.Inject(fakeupstream.Chat, 1, fakeupstream.Action{Hang: true})
	const deadline = 15 * time.Second
	flags := []string{"--fake-upstream", fu.URL(), "--model-call-deadline", deadline.String(), "--gateway-per-task-inflight", "1"}
	bDone := make(chan error, 1)
	id, first := s.killAtCallInFlight(fu, flags, func(id string) {
		// 调用 B：同一 attempt 的另一调用，等待 A 占用的每任务在途槽位，停在 resolving（Tx1 已提交、没有 try）。
		sock := s.gwSocket(s.currentAttempt(id))
		go func() {
			_, err := gwDo(sock, http.MethodPost, "/v1/chat/completions", probeCallID, chatBody(probeMessages))
			bDone <- err
		}()
		eventually(t, "调用 B 登记为 resolving 并等待槽位", func() bool {
			var n int
			pgQueryRow(t, s.dsn, `SELECT count(*) FROM calls WHERE task_id = $1 AND call_id = $2 AND state = 'resolving'
				AND resolving_since IS NOT NULL`, []any{id, probeCallID}, &n)
			return n == 1
		})
	})
	select {
	case err := <-bDone:
		if err == nil {
			t.Fatal("调用 B 不应在 server 被杀死之前得到响应")
		}
	case <-time.After(waitLimit):
		t.Fatal("server 被杀死后调用 B 的连接未结束")
	}
	a0, b0 := gwCallRow(t, s.dsn, id, e2eCallID), gwCallRow(t, s.dsn, id, probeCallID)
	tries0 := gwTries(t, s.dsn, id, e2eCallID)
	reqs := fu.Requests(fakeupstream.Chat)
	if a0.State != "in_flight" || a0.Tries != 1 || len(tries0) != 1 || tries0[0].Reservation != "held" ||
		b0.State != "resolving" || b0.ResolvingSince == nil || b0.Tries != 0 ||
		len(reqs) != 1 || reqs[0].Outcome != fakeupstream.OutcomeOK {
		t.Fatalf("被杀死时：A %+v tries %+v；B %+v；上游 %q；期望 A in_flight（已发出并返回、未结算）、B resolving",
			a0, tries0, b0, outcomes(reqs))
	}

	// 两个调用都超过 deadline_at（按数据库时间）之后再重启：新 attempt 的同 ID 请求必然在期限之后到达。
	eventuallyWithin(t, "两个调用都超过 deadline_at", deadline+waitLimit, 100*time.Millisecond, func() bool {
		var n int
		pgQueryRow(t, s.dsn, "SELECT count(*) FROM calls WHERE task_id = $1 AND now() >= deadline_at", []any{id}, &n)
		return n == 2
	})
	s.start("", flags...)
	try1 := assertRestartConverted(t, s, id, a0)
	if b1 := gwCallRow(t, s.dsn, id, probeCallID); b1.State != "resolving" || b1.ResolvingSince != nil || !b1.Deadline.Equal(b0.Deadline) {
		t.Fatalf("遗留的 resolving 调用应被复位（resolving_since 清空、期限不变）：%+v，原为 %+v", b1, b0)
	}

	v := s.waitTerminal(id)
	in := s.inspect(id)
	a2 := attemptByNo(in, 2)
	var got504 bool
	for _, e := range s.events(id) {
		got504 = got504 || (e.AttemptID == a2.AttemptID && e.Source == "worker" && strings.Contains(string(e.Payload), "call_deadline_exceeded"))
	}
	a := gwCallRow(t, s.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if v.Status != "failed" || !got504 || a.State != "unknown" || a.Tries != 1 || !a.Deadline.Equal(a0.Deadline) ||
		reserved != 0 || spent != 0 || unknown != try1.Amount || fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("任务 %s（attempt 2 的 Worker 收到 504：%v）；调用 A %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
			v.Status, got504, a, reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, first.logs.tail(1<<30)+s.srv.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E48（超期后重启）：deadline_at A=%s B=%s（重启前后相同）；try 1 %+v；新 attempt 的同 ID 请求 → 504；任务 %s/%s；"+
		"账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
		a0.Deadline.Format(time.RFC3339Nano), b0.Deadline.Format(time.RFC3339Nano), try1, v.Status, v.StatusReason,
		reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	s.finish()
}

// TestE48RestartBeforeDeadline：E48 的另一半——期限之内重启：账本转换把在途 try 按 unknown 结算后，新 attempt
// 的同 ID 请求在累计上限内新建 try 2（上游第二次收到同一请求体），调用完成，任务成功；deadline_at 不变；
// 账本 unknown = try 1 的估算、spent = try 2 的实际费用。
func TestE48RestartBeforeDeadline(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	flags := []string{"--fake-upstream", fu.URL()}
	id, first := s.killAtCallInFlight(fu, flags, nil)
	a0 := gwCallRow(t, s.dsn, id, e2eCallID)
	if a0.State != "in_flight" || fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("被杀死时调用 A %+v，上游请求 %d 个", a0, fu.Count(fakeupstream.Chat))
	}
	s.start("", flags...)
	try1 := assertRestartConverted(t, s, id, a0)
	v := s.waitTerminal(id)
	in := s.inspect(id)
	tries := gwTries(t, s.dsn, id, e2eCallID)
	a := gwCallRow(t, s.dsn, id, e2eCallID)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if v.Status != "succeeded" || len(tries) != 2 || tries[1].Outcome != "ok" || tries[1].Reservation != "settled" ||
		tries[0].AttemptID != attemptByNo(in, 1).AttemptID || tries[1].AttemptID != attemptByNo(in, 2).AttemptID ||
		a.State != "completed" || a.Tries != 2 || !a.PED || !a.Deadline.Equal(a0.Deadline) ||
		reserved != 0 || unknown != try1.Amount || spent != tries[1].Cost ||
		fu.Count(fakeupstream.Chat) != 2 || fu.Distinct(fakeupstream.Chat) != 1 {
		t.Fatalf("任务 %s；调用 %+v；tries %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个（不同请求体 %d）",
			v.Status, a, tries, reserved, spent, unknown, fu.Count(fakeupstream.Chat), fu.Distinct(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, first.logs.tail(1<<30)+s.srv.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E48（期限内重启）：deadline_at %s 不变；tries %+v；账本 reserved=%d spent=%d unknown=%d；上游请求 %d 个",
		a0.Deadline.Format(time.RFC3339Nano), tries, reserved, spent, unknown, fu.Count(fakeupstream.Chat))
	s.finish()
}

// TestE11bReservationCommitLost：E11b——ReserveTry 的 COMMIT 已执行而回复丢失（faultinject 点 reservation.commit，
// 经同一 reservation_id 重跑事务体，与提交结果未知后的重跑相同）→ 解析为原 try：恰一笔预留、一个 try，
// 上游只收到一次请求，调用正常完成；账本与 I3 一致。
func TestE11bReservationCommitLost(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	s := newSys(t)
	key := gwSecret()
	t.Setenv(modelKeyEnv, key)
	p := s.start(faultinject.ReservationCommit+":1", "--fake-upstream", fu.URL())
	id := s.submit("e11b", spec(nil, "e11b", nil, chatStep("s1")))
	v := s.waitTerminal(id)
	if v.Status != "succeeded" || !strings.Contains(p.logs.tail(1<<30), "faultinject: reply lost at "+faultinject.ReservationCommit+":1") {
		t.Fatalf("任务 %+v；server 日志应记录 %s 的回复丢失", v, faultinject.ReservationCommit)
	}
	tries := gwTries(t, s.dsn, id, e2eCallID)
	c := gwCallRow(t, s.dsn, id, e2eCallID)
	var reservations int
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM reservations WHERE task_id = $1", []any{id}, &reservations)
	reserved, spent, unknown := gwBudget(t, s.dsn, id)
	if reservations != 1 || len(tries) != 1 || tries[0].Outcome != "ok" || tries[0].Reservation != "settled" ||
		c.State != "completed" || c.Tries != 1 || reserved != 0 || unknown != 0 || spent != tries[0].Cost {
		t.Fatalf("预留 %d 笔；tries %+v；调用 %+v；账本 reserved=%d spent=%d unknown=%d；期望恰一笔预留与一个 ok try",
			reservations, tries, c, reserved, spent, unknown)
	}
	if n := fu.Count(fakeupstream.Chat); n != 1 {
		t.Fatalf("上游收到 %d 个请求，期望 1", n)
	}
	assertKeyOnlyUpstream(t, key, p.logs.tail(1<<30), s.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("E11b：预留 %d 笔；try %+v；账本 spent=%d；上游请求 %d 个", reservations, tries[0], spent, fu.Count(fakeupstream.Chat))
	s.finish()
}

// ---- M2：Gateway 真实沙箱冒烟与 G3（Plan 8 Task 4）----
//
// 真实沙箱（provider/local + 生产启动器，newRealHarness）中运行 sim_worker：chat 经沙箱内的
// /run/agentbox/gateway.sock 到达进程内 Gateway，再到 fake upstream。Worker 暂停在 checkpoint 上（Hold）时，
// 测试从宿主读取环境 cgroup 中各进程，并在同一环境中另起一个 workload 进程（与 E3 相同的 provider StartExec，
// uid 1000、同一 namespace）从沙箱内部观察。sim_worker 的 print 操作只能输出固定文本，无法输出环境，因此
// 沙箱内的 env 与 /proc/<pid>/environ 由该进程输出（G3 见 TestRealNoCredentialsInSandbox）。

const searchKeyEnv = "AGENTBOX_SEARCH_API_KEY" // 宿主上搜索供应商 Key 的环境变量（cmd/agentbox 只从这里读取）

// sandboxProcs 返回环境 cgroup 中的全部进程：宿主 pid → cmdline（NUL 换成空格）。
func (h *harness) sandboxProcs(envID string) map[int]string {
	out := map[int]string{}
	for _, pid := range cgroupPids(h.envCgroup(envID)) {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
			out[pid] = strings.TrimSpace(string(bytes.ReplaceAll(b, []byte{0}, []byte{' '})))
		}
	}
	return out
}

// workerPid 返回环境中 Worker 主进程（python3 -m <module>）的宿主 pid。
func (h *harness) workerPid(envID, module string) int {
	h.t.Helper()
	for pid, cmd := range h.sandboxProcs(envID) {
		if strings.Contains(cmd, "-m "+module) {
			return pid
		}
	}
	h.t.Fatalf("环境 %s 中没有 %s 进程：%v", envID, module, h.sandboxProcs(envID))
	return 0
}

// procUID 返回宿主进程 pid 的真实 uid（/proc/<pid>/status 的 Uid 行）。
func procUID(t *testing.T, pid int) uint32 {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "Uid:" {
			n, err := strconv.ParseUint(f[1], 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			return uint32(n)
		}
	}
	t.Fatalf("/proc/%d/status 没有 Uid 行", pid)
	return 0
}

// sandboxExec 在环境中以 workload 身份（与 Worker 相同的沙箱）运行 python3 -c script，返回 stdout；
// 退出状态非 0 时失败。
func (h *harness) sandboxExec(envID, execID string, env []string, script string) string {
	h.t.Helper()
	hd, err := h.local.StartExec(context.Background(), envID, provider.ExecSpec{ExecID: execID, Dir: "/tmp", Env: env,
		Argv: []string{"python3", "-c", script}})
	if err != nil {
		h.t.Fatalf("在环境中启动 %s: %v", execID, err)
	}
	if err := hd.Stdin().Close(); err != nil {
		h.t.Fatal(err)
	}
	var stdout, stderr lockedBuffer
	done := make(chan struct{}, 2)
	for _, p := range []struct {
		dst *lockedBuffer
		src io.Reader
	}{{&stdout, hd.Stdout()}, {&stderr, hd.Stderr()}} {
		go func() {
			_, _ = io.Copy(p.dst, p.src) // 读到 EOF 或管道关闭为止；内容由下方断言检查
			done <- struct{}{}
		}()
	}
	st, err := hd.Wait()
	for range 2 {
		select {
		case <-done:
		case <-time.After(waitLimit):
			h.t.Fatalf("%s 的输出未结束", execID)
		}
	}
	if err != nil || st.Code != 0 || st.Signal != 0 {
		h.t.Fatalf("%s 退出 %+v %v；stderr：\n%s", execID, st, err, stderr.tail(4<<10))
	}
	return stdout.tail(1 << 30)
}

// checkpointRefs 返回任务 scope 中 step_id 的 checkpoint 的 refs。
func (h *harness) checkpointRefs(taskID, stepID string) []string {
	h.t.Helper()
	var raw []byte
	h.queryRow("SELECT refs_json FROM checkpoints WHERE scope_kind = 'task' AND scope_id = $1 AND step_id = $2",
		[]any{taskID, stepID}, &raw)
	var refs []string
	if err := json.Unmarshal(raw, &refs); err != nil {
		h.t.Fatalf("checkpoint %s 的 refs_json %s: %v", stepID, raw, err)
	}
	return refs
}

// assertNoSecret：text 中不含任何 secret（只报告位置，不在失败信息中回显 Key）。
func assertNoSecret(t *testing.T, where, text string, secrets ...string) {
	t.Helper()
	for i, s := range secrets {
		if s == "" {
			t.Fatalf("第 %d 个 Key 为空：G3 须在配置了非空 Key 时验证", i+1)
		}
		if strings.Contains(text, s) {
			t.Fatalf("第 %d 个 Key 出现在%s中", i+1, where)
		}
	}
}

// TestRealGatewayChat：Gateway 真实沙箱冒烟——sim_worker 的 chat 经沙箱内 /run/agentbox/gateway.sock → Gateway →
// fake upstream 返回脚本化回复 → checkpoint 的 refs 带该结果 blob → result。socket 在宿主上属主为 Worker 的映射
// uid、0600，沙箱内为 uid 1000、0600 的 socket；inspect 显示 1 个 completed 调用、1 次 ok try。
func TestRealGatewayChat(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	key := gwSecret()
	h.start(gatewayCfg(fu, key, call.Limits{}))
	id := h.submit("gw-chat", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "gw chat", nil,
		chatStep("s1"), checkpointStep("c1"), progressStep("after chat")))
	h.waitHeld(id)
	in1, _ := h.rec(id).init(1)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	wuid := procUID(t, h.workerPid(envID, "sim_worker"))
	fi, err := os.Lstat(h.gwSocket(in1.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || fi.Mode().Type() != fs.ModeSocket || fi.Mode().Perm() != 0o600 || st.Uid != wuid || wuid == 0 {
		t.Fatalf("宿主上的 Gateway socket mode %v uid %v；期望 Worker 的映射 uid %d、0600 的 socket", fi.Mode(), fi.Sys(), wuid)
	}
	inside := strings.TrimSpace(h.sandboxExec(envID, "gw-stat", nil,
		"import os, stat\ns = os.stat('/run/agentbox/gateway.sock')\n"+
			"print(os.getuid(), s.st_uid, oct(stat.S_IMODE(s.st_mode)), stat.S_ISSOCK(s.st_mode))"))
	if inside != "1000 1000 0o600 True" {
		t.Fatalf("沙箱内 /run/agentbox/gateway.sock：%q；期望 uid 1000 的进程看到属主 1000、0600 的 socket", inside)
	}
	h.rec(id).releaseHold()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	if len(in.Calls) != 1 {
		t.Fatalf("inspect 中有 %d 个调用：%+v", len(in.Calls), in.Calls)
	}
	c := in.Calls[0]
	if c.CallID != e2eCallID || c.Endpoint != "/v1/chat/completions" || c.State != "completed" || c.TriesUsed != 1 ||
		len(c.Tries) != 1 || c.Tries[0].Outcome != "ok" || c.Tries[0].AttemptID != in1.AttemptID || c.ResultRef == "" {
		t.Fatalf("调用 %+v；期望 1 个 completed 调用、1 次 ok try", c)
	}
	if refs := h.checkpointRefs(id, "c1"); !slices.Contains(refs, c.ResultRef) {
		t.Fatalf("checkpoint c1 的 refs %v 不含结果 blob %s", refs, c.ResultRef)
	}
	var reply fakeupstream.ChatReply
	if err := json.Unmarshal(h.readBlob(c.ResultRef), &reply); err != nil {
		t.Fatal(err)
	}
	if len(reply.Choices) != 1 || !strings.Contains(reply.Choices[0].Message.Content, "fake 任务") {
		t.Fatalf("结果 blob 中的回复 %+v，期望 fake upstream 的计划阶段脚本回复", reply)
	}
	if fu.Count(fakeupstream.Chat) != 1 {
		t.Fatalf("上游收到 %d 个 chat 请求", fu.Count(fakeupstream.Chat))
	}
	assertKeyOnlyUpstream(t, key, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("Gateway 冒烟：宿主 socket uid %d 0600，沙箱内 %q；调用 %s → %s（try %+v），blob %s 在 checkpoint c1 的 refs 中",
		wuid, inside, c.CallID, c.State, c.Tries[0], c.ResultRef)
	h.finish()
}

// g3Script 在沙箱内输出自身的环境（os.environ 即 env）、/proc/self/environ，以及沙箱内可见的每个进程的
// cmdline 与 /proc/<pid>/environ（Worker 与该进程同为 uid 1000，可读；不可读的记录错误）。
const g3Script = `import os
print("ENV", sorted(os.environ.items()))
print("SELF", open("/proc/self/environ", "rb").read().split(b"\0"))
for p in sorted(os.listdir("/proc")):
    if not p.isdigit():
        continue
    try:
        cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        env = open(f"/proc/{p}/environ", "rb").read().split(b"\0")
        print("PROC", p, "[" + cmd + "]", len(env), env)
    except OSError as e:
        print("PROC", p, "unreadable", e.errno)
`

// TestRealNoCredentialsInSandbox：G3（§9.9）——配置了非空的模型与搜索 Key（宿主环境变量 AGENTBOX_MODEL_API_KEY、
// AGENTBOX_SEARCH_API_KEY 也设为同一值，与 cmd/agentbox 的来源相同；Worker 启动器继承本进程环境时会泄漏）时，
// Worker 经 Gateway 完成 chat 与搜索，而：
//   - 环境 cgroup 中每个进程（init、helper、Worker）的 /proc/<pid>/environ 与 cmdline（宿主读取）不含 Key 的值，
//     也没有这两个变量名；
//   - 沙箱内 uid 1000 进程输出的 env、/proc/self/environ 与可见进程（含 Worker）的 /proc/<pid>/environ 不含 Key；
//   - server 日志与整个事件表（payload）不含 Key；Key 只出现在发往模型上游的 Authorization 头中。
func TestRealNoCredentialsInSandbox(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	modelKey, searchKey := gwSecret(), gwSecret()
	t.Setenv(modelKeyEnv, modelKey)
	t.Setenv(searchKeyEnv, searchKey)
	cfg := gatewayCfg(fu, modelKey, call.Limits{})
	cfg.SearchAPIKey = searchKey
	h.start(cfg)
	id := h.submit("g3", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "g3", nil,
		chatStep("s1"),
		step{"op": "search", "step_id": "q1", "query": "固态电池", "max_results": 3},
		step{"op": "print", "message": "g3: 已完成 chat 与搜索"},
		checkpointStep("c1")))
	h.waitHeld(id)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	wpid := h.workerPid(envID, "sim_worker")
	procs := h.sandboxProcs(envID)
	var workerEnv []string
	for pid, cmd := range procs {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if err != nil {
			t.Fatalf("读取沙箱进程 %d（%s）的 environ: %v", pid, cmd, err)
		}
		where := fmt.Sprintf("沙箱进程 %d（%s）的 environ 或 cmdline", pid, cmd)
		assertNoSecret(t, where, string(b)+"\n"+cmd, modelKey, searchKey)
		if bytes.Contains(b, []byte(modelKeyEnv)) || bytes.Contains(b, []byte(searchKeyEnv)) {
			t.Fatalf("%s 含 Key 的变量名", where)
		}
		if pid == wpid {
			workerEnv = strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		}
	}
	// 沙箱内部：以 Worker 的环境运行观察进程（它看到的就是沙箱给 workload 的全部环境）。
	out := h.sandboxExec(envID, "g3-env", workerEnv, g3Script)
	assertNoSecret(t, "沙箱内的 env、/proc/self/environ 或进程 environ", out, modelKey, searchKey)
	var sawWorker bool
	for _, line := range strings.Split(out, "\n") {
		sawWorker = sawWorker || (strings.HasPrefix(line, "PROC ") && strings.Contains(line, "-m sim_worker]") &&
			strings.Contains(line, "PYTHONPATH="))
	}
	if !strings.HasPrefix(out, "ENV ") || !strings.Contains(out, "\nSELF ") || !sawWorker {
		t.Fatalf("沙箱内的观察输出缺少 env、/proc/self/environ 或 Worker 的 environ：\n%s", out)
	}
	h.rec(id).releaseHold()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	if len(in.Calls) != 2 || in.Calls[0].State != "completed" || in.Calls[1].State != "completed" ||
		fu.Count(fakeupstream.Chat) != 1 || fu.Count(fakeupstream.Search) != 1 {
		t.Fatalf("调用 %+v；上游 chat %d、search %d 个请求", in.Calls, fu.Count(fakeupstream.Chat), fu.Count(fakeupstream.Search))
	}
	assertNoSecret(t, "server 日志", h.logs.tail(1<<30), modelKey, searchKey)
	var leaked, total int
	for _, k := range []string{modelKey, searchKey} {
		var n int
		h.queryRow("SELECT count(*) FROM events WHERE strpos(payload::text, $1) > 0", []any{k}, &n)
		leaked += n
	}
	h.queryRow("SELECT count(*) FROM events", nil, &total)
	if leaked != 0 || total == 0 {
		t.Fatalf("事件表 %d 行中有 %d 行含 Key", total, leaked)
	}
	assertKeyOnlyUpstream(t, modelKey, h.logs.tail(1<<30), h.events(id), fu.Requests(fakeupstream.Chat))
	t.Logf("G3：沙箱进程 %d 个（%v）的 environ 与 cmdline、沙箱内观察输出 %d 字节、server 日志 %d 字节、事件表 %d 行均不含 Key；"+
		"Worker 环境 %v", len(procs), slices.Collect(maps.Values(procs)), len(out), len(h.logs.tail(1<<30)), total, workerEnv)
	h.finish()
}

// ---- M2：DeepResearch 业务验收（fake upstream，真实沙箱；Plan 8 Task 5，§16.4 自动化业务验收）----
//
// 真实沙箱中运行 python3 -m deepresearch，fake upstream 按阶段标记回复固定研究题目（2 个任务、每次搜索 3 条
// 结果、固定页面）。断言研究步骤（checkpoint plan、task-1、task-2、report）、report 产物、报告引用 ↔ 已保存的
// 证据 blob，以及杀死 Worker 后从已提交的 checkpoint 恢复且已完成的调用不再到达上游。每条用例以 finish（静止时
// 的不变量检查，即 verify-invariants --quiescent 的同一检查，含 I3、I14）结束。

const researchTopic = "固态电池的产业化进展"

var researchTasks = []fakeupstream.ResearchTask{
	{Title: "电解质路线", Intent: "比较硫化物、氧化物与聚合物电解质", Query: "固态电解质 技术路线"},
	{Title: "量产与成本", Intent: "梳理量产时间表与成本结构", Query: "固态电池 量产 成本"},
}

// researchMaxResults 是研究配置的 max_results（搜索请求体的一部分；直接重放探针须发送同一请求体）。
const researchMaxResults = 3

// researchCfg 是 deepresearch Worker 的 Gateway 配置（WorkerArgv 为 python3 -m deepresearch）。
func researchCfg(fu *fakeupstream.Server) app.Config {
	fu.SetResearch(researchTasks)
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.WorkerArgv = []string{"python3", "-m", "deepresearch"}
	return cfg
}

func researchSpec(d *directives) string {
	m := map[string]any{"topic": researchTopic, "max_tasks": 4, "max_results": researchMaxResults, "max_fetch": 3}
	if d != nil {
		m["e2e"] = d
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// upstreamCounts 是 fake upstream 的计数快照：阶段（plan、summarize、report）与类别（search、fetch）。
type upstreamCounts struct{ Plan, Summarize, Report, Search, Fetch int }

func countsOf(fu *fakeupstream.Server) upstreamCounts {
	return upstreamCounts{Plan: fu.StageCount(fakeupstream.StagePlan), Summarize: fu.StageCount(fakeupstream.StageSummarize),
		Report: fu.StageCount(fakeupstream.StageReport), Search: fu.Count(fakeupstream.Search), Fetch: fu.Count(fakeupstream.Fetch)}
}

// 完整研究的上游请求：1 次计划、每个任务 1 次搜索 + 3 次抓取 + 1 次摘要、1 次报告。
var researchFull = upstreamCounts{Plan: 1, Summarize: 2, Report: 1, Search: 2, Fetch: 6}

func stepOrder(in api.Inspection) (steps, attempts []string) {
	cps := slices.Clone(in.Checkpoints)
	slices.SortFunc(cps, func(a, b api.CheckpointView) int { return int(a.CommitSeq - b.CommitSeq) })
	for _, c := range cps {
		steps = append(steps, c.StepID)
		attempts = append(attempts, c.AttemptID)
	}
	return steps, attempts
}

// citation 是报告"证据"列表中的一项：[N] → 证据 blob 的 sha256 与来源 URL。
type citation struct {
	N        int
	URL, SHA string
}

var (
	evidenceLine = regexp.MustCompile(`^- \[(\d+)\] .+ — (\S+) — sha256:([0-9a-f]{64})$`)
	citeRef      = regexp.MustCompile(`\[(\d+)\]`)
)

// assertResearchReport：report 产物已保存（事件 artifact_saved(report) 与最新版本一致）；报告非空，正文中的每个 [n]
// 都在末尾"证据"列表中；列表恰为全部 6 条证据（编号 1..6、URL 是固定题目的搜索结果），每个 sha 的 blob 存在
// （BlobStore 内容哈希一致、blobs 表有记录）、授权到任务 scope（scope_blobs）、是抓取该 URL 的结果，并在 report
// checkpoint 的 refs 中。返回列表与正文中引用的编号。
func (h *harness) assertResearchReport(fu *fakeupstream.Server, taskID string) ([]citation, []int) {
	t := h.t
	t.Helper()
	av, err := h.store.LatestArtifact(context.Background(), taskID, "report")
	if err != nil {
		t.Fatalf("report 产物: %v", err)
	}
	var saved bool
	for _, e := range h.events(taskID) {
		var p struct {
			ArtifactID string `json:"artifact_id"`
			SHA256     string `json:"sha256"`
		}
		if e.Type == "artifact_saved" && json.Unmarshal(e.Payload, &p) == nil && p.ArtifactID == "report" && p.SHA256 == av.SHA256 {
			saved = true
		}
	}
	if !saved {
		t.Fatalf("事件中没有 artifact_saved(report, %s)", av.SHA256)
	}
	report := string(h.readBlob(av.SHA256))
	body, list, ok := strings.Cut(report, "\n## 证据\n")
	if !ok || !strings.HasPrefix(report, "# "+researchTopic+"\n") || strings.Contains(body, "参考来源") {
		t.Fatalf("报告结构不符（标题、证据节、模型自写的来源节应被替换）：\n%s", report)
	}
	urls := map[string]bool{}
	for _, task := range researchTasks {
		for i := 1; i <= 3; i++ {
			urls[fu.ResultURL(task.Query, i)] = true
		}
	}
	refs := h.checkpointRefs(taskID, "report")
	var cites []citation
	listed := map[int]bool{}
	for _, line := range strings.Split(strings.TrimSpace(list), "\n") {
		m := evidenceLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("证据列表行 %q 不符合 `- [n] 标题 — URL — sha256:<sha>`", line)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		c := citation{N: n, URL: m[2], SHA: m[3]}
		if c.N != len(cites)+1 || !urls[c.URL] {
			t.Fatalf("证据 %+v：编号应连续、URL 应是固定题目的搜索结果", c)
		}
		var fetched struct {
			URL     string `json:"url"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(h.readBlob(c.SHA), &fetched); err != nil {
			t.Fatalf("证据 [%d] 的 blob 不是抓取结果: %v", c.N, err)
		}
		var inBlobs, inScope int
		h.queryRow("SELECT count(*) FROM blobs WHERE sha256 = $1", []any{c.SHA}, &inBlobs)
		h.queryRow("SELECT count(*) FROM scope_blobs WHERE scope_kind = 'task' AND scope_id = $1 AND sha256 = $2",
			[]any{taskID, c.SHA}, &inScope)
		if fetched.URL != c.URL || !strings.Contains(fetched.Content, c.URL) || inBlobs != 1 || inScope != 1 || !slices.Contains(refs, c.SHA) {
			t.Fatalf("证据 [%d] %s：抓取 URL %q、blobs %d、scope_blobs %d、在 report refs 中 %v",
				c.N, c.SHA, fetched.URL, inBlobs, inScope, slices.Contains(refs, c.SHA))
		}
		listed[c.N] = true
		cites = append(cites, c)
	}
	if len(cites) != 3*len(researchTasks) {
		t.Fatalf("证据列表 %d 条，期望 %d", len(cites), 3*len(researchTasks))
	}
	var cited []int
	for _, m := range citeRef.FindAllStringSubmatch(body, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatal(err)
		}
		if !listed[n] {
			t.Fatalf("正文引用 [%d] 不在证据列表中：\n%s", n, report)
		}
		cited = append(cited, n)
	}
	if len(cited) == 0 {
		t.Fatalf("报告正文没有引用：\n%s", report)
	}
	return cites, cited
}

// assertCallsSettled：全部调用 completed，每个恰 1 次 ok try；返回 call_id → 调用。
func assertCallsSettled(t *testing.T, in api.Inspection, want int) map[string]api.CallView {
	t.Helper()
	calls := map[string]api.CallView{}
	for _, c := range in.Calls {
		if c.State != "completed" || c.TriesUsed != 1 || len(c.Tries) != 1 || c.Tries[0].Outcome != "ok" || c.ResultRef == "" {
			t.Fatalf("调用 %+v；期望 completed、恰 1 次 ok try", c)
		}
		calls[c.CallID] = c
	}
	if len(calls) != want {
		t.Fatalf("inspect 中 %d 个调用，期望 %d：%+v", len(calls), want, in.Calls)
	}
	return calls
}

// researchCalls 是完整研究的调用 ID：计划 1 个，每个任务 1 次搜索、3 次抓取、1 次摘要，报告 1 个。
func researchCalls() []string {
	ids := []string{"root/plan/chat/1"}
	for i := range researchTasks {
		step := fmt.Sprintf("root/task-%d", i+1)
		ids = append(ids, step+"/search/1", step+"/fetch/1", step+"/fetch/2", step+"/fetch/3", step+"/chat/1")
	}
	return append(ids, "root/report/chat/1")
}

// TestRealDeepResearchFixedTopic：固定题目的完整研究——checkpoint 依次为 plan、task-1、task-2、report；
// 事件含 artifact_saved(report)；报告引用均对应已保存、已授权到任务的证据 blob；每个调用恰到达上游一次。
func TestRealDeepResearchFixedTopic(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(researchCfg(fu))
	id := h.submit("dr-fixed", researchSpec(nil))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	steps, _ := stepOrder(in)
	if !slices.Equal(steps, []string{"plan", "task-1", "task-2", "report"}) || len(in.Attempts) != 1 {
		t.Fatalf("checkpoints %v，attempts %+v；期望 plan、task-1、task-2、report 与 1 个 attempt", steps, in.Attempts)
	}
	calls := assertCallsSettled(t, in, len(researchCalls()))
	for _, cid := range researchCalls() {
		if _, ok := calls[cid]; !ok {
			t.Fatalf("缺少调用 %s：%v", cid, slices.Collect(maps.Keys(calls)))
		}
	}
	cites, cited := h.assertResearchReport(fu, id)
	got := countsOf(fu)
	for _, k := range []fakeupstream.Kind{fakeupstream.Chat, fakeupstream.Search, fakeupstream.Fetch} {
		if fu.Count(k) != fu.Distinct(k) {
			t.Fatalf("%s：上游 %d 个请求、%d 个不同请求（不应重发）", k, fu.Count(k), fu.Distinct(k))
		}
	}
	if got != researchFull {
		t.Fatalf("上游计数 %+v，期望 %+v", got, researchFull)
	}
	if _, outs, _ := h.pinnedResult(id, in.Attempts[0].AttemptID); len(outs) != 1 || outs[0].ArtifactID != "report" {
		t.Fatalf("result 的固定输出 %+v", outs)
	}
	t.Logf("固定题目：checkpoints %v；上游计数 %+v；报告引用 %v；证据 %+v", steps, got, cited, cites)
	h.finish()
}

// TestRealDeepResearchKillAndResume：task-1 的 checkpoint 提交后、结果送达 Worker 之前杀死 Worker →
// 新 attempt 的 init.resume.checkpoint_id 为 task-1 的 checkpoint；计划调用的上游计数仍为 1；恢复后任务 1 的
// search、fetch、summarize 不再到达上游：任务 1 的上游计数在恢复时与结束时相同，inspect 中这些调用只有 attempt 1
// 的那一次 try；attempt 2 提交的 checkpoint 的 refs 含任务 1 的 blob；报告引用对应已保存的证据 blob。
//
// 观察点：上游第 2 次搜索（attempt 2 的 task-2 搜索）挂起，此时 attempt 1 已结束、attempt 2 尚未发出其他请求，
// 恢复时的计数快照是确定的。
func TestRealDeepResearchKillAndResume(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	fu.Inject(fakeupstream.Search, 2, fakeupstream.Action{Hang: true})
	h.start(researchCfg(fu))
	// committed 的 checkpoint_result 依次为 plan（第 1 个）、task-1（第 2 个）。
	id := h.submit("dr-kill", researchSpec(&directives{KillAfterCommitted: 2}))
	r := h.rec(id)
	eventually(t, "attempt 2 的 task-2 搜索到达上游并挂起", func() bool { return fu.Hanging() == 1 })
	in2, ok := r.init(2)
	cp1 := checkpointByStep(h.inspect(id), "task-1")
	if !ok || in2.Resume == nil || in2.Resume.StepID != "task-1" || in2.Resume.CheckpointID != cp1.CheckpointID || cp1.CheckpointID == "" {
		t.Fatalf("attempt 2 的 init.resume %+v；期望 task-1 的 checkpoint %+v", in2.Resume, cp1)
	}
	before := countsOf(fu)
	if want := (upstreamCounts{Plan: 1, Summarize: 1, Search: 2, Fetch: 3}); before != want {
		t.Fatalf("恢复后、task-2 搜索挂起时上游计数 %+v，期望 %+v（任务 1 未重发）", before, want)
	}
	t1Before := task1Counts(fu)
	if t1Before != (upstreamCounts{Summarize: 1, Search: 1, Fetch: 3}) {
		t.Fatalf("恢复时任务 1 的上游计数 %+v", t1Before)
	}
	fu.Release()

	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	in := h.inspect(id)
	a1, a2 := attemptByNo(in, 1), attemptByNo(in, 2)
	steps, cpAttempts := stepOrder(in)
	if len(in.Attempts) != 2 || a1.OutcomeClass != runner.ClassCrashedSignal || a2.OutcomeClass != runner.ClassSucceeded ||
		!slices.Equal(steps, []string{"plan", "task-1", "task-2", "report"}) ||
		!slices.Equal(cpAttempts, []string{a1.AttemptID, a1.AttemptID, a2.AttemptID, a2.AttemptID}) {
		t.Fatalf("attempts %s；checkpoints %v（attempt %v）；期望 attempt 1 被杀死于 task-1 之后、attempt 2 提交 task-2 与 report",
			attemptClasses(in), steps, cpAttempts)
	}
	// 计划与任务 1 的调用只有 attempt 1 的那一次 try；任务 2 与报告的调用由 attempt 2 首次发起。
	calls := assertCallsSettled(t, in, len(researchCalls()))
	for cid, c := range calls {
		want := a2.AttemptID
		if cid == "root/plan/chat/1" || strings.HasPrefix(cid, "root/task-1/") {
			want = a1.AttemptID
		}
		if c.FirstAttemptID != want || c.Tries[0].AttemptID != want {
			t.Fatalf("调用 %s 的首个 attempt %s、try 的 attempt %s，期望 %s", cid, c.FirstAttemptID, c.Tries[0].AttemptID, want)
		}
	}
	final := countsOf(fu)
	for _, k := range []fakeupstream.Kind{fakeupstream.Chat, fakeupstream.Search, fakeupstream.Fetch} {
		if fu.Count(k) != fu.Distinct(k) {
			t.Fatalf("%s：上游 %d 个请求、%d 个不同请求（不应重发）", k, fu.Count(k), fu.Distinct(k))
		}
	}
	if final != researchFull {
		t.Fatalf("上游计数 %+v，期望 %+v（计划 1 次、任务 1 不重发）", final, researchFull)
	}
	if t1After := task1Counts(fu); t1After != t1Before {
		t.Fatalf("恢复后任务 1 的上游计数 %+v，恢复时 %+v：任务 1 的调用不应再到达上游", t1After, t1Before)
	}
	// attempt 2 提交的 checkpoint（task-2、report）的累积 refs 含任务 1 的证据与摘要 blob（来自 attempt 1）。
	var t1Blobs []string
	for _, cid := range []string{"root/task-1/fetch/1", "root/task-1/fetch/2", "root/task-1/fetch/3", "root/task-1/chat/1"} {
		t1Blobs = append(t1Blobs, calls[cid].ResultRef)
	}
	for _, stepID := range []string{"task-2", "report"} {
		refs := h.checkpointRefs(id, stepID)
		for _, b := range t1Blobs {
			if !slices.Contains(refs, b) {
				t.Fatalf("attempt 2 的 checkpoint %s 的 refs %v 不含任务 1 的 blob %s", stepID, refs, b)
			}
		}
	}
	cites, cited := h.assertResearchReport(fu, id)
	t.Logf("杀死并恢复：attempts %s；init.resume = %s（%s）；checkpoints %v；上游计数 恢复时 %+v → 结束 %+v；"+
		"任务 1 上游计数 恢复时 %+v = 结束 %+v；attempt 2 refs 含任务 1 blob %v；报告引用 %v；证据 %+v",
		attemptClasses(in), in2.Resume.CheckpointID, in2.Resume.StepID, steps, before, final, t1Before, task1Counts(fu),
		t1Blobs, cited, cites)
	h.finish()
}

// task1Counts 是任务 1 的调用到达上游的请求数：查询为任务 1 的搜索、任务 1 搜索结果页面的抓取、标题为任务 1
// 的摘要（Plan 与 Report 恒为 0）。
func task1Counts(fu *fakeupstream.Server) upstreamCounts {
	task := researchTasks[0]
	pages := map[string]bool{}
	for i := 1; i <= 3; i++ {
		u, err := url.Parse(fu.ResultURL(task.Query, i))
		if err != nil {
			panic(err) // ResultURL 总是合法
		}
		pages[u.Path] = true
	}
	var c upstreamCounts
	for _, r := range fu.Requests(fakeupstream.Search) {
		if strings.Contains(r.Body, task.Query) {
			c.Search++
		}
	}
	for _, r := range fu.Requests(fakeupstream.Fetch) {
		if pages[r.Path] {
			c.Fetch++
		}
	}
	for _, r := range fu.Requests(fakeupstream.Chat) {
		if r.Stage == fakeupstream.StageSummarize && strings.Contains(r.Body, task.Title) {
			c.Summarize++
		}
	}
	return c
}
