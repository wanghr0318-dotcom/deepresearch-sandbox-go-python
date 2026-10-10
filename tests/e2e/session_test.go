//go:build linux

// M4：会话（E28–E33），进程内与 root 真实隔离两种装置。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ---- M4 Plan 12 Task 9：会话（基本流、工具额度、D5 与恢复、awaiting_input、用户隔离；E28–E33） ----
//
// 会话 Worker 是脚本化的 Go 程序 tests/e2e/sessionworker（CGO_ENABLED=0），行为由每条消息正文中的 key=value 选项选择
// （script=…、searches=N、sleep_ms=N、hold_ms=N、state=ref）。非 root 用例经 agentbox-e2e（procprov，不隔离）运行，
// SIGKILL 与重启都是真实的；Worker 的观察记录写在会话 workspace 的 .sw/log.jsonl。TestRealE2x/E3x 以 root 在真实隔离
// 环境中运行 cmd/agentbox（会话 Worker 复制到 rootfs.WorkerDir）。

var (
	swOnce sync.Once
	swDir  string
	swPath string
	swErr  error
)

// sessionWorkerBinary 以 CGO_ENABLED=0 构建 tests/e2e/sessionworker（每次 go test 一次）。
func sessionWorkerBinary(t *testing.T) string {
	t.Helper()
	swOnce.Do(func() {
		if swDir, swErr = os.MkdirTemp("", "agentbox-sw-"); swErr != nil {
			return
		}
		if swErr = os.Chmod(swDir, 0o755); swErr != nil {
			return
		}
		swPath = filepath.Join(swDir, "sessionworker")
		cmd := exec.Command(goBinary(), "build", "-o", swPath, "./sessionworker")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			swErr = fmt.Errorf("构建 sessionworker: %v\n%s", err, out)
		}
	})
	if swErr != nil {
		t.Fatal(swErr)
	}
	return swPath
}

// sessSys 是启用会话的系统级装置：agentbox-e2e（或 root 时的 cmd/agentbox）+ fake upstream + 会话 Worker。
type sessSys struct {
	*sysHarness
	flags []string
	real  bool
}

// newSessionSys：非 root、procprov。extra 是额外的 server 标志（每次启动都使用）。
func newSessionSys(t *testing.T, extra ...string) *sessSys {
	t.Helper()
	s := newSys(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	ss := &sessSys{sysHarness: s,
		flags: append([]string{"--fake-upstream", fu.URL(), "--session-worker", sessionWorkerBinary(t)}, extra...)}
	t.Cleanup(ss.dumpOnFailure)
	return ss
}

// dumpOnFailure 在失败时输出会话、incarnation、环境与 UID 范围的存储状态，以及各 server 除访问日志之外的日志
// （轮询请求会挤掉 tail 中有用的部分）。
func (s *sessSys) dumpOnFailure() {
	if !s.t.Failed() {
		return
	}
	ctx := context.Background()
	if c, err := pgx.Connect(ctx, s.dsn); err == nil {
		for _, q := range []string{
			`SELECT session_id, status, COALESCE(last_error, ''), COALESCE(current_incarnation_id, ''), COALESCE(uid_range_id, ''),
				COALESCE(current_task_id, ''), COALESCE(blocked_by_task_id, '') FROM sessions`,
			`SELECT incarnation_id, session_id, env_id, status, COALESCE(end_reason, '') FROM incarnations ORDER BY started_at`,
			`SELECT env_id, kind, COALESCE(attempt_id, ''), status, stopped_at IS NOT NULL, cleanup_state, COALESCE(uid_range_id, ''),
				COALESCE(cleanup_error, '') FROM environments ORDER BY created_at`,
			`SELECT uid_range_id, state, COALESCE(owner_id, ''), COALESCE(allocation_id, '') FROM uid_ranges WHERE state <> 'free'`,
			`SELECT task_id, status, status_reason, COALESCE(session_id, '') FROM tasks ORDER BY created_at`,
		} {
			rows, err := c.Query(ctx, q)
			if err != nil {
				s.t.Logf("%s: %v", q, err)
				continue
			}
			var lines []string
			for rows.Next() {
				vals, _ := rows.Values()
				lines = append(lines, fmt.Sprint(vals...))
			}
			rows.Close()
			s.t.Logf("%s\n  %s", strings.Fields(q)[1], strings.Join(lines, "\n  "))
		}
		_ = c.Close(ctx)
	}
	for i, p := range s.procs {
		var keep []string
		for _, line := range strings.Split(p.logs.tail(8<<20), "\n") {
			if !strings.Contains(line, `"msg":"api request"`) {
				keep = append(keep, line)
			}
		}
		if len(keep) > 300 {
			keep = keep[len(keep)-300:]
		}
		s.t.Logf("server #%d 日志（不含访问日志，末尾）：\n%s", i+1, strings.Join(keep, "\n"))
	}
}

func (s *sessSys) startS() *serverProc {
	s.t.Helper()
	if s.real {
		return s.startReal(s.flags...)
	}
	return s.start("", s.flags...)
}

// kill 以 SIGKILL 杀死当前 server，等待其数据库连接全部结束。
func (s *sessSys) kill() {
	s.t.Helper()
	if err := s.srv.cmd.Process.Kill(); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(s.srv); !killedBySIGKILL(ps) {
		s.t.Fatalf("server 退出状态 %v，期望被 SIGKILL", ps)
	}
	s.waitConnsGone()
}

func (s *sessSys) end() {
	s.t.Helper()
	if s.real {
		s.finishReal()
		return
	}
	s.finish()
}

func (s *sessSys) row(sql string, args []any, dest ...any) {
	s.t.Helper()
	pgQueryRow(s.t, s.dsn, sql, args, dest...)
}

// swRec 是会话 Worker 的一条观察记录。
type swRec map[string]any

func (r swRec) str(k string) string { v, _ := r[k].(string); return v }

func (r swRec) num(k string) int64 { v, _ := r[k].(float64); return int64(v) }

// swLog 读取会话 workspace 中 Worker 的观察记录（.sw/log.jsonl）。
func (s *sessSys) swLog(sessionID string) []swRec {
	s.t.Helper()
	b, err := os.ReadFile(filepath.Join(s.dir, "sessions", sessionID, "workspace", ".sw", "log.jsonl"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		s.t.Fatal(err)
	}
	var out []swRec
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var r swRec
		if err := json.Unmarshal(line, &r); err != nil {
			s.t.Fatalf("Worker 记录 %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// swFind 返回满足条件的记录。
func swFind(recs []swRec, f func(swRec) bool) []swRec {
	var out []swRec
	for _, r := range recs {
		if f(r) {
			out = append(out, r)
		}
	}
	return out
}

func swEvent(event string, kv ...string) func(swRec) bool {
	return func(r swRec) bool {
		if r.str("event") != event {
			return false
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if r.str(kv[i]) != kv[i+1] {
				return false
			}
		}
		return true
	}
}

// sessUser 是一个已登录的用户（cookie 跨 server 重启有效）。
type sessUser struct {
	s      *sessSys
	cookie string
}

func (s *sessSys) user(name string) *sessUser {
	s.t.Helper()
	resp, err := http.Post(s.srv.base+"/auth/register", "application/json",
		strings.NewReader(`{"username":"`+name+`","password":"Passw0rdX"}`))
	if err != nil {
		s.t.Fatal(err)
	}
	_ = resp.Body.Close()
	u := &sessUser{s: s}
	for _, c := range resp.Cookies() {
		if c.Name == "agentbox_session" {
			u.cookie = c.Name + "=" + c.Value
		}
	}
	if resp.StatusCode != http.StatusCreated || u.cookie == "" {
		s.t.Fatalf("注册 %s = %d", name, resp.StatusCode)
	}
	return u
}

func (u *sessUser) do(method, path, body string) (int, []byte) {
	u.s.t.Helper()
	req, err := http.NewRequest(method, u.s.srv.base+path, strings.NewReader(body))
	if err != nil {
		u.s.t.Fatal(err)
	}
	req.Header.Set("Cookie", u.cookie)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		u.s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		u.s.t.Fatal(err)
	}
	return resp.StatusCode, b
}

func (u *sessUser) createSession(requestID string) string {
	u.s.t.Helper()
	code, b := u.do("POST", "/sessions", `{"request_id":"`+requestID+`"}`)
	var v api.SessionView
	if code != http.StatusCreated || json.Unmarshal(b, &v) != nil || v.SessionID == "" {
		u.s.t.Fatalf("POST /sessions = %d %s", code, b)
	}
	return v.SessionID
}

func (u *sessUser) message(sessionID, requestID, text string) api.CreateTurnResult {
	u.s.t.Helper()
	body, _ := json.Marshal(map[string]any{"request_id": requestID, "text": text, "deep_research": false})
	code, b := u.do("POST", "/sessions/"+sessionID+"/messages", string(body))
	var r api.CreateTurnResult
	if code != http.StatusAccepted || json.Unmarshal(b, &r) != nil || r.TurnID == "" {
		u.s.t.Fatalf("POST messages %q = %d %s", text, code, b)
	}
	return r
}

// control 发送 turn 控制（stop | continue | finish）。
func (u *sessUser) control(turnID, action, requestID string) {
	u.s.t.Helper()
	if code, b := u.do("POST", "/turns/"+turnID+"/"+action, `{"request_id":"`+requestID+`"}`); code != http.StatusAccepted {
		u.s.t.Fatalf("POST /turns/%s/%s = %d %s", turnID, action, code, b)
	}
}

func (u *sessUser) turn(sessionID, turnID string) (api.TurnView, bool) {
	u.s.t.Helper()
	code, b := u.do("GET", "/sessions/"+sessionID+"/turns", "")
	var r struct {
		Turns []api.TurnView `json:"turns"`
	}
	if code != http.StatusOK || json.Unmarshal(b, &r) != nil {
		u.s.t.Fatalf("GET turns = %d %s", code, b)
	}
	for _, v := range r.Turns {
		if v.TurnID == turnID {
			return v, true
		}
	}
	return api.TurnView{}, false
}

func (u *sessUser) waitTurn(sessionID, turnID, what string, cond func(api.TurnView) bool) api.TurnView {
	u.s.t.Helper()
	var v api.TurnView
	eventuallyWithin(u.s.t, "turn "+turnID+" "+what, waitLimit, 100*time.Millisecond, func() bool {
		var ok bool
		v, ok = u.turn(sessionID, turnID)
		return ok && cond(v)
	})
	return v
}

func turnIs(status string) func(api.TurnView) bool {
	return func(v api.TurnView) bool { return v.Status == status }
}

// closeSession 删除会话并等待它 closed（workspace 删除、UID 范围归还）。
func (u *sessUser) closeSession(sessionID string) {
	u.s.t.Helper()
	if code, b := u.do("DELETE", "/sessions/"+sessionID, ""); code != http.StatusAccepted {
		u.s.t.Fatalf("DELETE /sessions/%s = %d %s", sessionID, code, b)
	}
	u.s.waitSession(sessionID, "closed")
}

// waitSession 等待会话的存储状态（sessions.status）。
func (s *sessSys) waitSession(sessionID, status string) {
	s.t.Helper()
	eventually(s.t, "会话 "+sessionID+" 进入 "+status, func() bool {
		var st string
		s.row("SELECT status FROM sessions WHERE session_id = $1", []any{sessionID}, &st)
		return st == status
	})
}

// sse 以该用户读取整个会话的事件流（Last-Event-ID = 0），直到 until 成立或期限到；返回每条事件的原始 JSON。
func (u *sessUser) sse(sessionID string, until func([]json.RawMessage) bool) []json.RawMessage {
	u.s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", u.s.srv.base+"/sessions/"+sessionID+"/events", nil)
	if err != nil {
		u.s.t.Fatal(err)
	}
	req.Header.Set("Cookie", u.cookie)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		u.s.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		u.s.t.Fatalf("GET events = %d", resp.StatusCode)
	}
	var out []json.RawMessage
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if data, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "data: "); ok {
			out = append(out, json.RawMessage(data))
			if until(out) {
				return out
			}
		}
		if err != nil {
			u.s.t.Fatalf("事件流在条件成立之前结束（%v），已读 %d 条", err, len(out))
		}
	}
}

func hasEventType(typ string) func([]json.RawMessage) bool {
	return func(evs []json.RawMessage) bool {
		for _, e := range evs {
			var v struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(e, &v) == nil && v.Type == typ {
				return true
			}
		}
		return false
	}
}

// internalKeys 返回 JSON 中任意深度出现的内部字段名（用户视图不得出现）。
func internalKeys(v any) []string {
	var out []string
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			switch k {
			case "internal", "attempt_id", "call_id", "worker_seq", "usage", "model", "task_seq":
				out = append(out, k)
			}
			if strings.HasPrefix(k, "cost") || strings.Contains(k, "price") {
				out = append(out, k)
			}
			out = append(out, internalKeys(sub)...)
		}
	case []any:
		for _, sub := range x {
			out = append(out, internalKeys(sub)...)
		}
	}
	return out
}

// assertSessionSeqContiguous：会话的 session_seq（task 事件与会话事件合计）从 1 起连续。
func (s *sessSys) assertSessionSeqContiguous(sessionID string) {
	s.t.Helper()
	var n, lo, hi int64
	s.row(`SELECT count(*), COALESCE(min(q), 0), COALESCE(max(q), 0) FROM (SELECT session_seq AS q FROM events WHERE session_id = $1
		UNION ALL SELECT session_seq FROM session_events WHERE session_id = $1) x`, []any{sessionID}, &n, &lo, &hi)
	if n == 0 || lo != 1 || hi != n {
		s.t.Fatalf("会话 %s 的 session_seq 不连续：%d 条，范围 %d..%d", sessionID, n, lo, hi)
	}
}

// TestSessionBasicFlow：创建会话 → 两条消息（第二条读到第一条提交的会话状态：base checkpoint、计数与 workspace 文件）
// → 事件流 session_seq 连续、用户视图没有内部字段；另一用户读写该会话一律 404；删除会话后环境停止、workspace 删除、
// UID 范围归还，不变量（含 I9、I10）成立。
func TestSessionBasicFlow(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	alice, bob := s.user("alice"), s.user("bob")
	sid := alice.createSession("cs-1")
	r1 := alice.message(sid, "m-1", "searches=2")
	v1 := alice.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	r2 := alice.message(sid, "m-2", "searches=1")
	v2 := alice.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	if v1.Summary != "count=1 task="+r1.TurnID || v2.Summary != "count=2 task="+r2.TurnID {
		t.Fatalf("第二条消息应读到第一条提交的会话状态：%q / %q", v1.Summary, v2.Summary)
	}
	if v1.ToolCallsUsed != 2 || v1.ToolCallLimit != 30 || v2.ToolCallsUsed != 1 {
		t.Errorf("工具额度 = %d/%d、%d", v1.ToolCallsUsed, v1.ToolCallLimit, v2.ToolCallsUsed)
	}
	logs := s.swLog(sid)
	st1 := swFind(logs, swEvent("task_start", "task_id", r1.TurnID))
	st2 := swFind(logs, swEvent("task_start", "task_id", r2.TurnID))
	if len(st1) != 1 || len(st2) != 1 || st1[0].str("base_session_checkpoint_id") != "" ||
		st2[0].str("base_session_checkpoint_id") != "sc-"+st1[0].str("attempt_id") || st1[0].num("pid") != st2[0].num("pid") {
		t.Fatalf("task_start 记录：%v / %v（两轮应在同一 incarnation 中，第二轮以第一轮的会话 checkpoint 为 base）", st1, st2)
	}
	if note, err := os.ReadFile(filepath.Join(s.dir, "sessions", sid, "workspace", "session", "note.txt")); err != nil ||
		string(note) != "count=2 task="+r2.TurnID {
		t.Errorf("workspace 会话文件 = %q, %v", note, err)
	}
	// 用户隔离：另一用户读写该会话与其 turn 一律 404。
	for _, req := range []struct{ method, path, body string }{
		{"GET", "/sessions/" + sid, ""}, {"GET", "/sessions/" + sid + "/turns", ""}, {"GET", "/sessions/" + sid + "/events", ""},
		{"POST", "/sessions/" + sid + "/messages", `{"request_id":"x","text":"hi","deep_research":false}`},
		{"POST", "/turns/" + r1.TurnID + "/stop", `{"request_id":"y"}`}, {"DELETE", "/sessions/" + sid, ""},
	} {
		if code, b := bob.do(req.method, req.path, req.body); code != http.StatusNotFound {
			t.Errorf("用户 B %s %s = %d %s，期望 404", req.method, req.path, code, b)
		}
	}
	// 事件流：用户视图没有内部字段，seq 递增；存储的 session_seq 连续。
	evs := alice.sse(sid, func(evs []json.RawMessage) bool {
		n := 0
		for _, e := range evs {
			var v struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
			}
			if json.Unmarshal(e, &v) == nil && v.Type == "turn_result" {
				n++
			}
		}
		return n == 2
	})
	last := int64(0)
	for _, e := range evs {
		var v map[string]any
		if err := json.Unmarshal(e, &v); err != nil {
			t.Fatal(err)
		}
		if keys := internalKeys(v); len(keys) > 0 {
			t.Errorf("用户事件含内部字段 %v：%s", keys, e)
		}
		seq := int64(v["seq"].(float64))
		if seq <= last {
			t.Errorf("事件 seq 不递增：%d 之后 %d", last, seq)
		}
		last = seq
	}
	s.assertSessionSeqContiguous(sid)
	alice.closeSession(sid)
	if _, err := os.Stat(filepath.Join(s.dir, "sessions", sid)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("关闭后会话目录仍存在：%v", err)
	}
	s.end()
}

// TestToolBudgetAcrossRestart：Worker 调用 35 次搜索——第 31 次起 429 tool_budget_exhausted，turn 仍 succeeded，
// tool_calls_used = 30；第 20 次调用之后 SIGKILL server 并重启：计数延续（调用以同一 call id 重放不计数），恢复后的
// attempt 至多再新建 10 个调用。
func TestToolBudgetAcrossRestart(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r := u.message(sid, "m-1", "searches=35 sleep_ms=100")
	var before int
	eventually(t, "至少 20 次搜索调用已登记", func() bool {
		s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &before)
		return before >= 20
	})
	s.kill()
	s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &before)
	if before >= 30 {
		t.Fatalf("SIGKILL 之前已登记 %d 个调用，测试前提不成立", before)
	}
	s.startS()
	v := u.waitTurn(sid, r.TurnID, "成功", turnIs("succeeded"))
	var calls, used int
	s.row("SELECT count(*) FROM calls WHERE task_id = $1", []any{r.TurnID}, &calls)
	s.row("SELECT tool_calls_used FROM budgets WHERE task_id = $1", []any{r.TurnID}, &used)
	if v.ToolCallsUsed != 30 || used != 30 || calls != 30 || calls-before > 10 {
		t.Fatalf("工具额度：Turn.tool_calls_used = %d，budgets = %d，调用 %d 个（重启前 %d 个）", v.ToolCallsUsed, used, calls, before)
	}
	searches := swFind(s.swLog(sid), swEvent("search", "task_id", r.TurnID))
	attempts := map[string]bool{}
	var exhausted int
	for _, rec := range searches {
		attempts[rec.str("attempt_id")] = true
		if rec.str("error") != "" { // SIGKILL 时在途的请求：连接中断，没有状态码
			continue
		}
		switch n := rec.num("n"); {
		case n > 30 && rec.num("status") != http.StatusTooManyRequests:
			t.Errorf("第 %d 次搜索 = %v，期望 429", n, rec["status"])
		case n > 30:
			exhausted++
		case rec.num("status") != http.StatusOK:
			t.Errorf("第 %d 次搜索 = %v，期望 200（新建或重放）", n, rec["status"])
		}
	}
	if len(attempts) != 2 || exhausted != 5 {
		t.Errorf("搜索记录：%d 个 attempt，%d 次 429（期望 2 个 attempt、最后一个 attempt 的第 31–35 次 429）", len(attempts), exhausted)
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionStopSupersedeRestore（D5 与恢复）：turn 1 运行中 stop → paused（turn_stopped 及其之前的事件可见）→ 新消息
// → turn 1 cancelled/superseded、turn 2 运行并带 carryover；restore turn 1 → turn 3 的首个 task_start.resume 为种子
// seed-…，额度重新为 30。
func TestSessionStopSupersedeRestore(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=2 hold_ms=120000")
	eventually(t, "turn 1 完成两次搜索", func() bool {
		return len(swFind(s.swLog(sid), swEvent("search", "task_id", r1.TurnID))) == 2
	})
	u.control(r1.TurnID, "stop", "st-1")
	v1 := u.waitTurn(sid, r1.TurnID, "暂停", turnIs("paused"))
	if v1.ToolCallsUsed != 2 {
		t.Errorf("暂停的 turn 1 tool_calls_used = %d", v1.ToolCallsUsed)
	}
	evs := u.sse(sid, hasEventType("turn_stopped"))
	if !hasEventType("turn_created")(evs) {
		t.Errorf("turn_stopped 之前的事件不可见：%s", evs)
	}
	r2 := u.message(sid, "m-2", "searches=1")
	if r2.SupersededTurnID != r1.TurnID {
		t.Fatalf("新消息应取代暂停的 turn 1：%+v", r2)
	}
	v1 = u.waitTurn(sid, r1.TurnID, "被取代", turnIs("cancelled"))
	if v1.StatusReason != "superseded" || !v1.Restorable {
		t.Errorf("turn 1 = %s/%s，restorable %v", v1.Status, v1.StatusReason, v1.Restorable)
	}
	u.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	st2 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r2.TurnID))
	if len(st2) != 1 {
		t.Fatalf("turn 2 的 task_start：%v", st2)
	}
	carry, _ := st2[0]["carryover"].(map[string]any)
	if carry["task_id"] != r1.TurnID || len(fmt.Sprint(carry["checkpoint_ref"])) != 64 {
		t.Errorf("turn 2 的 carryover = %v，期望指向 turn 1 的最新 checkpoint", st2[0]["carryover"])
	}
	code, b := u.do("POST", "/turns/"+r1.TurnID+"/restore", `{"request_id":"rs-1"}`)
	var r3 api.CreateTurnResult
	if code != http.StatusAccepted || json.Unmarshal(b, &r3) != nil || r3.TurnID == "" {
		t.Fatalf("POST restore = %d %s", code, b)
	}
	v3 := u.waitTurn(sid, r3.TurnID, "成功", turnIs("succeeded"))
	st3 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r3.TurnID))
	att1 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r1.TurnID))
	if len(st3) == 0 || len(att1) == 0 || st3[0].str("resume_checkpoint_id") != "seed-tc-"+att1[0].str("attempt_id") ||
		st3[0].str("restored_from_task_id") != r1.TurnID {
		t.Fatalf("turn 3 的首个 task_start = %v，期望从种子 seed-tc-%s 继续", st3, att1)
	}
	if v3.ToolCallLimit != 30 || v3.ToolCallsUsed != 2 || v3.RestoredFromTurnID != r1.TurnID { // 恢复的 turn 沿用源正文（2 次搜索），额度自 0 计
		t.Errorf("turn 3 = %+v，期望新的 30 次额度", v3)
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionStopGraceFallbackCard（停止修复 F2）：Worker 在停止的 grace（--session-pause-grace 1s）内不响应 pause，宿主
// 强制结束 attempt（platform_killed）→ turn paused，事件流中恰有一条宿主写的 turn_stopped（固定 findings，卡片取自该 turn
// 的计划、子主题、来源与宿主的工具额度，can_finish）；随后"继续"与"立即写报告"都从 checkpoint 恢复并成功，停止卡不重复。
func TestSessionStopGraceFallbackCard(t *testing.T) {
	s := newSessionSys(t, "--session-pause-grace", "1s")
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	// turnEvents 返回该 turn 的某类事件的 data。
	turnEvents := func(evs []json.RawMessage, turnID, typ string) []json.RawMessage {
		var out []json.RawMessage
		for _, e := range evs {
			var v struct {
				Type   string          `json:"type"`
				TurnID string          `json:"turn_id"`
				Data   json.RawMessage `json:"data"`
			}
			if json.Unmarshal(e, &v) == nil && v.Type == typ && v.TurnID == turnID {
				out = append(out, v.Data)
			}
		}
		return out
	}
	// 两个 turn 分别以"继续"（不带 directive）与"立即写报告"（directive finish_now）从宿主停止卡的暂停状态恢复。
	for i, c := range []struct{ action, directive string }{{"continue", ""}, {"finish", "finish_now"}} {
		n := fmt.Sprint(i + 1)
		r := u.message(sid, "m-"+n, "searches=1 progress=1 ignore_pause=1 hold_ms=120000")
		eventually(t, "turn "+n+" 发出研究进度", func() bool {
			return len(swFind(s.swLog(sid), swEvent("progress_sent", "task_id", r.TurnID))) == 1
		})
		u.control(r.TurnID, "stop", "st-"+n)
		u.waitTurn(sid, r.TurnID, "暂停", turnIs("paused"))
		if len(swFind(s.swLog(sid), swEvent("ignoring_pause", "task_id", r.TurnID))) != 1 {
			t.Fatalf("turn %s：Worker 应收到并忽略了 pause", n)
		}
		var killed bool
		s.row("SELECT platform_killed FROM attempts WHERE task_id = $1 AND attempt_no = 1", []any{r.TurnID}, &killed)
		if !killed {
			t.Fatalf("turn %s：grace 到期应由宿主强制结束 attempt（platform_killed）", n)
		}
		cards := turnEvents(u.sse(sid, func(evs []json.RawMessage) bool { return len(turnEvents(evs, r.TurnID, "turn_stopped")) > 0 }),
			r.TurnID, "turn_stopped")
		var card struct {
			Card      map[string]any `json:"card"`
			Findings  string         `json:"findings"`
			CanFinish bool           `json:"can_finish"`
		}
		if len(cards) != 1 || json.Unmarshal(cards[0], &card) != nil {
			t.Fatalf("turn %s 应恰有一条停止卡：%s", n, cards)
		}
		if card.Findings != api.FallbackStopFindings || !card.CanFinish {
			t.Errorf("turn %s 的宿主停止卡 = %s", n, cards[0])
		}
		todo, _ := card.Card["todo"].([]any)
		if card.Card["subtopics_done"] != 1.0 || card.Card["subtopics_total"] != 2.0 || card.Card["sources"] != 1.0 ||
			card.Card["tool_calls_used"] != 1.0 || card.Card["tool_call_limit"] != 30.0 || len(todo) != 2 {
			t.Errorf("turn %s 的停止卡内容 = %s", n, cards[0])
		}
		if s := string(cards[0]); strings.Contains(s, "attempt") || strings.Contains(s, "cost") || strings.Contains(s, fakeupstream.Model) {
			t.Errorf("停止卡含内部字段：%s", s)
		}

		u.control(r.TurnID, c.action, c.action+"-"+n)
		u.waitTurn(sid, r.TurnID, c.action+" 后成功", turnIs("succeeded"))
		starts := swFind(s.swLog(sid), swEvent("task_start", "task_id", r.TurnID))
		if len(starts) != 2 || starts[1].str("resume_checkpoint_id") != "tc-"+starts[0].str("attempt_id") {
			t.Fatalf("%s 的 task_start = %v，期望从第一个 attempt 的 checkpoint 恢复", c.action, starts)
		}
		dir, _ := starts[1]["directive"].(map[string]any)
		if got, _ := dir["kind"].(string); got != c.directive {
			t.Errorf("%s 的 directive = %v，期望 %q", c.action, starts[1]["directive"], c.directive)
		}
		evs := u.sse(sid, func(evs []json.RawMessage) bool { return len(turnEvents(evs, r.TurnID, "turn_result")) > 0 })
		if k := len(turnEvents(evs, r.TurnID, "turn_stopped")); k != 1 {
			t.Errorf("%s 之后 turn %s 的停止卡应仍只有一条，得到 %d", c.action, n, k)
		}
		var stored int
		s.row("SELECT count(*) FROM events WHERE task_id = $1 AND type = 'turn_stopped'", []any{r.TurnID}, &stored)
		if stored != 1 {
			t.Errorf("turn %s 存储中的停止卡 %d 条", n, stored)
		}
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionStopCrashFallbackCard（停止修复 F2，崩溃安全）：停止请求已应用（turn stopping）、Worker 仍在忽略 pause 时
// server 被 SIGKILL；重启后恢复以暂停裁决结束该 attempt，同一事务写出恰好一条宿主停止卡；之后"继续"照常成功。
func TestSessionStopCrashFallbackCard(t *testing.T) {
	s := newSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r := u.message(sid, "m-1", "searches=1 progress=1 ignore_pause=1 hold_ms=120000")
	eventually(t, "turn 发出研究进度", func() bool {
		return len(swFind(s.swLog(sid), swEvent("progress_sent", "task_id", r.TurnID))) == 1
	})
	u.control(r.TurnID, "stop", "st-1")
	eventually(t, "Worker 收到 pause", func() bool {
		return len(swFind(s.swLog(sid), swEvent("ignoring_pause", "task_id", r.TurnID))) == 1
	})
	s.kill()
	s.startS()
	u.waitTurn(sid, r.TurnID, "恢复后暂停", turnIs("paused"))
	var stored int
	s.row("SELECT count(*) FROM events WHERE task_id = $1 AND type = 'turn_stopped'", []any{r.TurnID}, &stored)
	if stored != 1 {
		t.Fatalf("恢复后存储中的停止卡 %d 条，期望 1", stored)
	}
	evs := u.sse(sid, hasEventType("turn_stopped"))
	var findings []string
	for _, e := range evs {
		var v struct {
			Type string `json:"type"`
			Data struct {
				Findings string `json:"findings"`
			} `json:"data"`
		}
		if json.Unmarshal(e, &v) == nil && v.Type == "turn_stopped" {
			findings = append(findings, v.Data.Findings)
		}
	}
	if len(findings) != 1 || findings[0] != api.FallbackStopFindings {
		t.Fatalf("恢复后的停止卡 = %v", findings)
	}
	u.control(r.TurnID, "continue", "ct-1")
	u.waitTurn(sid, r.TurnID, "继续后成功", turnIs("succeeded"))
	s.row("SELECT count(*) FROM events WHERE task_id = $1 AND type = 'turn_stopped'", []any{r.TurnID}, &stored)
	if stored != 1 {
		t.Errorf("继续后存储中的停止卡 %d 条", stored)
	}
	u.closeSession(sid)
	s.end()
}

// TestSessionStopQueuedFallbackCard（停止修复 F2）：--run-slots 1 下排队的 turn 被停止（queued → paused，没有 attempt）
// 也得到恰好一张宿主停止卡（固定 findings，不可写报告），之后"继续"照常运行并成功。
func TestSessionStopQueuedFallbackCard(t *testing.T) {
	s := newSessionSys(t, "--run-slots", "1")
	s.startS()
	alice, bob := s.user("alice"), s.user("bob")
	sa, sb := alice.createSession("cs-a"), bob.createSession("cs-b")
	ra := alice.message(sa, "m-a", "searches=1 hold_ms=120000")
	eventually(t, "alice 的 turn 占用 run slot", func() bool {
		return len(swFind(s.swLog(sa), swEvent("search", "task_id", ra.TurnID))) == 1
	})
	rb := bob.message(sb, "m-b", "searches=1")
	bob.waitTurn(sb, rb.TurnID, "排队", turnIs("queued"))
	bob.control(rb.TurnID, "stop", "st-b")
	bob.waitTurn(sb, rb.TurnID, "暂停", turnIs("paused"))
	evs := bob.sse(sb, hasEventType("turn_stopped"))
	var cards []string
	for _, e := range evs {
		var v struct {
			Type string `json:"type"`
			Data struct {
				Findings  string `json:"findings"`
				CanFinish bool   `json:"can_finish"`
			} `json:"data"`
		}
		if json.Unmarshal(e, &v) == nil && v.Type == "turn_stopped" {
			if v.Data.Findings != api.FallbackStopFindings || v.Data.CanFinish {
				t.Errorf("排队 turn 的停止卡 = %s", e)
			}
			cards = append(cards, string(e))
		}
	}
	if len(cards) != 1 {
		t.Fatalf("排队 turn 应恰有一张停止卡：%v", cards)
	}
	alice.control(ra.TurnID, "stop", "st-a")
	alice.waitTurn(sa, ra.TurnID, "暂停", turnIs("paused"))
	bob.control(rb.TurnID, "continue", "ct-b")
	bob.waitTurn(sb, rb.TurnID, "继续后成功", turnIs("succeeded"))
	var stored int
	s.row("SELECT count(*) FROM events WHERE task_id = $1 AND type = 'turn_stopped'", []any{rb.TurnID}, &stored)
	if stored != 1 {
		t.Errorf("排队 turn 存储中的停止卡 %d 条", stored)
	}
	alice.closeSession(sa)
	bob.closeSession(sb)
	s.end()
}

// TestSessionAwaitingInput：ask_user → turn awaiting_input，run slot 归还（--run-slots 1 下另一用户的 turn 照常完成）
// → answer → 同一 turn 从原位置继续并成功，Worker 收到 directive.answer（带待答提问的 question_id）。
func TestSessionAwaitingInput(t *testing.T) {
	s := newSessionSys(t, "--run-slots", "1")
	s.startS()
	alice, bob := s.user("alice"), s.user("bob")
	sa, sb := alice.createSession("cs-a"), bob.createSession("cs-b")
	r := alice.message(sa, "m-1", "script=ask_user")
	alice.waitTurn(sa, r.TurnID, "等待回答", turnIs("awaiting_input"))
	rb := bob.message(sb, "m-b", "searches=1")
	bob.waitTurn(sb, rb.TurnID, "成功（run slot 已归还）", turnIs("succeeded"))
	if code, b := alice.do("POST", "/turns/"+r.TurnID+"/answer",
		`{"request_id":"an-1","answers":[{"question_id":"1","choice":"A"}]}`); code != http.StatusAccepted {
		t.Fatalf("POST answer = %d %s", code, b)
	}
	alice.waitTurn(sa, r.TurnID, "回答后成功", turnIs("succeeded"))
	starts := swFind(s.swLog(sa), swEvent("task_start", "task_id", r.TurnID))
	if len(starts) != 2 {
		t.Fatalf("task_start 记录：%v", starts)
	}
	dir, _ := starts[1]["directive"].(map[string]any)
	answers, _ := dir["answers"].([]any)
	if dir["kind"] != "answer" || dir["question_id"] != "q-"+r.TurnID || len(answers) != 1 || starts[1].str("resume_checkpoint_id") != "tc-"+starts[0].str("attempt_id") {
		t.Fatalf("回答后的 task_start = %v，期望 directive.answer{question_id = q-%s} 并从提问前的 checkpoint 继续", starts[1], r.TurnID)
	}
	alice.closeSession(sa)
	bob.closeSession(sb)
	s.end()
}

// ---- 会话：root、真实隔离（cmd/agentbox + provider/local；E28–E33） ----

var (
	swInstallOnce sync.Once
	swInstallErr  error
)

// installSessionWorker 把 sessionworker 复制到 rootfs.WorkerDir（默认模板包含该目录），返回环境内的路径。
func installSessionWorker(t *testing.T) string {
	t.Helper()
	src := sessionWorkerBinary(t)
	dst := filepath.Join(rootfs.WorkerDir, "sessionworker")
	swInstallOnce.Do(func() { // 写临时文件后改名：旧二进制仍在运行时（ETXTBSY）也能替换
		b, err := os.ReadFile(src)
		tmp := dst + ".tmp"
		if err == nil {
			err = os.WriteFile(tmp, b, 0o755)
		}
		if err == nil {
			err = os.Chmod(tmp, 0o755)
		}
		if err == nil {
			err = os.Rename(tmp, dst)
		}
		swInstallErr = err
	})
	if swInstallErr != nil {
		t.Fatalf("安装 sessionworker 到 %s: %v", dst, swInstallErr)
	}
	return dst
}

// newRealSessionSys：root，cmd/agentbox 以生产启动器运行；模型与搜索指向 fake upstream（用户账号需要模型上游），
// 会话 Worker 为复制进 rootfs.WorkerDir 的 sessionworker。
func newRealSessionSys(t *testing.T, extra ...string) *sessSys {
	t.Helper()
	s := newRealSys(t)
	worker := installSessionWorker(t)
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	u, err := url.Parse(fu.URL())
	if err != nil {
		t.Fatal(err)
	}
	ss := &sessSys{sysHarness: s, real: true, flags: append([]string{
		"--model-base-url", fu.URL() + "/v1", "--model-name", fakeupstream.Model,
		"--user-orchestrator-model", fakeupstream.Model, "--user-worker-model", fakeupstream.Model,
		"--search-provider", upstream.SearchFake, "--search-base-url", fu.URL(), "--upstream-allow-private", u.Host,
		"--session-worker-argv", worker,
	}, extra...)}
	t.Cleanup(ss.dumpOnFailure)
	return ss
}

// incarnationOf 返回会话最近建立的 incarnation 与其环境。
func (s *sessSys) incarnationOf(sessionID string) (incID, envID, status, endReason string) {
	s.t.Helper()
	s.row(`SELECT incarnation_id, env_id, status, COALESCE(end_reason, '') FROM incarnations WHERE session_id = $1
		ORDER BY started_at DESC LIMIT 1`, []any{sessionID}, &incID, &envID, &status, &endReason)
	return
}

func (s *sessSys) envCgroupDir(envID string) string {
	return filepath.Join(s.installCgroup(), "env-"+envID)
}

// cgroupFrozen 读取环境 cgroup 的 cgroup.events 中的 frozen 值。
func cgroupFrozen(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "frozen "); ok {
			return strings.TrimSpace(v) == "1", nil
		}
	}
	return false, errors.New("cgroup.events 没有 frozen 行")
}

// sessionStateEvents 返回会话的 session_state 事件中的状态序列（按 session_seq）。
func (s *sessSys) sessionStateEvents(sessionID string) []string {
	s.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT payload->>'state' FROM session_events WHERE session_id = $1 AND type = 'session_state'
		ORDER BY session_seq`, sessionID)
	if err != nil {
		s.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		s.t.Fatal(err)
	}
	return out
}

// TestRealE28SessionColdRestore（E28）：会话 idle 时 Worker 死亡（exit_when_idle）→ evicted → 新消息 → restoring →
// 冷恢复：同一 workspace（Worker 读到上次写入 /workspace/session 的文件）、新的 incarnation 与入口 socket、同一 UID
// 范围。分别以 inline state 与 state_ref 的会话 checkpoint 各运行一次；state_ref 时 Worker 从
// /run/agentbox/restore/<sha> 读取，ready 之后暂存目录被删除；恢复期间（ready 之前）连接 Gateway 被拒。
func TestRealE28SessionColdRestore(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	for _, mode := range []string{"inline", "ref"} {
		sid := u.createSession("cs-" + mode)
		r1 := u.message(sid, "m1-"+mode, "searches=1 state="+mode+" script=exit_when_idle")
		u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
		s.waitSession(sid, "evicted")
		inc1, env1, st1, _ := s.incarnationOf(sid)
		if st1 != "ended" {
			t.Fatalf("%s：Worker 退出后 incarnation %s = %s", mode, inc1, st1)
		}
		r2 := u.message(sid, "m2-"+mode, "searches=1")
		v2 := u.waitTurn(sid, r2.TurnID, "冷恢复后成功", turnIs("succeeded"))
		if v2.Summary != "count=2 task="+r2.TurnID {
			t.Errorf("%s：冷恢复后的会话状态 = %q", mode, v2.Summary)
		}
		inc2, env2, _, _ := s.incarnationOf(sid)
		var range1, range2 string
		s.row("SELECT uid_range_id FROM environments WHERE env_id = $1", []any{env1}, &range1)
		s.row("SELECT uid_range_id FROM environments WHERE env_id = $1", []any{env2}, &range2)
		if inc2 == inc1 || env2 == env1 || range1 != range2 || range1 == "" {
			t.Errorf("%s：恢复应建立新的 incarnation（入口 inc-<id>.sock）并沿用同一 UID 范围：%s/%s → %s/%s，范围 %s → %s",
				mode, inc1, env1, inc2, env2, range1, range2)
		}
		states := s.sessionStateEvents(sid)
		if i := slices.Index(states, "evicted"); i < 0 || !slices.Contains(states[i:], "restoring") {
			t.Errorf("%s：会话状态事件 = %v，期望 evicted 之后 restoring", mode, states)
		}
		starts := swFind(s.swLog(sid), func(r swRec) bool { return r.str("event") == "start" && r["resumed"] == true })
		att1 := swFind(s.swLog(sid), swEvent("task_start", "task_id", r1.TurnID))
		if len(starts) != 1 || len(att1) != 1 {
			t.Fatalf("%s：恢复记录 %v，turn 1 记录 %v", mode, starts, att1)
		}
		rs := starts[0]
		if rs.str("checkpoint_id") != "sc-"+att1[0].str("attempt_id") || rs.str("note") != "count=1 task="+r1.TurnID ||
			rs["gateway_refused"] != true {
			t.Errorf("%s：恢复记录 = %v（期望 checkpoint sc-%s、读到上次写入的会话文件、恢复期间 Gateway 被拒）",
				mode, rs, att1[0].str("attempt_id"))
		}
		staged := rs.str("staged_state_path")
		if (mode == "ref") != strings.HasPrefix(staged, protocol.StagedStateDir) {
			t.Errorf("%s：staged_state_path = %q", mode, staged)
		}
		if _, err := os.Stat(filepath.Join(s.dir, "restore", env2)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s：ready 之后恢复暂存目录仍存在（%v）", mode, err)
		}
		u.closeSession(sid)
	}
	s.end()
}

// TestRealE29OutcomeLossAndReleaseTimeout（E29）：task_outcome 丢失一次 → Worker 以 task_outcome_query 查询后释放，
// turn 终态不变、incarnation 继续服务下一轮；Worker 永不发 task_released → T_release 后 incarnation 销毁
// （end_reason = release_timeout），turn 终态、会话指针与 fault_retries_used 不变，不重跑。
func TestRealE29OutcomeLossAndReleaseTimeout(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 script=drop_outcome_once")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	eventually(t, "Worker 查询裁决后释放", func() bool {
		return len(swFind(s.swLog(sid), swEvent("task_outcome"))) == 1
	})
	logs := s.swLog(sid)
	if len(swFind(logs, swEvent("outcome_dropped"))) != 1 {
		t.Fatalf("Worker 应丢弃第一次 task_outcome：%v", logs)
	}
	inc1, _, _, _ := s.incarnationOf(sid)
	r2 := u.message(sid, "m-2", "searches=1 script=never_release")
	u.waitTurn(sid, r2.TurnID, "成功", turnIs("succeeded"))
	if inc, _, _, _ := s.incarnationOf(sid); inc != inc1 {
		t.Fatalf("查询恢复之后 incarnation 应继续服务：%s → %s", inc1, inc)
	}
	eventuallyWithin(t, "T_release 后 incarnation 销毁", 2*waitLimit, 100*time.Millisecond, func() bool {
		_, _, st, reason := s.incarnationOf(sid)
		return st == "ended" && reason == "release_timeout"
	})
	starts := swFind(s.swLog(sid), swEvent("task_start", "task_id", r2.TurnID))
	var status, pointer string
	var faults int64
	s.row("SELECT status, fault_retries_used FROM tasks WHERE task_id = $1", []any{r2.TurnID}, &status, &faults)
	s.row("SELECT COALESCE(latest_checkpoint_id, '') FROM session_progress WHERE session_id = $1", []any{sid}, &pointer)
	if len(starts) != 1 || status != "succeeded" || faults != 0 || pointer != "sc-"+starts[0].str("attempt_id") {
		t.Fatalf("释放超时不改变裁决：task_start %d 次，状态 %s，fault_retries_used %d，会话指针 %s", len(starts), status, faults, pointer)
	}
	u.closeSession(sid)
	s.end()
}

// TestRealE30LeakedChildFailsRelease（E30）：T1 释放前留下子进程 → 释放核验（进程表回到基线）失败 → incarnation
// 销毁；T2 在新的 incarnation 中运行；calls 中没有 T1 的 attempt 在 T2 期间登记的调用。
func TestRealE30LeakedChildFailsRelease(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 script=leak_child")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	inc1, env1, _, _ := s.incarnationOf(sid)
	eventuallyWithin(t, "释放核验失败后 incarnation 销毁", 2*waitLimit, 100*time.Millisecond, func() bool {
		var st, reason string
		s.row("SELECT status, COALESCE(end_reason, '') FROM incarnations WHERE incarnation_id = $1", []any{inc1}, &st, &reason)
		return st == "ended" && reason == "release_timeout"
	})
	if len(swFind(s.swLog(sid), swEvent("leaked_child"))) != 1 {
		t.Fatal("Worker 没有留下子进程")
	}
	if pids := cgroupPids(s.envCgroupDir(env1)); len(pids) != 0 {
		t.Errorf("销毁之后环境 %s 仍有进程 %v", env1, pids)
	}
	r2 := u.message(sid, "m-2", "searches=1")
	u.waitTurn(sid, r2.TurnID, "在新 incarnation 中成功", turnIs("succeeded"))
	if inc2, _, _, _ := s.incarnationOf(sid); inc2 == inc1 {
		t.Fatalf("T2 应在新的 incarnation 中运行")
	}
	var late int
	s.row(`SELECT count(*) FROM calls c JOIN tasks t2 ON t2.task_id = $2 WHERE c.task_id = $1 AND c.created_at >= t2.created_at`,
		[]any{r1.TurnID, r2.TurnID}, &late)
	if late != 0 {
		t.Errorf("T1 在 T2 期间登记了 %d 个调用", late)
	}
	u.closeSession(sid)
	s.end()
}

// TestRealE31FreezeAndFreezeFailure（E31）：--session-idle-freeze 2s → frozen（会话事件出现时 cgroup.events 已是
// frozen 1）→ 新消息 thaw 并在同一 incarnation 中运行；冻结无法确认（cgroup.events 被替换为 frozen 0 的文件）→
// 驱逐（evicted），不记录 frozen。
func TestRealE31FreezeAndFreezeFailure(t *testing.T) {
	s := newRealSessionSys(t, "--session-idle-freeze", "2s")
	s.startS()
	u := s.user("alice")

	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	inc1, env1, _, _ := s.incarnationOf(sid)
	cg := s.envCgroupDir(env1)
	eventually(t, "会话冻结（事件出现时 cgroup 已冻结）", func() bool {
		if !slices.Contains(s.sessionStateEvents(sid), "frozen") {
			return false
		}
		frozen, err := cgroupFrozen(cg)
		if err != nil || !frozen {
			t.Fatalf("session_state{frozen} 已出现，但 cgroup.events frozen = %v（%v）", frozen, err)
		}
		return true
	})
	r2 := u.message(sid, "m-2", "searches=1")
	u.waitTurn(sid, r2.TurnID, "thaw 后成功", turnIs("succeeded"))
	if inc, _, _, _ := s.incarnationOf(sid); inc != inc1 {
		t.Fatalf("thaw 之后应在同一 incarnation 中运行：%s → %s", inc1, inc)
	}
	u.closeSession(sid)

	sid2 := u.createSession("cs-2")
	r3 := u.message(sid2, "m-3", "searches=1")
	u.waitTurn(sid2, r3.TurnID, "成功", turnIs("succeeded"))
	_, env2, _, _ := s.incarnationOf(sid2)
	events := filepath.Join(s.envCgroupDir(env2), "cgroup.events")
	fakeEvents := filepath.Join(t.TempDir(), "cgroup.events")
	if err := os.WriteFile(fakeEvents, []byte("populated 1\nfrozen 0\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount(fakeEvents, events, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("以 bind 替换 %s: %v", events, err)
	}
	mounted := true
	unmount := func() {
		if mounted {
			_ = syscall.Unmount(events, syscall.MNT_DETACH)
			mounted = false
		}
	}
	defer unmount()
	eventuallyWithin(t, "冻结无法确认 → 驱逐", 2*waitLimit, 50*time.Millisecond, func() bool {
		var st string
		s.row("SELECT status FROM sessions WHERE session_id = $1", []any{sid2}, &st)
		if st == "evicting" || st == "evicted" {
			unmount()
		}
		return st == "evicted"
	})
	if states := s.sessionStateEvents(sid2); slices.Contains(states, "frozen") {
		t.Errorf("冻结未确认却记录了 frozen：%v", states)
	}
	var reason string
	s.row("SELECT COALESCE(end_reason, '') FROM incarnations WHERE env_id = $1", []any{env2}, &reason)
	if reason != "freeze_failed" {
		t.Errorf("incarnation end_reason = %q，期望 freeze_failed", reason)
	}
	u.closeSession(sid2)
	s.end()
}

// TestRealE32CloseStopsEnvBeforeDeletingWorkspace（E32）：turn 暂停后 DELETE /sessions/{id} → 暂停的 turn
// cancelled → 环境的 stopped_at 早于 workspace 目录删除 → 会话 closed、UID 范围归还。
func TestRealE32CloseStopsEnvBeforeDeletingWorkspace(t *testing.T) {
	s := newRealSessionSys(t)
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1 hold_ms=120000")
	eventually(t, "turn 1 完成搜索", func() bool {
		return len(swFind(s.swLog(sid), swEvent("search", "task_id", r1.TurnID))) == 1
	})
	u.control(r1.TurnID, "stop", "st-1")
	u.waitTurn(sid, r1.TurnID, "暂停", turnIs("paused"))
	_, env, _, _ := s.incarnationOf(sid)
	ws := filepath.Join(s.dir, "sessions", sid, "workspace")
	if code, b := u.do("DELETE", "/sessions/"+sid, ""); code != http.StatusAccepted {
		t.Fatalf("DELETE = %d %s", code, b)
	}
	var deletedAt time.Time
	eventuallyWithin(t, "workspace 目录删除", waitLimit, 5*time.Millisecond, func() bool {
		if _, err := os.Stat(ws); errors.Is(err, os.ErrNotExist) {
			deletedAt = time.Now()
			return true
		}
		return false
	})
	s.waitSession(sid, "closed")
	var stoppedAt time.Time
	var status, rangeState string
	s.row("SELECT stopped_at FROM environments WHERE env_id = $1", []any{env}, &stoppedAt)
	s.row("SELECT status FROM tasks WHERE task_id = $1", []any{r1.TurnID}, &status)
	s.row(`SELECT COALESCE((SELECT state FROM uid_ranges WHERE owner_id = $1 AND state <> 'free'), 'free')`,
		[]any{"session:" + sid}, &rangeState)
	if status != "cancelled" || !stoppedAt.Before(deletedAt) || rangeState != "free" {
		t.Fatalf("关闭：turn %s，环境停止于 %s、workspace 删除于 %s，UID 范围 %s", status, stoppedAt, deletedAt, rangeState)
	}
	s.end()
}

// TestRealE33RestartWithFrozenSession（E33）：存在 frozen 会话时 SIGKILL server 并重启 → 会话 evicted（incarnation
// ended{lost_on_restart}）、冻结的旧环境被停止并回收 → 新消息冷恢复成功（读到冻结前提交的会话状态）。
func TestRealE33RestartWithFrozenSession(t *testing.T) {
	s := newRealSessionSys(t, "--session-idle-freeze", "2s")
	s.startS()
	u := s.user("alice")
	sid := u.createSession("cs-1")
	r1 := u.message(sid, "m-1", "searches=1")
	u.waitTurn(sid, r1.TurnID, "成功", turnIs("succeeded"))
	s.waitSession(sid, "frozen")
	inc1, env1, _, _ := s.incarnationOf(sid)
	if frozen, err := cgroupFrozen(s.envCgroupDir(env1)); err != nil || !frozen {
		t.Fatalf("frozen 会话的 cgroup.events frozen = %v（%v）", frozen, err)
	}
	s.kill()
	s.startS()
	s.waitSession(sid, "evicted")
	var st, reason string
	s.row("SELECT status, COALESCE(end_reason, '') FROM incarnations WHERE incarnation_id = $1", []any{inc1}, &st, &reason)
	if st != "ended" || reason != "lost_on_restart" {
		t.Errorf("重启后旧 incarnation = %s/%s", st, reason)
	}
	eventually(t, "冻结的旧环境被停止并回收", func() bool {
		var stopped bool
		var cleanup string
		s.row("SELECT stopped_at IS NOT NULL, cleanup_state FROM environments WHERE env_id = $1", []any{env1}, &stopped, &cleanup)
		_, err := os.Stat(s.envCgroupDir(env1))
		return stopped && cleanup == "done" && errors.Is(err, os.ErrNotExist)
	})
	r2 := u.message(sid, "m-2", "searches=1")
	v2 := u.waitTurn(sid, r2.TurnID, "冷恢复后成功", turnIs("succeeded"))
	if v2.Summary != "count=2 task="+r2.TurnID {
		t.Errorf("冷恢复后的会话状态 = %q", v2.Summary)
	}
	u.closeSession(sid)
	s.end()
}
