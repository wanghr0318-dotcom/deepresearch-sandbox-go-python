package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "s3cret-token-value"

type result struct {
	code        int
	stdout, err string
	sleeps      []time.Duration
}

func run(t *testing.T, srv *httptest.Server, env map[string]string, args ...string) result {
	t.Helper()
	var out, errb bytes.Buffer
	var res result
	n := 0
	e := Env{
		Sleep:  func(d time.Duration) { res.sleeps = append(res.sleeps, d) },
		Getenv: func(k string) string { return env[k] },
		NewID:  func() string { n++; return "rid-" + strconv.Itoa(n) },
	}
	if srv != nil {
		e.HTTP = srv.Client()
		args = append(args, "--addr", srv.URL)
	}
	res.code = RunEnv(e, args, &out, &errb)
	res.stdout, res.err = out.String(), errb.String()
	return res
}

func tokenEnv_() map[string]string { return map[string]string{tokenEnv: testToken} }

func TestWatchRandomDropsNoGapNoDup(t *testing.T) {
	const total = 40
	rng := rand.New(rand.NewSource(7))
	var mu sync.Mutex
	var reqs int32
	var afterTerminal int32
	var terminalSent atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if terminalSent.Load() {
			atomic.AddInt32(&afterTerminal, 1)
		}
		atomic.AddInt32(&reqs, 1)
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			http.Error(w, "no", 401)
			return
		}
		last, _ := strconv.Atoi(r.Header.Get("Last-Event-ID"))
		mu.Lock()
		budget := rng.Intn(6) // 0 个事件即立刻断开，也是合法场景
		mu.Unlock()
		replay := 0
		if last > 2 {
			replay = 2 // 故意重发已送达的事件，客户端须去重
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fmt.Fprint(w, ": heartbeat\n\n")
		fl := w.(http.Flusher)
		for seq := last - replay + 1; seq <= total; seq++ {
			if budget == 0 {
				// 在帧中间硬断开
				fmt.Fprintf(w, "id: %d\nevent: x\nda", seq)
				fl.Flush()
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
				return
			}
			typ := "task_event"
			if seq == total {
				typ = "task_terminal"
				terminalSent.Store(true)
			}
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: {\"task_seq\":%d,\"type\":%q}\n\n", seq, typ, seq, typ)
			fl.Flush()
			budget--
			if seq == total {
				return
			}
		}
	}))
	defer srv.Close()

	res := run(t, srv, tokenEnv_(), "task", "watch", "t1")
	if res.code != 0 {
		t.Fatalf("code=%d err=%s", res.code, res.err)
	}
	lines := strings.Split(strings.TrimSpace(res.stdout), "\n")
	if len(lines) != total {
		t.Fatalf("得到 %d 行，期望 %d", len(lines), total)
	}
	for i, l := range lines {
		var ev struct {
			TaskSeq int `json:"task_seq"`
		}
		if err := json.Unmarshal([]byte(l), &ev); err != nil || ev.TaskSeq != i+1 {
			t.Fatalf("第 %d 行不连续: %s", i, l)
		}
	}
	if atomic.LoadInt32(&reqs) < 3 {
		t.Fatalf("应当发生过重连，请求数=%d", reqs)
	}
	if len(res.sleeps) == 0 {
		t.Fatal("重连应经过退避")
	}
	// task_terminal 之后不得再有请求。
	n := atomic.LoadInt32(&reqs)
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&reqs) != n || atomic.LoadInt32(&afterTerminal) != 0 {
		t.Fatalf("task_terminal 之后仍有请求: %d", afterTerminal)
	}
}

func TestWatchStopsOnTerminalWithoutReconnect(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 1\nevent: task_terminal\ndata: {\"task_seq\":1}\n\n")
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "watch", "t1")
	if res.code != 0 || reqs != 1 || len(res.sleeps) != 0 {
		t.Fatalf("code=%d reqs=%d sleeps=%v", res.code, reqs, res.sleeps)
	}
}

func TestWatchFatalStatusDoesNotRetry(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		w.WriteHeader(404)
		fmt.Fprint(w, `{"code":"task_not_found","message":"任务不存在"}`)
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "watch", "nope")
	if res.code != 1 || reqs != 1 || !strings.Contains(res.err, "task_not_found") {
		t.Fatalf("code=%d reqs=%d err=%s", res.code, reqs, res.err)
	}
}

func TestResultVerifiesSHA256(t *testing.T) {
	body := []byte("pinned result bytes")
	sum := sha256.Sum256(body)
	good := hex.EncodeToString(sum[:])
	etag := good
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks/t1/result" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+etag+`"`)
		if _, err := w.Write(body); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()

	res := run(t, srv, nil, "task", "result", "t1")
	if res.code != 0 || res.stdout != string(body) {
		t.Fatalf("ok case: code=%d out=%q err=%s", res.code, res.stdout, res.err)
	}

	out := filepath.Join(t.TempDir(), "r.bin")
	res = run(t, srv, nil, "task", "result", "t1", "--out", out)
	if b, _ := os.ReadFile(out); res.code != 0 || !bytes.Equal(b, body) {
		t.Fatalf("--out case: code=%d err=%s", res.code, res.err)
	}

	etag = strings.Repeat("0", 64) // 哈希不符
	os.Remove(out)
	res = run(t, srv, nil, "task", "result", "t1", "--out", out)
	if res.code != 1 || !strings.Contains(res.err, "sha256 不符") {
		t.Fatalf("mismatch: code=%d err=%s", res.code, res.err)
	}
	if res.stdout != "" {
		t.Fatalf("校验失败不得输出内容: %q", res.stdout)
	}
	if _, err := os.Stat(out); err == nil {
		t.Fatal("校验失败不得写出文件")
	}
}

// TestResultArtifactVerifiesSHA256：--artifact 下载产物版本（--version 指定，缺省最新），以 ETag 中登记的
// sha256 校验；blob 被篡改（正文与登记的 sha256 不符）时报错，不输出也不写文件。
func TestResultArtifactVerifiesSHA256(t *testing.T) {
	registered := []byte("# report v2")
	sum := sha256.Sum256(registered)
	var body atomic.Value
	body.Store(registered)
	var gotURI atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI.Store(r.URL.RequestURI())
		if r.URL.Path != "/tasks/t1/artifacts/my report" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
		if _, err := w.Write(body.Load().([]byte)); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()

	res := run(t, srv, nil, "task", "result", "t1", "--artifact", "my report", "--version", "2")
	if res.code != 0 || res.stdout != string(registered) || gotURI.Load() != "/tasks/t1/artifacts/my%20report?version=2" {
		t.Fatalf("code=%d out=%q err=%s uri=%v", res.code, res.stdout, res.err, gotURI.Load())
	}
	res = run(t, srv, nil, "task", "result", "t1", "--artifact", "my report")
	if res.code != 0 || gotURI.Load() != "/tasks/t1/artifacts/my%20report" {
		t.Fatalf("缺省应下载最新版本：code=%d err=%s uri=%v", res.code, res.err, gotURI.Load())
	}

	body.Store([]byte("# tampered!")) // blob 被篡改：ETag 仍是登记的 sha256
	out := filepath.Join(t.TempDir(), "a.md")
	res = run(t, srv, nil, "task", "result", "t1", "--artifact", "my report", "--out", out)
	if res.code != 1 || !strings.Contains(res.err, "sha256 不符") || res.stdout != "" {
		t.Fatalf("篡改应报错：code=%d out=%q err=%s", res.code, res.stdout, res.err)
	}
	if _, err := os.Stat(out); err == nil {
		t.Fatal("校验失败不得写出文件")
	}

	if res := run(t, srv, nil, "task", "result", "t1", "--version", "1"); res.code != 2 {
		t.Fatalf("--version 不带 --artifact 应为用法错误：code=%d err=%s", res.code, res.err)
	}
}

func TestResultNotImplementedSurfaced(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		w.WriteHeader(501)
		fmt.Fprint(w, `{"code":"not_implemented","message":"固定输出的下载随 runner 集成提供"}`)
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "result", "t1")
	if res.code != 1 || reqs != 1 || !strings.Contains(res.err, "501") || !strings.Contains(res.err, "尚未实现") {
		t.Fatalf("code=%d reqs=%d err=%s", res.code, reqs, res.err)
	}
}

func TestSubmitRetryReusesRequestID(t *testing.T) {
	var ids []string
	var specs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			RequestID string          `json:"request_id"`
			Spec      json.RawMessage `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Errorf("解码请求: %v", err)
		}
		ids = append(ids, b.RequestID)
		specs = append(specs, string(b.Spec))
		switch len(ids) {
		case 1:
			w.WriteHeader(503)
			fmt.Fprint(w, `{"code":"commit_unknown","message":"retry"}`)
		case 2:
			hj, _, _ := w.(http.Hijacker).Hijack()
			hj.Close() // 网络错误
		default:
			w.WriteHeader(201)
			fmt.Fprint(w, `{"task_id":"t-9"}`)
		}
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "submit", "--spec", `{"goal":"x"}`)
	if res.code != 0 || !strings.Contains(res.stdout, "t-9") {
		t.Fatalf("code=%d out=%s err=%s", res.code, res.stdout, res.err)
	}
	if len(ids) != 3 || ids[0] != "rid-1" || ids[1] != ids[0] || ids[2] != ids[0] {
		t.Fatalf("request_id 不一致: %v", ids)
	}
	if specs[0] != specs[2] || len(res.sleeps) != 2 {
		t.Fatalf("specs=%v sleeps=%v", specs, res.sleeps)
	}
}

func TestSubmitConflictNotRetried(t *testing.T) {
	var reqs int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqs, 1)
		w.WriteHeader(409)
		fmt.Fprint(w, `{"code":"request_conflict","message":"x"}`)
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "submit", "--spec", `{}`, "--request-id", "mine")
	if res.code != 1 || reqs != 1 || !strings.Contains(res.err, "request_conflict") {
		t.Fatalf("code=%d reqs=%d err=%s", res.code, reqs, res.err)
	}
}

func TestTokenNeverLeaks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "api.token"), []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		if strings.Contains(r.URL.String(), testToken) {
			t.Error("token 出现在 URL 中")
		}
		// 恶意/有缺陷的服务端把 token 回显进错误信息。
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"code":"internal","message":"bad token %s"}`, testToken)
	}))
	defer srv.Close()

	for _, env := range []map[string]string{tokenEnv_(), {}} {
		res := run(t, srv, env, "status", "--data-dir", dir)
		if strings.Contains(res.stdout+res.err, testToken) {
			t.Fatalf("token 泄漏: %q %q", res.stdout, res.err)
		}
		if res.code != 1 || !strings.Contains(res.err, "[REDACTED]") {
			t.Fatalf("code=%d err=%s", res.code, res.err)
		}
		if gotAuth.Load() != "Bearer "+testToken {
			t.Fatalf("Authorization=%v", gotAuth.Load())
		}
	}
	// token 不接受为标志。
	res := run(t, srv, nil, "status", "--token", testToken)
	if res.code != 2 || strings.Contains(res.err, testToken) {
		t.Fatalf("--token 应被拒绝且不回显: code=%d err=%s", res.code, res.err)
	}
}

func TestControlAndInspect(t *testing.T) {
	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := new(bytes.Buffer)
		if _, err := b.ReadFrom(r.Body); err != nil {
			t.Errorf("读请求: %v", err)
		}
		gotPath, gotBody = r.Method+" "+r.URL.Path, b.String()
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer srv.Close()
	for _, sub := range []string{"cancel", "pause", "resume"} {
		res := run(t, srv, nil, "task", sub, "t1", "--reason", "why")
		if res.code != 0 || gotPath != "POST /tasks/t1/"+sub ||
			!strings.Contains(gotBody, `"request_id":"rid-1"`) || !strings.Contains(gotBody, `"reason":"why"`) {
			t.Fatalf("%s: code=%d path=%s body=%s err=%s", sub, res.code, gotPath, gotBody, res.err)
		}
	}
	if res := run(t, srv, nil, "task", "inspect", "t1"); res.code != 0 || gotPath != "GET /tasks/t1/inspect" {
		t.Fatalf("inspect: %d %s %s", res.code, gotPath, res.err)
	}
	if res := run(t, srv, nil, "status"); res.code != 0 || gotPath != "GET /status" {
		t.Fatalf("status: %d %s", res.code, gotPath)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{{}, {"bogus"}, {"task"}, {"task", "nope"}, {"task", "cancel"}, {"task", "submit"}} {
		if res := run(t, nil, nil, args...); res.code != 2 {
			t.Fatalf("%v: code=%d err=%s", args, res.code, res.err)
		}
	}
}

// ==== M4 Plan 14 Task 10：inspect 的 sub-run 段 ====

const inspectSubrunsBody = `{"task":{"task_id":"t1","status":"running","attempts_total":1},
"budget":{"limit_micro":2000,"reserved_micro":100,"spent_micro":170,"unknown_micro":5,"tool_call_limit":30,"tool_calls_used":7},
"attempts":[],"checkpoints":[],
"subruns":[
 {"subrun_id":"st1","parent_step_id":"research","status":"started","started_at":"2026-10-06T08:00:00Z","deadline_at":"2026-10-06T08:10:00Z",
  "cap_micro":400,"reserved_micro":100,"spent_micro":120,"unknown_micro":5,"calls":2},
 {"subrun_id":"st2","parent_step_id":"research","status":"timed_out","started_at":"2026-10-06T08:00:00Z","ended_at":"2026-10-06T08:01:30Z",
  "deadline_at":"2026-10-06T08:01:00Z","cancel_reason":"deadline","reserved_micro":0,"spent_micro":0,"unknown_micro":0,"calls":0},
 {"subrun_id":"st3","parent_step_id":"research","status":"failed","started_at":"2026-10-06T08:00:00Z","ended_at":"2026-10-06T08:00:30Z",
  "deadline_at":"2026-10-06T08:10:00Z","failure_reason":"model_unavailable","reserved_micro":0,"spent_micro":0,"unknown_micro":0,"calls":0}],
"calls":[{"call_id":"orch/1"},{"call_id":"st1/1","subrun_id":"st1"},{"call_id":"st1/2","subrun_id":"st1"}]}`

// TestInspectSubrunSection：--format text 输出 task 层账本与 sub-run 段（每个 sub-run 一行：状态、时间、两层费用中的
// sub-run 层、调用数与失败/取消原因）；缺省仍输出 JSON（含 subruns）；未知格式是用法错误。
func TestInspectSubrunSection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks/t1/inspect" {
			http.NotFound(w, r)
			return
		}
		if _, err := fmt.Fprint(w, inspectSubrunsBody); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()

	res := run(t, srv, nil, "task", "inspect", "t1", "--format", "text")
	if res.code != 0 {
		t.Fatalf("code=%d err=%s", res.code, res.err)
	}
	out := res.stdout
	for _, want := range []string{
		"task t1  running",
		"budget  spent $0.000170 / limit $0.002000  reserved $0.000100  unknown $0.000005  tool calls 7/30",
		"sub-runs (3)",
		"calls 3 (root 1, sub-run 2)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("输出缺少 %q：\n%s", want, out)
		}
	}
	line := func(id string) []string {
		for _, l := range strings.Split(out, "\n") {
			if f := strings.Fields(l); len(f) > 0 && f[0] == id {
				return f
			}
		}
		t.Fatalf("没有 %s 行：\n%s", id, out)
		return nil
	}
	if f := strings.Join(line("st1"), " "); !strings.Contains(f, "started") || !strings.Contains(f, "$0.000120/$0.000400") ||
		!strings.Contains(f, "running") || !strings.HasSuffix(f, " 2 -") {
		t.Errorf("st1 行 = %q", f)
	}
	if f := strings.Join(line("st2"), " "); !strings.Contains(f, "timed_out") || !strings.Contains(f, "1m30s") ||
		!strings.Contains(f, "$0.000000/-") || !strings.HasSuffix(f, "cancel: deadline") {
		t.Errorf("st2 行 = %q", f)
	}
	if f := strings.Join(line("st3"), " "); !strings.Contains(f, "failed") || !strings.HasSuffix(f, "failure: model_unavailable") {
		t.Errorf("st3 行 = %q", f)
	}

	res = run(t, srv, nil, "task", "inspect", "t1")
	var in map[string]any
	if res.code != 0 || json.Unmarshal([]byte(res.stdout), &in) != nil || len(in["subruns"].([]any)) != 3 {
		t.Fatalf("缺省 JSON 输出：code=%d %s", res.code, res.stdout)
	}
	if res = run(t, srv, nil, "task", "inspect", "t1", "--format", "yaml"); res.code != 2 {
		t.Fatalf("未知格式应为用法错误：code=%d", res.code)
	}
}

// TestInspectTextWithoutSubruns：没有 sub-run 时 sub-run 段写明"无"。
func TestInspectTextWithoutSubruns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprint(w, `{"task":{"task_id":"t9","status":"succeeded"},"attempts":[],"checkpoints":[],"subruns":[],"calls":[]}`); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "inspect", "t9", "--format", "text")
	if res.code != 0 || !strings.Contains(res.stdout, "sub-runs: none") || strings.Contains(res.stdout, "budget") {
		t.Fatalf("code=%d out=%s err=%s", res.code, res.stdout, res.err)
	}
}

// ==== M4 Plan 14 Task 10 段结束 ====

// TestInspectTextRouteTries：使用模型降级链时 --format text 为每个按路由执行的 try 输出供应商、延迟、结局、对冲与
// 此前跳过的供应商；没有 provider 的 try（单供应商）不列出。
func TestInspectTextRouteTries(t *testing.T) {
	const body = `{"task":{"task_id":"t6","status":"running"},"attempts":[],"checkpoints":[],"subruns":[],"calls":[
		{"call_id":"root/s1/chat/1","endpoint":"/v1/chat/completions","tries":[
			{"try_no":1,"state":"settled","outcome":"retryable","error":"upstream_unavailable","latency_ms":3,"provider":"primary"},
			{"try_no":2,"state":"settled","outcome":"ok","latency_ms":120,"provider":"backup","skipped":"primary:tried"}]},
		{"call_id":"root/s1/chat/2","endpoint":"/v1/chat/completions","tries":[
			{"try_no":1,"state":"settled","outcome":"ok","latency_ms":80,"provider":"backup","skipped":"primary:circuit_open","hedge":true}]},
		{"call_id":"root/s1/search/1","endpoint":"/v1/search","tries":[{"try_no":1,"state":"settled","outcome":"ok"}]}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprint(w, body); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "inspect", "t6", "--format", "text")
	if res.code != 0 {
		t.Fatalf("code=%d err=%s", res.code, res.err)
	}
	var rows []string
	for _, l := range strings.Split(res.stdout, "\n") {
		if strings.HasPrefix(l, "root/") || strings.HasPrefix(l, "MODEL CALL") {
			rows = append(rows, strings.Join(strings.Fields(l), " "))
		}
	}
	want := []string{
		"MODEL CALL TRY PROVIDER LATENCY OUTCOME HEDGE SKIPPED",
		"root/s1/chat/1 1 primary 3ms retryable (upstream_unavailable) - -",
		"root/s1/chat/1 2 backup 120ms ok - primary:tried",
		"root/s1/chat/2 1 backup 80ms ok hedge primary:circuit_open",
	}
	if strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Fatalf("路由行\n%s\n期望\n%s\n完整输出：\n%s", strings.Join(rows, "\n"), strings.Join(want, "\n"), res.stdout)
	}
}

// ==== M4 Plan 15 Task 10：inspect 的 exec 列 ====

// TestInspectTextExecTries：--format text 为 exec 调用的每个 try 输出环境、排队、运行、CPU、启动时间与结局；未测得的
// 列为 -；非 exec 调用不列出。
func TestInspectTextExecTries(t *testing.T) {
	const body = `{"task":{"task_id":"t5","status":"running"},"attempts":[],"checkpoints":[],"subruns":[],"calls":[
		{"call_id":"root/s1/chat/1","endpoint":"/v1/chat/completions","tries":[{"try_no":1,"state":"settled","outcome":"ok"}]},
		{"call_id":"root/s2/exec/1","endpoint":"/v1/exec","tries":[
			{"try_no":1,"env_id":"exec-aaa","state":"settled","outcome":"unknown","error":"lost_on_restart"},
			{"try_no":2,"env_id":"exec-bbb","state":"settled","outcome":"ok","queue_ms":12,"wall_ms":1500,"cpu_usec":250000,
			 "exec_started_at":"2026-10-06T08:00:00Z"}]}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprint(w, body); err != nil {
			t.Errorf("写响应: %v", err)
		}
	}))
	defer srv.Close()
	res := run(t, srv, nil, "task", "inspect", "t5", "--format", "text")
	if res.code != 0 {
		t.Fatalf("code=%d err=%s", res.code, res.err)
	}
	var rows []string
	for _, l := range strings.Split(res.stdout, "\n") {
		if strings.HasPrefix(l, "root/") || strings.HasPrefix(l, "EXEC CALL") {
			rows = append(rows, strings.Join(strings.Fields(l), " "))
		}
	}
	want := []string{
		"EXEC CALL TRY ENV QUEUE WALL CPU STARTED OUTCOME",
		"root/s2/exec/1 1 exec-aaa - - - - unknown (lost_on_restart)",
		"root/s2/exec/1 2 exec-bbb 12ms 1.5s 250ms 2026-10-06T08:00:00Z ok",
	}
	if strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Fatalf("exec 行\n%s\n期望\n%s\n完整输出：\n%s", strings.Join(rows, "\n"), strings.Join(want, "\n"), res.stdout)
	}
}
