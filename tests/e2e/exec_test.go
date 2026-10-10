//go:build linux

// M4：exec 真实链路与故障实验（E35–E38）。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/app"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/upstream"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/fakeupstream"
)

// ==== M4 Plan 15 Task 12：exec 真实链路与故障实验（规格 §10、§16 E35–E38；E34、E39 见段末说明） ====
//
// 全部为 root、真实隔离：Worker（sim_worker）在任务沙箱中经 /run/agentbox/gateway.sock 调 POST /v1/exec，
// Gateway 为每次 exec 建立独立的 exec 环境（provider/local，exec 模板，无 Gateway socket、无 workspace、netns 仅 lo），
// 在其中以 python3 -I -B /in/.agentbox/main.py 运行代码。进程内装置（newRealHarness）启用 exec 时取与
// cmd/agentbox 相同的 exec 模板摘要；E37 用真实子进程 cmd/agentbox（exec 默认开启，--exec-slots 4）。
//
// E34（权限边界）按项目负责人的决定不另写提权/攻击探针：由已有的 seccomp 拒绝探针（mount/unshare/setns/pivot_root/
// CLONE_NEW*、ptrace/process_vm_*、bpf/io_uring/keyctl/perf_event_open/userfaultfd、非 AF_UNIX socket）与能力集
// {KILL}、无 ptrace 的回归锁覆盖——不是穷尽证明。E39（UID 范围复用）由 internal/provider/local 的
// TestE39UIDRangeReuse（root）与 resource 的回收/隔离用例覆盖（Plan 15 Task 5 已合入）。

// execImageDigest 与 cmd/agentbox 的 prepareIsolation 相同：确认 exec 模板可用（python3 可解析）并返回其摘要。
func execImageDigest() (string, error) {
	et := rootfs.ExecTemplate()
	if err := et.EnsureExec(); err != nil {
		return "", err
	}
	return rootfs.TemplateDigest(et)
}

// execCfg 是启用 exec 的进程内 Gateway 配置（其余 exec 策略取默认值：slots 4、每任务 2、wall 60 s、内存 512 MiB）。
func execCfg(fu *fakeupstream.Server) app.Config {
	cfg := gatewayCfg(fu, gwSecret(), call.Limits{})
	cfg.Exec = app.ExecConfig{Slots: app.DefaultExecSlots}
	return cfg
}

// execStep 是 sim_worker 的 exec 操作（期望结果状态 completed；inputs 为 [sha256, path]）。
func execStep(stepID, code string, inputs ...[2]string) step {
	s := step{"op": "exec", "step_id": stepID, "code": code, "expect_status": "completed", "wall_ms": 60000}
	if len(inputs) > 0 {
		s["inputs"] = inputs
	}
	return s
}

// execResult 是 /v1/exec 的结果 JSON（结果 blob 与 200 响应体）中测试关心的字段。
type execResult struct {
	Status string `json:"status"`
	Exit   *struct {
		Code   int `json:"code"`
		Signal int `json:"signal"`
	} `json:"exit"`
	Diag *struct {
		OOMKillDelta uint64 `json:"oom_kill_delta"`
		OOMObserved  bool   `json:"oom_observed"`
	} `json:"diag"`
	Stdout          string `json:"stdout"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	Stderr          string `json:"stderr"`
	StderrTruncated bool   `json:"stderr_truncated"`
	Outputs         []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"outputs"`
	SkippedOutputs []struct {
		Reason string `json:"reason"`
	} `json:"skipped_outputs"`
	QueueMs int64 `json:"queue_ms"`
	WallMs  int64 `json:"wall_ms"`
	Limits  struct {
		MemoryBytes int64 `json:"memory_bytes"`
	} `json:"limits"`
}

func (r execResult) exitCode() (code, signal int) {
	if r.Exit == nil {
		return -1, -1
	}
	return r.Exit.Code, r.Exit.Signal
}

func (r execResult) output(path string) (sha string, size int64, ok bool) {
	for _, o := range r.Outputs {
		if o.Path == path {
			return o.SHA256, o.Size, true
		}
	}
	return "", 0, false
}

// execCall 返回任务中 callID 的调用（inspect）与其结果 blob 解析出的 exec 结果。
func (h *harness) execCall(taskID, callID string) (api.CallView, execResult) {
	h.t.Helper()
	for _, c := range h.inspect(taskID).Calls {
		if c.CallID != callID {
			continue
		}
		if c.Endpoint != "/v1/exec" || c.ResultRef == "" {
			h.t.Fatalf("调用 %s：%+v，期望有结果的 /v1/exec 调用", callID, c)
		}
		var r execResult
		if err := json.Unmarshal(h.readBlob(c.ResultRef), &r); err != nil {
			h.t.Fatalf("调用 %s 的结果 blob: %v", callID, err)
		}
		return c, r
	}
	h.t.Fatalf("任务 %s 没有调用 %s", taskID, callID)
	return api.CallView{}, execResult{}
}

// gwRaw 在 attempt 的 socket 上以一条新连接发出一个请求，返回状态码与响应体（exec 结果、/blobs 内容）。
func gwRaw(sock, method, path, callID, body string) (int, []byte, error) {
	tr := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequest(method, "http://gateway"+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if callID != "" {
		req.Header.Set("X-Agentbox-Call-Id", callID)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// execBody 是 /v1/exec 的请求体（memoryBytes 为 0 时不写内存限制）。
func execBody(code string, wallMs, memoryBytes int64) string {
	lim := map[string]int64{"wall_ms": wallMs}
	if memoryBytes > 0 {
		lim["memory_bytes"] = memoryBytes
	}
	b, err := json.Marshal(map[string]any{"language": "python3", "code": code, "limits": lim})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// heldTask 提交一个在第 1 个 checkpoint 处暂停的任务，等它到达暂停点，返回任务 ID 与其 attempt 的 Gateway socket。
func (h *harness) heldTask(requestID string) (string, string) {
	h.t.Helper()
	id := h.submit(requestID, spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, requestID, nil,
		checkpointStep("c1"), progressStep("after hold")))
	h.waitHeld(id)
	in, ok := h.rec(id).init(1)
	if !ok {
		h.t.Fatalf("任务 %s 没有 attempt 1 的 init", id)
	}
	return id, h.gwSocket(in.AttemptID)
}

// waitEnvGone 等待环境的 cgroup 与环境目录都不存在（执行树已清空、/out 已卸载删除）。
func (h *harness) waitEnvGone(envID string) {
	h.t.Helper()
	eventually(h.t, "环境 "+envID+" 的 cgroup 与目录已删除", func() bool {
		_, cgErr := os.Stat(h.envCgroup(envID))
		_, dirErr := os.Stat(filepath.Join(h.dir, "envs", envID))
		return errors.Is(cgErr, os.ErrNotExist) && errors.Is(dirErr, os.ErrNotExist)
	})
}

const (
	execPage    = "x,y\n1,2\n3,4\n10,20\n"
	execSumCode = `import csv, io, json
page = json.load(open("/in/page.json"))
rows = list(csv.reader(io.StringIO(page["content"])))
with open("/out/result.csv", "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["x", "y", "sum"])
    for x, y in rows[1:]:
        w.writerow([x, y, int(x) + int(y)])
print("rows", len(rows) - 1)
`
	execSumCSV    = "x,y,sum\n1,2,3\n3,4,7\n10,20,30\n"
	execTotalCode = `import csv
rows = list(csv.DictReader(open("/in/data/result.csv")))
total = sum(int(r["sum"]) for r in rows)
open("total.txt", "w").write(f"{total}\n")
print("total", total)
`
)

// TestRealExecViaGateway：真实链路——沙箱中的 sim_worker 先 fetch（fake upstream）得到结果 blob，再 exec 以该 blob 为
// /in/page.json、写 /out/result.csv；第二个 exec 以前者的输出为 /in/data/result.csv、写 total.txt（cwd 为 /out）。
// 两次 exec 都 completed（退出码 0），输出经 attempt 的 Gateway GET /blobs/{sha} 读取到预期内容，exec_count_used = 2，
// 每次 exec 在独立的 exec 环境中运行且已停止、清理；任务成功，静止时 I1–I16 成立。
func TestRealExecViaGateway(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	const pagePath = "/data/points.csv"
	fu.SetPage(pagePath, "text/csv", execPage)
	pageURL := fu.URL() + pagePath
	// fetch 的结果 blob 是 upstream.FetchResult 的 JSON（与 fetch adapter 的编码相同），sha 可预先算出。
	fr, err := json.Marshal(upstream.FetchResult{URL: pageURL, FinalURL: pageURL, Status: 200, ContentType: "text/csv",
		Encoding: "utf-8", Content: execPage})
	if err != nil {
		t.Fatal(err)
	}
	fetchSHA, csvSHA, totalSHA := sha256Hex(fr), sha256Hex([]byte(execSumCSV)), sha256Hex([]byte("40\n"))
	id := h.submit("exec-gw", spec(&directives{Hold: &holdSpec{Type: protocol.TypeCheckpoint, Nth: 1}}, "exec", nil,
		step{"op": "fetch", "step_id": "f1", "url": pageURL},
		execStep("x1", execSumCode, [2]string{fetchSHA, "page.json"}),
		execStep("x2", execTotalCode, [2]string{csvSHA, "data/result.csv"}),
		checkpointStep("c1"), progressStep("done")))
	h.waitHeld(id)
	in1, _ := h.rec(id).init(1)
	sock := h.gwSocket(in1.AttemptID)
	for sha, want := range map[string]string{csvSHA: execSumCSV, totalSHA: "40\n"} {
		st, b, err := gwRaw(sock, http.MethodGet, "/blobs/"+sha, "", "")
		if err != nil || st != http.StatusOK || string(b) != want {
			t.Fatalf("GET /blobs/%s = %d %q %v；期望 %q", sha, st, b, err, want)
		}
	}
	h.rec(id).releaseHold()
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	refs := h.checkpointRefs(id, "c1")
	for _, sha := range []string{fetchSHA, csvSHA, totalSHA} {
		if !slices.Contains(refs, sha) {
			t.Fatalf("checkpoint c1 的 refs %v 不含 %s", refs, sha)
		}
	}
	attemptEnv := attemptByNo(h.inspect(id), 1).EnvID
	envs := map[string]bool{}
	for _, c := range []struct{ callID, stdout, out, sha string }{
		{"root/x1/exec/1", "rows 3\n", "result.csv", csvSHA},
		{"root/x2/exec/1", "total 40\n", "total.txt", totalSHA},
	} {
		cv, r := h.execCall(id, c.callID)
		code, sig := r.exitCode()
		sha, size, ok := r.output(c.out)
		if cv.State != "completed" || cv.TriesUsed != 1 || len(cv.Tries) != 1 || cv.Tries[0].Outcome != "ok" ||
			r.Status != "completed" || code != 0 || sig != 0 || r.Stdout != c.stdout || !ok || sha != c.sha ||
			len(r.Outputs) != 1 || size != int64(len(h.readBlob(sha))) {
			t.Fatalf("调用 %s：%+v；结果 %+v", c.callID, cv, r)
		}
		envID := cv.Tries[0].EnvID
		if envID == "" || envID == attemptEnv || envs[envID] || cv.Tries[0].ExecStartedAt == nil || cv.Tries[0].CPUUsec == nil {
			t.Fatalf("调用 %s 的 try %+v：期望独立的、已启动并结算 CPU 的 exec 环境（attempt 环境 %s）", c.callID, cv.Tries[0], attemptEnv)
		}
		envs[envID] = true
		var kind, cleanup string
		var stopped bool
		h.queryRow("SELECT kind, stopped_at IS NOT NULL, cleanup_state FROM environments WHERE env_id = $1", []any{envID},
			&kind, &stopped, &cleanup)
		if kind != "exec" || !stopped {
			t.Fatalf("exec 环境 %s：kind %s，已停止 %v", envID, kind, stopped)
		}
		h.waitEnvGone(envID)
	}
	var used int64
	h.queryRow("SELECT exec_count_used FROM exec_quotas WHERE task_id = $1", []any{id}, &used)
	if used != 2 {
		t.Fatalf("exec_count_used = %d，期望 2", used)
	}
	t.Logf("exec 真实链路：fetch %s → exec x1 → %s（result.csv）→ exec x2 → %s（total.txt）；exec 环境 %v", fetchSHA[:12],
		csvSHA[:12], totalSHA[:12], slices.Collect(maps.Keys(envs)))
	h.finish()
}

// e35Code：主进程 fork 一个后台写入者后立即退出。写入者持续向 /out/log 追加整行 "<序号> <是否已成孤儿>"（O_APPEND 单次
// 写入，不间断；写失败时退出），并继承了 stdout/stderr 管道（主进程退出时管道不会关闭）。主进程等写入者写满 50 行
// 后才退出，此后写入者的 getppid 改变，行尾记为 1。
const e35Code = `import os
r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    parent = os.getppid()
    fd = os.open("/out/log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    i = 0
    while True:
        try:
            os.write(fd, f"{i} {int(os.getppid() != parent)}\n".encode())
        except OSError:
            os._exit(1)
        i += 1
        if i == 50:
            os.write(w, b"x")
            os.close(w)
os.close(w)
os.read(r, 1)
print("main exits; writer", pid, flush=True)
os._exit(0)
`

// TestRealE35BackgroundWriter：E35——主进程退出而后台写入者继续追加 /out/log。收集发生在执行树清空（populated 0）
// 之后：结果中 log 的 size 与 sha 与 blob 一致（两次读取相同）、内容是从 0 开始连续的完整行，且包含主进程退出之后
// 写入的行（写入者在主进程退出后仍在写，收集等它被停止）；调用不因写入者持有 stdout 管道而挂起；exec 环境的
// cgroup 与目录随后删除（文件不再可能增长）。
func TestRealE35BackgroundWriter(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	id := h.submit("e35", spec(nil, "e35", nil, execStep("bg", e35Code)))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	cv, r := h.execCall(id, "root/bg/exec/1")
	code, sig := r.exitCode()
	sha, size, ok := r.output("log")
	if r.Status != "completed" || code != 0 || sig != 0 || !ok || !strings.HasPrefix(r.Stdout, "main exits; writer ") {
		t.Fatalf("E35 结果 %+v", r)
	}
	first, second := h.readBlob(sha), h.readBlob(sha)
	if !bytes.Equal(first, second) || int64(len(first)) != size || sha256Hex(first) != sha {
		t.Fatalf("log 的两次读取不一致或与结果不符：size %d/%d/%d", size, len(first), len(second))
	}
	if !bytes.HasSuffix(first, []byte("\n")) {
		t.Fatalf("log 以不完整的行结束：%q", first[max(0, len(first)-64):])
	}
	lines := strings.Split(strings.TrimSuffix(string(first), "\n"), "\n")
	var orphan int
	for i, l := range lines {
		f := strings.Fields(l)
		if len(f) != 2 || f[0] != strconv.Itoa(i) {
			t.Fatalf("log 第 %d 行 %q 不是连续的完整行", i, l)
		}
		if f[1] == "1" {
			orphan++
		}
	}
	if len(lines) < 50 || orphan == 0 {
		t.Fatalf("log 共 %d 行、主进程退出后写入 %d 行；期望写入者在主进程退出后仍在写", len(lines), orphan)
	}
	envID := cv.Tries[0].EnvID
	h.waitEnvGone(envID)
	t.Logf("E35：log %d 行（主进程退出后 %d 行），%d 字节，sha %s；exec 环境 %s 已删除；wall %d ms", len(lines), orphan,
		size, sha[:12], envID, r.WallMs)
	h.finish()
}

// TestRealE36StdoutFlood：E36——exec 向 stdout 写 100 MB：调用不挂起，stdout 只保留前 1 MiB 并置 stdout_truncated，
// stderr 完整，任务按时完成。
func TestRealE36StdoutFlood(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	const code = `import sys
b = b"x" * (1 << 20)
for _ in range(100):
    sys.stdout.buffer.write(b)
sys.stdout.buffer.flush()
print("done", file=sys.stderr)
`
	start := time.Now()
	id := h.submit("e36", spec(nil, "e36", nil, execStep("flood", code)))
	if v := h.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("任务 %+v", v)
	}
	took := time.Since(start)
	_, r := h.execCall(id, "root/flood/exec/1")
	c, sig := r.exitCode()
	if r.Status != "completed" || c != 0 || sig != 0 || !r.StdoutTruncated || len(r.Stdout) != 1<<20 ||
		strings.Trim(r.Stdout, "x") != "" || r.Stderr != "done\n" || r.StderrTruncated {
		t.Fatalf("E36 结果：status %s exit %d/%d stdout %d 字节 truncated=%v stderr %q", r.Status, c, sig, len(r.Stdout),
			r.StdoutTruncated, r.Stderr)
	}
	if took > 60*time.Second {
		t.Fatalf("100 MB stdout 的任务用时 %s", took)
	}
	t.Logf("E36：100 MB stdout → 保留 %d 字节、stdout_truncated；exec wall %d ms，任务用时 %s", len(r.Stdout), r.WallMs,
		took.Round(time.Millisecond))
	h.finish()
}

// e38FillCode：/out 写满（tmpfs 64 MiB；页面计入环境 cgroup 的内存），然后分配内存直到超过 memory.max。
const e38FillCode = `import errno
written = 0
try:
    with open("/out/fill", "wb", buffering=0) as f:
        while True:
            f.write(b"\0" * (1 << 20))
            written += 1
except OSError as e:
    print("fill", errno.errorcode.get(e.errno), written, flush=True)
hog = []
for i in range(512):
    hog.append(bytearray(1 << 20))
print("survived", len(hog), flush=True)
`

// e38FilesCode：在 /out 中创建 2000 个文件（nr_inodes = 1024）。
const e38FilesCode = `import errno
n = 0
try:
    for i in range(2000):
        open(f"/out/f{i:04d}", "w").close()
        n += 1
except OSError as e:
    print("files", errno.errorcode.get(e.errno), n, flush=True)
else:
    print("files ok", n, flush=True)
`

// TestRealE38ExecPressure：E38——4 个暂停中的任务经各自 attempt 的 Gateway socket 同时发出 8 个 exec（每任务 2 个，
// 全局 slots 4）：
//   - ① /out 写满后分配内存超过 memory.max（96 MiB）：写满得到 ENOSPC/EFBIG，随后环境内 OOM，结果如实报告
//     （signal 9、diag.oom_observed）；
//   - ② 创建 2000 个文件：nr_inodes 用尽得到 ENOSPC，收集 256 个、其余报告 too_many；
//   - ③ 其余 6 个各 sleep 3 s：同时存活的 exec 环境从不超过 4 个（第 5 个起排队，queue_ms 记录等待）；
//   - 压力期间 GET /tasks/{id} 与 GET /status 每次都在 2 s 内响应；
//   - 全部 exec 环境回收，任务成功，静止时 I1、I11、I12 等不变量成立。
func TestRealE38ExecPressure(t *testing.T) {
	fu := fakeupstream.New()
	t.Cleanup(fu.Close)
	h := newRealHarness(t)
	h.start(execCfg(fu))
	ids, socks := make([]string, 4), make([]string, 4)
	for i := range ids {
		ids[i], socks[i] = h.heldTask(fmt.Sprintf("e38-%d", i))
	}

	// 控制面响应：压力期间持续轮询，记录最大延迟与失败。
	stopPoll := make(chan struct{})
	var (
		pollMu     sync.Mutex
		maxLatency time.Duration
		polls      int
		pollErrs   []string
	)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(100 * time.Millisecond):
			}
			for _, p := range []string{"/status", "/tasks/" + ids[0]} {
				t0 := time.Now()
				st, _, err := httpDo("GET", h.base+p, "")
				d := time.Since(t0)
				pollMu.Lock()
				polls++
				maxLatency = max(maxLatency, d)
				if err != nil || st != http.StatusOK || d > 2*time.Second {
					pollErrs = append(pollErrs, fmt.Sprintf("GET %s = %d %v（%s）", p, st, err, d))
				}
				pollMu.Unlock()
			}
		}
	}()
	// 同时存活的 exec 环境数（stopped_at 未记录；slot 在 stopped_at 持久化之后才归还）。
	var maxLive int
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ctx := context.Background()
		c, err := pgx.Connect(ctx, h.dsn)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(ctx) }()
		for {
			select {
			case <-stopPoll:
				return
			case <-time.After(20 * time.Millisecond):
			}
			var n int
			if c.QueryRow(ctx, "SELECT count(*) FROM environments WHERE kind = 'exec' AND stopped_at IS NULL").Scan(&n) == nil {
				pollMu.Lock()
				maxLive = max(maxLive, n)
				pollMu.Unlock()
			}
		}
	}()

	type job struct {
		task   int
		callID string
		body   string
	}
	const sleepCode = "import time\ntime.sleep(3)\nprint('slept')\n"
	jobs := []job{
		{0, "e2e/fill/exec/1", execBody(e38FillCode, 60000, 96<<20)},
		{0, "e2e/files/exec/1", execBody(e38FilesCode, 60000, 0)},
	}
	for i := 1; i < 4; i++ {
		for k := 1; k <= 2; k++ {
			jobs = append(jobs, job{i, fmt.Sprintf("e2e/sleep%d/exec/1", k), execBody(sleepCode, 60000, 0)})
		}
	}
	type answer struct {
		status int
		res    execResult
		raw    []byte
		err    error
	}
	answers := make([]answer, len(jobs))
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, b, err := gwRaw(socks[j.task], http.MethodPost, "/v1/exec", j.callID, j.body)
			a := answer{status: st, raw: b, err: err}
			if err == nil && st == http.StatusOK {
				a.err = json.Unmarshal(b, &a.res)
			}
			answers[i] = a
		}()
	}
	wg.Wait()
	close(stopPoll)
	<-pollDone
	<-sampleDone

	for i, a := range answers {
		if a.err != nil || a.status != http.StatusOK || a.res.Status != "completed" {
			t.Fatalf("exec %s（任务 %d）= %d %v：%s", jobs[i].callID, jobs[i].task, a.status, a.err, a.raw)
		}
	}
	fill, files := answers[0].res, answers[1].res
	if _, sig := fill.exitCode(); !strings.HasPrefix(fill.Stdout, "fill ") || sig != 9 || fill.Diag == nil || !fill.Diag.OOMObserved ||
		strings.Contains(fill.Stdout, "survived") || fill.Limits.MemoryBytes != 96<<20 {
		t.Fatalf("① /out 写满 + 内存压力：stdout %q，exit %+v，diag %+v，limits %+v；期望写满报错后环境内 OOM（signal 9、oom_observed）",
			fill.Stdout, fill.Exit, fill.Diag, fill.Limits)
	}
	if f := strings.Fields(fill.Stdout); len(f) < 2 || (f[1] != "ENOSPC" && f[1] != "EFBIG") {
		t.Fatalf("① 写满 /out 的错误 %q，期望 ENOSPC（tmpfs 写满）或 EFBIG（RLIMIT_FSIZE）", fill.Stdout)
	}
	ff := strings.Fields(files.Stdout)
	n := -1
	if len(ff) == 3 {
		n, _ = strconv.Atoi(ff[2]) // 非数字时为 0，由下方断言报告
	}
	var tooMany int
	for _, s := range files.SkippedOutputs {
		if s.Reason == "too_many" {
			tooMany++
		}
	}
	if len(ff) != 3 || ff[1] != "ENOSPC" || n >= 1024 || n < 256 || len(files.Outputs) != 256 || tooMany != n-256 {
		t.Fatalf("② 2000 个文件：stdout %q，收集 %d 个，too_many %d；期望 nr_inodes 用尽得到 ENOSPC、收集 256 个",
			files.Stdout, len(files.Outputs), tooMany)
	}
	var queued int
	for _, a := range answers {
		if a.res.QueueMs >= 100 {
			queued++
		}
	}
	pollMu.Lock()
	ml, lat, np, perrs := maxLive, maxLatency, polls, pollErrs
	pollMu.Unlock()
	if ml > app.DefaultExecSlots || ml == 0 || queued < len(jobs)-app.DefaultExecSlots {
		t.Fatalf("③ 同时存活的 exec 环境最多 %d 个（slots %d），排队 ≥ 100 ms 的 exec %d 个", ml, app.DefaultExecSlots, queued)
	}
	if len(perrs) != 0 || np == 0 {
		t.Fatalf("压力期间控制面轮询 %d 次，最大延迟 %s，异常：%v", np, lat, perrs)
	}
	for _, id := range ids {
		h.rec(id).releaseHold()
	}
	for _, id := range ids {
		if v := h.waitTerminal(id); v.Status != "succeeded" {
			t.Fatalf("任务 %+v", v)
		}
	}
	var envs, cleaned int
	h.queryRow("SELECT count(*), count(*) FILTER (WHERE cleanup_state = 'done') FROM environments WHERE kind = 'exec'", nil, &envs, &cleaned)
	if envs != len(jobs) {
		t.Fatalf("exec 环境 %d 个，期望 %d", envs, len(jobs))
	}
	t.Logf("E38：① %q exit %+v oom_kill_delta %d；② %q 收集 %d、too_many %d；③ 同时存活的 exec 环境最多 %d 个，排队 ≥ 100 ms 的 %d 个；"+
		"控制面轮询 %d 次、最大延迟 %s", strings.TrimSpace(fill.Stdout), fill.Exit, fill.Diag.OOMKillDelta, strings.TrimSpace(files.Stdout),
		len(files.Outputs), tooMany, ml, queued, np, lat.Round(time.Millisecond))
	h.finish()
}

// ---- E37：exec 进行中 SIGKILL server（cmd/agentbox 真实子进程） ----

const (
	e37CallID = "root/e1/exec/1"
	e37Code   = "import time\ntime.sleep(8)\nprint('done')\n"
)

// e37Try 是一次 exec try 与其环境。
type e37Try struct {
	TryNo     int
	EnvID     string
	Outcome   string
	Error     string
	StoppedAt *time.Time
	CreatedAt time.Time
}

func (s *sysHarness) e37Tries(taskID string) []e37Try {
	s.t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT ct.try_no, ct.env_id, ct.outcome, ct.error, e.stopped_at, e.created_at FROM call_tries ct
		JOIN environments e ON e.env_id = ct.env_id WHERE ct.task_id = $1 AND ct.call_id = $2 ORDER BY ct.try_no`, taskID, e37CallID)
	if err != nil {
		s.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (e37Try, error) {
		var x e37Try
		err := r.Scan(&x.TryNo, &x.EnvID, &x.Outcome, &x.Error, &x.StoppedAt, &x.CreatedAt)
		return x, err
	})
	if err != nil {
		s.t.Fatal(err)
	}
	return out
}

// e37Started 提交 exec sleep 的任务，等 exec 主进程在 exec 环境中运行，返回任务 ID 与该 exec 环境。
func (s *sysHarness) e37Started(requestID string) (string, string) {
	s.t.Helper()
	id := s.submit(requestID, spec(nil, requestID, nil, execStep("e1", e37Code)))
	var envID string
	eventually(s.t, "exec 主进程在 exec 环境中运行", func() bool {
		var started int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM call_tries WHERE task_id = $1 AND call_id = $2 AND exec_started_at IS NOT NULL`,
			[]any{id, e37CallID}, &started)
		if started == 0 {
			return false
		}
		pgQueryRow(s.t, s.dsn, `SELECT env_id FROM call_tries WHERE task_id = $1 AND call_id = $2 AND try_no = 1`,
			[]any{id, e37CallID}, &envID)
		for _, pid := range cgroupPids(filepath.Join(s.installCgroup(), "env-"+envID)) {
			if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil && bytes.Contains(b, []byte("/in/.agentbox/main.py")) {
				return true
			}
		}
		return false
	})
	return id, envID
}

// killServer 以 SIGKILL 杀死当前 server，等待其数据库连接全部结束。
func (s *sysHarness) killServer() {
	s.t.Helper()
	p := s.srv
	if err := p.cmd.Process.Kill(); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(p); !killedBySIGKILL(ps) {
		s.t.Fatalf("server 退出状态 %v", ps)
	}
	s.waitConnsGone()
}

// TestRealE37ServerKilledDuringExec：E37——exec（sleep 8 s）运行中 SIGKILL cmd/agentbox server 并重启：
//   - 旧 exec 环境的 stopped_at 在新 try 的环境建立之前记录（重跑前确认此前的 exec 环境已停止），旧 try 为 unknown；
//   - attempt 1 为 lost_on_restart，attempt 2 的 Worker 以同一 call id 重发，新 try 在新环境中完成，任务成功；
//   - 另一例：先取消任务（202 已返回）再立即杀死 server → 恢复后旧环境已停止，任务 cancelled，没有新的 try。
func TestRealE37ServerKilledDuringExec(t *testing.T) {
	t.Run("rerun", func(t *testing.T) {
		s := newRealSys(t)
		s.startReal()
		id, env1 := s.e37Started("e37")
		s.killServer()
		pre := s.e37Tries(id)
		if len(pre) != 1 || pre[0].StoppedAt != nil {
			t.Fatalf("server 被杀死时的 try：%+v；期望 1 个、环境未记录停止", pre)
		}
		s.startReal()
		v := s.waitTerminal(id)
		in := s.inspect(id)
		tries := s.e37Tries(id)
		if v.Status != "succeeded" || attemptByNo(in, 1).OutcomeClass != runner.ClassLostOnRestart || len(tries) != 2 {
			t.Fatalf("恢复后任务 %s，attempts %s，tries %+v", v.Status, attemptClasses(in), tries)
		}
		old, cur := tries[0], tries[1]
		if old.EnvID != env1 || old.Outcome != "unknown" || old.StoppedAt == nil || cur.EnvID == env1 || cur.Outcome != "ok" ||
			!old.StoppedAt.Before(cur.CreatedAt) {
			t.Fatalf("旧 try %+v，新 try %+v；期望旧环境先确认停止（stopped_at）再建立新 try 的环境", old, cur)
		}
		var res, state string
		pgQueryRow(t, s.dsn, `SELECT state, COALESCE(result_ref, '') FROM calls WHERE task_id = $1 AND call_id = $2`,
			[]any{id, e37CallID}, &state, &res)
		rc, err := s.blobs.Open(res)
		if err != nil {
			t.Fatalf("调用 %s 的结果 blob %q: %v", state, res, err)
		}
		b, err := io.ReadAll(rc)
		_ = rc.Close() // 只读句柄，关闭错误无关紧要
		var r execResult
		if err != nil || json.Unmarshal(b, &r) != nil || state != "completed" || r.Status != "completed" || r.Stdout != "done\n" {
			t.Fatalf("调用 %s；结果 %s %v", state, b, err)
		}
		t.Logf("E37：旧 exec 环境 %s stopped_at %s（unknown）→ 新 try 环境 %s 建立于 %s（ok）；attempts %s", old.EnvID,
			old.StoppedAt.Format(time.RFC3339Nano), cur.EnvID, cur.CreatedAt.Format(time.RFC3339Nano), attemptClasses(in))
		s.finishReal()
	})
	t.Run("cancelled_not_rerun", func(t *testing.T) {
		s := newRealSys(t)
		s.startReal()
		id, env1 := s.e37Started("e37-cancel")
		st, b, err := httpDo("POST", s.srv.base+"/tasks/"+id+"/cancel", `{"request_id":"e37-cancel-req","reason":"e2e"}`)
		if err != nil || (st != http.StatusAccepted && st != http.StatusOK) {
			t.Fatalf("取消 = %d %s %v", st, b, err)
		}
		s.killServer()
		s.startReal()
		v := s.waitTerminal(id)
		time.Sleep(time.Second) // 给任何（错误的）重跑留出时间
		tries := s.e37Tries(id)
		if v.Status != "cancelled" || len(tries) != 1 || tries[0].EnvID != env1 || tries[0].StoppedAt == nil {
			t.Fatalf("取消后杀死 server：任务 %s，tries %+v；期望 cancelled、只有原来的 1 个 try 且其环境已停止", v.Status, tries)
		}
		t.Logf("E37（取消）：任务 %s，唯一 try %+v", v.Status, tries[0])
		s.finishReal()
	})
}

// ==== M4 Plan 15 Task 12 段结束 ====
