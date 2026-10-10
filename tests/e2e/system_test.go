//go:build linux

// 系统级验收：真实 server 子进程（E5、E6、E13–E16）。装置与共用辅助函数见 e2e_test.go。

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/api"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/cli"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/datadir"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/faultinject"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/sandbox"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/tests/e2e/procprov"
)

// ==== 系统级验收：真实 server 子进程（E5、E6、E13–E16）====
//
// 以下用例把只供测试的 tests/e2e/agentbox-e2e（与 cmd/agentbox server 相同的 app.Run，provider 换成进程型
// 的 tests/e2e/procprov，并开启 internal/faultinject）构建一次，作为真实子进程运行：SIGKILL、重启、失锁与
// 数据库断连都作用于真实进程。每个用例结束时：环境全部停止并清理、UID 范围归还、没有存活的执行进程、没有
// 隔离记录，正常停止 server 后运行 `agentbox-e2e verify-invariants --quiescent`。

var (
	serverBinOnce sync.Once
	serverBinDir  string
	serverBinPath string
	serverBinErr  error
)

func TestMain(m *testing.M) {
	// 真实隔离用例（provider/local + 生产启动器）以 /proc/self/exe 再执行本测试二进制作为专用启动进程、
	// 沙箱 init 与 stage-2 helper：与 cmd/agentbox 的 main 相同，分流必须早于其他一切。
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case sandbox.HelperArg:
			sandbox.RunHelper() // 成功时 execve workload，不返回
			os.Exit(126)
		case sandbox.InitArg:
			if err := sandbox.RunInit(); err != nil {
				fmt.Fprintln(os.Stderr, "sandbox init:", err)
				os.Exit(1)
			}
			os.Exit(0)
		case sandbox.LaunchArg:
			if err := sandbox.RunLaunch(); err != nil {
				fmt.Fprintln(os.Stderr, "sandbox-launch:", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	code := m.Run()
	if serverBinDir != "" {
		_ = os.RemoveAll(serverBinDir)
	}
	if agentboxBinDir != "" {
		_ = os.RemoveAll(agentboxBinDir)
	}
	if swDir != "" {
		_ = os.RemoveAll(swDir)
	}
	if realCgRoot != "" {
		killRemove(realCgRoot)
	}
	os.Exit(code)
}

// goBinary 返回用于构建测试二进制的 go 命令：优先 PATH 中的 go；找不到时（例如 sudo 重置了 PATH）用 go 命令
// 传给测试进程的 GOROOT 环境变量（runtime.GOROOT 自 Go 1.24 起已弃用）。
func goBinary() string {
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	if root := os.Getenv("GOROOT"); root != "" {
		return filepath.Join(root, "bin", "go")
	}
	return "go"
}

// serverBinary 构建 agentbox-e2e（每次 go test 一次）。
func serverBinary(t *testing.T) string {
	t.Helper()
	serverBinOnce.Do(func() {
		goBin := goBinary()
		if serverBinDir, serverBinErr = os.MkdirTemp("", "agentbox-e2e-bin-"); serverBinErr != nil {
			return
		}
		serverBinPath = filepath.Join(serverBinDir, "agentbox-e2e")
		if out, err := exec.Command(goBin, "build", "-o", serverBinPath, "./agentbox-e2e").CombinedOutput(); err != nil {
			serverBinErr = fmt.Errorf("构建 agentbox-e2e: %v\n%s", err, out)
		}
	})
	if serverBinErr != nil {
		t.Fatal(serverBinErr)
	}
	return serverBinPath
}

// serverProc 是一个 server 子进程。
type serverProc struct {
	cmd  *exec.Cmd
	logs *lockedBuffer
	done chan struct{}
	base string
}

func (p *serverProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// sysHarness 是一个数据目录与一个数据库上的 server 子进程序列（启动、被杀死、重启）。
type sysHarness struct {
	t         *testing.T
	bin       string
	dir       string
	dsn       string // 测试侧连接（application_name = testAppName）
	serverDSN string // server 的 advisory lock 连接
	storeDSN  string // server 业务连接（E14 经 TCP 代理）
	pyPath    string
	procs     []*serverProc
	srv       *serverProc
	store     *postgres.Store
	blobs     *blob.Local
}

// testAppName 是测试侧数据库连接的 application_name：据此区分 server 的连接（E5 等待被杀死的 server 的
// 连接全部结束）。
const testAppName = "agentbox-e2e-test"

func withAppName(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("application_name", testAppName)
	u.RawQuery = q.Encode()
	return u.String()
}

func newSys(t *testing.T) *sysHarness {
	t.Helper()
	s := &sysHarness{t: t, pyPath: workerPath(t), serverDSN: newDatabase(t), dir: t.TempDir()}
	s.dsn = withAppName(t, s.serverDSN)
	s.bin = serverBinary(t)
	s.storeDSN = s.serverDSN
	t.Cleanup(func() {
		for _, p := range s.procs {
			if !p.exited() {
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		}
		if pids, _ := procprov.LiveUnder(s.dir); len(pids) > 0 {
			for _, pid := range pids {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
			t.Errorf("测试结束时仍有 %d 个执行进程（已杀死）", len(pids))
		}
		if s.store != nil {
			s.store.Close()
		}
		if t.Failed() {
			for i, p := range s.procs {
				t.Logf("server #%d 日志（末尾）：\n%s", i+1, p.logs.tail(16<<10))
			}
		}
	})
	return s
}

// start 启动 server 子进程（fault 非空时设置 AGENTBOX_FAULT），等待它开始监听。
func (s *sysHarness) start(fault string, extra ...string) *serverProc {
	s.t.Helper()
	addrFile := filepath.Join(s.dir, fmt.Sprintf("addr-%d", len(s.procs)+1))
	args := append([]string{"server", "--data-dir", s.dir, "--database-url", s.storeDSN, "--lock-database-url", s.serverDSN,
		"--addr-file", addrFile, "--pythonpath", s.pyPath}, extra...)
	cmd := exec.Command(s.bin, args...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, faultinject.Env+"=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if fault != "" {
		cmd.Env = append(cmd.Env, faultinject.Env+"="+fault)
	}
	p := &serverProc{cmd: cmd, logs: &lockedBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.procs = append(s.procs, p)
	s.srv = p
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	eventually(s.t, "server 开始监听", func() bool {
		if b, err := os.ReadFile(addrFile); err == nil && len(b) > 0 {
			p.base = "http://" + string(b)
			return true
		}
		if p.exited() {
			s.t.Fatalf("server 提前退出（%v）", cmd.ProcessState)
		}
		return false
	})
	if s.store == nil {
		st, err := postgres.Open(context.Background(), postgres.Options{DSN: s.dsn})
		if err != nil {
			s.t.Fatal(err)
		}
		s.store = st
		if s.blobs, err = blob.NewLocal(filepath.Join(s.dir, "blobs")); err != nil {
			s.t.Fatal(err)
		}
	}
	return p
}

func (s *sysHarness) waitExit(p *serverProc) *os.ProcessState {
	s.t.Helper()
	select {
	case <-p.done:
	case <-time.After(waitLimit):
		s.t.Fatal("server 未在期限内退出")
	}
	return p.cmd.ProcessState
}

func killedBySIGKILL(ps *os.ProcessState) bool {
	ws, ok := ps.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}

// stop 以 SIGTERM 正常停止当前 server，要求退出码 0。
func (s *sysHarness) stop() {
	s.t.Helper()
	if err := s.srv.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		s.t.Fatal(err)
	}
	if ps := s.waitExit(s.srv); !ps.Success() {
		s.t.Fatalf("server 正常停止的退出状态为 %v", ps)
	}
}

func (s *sysHarness) submitE(base, requestID, spec string) (string, error) {
	st, b, err := httpDo("POST", base+"/tasks", `{"request_id":"`+requestID+`","spec":`+spec+`}`)
	if err != nil {
		return "", err
	}
	var r struct {
		TaskID string `json:"task_id"`
	}
	if st != http.StatusCreated || json.Unmarshal(b, &r) != nil || r.TaskID == "" {
		return "", fmt.Errorf("POST /tasks = %d %s", st, b)
	}
	return r.TaskID, nil
}

func (s *sysHarness) submit(requestID, spec string) string {
	s.t.Helper()
	id, err := s.submitE(s.srv.base, requestID, spec)
	if err != nil {
		s.t.Fatal(err)
	}
	return id
}

func (s *sysHarness) waitTerminal(id string) api.TaskView {
	s.t.Helper()
	var v api.TaskView
	eventually(s.t, "任务 "+id+" 到达终态", func() bool {
		var err error
		v, err = s.store.GetTask(context.Background(), id)
		return err == nil && task.IsTerminal(v.Status)
	})
	return v
}

func (s *sysHarness) inspect(id string) api.Inspection {
	s.t.Helper()
	in, err := s.store.Inspect(context.Background(), id)
	if err != nil {
		s.t.Fatalf("Inspect %s: %v", id, err)
	}
	return in
}

func (s *sysHarness) loadTask(id string) task.TaskState {
	s.t.Helper()
	ts, err := s.store.LoadTask(context.Background(), id)
	if err != nil {
		s.t.Fatalf("LoadTask %s: %v", id, err)
	}
	return ts
}

func (s *sysHarness) events(id string) []api.Event {
	s.t.Helper()
	evs, err := s.store.ListEvents(context.Background(), id, 0, 100000)
	if err != nil {
		s.t.Fatal(err)
	}
	return evs
}

func (s *sysHarness) envDir(envID string) string { return filepath.Join(s.dir, "envs", envID) }

// scanner 是测试侧的只读 procprov（独立扫描数据目录与 /proc）。
func (s *sysHarness) scanner() *procprov.Provider {
	s.t.Helper()
	id, ok, err := datadir.NewIDFile(s.dir).Read()
	if err != nil || !ok {
		s.t.Fatalf("读取 install_id: %v %v", ok, err)
	}
	p, err := procprov.New(procprov.Options{DataDir: s.dir, InstallID: id, ReadOnly: true})
	if err != nil {
		s.t.Fatal(err)
	}
	return p
}

func pgQueryRow(t *testing.T, dsn, sql string, args []any, dest ...any) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if err := c.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// finish 是每个系统级用例结束时的检查：无泄漏、无隔离记录；正常停止 server 后 verify-invariants --quiescent
// 通过。
func (s *sysHarness) finish() {
	s.t.Helper()
	scan := s.scanner()
	eventuallyWithin(s.t, "环境全部停止并清理、UID 范围归还、没有执行进程与环境目录", waitLimit, 50*time.Millisecond, func() bool {
		var pending, held int
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM environments WHERE cleanup_state <> 'done' OR stopped_at IS NULL", nil, &pending)
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM uid_ranges WHERE state <> 'free'", nil, &held)
		envs, err := scan.List(context.Background())
		pids, perr := procprov.LiveUnder(s.dir)
		return pending == 0 && held == 0 && err == nil && len(envs) == 0 && perr == nil && len(pids) == 0
	})
	var quarantined int
	pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM quarantined_resources", nil, &quarantined)
	if quarantined != 0 {
		s.t.Fatalf("误隔离：%d 条隔离记录", quarantined)
	}
	s.stop()
	out, err := exec.Command(s.bin, "verify-invariants", "--quiescent", "--data-dir", s.dir, "--database-url", s.dsn).CombinedOutput()
	if err != nil {
		s.t.Fatalf("verify-invariants --quiescent：%v\n%s", err, out)
	}
	s.t.Logf("verify-invariants --quiescent：%s", strings.TrimSpace(string(out)))
}

// ---- 执行计数（sim_worker 的 exec_log）与执行日志（procprov.journal）----

type execRec struct {
	AttemptID string `json:"attempt_id"`
	AttemptNo int64  `json:"attempt_no"`
	Index     int    `json:"index"`
	Op        string `json:"op"`
}

func readExecLog(t *testing.T, path string) []execRec {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		t.Fatal(err)
	}
	var out []execRec
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var r execRec
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("exec_log 行 %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// sysSpec 是带执行计数的任务 spec。
func sysSpec(execLog, summary string, outputs []string, steps ...step) string {
	m := map[string]any{"steps": steps, "summary": summary, "outputs": outputs, "exec_log": execLog}
	if outputs == nil {
		m["outputs"] = []string{}
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// checkpointLoop 返回 n 轮"progress → sleep → checkpoint c<i>"，以及 step_id → 步骤下标。
func checkpointLoop(n, sleepMs int) ([]step, map[string]int) {
	var steps []step
	idx := map[string]int{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("c%d", i)
		steps = append(steps, progressStep(id), sleepStep(sleepMs), checkpointStep(id))
		idx[id] = len(steps) - 1
	}
	return steps, idx
}

// assertNoDuplicateExecution 由 sim_worker 的执行计数判断没有重复执行（§16.4 E5、E6）：
//   - 不同 attempt 的记录不交错（旧 attempt 在新 attempt 开始执行后不再执行任何步骤）；
//   - 每个 attempt 从"之前的 attempt 提交的最新 checkpoint 的下一步"开始，按序逐步执行，没有重做已提交
//     checkpoint 之前的步骤；
//   - 只有数据库中存在的 attempt 执行过；成功的最后一个 attempt 恰产生一次 result。
//
// 另以 procprov 的执行日志检查 I2 的物理侧：每个 attempt 的执行进程开始之前，上一个 attempt 的执行树已退出
// 或已确认停止。
func (s *sysHarness) assertNoDuplicateExecution(in api.Inspection, recs []execRec, stepIndex map[string]int, nSteps int) {
	s.t.Helper()
	byID := map[string]api.AttemptView{}
	for _, a := range in.Attempts {
		byID[a.AttemptID] = a
	}
	// 之前的 attempt 提交的最新 checkpoint → 起始步骤。
	startOf := func(no int64) int {
		var seq int64
		start := 0
		for _, c := range in.Checkpoints {
			if a, ok := byID[c.AttemptID]; ok && a.AttemptNo < no && c.CommitSeq > seq {
				seq, start = c.CommitSeq, stepIndex[c.StepID]+1
			}
		}
		return start
	}
	var lastNo int64
	next := map[int64]int{}
	results := map[int64]int{}
	for i, r := range recs {
		a, ok := byID[r.AttemptID]
		switch {
		case !ok || a.AttemptNo != r.AttemptNo:
			s.t.Fatalf("执行计数第 %d 行来自数据库中不存在的 attempt：%+v", i, r)
		case r.AttemptNo < lastNo:
			s.t.Fatalf("执行计数第 %d 行：attempt %d 在 attempt %d 开始执行之后仍在执行（%+v）", i, r.AttemptNo, lastNo, r)
		}
		lastNo = r.AttemptNo
		want, seen := next[r.AttemptNo]
		if !seen {
			want = startOf(r.AttemptNo)
		}
		if r.Index != want {
			s.t.Fatalf("attempt %d 执行了第 %d 步，期望第 %d 步（重复执行或跳过）：%+v", r.AttemptNo, r.Index, want, recs)
		}
		next[r.AttemptNo] = r.Index + 1
		if r.Op == "result" {
			results[r.AttemptNo]++
		}
	}
	last := in.Attempts[len(in.Attempts)-1]
	if last.OutcomeClass == runner.ClassSucceeded && (results[last.AttemptNo] != 1 || next[last.AttemptNo] != nSteps+1) {
		s.t.Fatalf("成功的 attempt %d 的执行计数不完整：%+v", last.AttemptNo, recs)
	}

	journal, err := procprov.ReadJournal(s.dir)
	if err != nil {
		s.t.Fatal(err)
	}
	started, dead := map[string]int64{}, map[string]int64{}
	for _, e := range journal {
		switch e.Event {
		case "start":
			if _, ok := started[e.EnvID]; !ok {
				started[e.EnvID] = e.TimeNs
			}
		case "exit", "stopped":
			if _, ok := dead[e.EnvID]; !ok && started[e.EnvID] != 0 {
				dead[e.EnvID] = e.TimeNs
			}
		}
	}
	atts := slices.Clone(in.Attempts)
	slices.SortFunc(atts, func(a, b api.AttemptView) int { return int(a.AttemptNo - b.AttemptNo) })
	for i := 1; i < len(atts); i++ {
		prev, cur := atts[i-1].EnvID, atts[i].EnvID
		if started[prev] == 0 || started[cur] == 0 {
			continue
		}
		if dead[prev] == 0 || started[cur] <= dead[prev] {
			s.t.Fatalf("attempt %d 的执行开始时 attempt %d 的执行树尚未确认结束（I2）：%+v", atts[i].AttemptNo, atts[i-1].AttemptNo, journal)
		}
	}
}

// readPinnedOutputs 读取 attempt 的 result 提议并按固定版本读回输出（哈希经 BlobStore 校验）。
func (s *sysHarness) readPinnedOutputs(taskID, attemptID string) map[string]string {
	s.t.Helper()
	ctx := context.Background()
	p, err := s.store.GetTerminalProposal(ctx, attemptID)
	if err != nil || p.Kind != "result" {
		s.t.Fatalf("attempt %s 的终态提议 %+v %v", attemptID, p, err)
	}
	read := func(sum string) []byte {
		rc, err := s.blobs.Open(sum)
		if err != nil {
			s.t.Fatalf("读取 blob %s: %v", sum, err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil || sha256Hex(b) != sum {
			s.t.Fatalf("blob %s 内容哈希不符（%v）", sum, err)
		}
		return b
	}
	var r struct {
		Outputs []pinnedRef `json:"outputs"`
	}
	if err := json.Unmarshal(read(p.Ref), &r); err != nil {
		s.t.Fatal(err)
	}
	out := map[string]string{}
	for _, o := range r.Outputs {
		v, err := s.store.GetArtifact(ctx, taskID, o.ArtifactID, o.SHA256)
		if err != nil || v.Version != o.Version {
			s.t.Fatalf("固定输出 %+v 在 Store 中为 %+v %v", o, v, err)
		}
		out[o.ArtifactID] = string(read(o.SHA256))
	}
	return out
}

// ---- E5 ----

// e5Phase 是崩溃时任务所处的阶段（由重启前的持久化事实判定），决定 §14.2 的恢复行为。
type e5Phase int

const (
	e5NoAttempt   e5Phase = iota // attempt 尚未创建：保持排队，重启后正常执行，不计故障重试
	e5OpenAttempt                // 有本次重启丢失的 attempt：确认停止后 lost_on_restart，按故障重试排队
	e5Terminal                   // 裁决已提交：保留终态，只处理残留资源
)

func (p e5Phase) String() string {
	return [...]string{"尚无 attempt", "attempt 未结束", "裁决已提交"}[p]
}

// e5Phases 是每个钩子点允许的阶段。停止环境与提交裁决由 actor 并发发出（task.Decide 的 finish 同时发出
// StopEnvironment 与 Finalize），因此停止前后崩溃时裁决可能已提交，也可能尚未提交；其余钩子点的阶段确定。
var e5Phases = map[string][]e5Phase{
	faultinject.AttemptCreateBefore:    {e5NoAttempt},
	faultinject.AttemptCreateAfter:     {e5OpenAttempt},
	faultinject.EnvCreateBefore:        {e5OpenAttempt},
	faultinject.EnvCreateAfter:         {e5OpenAttempt},
	faultinject.WorkerStarted:          {e5OpenAttempt},
	faultinject.CheckpointCommitBefore: {e5OpenAttempt},
	faultinject.CheckpointCommitAfter:  {e5OpenAttempt},
	faultinject.EnvStopBefore:          {e5OpenAttempt, e5Terminal},
	faultinject.EnvStopAfter:           {e5OpenAttempt, e5Terminal},
	faultinject.VerdictBefore:          {e5OpenAttempt},
	faultinject.VerdictAfter:           {e5Terminal},
	faultinject.CleanupDestroyed:       {e5Terminal}, // cleanup 只处理已有裁决的 attempt 的环境
}

// TestE5ServerKilledAtHookPoints：E5——在每个钩子点 SIGKILL server 并重启 → 按 §14.2 恢复；无重复执行
// （执行计数与执行日志）；孤儿回收（上一进程留下的存活执行树与环境目录全部停止并清理）；无误隔离。
// -short 只运行其中 4 个点。
func TestE5ServerKilledAtHookPoints(t *testing.T) {
	var points []string
	for _, p := range faultinject.Points {
		_, e5 := e5Phases[p]
		switch {
		case gatewayPoints[p] != "":
			continue // Gateway 的钩子点由各自的实验覆盖（任务不经 Gateway 时不会到达）
		case !e5:
			t.Fatalf("钩子点 %s 没有期望的恢复行为", p)
		}
		points = append(points, p)
	}
	if testing.Short() {
		points = []string{faultinject.WorkerStarted, faultinject.CheckpointCommitAfter, faultinject.VerdictBefore, faultinject.VerdictAfter}
	}
	start := time.Now()
	t.Cleanup(func() {
		t.Logf("E5：%d 个钩子点 × 重启，用时 %s", len(points), time.Since(start).Round(time.Millisecond))
	})
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			e5Case(t, point, e5Phases[point])
		})
	}
}

func e5Case(t *testing.T, point string, allowed []e5Phase) {
	s := newSys(t)
	execLog := filepath.Join(s.dir, "exec.log")
	steps := []step{
		progressStep("begin"),
		artifactStep("a1", "a1.txt", "one"),
		checkpointStep("s1"),
		sleepStep(50),
		artifactStep("a2", "a2.txt", "two"),
		checkpointStep("s2"),
		progressStep("end"),
	}
	stepIndex := map[string]int{"s1": 2, "s2": 5}
	first := s.start(point + ":1")
	id := s.submit("e5", sysSpec(execLog, "e5", []string{"a1", "a2"}, steps...))

	// server 在钩子点被 SIGKILL。
	ps := s.waitExit(first)
	if !killedBySIGKILL(ps) || !strings.Contains(first.logs.tail(1<<20), "faultinject: SIGKILL at "+point+":1") {
		t.Fatalf("server 应在 %s 被 SIGKILL，退出状态 %v", point, ps)
	}
	// 被杀死的进程已发出的 COMMIT 仍可能在数据库中完成：等它的连接全部结束，重启前的事实才是稳定的。
	eventuallyWithin(t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
	pre := s.inspect(id)
	preTask := s.loadTask(id)
	var phase e5Phase
	switch {
	case len(pre.Attempts) == 0 && !task.IsTerminal(pre.Task.Status):
		phase = e5NoAttempt
	case len(pre.Attempts) == 1 && pre.Attempts[0].Status != "ended" && !task.IsTerminal(pre.Task.Status):
		phase = e5OpenAttempt
	case len(pre.Attempts) == 1 && pre.Attempts[0].Status == "ended" && pre.Task.Status == "succeeded":
		phase = e5Terminal
	default:
		t.Fatalf("重启前的事实不属于任何阶段：%+v", pre)
	}
	if !slices.Contains(allowed, phase) {
		t.Fatalf("在 %s 崩溃后阶段为「%s」，期望 %v：%+v", point, phase, allowed, pre)
	}
	survivors, _ := procprov.LiveUnder(s.dir)
	preEnvs, _ := s.scanner().List(context.Background())

	s.start("")
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	if v.Status != "succeeded" || countType(s.events(id), "task_terminal") != 1 {
		t.Fatalf("恢复后任务 %+v，task_terminal 事件 %d 个", v, countType(s.events(id), "task_terminal"))
	}
	switch phase {
	case e5NoAttempt:
		if len(in.Attempts) != 1 || ts.FaultRetriesUsed != 0 {
			t.Fatalf("排队中崩溃：attempts %+v，fault_retries_used=%d；期望 1 个 attempt、不计重试", in.Attempts, ts.FaultRetriesUsed)
		}
	case e5OpenAttempt:
		a1 := attemptByNo(in, 1)
		if len(in.Attempts) != 2 || a1.OutcomeClass != "lost_on_restart" || ts.FaultRetriesUsed != preTask.FaultRetriesUsed+1 ||
			ts.AttemptsTotal != 2 {
			t.Fatalf("丢失的 attempt：attempts %+v，fault_retries_used=%d attempts_total=%d；期望 lost_on_restart 与 1 次故障重试",
				in.Attempts, ts.FaultRetriesUsed, ts.AttemptsTotal)
		}
	case e5Terminal:
		if len(in.Attempts) != 1 || ts.FaultRetriesUsed != preTask.FaultRetriesUsed || ts.AttemptsTotal != preTask.AttemptsTotal {
			t.Fatalf("裁决已提交：attempts %+v；终态之后不应有新 attempt 或计数变化", in.Attempts)
		}
	}
	last := in.Attempts[len(in.Attempts)-1]
	if last.OutcomeClass != runner.ClassSucceeded {
		t.Fatalf("最后一个 attempt %+v", last)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	if got := s.readPinnedOutputs(id, last.AttemptID); got["a1"] != "one" || got["a2"] != "two" || len(got) != 2 {
		t.Fatalf("固定输出 %q", got)
	}
	t.Logf("%s：重启前 %d 个环境目录、%d 个存活执行进程；恢复后 %d 个 attempt（%s），fault_retries_used=%d",
		point, len(preEnvs), len(survivors), len(in.Attempts), attemptClasses(in), ts.FaultRetriesUsed)
	s.finish()
}

func attemptClasses(in api.Inspection) string {
	var cs []string
	for _, a := range in.Attempts {
		cs = append(cs, a.OutcomeClass)
	}
	return strings.Join(cs, ", ")
}

// ---- E13 ----

// TestE13AdvisoryLockConnectionKilled：E13——终止 advisory lock 的连接（pg_locks 中 locktype = advisory 的
// 后端），业务连接仍存活，且有一个业务事务正在等待行锁（在途）。server 检测到失去所有权后进入
// ownership_lost、停止本机全部执行树并以非零状态退出；记录检测延迟与在途事务的最终结果；重启后完整恢复。
func TestE13AdvisoryLockConnectionKilled(t *testing.T) {
	s := newSys(t)
	ctx := context.Background()
	execLog := filepath.Join(s.dir, "exec.log")
	steps, stepIndex := checkpointLoop(30, 20)
	srv := s.start("", "--check-interval", "100ms")
	id := s.submit("e13", sysSpec(execLog, "e13", nil, steps...))
	eventually(t, "第一个 checkpoint 已提交", func() bool { return len(s.inspect(id).Checkpoints) >= 1 })

	// 在途业务事务：另一连接持有该任务 task_progress 行的锁，下一次 checkpoint 提交等待它。
	holder, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close(ctx) }()
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1 FROM task_progress WHERE task_id = $1 FOR UPDATE", id); err != nil {
		t.Fatal(err)
	}
	before := len(s.inspect(id).Checkpoints)
	admin, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	var inflight string
	eventually(t, "server 的业务事务在等待行锁", func() bool {
		err := admin.QueryRow(ctx, `SELECT query FROM pg_stat_activity WHERE datname = current_database()
			AND wait_event_type = 'Lock' AND pid <> pg_backend_pid() LIMIT 1`).Scan(&inflight)
		return err == nil
	})
	var lockPID, business int32
	if err := admin.QueryRow(ctx, `SELECT a.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND a.datname = current_database()`).Scan(&lockPID); err != nil {
		t.Fatalf("找不到 advisory lock 的后端：%v", err)
	}
	var holderPID int32
	if err := holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		AND pid NOT IN (pg_backend_pid(), $1, $2)`, lockPID, holderPID).Scan(&business); err != nil {
		t.Fatal(err)
	}
	killedAt := time.Now()
	var terminated bool
	if err := admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", lockPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("pg_terminate_backend(%d) = %v %v", lockPID, terminated, err)
	}

	ps := s.waitExit(srv)
	exitAfter := time.Since(killedAt)
	logs := srv.logs.tail(1 << 20)
	if ps.Success() || killedBySIGKILL(ps) || !strings.Contains(logs, "ownership_lost") {
		t.Fatalf("失锁后 server 应以 ownership_lost 非零退出，退出状态 %v", ps)
	}
	detected, ok := logTime(logs, func(m map[string]any) bool { return m["msg"] == "API 模式" && m["mode"] == "ownership_lost" })
	if !ok {
		t.Fatal("日志中没有进入 ownership_lost 的记录")
	}
	// 本地执行树在退出前全部停止。
	if pids, _ := procprov.LiveUnder(s.dir); len(pids) != 0 {
		t.Fatalf("server 退出后仍有执行进程 %v", pids)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	after := len(s.inspect(id).Checkpoints)
	result := "未提交（随 ownership_lost 取消，连接随进程退出关闭）"
	if after != before {
		result = fmt.Sprintf("已提交（checkpoint %d → %d）", before, after)
	}
	t.Logf("E13：终止锁连接（pid %d）时另有 %d 个业务连接存活；检测延迟 %s，进程退出 %s；在途事务 %q 的最终结果：%s",
		lockPID, business, detected.Sub(killedAt).Round(time.Millisecond), exitAfter.Round(time.Millisecond),
		strings.Join(strings.Fields(inflight), " "), result)
	if business == 0 {
		t.Fatal("终止锁连接时没有存活的业务连接，用例无效")
	}

	// 重启：完整恢复。丢失的 attempt 以 lost_on_restart 结束，新 attempt 从最新 checkpoint 继续。
	s.start("")
	if v := s.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("重启后任务结束为 %+v", v)
	}
	in := s.inspect(id)
	ts := s.loadTask(id)
	if len(in.Attempts) != 2 || attemptByNo(in, 1).OutcomeClass != "lost_on_restart" || ts.FaultRetriesUsed != 1 {
		t.Fatalf("attempts %+v，fault_retries_used=%d", in.Attempts, ts.FaultRetriesUsed)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	s.finish()
}

// logTime 返回第一条满足 match 的 JSON 日志的时间。
func logTime(logs string, match func(map[string]any) bool) (time.Time, bool) {
	for _, line := range strings.Split(logs, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || !match(m) {
			continue
		}
		s, _ := m["time"].(string)
		at, err := time.Parse(time.RFC3339Nano, s)
		return at, err == nil
	}
	return time.Time{}, false
}

// ---- E14 ----

// tcpProxy 是测试控制的 TCP 代理：Cut 断开全部连接并拒绝新连接（接受后立即关闭），Restore 恢复转发。
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	cut    bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() {
		_ = ln.Close()
		p.Cut()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *tcpProxy) serve(c net.Conn) {
	p.mu.Lock()
	if p.cut {
		p.mu.Unlock()
		_ = c.Close()
		return
	}
	p.mu.Unlock()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.cut {
		p.mu.Unlock()
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
	_ = c.Close()
	_ = up.Close()
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, up)
	p.mu.Unlock()
}

// Cut 断开全部现有连接，之后的新连接被立即关闭。
func (p *tcpProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		_ = c.Close()
	}
}

// Restore 恢复转发。
func (p *tcpProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = false
}

// TestE14PostgresUnavailable：E14——任务中途 PostgreSQL 不可用（server 的业务连接经测试控制的 TCP 代理，
// 代理切断全部连接；advisory lock 的专用连接直连，所有权保持），server 不重启：runner 达到 Store 故障阈值后
// 以 store_unavailable 终止，执行树被物理停止（不依赖数据库写入）；恢复连接后由现有 actor 与 coordinator
// 补提交停止、裁决与状态，任务按故障重试从已提交的 checkpoint 继续并成功。
func TestE14PostgresUnavailable(t *testing.T) {
	s := newSys(t)
	u, err := url.Parse(s.serverDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newTCPProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()
	s.storeDSN = u.String()
	execLog := filepath.Join(s.dir, "exec.log")
	// 每轮提交一个 checkpoint：切断时还有大量剩余步骤，恢复后的 attempt 只执行剩余部分。
	steps, stepIndex := checkpointLoop(150, 40)
	srv := s.start("")
	id := s.submit("e14", sysSpec(execLog, "e14", nil, steps...))
	eventually(t, "已提交 3 个 checkpoint", func() bool { return len(s.inspect(id).Checkpoints) >= 3 })
	env1 := attemptByNo(s.inspect(id), 1).EnvID
	if env1 == "" {
		t.Fatal("attempt 1 没有环境")
	}

	cutAt := time.Now()
	proxy.Cut()
	eventually(t, "Store 不可用期间执行树被物理停止", func() bool {
		pids, err := procprov.Marked(s.envDir(env1))
		return err == nil && len(pids) == 0
	})
	stoppedAfter := time.Since(cutAt)
	if srv.exited() {
		t.Fatalf("Store 不可用时 server 不应退出：%v", srv.cmd.ProcessState)
	}
	var stoppedNull bool
	var attStatus string
	pgQueryRow(t, s.dsn, "SELECT stopped_at IS NULL FROM environments WHERE env_id = $1", []any{env1}, &stoppedNull)
	pgQueryRow(t, s.dsn, "SELECT status FROM attempts WHERE env_id = $1", []any{env1}, &attStatus)
	if !stoppedNull || attStatus == "ended" {
		t.Fatalf("断连期间数据库被写入：stopped_at 为空=%v，attempt 状态 %s", stoppedNull, attStatus)
	}
	t.Logf("E14：切断数据库连接 %s 后执行树被物理停止", stoppedAfter.Round(time.Millisecond))

	proxy.Restore()
	if v := s.waitTerminal(id); v.Status != "succeeded" {
		t.Fatalf("恢复连接后任务结束为 %+v", v)
	}
	in := s.inspect(id)
	ts := s.loadTask(id)
	a1 := attemptByNo(in, 1)
	if a1.OutcomeClass != runner.ClassStoreUnavailable || a1.StoppedAt == nil || ts.FaultRetriesUsed != 1 || ts.AttemptsTotal != 2 {
		t.Fatalf("attempt 1 %+v；fault_retries_used=%d attempts_total=%d", a1, ts.FaultRetriesUsed, ts.AttemptsTotal)
	}
	if len(s.procs) != 1 || srv.exited() {
		t.Fatal("server 被重启或已退出：补齐应由现有所有者完成")
	}
	if n := countType(s.events(id), "attempt_ended"); n != 1 { // 与 E8 相同：故障结束的 attempt 一个
		t.Fatalf("attempt_ended 事件 %d 个", n)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), stepIndex, len(steps))
	s.finish()
}

// ---- E15 ----

// dropFirstPost 是 API 前的反向代理：第一个 POST /tasks 照常交给 server 处理（读完响应，服务端已提交），
// 但不把响应交给客户端，直接断开连接。
type dropFirstPost struct {
	backend *url.URL
	proxy   *httputil.ReverseProxy
	mu      sync.Mutex
	posts   int
	dropped int
}

func (d *dropFirstPost) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/tasks" {
		d.proxy.ServeHTTP(w, r)
		return
	}
	d.mu.Lock()
	d.posts++
	drop := d.dropped == 0
	if drop {
		d.dropped++
	}
	d.mu.Unlock()
	if !drop {
		d.proxy.ServeHTTP(w, r)
		return
	}
	body, _ := io.ReadAll(r.Body)
	req, _ := http.NewRequest(r.Method, d.backend.String()+r.URL.Path, bytes.NewReader(body))
	req.Header = r.Header.Clone()
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close() // 响应丢失
	}
}

// reverseProxy 转发到 backend，并把 Host 改为 backend（API 只接受允许列表中的 Host，§15.3）。
func reverseProxy(backend *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(backend) }, FlushInterval: -1}
}

func runCLI(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	env := cli.Env{Sleep: func(time.Duration) {}, Getenv: func(string) string { return "" }}
	code := cli.RunEnv(env, args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestE15LostCreateResponse：E15——`POST /tasks` 的响应丢失（代理丢弃），CLI 以同一 request_id 重试 → 只创建
// 一个任务；再以同一 request_id、不同内容提交 → 409 request_conflict，仍只有一个任务。
func TestE15LostCreateResponse(t *testing.T) {
	s := newSys(t)
	s.start("")
	backend, err := url.Parse(s.srv.base)
	if err != nil {
		t.Fatal(err)
	}
	d := &dropFirstPost{backend: backend, proxy: reverseProxy(backend)}
	px := httptest.NewServer(d)
	defer px.Close()
	execLog := filepath.Join(s.dir, "exec.log")
	spec1 := sysSpec(execLog, "e15", nil, progressStep("only"))
	code, out, errOut := runCLI("task", "submit", "--spec", spec1, "--request-id", "e15-req", "--addr", px.URL)
	if code != 0 {
		t.Fatalf("CLI submit 退出码 %d：%s", code, errOut)
	}
	var created struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil || created.TaskID == "" {
		t.Fatalf("CLI 输出 %q: %v", out, err)
	}
	d.mu.Lock()
	posts, dropped := d.posts, d.dropped
	d.mu.Unlock()
	if dropped != 1 || posts != 2 {
		t.Fatalf("代理看到 %d 次 POST、丢弃 %d 个响应；期望首个响应丢失后重试一次", posts, dropped)
	}
	var tasks, requests int
	var resource string
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM tasks", nil, &tasks)
	pgQueryRow(t, s.dsn, "SELECT count(*), max(resource_id) FROM api_requests WHERE request_id = 'e15-req'", nil, &requests, &resource)
	if tasks != 1 || requests != 1 || resource != created.TaskID {
		t.Fatalf("任务 %d 个、请求记录 %d 条（资源 %s）；期望只创建一个任务 %s", tasks, requests, resource, created.TaskID)
	}

	// 同一 request_id、不同内容 → 409 request_conflict（CLI 与直接请求）。
	spec2 := sysSpec(execLog, "e15-other", nil, progressStep("other"))
	code, _, errOut = runCLI("task", "submit", "--spec", spec2, "--request-id", "e15-req", "--addr", px.URL)
	if code != 1 || !strings.Contains(errOut, "409 request_conflict") {
		t.Fatalf("不同内容重试：退出码 %d，%s", code, errOut)
	}
	st, b, err := httpDo("POST", s.srv.base+"/tasks", `{"request_id":"e15-req","spec":`+spec2+`}`)
	if err != nil || st != http.StatusConflict || !strings.Contains(string(b), `"request_conflict"`) {
		t.Fatalf("不同内容重试 = %d %s %v", st, b, err)
	}
	pgQueryRow(t, s.dsn, "SELECT count(*) FROM tasks", nil, &tasks)
	if tasks != 1 {
		t.Fatalf("冲突请求创建了任务：%d 个", tasks)
	}
	if v := s.waitTerminal(created.TaskID); v.Status != "succeeded" {
		t.Fatalf("任务结束为 %+v", v)
	}
	s.finish()
}

// ---- E6、E16：SSE 断开与 CLI 重连 ----

// sseCutter 是 API 前的反向代理：事件流响应在随机字节数处被中断（前 forceCuts 个请求必中断；randomCuts
// 时其余以概率 3/4 中断；seed 记录在日志中）。记录每个事件流请求是否送达了 task_terminal。
type sseCutter struct {
	backend    *url.URL
	proxy      *httputil.ReverseProxy
	forceCuts  int
	randomCuts bool
	mu         sync.Mutex
	rng        *mrand.Rand
	requests   []sseRequest
}

type sseRequest struct {
	LastEventID string
	Cut         bool
	Terminal    bool
}

func newSSECutter(t *testing.T, base string, forceCuts int, randomCuts bool, seed uint64) (*sseCutter, *httptest.Server) {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	c := &sseCutter{backend: u, proxy: reverseProxy(u), forceCuts: forceCuts, randomCuts: randomCuts,
		rng: mrand.New(mrand.NewPCG(seed, 6))}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	return c, srv
}

func (c *sseCutter) snapshot() []sseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.requests)
}

func (c *sseCutter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/events") {
		c.proxy.ServeHTTP(w, r)
		return
	}
	c.mu.Lock()
	n := len(c.requests)
	limit := -1
	if n < c.forceCuts || (c.randomCuts && c.rng.IntN(4) != 0) {
		limit = 1 + c.rng.IntN(800)
	}
	c.requests = append(c.requests, sseRequest{LastEventID: r.Header.Get("Last-Event-ID")})
	c.mu.Unlock()
	record := func(f func(*sseRequest)) {
		c.mu.Lock()
		f(&c.requests[n])
		c.mu.Unlock()
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, c.backend.String()+r.URL.Path, nil)
	req.Header = r.Header.Clone()
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	flusher := w.(http.Flusher)
	var seen bytes.Buffer
	buf := make([]byte, 256)
	written := 0
	for {
		k, rerr := resp.Body.Read(buf)
		if k > 0 {
			chunk := buf[:k]
			if limit >= 0 && written+k >= limit {
				chunk = buf[:limit-written]
			}
			_, _ = w.Write(chunk)
			flusher.Flush()
			seen.Write(chunk)
			written += len(chunk)
			// task_terminal 的帧完整送达（帧以空行结束）。
			if i := bytes.Index(seen.Bytes(), []byte("event: "+api.EventTaskTerminal+"\n")); i >= 0 &&
				bytes.Contains(seen.Bytes()[i:], []byte("\n\n")) {
				record(func(q *sseRequest) { q.Terminal = true })
			}
			if limit >= 0 && written >= limit {
				record(func(q *sseRequest) { q.Cut = true })
				panic(http.ErrAbortHandler) // 中断连接：客户端读到不完整的流
			}
		}
		if rerr != nil {
			return
		}
	}
}

// watchCLI 以 CLI 的 `task watch` 跟随事件（经 base），返回退出码与输出的事件。
func watchCLI(t *testing.T, base, id string) (int, []api.Event, string) {
	t.Helper()
	type res struct {
		code     int
		out, err string
	}
	ch := make(chan res, 1)
	go func() {
		var out, errOut bytes.Buffer
		env := cli.Env{Sleep: func(time.Duration) { time.Sleep(10 * time.Millisecond) }, Getenv: func(string) string { return "" }}
		code := cli.RunEnv(env, []string{"task", "watch", id, "--addr", base}, &out, &errOut)
		ch <- res{code, out.String(), errOut.String()}
	}()
	var r res
	select {
	case r = <-ch:
	case <-time.After(waitLimit):
		t.Fatal("CLI watch 未在期限内结束")
	}
	var evs []api.Event
	for _, line := range strings.Split(strings.TrimSpace(r.out), "\n") {
		if line == "" {
			continue
		}
		var e struct {
			TaskSeq int64  `json:"task_seq"`
			Type    string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("CLI 输出行 %q: %v", line, err)
		}
		evs = append(evs, api.Event{TaskSeq: e.TaskSeq, Type: e.Type})
	}
	return r.code, evs, r.err
}

// assertSameEvents：CLI 收到的事件与数据库中直到 task_terminal 的事件完全一致——task_seq 从 1 连续、无缺失、
// 无重复，类型相同，最后一个是 task_terminal（服务端在它之后关闭流）。
func assertSameEvents(t *testing.T, got, want []api.Event) {
	t.Helper()
	for i, e := range want {
		if e.Type == api.EventTaskTerminal {
			want = want[:i+1]
			break
		}
	}
	if len(got) != len(want) {
		t.Fatalf("CLI 收到 %d 个事件，数据库中有 %d 个", len(got), len(want))
	}
	for i := range want {
		if got[i].TaskSeq != int64(i+1) || got[i].TaskSeq != want[i].TaskSeq || got[i].Type != want[i].Type {
			t.Fatalf("第 %d 个事件：CLI %+v，数据库 %+v", i, got[i], want[i])
		}
	}
	if len(got) == 0 || got[len(got)-1].Type != api.EventTaskTerminal {
		t.Fatal("最后一个事件不是 task_terminal")
	}
}

// TestE6SSERandomDisconnect：E6——事件流在随机位置被中断，CLI 以 Last-Event-ID 带退避重连 → 事件无缺失、
// 无重复；任务不重新执行（只有一个 attempt，执行计数中每步恰一次）。
func TestE6SSERandomDisconnect(t *testing.T) {
	s := newSys(t)
	s.start("")
	seed := uint64(time.Now().UnixNano())
	if v := os.Getenv("AGENTBOX_E2E_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	cutter, px := newSSECutter(t, s.srv.base, 2, true, seed)
	execLog := filepath.Join(s.dir, "exec.log")
	var steps []step
	for i := 0; i < 60; i++ {
		steps = append(steps, progressStep(fmt.Sprintf("p%d", i)), sleepStep(15))
	}
	id := s.submit("e6", sysSpec(execLog, "e6", nil, steps...))
	code, got, errOut := watchCLI(t, px.URL, id)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(id))
	reqs := cutter.snapshot()
	cuts := 0
	for _, r := range reqs {
		if r.Cut {
			cuts++
		}
	}
	if cuts < 2 {
		t.Fatalf("事件流只被中断 %d 次，用例无效", cuts)
	}
	in := s.inspect(id)
	if len(in.Attempts) != 1 || in.Task.Status != "succeeded" {
		t.Fatalf("任务被重新执行：%+v", in)
	}
	s.assertNoDuplicateExecution(in, readExecLog(t, execLog), nil, len(steps))
	t.Logf("E6：seed=%d（AGENTBOX_E2E_SEED 复现），%d 个事件，%d 次事件流请求，其中 %d 次被中断", seed, len(got), len(reqs), cuts)
	s.finish()
}

// TestE16WatchStopsAfterTerminal：E16——CLI 收到 task_terminal 后停止重连：已结束的任务只发出一个请求；
// 运行中的任务经中断的流跟随，送达 task_terminal 的请求是最后一个请求。非法游标与超出最新事件的游标
// → 400 invalid_cursor。
func TestE16WatchStopsAfterTerminal(t *testing.T) {
	s := newSys(t)
	s.start("")
	execLog := filepath.Join(s.dir, "exec.log")

	done := s.submit("e16-done", sysSpec(execLog, "e16", nil, progressStep("x")))
	s.waitTerminal(done)
	plain, px := newSSECutter(t, s.srv.base, 0, false, 0) // 只计数，不中断
	code, got, errOut := watchCLI(t, px.URL, done)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(done))
	if reqs := plain.snapshot(); len(reqs) != 1 || !reqs[0].Terminal {
		t.Fatalf("已结束任务的 watch 发出 %d 个请求 %+v；期望收到 task_terminal 后不再请求", len(reqs), reqs)
	}

	running := s.submit("e16-running", sysSpec(execLog, "e16", nil, progressStep("a"), sleepStep(300), progressStep("b")))
	cutter, px2 := newSSECutter(t, s.srv.base, 2, false, uint64(time.Now().UnixNano())) // 前两个请求中断
	code, got, errOut = watchCLI(t, px2.URL, running)
	if code != 0 {
		t.Fatalf("CLI watch 退出码 %d：%s", code, errOut)
	}
	assertSameEvents(t, got, s.events(running))
	reqs := cutter.snapshot()
	terminals := 0
	for _, r := range reqs {
		if r.Terminal {
			terminals++
		}
	}
	if terminals != 1 || !reqs[len(reqs)-1].Terminal {
		t.Fatalf("事件流请求 %+v：送达 task_terminal 的请求应是最后一个", reqs)
	}

	for _, cursor := range []string{"abc", "-1", "01", "1.5", "999999"} {
		req, _ := http.NewRequest(http.MethodGet, s.srv.base+"/tasks/"+done+"/events", nil)
		req.Header.Set("Last-Event-ID", cursor)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"invalid_cursor"`) {
			t.Fatalf("Last-Event-ID %q = %d %s，期望 400 invalid_cursor", cursor, resp.StatusCode, b)
		}
	}
	t.Logf("E16：已结束任务 1 个请求；运行中任务 %d 个请求（%d 次中断），task_terminal 之后没有请求", len(reqs), len(reqs)-1)
	s.finish()
}
