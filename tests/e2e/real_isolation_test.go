//go:build linux

// 真实隔离验收：provider/local + 生产启动器（root），以及 cmd/agentbox 真实子进程。装置与共用辅助函数见 e2e_test.go。

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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/datadir"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/protocol"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/local"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/runner"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// ==== 真实隔离验收（Plan 2 Task 13）：provider/local + 生产启动器 ====
//
// 以下用例需要 root 与 cgroup v2（非 root 时跳过；CI 的 linux-integration 作业以 root 运行，必须实际执行）。
// Worker 在真实沙箱中运行：user/pid/mount/net 等命名空间、映射 UID、只读 rootfs 模板（包含 /opt/agentbox）、
// seccomp、降权后的 stage-2 helper。worker 包由测试复制到 rootfs.WorkerDir（与 README 快速开始的安装一致）。
//
// 两种装置：
//   - 进程内（newRealHarness）：与上面的控制面验收共用 harness 与断言，只把 provider 换成 provider/local
//     （local.NewProcessStarter）。专用启动进程、沙箱 init 与 stage-2 helper 是本测试二进制（TestMain 分流到
//     与 cmd/agentbox 相同的 sandbox.RunLaunch/RunInit/RunHelper）。注入点（丢弃回复、暂停、杀死 Worker、
//     改写产物）由包装 StartExec 的 interceptProv 在 Worker 的 stdin/stdout 上实现；"杀死 Worker"从宿主向
//     环境 cgroup 中的 sim_worker 进程发 SIGKILL。
//   - 真实子进程（newRealSys）：构建并运行 cmd/agentbox（CGO_ENABLED 取调用方环境；CI 为 0），用于
//     server 被 SIGKILL 后重启（E5 物理回收）与累计运行时限跨重启；结束时运行 `agentbox verify-invariants
//     --quiescent`。
//
// 全部用例使用本次运行独占的 cgroup 根 /sys/fs/cgroup/agentbox-e2e-<随机>，TestMain 结束时删除。

// requireRealIsolation：非 root 时跳过；root 但不是 cgroup v2 时，CI 中失败、本地跳过。
func requireRealIsolation(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("需要 root：真实隔离验收（provider/local + 生产启动器）")
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil || st.Type != 0x63677270 { // CGROUP2_SUPER_MAGIC
		required(t, "cgroup v2（/sys/fs/cgroup）")
	}
}

var (
	workerOnce sync.Once
	workerErr  error
)

// installWorker 把仓库中的 worker 包（agentbox_worker、sim_worker、deepresearch）复制到 rootfs.WorkerDir
// （每次 go test 一次；不复制 __pycache__），返回该目录。等价于 README 快速开始中的
// `sudo install -d /opt/agentbox && sudo cp -r worker/agentbox_worker worker/sim_worker worker/deepresearch /opt/agentbox/`。
func installWorker(t *testing.T) string {
	t.Helper()
	src := workerPath(t)
	workerOnce.Do(func() { workerErr = copyWorker(src, rootfs.WorkerDir) })
	if workerErr != nil {
		t.Fatalf("安装 worker 包到 %s: %v", rootfs.WorkerDir, workerErr)
	}
	return rootfs.WorkerDir
}

func copyWorker(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, pkg := range []string{"agentbox_worker", "sim_worker", "deepresearch", "chatagent", "skills"} {
		err := filepath.WalkDir(filepath.Join(src, pkg), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			rel, err := filepath.Rel(src, p)
			if err != nil {
				return err
			}
			out := filepath.Join(dst, rel)
			if d.IsDir() {
				if err := os.MkdirAll(out, 0o755); err != nil {
					return err
				}
				return os.Chmod(out, 0o755)
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if err := os.WriteFile(out, b, 0o644); err != nil {
				return err
			}
			return os.Chmod(out, 0o644)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	realCgOnce sync.Once
	realCgRoot string // 本次运行独占的 cgroup 根；TestMain 结束时删除
	realCgErr  error
)

func realCgroupRoot(t *testing.T) string {
	t.Helper()
	realCgOnce.Do(func() {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		dir := "/sys/fs/cgroup/agentbox-e2e-" + hex.EncodeToString(b)
		if realCgErr = os.Mkdir(dir, 0o755); realCgErr == nil {
			realCgRoot = dir
		}
	})
	if realCgErr != nil {
		t.Fatalf("创建测试 cgroup 根: %v", realCgErr)
	}
	return realCgRoot
}

// killRemove 杀死 dir 下全部 cgroup（深者先）中的进程并删除它们，最后删除 dir 本身。尽力而为。
func killRemove(dir string) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if e.IsDir() {
			killRemove(filepath.Join(dir, e.Name()))
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "cgroup.kill"), []byte("1"), 0o644)
	for i := 0; i < 500; i++ {
		if err := os.Remove(dir); err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cgroupPids 返回 dir 及其子 cgroup 中的全部进程。
func cgroupPids(dir string) []int {
	var pids []int
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		b, _ := os.ReadFile(filepath.Join(p, "cgroup.procs"))
		for _, f := range strings.Fields(string(b)) {
			if n, err := strconv.Atoi(f); err == nil {
				pids = append(pids, n)
			}
		}
		return nil
	})
	return pids
}

var (
	starterOnce sync.Once
	realStarter *local.ProcessStarter
	starterErr  error
)

// processStarter 返回生产启动器（本测试进程随之成为 child subreaper）。
func processStarter(t *testing.T) *local.ProcessStarter {
	t.Helper()
	starterOnce.Do(func() { realStarter, starterErr = local.NewProcessStarter() })
	if starterErr != nil {
		t.Fatal(starterErr)
	}
	return realStarter
}

// realDataDir 返回 0711 的数据目录（沙箱 init 须能经过它到达 workspace；t.TempDir 的上级目录是 0700）。
func realDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "agentbox-e2e-data-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o711); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// newRealHarness 是进程内装置的真实隔离版本：provider/local + 生产启动器。
func newRealHarness(t *testing.T) *harness {
	t.Helper()
	requireRealIsolation(t)
	h := &harness{t: t, pyPath: installWorker(t), dsn: newDatabase(t), dir: realDataDir(t), real: true,
		cgRoot: realCgroupRoot(t), gate: &storeGate{failAfter: 500 * time.Millisecond, failed: map[string]int{}},
		tasks: map[string]*taskRec{}, controlRetries: map[string]int{}}
	starter := processStarter(t)
	h.newProvider = func(installID string) (provider.Provider, error) {
		p, err := local.New(local.Options{DataDir: h.dir, CgroupRoot: h.cgRoot, InstallID: installID, Starter: starter})
		if err != nil {
			return nil, err
		}
		h.local, h.installID = p, installID
		h.prov = &interceptProv{Provider: p, h: h}
		return h.prov, nil
	}
	h.cleanup()
	return h
}

// envCgroup 是环境的 cgroup 目录（provider/local：<root>/agentbox-<install_id>/env-<env_id>）。
func (h *harness) envCgroup(envID string) string {
	return filepath.Join(h.cgRoot, "agentbox-"+h.installID, "env-"+envID)
}

// killWorker 从宿主以 SIGKILL 杀死环境 cgroup 中的 Worker 进程（sim_worker 或 python3 -m deepresearch；
// 不经平台：分类为 crashed_signal）。
func (h *harness) killWorker(envID string) {
	for _, pid := range cgroupPids(h.envCgroup(envID)) {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil &&
			(bytes.Contains(b, []byte("sim_worker")) || bytes.Contains(b, []byte("-m\x00deepresearch"))) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// interceptProv 包装 provider/local：StartExec 返回的句柄在 Worker 的 stdin/stdout 上逐行转发并执行注入
// （与进程型 Program 的注入点相同）。其余操作原样委托。
type interceptProv struct {
	provider.Provider
	h *harness
}

func (p *interceptProv) StartExec(ctx context.Context, envID string, spec provider.ExecSpec) (provider.ExecHandle, error) {
	hd, err := p.Provider.StartExec(ctx, envID, spec)
	if err != nil {
		return nil, err
	}
	return p.h.intercept(envID, hd), nil
}

type interceptHandle struct {
	provider.ExecHandle
	stdin  *io.PipeWriter
	stdout *io.PipeReader
	exited func()
}

func (x *interceptHandle) Stdin() io.WriteCloser { return x.stdin }
func (x *interceptHandle) Stdout() io.ReadCloser { return x.stdout }

func (x *interceptHandle) Wait() (provider.ExitStatus, error) {
	st, err := x.ExecHandle.Wait()
	x.exited()
	return st, err
}

// intercept：runner 写给 Worker 的第一行是 init（记录并解析注入声明），之后两个方向逐行经 onHost/onWorker。
// Worker 退出（Wait 返回）时记录退出时刻并结束暂停。
func (h *harness) intercept(envID string, hd provider.ExecHandle) provider.ExecHandle {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	var (
		mu   sync.Mutex
		run  *workerRun
		once sync.Once
	)
	x := &interceptHandle{ExecHandle: hd, stdin: inW, stdout: outR}
	x.exited = func() {
		once.Do(func() {
			mu.Lock()
			r := run
			mu.Unlock()
			if r != nil {
				r.rec.mu.Lock()
				r.rec.exits[r.in.AttemptNo] = time.Now()
				r.rec.mu.Unlock()
			}
			cancel()
		})
	}
	go func() {
		defer outW.Close()
		br := bufio.NewReaderSize(inR, 1<<20)
		line, err := br.ReadBytes('\n')
		var in protocol.Init
		if jerr := json.Unmarshal(line, &in); err != nil || jerr != nil {
			// 不是 init：不注入，原样转发。
			_, _ = hd.Stdin().Write(line)
			go func() {
				_, _ = io.Copy(hd.Stdin(), br)
				_ = hd.Stdin().Close()
			}()
			_, _ = io.Copy(outW, hd.Stdout())
			return
		}
		r := h.newRun(in)
		r.killFn = func() { h.killWorker(envID) }
		mu.Lock()
		run = r
		mu.Unlock()
		_, _ = hd.Stdin().Write(line)
		go r.pumpHost(br, hd.Stdin())
		r.pumpWorker(ctx, hd.Stdout(), outW)
	}()
	return x
}

// postTask 经 HTTP 提交任务；limits 非空时随请求提交。
func postTask(base, requestID, spec, limits string) (string, error) {
	body := `{"request_id":"` + requestID + `","spec":` + spec
	if limits != "" {
		body += `,"limits":` + limits
	}
	st, b, err := httpDo("POST", base+"/tasks", body+"}")
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

// ---- 进程内装置上的真实隔离用例：与控制面验收相同的断言 ----

// TestRealFirstSlice：首个切片在真实沙箱中——提交 → checkpoint → 杀死 Worker → 恢复 → result。
func TestRealFirstSlice(t *testing.T) { testFirstSlice(t, newRealHarness(t)) }

// TestRealE1KillAfterSecondCheckpoint：E1 在真实沙箱中。
func TestRealE1KillAfterSecondCheckpoint(t *testing.T) { testE1(t, newRealHarness(t)) }

// TestRealE4CancelResultRace：E4 在真实沙箱中；随机交错降为 100 次（seed 记录在日志中）。
func TestRealE4CancelResultRace(t *testing.T) { testE4(t, newRealHarness(t), 100) }

// TestRealE7DropCheckpointResult：E7 在真实沙箱中。
func TestRealE7DropCheckpointResult(t *testing.T) { testE7(t, newRealHarness(t)) }

// TestRealE8StoreBlocked：E8 在真实沙箱中（执行树的物理停止经 cgroup）。
func TestRealE8StoreBlocked(t *testing.T) { testE8(t, newRealHarness(t)) }

// TestRealE10ArtifactTampering：E10 在真实沙箱中（产物文件属于映射 UID）。
func TestRealE10ArtifactTampering(t *testing.T) { testE10(t, newRealHarness(t)) }

// e2Limits 是 E2、E3 的内存限额：128 MiB（memory.swap.max 固定为 0）。
const e2Limits = `{"memory_max":134217728}`

// TestRealE2WorkerOOM：E2——sim_worker 主进程超出 memory.max（主进程是预期受害者）→ worker_oom_likely →
// 重试一次 → failed；两个 attempt 都被 SIGKILL 且 OOMKillDelta > 0；oom_retries_used = 1，不计故障重试；
// 无残留 cgroup。
func TestRealE2WorkerOOM(t *testing.T) {
	h := newRealHarness(t)
	h.start(baseConfig())
	id, err := postTask(h.base, "e2", spec(nil, "never", nil, progressStep("allocating"), step{"op": "allocate_mb", "mb": 512}), e2Limits)
	if err != nil {
		t.Fatal(err)
	}
	v := h.waitTerminal(id)
	in := h.inspect(id)
	ts := h.loadTask(id)
	if v.Status != "failed" || v.StatusReason != runner.ClassWorkerOOMLikely || len(in.Attempts) != 2 ||
		ts.OOMRetriesUsed != 1 || ts.FaultRetriesUsed != 0 {
		t.Fatalf("任务 %s/%s，attempts %+v，oom_retries_used=%d fault_retries_used=%d；期望 failed/worker_oom_likely、2 个 attempt、OOM 重试 1 次",
			v.Status, v.StatusReason, in.Attempts, ts.OOMRetriesUsed, ts.FaultRetriesUsed)
	}
	for _, a := range in.Attempts {
		if a.OutcomeClass != runner.ClassWorkerOOMLikely || a.ExitSignal == nil || *a.ExitSignal != int64(syscall.SIGKILL) ||
			a.OOMKillDelta <= 0 || a.PlatformKilled {
			t.Fatalf("attempt %d：%+v，期望 worker_oom_likely（SIGKILL、OOMKillDelta > 0、非平台终止）", a.AttemptNo, a)
		}
	}
	h.assertNoLeak()
	for _, a := range in.Attempts {
		if _, err := os.Stat(h.envCgroup(a.EnvID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("attempt %d 的环境 cgroup 仍存在（%v）", a.AttemptNo, err)
		}
	}
	t.Logf("E2：%d 个 attempt（%s），oom_kill_delta = %d / %d", len(in.Attempts), attemptClasses(in),
		in.Attempts[0].OOMKillDelta, in.Attempts[1].OOMKillDelta)
	h.finish()
}

// TestRealE3SideProcessOOM：E3——同一环境 cgroup 中的另一个进程超出 memory.max 被 OOM 杀死，Worker 主进程
// 正常结束 → 按 Worker 结果裁决（succeeded），记 oom_observed_in_attempt，不归因为 Worker OOM、不重试。
// sim_worker 没有派生子进程的步骤，OOM 进程由测试在 Worker 暂停期间经 provider 在同一环境中启动（与 Worker
// 的子进程同处环境 cgroup，平台的归因只依据该 cgroup 的 OOM 计数与 Worker 主进程的退出状态）。
func TestRealE3SideProcessOOM(t *testing.T) {
	h := newRealHarness(t)
	h.start(baseConfig())
	// 暂停在 checkpoint 上：Worker 等待 checkpoint_result，因而在 OOM 进程运行期间不会结束。
	id, err := postTask(h.base, "e3", spec(&directives{Hold: &holdSpec{Type: "checkpoint", Nth: 1}}, "e3 done", nil,
		progressStep("before hog"), checkpointStep("c1"), progressStep("after hog")), e2Limits)
	if err != nil {
		t.Fatal(err)
	}
	h.waitHeld(id)
	envID := attemptByNo(h.inspect(id), 1).EnvID
	hog, err := h.local.StartExec(context.Background(), envID, provider.ExecSpec{ExecID: "e3-hog", Dir: "/tmp",
		Argv: []string{"python3", "-c", "b = bytearray(512 << 20)\nfor i in range(0, len(b), 4096): b[i] = 1\n"}})
	if err != nil {
		t.Fatalf("在环境中启动 OOM 进程: %v", err)
	}
	_ = hog.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, hog.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, hog.Stderr()) }()
	st, err := hog.Wait()
	if err != nil || st.Signal != syscall.SIGKILL {
		t.Fatalf("OOM 进程退出 %+v %v，期望被 SIGKILL", st, err)
	}
	diag, err := h.local.ResourceDiag(context.Background(), envID)
	if err != nil || diag.OOMKillDelta == 0 {
		t.Fatalf("OOM 之后的诊断 %+v %v", diag, err)
	}
	h.rec(id).releaseHold()
	v := h.waitTerminal(id)
	in := h.inspect(id)
	ts := h.loadTask(id)
	if v.Status != "succeeded" || len(in.Attempts) != 1 || ts.OOMRetriesUsed != 0 || ts.FaultRetriesUsed != 0 {
		t.Fatalf("任务 %s/%s，attempts %+v；期望按 Worker 结果 succeeded、不重试", v.Status, v.StatusReason, in.Attempts)
	}
	a := in.Attempts[0]
	if a.OutcomeClass != runner.ClassOOMObserved || a.OOMKillDelta <= 0 || a.ExitCode == nil || *a.ExitCode != 0 {
		t.Fatalf("attempt %+v，期望 oom_observed_in_attempt（退出码 0、OOMKillDelta > 0）", a)
	}
	if summary, outs, _ := h.pinnedResult(id, a.AttemptID); summary != "e3 done" || len(outs) != 0 {
		t.Fatalf("result %q %+v", summary, outs)
	}
	t.Logf("E3：OOM 进程被杀死（oom_kill_delta = %d），attempt 分类 %s，任务 %s", a.OOMKillDelta, a.OutcomeClass, v.Status)
	h.finish()
}

// ---- 真实子进程：cmd/agentbox ----

var (
	agentboxOnce   sync.Once
	agentboxBinDir string
	agentboxBin    string
	agentboxErr    error
)

// agentboxBinary 构建 cmd/agentbox（每次 go test 一次）。
func agentboxBinary(t *testing.T) string {
	t.Helper()
	agentboxOnce.Do(func() {
		goBin := goBinary()
		if agentboxBinDir, agentboxErr = os.MkdirTemp("", "agentbox-bin-"); agentboxErr != nil {
			return
		}
		agentboxBin = filepath.Join(agentboxBinDir, "agentbox")
		if out, err := exec.Command(goBin, "build", "-o", agentboxBin, "../../cmd/agentbox").CombinedOutput(); err != nil {
			agentboxErr = fmt.Errorf("构建 cmd/agentbox: %v\n%s", err, out)
		}
	})
	if agentboxErr != nil {
		t.Fatal(agentboxErr)
	}
	return agentboxBin
}

// newRealSys 返回运行 cmd/agentbox 的系统级装置（复用 sysHarness 的读取与停止；启动与结束检查见
// startReal、finishReal）。
func newRealSys(t *testing.T) *sysHarness {
	t.Helper()
	requireRealIsolation(t)
	installWorker(t)
	realCgroupRoot(t)
	s := &sysHarness{t: t, serverDSN: newDatabase(t), dir: realDataDir(t), bin: agentboxBinary(t)}
	s.dsn = withAppName(t, s.serverDSN)
	s.storeDSN = s.serverDSN
	t.Cleanup(func() {
		for _, p := range s.procs {
			if !p.exited() {
				_ = p.cmd.Process.Kill()
				<-p.done
			}
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

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// startReal 以 `agentbox server` 启动 server 子进程（生产启动器、默认 rootfs 模板、本次运行的 cgroup 根），
// 等待 API 进入 normal 模式。
func (s *sysHarness) startReal(extra ...string) *serverProc {
	s.t.Helper()
	base := "http://127.0.0.1:" + freePort(s.t)
	cmd := exec.Command(s.bin, append([]string{"server", "--data-dir", s.dir, "--database-url", s.serverDSN,
		"--listen", strings.TrimPrefix(base, "http://"), "--cgroup-root", realCgRoot}, extra...)...)
	p := &serverProc{cmd: cmd, logs: &lockedBuffer{}, done: make(chan struct{}), base: base}
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	// server 被 SIGKILL 时，冻结的会话环境（E33）中的进程无法退出，仍持有继承的输出管道：Wait 在进程退出后至多
	// 再等 WaitDelay 读取输出，然后关闭管道返回。
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.procs = append(s.procs, p)
	s.srv = p
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	eventually(s.t, "server 就绪（API normal 模式）", func() bool {
		if p.exited() {
			s.t.Fatalf("server 提前退出（%v）：\n%s", cmd.ProcessState, p.logs.tail(4<<10))
		}
		st, b, err := httpDo("GET", base+"/status", "")
		return err == nil && st == http.StatusOK && bytes.Contains(b, []byte(`"normal"`))
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

// installCgroup 是该数据目录的安装 cgroup。
func (s *sysHarness) installCgroup() string {
	s.t.Helper()
	id, ok, err := datadir.NewIDFile(s.dir).Read()
	if err != nil || !ok {
		s.t.Fatalf("读取 install_id: %v %v", ok, err)
	}
	return filepath.Join(realCgRoot, "agentbox-"+id)
}

// waitConnsGone 等待被杀死的 server 的数据库连接全部结束（它已发出的 COMMIT 仍可能完成）。
func (s *sysHarness) waitConnsGone() {
	s.t.Helper()
	eventuallyWithin(s.t, "上一 server 的数据库连接全部结束", waitLimit, 20*time.Millisecond, func() bool {
		var n int
		pgQueryRow(s.t, s.dsn, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND pid <> pg_backend_pid() AND application_name <> $1`, []any{testAppName}, &n)
		return n == 0
	})
}

// finishReal：环境全部停止并清理、UID 范围归还、没有环境目录与环境 cgroup、没有隔离记录；正常停止 server
// 后 `agentbox verify-invariants --quiescent` 通过。
func (s *sysHarness) finishReal() {
	s.t.Helper()
	installCg := s.installCgroup()
	eventuallyWithin(s.t, "环境全部停止并清理、UID 范围归还、没有环境目录与环境 cgroup", waitLimit, 50*time.Millisecond, func() bool {
		var pending, held int
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM environments WHERE cleanup_state <> 'done' OR stopped_at IS NULL", nil, &pending)
		pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM uid_ranges WHERE state <> 'free'", nil, &held)
		envDirs, _ := os.ReadDir(filepath.Join(s.dir, "envs"))
		cgs, _ := filepath.Glob(filepath.Join(installCg, "env-*"))
		return pending == 0 && held == 0 && len(envDirs) == 0 && len(cgs) == 0 && len(cgroupPids(installCg)) == 0
	})
	var quarantined int
	pgQueryRow(s.t, s.dsn, "SELECT count(*) FROM quarantined_resources", nil, &quarantined)
	if quarantined != 0 {
		s.t.Fatalf("误隔离：%d 条隔离记录", quarantined)
	}
	s.stop()
	out, err := exec.Command(s.bin, "verify-invariants", "--quiescent", "--data-dir", s.dir, "--database-url", s.dsn,
		"--cgroup-root", realCgRoot).CombinedOutput()
	if err != nil {
		s.t.Fatalf("agentbox verify-invariants --quiescent：%v\n%s", err, out)
	}
	s.t.Logf("agentbox verify-invariants --quiescent：%s", strings.TrimSpace(string(out)))
}

// TestRealE5ServerKilledPhysicalRecovery：E5 的物理回收部分——cmd/agentbox server 在 Worker 运行中被
// SIGKILL（checkpoint s1 已提交）→ 重启：残留执行树经 cgroup 停止、孤儿环境（目录、cgroup）回收、无误隔离；
// attempt 1 为 lost_on_restart，attempt 2 从 s1 恢复且两个 attempt 的执行不交错（执行计数）；产物完整。
// 实测：server 退出后 init 读到控制连接 EOF 而退出，PID namespace 随之销毁其中全部进程，因此重启时通常
// 只剩环境目录与 cgroup（存活进程数记录在日志中；若有存活进程，断言它们在恢复后不再存活）。
func TestRealE5ServerKilledPhysicalRecovery(t *testing.T) {
	s := newRealSys(t)
	first := s.startReal()
	steps := []step{
		progressStep("begin"),
		artifactStep("a1", "a1.txt", "one"),
		checkpointStep("s1"),
		sleepStep(5000),
		artifactStep("a2", "a2.txt", "two"),
		progressStep("end"),
	}
	const s1Index = 2
	id := s.submit("e5-real", sysSpec("/workspace/exec.log", "e5", []string{"a1", "a2"}, steps...))
	eventually(t, "checkpoint s1 已提交", func() bool {
		in, err := s.store.Inspect(context.Background(), id)
		return err == nil && len(in.Checkpoints) == 1
	})
	env1 := attemptByNo(s.inspect(id), 1).EnvID
	envCg := filepath.Join(s.installCgroup(), "env-"+env1)
	if n := len(cgroupPids(envCg)); n == 0 {
		t.Fatalf("server 被杀死前环境 cgroup %s 中没有进程", envCg)
	}
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if ps := s.waitExit(first); !killedBySIGKILL(ps) {
		t.Fatalf("server 退出状态 %v", ps)
	}
	s.waitConnsGone()
	survivors := cgroupPids(envCg)
	_, dirErr := os.Stat(filepath.Join(s.dir, "envs", env1))
	t.Logf("server 被 SIGKILL 后：环境 cgroup 中 %d 个存活进程，环境目录存在=%v", len(survivors), dirErr == nil)
	if _, cgErr := os.Stat(envCg); dirErr != nil || cgErr != nil {
		t.Fatalf("server 被杀死后环境 %s 的目录与 cgroup 应作为孤儿残留：%v %v", env1, dirErr, cgErr)
	}

	s.startReal()
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	a1 := attemptByNo(in, 1)
	if v.Status != "succeeded" || len(in.Attempts) != 2 || a1.OutcomeClass != runner.ClassLostOnRestart ||
		ts.FaultRetriesUsed != 1 || countType(s.events(id), "task_terminal") != 1 {
		t.Fatalf("恢复后任务 %+v，attempts %+v，fault_retries_used=%d", v, in.Attempts, ts.FaultRetriesUsed)
	}
	if _, err := os.Stat(envCg); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("残留环境 cgroup %s 未被回收（%v）", envCg, err)
	}
	for _, pid := range survivors {
		if err := syscall.Kill(pid, 0); err == nil {
			if b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); !bytes.Contains(b, []byte(") Z ")) {
				t.Fatalf("残留执行进程 %d 在恢复后仍存活", pid)
			}
		}
	}
	// 执行计数：attempt 1 执行到 sleep（第 3 步）为止；attempt 2 从 s1 的下一步开始、不重做之前的步骤；
	// attempt 2 开始执行之后 attempt 1 没有再执行任何步骤（残留执行树已停止）。
	recs := readExecLog(t, filepath.Join(s.dir, "workspaces", id, "exec.log"))
	next := map[int64]int{1: 0, 2: s1Index + 1}
	var started2 bool
	for i, r := range recs {
		if r.AttemptNo == 2 {
			started2 = true
		} else if started2 {
			t.Fatalf("执行计数第 %d 行：attempt 2 开始执行后 attempt %d 仍在执行：%+v", i, r.AttemptNo, recs)
		}
		if r.Index != next[r.AttemptNo] {
			t.Fatalf("attempt %d 执行了第 %d 步，期望第 %d 步：%+v", r.AttemptNo, r.Index, next[r.AttemptNo], recs)
		}
		next[r.AttemptNo]++
	}
	if next[1] != s1Index+2 || next[2] != len(steps)+1 {
		t.Fatalf("执行计数不完整：%+v", recs)
	}
	last := in.Attempts[len(in.Attempts)-1]
	if got := s.readPinnedOutputs(id, last.AttemptID); got["a1"] != "one" || got["a2"] != "two" || len(got) != 2 {
		t.Fatalf("固定输出 %q", got)
	}
	t.Logf("E5（物理回收）：重启前 %d 个存活进程 + 环境目录；恢复后 %d 个 attempt（%s）", len(survivors), len(in.Attempts), attemptClasses(in))
	s.finishReal()
}

// TestRealRunTimeLimitAcrossRestart：累计运行时限（规格 §14.4）——时限 6 s 的任务运行约 3 s 后 server 被
// SIGKILL 并重启：未记账区间按墙钟计入、额度不重置，到期以 task_deadline_exceeded 终止；重启后的 attempt
// 运行时间明显短于完整时限。
func TestRealRunTimeLimitAcrossRestart(t *testing.T) {
	const limit = 6 * time.Second
	s := newRealSys(t)
	first := s.startReal()
	id, err := postTask(first.base, "deadline", spec(nil, "never", nil, checkpointStep("c1"), sleepStep(600000)),
		fmt.Sprintf(`{"max_run_time_ms":%d}`, limit.Milliseconds()))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "checkpoint c1 已提交", func() bool {
		in, err := s.store.Inspect(context.Background(), id)
		return err == nil && len(in.Checkpoints) == 1
	})
	time.Sleep(limit / 2)
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	s.waitExit(first)
	s.waitConnsGone()
	pre := s.loadTask(id)

	s.startReal()
	var start2 time.Time
	eventually(t, "重启后任务到达终态或创建 attempt 2", func() bool {
		v, err := s.store.GetTask(context.Background(), id)
		if err == nil && task.IsTerminal(v.Status) {
			return true
		}
		in, err := s.store.Inspect(context.Background(), id)
		if err == nil && len(in.Attempts) >= 2 && start2.IsZero() {
			start2 = time.Now()
		}
		return false
	})
	v := s.waitTerminal(id)
	in := s.inspect(id)
	ts := s.loadTask(id)
	if v.Status != "failed" || v.StatusReason != runner.ClassDeadlineExceeded || attemptByNo(in, 1).OutcomeClass != runner.ClassLostOnRestart {
		t.Fatalf("任务 %s/%s，attempts %+v；期望 task_deadline_exceeded 且 attempt 1 为 lost_on_restart", v.Status, v.StatusReason, in.Attempts)
	}
	// 重启前运行了至少 limit/2（持久化周期 10 s，尚未记账）：恢复按墙钟差全额计入。
	if ts.RunTimeMs < (limit / 2).Milliseconds() {
		t.Fatalf("恢复后 run_time_ms = %d，未计入重启前约 %s 的运行时间（重启前持久化 %d）", ts.RunTimeMs, limit/2, pre.RunTimeMs)
	}
	var ran2 time.Duration
	if !start2.IsZero() {
		ran2 = time.Since(start2)
		if ran2 > limit-limit/4 {
			t.Fatalf("重启后的 attempt 运行了 %s 才到期：额度被重置（时限 %s）", ran2.Round(time.Millisecond), limit)
		}
	}
	t.Logf("时限 %s：重启前持久化 run_time_ms=%d；重启后 %d 个 attempt（%s），attempt 2 存续约 %s；最终 run_time_ms=%d",
		limit, pre.RunTimeMs, len(in.Attempts), attemptClasses(in), ran2.Round(time.Millisecond), ts.RunTimeMs)
	s.finishReal()
}
