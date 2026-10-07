//go:build linux

package local

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/cgroup"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/providertest"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/rootfs"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/sandbox"
)

// ---------------------------------------------------------------------------
// Task 7：宿主侧 StartExec/ExecHandle 与执行闸门。宿主端 execClient 对接测试内的假 init：
// socketpair 另一端的 sandbox.Conn，由每个用例按脚本收发；不需要 root。

// newExecPair 返回宿主端 execClient 与假 init 端 Conn；测试结束时两端都关闭。
func newExecPair(t *testing.T) (*execClient, *sandbox.Conn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	host := sandbox.NewConn(os.NewFile(uintptr(fds[0]), "host"), 8)
	fake := sandbox.NewConn(os.NewFile(uintptr(fds[1]), "init"), 8)
	c := newExecClient(host)
	t.Cleanup(func() {
		c.close()
		fake.Close()
	})
	return c, fake
}

// recvStart 在假 init 端读一条 start，返回它与随帧收到的 3 个 FD（stdin、stdout、stderr 的子端）。
func recvStart(t *testing.T, fake *sandbox.Conn) (sandbox.Message, []*os.File) {
	t.Helper()
	m, files, err := fake.Recv()
	if err != nil {
		t.Errorf("假 init 读 start: %v", err)
		return m, nil
	}
	if m.Type != sandbox.MsgStart || len(files) != 3 {
		t.Errorf("假 init 收到 %s 与 %d 个 FD，期望 start 与 3 个", m.Type, len(files))
	}
	return m, files
}

func send(t *testing.T, fake *sandbox.Conn, m sandbox.Message) {
	t.Helper()
	if err := fake.Send(m, nil); err != nil {
		t.Errorf("假 init 发送 %s: %v", m.Type, err)
	}
}

func closeFiles(fs []*os.File) {
	for _, f := range fs {
		f.Close()
	}
}

type startResult struct {
	h   provider.ExecHandle
	err error
}

func goStart(ctx context.Context, c *execClient, g *gate, spec provider.ExecSpec) <-chan startResult {
	ch := make(chan startResult, 1)
	go func() {
		h, err := c.start(ctx, g, spec)
		ch <- startResult{h, err}
	}()
	return ch
}

var testSpec = provider.ExecSpec{ExecID: "e1", Argv: []string{"echo", "hi"}, Env: []string{"A=1"}, Dir: "/w"}

func TestExecSuccess(t *testing.T) {
	c, fake := newExecPair(t)
	initDone := make(chan string, 1)
	go func() {
		m, files := recvStart(t, fake)
		if len(files) != 3 {
			initDone <- ""
			return
		}
		if string(m.Spec) != `{"argv":["echo","hi"],"env":["A=1"],"dir":"/w"}` || m.ExecID != "e1" {
			t.Errorf("start = %+v", m)
		}
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
		in, _ := io.ReadAll(files[0])
		if _, err := files[1].WriteString("out"); err != nil {
			t.Errorf("写 stdout: %v", err)
		}
		if _, err := files[2].WriteString("err"); err != nil {
			t.Errorf("写 stderr: %v", err)
		}
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgExit, ExecID: "e1", Exit: &sandbox.ExitInfo{Code: 3}})
		initDone <- string(in)
	}()

	h, err := c.start(context.Background(), &gate{}, testSpec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.Stdin().Write([]byte("in")); err != nil {
		t.Fatalf("写 stdin: %v", err)
	}
	h.Stdin().Close()
	// 宿主在发送后关闭了子端副本，因此假 init 关闭它的副本后 stdout/stderr 读到 EOF。
	out, err := io.ReadAll(h.Stdout())
	if err != nil || string(out) != "out" {
		t.Errorf("stdout = %q, %v", out, err)
	}
	errOut, err := io.ReadAll(h.Stderr())
	if err != nil || string(errOut) != "err" {
		t.Errorf("stderr = %q, %v", errOut, err)
	}
	st, err := h.Wait()
	if err != nil || st != (provider.ExitStatus{Code: 3}) {
		t.Errorf("Wait = %+v, %v；期望 code 3", st, err)
	}
	if in := <-initDone; in != "in" {
		t.Errorf("init 读到 stdin %q", in)
	}
}

func TestExecStartErr(t *testing.T) {
	c, fake := newExecPair(t)
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartErr, ExecID: "e1", Reason: "launch"})
	}()
	_, err := c.start(context.Background(), &gate{}, testSpec)
	var se *provider.StartError
	if !errors.As(err, &se) || se.Reason != "launch" || !errors.Is(err, provider.ErrStartFailed) {
		t.Fatalf("start = %v，期望 *StartError{launch}", err)
	}
}

func TestExecControlLostBeforeAck(t *testing.T) {
	c, fake := newExecPair(t)
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		fake.Close()
	}()
	if _, err := c.start(context.Background(), &gate{}, testSpec); !errors.Is(err, provider.ErrControlLost) {
		t.Fatalf("start = %v，期望 ErrControlLost", err)
	}
}

func TestExecWaitControlLostAfterAck(t *testing.T) {
	c, fake := newExecPair(t)
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
	}()
	h, err := c.start(context.Background(), &gate{}, testSpec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	fake.Close()
	if _, err := h.Wait(); !errors.Is(err, provider.ErrControlLost) {
		t.Fatalf("退出前控制连接断开，Wait 应返回 ErrControlLost（退出状态未知），得到 %v", err)
	}
}

func TestExecCtxCancelBeforeAck(t *testing.T) {
	c, fake := newExecPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan struct{})
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		close(received)
	}()
	res := goStart(ctx, c, &gate{}, testSpec)
	<-received // start 已发出，ACK 未回
	cancel()
	r := <-res
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("start = %v，期望 context.Canceled", r.err)
	}
	// 迟到的 ACK 与 exit 被丢弃，连接仍可用于下一次执行。
	send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
	send(t, fake, sandbox.Message{Type: sandbox.MsgExit, ExecID: "e1", Exit: &sandbox.ExitInfo{}})
	go func() {
		m, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: m.ExecID, PID: 43})
	}()
	if _, err := c.start(context.Background(), &gate{}, provider.ExecSpec{ExecID: "e2", Argv: []string{"true"}}); err != nil {
		t.Fatalf("下一次 start: %v", err)
	}
}

func TestExecExitBeforeAck(t *testing.T) {
	c, fake := newExecPair(t)
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgExit, ExecID: "e1", Exit: &sandbox.ExitInfo{Code: 7}})
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
	}()
	h, err := c.start(context.Background(), &gate{}, testSpec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if st, err := h.Wait(); err != nil || st.Code != 7 {
		t.Fatalf("Wait = %+v, %v；期望 code 7", st, err)
	}
}

func TestExecTerminate(t *testing.T) {
	c, fake := newExecPair(t)
	got := make(chan sandbox.Message, 1)
	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
		m, _, err := fake.Recv()
		if err != nil {
			t.Errorf("假 init 读 terminate: %v", err)
		}
		got <- m
		send(t, fake, sandbox.Message{Type: sandbox.MsgExit, ExecID: "e1", Exit: &sandbox.ExitInfo{Signal: syscall.SIGKILL}})
	}()
	h, err := c.start(context.Background(), &gate{}, testSpec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := h.Terminate(1500 * time.Millisecond); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if m := <-got; m.Type != sandbox.MsgTerminate || m.ExecID != "e1" || m.GraceMS != 1500 {
		t.Fatalf("假 init 收到 %+v，期望 terminate{e1, grace_ms 1500}", m)
	}
	if st, err := h.Wait(); err != nil || st.Signal != syscall.SIGKILL {
		t.Fatalf("Wait = %+v, %v；期望 SIGKILL", st, err)
	}
}

func TestGateClosedSendsNothing(t *testing.T) {
	c, fake := newExecPair(t)
	g := &gate{}
	g.close()
	if _, err := c.start(context.Background(), g, testSpec); !errors.Is(err, provider.ErrStopping) {
		t.Fatalf("start = %v，期望 ErrStopping", err)
	}
	// 关闭宿主端后，假 init 读到的第一件事是 EOF：start 没有发出。
	c.close()
	if m, files, err := fake.Recv(); !errors.Is(err, io.EOF) {
		closeFiles(files)
		t.Fatalf("假 init 收到 %+v, %v，期望 EOF", m, err)
	}
}

func TestGateCloseDoesNotWaitInflight(t *testing.T) {
	c, fake := newExecPair(t)
	g := &gate{}
	entered, release := make(chan struct{}), make(chan struct{})
	c.beforeSend = func(string) {
		close(entered)
		<-release
	}
	res := goStart(context.Background(), c, g, testSpec)
	<-entered // 已登记在途启动，start 尚未发送

	g.close() // 不等待在途启动；若等待，本用例在此死锁
	if err := g.enter(); !errors.Is(err, provider.ErrStopping) {
		t.Fatalf("close 之后 enter = %v，期望 ErrStopping", err)
	}

	go func() {
		_, files := recvStart(t, fake)
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgStartAck, ExecID: "e1", PID: 42})
	}()
	close(release)
	if r := <-res; r.err != nil {
		t.Fatalf("在途启动: %v", r.err)
	}
	g.mu.Lock()
	n := g.inflight
	g.mu.Unlock()
	if n != 0 {
		t.Fatalf("inflight = %d，期望 0", n)
	}
}

// ---------------------------------------------------------------------------
// Task 8：环境生命周期（契约第 3–6 节），真实 cgroup v2，需要 root；非 root 跳过。
//
// 测试用环境启动器 testStarter 以测试辅助模式 re-exec 本测试二进制作为 init：clone 时经
// CLONE_INTO_CGROUP 直接进入环境 cgroup（运行任何代码之前），运行 sandbox.Serve 与直接 exec 的
// testLauncher。不建 namespace、不降权——只在测试中构造，生产装配无法选用。

const (
	envTestInit = "AGENTBOX_TEST_LOCAL_INIT" // 本进程是 testStarter 启动的 init
	envTestHog  = "AGENTBOX_TEST_LOCAL_HOG"  // 本进程是持续分配内存直到被 OOM 杀死的 workload
)

func TestMain(m *testing.M) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == sandbox.LaunchArg:
		// 生产启动器以 /proc/self/exe sandbox-launch 启动的专用启动进程（即本测试二进制）。
		if err := sandbox.RunLaunch(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	case len(os.Args) > 1 && os.Args[1] == sandbox.InitArg:
		// 专用启动进程在命名空间中以 /proc/self/exe init 启动的 init（见 Task 9 一节）。
		// 设置 envRealInit（经 ProcessStarter.initEnv 注入）时运行生产 init，走完整的 init + stage-2 helper 链。
		if os.Getenv(envRealInit) != "" {
			if err := sandbox.RunInit(); err != nil {
				fmt.Fprintln(os.Stderr, "init:", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
		os.Exit(runProdTestInit())
	case len(os.Args) > 1 && os.Args[1] == sandbox.HelperArg:
		sandbox.RunHelper() // 生产 init 经 execveat 启动的 stage-2 helper；不返回
		os.Exit(1)
	case os.Getenv(envTestInit) != "":
		os.Exit(runTestInit())
	case os.Getenv(envTestHog) != "":
		os.Exit(runHog())
	}
	code := m.Run()
	removeTestCgroupRoot()
	os.Exit(code)
}

// envRealInit 让本测试二进制作为 init 时运行生产 sandbox.RunInit（而非测试 init）。
const envRealInit = "AGENTBOX_TEST_REAL_INIT"

// runTestInit 是测试 init 的主函数：fd 3 是控制连接，fd 4 是就绪管道（写一个字节后关闭）。
// 设为 child subreaper，使双重 fork 脱离的后代仍由它收割（与沙箱内 init 的处境相同）。
func runTestInit() int {
	const prSetChildSubreaper = 36
	if _, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); e != 0 {
		fmt.Fprintf(os.Stderr, "test init: PR_SET_CHILD_SUBREAPER: %v\n", e)
		return 1
	}
	conn := sandbox.NewConn(os.NewFile(3, "control"), 64)
	reg := sandbox.NewRegistry(func(m sandbox.Message) error { return conn.Send(m, nil) })
	ready := os.NewFile(4, "ready")
	if _, err := ready.Write([]byte{1}); err != nil {
		fmt.Fprintf(os.Stderr, "test init: 就绪通知: %v\n", err)
		return 1
	}
	ready.Close()
	if err := sandbox.Serve(context.Background(), conn, reg, testLauncher{}); err != nil {
		fmt.Fprintf(os.Stderr, "test init: Serve: %v\n", err)
		return 1
	}
	return 0
}

// runHog 持续分配并触碰内存，直到被环境 cgroup 的 memory.max 触发 OOM 杀死。
func runHog() int {
	var keep [][]byte
	for i := 0; i < 4096; i++ {
		b := make([]byte, 1<<20)
		for j := 0; j < len(b); j += 4096 {
			b[j] = 1
		}
		keep = append(keep, b)
	}
	fmt.Println(len(keep))
	return 0
}

// testLauncher 按 StartSpec 直接 exec，让 workload 成为自己进程组的组长；不建 namespace、不降权。
// 只 Start 然后 Release，由 Registry 收割。
type testLauncher struct{}

func (testLauncher) Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (int, error) {
	var s sandbox.StartSpec
	if err := json.Unmarshal(spec, &s); err != nil {
		return 0, err
	}
	cmd := exec.Command(s.Argv[0], s.Argv[1:]...)
	cmd.Env, cmd.Dir = s.Env, s.Dir
	if cmd.Env == nil { // 不继承 init 的环境（其中有 envTestInit）
		cmd.Env = []string{"PATH=/usr/bin:/bin"}
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}

// testStarter 是测试用 EnvStarter（见本节开头）。
type testStarter struct{}

func (testStarter) StartInit(ctx context.Context, _ provider.EnvSpec, dir string, cg *cgroup.Group) (*sandbox.Conn, int, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, 0, err
	}
	host, child := os.NewFile(uintptr(fds[0]), "host"), os.NewFile(uintptr(fds[1]), "init")
	defer child.Close()
	rr, rw, err := os.Pipe()
	if err != nil {
		host.Close()
		return nil, 0, err
	}
	defer rr.Close()
	defer rw.Close()
	cgdir, err := os.Open(cg.Path())
	if err != nil {
		host.Close()
		return nil, 0, err
	}
	defer cgdir.Close()
	exe, err := os.Executable()
	if err != nil {
		host.Close()
		return nil, 0, err
	}

	cmd := exec.Command(exe, "-test.run=^$")
	cmd.Env = append(os.Environ(), envTestInit+"=1")
	cmd.Dir = dir
	cmd.ExtraFiles = []*os.File{child, rw}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(cgdir.Fd())}
	if err := cmd.Start(); err != nil {
		host.Close()
		return nil, 0, err
	}
	go func() { _ = cmd.Wait() }() // 收割 init；退出状态由契约测试另行断言
	child.Close()
	rw.Close()

	readyc := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := rr.Read(b[:])
		readyc <- err
	}()
	select {
	case err := <-readyc:
		if err != nil {
			host.Close()
			return nil, 0, fmt.Errorf("init 未就绪: %w", err)
		}
	case <-ctx.Done():
		rr.Close() // 唤醒读 goroutine，返回前它已结束
		<-readyc
		host.Close()
		return nil, 0, ctx.Err()
	}
	return sandbox.NewConn(host, 64), cmd.Process.Pid, nil
}

// requireRoot 在非 root 或 /sys/fs/cgroup 不是 cgroup v2 时跳过。
func requireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("需要 root")
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil || st.Type != 0x63677270 { // CGROUP2_SUPER_MAGIC
		t.Skip("/sys/fs/cgroup 不是 cgroup v2")
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

var (
	cgRootOnce sync.Once
	cgRoot     string // 本次测试运行独占的 cgroup 根 /sys/fs/cgroup/agentbox-test-<随机>
	cgRootErr  error
)

func testCgroupRoot(t *testing.T) string {
	t.Helper()
	cgRootOnce.Do(func() {
		// 子组要能设 cpu/memory/pids，cgroup 根必须先在 subtree_control 中启用它们（已启用时重复写无害）。
		_ = os.WriteFile("/sys/fs/cgroup/cgroup.subtree_control", []byte("+cpu +memory +pids"), 0o644)
		cgRoot = "/sys/fs/cgroup/agentbox-test-" + randHex(6)
		cgRootErr = os.Mkdir(cgRoot, 0o755)
	})
	if cgRootErr != nil {
		t.Fatalf("创建测试 cgroup 根: %v", cgRootErr)
	}
	return cgRoot
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

func removeTestCgroupRoot() {
	if cgRoot != "" {
		killRemove(cgRoot)
	}
}

// newTestProvider 返回使用 testStarter、独立数据目录与给定 install_id 的 Provider；测试结束时停止
// 其全部环境，并删除其 cgroup 与数据目录。
func newTestProvider(t *testing.T, installID string) *Provider {
	t.Helper()
	return newProviderWith(t, installID, testStarter{})
}

// newProviderWith 同 newTestProvider，但使用给定的启动器。
func newProviderWith(t *testing.T, installID string, starter EnvStarter) *Provider {
	t.Helper()
	requireRoot(t)
	root := testCgroupRoot(t)
	data, err := os.MkdirTemp("", "agentbox-data-")
	if err != nil {
		t.Fatal(err)
	}
	// 与装配相同（app 以 0711 建立数据目录）：exec 环境的 init 须经过它到达 /in 与 /out 的宿主目录。
	if err := os.Chmod(data, 0o711); err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{DataDir: data, CgroupRoot: root, InstallID: installID, Starter: starter})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		p.mu.Lock()
		var ids []string
		for id := range p.envs {
			ids = append(ids, id)
		}
		p.mu.Unlock()
		for _, id := range ids {
			_ = p.Stop(ctx, id)
		}
		killRemove(p.installCgroup())
		_ = unmountUnder(p.dataDir) // 失败用例留下的 exec /out tmpfs；尽力而为
		_ = os.RemoveAll(data)
	})
	return p
}

func testEnvSpec(installID, envID string) provider.EnvSpec {
	return provider.EnvSpec{
		EnvID: envID, InstallID: installID, Kind: provider.KindTask,
		UIDBase: 100000, UIDSize: 4096, Template: rootfs.DefaultTemplateName, // 测试 init 忽略模板内容；名称须可解析
		Limits: provider.Limits{MemoryMax: 256 << 20, PidsMax: 512, CPUQuotaUs: 100000},
	}
}

// testExecSpec 是 exec 环境的 spec：exec 模板、不提供 /in（provider 建立 InDir）、/out 1 MiB。
func testExecSpec(installID, envID string) provider.EnvSpec {
	s := testEnvSpec(installID, envID)
	s.Kind, s.Template, s.UIDBase = provider.KindExec, rootfs.ExecTemplateName, 400000
	s.Mounts = provider.Mounts{OutBytes: 1 << 20}
	return s
}

// blockStart 让 envID 上的下一次 StartExec 在登记在途启动之后、发送 start 之前阻塞（beforeSend 钩子）。
func blockStart(t *testing.T, p *Provider, envID string) (<-chan struct{}, func()) {
	t.Helper()
	st := p.state(envID)
	if st == nil {
		t.Fatalf("环境 %s 不在本进程中", envID)
	}
	blocked, rel := make(chan struct{}), make(chan struct{})
	var bonce, ronce sync.Once
	st.client.beforeSend = func(string) {
		bonce.Do(func() { close(blocked) })
		<-rel
	}
	return blocked, func() { ronce.Do(func() { close(rel) }) }
}

// residue 使 envID 成为中断的创建：owner.json 写入之后、建 cgroup 与启动 init 之前 ctx 被取消。
func residue(t *testing.T, p *Provider, spec provider.EnvSpec) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.afterOwner = cancel
	defer func() { p.afterOwner = nil }()
	if _, err := p.Create(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("中断的 Create = %v，期望 context.Canceled", err)
	}
}

// TestLocalContract：Provider 契约一致性全集运行在真实 cgroup 与测试 init 上。
func TestLocalContract(t *testing.T) {
	requireRoot(t)
	install := "c" + randHex(4)
	providertest.Run(t, providertest.Harness{
		New:        func(t *testing.T) provider.Provider { return newTestProvider(t, install) },
		Spec:       func(envID string) provider.EnvSpec { return testEnvSpec(install, envID) },
		Echo:       provider.ExecSpec{ExecID: "echo", Argv: []string{"/bin/sh", "-c", "echo hello; exit 3"}},
		EchoOutput: "hello",
		EchoCode:   3,
		Sleep:      provider.ExecSpec{ExecID: "sleep", Argv: []string{"/bin/sleep", "1000"}},
		Residue: func(t *testing.T, p provider.Provider, envID string) {
			residue(t, p.(*Provider), testEnvSpec(install, envID))
		},
		Foreign: func(t *testing.T, p provider.Provider, envID string) {
			if err := os.Mkdir(p.(*Provider).envDir(envID), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		BlockStart: func(t *testing.T, p provider.Provider, envID string) (<-chan struct{}, func()) {
			return blockStart(t, p.(*Provider), envID)
		},
		ExecSpec: func(envID string) provider.EnvSpec { return testExecSpec(install, envID) },
	})
}

// TestNewRequiresStarter：没有启动器时 New 失败（不存在可用于生产的启动器）。
func TestNewRequiresStarter(t *testing.T) {
	if _, err := New(Options{DataDir: t.TempDir(), CgroupRoot: "/sys/fs/cgroup", InstallID: "i"}); err == nil {
		t.Fatal("Starter 为 nil 时 New 应返回错误")
	}
}

// TestParseMountsUnder：按路径分量匹配数据目录（/d/envs2 不属于 /d/envs），并还原八进制转义。
func TestParseMountsUnder(t *testing.T) {
	mi := strings.Join([]string{
		`22 1 0:21 / /d/envs rw - tmpfs t rw`,
		`23 22 0:22 / /d/envs/a\040b/rootfs rw - tmpfs t rw`,
		`24 1 0:23 / /d/envs2 rw - tmpfs t rw`,
		`25 1 0:24 / /other rw - tmpfs t rw`,
	}, "\n")
	got := parseMountsUnder([]byte(mi), "/d/envs")
	if want := []string{"/d/envs", "/d/envs/a b/rootfs"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("得到 %q，期望 %q", got, want)
	}
}

// layersGone 断言环境的三层（数据目录下的挂载、环境 cgroup、环境目录）都不存在。
func layersGone(t *testing.T, p *Provider, envID string) {
	t.Helper()
	dir := p.envDir(envID)
	if ms, err := mountsUnder(dir); err != nil || len(ms) != 0 {
		t.Errorf("挂载层仍存在：%v %v", ms, err)
	}
	if ok, err := cgroup.Exists(p.installCgroup(), cgroupName(envID)); err != nil || ok {
		t.Errorf("cgroup 层仍存在：%v %v", ok, err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("目录层仍存在：%v", err)
	}
}

func mustGroup(t *testing.T, p *Provider, envID string) *cgroup.Group {
	t.Helper()
	g, ok, err := p.group(envID)
	if err != nil || !ok {
		t.Fatalf("环境 cgroup：%v %v", ok, err)
	}
	return g
}

// TestE9DetachedSleeperKilledByStop：workload 双重 fork 并 setsid 一个长睡眠进程后退出；Stop 成功、
// cgroup 中没有进程；Destroy 后三层都不存在。
func TestE9DetachedSleeperKilledByStop(t *testing.T) {
	install := "e" + randHex(4)
	p := newTestProvider(t, install)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := p.Create(ctx, testEnvSpec(install, "env-e9")); err != nil {
		t.Fatal(err)
	}
	h, err := p.StartExec(ctx, "env-e9", provider.ExecSpec{ExecID: "e9", Argv: []string{"/bin/sh", "-c",
		"(setsid /bin/sleep 1000 </dev/null >/dev/null 2>&1 &); echo started"}})
	if err != nil {
		t.Fatal(err)
	}
	h.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
	if out, err := io.ReadAll(h.Stdout()); err != nil || strings.TrimSpace(string(out)) != "started" {
		t.Fatalf("stdout = %q, %v", out, err)
	}
	if st, err := h.Wait(); err != nil || st.Code != 0 {
		t.Fatalf("Wait = %+v, %v", st, err)
	}

	g := mustGroup(t, p, "env-e9")
	sleeper := 0
	for deadline := time.Now().Add(10 * time.Second); sleeper == 0 && time.Now().Before(deadline); {
		pids, err := g.Procs()
		if err != nil {
			t.Fatal(err)
		}
		for _, pid := range pids {
			comm, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
			if strings.TrimSpace(string(comm)) == "sleep" && sessionLeader(pid) {
				sleeper = pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sleeper == 0 {
		t.Fatal("cgroup 中没有出现 setsid 的 sleep 进程")
	}

	if err := p.Stop(ctx, "env-e9"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if populated, err := g.Populated(); err != nil || populated {
		t.Fatalf("Stop 后 populated = %v, %v", populated, err)
	}
	if pids, err := g.Procs(); err != nil || len(pids) != 0 {
		t.Fatalf("Stop 后 cgroup 中仍有进程 %v（%v）", pids, err)
	}
	if err := p.Destroy(ctx, "env-e9"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	layersGone(t, p, "env-e9")
}

// sessionLeader 报告 pid 是否是自己会话的首进程（/proc/<pid>/stat 的 session 字段等于 pid）。
func sessionLeader(pid int) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return false
	}
	f := strings.Fields(s[i+1:]) // state ppid pgrp session ...
	return len(f) > 3 && f[3] == fmt.Sprint(pid)
}

// TestStartInFlightWhileStop：契约第 5 节的确定性用例。StartExec 登记在途启动后、发送 start 前阻塞；
// Stop 成功；放行后它以 ErrControlLost、ErrStopping 或 *StartError 返回，cgroup 中没有进程；
// 之后的 StartExec 为 ErrStopping。
func TestStartInFlightWhileStop(t *testing.T) {
	install := "f" + randHex(4)
	p := newTestProvider(t, install)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := p.Create(ctx, testEnvSpec(install, "env-s")); err != nil {
		t.Fatal(err)
	}
	g := mustGroup(t, p, "env-s")
	blocked, release := blockStart(t, p, "env-s")
	defer release()
	res := make(chan error, 1)
	go func() {
		_, err := p.StartExec(ctx, "env-s", provider.ExecSpec{ExecID: "late", Argv: []string{"/bin/sleep", "1000"}})
		res <- err
	}()
	<-blocked
	if err := p.Stop(ctx, "env-s"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	release()
	err := <-res
	var se *provider.StartError
	if !errors.Is(err, provider.ErrControlLost) && !errors.Is(err, provider.ErrStopping) && !errors.As(err, &se) {
		t.Fatalf("在途启动 = %v，期望 ErrControlLost、ErrStopping 或 *StartError", err)
	}
	if pids, err := g.Procs(); err != nil || len(pids) != 0 {
		t.Fatalf("cgroup 中有进程 %v（%v）", pids, err)
	}
	if _, err := p.StartExec(ctx, "env-s", provider.ExecSpec{ExecID: "after", Argv: []string{"/bin/true"}}); !errors.Is(err, provider.ErrStopping) {
		t.Fatalf("之后的 StartExec = %v，期望 ErrStopping", err)
	}
}

// TestInterruptedCreate：owner.json 之后、init 之前取消 ctx → 留下属于本安装的残留（目录与 owner.json，
// 无 cgroup）；再次 Create 为 ErrIncomplete；Stop + Destroy 后可重建。
func TestInterruptedCreate(t *testing.T) {
	install := "i" + randHex(4)
	p := newTestProvider(t, install)
	ctx := context.Background()
	spec := testEnvSpec(install, "env-r")
	residue(t, p, spec)
	if _, err := os.Stat(filepath.Join(p.envDir("env-r"), "owner.json")); err != nil {
		t.Fatalf("owner.json 应已写入：%v", err)
	}
	if ok, _ := cgroup.Exists(p.installCgroup(), cgroupName("env-r")); ok {
		t.Fatal("取消后不应继续创建 cgroup")
	}
	if _, err := p.Create(ctx, spec); !errors.Is(err, provider.ErrIncomplete) {
		t.Fatalf("再次 Create = %v，期望 ErrIncomplete", err)
	}
	if err := p.Stop(ctx, "env-r"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := p.Destroy(ctx, "env-r"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	layersGone(t, p, "env-r")
	if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
		t.Fatalf("重建 = %+v, %v", info, err)
	}
}

// TestDirWithoutOwnerIsForeign：没有 owner.json 的目录 → Create 为 ErrForeign 且不建 cgroup；Scan 以
// 目录名填写 EnvID 并报告 Unknown；Destroy 不删除它。
func TestDirWithoutOwnerIsForeign(t *testing.T) {
	install := "o" + randHex(4)
	p := newTestProvider(t, install)
	ctx := context.Background()
	dir := p.envDir("env-x")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Create(ctx, testEnvSpec(install, "env-x")); !errors.Is(err, provider.ErrForeign) {
		t.Fatalf("Create = %v，期望 ErrForeign", err)
	}
	if ok, _ := cgroup.Exists(p.installCgroup(), cgroupName("env-x")); ok {
		t.Fatal("ErrForeign 时不应创建 cgroup")
	}
	r, err := p.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range r.Items {
		if it.Layer == "env_dir" && it.Path == dir {
			found = true
			if it.EnvID != "env-x" || it.Owner != provider.Unknown {
				t.Fatalf("Scan 项 = %+v，期望 EnvID env-x、Unknown", it)
			}
		}
	}
	if !found {
		t.Fatalf("Scan 没有报告 %s：%+v", dir, r.Items)
	}
	if err := p.Destroy(ctx, "env-x"); !errors.Is(err, provider.ErrForeign) {
		t.Fatalf("Destroy = %v，期望 ErrForeign", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("Destroy 删除了外来目录：%v", err)
	}
}

// TestResourceDiagOOM：workload 超出 memory.max 被杀后，OOMKillDelta > 0 且 OOMObserved。
func TestResourceDiagOOM(t *testing.T) {
	install := "m" + randHex(4)
	p := newTestProvider(t, install)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	spec := testEnvSpec(install, "env-m")
	spec.Limits.MemoryMax = 64 << 20
	if _, err := p.Create(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if d, err := p.ResourceDiag(ctx, "env-m"); err != nil || d.OOMKillDelta != 0 || d.OOMObserved {
		t.Fatalf("初始 ResourceDiag = %+v, %v", d, err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	h, err := p.StartExec(ctx, "env-m", provider.ExecSpec{ExecID: "hog", Argv: []string{exe, "-test.run=^$"}, Env: []string{envTestHog + "=1"}})
	if err != nil {
		t.Fatal(err)
	}
	h.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, h.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
	if st, err := h.Wait(); err != nil || st.Signal != syscall.SIGKILL {
		t.Fatalf("Wait = %+v, %v；期望被 SIGKILL（OOM）", st, err)
	}
	d, err := p.ResourceDiag(ctx, "env-m")
	if err != nil || d.OOMKillDelta == 0 || !d.OOMObserved {
		t.Fatalf("ResourceDiag = %+v, %v；期望 OOMKillDelta > 0", d, err)
	}
}

// ---------------------------------------------------------------------------
// Task 9：生产启动器（ProcessStarter）。专用启动进程与 init 都是本测试二进制：TestMain 把
// sandbox-launch 分流到 sandbox.RunLaunch（生产代码），把 init 分流到 runProdTestInit——它在
// 命名空间中运行，先自报所在的 cgroup，再按生产的就绪约定（fd 3 控制连接、fd 4 就绪管道）运行
// sandbox.Serve 与直接 exec 的 testLauncher。init 环境建立（挂载、pivot_root、能力）属 Task 10。

// envInitReport 指向一个目录：init 启动时把 /proc/self/cgroup 写到 <目录>/<宿主 pid>。
const envInitReport = "AGENTBOX_TEST_INIT_REPORT"

// runProdTestInit 是生产启动器启动的测试 init 的主函数。
func runProdTestInit() int {
	if dir := os.Getenv(envInitReport); dir != "" {
		cg, err := os.ReadFile("/proc/self/cgroup")
		if err != nil {
			fmt.Fprintf(os.Stderr, "test init: 读取 cgroup: %v\n", err)
			return 1
		}
		// /proc 仍是宿主的 proc 实例（挂载在 Task 10 建立），/proc/self 指向宿主 pid。
		self, err := os.Readlink("/proc/self")
		if err != nil {
			fmt.Fprintf(os.Stderr, "test init: 读取宿主 pid: %v\n", err)
			return 1
		}
		if err := os.WriteFile(filepath.Join(dir, self), cg, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "test init: 写自报: %v\n", err)
			return 1
		}
	}
	return runTestInit()
}

// prodStarter 返回生产启动器（本进程成为 child subreaper）与 init 自报目录。
func prodStarter(t *testing.T) (*ProcessStarter, string) {
	t.Helper()
	requireRoot(t)
	st, err := NewProcessStarter()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "agentbox-init-report-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	// init 以映射后的宿主 uid 运行，须能在此创建文件。
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	st.initEnv = []string{envInitReport + "=" + dir}
	return st, dir
}

// procStatus 解析 /proc/<pid>/status 为 键 → 值（去掉首尾空白）。
func procStatus(t *testing.T, pid string) map[string]string {
	t.Helper()
	b, err := os.ReadFile("/proc/" + pid + "/status")
	if err != nil {
		t.Fatalf("读取 /proc/%s/status: %v", pid, err)
	}
	m := make(map[string]string)
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			m[k] = strings.TrimSpace(v)
		}
	}
	return m
}

// zombieChildren 返回本进程处于僵尸状态的子进程。
func zombieChildren(t *testing.T) []string {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	self := fmt.Sprint(os.Getpid())
	var out []string
	for _, e := range ents {
		b, err := os.ReadFile("/proc/" + e.Name() + "/status")
		if err != nil {
			continue
		}
		var state, ppid string
		for _, line := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(line, "State:"); ok {
				state = strings.TrimSpace(v)
			}
			if v, ok := strings.CutPrefix(line, "PPid:"); ok {
				ppid = strings.TrimSpace(v)
			}
		}
		if ppid == self && strings.HasPrefix(state, "Z") {
			out = append(out, e.Name())
		}
	}
	return out
}

// waitReaped 等待 pid 被收割（/proc/<pid> 消失）；超时仍为僵尸或仍存在则失败。
func waitReaped(t *testing.T, pid int) {
	t.Helper()
	path := fmt.Sprintf("/proc/%d", pid)
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return
		}
	}
	st, _ := os.ReadFile(path + "/stat")
	t.Fatalf("init %d 未被收割：%s", pid, st)
}

// TestProductionStarterIsolation：生产启动器启动若干环境（不同 UID 范围）：
//   - server（测试进程）的 Groups 前后不变（附加组只在专用启动进程中清空）；
//   - init：Groups 为空、NSpid 两级（ns 内为 1）、uid/gid 映射与范围一致、setgroups 为 deny、
//     宿主 uid/gid 为范围基址（ns root）、父进程是 server（启动进程退出后被收养）；
//   - init 在环境 cgroup 中：cgroup.procs 含 init，且 init 启动时自报的 cgroup 路径即环境 cgroup；
//   - workload 经该 init 运行并看到命名空间内的身份；
//   - Stop 后 init 由 server 收割（无僵尸）。
func TestProductionStarterIsolation(t *testing.T) {
	install := "p" + randHex(4)
	st, report := prodStarter(t)
	p := newProviderWith(t, install, st)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	self := fmt.Sprint(os.Getpid())
	groupsBefore := procStatus(t, self)["Groups"]

	bases := []uint32{100000, 200000, 300000}
	pids := make([]int, len(bases))
	for i, base := range bases {
		envID := fmt.Sprintf("env-p%d", i)
		spec := testEnvSpec(install, envID)
		spec.UIDBase = base
		if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
			t.Fatalf("Create %s = %+v, %v", envID, info, err)
		}
		pid := p.state(envID).pid
		pids[i] = pid
		ps := fmt.Sprint(pid)
		s := procStatus(t, ps)
		if s["Groups"] != "" {
			t.Errorf("%s: init 的 Groups = %q，期望为空", envID, s["Groups"])
		}
		if ns := strings.Fields(s["NSpid"]); len(ns) != 2 || ns[0] != ps || ns[1] != "1" {
			t.Errorf("%s: init 的 NSpid = %q，期望 \"%s 1\"", envID, s["NSpid"], ps)
		}
		if s["PPid"] != self {
			t.Errorf("%s: init 的 PPid = %s，期望 server %s", envID, s["PPid"], self)
		}
		b := fmt.Sprint(base)
		for _, k := range []string{"Uid", "Gid"} {
			if f := strings.Fields(s[k]); len(f) != 4 || f[0] != b || f[1] != b || f[2] != b || f[3] != b {
				t.Errorf("%s: init 的 %s = %q，期望全为 %s", envID, k, s[k], b)
			}
		}
		for _, m := range []string{"uid_map", "gid_map"} {
			raw, err := os.ReadFile("/proc/" + ps + "/" + m)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := strings.Fields(string(raw)), []string{"0", b, "4096"}; !reflect.DeepEqual(got, want) {
				t.Errorf("%s: %s = %q，期望 %q", envID, m, got, want)
			}
		}
		if raw, err := os.ReadFile("/proc/" + ps + "/setgroups"); err != nil || strings.TrimSpace(string(raw)) != "deny" {
			t.Errorf("%s: setgroups = %q, %v，期望 deny", envID, raw, err)
		}

		g := mustGroup(t, p, envID)
		procs, err := g.Procs()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, q := range procs {
			found = found || q == pid
		}
		if !found {
			t.Errorf("%s: cgroup.procs %v 不含 init %d", envID, procs, pid)
		}
		rep, err := os.ReadFile(filepath.Join(report, ps))
		if err != nil {
			t.Fatalf("%s: 读取 init 自报: %v", envID, err)
		}
		if got, want := strings.TrimSpace(string(rep)), "0::"+strings.TrimPrefix(g.Path(), "/sys/fs/cgroup"); got != want {
			t.Errorf("%s: init 自报的 cgroup = %q，期望 %q", envID, got, want)
		}

		h, err := p.StartExec(ctx, envID, provider.ExecSpec{ExecID: "id", Argv: []string{"/bin/sh", "-c", "id -u; id -G; echo $$"}})
		if err != nil {
			t.Fatal(err)
		}
		h.Stdin().Close()
		go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
		out, _ := io.ReadAll(h.Stdout())
		if es, err := h.Wait(); err != nil || es.Code != 0 {
			t.Fatalf("%s: workload = %+v, %v", envID, es, err)
		}
		// 命名空间内 uid 0；附加组为空，id -G 只列出主组 0；在 pid 命名空间中 pid 很小（非宿主 pid）。
		if f := strings.Fields(string(out)); len(f) != 3 || f[0] != "0" || f[1] != "0" || len(f[2]) > 3 {
			t.Errorf("%s: workload 输出 %q，期望 uid 0、组 0、命名空间内的小 pid", envID, out)
		}
	}
	if after := procStatus(t, self)["Groups"]; after != groupsBefore {
		t.Fatalf("server 的 Groups 由 %q 变为 %q", groupsBefore, after)
	}

	for i, pid := range pids {
		envID := fmt.Sprintf("env-p%d", i)
		if err := p.Stop(ctx, envID); err != nil {
			t.Fatalf("Stop %s: %v", envID, err)
		}
		waitReaped(t, pid)
		if err := p.Destroy(ctx, envID); err != nil {
			t.Fatalf("Destroy %s: %v", envID, err)
		}
		layersGone(t, p, envID)
	}
	if z := zombieChildren(t); len(z) != 0 {
		t.Fatalf("server 有僵尸子进程 %v", z)
	}
}

// TestLaunchFailureLeavesIncomplete：专用启动进程在 clone 之前、之后各注入失败 → Create 失败并给出
// 启动进程的原因；没有进程留在环境 cgroup 之外（server 无僵尸子进程，cgroup 中也没有进程）；再次
// Create 为 ErrIncomplete；Stop + Destroy 后三层不存在，去掉注入后可重建。
func TestLaunchFailureLeavesIncomplete(t *testing.T) {
	for _, at := range []string{sandbox.LaunchFailBeforeClone, sandbox.LaunchFailAfterClone} {
		t.Run(at, func(t *testing.T) {
			install := "l" + randHex(4)
			st, _ := prodStarter(t)
			st.failAt = at
			p := newProviderWith(t, install, st)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			spec := testEnvSpec(install, "env-l")
			_, err := p.Create(ctx, spec)
			if err == nil || !strings.Contains(err.Error(), "launch/"+at) {
				t.Fatalf("Create = %v，期望启动进程报告 launch/%s", err, at)
			}
			if z := zombieChildren(t); len(z) != 0 {
				t.Fatalf("server 有僵尸子进程 %v", z)
			}
			if procs, err := mustGroup(t, p, "env-l").Procs(); err != nil || len(procs) != 0 {
				t.Fatalf("环境 cgroup 中有进程 %v（%v）", procs, err)
			}
			if _, err := p.Create(ctx, spec); !errors.Is(err, provider.ErrIncomplete) {
				t.Fatalf("再次 Create = %v，期望 ErrIncomplete", err)
			}
			if err := p.Stop(ctx, "env-l"); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			if err := p.Destroy(ctx, "env-l"); err != nil {
				t.Fatalf("Destroy: %v", err)
			}
			layersGone(t, p, "env-l")
			st.failAt = ""
			if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
				t.Fatalf("重建 = %+v, %v", info, err)
			}
		})
	}
}

// TestLocalContractProductionStarter：Provider 契约一致性全集运行在生产启动器上（init 为测试 init）。
func TestLocalContractProductionStarter(t *testing.T) {
	requireRoot(t)
	install := "q" + randHex(4)
	providertest.Run(t, providertest.Harness{
		New: func(t *testing.T) provider.Provider {
			st, _ := prodStarter(t)
			return newProviderWith(t, install, st)
		},
		Spec:       func(envID string) provider.EnvSpec { return testEnvSpec(install, envID) },
		Echo:       provider.ExecSpec{ExecID: "echo", Argv: []string{"/bin/sh", "-c", "echo hello; exit 3"}},
		EchoOutput: "hello",
		EchoCode:   3,
		Sleep:      provider.ExecSpec{ExecID: "sleep", Argv: []string{"/bin/sleep", "1000"}},
		Residue: func(t *testing.T, p provider.Provider, envID string) {
			residue(t, p.(*Provider), testEnvSpec(install, envID))
		},
		Foreign: func(t *testing.T, p provider.Provider, envID string) {
			if err := os.Mkdir(p.(*Provider).envDir(envID), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		BlockStart: func(t *testing.T, p provider.Provider, envID string) (<-chan struct{}, func()) {
			return blockStart(t, p.(*Provider), envID)
		},
		ExecSpec: func(envID string) provider.EnvSpec { return testExecSpec(install, envID) },
	})
}

// TestLaunchSpecForFillsInit：生产启动器把 EnvSpec 的 Limits/Mounts/Template 填入 LaunchSpec.Init（规格 §4.5）；
// 规格未给出时取默认值（nofile 1024、/tmp 64 MiB、默认模板）；未知模板名报错。不需要 root。
func TestLaunchSpecForFillsInit(t *testing.T) {
	ex := provider.EnvSpec{
		EnvID: "e1", Kind: provider.KindExec, UIDBase: 100000, UIDSize: 4096,
		Limits: provider.Limits{NoFile: 256, FSize: 1 << 20, TmpBytes: 8 << 20},
		Mounts: provider.Mounts{In: "/srv/in", OutBytes: 4 << 20},
	}
	ls, err := launchSpecFor(ex, "/d/envs/e1")
	if err != nil {
		t.Fatal(err)
	}
	in := ls.Init
	if ls.GIDBase != 100000 || ls.GIDSize != 4096 || ls.Hostname != envHostname {
		t.Fatalf("LaunchSpec = %+v", ls)
	}
	if in.Kind != "exec" || in.NoFile != 256 || in.FSize != 1<<20 || in.TmpBytes != 8<<20 || in.In != "/srv/in" || in.OutBytes != 4<<20 ||
		in.Out != "/d/envs/e1/out" {
		t.Fatalf("Init = %+v", in)
	}
	if len(in.Template.Paths) == 0 {
		t.Fatal("默认模板未解析")
	}
	// 不提供 /in：使用 provider 建立的 <envdir>/in（D2）；exec 模板按名称解析。
	ex.Mounts.In, ex.Template = "", rootfs.ExecTemplateName
	if ls, err = launchSpecFor(ex, "/d/envs/e1"); err != nil || ls.Init.In != "/d/envs/e1/in" || ls.Init.Out != "/d/envs/e1/out" {
		t.Fatalf("Init = %+v, %v", ls.Init, err)
	}
	if !reflect.DeepEqual(ls.Init.Template.Paths, rootfs.ExecTemplate().Paths) {
		t.Fatalf("exec 模板未解析：%v", ls.Init.Template.Paths)
	}

	ls, err = launchSpecFor(provider.EnvSpec{Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096,
		Mounts: provider.Mounts{Workspace: "/var/ws", GatewaySocket: "/run/gw.sock"}}, "/d/envs/t1")
	if err != nil {
		t.Fatal(err)
	}
	if ls.Init.NoFile != defaultNoFile || ls.Init.TmpBytes != defaultTmpBytes || ls.Init.Workspace != "/var/ws" || ls.Init.GatewaySocket != "/run/gw.sock" ||
		ls.Init.In != "" || ls.Init.Out != "" {
		t.Fatalf("默认值未填充或编排环境带了 /in、/out：%+v", ls.Init)
	}
	// session 的冷恢复暂存目录（M4 Plan 12）原样交给 init。
	ls, err = launchSpecFor(provider.EnvSpec{Kind: provider.KindSession, UIDBase: 100000, UIDSize: 4096,
		Mounts: provider.Mounts{Workspace: "/var/ws", RestoreDir: "/d/restore/s1"}}, "/d/envs/s1")
	if err != nil || ls.Init.Kind != "session" || ls.Init.Restore != "/d/restore/s1" {
		t.Fatalf("session Init = %+v, %v", ls.Init, err)
	}
	if _, err := launchSpecFor(provider.EnvSpec{Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096, Template: "no-such"}, "/d"); err == nil {
		t.Fatal("未知模板应报错")
	}
}

// TestWorkloadLimitsApplied：生产启动路径（真实 init + stage-2 helper，无测试钩子）把 EnvSpec.Limits 的
// NoFile/FSize 真正施加到 workload：workload 自报 `ulimit -n` / `ulimit -f` 与配置一致（规格 §4.5）。
// 这是 provider 层第一条走完整生产链的用例；需要 root 与默认模板的宿主路径。
func TestWorkloadLimitsApplied(t *testing.T) {
	requireRoot(t)
	st, err := NewProcessStarter()
	if err != nil {
		t.Fatal(err)
	}
	st.initEnv = []string{envRealInit + "=1"} // 真实 init + helper（TestMain 据此分流）
	install := "l" + randHex(4)
	p := newProviderWith(t, install, st)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	run := func(envID string, spec provider.EnvSpec, script string) string {
		t.Helper()
		if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
			t.Fatalf("Create %s = %+v, %v", envID, info, err)
		}
		t.Cleanup(func() { _ = p.Stop(context.Background(), envID); _ = p.Destroy(context.Background(), envID) })
		h, err := p.StartExec(ctx, envID, provider.ExecSpec{ExecID: "lim", Argv: []string{"/usr/bin/sh", "-c", script}})
		if err != nil {
			t.Fatalf("StartExec %s: %v", envID, err)
		}
		h.Stdin().Close()
		var stderr strings.Builder
		go func() { _, _ = io.Copy(&stderr, h.Stderr()) }()
		out, _ := io.ReadAll(h.Stdout())
		if es, err := h.Wait(); err != nil || es.Code != 0 {
			t.Fatalf("%s: workload = %+v, %v; stderr %q", envID, es, err, stderr.String())
		}
		return strings.TrimSpace(string(out))
	}

	task := testEnvSpec(install, "env-lim-task")
	task.Limits.NoFile = 256
	if got := run("env-lim-task", task, "ulimit -n"); got != "256" {
		t.Errorf("task workload 的 RLIMIT_NOFILE = %q，期望 256", got)
	}

	ex := testEnvSpec(install, "env-lim-exec")
	ex.Kind = provider.KindExec
	ex.UIDBase = 200000
	ex.Limits.NoFile = 128
	ex.Limits.FSize = 1 << 20 // 1 MiB = 2048 个 512 字节块（sh 的 ulimit -f 单位）
	ex.Mounts = provider.Mounts{OutBytes: 4 << 20}
	if got := run("env-lim-exec", ex, "echo $(ulimit -n) $(ulimit -f)"); got != "128 2048" {
		t.Errorf("exec workload 的 (NOFILE, FSIZE) = %q，期望 \"128 2048\"", got)
	}
}

// TestPrepareGatewaySocket（Plan 7 Task 5）：Gateway socket（Mounts.GatewaySocket）chown 到映射 uid/gid 1000、
// 权限保持 0600；经符号链接给出的路径与非 socket 被拒绝，链接目标的属主不变。
func TestPrepareGatewaySocket(t *testing.T) {
	requireRoot(t)
	dir := t.TempDir()
	listen := func(name string) string {
		p := filepath.Join(dir, name)
		ln, err := net.Listen("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		if err := os.Chmod(p, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	const uidBase = 300000
	sock := listen("a1.sock")
	if err := prepareGatewaySocket(sock, uidBase+workloadID); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(sock, &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != uidBase+workloadID || st.Gid != uidBase+workloadID || st.Mode&0o7777 != 0o600 {
		t.Fatalf("socket 属主 %d:%d 权限 %#o，期望 %d、0600", st.Uid, st.Gid, st.Mode&0o7777, uidBase+workloadID)
	}
	other := listen("b.sock")
	link := filepath.Join(dir, "link.sock")
	if err := os.Symlink(other, link); err != nil {
		t.Fatal(err)
	}
	if err := prepareGatewaySocket(link, uidBase+workloadID); err == nil {
		t.Fatal("符号链接应被拒绝")
	}
	if err := syscall.Stat(other, &st); err != nil || st.Uid != 0 {
		t.Fatalf("符号链接的目标 %s 被改变属主（%d，%v）", other, st.Uid, err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareGatewaySocket(plain, uidBase+workloadID); err == nil || !strings.Contains(err.Error(), "不是 socket") {
		t.Fatalf("普通文件应被拒绝，得到 %v", err)
	}
}

// TestPrepareWorkspace（Task 13）：workspace 连同其中条目（不跟随符号链接）chown 到映射 uid/gid 1000；
// 上级目录缺少 o+x 时拒绝启动并指出该目录（沙箱 init 以映射 root 运行，无法经过它）。
func TestPrepareWorkspace(t *testing.T) {
	requireRoot(t)
	base := t.TempDir() // 上级是 os.MkdirTemp 建立的 0700 目录
	ws := filepath.Join(base, "ws")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(filepath.Join(ws, "out", "a1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "out", "a1", "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(ws, "link")); err != nil {
		t.Fatal(err)
	}
	const uidBase = 300000
	parent := filepath.Dir(base)
	if err := prepareWorkspace(ws, uidBase+workloadID); err == nil || !strings.Contains(err.Error(), parent) {
		t.Fatalf("上级目录 %s 为 0700 时 prepareWorkspace = %v，期望指出该目录", parent, err)
	}
	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := prepareWorkspace(ws, uidBase+workloadID); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{ws, filepath.Join(ws, "out"), filepath.Join(ws, "out", "a1"), filepath.Join(ws, "out", "a1", "f"), filepath.Join(ws, "link")} {
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil {
			t.Fatal(err)
		}
		if st.Uid != uidBase+workloadID || st.Gid != uidBase+workloadID {
			t.Fatalf("%s 属主 %d:%d，期望 %d", p, st.Uid, st.Gid, uidBase+workloadID)
		}
	}
	var st syscall.Stat_t
	if err := syscall.Stat(outside, &st); err != nil || st.Uid != 0 {
		t.Fatalf("符号链接的目标 %s 被改变属主（%d，%v）", outside, st.Uid, err)
	}
}

// ---------------------------------------------------------------------------
// Plan 15 Task 2：exec 环境的 /in 暂存、宿主侧 /out 与 OpenOutputs（规格 §10.2，D1、D2）。生产启动路径
// （真实 init + stage-2 helper）、exec 模板；需要 root 与宿主 /usr/bin/python3。

// newExecEnv 返回走生产启动路径的 Provider 与 exec 环境的 spec（exec 模板、Mounts{OutBytes: 1 MiB}）。
func newExecEnv(t *testing.T) (*Provider, provider.EnvSpec) {
	t.Helper()
	requireRoot(t)
	if err := rootfs.ExecTemplate().EnsureExec(); err != nil {
		t.Fatalf("宿主上的 exec 模板不可用: %v", err)
	}
	st, err := NewProcessStarter()
	if err != nil {
		t.Fatal(err)
	}
	st.initEnv = []string{envRealInit + "=1"}
	install := "x" + randHex(4)
	p := newProviderWith(t, install, st)
	spec := testExecSpec(install, "env-x")
	spec.Limits.NoFile, spec.Limits.FSize = 256, 64<<20
	return p, spec
}

// execEnv 是 exec 请求的固定执行环境（Plan 15 Global Constraints）。
var execEnv = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/tmp", "LANG=C.UTF-8", "PYTHONDONTWRITEBYTECODE=1", "PYTHONUNBUFFERED=1"}

// stageMain 把 code 写入 <inDir>/.agentbox/main.py（目录 0755、文件 0444，root 属主）。
func stageMain(t *testing.T, inDir, code string) {
	t.Helper()
	dir := filepath.Join(inDir, ".agentbox")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte(code), 0o444); err != nil {
		t.Fatal(err)
	}
}

// runMain 以 exec 的固定形式（python3 -I -B /in/.agentbox/main.py，cwd /out，stdin 立即关闭）运行，返回 stdout、
// stderr 与退出状态。
func runMain(ctx context.Context, t *testing.T, p *Provider, envID string) (string, string, provider.ExitStatus) {
	t.Helper()
	h, err := p.StartExec(ctx, envID, provider.ExecSpec{ExecID: "main", Argv: []string{"python3", "-I", "-B", "/in/.agentbox/main.py"},
		Env: execEnv, Dir: "/out"})
	if err != nil {
		t.Fatalf("StartExec: %v", err)
	}
	if err := h.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	errDone := make(chan struct{})
	go func() { _, _ = io.Copy(&stderr, h.Stderr()); close(errDone) }()
	out, err := io.ReadAll(h.Stdout())
	if err != nil {
		t.Fatal(err)
	}
	<-errDone
	es, err := h.Wait()
	if err != nil {
		t.Fatalf("Wait: %v（stderr %q）", err, stderr.String())
	}
	return string(out), stderr.String(), es
}

const execInOutScript = `
import errno, os
print(open('/in/data/x.csv').read().strip())
os.makedirs('/out/r')
with open('/out/r/a.txt', 'w') as f:
    f.write('result-a')
os.symlink('/etc/passwd', '/out/l')
os.mkfifo('/out/f')
os.makedirs('/out/x')
for i in range(300):
    open('/out/x/%03d' % i, 'w').close()
for probe, fn in (('opt', lambda: os.listdir('/opt/agentbox')), ('in', lambda: open('/in/w', 'w'))):
    try:
        fn()
        print(probe, 'ok')
    except OSError as e:
        print(probe, errno.errorcode[e.errno])
`

// TestExecEnvInOut：exec 环境的 /in 暂存与宿主侧 /out（§10.2，D1、D2）。
func TestExecEnvInOut(t *testing.T) {
	p, spec := newExecEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	info, err := p.Create(ctx, spec)
	if err != nil || !info.Complete {
		t.Fatalf("Create = %+v, %v", info, err)
	}
	envDir := p.envDir(spec.EnvID)
	if info.InDir != filepath.Join(envDir, "in") {
		t.Fatalf("InDir = %q，期望 %s/in", info.InDir, envDir)
	}
	for path, want := range map[string]uint32{envDir: 0o711, info.InDir: 0o755, filepath.Join(envDir, "owner.json"): 0o600} {
		var st syscall.Stat_t
		if err := syscall.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		if st.Mode&0o7777 != want || st.Uid != 0 || st.Gid != 0 {
			t.Errorf("%s：权限 %#o、属主 %d:%d，期望 %#o、root", path, st.Mode&0o7777, st.Uid, st.Gid, want)
		}
	}
	stageMain(t, info.InDir, execInOutScript)
	if err := os.Mkdir(filepath.Join(info.InDir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(info.InDir, "data", "x.csv"), []byte("1,2\n"), 0o444); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, es := runMain(ctx, t, p, spec.EnvID)
	if es.Code != 0 || es.Signal != 0 {
		t.Fatalf("workload = %+v；stderr %q", es, stderr)
	}
	if want := "1,2\nopt ENOENT\nin EROFS\n"; stdout != want {
		t.Fatalf("stdout = %q，期望 %q（stderr %q）", stdout, want, stderr)
	}
	if _, _, err := p.OpenOutputs(ctx, spec.EnvID, provider.MaxOutputFiles); !errors.Is(err, provider.ErrNotStopped) {
		t.Fatalf("Stop 之前 OpenOutputs = %v，期望 ErrNotStopped", err)
	}
	if err := p.Stop(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}

	files, skipped, err := p.OpenOutputs(ctx, spec.EnvID, provider.MaxOutputFiles)
	if err != nil {
		t.Fatal(err)
	}
	closeAll := func() {
		for _, f := range files {
			_ = f.File.Close() // 只读文件，关闭错误无关紧要
		}
		files = nil
	}
	defer closeAll()
	// 字典序：f、l、r/a.txt、x/000…x/299。普通文件中 r/a.txt 与 x/000…x/254 共 256 个被收集。
	if len(files) != provider.MaxOutputFiles {
		t.Fatalf("收集 %d 个文件，期望 %d", len(files), provider.MaxOutputFiles)
	}
	if files[0].Path != "r/a.txt" || files[0].Size != int64(len("result-a")) {
		t.Fatalf("第一个输出 = %+v", files[0])
	}
	if b, err := io.ReadAll(files[0].File); err != nil || string(b) != "result-a" {
		t.Fatalf("r/a.txt 内容 = %q, %v", b, err)
	}
	for i, f := range files[1:] {
		if want := fmt.Sprintf("x/%03d", i); f.Path != want || f.Size != 0 {
			t.Fatalf("第 %d 个输出 = %+v，期望 %s", i+1, f, want)
		}
	}
	wantSkipped := []provider.SkippedOutput{{Path: "f", Reason: provider.SkipNotRegular}, {Path: "l", Reason: provider.SkipSymlink}}
	for i := 255; i < 300; i++ {
		wantSkipped = append(wantSkipped, provider.SkippedOutput{Path: fmt.Sprintf("x/%03d", i), Reason: provider.SkipTooMany})
	}
	if !reflect.DeepEqual(skipped, wantSkipped) {
		t.Fatalf("跳过 = %v，期望 %v", skipped, wantSkipped)
	}

	// 已停止的环境不完整：同 spec 的 Create 为 ErrIncomplete（不补完）；完整时的幂等见 TestExecEnvInDirIdempotent。
	if _, err := p.Create(ctx, spec); !errors.Is(err, provider.ErrIncomplete) {
		t.Fatalf("停止后再次 Create = %v，期望 ErrIncomplete", err)
	}
	// 输出文件打开期间 /out 的 tmpfs 无法卸载：调用方须先关闭全部输出再 Destroy（契约"执行中修订（Plan 15）"）。
	if err := p.Destroy(ctx, spec.EnvID); err == nil || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("输出文件仍打开时 Destroy = %v，期望 EBUSY", err)
	}
	closeAll()
	if err := p.Destroy(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	layersGone(t, p, spec.EnvID)
	r, err := p.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range r.Items {
		if it.EnvID == spec.EnvID {
			t.Fatalf("Destroy 后 Scan 仍报告 %+v", it)
		}
	}
}

// TestExecEnvInDirIdempotent：完整的 exec 环境上重复 Create（同 spec）返回同一 InDir；/out 是宿主上
// 属主为映射 uid 1000、0700 的 tmpfs（D1），Scan 把它报告为该环境的 mount 层。
func TestExecEnvInDirIdempotent(t *testing.T) {
	p, spec := newExecEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	info, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	again, err := p.Create(ctx, spec)
	if err != nil || again.InDir != info.InDir || info.InDir == "" {
		t.Fatalf("再次 Create = %+v, %v；原 InDir %q", again, err, info.InDir)
	}
	out := filepath.Join(p.envDir(spec.EnvID), "out")
	var st syscall.Stat_t
	if err := syscall.Stat(out, &st); err != nil {
		t.Fatal(err)
	}
	if id := spec.UIDBase + workloadID; st.Uid != id || st.Gid != id || st.Mode&0o7777 != 0o700 {
		t.Fatalf("/out 属主 %d:%d 权限 %#o，期望 %d、0700", st.Uid, st.Gid, st.Mode&0o7777, id)
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(out, &fs); err != nil || fs.Type != 0x01021994 { // TMPFS_MAGIC
		t.Fatalf("/out 不是 tmpfs：%#x, %v", fs.Type, err)
	}
	r, err := p.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range r.Items {
		found = found || (it.Layer == "mount" && it.Path == out && it.EnvID == spec.EnvID && it.Owner == provider.OwnedComplete)
	}
	if !found {
		t.Fatalf("Scan 未把 %s 报告为该环境的 mount 层：%+v", out, r.Items)
	}
	if err := p.Stop(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	layersGone(t, p, spec.EnvID)
}

const execQuotaScript = `
import errno, os
def code(e):
    return errno.errorcode.get(e.errno, str(e.errno))
try:
    with open('/out/big', 'wb') as f:
        for _ in range(64):
            f.write(b'\0' * 65536)
    print('size ok')
except OSError as e:
    print('size', code(e))
os.unlink('/out/big')
n = 0
try:
    while n < 4096:
        open('/out/i%d' % n, 'w').close()
        n += 1
    print('inodes ok')
except OSError as e:
    print('inodes', code(e), n > 900)
`

const execOOMScript = `
with open('/out/fill', 'wb') as f:
    for _ in range(32):
        f.write(b'\1' * (1 << 20))
print('filled', flush=True)
hog = b'\1' * (48 << 20)
print('survived', len(hog))
`

// TestExecEnvOutQuota（E38 provider 部分）：/out 写满 → ENOSPC；inode 耗尽 → ENOSPC；/out 的 tmpfs 页面
// 计入环境 cgroup，与进程匿名内存合计超过 memory.max → 环境内 OOM（OOMKillDelta ≥ 1），宿主进程不受影响，
// Stop + Destroy 成功。
func TestExecEnvOutQuota(t *testing.T) {
	p, spec := newExecEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	info, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	stageMain(t, info.InDir, execQuotaScript)
	stdout, stderr, es := runMain(ctx, t, p, spec.EnvID)
	if es.Code != 0 || stdout != "size ENOSPC\ninodes ENOSPC True\n" {
		t.Fatalf("workload = %+v，stdout %q，stderr %q", es, stdout, stderr)
	}
	if err := p.Stop(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	layersGone(t, p, spec.EnvID)

	oom := spec
	oom.EnvID, oom.UIDBase = "env-oom", 404096
	oom.Limits.MemoryMax = 64 << 20
	oom.Mounts.OutBytes = 48 << 20
	info, err = p.Create(ctx, oom)
	if err != nil {
		t.Fatal(err)
	}
	stageMain(t, info.InDir, execOOMScript)
	stdout, stderr, es = runMain(ctx, t, p, oom.EnvID)
	if es.Signal != syscall.SIGKILL || stdout != "filled\n" {
		t.Fatalf("workload = %+v，stdout %q，stderr %q；期望写入 /out 后被 OOM SIGKILL", es, stdout, stderr)
	}
	d, err := p.ResourceDiag(ctx, oom.EnvID)
	if err != nil || d.OOMKillDelta < 1 || !d.OOMObserved {
		t.Fatalf("ResourceDiag = %+v, %v；期望 OOMKillDelta ≥ 1", d, err)
	}
	if err := syscall.Kill(os.Getpid(), 0); err != nil { // 宿主（测试）进程不受影响
		t.Fatal(err)
	}
	if err := p.Stop(ctx, oom.EnvID); err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(ctx, oom.EnvID); err != nil {
		t.Fatal(err)
	}
	layersGone(t, p, oom.EnvID)
}

// ---------------------------------------------------------------------------
// M4 Plan 12 Task 5：会话环境的冻结、解冻、进程表与冷恢复暂存（规格 §12.2、§12.4；E31）。生产启动路径
// （真实 init + stage-2 helper）、默认模板；需要 root。

// runSh 在环境中以 /usr/bin/sh -c script 运行一次执行并返回 stdout（退出码须为 0）。
func runSh(ctx context.Context, t *testing.T, p *Provider, envID, script string) string {
	t.Helper()
	h, err := p.StartExec(ctx, envID, provider.ExecSpec{ExecID: "sh-" + randHex(3), Argv: []string{"/usr/bin/sh", "-c", script}})
	if err != nil {
		t.Fatalf("StartExec: %v", err)
	}
	if err := h.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	errDone := make(chan struct{})
	go func() { _, _ = io.Copy(&stderr, h.Stderr()); close(errDone) }()
	out, err := io.ReadAll(h.Stdout())
	if err != nil {
		t.Fatal(err)
	}
	<-errDone
	if es, err := h.Wait(); err != nil || es.Code != 0 || es.Signal != 0 {
		t.Fatalf("%q = %+v, %v；stderr %q", script, es, err, stderr.String())
	}
	return string(out)
}

// frozenState 读取环境 cgroup.events 的 frozen。
func frozenState(t *testing.T, p *Provider, envID string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(mustGroup(t, p, envID).Path(), "cgroup.events"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "frozen "); ok {
			return v
		}
	}
	t.Fatalf("cgroup.events 中没有 frozen：%q", b)
	return ""
}

// readCounter 读取 workload 每 100 ms 递增的计数文件；写入瞬间可能读到空内容，此时重读。
func readCounter(t *testing.T, path string) int {
	t.Helper()
	for i := 0; i < 50; i++ {
		b, err := os.ReadFile(path)
		if err == nil {
			var n int
			if _, err := fmt.Sscanf(string(b), "%d", &n); err == nil {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("读取计数文件 %s 失败", path)
	return 0
}

// waitCounterAbove 等待计数超过 n（解冻后恢复递增）。
func waitCounterAbove(t *testing.T, path string, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if readCounter(t, path) > n {
			return
		}
	}
	t.Fatalf("计数在 5 s 内未超过 %d：环境未恢复运行", n)
}

// TestSessionEnvFreezeThawRestore：session 环境（workspace + 冷恢复暂存目录）：
//   - RestoreDir 只读可见于 /run/agentbox/restore（写入失败），宿主删除暂存文件后沙箱内随即不可见；
//   - Procs 含 init 与 workload；
//   - Freeze 后 cgroup.events 为 frozen 1，环境内每 100 ms 递增的计数在冻结期间不变；Thaw 后 frozen 0 且恢复递增；
//   - E31：冻结确认失败（注入不存在的 cgroup 事件文件）→ ErrFreezeUnconfirmed，并已解冻（计数继续递增）；
//   - 冻结状态下 Stop 成功、populated 0；Destroy 后三层不存在，调用方的暂存目录保留（由调用方删除）。
func TestSessionEnvFreezeThawRestore(t *testing.T) {
	requireRoot(t)
	st, err := NewProcessStarter()
	if err != nil {
		t.Fatal(err)
	}
	st.initEnv = []string{envRealInit + "=1"}
	install := "s" + randHex(4)
	p := newProviderWith(t, install, st)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ws := filepath.Join(p.dataDir, "ws")
	restore := filepath.Join(p.dataDir, "restore")
	const sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, d := range []string{ws, restore} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(restore, sha), []byte("state-v1"), 0o444); err != nil {
		t.Fatal(err)
	}
	spec := testEnvSpec(install, "env-sess")
	spec.Kind = provider.KindSession
	spec.Mounts = provider.Mounts{Workspace: ws, RestoreDir: restore}
	if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
		t.Fatalf("Create = %+v, %v", info, err)
	}

	// 冷恢复暂存：只读可见；写入失败且宿主上没有出现文件。
	got := runSh(ctx, t, p, spec.EnvID, "cat /run/agentbox/restore/"+sha+"; echo; "+
		"if (echo x > /run/agentbox/restore/w) 2>/dev/null; then echo writable; else echo readonly; fi")
	if got != "state-v1\nreadonly\n" {
		t.Fatalf("恢复暂存读取 = %q，期望 state-v1 且只读", got)
	}
	if _, err := os.Lstat(filepath.Join(restore, "w")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("沙箱写入了恢复暂存目录（%v）", err)
	}
	// incarnation 进入 idle 后宿主删除暂存文件：沙箱内随即不可见。
	if err := os.Remove(filepath.Join(restore, sha)); err != nil {
		t.Fatal(err)
	}
	if got := runSh(ctx, t, p, spec.EnvID, "if [ -e /run/agentbox/restore/"+sha+" ]; then echo present; else echo absent; fi"); got != "absent\n" {
		t.Fatalf("删除暂存文件后沙箱内 = %q，期望 absent", got)
	}

	// 计数 workload（后台运行）。
	h, err := p.StartExec(ctx, spec.EnvID, provider.ExecSpec{ExecID: "counter", Argv: []string{"/usr/bin/sh", "-c",
		"i=0; while :; do i=$((i+1)); echo $i > /workspace/counter; sleep 0.1; done"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, h.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
	counter := filepath.Join(ws, "counter")
	waitCounterAbove(t, counter, 2)

	pids, err := p.Procs(ctx, spec.EnvID)
	if err != nil {
		t.Fatal(err)
	}
	initPid := p.state(spec.EnvID).pid
	hasInit := false
	for _, pid := range pids {
		hasInit = hasInit || pid == initPid
	}
	if !hasInit || len(pids) < 2 {
		t.Fatalf("Procs = %v，期望含 init %d 与 workload", pids, initPid)
	}

	// Freeze：frozen 1 后计数不变；Thaw：frozen 0 后恢复递增。
	if err := p.Freeze(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	if s := frozenState(t, p, spec.EnvID); s != "1" {
		t.Fatalf("Freeze 后 frozen = %s", s)
	}
	before := readCounter(t, counter)
	time.Sleep(600 * time.Millisecond)
	if after := readCounter(t, counter); after != before {
		t.Fatalf("冻结期间计数由 %d 变为 %d", before, after)
	}
	if err := p.Thaw(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	if s := frozenState(t, p, spec.EnvID); s != "0" {
		t.Fatalf("Thaw 后 frozen = %s", s)
	}
	waitCounterAbove(t, counter, before)

	// E31：无法确认冻结 → ErrFreezeUnconfirmed，provider 已尝试解冻（计数继续递增）。
	p.eventsFile = "cgroup.nonexistent"
	short, cancelShort := context.WithTimeout(ctx, time.Second)
	err = p.Freeze(short, spec.EnvID)
	cancelShort()
	p.eventsFile = ""
	if !errors.Is(err, provider.ErrFreezeUnconfirmed) {
		t.Fatalf("冻结确认失败时 Freeze = %v，期望 ErrFreezeUnconfirmed", err)
	}
	if s := frozenState(t, p, spec.EnvID); s != "0" {
		t.Fatalf("冻结失败后 frozen = %s，期望已解冻", s)
	}
	waitCounterAbove(t, counter, readCounter(t, counter))

	// 冻结状态下 Stop：cgroup.kill 杀死冻结中的进程，populated 0。
	if err := p.Freeze(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(ctx, spec.EnvID); err != nil {
		t.Fatalf("冻结状态下 Stop: %v", err)
	}
	if populated, err := mustGroup(t, p, spec.EnvID).Populated(); err != nil || populated {
		t.Fatalf("Stop 后 populated = %v, %v", populated, err)
	}
	if es, err := h.Wait(); err == nil && es.Signal == 0 && es.Code == 0 {
		t.Fatalf("计数 workload 在 Stop 后正常退出（%+v），期望被杀死或连接断开", es)
	}
	if err := p.Destroy(ctx, spec.EnvID); err != nil {
		t.Fatal(err)
	}
	layersGone(t, p, spec.EnvID)
	if _, err := os.Stat(restore); err != nil {
		t.Fatalf("Destroy 不应删除调用方的暂存目录：%v", err)
	}
	if _, err := p.Procs(ctx, spec.EnvID); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("Destroy 后 Procs = %v，期望 ErrNotFound", err)
	}
	if err := p.Freeze(ctx, spec.EnvID); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("Destroy 后 Freeze = %v，期望 ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// Plan 15 Task 5：UID 范围文件回收与核查（E39、I11；计划 D13）。

// uidFileSet 以集合比较 UIDFiles 的结果（遍历顺序是目录项顺序，不排序）。
func uidFileSet(t *testing.T, p *Provider, base, size uint32, limit int) map[string]bool {
	t.Helper()
	paths, err := p.UIDFiles(context.Background(), base, size, limit)
	if err != nil {
		t.Fatalf("UIDFiles: %v", err)
	}
	out := make(map[string]bool, len(paths))
	for _, x := range paths {
		out[x] = true
	}
	return out
}

func sameSet(got map[string]bool, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !got[w] {
			return false
		}
	}
	return true
}

// TestUIDFilesWalk（不需要 root）：以本进程的 uid 为"范围"，UIDFiles 报告数据目录中属主落在范围内的全部条目
// （符号链接报告链接本身、不跟随到数据目录之外；blobs 整体跳过），并遵守 limit。
func TestUIDFilesWalk(t *testing.T) {
	data := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("o"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{DataDir: data, CgroupRoot: "/sys/fs/cgroup", InstallID: "walk", Starter: testStarter{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"workspaces/t1/d", "blobs/sha256", "stray"} {
		if err := os.MkdirAll(filepath.Join(p.dataDir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"workspaces/t1/d/f", "blobs/sha256/x", "stray/g"} {
		if err := os.WriteFile(filepath.Join(p.dataDir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(p.dataDir, "workspaces/t1/ln")); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Getuid())
	var want []string
	for _, rel := range []string{"envs", "workspaces", "workspaces/t1", "workspaces/t1/d", "workspaces/t1/d/f", "workspaces/t1/ln", "stray", "stray/g"} {
		want = append(want, filepath.Join(p.dataDir, rel))
	}
	if got := uidFileSet(t, p, uid, 1, 100); !sameSet(got, want...) {
		t.Fatalf("UIDFiles = %v\n期望 %v", got, want)
	}
	if got := uidFileSet(t, p, uid, 1, 3); len(got) != 3 {
		t.Fatalf("limit 3 时 UIDFiles 返回 %d 个", len(got))
	}
	if got := uidFileSet(t, p, uid+1, 4096, 100); len(got) != 0 {
		t.Fatalf("范围外的 UIDFiles = %v，期望为空", got)
	}
	if _, err := p.UIDFiles(context.Background(), uid, 1, 0); err == nil {
		t.Fatal("limit 0 应被拒绝")
	}
}

// TestE39UIDRangeReuse（root，E39）：范围 R 的 task 环境在 workspace 与 /tmp 写文件 → 清理（Destroy）后 workspace
// 文件回到 0:0、/tmp 随 tmpfs 消失，UIDFiles(R) 为空；范围 R 分配给新环境 B，B 的 uid 1000（与旧环境同一宿主 uid）
// 在其可见视图中找不到任何旧文件。另一轮：在数据目录其他位置植入属主 R+1000 的文件 → UIDFiles 报告该路径
// （resource 层据此隔离，见 resource_test）；blobs 中的与挂载点之内的不报告（挂载点本身报告），ReclaimUIDFiles
// 不改动 workspaces 之外的文件。
func TestE39UIDRangeReuse(t *testing.T) {
	requireRoot(t)
	st, err := NewProcessStarter()
	if err != nil {
		t.Fatal(err)
	}
	st.initEnv = []string{envRealInit + "=1"}
	install := "u" + randHex(4)
	p := newProviderWith(t, install, st)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	const base, size = 600000, 4096
	const hostUID = base + workloadID

	wsParent := filepath.Join(p.dataDir, "workspaces")
	if err := os.Mkdir(wsParent, 0o711); err != nil {
		t.Fatal(err)
	}
	newTask := func(envID, task string) string {
		t.Helper()
		ws := filepath.Join(wsParent, task)
		if err := os.Mkdir(ws, 0o700); err != nil {
			t.Fatal(err)
		}
		spec := testEnvSpec(install, envID)
		spec.UIDBase, spec.UIDSize = base, size
		spec.Mounts = provider.Mounts{Workspace: ws}
		if info, err := p.Create(ctx, spec); err != nil || !info.Complete {
			t.Fatalf("Create %s = %+v, %v", envID, info, err)
		}
		return ws
	}
	teardown := func(envID string) {
		t.Helper()
		if err := p.Stop(ctx, envID); err != nil {
			t.Fatal(err)
		}
		if err := p.Destroy(ctx, envID); err != nil {
			t.Fatal(err)
		}
		layersGone(t, p, envID)
	}

	// 环境 A：workspace 与 /tmp 中留下文件。
	wsA := newTask("env-a", "t-a")
	if got := runSh(ctx, t, p, "env-a", "id -u; echo s > /workspace/a.txt; mkdir /workspace/d; echo s > /workspace/d/b; "+
		"ln -s /etc/passwd /workspace/ln; echo s > /tmp/secret; ls /tmp"); got != "1000\nsecret\n" {
		t.Fatalf("环境 A 输出 %q", got)
	}
	teardown("env-a")
	oldFiles := []string{wsA, filepath.Join(wsA, "a.txt"), filepath.Join(wsA, "d"), filepath.Join(wsA, "d", "b"), filepath.Join(wsA, "ln")}
	if got := uidFileSet(t, p, base, size, 16); !sameSet(got, oldFiles...) {
		t.Fatalf("回收前 UIDFiles = %v，期望恰为 A 的 workspace（/tmp 随 tmpfs 消失）%v", got, oldFiles)
	}
	n, err := p.ReclaimUIDFiles(ctx, base, size)
	if err != nil || n != len(oldFiles) {
		t.Fatalf("ReclaimUIDFiles = %d, %v，期望 %d", n, err, len(oldFiles))
	}
	for _, f := range oldFiles {
		var s syscall.Stat_t
		if err := syscall.Lstat(f, &s); err != nil || s.Uid != 0 || s.Gid != 0 {
			t.Fatalf("%s 回收后属主 %d:%d（%v），期望 0:0", f, s.Uid, s.Gid, err)
		}
	}
	var target syscall.Stat_t
	if err := syscall.Stat("/etc/passwd", &target); err != nil || target.Uid != 0 {
		t.Fatalf("符号链接目标的属主被改动（%d，%v）", target.Uid, err)
	}
	if got := uidFileSet(t, p, base, size, 16); len(got) != 0 {
		t.Fatalf("回收后 UIDFiles = %v，期望为空", got)
	}

	// 环境 B 复用范围 R：同一宿主 uid，看不到 A 的任何文件。
	// find 跳过模板项（宿主 /usr、/lib* 等的只读 bind，环境写不进去）：CI runner 的 /usr 有数百万个文件，
	// 遍历它会耗尽 ctx，使随后的 Stop 拿到已过期的 ctx 而报 ErrStopUnconfirmed。
	prune := "-path /proc -o -path /sys"
	for _, tp := range rootfs.DefaultTemplate().Paths {
		prune += " -o -path " + tp
	}
	newTask("env-b", "t-b")
	got := runSh(ctx, t, p, "env-b", "id -u; for f in /tmp/secret /workspace/a.txt /workspace/d /workspace/ln; do "+
		"if [ -e \"$f\" ] || [ -L \"$f\" ]; then echo present \"$f\"; fi; done; "+
		"find / \\( "+prune+" \\) -prune -o -user 1000 -print 2>/dev/null; true")
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if lines[0] != "1000" {
		t.Fatalf("环境 B 的 uid = %q", lines[0])
	}
	for _, l := range lines[1:] {
		if strings.HasPrefix(l, "present") || strings.Contains(l, "a.txt") || strings.Contains(l, "secret") || strings.HasPrefix(l, "/workspace/") {
			t.Fatalf("环境 B 看到了旧文件：%q（全部输出 %q）", l, got)
		}
	}
	teardown("env-b")
	if _, err := p.ReclaimUIDFiles(ctx, base, size); err != nil {
		t.Fatal(err)
	}

	// 植入残留：数据目录其他位置、blobs 中、另一文件系统的挂载点（挂载点本身属主 R+1000，其中的文件不遍历）。
	stray := filepath.Join(p.dataDir, "stray", "x")
	blobFile := filepath.Join(p.dataDir, blobsDirName, "y")
	mnt := filepath.Join(p.dataDir, "mnt")
	for _, d := range []string{filepath.Dir(stray), filepath.Dir(blobFile), mnt} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mount("tmpfs", mnt, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, fmt.Sprintf("size=1m,uid=%d,gid=%d", hostUID, hostUID)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Unmount(mnt, syscall.MNT_DETACH) }()
	for _, f := range []string{stray, blobFile, filepath.Join(mnt, "inner")} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Lchown(f, hostUID, hostUID); err != nil {
			t.Fatal(err)
		}
	}
	if got := uidFileSet(t, p, base, size, 16); !sameSet(got, stray, mnt) {
		t.Fatalf("植入残留后 UIDFiles = %v，期望 %s 与挂载点 %s", got, stray, mnt)
	}
	if n, err := p.ReclaimUIDFiles(ctx, base, size); err != nil || n != 0 {
		t.Fatalf("ReclaimUIDFiles = %d, %v：workspaces 之外不应改动", n, err)
	}
	var s syscall.Stat_t
	if err := syscall.Lstat(stray, &s); err != nil || s.Uid != hostUID {
		t.Fatalf("%s 属主 %d（%v），期望不变", stray, s.Uid, err)
	}
	if got := uidFileSet(t, p, base+size, size, 16); len(got) != 0 {
		t.Fatalf("相邻范围的 UIDFiles = %v，期望为空", got)
	}
}
