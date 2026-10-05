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
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/providertest"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
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
	})
}

// TestLaunchSpecForFillsInit：生产启动器把 EnvSpec 的 Limits/Mounts/Template 填入 LaunchSpec.Init（规格 §4.5）；
// 规格未给出时取默认值（nofile 1024、/tmp 64 MiB、默认模板）；未知模板名报错。不需要 root。
func TestLaunchSpecForFillsInit(t *testing.T) {
	ls, err := launchSpecFor(provider.EnvSpec{
		EnvID: "e1", Kind: provider.KindExec, UIDBase: 100000, UIDSize: 4096,
		Limits: provider.Limits{NoFile: 256, FSize: 1 << 20, TmpBytes: 8 << 20},
		Mounts: provider.Mounts{In: "/srv/in", OutBytes: 4 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := ls.Init
	if ls.GIDBase != 100000 || ls.GIDSize != 4096 || ls.Hostname != envHostname {
		t.Fatalf("LaunchSpec = %+v", ls)
	}
	if in.Kind != "exec" || in.NoFile != 256 || in.FSize != 1<<20 || in.TmpBytes != 8<<20 || in.In != "/srv/in" || in.OutBytes != 4<<20 {
		t.Fatalf("Init = %+v", in)
	}
	if len(in.Template.Paths) == 0 {
		t.Fatal("默认模板未解析")
	}

	ls, err = launchSpecFor(provider.EnvSpec{Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096,
		Mounts: provider.Mounts{Workspace: "/var/ws", GatewaySocket: "/run/gw.sock"}})
	if err != nil {
		t.Fatal(err)
	}
	if ls.Init.NoFile != defaultNoFile || ls.Init.TmpBytes != defaultTmpBytes || ls.Init.Workspace != "/var/ws" || ls.Init.GatewaySocket != "/run/gw.sock" {
		t.Fatalf("默认值未填充：%+v", ls.Init)
	}
	if _, err := launchSpecFor(provider.EnvSpec{Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096, Template: "no-such"}); err == nil {
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
