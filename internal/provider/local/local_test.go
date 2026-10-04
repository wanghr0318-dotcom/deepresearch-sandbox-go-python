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
		files[1].WriteString("out")
		files[2].WriteString("err")
		closeFiles(files)
		send(t, fake, sandbox.Message{Type: sandbox.MsgExit, ExecID: "e1", Exit: &sandbox.ExitInfo{Code: 3}})
		initDone <- string(in)
	}()

	h, err := c.start(context.Background(), &gate{}, testSpec)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	h.Stdin().Write([]byte("in"))
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
	case os.Getenv(envTestInit) != "":
		os.Exit(runTestInit())
	case os.Getenv(envTestHog) != "":
		os.Exit(runHog())
	}
	code := m.Run()
	removeTestCgroupRoot()
	os.Exit(code)
}

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
	go cmd.Wait() // 收割 init
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
	requireRoot(t)
	root := testCgroupRoot(t)
	data, err := os.MkdirTemp("", "agentbox-data-")
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{DataDir: data, CgroupRoot: root, InstallID: installID, Starter: testStarter{}})
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
		UIDBase: 100000, UIDSize: 4096, Template: "test",
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
	go io.Copy(io.Discard, h.Stderr())
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
	go io.Copy(io.Discard, h.Stdout())
	go io.Copy(io.Discard, h.Stderr())
	if st, err := h.Wait(); err != nil || st.Signal != syscall.SIGKILL {
		t.Fatalf("Wait = %+v, %v；期望被 SIGKILL（OOM）", st, err)
	}
	d, err := p.ResourceDiag(ctx, "env-m")
	if err != nil || d.OOMKillDelta == 0 || !d.OOMObserved {
		t.Fatalf("ResourceDiag = %+v, %v；期望 OOMKillDelta > 0", d, err)
	}
}
