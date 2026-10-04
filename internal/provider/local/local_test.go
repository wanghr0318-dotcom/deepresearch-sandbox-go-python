//go:build linux

package local

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
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
