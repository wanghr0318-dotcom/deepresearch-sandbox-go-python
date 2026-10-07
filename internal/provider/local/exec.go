//go:build linux

// Package local 是 provider.Provider 的 Linux 实现：环境由 cgroup、rootfs 与沙箱 init 组成，
// 宿主经控制通道（internal/sandbox.Conn）在环境内启动执行。
package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/sandbox"
)

// 本文件实现宿主侧的 StartExec 与 ExecHandle（规格 §4.1–§4.4），以及每个环境的执行闸门
// （Provider 契约第 5 节）。

// gate 是一个环境的执行闸门：open → closed，不可逆。StartExec 在锁内检查 open 并登记一个在途
// 启动，在锁外发送 start；Stop 在锁内关闭闸门，之后到达的 StartExec 立即得到 ErrStopping。
// close 不等待在途启动：它们由 cgroup.kill 终止，并以 ErrControlLost 或 *StartError 返回。
type gate struct {
	mu       sync.Mutex
	closed   bool
	inflight int
}

// enter 登记一个在途启动；闸门已关闭时返回 provider.ErrStopping，不登记。
func (g *gate) enter() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return provider.ErrStopping
	}
	g.inflight++
	return nil
}

// leave 注销一个由 enter 登记的在途启动。
func (g *gate) leave() {
	g.mu.Lock()
	g.inflight--
	g.mu.Unlock()
}

// close 关闭闸门。不可逆，可重复调用，不等待在途启动。
func (g *gate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

// execState 是一次执行在宿主侧的状态；字段由 execClient.mu 保护。
type execState struct {
	resolved chan struct{} // 收到 start_ack 或 start_err 时关闭
	acked    bool
	pid      int
	startErr *provider.StartError

	exited chan struct{} // 收到 exit 时关闭；可先于 resolved（§4.3 的防御性暂存）
	exit   provider.ExitStatus
}

// execClient 在一条控制连接上复用多次执行：一个读 goroutine 按 exec_id 分发 init 的回复。
// 读循环结束（对端关闭、读错误或 init 违反协议）后，连接视为断开：等待 ACK 的启动得到
// ErrControlLost，等待退出的 Wait 得到控制连接错误。
type execClient struct {
	conn *sandbox.Conn

	// beforeSend 是测试钩子：在登记在途启动之后、创建管道与发送 start 之前调用。
	beforeSend func(execID string)

	mu    sync.Mutex
	execs map[string]*execState // 已发送 start、尚未同时收到 ACK 与 exit 的执行
	rerr  error                 // 读循环结束的原因；done 关闭后只读
	done  chan struct{}         // 读循环结束时关闭
}

// newExecClient 接管 conn 并启动读 goroutine。
func newExecClient(conn *sandbox.Conn) *execClient {
	c := &execClient{
		conn:  conn,
		execs: make(map[string]*execState),
		done:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// close 关闭控制连接并等待读 goroutine 退出。
func (c *execClient) close() error {
	err := c.conn.Close()
	<-c.done
	return err
}

func (c *execClient) readLoop() {
	var err error
	for err == nil {
		var (
			m     sandbox.Message
			files []*os.File
		)
		m, files, err = c.conn.Recv()
		if err != nil {
			break
		}
		for _, f := range files { // 只有 start 带 FD，而 init 不应发送 start
			f.Close()
		}
		err = c.dispatch(m)
	}
	if errors.Is(err, io.EOF) {
		err = errors.New("local: 控制连接被对端关闭")
	}
	c.mu.Lock()
	c.rerr = err
	c.mu.Unlock()
	close(c.done)
	// 协议错误时连接仍打开；init 不可信，关闭它（之后 init 读到 EOF 退出）。
	_ = c.conn.Close()
}

// dispatch 处理 init 的一条回复。未知 exec_id 的回复（例如 ctx 结束后迟到的 ACK）丢弃；
// init 不应发送的消息类型视为协议错误。
func (c *execClient) dispatch(m sandbox.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.execs[m.ExecID]
	switch m.Type {
	case sandbox.MsgStartAck:
		if st == nil || isClosed(st.resolved) {
			return nil
		}
		st.acked, st.pid = true, m.PID
		close(st.resolved)
	case sandbox.MsgStartErr:
		if st == nil || isClosed(st.resolved) {
			return nil
		}
		st.startErr = &provider.StartError{Reason: m.Reason}
		close(st.resolved)
	case sandbox.MsgExit:
		if st == nil || isClosed(st.exited) {
			return nil
		}
		st.exit = provider.ExitStatus{Code: m.Exit.Code, Signal: m.Exit.Signal}
		close(st.exited)
	default:
		return fmt.Errorf("%w: init 发送了 %s", sandbox.ErrProtocol, m.Type)
	}
	if st.startErr != nil || (st.acked && isClosed(st.exited)) {
		delete(c.execs, m.ExecID)
	}
	return nil
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (c *execClient) forget(execID string) {
	c.mu.Lock()
	delete(c.execs, execID)
	c.mu.Unlock()
}

// start 经控制连接启动一次执行（契约第 3 节 StartExec 行、第 5 节）。不幂等：结果不明
// （ErrControlLost、ctx 错误）时调用方不得重试，应停止并拆除环境。
//
//   - 闸门已关闭 → provider.ErrStopping，不发送任何消息。
//   - 控制连接在发送前已断开 → provider.ErrNotFound（没有可用的控制连接）。
//   - start_err → *provider.StartError（workload 未运行）。
//   - ACK 前连接断开或 start 发送失败 → provider.ErrControlLost。
//   - ACK 前 ctx 结束 → ctx 的错误。
func (c *execClient) start(ctx context.Context, g *gate, spec provider.ExecSpec) (provider.ExecHandle, error) {
	if spec.ExecID == "" {
		return nil, errors.New("local: ExecSpec 缺少 exec_id")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.enter(); err != nil {
		return nil, err
	}
	defer g.leave()

	if c.beforeSend != nil {
		c.beforeSend(spec.ExecID)
	}

	raw, err := json.Marshal(sandbox.StartSpec{Argv: spec.Argv, Env: spec.Env, Dir: spec.Dir})
	if err != nil {
		return nil, fmt.Errorf("local: 编码 spec: %w", err)
	}

	st := &execState{resolved: make(chan struct{}), exited: make(chan struct{})}
	c.mu.Lock()
	switch {
	case isClosed(c.done):
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: 控制连接已断开: %v", provider.ErrNotFound, c.rerr)
	case c.execs[spec.ExecID] != nil:
		c.mu.Unlock()
		return nil, fmt.Errorf("local: exec_id %q 已在执行", spec.ExecID)
	}
	c.execs[spec.ExecID] = st
	c.mu.Unlock()

	h, err := c.send(spec.ExecID, raw, st)
	if err != nil {
		c.forget(spec.ExecID)
		return nil, err
	}

	select {
	case <-st.resolved:
	case <-c.done:
		// 读循环可能在退出前刚好分发了回复。
		if !isClosed(st.resolved) {
			h.closeParents()
			c.forget(spec.ExecID)
			return nil, provider.ErrControlLost
		}
	case <-ctx.Done():
		if !isClosed(st.resolved) {
			h.closeParents()
			c.forget(spec.ExecID)
			return nil, ctx.Err()
		}
	}

	c.mu.Lock()
	startErr := st.startErr
	c.mu.Unlock()
	if startErr != nil {
		h.closeParents()
		return nil, startErr
	}
	return h, nil
}

// send 创建三个管道并在一次 sendmsg 中发送 start 与子端；无论成败都关闭子端副本，失败时
// 同时关闭父端（规格 §4.2）。
func (c *execClient) send(execID string, raw json.RawMessage, st *execState) (*execHandle, error) {
	var parents, children []*os.File
	closeAll := func(fs []*os.File) {
		for _, f := range fs {
			f.Close()
		}
	}
	for i := 0; i < 3; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			closeAll(parents)
			closeAll(children)
			return nil, fmt.Errorf("local: 创建管道: %w", err)
		}
		if i == 0 { // stdin：子进程读，宿主写
			parents, children = append(parents, w), append(children, r)
		} else { // stdout、stderr：子进程写，宿主读
			parents, children = append(parents, r), append(children, w)
		}
	}

	err := c.conn.Send(sandbox.Message{Type: sandbox.MsgStart, ExecID: execID, Spec: raw}, children)
	closeAll(children)
	if err != nil {
		closeAll(parents)
		if errors.Is(err, sandbox.ErrProtocol) { // 发送端拒绝，什么都没发出
			return nil, fmt.Errorf("local: start 不合协议: %w", err)
		}
		return nil, fmt.Errorf("%w: 发送 start: %v", provider.ErrControlLost, err)
	}
	return &execHandle{c: c, execID: execID, st: st, stdin: parents[0], stdout: parents[1], stderr: parents[2]}, nil
}

// execHandle 实现 provider.ExecHandle。
type execHandle struct {
	c      *execClient
	execID string
	st     *execState

	stdin  *os.File
	stdout *os.File
	stderr *os.File
}

var _ provider.ExecHandle = (*execHandle)(nil)

func (h *execHandle) Stdin() io.WriteCloser { return h.stdin }
func (h *execHandle) Stdout() io.ReadCloser { return h.stdout }
func (h *execHandle) Stderr() io.ReadCloser { return h.stderr }

func (h *execHandle) closeParents() {
	h.stdin.Close()
	h.stdout.Close()
	h.stderr.Close()
}

// Wait 返回 exit 中的退出状态；退出前控制连接断开时返回控制连接错误（退出状态未知）。
// 可重复调用。
func (h *execHandle) Wait() (provider.ExitStatus, error) {
	select {
	case <-h.st.exited:
	case <-h.c.done:
		if !isClosed(h.st.exited) {
			h.c.forget(h.execID)
			return provider.ExitStatus{}, fmt.Errorf("%w: 退出前控制连接断开，退出状态未知（%v）", provider.ErrControlLost, h.c.rerr)
		}
	}
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	return h.st.exit, nil
}

// Terminate 请求 init 终止该执行：向进程组发 SIGTERM，grace 到期后 SIGKILL（规格 §4.4）。
// 退出状态仍经 Wait 获得。
func (h *execHandle) Terminate(grace time.Duration) error {
	if grace < 0 {
		grace = 0
	}
	err := h.c.conn.Send(sandbox.Message{Type: sandbox.MsgTerminate, ExecID: h.execID, GraceMS: grace.Milliseconds()}, nil)
	if err != nil {
		return fmt.Errorf("local: 发送 terminate: %w", err)
	}
	return nil
}
