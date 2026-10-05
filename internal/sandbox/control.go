//go:build linux

package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"syscall"
)

// 本文件实现宿主与沙箱 init 之间的控制通道（规格 §4.2）：
//
//   - 底层是 SOCK_SEQPACKET 的 socketpair，一条消息恰好一帧，帧 ≤ 64 KiB，
//     start 的 spec ≤ 32 KiB；进程数据不经控制通道。
//   - start 与三个管道 FD 在同一 sendmsg 中传递；接收用
//     recvmsg(MSG_CMSG_CLOEXEC) 并检查 MSG_TRUNC 与 MSG_CTRUNC。
//   - start 必须恰好 3 个 FD，其他消息 0 个；任何违规关闭全部已收 FD，
//     返回 ErrProtocol。
//   - 每端一个写 goroutine 经有界队列串行写出；入队不阻塞，队列满返回
//     ErrQueueFull（调用方视为控制通道故障）。

// 消息类型（规格 §4.2–§4.4）。
const (
	MsgStart     = "start"
	MsgStartAck  = "start_ack"
	MsgStartErr  = "start_err"
	MsgExit      = "exit"
	MsgTerminate = "terminate"
)

const (
	// maxFrameSize 是一帧的上限（规格 §4.2）。
	maxFrameSize = 64 * 1024
	// maxSpecSize 是 start 中 spec 的上限（规格 §4.2）。
	maxSpecSize = 32 * 1024
	// startFDs 是 start 必须携带的 FD 数：stdin、stdout、stderr 三个管道端。
	startFDs = 3
)

var (
	// ErrProtocol 表示违反控制通道协议：发送端在发出前拒绝，接收端在
	// 关闭全部已收 FD 后返回。
	ErrProtocol = errors.New("sandbox: 控制通道协议错误")
	// ErrQueueFull 表示写队列已满；调用方应视为控制通道故障。
	ErrQueueFull = errors.New("sandbox: 控制通道写队列已满")
	// errConnClosed 是本端 Close 之后 Send 返回的错误。
	errConnClosed = fmt.Errorf("sandbox: 控制通道已关闭: %w", os.ErrClosed)
)

// ExitInfo 是 exit 消息中的退出状态，对应规格 §4.1 的 ExitStatus：
// 正常退出时 Code 为退出码、Signal 为 0；被信号杀死时 Signal 为该信号。
type ExitInfo struct {
	Code   int            `json:"code"`
	Signal syscall.Signal `json:"signal,omitempty"`
}

// StartSpec 是 start 消息中 spec 的编码：宿主侧（provider/local）编码、init 侧的 Launcher 解码，
// 两侧共用这一个定义。exec_id 在消息头中，不重复编码。
type StartSpec struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env,omitempty"`
	Dir  string   `json:"dir,omitempty"`
}

// Message 是控制通道上的一条消息。各类型使用的字段：
//
//	start      ExecID, Spec（随帧传递 3 个 FD）
//	start_ack  ExecID, PID
//	start_err  ExecID, Reason
//	exit       ExecID, Exit
//	terminate  ExecID
type Message struct {
	Type    string          `json:"type"`
	ExecID  string          `json:"exec_id"`
	Spec    json.RawMessage `json:"spec,omitempty"`
	PID     int             `json:"pid,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Exit    *ExitInfo       `json:"exit,omitempty"`
	GraceMS int64           `json:"grace_ms,omitempty"` // terminate：SIGTERM 之后到 SIGKILL 的宽限（规格 §4.4）
}

// validateMessage 检查一条消息在给定 FD 数下是否合法；发送端与接收端共用。
func validateMessage(m Message, nfds int) error {
	switch m.Type {
	case MsgStart, MsgStartAck, MsgStartErr, MsgExit, MsgTerminate:
	default:
		return fmt.Errorf("%w: 未知消息类型 %q", ErrProtocol, m.Type)
	}
	if m.ExecID == "" {
		return fmt.Errorf("%w: %s 缺少 exec_id", ErrProtocol, m.Type)
	}
	if m.Type == MsgStart {
		if nfds != startFDs {
			return fmt.Errorf("%w: start 携带 %d 个 FD，必须恰好 %d 个", ErrProtocol, nfds, startFDs)
		}
		if len(m.Spec) == 0 {
			return fmt.Errorf("%w: start 缺少 spec", ErrProtocol)
		}
		if len(m.Spec) > maxSpecSize {
			return fmt.Errorf("%w: spec 为 %d 字节，超过 %d", ErrProtocol, len(m.Spec), maxSpecSize)
		}
		return nil
	}
	if nfds != 0 {
		return fmt.Errorf("%w: %s 携带 %d 个 FD，只有 start 可以携带 FD", ErrProtocol, m.Type, nfds)
	}
	if len(m.Spec) != 0 {
		return fmt.Errorf("%w: %s 不得携带 spec", ErrProtocol, m.Type)
	}
	if m.Type == MsgExit && m.Exit == nil {
		return fmt.Errorf("%w: exit 缺少退出状态", ErrProtocol)
	}
	return nil
}

// outItem 是写队列中的一项。result 非 nil 时（带 FD 的 start），写 goroutine
// 在 sendmsg 返回后把结果送回，Send 等待它。
type outItem struct {
	frame  []byte
	files  []*os.File
	result chan error
}

// Conn 是控制通道的一端：一个写 goroutine 经有界队列串行写出。
//
// 并发约定：Send 可被多个 goroutine 并发调用；Recv 同一时刻只能有一个调用者；
// Close 可与二者并发，并让阻塞中的 Recv 与写出返回。
type Conn struct {
	f     *os.File
	rc    syscall.RawConn
	queue chan outItem
	done  chan struct{}
	wg    sync.WaitGroup

	mu     sync.Mutex
	closed bool
	werr   error // 第一次写出失败的错误；之后所有 Send 都返回它

	closeOnce sync.Once
	closeErr  error

	rbuf []byte
	oob  []byte
}

// NewConn 接管 f（一个 SOCK_SEQPACKET socket）并启动写 goroutine。
// queueLen 是写队列容量（至少为 1）。f 可以是阻塞或非阻塞的。
func NewConn(f *os.File, queueLen int) *Conn {
	if queueLen < 1 {
		queueLen = 1
	}
	c := &Conn{
		f:     f,
		queue: make(chan outItem, queueLen),
		done:  make(chan struct{}),
		rbuf:  make([]byte, maxFrameSize),
		oob:   make([]byte, syscall.CmsgSpace(startFDs*4)),
	}
	rc, err := f.SyscallConn()
	if err != nil {
		// *os.File 的 SyscallConn 只在文件已关闭时失败；记为写错误，
		// Send 与 Recv 都会据此失败。
		c.werr = fmt.Errorf("sandbox: 控制通道: %w", err)
	}
	c.rc = rc
	c.wg.Add(1)
	go c.writeLoop()
	return c
}

// Send 校验 m 并把它非阻塞地放入写队列。
//
//   - 违反协议（未知类型、缺字段、spec > 32 KiB、帧 > 64 KiB、start 不是恰好
//     3 个 FD、非 start 带 FD）→ ErrProtocol，什么都不发送。
//   - 队列满 → ErrQueueFull，调用方视为控制通道故障。
//   - 不带 FD 的消息入队即返回 nil，写出失败体现在之后的 Send 上（以及对端
//     连接断开）。
//   - 带 FD 的 start 等到 sendmsg 返回后才返回，结果即 sendmsg 的结果。
//
// Send 从不关闭 files。调用方在 Send 返回后总是关闭子端副本（files），
// Send 返回错误时还要关闭自己持有的父端（规格 §4.2）。
func (c *Conn) Send(m Message, files []*os.File) error {
	for _, f := range files {
		if f == nil {
			return fmt.Errorf("%w: files 中有 nil", ErrProtocol)
		}
	}
	if err := validateMessage(m, len(files)); err != nil {
		return err
	}
	frame, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("%w: 编码 %s: %v", ErrProtocol, m.Type, err)
	}
	if len(frame) > maxFrameSize {
		return fmt.Errorf("%w: 帧为 %d 字节，超过 %d", ErrProtocol, len(frame), maxFrameSize)
	}
	it := outItem{frame: frame, files: files}
	if len(files) > 0 {
		it.result = make(chan error, 1)
	}

	c.mu.Lock()
	switch {
	case c.closed:
		c.mu.Unlock()
		return errConnClosed
	case c.werr != nil:
		err := c.werr
		c.mu.Unlock()
		return err
	}
	select {
	case c.queue <- it:
	default:
		c.mu.Unlock()
		return ErrQueueFull
	}
	c.mu.Unlock()

	if it.result == nil {
		return nil
	}
	return <-it.result
}

// writeLoop 是本端唯一的写者。Close 之后，队列中剩余的项以 errConnClosed
// 答复（不带 FD 的消息直接丢弃）。
func (c *Conn) writeLoop() {
	defer c.wg.Done()
	for {
		select {
		case it := <-c.queue:
			c.write(it)
		case <-c.done:
			for {
				select {
				case it := <-c.queue:
					reply(it, errConnClosed)
				default:
					return
				}
			}
		}
	}
}

func reply(it outItem, err error) {
	if it.result != nil {
		it.result <- err
	}
}

// write 用一次 sendmsg 写出一帧及其 FD。第一次失败后不再写出。
func (c *Conn) write(it outItem) {
	c.mu.Lock()
	werr := c.werr
	c.mu.Unlock()
	if werr != nil {
		reply(it, werr)
		return
	}
	err := c.sendmsg(it.frame, it.files)
	if err != nil {
		err = fmt.Errorf("sandbox: 控制通道写出: %w", err)
		c.mu.Lock()
		if c.werr == nil {
			c.werr = err
		}
		c.mu.Unlock()
	}
	reply(it, err)
}

func (c *Conn) sendmsg(frame []byte, files []*os.File) error {
	var oob []byte
	if len(files) > 0 {
		fds := make([]int, len(files))
		for i, f := range files {
			// 用 Control 取 fd 而不是 f.Fd()：后者会把文件切成阻塞模式。
			sc, err := f.SyscallConn()
			if err != nil {
				return err
			}
			if err := sc.Control(func(fd uintptr) { fds[i] = int(fd) }); err != nil {
				return err
			}
		}
		oob = syscall.UnixRights(fds...)
	}
	var serr error
	err := c.rc.Write(func(fd uintptr) bool {
		serr = syscall.Sendmsg(int(fd), frame, oob, nil, syscall.MSG_NOSIGNAL)
		return serr != syscall.EAGAIN
	})
	// files 在 sendmsg 返回前必须保持打开；调用方持有它们，这里只防止被提前回收。
	runtime.KeepAlive(files)
	if err != nil {
		return err
	}
	return serr
}

// Recv 读取一帧。返回的 files 只在 start 时非空（恰好 3 个，带 FD_CLOEXEC），
// 归调用方所有。
//
//   - 对端关闭 → io.EOF。
//   - 帧被截断（MSG_TRUNC，即超过 64 KiB）、控制消息被截断（MSG_CTRUNC）、
//     非 SCM_RIGHTS 控制消息、无法解码或不合协议的消息、FD 数量不符
//     → 关闭本帧收到的全部 FD，返回 ErrProtocol。
func (c *Conn) Recv() (Message, []*os.File, error) {
	if c.rc == nil {
		return Message{}, nil, c.werr
	}
	var (
		n, oobn, flags int
		rerr           error
	)
	err := c.rc.Read(func(fd uintptr) bool {
		n, oobn, flags, _, rerr = syscall.Recvmsg(int(fd), c.rbuf, c.oob, syscall.MSG_CMSG_CLOEXEC)
		return rerr != syscall.EAGAIN
	})
	if err == nil {
		err = rerr
	}
	if err != nil {
		return Message{}, nil, err
	}

	fds, cerr := parseRights(c.oob[:oobn])
	fail := func(format string, args ...any) (Message, []*os.File, error) {
		for _, fd := range fds {
			syscall.Close(fd)
		}
		return Message{}, nil, fmt.Errorf("%w: "+format, append([]any{ErrProtocol}, args...)...)
	}
	switch {
	case flags&syscall.MSG_TRUNC != 0:
		return fail("帧超过 %d 字节（MSG_TRUNC）", maxFrameSize)
	case flags&syscall.MSG_CTRUNC != 0:
		return fail("控制消息被截断（MSG_CTRUNC），FD 多于 %d 个", startFDs)
	case cerr != nil:
		return fail("%v", cerr)
	case n == 0 && len(fds) == 0:
		return Message{}, nil, io.EOF
	}

	var m Message
	if err := json.Unmarshal(c.rbuf[:n], &m); err != nil {
		return fail("解码消息: %v", err)
	}
	if err := validateMessage(m, len(fds)); err != nil {
		for _, fd := range fds {
			syscall.Close(fd)
		}
		return Message{}, nil, err
	}
	var files []*os.File
	for i, fd := range fds {
		files = append(files, os.NewFile(uintptr(fd), fmt.Sprintf("control-fd-%d", i)))
	}
	return m, files, nil
}

// parseRights 取出控制消息中的全部 SCM_RIGHTS FD。即使返回错误，也返回
// 已解析出的 FD，以便调用方关闭它们。
func parseRights(oob []byte) ([]int, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("解析控制消息: %v", err)
	}
	var (
		fds []int
		bad error
	)
	for _, cm := range msgs {
		if cm.Header.Level != syscall.SOL_SOCKET || cm.Header.Type != syscall.SCM_RIGHTS {
			bad = fmt.Errorf("意外的控制消息 level=%d type=%d", cm.Header.Level, cm.Header.Type)
			continue
		}
		got, err := syscall.ParseUnixRights(&cm)
		if err != nil {
			bad = fmt.Errorf("解析 SCM_RIGHTS: %v", err)
			continue
		}
		fds = append(fds, got...)
	}
	return fds, bad
}

// Close 关闭本端：之后 Send 返回错误，队列中尚未写出的消息被丢弃（等待中的
// 带 FD 的 Send 得到错误），阻塞中的 Recv 返回。对端随之读到 io.EOF。
//
// 先 shutdown 再 close：阻塞模式的 fd（init 经 ExtraFiles 继承的那种）上，
// 单纯 close 唤不醒正阻塞在 recvmsg/sendmsg 里的调用。
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.done)
		c.mu.Unlock()
		if c.rc != nil {
			_ = c.rc.Control(func(fd uintptr) { _ = syscall.Shutdown(int(fd), syscall.SHUT_RDWR) })
		}
		c.wg.Wait()
		c.closeErr = c.f.Close()
	})
	return c.closeErr
}
