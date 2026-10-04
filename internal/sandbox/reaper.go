//go:build linux

package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// 本文件实现沙箱 init 的进程登记表与单一 reaper（规格 §4.3）：
//
//   - init 是沙箱内子进程的唯一 reaper。
//   - 一把锁 reg.mu 覆盖启动（启动 → 登记 pid → exec_id → 入队 start_ack）
//     与收割（循环 Wait4(-1, WNOHANG) → 查表 → 删登记 → 入队 exit，直至 0
//     或 ECHILD；EINTR 重试，其他错误返回给调用方）。
//   - 锁内只做非阻塞入队（out）；out 返回错误视为控制通道故障。
//   - 由于 start_ack 在锁内先于该 pid 的任何查表入队，且写出 FIFO，
//     start_ack 必先于对应 exit。
//   - 未登记的 pid（被收养的孤儿进程）收割后丢弃，不产生 exit。
//   - 不调用 Process.Wait / exec.Cmd.Wait：Wait4(-1) 是唯一的等待者。

// Launcher 启动一个 workload 并返回其 pid。生产实现是 Plan 1B 之后的
// stage-2 helper（规格 §4.6），测试用直接 exec。
//
// Launch 返回之后进程归 Registry 收割：实现不得等待该进程，也不得保留
// 会在进程退出后自行 wait 的句柄（使用 exec.Cmd 时只调用 Start，然后
// Process.Release）。
//
// stdin/stdout/stderr 来自控制通道（Conn.Recv）收到的 FD，可能处于非阻塞
// 模式（O_NONBLOCK 是打开文件描述的状态，随 SCM_RIGHTS 一起传递，宿主侧
// os.Pipe 创建的管道就是非阻塞的）。实现必须在交给子进程之前把它们切回
// 阻塞模式，否则 workload 读写会遇到 EAGAIN。用 exec.Cmd（或
// os.StartProcess）传递 *os.File 时，标准库经 File.Fd() 已把它们置为阻塞；
// 直接用 syscall.ForkExec 的实现必须自己清除 O_NONBLOCK。
//
// Launch 返回错误表示 workload 未运行（规格 §4.2 的 start_err）。
type Launcher interface {
	Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (pid int, err error)
}

// Registry 是 init 的进程登记表，pid → exec_id。
type Registry struct {
	// mu 即规格 §4.3 的 reg.mu：同时覆盖 Start 与 Reap 的全部步骤。
	mu   sync.Mutex
	out  func(Message) error
	pids map[int]string
	// wait4 默认为 syscall.Wait4；测试替换它以注入 EINTR 等错误。
	wait4 func(pid int, ws *syscall.WaitStatus, options int, ru *syscall.Rusage) (int, error)
}

// NewRegistry 返回一个空登记表。out 必须是非阻塞入队（例如 Conn.Send），
// 它在 reg.mu 内被调用；返回错误视为控制通道故障，由 Start/Reap 原样
// （包装后）返回给调用方，调用方应终止环境。
func NewRegistry(out func(Message) error) *Registry {
	return &Registry{out: out, pids: make(map[int]string), wait4: syscall.Wait4}
}

// Start 在 reg.mu 内启动 workload、登记 pid → execID 并入队 start_ack。
// Launch 失败时入队 start_err（Reason 为 Launch 的错误文本），不登记。
//
// fds 依次是 stdin、stdout、stderr；无论成败，Start 返回前都关闭它们
// （子进程持有自己的副本，init 不保留管道端）。
//
// 返回的错误只表示 out 失败（控制通道故障）；启动失败经 start_err 报告，
// 不作为 Start 的错误返回。
func (r *Registry) Start(execID string, l Launcher, spec json.RawMessage, fds [3]*os.File) error {
	defer func() {
		for _, f := range fds {
			if f != nil {
				f.Close()
			}
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	pid, err := l.Launch(spec, fds[0], fds[1], fds[2])
	if err != nil {
		if oerr := r.out(Message{Type: MsgStartErr, ExecID: execID, Reason: err.Error()}); oerr != nil {
			return fmt.Errorf("sandbox: 入队 start_err(%s): %w", execID, oerr)
		}
		return nil
	}
	r.pids[pid] = execID
	if err := r.out(Message{Type: MsgStartAck, ExecID: execID, PID: pid}); err != nil {
		return fmt.Errorf("sandbox: 入队 start_ack(%s): %w", execID, err)
	}
	return nil
}

// Reap 在 reg.mu 内循环 Wait4(-1, WNOHANG)，直到没有可收割的子进程
// （返回 0 或 ECHILD）。登记过的 pid 删除登记并入队 exit；未登记的 pid
// （被收养的孤儿）收割后丢弃。EINTR 重试；其他 Wait4 错误与 out 错误
// 立即返回。
//
// 调用方在收到 SIGCHLD 后调用；SIGCHLD 会合并，所以一次 Reap 必须收割
// 到没有为止。
func (r *Registry) Reap() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		var ws syscall.WaitStatus
		pid, err := r.wait4(-1, &ws, syscall.WNOHANG, nil)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ECHILD):
			return nil
		case err != nil:
			return fmt.Errorf("sandbox: wait4: %w", err)
		case pid <= 0:
			return nil
		}

		execID, ok := r.pids[pid]
		if !ok {
			continue // 被收养的孤儿：已收割，丢弃
		}
		delete(r.pids, pid)
		if err := r.out(Message{Type: MsgExit, ExecID: execID, Exit: exitInfo(ws)}); err != nil {
			return fmt.Errorf("sandbox: 入队 exit(%s): %w", execID, err)
		}
	}
}

// exitInfo 把 wait 状态转换为 exit 消息的退出状态：正常退出填 Code，
// 被信号杀死填 Signal（Code 为 0）。
func exitInfo(ws syscall.WaitStatus) *ExitInfo {
	if ws.Signaled() {
		return &ExitInfo{Signal: ws.Signal()}
	}
	return &ExitInfo{Code: ws.ExitStatus()}
}
