//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// RunInit 是沙箱内 1 号进程的主函数。它永不正常返回。
//
// 调用时机：进程以 `/proc/self/exe init` 启动，且 main 在做任何
// 其他初始化之前就分流到这里。
func RunInit() error {
	// root 目前只做判空校验；它的值本身留给 Task 6 去做 pivot_root
	// （把这个目录切换成新的根文件系统），这里先不用，避免引入
	// 一个只声明未使用、又要用 _ 掩盖的半成品变量。
	root := os.Getenv(envSandboxRoot)
	if root == "" {
		return fmt.Errorf("缺少环境变量 %s", envSandboxRoot)
	}
	if h := os.Getenv(envSandboxHostname); h != "" {
		if err := syscall.Sethostname([]byte(h)); err != nil {
			return fmt.Errorf("设置沙箱 hostname: %w", err)
		}
	}
	// pivot_root 与伪文件系统挂载尚未补齐。
	// 控制循环见 Serve；生产 Launcher（stage-2 helper，规格 §4.6）在 Plan 1B
	// 之后实现，在此之前 init 没有可用的启动器，不进入 Serve。
	return fmt.Errorf("init 尚未实现完整")
}

// ReasonDuplicateExecID 是 start 的 exec_id 已登记且尚未退出时 start_err 的 reason。
const ReasonDuplicateExecID = "duplicate_exec_id"

// reasonInvalidSpec 是 spec 无法按 StartSpec 解码（或 argv 为空）时 start_err 的 reason 前缀。
const reasonInvalidSpec = "invalid_spec"

// Serve 是沙箱 init 的控制循环（规格 §4.2–§4.4）：
//
//   - start：exec_id 已登记且未退出 → start_err{reason: duplicate_exec_id}；
//     spec 不能按 StartSpec 严格解码或 argv 为空 → start_err{reason: invalid_spec: …}；
//     否则清除 3 个 FD 的 O_NONBLOCK，经 reg.Start 用 l 启动（Launch 失败由
//     Start 回 start_err）。
//     无论哪种结果，收到的 3 个 FD 都被关闭，且不启动任何被拒绝的 workload。
//   - terminate{exec_id, grace_ms}：向该 workload 的进程组发 SIGTERM，并设定
//     grace_ms 的计时器，到期时若该进程仍未被收割则向进程组发 SIGKILL；
//     期间继续服务。未知（或已退出）的 exec_id 忽略。
//   - SIGCHLD：调用 reg.Reap（进入循环前也先 Reap 一次）。
//   - 其他消息类型（start_ack、start_err、exit）是协议错误。
//
// l 必须让每个 workload 成为自己进程组的组长（pgid == pid）：terminate 以
// 进程组为单位发信号；若该进程组不存在（Launcher 未建组），退而只向该 pid 发信号。
// 不依赖 Pdeathsig，不解析 workload 输出。
//
// 返回条件：宿主关闭连接 → nil；ctx 取消 → ctx.Err()；协议错误、Recv 失败、
// Reap 或 Start 报告的控制通道故障 → 该错误。Serve 返回前关闭 conn、停止
// 所有 grace 计时器；环境内残留的进程由 cgroup 回收（规格 §4.4）。
//
// reg 的 out 应向 conn 非阻塞入队（例如 func(m Message) error { return conn.Send(m, nil) }）。
// 一个进程只能运行一个 Serve：它经 Wait4(-1) 收割本进程的全部子进程。
func Serve(ctx context.Context, conn *Conn, reg *Registry, l Launcher) error {
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGCHLD)
	defer signal.Stop(sigch)

	type recvResult struct {
		m     Message
		files []*os.File
		err   error
	}
	recvch := make(chan recvResult)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			m, files, err := conn.Recv()
			select {
			case recvch <- recvResult{m, files, err}:
			case <-done:
				closeFiles(files)
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var timers []*time.Timer
	defer func() {
		for _, tm := range timers {
			tm.Stop()
		}
		close(done)
		conn.Close() // 唤醒阻塞中的 Recv
		wg.Wait()
	}()

	// Notify 之前退出的子进程的 SIGCHLD 不会送达；先收割一次。
	if err := reg.Reap(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sigch:
			if err := reg.Reap(); err != nil {
				return err
			}
		case r := <-recvch:
			if errors.Is(r.err, io.EOF) {
				return nil
			}
			if r.err != nil {
				return fmt.Errorf("sandbox: 控制通道接收: %w", r.err)
			}
			switch r.m.Type {
			case MsgStart:
				if len(r.files) != startFDs {
					// Recv 已保证 start 恰好 3 个 FD；防御性检查。
					closeFiles(r.files)
					return fmt.Errorf("%w: start 携带 %d 个 FD", ErrProtocol, len(r.files))
				}
				gl := guardLauncher{reg: reg, execID: r.m.ExecID, l: l}
				fds := [3]*os.File{r.files[0], r.files[1], r.files[2]}
				if err := reg.Start(r.m.ExecID, gl, r.m.Spec, fds); err != nil {
					return err
				}
			case MsgTerminate:
				if tm := terminate(reg, r.m.ExecID, r.m.GraceMS); tm != nil {
					timers = append(timers, tm)
				}
			default:
				closeFiles(r.files)
				return fmt.Errorf("%w: init 不接受来自宿主的 %s", ErrProtocol, r.m.Type)
			}
		}
	}
}

// guardLauncher 在 reg.Start 内（即持有 reg.mu 时）先拒绝重复的 exec_id 与
// 非法 spec，再交给真正的 Launcher；拒绝经 Launch 的错误变成 start_err，
// FD 由 Start 统一关闭。检查与登记在同一次持锁内，没有竞争窗口。
type guardLauncher struct {
	reg    *Registry
	execID string
	l      Launcher
}

func (g guardLauncher) Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (int, error) {
	// Registry.Start 在 reg.mu 内调用 Launch，这里可以直接读登记表。
	if _, ok := g.reg.pidOfLocked(g.execID); ok {
		return 0, errors.New(ReasonDuplicateExecID)
	}
	dec := json.NewDecoder(bytes.NewReader(spec))
	dec.DisallowUnknownFields()
	var s StartSpec
	if err := dec.Decode(&s); err != nil {
		return 0, fmt.Errorf("%s: %v", reasonInvalidSpec, err)
	}
	if dec.More() {
		return 0, fmt.Errorf("%s: spec 之后有多余数据", reasonInvalidSpec)
	}
	if len(s.Argv) == 0 {
		return 0, fmt.Errorf("%s: argv 为空", reasonInvalidSpec)
	}
	for _, f := range []*os.File{stdin, stdout, stderr} {
		if err := setBlocking(f); err != nil {
			return 0, err
		}
	}
	return g.l.Launch(spec, stdin, stdout, stderr)
}

// setBlocking 清除 f 的 O_NONBLOCK。
//
// 宿主 os.Pipe 建的管道端是非阻塞的，这个状态属于打开文件描述，随 SCM_RIGHTS
// 传到 init，再经 fork/exec 传给 workload，workload 读写会遇到 EAGAIN。
// os.NewFile 对已经非阻塞的 fd 不记 nonblock 标志，因此 File.Fd() 和
// exec.Cmd 都不会把它切回阻塞；必须在这里显式清除。这些描述只由 init 与
// workload 持有（宿主发送后即关闭其子端副本），清除不影响宿主自己的管道端。
func setBlocking(f *os.File) error {
	sc, err := f.SyscallConn()
	if err != nil {
		return fmt.Errorf("清除 O_NONBLOCK: %w", err)
	}
	var serr error
	if err := sc.Control(func(fd uintptr) { serr = syscall.SetNonblock(int(fd), false) }); err != nil {
		return fmt.Errorf("清除 O_NONBLOCK: %w", err)
	}
	if serr != nil {
		return fmt.Errorf("清除 O_NONBLOCK: %w", serr)
	}
	return nil
}

// pidOfLocked 返回 execID 当前登记的 pid。调用方必须持有 r.mu。
func (r *Registry) pidOfLocked(execID string) (int, bool) {
	for pid, id := range r.pids {
		if id == execID {
			return pid, true
		}
	}
	return 0, false
}

// signalExec 在 reg.mu 内向 execID 的进程组发 sig，返回发信号的 pid。
// want 非 0 时只在该 exec 仍登记为同一 pid 时发信号（grace 计时器用它避免
// 误杀同名的后继 exec）。exec 未登记（未知或已收割）时什么都不做，返回 0。
// 持锁保证 pid 未被收割，因而进程组号不会被复用。
func (r *Registry) signalExec(execID string, want int, sig syscall.Signal) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	pid, ok := r.pidOfLocked(execID)
	if !ok || (want != 0 && pid != want) {
		return 0
	}
	if err := syscall.Kill(-pid, sig); errors.Is(err, syscall.ESRCH) {
		_ = syscall.Kill(pid, sig) // Launcher 没有为它建进程组
	}
	return pid
}

// terminate 发 SIGTERM 并返回 grace 到期发 SIGKILL 的计时器；exec 未登记时返回 nil。
func terminate(reg *Registry, execID string, graceMS int64) *time.Timer {
	pid := reg.signalExec(execID, 0, syscall.SIGTERM)
	if pid == 0 {
		return nil
	}
	if graceMS < 0 {
		graceMS = 0
	}
	return time.AfterFunc(time.Duration(graceMS)*time.Millisecond, func() {
		reg.signalExec(execID, pid, syscall.SIGKILL)
	})
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}
