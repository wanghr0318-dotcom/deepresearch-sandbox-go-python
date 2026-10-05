//go:build linux

package sandbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"syscall"
)

// 本文件是 init 的生产 Launcher 与 exec 提交点判据（规格 §4.6 "每个 workload" 段与实现门槛 2）。
//
// 启动路径（Serve 收到 start 后，持 reg.mu）：Conn.Recv 已以 recvmsg(MSG_CMSG_CLOEXEC) 收到恰好 3 个 FD →
// pipe2(O_CLOEXEC) 作为 exec-status → 阻塞全部信号 → raw clone(SIGCHLD) → 子进程 dup3 到 0/1/2/3、
// setpgid(0, 0)、清空信号掩码、execveat(helper_fd, "", AT_EMPTY_PATH)（rawfork_linux.go）→ 父进程把 pid 登记为
// "启动中"后释放 reg.mu，在单独的 goroutine 中判定（Serve 继续服务：收割、其他 start、连接断开）。
//
// exec 提交点判据（门槛 2，逐条）：
//
//  1. 提交点：helper 的 execve(workload) 成功即"已启动"。start_ack 表示启动路径到达了提交点，**不**表示
//     workload 的首条指令已执行。
//  2. 失败路径：helper 任一降权步骤或 execve 失败 → 向 exec-status 写 `helper/<步骤>: <errno 文本>` 后
//     exit(126)；init 读到字节 → start_err{该原因}，workload 未运行。clone 之后、execveat 之前子进程的失败
//     同样经管道报告（init/<步骤>）。
//  3. 成功路径：execve 成功 → 内核按 FD_CLOEXEC 关闭写端 → init 读到 EOF 而没有字节。
//  4. 消歧：reaper 仍是唯一的 wait 者；登记表为启动中的 pid 保存 reaper 观察到的终止状态。init 在 EOF 无字节
//     之后的观察点上查询：
//     - 子进程存活（终止尚未开始）→ start_ack；之后的终止由 reaper 记录，判定方在 ack 之后投递 exit；
//     - 终止不晚于观察点（reaper 已记录，或观察点上内核已在退出该进程）→ 等待 reaper 的记录（条件变量，
//       不轮询），然后：以信号终止 → start_err{helper/died_before_exec: signal <sig>}；以 126 退出 →
//       start_err{helper/died_before_exec: exit 126}（helper 写失败后退出）；以其他码正常退出 → start_ack
//       然后 exit（workload 已运行并退出）。
//  5. 顺序：reaper 对启动中 pid 的终止只记录不投递（锁内、非阻塞）；判定方在锁内按序投递 ack → exit，
//     或只投递 start_err。判定期间不持 reg.mu。
//  6. 控制通道断开（Serve 返回）：abort 终止启动中 pid 的进程组、清理登记，不投递任何消息。
//  7. 残余竞争窗口（显式承认，不 overclaim）：helper 在最后一步与 execve 返回之间被 SIGKILL，与 workload 在
//     execve 返回后立即被 SIGKILL，在"EOF 无字节 + 以信号终止"上不可区分；按提交点归为 died_before_exec。
//     不用进程名、不轮询、不 ptrace；消除该窗口需要内核的正向 exec 事件，已评估并不采用（决策说明）。
//  8. exec 成功与协议层 ready 握手分开：start_ack 不等待 workload 的任何输出。
//
// 观察点上"终止是否已开始"的取证：子进程退出时内核先关闭它的 FD（管道 EOF），稍后才使它成为可被 wait 的
// 僵尸（并发送 SIGCHLD）。只看 reaper 的记录，"exec 之前死亡"会在这段间隙里被误判为存活。因此观察点除了查
// reaper 的记录，还读一次 /proc/<pid>/stat：PF_EXITING（do_exit 的第一步设置，早于关闭 FD）或僵尸状态即表示
// 终止已开始，判定方随后等待 reaper 的记录。这是一次性的内核状态读取，不是进程名判断，也不是轮询；execve 之后
// 才开始的终止（PF_EXITING 在观察点之后才置位）不影响已作出的 start_ack。

// committer 是经 exec 提交点判定启动的 Launcher（生产 Launcher）。spawn 在 reg.mu 内调用：clone 出子进程，
// 返回其 pid 与 exec-status 管道的读端；返回错误表示没有子进程被创建（start_err）。
type committer interface {
	spawn(spec json.RawMessage, stdin, stdout, stderr *os.File) (pid int, status *os.File, err error)
}

// helperLauncher 是 init 的生产 Launcher：经 stage-2 helper（helper.go）降权后 execve workload。
type helperLauncher struct {
	helperFD int    // helper 二进制（/proc/self/exe）的 O_PATH fd，close-on-exec，在高位
	kind     string // 环境类型
	noFile   uint64 // workload 的 RLIMIT_NOFILE
	fsize    uint64 // exec 环境 workload 的 RLIMIT_FSIZE
}

var _ committer = (*helperLauncher)(nil)

// newHelperLauncher 按环境建立的结果构造生产 Launcher。
//
// RLIMIT_FSIZE（仅 exec 环境）取可写 tmpfs 中最大者的大小（/tmp 与 /out）：单个文件不会合法地超过它。
func newHelperLauncher(helperFD int, s InitSpec) *helperLauncher {
	l := &helperLauncher{helperFD: helperFD, kind: s.Kind, noFile: workloadNoFile}
	if s.Kind == KindExec {
		l.fsize = uint64(max(s.TmpBytes, s.OutBytes))
	}
	return l
}

// Launch 满足 Launcher 接口；Serve 对 committer 走 startCommit，不调用它。
func (l *helperLauncher) Launch(json.RawMessage, *os.File, *os.File, *os.File) (int, error) {
	return 0, errors.New("init/launcher: 生产 Launcher 只经 exec 提交点判定启动")
}

// helperTestHook 是 helper 的测试钩子（helperSpec.Hook）。只由测试设置（生产二进制中恒为空）。
var helperTestHook string

// childFDMin 是子进程 fd 0..3 的来源在父进程中移到的最低编号（不得落在 0..3）。
const childFDMin = 10

func (l *helperLauncher) spawn(spec json.RawMessage, stdin, stdout, stderr *os.File) (int, *os.File, error) {
	var s StartSpec
	if err := json.Unmarshal(spec, &s); err != nil { // guardLauncher.check 已严格校验
		return 0, nil, fmt.Errorf("%s: %v", reasonInvalidSpec, err)
	}
	arg, err := json.Marshal(helperSpec{
		Argv: s.Argv, Env: s.Env, Dir: s.Dir, Kind: l.kind, NoFile: l.noFile, FSize: l.fsize, Hook: helperTestHook,
	})
	if err != nil {
		return 0, nil, fmt.Errorf("init/helper_spec: %w", err)
	}

	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_CLOEXEC); err != nil {
		return 0, nil, fmt.Errorf("init/exec_status: %w", err)
	}
	status := os.NewFile(uintptr(p[0]), "exec-status")
	defer syscall.Close(p[1]) // 子进程持有自己的副本；父进程不保留写端，否则读不到 EOF

	// 来源 FD 移到高位（close-on-exec）：子进程依次 dup3 到 0..3，来源不能与目标重叠。
	var srcs [4]int
	n := 0
	defer func() {
		for _, fd := range srcs[:n] {
			syscall.Close(fd)
		}
	}()
	for _, f := range []*os.File{stdin, stdout, stderr, nil} {
		fd := p[1]
		if f != nil {
			if fd, err = rawFD(f); err != nil {
				status.Close()
				return 0, nil, fmt.Errorf("init/dup: %w", err)
			}
		}
		hi, err := fcntl(fd, syscall.F_DUPFD_CLOEXEC, childFDMin)
		if err != nil {
			status.Close()
			return 0, nil, fmt.Errorf("init/dup: %w", err)
		}
		srcs[n] = hi
		n++
	}

	pid, err := forkExecveat(l.helperFD, []string{helperComm, HelperArg, string(arg)}, nil, srcs)
	if err != nil {
		status.Close()
		return 0, nil, fmt.Errorf("init/clone: %w", err)
	}
	return pid, status, nil
}

// rawFD 返回 f 的 fd，不改变其阻塞模式（File.Fd 会把文件切成阻塞模式）。
func rawFD(f *os.File) (int, error) {
	sc, err := f.SyscallConn()
	if err != nil {
		return 0, err
	}
	fd := -1
	if err := sc.Control(func(u uintptr) { fd = int(u) }); err != nil {
		return 0, err
	}
	return fd, nil
}

// maxStatusBytes 是 exec-status 管道读取的上限（helper 的原因不超过 PIPE_BUF）。
const maxStatusBytes = 4096

// 判定路径的测试钩子（只由测试设置，生产中为 nil）：
//
//   - commitBeforeRead 在读取 exec-status 之前调用（测试用它让 reaper 先于 EOF 观察记录终止）；
//   - commitAfterObserve 在观察点之后、投递之前调用（测试用它构造"EOF 先于终止"）；
//   - startLockProbe 在释放 reg.mu 之后、等待判定之前调用（测试用它断言等待期间 reg.mu 未被持有）。
var (
	commitBeforeRead   func(r *Registry, p *pending)
	commitAfterObserve func(r *Registry, p *pending)
	startLockProbe     func(r *Registry, execID string)
)

// startCommit 是经 exec 提交点判定的启动（规格 §4.6 门槛 2）。在 reg.mu 内调用 spawn（含重复 exec_id 与 spec
// 的检查）：失败 → 入队 start_err；成功 → 登记 pid → exec_id 与启动中记录，释放 reg.mu 后由单独的 goroutine
// 判定并投递。返回的错误只表示 out 失败（控制通道故障）。调用方负责关闭收到的 3 个 FD。
func (r *Registry) startCommit(execID string, spawn func() (int, *os.File, error)) error {
	r.mu.Lock()
	pid, status, err := spawn()
	if err != nil {
		oerr := r.out(Message{Type: MsgStartErr, ExecID: execID, Reason: err.Error()})
		r.mu.Unlock()
		if oerr != nil {
			return fmt.Errorf("sandbox: 入队 start_err(%s): %w", execID, oerr)
		}
		return nil
	}
	p := &pending{execID: execID, pid: pid}
	r.pids[pid] = execID
	r.starting[execID] = p
	r.commits.Add(1)
	r.mu.Unlock()

	go r.commit(p, status)
	return nil
}

// commit 判定一次启动并投递结果（见文件开头的判据 2–7）。
func (r *Registry) commit(p *pending, status *os.File) {
	defer r.commits.Done()
	if startLockProbe != nil {
		startLockProbe(r, p.execID)
	}
	if commitBeforeRead != nil {
		commitBeforeRead(r, p)
	}

	data, rerr := io.ReadAll(io.LimitReader(status, maxStatusBytes))
	status.Close()

	// 观察点：EOF 无字节时，终止是否已开始（reaper 已记录，或内核已在退出该进程）。
	var reason string
	terminating := false
	switch {
	case rerr != nil:
		reason = "init/exec_status: " + rerr.Error()
	case len(data) > 0:
		reason = statusReason(data)
	default:
		var err error
		if terminating, err = r.terminationBegun(p); err != nil {
			reason = "init/exec_status: " + err.Error()
		}
	}
	if commitAfterObserve != nil {
		commitAfterObserve(r, p)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if terminating {
		for !p.exited && !r.aborted {
			r.cond.Wait()
		}
		if p.exited {
			reason = diedBeforeExec(p.ws)
		}
	}
	if r.aborted {
		return // abort 已终止进程组并清理登记；不投递任何消息
	}
	delete(r.starting, p.execID)
	if reason != "" {
		if !p.exited {
			// 不再登记：之后作为未登记的 pid 被收割并丢弃。读取失败时进程可能仍在运行，终止它
			// （start_err 承诺 workload 不运行）。
			delete(r.pids, p.pid)
			if rerr != nil || len(data) == 0 {
				killGroup(p.pid)
			}
		}
		r.emit(Message{Type: MsgStartErr, ExecID: p.execID, Reason: reason})
		return
	}
	if r.emit(Message{Type: MsgStartAck, ExecID: p.execID, PID: p.pid}) && p.exited {
		// 判定期间 reaper 已记录终止（pid 已从 pids 删除）：紧随 ack 投递 exit。
		r.emit(Message{Type: MsgExit, ExecID: p.execID, Exit: exitInfo(p.ws)})
	}
}

// emit 在 reg.mu 内入队一条消息；失败时把错误交给 Serve（控制通道故障）并返回 false。
func (r *Registry) emit(m Message) bool {
	if err := r.out(m); err != nil {
		select {
		case r.errc <- fmt.Errorf("sandbox: 入队 %s(%s): %w", m.Type, m.ExecID, err):
		default:
		}
		return false
	}
	return true
}

// terminationBegun 报告观察点上 p 的终止是否已开始：reaper 已记录，或 /proc/<pid>/stat 显示内核正在退出它
// （PF_EXITING 或僵尸状态）。pid 已不存在（已被 reaper 收割）同样视为已开始。
func (r *Registry) terminationBegun(p *pending) (bool, error) {
	r.mu.Lock()
	exited := p.exited
	r.mu.Unlock()
	if exited {
		return true, nil
	}
	return taskExiting(p.pid)
}

// pfExiting 是 task_struct.flags 的 PF_EXITING（include/linux/sched.h）。
const pfExiting = 0x4

// taskExiting 读一次 /proc/<pid>/stat：状态为 Z/X 或 flags 含 PF_EXITING → true；pid 不存在 → true。
func taskExiting(pid int) (bool, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("读取 /proc/%d/stat: %w", pid, err)
	}
	// 格式：pid (comm) state ppid pgrp session tty_nr tpgid flags ...；comm 可含空格与括号，从最后一个 ')' 之后解析。
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return false, fmt.Errorf("无法解析 /proc/%d/stat", pid)
	}
	f := bytes.Fields(b[i+1:])
	if len(f) < 7 {
		return false, fmt.Errorf("无法解析 /proc/%d/stat", pid)
	}
	if s := f[0]; len(s) == 1 && (s[0] == 'Z' || s[0] == 'X') {
		return true, nil
	}
	flags, err := strconv.ParseUint(string(f[6]), 10, 64)
	if err != nil {
		return false, fmt.Errorf("解析 /proc/%d/stat 的 flags: %w", pid, err)
	}
	return flags&pfExiting != 0, nil
}

// diedBeforeExec 按 reaper 记录的 wait 状态分类"终止不晚于观察点"的启动：以信号终止或以 126 退出 →
// died_before_exec 的原因；以其他码正常退出（workload 已运行并退出）→ ""（start_ack 然后 exit）。
//
// 以 126 退出的分类只对"终止不晚于 EOF 观察"生效：已越过观察点并以 126 退出的 workload 照常 start_ack。
func diedBeforeExec(ws syscall.WaitStatus) string {
	switch {
	case ws.Signaled():
		return "helper/died_before_exec: signal " + ws.Signal().String()
	case ws.ExitStatus() == 126:
		return "helper/died_before_exec: exit 126"
	}
	return ""
}

// statusReason 把 exec-status 管道中的字节转换为 start_err 的原因：helper 的 `helper/<步骤>: …` 原样使用；
// 子进程（execveat 之前）的 6 字节记录转换为 `init/<步骤>: <errno>`；其他内容视为损坏。
func statusReason(b []byte) string {
	if r, ok := childFailure(b); ok {
		return r
	}
	if bytes.HasPrefix(b, []byte("helper/")) {
		return string(b)
	}
	return fmt.Sprintf("init/exec_status: 无法识别的 %d 字节", len(b))
}

// killGroup 向 pid 的进程组与 pid 本身发 SIGKILL。调用方持有 reg.mu 且 pid 尚未被收割（因而不会被复用）。
func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

// abort 在 Serve 返回前调用（控制通道断开、协议错误、ctx 取消）：终止每个启动中 pid 的进程组、清理登记，
// 不投递任何消息；然后等待全部判定 goroutine 结束。之后的判定结果全部丢弃。logf 报告被终止的启动。
func (r *Registry) abort(logf func(format string, a ...any)) {
	r.mu.Lock()
	r.aborted = true
	for id, p := range r.starting {
		if !p.exited {
			killGroup(p.pid)
			delete(r.pids, p.pid)
		}
		delete(r.starting, id)
		logf("控制连接断开：终止启动中的 exec %s（pid %d），登记已清理", id, p.pid)
	}
	r.cond.Broadcast()
	r.mu.Unlock()
	r.commits.Wait()
}
