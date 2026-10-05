package sandbox

import (
	"errors"
	"runtime"
	"syscall"
	"unsafe"
)

// 本文件是 init 启动 stage-2 helper 的 fork 路径（规格 §4.6 "每个 workload" 段；移植自 Plan 1B spike 的 rawfork.go）。
//
// os/exec 不能以 execveat(fd, "", AT_EMPTY_PATH) 启动程序，因此 init 用 raw clone 分叉，并在 nosplit 函数中
// exec（与 syscall.forkAndExecInChild 的做法相同）：clone 与 execveat 之间子进程只做原始系统调用（不分配内存、
// 不增长栈）。clone 前后阻塞全部信号，使子进程中不会运行 Go 的信号处理函数；子进程在 execveat 之前装上空掩码，
// helper（以及之后的 workload）不继承被阻塞的信号。

// 架构相关的系统调用号（syscall 包未导出或各架构不同）。close_range 在 x86_64 与 aarch64 上相同。
var (
	sysSeccomp  uintptr
	sysExecveat uintptr
)

const sysCloseRange = 436

func init() {
	switch runtime.GOARCH {
	case "amd64":
		sysSeccomp, sysExecveat = 317, 322
	case "arm64":
		sysSeccomp, sysExecveat = 277, 281
	}
}

const sigSetmask = 2 // rt_sigprocmask 的 SIG_SETMASK

// 子进程（execveat 之前）失败时写入 exec-status 管道的记录：首字节 0（helper 的原因文本总以 "helper/" 开头，
// 不会以 0 开头），然后是步骤编号与 errno（小端 32 位），共 6 字节；随后以 127 退出。
const (
	childStepDup3     = 1
	childStepSetpgid  = 2
	childStepExecveat = 3
	childRecordLen    = 6
)

var childStepNames = map[byte]string{
	childStepDup3:     "dup3",
	childStepSetpgid:  "setpgid",
	childStepExecveat: "execveat",
}

type forkArgs struct {
	execveat uintptr
	dirfd    uintptr
	path     *byte    // ""（AT_EMPTY_PATH）
	argv     **byte   // 以 NULL 结尾
	envv     **byte   // 以 NULL 结尾
	fds      [4]int32 // 子进程 fd 0..3 的来源；3 是 exec-status 管道写端
	empty    uint64   // 空信号掩码
	all      uint64   // 全集信号掩码
}

//go:nosplit
//go:norace
func rawForkExecveat(a *forkArgs, oldmask *uint64) (pid uintptr, errno syscall.Errno) {
	_, _, e := syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(&a.all)), uintptr(unsafe.Pointer(oldmask)), 8, 0, 0)
	if e != 0 {
		return 0, e
	}
	r1, _, e := syscall.RawSyscall6(syscall.SYS_CLONE, uintptr(syscall.SIGCHLD), 0, 0, 0, 0, 0)
	if e != 0 || r1 != 0 {
		// 父进程（或 clone 失败）：恢复掩码。参数在上面已验证可用，恢复失败无可补救，返回 clone 的结果。
		_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(oldmask)), 0, 8, 0, 0)
		return r1, e
	}

	// 子进程。
	var step byte
	for i := 0; i < 4; i++ {
		if _, _, e = syscall.RawSyscall(syscall.SYS_DUP3, uintptr(a.fds[i]), uintptr(i), 0); e != 0 {
			step = childStepDup3
			goto fail
		}
	}
	if _, _, e = syscall.RawSyscall(syscall.SYS_SETPGID, 0, 0, 0); e != 0 {
		step = childStepSetpgid
		goto fail
	}
	// 清空掩码：与上面相同的合法参数不会失败；子进程中无法再做任何处理，继续 execveat。
	_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(&a.empty)), 0, 8, 0, 0)
	_, _, e = syscall.RawSyscall6(a.execveat, a.dirfd, uintptr(unsafe.Pointer(a.path)),
		uintptr(unsafe.Pointer(a.argv)), uintptr(unsafe.Pointer(a.envv)), atEmptyPath, 0)
	step = childStepExecveat
fail:
	{
		// 写到来源 fd（而不是 3）：dup3 失败时 fd 3 可能尚未就位。
		var buf [childRecordLen]byte
		buf[1] = step
		buf[2] = byte(e)
		buf[3] = byte(e >> 8)
		buf[4] = byte(e >> 16)
		buf[5] = byte(e >> 24)
		// 尽力写出失败记录；写失败时父进程读到 EOF 无字节，由 reaper 的 wait 状态（exit 127）归为 died_before_exec。
		_, _, _ = syscall.RawSyscall(syscall.SYS_WRITE, uintptr(a.fds[3]), uintptr(unsafe.Pointer(&buf[0])), childRecordLen)
		for {
			_, _, _ = syscall.RawSyscall(syscall.SYS_EXIT_GROUP, 127, 0, 0) // 不返回
		}
	}
}

// forkExecveat 分叉一个子进程，在其中把 fds 依次 dup3 到 0..3、setpgid(0, 0)、清空信号掩码，然后
// execveat(dirfd, "", AT_EMPTY_PATH) 执行 argv/envv。返回子进程 pid。
//
// fds 在父进程中应为 close-on-exec，且不得落在 0..3（调用方先把它们移到高位）。子进程在 execveat 之前的失败经
// fds[3] 写入 6 字节记录（见 childRecordLen）后以 127 退出。
func forkExecveat(dirfd int, argv, envv []string, fds [4]int) (int, error) {
	if sysExecveat == 0 {
		return 0, errors.New("execveat: 不支持的架构 " + runtime.GOARCH)
	}
	for _, fd := range fds {
		if fd < 4 {
			return 0, errors.New("forkExecveat: 来源 fd 落在 0..3")
		}
	}
	argvp, err := syscall.SlicePtrFromStrings(argv)
	if err != nil {
		return 0, err
	}
	envvp, err := syscall.SlicePtrFromStrings(envv)
	if err != nil {
		return 0, err
	}
	empty := []byte{0}
	a := &forkArgs{
		execveat: sysExecveat,
		dirfd:    uintptr(dirfd),
		path:     &empty[0],
		argv:     &argvp[0],
		envv:     &envvp[0],
		all:      ^uint64(0),
	}
	for i := range fds {
		a.fds[i] = int32(fds[i])
	}
	var oldmask uint64
	runtime.LockOSThread()
	pid, e := rawForkExecveat(a, &oldmask)
	runtime.UnlockOSThread()
	runtime.KeepAlive(a)
	runtime.KeepAlive(argvp)
	runtime.KeepAlive(envvp)
	runtime.KeepAlive(empty)
	if e != 0 {
		return 0, e
	}
	return int(pid), nil
}

// childFailure 解析子进程的 6 字节失败记录；不是这种记录时返回 false。
func childFailure(b []byte) (string, bool) {
	if len(b) != childRecordLen || b[0] != 0 {
		return "", false
	}
	name, ok := childStepNames[b[1]]
	if !ok {
		return "", false
	}
	e := syscall.Errno(uint32(b[2]) | uint32(b[3])<<8 | uint32(b[4])<<16 | uint32(b[5])<<24)
	return "init/" + name + ": " + e.Error(), true
}
