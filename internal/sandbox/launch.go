//go:build linux

// Package sandbox 负责沙箱进程的启动与其内部的 1 号进程。
package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

// 本文件实现专用启动进程（`sandbox-launch` 子命令，规格 §4.6 开头两步与实现门槛 1）：
//
//   - server 以 `/proc/self/exe sandbox-launch` 为每个环境 re-exec 一个短生命周期的启动进程，
//     经继承的 FD 传入：控制 socket 的沙箱端、环境 cgroup 目录 fd、启动规格（JSON）、交还结果的
//     SEQPACKET socket，以及 init 的就绪管道写端。
//   - 启动进程清空**自身**附加组并断言为空（server 的凭据不变），再以 user namespace 等启动
//     `/proc/self/exe init`：CLONE_INTO_CGROUP 使 init 在执行任何代码之前已在环境 cgroup 中。
//   - init 的 pidfd 与 pid 经结果 socket 以 SCM_RIGHTS 交还 server，启动进程随即退出；server 是
//     child subreaper，init 由它收养并收割。
//   - 任何一步失败：经结果 socket 报告原因文本，以非零码退出，不留进程（clone 之后的失败先以
//     pidfd_send_signal 发 SIGKILL 并收割 init）。
//
// 改变凭据的系统调用（setgroups 等）只允许出现在本文件、init 的能力设置（caps.go）与 stage-2 helper 中
// （archtest 以源码检查固定）；三者都运行在 re-exec 出的独立进程里。

// InitArg 是 re-exec 沙箱 init 时传给自身的子命令名。
const InitArg = "init"

// LaunchArg 是 re-exec 专用启动进程时传给自身的子命令名。
const LaunchArg = "sandbox-launch"

// init 继承的 FD：控制 socket 的沙箱端、就绪管道的写端，以及启动规格（LaunchSpec 的 JSON，读到 EOF；
// init 的全部输入经它与继承的 FD 传入，不经环境变量）。
//
// 就绪约定：init 完成环境建立、可经控制连接接收 start 时，向 InitReadyFD 写入一个字节
// InitReadyByte 后关闭它；未就绪即失败时写入原因文本（不以 InitReadyByte 开头）或直接退出（EOF）。
const (
	InitControlFD = 3
	InitReadyFD   = 4
	InitSpecFD    = 5
	InitReadyByte = 0x01
)

// 启动进程继承的 FD（顺序即 LaunchCommand 的 ExtraFiles）。
const (
	launchControlFD = 3 // 控制 socket 的沙箱端，作为 init 的 InitControlFD
	launchCgroupFD  = 4 // 环境 cgroup 目录（O_RDONLY|O_DIRECTORY）
	launchSpecFD    = 5 // 启动规格（JSON），读到 EOF
	launchResultFD  = 6 // 交还结果的 SEQPACKET socket
	launchReadyFD   = 7 // init 就绪管道的写端，作为 init 的 InitReadyFD
)

// maxLaunchMsg 是启动规格与结果消息的上限。
const maxLaunchMsg = 64 * 1024

// cloneFlags 是 init 进入的命名空间集合（规格 §4.5、§4.6）。CLONE_NEWPID 同时提供整组回收的边界：
// 1 号进程退出时内核杀死该命名空间内的全部进程。
const cloneFlags = syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID |
	syscall.CLONE_NEWUTS | syscall.CLONE_NEWIPC | syscall.CLONE_NEWNET

// 启动进程的失败注入点（只供测试：生产的启动器从不设置 LaunchSpec.FailAt）。
const (
	LaunchFailBeforeClone = "before_clone"
	LaunchFailAfterClone  = "after_clone"
)

// LaunchSpec 是 server 传给启动进程的启动规格。命名空间内的 0 映射到范围基址，长度为范围大小。
type LaunchSpec struct {
	UIDBase  uint32 `json:"uid_base"`
	UIDSize  uint32 `json:"uid_size"`
	GIDBase  uint32 `json:"gid_base"`
	GIDSize  uint32 `json:"gid_size"`
	Hostname string `json:"hostname"`
	// Init 是 init 环境建立的输入（挂载、模板等，见 init.go）；由 init 校验，启动进程原样转交。
	Init InitSpec `json:"init"`
	// Env 是附加给 init 的环境变量（测试钩子；生产为空）。init 不继承 server 的环境。
	Env []string `json:"env,omitempty"`
	// FailAt 在给定步骤注入失败（测试钩子；生产为空）。
	FailAt string `json:"fail_at,omitempty"`
}

func (s LaunchSpec) validate() error {
	check := func(what string, base, size uint32) error {
		switch {
		case base == 0:
			// 基址 0 会把命名空间 root 映射为宿主 root。
			return fmt.Errorf("%s 范围基址为 0", what)
		case size == 0:
			return fmt.Errorf("%s 范围为空", what)
		case uint64(base)+uint64(size) > 1<<32-1:
			return fmt.Errorf("%s 范围 [%d, +%d) 越界", what, base, size)
		}
		return nil
	}
	if err := check("uid", s.UIDBase, s.UIDSize); err != nil {
		return err
	}
	if err := check("gid", s.GIDBase, s.GIDSize); err != nil {
		return err
	}
	switch s.FailAt {
	case "", LaunchFailBeforeClone, LaunchFailAfterClone:
	default:
		return fmt.Errorf("未知的注入点 %q", s.FailAt)
	}
	return nil
}

// launchResult 是启动进程经结果 socket 交还的一帧：成功时 PID 非零并携带 init 的 pidfd，失败时只有 Reason。
type launchResult struct {
	PID    int    `json:"pid,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// LaunchFiles 是 server 交给启动进程的子端 FD。LaunchCommand 只把它们挂到 ExtraFiles 上；
// cmd.Start 之后调用方关闭自己持有的这一份（否则 init 与启动进程退出时对端读不到 EOF）。
type LaunchFiles struct {
	Control *os.File // 控制 socketpair 的沙箱端
	Cgroup  *os.File // 环境 cgroup 目录
	Spec    *os.File // 已写入启动规格并关闭写端的管道读端
	Result  *os.File // 结果 socketpair 的启动进程端（SOCK_SEQPACKET）
	Ready   *os.File // 就绪管道的写端
}

// LaunchCommand 返回启动进程的命令：`/proc/self/exe sandbox-launch`，空环境（不把 server 的环境
// 传进沙箱），stdout 与 stderr 并入 server 的 stderr。
func LaunchCommand(f LaunchFiles) *exec.Cmd {
	cmd := exec.Command("/proc/self/exe", LaunchArg)
	cmd.Env = []string{}
	cmd.Dir = "/"
	cmd.ExtraFiles = []*os.File{f.Control, f.Cgroup, f.Spec, f.Result, f.Ready}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd
}

// LaunchError 是启动进程报告的失败。
type LaunchError struct{ Reason string }

func (e *LaunchError) Error() string { return "sandbox-launch: " + e.Reason }

// RecvLaunchResult 从结果 socket（server 端，阻塞 fd）读取启动进程交还的一帧。
// 成功时返回 init 的 pid 与 pidfd（close-on-exec，归调用方所有）；启动进程报告失败时返回 *LaunchError；
// 对端关闭而没有结果时返回 io.EOF。
func RecvLaunchResult(fd int) (pid, pidfd int, err error) {
	buf := make([]byte, maxLaunchMsg)
	oob := make([]byte, syscall.CmsgSpace(4*4))
	var n, oobn, flags int
	for {
		n, oobn, flags, _, err = syscall.Recvmsg(fd, buf, oob, syscall.MSG_CMSG_CLOEXEC)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return 0, -1, fmt.Errorf("sandbox-launch: 接收结果: %w", err)
	}
	fds, perr := parseRights(oob[:oobn])
	closeAll := func() {
		for _, f := range fds {
			syscall.Close(f)
		}
	}
	switch {
	case perr != nil:
		closeAll()
		return 0, -1, fmt.Errorf("sandbox-launch: 结果的控制消息: %w", perr)
	case flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0:
		closeAll()
		return 0, -1, errors.New("sandbox-launch: 结果被截断")
	case n == 0 && len(fds) == 0:
		return 0, -1, io.EOF
	}
	var r launchResult
	if err := json.Unmarshal(buf[:n], &r); err != nil {
		closeAll()
		return 0, -1, fmt.Errorf("sandbox-launch: 解码结果: %w", err)
	}
	if r.PID <= 0 {
		closeAll()
		if r.Reason == "" {
			r.Reason = "未给出原因"
		}
		return 0, -1, &LaunchError{Reason: r.Reason}
	}
	if len(fds) != 1 {
		closeAll()
		return 0, -1, fmt.Errorf("sandbox-launch: 成功结果携带 %d 个 FD，期望 1 个 pidfd", len(fds))
	}
	return r.PID, fds[0], nil
}

// RunLaunch 是专用启动进程的主函数（main 在 init 之后分流到这里）。返回 nil 即 init 的 pidfd 与 pid
// 已交还 server；返回错误前已经结果 socket 报告原因（尽力而为），且没有留下 init 进程。
func RunLaunch() error {
	// 继承的 FD 不带 close-on-exec；先全部设上，避免泄漏给 init（init 需要的两个经 ExtraFiles 重新传入）。
	for fd := launchControlFD; fd <= launchReadyFD; fd++ {
		if _, err := fcntl(fd, syscall.F_GETFD, 0); err != nil {
			return fmt.Errorf("sandbox-launch: 缺少继承的 fd %d: %w", fd, err)
		}
		syscall.CloseOnExec(fd)
	}
	fail := func(step string, err error) error {
		reason := "launch/" + step + ": " + err.Error()
		b, _ := json.Marshal(launchResult{Reason: reason})
		_ = sendmsgRetry(launchResultFD, b, nil)
		return errors.New(reason)
	}

	spec, err := readLaunchSpec(launchSpecFD)
	if err != nil {
		return fail("spec", err)
	}
	if spec.FailAt == LaunchFailBeforeClone {
		return fail(LaunchFailBeforeClone, errors.New("注入的失败"))
	}

	// 实现门槛 1：只在本进程清空附加组（Go 的 Setgroups 作用于全部线程），并断言为空；
	// clone 出的 init 继承空组。setgroups=deny 之后命名空间内无法再补救（实验记录 §3）。
	if err := syscall.Setgroups([]int{}); err != nil {
		return fail("setgroups", err)
	}
	if gs, err := syscall.Getgroups(); err != nil {
		return fail("getgroups", err)
	} else if len(gs) != 0 {
		return fail("getgroups", fmt.Errorf("清空后附加组仍为 %v", gs))
	}

	// 启动规格原样（重新编码）转交 init：经管道作为 InitSpecFD，clone 之后写入（init 首先读取它）。
	initSpec, err := json.Marshal(spec)
	if err != nil {
		return fail("spec", err)
	}
	specR, specW, err := os.Pipe()
	if err != nil {
		return fail("spec", err)
	}
	defer specR.Close()
	defer specW.Close()

	pidfd := -1
	cmd := exec.Command("/proc/self/exe", InitArg)
	cmd.Env = append([]string{}, spec.Env...)
	cmd.Dir = "/"
	cmd.ExtraFiles = []*os.File{
		os.NewFile(launchControlFD, "control"), // → InitControlFD
		os.NewFile(launchReadyFD, "ready"),     // → InitReadyFD
		specR,                                  // → InitSpecFD
	}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  cloneFlags,
		UseCgroupFD: true, // clone3 的 CLONE_INTO_CGROUP
		CgroupFD:    launchCgroupFD,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: int(spec.UIDBase), Size: int(spec.UIDSize)}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: int(spec.GIDBase), Size: int(spec.GIDSize)}},
		// Go 先写 /proc/<pid>/setgroups 为 deny，再写 gid_map。
		GidMappingsEnableSetgroups: false,
		// 在命名空间内成为 uid/gid 0，使 execve init 后保留命名空间能力；不调用 setgroups（已为空）。
		Credential: &syscall.Credential{Uid: 0, Gid: 0, NoSetGroups: true},
		PidFD:      &pidfd,
		// 不设 Pdeathsig：本进程交还结果后即退出，init 由 server 收养。
	}
	if err := cmd.Start(); err != nil {
		return fail("clone", err)
	}
	// clone 之后的失败：先杀死并收割 init，再报告。
	abort := func(step string, err error) error {
		if kerr := pidfdSendSignal(pidfd, syscall.SIGKILL); kerr != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
		syscall.Close(pidfd)
		return fail(step, err)
	}
	if pidfd < 0 {
		return abort("pidfd", errors.New("clone 没有返回 pidfd"))
	}
	// 关闭本进程的读端后写入：init 未读完即退出时写入以 EPIPE 失败，而不是阻塞。
	specR.Close()
	if _, err := specW.Write(initSpec); err != nil {
		return abort("spec", err)
	}
	specW.Close()
	if spec.FailAt == LaunchFailAfterClone {
		return abort(LaunchFailAfterClone, errors.New("注入的失败"))
	}
	b, err := json.Marshal(launchResult{PID: cmd.Process.Pid})
	if err != nil {
		return abort("result", err)
	}
	if err := sendmsgRetry(launchResultFD, b, syscall.UnixRights(pidfd)); err != nil {
		return abort("result", err)
	}
	return nil
}

// readLaunchSpec 读取并校验启动规格（读到 EOF，不超过 maxLaunchMsg）。
func readLaunchSpec(fd int) (LaunchSpec, error) {
	f := os.NewFile(uintptr(fd), "spec")
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxLaunchMsg+1))
	if err != nil {
		return LaunchSpec{}, err
	}
	if len(b) > maxLaunchMsg {
		return LaunchSpec{}, fmt.Errorf("启动规格超过 %d 字节", maxLaunchMsg)
	}
	var s LaunchSpec
	if err := json.Unmarshal(b, &s); err != nil {
		return LaunchSpec{}, fmt.Errorf("解码启动规格: %w", err)
	}
	if err := s.validate(); err != nil {
		return LaunchSpec{}, err
	}
	return s, nil
}

func sendmsgRetry(fd int, b, oob []byte) error {
	for {
		err := syscall.Sendmsg(fd, b, oob, nil, 0)
		if err != syscall.EINTR {
			return err
		}
	}
}

func fcntl(fd, cmd, arg int) (int, error) {
	r, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	if e != 0 {
		return 0, e
	}
	return int(r), nil
}

// sysPidfdSendSignal 是 pidfd_send_signal 的系统调用号（x86_64 与 aarch64 相同）。
const sysPidfdSendSignal = 424

// pidfdSendSignal 经 pidfd 向进程发信号；与 kill(pid) 不同，pid 被复用后不会误杀其他进程。
func pidfdSendSignal(pidfd int, sig syscall.Signal) error {
	_, _, e := syscall.Syscall6(sysPidfdSendSignal, uintptr(pidfd), uintptr(sig), 0, 0, 0, 0)
	if e != 0 {
		return e
	}
	return nil
}

// PidfdKill 经 pidfd 向进程发 SIGKILL（供 server 端在 init 未就绪时清理）。
func PidfdKill(pidfd int) error { return pidfdSendSignal(pidfd, syscall.SIGKILL) }
