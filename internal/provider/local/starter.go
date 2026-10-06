//go:build linux

package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/sandbox"
)

// 本文件是生产 EnvStarter（规格 §4.6，Plan 2 Task 9）：每个环境 re-exec 一个专用启动进程
// （sandbox.RunLaunch），由它在 user namespace 等命名空间中、以 CLONE_INTO_CGROUP 启动 init，
// 并交还 init 的 pidfd 与 pid。server 进程本身不调用任何改变凭据的系统调用（实现门槛 1）。
//
// EnvStarter 的保证（local.go）在这里的落实：
//   - init 在执行任何代码之前已在环境 cgroup 中：clone3 的 CLONE_INTO_CGROUP。
//   - 成功返回即 init 就绪：等到 init 经就绪管道写入 sandbox.InitReadyByte。
//   - starter 收割 init：server 是 child subreaper，启动进程退出后 init 由 server 收养；
//     先收割启动进程（此时 init 已被收养），再以 waitid(P_PIDFD) 等待并收割 init。
//   - ctx 结束：停止推进并返回 ctx 的错误；已启动的 init 留在环境 cgroup 中，由 Stop 回收
//     （仍由本 starter 收割）。

// envHostname 是沙箱内的主机名：固定值，不把 env_id 等宿主信息暴露给 workload。
const envHostname = "agentbox"

// ProcessStarter 是生产 EnvStarter。只能经 NewProcessStarter 构造。
type ProcessStarter struct {
	// 测试钩子：附加给 init 的环境变量、启动进程的失败注入点。生产构造不设置它们。
	initEnv []string
	failAt  string
}

var _ EnvStarter = (*ProcessStarter)(nil)

// prSetChildSubreaper、prGetChildSubreaper 是 prctl 的选项号。
const (
	prSetChildSubreaper = 36
	prGetChildSubreaper = 37
)

// NewProcessStarter 返回生产启动器，并把本进程设为 child subreaper（启动进程退出后由本进程收养 init）。
// subreaper 是进程属性而非凭据；本进程的 uid、gid 与附加组不变。
func NewProcessStarter() (*ProcessStarter, error) {
	if _, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); e != 0 {
		return nil, fmt.Errorf("local: PR_SET_CHILD_SUBREAPER: %w", e)
	}
	return &ProcessStarter{}, nil
}

// 规格未给出时的默认值：RLIMIT_NOFILE 1024（Go 运行时会把自身软限调高，workload 不继承）；/tmp tmpfs 64 MiB。
const (
	defaultNoFile   = 1024
	defaultTmpBytes = 64 << 20
)

// launchSpecFor 由 provider.EnvSpec 构造启动进程转交给 init 的 LaunchSpec（规格 §4.5 的隔离配置）：
// GID 范围与 UID 范围相同；模板按名称解析（空为默认模板）；rlimit 与挂载来自 Limits/Mounts。
// 只做形状校验（rootfs.Template.Validate）；宿主路径是否存在由装配在启动时 Ensure。
// exec 环境的 /in 为 Mounts.In，未提供时为 provider 建立的 <dir>/in；/out 总是 <dir>/out 上的宿主 tmpfs（D1、D2）。
func launchSpecFor(spec provider.EnvSpec, dir string) (sandbox.LaunchSpec, error) {
	name := spec.Template
	if name == "" {
		name = rootfs.DefaultTemplateName
	}
	tpl, err := rootfs.ResolveTemplate(name)
	if err != nil {
		return sandbox.LaunchSpec{}, fmt.Errorf("local: 模板 %q: %w", name, err)
	}
	tmp := spec.Limits.TmpBytes
	if tmp <= 0 {
		tmp = defaultTmpBytes
	}
	nofile := spec.Limits.NoFile
	if nofile == 0 {
		nofile = defaultNoFile
	}
	in, out := spec.Mounts.In, ""
	if spec.Kind == provider.KindExec {
		if in == "" {
			in = inDir(spec, dir)
		}
		out = filepath.Join(dir, execOutName)
	}
	return sandbox.LaunchSpec{
		UIDBase: spec.UIDBase, UIDSize: spec.UIDSize,
		GIDBase: spec.UIDBase, GIDSize: spec.UIDSize,
		Hostname: envHostname,
		Init: sandbox.InitSpec{
			Kind: string(spec.Kind), Template: tpl, TmpBytes: tmp,
			Workspace: spec.Mounts.Workspace, GatewaySocket: spec.Mounts.GatewaySocket,
			In: in, Out: out, OutBytes: spec.Mounts.OutBytes,
			NoFile: nofile, FSize: spec.Limits.FSize,
		},
	}, nil
}

// checkReachable 确认 init 能够到达 p：init 在 user namespace 中以映射 root 运行，对宿主 root 所有的上级目录
// 只有"其他人"的权限，因此 p 的每个上级目录都须有 o+x。
func checkReachable(p string) error {
	for d := filepath.Dir(p); ; d = filepath.Dir(d) {
		fi, err := os.Stat(d)
		if err != nil {
			return fmt.Errorf("local: %s 的上级目录 %s: %w", p, d, err)
		}
		if fi.Mode().Perm()&0o001 == 0 {
			return fmt.Errorf("local: 上级目录 %s 的权限 %v 缺少 o+x，沙箱 init 无法到达 %s", d, fi.Mode().Perm(), p)
		}
		if d == "/" || d == "." {
			return nil
		}
	}
}

// workloadID 是沙箱内 workload 的映射 uid/gid（规格 §4.5；与 sandbox 中的取值相同）。
const workloadID = 1000

// prepareWorkspace 是规格 §4.5"宿主创建并 chown 到映射 UID"的宿主侧一步：把 workspace 连同其中全部
// 条目（不跟随符号链接）的属主改为本环境的映射 uid/gid 1000（宿主 UIDBase+1000），并确认 init 能够
// 到达它——init 在 user namespace 中以映射 root 运行，对宿主 root 所有的上级目录只有"其他人"的权限，
// 因此每个上级目录都须有 o+x（数据目录与 workspaces 目录由装配以 0711 建立）。
// 同一任务的后续 attempt 可能分到不同的 UID 范围，所以每次启动都重新 chown。
func prepareWorkspace(ws string, id uint32) error {
	if err := checkReachable(ws); err != nil {
		return err
	}
	err := filepath.WalkDir(ws, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Lchown(p, int(id), int(id))
	})
	if err != nil {
		return fmt.Errorf("local: chown workspace %s: %w", ws, err)
	}
	return nil
}

// prepareGatewaySocket 是 Gateway socket 的宿主侧一步（规格 §9.1）：把 Gateway 建立的 socket（0600）的属主
// 改为本环境的映射 uid/gid 1000（宿主 UIDBase+1000），init 挂载前核对"属主 uid 1000、0600 的 socket"。
// 只改 socket 本身（Lchown，不跟随符号链接）；路径不是 socket 时拒绝。上级目录的 o+x 由 init 的打开检查报告。
func prepareGatewaySocket(path string, id uint32) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("local: Gateway socket %s: %w", path, err)
	}
	if fi.Mode().Type() != fs.ModeSocket {
		return fmt.Errorf("local: Gateway socket %s 不是 socket（%v）", path, fi.Mode().Type())
	}
	if err := os.Lchown(path, int(id), int(id)); err != nil {
		return fmt.Errorf("local: chown Gateway socket %s: %w", path, err)
	}
	return nil
}

func isSubreaper() bool {
	var v int32
	_, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prGetChildSubreaper, uintptr(unsafe.Pointer(&v)), 0, 0, 0, 0)
	return e == 0 && v != 0
}

// StartInit 实现 EnvStarter。dir 是环境目录：exec 环境的 /in 暂存与 /out 挂载点在其下（Create 已建立）。
func (s *ProcessStarter) StartInit(ctx context.Context, spec provider.EnvSpec, dir string, cg *cgroup.Group) (*sandbox.Conn, int, error) {
	if !isSubreaper() {
		// 否则 init 会被收养到 server 之外，starter 无法收割它。
		return nil, 0, errors.New("local: 本进程不是 child subreaper（须经 NewProcessStarter 构造启动器）")
	}
	ls, err := launchSpecFor(spec, dir)
	if err != nil {
		return nil, 0, err
	}
	for _, p := range []string{ls.Init.In, ls.Init.Out} {
		if p == "" {
			continue
		}
		if err := checkReachable(p); err != nil {
			return nil, 0, err
		}
	}
	ls.Env, ls.FailAt = s.initEnv, s.failAt
	if spec.Mounts.Workspace != "" {
		if err := prepareWorkspace(spec.Mounts.Workspace, spec.UIDBase+workloadID); err != nil {
			return nil, 0, err
		}
	}
	if spec.Mounts.GatewaySocket != "" {
		if err := prepareGatewaySocket(spec.Mounts.GatewaySocket, spec.UIDBase+workloadID); err != nil {
			return nil, 0, err
		}
	}
	b, err := json.Marshal(ls)
	if err != nil {
		return nil, 0, err
	}

	var closers []io.Closer // 子端与临时 FD：Start 之后（或失败时）全部关闭
	closeAll := func() {
		for _, c := range closers {
			c.Close()
		}
		closers = nil
	}
	defer closeAll()

	ctl, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("local: 控制 socketpair: %w", err)
	}
	host := os.NewFile(uintptr(ctl[0]), "control-host")
	ctlChild := os.NewFile(uintptr(ctl[1]), "control-init")
	closers = append(closers, ctlChild)
	ok := false
	defer func() {
		if !ok {
			host.Close()
		}
	}()

	res, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("local: 结果 socketpair: %w", err)
	}
	resFD := res[0] // 阻塞 fd，只经 RecvLaunchResult 读取；收尾时关闭
	defer syscall.Close(resFD)
	resChild := os.NewFile(uintptr(res[1]), "launch-result")
	closers = append(closers, resChild)

	specR, specW, err := os.Pipe()
	if err != nil {
		return nil, 0, err
	}
	closers = append(closers, specR)
	_, werr := specW.Write(b) // 规格远小于管道缓冲区，不会阻塞
	specW.Close()
	if werr != nil {
		return nil, 0, fmt.Errorf("local: 写启动规格: %w", werr)
	}

	readyR, readyW, err := os.Pipe()
	if err != nil {
		return nil, 0, err
	}
	defer readyR.Close()
	closers = append(closers, readyW)

	cgdir, err := os.OpenFile(cg.Path(), os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("local: 打开环境 cgroup: %w", err)
	}
	closers = append(closers, cgdir)

	cmd := sandbox.LaunchCommand(sandbox.LaunchFiles{
		Control: ctlChild, Cgroup: cgdir, Spec: specR, Result: resChild, Ready: readyW,
	})
	if err := cmd.Start(); err != nil {
		return nil, 0, fmt.Errorf("local: 启动 sandbox-launch: %w", err)
	}
	// 关闭本进程持有的子端：启动进程与 init 退出后对端才能读到 EOF。
	closeAll()

	launcherDone := make(chan error, 1)
	go func() { launcherDone <- cmd.Wait() }()
	type result struct {
		pid, pidfd int
		err        error
	}
	results := make(chan result, 1)
	go func() {
		pid, pidfd, err := sandbox.RecvLaunchResult(resFD)
		results <- result{pid, pidfd, err}
	}()

	var r result
	select {
	case r = <-results:
	case <-ctx.Done():
		// 不再推进。shutdown 使启动进程交还结果失败（它随即杀死并收割 init）并唤醒接收；
		// 结果若已在途，init 已被收养：交给收割 goroutine，并杀死它（残留只可能在环境 cgroup 中）。
		_ = syscall.Shutdown(resFD, syscall.SHUT_RDWR)
		r = <-results
		<-launcherDone
		if r.err == nil {
			w := newInitWaiter(r.pidfd)
			w.kill()
		}
		return nil, 0, ctx.Err()
	}
	lerr := <-launcherDone // 启动进程交还结果后立即退出；收割它之后 init 已由本进程收养
	if r.err != nil {
		if errors.Is(r.err, io.EOF) {
			return nil, 0, fmt.Errorf("local: sandbox-launch 未交还结果即退出（%v）", lerr)
		}
		return nil, 0, fmt.Errorf("local: %w", r.err)
	}
	w := newInitWaiter(r.pidfd)

	readyc := make(chan error, 1)
	go func() { readyc <- readReady(readyR) }()
	select {
	case err := <-readyc:
		if err != nil {
			w.kill()
			return nil, 0, fmt.Errorf("local: init 未就绪: %w", err)
		}
	case <-ctx.Done():
		readyR.Close() // 唤醒读 goroutine；返回前它已结束
		<-readyc
		// init 留在环境 cgroup 中（关闭宿主端后它读到 EOF 退出），由 Stop 回收；收割仍由 w 负责。
		return nil, 0, ctx.Err()
	}
	ok = true
	return sandbox.NewConn(host, 64), r.pid, nil
}

// readReady 读取 init 的就绪通知：首字节为 sandbox.InitReadyByte 即就绪；否则读到 EOF，
// 以收到的文本（或"未就绪即退出"）作为原因。
func readReady(f *os.File) error {
	buf := make([]byte, 4096)
	n, err := f.Read(buf)
	if n > 0 && buf[0] == sandbox.InitReadyByte {
		return nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	reason := buf[:n]
	if err == nil {
		rest, rerr := io.ReadAll(io.LimitReader(f, 4096))
		if rerr != nil {
			return rerr
		}
		reason = append(reason, rest...)
	}
	if len(reason) == 0 {
		return errors.New("init 未通知就绪即退出")
	}
	return errors.New(string(reason))
}

// initWaiter 拥有 init 的 pidfd：一个 goroutine 以 waitid(P_PIDFD, WEXITED) 等待并收割 init，
// 之后关闭 pidfd。kill 与关闭互斥，pidfd 不会在关闭后被使用。
type initWaiter struct {
	mu     sync.Mutex
	pidfd  int
	closed bool
}

func newInitWaiter(pidfd int) *initWaiter {
	w := &initWaiter{pidfd: pidfd}
	go w.wait()
	return w
}

// pPidfd 是 waitid 的 idtype P_PIDFD。
const pPidfd = 3

func (w *initWaiter) wait() {
	var info [128]byte // siginfo_t
	for {
		_, _, e := syscall.Syscall6(syscall.SYS_WAITID, pPidfd, uintptr(w.pidfd), uintptr(unsafe.Pointer(&info[0])), syscall.WEXITED, 0, 0)
		if e != syscall.EINTR {
			if e != 0 {
				fmt.Fprintf(os.Stderr, "local: 收割 init: waitid: %v\n", e)
			}
			break
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	syscall.Close(w.pidfd)
	w.closed = true
}

func (w *initWaiter) kill() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		_ = sandbox.PidfdKill(w.pidfd)
	}
}
