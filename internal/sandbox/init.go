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
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
)

// 环境类型（与 provider.EnvKind 的取值一致；sandbox 不依赖 provider 包）。
const (
	KindTask    = "task"
	KindSession = "session"
	KindExec    = "exec"
)

// InitSpec 是 init 建立环境所需的输入（规格 §4.5），随 LaunchSpec 经 InitSpecFD 传给 init。
// 路径都是宿主路径；init 在挂载之前以 O_PATH 固定它们。
type InitSpec struct {
	Kind     string          `json:"kind"`
	Template rootfs.Template `json:"template"`
	// TmpBytes 是 /tmp（编排环境另有 /run）tmpfs 的限额，必须为正（tmpfs 的 size=0 表示不限）。
	TmpBytes int64 `json:"tmp_bytes"`
	// 编排环境（task、session）：workspace 目录挂到 /workspace（可写，nosuid、nodev）；
	// Gateway socket 挂到 /run/agentbox/gateway.sock（须属主为映射 uid 1000、0600）。均可为空。
	Workspace     string `json:"workspace,omitempty"`
	GatewaySocket string `json:"gateway_socket,omitempty"`
	// exec 环境：/in 只读输入目录（可为空）；/out tmpfs 的大小（必须为正，nr_inodes=1024）。
	In       string `json:"in,omitempty"`
	OutBytes int64  `json:"out_bytes,omitempty"`
	// NoFile 是 workload 的 RLIMIT_NOFILE（软、硬限相同），0 由 validate 填为 workloadNoFile；FSize 是 exec 环境 workload 的 RLIMIT_FSIZE，
	// 0 表示由 init 取可写 tmpfs（/tmp、/out）中最大者。二者来自 provider.Limits（规格 §4.5）。
	NoFile uint64 `json:"nofile"`
	FSize  uint64 `json:"fsize,omitempty"`
}

func (s *InitSpec) validate() error {
	if err := s.Template.Validate(); err != nil {
		return fmt.Errorf("rootfs 模板: %w", err)
	}
	if s.TmpBytes <= 0 {
		return fmt.Errorf("tmp_bytes 必须为正（实际 %d）", s.TmpBytes)
	}
	if s.NoFile == 0 {
		s.NoFile = workloadNoFile // 规格 §4.5 的默认值；launcher 据此设置 RLIMIT_NOFILE
	}
	abs := func(what, p string) error {
		if p != "" && (!filepath.IsAbs(p) || filepath.Clean(p) != p) {
			return fmt.Errorf("%s %q 不是规范的绝对路径", what, p)
		}
		return nil
	}
	switch s.Kind {
	case KindTask, KindSession:
		if s.In != "" || s.OutBytes != 0 {
			return fmt.Errorf("%s 环境不能有 /in 或 /out", s.Kind)
		}
		if err := abs("workspace", s.Workspace); err != nil {
			return err
		}
		return abs("gateway_socket", s.GatewaySocket)
	case KindExec:
		if s.Workspace != "" || s.GatewaySocket != "" {
			return errors.New("exec 环境不能有 workspace 或 Gateway socket")
		}
		if s.OutBytes <= 0 {
			return fmt.Errorf("exec 环境的 out_bytes 必须为正（实际 %d）", s.OutBytes)
		}
		return abs("in", s.In)
	default:
		return fmt.Errorf("未知的环境类型 %q", s.Kind)
	}
}

// init 环境建立的步骤名；失败原因为 `init/<步骤>: <原因>`（实验记录 §8）。
const (
	stepControlFD      = "control_fd"
	stepSpec           = "spec"
	stepHostname       = "sethostname"
	stepMountPrivate   = "mount_private"
	stepMountRootfs    = "mount_rootfs"
	stepMountIn        = "mount_in"
	stepMountTmpfs     = "mount_tmpfs"
	stepMountDev       = "mount_dev"
	stepMountGateway   = "mount_gateway"
	stepMountWorkspace = "mount_workspace"
	stepMountProc      = "mount_proc"
	stepMaskProc       = "mask_proc"
	stepOpenSelf       = "open_self"
	stepPivotRoot      = "pivot_root"
	stepUmountOldroot  = "umount_oldroot"
	stepInitCaps       = "init_caps"
)

// 控制 socket 与 helper 二进制 fd 移到的最低编号：高于 workload 启动时 dup3 覆盖的 0..3，
// 也高于继承的 3..5。
const (
	controlFDMin = 100
	helperFDMin  = 200
)

// stepErr 给错误标上产生它的步骤。
type stepErr struct {
	step string
	err  error
}

func (e *stepErr) Error() string { return e.step + ": " + e.err.Error() }
func (e *stepErr) Unwrap() error { return e.err }

// initFailAt 是测试钩子：在该步骤注入失败。只由测试设置（生产二进制中恒为空）。
var initFailAt string

var errInjected = errors.New("注入的失败")

func checkFailpoint(step string) error {
	if initFailAt != "" && initFailAt == step {
		return &stepErr{step, errInjected}
	}
	return nil
}

// initEnv 是环境建立的结果：init 此后持有的两个 FD（均为 close-on-exec 且在高位）。
type initEnv struct {
	control  *os.File
	helperFD int      // stage-2 helper 二进制（/proc/self/exe）的 O_PATH fd，供生产 Launcher 使用
	spec     InitSpec // 已校验的环境输入（生产 Launcher 据此选择 seccomp 配置与 rlimit）
}

// RunInit 是沙箱内 1 号进程的主函数（规格 §4.6 init 段）。
//
// 调用时机：进程由专用启动进程（RunLaunch）以 `/proc/self/exe init` 启动，main 在做任何其他初始化
// 之前就分流到这里。继承的 FD：InitControlFD（控制 socket）、InitReadyFD（就绪管道写端）、
// InitSpecFD（LaunchSpec 的 JSON，读到 EOF）。
//
// 环境建立完成后向就绪管道写入 InitReadyByte 并关闭它，然后进入 Serve，直到宿主关闭控制连接；
// 任一步失败则把 `init/<步骤>: <原因>` 写入就绪管道（init_err，不经控制 socket）并返回该错误，
// 此时没有进入 Serve，不会运行任何 workload。
func RunInit() error {
	syscall.CloseOnExec(InitReadyFD)
	env, err := establish()
	if err != nil {
		reason := "init/" + err.Error()
		_, _ = syscall.Write(InitReadyFD, []byte(reason))
		syscall.Close(InitReadyFD)
		return errors.New(reason)
	}
	if _, err := syscall.Write(InitReadyFD, []byte{InitReadyByte}); err != nil {
		return fmt.Errorf("init/ready: %w", err)
	}
	syscall.Close(InitReadyFD)
	conn := NewConn(env.control, 64)
	reg := NewRegistry(func(m Message) error { return conn.Send(m, nil) })
	return Serve(context.Background(), conn, reg, newHelperLauncher(env.helperFD, env.spec))
}

// establish 按规格 §4.6 的 init 段逐步建立环境。返回的错误都是 *stepErr。
func establish() (initEnv, error) {
	// 控制 socket 立即移到高位并带上 close-on-exec（ExtraFiles 继承时不带 FD_CLOEXEC）。
	ctl, err := fcntl(InitControlFD, syscall.F_DUPFD_CLOEXEC, controlFDMin)
	if err != nil {
		return initEnv{}, &stepErr{stepControlFD, err}
	}
	syscall.Close(InitControlFD)
	control := os.NewFile(uintptr(ctl), "control")
	ok := false
	defer func() {
		if !ok {
			control.Close()
		}
	}()

	spec, err := readInitSpec()
	if err != nil {
		return initEnv{}, &stepErr{stepSpec, err}
	}
	if spec.Hostname != "" {
		if err := syscall.Sethostname([]byte(spec.Hostname)); err != nil {
			return initEnv{}, &stepErr{stepHostname, err}
		}
	}
	logf := func(format string, a ...any) { fmt.Fprintf(os.Stderr, "sandbox init: "+format+"\n", a...) }
	if err := setupMounts(&spec.Init, checkFailpoint, logf); err != nil {
		return initEnv{}, err
	}

	// helper 二进制：pivot_root 之前打开（之后旧根已脱离），移到高位，close-on-exec。
	if err := checkFailpoint(stepOpenSelf); err != nil {
		return initEnv{}, err
	}
	fd, err := syscall.Open("/proc/self/exe", oPath|syscall.O_CLOEXEC, 0)
	if err != nil {
		return initEnv{}, &stepErr{stepOpenSelf, err}
	}
	helper, err := fcntl(fd, syscall.F_DUPFD_CLOEXEC, helperFDMin)
	syscall.Close(fd)
	if err != nil {
		return initEnv{}, &stepErr{stepOpenSelf, err}
	}
	defer func() {
		if !ok {
			syscall.Close(helper)
		}
	}()

	if err := pivotRoot(checkFailpoint); err != nil {
		return initEnv{}, err
	}

	if err := checkFailpoint(stepInitCaps); err != nil {
		return initEnv{}, err
	}
	if err := applyCaps(initCaps); err != nil {
		return initEnv{}, &stepErr{stepInitCaps, err}
	}
	ok = true
	return initEnv{control: control, helperFD: helper, spec: spec.Init}, nil
}

// readInitSpec 从 InitSpecFD 读取 LaunchSpec（读到 EOF 后关闭），校验其中的 InitSpec。
func readInitSpec() (LaunchSpec, error) {
	f := os.NewFile(InitSpecFD, "init-spec")
	if f == nil {
		return LaunchSpec{}, errors.New("缺少启动规格 fd")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxLaunchMsg+1))
	f.Close()
	if err != nil {
		return LaunchSpec{}, fmt.Errorf("读取启动规格: %w", err)
	}
	if len(b) > maxLaunchMsg {
		return LaunchSpec{}, fmt.Errorf("启动规格超过 %d 字节", maxLaunchMsg)
	}
	var s LaunchSpec
	if err := json.Unmarshal(b, &s); err != nil {
		return LaunchSpec{}, fmt.Errorf("解码启动规格: %w", err)
	}
	if err := s.Init.validate(); err != nil {
		return LaunchSpec{}, err
	}
	return s, nil
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
//     Start 回 start_err）。l 实现 committer（生产 Launcher）时改经 reg.startCommit：
//     持锁 clone 并登记"启动中"，按 exec 提交点在锁外判定后投递（launcher.go）。
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
// Reap、Start 或判定方报告的控制通道故障 → 该错误。Serve 返回前终止判定尚未完成的启动
// （reg.abort，不投递消息）、关闭 conn、停止所有 grace 计时器；环境内残留的进程由 cgroup 回收（规格 §4.4）。
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
		// 判定尚未完成的启动：终止其进程组、清理登记，不投递任何消息（规格 §4.6 门槛 2）。
		reg.abort(func(format string, a ...any) { fmt.Fprintf(os.Stderr, "sandbox init: "+format+"\n", a...) })
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
		case err := <-reg.errc:
			return err // 判定方投递失败：控制通道故障
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
				if c, ok := l.(committer); ok {
					// 生产 Launcher：exec 提交点判定（launcher.go），判定在锁外、不阻塞本循环。
					err := reg.startCommit(r.m.ExecID, func() (int, *os.File, error) {
						if err := gl.check(r.m.Spec, fds[0], fds[1], fds[2]); err != nil {
							return 0, nil, err
						}
						return c.spawn(r.m.Spec, fds[0], fds[1], fds[2])
					})
					closeFiles(fds[:])
					if err != nil {
						return err
					}
				} else if err := reg.Start(r.m.ExecID, gl, r.m.Spec, fds); err != nil {
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
	if err := g.check(spec, stdin, stdout, stderr); err != nil {
		return 0, err
	}
	return g.l.Launch(spec, stdin, stdout, stderr)
}

// check 是 Launch 与 exec 提交点启动路径共用的检查。调用方持有 reg.mu（Registry.Start 与 startCommit
// 都在锁内调用），这里可以直接读登记表；判定尚未完成的启动同样算已登记。
func (g guardLauncher) check(spec json.RawMessage, stdin, stdout, stderr *os.File) error {
	if _, ok := g.reg.pidOfLocked(g.execID); ok {
		return errors.New(ReasonDuplicateExecID)
	}
	if _, ok := g.reg.starting[g.execID]; ok {
		return errors.New(ReasonDuplicateExecID)
	}
	dec := json.NewDecoder(bytes.NewReader(spec))
	dec.DisallowUnknownFields()
	var s StartSpec
	if err := dec.Decode(&s); err != nil {
		return fmt.Errorf("%s: %v", reasonInvalidSpec, err)
	}
	if dec.More() {
		return fmt.Errorf("%s: spec 之后有多余数据", reasonInvalidSpec)
	}
	if len(s.Argv) == 0 {
		return fmt.Errorf("%s: argv 为空", reasonInvalidSpec)
	}
	for _, f := range []*os.File{stdin, stdout, stderr} {
		if err := setBlocking(f); err != nil {
			return err
		}
	}
	return nil
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
