//go:build linux

// Package sandbox 负责沙箱进程的启动与其内部的 1 号进程。
package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// InitArg 是 re-exec 时传给自身的子命令名。
const InitArg = "init"

// envSandboxRoot 把新根目录传给 init 侧。
const envSandboxRoot = "AGENTBOX_ROOT"

// envSandboxHostname 把沙箱主机名传给 init 侧。
const envSandboxHostname = "AGENTBOX_HOSTNAME"

// CloneFlags 是沙箱进程要进入的命名空间集合。
//
// 不含 CLONE_NEWUSER：M1 以 root 运行，rootless 是后续待定项。
// 其中 CLONE_NEWPID 尤为关键——它不只隔离进程视图，更提供了一个
// 可靠的“整组杀”边界：杀掉该 namespace 的 1 号进程，内核会连带
// 清理其中所有进程，孙子进程也跑不掉。
const CloneFlags = syscall.CLONE_NEWNS |
	syscall.CLONE_NEWPID |
	syscall.CLONE_NEWUTS |
	syscall.CLONE_NEWIPC |
	syscall.CLONE_NEWNET

// SpawnConfig 是启动一个沙箱 init 进程所需的参数。
type SpawnConfig struct {
	// Root 是 overlayfs 的 merged 目录，将成为沙箱的新根。
	Root string
	// Hostname 是沙箱内的主机名。
	Hostname string
	// ControlFD 是 socketpair 的沙箱侧端点，会作为 fd 3 传给 init。
	//
	// 所有权约定：Spawn 只负责把它挂到子进程的 ExtraFiles 上，
	// 不会关闭调用方传入的这一份。子进程 fork 时会复制一份 fd，
	// 所以 Spawn 返回后，调用方必须自行关闭 ControlFD 持有的这一端。
	// 这不是可选的清理动作，而是协议的一部分：Task 8 会用这条
	// socketpair 检测子进程断开——判定方式是读到 EOF，而只要宿主
	// 侧还留着一份未关闭的沙箱侧端点，内核就不会认为该端已经
	// 没有写者，EOF 也就永远不会出现。调用方的标准写法是
	// Spawn(...) 后紧跟一次 sandboxFD.Close()。
	ControlFD *os.File
}

// Spawn 以新的命名空间启动沙箱 init 进程。
//
// 启动的是 /proc/self/exe init——即本程序自身。这么做是因为
// pivot_root 一类调用是线程级的，而 goroutine 会在 OS 线程间迁移；
// 只有在 Go runtime 尚未铺开的新进程最早期执行它们才安全。
//
// 返回值是裸的 *os.Process，不是 *exec.Cmd：Spawn 内部不保留对
// 子进程的引用，也不会替调用方 Wait() 它。调用方必须自己在某个
// 时机 Wait() 这个 Process（成功也好失败也好），否则子进程退出后
// 会在宿主上留下僵尸进程，仅凭之后收割孤儿是弥补不了这个责任的——
// 那处理的是沙箱内部的孙子进程，不是这个直接子进程本身。
//
// ControlFD 的关闭责任在调用方，见 SpawnConfig.ControlFD 的注释。
func Spawn(cfg SpawnConfig) (*os.Process, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("SpawnConfig.Root 不能为空")
	}
	if cfg.ControlFD == nil {
		return nil, fmt.Errorf("SpawnConfig.ControlFD 不能为空")
	}

	cmd := exec.Command("/proc/self/exe", InitArg)
	cmd.Env = []string{
		envSandboxRoot + "=" + cfg.Root,
		envSandboxHostname + "=" + cfg.Hostname,
	}
	cmd.ExtraFiles = []*os.File{cfg.ControlFD}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: CloneFlags,
		// 单独的进程组，便于在 init 尚未就绪时也能整组发信号。
		Setpgid: true,
		// 注意 PR_SET_PDEATHSIG 的真实语义是线程级的，不是进程级的：
		// 内核会在“创建这个子进程的那个 OS 线程”终止时发信号，而不是
		// 等宿主整个进程退出。Go 标准库文档原话是
		// "the signal is sent on thread termination, which may happen
		// before the process terminates"——两者并不等价，一条线程可以
		// 先于进程整体退出而消失（比如被运行时回收）。
		// cmd.Start() 内部的 fork/exec 跑在 Go runtime 当前调度到的
		// 某个 OS 线程上；本调用路径下没有手工 LockOSThread 或提前
		// 让该线程退出的逻辑，因此该线程的生命周期与宿主进程一致，
		// Pdeathsig 在这里实践上等效于“父进程退出即 SIGKILL”。
		// 这是当前调用方式下的经验事实，不是内核保证——后续任务如果
		// 在 Spawn 附近引入并发（例如把 fork/exec 挪到某个可能提前
		// 退出的 goroutine/线程上），这个等效关系就可能失效，届时
		// 需要重新核实。
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stderr // init 自身的日志并入宿主 stderr，便于排障
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("启动沙箱 init: %w", err)
	}
	return cmd.Process, nil
}
