//go:build linux

// Package runtime 负责沙箱进程的启动与其内部的 1 号进程。
package runtime

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// InitArg 是 re-exec 时传给自身的子命令名。
const InitArg = "init"

// envInitProbe 仅供测试用于识别被 re-exec 出来的那一侧。
const envInitProbe = "AGENTBOX_INIT_PROBE"

// envSandboxRoot 把新根目录传给 init 侧。
const envSandboxRoot = "AGENTBOX_ROOT"

// envSandboxHostname 把沙箱主机名传给 init 侧。
const envSandboxHostname = "AGENTBOX_HOSTNAME"

// controlFD 是控制连接在 init 侧的文件描述符号。
// 0/1/2 是标准流，ExtraFiles 的第一个元素落在 3。
const controlFD = 3

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
	ControlFD *os.File
}

// Spawn 以新的命名空间启动沙箱 init 进程。
//
// 启动的是 /proc/self/exe init——即本程序自身。这么做是因为
// pivot_root 一类调用是线程级的，而 goroutine 会在 OS 线程间迁移；
// 只有在 Go runtime 尚未铺开的新进程最早期执行它们才安全。
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
		// 父进程意外退出时，内核给 init 发 SIGKILL，避免沙箱变孤儿。
		Pdeathsig: syscall.SIGKILL,
	}
	cmd.Stdout = os.Stderr // init 自身的日志并入宿主 stderr，便于排障
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("启动沙箱 init: %w", err)
	}
	return cmd.Process, nil
}
