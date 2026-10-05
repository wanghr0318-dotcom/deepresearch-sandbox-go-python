//go:build linux

package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// 本文件是 stage-2 helper（`exec-stage2` 子命令，规格 §4.6 helper 段）：init 以
// execveat(helper_fd, "", AT_EMPTY_PATH) 启动它（rawfork_linux.go），它在 workload 的进程里逐步降权，最后
// execve workload。继承的 FD：0/1/2 是 workload 的 stdin/stdout/stderr，3 是 exec-status 管道的写端。
//
// 降权序列（逐条，顺序即规格）：
//
//	PR_SET_NAME → fd 3 设 FD_CLOEXEC、close_range(4, ~0, CLOSE_RANGE_CLOEXEC) → 清空 bounding
//	→ 设置并锁定 securebits 0x0f（必须在 setresuid 之前：切换 UID 后 CAP_SETPCAP 不可用）
//	→ setresgid/setresuid(1000) → 显式清空 eff/prm/inh 与 ambient
//	→ 显式设置 rlimit（RLIMIT_NOFILE、RLIMIT_CORE=0；exec 环境另 RLIMIT_FSIZE）
//	→ no_new_privs → seccomp（SECCOMP_FILTER_FLAG_TSYNC）→ execve workload
//
// 主 goroutine 锁定在一个 OS 线程上；身份与能力相关的调用作用于本进程的全部线程（syscall 包的
// all-threads 变体，要求 CGO_ENABLED=0）。任一步失败：向 fd 3 写 `helper/<步骤>: <原因>` 后以 126 退出，
// workload 不运行。execve 成功时内核按 FD_CLOEXEC 关闭 fd 3，init 读到 EOF 而没有字节（exec 提交点，见 launcher.go）。
//
// PR_SET_NAME 只用于诊断（ps 中可辨认）；init 的启动判定不看进程名。

// HelperArg 是 init 启动 stage-2 helper 时传给自身的子命令名（os.Args[1]）。
const HelperArg = "exec-stage2"

// helperComm 是 helper 的线程名（PR_SET_NAME，最长 15 字节）。
const helperComm = "agentbox-helper"

// helper 的步骤名；失败原因为 `helper/<步骤>: <原因>`。
const (
	hStepSpec         = "spec"
	hStepSetName      = "set_name"
	hStepFDHygiene    = "fd_hygiene"
	hStepDropBounding = "drop_bounding"
	hStepSecurebits   = "securebits"
	hStepSetresgid    = "setresgid"
	hStepSetresuid    = "setresuid"
	hStepClearCaps    = "clear_caps"
	hStepClearAmbient = "clear_ambient"
	hStepRlimit       = "rlimit"
	hStepNoNewPrivs   = "no_new_privs"
	hStepSeccomp      = "seccomp"
	hStepExecve       = "execve"
)

// helperSteps 是降权序列的 12 个步骤（即 12 个失败注入点），按执行顺序。
var helperSteps = []string{
	hStepSetName, hStepFDHygiene, hStepDropBounding, hStepSecurebits, hStepSetresgid, hStepSetresuid,
	hStepClearCaps, hStepClearAmbient, hStepRlimit, hStepNoNewPrivs, hStepSeccomp, hStepExecve,
}

// helper 的测试钩子（helperSpec.Hook；只由测试经 init 的 helperTestHook 设置，生产中恒为空）。
const (
	hookFailPrefix = "fail:"   // fail:<步骤>：在该步骤注入失败
	hookSigkill    = "sigkill" // 全部降权步骤之后、execve 之前 SIGKILL 自身
	hookPause      = "pause"   // execve 之前挂起（直到被杀）
	hookDelay      = "delay"   // execve 之前等待 helperHookDelay
)

const helperHookDelay = 500 * time.Millisecond

// prctl 选项与取值（linux/prctl.h、linux/securebits.h）。
const (
	prSetName         = 15
	prSetSecurebits   = 28
	prSetNoNewPrivs   = 38
	closeRangeCloexec = 1 << 2

	// workloadSecurebits = SECBIT_NOROOT | SECBIT_NOROOT_LOCKED | SECBIT_NO_SETUID_FIXUP | SECBIT_NO_SETUID_FIXUP_LOCKED。
	workloadSecurebits = 0x0f
)

// workloadNoFile 是 workload 的 RLIMIT_NOFILE（软、硬限相同）。Go 运行时把自身的软限调高到硬限，
// 不显式设置时 workload 会继承调高后的值（实验记录 R4）。
const workloadNoFile = 1024

// helperSpec 是 init 交给 helper 的输入（os.Args[2]，JSON）。
type helperSpec struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env,omitempty"`
	Dir  string   `json:"dir,omitempty"`
	// Kind 是环境类型：决定 seccomp 配置（exec → ProfileExec，其余 → ProfileOrchestrator）与是否设置 RLIMIT_FSIZE。
	Kind   string `json:"kind"`
	NoFile uint64 `json:"nofile"`
	FSize  uint64 `json:"fsize,omitempty"`
	Hook   string `json:"hook,omitempty"`
}

// sockFprog 与内核 struct sock_fprog 布局一致。
type sockFprog struct {
	Len    uint16
	_      [6]byte
	Filter *SockFilter
}

// RunHelper 是 stage-2 helper 的主函数（main 在做任何其他初始化之前分流到这里）。成功时 execve workload，
// 不返回；失败时向 fd 3 报告原因并以 126 退出，也不返回。
func RunHelper() {
	runtime.LockOSThread()

	var s helperSpec
	if len(os.Args) != 3 {
		helperFail(hStepSpec, fmt.Errorf("参数个数 %d，期望 3", len(os.Args)))
	}
	if err := json.Unmarshal([]byte(os.Args[2]), &s); err != nil {
		helperFail(hStepSpec, err)
	}
	if len(s.Argv) == 0 {
		helperFail(hStepSpec, errors.New("argv 为空"))
	}
	profile := ProfileOrchestrator
	switch s.Kind {
	case KindTask, KindSession:
	case KindExec:
		profile = ProfileExec
	default:
		helperFail(hStepSpec, fmt.Errorf("未知的环境类型 %q", s.Kind))
	}
	if s.NoFile == 0 {
		helperFail(hStepSpec, errors.New("nofile 为 0"))
	}

	step := func(name string, f func() error) {
		var err error
		if s.Hook == hookFailPrefix+name {
			err = errInjected
		} else {
			err = f()
		}
		if err != nil {
			helperFail(name, err)
		}
	}

	step(hStepSetName, func() error {
		name := append([]byte(helperComm), 0)
		return errnoErr(syscall.RawSyscall(syscall.SYS_PRCTL, prSetName, uintptr(unsafe.Pointer(&name[0])), 0))
	})
	step(hStepFDHygiene, func() error {
		// fd 3（exec-status）在 workload execve 成功时由内核关闭；4 及以上全部 close-on-exec。
		if _, err := fcntl(3, syscall.F_SETFD, syscall.FD_CLOEXEC); err != nil {
			return fmt.Errorf("fd 3: %w", err)
		}
		return errnoErr(syscall.RawSyscall(sysCloseRange, 4, uintptr(^uint32(0)), closeRangeCloexec))
	})
	step(hStepDropBounding, func() error {
		for n := 0; n < 64; n++ {
			if _, _, e := syscall.RawSyscall(syscall.SYS_PRCTL, prCapbsetRead, uintptr(n), 0); e != 0 {
				break // EINVAL：超过 CAP_LAST_CAP
			}
			if _, _, e := syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prCapbsetDrop, uintptr(n), 0); e != 0 {
				return fmt.Errorf("能力 %d: %w", n, e)
			}
		}
		return nil
	})
	step(hStepSecurebits, func() error {
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prSetSecurebits, workloadSecurebits, 0))
	})
	step(hStepSetresgid, func() error {
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_SETRESGID, workloadID, workloadID, workloadID))
	})
	step(hStepSetresuid, func() error {
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_SETRESUID, workloadID, workloadID, workloadID))
	})
	step(hStepClearCaps, func() error {
		// NO_SETUID_FIXUP 下切换 UID 不清空能力，必须显式清空。
		capHdr.Version, capHdr.Pid = linuxCapabilityVer3, 0
		capData = [capabilityU32sPerSet]struct{ Effective, Permitted, Inheritable uint32 }{}
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&capHdr)), uintptr(unsafe.Pointer(&capData[0])), 0))
	})
	step(hStepClearAmbient, func() error {
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prCapAmbient, prCapAmbientClearAll, 0))
	})
	step(hStepRlimit, func() error {
		// rlimit 是进程属性（不分线程）。syscall.Setrlimit 设置 NOFILE 后，Go 不再在 exec 时恢复其原值。
		set := func(what string, res int, v uint64) error {
			if err := syscall.Setrlimit(res, &syscall.Rlimit{Cur: v, Max: v}); err != nil {
				return fmt.Errorf("%s: %w", what, err)
			}
			return nil
		}
		if err := set("RLIMIT_NOFILE", syscall.RLIMIT_NOFILE, s.NoFile); err != nil {
			return err
		}
		if err := set("RLIMIT_CORE", syscall.RLIMIT_CORE, 0); err != nil {
			return err
		}
		if s.Kind == KindExec && s.FSize > 0 {
			return set("RLIMIT_FSIZE", syscall.RLIMIT_FSIZE, s.FSize)
		}
		return nil
	})
	step(hStepNoNewPrivs, func() error {
		return errnoErr(syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prSetNoNewPrivs, 1, 0))
	})
	step(hStepSeccomp, func() error { return installSeccomp(profile) })
	step(hStepExecve, func() error {
		switch s.Hook {
		case hookSigkill:
			// 测试：降权全部完成、execve 之前死亡（规格 §4.6 门槛 2 的 died_before_exec）。
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
		case hookPause:
			time.Sleep(time.Hour)
		case hookDelay:
			time.Sleep(helperHookDelay)
		}
		if s.Dir != "" {
			if err := syscall.Chdir(s.Dir); err != nil {
				return fmt.Errorf("chdir %s: %w", s.Dir, err)
			}
		}
		return execWorkload(s.Argv, s.Env)
	})
	// execWorkload 成功时不返回；失败已在 step 中报告并退出。
}

// helperFail 向 exec-status 管道（fd 3）报告失败并以 126 退出。原因不超过 PIPE_BUF，一次写入是原子的。
func helperFail(step string, err error) {
	msg := "helper/" + step + ": " + err.Error()
	if len(msg) > 4096 {
		msg = msg[:4096]
	}
	_, _ = syscall.Write(3, []byte(msg))
	os.Exit(126)
}

func errnoErr(_, _ uintptr, e syscall.Errno) error {
	if e != 0 {
		return e
	}
	return nil
}

// installSeccomp 生成 profile 的过滤器（Task 3 的 BuildFilter，clone3 → ENOSYS）并以 SECCOMP_FILTER_FLAG_TSYNC
// 安装到全部线程。调用前必须已设置 no_new_privs（本进程此时已无 CAP_SYS_ADMIN）。
func installSeccomp(profile SeccompProfile) error {
	arch, err := NativeArch()
	if err != nil {
		return err
	}
	prog, err := BuildFilter(profile, arch)
	if err != nil {
		return err
	}
	fprog := sockFprog{Len: uint16(len(prog)), Filter: &prog[0]}
	const setModeFilter, flagTsync = 1, 1
	r, _, e := syscall.RawSyscall(sysSeccomp, setModeFilter, flagTsync, uintptr(unsafe.Pointer(&fprog)))
	runtime.KeepAlive(prog)
	if e != 0 {
		return e
	}
	if r != 0 {
		return fmt.Errorf("TSYNC 未能同步线程 %d", r)
	}
	return nil
}

// execWorkload 以 execve 执行 argv：argv[0] 含 "/" 时直接执行，否则按 env 中的 PATH（缺省
// /usr/local/bin:/usr/bin:/bin）逐个目录尝试（execvp 语义）。只在失败时返回。
func execWorkload(argv, env []string) error {
	name := argv[0]
	if strings.Contains(name, "/") {
		return syscall.Exec(name, argv, env)
	}
	path := "/usr/local/bin:/usr/bin:/bin"
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			path = v
		}
	}
	var denied error
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			dir = "."
		}
		err := syscall.Exec(dir+"/"+name, argv, env)
		switch {
		case errors.Is(err, syscall.ENOENT), errors.Is(err, syscall.ENOTDIR):
		case errors.Is(err, syscall.EACCES):
			if denied == nil {
				denied = err
			}
		default:
			return err
		}
	}
	if denied != nil {
		return fmt.Errorf("%s: %w", name, denied)
	}
	return fmt.Errorf("%s: 在 PATH 中找不到: %w", name, syscall.ENOENT)
}
