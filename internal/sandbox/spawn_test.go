//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/rootfs"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// TestMain 让这个测试二进制在被以 "init" 或 "sandbox-launch" re-exec 时，表现得
// 跟生产的 /proc/self/exe 一样：直接跑 RunInit / RunLaunch 并按其错误退出，
// 而不是钻进 go test 自己的框架（LaunchCommand 硬编码 re-exec /proc/self/exe，
// 从测试里调用时那就是这个测试二进制自己）。
func TestMain(m *testing.M) {
	// §16.2 检查器分流（在沙箱内作为 workload 运行，见 Task 12 一节）：必须先于任何打开文件的操作（FD 快照）。
	if len(os.Args) > 1 && os.Args[1] == isolationCheckArg {
		os.Exit(runIsolationCheck())
	}
	// stage-2 helper 分流（init 以 execveat 启动本测试二进制）：成功时 execve workload，不返回。
	if len(os.Args) > 1 && os.Args[1] == HelperArg {
		RunHelper()
		os.Exit(126)
	}
	// seccomp 子进程分流：见 TestSeccompOrchestratorFilterInChildProcess。
	// 放在 TestMain 而不是一个“非子进程时跳过”的辅助用例里，是因为
	// linux-integration 不允许出现任何 skip。
	if os.Getenv(envSeccompChild) == "1" {
		os.Exit(runSeccompChild())
	}
	// reaper 子进程分流：见 runReaperCase。
	if name := os.Getenv(envReaperChild); name != "" {
		os.Exit(runReaperChild(name))
	}
	// init 服务循环子进程分流：见 runServeCase。
	if name := os.Getenv(envServeChild); name != "" {
		os.Exit(runServeChild(name))
	}
	if len(os.Args) > 1 && os.Args[1] == InitArg {
		// init 的测试钩子（只在测试构建中可设置）：经 LaunchSpec.Env 传入的环境变量。
		initFailAt = os.Getenv(envTestInitFail)
		forceClassicMounts = os.Getenv(envTestInitClassic) == "1"
		helperTestHook = os.Getenv(envTestHelperHook)
		if os.Getenv(envTestLockProbe) == "1" {
			startLockProbe = lockProbe
		}
		if err := RunInit(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == LaunchArg {
		if err := RunLaunch(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// startLaunch 以 LaunchCommand 启动一个专用启动进程（本测试二进制），传入给定的启动规格，
// 返回它交还的结果与退出状态。cgroup 目录用一个临时目录代替：spec 校验失败时启动进程
// 不会走到 clone。
func startLaunch(t *testing.T, spec []byte) (pid, pidfd int, resErr, waitErr error) {
	t.Helper()
	ctl, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctlHost, ctlChild := os.NewFile(uintptr(ctl[0]), "ctl-host"), os.NewFile(uintptr(ctl[1]), "ctl-child")
	defer ctlHost.Close()
	res, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(res[0])
	resChild := os.NewFile(uintptr(res[1]), "res-child")
	specR, specW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := specW.Write(spec); err != nil {
		t.Fatal(err)
	}
	specW.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	cgdir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cmd := LaunchCommand(LaunchFiles{Control: ctlChild, Cgroup: cgdir, Spec: specR, Result: resChild, Ready: readyW})
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*os.File{ctlChild, cgdir, specR, resChild, readyW} {
		f.Close()
	}
	pid, pidfd, resErr = RecvLaunchResult(res[0])
	return pid, pidfd, resErr, cmd.Wait()
}

// TestLaunchRejectsUnsafeSpec：启动规格中 uid/gid 范围基址为 0（会把命名空间 root 映射为宿主 root）、
// 范围为空或越界时，启动进程经结果 socket 报告 launch/spec 失败并以非零码退出，不交还 pidfd。
// 校验发生在 setgroups 与 clone 之前，不需要 root。
func TestLaunchRejectsUnsafeSpec(t *testing.T) {
	for name, s := range map[string]LaunchSpec{
		"uid 基址 0": {UIDBase: 0, UIDSize: 4096, GIDBase: 100000, GIDSize: 4096},
		"gid 基址 0": {UIDBase: 100000, UIDSize: 4096, GIDBase: 0, GIDSize: 4096},
		"uid 范围为空": {UIDBase: 100000, UIDSize: 0, GIDBase: 100000, GIDSize: 4096},
		"gid 范围越界": {UIDBase: 100000, UIDSize: 4096, GIDBase: 1<<32 - 10, GIDSize: 4096},
		"未知注入点":    {UIDBase: 100000, UIDSize: 4096, GIDBase: 100000, GIDSize: 4096, FailAt: "nope"},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			_, pidfd, resErr, waitErr := startLaunch(t, b)
			var le *LaunchError
			if !errors.As(resErr, &le) || !strings.HasPrefix(le.Reason, "launch/spec: ") {
				if pidfd >= 0 {
					syscall.Close(pidfd)
				}
				t.Fatalf("结果 = %v，期望 launch/spec 失败", resErr)
			}
			var ee *exec.ExitError
			if !errors.As(waitErr, &ee) || ee.ExitCode() == 0 {
				t.Fatalf("启动进程退出 = %v，期望非零退出码", waitErr)
			}
		})
	}
}

// TestMainDispatchesInitToRunInit 覆盖 cmd/agentbox/main.go 里的
// os.Args[1] == "init" 分流：构建真正的 agentbox 二进制，直接
// `agentbox init` 执行它，证明这条 argv 分流确实通到了
// runSandboxInit() -> sandbox.RunInit()。
//
// 不需要 root：直接执行时没有继承的控制 socket（fd 3），RunInit 在第一步
// （init/control_fd）就失败，不触及命名空间与挂载。
func TestMainDispatchesInitToRunInit(t *testing.T) {
	repoRoot := repoRootDir(t)

	binPath := filepath.Join(t.TempDir(), "agentbox")
	build := exec.Command(goBinary(), "build", "-o", binPath, "./cmd/agentbox")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("构建 agentbox 失败: %v\n输出:\n%s", err, out)
	}

	cmd := exec.Command(binPath, InitArg)
	out, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("agentbox init 应以非零码退出，err=%v\n输出:\n%s", err, out)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("agentbox init 退出码 = %d, want 1\n输出:\n%s", exitErr.ExitCode(), out)
	}
	if !strings.Contains(string(out), "init/"+stepControlFD+": ") {
		t.Fatalf("输出未包含 init/%s 的失败原因，实际输出:\n%s", stepControlFD, out)
	}

	// sandbox-launch 分流：没有继承的 FD 时 RunLaunch 立即失败（不触及凭据与命名空间）。
	cmd = exec.Command(binPath, LaunchArg)
	out, err = cmd.CombinedOutput()
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("agentbox sandbox-launch 应以退出码 1 失败，err=%v\n输出:\n%s", err, out)
	}
	if !strings.Contains(string(out), "sandbox-launch:") {
		t.Fatalf("输出未包含 sandbox-launch 的失败原因，实际输出:\n%s", out)
	}
}

// repoRootDir 返回仓库根目录。go test 把工作目录设为包所在目录
// （internal/sandbox），仓库根就是它的上两级。
func repoRootDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取工作目录: %v", err)
	}
	return filepath.Join(wd, "..", "..")
}

// goBinary 尽量找到 go 工具链：优先用 PATH 里的 go，非交互 shell
// （PATH 里没有 go）时退回本项目环境里的已知安装路径。
func goBinary() string {
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	return "/usr/local/go/bin/go"
}

// ---------------------------------------------------------------------------
// seccomp 配置（规格 §4.5）
// ---------------------------------------------------------------------------

// seccompTestNrs 是测试侧独立抄写的系统调用号，故意不复用 seccomp.go 里的
// 表：两边按同一份内核头文件各抄一遍，任何一边抄错都会让规则用例失败。
// 来源：amd64 为 arch/x86/entry/syscalls/syscall_64.tbl（64 位 ABI），
// arm64 为 include/uapi/asm-generic/unistd.h。
// TestSeccompSyscallNumbersMatchStdlib 再用标准库 syscall.SYS_* 交叉核对其中
// 本机架构上标准库有定义的部分。
var seccompTestNrs = map[Arch]map[string]uint32{
	ArchAMD64: {
		"read": 0, "getpid": 39, "socket": 41, "clone": 56, "ptrace": 101,
		"pivot_root": 155, "mount": 165, "umount2": 166, "init_module": 175,
		"delete_module": 176, "kexec_load": 246, "add_key": 248, "request_key": 249,
		"keyctl": 250, "unshare": 272, "perf_event_open": 298, "open_by_handle_at": 304,
		"setns": 308, "process_vm_readv": 310, "process_vm_writev": 311,
		"finit_module": 313, "kexec_file_load": 320, "bpf": 321, "userfaultfd": 323,
		"io_uring_setup": 425, "io_uring_enter": 426, "io_uring_register": 427,
		"clone3": 435,
	},
	ArchARM64: {
		"read": 63, "getpid": 172, "socket": 198, "clone": 220, "ptrace": 117,
		"pivot_root": 41, "mount": 40, "umount2": 39, "init_module": 105,
		"delete_module": 106, "kexec_load": 104, "add_key": 217, "request_key": 218,
		"keyctl": 219, "unshare": 97, "perf_event_open": 241, "open_by_handle_at": 265,
		"setns": 268, "process_vm_readv": 270, "process_vm_writev": 271,
		"finit_module": 273, "kexec_file_load": 294, "bpf": 280, "userfaultfd": 282,
		"io_uring_setup": 425, "io_uring_enter": 426, "io_uring_register": 427,
		"clone3": 435,
	},
}

// seccompDeniedEPERM 是规格 §4.5 中无条件返回 EPERM 的系统调用。
var seccompDeniedEPERM = []string{
	"mount", "umount2", "pivot_root", "unshare", "setns", "ptrace",
	"process_vm_readv", "process_vm_writev", "bpf", "perf_event_open",
	"userfaultfd", "io_uring_setup", "io_uring_enter", "io_uring_register",
	"keyctl", "add_key", "request_key", "open_by_handle_at", "init_module",
	"finit_module", "delete_module", "kexec_load", "kexec_file_load",
}

// 测试侧独立写出的内核常量（include/uapi/linux/audit.h、seccomp.h、
// sched.h、socket.h、asm-generic/errno-base.h、errno.h）。
const (
	tAuditArchX86_64  = 0xC000003E
	tAuditArchI386    = 0x40000003
	tAuditArchAArch64 = 0xC00000B7
	tAuditArchARM     = 0x40000028
	tX32SyscallBit    = 0x40000000

	tRetKillProcess = 0x80000000
	tRetAllow       = 0x7FFF0000
	tRetErrnoEPERM  = 0x00050000 | 1
	tRetErrnoENOSYS = 0x00050000 | 38

	tAFUnix    = 1
	tAFInet    = 2
	tAFNetlink = 16
	tAFPacket  = 17
	tAFInet6   = 10
)

// seccompData 对应内核 struct seccomp_data（include/uapi/linux/seccomp.h）：
// nr@0、arch@4、instruction_pointer@8、args[6]@16。
type seccompData struct {
	nr   uint32
	arch uint32
	ip   uint64
	args [6]uint64
}

// loadWord 按小端布局（x86_64 与 aarch64 都是小端）取 seccomp_data 中
// 偏移 off 处的 32 位字；非对齐或越界的偏移返回 false。
func (d seccompData) loadWord(off uint32) (uint32, bool) {
	switch {
	case off == 0:
		return d.nr, true
	case off == 4:
		return d.arch, true
	case off == 8:
		return uint32(d.ip), true
	case off == 12:
		return uint32(d.ip >> 32), true
	case off >= 16 && off < 64 && off%4 == 0:
		a := d.args[(off-16)/8]
		if (off-16)%8 == 0 {
			return uint32(a), true
		}
		return uint32(a >> 32), true
	}
	return 0, false
}

// runSeccompBPF 是一个最小 classic-BPF 解释器，只支持本过滤器用到的指令：
// BPF_LD|BPF_W|BPF_ABS、BPF_JMP|{BPF_JEQ,BPF_JGE,BPF_JSET}|BPF_K、
// BPF_RET|BPF_K。遇到其他指令、越界跳转或程序在 RET 之前结束都判为失败，
// 这同时校验了程序结构（内核校验器也会拒绝这些情况）。
func runSeccompBPF(t *testing.T, prog []SockFilter, d seccompData) uint32 {
	t.Helper()
	if len(prog) == 0 || len(prog) > 4096 { // BPF_MAXINSNS
		t.Fatalf("程序长度 %d 不合法", len(prog))
	}
	var acc uint32
	for pc := 0; pc < len(prog); {
		ins := prog[pc]
		switch ins.Code {
		case 0x20: // BPF_LD(0x00)|BPF_W(0x00)|BPF_ABS(0x20)
			v, ok := d.loadWord(ins.K)
			if !ok {
				t.Fatalf("pc=%d: 非法的 seccomp_data 偏移 %d", pc, ins.K)
			}
			acc = v
			pc++
		case 0x05 | 0x10, 0x05 | 0x30, 0x05 | 0x40: // BPF_JMP|{JEQ,JGE,JSET}|BPF_K
			var cond bool
			switch ins.Code &^ 0x07 {
			case 0x10:
				cond = acc == ins.K
			case 0x30:
				cond = acc >= ins.K
			case 0x40:
				cond = acc&ins.K != 0
			}
			off := int(ins.Jf)
			if cond {
				off = int(ins.Jt)
			}
			pc += 1 + off
		case 0x06: // BPF_RET|BPF_K
			return ins.K
		default:
			t.Fatalf("pc=%d: 解释器不支持的指令 code=%#x", pc, ins.Code)
		}
	}
	t.Fatalf("程序在 RET 之前结束（跳出末尾）")
	return 0
}

// TestSeccompFilterRules 用解释器对两套配置、两种架构逐条核对规格 §4.5 的规则。
// exec 配置“在 orchestrator 基础上不额外放宽”，因此两者跑同一张期望表。
func TestSeccompFilterRules(t *testing.T) {
	type arch struct {
		name       string
		arch       Arch
		auditArch  uint32
		foreignABI []uint32 // 必须被杀死的其他 audit arch
	}
	arches := []arch{
		{"amd64", ArchAMD64, tAuditArchX86_64, []uint32{tAuditArchI386, tAuditArchAArch64, tAuditArchARM}},
		{"arm64", ArchARM64, tAuditArchAArch64, []uint32{tAuditArchARM, tAuditArchX86_64, tAuditArchI386}},
	}
	profiles := map[string]SeccompProfile{"orchestrator": ProfileOrchestrator, "exec": ProfileExec}

	for pname, profile := range profiles {
		for _, a := range arches {
			nrs := seccompTestNrs[a.arch]
			t.Run(pname+"/"+a.name, func(t *testing.T) {
				prog, err := BuildFilter(profile, a.arch)
				if err != nil {
					t.Fatalf("BuildFilter: %v", err)
				}
				call := func(name string, args ...uint64) seccompData {
					nr, ok := nrs[name]
					if !ok {
						t.Fatalf("测试表缺少 %s", name)
					}
					d := seccompData{nr: nr, arch: a.auditArch, ip: 0x400000}
					copy(d.args[:], args)
					return d
				}
				expect := func(what string, d seccompData, want uint32) {
					t.Helper()
					if got := runSeccompBPF(t, prog, d); got != want {
						t.Errorf("%s: 动作 = %#x, want %#x", what, got, want)
					}
				}

				// 默认允许。
				expect("read", call("read", 0, 0, 0), tRetAllow)
				expect("getpid", call("getpid"), tRetAllow)

				// 显式拒绝列表 → EPERM。
				for _, name := range seccompDeniedEPERM {
					expect(name, call(name), tRetErrnoEPERM)
				}

				// clone：带任一 CLONE_NEW* → EPERM；线程/fork 式的普通 clone → 允许。
				newFlags := map[string]uint64{
					"CLONE_NEWNS": 0x00020000, "CLONE_NEWCGROUP": 0x02000000,
					"CLONE_NEWUTS": 0x04000000, "CLONE_NEWIPC": 0x08000000,
					"CLONE_NEWUSER": 0x10000000, "CLONE_NEWPID": 0x20000000,
					"CLONE_NEWNET": 0x40000000,
				}
				const sigchld = 17
				for fname, f := range newFlags {
					expect("clone "+fname, call("clone", f|sigchld), tRetErrnoEPERM)
				}
				// 高 32 位垃圾不能掩盖低位的 CLONE_NEWUSER。
				expect("clone CLONE_NEWUSER+高位", call("clone", 0xFFFFFFFF00000000|0x10000000), tRetErrnoEPERM)
				// Go/glibc 创建线程用的标志组合（CLONE_VM|FS|FILES|SIGHAND|SYSVSEM|THREAD|SETTLS|PARENT_SETTID|CHILD_CLEARTID）。
				expect("clone 线程", call("clone", 0x003D0F00), tRetAllow)
				expect("clone fork", call("clone", sigchld), tRetAllow)

				// socket：仅 AF_UNIX 允许。
				expect("socket AF_UNIX", call("socket", tAFUnix, 1, 0), tRetAllow)
				for fname, fam := range map[string]uint64{"AF_INET": tAFInet, "AF_INET6": tAFInet6, "AF_NETLINK": tAFNetlink, "AF_PACKET": tAFPacket} {
					expect("socket "+fname, call("socket", fam, 1, 0), tRetErrnoEPERM)
				}
				// family 是 int，内核只看低 32 位：高位垃圾不能让 AF_INET 绕过。
				expect("socket AF_INET+高位", call("socket", 0xFFFFFFFF00000000|tAFInet, 1, 0), tRetErrnoEPERM)

				// clone3 → ENOSYS（让 glibc 回退到 clone）。
				expect("clone3", call("clone3", 0, 88), tRetErrnoENOSYS)

				// 非原生 ABI → KILL_PROCESS，无论调用号是什么（包括本会被允许的 read）。
				for _, foreign := range a.foreignABI {
					for _, name := range []string{"read", "socket", "clone3"} {
						d := call(name)
						d.arch = foreign
						expect(fmt.Sprintf("arch %#x %s", foreign, name), d, tRetKillProcess)
					}
				}
				if a.arch == ArchAMD64 {
					// x32：arch 仍是 AUDIT_ARCH_X86_64，但调用号带 __X32_SYSCALL_BIT。
					for _, name := range []string{"read", "socket", "mount"} {
						d := call(name)
						d.nr |= tX32SyscallBit
						expect("x32 "+name, d, tRetKillProcess)
					}
				}
			})
		}
	}
}

// TestSeccompBuildFilterRejectsUnknownInputs 确认未知配置或架构返回错误而不是
// 生成一个默认允许的空过滤器。
func TestSeccompBuildFilterRejectsUnknownInputs(t *testing.T) {
	if _, err := BuildFilter(SeccompProfile(99), ArchAMD64); err == nil {
		t.Errorf("未知配置应返回错误")
	}
	if _, err := BuildFilter(ProfileOrchestrator, Arch(99)); err == nil {
		t.Errorf("未知架构应返回错误")
	}
	native, err := NativeArch()
	if err != nil {
		t.Fatalf("NativeArch: %v", err)
	}
	want := map[string]Arch{"amd64": ArchAMD64, "arm64": ArchARM64}[runtime.GOARCH]
	if native != want {
		t.Errorf("NativeArch() = %d, want %d（GOARCH=%s）", native, want, runtime.GOARCH)
	}
}

// TestSeccompSyscallNumbersMatchStdlib 用标准库 syscall.SYS_* 交叉核对测试表
// 中本机架构的调用号（只核对两种架构的标准库都定义了的那部分）。
func TestSeccompSyscallNumbersMatchStdlib(t *testing.T) {
	native, err := NativeArch()
	if err != nil {
		t.Fatalf("NativeArch: %v", err)
	}
	std := map[string]uintptr{
		"read": syscall.SYS_READ, "getpid": syscall.SYS_GETPID, "socket": syscall.SYS_SOCKET,
		"clone": syscall.SYS_CLONE, "ptrace": syscall.SYS_PTRACE, "pivot_root": syscall.SYS_PIVOT_ROOT,
		"mount": syscall.SYS_MOUNT, "umount2": syscall.SYS_UMOUNT2, "init_module": syscall.SYS_INIT_MODULE,
		"delete_module": syscall.SYS_DELETE_MODULE, "kexec_load": syscall.SYS_KEXEC_LOAD,
		"add_key": syscall.SYS_ADD_KEY, "request_key": syscall.SYS_REQUEST_KEY,
		"keyctl": syscall.SYS_KEYCTL, "unshare": syscall.SYS_UNSHARE,
		"perf_event_open": syscall.SYS_PERF_EVENT_OPEN,
	}
	for name, want := range std {
		if got := seccompTestNrs[native][name]; uintptr(got) != want {
			t.Errorf("%s: 测试表 %d, 标准库 %d", name, got, want)
		}
	}
}

// envSeccompChild 让测试二进制在 TestMain 中直接进入 runSeccompChild。
const envSeccompChild = "AGENTBOX_TEST_SECCOMP_CHILD"

// TestSeccompOrchestratorFilterInChildProcess 在子进程里真实安装 orchestrator
// 配置并发起系统调用，验证内核执行的结果与规则一致。只验证配置内容；
// 安装时机与 TSYNC 属于 Plan 1B。不需要 root（先设置 no_new_privs）。
func TestSeccompOrchestratorFilterInChildProcess(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envSeccompChild+"=1")
	out, err := cmd.CombinedOutput()
	t.Logf("子进程输出:\n%s", out)
	if err != nil {
		t.Fatalf("子进程失败: %v", err)
	}
	if !strings.Contains(string(out), "seccomp child: ok") {
		t.Fatalf("子进程没有报告完成")
	}
}

// runSeccompChild 在当前线程上设置 no_new_privs、安装过滤器，再在同一线程上
// 发起系统调用（未用 TSYNC，过滤器只作用于本线程，因此全程锁定 OS 线程）。
// 每项期望都选在“没有过滤器时结果不同”的调用上：unshare(0) 与
// socket(AF_INET) 本会成功，clone3(NULL, 0) 本会返回 EINVAL。
func runSeccompChild() int {
	runtime.LockOSThread()

	fail := func(format string, args ...any) int {
		fmt.Printf("seccomp child: FAIL: "+format+"\n", args...)
		return 1
	}
	native, err := NativeArch()
	if err != nil {
		return fail("NativeArch: %v", err)
	}
	prog, err := BuildFilter(ProfileOrchestrator, native)
	if err != nil {
		return fail("BuildFilter: %v", err)
	}
	kprog := make([]syscall.SockFilter, len(prog))
	for i, ins := range prog {
		kprog[i] = syscall.SockFilter{Code: ins.Code, Jt: ins.Jt, Jf: ins.Jf, K: ins.K}
	}
	fprog := syscall.SockFprog{Len: uint16(len(kprog)), Filter: &kprog[0]}

	// 对照：安装过滤器之前先以同样的参数调用每个探针，记录未过滤的结果。
	probes := seccompDenyProbes(native)
	isRoot := os.Geteuid() == 0
	control := make([]syscall.Errno, len(probes))
	for i, p := range probes {
		control[i] = p.call()
	}

	const prSetNoNewPrivs = 38 // include/uapi/linux/prctl.h
	if _, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetNoNewPrivs, 1, 0, 0, 0, 0); e != 0 {
		return fail("PR_SET_NO_NEW_PRIVS: %v", e)
	}
	// seccomp(2) 调用号：arch/x86/entry/syscalls/syscall_64.tbl 317；asm-generic/unistd.h 277。
	sysSeccomp := map[Arch]uintptr{ArchAMD64: 317, ArchARM64: 277}[native]
	const seccompSetModeFilter = 1 // include/uapi/linux/seccomp.h
	if _, _, e := syscall.RawSyscall(sysSeccomp, seccompSetModeFilter, 0, uintptr(unsafe.Pointer(&fprog))); e != 0 {
		return fail("seccomp(SECCOMP_SET_MODE_FILTER): %v", e)
	}
	runtime.KeepAlive(kprog)

	if pid, _, e := syscall.RawSyscall(syscall.SYS_GETPID, 0, 0, 0); e != 0 || int(pid) != os.Getpid() {
		return fail("getpid = %d, %v", pid, e)
	}
	if _, _, e := syscall.RawSyscall(syscall.SYS_UNSHARE, 0, 0, 0); e != syscall.EPERM {
		return fail("unshare(0) errno = %v, want EPERM", e)
	}
	if fd, _, e := syscall.RawSyscall(syscall.SYS_SOCKET, syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0); e != syscall.EPERM {
		if e == 0 {
			syscall.Close(int(fd))
		}
		return fail("socket(AF_INET) errno = %v, want EPERM", e)
	}
	fd, _, e := syscall.RawSyscall(syscall.SYS_SOCKET, syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if e != 0 {
		return fail("socket(AF_UNIX) errno = %v, want 成功", e)
	}
	syscall.Close(int(fd))
	const sysClone3 = 435 // 两种架构相同
	if _, _, e := syscall.RawSyscall(sysClone3, 0, 0, 0); e != syscall.ENOSYS {
		return fail("clone3 errno = %v, want ENOSYS", e)
	}

	// 拒绝列表逐项：过滤后必须是 EPERM；对照必须不是 EPERM（证明 EPERM 来自
	// 过滤器而不是权限不足），也不能是 ENOSYS（ENOSYS 说明调用号不存在或该功能
	// 未编入内核，都无法证明调用号正确）。需要特权的探针在非 root 下对照本就是
	// EPERM，只在 root 下判定对照。唯一例外是 configOptional 的探针：其功能
	// 可能未编入内核（例如 WSL2 内核未开 CONFIG_KEXEC，kexec_load 走 sys_ni
	// 返回 ENOSYS），此时只记录“无法区分”，调用号由 TestSeccompSyscallNumbersMatchStdlib
	// 与 Go 自带的 x/sys/unix 表交叉核对。
	failed := 0
	for i, p := range probes {
		filtered := p.call()
		verdict := "ok"
		switch {
		case filtered != syscall.EPERM:
			verdict = "FAIL: 过滤后不是 EPERM"
		case control[i] == syscall.ENOSYS && p.configOptional:
			verdict = "无法区分：功能未编入本内核（ENOSYS）"
		case control[i] == syscall.ENOSYS:
			verdict = "FAIL: 对照为 ENOSYS，无法区分"
		case control[i] == syscall.EPERM && (isRoot || !p.privileged):
			verdict = "FAIL: 对照也是 EPERM，无法区分"
		case control[i] == syscall.EPERM:
			verdict = "需 root 对照（本次非 root）"
		}
		if strings.HasPrefix(verdict, "FAIL") {
			failed++
		}
		fmt.Printf("probe %-28s nr=%-4d filtered=%-8s control=%-8s %s\n",
			p.name, p.nr, errnoName(filtered), errnoName(control[i]), verdict)
	}
	if failed > 0 {
		return fail("%d 个拒绝列表探针未通过", failed)
	}
	fmt.Printf("seccomp child: ok (root=%v; getpid ok, unshare EPERM, socket(AF_INET) EPERM, socket(AF_UNIX) ok, clone3 ENOSYS, %d deny probes)\n", isRoot, len(probes))
	return 0
}

// seccompProbe 是拒绝列表中一个系统调用的真实调用，参数故意无效且无害：
// 未过滤时内核返回 EPERM 以外的错误（EINVAL、EBADF、EFAULT 等），因此
// 过滤后的 EPERM 只可能来自过滤器。调用号取自 seccomp.go 的生产表，
// 由此在真实内核上核对这些调用号。
type seccompProbe struct {
	name string
	nr   uint32
	args [6]uintptr
	// privileged 表示内核在检查参数之前先检查能力，非 root 时对照必然是 EPERM。
	privileged bool
	// configOptional 表示该调用依赖可能未编入内核的配置项（返回 ENOSYS）。
	configOptional bool
}

func (p seccompProbe) call() syscall.Errno {
	r, _, e := syscall.Syscall6(uintptr(p.nr), p.args[0], p.args[1], p.args[2], p.args[3], p.args[4], p.args[5])
	if e == 0 && strings.HasPrefix(p.name, "socket") {
		syscall.Close(int(r))
	}
	return e
}

func errnoName(e syscall.Errno) string {
	switch e {
	case 0:
		return "success"
	case syscall.EPERM:
		return "EPERM"
	case syscall.EINVAL:
		return "EINVAL"
	case syscall.EBADF:
		return "EBADF"
	case syscall.EFAULT:
		return "EFAULT"
	case syscall.ESRCH:
		return "ESRCH"
	case syscall.ENOSYS:
		return "ENOSYS"
	case syscall.ENOEXEC:
		return "ENOEXEC"
	case syscall.EOPNOTSUPP:
		return "EOPNOTSUPP"
	}
	return fmt.Sprintf("errno%d", int(e))
}

// seccompDenyProbes 列出拒绝列表每一项（含 clone 的每个 CLONE_NEW* 与
// socket 的若干非 AF_UNIX 地址族）的无害无效调用。
func seccompDenyProbes(arch Arch) []seccompProbe {
	nrs := map[Arch]map[string]uint32{ArchAMD64: syscallNrAMD64, ArchARM64: syscallNrARM64}[arch]
	const bad = ^uintptr(0) // -1：无效 fd / 无效 flags
	pid := uintptr(os.Getpid())
	mk := func(name, syscallName string, priv bool, args ...uintptr) seccompProbe {
		p := seccompProbe{name: name, nr: nrs[syscallName], privileged: priv}
		copy(p.args[:], args)
		return p
	}
	// 先查 CAP_SYS_BOOT；root 下未知 flags → EINVAL。依赖 CONFIG_KEXEC。
	kexecLoad := mk("kexec_load", "kexec_load", true, 0, 0, 0, 0xFFFFFFFF)
	kexecLoad.configOptional = true
	probes := []seccompProbe{
		mk("mount", "mount", false, 0, 0, 0, 0, 0),                                  // dir_name=NULL → EFAULT（能力检查在路径查找之后）
		mk("umount2", "umount2", false, 0, 0xFFFFFFFF),                              // 未知 flags → EINVAL
		mk("pivot_root", "pivot_root", true, 0, 0),                                  // 先查 may_mount；root 下 NULL 路径 → EFAULT
		mk("unshare", "unshare", false, 0x1),                                        // CSIGNAL 位不是合法的 unshare 标志 → EINVAL
		mk("setns", "setns", false, bad, 0),                                         // fd=-1 → EBADF
		mk("ptrace", "ptrace", false, 3, 0, 0, 0),                                   // PTRACE_PEEKUSR, pid=0 → ESRCH
		mk("process_vm_readv", "process_vm_readv", false, pid, 0, 0, 0, 0, 1),       // flags≠0 → EINVAL
		mk("process_vm_writev", "process_vm_writev", false, pid, 0, 0, 0, 0, 1),     // flags≠0 → EINVAL
		mk("bpf", "bpf", false, 1000, 0, 0),                                         // 未知 cmd / NULL attr
		mk("perf_event_open", "perf_event_open", false, 0, 0, bad, bad, 0xFFFFFFFF), // 未知 flags → EINVAL
		mk("userfaultfd", "userfaultfd", false, 0x1|0x2),                            // UFFD_USER_MODE_ONLY 通过权限闸门，0x2 未知 → EINVAL
		mk("io_uring_setup", "io_uring_setup", false, 0, 0),                         // entries=0 / params=NULL
		mk("io_uring_enter", "io_uring_enter", false, bad, 0, 0, 0, 0, 0),           // fd=-1 → EBADF
		mk("io_uring_register", "io_uring_register", false, bad, 0, 0, 0),           // fd=-1 → EBADF
		mk("keyctl", "keyctl", false, 0xFFFF, 0, 0, 0, 0),                           // 未知操作 → EOPNOTSUPP
		mk("add_key", "add_key", false, 0, 0, 0, 0, 0),                              // type=NULL → EFAULT
		mk("request_key", "request_key", false, 0, 0, 0, 0),                         // type=NULL → EFAULT
		mk("open_by_handle_at", "open_by_handle_at", true, bad, 0, 0),               // 先查 CAP_DAC_READ_SEARCH
		mk("init_module", "init_module", true, 0, 0, 0),                             // 先查 CAP_SYS_MODULE；root 下 len=0
		mk("finit_module", "finit_module", true, bad, 0, 0),                         // 先查 CAP_SYS_MODULE；root 下 fd=-1
		mk("delete_module", "delete_module", true, 0, 0),                            // 先查 CAP_SYS_MODULE；root 下 NULL 名字 → EFAULT
		kexecLoad,
		mk("kexec_file_load", "kexec_file_load", true, bad, bad, 0, 0, 0xFFFFFFFF), // 先查 CAP_SYS_BOOT；root 下未知 flags → EINVAL
	}
	// clone：每个 CLONE_NEW* 搭配 CLONE_THREAD 而不带 CLONE_SIGHAND，内核
	// 在创建任何东西之前就返回 EINVAL，未过滤时也不会真的 fork。
	const cloneThread = 0x00010000
	for _, f := range []struct {
		name string
		flag uintptr
	}{
		{"NEWNS", 0x00020000}, {"NEWCGROUP", 0x02000000}, {"NEWUTS", 0x04000000},
		{"NEWIPC", 0x08000000}, {"NEWUSER", 0x10000000}, {"NEWPID", 0x20000000},
		{"NEWNET", 0x40000000},
	} {
		probes = append(probes, mk("clone(CLONE_"+f.name+")", "clone", false, f.flag|cloneThread, 0, 0, 0, 0))
	}
	// socket：非 AF_UNIX 地址族配无效 type（99 ≥ SOCK_MAX）→ EINVAL。
	for _, f := range []struct {
		name string
		fam  uintptr
	}{{"AF_INET", syscall.AF_INET}, {"AF_INET6", syscall.AF_INET6}, {"AF_NETLINK", syscall.AF_NETLINK}} {
		probes = append(probes, mk("socket("+f.name+")", "socket", false, f.fam, 99, 0))
	}
	return probes
}

// ---------------------------------------------------------------------------
// 控制通道（规格 §4.2）
// ---------------------------------------------------------------------------

// controlPair 返回一对 SOCK_SEQPACKET socket 的两端文件。两端都是非阻塞的，
// 与宿主侧的用法一致（init 侧继承到的是阻塞 fd，见 TestControlBlockingFDCloseUnblocksRecv）。
func controlPair(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	return os.NewFile(uintptr(fds[0]), "control-a"), os.NewFile(uintptr(fds[1]), "control-b")
}

// controlConnPair 返回两端 Conn，测试结束时关闭。
func controlConnPair(t *testing.T, queueLen int) (*Conn, *Conn) {
	t.Helper()
	a, b := controlPair(t)
	ca, cb := NewConn(a, queueLen), NewConn(b, queueLen)
	t.Cleanup(func() { ca.Close(); cb.Close() })
	return ca, cb
}

// controlPipes 创建 n 个管道，返回读端与写端；测试结束时全部关闭。
func controlPipes(t *testing.T, n int) (rs, ws []*os.File) {
	t.Helper()
	for i := 0; i < n; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}
		rs, ws = append(rs, r), append(ws, w)
	}
	t.Cleanup(func() {
		for _, f := range append(append([]*os.File{}, rs...), ws...) {
			f.Close()
		}
	})
	return rs, ws
}

// countFDs 返回本进程当前打开的 fd 数量。
func countFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("读取 /proc/self/fd: %v", err)
	}
	return len(ents)
}

// rawFd 取出文件的 fd 而不改变其阻塞模式（os.File.Fd 会把它切成阻塞）。
func rawFd(t *testing.T, f *os.File) int {
	t.Helper()
	sc, err := f.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var fd int
	if err := sc.Control(func(p uintptr) { fd = int(p) }); err != nil {
		t.Fatalf("Control: %v", err)
	}
	return fd
}

// rawSend 绕过 Conn 的发送端校验，直接向 socket 写一帧（可附带 fd）。
func rawSend(t *testing.T, sock *os.File, frame []byte, files []*os.File) {
	t.Helper()
	var oob []byte
	if len(files) > 0 {
		fds := make([]int, len(files))
		for i, f := range files {
			fds[i] = rawFd(t, f)
		}
		oob = syscall.UnixRights(fds...)
	}
	if err := syscall.Sendmsg(rawFd(t, sock), frame, oob, nil, 0); err != nil {
		t.Fatalf("sendmsg: %v", err)
	}
}

func controlStartMsg() Message {
	return Message{Type: "start", ExecID: "e1", Spec: json.RawMessage(`{"argv":["true"]}`)}
}

// TestControlStartRoundTripPassesThreeFDs：start 与三个管道 FD 在同一帧中送达，
// 接收端 FD 带 FD_CLOEXEC，经收到的 FD 写入的数据能从对应管道读出。
func TestControlStartRoundTripPassesThreeFDs(t *testing.T) {
	host, child := controlConnPair(t, 8)
	rs, ws := controlPipes(t, 3)

	if err := host.Send(controlStartMsg(), ws); err != nil {
		t.Fatalf("Send(start): %v", err)
	}
	m, files, err := child.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	if m.Type != "start" || m.ExecID != "e1" || string(m.Spec) != `{"argv":["true"]}` {
		t.Fatalf("收到 %+v", m)
	}
	if len(files) != 3 {
		t.Fatalf("收到 %d 个 FD, want 3", len(files))
	}
	for i, f := range files {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(rawFd(t, f)), syscall.F_GETFD, 0)
		if errno != 0 {
			t.Fatalf("F_GETFD: %v", errno)
		}
		if flags&syscall.FD_CLOEXEC == 0 {
			t.Errorf("FD %d 未带 FD_CLOEXEC（未使用 MSG_CMSG_CLOEXEC）", i)
		}
		want := fmt.Sprintf("via-fd-%d", i)
		if _, err := f.Write([]byte(want)); err != nil {
			t.Fatalf("经收到的 FD %d 写入: %v", i, err)
		}
		buf := make([]byte, 64)
		n, err := rs[i].Read(buf)
		if err != nil || string(buf[:n]) != want {
			t.Fatalf("管道 %d 读出 %q, %v; want %q", i, buf[:n], err, want)
		}
	}

	// 非 start 消息往返，确认字段完整。
	ack := Message{Type: "exit", ExecID: "e1", Exit: &ExitInfo{Code: 3, Signal: syscall.SIGKILL}}
	if err := child.Send(ack, nil); err != nil {
		t.Fatalf("Send(exit): %v", err)
	}
	got, gotFiles, err := host.Recv()
	if err != nil || len(gotFiles) != 0 {
		t.Fatalf("Recv(exit): %v, %d 个 FD", err, len(gotFiles))
	}
	if got.Type != "exit" || got.Exit == nil || *got.Exit != *ack.Exit {
		t.Fatalf("收到 %+v", got)
	}
}

// TestControlSenderRejectsInvalidMessages：发送端拒绝 spec 超 32 KiB、帧超 64 KiB、
// start 不带恰好 3 个 FD、非 start 带 FD；全部为 ErrProtocol，且什么都没发出去。
func TestControlSenderRejectsInvalidMessages(t *testing.T) {
	host, child := controlConnPair(t, 8)
	_, ws := controlPipes(t, 3)

	bigSpec := json.RawMessage(`"` + strings.Repeat("x", 32*1024) + `"`)
	cases := []struct {
		name  string
		m     Message
		files []*os.File
	}{
		{"spec 超 32 KiB", Message{Type: "start", ExecID: "e1", Spec: bigSpec}, ws},
		{"帧超 64 KiB", Message{Type: "start_err", ExecID: "e1", Reason: strings.Repeat("r", 64*1024)}, nil},
		{"start 带 2 个 FD", controlStartMsg(), ws[:2]},
		{"start 不带 FD", controlStartMsg(), nil},
		{"非 start 带 FD", Message{Type: "start_ack", ExecID: "e1", PID: 7}, ws},
		{"未知类型", Message{Type: "bogus", ExecID: "e1"}, nil},
	}
	for _, c := range cases {
		if err := host.Send(c.m, c.files); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: Send = %v, want ErrProtocol", c.name, err)
		}
	}

	// 对端什么都没收到：之后发出的第一条合法消息就是它读到的第一帧。
	if err := host.Send(Message{Type: "terminate", ExecID: "marker"}, nil); err != nil {
		t.Fatalf("Send(terminate): %v", err)
	}
	m, _, err := child.Recv()
	if err != nil || m.ExecID != "marker" {
		t.Fatalf("Recv = %+v, %v; want 只有 marker", m, err)
	}
}

// TestControlReceiverRejectsViolations：接收端遇到超长帧（MSG_TRUNC）、start 带 2 个 FD、
// 非 start 带 FD、start 带 4 或 5 个 FD（后者触发 MSG_CTRUNC）时返回 ErrProtocol，并关闭全部已收 FD。
func TestControlReceiverRejectsViolations(t *testing.T) {
	startFrame, _ := json.Marshal(controlStartMsg())
	ackFrame, _ := json.Marshal(Message{Type: "start_ack", ExecID: "e1", PID: 7})
	cases := []struct {
		name  string
		frame []byte
		nfds  int
	}{
		{"超长帧", append([]byte(`{"type":"start_err","exec_id":"e1","reason":"`), append(make([]byte, 70*1024), '"', '}')...), 0},
		{"start 带 2 个 FD", startFrame, 2},
		{"start 带 4 个 FD", startFrame, 4},
		{"start 带 5 个 FD（超出接收缓冲区，MSG_CTRUNC）", startFrame, 5},
		{"非 start 带 FD", ackFrame, 1},
		{"超长帧且带 FD", append([]byte(`{"type":"start","exec_id":"e1","spec":"`), append(make([]byte, 70*1024), '"', '}')...), 3},
		{"非法 JSON 且带 FD", []byte("{not json"), 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a, b := controlPair(t)
			recv := NewConn(b, 4)
			t.Cleanup(func() { recv.Close(); a.Close() })
			_, ws := controlPipes(t, c.nfds)

			before := countFDs(t)
			rawSend(t, a, c.frame, ws)
			m, files, err := recv.Recv()
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("Recv = %+v, %d 个 FD, %v; want ErrProtocol", m, len(files), err)
			}
			if len(files) != 0 {
				t.Fatalf("违规时仍返回了 %d 个 FD", len(files))
			}
			if after := countFDs(t); after != before {
				t.Fatalf("/proc/self/fd 数量 %d → %d：收到的 FD 未全部关闭", before, after)
			}
		})
	}
}

// TestControlQueueFullReturnsErrQueueFull：对端不读，socket 缓冲区写满后写 goroutine 阻塞，
// 队列随之写满，Send 不阻塞而是返回 ErrQueueFull。
func TestControlQueueFullReturnsErrQueueFull(t *testing.T) {
	host, _ := controlConnPair(t, 2)
	m := Message{Type: "terminate", ExecID: strings.Repeat("q", 1024)}
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; ; i++ {
		err := host.Send(m, nil)
		if errors.Is(err, ErrQueueFull) {
			return
		}
		if err != nil {
			t.Fatalf("第 %d 次 Send: %v", i, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("发送 %d 条后仍未出现 ErrQueueFull", i)
		}
	}
}

// TestControlPeerCloseGivesEOF：对端关闭后 Recv 返回 io.EOF。
func TestControlPeerCloseGivesEOF(t *testing.T) {
	host, child := controlConnPair(t, 4)
	if err := host.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := child.Recv(); err != io.EOF {
		t.Fatalf("Recv = %v, want io.EOF", err)
	}
}

// TestControlBlockingFDCloseUnblocksRecv：init 侧经 ExtraFiles 继承的是阻塞 fd；
// 阻塞在 Recv 中的读者在本端 Close 后必须返回，且 Send 返回错误。
func TestControlBlockingFDCloseUnblocksRecv(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	peer := os.NewFile(uintptr(fds[0]), "peer")
	defer peer.Close()
	c := NewConn(os.NewFile(uintptr(fds[1]), "init-end"), 4)

	done := make(chan error, 1)
	go func() { _, _, err := c.Recv(); done <- err }()
	time.Sleep(50 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close 后 Recv 返回 nil 错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close 后 Recv 仍阻塞")
	}
	if err := c.Send(Message{Type: "terminate", ExecID: "e1"}, nil); err == nil {
		t.Fatal("Close 后 Send 返回 nil")
	}
}

// ---------------------------------------------------------------------------
// 登记表与单一 reaper（规格 §4.3）
// ---------------------------------------------------------------------------

// envReaperChild 让测试二进制在 TestMain 中直接进入 runReaperChild。
//
// 每个 reaper 用例都在 re-exec 出来的子进程里运行：Reap 调用 Wait4(-1)，
// 会收割本进程的任何子进程，并且用例要把进程设为 child subreaper；放在
// 独立进程里，这两件事都不会影响同一测试二进制中的其他用例（它们自己
// exec.Cmd.Wait 子进程）。
const envReaperChild = "AGENTBOX_TEST_REAPER_CHILD"

// reaperChildren 是子进程侧的用例表；返回 nil 表示通过。
var reaperChildren = map[string]func() error{
	"order":  reaperChildOrder,
	"eintr":  reaperChildEINTR,
	"orphan": reaperChildOrphan,
	"outerr": reaperChildOutErr,
	// Task 11：exec 提交点的边界分类（见 TestCommitBoundaryClassification）。
	"commit-reap-first-signal": commitChildReapFirstSignal,
	"commit-reap-first-126":    commitChildReapFirst126,
	"commit-eof-first":         commitChildEOFFirst,
}

// runReaperCase 以 envReaperChild=name re-exec 测试二进制，要求子进程报告通过。
func runReaperCase(t *testing.T, name string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envReaperChild+"="+name)
	out, err := cmd.CombinedOutput()
	t.Logf("子进程输出:\n%s", out)
	if err != nil {
		t.Fatalf("子进程失败: %v", err)
	}
	if !strings.Contains(string(out), "reaper child: ok") {
		t.Fatalf("子进程没有报告完成")
	}
}

// runReaperChild 把本进程设为 child subreaper（被收养的孤儿归本进程收割，
// 与沙箱内的 init 同样处境；不需要 root），然后运行用例 name。
func runReaperChild(name string) int {
	fn, ok := reaperChildren[name]
	if !ok {
		fmt.Printf("reaper child: FAIL: 未知用例 %q\n", name)
		return 1
	}
	const prSetChildSubreaper = 36 // include/uapi/linux/prctl.h
	if _, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); e != 0 {
		fmt.Printf("reaper child: FAIL: PR_SET_CHILD_SUBREAPER: %v\n", e)
		return 1
	}
	if err := fn(); err != nil {
		fmt.Printf("reaper child: FAIL: %s: %v\n", name, err)
		return 1
	}
	fmt.Printf("reaper child: ok (%s)\n", name)
	return 0
}

// TestReaperStartAckPrecedesExit：立即退出的进程也是先 start_ack 后 exit（重复 200 次）。
func TestReaperStartAckPrecedesExit(t *testing.T) { runReaperCase(t, "order") }

// TestReaperRetriesEINTR：Wait4 返回 EINTR 时重试，一次 Reap 收割全部已退出进程。
func TestReaperRetriesEINTR(t *testing.T) { runReaperCase(t, "eintr") }

// TestReaperDropsAdoptedOrphan：双重 fork 的孙进程被收割，但不产生 exit。
func TestReaperDropsAdoptedOrphan(t *testing.T) { runReaperCase(t, "orphan") }

// TestReaperOutErrorPropagates：out 返回错误时 Start 与 Reap 返回该错误。
func TestReaperOutErrorPropagates(t *testing.T) { runReaperCase(t, "outerr") }

// execLauncher 是测试用 Launcher：spec 为 {"argv":[...]}，直接 exec，不建
// namespace、不降权。只调用 exec.Cmd.Start（经 File.Fd() 把收到的 FD 置为阻塞），
// 然后 Release，不等待进程。pids 非 nil 时记录每次启动的 pid。
type execLauncher struct {
	pids *[]int
}

func (l execLauncher) Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (int, error) {
	var s struct {
		Argv []string `json:"argv"`
	}
	if err := json.Unmarshal(spec, &s); err != nil {
		return 0, err
	}
	if len(s.Argv) == 0 {
		return 0, errors.New("argv 为空")
	}
	cmd := exec.Command(s.Argv[0], s.Argv[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	if l.pids != nil {
		*l.pids = append(*l.pids, pid)
	}
	return pid, nil
}

func argvSpec(argv ...string) json.RawMessage {
	b, _ := json.Marshal(map[string][]string{"argv": argv})
	return b
}

// reaperSink 记录 out 收到的消息；exit 另外送进 exits（非阻塞，满则报错）。
type reaperSink struct {
	mu    sync.Mutex
	msgs  []Message
	exits chan string
}

func newReaperSink(n int) *reaperSink { return &reaperSink{exits: make(chan string, n)} }

func (s *reaperSink) out(m Message) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, m)
	s.mu.Unlock()
	if m.Type == MsgExit {
		select {
		case s.exits <- m.ExecID:
		default:
			return errors.New("exits 已满")
		}
	}
	return nil
}

func (s *reaperSink) snapshot() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.msgs...)
}

// devNullFDs 返回三个 /dev/null 文件，作为 Start 的 stdin/stdout/stderr（Start 负责关闭）。
func devNullFDs() ([3]*os.File, error) {
	var fds [3]*os.File
	for i := range fds {
		f, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			for _, g := range fds[:i] {
				g.Close()
			}
			return fds, err
		}
		fds[i] = f
	}
	return fds, nil
}

// waitZombie 阻塞直到 pid 退出成为僵尸，但不收割它（waitid 带 WNOWAIT），
// 使测试能在确定的时刻调用 Reap，而不靠 sleep。
func waitZombie(pid int) error {
	const pPID = 1     // include/uapi/linux/wait.h
	var info [128]byte // siginfo_t
	for {
		_, _, e := syscall.Syscall6(syscall.SYS_WAITID, pPID, uintptr(pid),
			uintptr(unsafe.Pointer(&info[0])), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		switch e {
		case 0:
			return nil
		case syscall.EINTR:
			continue
		default:
			return fmt.Errorf("waitid(%d): %w", pid, e)
		}
	}
}

// ackPID 在 msgs 中找到 execID 的 start_ack 并返回其 pid。
func ackPID(msgs []Message, execID string) (int, error) {
	for _, m := range msgs {
		if m.Type == MsgStartAck && m.ExecID == execID {
			return m.PID, nil
		}
	}
	return 0, fmt.Errorf("没有 %s 的 start_ack", execID)
}

// reaperChildOrder：另一个 goroutine 持续 Reap，同时逐个启动 200 个立即退出的
// 进程。进程可能在 Launch 返回前就已退出，但 Reap 在 Start 持锁期间无法收割它；
// 每个 exec 必须恰好一条 start_ack、一条 exit，且 start_ack 在前。若 Reap 抢在
// 登记之前收割，该 pid 会被当作孤儿丢弃，exit 永远不会到达。
func reaperChildOrder() error {
	const n = 200
	sink := newReaperSink(n)
	reg := NewRegistry(sink.out)

	var stop atomic.Bool
	reapErr := make(chan error, 1)
	go func() {
		for !stop.Load() {
			if err := reg.Reap(); err != nil {
				reapErr <- err
				return
			}
			runtime.Gosched()
		}
		reapErr <- nil
	}()

	for i := 0; i < n; i++ {
		fds, err := devNullFDs()
		if err != nil {
			return err
		}
		if err := reg.Start(fmt.Sprintf("e%d", i), execLauncher{}, argvSpec("/bin/true"), fds); err != nil {
			return fmt.Errorf("Start e%d: %w", i, err)
		}
	}

	// 等待全部 exit；超时只是防止挂死的兜底，不参与同步。
	timeout := time.After(60 * time.Second)
	for got := 0; got < n; got++ {
		select {
		case <-sink.exits:
		case err := <-reapErr:
			return fmt.Errorf("Reap 提前结束: %v（已收到 %d 个 exit）", err, got)
		case <-timeout:
			return fmt.Errorf("只收到 %d/%d 个 exit", got, n)
		}
	}
	stop.Store(true)
	if err := <-reapErr; err != nil {
		return fmt.Errorf("Reap: %w", err)
	}

	type seen struct{ ack, exit, acks, exits int }
	idx := map[string]*seen{}
	for i, m := range sink.snapshot() {
		s := idx[m.ExecID]
		if s == nil {
			s = &seen{ack: -1, exit: -1}
			idx[m.ExecID] = s
		}
		switch m.Type {
		case MsgStartAck:
			if m.PID <= 0 {
				return fmt.Errorf("%s: start_ack pid = %d", m.ExecID, m.PID)
			}
			s.ack, s.acks = i, s.acks+1
		case MsgExit:
			if m.Exit == nil || *m.Exit != (ExitInfo{}) {
				return fmt.Errorf("%s: exit = %+v, want code 0", m.ExecID, m.Exit)
			}
			s.exit, s.exits = i, s.exits+1
		default:
			return fmt.Errorf("意外消息 %+v", m)
		}
	}
	if len(idx) != n {
		return fmt.Errorf("消息涉及 %d 个 exec, want %d", len(idx), n)
	}
	for id, s := range idx {
		if s.acks != 1 || s.exits != 1 {
			return fmt.Errorf("%s: %d 条 start_ack、%d 条 exit, want 各 1", id, s.acks, s.exits)
		}
		if s.ack > s.exit {
			return fmt.Errorf("%s: exit（#%d）先于 start_ack（#%d）", id, s.exit, s.ack)
		}
	}
	// 全部收割后再 Reap：Wait4 返回 ECHILD，Reap 返回 nil。
	if err := reg.Reap(); err != nil {
		return fmt.Errorf("无子进程时 Reap = %v, want nil", err)
	}
	return nil
}

// reaperChildEINTR：注入的 wait4 在每次真实调用之前先返回一次 EINTR，同时另一个
// goroutine 不断向本进程发送 SIGUSR1。20 个以不同退出码退出的进程与 1 个被
// SIGKILL 杀死的进程全部成为僵尸之后，只调用一次 Reap：它必须重试 EINTR 并
// 收割到没有为止，交出全部 21 条 exit，退出状态逐个正确。
func reaperChildEINTR() error {
	sigs := make(chan os.Signal, 64)
	signal.Notify(sigs, syscall.SIGUSR1)
	defer signal.Stop(sigs)
	var stop atomic.Bool
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		for !stop.Load() {
			_ = syscall.Kill(os.Getpid(), syscall.SIGUSR1) // 向自身发信号；失败只会减少洪泛量
			select {
			case <-sigs:
			default:
			}
			runtime.Gosched()
		}
	}()
	defer func() { stop.Store(true); <-flooded }()

	const n = 20
	sink := newReaperSink(n + 1)
	reg := NewRegistry(sink.out)
	var injected, real atomic.Int64
	reg.wait4 = func(pid int, ws *syscall.WaitStatus, options int, ru *syscall.Rusage) (int, error) {
		if injected.Load() == real.Load() {
			injected.Add(1)
			return 0, syscall.EINTR
		}
		real.Add(1)
		return syscall.Wait4(pid, ws, options, ru)
	}

	want := map[string]ExitInfo{}
	var pids []int
	l := execLauncher{pids: &pids}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("code%d", i)
		want[id] = ExitInfo{Code: i}
		fds, err := devNullFDs()
		if err != nil {
			return err
		}
		if err := reg.Start(id, l, argvSpec("/bin/sh", "-c", "exit "+strconv.Itoa(i)), fds); err != nil {
			return fmt.Errorf("Start %s: %w", id, err)
		}
	}
	want["killed"] = ExitInfo{Signal: syscall.SIGKILL}
	fds, err := devNullFDs()
	if err != nil {
		return err
	}
	if err := reg.Start("killed", l, argvSpec("/bin/sh", "-c", "kill -9 $$"), fds); err != nil {
		return fmt.Errorf("Start killed: %w", err)
	}
	for _, pid := range pids {
		if err := waitZombie(pid); err != nil {
			return err
		}
	}

	if err := reg.Reap(); err != nil {
		return fmt.Errorf("Reap: %w", err)
	}
	if injected.Load() < int64(n+2) {
		return fmt.Errorf("只注入了 %d 次 EINTR", injected.Load())
	}
	got := map[string]ExitInfo{}
	for _, m := range sink.snapshot() {
		if m.Type == MsgExit {
			got[m.ExecID] = *m.Exit
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("一次 Reap 交出 %d 条 exit, want %d（EINTR 后丢失了退出）", len(got), len(want))
	}
	for id, w := range want {
		if got[id] != w {
			return fmt.Errorf("%s: exit = %+v, want %+v", id, got[id], w)
		}
	}
	return nil
}

// reaperChildOrphan：登记的 workload 后台启动一个孙进程（读 stdin 管道直到 EOF）
// 后以 7 退出。孙进程被收养给本进程（subreaper）。workload 的 exit 照常送出；
// 关闭管道让孙进程退出后，Reap 收割它（之后 Wait4 对其返回 ECHILD），但不产生
// 任何消息。
func reaperChildOrphan() error {
	sink := newReaperSink(4)
	reg := NewRegistry(sink.out)

	inR, inW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer inW.Close()
	outR, outW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer outR.Close()
	errF, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	// 非交互 sh 会把后台命令的 stdin 改为 /dev/null，所以先把管道复制到 fd 3。
	script := `exec 3<&0; cat <&3 >/dev/null 3<&- & echo $!; exit 7`
	if err := reg.Start("parent", execLauncher{}, argvSpec("/bin/sh", "-c", script), [3]*os.File{inR, outW, errF}); err != nil {
		return fmt.Errorf("Start: %w", err)
	}
	// Start 已关闭本进程的 outW；孙进程的 stdout 是 /dev/null，所以 workload 退出即 EOF。
	b, err := io.ReadAll(outR)
	if err != nil {
		return err
	}
	gpid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return fmt.Errorf("孙进程 pid %q: %w", b, err)
	}
	ppid, err := ackPID(sink.snapshot(), "parent")
	if err != nil {
		return err
	}

	if err := waitZombie(ppid); err != nil {
		return err
	}
	if err := reg.Reap(); err != nil {
		return fmt.Errorf("Reap（workload）: %w", err)
	}
	// workload 退出时孙进程已被收养；它仍在运行（管道写端在本进程手里）。
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", gpid))
	if err != nil {
		return fmt.Errorf("读取孙进程 stat: %w", err)
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 2 || fields[1] != strconv.Itoa(os.Getpid()) {
		return fmt.Errorf("孙进程 %d 的父进程 = %v, want 本进程 %d（未被收养）", gpid, fields, os.Getpid())
	}

	inW.Close()
	if err := waitZombie(gpid); err != nil {
		return err
	}
	if err := reg.Reap(); err != nil {
		return fmt.Errorf("Reap（孤儿）: %w", err)
	}
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(gpid, &ws, syscall.WNOHANG, nil); err != syscall.ECHILD {
		return fmt.Errorf("Reap 之后 Wait4(孙进程) = %v, want ECHILD（孤儿未被收割）", err)
	}

	msgs := sink.snapshot()
	if len(msgs) != 2 || msgs[0].Type != MsgStartAck || msgs[1].Type != MsgExit ||
		msgs[1].ExecID != "parent" || *msgs[1].Exit != (ExitInfo{Code: 7}) {
		return fmt.Errorf("消息 = %+v, want 仅 parent 的 start_ack 与 exit(code 7)", msgs)
	}
	return nil
}

// reaperChildOutErr：out 返回错误时 Start（start_ack、start_err 两条路径）与
// Reap 都返回包装了该错误的错误；out 正常时 Launch 失败只入队 start_err，
// Start 返回 nil。
func reaperChildOutErr() error {
	boom := errors.New("队列已满（测试）")
	reg := NewRegistry(func(Message) error { return boom })
	var pids []int
	fds, err := devNullFDs()
	if err != nil {
		return err
	}
	if err := reg.Start("ack", execLauncher{pids: &pids}, argvSpec("/bin/true"), fds); !errors.Is(err, boom) {
		return fmt.Errorf("start_ack 入队失败时 Start = %v, want 包装 %v", err, boom)
	}
	if len(pids) != 1 {
		return fmt.Errorf("启动了 %d 个进程, want 1", len(pids))
	}
	if err := waitZombie(pids[0]); err != nil {
		return err
	}
	if err := reg.Reap(); !errors.Is(err, boom) {
		return fmt.Errorf("exit 入队失败时 Reap = %v, want 包装 %v", err, boom)
	}

	fds, err = devNullFDs()
	if err != nil {
		return err
	}
	if err := reg.Start("bad", execLauncher{}, argvSpec("/nonexistent/agentbox-test"), fds); !errors.Is(err, boom) {
		return fmt.Errorf("start_err 入队失败时 Start = %v, want 包装 %v", err, boom)
	}

	sink := newReaperSink(1)
	ok := NewRegistry(sink.out)
	fds, err = devNullFDs()
	if err != nil {
		return err
	}
	if err := ok.Start("bad", execLauncher{}, argvSpec("/nonexistent/agentbox-test"), fds); err != nil {
		return fmt.Errorf("Launch 失败时 Start = %v, want nil（经 start_err 报告）", err)
	}
	msgs := sink.snapshot()
	if len(msgs) != 1 || msgs[0].Type != MsgStartErr || msgs[0].ExecID != "bad" || msgs[0].Reason == "" {
		return fmt.Errorf("消息 = %+v, want 一条带 reason 的 start_err", msgs)
	}
	return nil
}

// ---------------------------------------------------------------------------
// init 服务循环（规格 §4.2–§4.4）
// ---------------------------------------------------------------------------

// envServeChild 让测试二进制在 TestMain 中直接进入 runServeChild。
//
// 每个用例在 re-exec 出来的子进程里扮演 init：Serve 注册 SIGCHLD 并经
// Wait4(-1) 收割本进程的全部子进程，放在独立进程里不影响同一测试二进制中
// 其他自己 Wait 子进程的用例。控制通道是子进程内的一对 socketpair：init 端
// 阻塞（与经 ExtraFiles 继承的生产 fd 一致），宿主端非阻塞。
const envServeChild = "AGENTBOX_TEST_SERVE_CHILD"

var serveChildren = map[string]func() error{
	"ack-exit":   serveChildAckExit,
	"start-err":  serveChildStartErr,
	"terminate":  serveChildTerminate,
	"parallel":   serveChildParallel,
	"duplicate":  serveChildDuplicate,
	"unexpected": serveChildUnexpected,
	"host-close": serveChildHostClose,
}

// runServeCase 以 envServeChild=name re-exec 测试二进制，要求子进程报告通过。
// 60 秒只是防挂死的上限，不用于同步。
func runServeCase(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envServeChild+"="+name)
	out, err := cmd.CombinedOutput()
	t.Logf("子进程输出:\n%s", out)
	if err != nil {
		t.Fatalf("子进程失败: %v", err)
	}
	if !strings.Contains(string(out), "serve child: ok") {
		t.Fatalf("子进程没有报告完成")
	}
}

// runServeChild 把本进程设为 child subreaper（与沙箱内 init 的处境相同），
// 然后运行用例 name。
func runServeChild(name string) int {
	fn, ok := serveChildren[name]
	if !ok {
		fmt.Printf("serve child: FAIL: 未知用例 %q\n", name)
		return 1
	}
	const prSetChildSubreaper = 36 // include/uapi/linux/prctl.h
	if _, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetChildSubreaper, 1, 0, 0, 0, 0); e != 0 {
		fmt.Printf("serve child: FAIL: PR_SET_CHILD_SUBREAPER: %v\n", e)
		return 1
	}
	if err := fn(); err != nil {
		fmt.Printf("serve child: FAIL: %s: %v\n", name, err)
		return 1
	}
	fmt.Printf("serve child: ok (%s)\n", name)
	return 0
}

// TestServeStartAckThenExit：start → start_ack{pid}，workload 输出经传入的管道送达，然后 exit{code}。
func TestServeStartAckThenExit(t *testing.T) { runServeCase(t, "ack-exit") }

// TestServeLaunchFailureSendsStartErr：Launcher 失败或 spec 非法 → start_err，无 start_ack，FD 已关闭。
func TestServeLaunchFailureSendsStartErr(t *testing.T) { runServeCase(t, "start-err") }

// TestServeTerminateEscalatesToSIGKILL：忽略 SIGTERM 的进程在 grace 到期后被 SIGKILL；
// grace 期间继续服务（另一个 exec 被 SIGTERM 终止并先收到 exit）。
func TestServeTerminateEscalatesToSIGKILL(t *testing.T) { runServeCase(t, "terminate") }

// TestServeParallelExecs：两个并行 exec 各自收到自己的 exit。
func TestServeParallelExecs(t *testing.T) { runServeCase(t, "parallel") }

// TestServeDuplicateExecIDRejected：exec_id 已登记且未退出 → start_err{duplicate_exec_id}，
// 不启动、关闭 FD；该 exec 退出后同一 exec_id 可再次启动。
func TestServeDuplicateExecIDRejected(t *testing.T) { runServeCase(t, "duplicate") }

// TestServeUnexpectedMessageIsProtocolError：宿主发来 start/terminate 以外的消息 →
// Serve 关闭连接并返回 ErrProtocol。
func TestServeUnexpectedMessageIsProtocolError(t *testing.T) { runServeCase(t, "unexpected") }

// TestServeReturnsWhenHostCloses：宿主关闭连接 → Serve 返回 nil。
func TestServeReturnsWhenHostCloses(t *testing.T) { runServeCase(t, "host-close") }

// pgLauncher 是测试用 Launcher：按 StartSpec 解码，直接 exec 并让 workload 成为
// 自己进程组的组长（Setpgid），不建 namespace、不降权。只调用 exec.Cmd.Start，
// 然后 Release，不等待进程。收到的 FD 已由 Serve 清除 O_NONBLOCK（exec.Cmd
// 不会清除：os.NewFile 对已非阻塞的 fd 不记 nonblock，File.Fd() 不切换）。
type pgLauncher struct{}

func (pgLauncher) Launch(spec json.RawMessage, stdin, stdout, stderr *os.File) (int, error) {
	var s StartSpec
	if err := json.Unmarshal(spec, &s); err != nil {
		return 0, err
	}
	cmd := exec.Command(s.Argv[0], s.Argv[1:]...)
	cmd.Env, cmd.Dir = s.Env, s.Dir
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}

// serveHost 是用例中的宿主端：conn 是控制通道宿主端，errc 收到 Serve 的返回值。
type serveHost struct {
	conn *Conn
	errc chan error
}

// hostExec 是宿主持有的一个 exec 的管道端：stdin 写端与 stdout/stderr 合并的读端。
type hostExec struct {
	stdin, stdout *os.File
}

func (e hostExec) close() {
	e.stdin.Close()
	e.stdout.Close()
}

// startServe 创建控制通道并在 goroutine 中运行 Serve。
func startServe() (*serveHost, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("socketpair: %w", err)
	}
	if err := syscall.SetNonblock(fds[0], true); err != nil {
		return nil, fmt.Errorf("SetNonblock: %w", err)
	}
	host := NewConn(os.NewFile(uintptr(fds[0]), "control-host"), 64)
	initConn := NewConn(os.NewFile(uintptr(fds[1]), "control-init"), 64)
	reg := NewRegistry(func(m Message) error { return initConn.Send(m, nil) })
	h := &serveHost{conn: host, errc: make(chan error, 1)}
	go func() { h.errc <- Serve(context.Background(), initConn, reg, pgLauncher{}) }()
	return h, nil
}

// start 发送 start 及三个 FD（stdin 管道读端；stdout 与 stderr 共用一个管道写端），
// 并按规格 §4.2 在 Send 返回后关闭子端副本。
func (h *serveHost) start(execID string, spec json.RawMessage) (hostExec, error) {
	inR, inW, err := os.Pipe()
	if err != nil {
		return hostExec{}, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		return hostExec{}, err
	}
	err = h.conn.Send(Message{Type: MsgStart, ExecID: execID, Spec: spec}, []*os.File{inR, outW, outW})
	inR.Close()
	outW.Close()
	e := hostExec{stdin: inW, stdout: outR}
	if err != nil {
		e.close()
		return hostExec{}, fmt.Errorf("Send(start %s): %w", execID, err)
	}
	return e, nil
}

// expect 接收下一条消息，要求其类型与 exec_id 符合。
func (h *serveHost) expect(typ, execID string) (Message, error) {
	m, files, err := h.conn.Recv()
	for _, f := range files {
		f.Close()
	}
	if err != nil {
		return m, fmt.Errorf("等待 %s(%s): %w", typ, execID, err)
	}
	if m.Type != typ || m.ExecID != execID {
		if m.Exit != nil {
			return m, fmt.Errorf("收到 %+v（exit %+v）, want %s(%s)", m, *m.Exit, typ, execID)
		}
		return m, fmt.Errorf("收到 %+v, want %s(%s)", m, typ, execID)
	}
	return m, nil
}

// expectExit 接收下一条消息，要求它是 execID 的 exit 且退出状态为 want。
func (h *serveHost) expectExit(execID string, want ExitInfo) error {
	m, err := h.expect(MsgExit, execID)
	if err != nil {
		return err
	}
	if *m.Exit != want {
		return fmt.Errorf("exit(%s) = %+v, want %+v", execID, *m.Exit, want)
	}
	return nil
}

// closeAndWait 关闭宿主端并要求 Serve 返回 nil。
func (h *serveHost) closeAndWait() error {
	h.conn.Close()
	if err := <-h.errc; err != nil {
		return fmt.Errorf("宿主关闭后 Serve = %v, want nil", err)
	}
	return nil
}

func shSpec(script string) json.RawMessage { return argvSpec("/bin/sh", "-c", script) }

// readEOF 读到 EOF 并返回全部内容；EOF 说明所有写端副本（包括 init 的）都已关闭。
func readEOF(f *os.File) (string, error) {
	b, err := io.ReadAll(f)
	return string(b), err
}

func serveChildAckExit() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	// workload 报告自己 fd 0/1/2 的打开标志（八进制）与进程组号。宿主的管道端
	// 是非阻塞的；workload 拿到的必须是阻塞的，且自成进程组（terminate 依赖它）。
	e, err := h.start("e1", shSpec(`awk '/^flags/{print $2}' /proc/$$/fdinfo/0 /proc/$$/fdinfo/1 /proc/$$/fdinfo/2; cut -d' ' -f5 /proc/$$/stat; exit 5`))
	if err != nil {
		return err
	}
	defer e.close()
	m, err := h.expect(MsgStartAck, "e1")
	if err != nil {
		return err
	}
	if m.PID <= 0 {
		return fmt.Errorf("start_ack pid = %d", m.PID)
	}
	out, err := readEOF(e.stdout)
	lines := strings.Fields(out)
	if err != nil || len(lines) != 4 {
		return fmt.Errorf("stdout = %q, %v; want 3 行标志与 1 行进程组号", out, err)
	}
	for i, s := range lines[:3] {
		flags, err := strconv.ParseUint(s, 8, 32)
		if err != nil {
			return fmt.Errorf("fd %d 标志 %q: %v", i, s, err)
		}
		if flags&syscall.O_NONBLOCK != 0 {
			return fmt.Errorf("workload 的 fd %d 是非阻塞的（flags 0%o）", i, flags)
		}
	}
	if lines[3] != strconv.Itoa(m.PID) {
		return fmt.Errorf("workload 进程组 = %s, want pid %d", lines[3], m.PID)
	}
	if err := h.expectExit("e1", ExitInfo{Code: 5}); err != nil {
		return err
	}
	return h.closeAndWait()
}

func serveChildStartErr() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	cases := []struct {
		id, reason string
		spec       json.RawMessage
	}{
		{"missing", "", argvSpec("/nonexistent/agentbox-test")},
		{"empty-argv", reasonInvalidSpec, json.RawMessage(`{"argv":[]}`)},
		{"unknown-field", reasonInvalidSpec, json.RawMessage(`{"argv":["/bin/true"],"cmd":"x"}`)},
	}
	for _, c := range cases {
		e, err := h.start(c.id, c.spec)
		if err != nil {
			return err
		}
		m, err := h.expect(MsgStartErr, c.id)
		if err != nil {
			e.close()
			return err
		}
		if m.Reason == "" || !strings.HasPrefix(m.Reason, c.reason) {
			e.close()
			return fmt.Errorf("start_err(%s).reason = %q, want 前缀 %q", c.id, m.Reason, c.reason)
		}
		// init 已关闭它的 FD 副本、没有进程持有写端：stdout 立即 EOF。
		out, err := readEOF(e.stdout)
		e.close()
		if err != nil || out != "" {
			return fmt.Errorf("start_err(%s) 后 stdout = %q, %v; want 立即 EOF", c.id, out, err)
		}
	}
	// 失败的 exec 没有 start_ack/exit：下一条消息属于新的 exec。
	e, err := h.start("ok", argvSpec("/bin/true"))
	if err != nil {
		return err
	}
	defer e.close()
	if _, err := h.expect(MsgStartAck, "ok"); err != nil {
		return err
	}
	if err := h.expectExit("ok", ExitInfo{Code: 0}); err != nil {
		return err
	}
	return h.closeAndWait()
}

// waitReady 从 stdout 读出一行 "ready"：此时 workload 已装好信号处置。
func waitReady(e hostExec) error {
	buf := make([]byte, len("ready\n"))
	if _, err := io.ReadFull(e.stdout, buf); err != nil || string(buf) != "ready\n" {
		return fmt.Errorf("等待 ready: %q, %v", buf, err)
	}
	return nil
}

func serveChildTerminate() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	// stubborn 忽略 SIGTERM（忽略的处置在 exec 后保留），只能被 SIGKILL 结束。
	stubborn, err := h.start("stubborn", shSpec(`trap "" TERM; echo ready; exec sleep 1000`))
	if err != nil {
		return err
	}
	defer stubborn.close()
	if _, err := h.expect(MsgStartAck, "stubborn"); err != nil {
		return err
	}
	polite, err := h.start("polite", shSpec(`echo ready; exec sleep 1000`))
	if err != nil {
		return err
	}
	defer polite.close()
	if _, err := h.expect(MsgStartAck, "polite"); err != nil {
		return err
	}
	if err := waitReady(stubborn); err != nil {
		return err
	}
	if err := waitReady(polite); err != nil {
		return err
	}

	const grace = 2 * time.Second
	sent := time.Now()
	if err := h.conn.Send(Message{Type: MsgTerminate, ExecID: "stubborn", GraceMS: grace.Milliseconds()}, nil); err != nil {
		return err
	}
	// 未知 exec_id 被忽略，不产生任何消息。
	if err := h.conn.Send(Message{Type: MsgTerminate, ExecID: "nobody", GraceMS: 1}, nil); err != nil {
		return err
	}
	// grace 期间 Serve 继续服务：polite 收到 SIGTERM 即退出，它的 exit 先到。
	if err := h.conn.Send(Message{Type: MsgTerminate, ExecID: "polite", GraceMS: time.Hour.Milliseconds()}, nil); err != nil {
		return err
	}
	if err := h.expectExit("polite", ExitInfo{Signal: syscall.SIGTERM}); err != nil {
		return err
	}
	if err := h.expectExit("stubborn", ExitInfo{Signal: syscall.SIGKILL}); err != nil {
		return err
	}
	if d := time.Since(sent); d < grace {
		return fmt.Errorf("SIGKILL 在 terminate 后 %v 到达，早于 grace %v", d, grace)
	}
	// polite 的一小时计时器仍在等待；Serve 返回时停止它。
	return h.closeAndWait()
}

func serveChildParallel() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	a, err := h.start("a", shSpec("cat >/dev/null; exit 1"))
	if err != nil {
		return err
	}
	defer a.close()
	b, err := h.start("b", shSpec("cat >/dev/null; exit 2"))
	if err != nil {
		return err
	}
	defer b.close()
	if _, err := h.expect(MsgStartAck, "a"); err != nil {
		return err
	}
	if _, err := h.expect(MsgStartAck, "b"); err != nil {
		return err
	}
	// 两者同时在运行；按与启动相反的顺序让它们退出。
	b.stdin.Close()
	if err := h.expectExit("b", ExitInfo{Code: 2}); err != nil {
		return err
	}
	a.stdin.Close()
	if err := h.expectExit("a", ExitInfo{Code: 1}); err != nil {
		return err
	}
	return h.closeAndWait()
}

func serveChildDuplicate() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	first, err := h.start("e1", shSpec("cat >/dev/null; exit 3"))
	if err != nil {
		return err
	}
	defer first.close()
	if _, err := h.expect(MsgStartAck, "e1"); err != nil {
		return err
	}

	dup, err := h.start("e1", argvSpec("/bin/true"))
	if err != nil {
		return err
	}
	m, err := h.expect(MsgStartErr, "e1")
	if err != nil {
		dup.close()
		return err
	}
	if m.Reason != ReasonDuplicateExecID {
		dup.close()
		return fmt.Errorf("start_err.reason = %q, want %q", m.Reason, ReasonDuplicateExecID)
	}
	out, err := readEOF(dup.stdout)
	dup.close()
	if err != nil || out != "" {
		return fmt.Errorf("重复 start 的 stdout = %q, %v; want 立即 EOF（FD 已关闭、未启动）", out, err)
	}

	// 原 exec 不受影响；它退出后同一 exec_id 可以再次启动。
	first.stdin.Close()
	if err := h.expectExit("e1", ExitInfo{Code: 3}); err != nil {
		return err
	}
	again, err := h.start("e1", shSpec("exit 4"))
	if err != nil {
		return err
	}
	defer again.close()
	if _, err := h.expect(MsgStartAck, "e1"); err != nil {
		return err
	}
	if err := h.expectExit("e1", ExitInfo{Code: 4}); err != nil {
		return err
	}
	return h.closeAndWait()
}

func serveChildUnexpected() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	defer h.conn.Close()
	if err := h.conn.Send(Message{Type: MsgStartAck, ExecID: "e1", PID: 1}, nil); err != nil {
		return err
	}
	if err := <-h.errc; !errors.Is(err, ErrProtocol) {
		return fmt.Errorf("Serve = %v, want ErrProtocol", err)
	}
	// Serve 已关闭 init 端：宿主读到 EOF。
	if _, _, err := h.conn.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("Serve 返回后宿主 Recv = %v, want io.EOF", err)
	}
	return nil
}

func serveChildHostClose() error {
	h, err := startServe()
	if err != nil {
		return err
	}
	// 有 workload 仍在运行时宿主关闭连接，Serve 也返回（残留进程由 cgroup 回收）。
	e, err := h.start("e1", shSpec("cat >/dev/null"))
	if err != nil {
		return err
	}
	defer e.close()
	if _, err := h.expect(MsgStartAck, "e1"); err != nil {
		return err
	}
	return h.closeAndWait()
}

// ---------------------------------------------------------------------------
// Task 10：init 环境建立（规格 §4.5、§4.6 init 段）。init 由真正的启动进程（RunLaunch）在 user namespace、
// 测试专用 cgroup 中启动（本测试二进制即 /proc/self/exe）；检查全部从宿主经 /proc/<init pid> 进行
// （mountinfo 与 /proc/<pid>/root 呈现的是 init 的 mount namespace 与根）。
// ---------------------------------------------------------------------------

// init 的测试钩子环境变量（TestMain 读取后设置 initFailAt、forceClassicMounts）。
const (
	envTestInitFail    = "AGENTBOX_TEST_INIT_FAIL"
	envTestInitClassic = "AGENTBOX_TEST_INIT_CLASSIC"
)

// initTestIDBase 是测试环境的 UID/GID 范围基址（大小 4096，含 workload 的 1000）。
const initTestIDBase = 400000

// initInjectionPoints 是实验记录 §8 的 9 个 init 注入点。
var initInjectionPoints = []string{
	stepMountPrivate, stepMountRootfs, stepMountTmpfs, stepMountProc, stepMaskProc,
	stepOpenSelf, stepPivotRoot, stepUmountOldroot, stepInitCaps,
}

// sandboxInit 是一个已启动的 init：ready 为 nil 即已就绪（否则为 init_err 的原因）。
type sandboxInit struct {
	pid, pidfd int
	conn       *Conn
	ready      error
	stderr     *os.File
}

// output 返回启动进程与 init 的 stderr。
func (s *sandboxInit) output() string {
	b, _ := os.ReadFile(s.stderr.Name())
	return string(b)
}

// waitPidfdExit 等待 pidfd 对应的进程退出（pidfd 可读），超时返回 false。init 不是本进程的子进程，不收割它。
func waitPidfdExit(pidfd int, d time.Duration) bool {
	type pollFd struct {
		fd             int32
		events, revent int16
	}
	deadline := time.Now().Add(d)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		fds := []pollFd{{fd: int32(pidfd), events: 0x1}} // POLLIN
		ts := syscall.NsecToTimespec(left.Nanoseconds())
		n, _, e := syscall.Syscall6(syscall.SYS_PPOLL, uintptr(unsafe.Pointer(&fds[0])), 1, uintptr(unsafe.Pointer(&ts)), 0, 0, 0)
		if e == syscall.EINTR {
			continue
		}
		return e == 0 && n == 1
	}
}

// startSandboxInit 在新建的测试 cgroup 中经 RunLaunch 启动 init 并等待就绪结果。测试结束时关闭控制连接
// （init 的 Serve 返回、init 退出），必要时以 SIGKILL 兜底，然后删除 cgroup。
func startSandboxInit(t *testing.T, spec LaunchSpec) *sandboxInit {
	t.Helper()
	cg := filepath.Join("/sys/fs/cgroup", fmt.Sprintf("agentbox-t10-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := os.Mkdir(cg, 0o755); err != nil {
		t.Fatalf("创建测试 cgroup: %v", err)
	}
	s := &sandboxInit{pidfd: -1}
	t.Cleanup(func() {
		if s.conn != nil {
			s.conn.Close()
		}
		if s.pidfd >= 0 {
			if !waitPidfdExit(s.pidfd, 10*time.Second) {
				t.Errorf("init %d 在控制连接关闭后未退出，SIGKILL", s.pid)
				_ = PidfdKill(s.pidfd)
				waitPidfdExit(s.pidfd, 10*time.Second)
			}
			syscall.Close(s.pidfd)
		}
		var err error
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if err = syscall.Rmdir(cg); err == nil || errors.Is(err, syscall.ENOENT) {
				return
			}
		}
		t.Errorf("删除测试 cgroup %s: %v", cg, err)
	})

	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctl, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	host, ctlChild := os.NewFile(uintptr(ctl[0]), "ctl-host"), os.NewFile(uintptr(ctl[1]), "ctl-child")
	res, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(res[0])
	resChild := os.NewFile(uintptr(res[1]), "res-child")
	specR, specW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := specW.Write(b); err != nil {
		t.Fatal(err)
	}
	specW.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	cgdir, err := os.OpenFile(cg, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// stderr 用普通文件：管道会让 cmd.Wait 等到 init 退出才返回。
	s.stderr, err = os.CreateTemp("", "agentbox-t10-stderr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.stderr.Close(); os.Remove(s.stderr.Name()) })
	cmd := LaunchCommand(LaunchFiles{Control: ctlChild, Cgroup: cgdir, Spec: specR, Result: resChild, Ready: readyW})
	cmd.Stdout, cmd.Stderr = s.stderr, s.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*os.File{ctlChild, cgdir, specR, resChild, readyW} {
		f.Close()
	}
	s.conn = NewConn(host, 16)
	pid, pidfd, rerr := RecvLaunchResult(res[0])
	werr := cmd.Wait()
	if rerr != nil {
		t.Fatalf("启动进程失败: %v（退出 %v）\n%s", rerr, werr, s.output())
	}
	s.pid, s.pidfd = pid, pidfd

	buf, _ := io.ReadAll(io.LimitReader(readyR, 4096))
	switch {
	case len(buf) == 1 && buf[0] == InitReadyByte:
	case len(buf) == 0:
		s.ready = errors.New("init 未通知就绪即退出")
	default:
		s.ready = errors.New(string(buf))
	}
	return s
}

// initTestDir 返回一个宿主临时目录：init 以未映射的宿主身份访问宿主路径（对宿主 root 拥有的目录没有
// DAC 豁免），因此目录须对其他用户可进入。
func initTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "agentbox-t10-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// hostTestTemplate 是由宿主系统目录组成的模板（同 rootfs.DefaultTemplate，但不含宿主上没有的 worker 目录）。
func hostTestTemplate(t *testing.T) rootfs.Template {
	t.Helper()
	var tm rootfs.Template
	for _, p := range rootfs.DefaultTemplate().Paths {
		if p != rootfs.WorkerDir {
			tm.Paths = append(tm.Paths, p)
		}
	}
	if err := tm.Ensure(); err != nil {
		t.Fatalf("宿主测试模板不可用: %v", err)
	}
	return tm
}

// initTestSpec 返回给定环境类型的启动规格：task 带 workspace（属主为映射 uid 1000，内有标记文件）与
// Gateway socket（属主映射 uid 1000、0600）；exec 带 /in（内有标记文件）与 /out。
func initTestSpec(t *testing.T, kind string, classic bool) LaunchSpec {
	t.Helper()
	dir := initTestDir(t)
	is := InitSpec{Kind: kind, Template: hostTestTemplate(t), TmpBytes: 64 << 20}
	hostWorkload := initTestIDBase + workloadID
	switch kind {
	case KindExec:
		is.In = filepath.Join(dir, "in")
		is.OutBytes = 1 << 20
		if err := os.Mkdir(is.In, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(is.In, "marker"), []byte("in"), 0o644); err != nil {
			t.Fatal(err)
		}
	default:
		is.Workspace = filepath.Join(dir, "ws")
		if err := os.Mkdir(is.Workspace, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(is.Workspace, "marker"), []byte("ws"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(is.Workspace, hostWorkload, hostWorkload); err != nil {
			t.Fatal(err)
		}
		is.GatewaySocket = gatewayTestSocket(t, dir, hostWorkload)
	}
	spec := LaunchSpec{
		UIDBase: initTestIDBase, UIDSize: 4096, GIDBase: initTestIDBase, GIDSize: 4096,
		Hostname: "agentbox", Init: is,
	}
	if classic {
		spec.Env = append(spec.Env, envTestInitClassic+"=1")
	}
	return spec
}

// gatewayTestSocket 在 dir 下建一个监听中的 unix socket，属主设为 uid（宿主视角），权限 0600。
func gatewayTestSocket(t *testing.T, dir string, uid int) string {
	t.Helper()
	path := filepath.Join(dir, "gw.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := os.Chown(path, uid, uid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// mountinfoEntry 是 /proc/<pid>/mountinfo 的一行。
type mountinfoEntry struct {
	point, fstype string
	opts, super   []string
	optional      []string
}

func readMountinfo(t *testing.T, pid int) []mountinfoEntry {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", pid))
	if err != nil {
		t.Fatal(err)
	}
	var out []mountinfoEntry
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		f := strings.Fields(line)
		sep := -1
		for i := 6; i < len(f); i++ {
			if f[i] == "-" {
				sep = i
				break
			}
		}
		if len(f) < 10 || sep < 0 || sep+3 >= len(f) {
			t.Fatalf("无法解析 mountinfo 行 %q", line)
		}
		out = append(out, mountinfoEntry{
			point: unescapeMountinfo(f[4]), opts: strings.Split(f[5], ","), optional: f[6:sep],
			fstype: f[sep+1], super: strings.Split(f[sep+3], ","),
		})
	}
	return out
}

func hasAll(have []string, want ...string) bool {
	for _, w := range want {
		found := false
		for _, h := range have {
			found = found || h == w
		}
		if !found {
			return false
		}
	}
	return true
}

// under 报告 p 是否等于 base 或位于其下。
func under(p, base string) bool { return p == base || strings.HasPrefix(p, base+"/") }

// procStatusFields 解析一个 status 文件。
func procStatusFields(t *testing.T, path string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := make(map[string]string)
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			m[k] = strings.TrimSpace(v)
		}
	}
	return m
}

// TestInitEnvironment：编排环境（task：workspace + Gateway socket）与 exec 环境（/in + /out，无 Gateway），
// 各以新挂载 API 与 mount(2) 回退路径运行一次。init 就绪后检查：
//   - mountinfo：全部私有传播；旧根已脱离（只有一个 / 且为只读 tmpfs，挂载点全部属于预期集合）；rootfs 模板各项
//     （含子挂载）与 /in 为 ro,nosuid,nodev；/proc 只读路径为 ro；/sys 与 cgroupfs 未挂载；
//   - /proc 掩蔽项生效（目录为只读空 tmpfs，文件为 /dev/null）；/dev 只有 null/zero/random/urandom，/dev/shm 不存在；
//   - tmpfs 限额；task 的 Gateway socket（属主映射 uid 1000、0600）与 workspace（可写、nosuid、nodev）；exec 环境
//     没有 /run（Gateway socket 不可见）也没有 /workspace，/out 的 nr_inodes=1024；
//   - 每个线程的能力集合为 initCaps，inheritable 与 ambient 为空；
//   - fd 3、4、5 已关闭，其余 fd（除 0–2）都带 close-on-exec，其中有高位的控制 socket 与指向本二进制的 helper fd；
//   - init 已进入 Serve：start 经生产 Launcher（stage-2 helper）运行 /bin/true，得到 start_ack 然后 exit 0。
func TestInitEnvironment(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	for _, kind := range []string{KindTask, KindExec} {
		for _, classic := range []bool{false, true} {
			name := kind + "/new-mount-api"
			if classic {
				name = kind + "/mount2-fallback"
			}
			t.Run(name, func(t *testing.T) { checkInitEnvironment(t, kind, classic) })
		}
	}
}

func checkInitEnvironment(t *testing.T, kind string, classic bool) {
	spec := initTestSpec(t, kind, classic)
	s := startSandboxInit(t, spec)
	if s.ready != nil {
		t.Fatalf("init 未就绪: %v\n%s", s.ready, s.output())
	}
	if fell := strings.Contains(s.output(), "mount(2) 回退路径"); fell != classic {
		t.Fatalf("回退路径使用 = %v，期望 %v；输出:\n%s", fell, classic, s.output())
	}
	is := spec.Init
	rootDir := fmt.Sprintf("/proc/%d/root", s.pid)
	mounts := readMountinfo(t, s.pid)

	// 预期的挂载点集合。
	allowed := []string{"/tmp", "/dev/null", "/dev/zero", "/dev/random", "/dev/urandom", "/dev", "/proc"}
	if kind == KindExec {
		allowed = append(allowed, "/out", "/in")
	} else {
		allowed = append(allowed, "/run", gatewaySocket, "/workspace")
	}
	allowed = append(allowed, is.Template.Paths...)
	roots := 0
	for _, m := range mounts {
		if len(m.optional) != 0 {
			t.Errorf("挂载 %s 的传播不是私有: %v", m.point, m.optional)
		}
		if under(m.point, "/sys") || m.fstype == "sysfs" || m.fstype == "cgroup" || m.fstype == "cgroup2" {
			t.Errorf("沙箱内挂载了 %s（%s）", m.point, m.fstype)
		}
		if m.point == "/" {
			roots++
			if m.fstype != "tmpfs" || !hasAll(m.opts, "ro", "nosuid", "nodev") {
				t.Errorf("根挂载 = %s %v，期望只读 tmpfs（ro,nosuid,nodev）", m.fstype, m.opts)
			}
			continue
		}
		ok := under(m.point, "/proc")
		for _, a := range allowed {
			ok = ok || under(m.point, a)
		}
		if !ok {
			t.Errorf("意外的挂载点 %s（%s）：旧根未脱离或多余挂载", m.point, m.fstype)
		}
		ro := kind == KindExec && under(m.point, "/in")
		for _, p := range is.Template.Paths {
			ro = ro || under(m.point, p)
		}
		if ro && !hasAll(m.opts, "ro", "nosuid", "nodev") {
			t.Errorf("模板/只读挂载 %s 的选项 %v，期望含 ro,nosuid,nodev", m.point, m.opts)
		}
	}
	if roots != 1 {
		t.Errorf("挂载点 / 有 %d 个，期望 1 个（旧根已脱离）", roots)
	}
	find := func(point string) *mountinfoEntry {
		var last *mountinfoEntry
		for i := range mounts {
			if mounts[i].point == point {
				last = &mounts[i]
			}
		}
		return last
	}
	for _, p := range is.Template.Paths {
		if fi, err := os.Lstat(p); err == nil && fi.Mode()&os.ModeSymlink == 0 && find(p) == nil {
			t.Errorf("模板项 %s 没有挂载", p)
		}
	}

	// /proc 只读路径与掩蔽清单（宿主上同一内核存在的项）。
	for _, p := range procReadonlyPaths {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		if m := find(p); m == nil || !hasAll(m.opts, "ro") {
			t.Errorf("只读路径 %s 的挂载 = %+v，期望只读", p, m)
		}
	}
	for _, p := range procMaskPaths {
		hfi, err := os.Stat(p)
		if err != nil {
			continue
		}
		if hfi.IsDir() {
			m := find(p)
			ents, rerr := os.ReadDir(rootDir + p)
			if m == nil || m.fstype != "tmpfs" || !hasAll(m.opts, "ro") || rerr != nil || len(ents) != 0 {
				t.Errorf("目录掩蔽 %s：挂载 %+v，内容 %v（%v），期望只读空 tmpfs", p, m, ents, rerr)
			}
			continue
		}
		var st syscall.Stat_t
		if err := syscall.Stat(rootDir+p, &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFCHR || st.Rdev != 1<<8|3 {
			t.Errorf("文件掩蔽 %s：stat = %+v（%v），期望 /dev/null（字符设备 1:3）", p, st, err)
		}
	}

	// /dev。
	ents, err := os.ReadDir(rootDir + "/dev")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "null,random,urandom,zero" {
		t.Errorf("/dev 内容 = %v，期望 null、random、urandom、zero", names)
	}
	if _, err := os.Lstat(rootDir + "/dev/shm"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("/dev/shm 存在（%v），规格 §4.5 不提供", err)
	}
	if m := find("/dev"); m == nil || !hasAll(m.opts, "ro", "nosuid", "noexec") {
		t.Errorf("/dev 挂载 = %+v，期望只读 tmpfs", m)
	}

	// tmpfs 限额与环境类型相关的挂载。
	tmpSize := fmt.Sprintf("size=%dk", is.TmpBytes>>10)
	if m := find("/tmp"); m == nil || m.fstype != "tmpfs" || !hasAll(m.super, tmpSize) || !hasAll(m.opts, "rw", "nosuid", "nodev") {
		t.Errorf("/tmp 挂载 = %+v，期望 tmpfs %s", m, tmpSize)
	}
	hostWorkload := uint32(initTestIDBase + workloadID)
	if kind == KindExec {
		if m := find("/out"); m == nil || m.fstype != "tmpfs" || !hasAll(m.super, fmt.Sprintf("size=%dk", is.OutBytes>>10), "nr_inodes=1024") {
			t.Errorf("/out 挂载 = %+v，期望 tmpfs size=%dk,nr_inodes=1024", m, is.OutBytes>>10)
		}
		var st syscall.Stat_t
		if err := syscall.Stat(rootDir+"/out", &st); err != nil || st.Uid != hostWorkload {
			t.Errorf("/out 属主 = %d（%v），期望映射 uid 1000（宿主 %d）", st.Uid, err, hostWorkload)
		}
		if b, err := os.ReadFile(rootDir + "/in/marker"); err != nil || string(b) != "in" {
			t.Errorf("/in/marker = %q, %v", b, err)
		}
		for _, p := range []string{"/run", "/workspace"} {
			if _, err := os.Lstat(rootDir + p); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("exec 环境中 %s 存在（%v）：Gateway socket 与 workspace 不应可见", p, err)
			}
		}
	} else {
		if m := find("/run"); m == nil || m.fstype != "tmpfs" || !hasAll(m.super, tmpSize) {
			t.Errorf("/run 挂载 = %+v，期望 tmpfs %s", m, tmpSize)
		}
		var st syscall.Stat_t
		if err := syscall.Stat(rootDir+gatewaySocket, &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFSOCK ||
			st.Mode&0o7777 != 0o600 || st.Uid != hostWorkload {
			t.Errorf("Gateway socket stat = mode %#o uid %d（%v），期望属主映射 uid 1000 的 0600 socket", st.Mode, st.Uid, err)
		}
		if m := find(gatewaySocket); m == nil || !hasAll(m.opts, "nosuid", "nodev") {
			t.Errorf("Gateway socket 挂载 = %+v", m)
		}
		if b, err := os.ReadFile(rootDir + "/workspace/marker"); err != nil || string(b) != "ws" {
			t.Errorf("/workspace/marker = %q, %v", b, err)
		}
		if m := find("/workspace"); m == nil || !hasAll(m.opts, "rw", "nosuid", "nodev") {
			t.Errorf("/workspace 挂载 = %+v，期望 rw,nosuid,nodev", m)
		}
	}

	// 能力集合（逐线程）。
	tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", s.pid))
	if err != nil || len(tasks) == 0 {
		t.Fatalf("读取 init 的线程: %v", err)
	}
	want := map[string]string{
		"CapBnd": fmt.Sprintf("%016x", initCaps.Bounding), "CapPrm": fmt.Sprintf("%016x", initCaps.Permitted),
		"CapEff": fmt.Sprintf("%016x", initCaps.Effective), "CapInh": "0000000000000000", "CapAmb": "0000000000000000",
	}
	for _, task := range tasks {
		st := procStatusFields(t, fmt.Sprintf("/proc/%d/task/%s/status", s.pid, task.Name()))
		for k, v := range want {
			if st[k] != v {
				t.Errorf("线程 %s 的 %s = %s，期望 %s", task.Name(), k, st[k], v)
			}
		}
	}

	// FD：继承的 3、4、5 已关闭；其余（除 0–2）都带 close-on-exec；控制 socket 与 helper fd 在高位。
	var self syscall.Stat_t
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(exe, &self); err != nil {
		t.Fatal(err)
	}
	fdDir := fmt.Sprintf("/proc/%d/fd", s.pid)
	fds, err := os.ReadDir(fdDir)
	if err != nil {
		t.Fatal(err)
	}
	var control, helper bool
	for _, e := range fds {
		n, _ := strconv.Atoi(e.Name())
		if n <= 2 {
			continue
		}
		if n <= InitSpecFD {
			t.Errorf("继承的 fd %d 仍打开", n)
		}
		info, err := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/%d", s.pid, n))
		if err != nil {
			continue // 读取期间关闭
		}
		var flags uint64
		for _, line := range strings.Split(string(info), "\n") {
			if v, ok := strings.CutPrefix(line, "flags:"); ok {
				flags, _ = strconv.ParseUint(strings.TrimSpace(v), 8, 64)
			}
		}
		if flags&syscall.O_CLOEXEC == 0 {
			t.Errorf("init 的 fd %d 没有 close-on-exec（flags %o）", n, flags)
		}
		link, _ := os.Readlink(filepath.Join(fdDir, e.Name()))
		if n >= controlFDMin && n < helperFDMin && strings.HasPrefix(link, "socket:") {
			control = true
		}
		var st syscall.Stat_t
		if n >= helperFDMin && syscall.Stat(filepath.Join(fdDir, e.Name()), &st) == nil && st.Dev == self.Dev && st.Ino == self.Ino {
			helper = true
		}
	}
	if !control || !helper {
		t.Errorf("高位控制 socket 存在 = %v，helper fd 存在 = %v；期望都存在", control, helper)
	}

	// init 已进入 Serve：start 被拒绝（start_err），不运行 workload。
	nullFDs, err := devNullFDs()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.conn.Send(Message{Type: MsgStart, ExecID: "e1", Spec: argvSpec("/bin/true")}, nullFDs[:]); err != nil {
		t.Fatal(err)
	}
	for _, f := range nullFDs {
		f.Close()
	}
	for _, want := range []string{MsgStartAck, MsgExit} {
		m, _, err := s.conn.Recv()
		if err != nil || m.Type != want || m.ExecID != "e1" || (want == MsgExit && (m.Exit.Code != 0 || m.Exit.Signal != 0)) {
			t.Fatalf("start 的回复 = %+v, %v；期望 %s（exit 0）\n%s", m, err, want, s.output())
		}
	}
}

// TestInitFailurePointsReportInitErr：实验记录 §8 的 9 个注入点各自让环境建立失败：就绪管道收到
// init_err 的原因 `init/<步骤>: …`；init 退出，控制连接直接 EOF（没有任何消息，未进入 Serve，workload 未运行）。
func TestInitFailurePointsReportInitErr(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	for _, step := range initInjectionPoints {
		t.Run(step, func(t *testing.T) {
			spec := initTestSpec(t, KindTask, false)
			spec.Env = append(spec.Env, envTestInitFail+"="+step)
			s := startSandboxInit(t, spec)
			if s.ready == nil || !strings.HasPrefix(s.ready.Error(), "init/"+step+": ") || !strings.Contains(s.ready.Error(), errInjected.Error()) {
				t.Fatalf("就绪结果 = %v，期望 init/%s: %v\n%s", s.ready, step, errInjected, s.output())
			}
			assertInitGone(t, s)
		})
	}
}

// TestInitRealFailuresReportInitErr：不经注入的真实失败同样报告 init_err：规格不合法（tmp_bytes 为 0）、
// 模板路径在宿主上不存在、Gateway socket 的属主不是映射 uid 1000。
func TestInitRealFailuresReportInitErr(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	cases := map[string]struct {
		step   string
		mutate func(t *testing.T, s *LaunchSpec)
	}{
		"tmp_bytes 为 0": {stepSpec, func(t *testing.T, s *LaunchSpec) { s.Init.TmpBytes = 0 }},
		"模板路径不存在": {stepMountRootfs, func(t *testing.T, s *LaunchSpec) {
			s.Init.Template.Paths = append(s.Init.Template.Paths, "/nonexistent-agentbox-t10")
		}},
		"Gateway socket 属主": {stepMountGateway, func(t *testing.T, s *LaunchSpec) { s.Init.GatewaySocket = gatewayTestSocket(t, initTestDir(t), 0) }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			spec := initTestSpec(t, KindTask, false)
			c.mutate(t, &spec)
			s := startSandboxInit(t, spec)
			if s.ready == nil || !strings.HasPrefix(s.ready.Error(), "init/"+c.step+": ") {
				t.Fatalf("就绪结果 = %v，期望 init/%s 失败\n%s", s.ready, c.step, s.output())
			}
			assertInitGone(t, s)
		})
	}
}

// assertInitGone：init 已退出，控制连接读到 EOF 而没有任何消息。
func assertInitGone(t *testing.T, s *sandboxInit) {
	t.Helper()
	if !waitPidfdExit(s.pidfd, 10*time.Second) {
		t.Fatalf("init_err 之后 init %d 未退出", s.pid)
	}
	if m, _, err := s.conn.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("init_err 之后控制连接收到 %+v, %v；期望 EOF", m, err)
	}
}

// TestInitSpecValidate：init 拒绝不合法的输入（按环境类型的挂载组合、限额、路径形式、模板），不需要 root。
func TestInitSpecValidate(t *testing.T) {
	base := func() InitSpec {
		return InitSpec{Kind: KindTask, Template: rootfs.Template{Paths: []string{"/usr"}}, TmpBytes: 1 << 20,
			Workspace: "/w", GatewaySocket: "/g.sock"}
	}
	execSpec := func() InitSpec {
		return InitSpec{Kind: KindExec, Template: rootfs.Template{Paths: []string{"/usr"}}, TmpBytes: 1 << 20, In: "/i", OutBytes: 1 << 20}
	}
	for _, ok := range []InitSpec{base(), execSpec(), {Kind: KindSession, Template: rootfs.Template{Paths: []string{"/usr"}}, TmpBytes: 1}} {
		if err := ok.validate(); err != nil {
			t.Errorf("%+v: %v", ok, err)
		}
	}
	bad := map[string]InitSpec{}
	add := func(name string, s InitSpec, f func(*InitSpec)) { f(&s); bad[name] = s }
	add("未知类型", base(), func(s *InitSpec) { s.Kind = "vm" })
	add("tmp_bytes 为 0", base(), func(s *InitSpec) { s.TmpBytes = 0 })
	add("模板为空", base(), func(s *InitSpec) { s.Template.Paths = nil })
	add("模板与挂载点重叠", base(), func(s *InitSpec) { s.Template.Paths = []string{"/tmp/x"} })
	add("编排环境带 /out", base(), func(s *InitSpec) { s.OutBytes = 1 })
	add("编排环境带 /in", base(), func(s *InitSpec) { s.In = "/i" })
	add("workspace 相对路径", base(), func(s *InitSpec) { s.Workspace = "w" })
	add("exec 带 workspace", execSpec(), func(s *InitSpec) { s.Workspace = "/w" })
	add("exec 带 Gateway", execSpec(), func(s *InitSpec) { s.GatewaySocket = "/g" })
	add("exec 的 out_bytes 为 0", execSpec(), func(s *InitSpec) { s.OutBytes = 0 })
	add("exec 的 in 不规范", execSpec(), func(s *InitSpec) { s.In = "/i/../j" })
	for name, s := range bad {
		if err := s.validate(); err == nil {
			t.Errorf("%s: validate 通过，期望拒绝", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Task 11：stage-2 helper、init 的生产 Launcher 与 exec 提交点判据（规格 §4.6 helper 段与实现门槛 2）。
// 除用例 8（判定逻辑的时序边界，在 reaper 子进程中以测试钩子构造）外，都在真实沙箱中运行：init 由 RunLaunch
// 在 user namespace 中启动，helper 是本测试二进制（TestMain 的 exec-stage2 分流）。
// ---------------------------------------------------------------------------

// init 的 Task 11 测试钩子环境变量（TestMain 读取后设置 helperTestHook、startLockProbe）。
const (
	envTestHelperHook = "AGENTBOX_TEST_HELPER_HOOK"
	envTestLockProbe  = "AGENTBOX_TEST_LOCK_PROBE"
)

// lockProbe 是 init 的 startLockProbe：判定等待开始时获取一次 reg.mu，向 stderr 报告等待时长。
// 若启动路径在等待判定期间持有 reg.mu，等待会长达 helper 的延迟（同一 goroutine 持有则死锁，测试超时）。
func lockProbe(r *Registry, execID string) {
	t0 := time.Now()
	r.mu.Lock()
	w := time.Since(t0)
	r.mu.Unlock()
	fmt.Fprintf(os.Stderr, "lock-probe %s waited_us=%d\n", execID, w.Microseconds())
}

// helperTestInit 启动一个 exec 环境的 init（生产 Launcher）；env 是附加给 init 的测试钩子，mutate 可修改规格。
// 返回 init 与其控制连接上的消息流。
func helperTestInit(t *testing.T, mutate func(*LaunchSpec), env ...string) (*sandboxInit, <-chan Message) {
	t.Helper()
	spec := initTestSpec(t, KindExec, false)
	spec.Env = append(spec.Env, env...)
	if mutate != nil {
		mutate(&spec)
	}
	s := startSandboxInit(t, spec)
	if s.ready != nil {
		t.Fatalf("init 未就绪: %v\n%s", s.ready, s.output())
	}
	return s, pumpMessages(s.conn)
}

// pumpMessages 在单独的 goroutine 中读取 c，直到连接结束（之后关闭返回的 channel）。
func pumpMessages(c *Conn) <-chan Message {
	ch := make(chan Message, 256)
	go func() {
		defer close(ch)
		for {
			m, files, err := c.Recv()
			closeFiles(files)
			if err != nil {
				return
			}
			ch <- m
		}
	}()
	return ch
}

func nextMsg(t *testing.T, s *sandboxInit, ch <-chan Message) Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatalf("控制连接在期望的消息之前结束\n%s", s.output())
		}
		return m
	case <-time.After(20 * time.Second):
		t.Fatalf("等待消息超时\n%s", s.output())
	}
	return Message{}
}

// sendStart 发送 start：stdin、stderr 为 /dev/null，stdout 为 out（nil 时为 /dev/null）。发送后关闭这些子端副本。
func sendStart(t *testing.T, s *sandboxInit, execID string, out *os.File, argv ...string) {
	t.Helper()
	fds, err := devNullFDs()
	if err != nil {
		t.Fatal(err)
	}
	files := fds[:]
	if out != nil {
		fds[1].Close()
		files = []*os.File{fds[0], out, fds[2]}
	}
	err = s.conn.Send(Message{Type: MsgStart, ExecID: execID, Spec: argvSpec(argv...)}, files)
	closeFiles(files)
	if err != nil {
		t.Fatal(err)
	}
}

// closeAndDrain 半关闭控制连接（宿主不再发送；init 的 Serve 读到 EOF 后返回），断言此后直到连接结束没有任何
// 消息，然后等待 init 退出。
func closeAndDrain(t *testing.T, s *sandboxInit, ch <-chan Message) {
	t.Helper()
	if err := s.conn.rc.Control(func(fd uintptr) { _ = syscall.Shutdown(int(fd), syscall.SHUT_WR) }); err != nil {
		t.Fatal(err)
	}
	timeout := time.After(20 * time.Second)
drain:
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				break drain
			}
			t.Errorf("半关闭之后收到意外的消息 %+v", m)
		case <-timeout:
			t.Fatalf("半关闭之后控制连接未结束\n%s", s.output())
		}
	}
	if !waitPidfdExit(s.pidfd, 10*time.Second) {
		t.Fatalf("init 未退出\n%s", s.output())
	}
}

// inSandbox 返回沙箱内路径 p 在宿主上的路径（经 init 的根）。只在 init 存活时有效。
func inSandbox(s *sandboxInit, p string) string { return fmt.Sprintf("/proc/%d/root%s", s.pid, p) }

func fileExists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// expectAckExit 读取 execID 的 start_ack 与随后的 exit（顺序断言），返回 ack 的 pid 与退出状态。
func expectAckExit(t *testing.T, s *sandboxInit, ch <-chan Message, execID string) (int, ExitInfo) {
	t.Helper()
	ack := nextMsg(t, s, ch)
	if ack.Type != MsgStartAck || ack.ExecID != execID || ack.PID <= 0 {
		t.Fatalf("第一条消息 = %+v，期望 %s 的 start_ack\n%s", ack, execID, s.output())
	}
	ex := nextMsg(t, s, ch)
	if ex.Type != MsgExit || ex.ExecID != execID || ex.Exit == nil {
		t.Fatalf("start_ack 之后的消息 = %+v，期望 %s 的 exit\n%s", ex, execID, s.output())
	}
	return ack.PID, *ex.Exit
}

// TestHelperFailurePoints：提交点矩阵 (1)——helper 降权序列的 12 个步骤各自注入失败：start_err 的原因为
// `helper/<步骤>: <原因>`，workload 未运行（它应写的标记文件不存在），且之后没有 start_ack 与 exit。
// 另有一个不经注入的真实失败：execve 不存在的路径（ENOENT）。
func TestHelperFailurePoints(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	if len(helperSteps) != 12 {
		t.Fatalf("helper 的注入点有 %d 个，期望 12 个", len(helperSteps))
	}
	for _, step := range helperSteps {
		t.Run(step, func(t *testing.T) {
			s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookFailPrefix+step)
			sendStart(t, s, "e1", nil, "/bin/sh", "-c", "touch /tmp/started")
			m := nextMsg(t, s, ch)
			want := "helper/" + step + ": " + errInjected.Error()
			if m.Type != MsgStartErr || m.ExecID != "e1" || m.Reason != want {
				t.Fatalf("回复 = %+v，期望 start_err{%s}\n%s", m, want, s.output())
			}
			if fileExists(inSandbox(s, "/tmp/started")) {
				t.Fatal("start_err 之后 workload 的标记文件存在：workload 运行了")
			}
			closeAndDrain(t, s, ch)
		})
	}
	t.Run("execve 真实失败", func(t *testing.T) {
		s, ch := helperTestInit(t, nil)
		sendStart(t, s, "e1", nil, "/nonexistent/workload")
		m := nextMsg(t, s, ch)
		if m.Type != MsgStartErr || !strings.HasPrefix(m.Reason, "helper/execve: ") || !strings.Contains(m.Reason, "no such file") {
			t.Fatalf("回复 = %+v，期望 start_err{helper/execve: no such file or directory}\n%s", m, s.output())
		}
		closeAndDrain(t, s, ch)
	})
}

// TestHelperDiedBeforeExec：提交点矩阵 (2)——helper 完成全部降权步骤后、execve 之前 SIGKILL 自身（exec-status
// 管道同样是"EOF 无字节"）：start_err{helper/died_before_exec: signal killed}，workload 未运行，没有 start_ack
// 与 exit。重复 5 次，判定确定（不依赖 reaper 与 EOF 的先后）。
func TestHelperDiedBeforeExec(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookSigkill)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("e%d", i)
		sendStart(t, s, id, nil, "/bin/sh", "-c", "touch /tmp/started")
		m := nextMsg(t, s, ch)
		if m.Type != MsgStartErr || m.ExecID != id || m.Reason != "helper/died_before_exec: signal killed" {
			t.Fatalf("第 %d 次回复 = %+v，期望 start_err{helper/died_before_exec: signal killed}\n%s", i, m, s.output())
		}
	}
	if fileExists(inSandbox(s, "/tmp/started")) {
		t.Fatal("workload 的标记文件存在：workload 运行了")
	}
	closeAndDrain(t, s, ch)
}

// TestHelperCommitFastExit：提交点矩阵 (3)——workload /bin/true 立即退出：start_ack 然后 exit 0（顺序断言），
// 重复 20 次（覆盖观察点上 workload 已在退出、判定等待 reaper 记录的路径）。
func TestHelperCommitFastExit(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("e%d", i)
		sendStart(t, s, id, nil, "/bin/true")
		if _, ex := expectAckExit(t, s, ch, id); ex.Code != 0 || ex.Signal != 0 {
			t.Fatalf("%s 的退出状态 = %+v，期望 0", id, ex)
		}
	}
	closeAndDrain(t, s, ch)
}

// TestHelperCommitExit126：提交点矩阵 (4)——已 exec 的 workload 以 126 退出。两种结局都是规格 §4.6 门槛 2 接受的：
//   - start_ack 然后 exit 126：init 在 EOF 后的单次状态读取时 workload 仍存活；
//   - start_err{helper/died_before_exec: exit 126}：workload 在 init 读取状态前已退出——126 是 helper 保留的失败退出码，
//     "EOF 无字节 + 退出码 126"对 init 不可区分（残余边界窗口，慢机器上更常见）。
//
// 两种情形标记文件都必须存在（workload 确实运行过），并记录各自出现的次数。
func TestHelperCommitExit126(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil)
	acked, died := 0, 0
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("e%d", i)
		marker := fmt.Sprintf("/tmp/ran126-%d", i)
		sendStart(t, s, id, nil, "/bin/sh", "-c", "touch "+marker+"; exit 126")
		m := nextMsg(t, s, ch)
		switch {
		case m.Type == MsgStartAck && m.ExecID == id && m.PID > 0:
			ex := nextMsg(t, s, ch)
			if ex.Type != MsgExit || ex.ExecID != id || ex.Exit == nil || ex.Exit.Code != 126 || ex.Exit.Signal != 0 {
				t.Fatalf("%s：start_ack 之后的消息 = %+v，期望 exit 126\n%s", id, ex, s.output())
			}
			acked++
		case m.Type == MsgStartErr && m.ExecID == id && m.Reason == "helper/died_before_exec: exit 126":
			died++
		default:
			t.Fatalf("%s 的第一条消息 = %+v，期望 start_ack 或 died_before_exec: exit 126\n%s", id, m, s.output())
		}
		if !fileExists(inSandbox(s, marker)) {
			t.Fatalf("%s 的标记文件不存在：workload 没有运行", id)
		}
	}
	t.Logf("exit 126 的快速退出：start_ack %d 次，died_before_exec（残余窗口）%d 次", acked, died)
	closeAndDrain(t, s, ch)
}

// TestHelperCommitSameName：提交点矩阵 (5)——workload 二进制复制为与 helper 同名（agentbox-helper，进程名与
// helper 的 PR_SET_NAME 相同）：判定不受影响，start_ack 然后 exit 3。workload 记录自己的进程名以证明同名。
func TestHelperCommitSameName(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, func(spec *LaunchSpec) {
		src, err := filepath.EvalSymlinks("/bin/sh")
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(spec.Init.In, helperComm), b, 0o755); err != nil {
			t.Fatal(err)
		}
	})
	sendStart(t, s, "e1", nil, "/in/"+helperComm, "-c", "read c < /proc/$$/comm; echo \"$c\" > /tmp/same; exit 3")
	if _, ex := expectAckExit(t, s, ch, "e1"); ex.Code != 3 || ex.Signal != 0 {
		t.Fatalf("退出状态 = %+v，期望 3", ex)
	}
	if b, err := os.ReadFile(inSandbox(s, "/tmp/same")); err != nil || strings.TrimSpace(string(b)) != helperComm {
		t.Fatalf("workload 的进程名 = %q（%v），期望 %q", b, err, helperComm)
	}
	closeAndDrain(t, s, ch)
}

// TestHelperCommitKilledAfterStart：提交点矩阵 (6)——workload 启动后立即 SIGKILL 自身：start_ack 然后
// exit{signal killed}；若落入残余竞争窗口（观察点上内核已在退出该进程）则为 start_err{helper/died_before_exec:
// signal killed} 且没有 exit。两者都接受，记录频次（-v 输出）。
func TestHelperCommitKilledAfterStart(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil)
	const n = 50
	acked, died := 0, 0
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("e%d", i)
		sendStart(t, s, id, nil, "/bin/sh", "-c", "kill -9 $$")
		m := nextMsg(t, s, ch)
		switch {
		case m.Type == MsgStartAck && m.ExecID == id:
			ex := nextMsg(t, s, ch)
			if ex.Type != MsgExit || ex.ExecID != id || ex.Exit == nil || ex.Exit.Signal != syscall.SIGKILL {
				t.Fatalf("start_ack 之后 = %+v，期望 exit{signal killed}", ex)
			}
			acked++
		case m.Type == MsgStartErr && m.ExecID == id && m.Reason == "helper/died_before_exec: signal killed":
			died++
		default:
			t.Fatalf("回复 = %+v，期望 start_ack 或 start_err{helper/died_before_exec: signal killed}\n%s", m, s.output())
		}
	}
	t.Logf("用例 6 频次：start_ack + exit(SIGKILL) %d 次，died_before_exec %d 次（共 %d 次）", acked, died, n)
	closeAndDrain(t, s, ch) // died_before_exec 的那些没有迟到的 exit
}

// TestHelperCommitControlLost：提交点矩阵 (7)——判定期间（helper 挂起在 execve 之前）宿主断开控制连接：init 终止
// 该 pid 的进程组、清理登记，不投递任何消息（宿主侧按 ErrControlLost 处理）。
func TestHelperCommitControlLost(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookPause)
	sendStart(t, s, "e1", nil, "/bin/sh", "-c", "touch /tmp/started")
	select {
	case m := <-ch:
		t.Fatalf("判定期间收到 %+v\n%s", m, s.output())
	case <-time.After(time.Second):
	}
	if fileExists(inSandbox(s, "/tmp/started")) {
		t.Fatal("helper 挂起期间 workload 的标记文件存在")
	}
	closeAndDrain(t, s, ch)
	if out := s.output(); !strings.Contains(out, "终止启动中的 exec e1") || !strings.Contains(out, "登记已清理") {
		t.Fatalf("init 没有报告终止启动中的 exec；输出:\n%s", out)
	}
}

// TestCommitBoundaryClassification：提交点矩阵 (8)——判定的时序边界，以测试钩子控制 reaper 记录相对 EOF 观察的
// 先后（reaper 子进程中运行真实的 Registry、startCommit 与 SIGCHLD 收割；子进程持有 exec-status 写端作为 fd 3）：
//   - reaper 先于 EOF 观察记录信号终止 → start_err{helper/died_before_exec: signal killed}，没有 exit；
//   - reaper 先于 EOF 观察记录以 126 退出（管道无字节）→ start_err{helper/died_before_exec: exit 126}；
//   - EOF 先于终止（观察点之后才 SIGKILL）→ start_ack 然后 exit{signal killed}。
func TestCommitBoundaryClassification(t *testing.T) {
	for _, name := range []string{"commit-reap-first-signal", "commit-reap-first-126", "commit-eof-first"} {
		t.Run(name, func(t *testing.T) { runReaperCase(t, name) })
	}
}

// waitRecorded 阻塞直到 reaper 记录了 p 的终止。
func waitRecorded(r *Registry, p *pending) {
	r.mu.Lock()
	for !p.exited {
		r.cond.Wait()
	}
	r.mu.Unlock()
}

// commitChild 用真实的 Registry 判定一次启动：spawn（在 reg.mu 内）以 exec.Cmd 启动 argv，exec-status 管道的
// 写端作为其 fd 3；另一个 goroutine 在 SIGCHLD 时 Reap。收集 want 条消息后再等 300ms，确认没有多余的消息。
func commitChild(argv []string, want int, before, after func(*Registry, *pending)) ([]Message, error) {
	commitBeforeRead, commitAfterObserve = before, after
	defer func() { commitBeforeRead, commitAfterObserve = nil, nil }()

	msgs := make(chan Message, 16)
	reg := NewRegistry(func(m Message) error { msgs <- m; return nil })
	sigch := make(chan os.Signal, 8)
	signal.Notify(sigch, syscall.SIGCHLD)
	defer signal.Stop(sigch)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-sigch:
				_ = reg.Reap()
			case <-stop:
				return
			}
		}
	}()

	err := reg.startCommit("c1", func() (int, *os.File, error) {
		r, w, err := os.Pipe()
		if err != nil {
			return 0, nil, err
		}
		defer w.Close()
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.ExtraFiles = []*os.File{w}
		if err := cmd.Start(); err != nil {
			r.Close()
			return 0, nil, err
		}
		pid := cmd.Process.Pid
		_ = cmd.Process.Release()
		return pid, r, nil
	})
	if err != nil {
		return nil, err
	}
	var got []Message
	timeout := time.After(10 * time.Second)
	for len(got) < want {
		select {
		case m := <-msgs:
			got = append(got, m)
		case <-timeout:
			return got, fmt.Errorf("只收到 %d 条消息 %+v，期望 %d 条", len(got), got, want)
		}
	}
	reg.commits.Wait()
	select {
	case m := <-msgs:
		return got, fmt.Errorf("多余的消息 %+v（已收到 %+v）", m, got)
	case <-time.After(300 * time.Millisecond):
	}
	return got, nil
}

func commitChildReapFirstSignal() error {
	got, err := commitChild([]string{"/bin/sh", "-c", "kill -9 $$"}, 1, waitRecorded, nil)
	if err != nil {
		return err
	}
	if m := got[0]; m.Type != MsgStartErr || m.Reason != "helper/died_before_exec: signal killed" {
		return fmt.Errorf("消息 = %+v，期望 start_err{helper/died_before_exec: signal killed}", m)
	}
	return nil
}

func commitChildReapFirst126() error {
	got, err := commitChild([]string{"/bin/sh", "-c", "exit 126"}, 1, waitRecorded, nil)
	if err != nil {
		return err
	}
	if m := got[0]; m.Type != MsgStartErr || m.Reason != "helper/died_before_exec: exit 126" {
		return fmt.Errorf("消息 = %+v，期望 start_err{helper/died_before_exec: exit 126}", m)
	}
	return nil
}

func commitChildEOFFirst() error {
	killAfterObserve := func(r *Registry, p *pending) {
		_ = syscall.Kill(p.pid, syscall.SIGKILL)
		waitRecorded(r, p)
	}
	got, err := commitChild([]string{"/bin/sh", "-c", "exec 3>&-; exec sleep 30"}, 2, nil, killAfterObserve)
	if err != nil {
		return err
	}
	if m := got[0]; m.Type != MsgStartAck || m.PID <= 0 {
		return fmt.Errorf("第一条消息 = %+v，期望 start_ack", m)
	}
	if m := got[1]; m.Type != MsgExit || m.Exit == nil || m.Exit.Signal != syscall.SIGKILL {
		return fmt.Errorf("第二条消息 = %+v，期望 exit{signal killed}", m)
	}
	return nil
}

// TestHelperConcurrentStarts：提交点矩阵 (9)——32 次启动并发判定（helper 在 execve 之前延迟 500ms，使全部判定
// 重叠）：每个 workload 报告的 pid 与其 start_ack 的 pid 一致、互不相同，每个 exec 先 start_ack 后 exit 0；
// 锁等待钩子在每次判定等待开始时获取 reg.mu，等待均不超过 100ms（启动路径在判定期间不持有 reg.mu），
// 总耗时远小于串行所需。
func TestHelperConcurrentStarts(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookDelay, envTestLockProbe+"=1")
	const n = 32
	outs := make([]*os.File, n)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		outs[i] = r
		defer r.Close()
		sendStart(t, s, fmt.Sprintf("e%d", i), w, "/bin/sh", "-c", "echo $$")
	}
	acks := make(map[string]int)
	exits := make(map[string]bool)
	for len(acks) < n || len(exits) < n {
		m := nextMsg(t, s, ch)
		switch m.Type {
		case MsgStartAck:
			if _, dup := acks[m.ExecID]; dup || exits[m.ExecID] {
				t.Fatalf("%s 的 start_ack 重复或晚于 exit", m.ExecID)
			}
			acks[m.ExecID] = m.PID
		case MsgExit:
			if _, ok := acks[m.ExecID]; !ok {
				t.Fatalf("%s 的 exit 早于 start_ack", m.ExecID)
			}
			if m.Exit.Code != 0 || m.Exit.Signal != 0 {
				t.Fatalf("%s 的退出状态 = %+v", m.ExecID, m.Exit)
			}
			exits[m.ExecID] = true
		default:
			t.Fatalf("意外的消息 %+v\n%s", m, s.output())
		}
	}
	elapsed := time.Since(t0)
	seen := make(map[int]string)
	for i, r := range outs {
		id := fmt.Sprintf("e%d", i)
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || pid != acks[id] {
			t.Errorf("%s：workload 报告 pid %q，start_ack 的 pid 为 %d（pid 错配）", id, b, acks[id])
		}
		if other, dup := seen[pid]; dup {
			t.Errorf("%s 与 %s 的 pid 相同（%d）", id, other, pid)
		}
		seen[pid] = id
	}
	closeAndDrain(t, s, ch)

	var probes int
	var maxWait time.Duration
	for _, line := range strings.Split(s.output(), "\n") {
		_, v, ok := strings.Cut(line, "waited_us=")
		if !strings.HasPrefix(line, "lock-probe ") || !ok {
			continue
		}
		us, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			t.Fatalf("无法解析 %q", line)
		}
		probes++
		maxWait = max(maxWait, time.Duration(us)*time.Microsecond)
	}
	t.Logf("32 次并发启动：总耗时 %v，锁等待钩子 %d 次，最长等待 %v", elapsed, probes, maxWait)
	if probes != n {
		t.Errorf("锁等待钩子触发 %d 次，期望 %d 次", probes, n)
	}
	if maxWait > 100*time.Millisecond {
		t.Errorf("判定等待期间获取 reg.mu 最长等待 %v：启动路径在判定期间持有 reg.mu", maxWait)
	}
	if limit := n * helperHookDelay / 4; elapsed > limit {
		t.Errorf("32 次启动耗时 %v，超过 %v：判定被串行化", elapsed, limit)
	}
}

// identityScript 是 workload 内核验身份与限制的 Python 程序：在导入其他模块、打开任何文件之前探测 fd 3..1023
// （os 在解释器启动时已加载），再起一个线程读取每个线程的能力集合，输出 JSON。
const identityScript = `
import os
fds = []
for fd in range(3, 1024):
    try:
        os.fstat(fd)
        fds.append('%d->%s' % (fd, os.readlink('/proc/self/fd/%d' % fd)))
    except OSError:
        pass
import ctypes, json, resource, threading
ev = threading.Event()
th = threading.Thread(target=ev.wait)
th.start()
tasks = {}
for tid in os.listdir('/proc/self/task'):
    d = {}
    with open('/proc/self/task/%s/status' % tid) as f:
        for line in f:
            k, _, v = line.partition(':')
            d[k] = v.strip()
    tasks[tid] = {k: d.get(k) for k in ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb', 'NoNewPrivs', 'Seccomp')}
ev.set()
th.join()
libc = ctypes.CDLL(None, use_errno=True)
print(json.dumps({
    'fds': fds, 'resuid': os.getresuid(), 'resgid': os.getresgid(), 'groups': os.getgroups(), 'tasks': tasks,
    'securebits': libc.prctl(27, 0, 0, 0, 0),
    'nofile': resource.getrlimit(resource.RLIMIT_NOFILE), 'core': resource.getrlimit(resource.RLIMIT_CORE),
    'fsize': resource.getrlimit(resource.RLIMIT_FSIZE),
}))
`

// TestHelperWorkloadIdentity：workload 内核验降权结果——uid/gid 均为 1000、无附加组；每个线程的能力集合（含
// bounding）全 0、NoNewPrivs 1、seccomp 过滤模式；securebits 0x0f（NOROOT、NO_SETUID_FIXUP 及其锁定位）；
// 除 0/1/2 外没有打开的 fd；RLIMIT_NOFILE、RLIMIT_CORE=0 与 exec 环境的 RLIMIT_FSIZE 为 helper 显式设置的值。
func TestHelperWorkloadIdentity(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	s, ch := helperTestInit(t, nil)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	sendStart(t, s, "id", w, "/usr/bin/python3", "-c", identityScript)
	if _, ex := expectAckExit(t, s, ch, "id"); ex.Code != 0 || ex.Signal != 0 {
		t.Fatalf("workload 退出状态 = %+v\n%s", ex, s.output())
	}
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		FDs        []string                     `json:"fds"`
		ResUID     []int                        `json:"resuid"`
		ResGID     []int                        `json:"resgid"`
		Groups     []int                        `json:"groups"`
		Tasks      map[string]map[string]string `json:"tasks"`
		Securebits int                          `json:"securebits"`
		NoFile     []uint64                     `json:"nofile"`
		Core       []uint64                     `json:"core"`
		FSize      []uint64                     `json:"fsize"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("解析 workload 输出 %q: %v", b, err)
	}
	ids := fmt.Sprint(workloadID, workloadID, workloadID)
	if fmt.Sprint(got.ResUID) != "["+ids+"]" || fmt.Sprint(got.ResGID) != "["+ids+"]" || len(got.Groups) != 0 {
		t.Errorf("身份 resuid=%v resgid=%v groups=%v，期望 1000 且无附加组", got.ResUID, got.ResGID, got.Groups)
	}
	if len(got.Tasks) < 2 {
		t.Errorf("只检查到 %d 个线程，期望至少 2 个", len(got.Tasks))
	}
	for tid, st := range got.Tasks {
		for _, k := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
			if st[k] != "0000000000000000" {
				t.Errorf("线程 %s 的 %s = %s，期望全 0", tid, k, st[k])
			}
		}
		if st["NoNewPrivs"] != "1" || st["Seccomp"] != "2" {
			t.Errorf("线程 %s：NoNewPrivs=%s Seccomp=%s，期望 1 与 2", tid, st["NoNewPrivs"], st["Seccomp"])
		}
	}
	if got.Securebits != workloadSecurebits {
		t.Errorf("securebits = %#x，期望 %#x", got.Securebits, workloadSecurebits)
	}
	if len(got.FDs) != 0 {
		t.Errorf("除 0/1/2 外打开的 fd: %v", got.FDs)
	}
	fsize := uint64(64 << 20) // initTestSpec：max(TmpBytes 64 MiB, OutBytes 1 MiB)
	if fmt.Sprint(got.NoFile) != fmt.Sprint([]uint64{workloadNoFile, workloadNoFile}) ||
		fmt.Sprint(got.Core) != "[0 0]" || fmt.Sprint(got.FSize) != fmt.Sprint([]uint64{fsize, fsize}) {
		t.Errorf("rlimit nofile=%v core=%v fsize=%v，期望 [%d %d]、[0 0]、[%d %d]",
			got.NoFile, got.Core, got.FSize, workloadNoFile, workloadNoFile, fsize, fsize)
	}
	closeAndDrain(t, s, ch)
}

// TestParseStatStateFlags：观察点的 /proc/<pid>/stat 解析只取最后一个 ')' 之后的 state 与 flags（不需要 root）：
// 存活（S/R，flags 不含 PF_EXITING）、僵尸 Z、R 但 PF_EXITING 已置位、comm 含空格与括号（`) ) weird name (`）
// 且状态为 Z——都按 state/flags 正确分类；签名只返回 state、flags 与错误（不读取、不返回 comm）。
func TestParseStatStateFlags(t *testing.T) {
	if ft := reflect.TypeOf(parseStatStateFlags); ft.NumOut() != 3 || ft.Out(0).Kind() != reflect.Uint8 ||
		ft.Out(1).Kind() != reflect.Uint64 || ft.Out(2) != reflect.TypeOf((*error)(nil)).Elem() {
		t.Fatalf("parseStatStateFlags 的签名 = %v，期望只返回 (state byte, flags uint64, error)", ft)
	}
	// 字段：state ppid pgrp session tty_nr tpgid flags ...（flags 为十进制）
	line := func(comm string, state byte, flags uint64) []byte {
		return []byte(fmt.Sprintf("42 (%s) %c 1 42 42 0 -1 %d 0 0 0 0 1 2 3 4 20 0 1 0 100 0 0\n", comm, state, flags))
	}
	const otherFlags = 0x40 | 0x400000 // PF_FORKNOEXEC | PF_RANDOMIZE：不含 PF_EXITING
	cases := []struct {
		name    string
		b       []byte
		exiting bool
	}{
		{"存活 S", line("sh", 'S', otherFlags), false},
		{"存活 R", line("agentbox-helper", 'R', otherFlags), false},
		{"僵尸 Z", line("sh", 'Z', otherFlags), true},
		{"R 且 PF_EXITING", line("sh", 'R', otherFlags|pfExiting), true},
		{"comm 含括号与空格，状态 Z", line(") ) weird name (", 'Z', otherFlags), true},
		{"comm 含括号与空格，存活 S", line(") ) Z 1 2 3 4 5 4 (", 'S', otherFlags), false},
	}
	for _, c := range cases {
		state, flags, err := parseStatStateFlags(c.b)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got := state == 'Z' || state == 'X' || flags&pfExiting != 0; got != c.exiting {
			t.Errorf("%s: state=%c flags=%#x → 终止已开始 = %v，期望 %v", c.name, state, flags, got, c.exiting)
		}
	}
	for _, bad := range []string{"", "42 sh S 1", "42 (sh) S 1 2", "42 (sh) S 1 2 3 4 5 notanumber"} {
		if _, _, err := parseStatStateFlags([]byte(bad)); err == nil {
			t.Errorf("%q: 解析成功，期望错误", bad)
		}
	}
	// 本进程（存活）：真实的 /proc/self/stat 可解析，且终止未开始。
	if exiting, err := taskExiting(os.Getpid()); err != nil || exiting {
		t.Errorf("taskExiting(self) = %v, %v；期望 false", exiting, err)
	}
}

// ---------------------------------------------------------------------------
// Task 12（12b）：cgo 守卫（规格 §4.6 实现门槛 3）与 §16.2 生产验收。
// 检查器是本测试二进制的 isolation-check 子命令（移植自 experiments/spike-1b/check.go）：复制进沙箱后，经生产启动
// 路径——专用启动进程（RunLaunch）→ init（RunInit：命名空间、挂载、pivot_root、init 能力集）→ 生产 Launcher
// （rawfork + execveat）→ stage-2 helper（降权、seccomp）→ execve——作为 workload 运行，在 workload 内核验并逐项输出
// `CHECK <名称> PASS|FAIL <细节>`；宿主侧按 §16.2 条目汇总，每个条目一个子测试。
// ---------------------------------------------------------------------------

// TestHelperRejectsCgoBuild：helper 的第一步是 cgo 守卫（不需要 root）。以 exec-stage2 子命令直接运行本测试
// 二进制（fd 3 为 exec-status 管道，规格为空对象）：cgo 构建中原因为 helper/cgo_enabled，先于规格解析；
// CGO_ENABLED=0 构建中守卫不存在，原因为规格解析失败（helper/spec）。两者都以 126 退出。
func TestHelperRejectsCgoBuild(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cmd := exec.Command(exe, HelperArg, "{}")
	cmd.ExtraFiles = []*os.File{w}
	runErr := cmd.Run()
	w.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	want := "helper/" + hStepSpec + ": argv 为空"
	if cgoBuild {
		want = "helper/" + hStepCgo + ": " + errCgoBuild.Error()
	}
	var ee *exec.ExitError
	if !errors.As(runErr, &ee) || ee.ExitCode() != 126 || string(b) != want {
		t.Fatalf("helper 退出 %v，exec-status = %q；期望 126 与 %q", runErr, b, want)
	}
	t.Logf("cgo 构建 = %v；helper 回报 %q", cgoBuild, b)
}

// isolationCheckArg 是检查器子命令（os.Args[1]）；os.Args[2] 是 JSON 的 checkConfig。
const isolationCheckArg = "isolation-check"

// checkerName 是检查器复制进沙箱后的文件名（exec 环境 /in 下，编排环境 /workspace 下）。
const checkerName = "agentbox-check"

// checkConfig 是宿主交给检查器的期望值。
type checkConfig struct {
	Kind     string   `json:"kind"`
	Baseline int      `json:"baseline"` // 宿主同法读取的 Seccomp_filters 基线
	Template []string `json:"template"` // rootfs 模板路径（允许的只读挂载点）
	Gateway  bool     `json:"gateway"`  // 编排环境：Gateway socket 应对 uid 1000 可访问（回复 pong）
	WSPhase  string   `json:"ws_phase,omitempty"`
	// WSWritable：/workspace 应可写（保留范围）；外来范围下为 false。
	WSWritable bool `json:"ws_writable"`
}

// checker 汇总检查结果（输出到 stdout，宿主解析）。
type checker struct {
	cfg   checkConfig
	fails int
}

func (c *checker) check(name string, ok bool, format string, a ...any) {
	res := "PASS"
	if !ok {
		res = "FAIL"
		c.fails++
	}
	fmt.Printf("CHECK %s %s %s\n", name, res, strings.ReplaceAll(fmt.Sprintf(format, a...), "\n", " "))
}

func chkErr(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// runIsolationCheck 是检查器主函数。FD 快照必须是第一步（在打开任何文件之前）。
func runIsolationCheck() int {
	fds := chkListFDs()
	var c checker
	if len(os.Args) != 3 || json.Unmarshal([]byte(os.Args[2]), &c.cfg) != nil {
		c.check("config", false, "参数 %q", os.Args[1:])
		return 1
	}
	fmt.Printf("WORKLOAD_STARTED pid=%d\n", os.Getpid())
	c.checkFDs(fds)
	c.checkThreads()
	c.checkSecurebits()
	c.checkIDs()
	c.checkSyscalls()
	c.checkMounts()
	c.checkMisc()
	c.checkEnvSpecific()
	c.checkPython()
	fmt.Printf("CHECK_SUMMARY fail=%d\n", c.fails)
	if c.fails > 0 {
		return 1
	}
	return 0
}

// chkListFDs 列出本进程打开的 FD（`名->目标`），排除列举自身打开的目录 fd。
func chkListFDs() []string {
	dfd, err := syscall.Open("/proc/self/fd", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return []string{"open /proc/self/fd: " + err.Error()}
	}
	defer syscall.Close(dfd)
	var names []string
	buf := make([]byte, 8192)
	for {
		n, err := syscall.ReadDirent(dfd, buf)
		if err != nil || n <= 0 {
			break
		}
		_, _, names = syscall.ParseDirent(buf[:n], -1, names)
	}
	var out []string
	for _, nm := range names {
		if fd, err := strconv.Atoi(nm); err != nil || fd == dfd {
			continue
		}
		target, _ := os.Readlink("/proc/self/fd/" + nm)
		out = append(out, nm+"->"+target)
	}
	return out
}

// checkFDs：除 0/1/2 外无继承 FD；0/1/2 逐个解析，都是宿主交来的管道（不是宿主路径、控制 socket 或 helper 二进制）。
func (c *checker) checkFDs(fds []string) {
	var extra, std []string
	stdOK := true
	for _, l := range fds {
		nm, target, _ := strings.Cut(l, "->")
		if nm == "0" || nm == "1" || nm == "2" {
			std = append(std, l)
			stdOK = stdOK && strings.HasPrefix(target, "pipe:[")
			continue
		}
		extra = append(extra, l)
	}
	c.check("fds_no_extra", len(extra) == 0, "extra=%q", extra)
	c.check("fds_012_pipes", stdOK && len(std) == 3, "std=%q", std)
}

func chkStatus(path string) map[string]string {
	b, _ := os.ReadFile(path)
	m := make(map[string]string)
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			m[k] = strings.TrimSpace(v)
		}
	}
	return m
}

// checkThreads：逐线程（另起 4 个锁定 OS 线程的 goroutine，保证有多个线程可查）能力集合全 0、NoNewPrivs 1、
// Seccomp 2 且 Seccomp_filters ≥ 宿主基线 + 1。
func (c *checker) checkThreads() {
	var ready sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		ready.Add(1)
		go func() {
			runtime.LockOSThread()
			ready.Done()
			<-stop
		}()
	}
	ready.Wait()
	defer close(stop)

	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		c.check("threads", false, "%v", err)
		return
	}
	capsOK, nnpOK, scOK := true, true, true
	var bad, filters []string
	for _, e := range ents {
		f := chkStatus("/proc/self/task/" + e.Name() + "/status")
		for _, k := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
			if f[k] != "0000000000000000" {
				capsOK = false
				bad = append(bad, fmt.Sprintf("tid %s %s=%s", e.Name(), k, f[k]))
			}
		}
		if f["NoNewPrivs"] != "1" {
			nnpOK = false
			bad = append(bad, fmt.Sprintf("tid %s NoNewPrivs=%s", e.Name(), f["NoNewPrivs"]))
		}
		n, err := strconv.Atoi(f["Seccomp_filters"])
		if f["Seccomp"] != "2" || err != nil || n < c.cfg.Baseline+1 {
			scOK = false
			bad = append(bad, fmt.Sprintf("tid %s Seccomp=%s filters=%s", e.Name(), f["Seccomp"], f["Seccomp_filters"]))
		}
		filters = append(filters, f["Seccomp_filters"])
	}
	c.check("threads_caps_all_zero", capsOK && len(ents) >= 5, "threads=%d bad=%q", len(ents), bad)
	c.check("threads_no_new_privs", nnpOK, "threads=%d", len(ents))
	c.check("threads_seccomp_filter", scOK, "threads=%d Seccomp=2 filters=%v（宿主基线 %d，要求 ≥ %d）",
		len(ents), filters, c.cfg.Baseline, c.cfg.Baseline+1)
}

// checkSecurebits：PR_GET_SECUREBITS 为 0x0f（NOROOT、NO_SETUID_FIXUP 及其锁定位），且清除被拒绝。
func (c *checker) checkSecurebits() {
	const prGetSecurebits = 27
	r, _, e := syscall.RawSyscall(syscall.SYS_PRCTL, prGetSecurebits, 0, 0)
	c.check("securebits_value", e == 0 && r == workloadSecurebits, "got=%#x want=%#x errno=%v", r, workloadSecurebits, e)
	const lockBits = 1<<1 | 1<<3 // SECBIT_NOROOT_LOCKED | SECBIT_NO_SETUID_FIXUP_LOCKED
	_, _, e2 := syscall.RawSyscall(syscall.SYS_PRCTL, prSetSecurebits, 0, 0)
	c.check("securebits_locked", r&lockBits == lockBits && e2 != 0, "锁定位=%v；清除尝试 -> %v", r&lockBits == lockBits, e2)
}

// checkIDs：resuid/resgid 均为 1000，附加组为空。
func (c *checker) checkIDs() {
	var ru, eu, su, rg, eg, sg uint32
	_, _, eu0 := syscall.RawSyscall(syscall.SYS_GETRESUID, uintptr(unsafe.Pointer(&ru)), uintptr(unsafe.Pointer(&eu)), uintptr(unsafe.Pointer(&su)))
	_, _, eg0 := syscall.RawSyscall(syscall.SYS_GETRESGID, uintptr(unsafe.Pointer(&rg)), uintptr(unsafe.Pointer(&eg)), uintptr(unsafe.Pointer(&sg)))
	groups, gerr := syscall.Getgroups()
	c.check("uid", eu0 == 0 && ru == workloadID && eu == workloadID && su == workloadID, "resuid=%d,%d,%d errno=%v", ru, eu, su, eu0)
	c.check("gid", eg0 == 0 && rg == workloadID && eg == workloadID && sg == workloadID, "resgid=%d,%d,%d errno=%v", rg, eg, sg, eg0)
	st := chkStatus("/proc/self/status")
	c.check("groups_empty", gerr == nil && len(groups) == 0 && st["Groups"] == "", "getgroups=%v err=%v status.Groups=%q", groups, gerr, st["Groups"])
}

// checkSyscalls：clone3 → ENOSYS；拒绝列表（seccompDenyProbes，与 seccomp 子进程用例同一组无害无效调用）全部 EPERM；
// socket(AF_UNIX) 允许。
func (c *checker) checkSyscalls() {
	const sysClone3 = 435
	_, _, e := syscall.RawSyscall(sysClone3, 0, 0, 0)
	c.check("clone3_enosys", e == syscall.ENOSYS, "clone3(NULL,0) -> %v（无过滤器时内核返回 EINVAL）", errnoName(e))
	arch, err := NativeArch()
	if err != nil {
		c.check("seccomp_denylist_eperm", false, "%v", err)
		return
	}
	runtime.LockOSThread()
	allEPERM := true
	var res []string
	for _, p := range seccompDenyProbes(arch) {
		got := p.call()
		if got != syscall.EPERM {
			allEPERM = false
			res = append(res, p.name+"="+errnoName(got))
		}
	}
	runtime.UnlockOSThread()
	c.check("seccomp_denylist_eperm", allEPERM, "probes=%d 非 EPERM=%q", len(seccompDenyProbes(arch)), res)
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err == nil {
		syscall.Close(fd)
	}
	c.check("seccomp_allows_af_unix", err == nil, "socket(AF_UNIX) -> %s", chkErr(err))
}

type chkMount struct {
	point, fstype string
	opts          []string
	optional      []string
}

func chkMountinfo() []chkMount {
	b, _ := os.ReadFile("/proc/self/mountinfo")
	var out []chkMount
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		pre, post, _ := strings.Cut(l, " - ")
		f, g := strings.Fields(pre), strings.Fields(post)
		if len(f) < 6 || len(g) < 1 {
			out = append(out, chkMount{point: "<无法解析> " + l})
			continue
		}
		out = append(out, chkMount{point: unescapeMountinfo(f[4]), opts: strings.Split(f[5], ","), optional: f[6:], fstype: g[0]})
	}
	return out
}

// checkMounts：私有传播；旧根已脱离（只有一个 / 且为只读 tmpfs，挂载点全部属于预期集合）；模板与 /in 为
// ro,nosuid,nodev；无 /sys 与 cgroupfs；/proc 掩蔽与只读路径。
func (c *checker) checkMounts() {
	ms := chkMountinfo()
	allowed := []string{"/tmp", "/dev", "/proc"}
	if c.cfg.Kind == KindExec {
		allowed = append(allowed, "/out", "/in")
	} else {
		allowed = append(allowed, "/run", "/workspace")
	}
	allowed = append(allowed, c.cfg.Template...)
	private := true
	var roots, foreign, notPrivate []string
	byPoint := map[string]chkMount{}
	for _, m := range ms {
		if len(m.optional) != 0 {
			private = false
			notPrivate = append(notPrivate, m.point+":"+strings.Join(m.optional, " "))
		}
		byPoint[m.point] = m
		if m.point == "/" {
			roots = append(roots, m.fstype+":"+strings.Join(m.opts, ","))
			if m.fstype != "tmpfs" || !hasAll(m.opts, "ro", "nosuid", "nodev") {
				foreign = append(foreign, "/("+m.fstype+")")
			}
			continue
		}
		ok := false
		for _, a := range allowed {
			ok = ok || under(m.point, a)
		}
		if !ok {
			foreign = append(foreign, m.point)
		}
	}
	c.check("mounts_private_propagation", private, "entries=%d 非私有=%q", len(ms), notPrivate)
	c.check("old_root_detached", len(roots) == 1 && len(foreign) == 0, "挂载点 / = %q（期望 1 个只读 tmpfs）；意外挂载点=%q", roots, foreign)

	roOK := true
	var roBad []string
	n := 0
	for _, m := range ms {
		ro := c.cfg.Kind == KindExec && under(m.point, "/in")
		for _, p := range c.cfg.Template {
			ro = ro || under(m.point, p)
		}
		if ro {
			n++
			if !hasAll(m.opts, "ro", "nosuid", "nodev") {
				roOK = false
				roBad = append(roBad, m.point+"="+strings.Join(m.opts, ","))
			}
		}
	}
	c.check("rootfs_ro_nosuid_nodev", roOK && n > 0, "模板/只读挂载 %d 个，不合格=%q", n, roBad)

	sysEnts, _ := os.ReadDir("/sys")
	noSys := len(sysEnts) == 0
	for _, m := range ms {
		noSys = noSys && m.fstype != "sysfs" && m.fstype != "cgroup2" && m.fstype != "cgroup"
	}
	c.check("no_sys_no_cgroupfs", noSys, "/sys entries=%d", len(sysEnts))

	maskOK := true
	var maskRes []string
	for _, p := range procMaskPaths {
		st, err := os.Stat(p)
		if err != nil {
			maskRes = append(maskRes, p+"=absent")
			continue
		}
		if st.IsDir() {
			ents, _ := os.ReadDir(p)
			m := byPoint[p]
			ok := len(ents) == 0 && m.fstype == "tmpfs" && hasAll(m.opts, "ro")
			maskOK = maskOK && ok
			maskRes = append(maskRes, fmt.Sprintf("%s=空只读tmpfs:%v", p, ok))
			continue
		}
		sys := st.Sys().(*syscall.Stat_t)
		ok := st.Mode()&os.ModeCharDevice != 0 && sys.Rdev == 1<<8|3
		if ok { // 只读一个字节：未掩蔽的 /proc/kcore 等可能极大
			f, err := os.Open(p)
			n := 0
			if err == nil {
				n, _ = f.Read(make([]byte, 1))
				f.Close()
			}
			ok = err == nil && n == 0
		}
		maskOK = maskOK && ok
		maskRes = append(maskRes, fmt.Sprintf("%s=devnull:%v", p, ok))
	}
	c.check("proc_masked", maskOK, "%s", strings.Join(maskRes, " "))
	roOK = true
	var roRes []string
	for _, p := range procReadonlyPaths {
		if _, err := os.Stat(p); err != nil {
			roRes = append(roRes, p+"=absent")
			continue
		}
		m, ok := byPoint[p]
		good := ok && hasAll(m.opts, "ro")
		roOK = roOK && good
		roRes = append(roRes, fmt.Sprintf("%s=ro:%v", p, good))
	}
	c.check("proc_readonly", roOK, "%s", strings.Join(roRes, " "))
}

// checkMisc：可写区域、netns 只有 lo、信号掩码与忽略集为空。
func (c *checker) checkMisc() {
	var res []string
	ok := true
	try := func(path string, wantOK bool) {
		err := os.WriteFile(path, []byte("x"), 0o644)
		res = append(res, fmt.Sprintf("%s=%s", path, chkErr(err)))
		if (err == nil) != wantOK {
			ok = false
		}
		if err == nil {
			os.Remove(path)
		}
	}
	try("/tmp/w", true)
	try("/w", false)
	try("/usr/w", false)
	if c.cfg.Kind == KindExec {
		try("/out/w", true)
		try("/in/w", false)
	} else {
		try("/run/w", true)
		try("/workspace/w", c.cfg.WSWritable)
	}
	c.check("writable_areas", ok, "%s", strings.Join(res, " "))

	b, _ := os.ReadFile("/proc/net/dev")
	var ifs []string
	lines := strings.Split(string(b), "\n")
	for i, l := range lines {
		if i < 2 {
			continue
		}
		if n, _, found := strings.Cut(strings.TrimSpace(l), ":"); found {
			ifs = append(ifs, n)
		}
	}
	c.check("netns_only_lo", len(ifs) == 1 && ifs[0] == "lo", "interfaces=%v", ifs)

	st := chkStatus("/proc/self/status")
	c.check("signal_mask_clean", st["SigBlk"] == "0000000000000000" && st["SigIgn"] == "0000000000000000", "SigBlk=%s SigIgn=%s", st["SigBlk"], st["SigIgn"])
}

// checkEnvSpecific：Gateway socket（编排环境对 uid 1000 可访问、exec 环境不可见）与 session workspace 的三个阶段。
func (c *checker) checkEnvSpecific() {
	if c.cfg.Kind == KindExec {
		_, err := os.Stat(gatewaySocket)
		_, err2 := os.Stat(gatewayDir)
		c.check("gateway_socket_absent_in_exec", errors.Is(err, os.ErrNotExist) && errors.Is(err2, os.ErrNotExist),
			"stat %s -> %v；%s -> %v", gatewaySocket, err, gatewayDir, err2)
	} else if c.cfg.Gateway {
		owner := ""
		if st, err := os.Stat(gatewaySocket); err == nil {
			s := st.Sys().(*syscall.Stat_t)
			owner = fmt.Sprintf("uid=%d gid=%d mode=%v", s.Uid, s.Gid, st.Mode())
		}
		reply := ""
		fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
		if err == nil {
			err = syscall.Connect(fd, &syscall.SockaddrUnix{Name: gatewaySocket})
			if err == nil {
				_, err = syscall.Write(fd, []byte("ping\n"))
			}
			if err == nil {
				b := make([]byte, 64)
				n, rerr := syscall.Read(fd, b)
				if n > 0 {
					reply = strings.TrimSpace(string(b[:n]))
				}
				err = rerr
			}
			syscall.Close(fd)
		}
		c.check("gateway_socket_uid1000", err == nil && reply == "pong", "%s；connect/ping -> %s reply=%q", owner, chkErr(err), reply)
	}
	const state = "/workspace/state.txt"
	switch c.cfg.WSPhase {
	case "1":
		err := os.WriteFile(state, []byte("written in phase 1\n"), 0o600)
		c.check("session_workspace_write", err == nil, "write %s -> %s", state, chkErr(err))
	case "2":
		b, err := os.ReadFile(state)
		var err2 error
		if err == nil {
			err2 = os.WriteFile(state, append(b, []byte("appended in phase 2\n")...), 0o600)
		}
		c.check("session_workspace_resume", err == nil && err2 == nil && strings.HasPrefix(string(b), "written in phase 1"),
			"read -> %s %q；rewrite -> %s", chkErr(err), b, chkErr(err2))
	case "2-foreign":
		_, err := os.ReadFile(state)
		c.check("session_workspace_foreign_range_denied", errors.Is(err, os.ErrPermission), "以不同的 UID 范围读取 -> %v", err)
	}
}

// chkPython 在 workload 内（同一 seccomp 过滤器下）运行：clone3 → ENOSYS 时线程、subprocess、posix_spawn、fork 正常，
// 且 Python 进程的每个线程同样满足能力、NoNewPrivs 与 seccomp 条件。
const chkPython = `
import os, sys, threading, subprocess, ctypes, errno
def check(name, ok, detail):
    print("CHECK %s %s %s" % (name, "PASS" if ok else "FAIL", detail), flush=True)
base = int(os.environ["AGENTBOX_BASELINE_FILTERS"])
libc = ctypes.CDLL(None, use_errno=True)
r = libc.syscall(435, None, 0); e = ctypes.get_errno()
check("py_clone3_enosys", r == -1 and e == errno.ENOSYS, "r=%d errno=%s" % (r, errno.errorcode.get(e, e)))
out = []
ts = [threading.Thread(target=out.append, args=(i,)) for i in range(16)]
[t.start() for t in ts]; [t.join() for t in ts]
check("py_threads", sorted(out) == list(range(16)), "16 threads ran")
ev = threading.Event()
hs = [threading.Thread(target=ev.wait) for _ in range(4)]
[h.start() for h in hs]
bad = []
tids = os.listdir("/proc/self/task")
for tid in tids:
    d = {}
    for l in open("/proc/self/task/%s/status" % tid):
        k, _, v = l.partition(":")
        d[k] = v.strip()
    for k in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"):
        if d[k] != "0000000000000000": bad.append((tid, k, d[k]))
    if d["NoNewPrivs"] != "1" or d["Seccomp"] != "2" or int(d["Seccomp_filters"]) < base + 1: bad.append((tid, "nnp/seccomp", d["Seccomp_filters"]))
ev.set(); [h.join() for h in hs]
check("py_thread_status", not bad and len(tids) >= 5, "threads=%d bad=%r" % (len(tids), bad))
p = subprocess.run(["/usr/bin/echo", "sub-ok"], capture_output=True, text=True)
check("py_subprocess_run", p.returncode == 0 and p.stdout.strip() == "sub-ok", "rc=%d out=%r" % (p.returncode, p.stdout.strip()))
p = subprocess.run("echo shell-ok | tr a-z A-Z", shell=True, capture_output=True, text=True)
check("py_subprocess_shell", p.returncode == 0 and p.stdout.strip() == "SHELL-OK", "rc=%d out=%r" % (p.returncode, p.stdout.strip()))
pid = os.posix_spawn("/usr/bin/true", ["true"], {})
_, st = os.waitpid(pid, 0)
check("py_posix_spawn", st == 0, "status=%d" % st)
pid = os.fork()
if pid == 0:
    os._exit(7)
_, st = os.waitpid(pid, 0)
check("py_fork", os.waitstatus_to_exitcode(st) == 7, "exit=%d" % os.waitstatus_to_exitcode(st))
try:
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(8) as ex:
        res = list(ex.map(lambda x: x * x, range(32)))
    check("py_threadpool", res == [x * x for x in range(32)], "ok")
except Exception as ex:
    check("py_threadpool", False, repr(ex))
`

func (c *checker) checkPython() {
	cmd := exec.Command("/usr/bin/python3", "-c", chkPython)
	cmd.Env = []string{"PATH=/usr/bin:/bin", fmt.Sprintf("AGENTBOX_BASELINE_FILTERS=%d", c.cfg.Baseline)}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	err := cmd.Run()
	c.check("py_ran", err == nil, "python3 -> %s（由 Go os/exec 在同一过滤器下启动）", chkErr(err))
}

// ---- 宿主侧 ----

// checkResult 是检查器输出的一行 CHECK。
type checkResult struct {
	pass   bool
	detail string
}

// hostSeccompBaseline 以与检查器相同的方法（/proc/<self>/task/*/status 的 Seccomp_filters）读取宿主进程（本测试
// 进程）的过滤器数，取各线程的最大值：WSL2 的 init 给每个进程预装 1 个，GitHub runner 上为 0。
func hostSeccompBaseline(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	base := -1
	for _, e := range ents {
		f := chkStatus("/proc/self/task/" + e.Name() + "/status")
		v, ok := f["Seccomp_filters"]
		if !ok {
			continue // 线程已退出
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("线程 %s 的 Seccomp_filters = %q", e.Name(), v)
		}
		base = max(base, n)
	}
	if base < 0 {
		t.Fatal("宿主 /proc/self/task/*/status 没有 Seccomp_filters 字段（内核过旧），无法确定基线")
	}
	return base
}

// serverCreds 返回本进程（扮演 server）各线程凭据的去重集合：Uid、Gid、Groups、CapEff。
func serverCreds(t *testing.T) []string {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	for _, e := range ents {
		f := chkStatus("/proc/self/task/" + e.Name() + "/status")
		if _, ok := f["Uid"]; !ok {
			continue // 线程已退出
		}
		set[fmt.Sprintf("Uid=%s Gid=%s Groups=%q CapEff=%s", f["Uid"], f["Gid"], f["Groups"], f["CapEff"])] = true
	}
	var out []string
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// installChecker 把本测试二进制复制为 dir/agentbox-check（0755）。
func installChecker(t *testing.T, dir string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, checkerName)
	if err := os.WriteFile(p, b, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o755); err != nil {
		t.Fatal(err)
	}
}

// pongSocket 在 dir 下建一个 unix socket（属主宿主 uid、0600），每个连接读到 "ping" 回复 "pong"。
func pongSocket(t *testing.T, dir string, uid int) string {
	t.Helper()
	path := filepath.Join(dir, "gw-pong.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := os.Chown(path, uid, uid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				b := make([]byte, 64)
				n, _ := conn.Read(b)
				if strings.TrimSpace(string(b[:n])) == "ping" {
					_, _ = conn.Write([]byte("pong\n"))
				}
			}()
		}
	}()
	return path
}

// runChecker 经生产启动路径启动 spec 的环境，在其中以 workload 运行检查器（stdin 为管道，stdout 与 stderr 为同一
// 管道），断言 start_ack 后 exit，返回每个 CHECK 的结果。环境随后关闭（init 退出）。
func runChecker(t *testing.T, spec LaunchSpec, cfg checkConfig, path string) map[string]checkResult {
	t.Helper()
	s := startSandboxInit(t, spec)
	if s.ready != nil {
		t.Fatalf("init 未就绪: %v\n%s", s.ready, s.output())
	}
	ch := pumpMessages(s.conn)
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = s.conn.Send(Message{Type: MsgStart, ExecID: "check", Spec: argvSpec(path, isolationCheckArg, string(cfgJSON))},
		[]*os.File{inR, outW, outW})
	inR.Close()
	outW.Close()
	inW.Close() // workload 的 stdin 读到 EOF
	if err != nil {
		t.Fatal(err)
	}
	outc := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(outR)
		outc <- b
	}()
	_, ex := expectAckExit(t, s, ch, "check")
	var out []byte
	select {
	case out = <-outc:
	case <-time.After(20 * time.Second):
		t.Fatal("检查器输出未结束")
	}
	closeAndDrain(t, s, ch)

	res := map[string]checkResult{}
	for _, line := range strings.Split(string(out), "\n") {
		rest, ok := strings.CutPrefix(line, "CHECK ")
		if !ok {
			continue
		}
		f := strings.SplitN(rest, " ", 3)
		if len(f) < 2 {
			t.Errorf("无法解析检查器输出 %q", line)
			continue
		}
		if len(f) == 2 {
			f = append(f, "")
		}
		if _, dup := res[f[0]]; dup {
			t.Errorf("检查 %s 重复输出", f[0])
		}
		res[f[0]] = checkResult{pass: f[1] == "PASS", detail: f[2]}
	}
	if ex.Code != 0 || ex.Signal != 0 || !strings.Contains(string(out), "CHECK_SUMMARY fail=0") {
		t.Errorf("检查器退出状态 %+v（期望 0 且全部通过）；输出:\n%s", ex, out)
	}
	return res
}

// acceptItem 是 §16.2 的一个条目：要求的 (场景, 检查) 全部 PASS。
type acceptItem struct {
	title string
	need  [][2]string
}

// TestIsolationAcceptance16_2：§16.2 生产验收（每个条目一个子测试，-v 逐项输出）。检查器在生产启动路径启动的
// workload 内运行，五个场景：exec 环境（/in、/out）；task 编排环境（workspace、Gateway socket）；session 的三个
// 阶段（同一 workspace：阶段 1 写入；关闭后以同一 UID 范围重新启动，阶段 2 读回并追加；以另一 UID 范围启动，读取
// 被拒绝）。另外两个条目在宿主侧判定：启动成功判据（提交点语义，经生产 Launcher 与 helper 的测试钩子）；
// server（本测试进程）各线程的凭据在全部启动前后不变。
func TestIsolationAcceptance16_2(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	if cgoBuild {
		t.Fatal("§16.2 验收须以 CGO_ENABLED=0 构建运行（cgo 构建中 helper 拒绝运行）")
	}
	baseline := hostSeccompBaseline(t)
	credsBefore := serverCreds(t)
	t.Logf("宿主 Seccomp_filters 基线 = %d（workload 要求 ≥ %d）；server 凭据 %q", baseline, baseline+1, credsBefore)
	hostWorkload := initTestIDBase + workloadID
	results := map[string]map[string]checkResult{}

	// exec 环境。
	{
		spec := initTestSpec(t, KindExec, false)
		installChecker(t, spec.Init.In)
		results["exec"] = runChecker(t, spec, checkConfig{Kind: KindExec, Baseline: baseline, Template: spec.Init.Template.Paths},
			"/in/"+checkerName)
	}
	// task 编排环境：Gateway socket 换成回复 pong 的 socket。
	{
		spec := initTestSpec(t, KindTask, false)
		spec.Init.GatewaySocket = pongSocket(t, filepath.Dir(spec.Init.Workspace), hostWorkload)
		installChecker(t, spec.Init.Workspace)
		results["task"] = runChecker(t, spec, checkConfig{Kind: KindTask, Baseline: baseline, Template: spec.Init.Template.Paths,
			Gateway: true, WSWritable: true}, "/workspace/"+checkerName)
	}
	// session：同一 workspace 的三个阶段。
	{
		spec := initTestSpec(t, KindSession, false)
		spec.Init.GatewaySocket = pongSocket(t, filepath.Dir(spec.Init.Workspace), hostWorkload)
		installChecker(t, spec.Init.Workspace)
		cfg := checkConfig{Kind: KindSession, Baseline: baseline, Template: spec.Init.Template.Paths, Gateway: true, WSWritable: true}
		cfg.WSPhase = "1"
		results["session-1"] = runChecker(t, spec, cfg, "/workspace/"+checkerName)
		cfg.WSPhase = "2"
		results["session-2"] = runChecker(t, spec, cfg, "/workspace/"+checkerName)
		// 另一 UID 范围：Gateway socket 的属主在该范围中未映射（init 会拒绝），不挂载；workspace 不可写。
		foreign := spec
		foreign.UIDBase, foreign.GIDBase = initTestIDBase+10000, initTestIDBase+10000
		foreign.Init.GatewaySocket = ""
		cfg.WSPhase, cfg.Gateway, cfg.WSWritable = "2-foreign", false, false
		results["session-foreign"] = runChecker(t, foreign, cfg, "/workspace/"+checkerName)
	}

	all := []string{"exec", "task", "session-1", "session-2", "session-foreign"}
	every := func(checks ...string) [][2]string {
		var out [][2]string
		for _, sc := range all {
			for _, c := range checks {
				out = append(out, [2]string{sc, c})
			}
		}
		return out
	}
	items := []acceptItem{
		{"01 逐线程 Cap*=0、NoNewPrivs=1、Seccomp=2 且 filters≥基线+1",
			every("threads_caps_all_zero", "threads_no_new_privs", "threads_seccomp_filter", "py_thread_status")},
		{"02 securebits 预期位且已锁定", every("securebits_value", "securebits_locked")},
		{"03 uid、gid、附加组", every("uid", "gid", "groups_empty")},
		{"04 FD 无继承且目标逐个解析", every("fds_no_extra", "fds_012_pipes")},
		{"05 clone3=ENOSYS 时 Python 线程与 subprocess 正常", every("clone3_enosys", "py_clone3_enosys", "py_threads",
			"py_subprocess_run", "py_subprocess_shell", "py_posix_spawn", "py_fork", "py_threadpool", "py_ran")},
		{"06 私有传播、旧根已脱离", every("mounts_private_propagation", "old_root_detached")},
		{"07 Gateway socket：编排环境 uid 1000 可访问、exec 环境不可见", [][2]string{
			{"task", "gateway_socket_uid1000"}, {"session-1", "gateway_socket_uid1000"}, {"session-2", "gateway_socket_uid1000"},
			{"exec", "gateway_socket_absent_in_exec"}}},
		{"08 session 恢复后以保留的 UID 范围访问 workspace", [][2]string{
			{"session-1", "session_workspace_write"}, {"session-2", "session_workspace_resume"},
			{"session-foreign", "session_workspace_foreign_range_denied"}}},
		{"补充 §4.5 隔离配置（移植自 spike）", every("seccomp_denylist_eperm", "seccomp_allows_af_unix", "rootfs_ro_nosuid_nodev",
			"no_sys_no_cgroupfs", "proc_masked", "proc_readonly", "writable_areas", "netns_only_lo", "signal_mask_clean")},
	}
	used := map[[2]string]bool{}
	for _, it := range items {
		t.Run(it.title, func(t *testing.T) {
			for _, k := range it.need {
				used[k] = true
				r, ok := results[k[0]][k[1]]
				switch {
				case !ok:
					t.Errorf("%s/%s：检查器没有输出该项", k[0], k[1])
				case !r.pass:
					t.Errorf("%s/%s FAIL：%s", k[0], k[1], r.detail)
				default:
					t.Logf("%s/%s PASS：%s", k[0], k[1], r.detail)
				}
			}
		})
	}
	for sc, rs := range results {
		for name, r := range rs {
			if !used[[2]string{sc, name}] {
				t.Errorf("检查器输出了未归入任何条目的检查 %s/%s（pass=%v）", sc, name, r.pass)
			}
		}
	}

	t.Run("09 启动成功判据（提交点语义）", func(t *testing.T) {
		t.Run("快速退出报告为 start_ack 与退出状态", func(t *testing.T) {
			s, ch := helperTestInit(t, nil)
			for i, c := range []int{0, 0, 0, 7, 0} {
				id := fmt.Sprintf("e%d", i)
				sendStart(t, s, id, nil, "/bin/sh", "-c", fmt.Sprintf("exit %d", c))
				if _, ex := expectAckExit(t, s, ch, id); ex.Code != c || ex.Signal != 0 {
					t.Fatalf("%s 的退出状态 = %+v，期望 %d", id, ex, c)
				}
			}
			closeAndDrain(t, s, ch)
			t.Logf("5 次快速退出：均为 start_ack 然后 exit（含退出码 7）")
		})
		t.Run("helper 在 execve 前失败报告为 start_err，workload 未运行", func(t *testing.T) {
			for _, step := range []string{hStepSetName, hStepSetresuid, hStepSeccomp, hStepExecve} {
				s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookFailPrefix+step)
				sendStart(t, s, "e1", nil, "/bin/sh", "-c", "touch /tmp/started")
				m := nextMsg(t, s, ch)
				if want := "helper/" + step + ": " + errInjected.Error(); m.Type != MsgStartErr || m.Reason != want {
					t.Fatalf("回复 = %+v，期望 start_err{%s}", m, want)
				}
				if fileExists(inSandbox(s, "/tmp/started")) {
					t.Fatalf("%s：workload 运行了", step)
				}
				closeAndDrain(t, s, ch)
				t.Logf("注入 %s 失败：start_err{%s}，workload 未运行，此后无消息", step, m.Reason)
			}
			s, ch := helperTestInit(t, nil)
			sendStart(t, s, "e1", nil, "/nonexistent/workload")
			m := nextMsg(t, s, ch)
			if m.Type != MsgStartErr || !strings.HasPrefix(m.Reason, "helper/execve: ") {
				t.Fatalf("回复 = %+v，期望 start_err{helper/execve: …}", m)
			}
			closeAndDrain(t, s, ch)
			t.Logf("真实 execve 失败：start_err{%s}", m.Reason)
		})
		t.Run("helper 在 execve 前被 SIGKILL 报告为 start_err，workload 未运行", func(t *testing.T) {
			s, ch := helperTestInit(t, nil, envTestHelperHook+"="+hookSigkill)
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("e%d", i)
				sendStart(t, s, id, nil, "/bin/sh", "-c", "touch /tmp/started")
				if m := nextMsg(t, s, ch); m.Type != MsgStartErr || m.Reason != "helper/died_before_exec: signal killed" {
					t.Fatalf("回复 = %+v，期望 start_err{helper/died_before_exec: signal killed}", m)
				}
			}
			if fileExists(inSandbox(s, "/tmp/started")) {
				t.Fatal("workload 运行了")
			}
			closeAndDrain(t, s, ch)
			t.Logf("3 次 execve 前 SIGKILL：均为 start_err{helper/died_before_exec: signal killed}，workload 未运行")
		})
		t.Run("workload 与 helper 同名不影响判定", func(t *testing.T) {
			s, ch := helperTestInit(t, func(spec *LaunchSpec) {
				src, err := filepath.EvalSymlinks("/bin/sh")
				if err != nil {
					t.Fatal(err)
				}
				b, err := os.ReadFile(src)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(spec.Init.In, helperComm), b, 0o755); err != nil {
					t.Fatal(err)
				}
			})
			sendStart(t, s, "e1", nil, "/in/"+helperComm, "-c", "read c < /proc/$$/comm; echo \"$c\" > /tmp/same; exit 3")
			if _, ex := expectAckExit(t, s, ch, "e1"); ex.Code != 3 || ex.Signal != 0 {
				t.Fatalf("退出状态 = %+v，期望 3", ex)
			}
			b, err := os.ReadFile(inSandbox(s, "/tmp/same"))
			if err != nil || strings.TrimSpace(string(b)) != helperComm {
				t.Fatalf("workload 的进程名 = %q（%v），期望 %q", b, err, helperComm)
			}
			closeAndDrain(t, s, ch)
			t.Logf("workload 进程名 %q（与 helper 相同）：start_ack 然后 exit 3", helperComm)
		})
	})

	t.Run("10 专用启动进程之外的 server 凭据不变", func(t *testing.T) {
		after := serverCreds(t)
		if !reflect.DeepEqual(after, credsBefore) {
			t.Fatalf("server 各线程凭据由 %q 变为 %q", credsBefore, after)
		}
		t.Logf("全部启动（5 个检查器环境与条目 09 的环境）前后 server 各线程凭据相同：%q", after)
	})
}
