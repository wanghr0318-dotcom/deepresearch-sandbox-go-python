//go:build linux

package sandbox

import (
	"fmt"
	"runtime"
)

// 本文件只生成 seccomp 过滤器的内容（规格 §4.5）。何时、由哪个进程、
// 是否带 SECCOMP_FILTER_FLAG_TSYNC 安装，由 Plan 1B 的 spike 决定。
//
// 模块代理里没有 golang.org/x/sys，因此下面的内核常量在本文件中自行定义，
// 每组注明来源头文件（路径相对于 Linux 源码树）。

// SeccompProfile 选择一套 seccomp 配置。
type SeccompProfile int

const (
	// ProfileOrchestrator 用于沙箱内的编排进程（Python Worker）。
	ProfileOrchestrator SeccompProfile = iota + 1
	// ProfileExec 用于 exec 启动的工作负载进程。规格要求它在 orchestrator
	// 的基础上“不额外放宽”；目前两者规则相同。
	ProfileExec
)

// Arch 是过滤器针对的原生架构。
type Arch int

const (
	// ArchAMD64 是 x86_64 的 64 位 ABI。
	ArchAMD64 Arch = iota + 1
	// ArchARM64 是 aarch64。
	ArchARM64
)

// NativeArch 按 runtime.GOARCH 返回本机架构；不支持的架构返回错误。
func NativeArch() (Arch, error) {
	switch runtime.GOARCH {
	case "amd64":
		return ArchAMD64, nil
	case "arm64":
		return ArchARM64, nil
	}
	return 0, fmt.Errorf("seccomp: 不支持的架构 %s", runtime.GOARCH)
}

// SockFilter 是一条 classic BPF 指令，内存布局与内核 struct sock_filter
// （include/uapi/linux/filter.h）一致：code u16、jt u8、jf u8、k u32。
type SockFilter struct {
	Code uint16
	Jt   uint8
	Jf   uint8
	K    uint32
}

// classic BPF 操作码（include/uapi/linux/bpf_common.h）。
const (
	bpfLD   = 0x00
	bpfJMP  = 0x05
	bpfRET  = 0x06
	bpfW    = 0x00
	bpfABS  = 0x20
	bpfJEQ  = 0x10
	bpfJGE  = 0x30
	bpfJSET = 0x40
	bpfK    = 0x00

	bpfMaxInsns = 4096 // BPF_MAXINSNS
)

// struct seccomp_data 的字段偏移（include/uapi/linux/seccomp.h）：
// int nr; __u32 arch; __u64 instruction_pointer; __u64 args[6]。
// 两种支持的架构都是小端，参数的低 32 位在 args[i] 偏移处。
const (
	seccompDataNr      = 0
	seccompDataArch    = 4
	seccompDataArgs    = 16
	seccompDataArgSize = 8
)

// seccomp 返回动作（include/uapi/linux/seccomp.h）。
const (
	seccompRetKillProcess = 0x80000000
	seccompRetErrno       = 0x00050000
	seccompRetAllow       = 0x7fff0000
)

// errno 值（include/uapi/asm-generic/errno-base.h、errno.h；x86_64 与 aarch64 相同）。
const (
	errnoEPERM  = 1
	errnoENOSYS = 38
)

// AUDIT_ARCH_* 值（include/uapi/linux/audit.h、elf-em.h）：
// EM_X86_64=62、EM_AARCH64=183，|__AUDIT_ARCH_64BIT(0x80000000)|__AUDIT_ARCH_LE(0x40000000)。
const (
	auditArchX86_64  = 0xC000003E
	auditArchAArch64 = 0xC00000B7
)

// x32SyscallBit 是 x32 ABI 调用号上的标记位
// （arch/x86/include/uapi/asm/unistd.h 的 __X32_SYSCALL_BIT）。x32 调用与
// 原生 x86_64 共用 AUDIT_ARCH_X86_64，只能靠这一位区分。
const x32SyscallBit = 0x40000000

// cloneNewMask 是 clone(2) 的 flags 中所有 CLONE_NEW* 位（include/uapi/linux/sched.h）：
// CLONE_NEWNS 0x00020000、CLONE_NEWCGROUP 0x02000000、CLONE_NEWUTS 0x04000000、
// CLONE_NEWIPC 0x08000000、CLONE_NEWUSER 0x10000000、CLONE_NEWPID 0x20000000、
// CLONE_NEWNET 0x40000000。
//
// 不含 CLONE_NEWTIME（0x80）：它落在 CSIGNAL（0xff，退出信号）范围内，旧 clone
// 无法表达它，只能通过 clone3 或 unshare 使用，而这两者已整体拒绝。
const cloneNewMask = 0x00020000 | 0x02000000 | 0x04000000 | 0x08000000 |
	0x10000000 | 0x20000000 | 0x40000000

// afUnix 是 AF_UNIX（include/linux/socket.h）。
const afUnix = 1

// seccompSyscalls 是过滤器用到的系统调用号。
type seccompSyscalls struct {
	auditArch uint32
	// deny 是无条件返回 EPERM 的调用号。
	deny   []uint32
	clone  uint32
	socket uint32
	clone3 uint32
}

// seccompDenyNames 是规格 §4.5 中无条件拒绝（EPERM）的系统调用，按此顺序生成。
var seccompDenyNames = []string{
	"mount", "umount2", "pivot_root", "unshare", "setns",
	"ptrace", "process_vm_readv", "process_vm_writev",
	"bpf", "perf_event_open", "userfaultfd",
	"io_uring_setup", "io_uring_enter", "io_uring_register",
	"keyctl", "add_key", "request_key", "open_by_handle_at",
	"init_module", "finit_module", "delete_module",
	"kexec_load", "kexec_file_load",
}

// syscallNrAMD64 来自 arch/x86/entry/syscalls/syscall_64.tbl（common/64 条目）。
var syscallNrAMD64 = map[string]uint32{
	"mount": 165, "umount2": 166, "pivot_root": 155, "unshare": 272, "setns": 308,
	"ptrace": 101, "process_vm_readv": 310, "process_vm_writev": 311,
	"bpf": 321, "perf_event_open": 298, "userfaultfd": 323,
	"io_uring_setup": 425, "io_uring_enter": 426, "io_uring_register": 427,
	"keyctl": 250, "add_key": 248, "request_key": 249, "open_by_handle_at": 304,
	"init_module": 175, "finit_module": 313, "delete_module": 176,
	"kexec_load": 246, "kexec_file_load": 320,
	"clone": 56, "socket": 41, "clone3": 435,
}

// syscallNrARM64 来自 include/uapi/asm-generic/unistd.h（aarch64 使用通用表）。
var syscallNrARM64 = map[string]uint32{
	"mount": 40, "umount2": 39, "pivot_root": 41, "unshare": 97, "setns": 268,
	"ptrace": 117, "process_vm_readv": 270, "process_vm_writev": 271,
	"bpf": 280, "perf_event_open": 241, "userfaultfd": 282,
	"io_uring_setup": 425, "io_uring_enter": 426, "io_uring_register": 427,
	"keyctl": 219, "add_key": 217, "request_key": 218, "open_by_handle_at": 265,
	"init_module": 105, "finit_module": 273, "delete_module": 106,
	"kexec_load": 104, "kexec_file_load": 294,
	"clone": 220, "socket": 198, "clone3": 435,
}

func syscallsFor(arch Arch) (seccompSyscalls, error) {
	var table map[string]uint32
	var s seccompSyscalls
	switch arch {
	case ArchAMD64:
		table, s.auditArch = syscallNrAMD64, auditArchX86_64
	case ArchARM64:
		table, s.auditArch = syscallNrARM64, auditArchAArch64
	default:
		return s, fmt.Errorf("seccomp: 未知架构 %d", arch)
	}
	lookup := func(name string) (uint32, error) {
		nr, ok := table[name]
		if !ok {
			return 0, fmt.Errorf("seccomp: 架构 %d 缺少 %s 的调用号", arch, name)
		}
		return nr, nil
	}
	for _, name := range seccompDenyNames {
		nr, err := lookup(name)
		if err != nil {
			return s, err
		}
		s.deny = append(s.deny, nr)
	}
	var err error
	if s.clone, err = lookup("clone"); err != nil {
		return s, err
	}
	if s.socket, err = lookup("socket"); err != nil {
		return s, err
	}
	if s.clone3, err = lookup("clone3"); err != nil {
		return s, err
	}
	return s, nil
}

// BuildFilter 生成 profile 在 arch 上的 classic BPF 程序（规格 §4.5）：
//
//  1. seccomp_data.arch 不是原生架构 → KILL_PROCESS；amd64 上调用号带
//     __X32_SYSCALL_BIT（x32 ABI）→ KILL_PROCESS；
//  2. 显式拒绝列表 → ERRNO(EPERM)；
//  3. clone 的 flags 带任一 CLONE_NEW* → ERRNO(EPERM)；
//  4. socket 的 family 不是 AF_UNIX → ERRNO(EPERM)；
//  5. clone3 → ERRNO(ENOSYS)（seccomp 无法解引用 clone_args，让 libc 回退到 clone）；
//  6. 其余 → ALLOW。
//
// 参数只比较低 32 位：clone 的 flags 在内核中按 lower_32_bits 截断，
// socket 的 family 是 int，高 32 位不影响内核行为，也就无法用来绕过。
func BuildFilter(p SeccompProfile, arch Arch) ([]SockFilter, error) {
	switch p {
	case ProfileOrchestrator, ProfileExec:
		// exec 在 orchestrator 基础上不额外放宽，目前规则相同。
	default:
		return nil, fmt.Errorf("seccomp: 未知配置 %d", p)
	}
	sc, err := syscallsFor(arch)
	if err != nil {
		return nil, err
	}

	var b bpfBuilder
	b.load(seccompDataArch)
	b.jumpIf(bpfJEQ, sc.auditArch, "", "kill")
	b.load(seccompDataNr)
	if arch == ArchAMD64 {
		b.jumpIf(bpfJGE, x32SyscallBit, "kill", "")
	}
	for _, nr := range sc.deny {
		b.jumpIf(bpfJEQ, nr, "eperm", "")
	}
	b.jumpIf(bpfJEQ, sc.clone, "clone", "")
	b.jumpIf(bpfJEQ, sc.socket, "socket", "")
	b.jumpIf(bpfJEQ, sc.clone3, "enosys", "")
	b.ret(seccompRetAllow)

	b.label("clone")
	b.load(seccompDataArgs) // args[0] 低 32 位：flags
	b.jumpIf(bpfJSET, cloneNewMask, "eperm", "allow")

	b.label("socket")
	b.load(seccompDataArgs) // args[0] 低 32 位：family
	b.jumpIf(bpfJEQ, afUnix, "allow", "eperm")

	b.label("allow")
	b.ret(seccompRetAllow)
	b.label("eperm")
	b.ret(seccompRetErrno | errnoEPERM)
	b.label("enosys")
	b.ret(seccompRetErrno | errnoENOSYS)
	b.label("kill")
	b.ret(seccompRetKillProcess)

	return b.finish()
}

// bpfBuilder 以符号标签组装 classic BPF，最后统一解析为相对跳转偏移。
// 标签 "" 表示落到下一条指令。
type bpfBuilder struct {
	insns  []SockFilter
	jumps  map[int][2]string // 指令下标 → {jt 标签, jf 标签}
	labels map[string]int
}

func (b *bpfBuilder) load(off uint32) {
	b.insns = append(b.insns, SockFilter{Code: bpfLD | bpfW | bpfABS, K: off})
}

func (b *bpfBuilder) jumpIf(op uint16, k uint32, jt, jf string) {
	if b.jumps == nil {
		b.jumps = map[int][2]string{}
	}
	b.jumps[len(b.insns)] = [2]string{jt, jf}
	b.insns = append(b.insns, SockFilter{Code: bpfJMP | op | bpfK, K: k})
}

func (b *bpfBuilder) ret(k uint32) {
	b.insns = append(b.insns, SockFilter{Code: bpfRET | bpfK, K: k})
}

func (b *bpfBuilder) label(name string) {
	if b.labels == nil {
		b.labels = map[string]int{}
	}
	b.labels[name] = len(b.insns)
}

func (b *bpfBuilder) finish() ([]SockFilter, error) {
	if len(b.insns) > bpfMaxInsns {
		return nil, fmt.Errorf("seccomp: 程序长度 %d 超过 BPF_MAXINSNS", len(b.insns))
	}
	resolve := func(at int, name string) (uint8, error) {
		if name == "" {
			return 0, nil
		}
		target, ok := b.labels[name]
		if !ok {
			return 0, fmt.Errorf("seccomp: 未定义的标签 %q", name)
		}
		off := target - (at + 1)
		if off < 0 || off > 255 {
			return 0, fmt.Errorf("seccomp: 标签 %q 的跳转偏移 %d 超出范围", name, off)
		}
		return uint8(off), nil
	}
	for at, t := range b.jumps {
		jt, err := resolve(at, t[0])
		if err != nil {
			return nil, err
		}
		jf, err := resolve(at, t[1])
		if err != nil {
			return nil, err
		}
		b.insns[at].Jt, b.insns[at].Jf = jt, jf
	}
	return b.insns, nil
}
