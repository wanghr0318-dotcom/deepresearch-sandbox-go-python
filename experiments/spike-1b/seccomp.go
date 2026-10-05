//go:build linux

package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Minimal seccomp deny list for the spike (spec §4.5). x86_64 only; the
// production builder (Plan 2 Task 3) also handles aarch64. Default action:
// allow; listed syscalls -> EPERM; clone3 -> ENOSYS; foreign ABI -> kill.

const (
	auditArchX86_64 = 0xc000003e
	x32SyscallBit   = 0x40000000

	seccompRetKillProcess = 0x80000000
	seccompRetErrno       = 0x00050000
	seccompRetAllow       = 0x7fff0000

	bpfLdWAbs = 0x20 // BPF_LD|BPF_W|BPF_ABS
	bpfJeqK   = 0x15 // BPF_JMP|BPF_JEQ|BPF_K
	bpfJgeK   = 0x35 // BPF_JMP|BPF_JGE|BPF_K
	bpfJsetK  = 0x45 // BPF_JMP|BPF_JSET|BPF_K
	bpfRetK   = 0x06 // BPF_RET|BPF_K

	offNr   = 0
	offArch = 4
	offArg0 = 16 // low 32 bits of args[0] (little endian)
	afUnix  = 1
	enosys  = 38
	eperm   = 1
	// CLONE_NEWNS|NEWCGROUP|NEWUTS|NEWIPC|NEWUSER|NEWPID|NEWNET. CLONE_NEWTIME
	// (0x80) overlaps clone()'s exit-signal byte and only works via clone3/unshare,
	// both of which are already refused.
	cloneNewMask = 0x7e020000
)

// x86_64 numbers of the syscalls denied with EPERM.
var seccompDenyEPERM = map[string]uint32{
	"mount": 165, "umount2": 166, "pivot_root": 155, "unshare": 272, "setns": 308,
	"ptrace": 101, "process_vm_readv": 310, "process_vm_writev": 311,
	"bpf": 321, "perf_event_open": 298, "userfaultfd": 323,
	"io_uring_setup": 425, "io_uring_enter": 426, "io_uring_register": 427,
	"keyctl": 250, "add_key": 248, "request_key": 249, "open_by_handle_at": 304,
	"init_module": 175, "finit_module": 313, "delete_module": 176,
	"kexec_load": 246, "kexec_file_load": 320,
}

const (
	nrClone  = 56
	nrSocket = 41
)

type sockFilter struct {
	Code uint16
	Jt   uint8
	Jf   uint8
	K    uint32
}

type sockFprog struct {
	Len    uint16
	_      [6]byte
	Filter *sockFilter
}

type bpfInsn struct {
	code   uint16
	k      uint32
	jt, jf string // label names; "" means the next instruction
	label  string // non-empty: this entry is a label, not an instruction
}

func buildSeccompFilter(clone3Errno uint32) ([]sockFilter, error) {
	var p []bpfInsn
	ins := func(code uint16, k uint32, jt, jf string) { p = append(p, bpfInsn{code: code, k: k, jt: jt, jf: jf}) }
	lbl := func(name string) { p = append(p, bpfInsn{label: name}) }

	ins(bpfLdWAbs, offArch, "", "")
	ins(bpfJeqK, auditArchX86_64, "", "kill")
	ins(bpfLdWAbs, offNr, "", "")
	ins(bpfJgeK, x32SyscallBit, "kill", "")
	ins(bpfJeqK, sysClone3, "enosys", "")
	for _, nr := range seccompDenyEPERM {
		ins(bpfJeqK, nr, "eperm", "")
	}
	ins(bpfJeqK, nrClone, "clone", "")
	ins(bpfJeqK, nrSocket, "socket", "")
	ins(bpfRetK, seccompRetAllow, "", "")
	lbl("clone")
	ins(bpfLdWAbs, offArg0, "", "")
	ins(bpfJsetK, cloneNewMask, "eperm", "allow")
	lbl("socket")
	ins(bpfLdWAbs, offArg0, "", "")
	ins(bpfJeqK, afUnix, "allow", "eperm")
	lbl("kill")
	ins(bpfRetK, seccompRetKillProcess, "", "")
	lbl("enosys")
	ins(bpfRetK, seccompRetErrno|clone3Errno, "", "")
	lbl("eperm")
	ins(bpfRetK, seccompRetErrno|eperm, "", "")
	lbl("allow")
	ins(bpfRetK, seccompRetAllow, "", "")

	// Resolve labels to instruction indexes.
	pos := map[string]int{}
	n := 0
	for _, x := range p {
		if x.label != "" {
			pos[x.label] = n
		} else {
			n++
		}
	}
	var out []sockFilter
	i := 0
	for _, x := range p {
		if x.label != "" {
			continue
		}
		jump := func(l string) (uint8, error) {
			if l == "" {
				return 0, nil
			}
			t, ok := pos[l]
			if !ok {
				return 0, fmt.Errorf("unknown label %q", l)
			}
			off := t - (i + 1)
			if off < 0 || off > 255 {
				return 0, fmt.Errorf("jump to %q out of range: %d", l, off)
			}
			return uint8(off), nil
		}
		jt, err := jump(x.jt)
		if err != nil {
			return nil, err
		}
		jf, err := jump(x.jf)
		if err != nil {
			return nil, err
		}
		out = append(out, sockFilter{Code: x.code, Jt: jt, Jf: jf, K: x.k})
		i++
	}
	return out, nil
}

// installSeccomp loads the filter with SECCOMP_FILTER_FLAG_TSYNC. An empty
// program is passed through unchanged so a real kernel error can be injected.
func installSeccomp(prog []sockFilter) error {
	fprog := sockFprog{Len: uint16(len(prog))}
	if len(prog) > 0 {
		fprog.Filter = &prog[0]
	}
	const setModeFilter, flagTsync = 1, 1
	r, _, e := syscall.RawSyscall(sysSeccomp, setModeFilter, flagTsync, uintptr(unsafe.Pointer(&fprog)))
	if e != 0 {
		return e
	}
	if r != 0 {
		return fmt.Errorf("TSYNC failed on thread %d", r)
	}
	return nil
}
