//go:build linux

// Command spike1b is the Plan 1B privilege-drop spike (experiment only, not
// production code). One binary plays four roles, selected by argv[1]:
//
//	launch  host side, run as real root: cgroup, clone into namespaces, control socket
//	init    PID 1 of the sandbox: mounts, pivot_root, capability reduction, reaping
//	helper  stage-2 helper started by init via execveat(fd, "", AT_EMPTY_PATH)
//	check   the workload: verifies spec §16.2 from inside the sandbox
package main

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// x86_64 syscall numbers that package syscall does not export.
const (
	sysSeccomp      = 317
	sysExecveat     = 322
	sysOpenTree     = 428
	sysMoveMount    = 429
	sysClone3       = 435
	sysCloseRange   = 436
	sysMountSetattr = 442
)

// prctl options and flags.
const (
	prCapbsetRead     = 23
	prCapbsetDrop     = 24
	prGetSecurebits   = 27
	prSetSecurebits   = 28
	prSetNoNewPrivs   = 38
	prGetNoNewPrivs   = 39
	prCapAmbient      = 47
	prCapAmbientClear = 4

	secbitNoroot             = 1 << 0
	secbitNorootLocked       = 1 << 1
	secbitNoSetuidFixup      = 1 << 2
	secbitNoSetuidFixupLockd = 1 << 3
	// The value the spec asks for: NOROOT | NO_SETUID_FIXUP, both locked.
	wantSecurebits = secbitNoroot | secbitNorootLocked | secbitNoSetuidFixup | secbitNoSetuidFixupLockd

	capKill    = 5
	capSetgid  = 6
	capSetuid  = 7
	capSetpcap = 8

	closeRangeCloexec = 1 << 2
	atEmptyPath       = 0x1000
	oPath             = 0x200000
	sigSetmask        = 2
)

const (
	workloadUID = 1000
	workloadGID = 1000
	// Host-side ID range for the spike: sandbox 0..4095 -> host 300000..304095.
	hostIDBase = 300000
	idRange    = 4096
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: spike1b launch|init|helper|check ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "launch":
		os.Exit(launchMain(os.Args[2:]))
	case "init":
		os.Exit(initMain())
	case "helper":
		helperMain(os.Args[2:]) // only returns on failure
		os.Exit(126)
	case "check":
		os.Exit(checkMain(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q\n", os.Args[1])
		os.Exit(2)
	}
}

func prctl(option, a2, a3 uintptr) (uintptr, error) {
	r, _, e := syscall.RawSyscall6(syscall.SYS_PRCTL, option, a2, a3, 0, 0, 0)
	if e != 0 {
		return r, e
	}
	return r, nil
}

// allThreads runs a 3-argument syscall on every OS thread of this Go process
// (requires CGO_ENABLED=0). perThread runs it on the calling thread only.
func allThreads(trap, a1, a2, a3 uintptr) error {
	_, _, e := syscall.AllThreadsSyscall(trap, a1, a2, a3)
	if e != 0 {
		return e
	}
	return nil
}

func perThread(trap, a1, a2, a3 uintptr) error {
	_, _, e := syscall.RawSyscall(trap, a1, a2, a3)
	if e != 0 {
		return e
	}
	return nil
}

type capHeader struct {
	Version uint32
	Pid     int32
}

type capData struct {
	Effective   uint32
	Permitted   uint32
	Inheritable uint32
}

const linuxCapV3 = 0x20080522

// capset sets this task's effective/permitted/inheritable sets.
func capset(eff, prm, inh uint64, all bool) error {
	hdr := capHeader{Version: linuxCapV3}
	data := [2]capData{
		{uint32(eff), uint32(prm), uint32(inh)},
		{uint32(eff >> 32), uint32(prm >> 32), uint32(inh >> 32)},
	}
	if all {
		return allThreads(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&hdr)), uintptr(unsafe.Pointer(&data[0])), 0)
	}
	return perThread(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&hdr)), uintptr(unsafe.Pointer(&data[0])), 0)
}

// dropBoundingExcept drops every bounding capability not in keep.
func dropBoundingExcept(keep map[int]bool, all bool) error {
	for c := 0; c < 64; c++ {
		if _, err := prctl(prCapbsetRead, uintptr(c), 0); err != nil {
			break // EINVAL past CAP_LAST_CAP
		}
		if keep[c] {
			continue
		}
		var err error
		if all {
			err = allThreads(syscall.SYS_PRCTL, prCapbsetDrop, uintptr(c), 0)
		} else {
			err = perThread(syscall.SYS_PRCTL, prCapbsetDrop, uintptr(c), 0)
		}
		if err != nil {
			return fmt.Errorf("drop cap %d: %w", c, err)
		}
	}
	return nil
}

// taskStatusSummary returns one line per thread with the security-relevant
// fields of /proc/self/task/<tid>/status.
func taskStatusSummary() []string {
	var out []string
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return []string{"read task dir: " + err.Error()}
	}
	for _, e := range ents {
		f := readStatus("/proc/self/task/" + e.Name() + "/status")
		out = append(out, fmt.Sprintf("tid=%s Uid=[%s] Gid=[%s] Groups=[%s] CapInh=%s CapPrm=%s CapEff=%s CapBnd=%s CapAmb=%s NoNewPrivs=%s Seccomp=%s Seccomp_filters=%s",
			e.Name(), f["Uid"], f["Gid"], f["Groups"], f["CapInh"], f["CapPrm"], f["CapEff"], f["CapBnd"], f["CapAmb"], f["NoNewPrivs"], f["Seccomp"], f["Seccomp_filters"]))
	}
	return out
}

func readStatus(path string) map[string]string {
	m := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		m["error"] = err.Error()
		return m
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		m[k] = strings.Join(strings.Fields(v), " ")
	}
	return m
}
