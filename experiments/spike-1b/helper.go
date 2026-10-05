//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// helperSpec travels in the start message and on to the helper's environment.
type helperSpec struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env"`
	// Fail names one step that must fail (injected or real, see below).
	Fail string `json:"fail,omitempty"`
	// Order: "final" (the result of this spike) or "candidate" (spec §4.6 text).
	Order string `json:"order,omitempty"`
	// Capmode: "all" (AllThreadsSyscall), "perthread" (locked thread only),
	// "perthread-nolock" (no LockOSThread: B4 negative variant).
	Capmode string `json:"capmode,omitempty"`
	// NoFdHygiene skips close_range(4, ~0, CLOSE_RANGE_CLOEXEC) (B5 variant).
	NoFdHygiene bool `json:"no_fd_hygiene,omitempty"`
	// Dump prints every thread's status to stderr just before execve.
	Dump bool `json:"dump,omitempty"`
	// Clone3Errno "eperm" makes clone3 fail with EPERM instead of ENOSYS (B4 control).
	Clone3Errno string `json:"clone3_errno,omitempty"`
}

var (
	finalOrder = []string{"fd_hygiene", "drop_bounding", "securebits", "setresgid", "setresuid",
		"clear_caps", "clear_ambient", "no_new_privs", "seccomp", "execve"}
	// Spec §4.6 candidate text: securebits after the UID switch and cap clearing.
	candidateOrder = []string{"fd_hygiene", "drop_bounding", "setresgid", "setresuid",
		"clear_caps", "clear_ambient", "securebits", "no_new_privs", "seccomp", "execve"}
)

// helperReport sends a failure reason to init on fd 3 and exits. The
// workload never runs.
func helperReport(step string, err error) {
	msg := "R" + step + ": " + err.Error()
	syscall.Write(3, []byte(msg))
	os.Exit(126)
}

func helperMain(_ []string) {
	var spec helperSpec
	if err := json.Unmarshal([]byte(os.Getenv("SPIKE_HELPER_SPEC")), &spec); err != nil {
		helperReport("spec", err)
	}
	if _, err := prctl(15, uintptr(unsafe.Pointer(&append([]byte(helperComm), 0)[0])), 0); err != nil { // PR_SET_NAME
		helperReport("set_name", err)
	}
	if spec.Capmode == "" {
		spec.Capmode = "all"
	}
	if spec.Capmode != "perthread-nolock" {
		runtime.LockOSThread()
	} else {
		// Encourage goroutine migration between OS threads.
		for i := 0; i < 8; i++ {
			go func() {
				for {
					runtime.Gosched()
					time.Sleep(50 * time.Microsecond)
				}
			}()
		}
	}
	all := spec.Capmode == "all"
	order := finalOrder
	if spec.Order == "candidate" {
		order = candidateOrder
	} else if spec.Order == "final-noclear" {
		// B3 control: no explicit capset/ambient clearing; under
		// NO_SETUID_FIXUP the helper keeps its permitted set after setresuid
		order = []string{"fd_hygiene", "drop_bounding", "securebits", "setresgid", "setresuid", "no_new_privs", "seccomp", "execve"}
	}
	clone3Errno := uint32(enosys)
	if spec.Clone3Errno == "eperm" {
		clone3Errno = eperm
	}
	prog, err := buildSeccompFilter(clone3Errno)
	if err != nil {
		helperReport("seccomp_build", err)
	}

	sys := func(trap, a1, a2, a3 uintptr) error {
		if all {
			return allThreads(trap, a1, a2, a3)
		}
		return perThread(trap, a1, a2, a3)
	}
	idFor := func(step string, id int) uintptr {
		if spec.Fail == step {
			return 99999 // real failure: unmapped ID -> EINVAL
		}
		return uintptr(id)
	}

	for _, step := range order {
		if spec.Capmode == "perthread-nolock" {
			runtime.Gosched()
			time.Sleep(200 * time.Microsecond)
		}
		// Injected failures for steps whose real failure is hard to provoke.
		switch step {
		case "fd_hygiene", "drop_bounding", "securebits", "clear_caps", "clear_ambient", "no_new_privs":
			if spec.Fail == step {
				helperReport(step, errInjected)
			}
		}
		var err error
		switch step {
		case "fd_hygiene":
			syscall.CloseOnExec(3) // exec-status pipe: closes when the workload execs
			if !spec.NoFdHygiene {
				_, _, e := syscall.RawSyscall(sysCloseRange, 4, uintptr(^uint32(0)), closeRangeCloexec)
				if e != 0 {
					err = e
				}
			}
		case "drop_bounding":
			err = dropBoundingExcept(nil, all)
		case "securebits":
			err = sys(syscall.SYS_PRCTL, prSetSecurebits, wantSecurebits, 0)
		case "setresgid":
			g := idFor(step, workloadGID)
			err = sys(syscall.SYS_SETRESGID, g, g, g)
		case "setresuid":
			u := idFor(step, workloadUID)
			err = sys(syscall.SYS_SETRESUID, u, u, u)
		case "clear_caps":
			err = capset(0, 0, 0, all)
		case "clear_ambient":
			err = sys(syscall.SYS_PRCTL, prCapAmbient, prCapAmbientClear, 0)
		case "no_new_privs":
			err = sys(syscall.SYS_PRCTL, prSetNoNewPrivs, 1, 0)
		case "seccomp":
			p := prog
			if spec.Fail == step {
				p = nil // real failure: empty filter -> EINVAL
			}
			err = installSeccomp(p)
		case "execve":
			if spec.Fail == "helper_sigkill" {
				// the helper dies after every check but before execve
				syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
			}
			if spec.Dump {
				fmt.Fprintf(os.Stderr, "[helper] capmode=%s threads just before execve:\n  %s\n", spec.Capmode,
					strings.Join(taskStatusSummary(), "\n  "))
			}
			path := spec.Argv[0]
			if spec.Fail == step {
				path = "/nonexistent/workload" // real failure: ENOENT
			}
			err = syscall.Exec(path, spec.Argv, spec.Env)
		}
		if err != nil {
			helperReport(step, err)
		}
	}
}
