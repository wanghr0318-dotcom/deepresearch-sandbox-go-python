//go:build linux

package main

import (
	"runtime"
	"syscall"
	"unsafe"
)

// Go's os/exec cannot start a program with execveat(fd, "", AT_EMPTY_PATH), so
// init forks with a raw clone and execs from a nosplit function, the way
// syscall.forkAndExecInChild does. Between clone and execveat the child calls
// only raw syscalls (no allocation, no stack growth). Signals are blocked
// around the clone so no Go handler runs in the child; the child installs an
// empty mask just before execveat so the helper (and the workload after it)
// do not inherit a blocked SIGTERM.

type forkArgs struct {
	dirfd uintptr
	path  *byte    // "" (AT_EMPTY_PATH)
	argv  **byte   // NULL-terminated
	envv  **byte   // NULL-terminated
	fds   [4]int32 // sources for child fds 0,1,2,3
	empty uint64   // empty signal mask
	all   uint64   // full signal mask
}

//go:nosplit
//go:norace
func rawForkExecveat(a *forkArgs, oldmask *uint64) (pid uintptr, errno syscall.Errno) {
	_, _, e := syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(&a.all)), uintptr(unsafe.Pointer(oldmask)), 8, 0, 0)
	if e != 0 {
		return 0, e
	}
	r1, _, e := syscall.RawSyscall6(syscall.SYS_CLONE, uintptr(syscall.SIGCHLD), 0, 0, 0, 0, 0)
	if e != 0 || r1 != 0 {
		// parent (or clone failure): restore mask
		syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(oldmask)), 0, 8, 0, 0)
		return r1, e
	}
	// child
	for i := 0; i < 4; i++ {
		if _, _, e = syscall.RawSyscall(syscall.SYS_DUP3, uintptr(a.fds[i]), uintptr(i), 0); e != 0 {
			goto fail
		}
	}
	syscall.RawSyscall(syscall.SYS_SETPGID, 0, 0, 0)
	syscall.RawSyscall6(syscall.SYS_RT_SIGPROCMASK, sigSetmask, uintptr(unsafe.Pointer(&a.empty)), 0, 8, 0, 0)
	_, _, e = syscall.RawSyscall6(sysExecveat, a.dirfd, uintptr(unsafe.Pointer(a.path)),
		uintptr(unsafe.Pointer(a.argv)), uintptr(unsafe.Pointer(a.envv)), atEmptyPath, 0)
fail:
	{
		// Report "E" + errno (little endian) on fd 3, the exec-status pipe.
		var buf [5]byte
		buf[0] = 'E'
		buf[1] = byte(e)
		buf[2] = byte(e >> 8)
		buf[3] = byte(e >> 16)
		buf[4] = byte(e >> 24)
		syscall.RawSyscall(syscall.SYS_WRITE, 3, uintptr(unsafe.Pointer(&buf[0])), 5)
		for {
			syscall.RawSyscall(syscall.SYS_EXIT_GROUP, 127, 0, 0)
		}
	}
}

// forkExecveat starts argv from the open file dirfd. fds are the sources of
// the child's fds 0..3; they should be close-on-exec in the parent and must
// not collide with 0..3 (callers dup them high first).
func forkExecveat(dirfd int, argv, envv []string, fds [4]int) (int, error) {
	argvp, err := syscall.SlicePtrFromStrings(argv)
	if err != nil {
		return 0, err
	}
	envvp, err := syscall.SlicePtrFromStrings(envv)
	if err != nil {
		return 0, err
	}
	empty := []byte{0}
	a := &forkArgs{
		dirfd: uintptr(dirfd),
		path:  &empty[0],
		argv:  &argvp[0],
		envv:  &envvp[0],
		all:   ^uint64(0),
	}
	for i := range fds {
		a.fds[i] = int32(fds[i])
	}
	var oldmask uint64
	runtime.LockOSThread()
	pid, e := rawForkExecveat(a, &oldmask)
	runtime.UnlockOSThread()
	runtime.KeepAlive(argvp)
	runtime.KeepAlive(envvp)
	runtime.KeepAlive(empty)
	if e != 0 {
		return 0, e
	}
	return int(pid), nil
}
