//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

// The checker workload. It must take its FD snapshot before anything else
// opens a file, so the snapshot is the first statement of checkMain.

var checkFails int

func check(name string, ok bool, format string, a ...any) {
	res := "PASS"
	if !ok {
		res = "FAIL"
		checkFails++
	}
	fmt.Printf("CHECK %s %s %s\n", name, res, fmt.Sprintf(format, a...))
}

func info(format string, a ...any) { fmt.Printf("INFO "+format+"\n", a...) }

func checkMain(_ []string) int {
	fds := listFDsLabeled() // excludes its own directory fd
	fmt.Printf("WORKLOAD_STARTED pid=%d\n", os.Getpid())

	checkFDs(fds)
	checkThreads()
	checkSecurebits()
	checkIDs()
	checkSyscalls()
	checkMounts()
	checkMisc()
	checkEnvSpecific()
	checkPython()
	fmt.Printf("CHECK_SUMMARY go_fail=%d\n", checkFails)
	if checkFails > 0 {
		return 1
	}
	return 0
}

func checkFDs(fds []string) {
	var extra, std []string
	stdOK := true
	for _, l := range fds {
		nm, rest, _ := strings.Cut(l, "->")
		target, _, _ := strings.Cut(rest, " cloexec=")
		if nm == "0" || nm == "1" || nm == "2" {
			std = append(std, l)
			if !strings.HasPrefix(target, "pipe:[") {
				stdOK = false
			}
			continue
		}
		extra = append(extra, l)
	}
	check("fds_no_extra", len(extra) == 0, "extra=%q", extra)
	check("fds_012_pipes", stdOK && len(std) == 3, "std=%q", std)
}

func checkThreads() {
	// Make sure there are several OS threads to inspect.
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
		check("threads", false, "%v", err)
		return
	}
	capsOK, nnpOK, scOK := true, true, true
	var bad []string
	for _, e := range ents {
		f := readStatus("/proc/self/task/" + e.Name() + "/status")
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
		n, _ := strconv.Atoi(f["Seccomp_filters"])
		if f["Seccomp"] != "2" || n < baselineFilters()+1 {
			scOK = false
			bad = append(bad, fmt.Sprintf("tid %s Seccomp=%s filters=%s", e.Name(), f["Seccomp"], f["Seccomp_filters"]))
		}
	}
	for _, l := range taskStatusSummary() {
		info("thread %s", l)
	}
	check("threads_caps_all_zero", capsOK && len(ents) >= 5, "threads=%d bad=%q", len(ents), bad)
	check("threads_no_new_privs", nnpOK, "threads=%d", len(ents))
	check("threads_seccomp_filter", scOK, "threads=%d", len(ents))
}

func checkSecurebits() {
	r, err := prctl(prGetSecurebits, 0, 0)
	check("securebits_value", err == nil && r == wantSecurebits, "got=%#x want=%#x err=%v", r, wantSecurebits, err)
	locked := r&(secbitNorootLocked|secbitNoSetuidFixupLockd) == secbitNorootLocked|secbitNoSetuidFixupLockd
	_, err2 := prctl(prSetSecurebits, 0, 0)
	check("securebits_locked", locked && err2 != nil, "lock bits set=%v; clearing attempt -> %v", locked, err2)
}

func checkIDs() {
	var ru, eu, su, rg, eg, sg int32
	syscall.RawSyscall(syscall.SYS_GETRESUID, uintptr(unsafe.Pointer(&ru)), uintptr(unsafe.Pointer(&eu)), uintptr(unsafe.Pointer(&su)))
	syscall.RawSyscall(syscall.SYS_GETRESGID, uintptr(unsafe.Pointer(&rg)), uintptr(unsafe.Pointer(&eg)), uintptr(unsafe.Pointer(&sg)))
	groups, gerr := syscall.Getgroups()
	check("uid", ru == workloadUID && eu == workloadUID && su == workloadUID, "resuid=%d,%d,%d", ru, eu, su)
	check("gid", rg == workloadGID && eg == workloadGID && sg == workloadGID, "resgid=%d,%d,%d", rg, eg, sg)
	st := readStatus("/proc/self/status")
	check("groups_empty", gerr == nil && len(groups) == 0 && st["Groups"] == "", "getgroups=%v err=%v status.Groups=%q", groups, gerr, st["Groups"])
	for _, f := range []string{"uid_map", "gid_map", "setgroups"} {
		b, _ := os.ReadFile("/proc/self/" + f)
		info("/proc/self/%s: %s", f, strings.Join(strings.Fields(string(b)), " "))
	}
}

//go:nosplit
//go:norace
func rawCloneNewNS() (uintptr, syscall.Errno) {
	r, _, e := syscall.RawSyscall6(syscall.SYS_CLONE, syscall.CLONE_NEWNS|uintptr(syscall.SIGCHLD), 0, 0, 0, 0, 0)
	if e == 0 && r == 0 {
		for {
			syscall.RawSyscall(syscall.SYS_EXIT_GROUP, 0, 0, 0)
		}
	}
	return r, e
}

func checkSyscalls() {
	_, _, e := syscall.RawSyscall(sysClone3, 0, 0, 0)
	check("clone3_enosys", e == syscall.ENOSYS, "clone3(NULL,0) -> %v (without seccomp the kernel returns EINVAL)", e)

	type probe struct {
		name string
		fn   func() error
	}
	errOf := func(e syscall.Errno) error {
		if e == 0 {
			return nil
		}
		return e
	}
	probes := []probe{
		{"mount", func() error { return syscall.Mount("tmpfs", "/tmp", "tmpfs", 0, "") }},
		{"unshare(CLONE_NEWUSER)", func() error { return syscall.Unshare(syscall.CLONE_NEWUSER) }},
		{"clone(CLONE_NEWNS)", func() error {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			_, e := rawCloneNewNS()
			return errOf(e)
		}},
		{"socket(AF_INET)", func() error {
			fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
			if err == nil {
				syscall.Close(fd)
			}
			return err
		}},
		{"io_uring_setup", func() error {
			var params [120]byte
			r, _, e := syscall.RawSyscall(425, 1, uintptr(unsafe.Pointer(&params[0])), 0)
			if e == 0 {
				syscall.Close(int(r))
			}
			return errOf(e)
		}},
		{"keyctl(GET_KEYRING_ID)", func() error {
			_, _, e := syscall.RawSyscall(250, 0, ^uintptr(2), 0) // KEY_SPEC_SESSION_KEYRING = -3
			return errOf(e)
		}},
		{"ptrace(PTRACE_TRACEME)", func() error {
			_, _, e := syscall.RawSyscall(syscall.SYS_PTRACE, syscall.PTRACE_TRACEME, 0, 0)
			return errOf(e)
		}},
	}
	allEPERM := true
	var res []string
	for _, p := range probes {
		err := p.fn()
		res = append(res, fmt.Sprintf("%s=%v", p.name, errString(err)))
		if !errors.Is(err, syscall.EPERM) {
			allEPERM = false
		}
	}
	check("seccomp_denylist_eperm", allEPERM, "%s", strings.Join(res, "; "))
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err == nil {
		syscall.Close(fd)
	}
	check("seccomp_allows_af_unix", err == nil, "socket(AF_UNIX) -> %v", errString(err))
}

type mountEntry struct {
	id, parent, root, point, opts, optional, fstype, source, superOpts string
}

func readMountinfo() []mountEntry {
	b, _ := os.ReadFile("/proc/self/mountinfo")
	var out []mountEntry
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		pre, post, _ := strings.Cut(l, " - ")
		f := strings.Fields(pre)
		g := strings.Fields(post)
		if len(f) < 6 || len(g) < 3 {
			continue
		}
		out = append(out, mountEntry{id: f[0], parent: f[1], root: f[3], point: f[4], opts: f[5],
			optional: strings.Join(f[6:], " "), fstype: g[0], source: g[1], superOpts: g[2]})
	}
	return out
}

func hasOpt(opts, o string) bool {
	for _, x := range strings.Split(opts, ",") {
		if x == o {
			return true
		}
	}
	return false
}

func checkMounts() {
	ms := readMountinfo()
	b, _ := os.ReadFile("/proc/self/mountinfo")
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		info("mountinfo %s", l)
	}
	private := true
	var roots []mountEntry
	byPoint := map[string]mountEntry{}
	var foreign []string
	allowed := map[string]bool{"/": true, "/usr": true, "/etc": true, "/in": true, "/tmp": true, "/run": true,
		"/out": true, "/dev": true, "/dev/null": true, "/dev/zero": true, "/dev/random": true, "/dev/urandom": true, "/proc": true, "/workspace": true, "/run/agentbox/gateway.sock": true}
	for _, m := range ms {
		if m.optional != "" {
			private = false
		}
		if m.point == "/" {
			roots = append(roots, m)
		}
		byPoint[m.point] = m
		if !allowed[m.point] && !strings.HasPrefix(m.point, "/proc/") && !strings.HasPrefix(m.point, "/usr/") {
			foreign = append(foreign, m.point)
		}
	}
	check("mounts_private_propagation", private, "entries=%d (no shared:/master:/propagate_from tags)", len(ms))
	rootOK := len(roots) == 1 && roots[0].root == "/tmp/spike-1b/rootfs"
	check("old_root_detached", rootOK && len(foreign) == 0, "mounts at /=%d root=%q unexpected mountpoints=%q", len(roots), func() string {
		if len(roots) > 0 {
			return roots[0].root
		}
		return ""
	}(), foreign)
	roOK := true
	var roDetail []string
	for _, m := range ms {
		if m.point == "/" || m.point == "/usr" || m.point == "/etc" || m.point == "/in" || strings.HasPrefix(m.point, "/usr/") {
			good := hasOpt(m.opts, "ro") && hasOpt(m.opts, "nosuid") && hasOpt(m.opts, "nodev")
			roOK = roOK && good
			roDetail = append(roDetail, fmt.Sprintf("%s=%s", m.point, m.opts))
		}
	}
	check("rootfs_ro_nosuid_nodev", roOK, "%s", strings.Join(roDetail, " "))

	var sysEnts []os.DirEntry
	sysEnts, _ = os.ReadDir("/sys")
	noSys := len(sysEnts) == 0
	for _, m := range ms {
		if m.fstype == "sysfs" || m.fstype == "cgroup2" || m.fstype == "cgroup" {
			noSys = false
		}
	}
	check("no_sys_no_cgroupfs", noSys, "/sys entries=%d", len(sysEnts))

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
			ok := len(ents) == 0 && m.fstype == "tmpfs" && hasOpt(m.opts, "ro")
			maskOK = maskOK && ok
			maskRes = append(maskRes, fmt.Sprintf("%s=dir(tmpfs ro empty:%v)", p, ok))
		} else {
			sys := st.Sys().(*syscall.Stat_t)
			ok := st.Mode()&os.ModeCharDevice != 0 && sys.Rdev == 0x103 // 1:3 = /dev/null
			b, _ := os.ReadFile(p)
			ok = ok && len(b) == 0
			maskOK = maskOK && ok
			maskRes = append(maskRes, fmt.Sprintf("%s=devnull:%v", p, ok))
		}
	}
	check("proc_masked", maskOK, "%s", strings.Join(maskRes, " "))
	roOK = true
	var roRes []string
	for _, p := range procReadonlyPaths {
		m, ok := byPoint[p]
		good := ok && hasOpt(m.opts, "ro")
		roOK = roOK && good
		roRes = append(roRes, fmt.Sprintf("%s=%v", p, good))
	}
	check("proc_readonly", roOK, "%s", strings.Join(roRes, " "))
}

func checkMisc() {
	var res []string
	ok := true
	try := func(path string, wantOK bool) {
		err := os.WriteFile(path, []byte("x"), 0o644)
		res = append(res, fmt.Sprintf("%s=%s", path, errString(err)))
		if (err == nil) != wantOK {
			ok = false
		}
		if err == nil {
			os.Remove(path)
		}
	}
	try("/tmp/w", true)
	try("/out/w", true)
	try("/w", false)
	try("/usr/w", false)
	try("/in/w", false)
	check("writable_areas", ok, "%s", strings.Join(res, " "))

	b, _ := os.ReadFile("/proc/net/dev")
	var ifs []string
	for _, l := range strings.Split(string(b), "\n")[2:] {
		if n, _, ok := strings.Cut(strings.TrimSpace(l), ":"); ok {
			ifs = append(ifs, n)
		}
	}
	check("netns_only_lo", len(ifs) == 1 && ifs[0] == "lo", "interfaces=%v", ifs)

	st := readStatus("/proc/self/status")
	check("signal_mask_clean", st["SigBlk"] == "0000000000000000" && st["SigIgn"] == "0000000000000000", "SigBlk=%s SigIgn=%s", st["SigBlk"], st["SigIgn"])
	h, _ := os.Hostname()
	var rl syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl)
	info("hostname=%s pid=%d ppid=%d RLIMIT_NOFILE=%d/%d", h, os.Getpid(), os.Getppid(), rl.Cur, rl.Max)
}

const pythonCheck = `
import os, sys, threading, subprocess, ctypes, errno
def check(name, ok, detail):
    print("CHECK %s %s %s" % (name, "PASS" if ok else "FAIL", detail), flush=True)
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
    if d["NoNewPrivs"] != "1" or d["Seccomp"] != "2" or int(d["Seccomp_filters"]) < int(os.environ.get("SPIKE_BASELINE_FILTERS", "0")) + 1: bad.append((tid, "nnp/seccomp"))
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
try:
    import multiprocessing as mp
    with mp.get_context("fork").Pool(2) as pool:
        r = pool.map(abs, [-1, -2, -3])
    print("INFO py_multiprocessing_fork_pool ok %r" % r)
except Exception as ex:
    print("INFO py_multiprocessing_fork_pool error %r" % ex)
`

func checkPython() {
	cmd := exec.Command("/usr/bin/python3", "-c", pythonCheck)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	check("py_ran", err == nil, "python3 -> %v (Go os/exec also starts it under the same filter)", errString(err))
}

// baselineFilters is the number of seccomp filters every process on this host
// already carries (WSL2's init installs one), passed in by the launcher. Our
// filter must be on top of it.
func baselineFilters() int {
	n, _ := strconv.Atoi(os.Getenv("SPIKE_BASELINE_FILTERS"))
	return n
}

// checkEnvSpecific covers the last two §16.2 items: Gateway socket visibility
// and session workspace access under a retained UID range.
func checkEnvSpecific() {
	const sock = "/run/agentbox/gateway.sock"
	switch os.Getenv("SPIKE_ENV") {
	case "orchestrator":
		st, err := os.Stat(sock)
		owner := ""
		if err == nil {
			s := st.Sys().(*syscall.Stat_t)
			owner = fmt.Sprintf("uid=%d gid=%d mode=%v", s.Uid, s.Gid, st.Mode())
		}
		reply := ""
		fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
		if err == nil {
			err = syscall.Connect(fd, &syscall.SockaddrUnix{Name: sock})
			if err == nil {
				syscall.Write(fd, []byte("ping\n"))
				b := make([]byte, 64)
				n, _ := syscall.Read(fd, b)
				if n > 0 {
					reply = strings.TrimSpace(string(b[:n]))
				}
			}
			syscall.Close(fd)
		}
		check("gateway_socket_uid1000", err == nil && reply == "pong", "%s; connect=%s reply=%q", owner, errString(err), reply)
	case "exec":
		_, err := os.Stat(sock)
		_, err2 := os.Stat("/run/agentbox")
		check("gateway_socket_absent_in_exec", errors.Is(err, os.ErrNotExist) && errors.Is(err2, os.ErrNotExist), "stat %s -> %v; /run/agentbox -> %v", sock, err, err2)
	}
	switch phase := os.Getenv("SPIKE_WS_PHASE"); phase {
	case "1":
		err := os.WriteFile("/workspace/state.txt", []byte("written in phase 1\n"), 0o600)
		check("session_workspace_write", err == nil, "write /workspace/state.txt -> %s", errString(err))
	case "2":
		b, err := os.ReadFile("/workspace/state.txt")
		var err2 error
		if err == nil {
			err2 = os.WriteFile("/workspace/state.txt", append(b, []byte("appended in phase 2\n")...), 0o600)
		}
		check("session_workspace_resume", err == nil && err2 == nil && strings.HasPrefix(string(b), "written in phase 1"),
			"read -> %s %q; rewrite -> %s", errString(err), string(b), errString(err2))
	case "2-foreign":
		_, err := os.ReadFile("/workspace/state.txt")
		check("session_workspace_foreign_range_denied", errors.Is(err, os.ErrPermission), "read with a different UID range -> %v", err)
	}
}
