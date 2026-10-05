//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ctlMsg is the spike's control-channel message (one SEQPACKET frame each).
type ctlMsg struct {
	Type   string      `json:"type"`
	ExecID string      `json:"exec_id,omitempty"`
	Helper *helperSpec `json:"helper,omitempty"`
	Pid    int         `json:"pid,omitempty"`
	Reason string      `json:"reason,omitempty"`
	Code   int         `json:"code,omitempty"`
	Signal int         `json:"signal,omitempty"`
	Info   []string    `json:"info,omitempty"`
}

const (
	fDupfd        = 0
	fDupfdCloexec = 1030
)

func fcntl(fd, cmd, arg int) (int, error) {
	r, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	if e != 0 {
		return -1, e
	}
	return int(r), nil
}

type initState struct {
	ctl    int
	sendMu sync.Mutex
	selfFd int
	fail   string

	mu  sync.Mutex // reg.mu: covers start (fork -> register -> enqueue ack) and reaping
	reg map[int]string
}

func ilog(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[init] "+format+"\n", a...)
}

func (s *initState) send(m ctlMsg) {
	b, _ := json.Marshal(m)
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := syscall.Sendmsg(s.ctl, b, nil, nil, 0); err != nil {
		ilog("send %s: %v", m.Type, err)
	}
}

func initMain() int {
	leak := os.Getenv("SPIKE_LEAK")
	// B5 evidence: how the control socket arrives from os/exec ExtraFiles.
	fl, _ := fcntl(3, syscall.F_GETFD, 0)
	ilog("pid=%d; fd 3 (control socket from ExtraFiles) inherited with FD_CLOEXEC=%v", os.Getpid(), fl&syscall.FD_CLOEXEC != 0)
	dupCmd := fDupfdCloexec
	if leak == "ctl" {
		dupCmd = fDupfd // deliberately leaky variant
	}
	ctl, err := fcntl(3, dupCmd, 10)
	if err != nil {
		ilog("dup control socket: %v", err)
		return 1
	}
	syscall.Close(3)
	s := &initState{ctl: ctl, reg: map[int]string{}, fail: os.Getenv("SPIKE_FAIL_INIT")}

	info, err := s.setup(leak)
	if err != nil {
		ilog("setup failed: %v", err)
		s.send(ctlMsg{Type: "init_err", Reason: "init/" + err.Error()})
		return 1
	}
	s.send(ctlMsg{Type: "ready", Info: info})

	sigc := make(chan os.Signal, 16)
	signal.Notify(sigc, syscall.SIGCHLD)
	go func() {
		for range sigc {
			s.reap()
		}
	}()
	s.serve()
	// Control channel closed: kill everything in the PID namespace and exit.
	syscall.Kill(-1, syscall.SIGKILL)
	s.reap()
	return 0
}

func (s *initState) setup(leak string) ([]string, error) {
	root := os.Getenv("SPIKE_ROOT")
	if err := syscall.Sethostname([]byte("agentbox-spike")); err != nil {
		return nil, wrap("sethostname", err)
	}
	if err := setupMounts(os.Getenv("SPIKE_MOUNTS"), root, os.Getenv("SPIKE_IN"), s.fail, ilog); err != nil {
		return nil, err
	}
	// O_PATH fd on our own binary, opened before pivot_root (spec §4.6).
	if err := failpoint(s.fail, "open_self"); err != nil {
		return nil, err
	}
	flags := oPath | syscall.O_CLOEXEC
	if leak == "self" {
		flags = oPath // deliberately leaky variant
	}
	// SPIKE_SELF selects where the helper binary fd comes from (B3/B5 experiment):
	//   procexe (default) /proc/self/exe before pivot_root: the file as seen through the OLD root mount
	//   newroot           <root>/opt/spike/spike1b before pivot_root: through the new root mount
	//   after             /opt/spike/spike1b after pivot_root
	selfMode := os.Getenv("SPIKE_SELF")
	openSelf := func(p string) error {
		fd, err := syscall.Open(p, flags, 0)
		if err != nil {
			return wrap("open_self", err)
		}
		// Move it out of 0..3, which the fork child overwrites with dup3. (First
		// run: the fd was 3, was replaced by the exec-status pipe, and
		// execveat failed with EACCES.)
		dcmd := fDupfdCloexec
		if leak == "self" {
			dcmd = fDupfd
		}
		hi, err := fcntl(fd, dcmd, 200)
		syscall.Close(fd)
		if err != nil {
			return wrap("open_self", err)
		}
		fd = hi
		s.selfFd = fd
		t, _ := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
		ilog("helper binary fd %d opened from %s (mode=%q) -> %s", fd, p, selfMode, t)
		return nil
	}
	switch selfMode {
	case "", "procexe":
		if err := openSelf("/proc/self/exe"); err != nil {
			return nil, err
		}
	case "newroot":
		if err := openSelf(root + "/opt/spike/spike1b"); err != nil {
			return nil, err
		}
	}
	if err := pivotInto(root, s.fail); err != nil {
		return nil, err
	}
	if selfMode == "after" {
		if err := openSelf("/opt/spike/spike1b"); err != nil {
			return nil, err
		}
	}
	if t, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", s.selfFd)); err == nil {
		ilog("after pivot_root, helper binary fd %d -> %s", s.selfFd, t)
	}
	// init capability state: bounding {KILL,SETUID,SETGID,SETPCAP}, effective
	// and permitted {KILL}. Applied to every thread of this Go process.
	if err := failpoint(s.fail, "init_caps"); err != nil {
		return nil, err
	}
	keep := map[int]bool{capKill: true, capSetgid: true, capSetuid: true, capSetpcap: true}
	if err := dropBoundingExcept(keep, true); err != nil {
		return nil, wrap("init_caps", err)
	}
	if os.Getenv("SPIKE_INIT_NOROOT") == "1" {
		// B3 negative variant: lock SECBIT_NOROOT in init, before the helper exec.
		if err := allThreads(syscall.SYS_PRCTL, prSetSecurebits, wantSecurebits, 0); err != nil {
			return nil, wrap("init_securebits", err)
		}
		ilog("B3 variant: init locked securebits=%#x before starting the helper", wantSecurebits)
	}
	if err := capset(1<<capKill, 1<<capKill, 0, true); err != nil {
		return nil, wrap("init_caps", err)
	}
	var info []string
	for _, l := range taskStatusSummary() {
		info = append(info, "init thread: "+l)
	}
	mi, _ := os.ReadFile("/proc/self/mountinfo")
	for _, l := range strings.Split(strings.TrimSpace(string(mi)), "\n") {
		info = append(info, "init mountinfo: "+l)
	}
	info = append(info, fmt.Sprintf("init fds: %s", strings.Join(listFDsLabeled(), " | ")))
	return info, nil
}

func (s *initState) serve() {
	buf := make([]byte, 64*1024)
	oob := make([]byte, syscall.CmsgSpace(4*4))
	for {
		n, oobn, flags, _, err := syscall.Recvmsg(s.ctl, buf, oob, syscall.MSG_CMSG_CLOEXEC)
		if err == syscall.EINTR {
			continue
		}
		if err != nil || n == 0 {
			ilog("control channel closed (n=%d err=%v)", n, err)
			return
		}
		var fds []int
		if oobn > 0 {
			cms, _ := syscall.ParseSocketControlMessage(oob[:oobn])
			for _, cm := range cms {
				r, _ := syscall.ParseUnixRights(&cm)
				fds = append(fds, r...)
			}
		}
		var m ctlMsg
		if flags&(syscall.MSG_TRUNC|syscall.MSG_CTRUNC) != 0 || json.Unmarshal(buf[:n], &m) != nil || m.Type != "start" || len(fds) != 3 || m.Helper == nil {
			for _, fd := range fds {
				syscall.Close(fd)
			}
			s.send(ctlMsg{Type: "start_err", ExecID: m.ExecID, Reason: "protocol: malformed start"})
			continue
		}
		s.start(m, fds)
	}
}

func (s *initState) start(m ctlMsg, fds []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := func(reason string) { s.send(ctlMsg{Type: "start_err", ExecID: m.ExecID, Reason: reason}) }

	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_CLOEXEC); err != nil {
		closeAll(fds)
		fail("init/exec_status_pipe: " + err.Error())
		return
	}
	errR, errW := p[0], p[1]
	var hi [4]int
	for i, fd := range append(fds, errW) {
		h, err := fcntl(fd, fDupfdCloexec, 100)
		if err != nil {
			fail("init/dup: " + err.Error())
			return
		}
		hi[i] = h
		syscall.Close(fd)
	}
	spec, _ := json.Marshal(m.Helper)
	argv := []string{"agentbox-helper", "helper"}
	env := []string{"SPIKE_HELPER_SPEC=" + string(spec)}
	dirfd := s.selfFd
	if m.Helper.Fail == "execveat" {
		dirfd = 9999 // real failure: EBADF from execveat
	}
	pid, err := forkExecveat(dirfd, argv, env, hi)
	for _, h := range hi {
		syscall.Close(h)
	}
	if err != nil {
		syscall.Close(errR)
		fail("init/fork: " + err.Error())
		return
	}
	// Wait for the exec-status pipe: EOF = the helper exec'd the workload
	// (fd 3 is close-on-exec in the helper); data = a step failed.
	var out []byte
	b := make([]byte, 4096)
	for {
		n, err := syscall.Read(errR, b)
		if err == syscall.EINTR {
			continue
		}
		if n <= 0 {
			break
		}
		out = append(out, b[:n]...)
	}
	syscall.Close(errR)
	if len(out) > 0 {
		var reason string
		switch out[0] {
		case 'E':
			var e uint32
			for i := 0; i < 4 && 1+i < len(out); i++ {
				e |= uint32(out[1+i]) << (8 * i)
			}
			reason = "init/execveat: " + syscall.Errno(e).Error()
		case 'R':
			reason = "helper/" + string(out[1:])
		default:
			reason = "init/exec_status: garbled"
		}
		s.reg[pid] = "" // failed start: reaped silently
		fail(reason)
		return
	}
	if os.Getenv("SPIKE_NAIVE_ACK") != "1" {
		if reason := confirmExec(pid); reason != "" {
			fail(reason)
			return
		}
	}
	s.reg[pid] = m.ExecID
	s.send(ctlMsg{Type: "start_ack", ExecID: m.ExecID, Pid: pid})
}

func closeAll(fds []int) {
	for _, fd := range fds {
		syscall.Close(fd)
	}
}

func (s *initState) reap() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil || pid <= 0 {
			return
		}
		id, ok := s.reg[pid]
		delete(s.reg, pid)
		if !ok || id == "" {
			ilog("reaped pid %d (not a started workload) status=%v", pid, ws)
			continue
		}
		m := ctlMsg{Type: "exit", ExecID: id, Code: ws.ExitStatus()}
		if ws.Signaled() {
			m.Signal = int(ws.Signal())
		}
		s.send(m)
	}
}

// listFDsLabeled lists this process's fds with their targets and FD_CLOEXEC.
func listFDsLabeled() []string {
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
		var fd int
		fmt.Sscan(nm, &fd)
		if fd == dfd {
			continue
		}
		t, _ := os.Readlink("/proc/self/fd/" + nm)
		fl, _ := fcntl(fd, syscall.F_GETFD, 0)
		out = append(out, fmt.Sprintf("%s->%s cloexec=%v", nm, t, fl&syscall.FD_CLOEXEC != 0))
	}
	return out
}

const helperComm = "agentbox-helper"

// confirmExec tells "the helper exec'd the workload" apart from "the helper
// died before exec": both close the exec-status pipe with no data. The helper
// names itself agentbox-helper and execve renames the task after the new
// image (comm is set just after close-on-exec fds are closed, hence the
// short poll). A helper that is a zombie still named agentbox-helper never
// reached the workload; it is reaped here and reported as start_err.
func confirmExec(pid int) string {
	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			return "init/confirm_exec: " + err.Error()
		}
		st := string(b)
		l, r := strings.IndexByte(st, '('), strings.LastIndexByte(st, ')')
		if l < 0 || r < 0 || r+2 >= len(st) {
			return "init/confirm_exec: bad stat"
		}
		comm, state := st[l+1:r], st[r+2]
		if comm != helperComm {
			return ""
		}
		if state == 'Z' {
			var ws syscall.WaitStatus
			syscall.Wait4(pid, &ws, 0, nil)
			if ws.Signaled() {
				return "helper/died_before_exec: signal " + ws.Signal().String()
			}
			return fmt.Sprintf("helper/died_before_exec: exit %d", ws.ExitStatus())
		}
		if time.Now().After(deadline) {
			return "init/confirm_exec: timeout"
		}
		time.Sleep(time.Millisecond)
	}
}
