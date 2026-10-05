//go:build linux

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	scratch    = "/tmp/spike-1b"
	rootfsDir  = scratch + "/rootfs"
	inDir      = scratch + "/in"
	cgroupPath = "/sys/fs/cgroup/agentbox-spike-1b"
)

func llog(format string, a ...any) { fmt.Printf("[launch] "+format+"\n", a...) }

func launchMain(args []string) int {
	fs := flag.NewFlagSet("launch", flag.ExitOnError)
	label := fs.String("label", "run", "scenario label")
	mounts := fs.String("mounts", "classic", "classic | classic-nonrec | newapi | hostprep | hostprep-norebind")
	groups := fs.String("groups", "clear", "clear (setgroups([]) in the host before clone) | inherit | credential | credential-nonempty (SysProcAttr.Credential.Groups inside the userns)")
	leak := fs.String("leak", "", "ctl | self: init keeps that fd without close-on-exec")
	selfMode := fs.String("self", "procexe", "procexe | newroot | after: source of init's helper-binary fd")
	skipDetach := fs.Bool("skip-detach", false, "diagnostic: keep the old root attached")
	failInit := fs.String("fail-init", "", "init setup step to fail")
	initNoroot := fs.Bool("init-noroot", false, "init locks SECBIT_NOROOT before starting the helper")
	fail := fs.String("fail", "", "helper/start step to fail")
	order := fs.String("order", "final", "final | candidate")
	capmode := fs.String("capmode", "all", "all | perthread | perthread-nolock")
	noFdHygiene := fs.Bool("no-fd-hygiene", false, "helper skips close_range(CLOEXEC)")
	dump := fs.Bool("dump", false, "helper dumps thread status before execve")
	naiveAck := fs.Bool("naive-ack", false, "init treats exec-status EOF alone as success")
	clone3 := fs.String("clone3", "enosys", "enosys | eperm: seccomp action for clone3")
	workload := fs.String("workload", "check", "check | true")
	envKind := fs.String("env", "exec", "exec | orchestrator (orchestrator gets the Gateway socket)")
	wsPhase := fs.String("ws-phase", "", "1 | 2 | 2-foreign: session workspace phase (bind /tmp/spike-1b/ws at /workspace)")
	idBase := fs.Int("idbase", hostIDBase, "host ID base of the namespace range")
	fs.Parse(args)

	res := map[string]string{"label": *label, "outcome": "?", "reason": "", "workload_started": "no"}
	defer func() {
		fmt.Printf("RESULT label=%s outcome=%s workload_started=%s pass=%s fail=%s exit=%s reason=%q\n",
			res["label"], res["outcome"], res["workload_started"], res["pass"], res["fail"], res["exit"], res["reason"])
	}()
	if os.Geteuid() != 0 {
		res["outcome"], res["reason"] = "error", "launcher must run as root"
		return 1
	}

	st := readStatus("/proc/self/status")
	llog("launcher supplementary groups before: [%s]", st["Groups"])
	switch *groups {
	case "clear":
		if err := syscall.Setgroups([]int{}); err != nil {
			res["outcome"], res["reason"] = "error", "setgroups: "+err.Error()
			return 1
		}
		llog("launcher cleared its supplementary groups before clone: [%s]", readStatus("/proc/self/status")["Groups"])
	case "inherit", "credential", "credential-nonempty":
	default:
		res["outcome"], res["reason"] = "error", "bad -groups"
		return 1
	}

	if *envKind == "orchestrator" {
		stop, err := startGateway(*idBase + workloadUID)
		if err != nil {
			res["outcome"], res["reason"] = "error", "gateway: "+err.Error()
			return 1
		}
		defer stop()
	}
	if *wsPhase != "" {
		if err := prepareWorkspace(*idBase + workloadUID); err != nil {
			res["outcome"], res["reason"] = "error", "workspace: "+err.Error()
			return 1
		}
	}
	cgfd, err := setupCgroup()
	if err != nil {
		res["outcome"], res["reason"] = "error", "cgroup: "+err.Error()
		return 1
	}
	defer cleanupCgroup(cgfd)

	if strings.HasPrefix(*mounts, "hostprep") {
		if err := hostPrepMounts(); err != nil {
			res["outcome"], res["reason"] = "error", "hostprep: "+err.Error()
			hostPrepCleanup()
			return 1
		}
		defer hostPrepCleanup()
	}

	sp, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		res["outcome"], res["reason"] = "error", err.Error()
		return 1
	}
	ctl := sp[0]
	child := os.NewFile(uintptr(sp[1]), "ctl-child")

	credGroups := []uint32{}
	if *groups == "credential-nonempty" {
		credGroups = []uint32{0}
	}
	cmd := exec.Command(rootfsDir+"/opt/spike/spike1b", "init")
	cmd.Args[0] = "agentbox-init"
	cmd.Env = []string{"SPIKE_ROOT=" + rootfsDir, "SPIKE_IN=" + inDir, "SPIKE_MOUNTS=" + *mounts,
		"SPIKE_FAIL_INIT=" + *failInit, "SPIKE_LEAK=" + *leak, "SPIKE_SELF=" + *selfMode}
	if *envKind == "orchestrator" {
		cmd.Env = append(cmd.Env, "SPIKE_GW="+gatewaySock)
	}
	if *wsPhase != "" {
		cmd.Env = append(cmd.Env, "SPIKE_WS="+wsDir)
	}
	if *naiveAck {
		cmd.Env = append(cmd.Env, "SPIKE_NAIVE_ACK=1")
	}
	if *skipDetach {
		cmd.Env = append(cmd.Env, "SPIKE_SKIP_DETACH=1")
	}
	if *initNoroot {
		cmd.Env = append(cmd.Env, "SPIKE_INIT_NOROOT=1")
	}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stdout
	cmd.ExtraFiles = []*os.File{child}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS | syscall.CLONE_NEWPID |
			syscall.CLONE_NEWUTS | syscall.CLONE_NEWIPC | syscall.CLONE_NEWNET,
		UidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: *idBase, Size: idRange}},
		GidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: *idBase, Size: idRange}},
		GidMappingsEnableSetgroups: false, // Go writes "deny" to /proc/<pid>/setgroups before gid_map
		// Become uid/gid 0 *inside* the namespace so init's execve keeps the
		// namespace capabilities. Groups cannot be cleared here: with
		// setgroups=deny, Go silently skips setgroups for an empty Groups list
		// (-groups=credential: host groups survive as 65534) and a non-empty
		// one fails with EPERM (-groups=credential-nonempty).
		Credential:  &syscall.Credential{Uid: 0, Gid: 0, Groups: credGroups, NoSetGroups: !strings.HasPrefix(*groups, "credential")},
		UseCgroupFD: true,
		CgroupFD:    cgfd,
		Pdeathsig:   syscall.SIGKILL,
	}
	if err := cmd.Start(); err != nil {
		child.Close()
		syscall.Close(ctl)
		res["outcome"], res["reason"] = "create_err", err.Error()
		return 0
	}
	child.Close()
	initPid := cmd.Process.Pid
	llog("init started, host pid %d", initPid)
	timer := time.AfterFunc(60*time.Second, func() {
		llog("TIMEOUT: killing init")
		cmd.Process.Kill()
	})
	defer timer.Stop()

	recv := func() (ctlMsg, error) {
		buf := make([]byte, 256*1024)
		for {
			n, _, _, _, err := syscall.Recvmsg(ctl, buf, nil, 0)
			if err == syscall.EINTR {
				continue
			}
			if err != nil {
				return ctlMsg{}, err
			}
			if n == 0 {
				return ctlMsg{}, io.EOF
			}
			var m ctlMsg
			err = json.Unmarshal(buf[:n], &m)
			return m, err
		}
	}

	finish := func() {
		syscall.Close(ctl)
		werr := cmd.Wait()
		llog("init exited: %v", errString(werr))
	}

	m, err := recv()
	if err != nil || m.Type != "ready" {
		res["outcome"] = "init_err"
		if err != nil {
			res["reason"] = "control: " + err.Error()
		} else {
			res["reason"] = m.Reason
		}
		finish()
		return 0
	}
	for _, l := range m.Info {
		llog("from init: %s", l)
	}
	hostView(initPid)

	// Workload stdio pipes, passed to init with the start message (SCM_RIGHTS).
	var pr [3][2]int
	for i := range pr {
		if err := syscall.Pipe2(pr[i][:], syscall.O_CLOEXEC); err != nil {
			res["outcome"], res["reason"] = "error", err.Error()
			finish()
			return 1
		}
	}
	argv := []string{"/opt/spike/spike1b", "check"}
	if *workload == "true" {
		argv = []string{"/usr/bin/true"}
	}
	baseline := readStatus("/proc/self/status")["Seccomp_filters"]
	llog("host baseline: launcher Seccomp=%s Seccomp_filters=%s", readStatus("/proc/self/status")["Seccomp"], baseline)
	spec := &helperSpec{Argv: argv, Env: []string{"PATH=/usr/bin:/bin", "HOME=/tmp", "LANG=C.UTF-8", "SPIKE_BASELINE_FILTERS=" + baseline, "SPIKE_ENV=" + *envKind, "SPIKE_WS_PHASE=" + *wsPhase},
		Fail: *fail, Order: *order, Capmode: *capmode, NoFdHygiene: *noFdHygiene, Dump: *dump, Clone3Errno: *clone3}
	b, _ := json.Marshal(ctlMsg{Type: "start", ExecID: "e1", Helper: spec})
	rights := syscall.UnixRights(pr[0][0], pr[1][1], pr[2][1])
	serr := syscall.Sendmsg(ctl, b, rights, nil, 0)
	// The host always closes its copies of the child ends after sendmsg.
	syscall.Close(pr[0][0])
	syscall.Close(pr[1][1])
	syscall.Close(pr[2][1])
	syscall.Close(pr[0][1]) // no stdin for the checker
	if serr != nil {
		res["outcome"], res["reason"] = "error", "sendmsg: "+serr.Error()
		finish()
		return 1
	}

	var wg sync.WaitGroup
	var outMu sync.Mutex
	pass, failc := 0, 0
	relay := func(fd int, tag string) {
		defer wg.Done()
		f := os.NewFile(uintptr(fd), tag)
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			outMu.Lock()
			fmt.Printf("[wl:%s] %s\n", tag, line)
			if strings.HasPrefix(line, "WORKLOAD_STARTED") {
				res["workload_started"] = "yes"
			}
			if strings.HasPrefix(line, "CHECK ") {
				if f := strings.Fields(line); len(f) >= 3 && f[2] == "PASS" {
					pass++
				} else {
					failc++
				}
			}
			outMu.Unlock()
		}
	}
	wg.Add(2)
	go relay(pr[1][0], "out")
	go relay(pr[2][0], "err")

	m, err = recv()
	switch {
	case err != nil:
		res["outcome"], res["reason"] = "control_lost", err.Error()
	case m.Type == "start_err":
		res["outcome"], res["reason"] = "start_err", m.Reason
	case m.Type == "start_ack":
		res["outcome"] = "start_ack"
		llog("start_ack pid=%d (sandbox pid)", m.Pid)
		m, err = recv()
		if err == nil && m.Type == "exit" {
			res["exit"] = fmt.Sprintf("code=%d,signal=%d", m.Code, m.Signal)
		} else {
			res["reason"] = fmt.Sprintf("waiting for exit: %v %+v", err, m)
		}
	default:
		res["outcome"], res["reason"] = "protocol", m.Type
	}
	finish()
	wg.Wait()
	res["pass"], res["fail"] = fmt.Sprint(pass), fmt.Sprint(failc)
	return 0
}

// hostView records what the host sees while the sandbox is running.
func hostView(pid int) {
	st := readStatus(fmt.Sprintf("/proc/%d/status", pid))
	llog("host view of init: Uid=[%s] Gid=[%s] Groups=[%s] CapEff=%s CapBnd=%s", st["Uid"], st["Gid"], st["Groups"], st["CapEff"], st["CapBnd"])
	for _, f := range []string{"uid_map", "gid_map", "setgroups"} {
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/%s", pid, f))
		llog("host view of init %s: %s", f, strings.Join(strings.Fields(string(b)), " "))
	}
	b, _ := os.ReadFile("/proc/self/mountinfo")
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, scratch) {
			n++
			llog("host mountinfo: %s", l)
		}
	}
	llog("host mountinfo entries under %s while the sandbox runs: %d", scratch, n)
}

func setupCgroup() (int, error) {
	if _, err := os.Stat(cgroupPath); err == nil {
		os.WriteFile(cgroupPath+"/cgroup.kill", []byte("1"), 0)
		time.Sleep(100 * time.Millisecond)
		if err := os.Remove(cgroupPath); err != nil {
			return -1, fmt.Errorf("stale cgroup: %w", err)
		}
	}
	if err := os.Mkdir(cgroupPath, 0o755); err != nil {
		return -1, err
	}
	for k, v := range map[string]string{"pids.max": "256", "memory.max": "536870912", "memory.swap.max": "0"} {
		if err := os.WriteFile(cgroupPath+"/"+k, []byte(v), 0); err != nil {
			llog("cgroup %s: %v", k, err)
		}
	}
	return syscall.Open(cgroupPath, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
}

func cleanupCgroup(fd int) {
	syscall.Close(fd)
	os.WriteFile(cgroupPath+"/cgroup.kill", []byte("1"), 0)
	for i := 0; i < 100; i++ {
		b, _ := os.ReadFile(cgroupPath + "/cgroup.events")
		if strings.Contains(string(b), "populated 0") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := os.Remove(cgroupPath); err != nil {
		llog("cgroup cleanup: %v", err)
	}
}

// hostPrepMounts: the "host-side mounts before clone" strategy (B2 option A).
func hostPrepMounts() error {
	if err := syscall.Mount(rootfsDir, rootfsDir, "", syscall.MS_BIND, ""); err != nil {
		return err
	}
	if err := syscall.Mount("", rootfsDir, "", syscall.MS_PRIVATE, ""); err != nil {
		return err
	}
	for _, b := range [][2]string{{"/usr", rootfsDir + "/usr"}, {"/etc", rootfsDir + "/etc"}, {inDir, rootfsDir + "/in"}} {
		if err := bindRO(b[0], b[1], false); err != nil {
			return err
		}
	}
	return syscall.Mount("", rootfsDir, "", syscall.MS_BIND|syscall.MS_REMOUNT|roFlags, "")
}

func hostPrepCleanup() {
	for _, p := range []string{rootfsDir + "/in", rootfsDir + "/etc", rootfsDir + "/usr", rootfsDir} {
		if err := syscall.Unmount(p, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) {
			llog("hostprep cleanup %s: %v", p, err)
		}
	}
}

const (
	gatewaySock = scratch + "/gw/gateway.sock"
	wsDir       = scratch + "/ws"
)

// startGateway listens on a Unix socket owned by the host UID that maps to
// sandbox uid 1000, mode 0600, and answers "pong".
func startGateway(hostUID int) (func(), error) {
	os.MkdirAll(scratch+"/gw", 0o755)
	os.Remove(gatewaySock)
	l, err := net.Listen("unix", gatewaySock)
	if err != nil {
		return nil, err
	}
	if err := os.Chown(gatewaySock, hostUID, hostUID); err != nil {
		l.Close()
		return nil, err
	}
	if err := os.Chmod(gatewaySock, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Write([]byte("pong\n"))
			c.Close()
		}
	}()
	return func() { l.Close(); os.Remove(gatewaySock) }, nil
}

// prepareWorkspace creates the session workspace once (owned by the host UID
// of sandbox uid 1000 for the range that created it, mode 0700).
func prepareWorkspace(hostUID int) error {
	if _, err := os.Stat(wsDir); err == nil {
		st, _ := os.Stat(wsDir)
		s := st.Sys().(*syscall.Stat_t)
		llog("workspace exists: owner host uid %d gid %d mode %v (this run maps sandbox 1000 -> host %d)", s.Uid, s.Gid, st.Mode(), hostUID)
		return nil
	}
	if err := os.Mkdir(wsDir, 0o700); err != nil {
		return err
	}
	llog("workspace created, chown to host uid %d", hostUID)
	return os.Chown(wsDir, hostUID, hostUID)
}
