//go:build linux

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

// paths are overridable for tests (the helper itself always runs with the defaults).
var (
	execDir    = envOr("AGENTBOX_PODAGENT_EXEC_DIR", podapi.ExecDir)
	procRoot   = envOr("AGENTBOX_PODAGENT_PROC", "/proc")
	cgroupRoot = envOr("AGENTBOX_PODAGENT_CGROUP", "/sys/fs/cgroup")
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var validID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: agentbox-podagent init|exec|kill|status|freeze|thaw|procs|diag|shutdown")
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "init":
		return runInit()
	case "exec":
		return runExec(rest, stdin, stdout, stderr)
	case "kill":
		err = runKill(rest)
	case "status":
		err = runStatus(rest, stdout)
	case "freeze":
		err = runSignalAll(rest, syscall.SIGSTOP, true)
	case "thaw":
		err = runSignalAll(rest, syscall.SIGCONT, false)
	case "procs":
		err = runProcs(stdout)
	case "diag":
		err = runDiag(stdout)
	case "shutdown":
		// Ask PID 1 (init) to kill every process and exit, so the container ends now.
		err = syscall.Kill(1, syscall.SIGTERM)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(stderr, "agentbox-podagent:", err)
		return 1
	}
	return 0
}

// runInit is PID 1 of the sandbox container: it reaps orphaned children and, on SIGTERM (kubelet stopping
// the Pod, e.g. after activeDeadlineSeconds), SIGKILLs every other process in the PID namespace and exits.
func runInit() int {
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGCHLD)
	for s := range sigs {
		if s == syscall.SIGCHLD {
			reap()
			continue
		}
		_ = syscall.Kill(-1, syscall.SIGKILL) // PID 1 is never the target of kill(-1)
		reap()
		return 0
	}
	return 0
}

func reap() {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
	}
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(s string) error { *l = append(*l, s); return nil }

// runExec starts argv, acknowledges it on stdout and relays the workload's stdout after the ack.
func runExec(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(stderr)
	id := fs.String("id", "", "exec id")
	dir := fs.String("dir", "/", "working directory")
	nofile := fs.Uint64("nofile", 0, "RLIMIT_NOFILE")
	fsize := fs.Uint64("fsize", 0, "RLIMIT_FSIZE")
	var env listFlag
	fs.Var(&env, "env", "KEY=VALUE (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	argv := fs.Args()
	startErr := func(err error) int {
		fmt.Fprintf(stdout, "%s%s\n", podapi.AckStartErr, strings.ReplaceAll(err.Error(), "\n", " "))
		return podapi.StartErrCode
	}
	if !validID.MatchString(*id) {
		return startErr(fmt.Errorf("invalid exec id %q", *id))
	}
	if len(argv) == 0 {
		return startErr(errors.New("empty argv"))
	}
	path, err := lookPath(argv[0], env)
	if err != nil {
		return startErr(err)
	}
	for _, l := range []struct {
		res int
		v   uint64
	}{{syscall.RLIMIT_NOFILE, *nofile}, {syscall.RLIMIT_FSIZE, *fsize}} {
		if l.v == 0 {
			continue
		}
		if err := syscall.Setrlimit(l.res, &syscall.Rlimit{Cur: l.v, Max: l.v}); err != nil {
			return startErr(fmt.Errorf("setrlimit %d: %w", l.res, err))
		}
	}
	if err := os.MkdirAll(execDir, 0o700); err != nil {
		return startErr(err)
	}
	c := exec.Command(path, argv[1:]...)
	c.Args = argv
	c.Env = env
	c.Dir = *dir
	c.Stdin = stdin
	c.Stderr = stderr
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := c.StdoutPipe()
	if err != nil {
		return startErr(err)
	}
	if err := c.Start(); err != nil {
		return startErr(err)
	}
	pid := c.Process.Pid
	_ = os.WriteFile(filepath.Join(execDir, *id+".pid"), []byte(strconv.Itoa(pid)), 0o600)
	// Forward termination requests aimed at the helper to the workload's process group.
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for s := range sigs {
			_ = syscall.Kill(-pid, s.(syscall.Signal))
		}
	}()
	fmt.Fprintf(stdout, "%s%d\n", podapi.AckStarted, pid)
	_, _ = io.Copy(stdout, out) // ends when every holder of the pipe's write end has exited or closed it
	err = c.Wait()
	st := podapi.Status{}
	if ws, ok := c.ProcessState.Sys().(syscall.WaitStatus); ok {
		switch {
		case ws.Signaled():
			st.Signal = int(ws.Signal())
		default:
			st.Code = ws.ExitStatus()
		}
	} else if err != nil {
		st.Code = 1
	}
	b, _ := json.Marshal(st)
	_ = os.WriteFile(filepath.Join(execDir, *id+".status"), b, 0o600)
	_ = os.Remove(filepath.Join(execDir, *id+".pid"))
	if st.Signal != 0 {
		return 128 + st.Signal
	}
	return st.Code
}

// lookPath resolves name like execvp with PATH taken from the workload's env (default
// /usr/local/bin:/usr/bin:/bin), matching the local sandbox launcher.
func lookPath(name string, env []string) (string, error) {
	if strings.Contains(name, "/") {
		return name, nil
	}
	path := "/usr/local/bin:/usr/bin:/bin"
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			path = v
		}
	}
	for _, d := range filepath.SplitList(path) {
		if d == "" {
			d = "."
		}
		p := filepath.Join(d, name)
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s: not found in PATH: %w", name, syscall.ENOENT)
}

func idFlag(name string, args []string) (*flag.FlagSet, *string, *int) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "exec id")
	ms := fs.Int("grace-ms", 0, "grace period before SIGKILL")
	return fs, id, ms
}

// runKill sends SIGTERM to the exec's process group and SIGKILL after the grace period. An exec that has
// already exited (no pid file) is not an error.
func runKill(args []string) error {
	fs, id, grace := idFlag("kill", args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !validID.MatchString(*id) {
		return fmt.Errorf("invalid exec id %q", *id)
	}
	b, err := os.ReadFile(filepath.Join(execDir, *id+".pid"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		return fmt.Errorf("malformed pid file for %s", *id)
	}
	if *grace > 0 {
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		deadline := time.Now().Add(time.Duration(*grace) * time.Millisecond)
		for time.Now().Before(deadline) {
			if syscall.Kill(-pid, 0) != nil {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

func runStatus(args []string, stdout io.Writer) error {
	fs, id, _ := idFlag("status", args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !validID.MatchString(*id) {
		return fmt.Errorf("invalid exec id %q", *id)
	}
	b, err := os.ReadFile(filepath.Join(execDir, *id+".status"))
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(b, '\n'))
	return err
}

// others returns every pid in the PID namespace except PID 1 and this process.
func others() ([]int, error) {
	ents, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, err
	}
	self := os.Getpid()
	var pids []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == 1 || pid == self {
			continue
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids, nil
}

// procState returns the state letter from /proc/<pid>/stat ("" if the process is gone).
func procState(pid int) string {
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return ""
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return ""
	}
	return s[i+2 : i+3]
}

// runSignalAll sends sig to every other process. With confirm it waits until each one is stopped
// (state T/t) or gone, up to --timeout-ms (default 5000).
func runSignalAll(args []string, sig syscall.Signal, confirm bool) error {
	fs := flag.NewFlagSet("signal", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeout := fs.Int("timeout-ms", 5000, "confirmation deadline")
	if err := fs.Parse(args); err != nil {
		return err
	}
	pids, err := others()
	if err != nil {
		return err
	}
	for _, p := range pids {
		_ = syscall.Kill(p, sig)
	}
	if !confirm {
		return nil
	}
	deadline := time.Now().Add(time.Duration(*timeout) * time.Millisecond)
	for {
		pending := 0
		for _, p := range pids {
			if st := procState(p); st != "" && st != "T" && st != "t" && st != "Z" && st != "X" {
				pending++
			}
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d processes not stopped", pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runProcs(stdout io.Writer) error {
	pids, err := others()
	if err != nil {
		return err
	}
	if pids == nil {
		pids = []int{}
	}
	return json.NewEncoder(stdout).Encode(pids)
}

// runDiag reads the container cgroup's cpu.stat usage_usec and memory.events oom_kill.
func runDiag(stdout io.Writer) error {
	var d podapi.Diag
	d.CPUUsageUsec = readKey(filepath.Join(cgroupRoot, "cpu.stat"), "usage_usec")
	d.OOMKill = readKey(filepath.Join(cgroupRoot, "memory.events"), "oom_kill")
	return json.NewEncoder(stdout).Encode(d)
}

func readKey(path, key string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if ok && k == key {
			n, _ := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
			return n
		}
	}
	return 0
}
