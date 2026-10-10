//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/k8s/podapi"
)

func withExecDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	old := execDir
	execDir = d
	t.Cleanup(func() { execDir = old })
	return d
}

func TestExecAcksBeforeOutputAndRecordsExitCode(t *testing.T) {
	d := withExecDir(t)
	var out, errb bytes.Buffer
	code := run([]string{"exec", "--id", "e1", "--env", "PATH=/usr/bin:/bin", "--env", "X=42", "--",
		"sh", "-c", `echo "out $X"; echo err >&2; exit 3`}, nil, &out, &errb)
	if code != 3 {
		t.Fatalf("exit code = %d, want 3 (stderr %q)", code, errb.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], podapi.AckStarted) || lines[1] != "out 42" {
		t.Fatalf("stdout = %q", out.String())
	}
	if started, pid, _, err := podapi.ParseAck(lines[0]); !started || pid <= 0 || err != nil {
		t.Fatalf("ParseAck(%q) = %v %d %v", lines[0], started, pid, err)
	}
	if errb.String() != "err\n" {
		t.Fatalf("stderr = %q", errb.String())
	}
	var st podapi.Status
	b, err := os.ReadFile(filepath.Join(d, "e1.status"))
	if err != nil || json.Unmarshal(b, &st) != nil || st != (podapi.Status{Code: 3}) {
		t.Fatalf("status file = %s %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(d, "e1.pid")); !os.IsNotExist(err) {
		t.Fatalf("pid file should be removed after exit: %v", err)
	}
	var sb bytes.Buffer
	if run([]string{"status", "--id", "e1"}, nil, &sb, &errb) != 0 || strings.TrimSpace(sb.String()) != string(b) {
		t.Fatalf("status output %q", sb.String())
	}
}

func TestExecStartErrorForMissingProgram(t *testing.T) {
	withExecDir(t)
	var out bytes.Buffer
	code := run([]string{"exec", "--id", "e2", "--", "definitely-not-a-program"}, nil, &out, &bytes.Buffer{})
	if code != podapi.StartErrCode {
		t.Fatalf("code = %d", code)
	}
	started, _, reason, err := podapi.ParseAck(strings.TrimSpace(out.String()))
	if started || err != nil || !strings.Contains(reason, "not found") {
		t.Fatalf("ack = %q (%v %q %v)", out.String(), started, reason, err)
	}
}

func TestExecRejectsBadID(t *testing.T) {
	withExecDir(t)
	var out bytes.Buffer
	if code := run([]string{"exec", "--id", "../x", "--", "true"}, nil, &out, &bytes.Buffer{}); code != podapi.StartErrCode {
		t.Fatalf("code = %d, out %q", code, out.String())
	}
}

func TestExecAppliesNoFile(t *testing.T) {
	withExecDir(t)
	// Run in a child process: setrlimit would otherwise lower the test binary's own limit.
	if os.Getenv("PODAGENT_CHILD") == "1" {
		os.Exit(run([]string{"exec", "--id", "e3", "--nofile", "77", "--", "sh", "-c", "ulimit -n"}, nil, os.Stdout, os.Stderr))
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestExecAppliesNoFile$")
	cmd.Env = append(os.Environ(), "PODAGENT_CHILD=1", "AGENTBOX_PODAGENT_EXEC_DIR="+t.TempDir())
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("child: %v", err)
	}
	if !strings.Contains(string(out), "\n77\n") {
		t.Fatalf("output %q", out)
	}
}

func TestKillTerminatesProcessGroupAndStatusRecordsSignal(t *testing.T) {
	d := withExecDir(t)
	done := make(chan int, 1)
	var out bytes.Buffer
	go func() {
		done <- run([]string{"exec", "--id", "k1", "--", "sh", "-c", "sleep 100 & wait"}, nil, &out, &bytes.Buffer{})
	}()
	waitFile(t, filepath.Join(d, "k1.pid"))
	if code := run([]string{"kill", "--id", "k1", "--grace-ms", "200"}, nil, &bytes.Buffer{}, &bytes.Buffer{}); code != 0 {
		t.Fatalf("kill = %d", code)
	}
	select {
	case code := <-done:
		if code != 128+int(syscall.SIGTERM) {
			t.Fatalf("exit = %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("exec did not return after kill")
	}
	b, _ := os.ReadFile(filepath.Join(d, "k1.status"))
	var st podapi.Status
	if json.Unmarshal(b, &st) != nil || st.Signal != int(syscall.SIGTERM) {
		t.Fatalf("status %s", b)
	}
	// Killing an exec that already exited is not an error.
	if code := run([]string{"kill", "--id", "k1"}, nil, &bytes.Buffer{}, &bytes.Buffer{}); code != 0 {
		t.Fatalf("second kill = %d", code)
	}
}

func TestProcStateParsesStatLine(t *testing.T) {
	root := t.TempDir()
	old := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = old })
	_ = os.MkdirAll(filepath.Join(root, "42"), 0o755)
	_ = os.WriteFile(filepath.Join(root, "42", "stat"), []byte("42 (a) b) T 1 2 3"), 0o644)
	if s := procState(42); s != "T" {
		t.Fatalf("state = %q", s)
	}
	if s := procState(43); s != "" {
		t.Fatalf("missing = %q", s)
	}
}

func TestDiagReadsCgroupFiles(t *testing.T) {
	root := t.TempDir()
	old := cgroupRoot
	cgroupRoot = root
	t.Cleanup(func() { cgroupRoot = old })
	_ = os.WriteFile(filepath.Join(root, "cpu.stat"), []byte("usage_usec 1234\nuser_usec 1\n"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "memory.events"), []byte("low 0\noom 2\noom_kill 1\n"), 0o644)
	var out bytes.Buffer
	if code := run([]string{"diag"}, nil, &out, &bytes.Buffer{}); code != 0 {
		t.Fatal(code)
	}
	var d podapi.Diag
	if json.Unmarshal(out.Bytes(), &d) != nil || d != (podapi.Diag{CPUUsageUsec: 1234, OOMKill: 1}) {
		t.Fatalf("diag %s", out.String())
	}
}

func waitFile(t *testing.T, p string) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if _, err := os.Stat(p); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not appear", p)
}

// pgrp reads the process group from /proc/<pid>/stat (0 if unreadable).
func pgrp(pid int) int {
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	if len(f) < 3 {
		return 0
	}
	g, _ := strconv.Atoi(f[2])
	return g
}

// TestFreezeCatchesForkingProcesses: a shell forking continuously must end up entirely stopped — children
// forked between a /proc listing and the parent's SIGSTOP are caught by the re-listing passes.
func TestFreezeCatchesForkingProcesses(t *testing.T) {
	cmd := exec.Command("sh", "-c", "while :; do sleep 2 & sleep 0.005; done")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pg := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-pg, syscall.SIGKILL)
		_ = syscall.Kill(-pg, syscall.SIGCONT)
		_ = cmd.Wait()
	})
	old := inScope
	inScope = func(pid int) bool { return pgrp(pid) == pg }
	t.Cleanup(func() { inScope = old })
	time.Sleep(200 * time.Millisecond) // let it fork a few dozen children
	if code := run([]string{"freeze", "--timeout-ms", "5000"}, nil, &bytes.Buffer{}, os.Stderr); code != 0 {
		t.Fatalf("freeze = %d", code)
	}
	check := func() (int, int) {
		pids, err := others()
		if err != nil {
			t.Fatal(err)
		}
		running := 0
		for _, p := range pids {
			if st := procState(p); st != "" && st != "T" && st != "t" && st != "Z" {
				running++
			}
		}
		return len(pids), running
	}
	n1, r1 := check()
	time.Sleep(100 * time.Millisecond)
	n2, r2 := check()
	if r1 != 0 || r2 != 0 || n2 > n1 || n1 == 0 {
		t.Fatalf("after freeze: %d procs (%d running), 100 ms later %d procs (%d running)", n1, r1, n2, r2)
	}
	if code := run([]string{"thaw"}, nil, &bytes.Buffer{}, os.Stderr); code != 0 {
		t.Fatalf("thaw = %d", code)
	}
	if _, r := check(); r == 0 {
		t.Fatal("thaw did not resume the processes")
	}
}
