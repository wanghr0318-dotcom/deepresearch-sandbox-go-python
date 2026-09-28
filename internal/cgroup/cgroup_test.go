//go:build linux

package cgroup

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

const testRoot = "/sys/fs/cgroup/agentbox-test"

func TestGroupLifecycle(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "lifecycle")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	if _, err := os.Stat(g.Path()); err != nil {
		t.Fatalf("cgroup 目录未创建: %v", err)
	}

	if err := g.Apply(Limits{CPUMax: "50000 100000", MemoryMax: 64 << 20, PidsMax: 32}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertFile(t, filepath.Join(g.Path(), "cpu.max"), "50000 100000")
	assertFile(t, filepath.Join(g.Path(), "memory.max"), "67108864")
	assertFile(t, filepath.Join(g.Path(), "pids.max"), "32")
}

func TestAddProcAndProcs(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "addproc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	defer cmd.Process.Kill()

	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	pids, err := g.Procs()
	if err != nil {
		t.Fatalf("Procs: %v", err)
	}
	if len(pids) != 1 || pids[0] != cmd.Process.Pid {
		t.Fatalf("Procs() = %v, want [%d]", pids, cmd.Process.Pid)
	}
}

func TestFreezeAndThaw(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "freeze")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Destroy()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	defer cmd.Process.Kill()
	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	if err := g.Freeze(); err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	waitFrozen(t, g, true)

	if err := g.Thaw(); err != nil {
		t.Fatalf("Thaw: %v", err)
	}
	waitFrozen(t, g, false)
}

func TestDestroyFailsWhileProcsRemain(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "destroy-busy")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}

	if err := g.Destroy(); err == nil {
		cmd.Process.Kill()
		g.Destroy()
		t.Fatal("cgroup 内仍有进程时 Destroy 应当失败")
	}

	cmd.Process.Kill()
	cmd.Wait()
	// 内核回收是异步的，重试到成功或超时。
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := g.Destroy(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("进程退出后 Destroy 仍持续失败")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	if got := strings.TrimSpace(string(b)); got != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func waitFrozen(t *testing.T, g *Group, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(g.Path(), "cgroup.events"))
		if err != nil {
			t.Fatalf("读取 cgroup.events: %v", err)
		}
		frozen := strings.Contains(string(b), "frozen 1")
		if frozen == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 frozen=%v 超时，events:\n%s", want, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
