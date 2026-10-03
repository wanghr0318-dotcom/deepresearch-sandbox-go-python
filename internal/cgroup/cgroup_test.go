//go:build linux

package cgroup

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

const testRoot = "/sys/fs/cgroup/agentbox-test"

// TestMain 在测试跑完后收拾 testRoot。
//
// 这些组建在真实 sysfs 上，不是 t.TempDir()——不收拾就会把
// /sys/fs/cgroup/agentbox-test 永久留在宿主机上。
//
// 注意：New() 对 /sys/fs/cgroup 根 cgroup.subtree_control 的委派改动
// 【不】在此撤销——那是产品运行本身就需要的状态，撤销反而会让后续
// 测试和真实沙箱都建不出限额。
func TestMain(m *testing.M) {
	code := m.Run()
	// 清理失败必须发声。静默忽略的话，泄漏的 cgroup 会一直累积在宿主机上，
	// 而测试始终显示全绿。
	if err := os.Remove(testRoot); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "警告：未能清理 %s: %v\n", testRoot, err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func TestNewRejectsInvalidName(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	// 这些名字一旦拼进路径就能逃出 cgroup 根，限额会静默落到别的组上。
	for _, name := range []string{"", ".", "..", "../escape", "a/b", "/abs"} {
		if _, err := New(testRoot, name); err == nil {
			t.Errorf("New(%q, %q) 应当报错，实际通过", testRoot, name)
		}
	}
}

func TestGroupLifecycle(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	g, err := New(testRoot, "lifecycle")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cleanupGroup(t, g)

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
	cleanupGroup(t, g)
	cmd := startSleeper(t)

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
	cleanupGroup(t, g)
	cmd := startSleeper(t)

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
	cleanupGroup(t, g)
	cmd := startSleeper(t)

	if err := g.AddProc(cmd.Process.Pid); err != nil {
		t.Fatalf("AddProc: %v", err)
	}
	if err := g.Destroy(); err == nil {
		t.Fatal("cgroup 内仍有进程时 Destroy 应当失败")
	}

	killAndReap(t, cmd)
	if err := destroyWithin(g, 3*time.Second); err != nil {
		t.Fatalf("进程退出并收割后 Destroy 仍持续失败: %v", err)
	}
}

// cleanupGroup 注册 cgroup 的删除，覆盖用例的所有退出路径。
// 删除失败报告为测试错误：静默丢弃会让泄漏的 cgroup 伪装成测试通过。
func cleanupGroup(t *testing.T, g *Group) {
	t.Helper()
	t.Cleanup(func() {
		if err := destroyWithin(g, 3*time.Second); err != nil {
			t.Errorf("清理 cgroup %s 失败: %v", g.Path(), err)
		}
	})
}

// startSleeper 启动一个长睡眠进程，并注册"杀死并收割"的清理。
//
// 必须在 cleanupGroup 之后调用：t.Cleanup 按注册的逆序执行，
// 进程先被收割，cgroup 后被删除。只杀不收割的进程会成为僵尸，
// 僵尸仍计入 cgroup.procs，删除随即失败。
func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	t.Cleanup(func() { killAndReap(t, cmd) })
	return cmd
}

// killAndReap 杀死并收割进程；已收割的进程直接返回。
func killAndReap(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.ProcessState != nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("杀死进程 %d: %v", cmd.Process.Pid, err)
	}
	// 被杀死的进程必然以非 nil 的退出错误结束；这里只为收割。
	_ = cmd.Wait()
}

// destroyWithin 重试 Destroy 直到成功或超时：进程退出后内核异步回收，
// 删除可能短暂失败。目录已不存在视为成功。
func destroyWithin(g *Group, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(g.Path()); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		err := g.Destroy()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
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
