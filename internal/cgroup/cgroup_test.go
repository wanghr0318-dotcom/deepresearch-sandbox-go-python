//go:build linux

package cgroup

import (
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
	// 兜底清理：用例自身会断言 Destroy 的结果，这里的失败不改变用例结论。
	defer func() { _ = g.Destroy() }()

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

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	// 顺序要紧：先杀进程并【收尸】，再删 cgroup。
	// 只 Kill 不 Wait 的话进程会变成僵尸，而僵尸仍计入 cgroup.procs，
	// rmdir 随即撞 EBUSY——这正是 Destroy 必须由调用方保证前置条件的原因。
	// Destroy 的错误也必须报出来：静默丢弃会让泄漏的 cgroup 伪装成测试通过。
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if err := g.Destroy(); err != nil {
			t.Errorf("清理 cgroup %s 失败: %v", g.Path(), err)
		}
	})

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

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 sleep: %v", err)
	}
	// 顺序要紧：先杀进程并【收尸】，再删 cgroup。
	// 只 Kill 不 Wait 的话进程会变成僵尸，而僵尸仍计入 cgroup.procs，
	// rmdir 随即撞 EBUSY——这正是 Destroy 必须由调用方保证前置条件的原因。
	// Destroy 的错误也必须报出来：静默丢弃会让泄漏的 cgroup 伪装成测试通过。
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if err := g.Destroy(); err != nil {
			t.Errorf("清理 cgroup %s 失败: %v", g.Path(), err)
		}
	})
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
		// 已经判定失败，以下只是尽力清理。
		_ = cmd.Process.Kill()
		_ = g.Destroy()
		t.Fatal("cgroup 内仍有进程时 Destroy 应当失败")
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	// 被 kill 的进程必然返回非 nil 的退出错误；这里只为收割。
	_ = cmd.Wait()
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
