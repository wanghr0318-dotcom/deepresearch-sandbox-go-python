//go:build linux

package runtime

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// TestNamespacesAreIsolated 验证 clone flags 确实生效：
// 子进程在自己的 PID namespace 里是 1 号，且改主机名不影响宿主。
func TestNamespacesAreIsolated(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	hostBefore, err := os.Hostname()
	if err != nil {
		t.Fatalf("读取宿主 hostname: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", "hostname sandbox-test && echo $$ && hostname")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: CloneFlags,
		Setpgid:    true,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("运行隔离进程: %v\n输出:\n%s", err, out)
	}

	lines := strings.Fields(string(out))
	if len(lines) < 2 {
		t.Fatalf("输出格式意外: %q", out)
	}
	if lines[0] != "1" {
		t.Fatalf("子进程 PID = %s, want 1（PID namespace 未生效）", lines[0])
	}
	if lines[1] != "sandbox-test" {
		t.Fatalf("沙箱内 hostname = %s, want sandbox-test", lines[1])
	}

	hostAfter, err := os.Hostname()
	if err != nil {
		t.Fatalf("重新读取宿主 hostname: %v", err)
	}
	if hostAfter != hostBefore {
		t.Fatalf("宿主 hostname 被改成了 %s（UTS namespace 未生效）", hostAfter)
	}
}

// TestReExecRunsInitBranch 验证 /proc/self/exe init 这条分流确实被走到。
func TestReExecRunsInitBranch(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	if os.Getenv(envInitProbe) == "1" {
		// 这是被 re-exec 出来的那一侧，打个标记就退出。
		os.Stdout.WriteString("init-branch-reached")
		os.Exit(0)
	}

	cmd := exec.Command("/proc/self/exe", "-test.run=TestReExecRunsInitBranch")
	cmd.Env = append(os.Environ(), envInitProbe+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("re-exec 自身: %v\n输出:\n%s", err, out)
	}
	if !strings.Contains(string(out), "init-branch-reached") {
		t.Fatalf("未走到 init 分支，输出: %q", out)
	}
}
