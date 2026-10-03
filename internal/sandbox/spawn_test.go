//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// TestMain 让这个测试二进制在被 Spawn 以 "init" re-exec 时，表现得
// 跟生产的 /proc/self/exe init 一样：直接跑 RunInit 并按它的错误退出，
// 而不是钻进 go test 自己的框架。
//
// 这是 TestSpawnStartsInitProcess 能验证真实 "拼装命令 -> re-exec ->
// 进入 namespace -> 跑 RunInit" 链路的前提：Spawn 内部硬编码 re-exec
// /proc/self/exe，从测试里调用时那就是这个测试二进制自己。
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == InitArg {
		if err := RunInit(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

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

// TestSpawnRejectsEmptyRoot 覆盖 Spawn 的参数校验：Root 为空必须报错。
// 校验发生在 fork/exec 之前，不涉及命名空间，不需要 root。
func TestSpawnRejectsEmptyRoot(t *testing.T) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("打开 %s: %v", os.DevNull, err)
	}
	defer func() {
		if err := devNull.Close(); err != nil {
			t.Errorf("关闭 devNull: %v", err)
		}
	}()

	if _, err := Spawn(SpawnConfig{Root: "", ControlFD: devNull}); err == nil {
		t.Fatal("Root 为空时 Spawn 应当返回错误，实际未报错")
	}
}

// TestSpawnRejectsNilControlFD 覆盖 Spawn 的参数校验：ControlFD 为 nil
// 必须报错。同样发生在 fork/exec 之前，不需要 root。
func TestSpawnRejectsNilControlFD(t *testing.T) {
	if _, err := Spawn(SpawnConfig{Root: t.TempDir(), ControlFD: nil}); err == nil {
		t.Fatal("ControlFD 为 nil 时 Spawn 应当返回错误，实际未报错")
	}
}

// TestSpawnStartsInitProcess 覆盖 Spawn 本身：参数拼装、ExtraFiles 挂载、
// SysProcAttr（Cloneflags/Setpgid/Pdeathsig）组合，以及 /proc/self/exe
// init 的 re-exec 调用方式。借助 TestMain 的拦截，子进程侧真实跑到了
// RunInit，并按其“init 尚未实现完整”的既定中间态非零退出。
func TestSpawnStartsInitProcess(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("打开 %s: %v", os.DevNull, err)
	}
	defer func() {
		if err := devNull.Close(); err != nil {
			t.Errorf("关闭 ControlFD: %v", err)
		}
	}()

	proc, err := Spawn(SpawnConfig{
		Root:      t.TempDir(),
		Hostname:  "spawn-test",
		ControlFD: devNull,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// Spawn 返回裸 *os.Process，调用方必须自己 Wait() 掉它，否则留
	// 僵尸进程。用 t.Cleanup 兜底，且失败要用 t.Errorf 报出来——
	// 静默丢弃的话，进程泄漏会伪装成测试通过（本项目在 cgroup 和
	// rootfs 两个包都踩过这个坑）。
	var state *os.ProcessState
	t.Cleanup(func() {
		if state != nil {
			return // 已经在测试主体里 Wait 过了
		}
		var waitErr error
		if state, waitErr = proc.Wait(); waitErr != nil {
			t.Errorf("兜底 Wait 子进程失败: %v", waitErr)
		}
	})

	state, err = proc.Wait()
	if err != nil {
		t.Fatalf("Wait 子进程失败: %v", err)
	}
	if state.ExitCode() == 0 {
		t.Fatalf("子进程 ExitCode = 0, want 非零（RunInit 应以“init 尚未实现完整”失败退出）")
	}
}

// TestMainDispatchesInitToRunInit 覆盖 cmd/agentbox/main.go 里的
// os.Args[1] == "init" 分流：构建真正的 agentbox 二进制，直接
// `agentbox init` 执行它，证明这条 argv 分流确实通到了
// runSandboxInit() -> sandbox.RunInit()。
//
// 不需要 root：RunInit 只有在设置了 AGENTBOX_HOSTNAME 时才会调用
// Sethostname（需要特权），这里不设置，所以能在普通用户下跑通到
// “init 尚未实现完整”这一步。
func TestMainDispatchesInitToRunInit(t *testing.T) {
	repoRoot := repoRootDir(t)

	binPath := filepath.Join(t.TempDir(), "agentbox")
	build := exec.Command(goBinary(), "build", "-o", binPath, "./cmd/agentbox")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("构建 agentbox 失败: %v\n输出:\n%s", err, out)
	}

	cmd := exec.Command(binPath, InitArg)
	cmd.Env = append(os.Environ(), envSandboxRoot+"="+t.TempDir())
	out, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("agentbox init 应以非零码退出，err=%v\n输出:\n%s", err, out)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("agentbox init 退出码 = %d, want 1\n输出:\n%s", exitErr.ExitCode(), out)
	}
	if !strings.Contains(string(out), "init 尚未实现完整") {
		t.Fatalf("stderr 未包含预期的“init 尚未实现完整”，实际输出:\n%s", out)
	}
}

// repoRootDir 返回仓库根目录。go test 把工作目录设为包所在目录
// （internal/sandbox），仓库根就是它的上两级。
func repoRootDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取工作目录: %v", err)
	}
	return filepath.Join(wd, "..", "..")
}

// goBinary 尽量找到 go 工具链：优先用 PATH 里的 go，非交互 shell
// （PATH 里没有 go）时退回本项目环境里的已知安装路径。
func goBinary() string {
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}
	return "/usr/local/go/bin/go"
}
