package faultinject

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

const childEnv = "FAULTINJECT_TEST_CHILD"

// TestMain 在设置了 childEnv 时作为 TestKillsOnNthHit 的子进程运行（不进入测试框架，因此不产生被
// 跳过的用例）：以 AGENTBOX_FAULT 武装后依次到达钩子点并报告进度。
func TestMain(m *testing.M) {
	if os.Getenv(childEnv) != "" {
		runChild()
		return
	}
	os.Exit(m.Run())
}

func runChild() {
	if err := enable(os.Getenv(Env)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	Point(VerdictAfter) // 其他点不计数
	fmt.Println("hit-1")
	Point(VerdictBefore)
	fmt.Println("hit-2")
	Point(VerdictBefore) // 第 2 次：SIGKILL
	fmt.Println("survived")
	os.Exit(0)
}

// TestKillsOnNthHit：武装后在目标点第 n 次到达时以 SIGKILL 杀死进程，之前的到达与其他点不触发。
func TestKillsOnNthHit(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SIGKILL 语义只在 Linux 上验证")
	}
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childEnv+"=1", Env+"="+VerdictBefore+":2")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if err == nil || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("子进程应被 SIGKILL：%v，stdout %q，stderr %q", err, stdout.String(), stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "hit-2") || strings.Contains(out, "survived") {
		t.Fatalf("子进程输出 %q：应在第 2 次到达目标点时被杀死", out)
	}
	if !strings.Contains(stderr.String(), "faultinject: SIGKILL at "+VerdictBefore+":2") {
		t.Fatalf("stderr 缺少记录：%q", stderr.String())
	}
}

// TestInertUnlessEnabled：未调用 Enable 时即使设置了环境变量也是空操作；Enable 在变量未设置时不武装。
func TestInertUnlessEnabled(t *testing.T) {
	t.Setenv(Env, VerdictBefore+":1")
	for range 3 {
		Point(VerdictBefore) // 未武装：不杀死测试进程
	}
	t.Setenv(Env, "")
	if err := Enable(); err != nil || armed.Load() {
		t.Fatalf("变量为空时 Enable 应保持未武装：%v armed=%v", err, armed.Load())
	}
}

// TestEnableRejectsMalformed：格式错误、次数非正或未知点名都拒绝且不武装。
func TestEnableRejectsMalformed(t *testing.T) {
	for _, spec := range []string{"verdict.before", "verdict.before:0", "verdict.before:x", "nowhere:1"} {
		if err := enable(spec); err == nil || armed.Load() {
			t.Fatalf("%q 应被拒绝且不武装：%v armed=%v", spec, err, armed.Load())
		}
	}
}
