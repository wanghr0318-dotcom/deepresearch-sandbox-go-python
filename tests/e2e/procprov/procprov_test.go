//go:build linux

package procprov

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider/providertest"
)

const installID = "install-test"

func spec(envID string) provider.EnvSpec {
	return provider.EnvSpec{EnvID: envID, InstallID: installID, Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096,
		Template: "sim", Limits: provider.Limits{MemoryMax: 64 << 20, PidsMax: 64}}
}

func newProvider(t *testing.T, dir string) *Provider {
	t.Helper()
	p, err := New(Options{DataDir: dir, InstallID: installID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { killAll(dir) })
	return p
}

// killAll 杀死数据目录下仍存活的进程（测试失败时不留下进程）。
func killAll(dir string) {
	pids, _ := LiveUnder(dir)
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// TestContract 在进程型 provider 上运行 Provider 契约一致性测试。残留由另一个 provider 实例（"上一个
// server 进程"）创建：磁盘上属于本安装，但不是本进程创建的。
func TestContract(t *testing.T) {
	providertest.Run(t, providertest.Harness{
		New:        func(t *testing.T) provider.Provider { return newProvider(t, t.TempDir()) },
		Spec:       spec,
		Echo:       provider.ExecSpec{ExecID: "e", Argv: []string{"sh", "-c", "echo hello; exit 3"}},
		EchoOutput: "hello",
		EchoCode:   3,
		Sleep:      provider.ExecSpec{ExecID: "s", Argv: []string{"sleep", "1000"}},
		Residue: func(t *testing.T, p provider.Provider, envID string) {
			prev, err := New(p.(*Provider).opt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prev.Create(context.Background(), spec(envID)); err != nil {
				t.Fatal(err)
			}
		},
		Foreign: func(t *testing.T, p provider.Provider, envID string) {
			if err := os.MkdirAll(filepath.Join(p.(*Provider).envsDir, envID), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		BlockStart: func(_ *testing.T, p provider.Provider, envID string) (<-chan struct{}, func()) {
			return p.(*Provider).BlockStart(envID)
		},
	})
}

// TestSurvivesServerDeathAndRestartStops：创建它的 provider 实例消失（server 被 SIGKILL）后，执行树仍然存活
// （keeper 保持 Worker 的管道），连 setsid 脱离进程组的后代也带着标记；新实例把环境报告为本安装的残留，
// Stop 杀死全部进程（含脱离进程组的后代）并确认，Destroy 之后不再报告。
func TestSurvivesServerDeathAndRestartStops(t *testing.T) {
	dir := t.TempDir()
	first := newProvider(t, dir)
	ctx := context.Background()
	if _, err := first.Create(ctx, spec("env-a")); err != nil {
		t.Fatal(err)
	}
	// Worker 读 stdin 直到 EOF；另起一个 setsid 的后代。
	h, err := first.StartExec(ctx, "env-a", provider.ExecSpec{ExecID: "x", Argv: []string{"sh", "-c", "setsid sleep 1000 & cat >/dev/null"}})
	if err != nil {
		t.Fatal(err)
	}
	envDir := filepath.Join(dir, "envs", "env-a")
	waitFor(t, "3 个带标记的存活进程（keeper、Worker、setsid 后代）", func() bool { pids, _ := Marked(envDir); return len(pids) >= 3 })
	// "server 死亡"：宿主端的管道句柄全部关闭，但不调用 Stop、Terminate 或 Stdin().Close()。
	_ = h.Stdout().Close()
	_ = h.Stderr().Close()

	second := newProvider(t, dir)
	if _, err := second.StartExec(ctx, "env-a", provider.ExecSpec{ExecID: "y", Argv: []string{"true"}}); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("上一进程的环境 StartExec 应为 ErrNotFound，得到 %v", err)
	}
	infos, err := second.List(ctx)
	if err != nil || len(infos) != 1 || infos[0].Complete || !infos[0].Running {
		t.Fatalf("新实例应报告不完整但仍在运行的环境：%+v %v", infos, err)
	}
	if err := second.Destroy(ctx, "env-a"); !errors.Is(err, provider.ErrNotStopped) {
		t.Fatalf("存活进程时 Destroy 应为 ErrNotStopped，得到 %v", err)
	}
	r, err := second.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var procs int
	for _, it := range r.Items {
		if it.EnvID != "env-a" || it.Owner != provider.OwnedPartial {
			t.Fatalf("扫描项 %+v 应为本安装的残留", it)
		}
		if it.Layer == "process" {
			procs++
		}
	}
	if procs < 3 {
		t.Fatalf("扫描应报告存活进程：%+v", r.Items)
	}
	if err := second.Stop(ctx, "env-a"); err != nil {
		t.Fatal(err)
	}
	if pids, _ := LiveUnder(dir); len(pids) != 0 {
		t.Fatalf("Stop 之后仍有存活进程 %v", pids)
	}
	if _, err := h.Wait(); err != nil { // 第一个实例的句柄同样看到 Worker 结束
		t.Fatal(err)
	}
	if err := second.Destroy(ctx, "env-a"); err != nil {
		t.Fatal(err)
	}
	if infos, _ := second.List(ctx); len(infos) != 0 {
		t.Fatalf("Destroy 后仍报告 %+v", infos)
	}
	j, err := ReadJournal(dir)
	if err != nil || len(j) < 2 || j[0].Event != "start" || j[len(j)-1].Event != "stopped" {
		t.Fatalf("执行日志 %+v %v", j, err)
	}
}

// TestPidReuseGuard：记录中启动时间不一致的 pid（已被复用）不算该环境的进程。
func TestPidReuseGuard(t *testing.T) {
	dir := t.TempDir()
	p := newProvider(t, dir)
	if _, err := p.Create(context.Background(), spec("env-a")); err != nil {
		t.Fatal(err)
	}
	start, ok := startTime(os.Getpid())
	if !ok {
		t.Fatal("读取启动时间失败")
	}
	// 本测试进程没有标记；以错误的启动时间记录它，模拟 pid 被复用。
	rec := []byte(`{"exec_id":"old","pgid":0,"procs":[{"pid":` + itoa(os.Getpid()) + `,"start":` + utoa(start+1) + `}]}`)
	if err := os.WriteFile(filepath.Join(dir, "envs", "env-a", "procs", "old.json"), rec, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Stop(ctx, "env-a"); err != nil {
		t.Fatalf("pid 已被复用的记录不应阻止 Stop：%v", err)
	}
	if err := p.Destroy(ctx, "env-a"); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int) string    { return strconv.Itoa(n) }
func utoa(n uint64) string { return strconv.FormatUint(n, 10) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时：%s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
