// Package providertest 是 Provider 契约的一致性测试（契约第 5 节）：同一组行为测试运行在
// internal/provider/fake 与 internal/provider/local 上，使两侧对契约的理解由同一组测试固定。
package providertest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// Harness 由各实现提供：构造 provider、spec 与两个程序，以及注入残留、外来资源与"启动在途"的手段。
type Harness struct {
	New  func(t *testing.T) provider.Provider
	Spec func(envID string) provider.EnvSpec
	// Echo 输出 EchoOutput 并以 EchoCode 退出；Sleep 一直运行直到被终止。
	Echo       provider.ExecSpec
	EchoOutput string
	EchoCode   int
	Sleep      provider.ExecSpec
	// Residue 使 envID 成为属于本安装但不完整的环境（中断的创建）。
	Residue func(t *testing.T, p provider.Provider, envID string)
	// Foreign 使 envID 成为无法证明属于本安装的资源（例如无 owner.json 的目录）。
	Foreign func(t *testing.T, p provider.Provider, envID string)
	// BlockStart 使 envID 上的下一次 StartExec 在登记在途启动之后、发送 start 之前阻塞；
	// blocked 在阻塞发生时关闭，release 放行。
	BlockStart func(t *testing.T, p provider.Provider, envID string) (blocked <-chan struct{}, release func())
	// ExecSpec 返回 exec 环境的 spec（Mounts.In 为空，由 provider 建立 InDir）。为 nil 时（实现不支持 exec 环境）
	// 不运行 exec 环境的用例。
	ExecSpec func(envID string) provider.EnvSpec
}

const wait = 10 * time.Second

// Run 运行全部契约行为测试。
func Run(t *testing.T, h Harness) {
	t.Run("Create 后 List 报告完整", func(t *testing.T) {
		p := h.New(t)
		create(t, p, h.Spec("env-a"))
		if info := find(t, p, "env-a"); info == nil || !info.Complete {
			t.Fatalf("List 应报告完整环境：%+v", info)
		}
	})
	t.Run("同 spec 重复 Create 返回现状", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		first := create(t, p, h.Spec("env-a"))
		again, err := p.Create(ctx, h.Spec("env-a"))
		if err != nil || again.EnvID != first.EnvID || !again.Complete {
			t.Fatalf("应返回现状：%+v %v", again, err)
		}
	})
	t.Run("同 env_id 不同 spec 为冲突", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		create(t, p, h.Spec("env-a"))
		other := h.Spec("env-a")
		other.Limits.PidsMax++
		if _, err := p.Create(ctx, other); !errors.Is(err, provider.ErrConflict) {
			t.Fatalf("应为 ErrConflict，得到 %v", err)
		}
	})
	t.Run("残留为 ErrIncomplete，停止并销毁后可重建", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		h.Residue(t, p, "env-r")
		if _, err := p.Create(ctx, h.Spec("env-r")); !errors.Is(err, provider.ErrIncomplete) {
			t.Fatalf("残留应为 ErrIncomplete，得到 %v", err)
		}
		if info := find(t, p, "env-r"); info == nil || info.Complete {
			t.Fatalf("List 应报告不完整：%+v", info)
		}
		stopDestroy(t, p, "env-r")
		create(t, p, h.Spec("env-r"))
	})
	t.Run("外来资源不认领", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		h.Foreign(t, p, "env-f")
		if _, err := p.Create(ctx, h.Spec("env-f")); !errors.Is(err, provider.ErrForeign) {
			t.Fatalf("应为 ErrForeign，得到 %v", err)
		}
		if info := find(t, p, "env-f"); info != nil {
			t.Fatalf("List 不应报告外来资源：%+v", info)
		}
		owner, ok := scanOwner(t, p, "env-f")
		if !ok || (owner != provider.Foreign && owner != provider.Unknown) {
			t.Fatalf("Scan 应报告 Foreign 或 Unknown，得到 %v（存在 %v）", owner, ok)
		}
		_ = p.Destroy(ctx, "env-f") // 无论返回什么，都不能删除外来资源
		if _, ok := scanOwner(t, p, "env-f"); !ok {
			t.Fatal("Destroy 不应删除外来资源")
		}
	})
	t.Run("StartExec 回显输出与退出码", func(t *testing.T) {
		p := h.New(t)
		create(t, p, h.Spec("env-a"))
		out, status := run(t, p, "env-a", h.Echo)
		if strings.TrimSpace(out) != h.EchoOutput || status.Code != h.EchoCode || status.Signal != 0 {
			t.Fatalf("得到 (%q, %+v)，期望 (%q, %d)", out, status, h.EchoOutput, h.EchoCode)
		}
	})
	t.Run("未知环境 StartExec 为 ErrNotFound", func(t *testing.T) {
		p := h.New(t)
		if _, err := p.StartExec(context.Background(), "missing", h.Echo); !errors.Is(err, provider.ErrNotFound) {
			t.Fatalf("应为 ErrNotFound，得到 %v", err)
		}
	})
	t.Run("Stop 终止执行并关闭闸门", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		create(t, p, h.Spec("env-a"))
		handle, err := p.StartExec(ctx, "env-a", h.Sleep)
		if err != nil {
			t.Fatal(err)
		}
		drain(handle)
		if err := p.Stop(ctx, "env-a"); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		waitDone(t, handle)
		if info := find(t, p, "env-a"); info == nil || info.Running {
			t.Fatalf("停止后不应有运行中的进程：%+v", info)
		}
		if _, err := p.StartExec(ctx, "env-a", h.Echo); !errors.Is(err, provider.ErrStopping) {
			t.Fatalf("停止后 StartExec 应为 ErrStopping，得到 %v", err)
		}
	})
	t.Run("start 在途时 stop", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		create(t, p, h.Spec("env-a"))
		blocked, release := h.BlockStart(t, p, "env-a")
		result := make(chan error, 1)
		go func() {
			handle, err := p.StartExec(ctx, "env-a", h.Sleep)
			if err == nil {
				drain(handle)
				_, err = handle.Wait() // 若启动成功，进程必须已被 Stop 杀死
				if err == nil {
					err = errors.New("在途启动得到了存活进程")
				}
			}
			result <- err
		}()
		select {
		case <-blocked:
		case <-time.After(wait):
			t.Fatal("StartExec 没有进入阻塞点")
		}
		if err := p.Stop(ctx, "env-a"); err != nil {
			t.Fatalf("在途启动不应阻止 Stop：%v", err)
		}
		release()
		select {
		case err := <-result:
			var se *provider.StartError
			if !errors.Is(err, provider.ErrControlLost) && !errors.Is(err, provider.ErrStopping) && !errors.As(err, &se) {
				t.Fatalf("在途启动应以 ErrControlLost、ErrStopping 或 StartError 结束，得到 %v", err)
			}
		case <-time.After(wait):
			t.Fatal("在途启动没有返回")
		}
		if info := find(t, p, "env-a"); info == nil || info.Running {
			t.Fatalf("不应有存活进程：%+v", info)
		}
		if _, err := p.StartExec(ctx, "env-a", h.Echo); !errors.Is(err, provider.ErrStopping) {
			t.Fatalf("之后的 StartExec 应为 ErrStopping，得到 %v", err)
		}
	})
	t.Run("Destroy 要求先停止，完成后不再报告", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		create(t, p, h.Spec("env-a"))
		handle, err := p.StartExec(ctx, "env-a", h.Sleep)
		if err != nil {
			t.Fatal(err)
		}
		drain(handle)
		if err := p.Destroy(ctx, "env-a"); !errors.Is(err, provider.ErrNotStopped) {
			t.Fatalf("未停止时 Destroy 应为 ErrNotStopped，得到 %v", err)
		}
		stopDestroy(t, p, "env-a")
		waitDone(t, handle)
		if info := find(t, p, "env-a"); info != nil {
			t.Fatalf("Destroy 后 List 不应报告：%+v", info)
		}
		if _, ok := scanOwner(t, p, "env-a"); ok {
			t.Fatal("Destroy 后 Scan 不应报告")
		}
		if _, err := p.ResourceDiag(ctx, "env-a"); !errors.Is(err, provider.ErrNotFound) {
			t.Fatalf("Destroy 后 ResourceDiag 应为 ErrNotFound，得到 %v", err)
		}
	})
	t.Run("OpenOutputs 对编排环境报错、未知环境为 ErrNotFound", func(t *testing.T) {
		p, ctx := h.New(t), context.Background()
		create(t, p, h.Spec("env-a"))
		if err := p.Stop(ctx, "env-a"); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		files, _, err := p.OpenOutputs(ctx, "env-a", provider.MaxOutputFiles)
		if err == nil || errors.Is(err, provider.ErrNotStopped) || len(files) != 0 {
			t.Fatalf("编排环境的 OpenOutputs 应报错（非 ErrNotStopped），得到 %d 个文件、%v", len(files), err)
		}
		if _, _, err := p.OpenOutputs(ctx, "missing", provider.MaxOutputFiles); !errors.Is(err, provider.ErrNotFound) {
			t.Fatalf("未知环境的 OpenOutputs 应为 ErrNotFound，得到 %v", err)
		}
		stopDestroy(t, p, "env-a")
	})
	if h.ExecSpec != nil {
		t.Run("exec 环境：InDir 幂等，OpenOutputs 要求先停止", func(t *testing.T) {
			p, ctx := h.New(t), context.Background()
			info := create(t, p, h.ExecSpec("env-x"))
			if info.InDir == "" || info.Kind != provider.KindExec {
				t.Fatalf("exec 环境应返回 InDir：%+v", info)
			}
			if again, err := p.Create(ctx, h.ExecSpec("env-x")); err != nil || again.InDir != info.InDir {
				t.Fatalf("幂等 Create 应返回同一 InDir：%+v %v（原 %q）", again, err, info.InDir)
			}
			if _, _, err := p.OpenOutputs(ctx, "env-x", provider.MaxOutputFiles); !errors.Is(err, provider.ErrNotStopped) {
				t.Fatalf("停止前 OpenOutputs 应为 ErrNotStopped，得到 %v", err)
			}
			if err := p.Stop(ctx, "env-x"); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			files, skipped, err := p.OpenOutputs(ctx, "env-x", provider.MaxOutputFiles)
			if err != nil || len(files) != 0 || len(skipped) != 0 {
				t.Fatalf("停止后空 /out 的 OpenOutputs = %d、%v、%v", len(files), skipped, err)
			}
			stopDestroy(t, p, "env-x")
		})
	}
}

func create(t *testing.T, p provider.Provider, s provider.EnvSpec) provider.EnvInfo {
	t.Helper()
	info, err := p.Create(context.Background(), s)
	if err != nil || !info.Complete {
		t.Fatalf("Create(%s): %+v %v", s.EnvID, info, err)
	}
	return info
}

func stopDestroy(t *testing.T, p provider.Provider, envID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := p.Stop(ctx, envID); err != nil {
		t.Fatalf("Stop(%s): %v", envID, err)
	}
	if err := p.Destroy(ctx, envID); err != nil {
		t.Fatalf("Destroy(%s): %v", envID, err)
	}
}

func find(t *testing.T, p provider.Provider, envID string) *provider.EnvInfo {
	t.Helper()
	infos, err := p.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range infos {
		if infos[i].EnvID == envID {
			return &infos[i]
		}
	}
	return nil
}

func scanOwner(t *testing.T, p provider.Provider, envID string) (provider.Owner, bool) {
	t.Helper()
	r, err := p.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range r.Items {
		if it.EnvID == envID || strings.Contains(it.Path, envID) {
			return it.Owner, true
		}
	}
	return 0, false
}

// run 启动程序、关闭 stdin、读完 stdout 并等待退出。
func run(t *testing.T, p provider.Provider, envID string, spec provider.ExecSpec) (string, provider.ExitStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	handle, err := p.StartExec(ctx, envID, spec)
	if err != nil {
		t.Fatalf("StartExec: %v", err)
	}
	_ = handle.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, handle.Stderr()) }()
	out, err := io.ReadAll(handle.Stdout())
	if err != nil {
		t.Fatalf("读取 stdout: %v", err)
	}
	status, err := handle.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	return string(out), status
}

// drain 在后台读尽输出，避免程序因管道满而阻塞。
func drain(h provider.ExecHandle) {
	go func() { _, _ = io.Copy(io.Discard, h.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, h.Stderr()) }()
}

func waitDone(t *testing.T, h provider.ExecHandle) {
	t.Helper()
	done := make(chan struct{})
	go func() { _, _ = h.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(wait):
		t.Fatal("进程没有结束")
	}
}
