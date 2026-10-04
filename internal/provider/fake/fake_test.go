package fake

import (
	"context"
	"io"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider/providertest"
)

// program：argv[0] 为 "echo" 时输出 hello 并以 3 退出；"sleep" 时一直运行到被终止。
func program(ctx context.Context, spec provider.ExecSpec, _ io.Reader, stdout, _ io.Writer) provider.ExitStatus {
	if spec.Argv[0] == "echo" {
		_, _ = io.WriteString(stdout, "hello\n")
		return provider.ExitStatus{Code: 3}
	}
	<-ctx.Done()
	return provider.ExitStatus{}
}

func spec(envID string) provider.EnvSpec {
	return provider.EnvSpec{EnvID: envID, InstallID: "install-test", Kind: provider.KindTask, UIDBase: 100000, UIDSize: 4096,
		Template: "sim", Limits: provider.Limits{MemoryMax: 64 << 20, PidsMax: 64}}
}

// TestContract 在 fake 上运行 Provider 契约一致性测试。
func TestContract(t *testing.T) {
	providertest.Run(t, providertest.Harness{
		New:        func(*testing.T) provider.Provider { return New(program) },
		Spec:       spec,
		Echo:       provider.ExecSpec{ExecID: "e", Argv: []string{"echo"}},
		EchoOutput: "hello",
		EchoCode:   3,
		Sleep:      provider.ExecSpec{ExecID: "s", Argv: []string{"sleep"}},
		Residue:    func(_ *testing.T, p provider.Provider, envID string) { p.(*Provider).InjectResidue(spec(envID)) },
		Foreign:    func(_ *testing.T, p provider.Provider, envID string) { p.(*Provider).InjectForeign(envID) },
		BlockStart: func(_ *testing.T, p provider.Provider, envID string) (<-chan struct{}, func()) {
			return p.(*Provider).BlockStart(envID)
		},
	})
}

// TestFailStopIsUnconfirmed：注入的 Stop 失败原样返回，环境仍不可启动新执行。
func TestFailStopIsUnconfirmed(t *testing.T) {
	p := New(program)
	if _, err := p.Create(context.Background(), spec("env-a")); err != nil {
		t.Fatal(err)
	}
	p.FailStop("env-a", provider.ErrStopUnconfirmed)
	if err := p.Stop(context.Background(), "env-a"); err != provider.ErrStopUnconfirmed {
		t.Fatalf("应返回注入的 ErrStopUnconfirmed，得到 %v", err)
	}
	if err := p.Destroy(context.Background(), "env-a"); err != provider.ErrNotStopped {
		t.Fatalf("未确认停止时 Destroy 应为 ErrNotStopped，得到 %v", err)
	}
}
