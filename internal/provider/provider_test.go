package provider

import (
	"errors"
	"testing"
)

func spec() EnvSpec {
	return EnvSpec{EnvID: "env-1", InstallID: "install-1", Kind: KindTask, UIDBase: 100000, UIDSize: 4096, Template: "sim",
		Limits: Limits{MemoryMax: 256 << 20, PidsMax: 128}, Mounts: Mounts{Workspace: "/w", GatewaySocket: "/w/gw.sock"}}
}

// TestSpecHash：同一 spec 的哈希稳定，任一字段变化都会改变哈希。
func TestSpecHash(t *testing.T) {
	base := SpecHash(spec())
	if SpecHash(spec()) != base || len(base) != 64 {
		t.Fatalf("哈希应稳定且为 64 个十六进制字符：%q", base)
	}
	for name, mutate := range map[string]func(*EnvSpec){
		"env_id":   func(s *EnvSpec) { s.EnvID = "env-2" },
		"uid":      func(s *EnvSpec) { s.UIDBase++ },
		"memory":   func(s *EnvSpec) { s.Limits.MemoryMax++ },
		"socket":   func(s *EnvSpec) { s.Mounts.GatewaySocket = "" },
		"template": func(s *EnvSpec) { s.Template = "other" },
	} {
		s := spec()
		mutate(&s)
		if SpecHash(s) == base {
			t.Errorf("改变 %s 后哈希应变化", name)
		}
	}
}

// TestValidate：必填字段与按 Kind 的挂载组合（规格 §4.5）。
func TestValidate(t *testing.T) {
	if err := spec().Validate(); err != nil {
		t.Fatalf("合法 spec 被拒：%v", err)
	}
	exec := spec()
	exec.Kind, exec.Mounts = KindExec, Mounts{In: "/in", OutBytes: 64 << 20}
	if err := exec.Validate(); err != nil {
		t.Fatalf("合法 exec spec 被拒：%v", err)
	}
	for name, mutate := range map[string]func(*EnvSpec){
		"exec 带 Gateway socket": func(s *EnvSpec) { s.Kind, s.Mounts = KindExec, Mounts{GatewaySocket: "/gw"} },
		"编排环境带 /in":             func(s *EnvSpec) { s.Mounts.In = "/in" },
		"编排环境带 /out":            func(s *EnvSpec) { s.Mounts.OutBytes = 1 },
		"缺少 env_id":             func(s *EnvSpec) { s.EnvID = "" },
		"UID 范围为空":              func(s *EnvSpec) { s.UIDSize = 0 },
		"未知类型":                  func(s *EnvSpec) { s.Kind = "vm" },
	} {
		s := spec()
		mutate(&s)
		if s.Validate() == nil {
			t.Errorf("%s 应被拒绝", name)
		}
	}
}

func TestStartErrorIsStartFailed(t *testing.T) {
	var err error = &StartError{Reason: "setresuid"}
	if !errors.Is(err, ErrStartFailed) {
		t.Fatal("StartError 应满足 errors.Is(ErrStartFailed)")
	}
}
