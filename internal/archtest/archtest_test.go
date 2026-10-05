// Package archtest 以测试固定包之间的依赖规则（代码组织设计 §3.2）。
//
// 规则 1–4 的包依赖部分按传递依赖检查（比直接依赖边更严）；规则 2 的同包 Decide/actor 区分与规则 5 由代码评审保证。
package archtest

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const module = "github.com/wanghr0318-dotcom/go-agentbox"

// deps 返回包的全部传递依赖（含自身），以 GOOS=linux 计算。
func deps(t *testing.T, pkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", module+"/"+pkg)
	cmd.Env = append(cmd.Environ(), "GOOS=linux")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	return strings.Fields(string(out))
}

// TestConsumersDoNotDependOnPostgres：消费者包与引导决策不依赖 PostgreSQL 实现或数据库驱动；
// 实现（internal/persistence/postgres）反向依赖消费者的契约（设计 §1.1）。
func TestConsumersDoNotDependOnPostgres(t *testing.T) {
	forbidden := []string{module + "/internal/persistence/postgres", "github.com/jackc/pgx"}
	for _, pkg := range []string{
		"internal/api", "internal/task", "internal/runner", "internal/resource",
		"internal/ownership", "internal/persistence", "internal/datadir", "internal/blob",
	} {
		forbid(t, pkg, forbidden)
	}
}

// TestControlPlaneUsesProviderContractOnly：控制面包只依赖 internal/provider（契约），不依赖 provider/local、
// sandbox、cgroup、rootfs（代码组织 §2.1、Provider 契约第 1 节）；生产入口不导入只供测试的 provider/fake。
func TestControlPlaneUsesProviderContractOnly(t *testing.T) {
	lowLevel := []string{
		module + "/internal/provider/local", module + "/internal/provider/fake",
		module + "/internal/sandbox", module + "/internal/cgroup", module + "/internal/rootfs",
	}
	for _, pkg := range []string{"internal/api", "internal/task", "internal/runner", "internal/resource", "internal/provider"} {
		forbid(t, pkg, lowLevel)
	}
	forbid(t, "cmd/agentbox", []string{module + "/internal/provider/fake"})
}

// TestReconcileIsPlanOnly：reconcile 只生成计划，不依赖 task、session、recovery（代码组织规则 4、§6.1）。
func TestReconcileIsPlanOnly(t *testing.T) {
	forbid(t, "internal/reconcile", []string{
		module + "/internal/task", module + "/internal/session", module + "/internal/recovery",
	})
}

// TestLowLevelDoesNotDependOnControlPlane：规则 1——sandbox、cgroup、rootfs、provider/local 不依赖控制面。
func TestLowLevelDoesNotDependOnControlPlane(t *testing.T) {
	control := []string{
		module + "/internal/task", module + "/internal/session", module + "/internal/gateway",
		module + "/internal/persistence", module + "/internal/runner", module + "/internal/api",
	}
	for _, pkg := range []string{"internal/sandbox", "internal/cgroup", "internal/rootfs", "internal/provider/local"} {
		forbid(t, pkg, control)
	}
}

// TestDecisionCodeHasNoSideEffectDeps：规则 2——task 的决策代码不依赖 HTTP、Redis、PostgreSQL 驱动与进程。
// 文件系统访问（os）无法按包排除，由代码评审保证。
func TestDecisionCodeHasNoSideEffectDeps(t *testing.T) {
	forbid(t, "internal/task", []string{"net/http", "os/exec", "github.com/jackc/pgx", "github.com/redis"})
}

// TestProtocolIsStdlibOnly：规则 3——protocol 只依赖标准库。
func TestProtocolIsStdlibOnly(t *testing.T) {
	for _, d := range deps(t, "internal/protocol") {
		if d == module+"/internal/protocol" {
			continue
		}
		if first, _, _ := strings.Cut(d, "/"); strings.Contains(first, ".") {
			t.Errorf("internal/protocol 依赖了非标准库包 %s", d)
		}
	}
}

// TestTestOnlyPackagesNotInProduction：provider/fake、providertest 只被测试导入（-deps 不含测试文件的导入）。
func TestTestOnlyPackagesNotInProduction(t *testing.T) {
	cmd := exec.Command("go", "list", module+"/...")
	cmd.Env = append(cmd.Environ(), "GOOS=linux")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	// tests/ 下的包（进程型测试 provider procprov、只供测试的 main agentbox-e2e）同样只供测试。
	testOnly := []string{module + "/internal/provider/fake", module + "/internal/provider/providertest", module + "/tests"}
	for _, pkg := range strings.Fields(string(out)) {
		rel := strings.TrimPrefix(pkg, module+"/")
		if rel == "internal/provider/fake" || rel == "internal/provider/providertest" || strings.HasPrefix(rel, "tests/") {
			continue
		}
		forbid(t, rel, testOnly)
	}
}

// TestFaultInjectEnabledOnlyByE2E：故障注入只能由只供测试的 tests/e2e/... 开启（faultinject.Enable）；
// 生产入口 cmd/agentbox 与其他任何包都不调用它，因此生产二进制中的钩子点恒为空操作。调用关系无法用
// 导入边表达（钩子点所在的包本身导入 faultinject），以源码检查：除 internal/faultinject 自身与 tests/e2e
// 之外，任何 .go 文件不得出现 faultinject.Enable，也不得以别名或点导入 faultinject（否则可绕过检查）。
func TestFaultInjectEnabledOnlyByE2E(t *testing.T) {
	root := filepath.Join("..", "..")
	importPath := `"` + module + `/internal/faultinject"`
	aliased := regexp.MustCompile(`(?m)^\s*(?:import\s+)?([\w.]+)\s+` + regexp.QuoteMeta(importPath))
	// 拼接而成，使本文件自身不含该调用文本。
	enableCall := "faultinject." + "Enable("
	var e2eCalls int
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "worker" || rel == "web" || d.Name() == "node_modules" || (strings.HasPrefix(d.Name(), ".") && rel != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasPrefix(rel, "internal/faultinject/") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(b)
		calls := strings.Count(src, enableCall)
		if strings.HasPrefix(rel, "tests/e2e/") {
			e2eCalls += calls
			return nil
		}
		if calls > 0 {
			t.Errorf("%s 调用了 faultinject.Enable：只有 tests/e2e 可以开启故障注入", rel)
		}
		if m := aliased.FindStringSubmatch(src); m != nil && m[1] != "import" {
			t.Errorf("%s 以别名导入 faultinject（使 Enable 的调用无法被检查）", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if e2eCalls == 0 {
		t.Fatal("tests/e2e 中没有找到 faultinject.Enable 的调用：检查本身失效")
	}
}

// TestCredentialSyscallsOnlyInLauncher：进程级凭据边界（规格 §4.6 实现门槛 1）——server 进程不调用改变
// 自身凭据的系统调用（setgroups、set*uid/set*gid、capset，以及作用于全部线程的 AllThreadsSyscall）。
// 以源码检查：这些调用只出现在 internal/sandbox 的专用启动进程（launch.go）、沙箱 init 的能力设置（caps.go）
// 与 stage-2 helper（helper.go）中，三者都运行在 re-exec 出的独立进程里。experiments/ 下的 spike 不进入任何
// 二进制，不检查。
func TestCredentialSyscallsOnlyInLauncher(t *testing.T) {
	root := filepath.Join("..", "..")
	allowed := map[string]bool{
		"internal/sandbox/launch.go": true, "internal/sandbox/caps.go": true, "internal/sandbox/helper.go": true,
	}
	// 模式本身的写法不匹配自身（名字被拆成 "Set" 与分组）。
	cred := regexp.MustCompile(`\b(?:Set(?:groups|resuid|resgid|reuid|regid|uid|gid|fsuid|fsgid)|AllThreadsSyscall6?)\(` +
		`|\bSYS_(?:SET(?:GROUPS|RESUID|RESGID|REUID|REGID|UID|GID|FSUID|FSGID)(?:32)?|CAPSET)\b`)
	var inLauncher int
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "worker" || rel == "web" || rel == "experiments" || d.Name() == "node_modules" ||
				(strings.HasPrefix(d.Name(), ".") && rel != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		ms := cred.FindAllString(string(b), -1)
		if allowed[rel] {
			inLauncher += len(ms)
			return nil
		}
		if len(ms) > 0 {
			t.Errorf("%s 调用了改变凭据的系统调用 %q：只允许在 internal/sandbox 的启动进程与 helper 中", rel, ms)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if inLauncher == 0 {
		t.Fatal("internal/sandbox/launch.go 中没有找到 setgroups 的调用：检查本身失效")
	}
}

// forbid 断言 pkg 的传递依赖不含 forbidden 中的包及其子包。
func forbid(t *testing.T, pkg string, forbidden []string) {
	t.Helper()
	for _, d := range deps(t, pkg) {
		for _, f := range forbidden {
			if d == f || strings.HasPrefix(d, f+"/") {
				t.Errorf("%s 依赖了 %s", pkg, d)
			}
		}
	}
}
