//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestEnsureTemplateRejectsMissingDir(t *testing.T) {
	if err := EnsureTemplate("/definitely/not/here"); err == nil {
		t.Fatal("模板目录不存在时 EnsureTemplate 应当报错")
	}
}

func TestEnsureTemplateRejectsIncompleteRootfs(t *testing.T) {
	dir := t.TempDir()
	// 只有 bin，缺 etc 和 usr——这是解包到一半或解错目录的典型症状。
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := EnsureTemplate(dir)
	if err == nil {
		t.Fatal("模板缺少必需目录时 EnsureTemplate 应当报错")
	}
}

func TestEnsureTemplateAcceptsCompleteRootfs(t *testing.T) {
	dir := t.TempDir()
	for _, d := range requiredTemplateDirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("写 bin/sh: %v", err)
	}
	if err := EnsureTemplate(dir); err != nil {
		t.Fatalf("EnsureTemplate 对完整模板应当通过，实际: %v", err)
	}
}

// TestEnsureTemplateAcceptsAbsoluteShellSymlink 覆盖 alpine 的真实形态：
// bin/sh 是指向 /bin/busybox 的绝对符号链接。在宿主上该链接悬空
// （宿主没有 /bin/busybox），跟随它会把好模板误判为损坏。
func TestEnsureTemplateAcceptsAbsoluteShellSymlink(t *testing.T) {
	dir := t.TempDir()
	for _, d := range requiredTemplateDirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "busybox"), []byte("ELF"), 0o755); err != nil {
		t.Fatalf("写 bin/busybox: %v", err)
	}
	if err := os.Symlink("/bin/busybox", filepath.Join(dir, "bin", "sh")); err != nil {
		t.Fatalf("创建 bin/sh 符号链接: %v", err)
	}
	if err := EnsureTemplate(dir); err != nil {
		t.Fatalf("EnsureTemplate 对含绝对 sh 链接的模板应当通过，实际: %v", err)
	}
}

// TestEnsureTemplateRejectsDanglingShellSymlink 确认真正损坏的模板仍被拒：
// bin/sh 指向模板内并不存在的目标。
func TestEnsureTemplateRejectsDanglingShellSymlink(t *testing.T) {
	dir := t.TempDir()
	for _, d := range requiredTemplateDirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.Symlink("/bin/busybox", filepath.Join(dir, "bin", "sh")); err != nil {
		t.Fatalf("创建 bin/sh 符号链接: %v", err)
	}
	if err := EnsureTemplate(dir); err == nil {
		t.Fatal("bin/sh 的目标在模板内不存在时，EnsureTemplate 应当报错")
	}
}

// TestTemplateValidate：M1 模板（宿主路径集合）的语法规则——规范的绝对路径、不是 /、不与沙箱挂载点
// 重叠（会被 init 的 tmpfs、/dev、proc 等遮盖）、互不重复或嵌套。
func TestTemplateValidate(t *testing.T) {
	if err := (Template{Paths: []string{"/usr", "/bin", "/etc/passwd", "/opt/agentbox"}}).Validate(); err != nil {
		t.Fatalf("合法模板被拒绝: %v", err)
	}
	for name, paths := range map[string][]string{
		"空":      nil,
		"相对路径":   {"usr"},
		"不规范":    {"/usr/../etc"},
		"根":      {"/"},
		"位于挂载点下": {"/tmp/x"},
		"是挂载点":   {"/proc"},
		"重复":     {"/usr", "/usr"},
		"嵌套":     {"/usr", "/usr/lib"},
	} {
		if err := (Template{Paths: paths}).Validate(); err == nil {
			t.Errorf("%s（%v）：Validate 通过，期望拒绝", name, paths)
		}
	}
}

// TestTemplateEnsure：宿主上的校验——每项存在，且沙箱内的 /bin/sh 按沙箱根解析（merged-usr 的 /bin → usr/bin
// 等符号链接逐级落在模板内）为非目录文件。用宿主自己的系统目录组成模板。
func TestTemplateEnsure(t *testing.T) {
	var sys []string
	for _, p := range DefaultTemplate().Paths {
		if p != WorkerDir {
			sys = append(sys, p)
		}
	}
	if err := (Template{Paths: sys}).Ensure(); err != nil {
		t.Fatalf("宿主系统目录组成的模板应当可用: %v", err)
	}
	if err := (Template{Paths: append(sys, "/nonexistent-agentbox")}).Ensure(); err == nil {
		t.Error("模板项不存在时 Ensure 应当报错")
	}
	// 只有 /etc 的子集：沙箱内没有 /bin/sh。
	if err := (Template{Paths: []string{"/etc/passwd"}}).Ensure(); err == nil {
		t.Error("模板不含 /bin/sh 时 Ensure 应当报错")
	}
	// /bin/sh 的链接目标不在模板内（宿主为 merged-usr 时 /bin 是指向 usr/bin 的链接，缺 /usr 即无法解析）。
	if fi, err := os.Lstat("/bin"); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if err := (Template{Paths: []string{"/bin", "/etc/passwd"}}).Ensure(); err == nil {
			t.Error("/bin/sh 解析到模板之外时 Ensure 应当报错")
		}
	}
}

// TestResolveTemplate：默认标识解析为包含 worker 包目录的默认模板；未知标识报错。
func TestResolveTemplate(t *testing.T) {
	tm, err := ResolveTemplate(DefaultTemplateName)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range tm.Paths {
		found = found || p == WorkerDir
	}
	if !found || tm.Validate() != nil {
		t.Fatalf("默认模板 %v：须包含 %s 且语法合法", tm.Paths, WorkerDir)
	}
	if _, err := ResolveTemplate("nope"); err == nil {
		t.Fatal("未知模板标识应当报错")
	}
}

// ---------------------------------------------------------------------------
// Plan 15 Task 2：exec 模板与模板摘要（D5）。

// TestExecTemplate：exec 模板不含 worker 包目录与 /etc/ssl，语法合法，可按标识解析。
func TestExecTemplate(t *testing.T) {
	tm := ExecTemplate()
	if err := tm.Validate(); err != nil {
		t.Fatalf("exec 模板语法不合法: %v", err)
	}
	for _, p := range tm.Paths {
		if p == WorkerDir || p == "/etc/ssl" {
			t.Fatalf("exec 模板 %v 不应包含 %s", tm.Paths, p)
		}
	}
	if r, err := ResolveTemplate(ExecTemplateName); err != nil || !reflect.DeepEqual(r.Paths, tm.Paths) {
		t.Fatalf("ResolveTemplate(%q) = %v, %v", ExecTemplateName, r.Paths, err)
	}
}

// fakeExecRoot 在临时目录中构造 merged-usr 形态的最小模板根：/bin → usr/bin、/usr/bin/sh；withPython 时
// 另有 /usr/bin/python3 → python3.99（相对链接）与解释器文件。返回模板与解释器的宿主路径。
func fakeExecRoot(t *testing.T, withPython bool) (Template, string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "usr", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "sh"), []byte("sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("usr/bin", filepath.Join(root, "bin")); err != nil {
		t.Fatal(err)
	}
	py := filepath.Join(bin, "python3.99")
	if withPython {
		if err := os.WriteFile(py, []byte("interpreter v1"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("python3.99", filepath.Join(bin, "python3")); err != nil {
			t.Fatal(err)
		}
	}
	return Template{Paths: []string{"/usr", "/bin"}, hostRoot: root}, py
}

// TestEnsureExecRequiresPython：沙箱视图中缺 /usr/bin/python3 时 EnsureExec 报告该路径（Ensure 仍通过）。
func TestEnsureExecRequiresPython(t *testing.T) {
	tm, _ := fakeExecRoot(t, false)
	if err := tm.Ensure(); err != nil {
		t.Fatalf("Ensure 不要求 python3: %v", err)
	}
	if err := tm.EnsureExec(); err == nil || !strings.Contains(err.Error(), "/usr/bin/python3") {
		t.Fatalf("缺 python3 时 EnsureExec = %v，期望指出 /usr/bin/python3", err)
	}
	tm, _ = fakeExecRoot(t, true)
	if err := tm.EnsureExec(); err != nil {
		t.Fatalf("有 python3 时 EnsureExec: %v", err)
	}
}

// TestTemplateDigest：同一输入摘要稳定且与路径顺序无关；解释器内容或路径集合变化时摘要改变。
func TestTemplateDigest(t *testing.T) {
	tm, py := fakeExecRoot(t, true)
	d1, err := TemplateDigest(tm)
	if err != nil || len(d1) != 64 {
		t.Fatalf("TemplateDigest = %q, %v", d1, err)
	}
	reordered := Template{Paths: []string{"/bin", "/usr"}, hostRoot: tm.hostRoot}
	if d, err := TemplateDigest(reordered); err != nil || d != d1 {
		t.Fatalf("路径顺序不同时摘要应相同：%q vs %q（%v）", d, d1, err)
	}
	if err := os.WriteFile(py, []byte("interpreter v2"), 0o755); err != nil {
		t.Fatal(err)
	}
	d2, err := TemplateDigest(tm)
	if err != nil || d2 == d1 {
		t.Fatalf("解释器内容变化后摘要应改变：%q（%v）", d2, err)
	}
	if err := os.WriteFile(filepath.Join(tm.hostRoot, "extra"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	more := Template{Paths: []string{"/usr", "/bin", "/extra"}, hostRoot: tm.hostRoot}
	if d, err := TemplateDigest(more); err != nil || d == d2 {
		t.Fatalf("路径集合变化后摘要应改变：%q（%v）", d, err)
	}
	if _, err := TemplateDigest(Template{Paths: []string{"/usr", "/bin"}, hostRoot: t.TempDir()}); err == nil {
		t.Fatal("模板不可用时 TemplateDigest 应报错")
	}
}
