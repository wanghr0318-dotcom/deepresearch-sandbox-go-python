//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
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
