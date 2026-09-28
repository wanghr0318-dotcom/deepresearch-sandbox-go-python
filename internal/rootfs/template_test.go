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
