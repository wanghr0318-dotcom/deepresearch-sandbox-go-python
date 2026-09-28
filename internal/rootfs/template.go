//go:build linux

package rootfs

import (
	"fmt"
	"os"
	"path/filepath"
)

// requiredTemplateDirs 是一份可用模板 rootfs 必须具备的目录。
// 缺项通常意味着解包到一半或解错了目录——与其等 pivot_root 之后
// 在沙箱里报一个难以归因的错误，不如在建箱前就失败。
var requiredTemplateDirs = []string{"bin", "etc", "usr", "proc", "sys", "dev"}

// EnsureTemplate 校验模板 rootfs 是否就位可用。
func EnsureTemplate(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("模板目录 %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("模板路径 %s 不是目录", dir)
	}
	for _, d := range requiredTemplateDirs {
		p := filepath.Join(dir, d)
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("模板 %s 缺少必需目录 %s（模板可能未解包完整）: %w", dir, d, err)
		}
	}
	return checkShell(dir)
}

// checkShell 校验模板内存在可用的 /bin/sh。
//
// 不能直接用 os.Stat：alpine 的 bin/sh 是指向 /bin/busybox 的【绝对】
// 符号链接。在宿主上 os.Stat 会把它解析成宿主的 /bin/busybox——那个
// 文件通常不存在，于是一个完全正确的模板会被判为损坏。绝对链接只有
// 在 pivot_root 之后才解析正确，所以这里必须按模板根重新解析它。
func checkShell(dir string) error {
	shell := filepath.Join(dir, "bin", "sh")
	fi, err := os.Lstat(shell)
	if err != nil {
		return fmt.Errorf("模板 %s 缺少 /bin/sh: %w", dir, err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return nil // 普通文件，直接可用
	}
	target, err := os.Readlink(shell)
	if err != nil {
		return fmt.Errorf("读取 %s 的链接目标: %w", shell, err)
	}
	// 绝对链接按模板根解析，相对链接按所在目录解析。
	resolved := filepath.Join(filepath.Dir(shell), target)
	if filepath.IsAbs(target) {
		resolved = filepath.Join(dir, target)
	}
	if _, err := os.Stat(resolved); err != nil {
		return fmt.Errorf("模板 %s 的 /bin/sh 指向 %s，该目标在模板内不存在: %w",
			dir, target, err)
	}
	return nil
}
