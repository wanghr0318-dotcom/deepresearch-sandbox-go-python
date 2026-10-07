//go:build linux

// Package rootfs 描述与校验沙箱的只读根文件系统模板（一组宿主路径）。
//
// 规格不使用 overlayfs（代码组织 §9.2）。按 Plan 1B 的结论，模板的只读 bind 由 init 在沙箱自己的
// mount namespace 中建立（internal/sandbox 的 mounts.go），不在宿主侧预挂载。
package rootfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ---------------------------------------------------------------------------
// M1 模板：宿主路径集合（规格 §4.5，Plan 2 Task 10）

// Template 描述 M1 的只读 rootfs 模板：一组宿主路径，每一项在沙箱内以**同一路径**只读 bind
// （`ro,nosuid,nodev`，私有传播）：
//
//   - 目录：递归 bind（连同其下的子挂载；非递归 bind 在 user namespace 中会被内核拒绝，实验记录 §4）；
//   - 普通文件或 socket 等：单独 bind（如 `/etc` 的子集逐个文件列出）；
//   - 符号链接：在沙箱根中按原目标重建（如 merged-usr 的 `/bin → usr/bin`）。
//
// 沙箱根本身是 init 在自己的 mount namespace 中建立的只读 tmpfs，只含各项的上级目录、
// 重建的符号链接与 init 自己的挂载点；宿主不可见，进程消失即随命名空间释放。不提供可写 rootfs。
// 模板路径不能位于 init 自己建立的挂载点（ReservedPaths）之上或之下。
type Template struct {
	Paths []string `json:"paths"`

	// hostRoot 只供本包测试：非空时沙箱路径 p 对应宿主路径 hostRoot+p（在临时目录中构造模板）。
	// 生产模板为空，沙箱路径即宿主路径。不编码进 JSON，init 看不到它。
	hostRoot string
}

// host 返回沙箱路径 p 在宿主上的路径。
func (t Template) host(p string) string {
	if t.hostRoot == "" {
		return p
	}
	return filepath.Join(t.hostRoot, p)
}

// WorkerDir 是 worker 包目录，M1 默认模板包含它。
const WorkerDir = "/opt/agentbox"

// DefaultTemplateName 是 M1 默认模板的标识（EnvSpec.Template 的默认值）。
const DefaultTemplateName = "default"

// ReservedPaths 是 init 在沙箱根中自建的挂载点（tmpfs、/dev、proc、workspace、/in、/out 等）；
// 模板路径不能等于或位于它们之下（会被遮盖），也不能是它们的上级。
var ReservedPaths = []string{"/proc", "/sys", "/dev", "/tmp", "/run", "/workspace", "/in", "/out"}

// 默认模板的组成：必需项总是列入（缺失由 Ensure 报告）；可选项只在宿主上存在时列入。
var (
	defaultRequired = []string{"/usr", "/etc/passwd", "/etc/group", WorkerDir}
	defaultOptional = []string{
		"/bin", "/sbin", "/lib", "/lib32", "/lib64", "/libx32",
		"/etc/hosts", "/etc/nsswitch.conf", "/etc/ld.so.cache", "/etc/ld.so.conf", "/etc/ld.so.conf.d",
		"/etc/localtime", "/etc/alternatives", "/etc/ssl",
	}
)

// DefaultTemplate 返回 M1 默认模板：宿主的 `/usr`、`/bin`、`/sbin`、`/lib*`、`/etc` 的子集与 worker 包目录。
func DefaultTemplate() Template {
	paths := append([]string(nil), defaultRequired...)
	for _, p := range defaultOptional {
		if _, err := os.Lstat(p); err == nil {
			paths = append(paths, p)
		}
	}
	return Template{Paths: paths}
}

// ExecTemplateName 是 exec 环境的模板标识（Plan 15 D5）。
const ExecTemplateName = "exec"

// execPython 是 exec 环境的解释器在沙箱内的路径（规格 §10.1，v0.2 只有 python3）。
const execPython = "/usr/bin/python3"

// ExecTemplate 返回 exec 环境的模板：默认模板去掉 WorkerDir 与 /etc/ssl（exec 无网络、不运行 Worker、不需要 SDK）。
func ExecTemplate() Template {
	var paths []string
	for _, p := range DefaultTemplate().Paths {
		if p != WorkerDir && p != "/etc/ssl" {
			paths = append(paths, p)
		}
	}
	return Template{Paths: paths}
}

// ResolveTemplate 把模板标识解析为模板：默认模板与 exec 模板。
func ResolveTemplate(name string) (Template, error) {
	switch name {
	case DefaultTemplateName:
		return DefaultTemplate(), nil
	case ExecTemplateName:
		return ExecTemplate(), nil
	}
	return Template{}, fmt.Errorf("未知的 rootfs 模板 %q", name)
}

// Validate 只做语法检查（不访问文件系统；init 在建立挂载前调用）：非空；每项是规范的绝对路径、
// 不是 "/"；不与 ReservedPaths 重叠；各项互不重复、互不嵌套。
func (t Template) Validate() error {
	if len(t.Paths) == 0 {
		return fmt.Errorf("模板为空")
	}
	for i, p := range t.Paths {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || strings.ContainsRune(p, 0) {
			return fmt.Errorf("模板路径 %q 不是规范的绝对路径（或为 /）", p)
		}
		for _, r := range ReservedPaths {
			if overlaps(p, r) {
				return fmt.Errorf("模板路径 %s 与沙箱挂载点 %s 重叠", p, r)
			}
		}
		for _, q := range t.Paths[:i] {
			if overlaps(p, q) {
				return fmt.Errorf("模板路径 %s 与 %s 重复或嵌套", p, q)
			}
		}
	}
	return nil
}

// Ensure 在宿主上校验模板可用：Validate 通过；每项存在；沙箱内的 /bin/sh 可解析为模板中的
// 非目录文件（符号链接按沙箱根解析，每一级都必须落在模板内）。
func (t Template) Ensure() error {
	if err := t.Validate(); err != nil {
		return err
	}
	for _, p := range t.Paths {
		if _, err := os.Lstat(t.host(p)); err != nil {
			return fmt.Errorf("模板路径 %s: %w", p, err)
		}
	}
	_, err := t.resolveFile("/bin/sh")
	return err
}

// EnsureExec 在 Ensure 之外要求沙箱内的 execPython（/usr/bin/python3）可解析为模板中的普通文件。
func (t Template) EnsureExec() error {
	_, err := t.ensureExec()
	return err
}

// ensureExec 返回 python3 在沙箱视图中解析出的路径（hostRoot 为空时即宿主真实路径）。
func (t Template) ensureExec() (string, error) {
	if err := t.Ensure(); err != nil {
		return "", err
	}
	py, err := t.resolveFile(execPython)
	if err != nil {
		return "", err
	}
	if fi, err := os.Stat(t.host(py)); err != nil || !fi.Mode().IsRegular() {
		return "", fmt.Errorf("模板内的 %s（%s）不是普通文件（%v）", execPython, py, err)
	}
	return py, nil
}

// resolveFile 按沙箱视图解析 p，要求结果存在且不是目录；返回解析出的沙箱路径。
func (t Template) resolveFile(p string) (string, error) {
	r, err := t.resolve(p)
	if err != nil {
		return "", fmt.Errorf("模板内的 %s: %w", p, err)
	}
	fi, err := os.Stat(t.host(r))
	if err != nil {
		return "", fmt.Errorf("模板内的 %s（%s）: %w", p, r, err)
	}
	if fi.IsDir() {
		return "", fmt.Errorf("模板内的 %s（%s）是目录", p, r)
	}
	return r, nil
}

// overlaps 报告 a、b 是否相同或一个位于另一个之下。
func overlaps(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// covers 报告 p 是否在模板某一项上或其下（沙箱内看到的是宿主内容）。
func (t Template) covers(p string) bool {
	for _, q := range t.Paths {
		if p == q || strings.HasPrefix(p, q+"/") {
			return true
		}
	}
	return false
}

// isAncestor 报告 p 是否为模板某一项的上级目录（在沙箱根中作为空目录存在）。
func (t Template) isAncestor(p string) bool {
	for _, q := range t.Paths {
		if strings.HasPrefix(q, p+"/") {
			return true
		}
	}
	return false
}

// resolve 按沙箱内的视图解析 p 中的符号链接：每一级要么在模板内（读宿主），要么是模板项的上级目录。
func (t Template) resolve(p string) (string, error) {
	for hops := 0; hops < 40; hops++ {
		parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
		cur, next := "/", ""
		relinked := false
		for i, part := range parts {
			next = filepath.Join(cur, part)
			if !t.covers(next) {
				if t.isAncestor(next) {
					cur = next
					continue
				}
				return "", fmt.Errorf("%s 不在模板内", next)
			}
			fi, err := os.Lstat(t.host(next))
			if err != nil {
				return "", err
			}
			if fi.Mode()&os.ModeSymlink == 0 {
				cur = next
				continue
			}
			target, err := os.Readlink(t.host(next))
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(cur, target)
			}
			p = filepath.Join(append([]string{target}, parts[i+1:]...)...)
			relinked = true
			break
		}
		if !relinked {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s: 符号链接层数过多", p)
}
