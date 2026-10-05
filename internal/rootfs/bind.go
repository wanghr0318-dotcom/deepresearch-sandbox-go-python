//go:build linux

// Package rootfs 负责沙箱根文件系统的描述与校验：M1 模板（template.go 的 Template，宿主路径集合）
// 与宿主侧的 bind/unmount 原语。
//
// 规格不再使用 overlayfs（代码组织 §9.2）。按 Plan 1B 的结论，模板的只读 bind 由 init 在沙箱自己的
// mount namespace 中建立（internal/sandbox 的 mounts.go），不在宿主侧预挂载。
package rootfs

import (
	"fmt"
	"syscall"
)

// BindMount 把 src 绑定挂载到 dst，递归包含其下的子挂载。
func BindMount(src, dst string) error {
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("bind 挂载 %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Unmount 卸载 path。它是幂等的：路径本就未挂载时返回 nil。
//
// 常规卸载遇到 EBUSY（仍有进程持有该挂载）时，退化为 lazy umount
// （MNT_DETACH）：先把挂载点从命名空间摘除，等引用归零后由内核自行清理。
// 这是清理路径上必须的兜底——否则一个还没退干净的进程就能让整箱卸不掉。
//
// 调用方契约（实测数据见下）：
//  1. MNT_DETACH 确实会递归断开整个子树，不会留下孤儿挂载——对一个带嵌套
//     子挂载的目录做 mount --rbind 后（挂载条目从 1 增至 3：源的 inner、
//     目标、目标的 inner），MNT_DETACH 后目标子树在 /proc/self/mountinfo
//     里的残留条目为 0。
//  2. 但只要 bind 源下面有嵌套挂载，普通 umount 必然先撞 EBUSY，因此每次
//     都会走 lazy 路径——调用方永远拿不到"同步卸载完成"的保证：Unmount
//     返回 nil 只表示挂载点已从当前命名空间摘除，不能假定底层资源
//     （如源文件系统的引用）已经立即释放。
func Unmount(path string) error {
	if path == "" {
		return fmt.Errorf("Unmount: path 为空")
	}
	err := syscall.Unmount(path, 0)
	switch {
	case err == nil:
		return nil
	case err == syscall.EINVAL, err == syscall.ENOENT:
		// EINVAL：该路径不是挂载点。视作已卸载。
		return nil
	case err == syscall.EBUSY:
		if err := syscall.Unmount(path, syscall.MNT_DETACH); err != nil {
			return fmt.Errorf("lazy 卸载 %s: %w", path, err)
		}
		return nil
	default:
		return fmt.Errorf("卸载 %s: %w", path, err)
	}
}
