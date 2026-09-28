//go:build linux

// Package rootfs 负责沙箱根文件系统的挂载。
//
// 采用 overlayfs 分层：模板作为只读 lower 层被所有实例共享，
// 每个实例只持有一个空的 upper 层。建箱因此不需要拷贝整个 rootfs，
// 打快照也只需打包 upper 层。
package rootfs

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

// Overlay 描述一次 overlayfs 挂载所需的四个目录。
type Overlay struct {
	Lower  string // 只读底层，通常是模板 rootfs
	Upper  string // 可写层，实例的全部改动落在这里
	Work   string // overlayfs 内部工作目录，必须与 Upper 同一文件系统
	Merged string // 挂载点，即沙箱看到的根
}

// Mount 执行 overlayfs 挂载。四个目录都必须已存在。
func (o Overlay) validate() error {
	for name, dir := range map[string]string{
		"Lower": o.Lower, "Upper": o.Upper, "Work": o.Work, "Merged": o.Merged,
	} {
		if dir == "" {
			return fmt.Errorf("Overlay.%s 为空", name)
		}
		if strings.ContainsAny(dir, ",:") {
			// overlayfs 挂载选项用逗号分隔、冒号分隔多个 lowerdir，
			// 路径里带这两个字符会把选项串解析坏。
			return fmt.Errorf("Overlay.%s = %q 含有 overlayfs 选项分隔符（, 或 :）", name, dir)
		}
		if fi, err := os.Stat(dir); err != nil {
			return fmt.Errorf("Overlay.%s = %q: %w", name, dir, err)
		} else if !fi.IsDir() {
			return fmt.Errorf("Overlay.%s = %q 不是目录", name, dir)
		}
	}
	return nil
}

// Mount 把 Lower/Upper/Work 三层联合挂载到 Merged。
func Mount(o Overlay) error {
	if err := o.validate(); err != nil {
		return err
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", o.Lower, o.Upper, o.Work)
	if err := syscall.Mount("overlay", o.Merged, "overlay", 0, opts); err != nil {
		return fmt.Errorf("挂载 overlay 到 %s (%s): %w", o.Merged, opts, err)
	}
	return nil
}

// BindMount 把 src 绑定挂载到 dst，递归包含其下的子挂载。
func BindMount(src, dst string) error {
	if err := syscall.Mount(src, dst, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("bind 挂载 %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Unmount 卸载 path。它是幂等的：路径本就未挂载时返回 nil。
//
// 常规卸载遇到 EBUSY（仍有进程持有该挂载）时，退化为 lazy umount：
// 先把挂载点从命名空间摘除，等引用归零后由内核自行清理。
// 这是清理路径上必须的兜底——否则一个还没退干净的进程就能让整箱卸不掉。
func Unmount(path string) error {
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
