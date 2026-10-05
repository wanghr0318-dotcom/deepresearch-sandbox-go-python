//go:build linux

package hostcheck

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"
)

// 这些系统调用号在 x86_64 与 aarch64 上相同（统一号段）。
const (
	sysOpenTree     = 428
	sysCloseRange   = 436
	sysMountSetattr = 442

	openTreeClone   = 1       // OPEN_TREE_CLONE
	openTreeCloexec = 0x80000 // OPEN_TREE_CLOEXEC（= O_CLOEXEC）
	atFdcwd         = -100    // AT_FDCWD
	atEmptyPath     = 0x1000  // AT_EMPTY_PATH
)

// unavailable 判断 errno 是否表示"此能力不可用"（内核未提供、被 seccomp 或权限拒绝）。
func unavailable(err error) bool {
	return errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM)
}

// probeNewMountAPI 对 "/" 做 open_tree(OPEN_TREE_CLONE) 并关闭得到的 fd，
// 再以非法参数调用 mount_setattr：存在则返回 EBADF/EINVAL，不存在返回 ENOSYS。
func probeNewMountAPI() (bool, string) {
	path, err := syscall.BytePtrFromString("/")
	if err != nil {
		return false, err.Error()
	}
	dfd := atFdcwd
	fd, _, errno := syscall.Syscall(sysOpenTree, uintptr(dfd),
		uintptr(unsafe.Pointer(path)), uintptr(openTreeClone|openTreeCloexec))
	if errno != 0 {
		if unavailable(errno) {
			return false, fmt.Sprintf("open_tree 不可用：%v", errno)
		}
		return false, fmt.Sprintf("open_tree(\"/\", OPEN_TREE_CLONE) 失败：%v", errno)
	}
	_ = syscall.Close(int(fd))

	empty, _ := syscall.BytePtrFromString("")
	_, _, errno = syscall.Syscall6(sysMountSetattr, ^uintptr(0),
		uintptr(unsafe.Pointer(empty)), uintptr(atEmptyPath), 0, 0, 0)
	if unavailable(errno) {
		return false, fmt.Sprintf("mount_setattr 不可用：%v", errno)
	}
	return true, "open_tree 与 mount_setattr 可用"
}

// probeCloseRange 以空区间（first > last）调用 close_range，不关闭任何 fd。
func probeCloseRange() (bool, string) {
	_, _, errno := syscall.Syscall(sysCloseRange, 2, 1, 0)
	switch {
	case errno == 0:
		return true, "close_range 可用"
	case errno == syscall.EINVAL:
		return true, "close_range 可用（空区间返回 EINVAL）"
	case unavailable(errno):
		return false, fmt.Sprintf("close_range 不可用：%v", errno)
	}
	return false, fmt.Sprintf("close_range 探测失败：%v", errno)
}

// probeUserNS 启动一个带 CLONE_NEWUSER 的子进程。AppArmor 的非特权 userns 限制
// 只作用于无 CAP_SYS_ADMIN 的进程，不影响 root，因此本检查应在 root 下运行。
func probeUserNS() (bool, string) {
	truePath, err := exec.LookPath("true")
	if err != nil {
		truePath = "/bin/true"
	}
	cmd := exec.Command(truePath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWUSER}
	if err := cmd.Run(); err != nil {
		return false, fmt.Sprintf("无法创建 user namespace：%v", err)
	}
	return true, "可创建 user namespace"
}
