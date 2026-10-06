//go:build linux

package hostcheck

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"unsafe"
)

// 这些系统调用号在 x86_64 与 aarch64 上相同（统一号段）。
const (
	sysOpenTree     = 428
	sysCloseRange   = 436
	sysMountSetattr = 442

	// pidfd 系统调用号在所有架构上统一（4xx 号段）。
	sysPidfdSendSignal = 424
	sysPidfdOpen       = 434

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

// readProc 读取一个 /proc 文件；失败时返回带路径的原因。
func readProc(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("无法读取 %s：%w", path, err)
	}
	return string(b), nil
}

// probeUserNSMax 检查 /proc/sys/user/max_user_namespaces > 0。
func probeUserNSMax() (bool, string) {
	s, err := readProc("/proc/sys/user/max_user_namespaces")
	if err != nil {
		return false, err.Error()
	}
	return checkUserNS(s)
}

// probeSeccompActions 检查内核提供 seccomp 配置所需的动作。
func probeSeccompActions() (bool, string) {
	s, err := readProc("/proc/sys/kernel/seccomp/actions_avail")
	if err != nil {
		return false, err.Error()
	}
	return checkSeccompActions(s)
}

// probeCloneIntoCgroup 按内核版本判断 CLONE_INTO_CGROUP（≥ 5.7）。
func probeCloneIntoCgroup() (bool, string) {
	s, err := readProc("/proc/sys/kernel/osrelease")
	if err != nil {
		return false, err.Error()
	}
	return checkCloneIntoCgroup(strings.TrimSpace(s))
}

// probePidfd 以非法参数调用 pidfd_open（pid 0 → EINVAL）与 pidfd_send_signal
// （fd -1 → EBADF），不打开、不发信号给任何进程；ENOSYS（或被 seccomp 拒绝的 EPERM）即不可用。
func probePidfd() (bool, string) {
	if _, _, errno := syscall.Syscall(sysPidfdOpen, 0, 0, 0); unavailable(errno) {
		return false, fmt.Sprintf("pidfd_open 不可用：%v", errno)
	}
	badFd := -1
	if _, _, errno := syscall.Syscall6(sysPidfdSendSignal, uintptr(badFd), 0, 0, 0, 0, 0); unavailable(errno) {
		return false, fmt.Sprintf("pidfd_send_signal 不可用：%v", errno)
	}
	return true, "pidfd_open 与 pidfd_send_signal 可用"
}
