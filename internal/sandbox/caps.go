//go:build linux

package sandbox

import (
	"fmt"
	"syscall"
	"unsafe"
)

// 本文件设置 init 环境建立完成后的能力集合（规格 §4.6 init 段最后一步）。能力集合是线程属性：
// 每个调用都经 syscall.AllThreadsSyscall 作用于本进程的全部线程（要求 CGO_ENABLED=0，cgo 下返回 ENOTSUP），
// 此后 Go 运行时新建的线程继承创建它的线程的集合。

// 能力编号（linux/capability.h）。
const (
	capKill    = 5
	capSetgid  = 6
	capSetuid  = 7
	capSetpcap = 8
)

// prctl 选项。
const (
	prCapbsetRead        = 23
	prCapbsetDrop        = 24
	prCapAmbient         = 47
	prCapAmbientClearAll = 4
	linuxCapabilityVer3  = 0x20080522
	capabilityU32sPerSet = 2
)

// capSets 是一组能力集合（位 n 即能力 n）。inheritable 与 ambient 总是清空。
type capSets struct {
	Bounding  uint64
	Permitted uint64
	Effective uint64
}

// initCaps 是 init 的能力集合（规格 §4.6）：bounding = {KILL, SETUID, SETGID, SETPCAP}；
// permitted = effective = {KILL}；inheritable、ambient 为空；不设 securebits。
//
// 这是唯一的定义处。Plan 2 的 permitted 扩展（{KILL, SETUID, SETGID, SETPCAP}，R3 判据所需）是有条件
// 授权的增量：须经 Task 11 的有界实验验证后才采纳，届时只改这里。
var initCaps = capSets{
	Bounding:  1<<capKill | 1<<capSetuid | 1<<capSetgid | 1<<capSetpcap,
	Permitted: 1 << capKill,
	Effective: 1 << capKill,
}

// capset 的参数放在包级变量中：AllThreadsSyscall 在各线程上执行时，参数地址必须一直有效且不随栈移动。
var (
	capHdr  struct{ Version, Pid uint32 }
	capData [capabilityU32sPerSet]struct{ Effective, Permitted, Inheritable uint32 }
)

// applyCaps 把全部线程的能力集合设为 c：先收缩 bounding（需要 CAP_SETPCAP，此时仍持有），再清空 ambient，
// 最后 capset 设置 effective、permitted，inheritable 置 0。
func applyCaps(c capSets) error {
	for n := 0; n < 64; n++ {
		if _, _, e := syscall.RawSyscall(syscall.SYS_PRCTL, prCapbsetRead, uintptr(n), 0); e != 0 {
			break // EINVAL：超过 CAP_LAST_CAP
		}
		if c.Bounding&(1<<n) != 0 {
			continue
		}
		if _, _, e := syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prCapbsetDrop, uintptr(n), 0); e != 0 {
			return fmt.Errorf("收缩 bounding（能力 %d）: %w", n, e)
		}
	}
	if _, _, e := syscall.AllThreadsSyscall(syscall.SYS_PRCTL, prCapAmbient, prCapAmbientClearAll, 0); e != 0 {
		return fmt.Errorf("清空 ambient: %w", e)
	}
	capHdr.Version, capHdr.Pid = linuxCapabilityVer3, 0
	for i := range capData {
		capData[i].Effective = uint32(c.Effective >> (32 * i))
		capData[i].Permitted = uint32(c.Permitted >> (32 * i))
		capData[i].Inheritable = 0
	}
	if _, _, e := syscall.AllThreadsSyscall(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(&capHdr)), uintptr(unsafe.Pointer(&capData[0])), 0); e != 0 {
		return fmt.Errorf("capset: %w", e)
	}
	return nil
}
