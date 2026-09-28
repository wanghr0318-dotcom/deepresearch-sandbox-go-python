// Package hostcheck 检查宿主机是否具备运行本地沙箱的条件。
package hostcheck

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
)

// DefaultCgroupRoot 是 cgroup v2 的标准挂载点。
const DefaultCgroupRoot = "/sys/fs/cgroup"

// cgroup2Magic 是 cgroup2 文件系统的 statfs 魔数（见 linux/magic.h）。
const cgroup2Magic = 0x63677270

// Report 是一次宿主环境自检的结果。
type Report struct {
	IsLinux    bool
	IsRoot     bool
	CgroupV2   bool
	CgroupRoot string
	OverlayFS  bool
	Problems   []string
}

// Check 对当前宿主机做一次自检。
func Check() Report {
	r := Report{
		IsLinux:    runtime.GOOS == "linux",
		IsRoot:     os.Geteuid() == 0,
		CgroupRoot: DefaultCgroupRoot,
	}

	if !r.IsLinux {
		r.Problems = append(r.Problems, "本地沙箱仅支持 Linux，当前为 "+runtime.GOOS)
		return r
	}
	if !r.IsRoot {
		r.Problems = append(r.Problems, "需要 root 权限（M1 未启用 user namespace）")
	}

	ok, err := checkCgroupV2(r.CgroupRoot)
	switch {
	case err != nil:
		r.Problems = append(r.Problems, fmt.Sprintf("无法检查 %s：%v", r.CgroupRoot, err))
	case !ok:
		r.Problems = append(r.Problems, r.CgroupRoot+" 不是 cgroup2fs，需要 cgroup v2")
	default:
		r.CgroupV2 = true
	}

	if r.OverlayFS = checkOverlayFS(); !r.OverlayFS {
		r.Problems = append(r.Problems, "内核未提供 overlay 文件系统")
	}

	return r
}

// Err 在存在任何问题时返回一个汇总错误，否则返回 nil。
func (r Report) Err() error {
	if len(r.Problems) == 0 {
		return nil
	}
	return errors.New("宿主环境不满足要求：\n  - " + strings.Join(r.Problems, "\n  - "))
}

func checkOverlayFS() bool {
	b, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(strings.TrimPrefix(line, "nodev")) == "overlay" {
			return true
		}
	}
	return false
}
