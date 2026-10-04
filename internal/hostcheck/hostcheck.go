// Package hostcheck 检查宿主机是否具备运行本地沙箱的条件。
package hostcheck

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	release := "" // 读不到则留空，由 checkCgroup 报为无法确认 cgroup.kill
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		release = strings.TrimSpace(string(b))
	}
	var problems []string
	r.CgroupV2, problems = checkCgroup(r.CgroupRoot, release, checkCgroupV2)
	r.Problems = append(r.Problems, problems...)

	// 挂载方式相关的前置条件（规格不再使用 overlayfs）待 Plan 1B 确定后补充。
	return r
}

// requiredControllers 是沙箱 cgroup 必须能启用的控制器。
var requiredControllers = []string{"memory", "pids", "cpu"}

// checkCgroup 检查 cgroup v2 前置条件：统一层级、cgroup.kill（内核 ≥ 5.14）、
// 必需控制器可启用。isV2 与 kernelRelease 由调用方注入，便于用假的
// cgroup 根目录驱动测试。
//
// cgroup.kill 只出现在非根 cgroup 里，根上无法直接探测，所以按内核版本判断。
func checkCgroup(root, kernelRelease string, isV2 func(string) (bool, error)) (v2 bool, problems []string) {
	ok, err := isV2(root)
	switch {
	case err != nil:
		return false, []string{fmt.Sprintf("无法检查 %s：%v", root, err)}
	case !ok:
		return false, []string{root + " 不是 cgroup2fs，需要 cgroup v2（统一层级）"}
	}

	if !kernelAtLeast(kernelRelease, 5, 14) {
		problems = append(problems, fmt.Sprintf(
			"内核 %q 不支持 cgroup.kill（需要 ≥ 5.14）", kernelRelease))
	}

	b, err := os.ReadFile(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return true, append(problems, fmt.Sprintf("无法读取 %s/cgroup.controllers：%v", root, err))
	}
	have := map[string]bool{}
	for _, c := range strings.Fields(string(b)) {
		have[c] = true
	}
	for _, c := range requiredControllers {
		if !have[c] {
			problems = append(problems, fmt.Sprintf("cgroup 控制器 %s 不可用（%s/cgroup.controllers 中没有）", c, root))
		}
	}
	return true, problems
}

// kernelAtLeast 解析 "6.6.87.2-microsoft..." 形式的内核版本，判断是否 ≥ major.minor。
// 无法解析时返回 false。
func kernelAtLeast(release string, major, minor int) bool {
	var a, b int
	if _, err := fmt.Sscanf(release, "%d.%d", &a, &b); err != nil {
		return false
	}
	return a > major || (a == major && b >= minor)
}

// Err 在存在任何问题时返回一个汇总错误，否则返回 nil。
func (r Report) Err() error {
	if len(r.Problems) == 0 {
		return nil
	}
	return errors.New("宿主环境不满足要求：\n  - " + strings.Join(r.Problems, "\n  - "))
}
