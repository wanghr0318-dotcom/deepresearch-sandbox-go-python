// Package hostcheck 检查宿主机是否具备运行本地沙箱的条件。
package hostcheck

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
	// Warnings 不阻止启动，由 doctor 与 server 启动日志输出（例如 aarch64 未经验证）。
	Warnings []string
	// Items 是逐项检查结果（名称、是否通过、原因），供 agentbox doctor 逐项输出。
	Items []Item
}

// Item 是一项宿主能力检查的结果。
type Item struct {
	Name   string
	OK     bool
	Detail string
}

// probes 是需要实际调用内核探测的能力检查；测试中可替换。
var probes = []struct {
	name string
	run  func() (bool, string)
}{
	{"新挂载 API（open_tree/mount_setattr）", probeNewMountAPI},
	{"close_range", probeCloseRange},
	{"user namespace 可创建", probeUserNS},
	{"user namespace 数量上限", probeUserNSMax},
	{"seccomp 动作（kill_process/errno/allow）", probeSeccompActions},
	{"CLONE_INTO_CGROUP", probeCloneIntoCgroup},
	{"pidfd（pidfd_open/pidfd_send_signal）", probePidfd},
}

// goarch 与 geteuid 是架构与 euid 的来源；测试中可替换。
var (
	goarch  = runtime.GOARCH
	geteuid = os.Geteuid
)

// Check 对当前宿主机做一次自检。
func Check() Report {
	r := Report{
		IsLinux:    runtime.GOOS == "linux",
		IsRoot:     geteuid() == 0,
		CgroupRoot: DefaultCgroupRoot,
	}

	if !r.IsLinux {
		r.Problems = append(r.Problems, "本地沙箱仅支持 Linux，当前为 "+runtime.GOOS)
		return r
	}
	if !r.IsRoot {
		r.Problems = append(r.Problems, "需要 root 权限（特权 Runtime 以 user namespace 隔离 workload，服务本身不是 rootless，规格 §4.5）")
	}

	archItem, archProblem, archWarning := checkArch(goarch)
	r.Items = append(r.Items, archItem)
	if archProblem != "" {
		r.Problems = append(r.Problems, archProblem)
	}
	if archWarning != "" {
		r.Warnings = append(r.Warnings, archWarning)
	}

	release := "" // 读不到则留空，由 checkCgroup 报为无法确认 cgroup.kill
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		release = strings.TrimSpace(string(b))
	}
	var problems []string
	r.CgroupV2, problems = checkCgroup(r.CgroupRoot, release, checkCgroupV2)
	r.Problems = append(r.Problems, problems...)
	cgDetail := "cgroup v2、cgroup.kill（内核 ≥ 5.14）与必需控制器可用；内核 " + release
	if len(problems) > 0 {
		cgDetail = strings.Join(problems, "；")
	}
	r.Items = append(r.Items, Item{"cgroup v2", r.CgroupV2 && len(problems) == 0, cgDetail})

	for _, p := range probes {
		ok, detail := p.run()
		r.Items = append(r.Items, Item{p.name, ok, detail})
		if !ok {
			r.Problems = append(r.Problems, p.name+" 检查未通过："+detail)
		}
	}
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

// aarch64Warning 是 arm64 宿主上的警告：可以启动，但不构成支持主张。
const aarch64Warning = "aarch64 未经单独验证，不构成支持主张（规格 §16.2）"

// checkArch 声明架构支持范围：amd64 支持；arm64 可运行但只给警告；其余为问题。
func checkArch(arch string) (item Item, problem, warning string) {
	switch arch {
	case "amd64":
		return Item{"架构", true, "amd64（x86_64），受支持"}, "", ""
	case "arm64":
		return Item{"架构", true, "arm64：" + aarch64Warning}, "", aarch64Warning
	}
	detail := fmt.Sprintf("架构 %s 不受支持（仅支持 amd64；arm64 未经验证）", arch)
	return Item{"架构", false, detail}, detail, ""
}

// checkUserNS 解析 /proc/sys/user/max_user_namespaces：必须是正整数。
func checkUserNS(content string) (bool, string) {
	s := strings.TrimSpace(content)
	n, err := strconv.Atoi(s)
	switch {
	case err != nil:
		return false, fmt.Sprintf("无法解析 max_user_namespaces %q", s)
	case n <= 0:
		return false, fmt.Sprintf("max_user_namespaces = %d，user namespace 被禁用", n)
	}
	return true, fmt.Sprintf("max_user_namespaces = %d", n)
}

// requiredSeccompActions 是 seccomp 配置用到的动作（规格 §4.5）。
var requiredSeccompActions = []string{"kill_process", "errno", "allow"}

// checkSeccompActions 检查 /proc/sys/kernel/seccomp/actions_avail 是否含所需动作。
func checkSeccompActions(content string) (bool, string) {
	have := map[string]bool{}
	for _, a := range strings.Fields(content) {
		have[a] = true
	}
	var missing []string
	for _, a := range requiredSeccompActions {
		if !have[a] {
			missing = append(missing, a)
		}
	}
	if len(missing) > 0 {
		return false, "seccomp 缺少动作：" + strings.Join(missing, "、")
	}
	return true, "seccomp 动作 " + strings.Join(requiredSeccompActions, "、") + " 可用"
}

// checkCloneIntoCgroup 按内核版本判断 clone3 的 CLONE_INTO_CGROUP（≥ 5.7）。
func checkCloneIntoCgroup(release string) (bool, string) {
	if !kernelAtLeast(release, 5, 7) {
		return false, fmt.Sprintf("内核 %q 不支持 CLONE_INTO_CGROUP（需要 ≥ 5.7）", release)
	}
	return true, "内核 " + release + " 支持 CLONE_INTO_CGROUP（≥ 5.7）"
}

// Err 在存在任何问题时返回一个汇总错误，否则返回 nil。
func (r Report) Err() error {
	if len(r.Problems) == 0 {
		return nil
	}
	return errors.New("宿主环境不满足要求：\n  - " + strings.Join(r.Problems, "\n  - "))
}
