//go:build linux

package hostcheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

// requireCgroupV2Host 在宿主未使用 cgroup v2 时跳过测试。
//
// 探测手段刻意独立于被测函数：cgroup.controllers 是 cgroup v2 独有的文件，
// 而 checkCgroupV2 判断的是 statfs 魔数。两条路径互不依赖，下面的断言才是
// 真的在交叉验证，而不是拿被测函数证明它自己。
func requireCgroupV2Host(t *testing.T) {
	t.Helper()
	marker := filepath.Join(DefaultCgroupRoot, "cgroup.controllers")
	if _, err := os.Stat(marker); err != nil {
		t.Skipf("宿主未使用 cgroup v2（%s 不可达: %v），跳过正例验证", marker, err)
	}
}

func TestCheckCgroupV2_TempDirIsNotCgroup2(t *testing.T) {
	dir := t.TempDir()
	ok, err := checkCgroupV2(dir)
	if err != nil {
		t.Fatalf("checkCgroupV2(%q) returned error: %v", dir, err)
	}
	if ok {
		t.Fatalf("checkCgroupV2(%q) = true, want false for a plain temp dir", dir)
	}
}

func TestCheckCgroupV2_MissingPath(t *testing.T) {
	ok, err := checkCgroupV2("/definitely/not/here")
	if err == nil {
		t.Fatal("checkCgroupV2 on a missing path should return an error")
	}
	if ok {
		t.Fatal("checkCgroupV2 on a missing path should return false")
	}
}

// TestCheckCgroupV2_DefaultCgroupRoot 验证正例：真实的 cgroup v2 挂载点应被
// 识别为 cgroup2fs。不需要 root，只需要内核提供 cgroup v2（现代 Linux 发行版的
// 默认配置，包括 WSL2）。
func TestCheckCgroupV2_DefaultCgroupRoot(t *testing.T) {
	requireCgroupV2Host(t)

	ok, err := checkCgroupV2(DefaultCgroupRoot)
	if err != nil {
		t.Fatalf("checkCgroupV2(%q) returned error: %v", DefaultCgroupRoot, err)
	}
	if !ok {
		t.Fatalf("checkCgroupV2(%q) = false, want true on a cgroup v2 host", DefaultCgroupRoot)
	}
}

// TestCheckOverlayFS 验证 checkOverlayFS 能从 /proc/filesystems 中读出内核是否
// 提供 overlay 文件系统。现代 Linux 内核（包括 WSL2 默认内核）都编译了 overlay
// 支持，因此这里断言为 true。
func TestCheckOverlayFS(t *testing.T) {
	// 独立读一遍 /proc/filesystems 做环境探测。这里用的是宽松的子串匹配，
	// 与被测函数按行严格解析的方式不同——两者若不一致，正说明解析逻辑有问题，
	// 断言因此是有意义的交叉验证。
	raw, err := os.ReadFile("/proc/filesystems")
	if err != nil {
		t.Skipf("读不到 /proc/filesystems，跳过: %v", err)
	}
	if !strings.Contains(string(raw), "overlay") {
		t.Skip("内核未提供 overlay 文件系统，跳过正例验证")
	}

	if !checkOverlayFS() {
		t.Fatal("checkOverlayFS() = false，但 /proc/filesystems 中确有 overlay——解析逻辑有误")
	}
}

// TestCheck_SuccessPath 验证 Check() 在所有条件都满足时的成功路径：
// IsLinux/IsRoot/CgroupV2/OverlayFS 均为 true 且 Err() 为 nil。
// 需要 root，非 root 环境下会被跳过。
func TestCheck_SuccessPath(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	r := Check()
	if err := r.Err(); err != nil {
		t.Fatalf("Check().Err() = %v, want nil", err)
	}
	if !r.IsLinux {
		t.Error("Report.IsLinux = false, want true")
	}
	if !r.IsRoot {
		t.Error("Report.IsRoot = false, want true")
	}
	if !r.CgroupV2 {
		t.Error("Report.CgroupV2 = false, want true")
	}
	if !r.OverlayFS {
		t.Error("Report.OverlayFS = false, want true")
	}
}
