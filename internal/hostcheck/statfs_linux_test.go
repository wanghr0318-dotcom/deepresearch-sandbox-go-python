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

// TestCheck_SuccessPath 验证 Check() 在所有条件都满足时的成功路径：
// IsLinux/IsRoot/CgroupV2 均为 true 且 Err() 为 nil。
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
}

// TestProbes_RootHost 在 root 下实际调用内核：三项能力都应可用（CI 与 WSL2 验收环境）。
func TestProbes_RootHost(t *testing.T) {
	testutil.RequireLinuxRoot(t)
	for _, p := range probes {
		ok, detail := p.run()
		if !ok {
			t.Errorf("%s 不可用：%s", p.name, detail)
		}
		if detail == "" {
			t.Errorf("%s 的 detail 不应为空", p.name)
		}
	}
}

// TestCheck_FailedProbeBecomesProblem 失败的探测必须进入 Problems 与 Items，且带原因。
func TestCheck_FailedProbeBecomesProblem(t *testing.T) {
	saved := probes
	defer func() { probes = saved }()
	probes = []struct {
		name string
		run  func() (bool, string)
	}{{"假能力", func() (bool, string) { return false, "ENOSYS" }}}

	r := Check()
	var found bool
	for _, it := range r.Items {
		if it.Name == "假能力" && !it.OK && it.Detail == "ENOSYS" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Items 中缺少失败的假能力：%+v", r.Items)
	}
	if err := r.Err(); err == nil || !strings.Contains(err.Error(), "假能力") || !strings.Contains(err.Error(), "ENOSYS") {
		t.Fatalf("Err() 应包含能力名与原因，实际 %v", err)
	}
}
