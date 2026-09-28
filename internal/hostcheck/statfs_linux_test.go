//go:build linux

package hostcheck

import (
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

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
	if !checkOverlayFS() {
		t.Fatal("checkOverlayFS() = false, want true on a kernel with overlay support")
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
