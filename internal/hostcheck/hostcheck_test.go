package hostcheck

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// ---- Plan 15 Task 4：user namespace、seccomp 动作、CLONE_INTO_CGROUP、架构 ----

func TestCheckUserNS(t *testing.T) {
	cases := []struct {
		content string
		ok      bool
	}{
		{"62777\n", true},
		{"1", true},
		{"0\n", false},  // 管理员禁用了 user namespace
		{"", false},     // 读不到内容
		{"abc", false},  // 无法解析
		{"-1\n", false}, // 非法值
	}
	for _, tc := range cases {
		ok, detail := checkUserNS(tc.content)
		if ok != tc.ok {
			t.Errorf("checkUserNS(%q) = %v（%s），want %v", tc.content, ok, detail, tc.ok)
		}
		if detail == "" {
			t.Errorf("checkUserNS(%q) 的 detail 不应为空", tc.content)
		}
	}
}

func TestCheckSeccompActions(t *testing.T) {
	cases := []struct {
		name    string
		content string
		missing string // 空表示应通过
	}{
		{"WSL2 实际内容", "kill_process kill_thread trap errno user_notif trace log allow\n", ""},
		{"缺 kill_process", "kill_thread trap errno trace log allow\n", "kill_process"},
		{"缺 errno", "kill_process kill_thread allow", "errno"},
		{"缺 allow", "kill_process errno", "allow"},
		{"空文件", "", "kill_process"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, detail := checkSeccompActions(tc.content)
			if tc.missing == "" {
				if !ok {
					t.Fatalf("应通过，实际：%s", detail)
				}
				return
			}
			if ok || !strings.Contains(detail, tc.missing) {
				t.Fatalf("应因缺 %s 失败，实际 ok=%v detail=%q", tc.missing, ok, detail)
			}
		})
	}
}

// CLONE_INTO_CGROUP 需要内核 ≥ 5.7，沿用 kernelAtLeast 的边界。
func TestCloneIntoCgroupKernelBoundary(t *testing.T) {
	if kernelAtLeast("5.6.0", 5, 7) {
		t.Fatal(`kernelAtLeast("5.6.0", 5, 7) 应为 false`)
	}
	if !kernelAtLeast("5.7.0", 5, 7) {
		t.Fatal(`kernelAtLeast("5.7.0", 5, 7) 应为 true`)
	}
	if ok, _ := checkCloneIntoCgroup("5.6.19-generic"); ok {
		t.Fatal("5.6 内核不应通过 CLONE_INTO_CGROUP 检查")
	}
	if ok, detail := checkCloneIntoCgroup("6.6.87.2-microsoft-standard-WSL2"); !ok {
		t.Fatalf("6.6 内核应通过，实际：%s", detail)
	}
}

func TestCheckArch(t *testing.T) {
	cases := []struct {
		arch        string
		wantProblem bool
		wantWarning string
	}{
		{"amd64", false, ""},
		{"arm64", false, "aarch64 未经单独验证，不构成支持主张（规格 §16.2）"},
		{"riscv64", true, ""},
		{"386", true, ""},
	}
	for _, tc := range cases {
		item, problem, warning := checkArch(tc.arch)
		if (problem != "") != tc.wantProblem {
			t.Errorf("checkArch(%q) problem = %q，want problem=%v", tc.arch, problem, tc.wantProblem)
		}
		if item.OK == tc.wantProblem {
			t.Errorf("checkArch(%q) item.OK = %v 与 problem 不一致", tc.arch, item.OK)
		}
		if warning != tc.wantWarning {
			t.Errorf("checkArch(%q) warning = %q，want %q", tc.arch, warning, tc.wantWarning)
		}
		if item.Detail == "" || !strings.Contains(item.Detail, tc.arch) {
			t.Errorf("checkArch(%q) detail 应写明架构，实际 %q", tc.arch, item.Detail)
		}
	}
}

// arm64 只进入 Warnings，不阻止启动；不支持的架构进入 Problems。
func TestCheck_ArchWarningsAndProblems(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Check 只在 Linux 上走到架构检查")
	}
	saved := goarch
	defer func() { goarch = saved }()

	goarch = "arm64"
	r := Check()
	if len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "aarch64") {
		t.Fatalf("arm64 应产生一条 aarch64 警告，实际 %v", r.Warnings)
	}
	for _, p := range r.Problems {
		if strings.Contains(p, "架构") {
			t.Fatalf("arm64 不应进入 Problems：%q", p)
		}
	}

	goarch = "riscv64"
	r = Check()
	if len(r.Warnings) != 0 {
		t.Fatalf("riscv64 不应产生警告，实际 %v", r.Warnings)
	}
	if err := r.Err(); err == nil || !strings.Contains(err.Error(), "riscv64") {
		t.Fatalf("riscv64 应进入 Problems，实际 %v", err)
	}
}

// 非 root 时的文案不再声称"未启用 user namespace"（规格 §4.5）。
// 注入 euid，root 与非 root 下都实际运行（CI root 作业不允许 skip）。
func TestCheck_NonRootMessage(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Check 只在 Linux 上检查 root")
	}
	saved := geteuid
	defer func() { geteuid = saved }()
	geteuid = func() int { return 1000 }

	r := Check()
	if r.IsRoot {
		t.Fatal("注入 euid=1000 时 IsRoot 应为 false")
	}
	err := r.Err()
	if err == nil || !strings.Contains(err.Error(), "特权 Runtime 以 user namespace 隔离 workload") {
		t.Fatalf("非 root 问题文案不对：%v", err)
	}
	if strings.Contains(err.Error(), "M1 未启用 user namespace") {
		t.Fatalf("仍含过时文案：%v", err)
	}
}

func TestCheck_ReportsRootCorrectly(t *testing.T) {
	r := Check()
	wantRoot := os.Geteuid() == 0
	if r.IsRoot != wantRoot {
		t.Fatalf("Report.IsRoot = %v, want %v", r.IsRoot, wantRoot)
	}
}

// fakeCgroupRoot 造一个只含 cgroup.controllers 的假根目录。
func fakeCgroupRoot(t *testing.T, controllers string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte(controllers+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestCheckCgroup(t *testing.T) {
	notV2 := func(string) (bool, error) { return false, nil }
	v2 := func(string) (bool, error) { return true, nil }
	cases := []struct {
		name        string
		root        string
		v2          func(string) (bool, error)
		kernel      string
		wantProblem string // 空表示不应有任何问题
	}{
		{"全部满足", fakeCgroupRoot(t, "cpuset cpu io memory hugetlb pids"), v2, "6.6.87.2-microsoft-standard-WSL2", ""},
		{"恰为 5.14", fakeCgroupRoot(t, "cpu memory pids"), v2, "5.14.0", ""},
		{"缺 pids 控制器", fakeCgroupRoot(t, "cpu memory"), v2, "6.6.0", "pids"},
		{"缺 memory 控制器", fakeCgroupRoot(t, "cpu pids"), v2, "6.6.0", "memory"},
		{"内核早于 5.14 无 cgroup.kill", fakeCgroupRoot(t, "cpu memory pids"), v2, "5.10.0-generic", "cgroup.kill"},
		{"内核版本无法解析", fakeCgroupRoot(t, "cpu memory pids"), v2, "weird", "cgroup.kill"},
		{"不是 cgroup v2", fakeCgroupRoot(t, "cpu memory pids"), notV2, "6.6.0", "cgroup v2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, problems := checkCgroup(tc.root, tc.kernel, tc.v2)
			joined := strings.Join(problems, "\n")
			if tc.wantProblem == "" {
				if len(problems) != 0 {
					t.Fatalf("不应有问题，实际: %v", problems)
				}
				return
			}
			if !strings.Contains(joined, tc.wantProblem) {
				t.Fatalf("问题 %q 应包含 %q", joined, tc.wantProblem)
			}
		})
	}
}

// overlay 已不是必需项（规格不再使用 overlayfs）。
func TestCheckDoesNotRequireOverlay(t *testing.T) {
	for _, p := range Check().Problems {
		if strings.Contains(strings.ToLower(p), "overlay") {
			t.Fatalf("Check 不应再因 overlay 报问题: %q", p)
		}
	}
}

func TestReport_Err_NoProblems(t *testing.T) {
	r := Report{}
	if err := r.Err(); err != nil {
		t.Fatalf("Report{}.Err() = %v, want nil", err)
	}
}

func TestReport_Err_WithProblems(t *testing.T) {
	r := Report{Problems: []string{"problem one", "problem two"}}
	err := r.Err()
	if err == nil {
		t.Fatal("Report.Err() = nil, want an error when Problems is non-empty")
	}
	for _, p := range r.Problems {
		if !strings.Contains(err.Error(), p) {
			t.Fatalf("Report.Err() = %q, want it to contain %q", err.Error(), p)
		}
	}
}
