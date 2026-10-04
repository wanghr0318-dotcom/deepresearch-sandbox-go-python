package hostcheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
