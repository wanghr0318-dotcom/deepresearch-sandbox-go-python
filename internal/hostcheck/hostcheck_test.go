package hostcheck

import (
	"os"
	"testing"
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

func TestCheck_ReportsRootCorrectly(t *testing.T) {
	r := Check()
	wantRoot := os.Geteuid() == 0
	if r.IsRoot != wantRoot {
		t.Fatalf("Report.IsRoot = %v, want %v", r.IsRoot, wantRoot)
	}
}
