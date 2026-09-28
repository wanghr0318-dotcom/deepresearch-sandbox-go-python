package hostcheck

import (
	"os"
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
