package eval

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMainRunReportCompare(t *testing.T) {
	fs := newFakeServer(t, testPlan)
	dir := t.TempDir()
	suite := filepath.Join(dir, "s.yaml")
	if err := os.WriteFile(suite, []byte(runnerSuite), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "api.token"), []byte(fs.token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := func(k string) string {
		if k == envAddr {
			return strings.TrimPrefix(fs.srv.URL, "http://")
		}
		return ""
	}
	out := filepath.Join(dir, "runs")
	run := func(id string, extra ...string) (int, string, string) {
		var so, se strings.Builder
		args := append([]string{"run", "--suite", suite, "--data-dir", dataDir, "--out", out, "--run-id", id,
			"--agent", "reference", "--tasks", "ok,bug"}, extra...)
		code := Main(args, &so, &se, env)
		return code, so.String(), se.String()
	}
	code, so, se := run("a")
	if code != ExitOK || !strings.Contains(so, "success 1/2") || strings.Contains(so+se, fs.token) {
		t.Fatalf("code %d\n%s\n%s", code, so, se)
	}
	if code, _, se = run("b", "--min-success", "0.9"); code != ExitBelowTarget {
		t.Fatalf("min-success: %d %s", code, se)
	}
	var so2, se2 strings.Builder
	if code := Main([]string{"report", filepath.Join(out, "a")}, &so2, &se2, env); code != 0 || !strings.Contains(so2.String(), "# Eval run a") {
		t.Fatalf("report %d %s", code, se2.String())
	}
	so2.Reset()
	if code := Main([]string{"compare", filepath.Join(out, "a"), filepath.Join(out, "b")}, &so2, &se2, env); code != 0 || !strings.Contains(so2.String(), "Eval compare: a → b") {
		t.Fatalf("compare %d %s", code, so2.String())
	}
	so2.Reset()
	if code := Main([]string{"compare", "--json", filepath.Join(out, "a"), filepath.Join(out, "b")}, &so2, &se2, env); code != 0 || !strings.Contains(so2.String(), `"run_a": "a"`) {
		t.Fatalf("compare json %d %s", code, so2.String())
	}
}

func TestMainUsageErrors(t *testing.T) {
	cases := [][]string{
		{},
		{"bogus"},
		{"run"},
		{"run", "--suite", "x", "--concurrency", "0"},
		{"run", "--suite", "x", "--agent", "oracle"},
		{"run", "--suite", "x", "--judge-model", "m"},
		{"run", "--suite", "x", "--min-success", "2"},
		{"run", "--suite", "x", "extra"},
		{"report"},
		{"compare", "a"},
		{"compare", "--bogus", "a", "b"},
	}
	for _, c := range cases {
		var so, se strings.Builder
		if code := Main(c, &so, &se, func(string) string { return "" }); code != ExitUsage {
			t.Errorf("%v: code %d", c, code)
		}
	}
	if _, _, err := parsePrice("1:x"); err == nil {
		t.Error("bad price accepted")
	}
	if in, out, err := parsePrice("1000:2000"); err != nil || in != 1000 || out != 2000 {
		t.Error("price")
	}
	var so, se strings.Builder
	if code := Main([]string{"run", "--suite", "/nonexistent.yaml"}, &so, &se, nil); code != ExitError {
		t.Errorf("missing suite: %d", code)
	}
}
