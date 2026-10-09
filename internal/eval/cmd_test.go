package eval

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cliFixture writes the runner suite and an api.token for the fake server.
func cliFixture(t *testing.T, fs *fakeServer, suiteText string) (suite, dataDir, out string, env func(string) string) {
	t.Helper()
	dir := t.TempDir()
	suite = filepath.Join(dir, "s.yaml")
	if err := os.WriteFile(suite, []byte(suiteText), 0o644); err != nil {
		t.Fatal(err)
	}
	dataDir = filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "api.token"), []byte(fs.token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env = func(k string) string {
		if k == envAddr {
			return strings.TrimPrefix(fs.srv.URL, "http://")
		}
		return ""
	}
	return suite, dataDir, filepath.Join(dir, "runs"), env
}

func TestMainRunReportCompare(t *testing.T) {
	fs := newFakeServer(t, testPlan)
	suite, dataDir, out, env := cliFixture(t, fs, runnerSuite)
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

// TestMainJudgeKeyNeverRecorded: with the judge on, the judge key (environment only) appears in no run file,
// the manifest records the judge host and prices, and the report shows the judge usage.
func TestMainJudgeKeyNeverRecorded(t *testing.T) {
	const key = "judge-secret-key-0123456789"
	fs := newFakeServer(t, testPlan)
	judgeSuite := strings.Replace(runnerSuite, "    expect: {stdout_contains: [\"PASS\"]}\n",
		"    expect: {stdout_contains: [\"PASS\"]}\n    judge: {rubric: r}\n", 1)
	suite, dataDir, out, env := cliFixture(t, fs, judgeSuite)
	jsrv, calls := okJudgeServer(t)
	withKey := func(k string) string {
		if k == envJudgeKey {
			return key
		}
		return env(k)
	}
	var so, se strings.Builder
	code := Main([]string{"run", "--suite", suite, "--data-dir", dataDir, "--out", out, "--run-id", "j", "--agent", "reference",
		"--tasks", "ok", "--judge-model", "jm", "--judge-base-url", jsrv.URL + "/v1", "--judge-price", "1000:2000",
		"--judge-max-calls", "5", "--judge-required"}, &so, &se, withKey)
	if code != ExitOK || calls.Load() != 1 {
		t.Fatalf("code %d calls %d\n%s\n%s", code, calls.Load(), so.String(), se.String())
	}
	runDir := filepath.Join(out, "j")
	for _, f := range []string{"manifest.json", "trajectories.jsonl", "summary.json", "report.md"} {
		b, err := os.ReadFile(filepath.Join(runDir, f))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), key) || strings.Contains(string(b), fs.token) {
			t.Fatalf("a secret leaked into %s", f)
		}
	}
	lr, err := LoadRun(runDir)
	if err != nil {
		t.Fatal(err)
	}
	if j := lr.Manifest.Judge; j == nil || j.Host == "" || j.PriceInMicroPerMTok != 1000 || j.MaxCalls != 5 || !j.Required {
		t.Fatalf("judge ref %+v", lr.Manifest.Judge)
	}
	md, _ := os.ReadFile(filepath.Join(runDir, "report.md"))
	if !strings.Contains(string(md), "0 errors") {
		t.Fatalf("report lacks judge errors:\n%s", md)
	}
}

// TestRequestIDs: request ids are bounded and unique per run even when the run id repeats.
func TestRequestIDs(t *testing.T) {
	long := strings.Repeat("x", 64)
	id := requestID("0123456789abcdef", long, long, 100)
	if len(id) > 64 || id != requestID("0123456789abcdef", long, long, 100) || id == requestID("0123456789abcdef", long, long, 99) {
		t.Fatalf("request id %q", id)
	}
	if requestID("a", "r", "t", 1) == requestID("b", "r", "t", 1) {
		t.Fatal("nonce does not change the request id")
	}
	// rerunning with the same --run-id (into another out dir) creates new server tasks
	fs := newFakeServer(t, testPlan)
	suite, dataDir, out, env := cliFixture(t, fs, runnerSuite)
	for i, o := range []string{out, out + "2"} {
		var so, se strings.Builder
		if code := Main([]string{"run", "--suite", suite, "--data-dir", dataDir, "--out", o, "--run-id", long, "--agent", "reference",
			"--tasks", "ok"}, &so, &se, env); code != ExitOK {
			t.Fatalf("run %d: %d %s", i, code, se.String())
		}
	}
	if len(fs.tasks) != 2 {
		t.Fatalf("server tasks %d, want 2 (a rerun must not replay the first run's task)", len(fs.tasks))
	}
	for rid := range fs.byRequest {
		if len(rid) > 128 {
			t.Fatalf("request id too long: %d", len(rid))
		}
	}
	var m Manifest
	b, _ := os.ReadFile(filepath.Join(out, long, "manifest.json"))
	if json.Unmarshal(b, &m) != nil || len(m.Nonce) != 16 {
		t.Fatalf("manifest nonce %q", m.Nonce)
	}
}

func TestRedactURL(t *testing.T) {
	if got := redactURL("http://user:pw@127.0.0.1:8080/x"); got != "http://127.0.0.1:8080/x" {
		t.Fatalf("redactURL = %q", got)
	}
	if got := urlHost("https://k:s@api.example.com/v1"); got != "api.example.com" {
		t.Fatalf("urlHost = %q", got)
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
		{"run", "--suite", "x", "--judge-model", "m", "--judge-base-url", "http://j/v1"}, // no prices
		{"run", "--suite", "x", "--judge-required"},
		{"run", "--suite", "x", "--judge-model", "m", "--judge-base-url", "http://j/v1", "--judge-price", "1:1", "--judge-max-calls", "0"},
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
	for _, bad := range []string{"1:x", "0:1", "1:0", "", "5"} {
		if _, _, err := parsePrice(bad); err == nil {
			t.Errorf("price %q accepted", bad)
		}
	}
	if in, out, err := parsePrice("1000:2000"); err != nil || in != 1000 || out != 2000 {
		t.Error("price")
	}
	var so, se strings.Builder
	if code := Main([]string{"run", "--suite", "/nonexistent.yaml"}, &so, &se, nil); code != ExitError {
		t.Errorf("missing suite: %d", code)
	}
}
