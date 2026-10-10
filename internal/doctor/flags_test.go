package doctor

import (
	"errors"
	"strings"
	"testing"
)

const sampleFlags = `# before: mis-configured on purpose
--listen=127.0.0.1:8091
--worker-argv python3,-m,evalworker

--model-fallback-file=/tmp/fallback.json
--call-deadline=1s
--worker-subruns
`

func TestParseFlagsRoundTrip(t *testing.T) {
	f, err := ParseFlags([]byte(strings.ReplaceAll(sampleFlags, "\n", "\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := f.Get("--call-deadline"); !ok || v != "1s" {
		t.Fatalf("call-deadline %q %v", v, ok)
	}
	if v, ok := f.Get("worker-argv"); !ok || v != "python3,-m,evalworker" {
		t.Fatalf("worker-argv %q", v)
	}
	if _, ok := f.Get("model-try-timeout"); ok {
		t.Fatal("absent flag found")
	}
	want := strings.Replace(sampleFlags, "--worker-argv python3", "--worker-argv=python3", 1)
	if got := string(f.Bytes()); got != want {
		t.Fatalf("bytes:\n%s\nwant:\n%s", got, want)
	}
	args := f.Args()
	if len(args) != 5 || args[4] != "--worker-subruns" || args[1] != "--worker-argv=python3,-m,evalworker" {
		t.Fatalf("args %q", args)
	}
	if _, err := ParseFlags([]byte("listen=x\n")); err == nil {
		t.Fatal("non-flag line accepted")
	}
}

func TestValidateAllowlist(t *testing.T) {
	cases := []struct {
		flag, value, want string
		bad               bool
	}{
		{"--model-try-timeout", "5s", "5s", false},
		{"--model-try-timeout", "0", "0s", false},
		{"--model-try-timeout", "500ms", "", true},
		{"--model-try-timeout", "10m", "", true},
		{"--call-deadline", "90000ms", "1m30s", false},
		{"--model-breaker-failures", "2", "2", false},
		{"--model-breaker-failures", "0", "", true},
		{"--turn-tool-budget", "x", "", true},
		{"--model-hedge-delay", "100ms", "", true},
	}
	for _, c := range cases {
		got, err := Validate(c.flag, c.value)
		if (err != nil) != c.bad || got != c.want {
			t.Errorf("Validate(%s, %s) = %q, %v", c.flag, c.value, got, err)
		}
	}
	if _, err := Validate("--upstream-allow-private", "evil:80"); !errors.Is(err, errNotAllowed) {
		t.Fatalf("non-allowlisted flag: %v", err)
	}
}

func TestMergeExperimentAndPatch(t *testing.T) {
	cfg, _ := ParseFlags([]byte(sampleFlags))
	ps := MergeProposals([]Proposal{
		{Kind: KindApply, Flag: "--call-deadline", From: "1s", To: "10s", Rule: "fetch_deadline", Source: "rule", Reason: "a"},
		{Kind: KindApply, Flag: "--model-try-timeout", From: "default 0s", To: "2s", Rule: "model_try_hang", Source: "rule", Reason: "b"},
		{Kind: KindApply, Flag: "--call-deadline", From: "1s", To: "20s", Rule: "x", Source: "model", Reason: "c"},
		{Kind: KindApply, Flag: "--call-deadline", From: "1s", To: "5s", Rule: "y", Source: "model", Reason: "d"},
		{Kind: KindAdvisory, Text: "make backup the primary", Rule: "model_upstream_errors", Source: "rule"},
		{Kind: KindAdvisory, Text: "make backup the primary", Rule: "model_upstream_errors", Source: "rule"},
	})
	if len(ps) != 3 || ps[0].To != "20s" || ps[1].To != "2s" || ps[2].Kind != KindAdvisory {
		t.Fatalf("merged %+v", ps)
	}
	exp := Experiment(cfg, ps)
	if v, _ := exp.Get("call-deadline"); v != "20s" {
		t.Fatalf("experiment call-deadline %q", v)
	}
	if v, _ := exp.Get("model-try-timeout"); v != "2s" {
		t.Fatalf("experiment try timeout %q", v)
	}
	if v, _ := cfg.Get("call-deadline"); v != "1s" {
		t.Fatal("input config modified")
	}
	patch := Patch("before.flags", cfg, ps)
	for _, want := range []string{
		"# advisory (model_upstream_errors, not applied): make backup the primary\n",
		"--- a/before.flags\n+++ b/before.flags\n",
		"---call-deadline=1s\n+--call-deadline=20s\n",
		"+--model-try-timeout=2s\n",
	} {
		if !strings.Contains(patch, want) {
			t.Errorf("patch lacks %q:\n%s", want, patch)
		}
	}
}

func TestUnifiedDiffHunks(t *testing.T) {
	a := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	b := "1\n2\nthree\n4\n5\n6\n7\n8\n9\n10\n11\n"
	got := UnifiedDiff("a", "b", a, b)
	want := "--- a\n+++ b\n@@ -1,6 +1,6 @@\n 1\n 2\n-3\n+three\n 4\n 5\n 6\n@@ -8,3 +8,4 @@\n 8\n 9\n 10\n+11\n"
	if got != want {
		t.Fatalf("diff:\n%s\nwant:\n%s", got, want)
	}
	if UnifiedDiff("a", "b", a, a) != "" {
		t.Fatal("equal texts diff")
	}
	if got := UnifiedDiff("a", "b", "", "x\n"); got != "--- a\n+++ b\n@@ -0,0 +1 @@\n+x\n" {
		t.Fatalf("from empty:\n%s", got)
	}
}
