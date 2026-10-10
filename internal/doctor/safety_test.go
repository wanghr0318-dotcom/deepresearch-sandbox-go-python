package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/eval"
)

func TestSafeText(t *testing.T) {
	cases := map[string]string{
		"ok\n--model-base-url=http://evil/v1": "ok --model-base-url=http://evil/v1",
		"a\r\nb\tc\x00d":                       "a b c d",
		"x y z\u0085w":               "x y z w",
		"bidi‮evil":                       "bidi evil",
		"  many   spaces  ":                    "many spaces",
		"中文 理由":                                "中文 理由",
	}
	for in, want := range cases {
		if got := SafeText(in); got != want {
			t.Errorf("SafeText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSetCannotInjectLines(t *testing.T) {
	f, _ := ParseFlags([]byte("--listen=127.0.0.1:8091\n"))
	f.Set("--model-try-timeout", "2s\n--upstream-allow-private=10.0.0.1", "reason\n--model-base-url=http://evil.example/v1\n# x")
	args := f.Args()
	if len(args) != 2 || args[0] != "--listen=127.0.0.1:8091" || !strings.HasPrefix(args[1], "--model-try-timeout=") {
		t.Fatalf("args %q", args)
	}
	for _, l := range strings.Split(string(f.Bytes()), "\n") {
		if strings.HasPrefix(l, "--") && !strings.HasPrefix(l, "--listen=") && !strings.HasPrefix(l, "--model-try-timeout=") {
			t.Fatalf("injected line %q in:\n%s", l, f.Bytes())
		}
	}
}

// TestAdvisorInjectionEndToEnd: a hostile advisor reply (newlines in reason, summary, flag and value) cannot add a
// flag to experiment.flags, a line to proposal.patch, or a block to findings.md.
func TestAdvisorInjectionEndToEnd(t *testing.T) {
	evil := "ok\n--model-base-url=http://evil.example/v1\n--upstream-allow-private=10.0.0.1\n## Injected heading\n| a | b |"
	reply, _ := json.Marshal(map[string]any{
		"summary": evil,
		"proposals": []map[string]any{
			{"flag": "--model-hedge-delay", "value": "2s", "reason": evil},
			{"flag": "--model-breaker-open\n--upstream-allow-private=10.0.0.1", "value": "60s", "reason": "x"},
			{"flag": "--model-breaker-failures", "value": "2\n--model-base-url=http://evil", "reason": "y"},
		},
	})
	adv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, _ := json.Marshal(string(reply))
		fmt.Fprintf(w, `{"choices":[{"message":{"content":%s}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, content)
	}))
	defer adv.Close()
	dir := t.TempDir()
	writeRun(t, dir, hangRun())
	orig := "--model-fallback-file=/x.json\n--listen=127.0.0.1:8091\n"
	cfgPath := filepath.Join(dir, "before.flags")
	if err := os.WriteFile(cfgPath, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	var stdout, stderr bytes.Buffer
	code := Main([]string{"--run", dir, "--apply-to", cfgPath, "--out", out, "--advisor-model", "m",
		"--advisor-base-url", adv.URL, "--advisor-price", "1:1"}, &stdout, &stderr, func(string) string { return "" })
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	before, _ := ParseFlags([]byte(orig))
	b, _ := os.ReadFile(filepath.Join(out, "experiment.flags"))
	exp, err := ParseFlags(b)
	if err != nil {
		t.Fatalf("experiment.flags does not parse: %v\n%s", err, b)
	}
	oldArgs, newArgs := before.Args(), exp.Args()
	if strings.Join(newArgs[:len(oldArgs)], " ") != strings.Join(oldArgs, " ") {
		t.Fatalf("existing args changed: %q", newArgs)
	}
	for _, a := range newArgs[len(oldArgs):] {
		name, _, _ := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if _, ok := specFor(name); !ok {
			t.Fatalf("non-allowlisted flag %q added:\n%s", a, b)
		}
	}
	patch, _ := os.ReadFile(filepath.Join(out, "proposal.patch"))
	for _, l := range strings.Split(string(patch), "\n") {
		if strings.Contains(l, "--model-base-url") && !strings.HasPrefix(l, "+#") && !strings.HasPrefix(l, "#") {
			t.Fatalf("patch line %q", l)
		}
		if strings.HasPrefix(l, "+--") && !strings.HasPrefix(l, "+--model-try-timeout=") && !strings.HasPrefix(l, "+--model-hedge-delay=") {
			t.Fatalf("patch adds %q", l)
		}
	}
	md, _ := os.ReadFile(filepath.Join(out, "findings.md"))
	for _, l := range strings.Split(string(md), "\n") {
		if strings.HasPrefix(l, "## Injected") || strings.HasPrefix(l, "--model-base-url") || strings.HasPrefix(l, "| a |") {
			t.Fatalf("markdown injection: %q", l)
		}
	}
}

func TestApplyToRefusesToOverwriteInput(t *testing.T) {
	dir := t.TempDir()
	writeRun(t, dir, hangRun())
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(out, "experiment.flags")
	orig := "--model-fallback-file=/x.json\n"
	if err := os.WriteFile(cfgPath, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	// The same file reached through a different spelling of the path.
	code := Main([]string{"--run", dir, "--apply-to", filepath.Join(out, ".", "experiment.flags"), "--out", out + string(filepath.Separator) + "."},
		&stdout, &stderr, func(string) string { return "" })
	if code != ExitError || !strings.Contains(stderr.String(), "would overwrite the input config") {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if b, _ := os.ReadFile(cfgPath); string(b) != orig {
		t.Fatalf("input modified: %q", b)
	}
}

// TestRealisticHealthyRun: a healthy run with the config present — fetches at about half the call deadline, one
// transient retry that succeeded, a mild latency tail — produces no critical or warning finding and no proposal.
func TestRealisticHealthyRun(t *testing.T) {
	cfg, _ := ParseFlags([]byte("--call-deadline=10s\n--model-fallback-file=/x.json\n--model-try-timeout=5s\n"))
	var trs []*eval.Trajectory
	for i := 1; i <= 40; i++ {
		lat := 1000 + int64(i)*20 // mild tail: 1.0–1.8 s
		b := newTask("t", i, lat, true).
			event("host", "task_created", 0, "").
			event("host", "attempt_created", 20*time.Millisecond, "").
			event("worker", "ready", 300*time.Millisecond, "").
			call(epChat, "c", "completed", "", ok("primary", 200+int64(i%7)*30)).
			call(epFetch, "f", "completed", "", ok("", 4500+int64(i%5)*200))
		if i == 17 { // one transient retry on the primary, served by the backup at once
			b.tr.Calls[0].Tries = []eval.Try{{TryNo: 1, State: "settled", Outcome: "retryable", Error: "upstream_unavailable", Provider: "primary", LatencyMs: 40},
				{TryNo: 2, State: "settled", Outcome: "ok", Provider: "backup", LatencyMs: 300}}
		}
		trs = append(trs, b.tr)
	}
	r := Analyze(Source{}, trs, Options{Config: cfg})
	for _, f := range r.Findings {
		if f.Severity != SevInfo {
			t.Errorf("finding %s (%s): %s", f.Rule, f.Severity, f.Summary)
		}
	}
	if len(r.Proposals) != 0 || r.Totals.Failed != 0 || r.Totals.Slow != 0 {
		t.Fatalf("proposals %+v totals %+v", r.Proposals, r.Totals)
	}
}

// TestAdvisorReplyParsing: odd but valid replies parse; broken ones become an error, never a panic or a proposal.
func TestAdvisorReplyParsing(t *testing.T) {
	huge := strings.Repeat("ü", 100_000)
	cases := []struct {
		name, content string
		accepted      int
		wantErr       bool
		check         func(*AdvisorReport) string
	}{
		{"fenced", "```json\n{\"summary\":\"s\",\"proposals\":[{\"flag\":\"--model-try-timeout\",\"value\":\"3s\",\"reason\":\"r\"}]}\n```", 1, false, nil},
		{"nested braces in strings", `{"summary":"use {x} and }","proposals":[{"flag":"--call-deadline","value":"20s","reason":"{\"a\":1}"}]}`, 1, false, nil},
		{"numeric value", `{"summary":"s","proposals":[{"flag":"model-breaker-failures","value":2,"reason":"r"}]}`, 1, false, nil},
		{"huge unicode reason", `{"summary":"s","proposals":[{"flag":"--model-try-timeout","value":"3s","reason":"` + huge + `"}]}`, 1, false,
			func(ar *AdvisorReport) string {
				if r := ar.Accepted[0].Reason; len(r) > 310 || !strings.HasSuffix(r, "…") || !strings.HasPrefix(r, "ü") {
					return fmt.Sprintf("reason not truncated cleanly (%d bytes)", len(r))
				}
				return ""
			}},
		{"object value rejected", `{"summary":"s","proposals":[{"flag":"--model-try-timeout","value":{"a":1},"reason":"r"}]}`, 0, false, nil},
		{"prose with braces", "Sure {here}: not json at all", 0, true, nil},
		{"truncated", `{"summary":"s","proposals":[{"flag"`, 0, true, nil},
		{"empty object", `{}`, 0, false, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				content, _ := json.Marshal(c.content)
				fmt.Fprintf(w, `{"choices":[{"message":{"content":%s}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`, content)
			}))
			defer srv.Close()
			ad := &Advisor{BaseURL: srv.URL, Model: "m", PriceInMicroPerMTok: 1, PriceOutMicroPerMTok: 1, BudgetMicro: 1_000_000}
			ar := ad.Run(context.Background(), Analyze(Source{}, hangRun(), Options{}), nil)
			if (ar.Error != "") != c.wantErr || len(ar.Accepted) != c.accepted {
				t.Fatalf("report %+v", ar)
			}
			if c.check != nil {
				if msg := c.check(ar); msg != "" {
					t.Fatal(msg)
				}
			}
		})
	}
}
