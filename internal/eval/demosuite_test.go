package eval

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// demoFakePasses are the coding tasks whose scripted fake_reply is correct; the others are wrong on purpose
// (clamp and median are reward hacks that exit 0 at import and must fail).
var demoFakePasses = map[string]bool{
	"fizzbuzz": true, "word-freq": true, "roman": true, "json-flatten": true, "log-5xx": true, "rle": true,
}

var fence = regexp.MustCompile("(?is)```[ \\t]*(?:python3?|py)?[ \\t]*\\n(.*?)```")

// extractCode mirrors worker/evalworker/coding.py extract_code.
func extractCode(reply string) string {
	if m := fence.FindStringSubmatch(reply); m != nil {
		return strings.TrimRight(m[1], " \t\r\n") + "\n"
	}
	return strings.TrimSpace(reply) + "\n"
}

// demoToken stands in for the exec harness's completion token: the checker prints it after its last statement,
// so a solution that exits early (os._exit(0), sys.exit(0)) never passes, even with exit status 0.
const demoToken = "DEMO-CHECK-DONE"

// runCheck runs a coding task's checker against solution with the local python3, like the exec harness:
// passed requires exit 0 and the completion token as the last output line.
func runCheck(t *testing.T, py string, task *Task, solution string, timeout time.Duration) (passed bool, out string, err error) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"solution.py": solution, "check.py": task.Check + "\nprint(\"" + demoToken + "\")\n"}
	for k, v := range task.Files {
		files[k] = v
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, py, "-E", "-s", "-B", "check.py")
	cmd.Dir = dir
	b, err := cmd.CombinedOutput()
	out = string(b)
	if ctx.Err() != nil {
		return false, out, context.DeadlineExceeded
	}
	return err == nil && strings.HasSuffix(strings.TrimSpace(out), demoToken), out, err
}

// TestDemoSuite validates eval/suites/demo.yaml offline: it parses, every reference solution passes its
// checker, and exactly the intended scripted fake replies pass (the others, including two reward hacks that
// exit 0 at import, fail).
func TestDemoSuite(t *testing.T) {
	s, err := LoadSuite(filepath.Join("..", "..", "eval", "suites", "demo.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var coding, research int
	for _, task := range s.Tasks {
		if task.Kind == KindCoding {
			coding++
			if task.Reference == "" || task.FakeReply == "" {
				t.Errorf("%s: demo coding tasks need reference and fake_reply", task.ID)
			}
		} else {
			research++
		}
	}
	if coding != 12 || research != 3 {
		t.Fatalf("demo suite has %d coding and %d research tasks, want 12 and 3", coding, research)
	}
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found; checker validation skipped")
	}
	for i := range s.Tasks {
		task := &s.Tasks[i]
		if task.Kind != KindCoding {
			continue
		}
		ok, out, err := runCheck(t, py, task, task.Reference, 60*time.Second)
		if !ok {
			t.Errorf("%s: reference fails its checker: %v\n%s", task.ID, err, out)
		}
		passed, out, err := runCheck(t, py, task, extractCode(task.FakeReply), 5*time.Second)
		if passed != demoFakePasses[task.ID] {
			t.Errorf("%s: fake reply passed=%v, want %v (err %v)\n%s", task.ID, passed, demoFakePasses[task.ID], err, out)
		}
		if task.ID == "primes-fast" && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("primes-fast: the naive fake reply should time out, got %v", err)
		}
		if (task.ID == "clamp" || task.ID == "median") && err != nil {
			t.Errorf("%s: the reward hack should exit 0 (and still fail), got %v", task.ID, err)
		}
	}
}
