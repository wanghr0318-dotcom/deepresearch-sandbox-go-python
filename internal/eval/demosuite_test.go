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

// demoFakePasses are the coding tasks whose scripted fake_reply is correct; the others are wrong on purpose.
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

// runCheck runs a coding task's checker against solution with the local python3, like the exec harness.
func runCheck(t *testing.T, py string, task *Task, solution string, timeout time.Duration) (string, error) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"solution.py": solution, "check.py": task.Check}
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
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(out), context.DeadlineExceeded
	}
	return string(out), err
}

// TestDemoSuite validates eval/suites/demo.yaml offline: it parses, every reference solution passes its
// checker, and exactly the intended scripted fake replies pass (the others fail on purpose).
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
	if coding != 10 || research != 3 {
		t.Fatalf("demo suite has %d coding and %d research tasks, want 10 and 3", coding, research)
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
		out, err := runCheck(t, py, task, task.Reference, 60*time.Second)
		if err != nil || !strings.Contains(out, "PASS") {
			t.Errorf("%s: reference fails its checker: %v\n%s", task.ID, err, out)
		}
		out, err = runCheck(t, py, task, extractCode(task.FakeReply), 5*time.Second)
		passed := err == nil && strings.Contains(out, "PASS")
		if passed != demoFakePasses[task.ID] {
			t.Errorf("%s: fake reply passed=%v, want %v (err %v)\n%s", task.ID, passed, demoFakePasses[task.ID], err, out)
		}
		if task.ID == "primes-fast" && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("primes-fast: the naive fake reply should time out, got %v", err)
		}
	}
}
