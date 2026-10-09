package eval

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const yamlSuite = `
name: mini
version: 1
defaults:
  timeout: 2m
  limits: {budget_micro: 100000}
tasks:
  - id: add
    kind: coding
    prompt: "Write add(a, b)."
    reference: |
      def add(a, b):
          return a + b
    check: |
      from solution import add
      assert add(1, 2) == 3
      print("ok")
    files: {"data.csv": "a,b\n1,2\n"}
    expect: {exit_code: 0, stdout_contains: ["ok"]}
  - id: topic-1
    kind: research
    topic: "固态电池"
    research: {max_tasks: 2}
    timeout: 10m
    expect: {min_citations: 1, must_mention: ["电解质"]}
`

const jsonSuite = `{
 "name": "mini", "version": 1,
 "defaults": {"timeout": "2m", "limits": {"budget_micro": 100000}},
 "tasks": [
  {"id": "add", "kind": "coding", "prompt": "Write add(a, b).",
   "reference": "def add(a, b):\n    return a + b\n",
   "check": "from solution import add\nassert add(1, 2) == 3\nprint(\"ok\")\n",
   "files": {"data.csv": "a,b\n1,2\n"},
   "expect": {"exit_code": 0, "stdout_contains": ["ok"]}},
  {"id": "topic-1", "kind": "research", "topic": "固态电池", "research": {"max_tasks": 2},
   "timeout": "10m", "expect": {"min_citations": 1, "must_mention": ["电解质"]}}
 ]}`

func TestParseSuiteYAMLAndJSONHashEqual(t *testing.T) {
	y, err := ParseSuite([]byte(yamlSuite), true)
	if err != nil {
		t.Fatal(err)
	}
	j, err := ParseSuite([]byte(jsonSuite), false)
	if err != nil {
		t.Fatal(err)
	}
	hy, err := y.Hash()
	if err != nil {
		t.Fatal(err)
	}
	hj, _ := j.Hash()
	if hy != hj || len(hy) != 64 {
		t.Fatalf("hash yaml %s != json %s", hy, hj)
	}
	if len(y.Tasks) != 2 || y.Tasks[0].Files["data.csv"] != "a,b\n1,2\n" || *y.Tasks[0].Expect.ExitCode != 0 {
		t.Fatalf("parsed: %+v", y.Tasks[0])
	}
	// any change changes the hash
	y.Tasks[0].Prompt += "!"
	if h2, _ := y.Hash(); h2 == hy {
		t.Fatal("hash did not change")
	}
}

func TestSuiteEffectiveDefaults(t *testing.T) {
	s, err := ParseSuite([]byte(yamlSuite), true)
	if err != nil {
		t.Fatal(err)
	}
	add, _ := s.TaskByID("add")
	res, _ := s.TaskByID("topic-1")
	if d := s.EffectiveTimeout(add, time.Hour); d != 2*time.Minute {
		t.Fatalf("add timeout %v", d)
	}
	if d := s.EffectiveTimeout(res, time.Hour); d != 10*time.Minute {
		t.Fatalf("research timeout %v", d)
	}
	if l := string(s.EffectiveLimits(add)); !strings.Contains(l, "100000") {
		t.Fatalf("limits %s", l)
	}
	if _, ok := s.TaskByID("nope"); ok {
		t.Fatal("unexpected task")
	}
}

func TestParseSuiteRejects(t *testing.T) {
	cases := map[string]string{
		"unknown field":   `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","bogus":1}]}`,
		"no tasks":        `{"name":"x","tasks":[]}`,
		"bad name":        `{"name":"x y","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c"}]}`,
		"dup id":          `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c"},{"id":"a","kind":"coding","prompt":"p","check":"c"}]}`,
		"bad kind":        `{"name":"x","tasks":[{"id":"a","kind":"chat"}]}`,
		"coding no check": `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p"}]}`,
		"research topic":  `{"name":"x","tasks":[{"id":"a","kind":"research"}]}`,
		"research check":  `{"name":"x","tasks":[{"id":"a","kind":"research","topic":"t","check":"c"}]}`,
		"bad file":        `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","files":{"../x":"1"}}]}`,
		"harness file":    `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","files":{"_eval_codec.py":"1"}}]}`,
		"reserved file":   `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","files":{"solution.py":"1"}}]}`,
		"bad regex":       `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","expect":{"stdout_regex":"("}}]}`,
		"bad timeout":     `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","timeout":"soon"}]}`,
		"bad limits":      `{"name":"x","tasks":[{"id":"a","kind":"coding","prompt":"p","check":"c","limits":[1]}]}`,
		"bad ratio":       `{"name":"x","tasks":[{"id":"a","kind":"research","topic":"t","expect":{"min_locatable_ratio":2}}]}`,
		"judge rubric":    `{"name":"x","tasks":[{"id":"a","kind":"research","topic":"t","judge":{}}]}`,
		"trailing":        `{"name":"x","tasks":[{"id":"a","kind":"research","topic":"t"}]} {}`,
	}
	for name, src := range cases {
		if _, err := ParseSuite([]byte(src), false); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ParseSuite([]byte("name: [unclosed"), true); err == nil {
		t.Error("bad yaml accepted")
	}
}

func TestLoadSuiteByExtension(t *testing.T) {
	dir := t.TempDir()
	py := filepath.Join(dir, "s.yaml")
	pj := filepath.Join(dir, "s.json")
	if err := os.WriteFile(py, []byte(yamlSuite), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pj, []byte(jsonSuite), 0o644); err != nil {
		t.Fatal(err)
	}
	a, err := LoadSuite(py)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadSuite(pj)
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != b.Name {
		t.Fatal("names differ")
	}
	if _, err := LoadSuite(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("missing file accepted")
	}
}
