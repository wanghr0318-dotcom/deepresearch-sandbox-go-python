package call

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// Workspace shell commands (shell.go): same pipeline as /v1/exec with the shell variant.

func sinv(callID, cmd string, files ...ShellFile) ShellInvoke {
	return ShellInvoke{TaskID: "t1", AttemptID: "a1", CallID: callID, Command: cmd, Files: files}
}

func (h *execHarness) shell(t *testing.T, in ShellInvoke) Result {
	t.Helper()
	r, err := h.c.ExecShell(context.Background(), in)
	if err != nil {
		t.Fatalf("ExecShell(%s)：%v", in.CallID, err)
	}
	return r
}

// putBlob stores content without authorizing it to the task (workspace files are trusted by construction).
func (h *execHarness) putBlob(t *testing.T, content string) string {
	t.Helper()
	ref, err := h.blobs.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	return ref.SHA256
}

func TestShellHappyPath(t *testing.T) {
	h := newExecHarness(t, nil, execScript{stdout: "ok\n", exitCode: 0,
		outputs:     map[string]string{"src/a.py": "print(1)\n", "run.sh": "#!/bin/sh\n"},
		execOutputs: map[string]bool{"run.sh": true},
		skipped:     []provider.SkippedOutput{{Path: "evil", Reason: provider.SkipSymlink}}})
	a := h.putBlob(t, "print(1)\n")
	run := h.putBlob(t, "#!/bin/sh\n")
	r := h.shell(t, sinv("s1", "python3 src/a.py", ShellFile{Path: "src/a.py", SHA256: a, Size: 9},
		ShellFile{Path: "run.sh", SHA256: run, Size: 10, Executable: true}))
	v := parseExecResult(t, r)
	if v.Status != "completed" || v.Exit == nil || v.Exit.Code != 0 || v.Stdout != "ok\n" {
		t.Fatalf("result %+v", v)
	}
	spec := h.envs.lastSpec(t)
	if !equalStrings(spec.Argv, ShellArgv()) || spec.Dir != "/out" || !equalStrings(spec.Env, ShellEnviron()) {
		t.Fatalf("ExecSpec = %+v", spec)
	}
	if spec.Argv[0] != "/bin/bash" || !strings.Contains(spec.Argv[4], "cp -R --preserve=mode /in/ws/. /out/") ||
		!strings.Contains(spec.Argv[4], "/in/.agentbox/cmd.sh") {
		t.Fatalf("wrapper argv = %q", spec.Argv)
	}
	env := h.envs.env(h.envs.reqs[0].EnvID)
	if env.staged[".agentbox/cmd.sh"] != "python3 src/a.py" || env.staged["ws/src/a.py"] != "print(1)\n" ||
		env.staged["ws/run.sh"] != "#!/bin/sh\n" || len(env.staged) != 3 {
		t.Fatalf("/in staged = %v", env.staged)
	}
	if env.modes["ws/run.sh"] != 0o555 || env.modes["ws/src/a.py"] != 0o444 || env.modes[".agentbox/cmd.sh"] != 0o444 {
		t.Fatalf("/in modes = %v", env.modes)
	}
	// Outputs carry the executable bit (only in the shell variant).
	var raw struct {
		Outputs []map[string]any `json:"outputs"`
	}
	if err := json.Unmarshal(r.Body, &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw.Outputs) != 2 || raw.Outputs[0]["path"] != "run.sh" || raw.Outputs[0]["executable"] != true ||
		raw.Outputs[1]["path"] != "src/a.py" || raw.Outputs[1]["executable"] != nil {
		t.Fatalf("outputs = %v", raw.Outputs)
	}
	// Journaled and reserved as an exec call.
	rec, _ := h.call(t, "t1", "s1")
	if rec.State != StateCompleted || rec.Endpoint != ExecEndpoint {
		t.Fatalf("journal %+v", rec)
	}
	if q, _ := h.es.LoadExecQuota(context.Background(), "t1"); q.CountUsed != 1 {
		t.Fatalf("exec quota %+v", q)
	}
	// Replay by call id (files given in another order → same fingerprint).
	before := len(h.ops.list())
	r2 := h.shell(t, sinv("s1", "python3 src/a.py", ShellFile{Path: "run.sh", SHA256: run, Size: 10, Executable: true},
		ShellFile{Path: "src/a.py", SHA256: a, Size: 9}))
	if !r2.Replayed || r2.BlobSHA256 != r.BlobSHA256 || len(h.ops.list()) != before {
		t.Fatalf("replay %+v", r2)
	}
	// A different workspace under the same call id diverges (reported with the shell endpoint).
	r3 := h.shell(t, sinv("s1", "python3 src/a.py", ShellFile{Path: "src/a.py", SHA256: a, Size: 9}))
	if r3.Status != 409 || r3.Code != persistence.CodeFingerprintMismatch {
		t.Fatalf("divergence = %d %s", r3.Status, r3.Code)
	}
	if ev := h.events.list(); len(ev) != 1 || !strings.Contains(ev[0], "endpoint="+ShellEndpoint) {
		t.Fatalf("events %v", ev)
	}
}

// /v1/exec results never carry "executable", even for executable outputs (the /v1/exec result format is unchanged).
func TestExecResultHasNoExecutableField(t *testing.T) {
	h := newExecHarness(t, nil, execScript{outputs: map[string]string{"x": "1"}, execOutputs: map[string]bool{"x": true}})
	r := h.exec(t, xinv("c1", execBody("print(1)", "")))
	if strings.Contains(string(r.Body), "executable") {
		t.Fatalf("/v1/exec result mentions executable: %s", r.Body)
	}
}

// The shell fingerprint differs from any /v1/exec fingerprint; a /v1/exec call id cannot be replayed as shell.
func TestShellAndExecDoNotCollide(t *testing.T) {
	h := newExecHarness(t, nil)
	parseExecResult(t, h.exec(t, xinv("same", execBody("print(1)", ""))))
	r := h.shell(t, sinv("same", "print(1)"))
	if r.Status != 409 || r.Code != persistence.CodeFingerprintMismatch {
		t.Fatalf("shell under an exec call id = %d %s", r.Status, r.Code)
	}
}

func TestShellValidationAndLimits(t *testing.T) {
	h := newExecHarness(t, nil)
	sha := strings.Repeat("a", 64)
	bad := map[string]ShellInvoke{
		"empty command":   sinv("b1", ""),
		"too long":        sinv("b2", strings.Repeat("x", MaxShellCommandBytes+1)),
		"NUL":             sinv("b3", "echo \x00"),
		"invalid utf8":    sinv("b4", "echo \xff"),
		"dotdot":          sinv("b5", "true", ShellFile{Path: "../x", SHA256: sha}),
		"absolute":        sinv("b6", "true", ShellFile{Path: "/etc/passwd", SHA256: sha}),
		"unclean":         sinv("b7", "true", ShellFile{Path: "a//b", SHA256: sha}),
		"duplicate":       sinv("b8", "true", ShellFile{Path: "a", SHA256: sha}, ShellFile{Path: "a", SHA256: sha}),
		"file under file": sinv("b9", "true", ShellFile{Path: "a", SHA256: sha}, ShellFile{Path: "a/b", SHA256: sha}),
		"bad sha":         sinv("b10", "true", ShellFile{Path: "a", SHA256: "x"}),
	}
	for name, in := range bad {
		if r := h.shell(t, in); r.Status != 400 || r.Code != "invalid_request" {
			t.Errorf("%s: %d %s", name, r.Status, r.Code)
		}
	}
	if len(h.store.calls) != 0 {
		t.Fatalf("rejected shell commands must not be journaled")
	}
	in := sinv("w1", "sleep 1")
	in.WallMs = 10_000_000
	v := parseExecResult(t, h.shell(t, in))
	if v.Limits.WallMs != 300_000 {
		t.Fatalf("wall_ms = %d, want capped 300000", v.Limits.WallMs)
	}
}

func TestShellNotConfigured(t *testing.T) {
	h := newHarness(t, testLimits(), newAdapter("p1"))
	r, err := h.c.ExecShell(context.Background(), sinv("x", "true"))
	if err != nil || r.Status != 404 || r.Code != CodeEndpointNotConfigured {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestShellStagingFailureIsExecEnvUnavailable(t *testing.T) {
	h := newExecHarness(t, nil)
	missing := strings.Repeat("b", 64) // not in the blob store
	r := h.shell(t, sinv("m1", "true", ShellFile{Path: "a", SHA256: missing}))
	if r.Code != CodeExecEnvUnavailable {
		t.Fatalf("missing blob = %d %s", r.Status, r.Code)
	}
	if n := h.envs.heldCount(); n != 0 {
		t.Fatalf("%d environments still held", n)
	}
}

// The wrapper's staging marker at the start of stdout is stripped and reported as workspace_staged; without it the
// command's output is untouched and workspace_staged is false. /v1/exec results never carry the member.
func TestShellStagedMarker(t *testing.T) {
	h := newExecHarness(t, nil, execScript{stdout: string(StagedMarker) + "ok\n"}, execScript{stdout: "no marker\n"},
		execScript{stdout: string(StagedMarker)})
	for _, tc := range []struct {
		id, stdout string
		staged     bool
	}{{"m1", "ok\n", true}, {"m2", "no marker\n", false}} {
		var v struct {
			Stdout string `json:"stdout"`
			Staged *bool  `json:"workspace_staged"`
		}
		if err := json.Unmarshal(h.shell(t, sinv(tc.id, "true")).Body, &v); err != nil {
			t.Fatal(err)
		}
		if v.Stdout != tc.stdout || v.Staged == nil || *v.Staged != tc.staged {
			t.Fatalf("%s: stdout %q staged %v", tc.id, v.Stdout, v.Staged)
		}
	}
	r := h.exec(t, xinv("x1", execBody("print(1)", "")))
	if strings.Contains(string(r.Body), "workspace_staged") {
		t.Fatalf("/v1/exec result mentions workspace_staged: %s", r.Body)
	}
}
