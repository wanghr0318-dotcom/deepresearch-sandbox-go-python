package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// ---- fakes ----

type memBlobs struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (b *memBlobs) Put(_ context.Context, r io.Reader) (blob.Ref, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return blob.Ref{}, err
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	b.mu.Lock()
	b.m[sha] = data
	b.mu.Unlock()
	return blob.Ref{SHA256: sha, Size: int64(len(data))}, nil
}

func (b *memBlobs) Open(sha string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.m[sha]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(d)), nil
}

func (b *memBlobs) drop(sha string) {
	b.mu.Lock()
	delete(b.m, sha)
	b.mu.Unlock()
}

type fakeAccess struct{ revoked bool }

func (a *fakeAccess) CheckAccess(context.Context, string, string, string) (call.Result, error) {
	if a.revoked {
		return call.Result{Status: 403, Code: "access_revoked"}, nil
	}
	return call.Result{}, nil
}

type fakeTasks struct {
	mu       sync.Mutex
	terminal map[string]bool
}

func (f *fakeTasks) TaskTerminal(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminal[id], nil
}

// shellRun is what the fake exec "does": given the staged files it returns the /out snapshot.
type shellRun func(cmd string, files map[string]string) (status string, out map[string]string, exec map[string]bool, skipped []string)

type fakeExec struct {
	t     *testing.T
	blobs *memBlobs
	mu    sync.Mutex
	calls []call.ShellInvoke
	run   shellRun
	res   *call.Result // non-nil: returned as is (rejection)
	gate  chan struct{}
	seen  map[string]call.Result // journal replay by call id
	// unstaged: the wrapper did not signal a complete staging (workspace_staged false)
	unstaged bool
}

func (f *fakeExec) ExecShell(_ context.Context, in call.ShellInvoke) (call.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, in)
	if r, ok := f.seen[in.CallID]; ok {
		f.mu.Unlock()
		r.Replayed = true
		return r, nil
	}
	f.mu.Unlock()
	if f.gate != nil {
		<-f.gate
	}
	if f.res != nil {
		return *f.res, nil
	}
	files := map[string]string{}
	for _, sf := range in.Files {
		rc, err := f.blobs.Open(sf.SHA256)
		if err != nil {
			f.t.Fatalf("staged blob %s missing", sf.Path)
		}
		b, _ := io.ReadAll(rc)
		files[sf.Path] = string(b)
	}
	status, out, ex, skipped := f.run(in.Command, files)
	type o struct {
		Path       string `json:"path"`
		SHA256     string `json:"sha256"`
		Size       int64  `json:"size"`
		Executable bool   `json:"executable,omitempty"`
	}
	type s struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	}
	body := map[string]any{"status": status, "exit": map[string]int{"code": 0, "signal": 0}, "stdout": "ran " + in.Command,
		"outputs": []o{}, "skipped_outputs": []s{}, "workspace_staged": !f.unstaged}
	var outs []o
	for p, c := range out {
		ref, _ := f.blobs.Put(context.Background(), strings.NewReader(c))
		outs = append(outs, o{p, ref.SHA256, ref.Size, ex[p]})
	}
	if outs != nil {
		body["outputs"] = outs
	}
	var sk []s
	for _, p := range skipped {
		sk = append(sk, s{p, "symlink"})
	}
	if sk != nil {
		body["skipped_outputs"] = sk
	}
	b, _ := json.Marshal(body)
	ref, _ := f.blobs.Put(context.Background(), bytes.NewReader(b))
	r := call.Result{Status: 200, Body: b, BlobSHA256: ref.SHA256}
	f.mu.Lock()
	f.seen[in.CallID] = r
	f.mu.Unlock()
	return r, nil
}

type harness struct {
	m     *Manager
	dir   string
	blobs *memBlobs
	exec  *fakeExec
	acc   *fakeAccess
	tasks *fakeTasks
	now   time.Time
	mu    sync.Mutex
}

func newH(t *testing.T) *harness {
	t.Helper()
	h := &harness{dir: t.TempDir(), blobs: &memBlobs{m: map[string][]byte{}}, acc: &fakeAccess{},
		tasks: &fakeTasks{terminal: map[string]bool{}}, now: time.Unix(1_800_000_000, 0)}
	h.exec = &fakeExec{t: t, blobs: h.blobs, seen: map[string]call.Result{},
		run: func(_ string, files map[string]string) (string, map[string]string, map[string]bool, []string) {
			return "completed", files, nil, nil
		}}
	h.m = h.newManager(t)
	return h
}

func (h *harness) newManager(t *testing.T) *Manager {
	t.Helper()
	m, err := New(Config{Dir: h.dir, Exec: h.exec, Access: h.acc, Blobs: h.blobs, Tasks: h.tasks,
		Now: func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.now }, MaxFiles: 4, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

func req(body string) Request {
	return Request{TaskID: "t1", AttemptID: "a1", Body: []byte(body)}
}

func do(t *testing.T, f func(context.Context, Request) (call.Result, error), r Request) (call.Result, map[string]any) {
	t.Helper()
	res, err := f(context.Background(), r)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	var v map[string]any
	if res.Status == 200 {
		if err := json.Unmarshal(res.Body, &v); err != nil {
			t.Fatalf("body %s: %v", res.Body, err)
		}
	}
	return res, v
}

func wantCode(t *testing.T, res call.Result, status int, code string) {
	t.Helper()
	if res.Status != status || res.Code != code {
		t.Fatalf("got %d %s, want %d %s", res.Status, res.Code, status, code)
	}
}

// ---- tests ----

func TestValidPath(t *testing.T) {
	good := []string{"a", "a/b.txt", ".hidden", "dir/.git/config", "名字.txt", "a b"}
	bad := []string{"", ".", "..", "../x", "a/../b", "/abs", "a//b", "./a", "a/", "a\\b", "a\x00b", "a/./b",
		strings.Repeat("a", 513), strings.Repeat("a/", 32) + "a", "\xff",
		// Unicode control (Cc, incl. C1) and format (Cf) characters: bidi overrides/isolates, zero-width, BOM.
		"a\u0085b", "a\u202eb", "a\u2066b", "a\u200bb", "\ufeffa", "a\u00adb"}
	for _, p := range good {
		if !ValidPath(p) {
			t.Errorf("ValidPath(%q) = false", p)
		}
	}
	for _, p := range bad {
		if ValidPath(p) {
			t.Errorf("ValidPath(%q) = true", p)
		}
	}
}

func TestWriteReadList(t *testing.T) {
	h := newH(t)
	res, v := do(t, h.m.Write, req(`{"path":"src/calc.py","content":"def add(a, b):\n    return a + b\n"}`))
	if res.Status != 200 || v["created"] != true || v["size"].(float64) != 32 {
		t.Fatalf("write = %d %v", res.Status, v)
	}
	_, v = do(t, h.m.Write, req(`{"path":"run.sh","content":"#!/bin/sh\necho hi\n","executable":true}`))
	ws := v["workspace"].(map[string]any)
	if ws["files"].(float64) != 2 {
		t.Fatalf("workspace %v", ws)
	}
	// Overwrite keeps the executable bit unless given.
	_, v = do(t, h.m.Write, req(`{"path":"run.sh","content":"#!/bin/sh\necho bye\n"}`))
	if v["created"] != false {
		t.Fatalf("overwrite %v", v)
	}
	_, v = do(t, h.m.List, req(`{}`))
	ents := v["entries"].([]any)
	if len(ents) != 2 || ents[0].(map[string]any)["name"] != "run.sh" || ents[0].(map[string]any)["executable"] != true ||
		ents[1].(map[string]any)["name"] != "src" || ents[1].(map[string]any)["type"] != "dir" {
		t.Fatalf("list root %v", ents)
	}
	_, v = do(t, h.m.List, req(`{"path":"","recursive":true}`))
	if ents := v["entries"].([]any); len(ents) != 2 || ents[1].(map[string]any)["name"] != "src/calc.py" {
		t.Fatalf("list recursive %v", ents)
	}
	_, v = do(t, h.m.Read, req(`{"path":"src/calc.py","start_line":2,"max_lines":5}`))
	if v["content"] != "    return a + b\n" || v["total_lines"].(float64) != 2 || v["end_line"].(float64) != 2 || v["truncated"] != false {
		t.Fatalf("read %v", v)
	}
	_, v = do(t, h.m.Read, req(`{"path":"src/calc.py","max_lines":1}`))
	if v["content"] != "def add(a, b):\n" || v["truncated"] != true {
		t.Fatalf("read 1 line %v", v)
	}
	res, _ = do(t, h.m.Read, req(`{"path":"src"}`))
	wantCode(t, res, 400, CodeIsDirectory)
	res, _ = do(t, h.m.Read, req(`{"path":"nope"}`))
	wantCode(t, res, 404, CodeNotFound)
	res, _ = do(t, h.m.List, req(`{"path":"run.sh"}`))
	wantCode(t, res, 400, CodeNotDirectory)
	res, _ = do(t, h.m.List, req(`{"path":"nodir"}`))
	wantCode(t, res, 404, CodeNotFound)
	// file vs directory conflicts
	res, _ = do(t, h.m.Write, req(`{"path":"src","content":"x"}`))
	wantCode(t, res, 409, CodePathConflict)
	res, _ = do(t, h.m.Write, req(`{"path":"run.sh/x","content":"x"}`))
	wantCode(t, res, 409, CodePathConflict)
	// binary content
	h.m.mu.Lock()
	w := h.m.wss["t1"]
	h.m.mu.Unlock()
	ref, _ := h.blobs.Put(context.Background(), strings.NewReader("\xff\xfe"))
	w.mu.Lock()
	next := w.st.clone()
	next.Files["bin"] = entry{SHA256: ref.SHA256, Size: 2}
	if err := h.m.commit(w, next); err != nil {
		t.Fatal(err)
	}
	w.mu.Unlock()
	_, v = do(t, h.m.Read, req(`{"path":"bin"}`))
	if v["binary"] != true || v["content"] != "" {
		t.Fatalf("binary read %v", v)
	}
}

// Escape attempts are rejected by path validation before the workspace is touched.
func TestPathEscapesRejected(t *testing.T) {
	h := newH(t)
	for _, p := range []string{"../etc/passwd", "/etc/passwd", "a/../../x", "a\\..\\x", "./x", "x/", "a//b", "..", "."} {
		b, _ := json.Marshal(map[string]any{"path": p, "content": "x"})
		res, _ := do(t, h.m.Write, req(string(b)))
		wantCode(t, res, 400, CodeInvalidPath)
		b, _ = json.Marshal(map[string]any{"path": p})
		res, _ = do(t, h.m.Read, req(string(b)))
		wantCode(t, res, 400, CodeInvalidPath)
		if p != "." {
			res, _ = do(t, h.m.List, req(string(b)))
			wantCode(t, res, 400, CodeInvalidPath)
		}
	}
	if _, err := os.Stat(filepath.Join(h.dir, fileName("t1"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected requests must not create the workspace: %v", err)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newH(t)
	for name, tc := range map[string]struct {
		f    func(context.Context, Request) (call.Result, error)
		body string
		code string
	}{
		"unknown field": {h.m.Write, `{"path":"a","content":"x","mode":7}`, CodeUnsupportedField},
		"no content":    {h.m.Write, `{"path":"a"}`, CodeInvalidRequest},
		"not json":      {h.m.Read, `nope`, CodeInvalidRequest},
		"start 0":       {h.m.Read, `{"path":"a","start_line":0}`, CodeInvalidRequest},
		"too many":      {h.m.Read, `{"path":"a","max_lines":2001}`, CodeInvalidRequest},
		"empty cmd":     {h.m.Exec, `{"command":""}`, CodeInvalidRequest},
		"bad timeout":   {h.m.Exec, `{"command":"ls","timeout_ms":0}`, CodeInvalidRequest},
	} {
		res, _ := do(t, tc.f, req(tc.body))
		if res.Status != 400 || res.Code != tc.code {
			t.Errorf("%s: %d %s", name, res.Status, res.Code)
		}
	}
	big, _ := json.Marshal(map[string]any{"path": "a", "content": strings.Repeat("x", MaxWriteBytes+1)})
	res, _ := do(t, h.m.Write, req(string(big)))
	wantCode(t, res, 413, CodeContentTooLarge)
}

func TestLimits(t *testing.T) {
	h := newH(t) // MaxFiles 4, MaxBytes 100
	for i := 0; i < 4; i++ {
		res, _ := do(t, h.m.Write, req(`{"path":"f`+string(rune('0'+i))+`","content":"x"}`))
		if res.Status != 200 {
			t.Fatalf("write %d: %d %s", i, res.Status, res.Code)
		}
	}
	res, _ := do(t, h.m.Write, req(`{"path":"f9","content":"x"}`))
	wantCode(t, res, 409, CodeWorkspaceFull)
	big, _ := json.Marshal(map[string]any{"path": "f0", "content": strings.Repeat("y", 98)})
	res, _ = do(t, h.m.Write, req(string(big)))
	wantCode(t, res, 409, CodeWorkspaceFull)
}

func TestAccessCheckedFirst(t *testing.T) {
	h := newH(t)
	h.acc.revoked = true
	res, _ := do(t, h.m.Write, req(`{"path":"a","content":"x"}`))
	wantCode(t, res, 403, "access_revoked")
	res, _ = do(t, h.m.Exec, Request{TaskID: "t1", AttemptID: "a1", CallID: "c", Body: []byte(`{"command":"ls"}`)})
	wantCode(t, res, 403, "access_revoked")
	if len(h.exec.calls) != 0 {
		t.Fatal("exec must not run without access")
	}
}

func shellReq(callID, cmd string) Request {
	b, _ := json.Marshal(map[string]any{"command": cmd})
	return Request{TaskID: "t1", AttemptID: "a1", CallID: callID, Body: b}
}

func TestExecAppliesSnapshot(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"keep.txt","content":"k"}`))
	do(t, h.m.Write, req(`{"path":"gone.txt","content":"g"}`))
	do(t, h.m.Write, req(`{"path":"mod.txt","content":"1"}`))
	h.exec.run = func(cmd string, files map[string]string) (string, map[string]string, map[string]bool, []string) {
		if files["keep.txt"] != "k" || files["gone.txt"] != "g" {
			t.Errorf("staged files %v", files)
		}
		return "completed", map[string]string{"keep.txt": "k", "mod.txt": "2", "new/out.txt": "result\n", "t.sh": "#!"},
			map[string]bool{"t.sh": true}, []string{"link-to-etc-passwd"}
	}
	res, v := do(t, h.m.Exec, shellReq("orch/ws/1", "python3 test.py"))
	if res.Status != 200 || res.BlobSHA256 == "" || v["stdout"] != "ran python3 test.py" {
		t.Fatalf("exec %d %v", res.Status, v)
	}
	ch := v["changes"].(map[string]any)
	if s := toStrings(ch["added"]); strings.Join(s, ",") != "new/out.txt,t.sh" {
		t.Fatalf("added %v", s)
	}
	if s := toStrings(ch["modified"]); strings.Join(s, ",") != "mod.txt" {
		t.Fatalf("modified %v", s)
	}
	if s := toStrings(ch["deleted"]); strings.Join(s, ",") != "gone.txt" {
		t.Fatalf("deleted %v", s)
	}
	if sk := v["skipped_outputs"].([]any); len(sk) != 1 || sk[0].(map[string]any)["path"] != "link-to-etc-passwd" {
		t.Fatalf("skipped %v", sk)
	}
	// The symlink did not become a workspace file.
	r, _ := do(t, h.m.Read, req(`{"path":"link-to-etc-passwd"}`))
	wantCode(t, r, 404, CodeNotFound)
	_, rv := do(t, h.m.Read, req(`{"path":"new/out.txt"}`))
	if rv["content"] != "result\n" {
		t.Fatalf("read output %v", rv)
	}
	_, lv := do(t, h.m.List, req(`{"recursive":true}`))
	if e := lv["entries"].([]any); len(e) != 4 || e[3].(map[string]any)["name"] != "t.sh" || e[3].(map[string]any)["executable"] != true {
		t.Fatalf("entries %v", e)
	}
	// Exec bit and files go into the next command's staging.
	h.exec.run = func(cmd string, files map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", files, nil, nil
	}
	do(t, h.m.Exec, shellReq("orch/ws/2", "ls"))
	last := h.exec.calls[len(h.exec.calls)-1]
	if len(last.Files) != 4 || last.Files[3].Path != "t.sh" || !last.Files[3].Executable || last.Files[0].Path != "keep.txt" {
		t.Fatalf("staged %+v", last.Files)
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestExecRejectionsLeaveHeadUnchanged(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	h.exec.res = &call.Result{Status: 402, Code: "exec_quota_exhausted"}
	res, _ := do(t, h.m.Exec, shellReq("c1", "rm a"))
	wantCode(t, res, 402, "exec_quota_exhausted")
	h.exec.res = nil
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "cancelled", nil, nil, nil
	}
	_, v := do(t, h.m.Exec, shellReq("c2", "rm a"))
	if ch := v["changes"].(map[string]any); len(toStrings2(ch["deleted"])) != 0 {
		t.Fatalf("non-completed status must not change files: %v", ch)
	}
	_, rv := do(t, h.m.Read, req(`{"path":"a"}`))
	if rv["content"] != "1" {
		t.Fatalf("head changed: %v", rv)
	}
}

func toStrings2(v any) []any { return v.([]any) }

// A retry with the same call id after the head advanced returns the stored response, without running anything.
func TestExecAppliedReplay(t *testing.T) {
	h := newH(t)
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"x": "1"}, nil, nil
	}
	r1, _ := do(t, h.m.Exec, shellReq("c1", "echo 1 > x"))
	n := len(h.exec.calls)
	// Restart: a new Manager on the same directory recovers the head and the applied list.
	m2 := h.newManager(t)
	r2, err := m2.Exec(context.Background(), shellReq("c1", "echo 1 > x"))
	if err != nil || !r2.Replayed || !bytes.Equal(r2.Body, r1.Body) || r2.BlobSHA256 != r1.BlobSHA256 || len(h.exec.calls) != n {
		t.Fatalf("applied replay: %+v %v (calls %d→%d)", r2, err, n, len(h.exec.calls))
	}
	_, v := do(t, m2.Read, req(`{"path":"x"}`))
	if v["content"] != "1" {
		t.Fatalf("recovered head %v", v)
	}
}

// The Manager applies the result even if the client's context ended while the command ran.
func TestExecAppliesAfterClientGone(t *testing.T) {
	h := newH(t)
	h.exec.gate = make(chan struct{})
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"y": "2"}, nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan call.Result, 1)
	go func() {
		r, _ := h.m.Exec(ctx, shellReq("c1", "echo 2 > y"))
		done <- r
	}()
	cancel()
	time.Sleep(10 * time.Millisecond)
	close(h.exec.gate)
	if r := <-done; r.Status != 200 {
		t.Fatalf("exec %+v", r)
	}
	_, v := do(t, h.m.Read, req(`{"path":"y"}`))
	if v["content"] != "2" {
		t.Fatalf("not applied: %v", v)
	}
}

// Operations on one workspace are serialized: a read waits for a running command and sees its result.
func TestOpsSerialized(t *testing.T) {
	h := newH(t)
	h.exec.gate = make(chan struct{})
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"z": "3"}, nil, nil
	}
	go func() { _, _ = h.m.Exec(context.Background(), shellReq("c1", "echo 3 > z")) }()
	for len(h.execCalls()) == 0 {
		time.Sleep(time.Millisecond)
	}
	read := make(chan map[string]any, 1)
	go func() { _, v := do(t, h.m.Read, req(`{"path":"z"}`)); read <- v }()
	select {
	case <-read:
		t.Fatal("read completed while the command was running")
	case <-time.After(30 * time.Millisecond):
	}
	close(h.exec.gate)
	if v := <-read; v["content"] != "3" {
		t.Fatalf("read after exec %v", v)
	}
}

func (h *harness) execCalls() []call.ShellInvoke {
	h.exec.mu.Lock()
	defer h.exec.mu.Unlock()
	return append([]call.ShellInvoke(nil), h.exec.calls...)
}

func TestLifecycle(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	file := filepath.Join(h.dir, fileName("t1"))
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("state file: %v", err)
	}
	// Idle expiry → 410 workspace_expired.
	h.advance(DefaultIdleTimeout + time.Minute)
	if err := h.m.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, _ := do(t, h.m.Read, req(`{"path":"a"}`))
	wantCode(t, res, 410, CodeWorkspaceExpired)
	// Also after a restart.
	m2 := h.newManager(t)
	res, _ = do(t, m2.List, req(`{}`))
	wantCode(t, res, 410, CodeWorkspaceExpired)
	// Terminal task → destroyed (state file removed, also for workspaces of a previous process).
	h.tasks.terminal["t1"] = true
	if err := m2.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state file after destroy: %v", err)
	}
}

func TestRecoveryAndLost(t *testing.T) {
	h := newH(t)
	_, v := do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	sha := v["sha256"].(string)
	m2 := h.newManager(t)
	_, rv := do(t, m2.Read, req(`{"path":"a"}`))
	if rv["content"] != "1" {
		t.Fatalf("recovered %v", rv)
	}
	// A missing blob after restart → lost.
	h.blobs.drop(sha)
	m3 := h.newManager(t)
	res, _ := do(t, m3.Read, req(`{"path":"a"}`))
	wantCode(t, res, 410, CodeWorkspaceLost)
	// A corrupt state file → lost.
	other := Request{TaskID: "t2", AttemptID: "a2", Body: []byte(`{"path":"b","content":"2"}`)}
	do(t, m3.Write, other)
	if err := os.WriteFile(filepath.Join(h.dir, fileName("t2")), []byte("{garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	m4 := h.newManager(t)
	res, _ = do(t, m4.List, Request{TaskID: "t2", AttemptID: "a2"})
	wantCode(t, res, 410, CodeWorkspaceLost)
}

// The idle clock of a workspace that is only read is persisted (at most once a minute), so a restart does not
// make it look idle since its last change.
func TestUsedAtPersistedForReads(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	h.advance(DefaultIdleTimeout - 30*time.Minute)
	if res, _ := do(t, h.m.Read, req(`{"path":"a"}`)); res.Status != 200 {
		t.Fatalf("read %+v", res)
	}
	m2 := h.newManager(t) // restart: only the state file is left
	h.advance(time.Hour)  // 2.5 h after the write, 1 h after the read
	if err := m2.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if res, _ := do(t, m2.Read, req(`{"path":"a"}`)); res.Status != 200 {
		t.Fatalf("read after restart %+v (expired by the unpersisted idle clock)", res)
	}
}

// Sweep ages leftovers by the configured clock, not the wall clock.
func TestSweepUsesConfiguredClock(t *testing.T) {
	h := newH(t)
	tmp := filepath.Join(h.dir, ".tmp-leftover")
	if err := os.WriteFile(tmp, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, h.now, h.now); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("fresh leftover removed: %v", err)
	}
	h.advance(2 * time.Minute)
	if err := h.m.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old leftover kept: %v", err)
	}
}
