package workspace

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
)

// Review round 1 (PR #28): staging-compatible paths, staging signal, write metering, bounded reads, replay
// fingerprint, idle clock on reads, destroy tombstone, transient errors.

func (h *harness) managerWith(t *testing.T, mod func(*Config)) *Manager {
	t.Helper()
	cfg := Config{Dir: h.dir, Exec: h.exec, Access: h.acc, Blobs: h.blobs, Tasks: h.tasks,
		Now: func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.now }, MaxFiles: 4, MaxBytes: 100}
	mod(&cfg)
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m
}

func TestValidPathMatchesStaging(t *testing.T) {
	for _, p := range []string{".agentbox", ".agentbox/cmd.sh", "a\tb", "a\nb", "a\x7fb", "x/" + strings.Repeat("n", 256),
		strings.Repeat("n", 256)} {
		if ValidPath(p) {
			t.Errorf("ValidPath(%q) = true", p)
		}
	}
	for _, p := range []string{"src/.agentbox/x", strings.Repeat("n", 255), "x/" + strings.Repeat("n", 255)} {
		if !ValidPath(p) {
			t.Errorf("ValidPath(%q) = false", p)
		}
	}
	h := newH(t)
	res, _ := do(t, h.m.Write, req(`{"path":".agentbox/x","content":"1"}`))
	wantCode(t, res, 400, CodeInvalidPath)
}

// Outputs a command creates under invalid names are skipped (reason invalid_path) and never enter the manifest, so
// the next command can still be staged.
func TestInvalidOutputsSkipped(t *testing.T) {
	h := newH(t)
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"ok.txt": "1", ".agentbox/evil": "x", "bad\tname": "y"}, nil, nil
	}
	_, v := do(t, h.m.Exec, shellReq("c1", "make files"))
	var reasons []string
	for _, s := range v["skipped_outputs"].([]any) {
		m := s.(map[string]any)
		reasons = append(reasons, m["path"].(string)+"="+m["reason"].(string))
	}
	if strings.Join(reasons, ",") != ".agentbox/evil=invalid_path,bad\tname=invalid_path" &&
		strings.Join(reasons, ",") != "bad\tname=invalid_path,.agentbox/evil=invalid_path" {
		t.Fatalf("skipped %v", reasons)
	}
	if ws := v["workspace"].(map[string]any); ws["files"].(float64) != 1 {
		t.Fatalf("workspace %v", ws)
	}
	h.exec.run = func(_ string, files map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", files, nil, nil
	}
	if res, _ := do(t, h.m.Exec, shellReq("c2", "ls")); res.Status != 200 {
		t.Fatalf("next command %d %s", res.Status, res.Code)
	}
}

// Without the wrapper's staging signal the snapshot is not applied (e.g. a near-full workspace that did not fit).
func TestStagingFailureKeepsHead(t *testing.T) {
	h := newH(t) // MaxBytes 100
	do(t, h.m.Write, req(`{"path":"big","content":"`+strings.Repeat("x", 99)+`"}`))
	h.exec.unstaged = true
	h.exec.run = func(string, map[string]string) (string, map[string]string, map[string]bool, []string) {
		return "completed", map[string]string{"partial": "1"}, nil, nil
	}
	res, _ := do(t, h.m.Exec, shellReq("c1", "true"))
	wantCode(t, res, 503, CodeStagingFailed)
	_, v := do(t, h.m.List, req(`{"recursive":true}`))
	if e := v["entries"].([]any); len(e) != 1 || e[0].(map[string]any)["name"] != "big" {
		t.Fatalf("head changed: %v", e)
	}
}

func TestDelete(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"d/a","content":"1"}`))
	do(t, h.m.Write, req(`{"path":"d/b","content":"2"}`))
	do(t, h.m.Write, req(`{"path":"c","content":"3"}`))
	res, v := do(t, h.m.Write, req(`{"path":"d","delete":true}`))
	if res.Status != 200 || len(v["deleted"].([]any)) != 2 {
		t.Fatalf("delete dir %d %v", res.Status, v)
	}
	res, _ = do(t, h.m.Write, req(`{"path":"c","delete":true}`))
	if res.Status != 200 {
		t.Fatalf("delete file %d", res.Status)
	}
	res, _ = do(t, h.m.Write, req(`{"path":"c","delete":true}`))
	wantCode(t, res, 404, CodeNotFound)
	res, _ = do(t, h.m.Write, req(`{"path":"c","delete":true,"content":"x"}`))
	wantCode(t, res, 400, CodeInvalidRequest)
}

func TestWriteMetering(t *testing.T) {
	h := newH(t)
	m := h.managerWith(t, func(c *Config) { c.MaxWriteOps, c.MaxWrittenBytes, c.MaxFiles = 3, 10, 100 })
	for i := 0; i < 2; i++ {
		if res, _ := do(t, m.Write, req(`{"path":"a","content":"1234"}`)); res.Status != 200 {
			t.Fatalf("write %d: %d", i, res.Status)
		}
	}
	res, _ := do(t, m.Write, req(`{"path":"a","content":"12345"}`)) // 4+4+5 > 10 bytes
	wantCode(t, res, 429, CodeWriteQuota)
	do(t, m.Write, req(`{"path":"a","content":"1"}`)) // third op
	res, _ = do(t, m.Write, req(`{"path":"b","content":""}`))
	wantCode(t, res, 429, CodeWriteQuota)
	// Persisted: a restarted Manager still refuses.
	m2 := h.managerWith(t, func(c *Config) { c.MaxWriteOps, c.MaxWrittenBytes, c.MaxFiles = 3, 10, 100 })
	res, _ = do(t, m2.Write, req(`{"path":"b","content":""}`))
	wantCode(t, res, 429, CodeWriteQuota)
}

func TestReadWindowIsBounded(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 50_000; i++ {
		b.WriteString("line ")
		b.WriteString(strings.Repeat("x", i%7))
		b.WriteString("\n")
	}
	text := b.String()
	w, err := readWindow(strings.NewReader(text), 49_999, 10, MaxReadBytes)
	if err != nil || w.lines != 2 || w.truncated || w.total == nil || *w.total != 50_000 {
		t.Fatalf("tail window %+v %v", w, err)
	}
	w, _ = readWindow(strings.NewReader(text), 1, 5, MaxReadBytes)
	if w.lines != 5 || !w.truncated || w.total != nil {
		t.Fatalf("head window %+v", w)
	}
	long := strings.Repeat("é", MaxReadBytes) // one line longer than the byte cap, multi-byte runes
	w, _ = readWindow(strings.NewReader(long), 1, 10, MaxReadBytes)
	if !w.truncated || w.binary || len(w.data) > MaxReadBytes {
		t.Fatalf("long line %+v", w.truncated)
	}
	// Binary detection looks at the window only: a NUL after it does not make the window binary.
	w, _ = readWindow(strings.NewReader("a\nb\n\x00\x01"), 1, 2, MaxReadBytes)
	if w.binary || string(w.data) != "a\nb\n" {
		t.Fatalf("window %q binary=%v", w.data, w.binary)
	}
	w, _ = readWindow(strings.NewReader("x\x00y\n"), 1, 2, MaxReadBytes)
	if !w.binary {
		t.Fatal("NUL in window not binary")
	}
	w, _ = readWindow(strings.NewReader("a\nb"), 5, 2, MaxReadBytes)
	if w.lines != 0 || w.total == nil || *w.total != 2 {
		t.Fatalf("past end %+v", w)
	}
	// Through the API.
	h := newH(t)
	m := h.managerWith(t, func(c *Config) { c.MaxBytes = 4 << 20 })
	ref, _ := h.blobs.Put(context.Background(), strings.NewReader(text))
	do(t, m.Write, req(`{"path":"seed","content":"x"}`))
	w0 := m.get("t1")
	w0.mu.Lock()
	next := w0.st.clone()
	next.Files["big.txt"] = entry{SHA256: ref.SHA256, Size: ref.Size}
	if err := m.commit(w0, next); err != nil {
		t.Fatal(err)
	}
	w0.mu.Unlock()
	_, v := do(t, m.Read, req(`{"path":"big.txt","start_line":3,"max_lines":2}`))
	if v["content"] != "line xxx\nline xxxx\n" || v["truncated"] != true || v["total_lines"] != nil || v["end_line"].(float64) != 4 {
		t.Fatalf("read %v", v)
	}
}

func TestAppliedReplayRequiresSameRequest(t *testing.T) {
	h := newH(t)
	do(t, h.m.Exec, shellReq("c1", "echo 1"))
	res, _ := do(t, h.m.Exec, shellReq("c1", "echo 2"))
	wantCode(t, res, 409, CodeFingerprintMismatch)
	res, _ = do(t, h.m.Exec, shellReq("c1", "echo 1"))
	if !res.Replayed {
		t.Fatalf("same request should replay: %+v", res)
	}
}

func TestReadsKeepWorkspaceAlive(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	for i := 0; i < 3; i++ {
		h.advance(DefaultIdleTimeout * 2 / 3)
		do(t, h.m.Read, req(`{"path":"a"}`))
		if err := h.m.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if res, _ := do(t, h.m.Read, req(`{"path":"a"}`)); res.Status != 200 {
		t.Fatalf("expired although read regularly: %d %s", res.Status, res.Code)
	}
}

// A request that was waiting for the workspace while the sweeper destroyed it gets 410, not a fresh workspace.
func TestDestroyTombstone(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	w := h.m.get("t1")
	w.mu.Lock()
	done := make(chan call.Result, 1)
	go func() { r, _ := h.m.Read(context.Background(), req(`{"path":"a"}`)); done <- r }()
	time.Sleep(20 * time.Millisecond)
	h.m.destroy(w, w.st, filepath.Join(h.dir, fileName("t1")))
	w.mu.Unlock()
	wantCode(t, <-done, 410, CodeWorkspaceDestroyed)
	if _, err := os.Stat(filepath.Join(h.dir, fileName("t1"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destroyed workspace was recreated: %v", err)
	}
}

type flakyBlobs struct {
	*memBlobs
	err error
}

func (f *flakyBlobs) Open(sha string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.memBlobs.Open(sha)
}

// Transient I/O errors are errors, not "lost".
func TestTransientErrorsDoNotLoseWorkspace(t *testing.T) {
	h := newH(t)
	do(t, h.m.Write, req(`{"path":"a","content":"1"}`))
	fb := &flakyBlobs{memBlobs: h.blobs, err: errors.New("disk hiccup")}
	m2 := h.managerWith(t, func(c *Config) { c.Blobs = fb })
	if _, err := m2.Read(context.Background(), req(`{"path":"a"}`)); err == nil {
		t.Fatal("transient blob error should be returned")
	}
	fb.err = nil
	_, v := do(t, m2.Read, req(`{"path":"a"}`))
	if v["content"] != "1" {
		t.Fatalf("after the hiccup: %v", v)
	}
	// An unreadable state file (here: a directory) is an error, not corruption.
	other := filepath.Join(h.dir, fileName("t9"))
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.List(context.Background(), Request{TaskID: "t9", AttemptID: "a9"}); err == nil {
		t.Fatal("unreadable state file should be an error")
	}
	if st, err := os.Stat(other); err != nil || !st.IsDir() {
		t.Fatal("state path must be left alone")
	}

}
