package k8s

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// TestFailedSwingLosesNoData: when moving the workspace into a slot fails half-way, the entries already moved
// are moved back, the workspace path stays a directory, and nothing is lost; gc keeps a slot that still
// holds data of a workspace whose path exists (interrupted swing).
func TestFailedSwingLosesNoData(t *testing.T) {
	root := t.TempDir()
	s := slots{hostRoot: filepath.Join(root, "slots"), nodeRoot: "/n", uid: -1}
	_ = os.MkdirAll(s.hostRoot, 0o711)
	ws := filepath.Join(root, "workspaces", "t1")
	_ = os.MkdirAll(filepath.Join(ws, "b"), 0o700)
	_ = os.WriteFile(filepath.Join(ws, "a.txt"), []byte("A"), 0o600)
	_ = os.WriteFile(filepath.Join(ws, "b", "inner.txt"), []byte("B"), 0o600)
	_ = os.WriteFile(filepath.Join(ws, "c.txt"), []byte("C"), 0o600)
	if err := s.create("p1"); err != nil {
		t.Fatal(err)
	}
	// A non-empty directory "b" already in the slot makes rename(ws/b, slot/b) fail after "a.txt" moved.
	_ = os.MkdirAll(filepath.Join(s.workspace("p1"), "b", "blocker"), 0o700)
	if err := s.bind("p1", provider.Mounts{Workspace: ws}); err == nil {
		t.Fatal("swing should fail")
	}
	fi, err := os.Lstat(ws)
	if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("workspace should still be a directory: %v %v", fi, err)
	}
	for name, want := range map[string]string{"a.txt": "A", "b/inner.txt": "B", "c.txt": "C"} {
		if b, err := os.ReadFile(filepath.Join(ws, name)); err != nil || string(b) != want {
			t.Fatalf("%s after failed swing: %q %v", name, b, err)
		}
	}
	// The slot still has the blocker (non-empty) and its back-reference names an existing workspace: gc keeps it.
	if removed, _ := s.gc(map[string]bool{}, 0); len(removed) != 0 {
		t.Fatalf("gc removed a slot with possible workspace data: %v", removed)
	}
	// Once the workspace path is gone, the slot is collectable.
	_ = os.RemoveAll(ws)
	if removed, _ := s.gc(map[string]bool{}, 0); len(removed) != 1 {
		t.Fatalf("gc after workspace deletion: %v", removed)
	}
}

// TestInterruptedSwingMergedBack: a process that died mid-swing left some workspace entries in the new slot while
// the workspace path still resolves to the old location. recoverSwings (startup) moves them back, for both shapes
// of the workspace path (a directory on the first swing, a symlink to the previous slot later); a name that
// already exists in the workspace is left in the slot. Afterwards gc can collect the emptied slot.
func TestInterruptedSwingMergedBack(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	root := t.TempDir()
	s := slots{hostRoot: filepath.Join(root, "slots"), nodeRoot: "/n", uid: -1}
	_ = os.MkdirAll(s.hostRoot, 0o711)
	ws := filepath.Join(root, "workspaces", "t1")
	_ = os.MkdirAll(ws, 0o700)
	_ = os.WriteFile(filepath.Join(ws, "a.txt"), []byte("A"), 0o600)
	_ = os.WriteFile(filepath.Join(ws, "b.txt"), []byte("B"), 0o600)

	// 1. First swing (workspace is a directory) interrupted after a.txt moved into p1.
	if err := s.create("p1"); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(s.hostDir("p1"), workspaceRef), []byte(ws), 0o600)
	if err := os.Rename(filepath.Join(ws, "a.txt"), filepath.Join(s.workspace("p1"), "a.txt")); err != nil {
		t.Fatal(err)
	}
	if n := s.recoverSwings(log); n != 1 {
		t.Fatalf("recovered %d entries, want 1", n)
	}
	for name, want := range map[string]string{"a.txt": "A", "b.txt": "B"} {
		if b, err := os.ReadFile(filepath.Join(ws, name)); err != nil || string(b) != want {
			t.Fatalf("%s after recovery: %q %v", name, b, err)
		}
	}
	if removed, _ := s.gc(map[string]bool{}, 0); len(removed) != 1 || removed[0] != "p1" {
		t.Fatalf("gc after recovery: %v", removed)
	}

	// 2. A complete swing into p2, then a swing into p3 interrupted after a.txt moved; p3 also holds a name that
	// exists in the workspace (left there).
	for _, p := range []string{"p2", "p3"} {
		if err := s.create(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.bind("p2", provider.Mounts{Workspace: ws}); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(s.hostDir("p3"), workspaceRef), []byte(ws), 0o600)
	if err := os.Rename(filepath.Join(s.workspace("p2"), "a.txt"), filepath.Join(s.workspace("p3"), "a.txt")); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(s.workspace("p3"), "b.txt"), []byte("other"), 0o600)
	if n := s.recoverSwings(log); n != 1 {
		t.Fatalf("recovered %d entries, want 1", n)
	}
	if b, err := os.ReadFile(filepath.Join(ws, "a.txt")); err != nil || string(b) != "A" {
		t.Fatalf("a.txt through the workspace symlink: %q %v", b, err)
	}
	if b, _ := os.ReadFile(filepath.Join(ws, "b.txt")); string(b) != "B" {
		t.Fatalf("b.txt overwritten: %q", b)
	}
	if _, err := os.Stat(filepath.Join(s.workspace("p3"), "b.txt")); err != nil {
		t.Fatalf("conflicting entry should stay in the slot: %v", err)
	}
	// p2 is where the workspace lives: never touched.
	if n := s.recoverSwings(log); n != 0 {
		t.Fatalf("second recovery moved %d entries", n)
	}
}

// TestSwingRemovedDirectoryRecovered: the first swing died after removing the workspace directory and before creating
// the symlink (all entries are in the slot). Startup recreates the symlink; a workspace whose parent directory was
// deleted (closed session) is left alone and gc collects its slot.
func TestSwingRemovedDirectoryRecovered(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	root := t.TempDir()
	s := slots{hostRoot: filepath.Join(root, "slots"), nodeRoot: "/n", uid: -1}
	_ = os.MkdirAll(s.hostRoot, 0o711)
	ws := filepath.Join(root, "sessions", "s1", "workspace")
	_ = os.MkdirAll(filepath.Dir(ws), 0o700)
	if err := s.create("p1"); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(s.hostDir("p1"), workspaceRef), []byte(ws), 0o600)
	_ = os.WriteFile(filepath.Join(s.workspace("p1"), "a.txt"), []byte("A"), 0o600)
	if n := s.recoverSwings(log); n != 1 {
		t.Fatalf("repairs %d, want 1", n)
	}
	if dst, err := os.Readlink(ws); err != nil || dst != s.workspace("p1") {
		t.Fatalf("workspace symlink: %q %v", dst, err)
	}
	if b, err := os.ReadFile(filepath.Join(ws, "a.txt")); err != nil || string(b) != "A" {
		t.Fatalf("a.txt through the symlink: %q %v", b, err)
	}
	if n := s.recoverSwings(log); n != 0 {
		t.Fatalf("second recovery made %d repairs", n)
	}

	// A closed session: its whole directory is gone. Nothing is recreated; gc removes the slot.
	if err := os.RemoveAll(filepath.Join(root, "sessions", "s1")); err != nil {
		t.Fatal(err)
	}
	if n := s.recoverSwings(log); n != 0 {
		t.Fatalf("recreated a deleted workspace (%d repairs)", n)
	}
	if _, err := os.Lstat(ws); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace path exists again: %v", err)
	}
	if removed, _ := s.gc(map[string]bool{}, 0); len(removed) != 1 {
		t.Fatalf("gc after session deletion: %v", removed)
	}
}
