package k8s

import (
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
