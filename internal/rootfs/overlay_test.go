//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/testutil"
)

func TestOverlayWritesLandInUpper(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	o := Overlay{
		Lower:  filepath.Join(base, "lower"),
		Upper:  filepath.Join(base, "upper"),
		Work:   filepath.Join(base, "work"),
		Merged: filepath.Join(base, "merged"),
	}
	for _, d := range []string{o.Lower, o.Upper, o.Work, o.Merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	// lower 里放一个只读文件，验证它在 merged 里可见。
	if err := os.WriteFile(filepath.Join(o.Lower, "from-lower.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("写 lower 文件: %v", err)
	}

	if err := Mount(o); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	defer Unmount(o.Merged)

	// lower 的内容应当在 merged 里可见
	if b, err := os.ReadFile(filepath.Join(o.Merged, "from-lower.txt")); err != nil {
		t.Fatalf("merged 里读不到 lower 的文件: %v", err)
	} else if string(b) != "hello" {
		t.Fatalf("内容 = %q, want %q", b, "hello")
	}

	// 往 merged 里写，文件应落在 upper 而不是 lower
	if err := os.WriteFile(filepath.Join(o.Merged, "new.txt"), []byte("world"), 0o644); err != nil {
		t.Fatalf("写 merged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(o.Upper, "new.txt")); err != nil {
		t.Fatalf("新文件未落在 upper 层: %v", err)
	}
	if _, err := os.Stat(filepath.Join(o.Lower, "new.txt")); !os.IsNotExist(err) {
		t.Fatal("新文件不应出现在 lower 层——lower 必须保持只读")
	}
}

func TestUnmountIsIdempotent(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	dir := t.TempDir()
	if err := Unmount(dir); err != nil {
		t.Fatalf("对未挂载路径 Unmount 应当成功返回，实际: %v", err)
	}
}

func TestBindMount(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("bind"), 0o644); err != nil {
		t.Fatalf("写 src 文件: %v", err)
	}

	if err := BindMount(src, dst); err != nil {
		t.Fatalf("BindMount: %v", err)
	}
	defer Unmount(dst)

	b, err := os.ReadFile(filepath.Join(dst, "f.txt"))
	if err != nil {
		t.Fatalf("bind 目标读不到文件: %v", err)
	}
	if string(b) != "bind" {
		t.Fatalf("内容 = %q, want %q", b, "bind")
	}
}
