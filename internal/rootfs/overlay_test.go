//go:build linux

package rootfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	// lower 里再放一个文件，之后要通过 merged 修改它，验证 copy-up-on-write。
	if err := os.WriteFile(filepath.Join(o.Lower, "existing.txt"), []byte("original"), 0o644); err != nil {
		t.Fatalf("写 lower existing 文件: %v", err)
	}

	if err := Mount(o); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	t.Cleanup(func() {
		if err := Unmount(o.Merged); err != nil {
			t.Errorf("清理 %s 失败: %v", o.Merged, err)
		}
	})

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

	// 修改一个只存在于 lower 的已有文件（copy-up-on-write）：
	// 改动应当落在 upper，lower 里的原文件必须保持不变。
	if err := os.WriteFile(filepath.Join(o.Merged, "existing.txt"), []byte("modified"), 0o644); err != nil {
		t.Fatalf("修改 merged 里的既有文件: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(o.Upper, "existing.txt")); err != nil {
		t.Fatalf("修改后的文件未 copy-up 到 upper 层: %v", err)
	} else if string(b) != "modified" {
		t.Fatalf("upper 内容 = %q, want %q", b, "modified")
	}
	if b, err := os.ReadFile(filepath.Join(o.Lower, "existing.txt")); err != nil {
		t.Fatalf("重新读取 lower existing 文件: %v", err)
	} else if string(b) != "original" {
		t.Fatalf("lower 原文件被改动了：内容 = %q, want %q（lower 必须保持只读）", b, "original")
	}
}

func TestUnmountIsIdempotent(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	dir := t.TempDir()
	if err := Unmount(dir); err != nil {
		t.Fatalf("对未挂载路径 Unmount 应当成功返回，实际: %v", err)
	}
}

func TestUnmountRejectsEmptyPath(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	// syscall.Unmount("", 0) 本身会返回 ENOENT，而 Unmount 把 ENOENT 当作
	// "未挂载" 静默吞掉——如果不在这里单独拦空路径，调用方传入一个算错了的
	// 空路径会完全没有任何提示。
	if err := Unmount(""); err == nil {
		t.Fatal("Unmount(\"\") 应当报错，实际返回 nil")
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
	t.Cleanup(func() {
		if err := Unmount(dst); err != nil {
			t.Errorf("清理 %s 失败: %v", dst, err)
		}
	})

	b, err := os.ReadFile(filepath.Join(dst, "f.txt"))
	if err != nil {
		t.Fatalf("bind 目标读不到文件: %v", err)
	}
	if string(b) != "bind" {
		t.Fatalf("内容 = %q, want %q", b, "bind")
	}
}

// TestBindMountRecursivePropagatesNestedMounts 是一个回归护栏：
// BindMount 用的是 MS_BIND|MS_REC，如果哪次改动误把 MS_REC 丢了，
// 嵌套挂载就不会跟着一起过去，且卸载时会留下悬空条目。
//
// 场景：src 下先有一个嵌套的 tmpfs 子挂载，再把 src rbind 到 dst。
//   - 断言一：dst 下能读到嵌套 tmpfs 里的内容（证明 MS_REC 把子挂载也带过去了）。
//   - 断言二：Unmount(dst) 之后，/proc/self/mountinfo 里 dst 子树不再有
//     任何条目（证明 lazy 卸载确实递归清理干净，呼应 Unmount 文档注释里
//     写的两条调用方契约）。
//
// 本机实测：对带嵌套子挂载的目录做 rbind 后，挂载条目从 1 增至 3
// （源的 inner、目标、目标的 inner）；目标的普通 umount 必然先撞 EBUSY，
// 退化 MNT_DETACH 后目标子树残留条目为 0。
func TestBindMountRecursivePropagatesNestedMounts(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	src := filepath.Join(base, "src")
	inner := filepath.Join(src, "inner")
	dst := filepath.Join(base, "dst")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", inner, err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dst, err)
	}

	// 在 src 下先造一个子挂载。
	if err := syscall.Mount("tmpfs", inner, "tmpfs", 0, ""); err != nil {
		t.Fatalf("挂载 tmpfs 到 %s: %v", inner, err)
	}
	// 测试自己负责把源的 tmpfs 也卸掉，不能只指望卸 dst。
	t.Cleanup(func() {
		if err := Unmount(inner); err != nil {
			t.Errorf("清理源 tmpfs %s 失败: %v", inner, err)
		}
	})
	if err := os.WriteFile(filepath.Join(inner, "nested.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatalf("写嵌套挂载里的文件: %v", err)
	}

	if err := BindMount(src, dst); err != nil {
		t.Fatalf("BindMount: %v", err)
	}
	// 兜底：如果下面的断言提前 Fatal，这里仍然保证 dst 会被卸载。
	// Unmount 是幂等的，和下面显式的那次调用重复也无妨。
	t.Cleanup(func() {
		if err := Unmount(dst); err != nil {
			t.Errorf("清理 %s 失败: %v", dst, err)
		}
	})

	// 断言一：MS_REC 生效，嵌套挂载的内容能在 dst 下读到。
	dstInner := filepath.Join(dst, "inner")
	b, err := os.ReadFile(filepath.Join(dstInner, "nested.txt"))
	if err != nil {
		t.Fatalf("目标下读不到嵌套挂载的内容（MS_REC 未生效？）: %v", err)
	}
	if string(b) != "nested" {
		t.Fatalf("内容 = %q, want %q", b, "nested")
	}

	// 断言二：Unmount(dst) 之后 dst 子树在 mountinfo 里不再留任何条目——
	// 无论最终是普通卸载还是退化成了 lazy 卸载。
	if err := Unmount(dst); err != nil {
		t.Fatalf("Unmount(%s): %v", dst, err)
	}
	if n := mountCountUnder(t, dst); n != 0 {
		t.Fatalf("Unmount 后 %s 子树在 mountinfo 里仍有 %d 条挂载记录，未清理干净", dst, n)
	}
}

// mountCountUnder 统计 /proc/self/mountinfo 里挂载点等于 prefix
// 或位于 prefix 之下的条目数。
func mountCountUnder(t *testing.T, prefix string) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("读取 /proc/self/mountinfo: %v", err)
	}
	count := 0
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mp := fields[4] // mountinfo 第 5 个字段是挂载点
		if mp == prefix || strings.HasPrefix(mp, prefix+"/") {
			count++
		}
	}
	return count
}

func TestOverlayValidateRejectsBadInput(t *testing.T) {
	testutil.RequireLinuxRoot(t)

	base := t.TempDir()
	validDir := filepath.Join(base, "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", validDir, err)
	}
	filePath := filepath.Join(base, "a-file")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("写 %s: %v", filePath, err)
	}
	missingDir := filepath.Join(base, "does-not-exist")
	// 逗号、冒号对 Linux 文件名本身合法，这里真的建出目录来，
	// 确保下面触发的是分隔符检查本身，而不是"目录不存在"。
	commaDir := filepath.Join(base, "has,comma")
	if err := os.MkdirAll(commaDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", commaDir, err)
	}
	colonDir := filepath.Join(base, "has:colon")
	if err := os.MkdirAll(colonDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", colonDir, err)
	}

	baseline := func() Overlay {
		return Overlay{Lower: validDir, Upper: validDir, Work: validDir, Merged: validDir}
	}

	cases := []struct {
		name string
		bad  string
	}{
		{"空路径", ""},
		{"含逗号", commaDir},
		{"含冒号", colonDir},
		{"目录不存在", missingDir},
		{"路径是文件不是目录", filePath},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := baseline()
			o.Lower = tc.bad
			if err := o.validate(); err == nil {
				t.Fatalf("validate() 对 Lower=%q 应当报错，实际通过", tc.bad)
			}
		})
	}
}
