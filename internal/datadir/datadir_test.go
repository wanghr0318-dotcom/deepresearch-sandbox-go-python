//go:build unix

package datadir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLockIsExclusiveAndReleasable(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatalf("首次取锁: %v", err)
	}
	if _, err := Acquire(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("锁被持有时应返回 ErrLocked，得到 %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("释放: %v", err)
	}
	again, err := Acquire(dir)
	if err != nil {
		t.Fatalf("释放后重新取锁: %v", err)
	}
	_ = again.Release()
}

func TestIDFileWriteReadReplace(t *testing.T) {
	dir := t.TempDir()
	f := NewIDFile(dir)
	if _, exists, err := f.Read(); err != nil || exists {
		t.Fatalf("初始应不存在：exists=%v err=%v", exists, err)
	}
	for _, id := range []string{"install-a", "install-b"} {
		if err := f.Write(id); err != nil {
			t.Fatalf("写入 %s: %v", id, err)
		}
		got, exists, err := f.Read()
		if err != nil || !exists || got != id {
			t.Fatalf("读取得到 (%q, %v, %v)，期望 %q", got, exists, err, id)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(idName) {
		t.Fatalf("目录中应只剩 install_id，得到 %v", entries)
	}
}
