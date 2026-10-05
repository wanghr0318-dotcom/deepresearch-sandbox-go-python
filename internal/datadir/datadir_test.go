//go:build unix

package datadir

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// TestTokenFileStates：不存在、读取失败、损坏是三种不同状态；有效令牌只能复用，不能覆盖。
func TestTokenFileStates(t *testing.T) {
	token := make([]byte, TokenSize)
	for i := range token {
		token[i] = byte(i)
	}
	t.Run("不存在后写入再读取", func(t *testing.T) {
		f := NewTokenFile(t.TempDir())
		if got, exists, err := f.Read(); err != nil || exists || got != nil {
			t.Fatalf("初始应不存在：(%x, %v, %v)", got, exists, err)
		}
		if err := f.Write(token); err != nil {
			t.Fatal(err)
		}
		if got, exists, err := f.Read(); err != nil || !exists || string(got) != string(token) {
			t.Fatalf("读回 (%x, %v, %v)", got, exists, err)
		}
		if err := f.Write(make([]byte, TokenSize)); !errors.Is(err, ErrTokenExists) {
			t.Fatalf("已存在的令牌不应被覆盖，得到 %v", err)
		}
		if got, _, _ := f.Read(); string(got) != string(token) {
			t.Fatalf("令牌内容被改变：%x", got)
		}
	})
	t.Run("长度不对的令牌不写入", func(t *testing.T) {
		f := NewTokenFile(t.TempDir())
		if err := f.Write(token[:16]); err == nil {
			t.Fatal("16 字节的令牌应被拒绝")
		}
	})
	t.Run("损坏", func(t *testing.T) {
		for name, content := range map[string]string{
			"空":     "",
			"非十六进制": "zz" + hex.EncodeToString(token)[2:],
			"过短":    hex.EncodeToString(token[:31]),
			"过长":    hex.EncodeToString(append(token, 0)),
			"大写":    strings.ToUpper(hex.EncodeToString(token)),
		} {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, tokenName), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, exists, err := NewTokenFile(dir).Read(); !errors.Is(err, ErrTokenCorrupt) || exists {
					t.Fatalf("应为 ErrTokenCorrupt，得到 exists=%v err=%v", exists, err)
				}
			})
		}
	})
	t.Run("读取失败", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, tokenName), 0o700); err != nil { // 读目录必然失败
			t.Fatal(err)
		}
		_, exists, err := NewTokenFile(dir).Read()
		if err == nil || exists || errors.Is(err, ErrTokenCorrupt) {
			t.Fatalf("读取失败应返回错误而不是不存在或损坏，得到 exists=%v err=%v", exists, err)
		}
	})
}
