package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutIsContentAddressedAndIdempotent(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("# 报告\n")
	sum := sha256.Sum256(content)
	want := Ref{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content))}

	for i := 0; i < 2; i++ { // 第二次写入同一内容视为成功
		got, err := s.Put(context.Background(), bytes.NewReader(content))
		if err != nil || got != want {
			t.Fatalf("第 %d 次 Put 得到 (%+v, %v)，期望 %+v", i+1, got, err, want)
		}
	}
	r, err := s.Open(want.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if b, _ := io.ReadAll(r); !bytes.Equal(b, content) {
		t.Fatalf("读回内容不一致：%q", b)
	}
	if tmp, _ := os.ReadDir(filepath.Join(root, "tmp")); len(tmp) != 0 {
		t.Fatalf("临时目录应为空，得到 %d 项", len(tmp))
	}
}

func TestMissingAndInvalid(t *testing.T) {
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(strings.Repeat("a", 64)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("不存在应为 ErrNotFound，得到 %v", err)
	}
	if _, err := s.Open("../etc/passwd"); err == nil {
		t.Fatal("非法 sha256 应被拒绝")
	}
}

func TestPutHonoursCancellation(t *testing.T) {
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, strings.NewReader("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消的 context 应使 Put 失败，得到 %v", err)
	}
}
