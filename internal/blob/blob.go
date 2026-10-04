// Package blob 提供不可变的内容寻址存储（规格 §6；设计 §3.2）。
//
// 本包只存放字节；授权关联（scope_blobs）与来源（blob_provenance）由持久化层的事务用例写入。
// blob 文件在引用它的事务提交之前就已持久化。不做垃圾回收。
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrNotFound 表示 blob 不存在。
var ErrNotFound = errors.New("blob: 不存在")

// Ref 标识一个 blob：小写十六进制 sha256 与字节数。
type Ref struct {
	SHA256 string
	Size   int64
}

// Store 是内容寻址存储。
type Store interface {
	// Put 写入 r 的全部内容并返回其引用；内容已存在时视为成功。
	Put(ctx context.Context, r io.Reader) (Ref, error)
	// Open 打开 blob 以读取；不存在为 ErrNotFound。
	Open(sha256 string) (io.ReadCloser, error)
	// Stat 返回 blob 的引用；不存在为 ErrNotFound。
	Stat(sha256 string) (Ref, error)
}

// Local 是基于本地目录的实现：`<root>/tmp` 存放写入中的临时文件，
// `<root>/sha256/<前两位>/<其余>` 存放内容。
type Local struct {
	root string
}

// NewLocal 返回以 root 为根目录的本地存储，并确保目录存在。
func NewLocal(root string) (*Local, error) {
	for _, d := range []string{filepath.Join(root, "tmp"), filepath.Join(root, "sha256")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("blob: 创建目录: %w", err)
		}
	}
	return &Local{root: root}, nil
}

func (l *Local) path(sum string) string {
	return filepath.Join(l.root, "sha256", sum[:2], sum[2:])
}

// Put 边写边哈希到临时文件，fsync 后 rename 到内容路径，再 fsync 父目录。
func (l *Local) Put(ctx context.Context, r io.Reader) (Ref, error) {
	tmp, err := os.CreateTemp(filepath.Join(l.root, "tmp"), "put-*")
	if err != nil {
		return Ref{}, fmt.Errorf("blob: 创建临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), contextReader{ctx: ctx, r: r})
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return Ref{}, fmt.Errorf("blob: 写入: %w", err)
	}
	ref := Ref{SHA256: hex.EncodeToString(h.Sum(nil)), Size: size}
	if existing, err := l.Stat(ref.SHA256); err == nil {
		if existing.Size != ref.Size {
			return Ref{}, fmt.Errorf("blob: %s 已存在但大小不同（%d ≠ %d）", ref.SHA256, existing.Size, ref.Size)
		}
		return ref, nil
	}
	dest := l.path(ref.SHA256)
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return Ref{}, fmt.Errorf("blob: 创建目录: %w", err)
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		return Ref{}, fmt.Errorf("blob: rename: %w", err)
	}
	if err := syncDir(filepath.Dir(dest)); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// Open 打开 blob 以读取。
func (l *Local) Open(sum string) (io.ReadCloser, error) {
	if !validSHA256(sum) {
		return nil, fmt.Errorf("blob: 非法的 sha256 %q", sum)
	}
	f, err := os.Open(l.path(sum))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// Stat 返回 blob 的引用。
func (l *Local) Stat(sum string) (Ref, error) {
	if !validSHA256(sum) {
		return Ref{}, fmt.Errorf("blob: 非法的 sha256 %q", sum)
	}
	fi, err := os.Stat(l.path(sum))
	if errors.Is(err, os.ErrNotExist) {
		return Ref{}, ErrNotFound
	}
	if err != nil {
		return Ref{}, err
	}
	return Ref{SHA256: sum, Size: fi.Size()}, nil
}

func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// contextReader 使长时间的复制可以被取消。
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
