// Package datadir 管理本机数据目录的所有权与安装身份文件（规格 §7.4）。
//
// 它只负责本机文件：`<data>/agentbox.lock` 上的 flock、`<data>/install_id` 与
// `<data>/bootstrap_token` 的读取与持久化写入。数据库侧的所有权（advisory lock）由
// internal/persistence/postgres 负责。
package datadir

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	lockName  = "agentbox.lock"
	idName    = "install_id"
	tokenName = "bootstrap_token"

	// TokenSize 是引导令牌的字节数。
	TokenSize = 32
)

var (
	// ErrLocked 表示另一个进程正持有数据目录锁。
	ErrLocked = errors.New("datadir: 数据目录已被另一个进程锁定")
	// ErrTokenCorrupt 表示 bootstrap_token 存在但不是 32 字节的小写十六进制编码。
	ErrTokenCorrupt = errors.New("datadir: bootstrap_token 损坏")
	// ErrTokenExists 表示 bootstrap_token 已存在：有效令牌只能复用，不能覆盖。
	ErrTokenExists = errors.New("datadir: bootstrap_token 已存在")
)

// Lock 是 `<data>/agentbox.lock` 上的独占 flock，持有至 Release 或进程退出。
// 锁文件本身不删除、不重建（规格 §7.4）。
type Lock struct {
	f *os.File
}

// Acquire 以非阻塞方式取得数据目录锁；已被占用时返回 ErrLocked。
func Acquire(dir string) (*Lock, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("datadir: 打开锁文件: %w", err)
	}
	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Release 释放锁并关闭锁文件。
func (l *Lock) Release() error {
	return l.f.Close() // 关闭文件描述符即释放 flock
}

// IDFile 读写 `<data>/install_id`。
type IDFile struct {
	dir string
}

// NewIDFile 返回 dir 下的安装身份文件。
func NewIDFile(dir string) IDFile { return IDFile{dir: dir} }

// Read 读取安装身份；文件不存在时 exists 为 false。
func (f IDFile) Read() (id string, exists bool, err error) {
	b, err := os.ReadFile(filepath.Join(f.dir, idName))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("datadir: 读取 install_id: %w", err)
	}
	id = strings.TrimSpace(string(b))
	if id == "" {
		return "", false, fmt.Errorf("datadir: install_id 文件为空")
	}
	return id, true, nil
}

// Write 持久化写入安装身份。
func (f IDFile) Write(id string) error {
	return writeDurable(f.dir, idName, id+"\n")
}

// TokenFile 读写 `<data>/bootstrap_token`：本数据目录的引导令牌（规格 §7.4）。
// 三种状态严格区分：文件不存在（exists 为 false）、读取失败（返回错误）、内容不是 32 字节的
// 小写十六进制编码（ErrTokenCorrupt）。后两者都不能当作"不存在"，否则一次读错误会被误判为
// 首次安装而生成新身份。
type TokenFile struct {
	dir string
}

// NewTokenFile 返回 dir 下的引导令牌文件。
func NewTokenFile(dir string) TokenFile { return TokenFile{dir: dir} }

// Read 读取引导令牌。
func (f TokenFile) Read() (token []byte, exists bool, err error) {
	b, err := os.ReadFile(filepath.Join(f.dir, tokenName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("datadir: 读取 bootstrap_token: %w", err)
	}
	text := strings.TrimSuffix(string(b), "\n")
	token, err = hex.DecodeString(text)
	if err != nil || len(token) != TokenSize || hex.EncodeToString(token) != text {
		return nil, false, fmt.Errorf("%w：需要 %d 字节的小写十六进制编码", ErrTokenCorrupt, TokenSize)
	}
	return token, true, nil
}

// Write 持久化写入引导令牌。令牌已存在时返回 ErrTokenExists，不覆盖（调用方持有数据目录锁，
// 检查与写入之间没有其他写者）。
func (f TokenFile) Write(token []byte) error {
	if len(token) != TokenSize {
		return fmt.Errorf("datadir: bootstrap_token 必须是 %d 字节，得到 %d", TokenSize, len(token))
	}
	if _, err := os.Lstat(filepath.Join(f.dir, tokenName)); err == nil {
		return ErrTokenExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("datadir: 检查 bootstrap_token: %w", err)
	}
	return writeDurable(f.dir, tokenName, hex.EncodeToString(token)+"\n")
}

// writeDurable 持久化写入 dir/name：临时文件 → 写入并 fsync → rename → fsync 父目录。
// rename 保证原子替换（读者看到旧值或新值），父目录 fsync 保证断电后新目录项仍在。
func writeDurable(dir, name, content string) error {
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("datadir: 创建临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删
	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("datadir: 写入临时文件: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("datadir: fsync 临时文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("datadir: 关闭临时文件: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("datadir: rename: %w", err)
	}
	return syncDir(dir)
}
