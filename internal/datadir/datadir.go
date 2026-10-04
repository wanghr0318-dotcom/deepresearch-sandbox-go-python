// Package datadir 管理本机数据目录的所有权与安装身份文件（规格 §7.4）。
//
// 它只负责本机文件：`<data>/agentbox.lock` 上的 flock，以及 `<data>/install_id` 的读取与
// 持久化写入。数据库侧的所有权（advisory lock）由 internal/persistence/postgres 负责。
package datadir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	lockName = "agentbox.lock"
	idName   = "install_id"
)

// ErrLocked 表示另一个进程正持有数据目录锁。
var ErrLocked = errors.New("datadir: 数据目录已被另一个进程锁定")

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

// Write 持久化写入安装身份：临时文件 → 写入并 fsync → rename → fsync 父目录。
// rename 保证原子替换（读者看到旧值或新值），父目录 fsync 保证断电后新目录项仍在。
func (f IDFile) Write(id string) error {
	tmp, err := os.CreateTemp(f.dir, idName+".tmp-*")
	if err != nil {
		return fmt.Errorf("datadir: 创建临时文件: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // rename 成功后此处无文件可删
	if _, err := tmp.WriteString(id + "\n"); err != nil {
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
	if err := os.Rename(tmpPath, filepath.Join(f.dir, idName)); err != nil {
		return fmt.Errorf("datadir: rename: %w", err)
	}
	return syncDir(f.dir)
}
