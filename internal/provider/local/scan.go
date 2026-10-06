//go:build linux

package local

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/cgroup"
	"github.com/wanghr0318-dotcom/go-agentbox/internal/provider"
)

// 本文件实现独立原始扫描（契约第 3 节 Scan 行、规格 §14.1 第 5 步）与挂载表解析。
// Scan 不依赖本进程的内存状态来发现资源：逐层读取磁盘、挂载表与 cgroup 文件系统，
// 只在分类"完整"时参考 init 是否就绪。UID 范围文件属主一层（uid_files）需要已归还范围的集合，Scan 不知道它，
// 由 UIDFiles 与 provider.ScanUIDFiles 提供（M4 Plan 15 Task 5）。

// Scan 逐层报告：
//   - env_dir：<data>/envs/* 的每一项（含无或损坏 owner.json 的），EnvID 一律取目录名；
//   - mount：挂载表中位于数据目录下的挂载点；
//   - cgroup：<cgroup 根>/agentbox-*（含其他 install_id）下的环境 cgroup；
//   - listener：<data>/envs/*/gw.sock。
//
// 分类：owner.json 属于本安装且 init 就绪 → OwnedComplete，属于本安装但不完整 → OwnedPartial；
// 属于其他安装 → Foreign；owner.json 缺失或损坏 → Unknown。挂载与 listener 随所在环境目录分类；
// 本安装 cgroup 下的环境 cgroup 按命名属于本安装（完整 → OwnedComplete，否则 OwnedPartial），
// 其他安装的为 Foreign。
func (p *Provider) Scan(ctx context.Context) (provider.ScanReport, error) {
	var r provider.ScanReport
	add := func(layer, path, envID string, o provider.Owner) {
		r.Items = append(r.Items, provider.ScanItem{Layer: layer, Path: path, EnvID: envID, Owner: o})
	}

	// 环境目录与 listener。
	ents, err := os.ReadDir(p.envsDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return r, fmt.Errorf("local: 扫描环境目录: %w", err)
	}
	dirOwner := make(map[string]provider.Owner, len(ents))
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		name := e.Name()
		o := provider.Unknown
		if e.IsDir() {
			o, err = p.classifyDir(name)
			if err != nil {
				return r, err
			}
		}
		dirOwner[name] = o
		add("env_dir", p.envDir(name), name, o)
		sock := filepath.Join(p.envDir(name), "gw.sock")
		if _, err := os.Lstat(sock); err == nil {
			add("listener", sock, name, o)
		}
	}

	// 数据目录下的挂载。
	ms, err := mountsUnder(p.dataDir)
	if err != nil {
		return r, err
	}
	for _, m := range ms {
		envID, o := "", provider.Unknown
		if rel, err := filepath.Rel(p.envsDir, m); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
			envID = strings.SplitN(rel, string(filepath.Separator), 2)[0]
			if do, ok := dirOwner[envID]; ok {
				o = do
			}
		}
		add("mount", m, envID, o)
	}

	// agentbox-* cgroup。
	if err := ctx.Err(); err != nil {
		return r, err
	}
	roots, err := os.ReadDir(p.cgroupRoot)
	if err != nil {
		return r, fmt.Errorf("local: 扫描 cgroup 根: %w", err)
	}
	ours := filepath.Base(p.installCgroup())
	for _, e := range roots {
		name := e.Name()
		if !e.IsDir() || !strings.HasPrefix(name, "agentbox-") {
			continue
		}
		if ok, err := cgroup.Exists(p.cgroupRoot, name); err != nil {
			return r, err
		} else if !ok {
			continue
		}
		dir := filepath.Join(p.cgroupRoot, name)
		subs, err := os.ReadDir(dir)
		if err != nil {
			return r, fmt.Errorf("local: 扫描 cgroup %s: %w", dir, err)
		}
		found := false
		for _, s := range subs {
			if !s.IsDir() || !strings.HasPrefix(s.Name(), "env-") {
				continue
			}
			found = true
			envID := strings.TrimPrefix(s.Name(), "env-")
			o := provider.Foreign
			if name == ours {
				o = provider.OwnedPartial
				if dirOwner[envID] == provider.OwnedComplete {
					o = provider.OwnedComplete
				}
			}
			add("cgroup", filepath.Join(dir, s.Name()), envID, o)
		}
		if !found && name != ours {
			add("cgroup", dir, "", provider.Foreign)
		}
	}
	return r, nil
}

// classifyDir 把 <envs>/<name> 的归属映射为扫描分类。
func (p *Provider) classifyDir(name string) (provider.Owner, error) {
	own, _, err := p.readOwner(name)
	if err != nil {
		return 0, err
	}
	switch own {
	case dirOwned:
		if p.state(name).ready() {
			return provider.OwnedComplete, nil
		}
		return provider.OwnedPartial, nil
	case dirForeign:
		return provider.Foreign, nil
	default:
		return provider.Unknown, nil
	}
}

// ---------------------------------------------------------------------------
// 挂载表

// mountsUnder 返回本进程挂载表（/proc/self/mountinfo）中位于 dir 或其下的挂载点，按出现顺序。
func mountsUnder(dir string) ([]string, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("local: 读取 mountinfo: %w", err)
	}
	return parseMountsUnder(b, dir), nil
}

func parseMountsUnder(mountinfo []byte, dir string) []string {
	dir = filepath.Clean(dir)
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(mountinfo))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		// 字段：mount_id parent_id major:minor root mount_point options ... - fstype source super_options
		f := strings.Fields(sc.Text())
		if len(f) < 5 {
			continue
		}
		mp := unescapeMount(f[4])
		if mp == dir || strings.HasPrefix(mp, dir+"/") {
			out = append(out, mp)
		}
	}
	return out
}

// unescapeMount 还原 mountinfo 中以 \ooo 八进制转义的空白与反斜杠。
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// unmountUnder 卸载 dir 或其下的全部挂载，深者先卸；同一挂载点叠加的多层逐层卸载，直到挂载表中
// 不再出现。卸载失败（例如 EBUSY）作为宿主错误返回。
func unmountUnder(dir string) error {
	for round := 0; ; round++ {
		ms, err := mountsUnder(dir)
		if err != nil {
			return err
		}
		if len(ms) == 0 {
			return nil
		}
		if round >= 64 {
			return fmt.Errorf("local: %s 下的挂载无法全部卸载：%v", dir, ms)
		}
		sort.SliceStable(ms, func(i, j int) bool { return len(ms[i]) > len(ms[j]) })
		for _, m := range ms {
			if err := syscall.Unmount(m, 0); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
				return fmt.Errorf("local: 卸载 %s: %w", m, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// UID 范围文件属主（M4 Plan 15 Task 5；规格 §4.5 回收条件、I11、E39；计划 D13）

// 数据目录下的固定子目录：任务 workspace 的父目录与 BlobStore（内容寻址，只由宿主写入，不扫描）。
const (
	workspacesDirName = "workspaces"
	blobsDirName      = "blobs"
)

// oPath 是 O_PATH（asm-generic/fcntl.h 的 010000000；标准库 syscall 在 amd64 上未导出）。
const oPath = 0x200000

// atEmptyPath 是 AT_EMPTY_PATH（include/uapi/linux/fcntl.h；标准库 syscall 未导出）：fchownat 作用于 fd 本身。
const atEmptyPath = 0x1000

// ReclaimUIDFiles 实现 provider.Provider：<data>/workspaces 下属主落在范围内的条目改回 0:0（计划 D13）。
// workspaces 不存在时为 0。
func (p *Provider) ReclaimUIDFiles(ctx context.Context, base, size uint32) (int, error) {
	n := 0
	err := walkUIDs(ctx, filepath.Join(p.dataDir, workspacesDirName), "", base, size, func(fd int, rel string) (bool, error) {
		if err := syscall.Fchownat(fd, "", 0, 0, atEmptyPath); err != nil {
			return false, fmt.Errorf("local: 回收 workspaces/%s 的属主: %w", rel, err)
		}
		n++
		return true, nil
	})
	return n, err
}

// UIDFiles 实现 provider.Provider：数据目录（跳过 blobs）中属主落在范围内的条目，至多 limit 个宿主路径。
func (p *Provider) UIDFiles(ctx context.Context, base, size uint32, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("local: UIDFiles 的上限 %d 必须为正", limit)
	}
	var out []string
	err := walkUIDs(ctx, p.dataDir, blobsDirName, base, size, func(_ int, rel string) (bool, error) {
		out = append(out, filepath.Join(p.dataDir, rel))
		return len(out) < limit, nil
	})
	return out, err
}

// walkUIDs 遍历 root 之下的全部条目（不含 root 本身；顶层名为 skipTop 的条目整体跳过），对属主 uid 或 gid
// 落在 [base, base+size) 的条目调用 fn(fd, rel)：fd 是以 O_PATH|O_NOFOLLOW 打开的该条目本身（符号链接即链接
// 本身），fn 返回 false 时停止遍历。root 不存在时什么也不做。
//
// 每一级都相对已打开的目录 fd 解析单个名字，不跟随符号链接：遍历期间并发替换路径分量（存活环境可以改写自己的
// workspace）不能把操作引到 root 之外。子目录只在与 root 同一文件系统（st_dev 相同）时进入：挂载点本身按属主
// 检查，但不遍历其中的其他文件系统。目录并发消失（ENOENT）时跳过。
func walkUIDs(ctx context.Context, root, skipTop string, base, size uint32, fn func(fd int, rel string) (bool, error)) error {
	rfd, err := syscall.Open(root, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	} else if err != nil {
		return fmt.Errorf("local: 打开 %s: %w", root, err)
	}
	defer syscall.Close(rfd)
	var st syscall.Stat_t
	if err := syscall.Fstat(rfd, &st); err != nil {
		return fmt.Errorf("local: fstat %s: %w", root, err)
	}
	w := uidWalk{ctx: ctx, root: root, skipTop: skipTop, lo: uint64(base), hi: uint64(base) + uint64(size), dev: st.Dev, fn: fn}
	_, err = w.dir(rfd, "")
	return err
}

type uidWalk struct {
	ctx     context.Context
	root    string
	skipTop string
	lo, hi  uint64 // [lo, hi)
	dev     uint64
	fn      func(fd int, rel string) (bool, error)
}

func (w *uidWalk) owned(st *syscall.Stat_t) bool {
	in := func(id uint32) bool { return uint64(id) >= w.lo && uint64(id) < w.hi }
	return in(st.Uid) || in(st.Gid)
}

// dir 遍历已打开的目录 dirfd（相对 root 为 rel）；返回 true 表示 fn 要求停止。
func (w *uidWalk) dir(dirfd int, rel string) (bool, error) {
	ents, err := readDirents(dirfd)
	if err != nil {
		return false, fmt.Errorf("local: 读取 %s: %w", filepath.Join(w.root, rel), err)
	}
	for _, e := range ents {
		if err := w.ctx.Err(); err != nil {
			return false, err
		}
		if rel == "" && e.rel == w.skipTop {
			continue
		}
		child := filepath.Join(rel, e.rel)
		stop, err := w.entry(dirfd, e.rel, child)
		if stop || err != nil {
			return stop, err
		}
	}
	return false, nil
}

// entry 检查 dirfd 下的单个名字 name（相对 root 为 rel），是同一文件系统的目录时递归进入。
func (w *uidWalk) entry(dirfd int, name, rel string) (bool, error) {
	fd, err := syscall.Openat(dirfd, name, oPath|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return false, nil // 并发删除
	} else if err != nil {
		return false, fmt.Errorf("local: 打开 %s: %w", filepath.Join(w.root, rel), err)
	}
	defer syscall.Close(fd)
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		return false, fmt.Errorf("local: fstat %s: %w", filepath.Join(w.root, rel), err)
	}
	if w.owned(&st) {
		if more, err := w.fn(fd, rel); err != nil || !more {
			return !more, err
		}
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFDIR || st.Dev != w.dev {
		return false, nil
	}
	sub, err := syscall.Openat(fd, ".", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if errors.Is(err, syscall.ENOENT) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("local: 打开目录 %s: %w", filepath.Join(w.root, rel), err)
	}
	defer syscall.Close(sub)
	return w.dir(sub, rel)
}
