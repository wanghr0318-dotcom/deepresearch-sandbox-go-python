//go:build linux

package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"syscall"
	"unsafe"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// 本文件实现 OpenOutputs（Plan 15 Task 2、规格 §10.2）：在 exec 环境停止后，按规格 §5.6 的打开规则收集
// 宿主侧 /out tmpfs 中的普通文件。/out 的内容由不可信的 workload 写入，因此每一级都相对已打开的目录 FD、
// 以 openat2 解析，不跟随任何符号链接，也不越过挂载边界。规则与 internal/runner/artifact.go 相同，
// 实现各自独立（provider/local 不依赖 runner）。

// openat2(2) 的系统调用号在所有 Linux 架构上都是 437（统一编号表）。
const sysOpenat2 = 437

// RESOLVE_* 标志（include/uapi/linux/openat2.h）。
const (
	resolveNoXDev       = 0x01
	resolveNoMagiclinks = 0x02
	resolveNoSymlinks   = 0x04
	resolveBeneath      = 0x08
)

// openHow 对应内核的 struct open_how。
type openHow struct {
	Flags   uint64
	Mode    uint64
	Resolve uint64
}

// openat2 调用 openat2(2)。内核在路径解析与并发 rename/mount 竞争时返回 EAGAIN，有限次重试。
func openat2(dirfd int, name string, how *openHow) (int, error) {
	p, err := syscall.BytePtrFromString(name)
	if err != nil {
		return -1, err
	}
	for i := 0; ; i++ {
		fd, _, errno := syscall.Syscall6(sysOpenat2, uintptr(dirfd), uintptr(unsafe.Pointer(p)),
			uintptr(unsafe.Pointer(how)), unsafe.Sizeof(*how), 0, 0)
		if errno == syscall.EINTR || (errno == syscall.EAGAIN && i < 16) {
			continue
		}
		if errno != 0 {
			return -1, errno
		}
		return int(fd), nil
	}
}

// beneath 是 /out 之内一切解析的限制：不离开 /out、不跟随符号链接与魔法链接、不越过挂载点。
const beneath = resolveBeneath | resolveNoSymlinks | resolveNoMagiclinks | resolveNoXDev

// OpenOutputs 实现 provider.Provider（契约"执行中修订（Plan 15）"）：
//   - 环境目录不存在 → ErrNotFound；owner.json 缺失、损坏或属于其他安装 → ErrForeign；
//   - 本进程已知为非 exec 环境，或环境目录下没有 out/ → 错误；
//   - Stop 的权威检查不成立 → ErrNotStopped；
//   - 递归列出 /out，按相对路径的字典序：符号链接记 symlink，FIFO、设备、socket 记 not_regular；
//     普通文件以 O_RDONLY|O_NOFOLLOW|O_NONBLOCK 打开、fstat 确认后收集，超过 max 个的记 too_many，
//     打开或 fstat 失败的记 open_failed。
//
// 返回错误时关闭已打开的全部文件。
func (p *Provider) OpenOutputs(ctx context.Context, envID string, max int) (files []provider.OutputFile, skipped []provider.SkippedOutput, err error) {
	if err := checkName("env_id", envID); err != nil {
		return nil, nil, err
	}
	if max < 0 {
		return nil, nil, fmt.Errorf("local: OpenOutputs 的上限 %d 为负", max)
	}
	own, o, err := p.readOwner(envID)
	if err != nil {
		return nil, nil, err
	}
	switch own {
	case dirAbsent:
		return nil, nil, fmt.Errorf("%w: 环境 %q", provider.ErrNotFound, envID)
	case dirUnknown:
		return nil, nil, fmt.Errorf("%w: %s 没有有效的 owner.json", provider.ErrForeign, p.envDir(envID))
	case dirForeign:
		return nil, nil, fmt.Errorf("%w: %s 属于安装 %q", provider.ErrForeign, p.envDir(envID), o.InstallID)
	}
	if st := p.state(envID); st != nil && st.kind != provider.KindExec {
		return nil, nil, fmt.Errorf("local: 环境 %q 是 %s 环境，没有 /out", envID, st.kind)
	}
	if ok, err := p.stopped(envID); err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, provider.ErrNotStopped
	}

	// 环境目录由本进程建立、root 所有，可以普通方式打开；其下的 out 是挂载点（越过挂载，不加 NO_XDEV）。
	envFD, err := syscall.Open(p.envDir(envID), syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("local: 打开环境目录: %w", err)
	}
	defer syscall.Close(envFD)
	outFD, err := openat2(envFD, execOutName, &openHow{
		Flags:   syscall.O_RDONLY | syscall.O_DIRECTORY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC,
		Resolve: resolveBeneath | resolveNoSymlinks | resolveNoMagiclinks,
	})
	if errors.Is(err, syscall.ENOENT) {
		return nil, nil, fmt.Errorf("local: 环境 %q 不是 exec 环境（没有 /out）", envID)
	} else if err != nil {
		return nil, nil, fmt.Errorf("local: 打开 /out: %w", err)
	}
	defer syscall.Close(outFD)

	defer func() {
		if err != nil {
			for _, f := range files {
				_ = f.File.Close() // 已经返回错误，关闭错误无关紧要
			}
			files, skipped = nil, nil
		}
	}()
	ents, err := listOut(ctx, outFD)
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].rel < ents[j].rel })
	skip := func(rel, reason string) {
		skipped = append(skipped, provider.SkippedOutput{Path: rel, Reason: reason})
	}
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return files, skipped, err
		}
		switch {
		case e.typ == dtLnk:
			skip(e.rel, provider.SkipSymlink)
		case e.typ != dtReg:
			skip(e.rel, provider.SkipNotRegular)
		case len(files) >= max:
			skip(e.rel, provider.SkipTooMany)
		default:
			f, size, reason := openOutput(outFD, e.rel)
			if reason != "" {
				skip(e.rel, reason)
				continue
			}
			files = append(files, provider.OutputFile{Path: e.rel, Size: size, File: f})
		}
	}
	return files, skipped, nil
}

// getdents64 的 d_type 取值（include/linux/fs_types.h；标准库 syscall 未导出）。
const (
	dtDir = 4
	dtReg = 8
	dtLnk = 10
)

// outEntry 是 /out 中的一个非目录条目：相对 /out 的路径与 getdents 报告的类型（DT_*）。
type outEntry struct {
	rel string
	typ uint8
}

// listOut 递归列出 outFD 下的非目录条目。子目录相对 outFD 以 openat2(beneath) 与 O_DIRECTORY|O_NOFOLLOW 打开；
// 条目总数受 /out 的 nr_inodes 限制。tmpfs 总是填写 d_type；DT_UNKNOWN 按非普通文件处理。
func listOut(ctx context.Context, outFD int) ([]outEntry, error) {
	var out []outEntry
	dirs := []string{"."}
	for len(dirs) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rel := dirs[len(dirs)-1]
		dirs = dirs[:len(dirs)-1]
		fd := outFD
		if rel != "." {
			var err error
			fd, err = openat2(outFD, rel, &openHow{
				Flags:   syscall.O_RDONLY | syscall.O_DIRECTORY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC,
				Resolve: beneath,
			})
			if err != nil {
				return nil, fmt.Errorf("local: 打开 /out/%s: %w", rel, err)
			}
		}
		ents, err := readDirents(fd)
		if rel != "." {
			_ = syscall.Close(fd) // 只读目录或文件，关闭错误不影响结果
		}
		if err != nil {
			return nil, fmt.Errorf("local: 读取 /out/%s: %w", rel, err)
		}
		for _, e := range ents {
			child := path.Join(rel, e.rel)
			if e.typ == dtDir {
				dirs = append(dirs, child)
				continue
			}
			out = append(out, outEntry{rel: child, typ: e.typ})
		}
	}
	return out, nil
}

// readDirents 以 getdents64 读取新打开的目录 fd 的全部条目（不含 . 与 ..）。
// 不使用 os.File.ReadDir：它在 d_type 未知时按路径 lstat，而这里只允许相对目录 FD 的访问。
func readDirents(fd int) ([]outEntry, error) {
	var out []outEntry
	buf := make([]byte, 32<<10)
	for {
		n, err := syscall.Getdents(fd, buf)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return out, nil
		}
		for off := 0; off < n; {
			// struct linux_dirent64 { u64 d_ino; s64 d_off; u16 d_reclen; u8 d_type; char d_name[]; }
			reclen := int(*(*uint16)(unsafe.Pointer(&buf[off+16])))
			typ := buf[off+18]
			name := buf[off+19 : off+reclen]
			for i, c := range name {
				if c == 0 {
					name = name[:i]
					break
				}
			}
			off += reclen
			if s := string(name); s != "." && s != ".." {
				out = append(out, outEntry{rel: s, typ: typ})
			}
		}
	}
}

// openOutput 按规格 §5.6 打开 /out 下的 rel：openat2(beneath) 与 O_RDONLY|O_NOFOLLOW|O_NONBLOCK（FIFO 不会阻塞），
// fstat 须为普通文件。失败时返回跳过原因。
func openOutput(outFD int, rel string) (*os.File, int64, string) {
	fd, err := openat2(outFD, rel, &openHow{
		Flags:   syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK | syscall.O_CLOEXEC,
		Resolve: beneath,
	})
	if errors.Is(err, syscall.ELOOP) {
		return nil, 0, provider.SkipSymlink // 列出之后被替换为符号链接
	} else if err != nil {
		return nil, 0, provider.SkipOpenFailed
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd) // 只读目录或文件，关闭错误不影响结果
		return nil, 0, provider.SkipOpenFailed
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = syscall.Close(fd) // 只读目录或文件，关闭错误不影响结果
		return nil, 0, provider.SkipNotRegular
	}
	// 文件已打开为非阻塞；普通文件的读取不受 O_NONBLOCK 影响。
	return os.NewFile(uintptr(fd), rel), st.Size, ""
}
