//go:build linux

package runner

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// openat2(2) 在标准库 syscall 中没有封装；系统调用号在所有 Linux 架构上都是 437（统一编号表）。
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
func openat2(dirfd int, path string, how *openHow) (int, error) {
	p, err := syscall.BytePtrFromString(path)
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

// outDir 持有 attempt 的 out_dir 目录 FD（规格 §5.6），产物只能相对它打开。
type outDir struct {
	fd int
}

// openOutDir 打开（必要时创建）OutDir。OutDir 必须是 <workspace>/out/<attempt_id>：workspace 根
// 由宿主创建、可信，以普通方式打开；其下的 out 与 <attempt_id> 对 Worker 可写，逐级以
// mkdirat + openat2(RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS, O_DIRECTORY|O_NOFOLLOW) 打开，
// 因而上一个 attempt 的 Worker 留下的符号链接不能把 out_dir 引到别处。
func openOutDir(path string) (*outDir, error) {
	clean := filepath.Clean(path)
	attemptDir, out := filepath.Base(clean), filepath.Dir(clean)
	root, outName := filepath.Dir(out), filepath.Base(out)
	rootFD, err := syscall.Open(root, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: root, Err: err}
	}
	defer func() { _ = syscall.Close(rootFD) }()
	outFD, err := openDirBelow(rootFD, outName)
	if err != nil {
		return nil, &os.PathError{Op: "openat2", Path: out, Err: err}
	}
	defer func() { _ = syscall.Close(outFD) }()
	fd, err := openDirBelow(outFD, attemptDir)
	if err != nil {
		return nil, &os.PathError{Op: "openat2", Path: clean, Err: err}
	}
	return &outDir{fd: fd}, nil
}

// openDirBelow 在 dirfd 下创建（已存在则忽略）并打开单个目录分量，不跟随符号链接。
func openDirBelow(dirfd int, name string) (int, error) {
	if err := syscall.Mkdirat(dirfd, name, 0o755); err != nil && !errors.Is(err, syscall.EEXIST) {
		return -1, err
	}
	return openat2(dirfd, name, &openHow{
		Flags:   syscall.O_RDONLY | syscall.O_DIRECTORY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC,
		Resolve: resolveNoSymlinks | resolveNoMagiclinks,
	})
}

// open 以 RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV 与
// O_RDONLY|O_NOFOLLOW|O_NONBLOCK 相对 out_dir 打开产物，并以 fstat 要求普通文件（规格 §5.6）。
// O_NONBLOCK 使 FIFO 的打开不会阻塞；随后 fstat 拒绝它。返回已打开的文件与 fstat 时的大小。
func (d *outDir) open(rel string) (*os.File, int64, error) {
	fd, err := openat2(d.fd, rel, &openHow{
		Flags:   syscall.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK | syscall.O_CLOEXEC,
		Resolve: resolveBeneath | resolveNoSymlinks | resolveNoMagiclinks | resolveNoXDev,
	})
	if err != nil {
		return nil, 0, &artifactError{code: codePathInvalid, err: err}
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		_ = syscall.Close(fd)
		return nil, 0, &artifactError{code: codeSaveTimeout, err: err} // 暂时故障：Worker 可重试
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = syscall.Close(fd)
		return nil, 0, &artifactError{code: codeNotRegularFile, err: errors.New("不是普通文件")}
	}
	return os.NewFile(uintptr(fd), rel), st.Size, nil
}

func (d *outDir) Close() error { return syscall.Close(d.fd) }
