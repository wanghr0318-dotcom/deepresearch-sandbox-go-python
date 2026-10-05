//go:build linux

package sandbox

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

// 本文件实现 init 的挂载建立（规格 §4.5、§4.6 init 段；Plan 1B 实验记录 §4 的 B2 结论）。
// 全部挂载只存在于沙箱自己的 mount namespace：宿主不可见，init 消失即随命名空间释放，
// 不需要宿主侧清理，也没有崩溃后的残留。
//
// 新根是 init 在 stagingDir 上挂载的 tmpfs：其中只有模板项的上级目录、重建的符号链接与
// init 自己的挂载点；模板各项以只读 bind 挂到同一路径，随后根本身 remount 为只读。
// stagingDir 在 pivot_root 之后随旧根一起脱离，宿主的同名目录不受影响。
//
// 所有 bind 的源在挂载 stagingDir 之前先以 O_PATH 打开并固定：此后宿主路径被替换
// （或被 stagingDir 遮盖，例如位于 /tmp 下的 workspace）都不影响挂进沙箱的对象。

// x86_64 与 aarch64 上相同的系统调用号（新挂载 API，Linux 5.2+/5.12+）。
const (
	sysOpenTree     = 428
	sysMoveMount    = 429
	sysMountSetattr = 442
)

const (
	openTreeClone       = 0x1
	atEmptyPath         = 0x1000
	atRecursive         = 0x8000
	atFdcwd             = -100
	moveMountFEmptyPath = 0x4

	mountAttrRdonly = 0x1
	mountAttrNosuid = 0x2
	mountAttrNodev  = 0x4

	oPath = 0x200000
)

// stagingDir 是 pivot_root 之前新根所在的挂载点（在 init 的私有 mount namespace 中覆盖宿主的同名目录）。
const stagingDir = "/tmp"

// 沙箱内的路径。
const (
	gatewayDir    = "/run/agentbox"
	gatewaySocket = gatewayDir + "/gateway.sock" // 规格 §4.5：编排环境的 Gateway socket
)

// workloadID 是 workload 在命名空间内的 uid/gid（规格 §4.5、§4.6）。
const workloadID = 1000

// 规格 §4.5 的 /proc 掩蔽清单与只读路径；本内核上不存在的项跳过。
var (
	procMaskPaths = []string{"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
		"/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/proc/scsi"}
	procReadonlyPaths = []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}
	// devNodes 是 /dev 中 bind 自宿主的设备；不提供 /dev/shm（规格 §4.5 环境限制）。
	devNodes = []string{"null", "zero", "random", "urandom"}
)

// forceClassicMounts 是测试钩子：为 true 时 bind 一律走 mount(2) 回退路径。只由测试设置。
var forceClassicMounts bool

// mountAttr 是 mount_setattr(2) 的 struct mount_attr。
type mountAttr struct {
	AttrSet     uint64
	AttrClr     uint64
	Propagation uint64
	UsernsFd    uint64
}

// source 是一个已固定的 bind 源：符号链接只记录目标（在新根中重建），其他对象持有 O_PATH fd。
type source struct {
	path string
	fd   int // 符号链接为 -1
	link string
	dir  bool
}

// pinSource 以 O_PATH|O_NOFOLLOW 固定 path（符号链接只读取目标）。
func pinSource(path string) (source, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return source{}, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return source{}, err
		}
		return source{path: path, fd: -1, link: target}, nil
	}
	fd, err := syscall.Open(path, oPath|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return source{}, fmt.Errorf("打开 %s: %w", path, err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return source{}, fmt.Errorf("fstat %s: %w", path, err)
	}
	if st.Mode&syscall.S_IFMT == syscall.S_IFLNK || (st.Mode&syscall.S_IFMT == syscall.S_IFDIR) != fi.IsDir() {
		syscall.Close(fd)
		return source{}, fmt.Errorf("%s 在打开期间被替换", path)
	}
	return source{path: path, fd: fd, dir: fi.IsDir()}, nil
}

func (s source) close() {
	if s.fd >= 0 {
		syscall.Close(s.fd)
	}
}

// mounter 建立 bind：优先新挂载 API，不可用（ENOSYS）时回退 mount(2)，并记录一次。
type mounter struct {
	newAPI bool
	logf   func(format string, a ...any)
}

// bind 把已固定的 src 递归挂到 dst：ro 时 `ro,nosuid,nodev`，否则 `nosuid,nodev`；私有传播。
func (m *mounter) bind(src int, dst string, ro bool) error {
	if m.newAPI {
		err := bindNewAPI(src, dst, ro)
		if !errors.Is(err, syscall.ENOSYS) {
			return err
		}
		m.newAPI = false
		m.logf("新挂载 API 不可用（%v），使用 mount(2) 回退路径", err)
	}
	return bindClassic(src, dst, ro)
}

// bindNewAPI：open_tree(OPEN_TREE_CLONE|AT_RECURSIVE) → mount_setattr(AT_RECURSIVE) → move_mount。
// 属性在树游离时设置，挂载出现时已是最终属性，没有"已挂载但仍可写"的窗口；AT_RECURSIVE
// 一次覆盖全部子挂载。
func bindNewAPI(src int, dst string, ro bool) error {
	tree, _, e := syscall.Syscall(sysOpenTree, uintptr(src), uintptr(unsafe.Pointer(emptyPath())),
		openTreeClone|syscall.O_CLOEXEC|atRecursive|atEmptyPath)
	if e != 0 {
		return fmt.Errorf("open_tree: %w", e)
	}
	defer syscall.Close(int(tree))
	attr := &mountAttr{AttrSet: mountAttrNosuid | mountAttrNodev, Propagation: syscall.MS_PRIVATE}
	if ro {
		attr.AttrSet |= mountAttrRdonly
	}
	_, _, e = syscall.Syscall6(sysMountSetattr, tree, uintptr(unsafe.Pointer(emptyPath())), atEmptyPath|atRecursive,
		uintptr(unsafe.Pointer(attr)), unsafe.Sizeof(*attr), 0)
	if e != 0 {
		return fmt.Errorf("mount_setattr: %w", e)
	}
	d, err := syscall.BytePtrFromString(dst)
	if err != nil {
		return err
	}
	fdcwd := atFdcwd
	_, _, e = syscall.Syscall6(sysMoveMount, tree, uintptr(unsafe.Pointer(emptyPath())), uintptr(fdcwd),
		uintptr(unsafe.Pointer(d)), moveMountFEmptyPath, 0)
	if e != 0 {
		return fmt.Errorf("move_mount %s: %w", dst, e)
	}
	return nil
}

func emptyPath() *byte {
	b := [1]byte{}
	return &b[0]
}

// bindClassic 是 mount(2) 回退：MS_BIND|MS_REC 之后逐个子挂载 remount。必须递归：在非初始 user namespace 中，
// 宿主的子挂载是锁定的，非递归 bind 会被拒绝（EINVAL，实验记录 §4）；MS_REMOUNT 本身不递归，
// 因此每个子挂载单独 remount，并保留其已有的（可能被锁定的）ro、noexec 与 atime 标志。
func bindClassic(src int, dst string, ro bool) error {
	if err := syscall.Mount("/proc/self/fd/"+strconv.Itoa(src), dst, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("bind %s: %w", dst, err)
	}
	ms, err := mountsUnder(dst)
	if err != nil {
		return err
	}
	for _, e := range ms {
		flags := uintptr(syscall.MS_BIND | syscall.MS_REMOUNT | syscall.MS_NOSUID | syscall.MS_NODEV)
		if ro {
			flags |= syscall.MS_RDONLY
		}
		flags |= preservedFlags(e.opts)
		if err := syscall.Mount("", e.point, "", flags, ""); err != nil {
			return fmt.Errorf("remount %s: %w", e.point, err)
		}
	}
	return nil
}

// preservedFlags 从每挂载选项中取出 remount 必须保留的标志（user namespace 中它们可能被锁定，改变即 EPERM）。
func preservedFlags(opts []string) uintptr {
	var fl uintptr
	atime := false
	for _, o := range opts {
		switch o {
		case "ro":
			fl |= syscall.MS_RDONLY
		case "noexec":
			fl |= syscall.MS_NOEXEC
		case "noatime":
			fl |= syscall.MS_NOATIME
			atime = true
		case "relatime":
			fl |= syscall.MS_RELATIME
			atime = true
		case "nodiratime":
			fl |= syscall.MS_NODIRATIME
		}
	}
	if !atime {
		fl |= syscall.MS_STRICTATIME
	}
	return fl
}

// mountEntry 是 /proc/self/mountinfo 的一行中用到的字段。
type mountEntry struct {
	point  string
	opts   []string // 每挂载选项
	fstype string
}

// mountsUnder 返回挂载点为 dst 或位于其下的挂载（本进程视图）。
func mountsUnder(dst string) ([]mountEntry, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	var out []mountEntry
	for _, e := range parseMountinfo(string(b)) {
		if e.point == dst || strings.HasPrefix(e.point, dst+"/") {
			out = append(out, e)
		}
	}
	return out, nil
}

// parseMountinfo 解析 mountinfo（proc(5)）：第 5 列挂载点、第 6 列每挂载选项、"-" 之后第一列文件系统类型。
func parseMountinfo(s string) []mountEntry {
	var out []mountEntry
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 7 {
			continue
		}
		e := mountEntry{point: unescapeMountinfo(f[4]), opts: strings.Split(f[5], ",")}
		for i := 6; i < len(f)-1; i++ {
			if f[i] == "-" {
				e.fstype = f[i+1]
				break
			}
		}
		out = append(out, e)
	}
	return out
}

// unescapeMountinfo 还原 mountinfo 中的八进制转义（空格、制表符、换行、反斜杠）。
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// setupMounts 在 init 的命名空间中、pivot_root 之前建立全部挂载（规格 §4.6 init 段的挂载部分）。
// 每一步失败返回 *stepErr。
func setupMounts(s *InitSpec, check func(string) error, logf func(string, ...any)) error {
	if err := check(stepMountPrivate); err != nil {
		return err
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		return &stepErr{stepMountPrivate, err}
	}

	// 固定全部 bind 源（必须在挂载 stagingDir 之前）。
	var pinned []source
	defer func() {
		for _, p := range pinned {
			p.close()
		}
	}()
	pin := func(step, path string) (source, error) {
		src, err := pinSource(path)
		if err != nil {
			return source{}, &stepErr{step, err}
		}
		pinned = append(pinned, src)
		return src, nil
	}
	tmpl := make([]source, 0, len(s.Template.Paths))
	for _, p := range s.Template.Paths {
		src, err := pin(stepMountRootfs, p)
		if err != nil {
			return err
		}
		tmpl = append(tmpl, src)
	}
	in, ws, gw := source{fd: -1}, source{fd: -1}, source{fd: -1}
	var err error
	if s.In != "" {
		if in, err = pin(stepMountIn, s.In); err != nil {
			return err
		}
		if !in.dir {
			return &stepErr{stepMountIn, fmt.Errorf("%s 不是目录", s.In)}
		}
	}
	if s.Workspace != "" {
		if ws, err = pin(stepMountWorkspace, s.Workspace); err != nil {
			return err
		}
		if !ws.dir {
			return &stepErr{stepMountWorkspace, fmt.Errorf("%s 不是目录", s.Workspace)}
		}
	}
	if s.GatewaySocket != "" {
		if gw, err = pin(stepMountGateway, s.GatewaySocket); err != nil {
			return err
		}
	}

	m := &mounter{newAPI: !forceClassicMounts, logf: logf}
	if forceClassicMounts {
		logf("测试钩子：使用 mount(2) 回退路径")
	}
	root := func(p string) string { return stagingDir + p }

	// rootfs：只读 tmpfs 根 + 模板各项只读 bind 到同一路径。
	if err := check(stepMountRootfs); err != nil {
		return err
	}
	if err := syscall.Mount("tmpfs", stagingDir, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, "mode=755,size=1m"); err != nil {
		return &stepErr{stepMountRootfs, fmt.Errorf("新根 tmpfs: %w", err)}
	}
	points := []string{"/proc", "/dev", "/tmp"}
	if s.Kind == KindExec {
		points = append(points, "/out")
		if s.In != "" {
			points = append(points, "/in")
		}
	} else {
		points = append(points, "/run")
		if s.Workspace != "" {
			points = append(points, "/workspace")
		}
	}
	for _, p := range points {
		if err := os.Mkdir(root(p), 0o755); err != nil {
			return &stepErr{stepMountRootfs, err}
		}
	}
	for _, src := range tmpl {
		dst := root(src.path)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return &stepErr{stepMountRootfs, err}
		}
		switch {
		case src.fd < 0:
			err = os.Symlink(src.link, dst)
		case src.dir:
			err = os.Mkdir(dst, 0o755)
		default:
			err = createEmpty(dst, 0o444)
		}
		if err != nil {
			return &stepErr{stepMountRootfs, err}
		}
		if src.fd >= 0 {
			if err := m.bind(src.fd, dst, true); err != nil {
				return &stepErr{stepMountRootfs, fmt.Errorf("%s: %w", src.path, err)}
			}
		}
	}
	// /in：与模板同法（只读）。
	if in.fd >= 0 {
		if err := check(stepMountIn); err != nil {
			return err
		}
		if err := m.bind(in.fd, root("/in"), true); err != nil {
			return &stepErr{stepMountIn, err}
		}
	}
	if err := syscall.Mount("", stagingDir, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV, ""); err != nil {
		return &stepErr{stepMountRootfs, fmt.Errorf("新根 remount 只读: %w", err)}
	}

	// tmpfs：/tmp；编排环境 /run（限额）；exec 环境 /out（size、nr_inodes 即配额，属主为 workload）。
	if err := check(stepMountTmpfs); err != nil {
		return err
	}
	tmpfs := []struct{ dst, data string }{{"/tmp", fmt.Sprintf("mode=1777,size=%d", s.TmpBytes)}}
	if s.Kind == KindExec {
		tmpfs = append(tmpfs, struct{ dst, data string }{"/out",
			fmt.Sprintf("mode=755,size=%d,nr_inodes=1024,uid=%d,gid=%d", s.OutBytes, workloadID, workloadID)})
	} else {
		tmpfs = append(tmpfs, struct{ dst, data string }{"/run", fmt.Sprintf("mode=1777,size=%d", s.TmpBytes)})
	}
	for _, t := range tmpfs {
		if err := syscall.Mount("tmpfs", root(t.dst), "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, t.data); err != nil {
			return &stepErr{stepMountTmpfs, fmt.Errorf("%s: %w", t.dst, err)}
		}
	}

	// /dev：tmpfs + bind 宿主的 null/zero/random/urandom，然后 remount 只读。
	if err := check(stepMountDev); err != nil {
		return err
	}
	if err := syscall.Mount("tmpfs", root("/dev"), "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, "mode=755,size=64k"); err != nil {
		return &stepErr{stepMountDev, err}
	}
	for _, d := range devNodes {
		p := root("/dev/" + d)
		if err := createEmpty(p, 0o666); err != nil {
			return &stepErr{stepMountDev, err}
		}
		if err := syscall.Mount("/dev/"+d, p, "", syscall.MS_BIND, ""); err != nil {
			return &stepErr{stepMountDev, fmt.Errorf("%s: %w", d, err)}
		}
	}
	if err := syscall.Mount("", root("/dev"), "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
		return &stepErr{stepMountDev, fmt.Errorf("remount 只读: %w", err)}
	}

	// Gateway socket（编排环境）：属主须为映射 uid 1000、权限 0600、类型 socket。
	if gw.fd >= 0 {
		if err := check(stepMountGateway); err != nil {
			return err
		}
		var st syscall.Stat_t
		if err := syscall.Fstat(gw.fd, &st); err != nil {
			return &stepErr{stepMountGateway, err}
		}
		if st.Mode&syscall.S_IFMT != syscall.S_IFSOCK || st.Mode&0o7777 != 0o600 || st.Uid != workloadID {
			return &stepErr{stepMountGateway, fmt.Errorf("%s 须为属主 uid %d、权限 0600 的 socket（实际 mode %#o uid %d）",
				s.GatewaySocket, workloadID, st.Mode, st.Uid)}
		}
		if err := os.Mkdir(root(gatewayDir), 0o755); err != nil {
			return &stepErr{stepMountGateway, err}
		}
		if err := createEmpty(root(gatewaySocket), 0o600); err != nil {
			return &stepErr{stepMountGateway, err}
		}
		if err := m.bind(gw.fd, root(gatewaySocket), false); err != nil {
			return &stepErr{stepMountGateway, err}
		}
	}

	// workspace（宿主已创建并 chown 到映射 UID）：可写，nosuid、nodev。
	if ws.fd >= 0 {
		if err := check(stepMountWorkspace); err != nil {
			return err
		}
		if err := m.bind(ws.fd, root("/workspace"), false); err != nil {
			return &stepErr{stepMountWorkspace, err}
		}
	}

	// 新 proc 实例（本 pid namespace）。
	if err := check(stepMountProc); err != nil {
		return err
	}
	if err := syscall.Mount("proc", root("/proc"), "proc", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
		return &stepErr{stepMountProc, err}
	}

	if err := check(stepMaskProc); err != nil {
		return err
	}
	if err := maskProc(root); err != nil {
		return &stepErr{stepMaskProc, err}
	}
	return nil
}

// maskProc 应用规格 §4.5 的掩蔽清单（目录：只读空 tmpfs；文件：bind /dev/null）与只读路径。
func maskProc(root func(string) string) error {
	for _, p := range procMaskPaths {
		target := root(p)
		fi, err := os.Stat(target)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if fi.IsDir() {
			err = syscall.Mount("tmpfs", target, "tmpfs", syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, "size=0,mode=555")
		} else {
			err = syscall.Mount(root("/dev/null"), target, "", syscall.MS_BIND, "")
		}
		if err != nil {
			return fmt.Errorf("掩蔽 %s: %w", p, err)
		}
	}
	for _, p := range procReadonlyPaths {
		target := root(p)
		if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := syscall.Mount(target, target, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
			return fmt.Errorf("bind %s: %w", p, err)
		}
		if err := syscall.Mount("", target, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
			return fmt.Errorf("remount 只读 %s: %w", p, err)
		}
	}
	return nil
}

// pivotRoot 把 stagingDir 切换为新根并脱离旧根：pivot_root(".", ".") → umount2(".", MNT_DETACH) → chdir("/")。
func pivotRoot(check func(string) error) error {
	if err := check(stepPivotRoot); err != nil {
		return err
	}
	if err := syscall.Chdir(stagingDir); err != nil {
		return &stepErr{stepPivotRoot, err}
	}
	if err := syscall.PivotRoot(".", "."); err != nil {
		return &stepErr{stepPivotRoot, err}
	}
	if err := check(stepUmountOldroot); err != nil {
		return err
	}
	if err := syscall.Unmount(".", syscall.MNT_DETACH); err != nil {
		return &stepErr{stepUmountOldroot, err}
	}
	if err := syscall.Chdir("/"); err != nil {
		return &stepErr{stepUmountOldroot, err}
	}
	return nil
}

// createEmpty 创建一个空文件（作为文件 bind 的挂载点）。
func createEmpty(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	return f.Close()
}
