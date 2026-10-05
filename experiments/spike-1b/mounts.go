//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// Spec §4.5 /proc masking list.
var (
	procMaskPaths = []string{"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
		"/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/proc/scsi"}
	procReadonlyPaths = []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}
)

const (
	roFlags = syscall.MS_RDONLY | syscall.MS_NOSUID | syscall.MS_NODEV

	openTreeClone   = 1
	openTreeCloexec = syscall.O_CLOEXEC
	atRecursive     = 0x8000
	moveMountFEmpty = 0x4

	mountAttrRdonly = 0x1
	mountAttrNosuid = 0x2
	mountAttrNodev  = 0x4
)

type mountAttr struct {
	AttrSet     uint64
	AttrClr     uint64
	Propagation uint64
	UsernsFd    uint64
}

// stepErr tags an error with the startup step that produced it; the step name
// becomes the start_err / init_err reason prefix.
type stepErr struct {
	step string
	err  error
}

func (e *stepErr) Error() string { return e.step + ": " + e.err.Error() }

var errInjected = errors.New("injected failure")

// failpoint returns an injected error when the step matches the requested one.
func failpoint(want, step string) error {
	if want == step {
		return &stepErr{step, errInjected}
	}
	return nil
}

func wrap(step string, err error) error {
	if err == nil {
		return nil
	}
	return &stepErr{step, err}
}

// setupMounts runs inside the new user+mount namespace (as init, before
// pivot_root). root is the template directory that becomes "/".
func setupMounts(strategy, root, inDir, fail string, log func(string, ...any)) error {
	if err := failpoint(fail, "mount_private"); err != nil {
		return err
	}
	if err := wrap("mount_private", syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, "")); err != nil {
		return err
	}
	if err := failpoint(fail, "mount_rootfs"); err != nil {
		return err
	}
	switch strategy {
	case "classic", "classic-nonrec":
		if err := classicRootfs(root, inDir, strategy == "classic"); err != nil {
			return err
		}
	case "newapi":
		if err := newAPIRootfs(root, inDir); err != nil {
			return err
		}
	case "hostprep", "hostprep-norebind":
		// The launcher bind-mounted and remounted the rootfs on the host before
		// clone; the copies in this namespace are locked. Demonstrate that the
		// lock keeps them read-only even for namespace root.
		err := syscall.Mount("", root+"/usr", "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_NOSUID|syscall.MS_NODEV, "")
		log("hostprep: remount %s/usr rw from namespace root -> %v", root, errString(err))
		if strategy == "hostprep" {
			// The copied mounts are MNT_LOCKED in this namespace and pivot_root
			// refuses a locked new_root (EINVAL, see hostprep-norebind). A
			// recursive self-bind creates an unlocked top mount; the read-only
			// flags of the copies stay locked.
			if err := syscall.Mount(root, root, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
				return &stepErr{"mount_rootfs", fmt.Errorf("rebind locked root: %w", err)}
			}
			err = syscall.Mount("", root, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_NOSUID|syscall.MS_NODEV, "")
			log("hostprep: after rebind, remount root rw -> %v", errString(err))
		}
	default:
		return &stepErr{"mount_rootfs", fmt.Errorf("unknown strategy %q", strategy)}
	}
	if err := failpoint(fail, "mount_tmpfs"); err != nil {
		return err
	}
	tmpfs := []struct{ dst, data string }{
		{"/tmp", "mode=1777,size=64m"},
		{"/run", "mode=755,size=16m"},
		{"/out", "mode=755,size=64M,nr_inodes=1024,uid=1000,gid=1000"},
		{"/dev", "mode=755,size=64k"},
	}
	for _, t := range tmpfs {
		if err := syscall.Mount("tmpfs", root+t.dst, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, t.data); err != nil {
			return &stepErr{"mount_tmpfs", fmt.Errorf("%s: %w", t.dst, err)}
		}
	}
	for _, d := range []string{"null", "zero", "random", "urandom"} {
		p := root + "/dev/" + d
		if err := os.WriteFile(p, nil, 0o666); err != nil {
			return &stepErr{"mount_dev", err}
		}
		if err := syscall.Mount("/dev/"+d, p, "", syscall.MS_BIND, ""); err != nil {
			return &stepErr{"mount_dev", fmt.Errorf("%s: %w", d, err)}
		}
	}
	if err := syscall.Mount("", root+"/dev", "", syscall.MS_REMOUNT|syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, "mode=755,size=64k"); err != nil {
		return &stepErr{"mount_dev", fmt.Errorf("remount ro: %w", err)}
	}
	if err := extraMounts(root); err != nil {
		return err
	}
	if err := failpoint(fail, "mount_proc"); err != nil {
		return err
	}
	if err := syscall.Mount("proc", root+"/proc", "proc", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
		return &stepErr{"mount_proc", err}
	}
	if err := failpoint(fail, "mask_proc"); err != nil {
		return err
	}
	if err := maskProc(root, log); err != nil {
		return err
	}
	if strategy == "classic" || strategy == "classic-nonrec" {
		// Classic mount(2) cannot create a read-only bind in one step; the
		// rootfs stays writable until this remount.
		if err := syscall.Mount("", root, "", syscall.MS_BIND|syscall.MS_REMOUNT|roFlags, ""); err != nil {
			return &stepErr{"mount_rootfs", fmt.Errorf("remount root ro: %w", err)}
		}
	}
	return nil
}

// bindRO binds src at dst read-only/nosuid/nodev. Classic mount(2) needs one
// remount per mount: MS_REMOUNT is never recursive, so with rec every
// submount that came along is remounted individually, preserving its atime
// flags (locked in a user namespace; changing them is EPERM).
func bindRO(src, dst string, rec bool) error {
	fl := uintptr(syscall.MS_BIND)
	if rec {
		fl |= syscall.MS_REC
	}
	if err := syscall.Mount(src, dst, "", fl, ""); err != nil {
		return fmt.Errorf("bind %s (rec=%v): %w", src, rec, err)
	}
	for _, m := range mountsUnder(dst) {
		flags := uintptr(syscall.MS_BIND | syscall.MS_REMOUNT | roFlags)
		for _, o := range strings.Split(m.opts, ",") {
			switch o {
			case "noatime":
				flags |= syscall.MS_NOATIME
			case "relatime":
				flags |= syscall.MS_RELATIME
			case "nodiratime":
				flags |= syscall.MS_NODIRATIME
			case "noexec":
				flags |= syscall.MS_NOEXEC
			}
		}
		if err := syscall.Mount("", m.point, "", flags, ""); err != nil {
			return fmt.Errorf("remount ro %s: %w", m.point, err)
		}
		if !rec {
			break
		}
	}
	return nil
}

// mountsUnder returns the mounts at or below dst (outermost first), from
// this process's mountinfo.
func mountsUnder(dst string) []mountEntry {
	var out []mountEntry
	for _, m := range readMountinfo() {
		if m.point == dst || strings.HasPrefix(m.point, dst+"/") {
			out = append(out, m)
		}
	}
	if len(out) > 1 && out[0].point != dst {
		// keep dst first for the non-recursive case
		for i, m := range out {
			if m.point == dst {
				out[0], out[i] = out[i], out[0]
				break
			}
		}
	}
	return out
}

func classicRootfs(root, inDir string, rec bool) error {
	if err := syscall.Mount(root, root, "", syscall.MS_BIND, ""); err != nil {
		return &stepErr{"mount_rootfs", fmt.Errorf("bind template: %w", err)}
	}
	for _, b := range [][2]string{{"/usr", root + "/usr"}, {"/etc", root + "/etc"}, {inDir, root + "/in"}} {
		if err := bindRO(b[0], b[1], rec); err != nil {
			return &stepErr{"mount_rootfs", err}
		}
	}
	return nil
}

func openTree(path string, flags uintptr) (int, error) {
	p, err := syscall.BytePtrFromString(path)
	if err != nil {
		return -1, err
	}
	fd, _, e := syscall.RawSyscall(sysOpenTree, uintptr(^uint(99)), uintptr(unsafe.Pointer(p)), flags) // AT_FDCWD = -100
	if e != 0 {
		return -1, e
	}
	return int(fd), nil
}

func mountSetattr(dfd int, path string, flags uintptr, attr *mountAttr) error {
	p, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	_, _, e := syscall.RawSyscall6(sysMountSetattr, uintptr(dfd), uintptr(unsafe.Pointer(p)), flags, uintptr(unsafe.Pointer(attr)), unsafe.Sizeof(*attr), 0)
	if e != 0 {
		return e
	}
	return nil
}

func moveMount(fd int, dst string) error {
	empty, _ := syscall.BytePtrFromString("")
	d, err := syscall.BytePtrFromString(dst)
	if err != nil {
		return err
	}
	_, _, e := syscall.RawSyscall6(sysMoveMount, uintptr(fd), uintptr(unsafe.Pointer(empty)), uintptr(^uint(99)), uintptr(unsafe.Pointer(d)), moveMountFEmpty, 0)
	if e != 0 {
		return e
	}
	return nil
}

// attachRO clones src as a detached tree, makes it read-only/nosuid/nodev and
// private while still detached, then attaches it at dst: there is no moment
// at which the bind is visible and writable.
func attachRO(src, dst string) error {
	fd, err := openTree(src, openTreeClone|openTreeCloexec|atRecursive)
	if err != nil {
		return fmt.Errorf("open_tree %s: %w", src, err)
	}
	defer syscall.Close(fd)
	attr := mountAttr{AttrSet: mountAttrRdonly | mountAttrNosuid | mountAttrNodev, Propagation: syscall.MS_PRIVATE}
	if err := mountSetattr(fd, "", atEmptyPath|atRecursive, &attr); err != nil {
		return fmt.Errorf("mount_setattr %s: %w", src, err)
	}
	if err := moveMount(fd, dst); err != nil {
		return fmt.Errorf("move_mount %s -> %s: %w", src, dst, err)
	}
	return nil
}

func newAPIRootfs(root, inDir string) error {
	for _, b := range [][2]string{{root, root}, {"/usr", root + "/usr"}, {"/etc", root + "/etc"}, {inDir, root + "/in"}} {
		if err := attachRO(b[0], b[1]); err != nil {
			return &stepErr{"mount_rootfs", err}
		}
	}
	return nil
}

func maskProc(root string, log func(string, ...any)) error {
	for _, p := range procMaskPaths {
		target := root + p
		st, err := os.Stat(target)
		if errors.Is(err, os.ErrNotExist) {
			log("proc mask: %s absent on this kernel, skipped", p)
			continue
		}
		if err != nil {
			return &stepErr{"mask_proc", err}
		}
		if st.IsDir() {
			err = syscall.Mount("tmpfs", target, "tmpfs", syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, "size=0")
		} else {
			err = syscall.Mount(root+"/dev/null", target, "", syscall.MS_BIND, "")
		}
		if err != nil {
			return &stepErr{"mask_proc", fmt.Errorf("%s: %w", p, err)}
		}
	}
	for _, p := range procReadonlyPaths {
		target := root + p
		if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
			log("proc readonly: %s absent on this kernel, skipped", p)
			continue
		}
		if err := syscall.Mount(target, target, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
			return &stepErr{"mask_proc", fmt.Errorf("bind %s: %w", p, err)}
		}
		if err := syscall.Mount("", target, "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, ""); err != nil {
			return &stepErr{"mask_proc", fmt.Errorf("remount ro %s: %w", p, err)}
		}
	}
	return nil
}

// pivotInto makes root the new "/" and detaches the old root.
func pivotInto(root, fail string) error {
	if err := failpoint(fail, "pivot_root"); err != nil {
		return err
	}
	if err := syscall.Chdir(root); err != nil {
		return &stepErr{"pivot_root", err}
	}
	if err := syscall.PivotRoot(".", "."); err != nil {
		return &stepErr{"pivot_root", err}
	}
	if err := failpoint(fail, "umount_oldroot"); err != nil {
		return err
	}
	if os.Getenv("SPIKE_SKIP_DETACH") == "1" {
		// diagnostic only: keep the old root attached (stacked under the new root)
		return wrap("umount_oldroot", syscall.Chdir("/"))
	}
	if err := syscall.Unmount(".", syscall.MNT_DETACH); err != nil {
		return &stepErr{"umount_oldroot", err}
	}
	if err := syscall.Chdir("/"); err != nil {
		return &stepErr{"umount_oldroot", err}
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// extraMounts adds the §16.2 environment-specific mounts: the Gateway socket
// (orchestration environments only) and the session workspace.
func extraMounts(root string) error {
	if gw := os.Getenv("SPIKE_GW"); gw != "" {
		dir := root + "/run/agentbox"
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return &stepErr{"mount_gateway", err}
		}
		if err := os.WriteFile(dir+"/gateway.sock", nil, 0o600); err != nil {
			return &stepErr{"mount_gateway", err}
		}
		if err := syscall.Mount(gw, dir+"/gateway.sock", "", syscall.MS_BIND, ""); err != nil {
			return &stepErr{"mount_gateway", err}
		}
	}
	if ws := os.Getenv("SPIKE_WS"); ws != "" {
		if err := syscall.Mount(ws, root+"/workspace", "", syscall.MS_BIND, ""); err != nil {
			return &stepErr{"mount_workspace", err}
		}
		if err := syscall.Mount("", root+"/workspace", "", syscall.MS_BIND|syscall.MS_REMOUNT|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_RELATIME, ""); err != nil {
			return &stepErr{"mount_workspace", err}
		}
	}
	return nil
}
