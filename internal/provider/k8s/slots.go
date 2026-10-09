package k8s

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// Slot layout (design §2.2): <root>/<pod>/workspace → /workspace, <root>/<pod>/run → /run/agentbox.
const (
	slotWorkspace = "workspace"
	slotRun       = "run"
	gatewayName   = "gateway.sock"
	restoreName   = "restore"
)

// slots manages the host side of the per-Pod shared directories. hostRoot is the directory as seen by
// this process, nodeRoot the same directory as seen by the node (hostPath); with the documented
// deployment both are the same path.
type slots struct {
	hostRoot, nodeRoot string
	uid                int // owner of the slot directories and the Gateway socket link; <0 skips chown
}

func (s slots) enabled() bool              { return s.hostRoot != "" }
func (s slots) hostDir(pod string) string  { return filepath.Join(s.hostRoot, pod) }
func (s slots) nodeDir(pod string) string  { return filepath.ToSlash(filepath.Join(s.nodeRoot, pod)) }
func (s slots) workspace(pod string) string { return filepath.Join(s.hostDir(pod), slotWorkspace) }

func (s slots) chown(p string) error {
	if s.uid < 0 {
		return nil
	}
	return os.Lchown(p, s.uid, s.uid)
}

// create makes the slot directories of a new Pod (before the Pod exists, so the hostPath volumes resolve).
func (s slots) create(pod string) error {
	for _, d := range []string{s.hostDir(pod), filepath.Join(s.hostDir(pod), slotWorkspace), filepath.Join(s.hostDir(pod), slotRun)} {
		if err := os.Mkdir(d, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("k8s: slot %s: %w", d, err)
		}
		if d != s.hostDir(pod) {
			if err := s.chown(d); err != nil {
				return fmt.Errorf("k8s: slot %s: %w", d, err)
			}
		}
	}
	return nil
}

// bind connects an environment's host-side mounts to the Pod's slot: the Gateway socket and restore files
// are hard-linked into run/, the workspace's entries are moved into the slot and the workspace path becomes
// a symlink to it.
func (s slots) bind(pod string, m provider.Mounts) error {
	run := filepath.Join(s.hostDir(pod), slotRun)
	if m.GatewaySocket != "" {
		link := filepath.Join(run, gatewayName)
		_ = os.Remove(link)
		if err := os.Link(m.GatewaySocket, link); err != nil {
			return fmt.Errorf("k8s: link Gateway socket into slot: %w", err)
		}
		if err := s.chown(link); err != nil {
			return fmt.Errorf("k8s: chown Gateway socket: %w", err)
		}
	}
	if m.RestoreDir != "" {
		dst := filepath.Join(run, restoreName)
		if err := os.Mkdir(dst, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("k8s: restore slot: %w", err)
		}
		ents, err := os.ReadDir(m.RestoreDir)
		if err != nil {
			return fmt.Errorf("k8s: restore dir: %w", err)
		}
		for _, e := range ents {
			if !e.Type().IsRegular() {
				continue
			}
			if err := os.Link(filepath.Join(m.RestoreDir, e.Name()), filepath.Join(dst, e.Name())); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("k8s: link restore file: %w", err)
			}
		}
	}
	if m.Workspace != "" {
		return s.swingWorkspace(pod, m.Workspace)
	}
	return nil
}

// swingWorkspace moves the workspace's content into the Pod's slot and points the workspace path at it.
// The workspace is either a directory (first environment) or a symlink to an earlier Pod's slot.
func (s slots) swingWorkspace(pod, ws string) error {
	target := s.workspace(pod)
	fi, err := os.Lstat(ws)
	if err != nil {
		return fmt.Errorf("k8s: workspace: %w", err)
	}
	var src string
	switch {
	case fi.Mode()&os.ModeSymlink != 0:
		if src, err = os.Readlink(ws); err != nil {
			return fmt.Errorf("k8s: workspace: %w", err)
		}
		if src == target {
			return nil
		}
	case fi.IsDir():
		src = ws
	default:
		return fmt.Errorf("k8s: workspace %s is neither a directory nor a symlink", ws)
	}
	if ents, err := os.ReadDir(src); err == nil {
		for _, e := range ents {
			if err := os.Rename(filepath.Join(src, e.Name()), filepath.Join(target, e.Name())); err != nil {
				return fmt.Errorf("k8s: move workspace entry %s: %w", e.Name(), err)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("k8s: read workspace: %w", err)
	}
	if src == ws {
		if err := os.Remove(ws); err != nil {
			return fmt.Errorf("k8s: replace workspace directory: %w", err)
		}
		if err := os.Symlink(target, ws); err != nil {
			return fmt.Errorf("k8s: workspace symlink: %w", err)
		}
		return nil
	}
	tmp := ws + ".swing-" + pod
	_ = os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return fmt.Errorf("k8s: workspace symlink: %w", err)
	}
	if err := os.Rename(tmp, ws); err != nil {
		return fmt.Errorf("k8s: workspace symlink: %w", err)
	}
	// The previous slot's workspace is now empty; drop it and its slot directory if nothing else is left.
	if filepath.Dir(filepath.Dir(src)) == filepath.Clean(s.hostRoot) {
		_ = os.Remove(src)
		_ = os.Remove(filepath.Dir(src))
	}
	return nil
}

// unbindRun removes the Gateway socket link and restore links of a stopped Pod.
func (s slots) unbindRun(pod string) error {
	run := filepath.Join(s.hostDir(pod), slotRun)
	if err := os.Remove(filepath.Join(run, gatewayName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.RemoveAll(filepath.Join(run, restoreName)); err != nil {
		return err
	}
	return nil
}

// remove deletes a destroyed Pod's slot, except a workspace that ws (the environment's workspace path)
// still points to: that directory now holds the workspace's content.
func (s slots) remove(pod, ws string) error {
	if err := os.RemoveAll(filepath.Join(s.hostDir(pod), slotRun)); err != nil {
		return err
	}
	if ws != "" {
		if dst, err := os.Readlink(ws); err == nil && dst == s.workspace(pod) {
			return nil
		}
	}
	if err := os.RemoveAll(s.workspace(pod)); err != nil {
		return err
	}
	if err := os.Remove(s.hostDir(pod)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
