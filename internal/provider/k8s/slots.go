package k8s

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/provider"
)

// Slot layout (design §2.2): <root>/<pod>/workspace → /workspace, <root>/<pod>/run → /run/agentbox.
const (
	slotWorkspace = "workspace"
	slotRun       = "run"
	gatewayName   = "gateway.sock"
	restoreName   = "restore"
	// workspaceRef (host side only, not mounted into the Pod) records the workspace path whose symlink points
	// at this slot's workspace; gc uses it to decide whether the slot still holds live workspace data.
	workspaceRef = "workspace.ref"
)

// slots manages the host side of the per-Pod shared directories. hostRoot is the directory as seen by
// this process, nodeRoot the same directory as seen by the node (hostPath); with the documented
// deployment both are the same path.
type slots struct {
	hostRoot, nodeRoot string
	uid                int // owner of the slot directories and the Gateway socket link; <0 skips chown
}

func (s slots) enabled() bool               { return s.hostRoot != "" }
func (s slots) hostDir(pod string) string   { return filepath.Join(s.hostRoot, pod) }
func (s slots) nodeDir(pod string) string   { return filepath.ToSlash(filepath.Join(s.nodeRoot, pod)) }
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
	// The back-reference is written before any entry moves: if the process dies mid-swing, referenced()
	// keeps this slot (non-empty workspace, workspace path still present) instead of letting gc delete data.
	if err := os.WriteFile(filepath.Join(s.hostDir(pod), workspaceRef), []byte(ws), 0o600); err != nil {
		return fmt.Errorf("k8s: workspace back-reference: %w", err)
	}
	var moved []string
	rollback := func(cause error) error {
		for i := len(moved) - 1; i >= 0; i-- {
			if err := os.Rename(filepath.Join(target, moved[i]), filepath.Join(src, moved[i])); err != nil {
				return fmt.Errorf("%w (rollback of %s failed: %v; data is in %s)", cause, moved[i], err, target)
			}
		}
		return cause
	}
	if ents, err := os.ReadDir(src); err == nil {
		for _, e := range ents {
			if err := os.Rename(filepath.Join(src, e.Name()), filepath.Join(target, e.Name())); err != nil {
				return rollback(fmt.Errorf("k8s: move workspace entry %s: %w", e.Name(), err))
			}
			moved = append(moved, e.Name())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("k8s: read workspace: %w", err)
	}
	if src == ws {
		if err := os.Remove(ws); err != nil {
			return rollback(fmt.Errorf("k8s: replace workspace directory: %w", err))
		}
		if err := os.Symlink(target, ws); err != nil {
			if merr := os.Mkdir(ws, 0o700); merr != nil {
				return fmt.Errorf("k8s: workspace symlink: %w (and recreating %s: %v; data is in %s)", err, ws, merr, target)
			}
			return rollback(fmt.Errorf("k8s: workspace symlink: %w", err))
		}
		return nil
	}
	tmp := ws + ".swing-" + pod
	_ = os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return rollback(fmt.Errorf("k8s: workspace symlink: %w", err))
	}
	if err := os.Rename(tmp, ws); err != nil {
		_ = os.Remove(tmp)
		return rollback(fmt.Errorf("k8s: workspace symlink: %w", err))
	}
	// The previous slot's workspace is now empty; drop it, its back-reference and its slot directory if
	// nothing else is left.
	if old := filepath.Dir(src); filepath.Dir(old) == filepath.Clean(s.hostRoot) {
		_ = os.Remove(src)
		_ = os.Remove(filepath.Join(old, workspaceRef))
		_ = os.Remove(old)
	}
	return nil
}

// referenced reports whether the slot's workspace may still hold a workspace's data: its back-reference names
// a path that still exists and either is a symlink to this slot, or the slot's workspace is not empty (an
// interrupted swing: some entries were moved here but the workspace path was not switched yet). Only when
// the workspace path is gone (e.g. a closed session deleted it), or the slot is empty and unreferenced, may
// the slot be deleted.
func (s slots) referenced(pod string) bool {
	b, err := os.ReadFile(filepath.Join(s.hostDir(pod), workspaceRef))
	if err != nil {
		return false
	}
	ws := string(b)
	if _, err := os.Lstat(ws); err != nil {
		return false
	}
	if dst, err := os.Readlink(ws); err == nil && dst == s.workspace(pod) {
		return true
	}
	ents, err := os.ReadDir(s.workspace(pod))
	return err == nil && len(ents) > 0
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

// remove deletes a destroyed Pod's slot, except a workspace that its workspace path still points to: that
// directory now holds the workspace's content (gc removes it once the workspace path is gone).
func (s slots) remove(pod string) error {
	if err := os.RemoveAll(filepath.Join(s.hostDir(pod), slotRun)); err != nil {
		return err
	}
	if s.referenced(pod) {
		return nil
	}
	return os.RemoveAll(s.hostDir(pod))
}

// gc removes slot directories that belong to no Pod in live and whose workspace is no longer referenced (for
// example a closed session deleted its workspace symlink). Slots younger than minAge are skipped: a slot is
// created just before its Pod. Returns the removed slot names.
func (s slots) gc(live map[string]bool, minAge time.Duration) ([]string, error) {
	ents, err := os.ReadDir(s.hostRoot)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, e := range ents {
		name := e.Name()
		if !e.IsDir() || live[name] || s.referenced(name) {
			continue
		}
		if fi, err := e.Info(); err != nil || time.Since(fi.ModTime()) < minAge {
			continue
		}
		if err := os.RemoveAll(s.hostDir(name)); err != nil {
			return removed, err
		}
		removed = append(removed, name)
	}
	return removed, nil
}

// recoverSwings merges back the entries of interrupted swings (the process died after moving some workspace
// entries into a new slot but before switching the workspace path to it). It runs at startup, before any
// environment is created. A slot needs it when its back-reference names a workspace path that still exists and
// points elsewhere (a directory, or a symlink to another slot), and its own workspace is not empty: those
// entries are moved back to where the workspace path resolves. An entry whose name already exists there is
// left in the slot and logged (the slot stays referenced, so gc keeps it). Returns the number of entries moved.
func (s slots) recoverSwings(log *slog.Logger) int {
	ents, err := os.ReadDir(s.hostRoot)
	if err != nil {
		return 0
	}
	moved := 0
	for _, e := range ents {
		if e.IsDir() {
			moved += s.recoverSwing(e.Name(), log)
		}
	}
	return moved
}

func (s slots) recoverSwing(pod string, log *slog.Logger) int {
	b, err := os.ReadFile(filepath.Join(s.hostDir(pod), workspaceRef))
	if err != nil {
		return 0
	}
	ws, own := string(b), s.workspace(pod)
	fi, err := os.Lstat(ws)
	if err != nil {
		return 0 // the workspace is gone: gc decides
	}
	dest := ws
	if fi.Mode()&os.ModeSymlink != 0 {
		if dest, err = os.Readlink(ws); err != nil || dest == own {
			return 0 // complete swing: the workspace lives in this slot
		}
		if di, err := os.Stat(dest); err != nil || !di.IsDir() {
			log.Warn("k8s: interrupted swing: workspace target is not a directory", "slot", pod, "workspace", ws, "target", dest)
			return 0
		}
	} else if !fi.IsDir() {
		return 0
	}
	items, err := os.ReadDir(own)
	if err != nil || len(items) == 0 {
		return 0
	}
	moved := 0
	for _, it := range items {
		dst := filepath.Join(dest, it.Name())
		if _, err := os.Lstat(dst); err == nil {
			log.Warn("k8s: interrupted swing: entry exists in the workspace, left in the slot", "slot", pod, "entry", it.Name(), "workspace", ws)
			continue
		}
		if err := os.Rename(filepath.Join(own, it.Name()), dst); err != nil {
			log.Warn("k8s: interrupted swing: move back", "slot", pod, "entry", it.Name(), "err", err)
			continue
		}
		moved++
	}
	log.Info("k8s: merged back an interrupted workspace swing", "slot", pod, "workspace", ws, "entries", moved)
	return moved
}
