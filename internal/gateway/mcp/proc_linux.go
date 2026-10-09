//go:build linux

package mcp

import (
	"os"
	"os/exec"
	"syscall"
)

// contain puts the stdio server in its own process group (so a kill reaches its children), kills it when the
// Gateway's thread that started it dies (Pdeathsig), and drops to the configured uid/gid.
func contain(cmd *exec.Cmd, cfg ServerConfig) error {
	attr := &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if cfg.UID != nil || cfg.GID != nil {
		uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
		if cfg.UID != nil {
			uid = *cfg.UID
		}
		if cfg.GID != nil {
			gid = *cfg.GID
		}
		attr.Credential = &syscall.Credential{Uid: uid, Gid: gid, NoSetGroups: false, Groups: []uint32{}}
	}
	cmd.SysProcAttr = attr
	return nil
}

// killTree kills the server's whole process group.
func killTree(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // the group may already be gone
	_ = cmd.Process.Kill()
}

// chownTo gives the server's temp working directory to its configured uid/gid.
func chownTo(dir string, cfg ServerConfig) error {
	if cfg.UID == nil && cfg.GID == nil {
		return nil
	}
	uid, gid := -1, -1
	if cfg.UID != nil {
		uid = int(*cfg.UID)
	}
	if cfg.GID != nil {
		gid = int(*cfg.GID)
	}
	return os.Chown(dir, uid, gid)
}
