//go:build !linux

package mcp

import (
	"errors"
	"os/exec"
)

// contain is Linux-only (process group, Pdeathsig, credentials); elsewhere uid/gid cannot be honoured.
func contain(_ *exec.Cmd, cfg ServerConfig) error {
	if cfg.UID != nil || cfg.GID != nil {
		return errors.New("mcp: uid/gid for stdio servers is supported on Linux only")
	}
	return nil
}

func killTree(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill() // already exiting is fine
	}
}

func chownTo(string, ServerConfig) error { return nil }
