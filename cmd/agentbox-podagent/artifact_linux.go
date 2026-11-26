//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"syscall"
)

// readArtifact is called in the sandbox container; the runner verifies the
// resulting size and digest against the artifact event before registering it.
func readArtifact(args []string, dst io.Writer) error {
	if len(args) != 2 {
		return fmt.Errorf("read-artifact requires a relative path and byte limit")
	}
	limit, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || limit < 1 {
		return fmt.Errorf("invalid artifact byte limit %q", args[1])
	}
	workspace, err := os.OpenRoot("/workspace/out")
	if err != nil {
		return err
	}
	defer workspace.Close()
	file, err := workspace.OpenFile(args[0], os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("artifact is not a regular file")
	}
	_, err = io.Copy(dst, io.LimitReader(file, limit))
	return err
}
