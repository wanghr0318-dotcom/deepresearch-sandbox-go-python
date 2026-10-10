//go:build linux

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// reportHelper (in the helper process) starts a long-running child and writes "<child pid> <cwd>" to file.
func reportHelper(file string) {
	child := exec.Command("sleep", "300")
	if err := child.Start(); err != nil {
		os.Exit(3)
	}
	wd, _ := os.Getwd()
	if err := os.WriteFile(file, []byte(strconv.Itoa(child.Process.Pid)+" "+wd), 0o600); err != nil {
		os.Exit(3)
	}
}

func alive(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return false
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	return err == nil && !strings.Contains(string(b), ") Z ")
}

// Closing the hub (and a timed-out request) kills the stdio server's whole process group; the server runs in an
// empty temp working directory by default, or in the configured one.
func TestStdioProcessTreeContained(t *testing.T) {
	for _, dir := range []string{"", t.TempDir()} {
		report := filepath.Join(t.TempDir(), "report")
		cfg := stdioCfg("calculate")
		cfg.Env["MCPDEMO_REPORT"] = report
		cfg.Dir = dir
		h, err := NewHub(Config{Servers: []ServerConfig{cfg}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, uerr := h.Call(context.Background(), "calc", "calculate", json.RawMessage(`{"expression":"1+1"}`)); uerr != nil {
			t.Fatal(uerr)
		}
		b, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(b))
		pid, _ := strconv.Atoi(fields[0])
		wd := fields[1]
		if dir != "" && wd != dir {
			t.Fatalf("cwd %q, want %q", wd, dir)
		}
		if dir == "" && (!strings.Contains(wd, "agentbox-mcp-calc-") || wd == "/") {
			t.Fatalf("default cwd %q", wd)
		}
		if !alive(pid) {
			t.Fatalf("child %d not running before close", pid)
		}
		h.Close()
		deadline := time.Now().Add(5 * time.Second)
		for alive(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("child %d of the stdio server survived Close", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if dir == "" {
			if _, err := os.Stat(wd); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temp working directory %s not removed", wd)
			}
		}
	}
}
