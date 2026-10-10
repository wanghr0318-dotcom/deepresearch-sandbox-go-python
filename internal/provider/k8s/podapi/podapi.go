// Package podapi is the wire contract between the Kubernetes provider (internal/provider/k8s) and the
// in-image helper cmd/agentbox-podagent. It has no dependencies so the helper binary stays small.
package podapi

import (
	"fmt"
	"strconv"
	"strings"
)

// Binary is the helper's path inside the worker image.
const Binary = "/opt/agentbox/bin/agentbox-podagent"

// ExecDir is the per-Pod memory-backed directory holding exec pid and status files.
const ExecDir = "/run/agentbox-exec"

// Acknowledgement lines written by `exec` on stdout before any workload output.
const (
	AckStarted  = "ABX-STARTED "
	AckStartErr = "ABX-START-ERR "
)

// StartErrCode is the exit code of `exec` when the workload could not be started.
const StartErrCode = 127

// Status is the content of <ExecDir>/<id>.status and the output of `status`.
type Status struct {
	Code   int `json:"code"`
	Signal int `json:"signal"`
}

// Diag is the output of `diag`.
type Diag struct {
	CPUUsageUsec uint64 `json:"cpu_usage_usec"`
	OOMKill      uint64 `json:"oom_kill"`
}

// ParseAck parses the first stdout line of `exec` (without the trailing newline). started reports
// whether the workload runs; for a start error reason is the helper's message.
func ParseAck(line string) (started bool, pid int, reason string, err error) {
	switch {
	case strings.HasPrefix(line, AckStarted):
		pid, err = strconv.Atoi(strings.TrimPrefix(line, AckStarted))
		if err != nil || pid <= 0 {
			return false, 0, "", fmt.Errorf("podapi: malformed ack %q", line)
		}
		return true, pid, "", nil
	case strings.HasPrefix(line, AckStartErr):
		return false, 0, strings.TrimPrefix(line, AckStartErr), nil
	default:
		return false, 0, "", fmt.Errorf("podapi: unexpected first line %q", line)
	}
}

// ExecArgv builds the `exec` command line.
func ExecArgv(id string, env []string, dir string, nofile, fsize uint64, argv []string) []string {
	out := []string{Binary, "exec", "--id", id}
	for _, kv := range env {
		out = append(out, "--env", kv)
	}
	if dir != "" {
		out = append(out, "--dir", dir)
	}
	if nofile > 0 {
		out = append(out, "--nofile", strconv.FormatUint(nofile, 10))
	}
	if fsize > 0 {
		out = append(out, "--fsize", strconv.FormatUint(fsize, 10))
	}
	out = append(out, "--")
	return append(out, argv...)
}
