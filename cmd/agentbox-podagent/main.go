//go:build linux

// Command agentbox-podagent is the helper baked into the worker image for the Kubernetes provider
// (internal/provider/k8s; design docs/design/2026-10-10-k8s-provider-design.md §2.1). It runs inside the
// sandbox Pod, as the Pod's unprivileged user:
//
//	agentbox-podagent init                                   PID 1: reap children; on SIGTERM kill everything
//	agentbox-podagent exec --id ID [--env K=V]... [--dir D] [--nofile N] [--fsize N] -- argv...
//	agentbox-podagent kill --id ID [--grace-ms N]            SIGTERM the exec's process group, SIGKILL after grace
//	agentbox-podagent status --id ID                         print the exec's exit status (JSON)
//	agentbox-podagent freeze|thaw [--timeout-ms N]           SIGSTOP/SIGCONT every process except PID 1, confirm
//	agentbox-podagent procs                                  print the container's pids (JSON)
//	agentbox-podagent diag                                   print cpu usage and oom kills of the container cgroup
//	agentbox-podagent shutdown                               SIGTERM PID 1: the container ends (fast path of Stop)
//
// The provider reaches it through the pods/exec API. exec writes one line "ABX-STARTED <pid>" (or
// "ABX-START-ERR <reason>") on stdout before relaying the workload's stdout, so the provider learns that the
// workload is running before any workload output.
//
// Trust boundary: the helper runs with the same uid and in the same PID namespace as the untrusted workload,
// so the workload can tamper with everything the helper produces inside the Pod — pid and status files in
// /run/agentbox-exec, the ack line timing, freeze/procs/diag answers — and can signal or kill
// helper processes. The provider therefore treats helper output only as advisory, for the workload's own
// attempt (e.g. its exit status), and relies on API-server facts for every safety decision: container state
// and exit (Stop, Destroy, OOMKilled), activeDeadlineSeconds for killing, Pod deletion for cleanup.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
