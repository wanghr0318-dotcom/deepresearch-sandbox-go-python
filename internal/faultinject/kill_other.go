//go:build !unix

package faultinject

import "os"

// kill 立即终止本进程（非 Unix 平台只为能够编译；故障实验只在 Linux 上运行）。
func kill() {
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Kill()
	}
	os.Exit(137)
}
