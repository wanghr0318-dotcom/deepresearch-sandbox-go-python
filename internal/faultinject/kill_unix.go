//go:build unix

package faultinject

import (
	"os"
	"syscall"
)

// kill 以 SIGKILL 杀死本进程：不运行任何收尾（defer、信号处理、连接关闭），与崩溃相同。
func kill() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {} // 信号送达前不继续执行
}
