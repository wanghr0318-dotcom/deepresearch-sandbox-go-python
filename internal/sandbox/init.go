//go:build linux

package sandbox

import (
	"fmt"
	"os"
	"syscall"
)

// RunInit 是沙箱内 1 号进程的主函数。它永不正常返回。
//
// 调用时机：进程以 `/proc/self/exe init` 启动，且 main 在做任何
// 其他初始化之前就分流到这里。
func RunInit() error {
	// root 目前只做判空校验；它的值本身留给 Task 6 去做 pivot_root
	// （把这个目录切换成新的根文件系统），这里先不用，避免引入
	// 一个只声明未使用、又要用 _ 掩盖的半成品变量。
	root := os.Getenv(envSandboxRoot)
	if root == "" {
		return fmt.Errorf("缺少环境变量 %s", envSandboxRoot)
	}
	if h := os.Getenv(envSandboxHostname); h != "" {
		if err := syscall.Sethostname([]byte(h)); err != nil {
			return fmt.Errorf("设置沙箱 hostname: %w", err)
		}
	}
	// pivot_root 与伪文件系统挂载在 Task 6 补齐。
	// 收割与控制连接服务在 Task 7 / Task 8 补齐。
	return fmt.Errorf("init 尚未实现完整")
}
